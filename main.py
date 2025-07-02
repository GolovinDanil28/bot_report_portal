import requests
import telegram
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackContext,
    ContextTypes
)
import logging
from datetime import datetime, time
import os
from dotenv import load_dotenv
import pytz
import asyncio

# Конфигурация
load_dotenv()

# Проверка обязательных переменных окружения
required_env_vars = [
    "REPORT_PORTAL_USERNAME",
    "REPORT_PORTAL_PASSWORD",
    "TELEGRAM_TOKEN",
    "TELEGRAM_CHAT_ID"
]

for var in required_env_vars:
    if not os.getenv(var):
        logging.error(f"Отсутствует обязательная переменная окружения: {var}")
        exit(1)

REPORTPORTAL_URL = "https://reportportal.a2nta.ru"
AUTH_URL = f"{REPORTPORTAL_URL}/uat/sso/oauth/token"
SUPERADMIN_LAUNCHES_URL = f"{REPORTPORTAL_URL}/api/v1/superadmin_personal/launch"
LINUX_LAUNCHES_URL = f"{REPORTPORTAL_URL}/api/v1/linux_tests/launch"
AUTH_HEADERS = {
    "Authorization": "Basic dWk6dWltYW4=",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/x-www-form-urlencoded"
}
AUTH_DATA = {
    "grant_type": "password",
    "username": os.getenv("REPORT_PORTAL_USERNAME"),
    "password": os.getenv("REPORT_PORTAL_PASSWORD")
}
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID"))  # Конвертируем в число

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Отключение SSL-предупреждений
requests.packages.urllib3.disable_warnings()

def get_access_token():
    """Получаем access_token от ReportPortal"""
    try:
        response = requests.post(
            AUTH_URL,
            headers=AUTH_HEADERS,
            data=AUTH_DATA,
            verify=False
        )
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        logger.error(f"Ошибка при получении токена: {str(e)}")
        return None


def get_filtered_launches(access_token, endpoint_url, is_linux=False):
    """Получаем и фильтруем запуски для указанного endpoint"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json, text/plain, */*"
    }
    params = {
        "ids": "",
        "page.page": 1,
        "page.size": 50,
        "page.sort": "startTime,number,DESC"
    }

    try:
        response = requests.get(endpoint_url, headers=headers, params=params, verify=False)
        response.raise_for_status()
        launches = response.json().get("content", [])

        if is_linux:
            # Собираем все подходящие Linux прогоны
            linux_launches = []
            for launch in launches:
                attributes = launch.get("attributes", [])
                has_os = False
                has_db = False

                for attr in attributes:
                    if attr.get("key") == "OS" and attr.get("value") == "Linux":
                        has_os = True
                    elif attr.get("key") == "Database" and attr.get("value") == "PostgreSQL":
                        has_db = True

                # Отбираем только Linux/PostgreSQL прогоны
                if has_os and has_db:
                    linux_launches.append(launch)

            # Возвращаем два последних прогона
            return linux_launches[:2]

        else:
            # Оригинальная логика для superadmin_personal
            last_30 = None
            last_29 = None

            for launch in launches:
                attributes = launch.get("attributes", [])
                has_full_version = False
                has_relaunch = False
                has_db_type = False
                full_version = None

                for attr in attributes:
                    if attr.get("key") == "FullVersion":
                        full_version = attr.get("value")
                        if full_version and (
                                full_version.startswith("3.30") or
                                full_version.startswith("3.29")
                        ):
                            has_full_version = True
                    elif attr.get("key") == "Re-launch" and attr.get("value") == "true":
                        has_relaunch = True
                    elif attr.get("key") == "Db type" and attr.get("value") == "postgres":
                        has_db_type = True

                if has_full_version and has_relaunch and has_db_type:
                    if full_version.startswith("3.30") and (last_30 is None or
                                                            datetime.fromisoformat(
                                                                launch["startTime"].replace('Z', '+00:00')) >
                                                            datetime.fromisoformat(
                                                                last_30["startTime"].replace('Z', '+00:00'))):
                        last_30 = launch
                    elif full_version.startswith("3.29") and (last_29 is None or
                                                              datetime.fromisoformat(
                                                                  launch["startTime"].replace('Z', '+00:00')) >
                                                              datetime.fromisoformat(
                                                                  last_29["startTime"].replace('Z', '+00:00'))):
                        last_29 = launch

            return [launch for launch in [last_30, last_29] if launch]

    except Exception as e:
        logger.error(f"Ошибка при получении запусков: {e}")
        return []


def get_defect_links(access_token: str, launch_id: str, project: str = "superadmin_personal"):
    """Получаем список уникальных ссылок на дефекты для указанного launch_id"""
    url = f"{REPORTPORTAL_URL}/api/v1/{project}/item/v2"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    params = {
        "page.page": 1,
        "page.size": 100,  # Увеличиваем размер страницы для получения всех дефектов
        "page.sort": "startTime,ASC",
        "filter.eq.hasStats": "true",
        "filter.eq.hasChildren": "false",
        "filter.in.issueType": "pb001",
        "providerType": "launch",
        "launchId": launch_id
    }

    try:
        response = requests.get(url, headers=headers, params=params, verify=False)
        response.raise_for_status()

        defects = response.json().get("content", [])
        links = set()

        for defect in defects:
            issue = defect.get("issue", {})
            if issue.get("issueType") == "pb001":
                comment = issue.get("comment", "")
                if comment and comment.startswith("https://a2nta.ru/Issues/"):
                    links.add(comment)

        # Обработка пагинации, если результатов больше 100
        total_pages = response.json().get("page", {}).get("totalPages", 1)
        if total_pages > 1:
            for page in range(2, total_pages + 1):
                params["page.page"] = page
                response = requests.get(url, headers=headers, params=params, verify=False)
                response.raise_for_status()
                for defect in response.json().get("content", []):
                    issue = defect.get("issue", {})
                    if issue.get("issueType") == "pb001":
                        comment = issue.get("comment", "")
                        if comment and comment.startswith("https://a2nta.ru/Issues/"):
                            links.add(comment)

        logger.info(f"Найдено {len(links)} дефектов для launch_id {launch_id}")
        return sorted(links)

    except Exception as e:
        logger.error(f"Ошибка при получении дефектов: {e}", exc_info=True)
        return []


def format_statistics(launch, launch_type):
    """Форматируем статистику для вывода"""
    if not launch:
        return f"{launch_type}: нет данных о запуске"

    stats = launch.get("statistics", {}).get("executions", {})

    # Извлекаем версию и ветку
    version_key = "Version" if "Linux" in launch_type else "FullVersion"
    version = "Не указана"
    branch = "Не указана"

    for attr in launch.get("attributes", []):
        if attr.get("key") == version_key:
            version = attr.get("value")
        elif attr.get("key") == "Branch":
            branch = attr.get("value")

    project = "linux_tests" if "Linux" in launch_type else "superadmin_personal"

    return (
        f"{launch_type}\n"
        f"ID запуска: {launch.get('id')}\n"
        f"Версия: {version}\n"
        f"Ветка: {branch}\n"  # Добавлена ветка
        f"Название: {launch.get('name')}\n"
        f"Всего тестов: {stats.get('total', 0)}\n"
        f"Пройдено: {stats.get('passed', 0)}\n"
        f"Провалено: {stats.get('failed', 0)}\n"
        f"Пропущено: {stats.get('skipped', 0)}\n"
        f"Статус: {launch.get('status')}\n"
        f"Время начала: {launch.get('startTime')}\n"
        f"Ссылка: {REPORTPORTAL_URL}/ui/#{project}/launches/all/{launch.get('id')}\n"
    )


async def send_report_to_chat(context: CallbackContext, chat_id: int):
    """Функция для отправки отчета в указанный чат"""
    try:
        access_token = get_access_token()
        logger.info(f"Полученный токен: {access_token}")

        if not access_token:
            await context.bot.send_message(
                chat_id=chat_id,
                text="❌ Не удалось получить access_token"
            )
            return

        # Собираем информацию о запусках
        main_launches = get_filtered_launches(access_token, SUPERADMIN_LAUNCHES_URL)
        linux_launches = get_filtered_launches(access_token, LINUX_LAUNCHES_URL, is_linux=True)

        # Логирование информации о найденных запусках
        logger.info(f"Основные прогоны: {[l.get('id') for l in main_launches]}")
        logger.info(f"Linux прогоны: {[l.get('id') for l in linux_launches]}")

        # Получаем ID для разных версий
        version_ids = {
            "3.30": None,
            "3.29": None
        }

        for launch in main_launches:
            version = next(
                (attr.get("value") for attr in launch.get("attributes", [])
                 if attr.get("key") == "FullVersion"),
                None
            )
            if version and version.startswith("3.30"):
                version_ids["3.30"] = launch.get("id")
            elif version and version.startswith("3.29"):
                version_ids["3.29"] = launch.get("id")

        logger.info(f"Найденные ID версий: {version_ids}")

        # Формируем основной отчет
        report_parts = ["📊 <b>Ежедневный отчет о тестировании</b> 📊"]

        # Добавляем информацию о прогонах
        for launch_type, launches in [("Основные", main_launches), ("Linux", linux_launches)]:
            if launches:
                for launch in launches:
                    report_parts.append(format_statistics(launch, f"{launch_type} прогон"))
            else:
                report_parts.append(f"⚠️ {launch_type} прогоны не найдены")

        # Отправляем основной отчет частями
        current_message = []
        for part in report_parts:
            if len("\n\n".join(current_message + [part])) > 4096:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="\n\n".join(current_message),
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
                current_message = [part]
            else:
                current_message.append(part)

        if current_message:
            await context.bot.send_message(
                chat_id=chat_id,
                text="\n\n".join(current_message),
                parse_mode="HTML",
                disable_web_page_preview=True
            )

        # Отправляем дефекты для основных версий
        for version, launch_id in version_ids.items():
            if launch_id:
                defects = get_defect_links(access_token, launch_id)
                logger.info(f"Дефекты для {version}: {defects}")
                if defects:
                    message = [
                        f"🔴 <b>Список дефектов {version}:</b>",
                        *defects
                    ]
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="\n".join(message),
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🟢 Для версии {version} дефектов не найдено",
                        parse_mode="HTML"
                    )

        # ДОБАВЛЕНО: Отправляем дефекты для Linux прогонов
        if linux_launches:
            for launch in linux_launches:
                # Извлекаем информацию о ветке и версии для заголовка
                branch = "Не указана"
                version = "Не указана"
                for attr in launch.get("attributes", []):
                    if attr.get("key") == "Branch":
                        branch = attr.get("value")
                    elif attr.get("key") == "Version":
                        version = attr.get("value")

                defects = get_defect_links(access_token, launch.get("id"), project="linux_tests")
                logger.info(f"Дефекты для Linux прогона (ID: {launch.get('id')}): {defects}")

                if defects:
                    message = [
                        f"🔴 <b>Список дефектов Linux (Ветка: {branch}, Версия: {version}):</b>",
                        *defects
                    ]
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="\n".join(message),
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🟢 Для Linux прогона (Ветка: {branch}, Версия: {version}) дефектов не найдено",
                        parse_mode="HTML"
                    )

        logger.info("Отчет успешно отправлен в канал")
    except telegram.error.BadRequest as e:
        logger.error(f"Ошибка Telegram API: {e.message}")
        if "Chat not found" in str(e):
            logger.error("Проверьте правильность TELEGRAM_CHAT_ID")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🚨 Произошла ошибка: {str(e)}"
        )

async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /report"""
    await send_report_to_chat(context, update.effective_chat.id)


async def daily_report(context: CallbackContext):
    """Ежедневная отправка отчета"""
    await send_report_to_chat(context, TELEGRAM_CHAT_ID)


#async def post_init(application):
    """Действия после инициализации бота"""
    #job_queue = application.job_queue
    #job_queue.run_daily(daily_report, time=DAILY_REPORT_TIME)
    #await daily_report(application)


def main():
    """Запуск бота для одноразовой отправки отчета"""
    try:
        application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(send_report_to_chat(application, TELEGRAM_CHAT_ID))

        application.stop()
        application.shutdown()

    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}")
        exit(1)


if __name__ == '__main__':
    main()