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
from datetime import datetime, timedelta
from functools import wraps
import time as time_module

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


# Декоратор для повторных попыток
def retry_with_backoff(max_retries=3, backoff_factor=2, exceptions=(Exception,)):
    """Декоратор для повторных попыток с экспоненциальной задержкой"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries == max_retries:
                        logger.error(f"Функция {func.__name__} упала после {max_retries} попыток: {str(e)}")
                        raise

                    wait_time = backoff_factor ** retries
                    logger.warning(f"Попытка {retries}/{max_retries} не удалась для {func.__name__}. "
                                   f"Ждем {wait_time} секунд перед повторной попыткой. Ошибка: {str(e)}")
                    time_module.sleep(wait_time)
            return None

        return wrapper

    return decorator


def retry_with_backoff_async(max_retries=3, backoff_factor=2, exceptions=(Exception,)):
    """Асинхронный декоратор для повторных попыток с экспоненциальной задержкой"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries == max_retries:
                        logger.error(f"Функция {func.__name__} упала после {max_retries} попыток: {str(e)}")
                        raise

                    wait_time = backoff_factor ** retries
                    logger.warning(f"Попытка {retries}/{max_retries} не удалась для {func.__name__}. "
                                   f"Ждем {wait_time} секунд перед повторной попыткой. Ошибка: {str(e)}")
                    await asyncio.sleep(wait_time)
            return None

        return wrapper

    return decorator


@retry_with_backoff(max_retries=3, exceptions=(requests.exceptions.Timeout, requests.exceptions.ConnectionError))
def get_access_token():
    """Получаем access_token от ReportPortal"""
    try:
        response = requests.post(
            AUTH_URL,
            headers=AUTH_HEADERS,
            data=AUTH_DATA,
            verify=False,
            timeout=30
        )
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        logger.error(f"Ошибка при получении токена: {str(e)}")
        raise


@retry_with_backoff(max_retries=3, exceptions=(requests.exceptions.Timeout, requests.exceptions.ConnectionError))
def get_filtered_launches(access_token, endpoint_url, is_linux=False):
    """Получаем и фильтруем запуски для указанного endpoint"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json, text/plain, */*"
    }

    if is_linux:
        # Для Linux: 36 часов назад
        time_filter = (datetime.now() - timedelta(hours=36)).isoformat() + 'Z'
        # Параметры для Linux запусков
        params = {
            "ids": "",
            "page.page": 1,
            "page.size": 50,
            "page.sort": "startTime,number,DESC",
            "filter.gt.startTime": time_filter
        }
    else:
        # Для superadmin_personal: 24 часа назад
        time_filter = (datetime.now() - timedelta(hours=24)).isoformat() + 'Z'
        params = {
            "ids": "",
            "page.page": 1,
            "page.size": 100,
            "page.sort": "startTime,number,DESC",
            "filter.gt.startTime": time_filter
        }

    try:
        response = requests.get(endpoint_url, headers=headers, params=params, verify=False, timeout=60)
        response.raise_for_status()
        launches = response.json().get("content", [])

        # Фильтруем запуски, исключая те, которые в статусе IN_PROGRESS
        launches = [launch for launch in launches if launch.get("status") != "IN_PROGRESS"]

        if is_linux:
            # Собираем уникальные комбинации ветка+коммит
            unique_combinations = {}
            for launch in launches:
                attributes = launch.get("attributes", [])
                has_os = False
                has_db = False
                branch = None
                commit_hash = None

                for attr in attributes:
                    if attr.get("key") == "OS" and attr.get("value") == "Linux":
                        has_os = True
                    elif attr.get("key") == "Database" and attr.get("value") == "PostgreSQL":
                        has_db = True
                    elif attr.get("key") == "Branch":
                        branch = attr.get("value")
                    elif attr.get("key") == "Commit hash":
                        commit_hash = attr.get("value")

                # Отбираем только Linux/PostgreSQL прогоны с указанием ветки и коммита
                if has_os and has_db and branch and commit_hash:
                    combination_key = f"{branch}_{commit_hash}"

                    # Берем самый свежий запуск для каждой уникальной комбинации
                    existing_launch = unique_combinations.get(combination_key)
                    if not existing_launch or datetime.fromisoformat(
                            launch["startTime"].replace('Z', '+00:00')) > datetime.fromisoformat(
                        existing_launch["startTime"].replace('Z', '+00:00')):
                        unique_combinations[combination_key] = launch

            return list(unique_combinations.values())

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
                branch = None
                commit_hash = None

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
                    elif attr.get("key") == "Branch name":  # ИСПРАВЛЕНО: Branch -> Branch name
                        branch = attr.get("value")
                    elif attr.get("key") == "Version":
                        # Version содержит "3.29" или "3.30"
                        pass
                    elif attr.get("key") == "Commit hash":
                        commit_hash = attr.get("value")

                if has_full_version and has_relaunch and has_db_type:
                    # Сохраняем ветку в атрибуте запуска для последующего использования
                    if branch:
                        launch['_branch'] = branch
                    if commit_hash:
                        launch['_commit_hash'] = commit_hash

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
        raise


@retry_with_backoff(max_retries=3, exceptions=(requests.exceptions.Timeout, requests.exceptions.ConnectionError))
def get_defect_links(access_token: str, launch_id: str, project: str = "superadmin_personal"):
    """Получаем список уникальных ссылок на дефекты для указанного launch_id"""
    url = f"{REPORTPORTAL_URL}/api/v1/{project}/item/v2"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    params = {
        "page.page": 1,
        "page.size": 100,
        "page.sort": "startTime,ASC",
        "filter.eq.hasStats": "true",
        "filter.eq.hasChildren": "false",
        "filter.in.issueType": "pb001",
        "providerType": "launch",
        "launchId": launch_id
    }

    try:
        response = requests.get(url, headers=headers, params=params, verify=False, timeout=30)
        response.raise_for_status()

        defects = response.json().get("content", [])
        links = set()

        for defect in defects:
            issue = defect.get("issue", {})
            if issue.get("issueType") == "pb001":
                comment = issue.get("comment", "")
                if comment and (
                        comment.startswith("https://a2nta.ru/Issues/") or
                        comment.startswith("https://jira.a2nta.ru")
                ):
                    links.add(comment)

        # Обработка пагинации
        total_pages = response.json().get("page", {}).get("totalPages", 1)
        if total_pages > 1:
            for page in range(2, total_pages + 1):
                params["page.page"] = page
                response = requests.get(url, headers=headers, params=params, verify=False, timeout=30)
                response.raise_for_status()
                for defect in response.json().get("content", []):
                    issue = defect.get("issue", {})
                    if issue.get("issueType") == "pb001":
                        comment = issue.get("comment", "")
                        if comment and (
                                comment.startswith("https://a2nta.ru/Issues/") or
                                comment.startswith("https://jira.a2nta.ru")
                        ):
                            links.add(comment)

        logger.info(f"Найдено {len(links)} дефектов для launch_id {launch_id}")
        return sorted(links)

    except requests.exceptions.Timeout:
        logger.error(f"Таймаут при получении дефектов для launch_id {launch_id}")
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении дефектов: {e}", exc_info=True)
        raise


def format_statistics(launch, launch_type):
    """Форматируем статистику для вывода"""
    if not launch:
        return f"{launch_type}: нет данных о запуске"

    stats = launch.get("statistics", {}).get("executions", {})
    version = "Не указана"
    branch = "Не указана"
    commit_hash = "Не указан"

    # Сначала ищем в атрибутах, игнорируя временные поля _branch
    for attr in launch.get("attributes", []):
        if attr.get("key") == "Version":
            # Для Linux тестов Version содержит версию
            if "Linux" in launch_type:
                version = attr.get("value")
            # Для основных тестов Version содержит "3.29" или "3.30" - это НЕ ветка!
            # Для ветки используем Branch name
        elif attr.get("key") == "FullVersion":
            version = attr.get("value")
        elif attr.get("key") == "Branch name":
            branch = attr.get("value")
        elif attr.get("key") == "Branch":  # Для обратной совместимости
            if branch == "Не указана":
                branch = attr.get("value")
        elif attr.get("key") == "Commit hash":
            commit_hash = attr.get("value")

    # Если ветка не найдена в атрибутах, проверяем временное поле _branch
    if branch == "Не указана" and '_branch' in launch:
        branch = launch['_branch']

    # Если коммит не найден в атрибутах, проверяем временное поле _commit_hash
    if commit_hash == "Не указан" and '_commit_hash' in launch:
        commit_hash = launch['_commit_hash']

    project = "linux_tests" if "Linux" in launch_type else "superadmin_personal"

    return (
        f"{launch_type}\n"
        f"ID запуска: {launch.get('id')}\n"
        f"Версия: {version}\n"
        f"Ветка: {branch}\n"
        f"Коммит: {commit_hash}\n"
        f"Название: {launch.get('name')}\n"
        f"Всего тестов: {stats.get('total', 0)}\n"
        f"Пройдено: {stats.get('passed', 0)}\n"
        f"Провалено: {stats.get('failed', 0)}\n"
        f"Пропущено: {stats.get('skipped', 0)}\n"
        f"Статус: {launch.get('status')}\n"
        f"Время начала: {launch.get('startTime')}\n"
        f"Ссылка: {REPORTPORTAL_URL}/ui/#{project}/launches/all/{launch.get('id')}\n"
    )


@retry_with_backoff_async(max_retries=3, exceptions=(asyncio.TimeoutError, telegram.error.TimedOut,
                                                     requests.exceptions.Timeout, requests.exceptions.ConnectionError))
async def send_report_to_chat(context: CallbackContext, chat_id: int):
    """Функция для отправки отчета в указанный чат"""
    try:
        access_token = get_access_token()
        logger.info(f"Токен получен успешно")

        if not access_token:
            await context.bot.send_message(
                chat_id=chat_id,
                text="❌ Не удалось получить access_token"
            )
            return

        # Собираем информацию о запусках с таймаутом
        try:
            main_launches = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None, get_filtered_launches, access_token, SUPERADMIN_LAUNCHES_URL
                ),
                timeout=60
            )
            linux_launches = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None, get_filtered_launches, access_token, LINUX_LAUNCHES_URL, True
                ),
                timeout=60
            )
        except asyncio.TimeoutError as e:
            logger.error(f"Таймаут при получении данных о запусках: {e}")
            raise

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
                try:
                    defects = get_defect_links(access_token, launch_id)
                    logger.info(f"Дефекты для {version}: найдено {len(defects)}")
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
                except Exception as e:
                    logger.error(f"Ошибка при получении дефектов для {version}: {e}")
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"⚠️ Не удалось получить дефекты для версии {version}: {str(e)}",
                        parse_mode="HTML"
                    )

        # Отправляем дефекты для Linux прогонов
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

                try:
                    defects = get_defect_links(access_token, launch.get("id"), project="linux_tests")
                    logger.info(f"Дефекты для Linux прогона (ID: {launch.get('id')}): найдено {len(defects)}")

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
                except Exception as e:
                    logger.error(f"Ошибка при получении дефектов для Linux прогона: {e}")
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"⚠️ Не удалось получить дефекты для Linux прогона (Ветка: {branch}): {str(e)}",
                        parse_mode="HTML"
                    )

        logger.info("Отчет успешно отправлен в канал")
    except telegram.error.BadRequest as e:
        logger.error(f"Ошибка Telegram API: {e.message}")
        if "Chat not found" in str(e):
            logger.error("Проверьте правильность TELEGRAM_CHAT_ID")
        raise
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        raise


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /report"""
    try:
        await send_report_to_chat(context, update.effective_chat.id)
    except Exception as e:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"🚨 Не удалось отправить отчет после 3 попыток: {str(e)}"
        )


async def daily_report(context: CallbackContext):
    """Ежедневная отправка отчета"""
    try:
        await send_report_to_chat(context, TELEGRAM_CHAT_ID)
    except Exception as e:
        await context.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=f"🚨 Не удалось отправить отчет после 3 попыток: {str(e)}"
        )


async def main_async():
    """Асинхронная основная функция"""
    application = None
    try:
        application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

        # Отправляем отчет
        await send_report_to_chat(application, TELEGRAM_CHAT_ID)

        # Останавливаем приложение
        if application.running:
            await application.stop()
            await application.shutdown()

    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}")

        # Пытаемся отправить сообщение об ошибке, если приложение создано
        if application and hasattr(application, 'bot'):
            try:
                await application.bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=f"🚨 Не удалось отправить отчет после 3 попыток: {str(e)}"
                )
            except Exception as bot_error:
                logger.error(f"Не удалось отправить сообщение об ошибке: {bot_error}")

        # Завершаем приложение если оно создано
        if application and application.running:
            try:
                await application.stop()
                await application.shutdown()
            except Exception:
                pass

        exit(1)


def main():
    """Запуск бота для одноразовой отправки отчета"""
    try:
        # Создаем новый цикл событий и запускаем асинхронную функцию
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Работа прервана пользователем")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка: {e}")
        exit(1)


if __name__ == '__main__':
    main()