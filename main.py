import requests
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
    "username": os.getenv("REPORTPORTAL_USERNAME"),
    "password": os.getenv("REPORTPORTAL_PASSWORD")
}
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

DAILY_REPORT_TIME = time(hour=9, minute=0, tzinfo=pytz.timezone('Europe/Moscow'))

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def get_access_token():
    """Получаем access_token от ReportPortal"""
    try:
        response = requests.post(AUTH_URL, headers=AUTH_HEADERS, data=AUTH_DATA)
        response.raise_for_status()
        return response.json()["access_token"]
    except Exception as e:
        logger.error(f"Ошибка при получении токена: {e}")
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
        response = requests.get(endpoint_url, headers=headers, params=params)
        response.raise_for_status()
        launches = response.json().get("content", [])

        last_30 = None
        last_29 = None
        linux_version = None

        for launch in launches:
            attributes = launch.get("attributes", [])

            if is_linux:
                version = None
                has_os = False
                has_db = False

                for attr in attributes:
                    if attr.get("key") == "Version":
                        version = attr.get("value")
                    elif attr.get("key") == "OS" and attr.get("value") == "Linux":
                        has_os = True
                    elif attr.get("key") == "Database" and attr.get("value") == "PostgreSQL":
                        has_db = True

                if version and version.startswith("4.00") and has_os and has_db:
                    if launch.get("status") in ["FAILED", "PASSED"]:
                        linux_version = launch
                        break
            else:
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
                        datetime.fromisoformat(launch["startTime"].replace('Z', '+00:00')) >
                        datetime.fromisoformat(last_30["startTime"].replace('Z', '+00:00'))):
                        last_30 = launch
                    elif full_version.startswith("3.29") and (last_29 is None or
                        datetime.fromisoformat(launch["startTime"].replace('Z', '+00:00')) >
                        datetime.fromisoformat(last_29["startTime"].replace('Z', '+00:00'))):
                        last_29 = launch

        if is_linux:
            return [linux_version] if linux_version else []
        return [launch for launch in [last_30, last_29] if launch]

    except Exception as e:
        logger.error(f"Ошибка при получении запусков: {e}")
        return []

def format_statistics(launch, launch_type):
    """Форматируем статистику для вывода"""
    if not launch:
        return f"{launch_type}: нет данных о запуске"

    stats = launch.get("statistics", {}).get("executions", {})

    version_key = "Version" if launch_type == "Linux прогон" else "FullVersion"
    version = next(
        (attr.get("value") for attr in launch.get("attributes", [])
         if attr.get("key") == version_key),
        "Не указана"
    )

    return (
        f"{launch_type}\n"
        f"ID запуска: {launch.get('id')}\n"
        f"Версия: {version}\n"
        f"Название: {launch.get('name')}\n"
        f"Всего тестов: {stats.get('total', 0)}\n"
        f"Пройдено: {stats.get('passed', 0)}\n"
        f"Провалено: {stats.get('failed', 0)}\n"
        f"Пропущено: {stats.get('skipped', 0)}\n"
        f"Статус: {launch.get('status')}\n"
        f"Время начала: {launch.get('startTime')}\n"
        f"Ссылка: {REPORTPORTAL_URL}/ui/#{launch_type.split()[0].lower()}/launches/all/{launch.get('id')}\n"
    )

async def send_report_to_chat(context: CallbackContext, chat_id: int):
    """Функция для отправки отчета в указанный чат"""
    try:
        access_token = get_access_token()
        if not access_token:
            await context.bot.send_message(chat_id=chat_id, text="Не удалось получить access_token")
            return

        all_versions = []
        collected_launches = []

        # Основные прогоны
        main_launches = get_filtered_launches(access_token, SUPERADMIN_LAUNCHES_URL)
        for launch in main_launches:
            collected_launches.append(("superadmin_personal", launch))
            all_versions.extend([
                attr.get("value") for attr in launch.get("attributes", [])
                if attr.get("key") == "FullVersion"
            ])

        # Linux прогоны
        linux_launches = get_filtered_launches(access_token, LINUX_LAUNCHES_URL, is_linux=True)
        for launch in linux_launches:
            collected_launches.append(("linux_tests", launch))
            all_versions.extend([
                attr.get("value") for attr in launch.get("attributes", [])
                if attr.get("key") == "Version"
            ])

        # Отправка сообщений
        for launch_type, launch in collected_launches:
            message = format_statistics(launch, launch_type)
            await context.bot.send_message(chat_id=chat_id, text=message)

        # Итоговое сообщение
        if all_versions:
            versions_str = ", ".join(sorted(set(all_versions), reverse=True))
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"Отчет по последним запускам версий: {versions_str}"
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Не найдено актуальных запусков"
            )

    except Exception as e:
        logger.error(f"Ошибка при отправке отчета: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"Произошла ошибка: {e}")

async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /report"""
    await send_report_to_chat(context, update.effective_chat.id)

async def daily_report(context: CallbackContext):
    """Ежедневная отправка отчета"""
    await send_report_to_chat(context, TELEGRAM_CHAT_ID)

async def post_init(application):
    """Действия после инициализации бота"""
    job_queue = application.job_queue
    job_queue.run_daily(daily_report, time=DAILY_REPORT_TIME)
    await daily_report(application)

def main():
    """Запуск бота"""
    application = ApplicationBuilder() \
        .token(TELEGRAM_TOKEN) \
        .post_init(post_init) \
        .build()

    application.add_handler(CommandHandler("report", report_command))
    application.run_polling()

if __name__ == '__main__':
    main()