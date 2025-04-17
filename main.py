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
LAUNCHES_URL = f"{REPORTPORTAL_URL}/api/v1/superadmin_personal/launch"
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

# Время ежедневного отчета (09:00 по Москве)
DAILY_REPORT_TIME = time(hour=9, minute=0, tzinfo=pytz.timezone('Europe/Moscow'))

# Настройка логирования
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

def get_filtered_launches(access_token):
    """Получаем и фильтруем запуски, возвращаем последние для 3.30 и 3.29"""
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
        response = requests.get(LAUNCHES_URL, headers=headers, params=params)
        response.raise_for_status()
        launches = response.json().get("content", [])

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
                # Проверяем версию и обновляем последний соответствующий запуск
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

        # Собираем только последние запуски для каждой версии
        filtered_launches = []
        if last_30:
            filtered_launches.append(last_30)
        if last_29:
            filtered_launches.append(last_29)

        return filtered_launches
    except Exception as e:
        logger.error(f"Ошибка при получении запусков: {e}")
        return []

def format_statistics(launch):
    """Форматируем статистику для вывода"""
    stats = launch.get("statistics", {}).get("executions", {})

    # Получаем полную версию из атрибутов
    full_version = "Не указана"
    for attr in launch.get("attributes", []):
        if attr.get("key") == "FullVersion":
            full_version = attr.get("value")
            break

    return (
        f"ID запуска: {launch.get('id')}\n"
        f"Версия: {full_version}\n"
        f"Название: {launch.get('name')}\n"
        f"Всего тестов: {stats.get('total', 0)}\n"
        f"Пройдено: {stats.get('passed', 0)}\n"
        f"Провалено: {stats.get('failed', 0)}\n"
        f"Пропущено: {stats.get('skipped', 0)}\n"
        f"Статус: {launch.get('status')}\n"
        f"Время начала: {launch.get('startTime')}\n"
        f"Ссылка на запуск: {REPORTPORTAL_URL}/ui/#superadmin_personal/launches/all/{launch.get('id')}\n"
    )

async def send_report_to_chat(context: CallbackContext, chat_id: int):
    """Функция для отправки отчета в указанный чат"""
    try:
        # Получаем токен
        access_token = get_access_token()
        if not access_token:
            await context.bot.send_message(chat_id=chat_id, text="Не удалось получить access_token")
            return

        # Получаем и фильтруем запуски
        launches = get_filtered_launches(access_token)
        if not launches:
            await context.bot.send_message(chat_id=chat_id, text="Не найдено подходящих запусков")
            return

        # Отправляем статистику по каждому найденному запуску
        for launch in launches:
            message = format_statistics(launch)
            await context.bot.send_message(chat_id=chat_id, text=message)

        versions_found = ", ".join([attr.get("value") for launch in launches
                                  for attr in launch.get("attributes", [])
                                  if attr.get("key") == "FullVersion"])
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"Отчет по последним запускам версий: {versions_found}"
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
    # Устанавливаем ежедневный отчет
    job_queue = application.job_queue
    job_queue.run_daily(daily_report, time=DAILY_REPORT_TIME)

    # Отправляем отчет сразу при запуске
    await daily_report(application)

def main():
    """Запуск бота"""
    # Убедитесь, что установили пакет с job-queue:
    # pip install python-telegram-bot[job-queue]
    application = ApplicationBuilder() \
        .token(TELEGRAM_TOKEN) \
        .post_init(post_init) \
        .build()

    # Регистрируем обработчик команды
    application.add_handler(CommandHandler("report", report_command))

    # Запускаем бота
    application.run_polling()

if __name__ == '__main__':
    main()