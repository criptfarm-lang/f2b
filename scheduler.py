"""
Планировщик задач бота F2B PRO
- Утренняя сводка в 9:00
- Напоминание о дедлайнах в 10:00
- Вечерний срез по дебиторке в 18:00
"""

import logging
import os
from datetime import time

from telegram.ext import Application
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from claude_ai import generate_morning_summary

logger = logging.getLogger(__name__)

# ID группы куда слать сводки (берётся из .env)
def get_group_ids():
    raw = os.getenv("GROUP_CHAT_IDS", "")
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def setup_scheduler(app: Application, db):
    """Настраивает и запускает все запланированные задачи."""
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

    # 09:00 — утренняя сводка
    scheduler.add_job(
        morning_summary,
        CronTrigger(hour=9, minute=0),
        args=[app, db],
        id="morning_summary"
    )

    # 10:00 — напоминание о задачах на сегодня
    scheduler.add_job(
        remind_today_tasks,
        CronTrigger(hour=10, minute=0),
        args=[app, db],
        id="remind_today"
    )

    # 18:00 — запрос среза по дебиторке
    scheduler.add_job(
        debt_reminder,
        CronTrigger(hour=18, minute=0),
        args=[app, db],
        id="debt_reminder"
    )

    # Пн–пт 17:00 — напоминание прислать реестр (для Беляковой)
    scheduler.add_job(
        registry_reminder,
        CronTrigger(day_of_week="mon-fri", hour=17, minute=0),
        args=[app, db],
        id="registry_reminder"
    )

    scheduler.start()
    logger.info("✅ Планировщик запущен")


async def morning_summary(app: Application, db):
    """Отправляет утреннюю сводку в группы."""
    group_ids = get_group_ids()
    if not group_ids:
        return

    tasks_today = db.get_tasks_due_today()
    tasks_overdue = db.get_overdue_tasks()

    # Не отправляем если нет ничего важного
    if not tasks_today and not tasks_overdue:
        return

    text = await generate_morning_summary(tasks_today, tasks_overdue)

    for chat_id in group_ids:
        try:
            await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
            logger.info(f"Утренняя сводка отправлена в {chat_id}")
        except Exception as e:
            logger.error(f"Не удалось отправить сводку в {chat_id}: {e}")


async def remind_today_tasks(app: Application, db):
    """Напоминает о задачах на сегодня."""
    group_ids = get_group_ids()
    tasks = db.get_tasks_due_today()

    if not tasks or not group_ids:
        return

    lines = ["⏰ *Напоминание: задачи на сегодня*\n"]
    for t in tasks:
        exe = t.get('executor', 'Команда')
        lines.append(f"• *{exe}*: {t['text']}")

    text = "\n".join(lines)
    for chat_id in group_ids:
        try:
            await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Ошибка напоминания в {chat_id}: {e}")


async def debt_reminder(app: Application, db):
    """Запрашивает у менеджеров срез по дебиторке."""
    group_ids = get_group_ids()
    if not group_ids:
        return

    text = (
        "💰 *Срез по дебиторке — до 18:30*\n\n"
        "Менеджеры, напишите:\n"
        "1️⃣ Кто должен был оплатить сегодня — пришли?\n"
        "2️⃣ У кого срок оплаты завтра — клиент: сумма\n\n"
        "_Формат: Название клиента — сумма_"
    )

    for chat_id in group_ids:
        try:
            await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Ошибка debt_reminder в {chat_id}: {e}")


async def registry_reminder(app: Application, db):
    """Напоминает Беляковой прислать реестр."""
    group_ids = get_group_ids()
    if not group_ids:
        return

    text = (
        "📋 *Белякова Александра*, не забудь:\n"
        "Согласуй реестр и отправь в склад ✅"
    )

    for chat_id in group_ids:
        try:
            await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Ошибка registry_reminder в {chat_id}: {e}")
