"""
Планировщик задач бота F2B PRO
- Утренняя сводка в 9:00
- Напоминание о дедлайнах в 10:00
- Вечерняя сводка ПДЗ в 17:00 (только если был запущен /pdz)
- ПДЗ по командам через /pdz — НЕ автоматически
"""

import logging
import os
from datetime import date

from telegram.ext import Application
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from claude_ai import generate_morning_summary
from moysklad import get_overdue_demands, format_overdue_summary

logger = logging.getLogger(__name__)


def get_group_ids():
    raw = os.getenv("GROUP_CHAT_IDS", "")
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def get_group_chat_id():
    val = os.getenv("GROUP_CHAT_ID", "")
    return int(val) if val else None


# Менеджеры ПДЗ — порядок определяет очерёдность отправки (2 мин между каждым)
# chat_id подтягивается из БД (менеджер пишет /mychatid боту)
PDZ_MANAGERS = [
    {"name": "Карина",   "tag": "баласанян",  "name_fragment": "Баласанян"},
    {"name": "Елена",    "tag": "мерзлякова", "name_fragment": "Мерзлякова"},
    {"name": "Инесса",   "tag": "скляр",      "name_fragment": "Скляр"},
    {"name": "Алексей",  "tag": "леонтьев",   "name_fragment": "Леонтьев"},
    {"name": "Сергей",   "tag": "черентаев",  "name_fragment": "Черентаев"},
]

# Флаг — был ли запущен /pdz сегодня (для вечерней сводки)
pdz_launched_today: set = set()  # хранит даты когда был запуск

# Хранилище сообщений группы за текущий день ПДЗ
pdz_day_messages: dict = {}


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
        id="remind_today_tasks"
    )

    # 12:00 МСК — проверка стареющих клиентов
    scheduler.add_job(
        check_aging_clients,
        CronTrigger(hour=9, minute=0, timezone="UTC"),
        args=[app],
        id="aging_clients"
    )

    # 03:00 — очистка старых задач
    scheduler.add_job(
        cleanup_done_tasks,
        CronTrigger(hour=3, minute=0),
        id="cleanup_done_tasks"
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


async def pdz_morning_task(app: Application, mgr: dict):
    """
    Отправляет ПДЗ по менеджеру в группу.
    Затем шлёт менеджеру в личку задачу проработать дебиторку.
    """
    chat_id = get_group_chat_id()
    if not chat_id:
        return

    today = date.today().isoformat()
    if today not in pdz_day_messages:
        pdz_day_messages[today] = {}
    if mgr["tag"] not in pdz_day_messages[today]:
        pdz_day_messages[today][mgr["tag"]] = []

    try:
        items = await get_overdue_demands(tag=mgr["tag"])

        if not items:
            logger.info(f"pdz_morning_task: нет просрочки у {mgr['name']}")
            # В личку — нет долгов
            mgr_chat_id = mgr.get("chat_id")
            if mgr_chat_id:
                try:
                    await app.bot.send_message(
                        chat_id=mgr_chat_id,
                        text=f"✅ {mgr['name']}, у твоих клиентов сегодня нет просроченной дебиторки. Хорошая работа!"
                    )
                except Exception as e:
                    logger.warning(f"Не удалось написать {mgr['name']} в личку: {e}")
            return

        pdz_text = format_overdue_summary(items)

        # 1. Отправляем ПДЗ в группу
        group_msg = await app.bot.send_message(
            chat_id=chat_id,
            text=(
                f"📋 *Дебиторка — {mgr['name']}*\n\n"
                f"{pdz_text}"
            ),
            parse_mode="Markdown"
        )
        logger.info(f"pdz_morning_task: ПДЗ {mgr['name']} отправлена в группу")

        # 2. Пишем менеджеру в личку
        mgr_chat_id = mgr.get("chat_id")
        if mgr_chat_id:
            try:
                await app.bot.send_message(
                    chat_id=mgr_chat_id,
                    text=(
                        f"📋 *{mgr['name']}, задача по дебиторке*\n\n"
                        f"Твоя просроченная дебиторка опубликована в группе.\n"
                        f"Свяжись с каждым клиентом и пиши результаты *мне в личку* — "
                        f"кто и когда оплатит.\n\n"
                        f"Срок отчёта — до 17:00.\n\n"
                        f"Пример: _«Атмосфера — оплатит в пятницу 21.03»_"
                    ),
                    parse_mode="Markdown"
                )
                logger.info(f"pdz_morning_task: личка {mgr['name']} отправлена")
            except Exception as e:
                logger.warning(f"Не удалось написать {mgr['name']} в личку: {e}")

    except Exception as e:
        logger.error(f"Ошибка pdz_morning_task для {mgr['name']}: {e}", exc_info=True)


def record_group_message(sender_name: str, tag: str, text: str):
    """Записывает сообщение из группы в хранилище текущего дня ПДЗ."""
    today = date.today().isoformat()
    if today not in pdz_day_messages:
        pdz_day_messages[today] = {}
    if tag not in pdz_day_messages[today]:
        pdz_day_messages[today][tag] = []
    pdz_day_messages[today][tag].append(f"{sender_name}: {text}")


async def pdz_evening_summary(app: Application):
    """В 17:00 собирает результаты из БД и отправляет сводку по каждому менеджеру."""
    import asyncio

    today = date.today().isoformat()

    if today not in pdz_launched_today:
        logger.info("pdz_evening_summary: /pdz не запускался сегодня, пропускаем")
        return

    chat_id = get_group_chat_id()
    if not chat_id:
        return

    try:
        from database import Database
        db_local = Database()
        results = db_local.get_pdz_results_today()

        # Группируем по менеджеру
        by_manager = {}
        for r in results:
            name = r.get("manager_name", "Неизвестный")
            if name not in by_manager:
                by_manager[name] = []
            by_manager[name].append(r.get("result_text", ""))

        # Отправляем по каждому менеджеру с паузой 2 мин
        for i, mgr in enumerate(PDZ_MANAGERS):
            mgr_name = mgr["name"]
            # Ищем результаты по фрагменту имени
            mgr_results = []
            frag = mgr.get("name_fragment", mgr_name).lower()
            for full_name, msgs in by_manager.items():
                if frag in full_name.lower():
                    mgr_results = msgs
                    break

            # Текущая просрочка
            items = await get_overdue_demands(tag=mgr["tag"])
            from moysklad import format_overdue_demands
            pdz_text = format_overdue_demands(items) if items else "✅ Просрочек нет"

            if mgr_results:
                results_text = "\n".join(f"  — {r}" for r in mgr_results)
            else:
                results_text = "  — ответов не поступало"

            text = (
                f"📊 *Итог дня — {mgr_name}*\n\n"
                f"{pdz_text}\n\n"
                f"💬 *Результаты работы:*\n{results_text}"
            )

            await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
            logger.info(f"pdz_evening_summary отправлена для {mgr_name}")

            if i < len(PDZ_MANAGERS) - 1:
                await asyncio.sleep(120)

    except Exception as e:
        logger.error(f"Ошибка pdz_evening_summary: {e}", exc_info=True)


def cleanup_done_tasks():
    """Удаляет выполненные задачи старше 24 часов."""
    try:
        from database import Database
        db = Database()
        db.cleanup_done_tasks()
        logger.info("cleanup_done_tasks: старые выполненные задачи удалены")
    except Exception as e:
        logger.error(f"cleanup_done_tasks: {e}")


async def check_aging_clients(app: Application):
    """Ежедневно в 12:00 — проверяет клиентов без отгрузок 50 дней."""
    from moysklad import get_aging_clients
    from database import Database

    chat_id = get_group_chat_id()
    if not chat_id:
        return

    db = Database()

    try:
        clients = await get_aging_clients(days=50)
        if not clients:
            logger.info("check_aging_clients: стареющих клиентов нет")
            return

        MANAGER_TAG_MAP = {
            "баласанян": "Карина Баласанян",
            "мерзлякова": "Елена Мерзлякова",
            "скляр": "Инесса Скляр",
            "леонтьев": "Алексей Леонтьев",
            "черентаев": "Сергей Черентаев",
        }

        import asyncio
        for i, client in enumerate(clients):
            name = client["name"]
            tags = client.get("tags", [])
            last_date = client["last_demand_date"]
            days = client.get("days", client.get("days_ago", 50))

            # Определяем менеджера
            manager_name = "Без менеджера"
            manager_tag = None
            for tag in tags:
                if tag.lower() in MANAGER_TAG_MAP:
                    manager_name = MANAGER_TAG_MAP[tag.lower()]
                    manager_tag = tag.lower()
                    break

            # 1. Алерт в группу PRO
            await app.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"⚠️ *Стареющий клиент*\n\n"
                    f"👤 *{name}*\n"
                    f"📅 Последняя отгрузка: {last_date} ({days} дней назад)\n"
                    f"👔 Менеджер: {manager_name}\n\n"
                    f"Необходимо связаться с клиентом и сделать специальное предложение."
                ),
                parse_mode="Markdown"
            )

            # 2. Задача в список задач
            task_text = f"Связаться с {name} — нет отгрузок {days} дней. Сделать спецпредложение."
            db.save_task(
                text=task_text,
                executor=manager_name,
                deadline=None,
                source_chat=chat_id,
                source_message_id=0,
                created_by="Эф (авто)"
            )

            # 3. Личное сообщение менеджеру
            if manager_tag:
                mgr_chat_id = db.get_manager_chat_id(manager_name.split()[0])
                if mgr_chat_id:
                    try:
                        await app.bot.send_message(
                            chat_id=mgr_chat_id,
                            text=(
                                f"📋 *Задача: стареющий клиент*\n\n"
                                f"👤 *{name}* не делал заказов уже {days} дней.\n"
                                f"Последняя отгрузка: {last_date}\n\n"
                                f"Свяжись с клиентом, узнай причину и сделай специальное предложение.\n"
                                f"Результат напиши мне в личку."
                            ),
                            parse_mode="Markdown"
                        )
                    except Exception as e:
                        logger.warning(f"check_aging_clients: не удалось написать {manager_name}: {e}")

            logger.info(f"check_aging_clients: обработан {name} → {manager_name}")

            # Пауза 2 минуты между алертами
            if i < len(clients) - 1:
                await asyncio.sleep(120)

    except Exception as e:
        logger.error(f"check_aging_clients: {e}", exc_info=True)
