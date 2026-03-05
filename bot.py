"""
F2B PRO — Telegram Bot
Ассистент отдела продаж: задачи, фото, прайсы, дебиторка
"""

import logging
import os
import re
from datetime import datetime

from telegram import Update, Message
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from database import Database
from scheduler import setup_scheduler
from claude_ai import ask_claude, extract_tasks_from_message

# ─── Словарь сотрудников — варианты имён и склонений ─────────────────────────
EMPLOYEES = {
    "Белякова Александра": [
        "александра", "александры", "александре", "александру",
        "белякова", "беляковой", "белякову",
        "саша", "саши", "саше", "сашу",
    ],
    "Алексей Леонтьев": [
        "алексей", "алексея", "алексею", "алексеем",
        "леонтьев", "леонтьева", "леонтьеву",
        "лёша", "лёши", "лёше", "леша", "леши", "лёшу",
    ],
    "Ярослав": [
        "ярослав", "ярослава", "ярославу", "ярославом",
        "ярик", "ярика", "ярику",
    ],
    "Андрей Иванов": [
        "андрей", "андрея", "андрею", "андреем",
        "иванов", "иванова", "иванову",
    ],
    "Инесса Скляр": [
        "инесса", "инессы", "инессе", "инессу", "инессой",
        "скляр",
    ],
    "Маланчук Александр": [
        "маланчук", "маланчука", "маланчуку",
    ],
    "Карина Баласанян": [
        "карина", "карины", "карине", "карину", "кариной",
        "баласанян",
    ],
    "Елена Мерзлякова": [
        "елена", "елены", "елене", "елену", "еленой",
        "мерзлякова", "мерзляковой", "мерзлякову",
        "марзлякова", "марзляковой",
        "лена", "лены", "лене", "лену", "леной",
    ],
    "Татьяна Голубева": [
        "татьяна", "татьяны", "татьяне", "татьяну", "татьяной",
        "голубева", "голубевой", "голубеву",
        "таня", "тани", "тане", "таню", "таней",
    ],
}

def find_employee(query: str) -> str | None:
    """Ищет сотрудника по любому варианту имени/фамилии в запросе."""
    query_lower = query.lower()
    # Сначала ищем точное совпадение слова
    for full_name, variants in EMPLOYEES.items():
        for variant in variants:
            # Проверяем что вариант встречается как отдельное слово
            import re as _re
            if _re.search(r"\b" + _re.escape(variant) + r"\b", query_lower):
                return full_name
    return None



# ─── Логирование ────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Инициализация БД ────────────────────────────────────────────────────────
db = Database()

# ─── Определяем обращение к боту ─────────────────────────────────────────────
BOT_TRIGGERS = ["бот,", "бот ", "@бот", "bot,", "bot ", "@f2b_assistant_bot", "@f2b_assistant"]


def is_bot_addressed(text: str) -> bool:
    """Проверяет, обращаются ли к боту."""
    if not text:
        return False
    text_lower = text.lower().strip()
    # Реагируем на обращение в начале или @mention в любом месте
    if any(text_lower.startswith(t) for t in BOT_TRIGGERS):
        return True
    # @mention может быть в любом месте сообщения
    if "@f2b_assistant" in text_lower:
        return True
    return False


def clean_query(text: str) -> str:
    """Убирает обращение к боту из текста."""
    text_lower = text.lower()
    for trigger in BOT_TRIGGERS:
        if text_lower.startswith(trigger):
            return text[len(trigger):].strip()
    return text.strip()


# ─── Команды ─────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Привет! Я ассистент F2B PRO.\n\n"
        "Обращайся ко мне: *бот, [вопрос]*\n\n"
        "Примеры:\n"
        "• бот, пришли фото тунца\n"
        "• бот, какая цена на лосось?\n"
        "• бот, мои задачи\n"
        "• бот, контакт Малахова\n\n"
        "Команды:\n"
        "/tasks — мои задачи\n"
        "/report — отчёт\n"
        "/help — все команды",
        parse_mode="Markdown"
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📋 *Все команды:*\n\n"
        "*Задачи:*\n"
        "/tasks — мои задачи\n"
        "/all_tasks — все задачи команды\n"
        "/overdue — просроченные задачи\n\n"
        "*Отчёты:*\n"
        "/report — недельный отчёт\n"
        "/дебиторка — срез по дебиторке\n\n"
        "*База знаний:*\n"
        "/фото [товар] — найти фото\n"
        "/прайс — актуальный прайс\n"
        "/контакт [имя] — найти контакт\n\n"
        "*Обращение в свободной форме:*\n"
        "бот, [любой вопрос]",
        parse_mode="Markdown"
    )


async def cmd_my_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает задачи текущего пользователя."""
    user = update.effective_user
    name = user.full_name
    tasks = db.get_tasks_for_user(name)

    if not tasks:
        await update.message.reply_text(f"✅ {name}, у тебя нет открытых задач!")
        return

    lines = [f"📋 *Задачи для {name}:*\n"]
    for t in tasks:
        deadline_str = f" — до {t['deadline']}" if t.get('deadline') else ""
        status_icon = "🔴" if t.get('overdue') else "🟡"
        lines.append(f"{status_icon} {t['text']}{deadline_str}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_all_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Все открытые задачи команды."""
    tasks = db.get_all_open_tasks()
    if not tasks:
        await update.message.reply_text("✅ Нет открытых задач!")
        return

    # Группируем по исполнителю
    by_user = {}
    for t in tasks:
        exe = t.get('executor', 'Неизвестно')
        by_user.setdefault(exe, []).append(t)

    lines = ["📋 *Все открытые задачи:*\n"]
    for user, utasks in by_user.items():
        lines.append(f"*{user}* ({len(utasks)}):")
        for t in utasks:
            icon = "🔴" if t.get('overdue') else "🟡"
            deadline_str = f" [{t['deadline']}]" if t.get('deadline') else ""
            lines.append(f"  {icon} {t['text']}{deadline_str}")
        lines.append("")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_overdue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просроченные задачи."""
    tasks = db.get_overdue_tasks()
    if not tasks:
        await update.message.reply_text("✅ Просроченных задач нет!")
        return

    lines = [f"🔴 *Просроченные задачи ({len(tasks)}):*\n"]
    for t in tasks:
        lines.append(f"• *{t.get('executor', '?')}*: {t['text']} [срок: {t.get('deadline', '?')}]")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Недельный отчёт по команде."""
    stats = db.get_weekly_stats()

    lines = ["📊 *Отчёт за неделю:*\n"]
    for user, s in stats.items():
        pct = int(s['done'] / s['total'] * 100) if s['total'] > 0 else 0
        bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
        lines.append(
            f"*{user}*\n"
            f"  {bar} {pct}%\n"
            f"  ✅ {s['done']} выполнено  🔴 {s['overdue']} просрочено  📋 {s['total']} всего\n"
        )

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_debtors(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Срез по дебиторке."""
    debtors = db.get_debtors()
    if not debtors:
        await update.message.reply_text("✅ Просроченной дебиторки нет!")
        return

    lines = ["💰 *Дебиторка — требуют внимания:*\n"]
    for d in debtors:
        lines.append(f"• {d['client']} → *{d['manager']}* [{d['days']} дн.]")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Поиск фото по команде /фото [товар]."""
    query = " ".join(context.args) if context.args else ""
    if not query:
        await update.message.reply_text("Укажи товар: /photo тунец")
        return
    await search_and_send_photo(update, context, query)


async def cmd_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Последний актуальный прайс."""
    price = db.get_latest_price()
    if price:
        await update.message.reply_document(
            document=price['file_id'],
            caption=f"📄 Прайс от {price['date']}"
        )
    else:
        await update.message.reply_text("Прайс пока не загружен в базу. Скинь прайс в чат и я его сохраню!")


async def cmd_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Поиск контакта."""
    query = " ".join(context.args) if context.args else ""
    if not query:
        await update.message.reply_text("Укажи имя: /contact Малахов")
        return

    contacts = db.search_contacts(query)
    if not contacts:
        await update.message.reply_text(f"Контакт '{query}' не найден в базе.")
        return

    lines = [f"📞 *Контакты по запросу '{query}':*\n"]
    for c in contacts:
        lines.append(f"• *{c['name']}* — {c['phone']} ({c.get('company', '')})")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ─── Обработка обычных сообщений ──────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главный обработчик всех сообщений."""
    message = update.message
    if not message:
        return

    chat_id = message.chat_id
    user = message.from_user
    text = message.text or message.caption or ""

    # 1. Сохраняем фото и документы в базу
    if message.photo:
        await save_media(message, "photo")

    if message.document:
        fname = message.document.file_name or ""
        if any(fname.lower().endswith(ext) for ext in [".pdf", ".xlsx", ".xls", ".docx"]):
            await save_media(message, "document")
            # Если это прайс — помечаем отдельно
            if any(w in fname.lower() for w in ["прайс", "price", "price-list"]):
                db.save_price(
                    file_id=message.document.file_id,
                    filename=fname,
                    chat_id=chat_id,
                    uploader=user.full_name
                )
                await message.reply_text("✅ Прайс сохранён в базу!")

    if not text:
        return

    # 2. Автоматическое извлечение задач (анализируем ВСЕ сообщения руководителя)
    # Список ID руководителей — добавь в .env
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]

    if user.id in manager_ids and len(text) > 15:
        tasks = await extract_tasks_from_message(text, user.full_name)
        for task in tasks:
            db.save_task(
                text=task["task"],
                executor=task.get("executor", ""),
                deadline=task.get("deadline"),
                source_chat=chat_id,
                source_message_id=message.message_id,
                created_by=user.full_name
            )
            if task.get("executor") and task.get("task"):
                logger.info(f"Задача извлечена: {task['executor']} → {task['task']}")
                # Уведомляем в чат что задача зафиксирована
                executor = task.get("executor", "")
                deadline = task.get("deadline")
                deadline_str = f" до {deadline}" if deadline else ""
                await message.reply_text(
                    f"📌 Задача зафиксирована\n"
                    f"👤 Исполнитель: *{executor}*{deadline_str}\n"
                    f"📝 {task['task']}",
                    parse_mode="Markdown"
                )

    # 3. Реагируем на обращение к боту
    if not is_bot_addressed(text):
        return

    query = clean_query(text)
    query_lower = query.lower()

    # ── Поиск фото ──
    if any(w in query_lower for w in ["фото", "photo", "картинк", "покажи"]):
        product = re.sub(r"(фото|photo|картинк\w*|покажи|пришли|дай)", "", query_lower).strip()
        await search_and_send_photo(update, context, product or query)
        return

    # ── Прайс ──
    if any(w in query_lower for w in ["прайс", "цен", "price", "стоимость", "почём"]):
        await cmd_price(update, context)
        return

    # ── Задачи ──
    if any(w in query_lower for w in ["задачи", "что делать", "что надо", "мои задачи", "поручения", "задание"]):
        # Ищем упоминание сотрудника в запросе через словарь EMPLOYEES
        target_name = find_employee(query_lower)

        if target_name:
            tasks = db.get_tasks_for_user(target_name)
            if not tasks:
                await message.reply_text(
                    f"✅ У *{target_name}* нет открытых задач.",
                    parse_mode="Markdown"
                )
            else:
                lines = [f"📋 *Задачи — {target_name}:*\n"]
                for t in tasks:
                    deadline_str = f" — до {t['deadline']}" if t.get("deadline") else ""
                    icon = "🔴" if t.get("overdue") else "🟡"
                    lines.append(f"{icon} {t['text']}{deadline_str}")
                await message.reply_text("\n".join(lines), parse_mode="Markdown")
        else:
            # Имя не найдено — показываем задачи спрашивающего
            await cmd_my_tasks(update, context)
        return

    # ── Дебиторка ──
    if any(w in query_lower for w in ["дебиторк", "долг", "оплат", "должн"]):
        await cmd_debtors(update, context)
        return

    # ── Контакт ──
    if any(w in query_lower for w in ["контакт", "телефон", "номер", "связаться"]):
        name = re.sub(r"(контакт|телефон|номер|связаться с|найди)", "", query_lower).strip()
        contacts = db.search_contacts(name)
        if contacts:
            lines = [f"📞 *{c['name']}* — {c['phone']} ({c.get('company', '')})" for c in contacts]
            await message.reply_text("\n".join(lines), parse_mode="Markdown")
        else:
            await message.reply_text(f"Контакт '{name}' не найден.")
        return

    # ── Отчёт ──
    if any(w in query_lower for w in ["отчёт", "отчет", "статистик", "итог"]):
        await cmd_report(update, context)
        return

    # ── Всё остальное — отправляем Claude ──
    await message.reply_chat_action("typing")
    context_data = db.get_context_summary()
    response = await ask_claude(query, context_data)
    await message.reply_text(response)


async def save_media(message: Message, media_type: str):
    """Сохраняет фото/документ в базу с тегами из подписи."""
    caption = message.caption or ""
    chat_id = message.chat_id
    user = message.from_user.full_name if message.from_user else "unknown"

    if media_type == "photo":
        file_id = message.photo[-1].file_id  # берём наибольшее разрешение
        if not caption:
            # Уведомляем что фото сохранено без тега
            await message.reply_text(
                "Фото сохранено в базу без подписи.\n"
                "Чтобы его можно было найти, напиши следующим сообщением название товара — например: форель трим С",
            )
    else:
        file_id = message.document.file_id
        caption = caption or message.document.file_name or ""

    db.save_media(
        file_id=file_id,
        media_type=media_type,
        caption=caption,
        chat_id=chat_id,
        uploader=user,
        date=datetime.now().isoformat()
    )


async def search_and_send_photo(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str):
    """Ищет фото по запросу во всех группах."""
    results = db.search_media(query, media_type="photo")

    if not results:
        await update.message.reply_text(
            f"😕 Фото '{query}' не найдено в базе.\n"
            f"Скиньте фото с подписью '{query}' — я его сохраню!"
        )
        return

    # Отправляем первые 3 результата
    for r in results[:3]:
        try:
            await update.message.reply_photo(
                photo=r['file_id'],
                caption=f"📸 {r['caption']} (из чата, {r['date'][:10]})"
            )
        except Exception as e:
            logger.warning(f"Не удалось отправить фото: {e}")


# ─── Запуск ──────────────────────────────────────────────────────────────────

def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("Не задан TELEGRAM_BOT_TOKEN в переменных окружения!")

    app = Application.builder().token(token).build()

    # Команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("tasks", cmd_my_tasks))
    app.add_handler(CommandHandler("all_tasks", cmd_all_tasks))
    app.add_handler(CommandHandler("overdue", cmd_overdue))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("debts", cmd_debtors))
    app.add_handler(CommandHandler("photo", cmd_photo))
    app.add_handler(CommandHandler("price", cmd_price))
    app.add_handler(CommandHandler("contact", cmd_contact))

    # Все сообщения (текст + медиа)
    app.add_handler(MessageHandler(filters.ALL, handle_message))

    # Планировщик (утренние сводки, напоминания)
    setup_scheduler(app, db)

    logger.info("🤖 Бот запущен!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
