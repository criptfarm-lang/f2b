"""
F2B PRO — Telegram Bot
Ассистент отдела продаж: задачи, фото, прайсы, дебиторка
"""

import asyncio
import logging
import os
import re
from datetime import datetime

from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    TypeHandler,
    ContextTypes,
    filters,
)

from database import Database
from scheduler import setup_scheduler, record_group_message, PDZ_MANAGERS, get_group_chat_id
from claude_ai import dispatch, smart_answer, extract_tasks_from_message, detect_task_completion, parse_product_query
from amocrm import check_connection as amo_check  # оставляем для совместимости
from moysklad import (search_products, search_products_filtered, get_price_list, format_products,
    format_price_list, get_product_image, download_image, get_image_download_url,
    get_counterparty_balance, get_all_debtors, format_debtors_ms, format_counterparty_balance,
    find_counterparty_info, format_counterparty_info,
    get_debtors_by_tag, get_clients_by_tag, resolve_tag,
    format_debtors_by_tag, format_clients_by_tag,
    get_overdue_demands, format_overdue_demands, format_overdue_summary,
    format_reminders_for_manager, format_debt_reminder, fmt_money)

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
BOT_TRIGGERS = ["эф,", "эф ", "бот,", "бот ", "@эф", "bot,", "bot ", "@f2b_assistant_bot", "@f2b_assistant"]


def is_bot_addressed(text: str) -> bool:
    """Проверяет, обращаются ли к боту."""
    if not text:
        return False
    text_lower = text.lower().strip()
    # Реагируем на обращение в начале или @mention в любом месте
    if any(text_lower.startswith(t) for t in BOT_TRIGGERS):
        return True
    # @mention может быть в любом месте сообщения
    if "@f2b_assistant" in text_lower or "эф," in text_lower or text_lower.startswith("эф "):
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
        "👋 Привет! Я Эф — ассистент F2B PRO.\n\n"
        "Обращайся ко мне: *Эф, [вопрос]*\n\n"
        "Примеры:\n"
        "• Эф, пришли фото тунца\n"
        "• Эф, какая цена на лосось?\n"
        "• Эф, задачи Карины\n"
        "• Эф, кто нам должен?\n\n"
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


# Ожидающие привязки контактов — user_id → {chat_id, channel_id, wazzup_name, chat_type}
_pending_links: dict = {}


async def handle_wazzup_ignore_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Помечает контакт как 'не наш клиент' — больше не присылать уведомления."""
    query = update.callback_query
    await query.answer()
    parts = query.data.split("|")
    chat_id_val = parts[1] if len(parts) > 1 else ""
    if chat_id_val:
        db.link_wazzup_contact(
            chat_id=chat_id_val,
            chat_type="telegram",
            channel_id="",
            company_name="__ignore__",
            wazzup_name="",
            role="игнор",
        )
    await query.message.edit_text("🚫 Контакт помечен как 'не наш клиент'. Уведомления больше не придут.")


async def handle_wazzup_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатие кнопки привязки Telegram контакта к компании."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split("|")

    # Выбор компании из списка похожих: wazzup_pick|index|link_key
    if parts[0] == "wazzup_pick":
        idx = int(parts[1])
        link_key = parts[2]
        pending = _pending_links.get(link_key) or _pending_links.get(query.from_user.id)
        if not pending:
            await query.message.edit_text("❌ Сессия истекла, попробуй снова.")
            return
        suggestions = pending.get("suggestions", [])
        if idx >= len(suggestions):
            await query.message.edit_text("❌ Ошибка выбора, попробуй снова.")
            return
        cp_name = suggestions[idx]
        pending["company_name"] = cp_name
        _pending_links[query.from_user.id] = {**pending, "link_key": link_key}
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📢 Для рассылки", callback_data=f"wazzup_role|рассылка|{link_key}"),
                InlineKeyboardButton("👤 Иной контакт", callback_data=f"wazzup_role|иной|{link_key}"),
            ],
            [
            ]
        ])
        await query.message.edit_text(
            f"✅ Нашёл: *{cp_name}*\n\nКакая роль у этого контакта?",
            parse_mode="Markdown",
            reply_markup=keyboard
        )
        return

    # Выбор роли: wazzup_role|роль|link_key
    if parts[0] == "wazzup_role":
        role = parts[1]
        link_key = parts[2]

        # Отмена
        if role == "отмена":
            _pending_links.pop(link_key, None)
            # Убираем и из user_id если есть
            for uid, v in list(_pending_links.items()):
                if v.get("link_key") == link_key:
                    _pending_links.pop(uid, None)
            await query.message.edit_text("❌ Привязка отменена. Контакт пока не идентифицирован.")
            return
        pending = _pending_links.get(link_key)
        # Если не нашли по link_key — ищем по user_id среди всех pending
        if not pending or "company_name" not in pending:
            for uid, v in _pending_links.items():
                if isinstance(uid, int) and v.get("link_key") == link_key and "company_name" in v:
                    pending = v
                    break
        if not pending or "company_name" not in pending:
            await query.message.edit_text("❌ Сессия истекла, попробуй снова.")
            return
        # Очищаем
        _pending_links.pop(link_key, None)
        for uid in [k for k, v in _pending_links.items() if isinstance(k, int) and v.get("link_key") == link_key]:
            _pending_links.pop(uid, None)
        ok = db.link_wazzup_contact(
            chat_id=pending["chat_id"],
            chat_type=pending["chat_type"],
            channel_id=pending["channel_id"],
            company_name=pending["company_name"],
            wazzup_name=pending["wazzup_name"],
            role=role,
        )
        if ok:
            await query.message.edit_text(
                f"✅ *{pending['wazzup_name']}* → *{pending['company_name']}* ({role})\nЭф запомнил!",
                parse_mode="Markdown"
            )
        return

    # Первое нажатие — запрашиваем название компании: wazzup_link|link_key
    if len(parts) < 2:
        return
    link_key = parts[1]
    pending = _pending_links.get(link_key)
    if not pending:
        await query.message.edit_text("❌ Сессия истекла, попробуй снова.")
        return

    # Помечаем что ждём ввода от этого пользователя
    _pending_links[query.from_user.id] = {**pending, "link_key": link_key}
    _pending_links[link_key] = _pending_links[query.from_user.id]

    await query.message.edit_text(
        f"👤 Контакт в TG: *{pending['wazzup_name']}*\n\n"
        f"Как этот клиент называется в МойСклад?\n"
        f"_(напиши название или часть названия)_",
        parse_mode="Markdown"
    )


async def cmd_clear_wazzup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет запись из wazzup_contact_map по chat_id. Только для руководителя."""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return
    chat_id_val = context.args[0] if context.args else ""
    if not chat_id_val:
        await update.message.reply_text("Укажи chat_id: /clearwazzup 360092495")
        return
    try:
        with db.conn.cursor() as cur:
            cur.execute("DELETE FROM wazzup_contact_map WHERE chat_id=%s", (chat_id_val,))
            cur.execute("DELETE FROM wazzup_contacts WHERE chat_id=%s", (chat_id_val,))
        db.conn.commit()
        await update.message.reply_text(f"✅ Запись {chat_id_val} удалена.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_wazzup_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбрасывает привязку Wazzup контакта. /wazzup_reset <chat_id>"""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return
    args = context.args
    if not args:
        await update.message.reply_text("Использование: /wazzup_reset <chat_id>")
        return
    chat_id_val = args[0]
    try:
        with db.conn.cursor() as cur:
            cur.execute("DELETE FROM wazzup_contact_map WHERE chat_id = %s", (chat_id_val,))
        db.conn.commit()
        await update.message.reply_text(f"✅ Привязка для `{chat_id_val}` сброшена.", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_wazzup_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список каналов Wazzup с их ID."""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return

    api_key = os.getenv("WAZZUP_API_KEY", "")
    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://api.wazzup24.com/v3/channels",
            headers={"Authorization": f"Bearer {api_key}"}
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                await update.message.reply_text(f"❌ Ошибка: {resp.status} {text[:200]}")
                return
            data = await resp.json()

    channels = data.get("channels", data) if isinstance(data, dict) else data
    if not channels:
        await update.message.reply_text("Каналов не найдено.")
        return

    lines = ["📡 *Каналы Wazzup:*\n"]
    for ch in channels if isinstance(channels, list) else [channels]:
        ch_id = ch.get("id", ch.get("channelId", "?"))
        name = ch.get("name", "")
        transport = ch.get("transport", "")
        status = ch.get("state", ch.get("status", ""))
        lines.append(f"• `{ch_id}`\n  transport: *{transport}* name: {name} status: {status}\n")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_wazzup_setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Настраивает вебхук Wazzup. /wazzup_setup"""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return

    api_key = os.getenv("WAZZUP_API_KEY", "")
    if not api_key:
        await update.message.reply_text("❌ WAZZUP_API_KEY не задан в Railway.")
        return

    import aiohttp
    webhook_url = "https://f2b-production.up.railway.app/webhook/wazzup"

    async with aiohttp.ClientSession() as session:
        # Устанавливаем вебхук
        async with session.patch(
            "https://api.wazzup24.com/v3/webhooks",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"webhooksUri": webhook_url, "subscriptions": {"messagesAndStatuses": True}}
        ) as resp:
            if resp.status == 200:
                await update.message.reply_text(
                    f"✅ Wazzup вебхук настроен!\n"
                    f"📡 URL: `{webhook_url}`\n\n"
                    f"Теперь все сообщения менеджеров будут сохраняться автоматически.",
                    parse_mode="Markdown"
                )
            else:
                text = await resp.text()
                await update.message.reply_text(f"❌ Ошибка: {resp.status} {text[:200]}")


async def cmd_clear_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет ВСЕ открытые задачи. Только для руководителя."""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return
    try:
        with db.conn.cursor() as cur:
            cur.execute("UPDATE tasks SET status='done', completed_at=NOW(), result='Удалено руководителем' WHERE status='open'")
        db.conn.commit()
        await update.message.reply_text("✅ Все открытые задачи очищены.")
    except Exception as e:
        logger.error(f"cmd_clear_all error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверяет все основные функции Эфа. Только для руководителя."""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return

    await update.message.reply_text("🔍 Запускаю диагностику...", parse_mode="Markdown")

    results = []

    async def check(name: str, coro, timeout: int = 8):
        try:
            result = await asyncio.wait_for(coro, timeout=timeout)
            if result:
                results.append(f"✅ {name}")
            else:
                results.append(f"⚠️ {name} — пустой результат")
        except asyncio.TimeoutError:
            results.append(f"⚠️ {name} — таймаут (>{timeout}с)")
        except Exception as e:
            results.append(f"❌ {name} — {str(e)[:60]}")

    # 1. БД — задачи
    try:
        tasks = db.get_all_open_tasks()
        results.append(f"✅ База данных — {len(tasks)} открытых задач")
    except Exception as e:
        results.append(f"❌ База данных — {e}")

    # 2. МойСклад — токен и базовый запрос
    async def test_ms():
        from moysklad import get_headers, MS_BASE
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{MS_BASE}/entity/organization", headers=get_headers()) as resp:
                return resp.status == 200
    await check("МойСклад API", test_ms())

    # 3. МойСклад — поиск товара
    async def test_ms_search():
        from moysklad import search_products
        rows = await search_products("лосось")
        return len(rows) > 0
    await check("МойСклад поиск товаров", test_ms_search())

    # 4. МойСклад — баланс контрагента
    async def test_ms_balance():
        from moysklad import get_counterparty_balance
        rows = await get_counterparty_balance("джи")
        return len(rows) > 0
    await check("МойСклад баланс контрагента", test_ms_balance())

    # 5. МойСклад — ПДЗ (лёгкая проверка — просто один запрос)
    async def test_pdz():
        from moysklad import get_headers, MS_BASE
        import aiohttp
        async with aiohttp.ClientSession() as session:
            url = f"{MS_BASE}/entity/customerorder?limit=1&expand=attributes"
            async with session.get(url, headers=get_headers()) as resp:
                return resp.status == 200
    await check("МойСклад ПДЗ (заказы)", test_pdz())

    # 6. Claude API — диспетчер
    async def test_claude():
        from claude_ai import dispatch
        result = await dispatch("привет", "Test")
        return result.get("action") is not None
    await check("Claude AI диспетчер", test_claude())

    # 7. Поиск фото
    async def test_photo():
        photos = db.search_media("лосось", media_type="photo")
        return True  # просто проверяем что БД отвечает
    await check("Поиск фото (канал Контент)", test_photo())

    # 8. Геокодер
    async def test_geocoder():
        from moysklad import geocode_address
        coords = await geocode_address("Истра, Московская область")
        return coords is not None
    await check("Яндекс геокодер", test_geocoder())

    # 9. Webhook сервер
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get("https://f2b-production.up.railway.app/health") as resp:
                if resp.status == 200:
                    results.append("✅ Webhook сервер")
                else:
                    results.append(f"⚠️ Webhook сервер — статус {resp.status}")
    except Exception as e:
        results.append(f"❌ Webhook сервер — {e}")

    # Итог
    ok = sum(1 for r in results if r.startswith("✅"))
    warn = sum(1 for r in results if r.startswith("⚠️"))
    err = sum(1 for r in results if r.startswith("❌"))

    header = f"📊 Диагностика Эфа\n✅ {ok} ок  ⚠️ {warn} предупреждений  ❌ {err} ошибок\n\n"
    await update.message.reply_text(header + "\n".join(results))


async def cmd_clear_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет все открытые задачи кроме указанных ID. Только для руководителя."""
    logger.info(f"cmd_clear_tasks вызван от {update.effective_user.id} args={context.args}")
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return
    try:
        args = context.args
        if args and args[0] == "keep":
            keep_ids = [int(x) for x in args[1:] if x.isdigit()]
            db = Database()
            with db.conn.cursor() as cur:
                if keep_ids:
                    placeholders = ",".join(["%s"] * len(keep_ids))
                    cur.execute(f"UPDATE tasks SET status='done', completed_at=NOW(), result='Удалено руководителем' WHERE status='open' AND id NOT IN ({placeholders})", keep_ids)
                else:
                    cur.execute("UPDATE tasks SET status='done', completed_at=NOW(), result='Удалено руководителем' WHERE status='open'")
            db.conn.commit()
            db.conn.close()
            await update.message.reply_text(f"✅ Все задачи очищены." if not keep_ids else f"✅ Задачи очищены. Оставлены ID: {keep_ids}")
        else:
            db = Database()
            tasks = db.get_all_open_tasks()
            if not tasks:
                await update.message.reply_text("Нет открытых задач.")
                return
            lines = [f"ID {t['id']}: {t.get('executor','—')} — {t.get('text','')}" for t in tasks]
            lines.append("\nЧтобы оставить только нужные: /cleartasks keep 5 12")
            await update.message.reply_text("\n".join(lines))
    except Exception as e:
        logger.error(f"cmd_clear_tasks error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {e}")


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
    """Все открытые и недавно выполненные задачи команды."""
    tasks = db.get_all_open_tasks()
    done_tasks = db.get_recently_done(hours=24)

    if not tasks and not done_tasks:
        await update.message.reply_text("✅ Нет открытых задач!")
        return

    lines = []

    if tasks:
        # Группируем открытые по исполнителю
        by_user = {}
        for t in tasks:
            exe = t.get('executor') or 'Неизвестно'
            by_user.setdefault(exe, []).append(t)

        lines.append("📋 *Открытые задачи:*\n")
        MONTHS = ["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"]
        for user, utasks in by_user.items():
            lines.append(f"*{user}* ({len(utasks)}):")
            for t in utasks:
                icon = "🔴" if t.get('overdue') else "🟡"
                dl = t.get('deadline')
                if dl:
                    try:
                        from datetime import date as _date
                        d = _date.fromisoformat(str(dl)[:10])
                        deadline_str = f" · до {d.day} {MONTHS[d.month-1]}"
                    except Exception:
                        deadline_str = f" · до {dl}"
                else:
                    deadline_str = ""
                lines.append(f"  {icon} {t['text']}{deadline_str}")
            lines.append("")

    if done_tasks:
        lines.append("✅ *Выполнено за 24 часа:*\n")
        for t in done_tasks:
            exe = t.get('executor') or ''
            result = t.get('result') or ''
            result_str = f" — {result}" if result else ""
            lines.append(f"  ✅ *{exe}*: {t['text']}{result_str}")

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

    # Команды обрабатываются отдельными CommandHandler — пропускаем
    if message.text and message.text.startswith("/"):
        return

    chat_id = message.chat_id
    user = message.from_user
    text = message.text or message.caption or ""

    # В группе ИДЕНТИФИКАЦИЯ — только кнопки, текст игнорируем
    wazzup_id_chat = int(os.getenv("WAZZUP_ID_CHAT_ID", "0"))
    if wazzup_id_chat and chat_id == wazzup_id_chat:
        # Обрабатываем только ввод названия компании (pending_links)
        if user and user.id in _pending_links and text and not text.startswith("/"):
            pass  # продолжаем — это ввод названия компании
        else:
            return

    # Сохраняем все сообщения в историю чата
    if text and user:
        db.save_message(
            chat_id=chat_id,
            user_id=user.id,
            user_name=user.full_name,
            text=text[:1000],  # обрезаем очень длинные
        )


    # Записываем сообщения группы для анализа ПДЗ (пн/ср)
    group_chat_id = get_group_chat_id()
    if text and user and group_chat_id and chat_id == group_chat_id:
        from datetime import date as _date
        if _date.today().weekday() in (0, 2):  # 0=пн, 2=ср
            sender_lower = user.full_name.lower()
            matched = False
            for mgr in PDZ_MANAGERS:
                if mgr["name"].lower() in sender_lower or mgr["tag"] in sender_lower:
                    record_group_message(user.full_name, mgr["tag"], text)
                    matched = True
                    break
            if not matched:
                record_group_message(user.full_name, "_all", text)
    # 1. Сохраняем документы в базу (фото берём из МойСклад)
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

    # Логируем ID для диагностики
    logger.info(f"Message from user.id={user.id}, name={user.full_name}, chat_id={message.chat_id}, manager_ids={manager_ids}")

    if user.id in manager_ids and len(text) > 15:
        # Если сообщение адресовано боту — извлекаем задачи только если
        # в тексте есть имя другого сотрудника (поручение через бота)
        should_extract = True
        if is_bot_addressed(text):
            text_lower = text.lower()
            has_employee = any(
                name.lower() in text_lower
                for name in ["карина", "баласанян", "александра", "белякова", "саша",
                             "юля", "гераскина", "татьяна", "голубева", "алексей", "дубинин",
                             "андрей", "иванов", "антон", "кормилицын", "катя", "куревлева",
                             "леонтьев", "лёша", "маланчук", "малышкин", "елена", "лена",
                             "мерзлякова", "владимир", "петровский", "самир", "садыгов",
                             "оксана", "сайгашкина", "инесса", "скляр", "исрафил", "магаммед"]
            )
            should_extract = has_employee

        if should_extract:
            tasks = await extract_tasks_from_message(text, user.full_name)
            saved_count = 0
            task_lines = []
            for task in tasks:
                executor = task.get("executor", "")
                if not task.get("task") or not executor:
                    continue  # пропускаем задачи без исполнителя
                db.save_task(
                    text=task["task"],
                    executor=executor,
                    deadline=task.get("deadline"),
                    source_chat=chat_id,
                    source_message_id=message.message_id,
                    created_by=user.full_name
                )
                saved_count += 1
                deadline = task.get("deadline")
                if deadline:
                    from datetime import date
                    MONTHS = ["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"]
                    try:
                        d = date.fromisoformat(deadline)
                        deadline_str = f" · до {d.day} {MONTHS[d.month-1]}"
                    except Exception:
                        deadline_str = f" · до {deadline}"
                else:
                    deadline_str = ""
                task_lines.append(f"👤 *{executor}*{deadline_str}: {task['task']}")
                logger.info(f"Задача: {executor} → {task['task']}")

            if saved_count > 0:
                lines = [f"📌 Зафиксировано задач: {saved_count}\n"] + task_lines
                await message.reply_text("\n".join(lines), parse_mode="Markdown")

    # 3. Автозакрытие задач — Claude анализирует контекст
    sender_name = update.effective_user.full_name if update.effective_user else ""
    if not is_bot_addressed(text) and len(text) > 5:
        open_tasks = db.get_all_open_tasks()
        if open_tasks:
            completed_items = await detect_task_completion(text, open_tasks, author=sender_name)
            if completed_items:
                closed = []
                for item in completed_items:
                    task_id = item["id"]
                    result = item.get("result", "")
                    task = next((t for t in open_tasks if t['id'] == task_id), None)
                    if task:
                        db.complete_task(task_id, result=result, completed_by=sender_name)
                        executor = task.get('executor', '')
                        result_str = f" — {result}" if result else ""
                        closed.append(f"✅ *{executor}*: {task['text']}{result_str}")
                        logger.info(f"Автозакрытие задачи {task_id}: {task['text']} | результат: {result}")
                if closed:
                    lines = ["🤖 Эф зафиксировал выполнение:\n"] + closed
                    await message.reply_text("\n".join(lines), parse_mode="Markdown")

    # 4. Реагируем на обращение к боту
    # Автоматически реагируем на IT-проблемы даже без обращения "Эф,"
    IT_KEYWORDS = [
        "телеграм не", "telegram не", "амо не", "амосрм", "amocrm",
        "crm не", "срм не", "не отправляется", "не загружается",
        "не проходят звонки", "звонки не", "почта не", "не приходит письмо",
        "не работает телеграм", "не работает амо", "не работает crm",
        "слетела интеграция", "нет сообщений в амо", "не открывается амо",
    ]
    text_lower_it = text.lower()
    if not is_bot_addressed(text) and any(kw in text_lower_it for kw in IT_KEYWORDS):
        await message.reply_text(
            "По техническим вопросам (Telegram, amoCRM, звонки, почта) "
            "пишите в группу **IT8 & ОП ФИШ ТУ БИЗНЕС** 🛠",
            parse_mode="Markdown"
        )
        return

    # Проверяем ожидание привязки Wazzup контакта
    if user and user.id in _pending_links and not is_bot_addressed(text):
        pending_link = _pending_links[user.id]
        if "company_name" not in pending_link:
            # Если уже показали варианты — просим нажать кнопку
            if pending_link.get("suggestions"):
                await message.reply_text("👆 Выбери компанию из списка выше или нажми «Не привязывать».")
                return
            # Ищем компанию в МойСклад
            company_query = text.strip()
            counterparties = await get_counterparty_balance(company_query)
            if not counterparties:
                # Пробуем найти по каждому слову отдельно
                words = company_query.split()
                suggestions = []
                for word in words:
                    if len(word) >= 3:
                        found = await get_counterparty_balance(word)
                        for c in found:
                            if c not in suggestions:
                                suggestions.append(c)

                if suggestions:
                    link_key = pending_link.get("link_key", str(user.id))
                    # Сохраняем варианты в pending_link
                    pending_link["suggestions"] = [c.get("name","") for c in suggestions[:5]]
                    buttons = []
                    for i, c in enumerate(suggestions[:5]):
                        cp_name = c.get("name", "")
                        buttons.append([InlineKeyboardButton(
                            cp_name[:40],
                            callback_data=f"wazzup_pick|{i}|{link_key}"
                        )])
                    buttons.append([InlineKeyboardButton(
                        "🚫 Не привязывать",
                        callback_data=f"wazzup_role|отмена|{link_key}"
                    )])
                    await message.reply_text(
                        f"❓ *{company_query}* не найдена точно.\n\nВозможно имеется в виду:",
                        parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup(buttons)
                    )
                else:
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("🚫 Не привязывать", callback_data=f"wazzup_role|отмена|{pending_link.get('link_key', str(user.id))}")
                    ]])
                    await message.reply_text(
                        f"❌ Компания *{company_query}* не найдена в МойСклад.\n"
                        f"Попробуй написать название точнее или отмени привязку.",
                        parse_mode="Markdown",
                        reply_markup=keyboard
                    )
                return

            cp = counterparties[0]
            cp_name = cp.get("name", company_query)
            pending_link["company_name"] = cp_name
            link_key = pending_link.get("link_key", str(user.id))

            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📢 Для рассылки", callback_data=f"wazzup_role|рассылка|{link_key}"),
                    InlineKeyboardButton("👤 Иной контакт", callback_data=f"wazzup_role|иной|{link_key}"),
                ]
            ])
            await message.reply_text(
                f"✅ Нашёл: *{cp_name}*\n\nКакая роль у этого контакта?",
                parse_mode="Markdown",
                reply_markup=keyboard
            )
            return
        _pending_links.pop(user.id, None)
        return

    if not is_bot_addressed(text):
        return

    query = clean_query(text)

    # ── Всё через Claude — он сам разбирается что нужно ──
    await message.reply_chat_action("typing")
    context_data = db.get_context_summary()
    chat_history = db.format_history(chat_id, limit=40)
    memories = db.format_memories()

    logger.info(f"Dispatching query='{query}' from '{user.full_name}'")
    result = await dispatch(query, user.full_name, context_data,
                            chat_history=chat_history, memories=memories)
    logger.info(f"Dispatch result: {result}")
    action = result.get("action", "answer")
    params = result.get("params", {})

    if action == "get_tasks":
        employee = params.get("employee")
        if employee:
            tasks = db.get_tasks_for_user(employee)
            done = [t for t in db.get_recently_done(hours=24)
                    if employee.lower() in (t.get('executor') or '').lower()]
            if not tasks and not done:
                await message.reply_text(f"✅ У *{employee}* нет задач.", parse_mode="Markdown")
            else:
                lines = [f"📋 *Задачи — {employee}:*\n"]
                for t in tasks:
                    deadline_str = f" — до {t['deadline']}" if t.get("deadline") else ""
                    icon = "🔴" if t.get("overdue") else "🟡"
                    lines.append(f"{icon} {t['text']}{deadline_str}")
                if done:
                    lines.append("")
                    for t in done:
                        result_str = f" — {t['result']}" if t.get('result') else ""
                        lines.append(f"✅ {t['text']}{result_str}")
                await message.reply_text("\n".join(lines), parse_mode="Markdown")
        else:
            await cmd_all_tasks(update, context)

    elif action == "get_all_tasks":
        await cmd_all_tasks(update, context)

    elif action == "get_report":
        await cmd_report(update, context)

    elif action == "get_debtors":
        await message.reply_chat_action("typing")
        debtors = await get_all_debtors()
        text = format_debtors_ms(debtors)
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "get_debt":
        debt_query = params.get("query", "")
        await message.reply_chat_action("typing")
        counterparties = await get_counterparty_balance(debt_query)
        text = format_counterparty_balance(counterparties, debt_query)
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "find_counterparty":
        cp_query = params.get("query", "")
        await message.reply_chat_action("typing")
        counterparties = await find_counterparty_info(cp_query)
        text = format_counterparty_info(counterparties, cp_query)
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "get_group_debts":
        raw_tag = params.get("tag", "")
        tag = resolve_tag(raw_tag)
        await message.reply_chat_action("typing")
        items = await get_debtors_by_tag(tag)
        text = format_debtors_by_tag(items, tag)
        if len(text) > 4000:
            text = text[:3900] + "\n\n_...уточни запрос_"
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "get_group_clients":
        raw_tag = params.get("tag", "")
        tag = resolve_tag(raw_tag)
        await message.reply_chat_action("typing")
        items = await get_clients_by_tag(tag)
        text = format_clients_by_tag(items, tag)
        if len(text) > 4000:
            text = text[:3900] + "\n\n_...слишком много, уточни_"
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "get_overdue_debt":
        raw_tag = params.get("tag", "")
        raw_query = params.get("query", "")
        brief = params.get("brief", False)
        tag = resolve_tag(raw_tag) if raw_tag else None
        await message.reply_chat_action("typing")
        items = await get_overdue_demands(tag=tag, query=raw_query)
        label = raw_query or (tag or None)
        if brief and not raw_query:
            text = format_overdue_summary(items)
        else:
            text = format_overdue_demands(items, tag=label)
        if len(text) > 4000:
            text = text[:3900] + "\n\n_...уточни запрос_"
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "prepare_reminders":
        USER_MANAGER_TAGS = {
            "карина": "баласанян", "баласанян": "баласанян",
            "инесса": "скляр", "скляр": "скляр",
            "елена": "мерзлякова", "мерзлякова": "мерзлякова",
            "татьяна": "голубева", "голубева": "голубева",
            "алексей": "леонтьев", "леонтьев": "леонтьев",
        }
        USER_MANAGER_DISPLAY = {
            "баласанян": "Карина Баласанян",
            "скляр": "Инесса Скляр",
            "мерзлякова": "Елена Мерзлякова",
            "голубева": "Татьяна Голубева",
            "леонтьев": "Алексей Леонтьев",
        }
        full_name_lower = user.full_name.lower()
        manager_tag = None
        manager_display = user.full_name
        for key, tag in USER_MANAGER_TAGS.items():
            if key in full_name_lower:
                manager_tag = tag
                manager_display = USER_MANAGER_DISPLAY.get(tag, user.full_name)
                break
        # Руководитель может указать тег явно
        if not manager_tag and params.get("tag"):
            manager_tag = resolve_tag(params["tag"])
            manager_display = USER_MANAGER_DISPLAY.get(manager_tag, params["tag"].capitalize())
        raw_query = params.get("query", "")
        await message.reply_chat_action("typing")
        items = await get_overdue_demands(tag=manager_tag, query=raw_query)
        if not items:
            await message.reply_text("✅ Просроченных клиентов нет — напоминания не нужны.")
        else:
            header = (
                f"📋 *Напоминания об оплате — {manager_display}*\n"
                f"{len(items)} клиентов · скопируй и отправь каждому"
            )
            await message.reply_text(header, parse_mode="Markdown")
            for c in sorted(items, key=lambda x: x["overdue_sum"], reverse=True):
                reminder = format_debt_reminder(c)
                label = f"💬 {c['name']} — {fmt_money(c['overdue_sum'])}\n\n{reminder}"
                await message.reply_text(label)

    elif action == "find_photo":
        photo_query = params.get("query", query)
        await search_and_send_photo(update, context, photo_query)

    elif action == "get_price":
        # Сначала пробуем МойСклад
        ms_token = os.getenv("MOYSKLAD_TOKEN")
        if ms_token:
            await message.reply_chat_action("typing")
            products = await get_price_list(limit=50)
            if products:
                text = format_price_list(products)
                # Telegram ограничивает 4096 символов
                if len(text) > 4000:
                    text = text[:3900] + "\n\n_...показаны первые позиции_"
                await message.reply_text(text, parse_mode="Markdown")
                return
        await cmd_price(update, context)

    elif action == "ms_search":
        # Поиск товара в МойСклад — Claude разбирает запрос на фильтры
        ms_query = params.get("query", query)
        await message.reply_chat_action("typing")
        parsed = await parse_product_query(ms_query)
        logger.info(f"parse_product_query result: {parsed}")
        
        # Принудительный in_stock если пользователь явно спросил "в наличии" / "есть на складе"
        stock_keywords = ["в наличии", "на складе", "есть ли", "что есть", "имеется"]
        if any(kw in ms_query.lower() for kw in stock_keywords):
            parsed.setdefault("filters", {})["in_stock"] = True
            logger.info("Forced in_stock=True based on query keywords")
        
        products = await search_products_filtered(parsed)
        if not products:
            products = await search_products(ms_query)
        text = format_products(products, ms_query)
        if len(text) > 4000:
            text = text[:3900] + "\n\n_...слишком много результатов, уточни запрос_"
        await message.reply_text(text, parse_mode="Markdown")

        # Если один товар и есть фото — пробуем прислать
        if len(products) == 1 and products[0].get("image_href"):
            try:
                img_bytes = await download_image(products[0]["image_href"])
                if img_bytes:
                    import io as _io
                    await message.reply_photo(
                        photo=_io.BytesIO(img_bytes),
                        caption=f"📸 {products[0]['name']}"
                    )
            except Exception as e:
                logger.warning(f"Не удалось отправить фото из МойСклад: {e}")

    elif action == "find_buyers":
        product = params.get("product", "")
        period_days = params.get("period_days", 30)
        if not product:
            await message.reply_text("❌ Не указан товар.")
            return
        await message.reply_chat_action("typing")
        await message.reply_text(f"🔍 Ищу покупателей *{product}* за последние {period_days} дней...", parse_mode="Markdown")
        from moysklad import get_buyers_by_product
        result = await get_buyers_by_product(product, period_days=period_days)
        buyers = result.get("buyers", []) if isinstance(result, dict) else result
        found_name = result.get("product_name", product) if isinstance(result, dict) else product
        if not buyers:
            await message.reply_text(
                f"❌ Покупателей *{found_name}* за последние {period_days} дней не найдено.\n"
                f"_Искал товар: {found_name}_", parse_mode="Markdown")
            return
        lines = [f"👥 *Покупатели {found_name}* за {period_days} дней ({len(buyers)}):\n"]
        for b in buyers:
            lines.append(f"• {b['name']}")
        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:3900] + "\n\n_...и ещё_"
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "get_delivery_days":
        address = params.get("address", "")
        if not address:
            await message.reply_text("❌ Укажи адрес или город.")
            return
        await message.reply_chat_action("typing")
        from moysklad import check_delivery_schedule, DELIVERY_CITIES_COORDS, _CITY_INDEX, WEEKDAYS_RU, geocode_address, _haversine

        # Текстовый поиск по городу
        address_lower = address.lower()
        found_keyword = None
        for keyword in sorted(_CITY_INDEX.keys(), key=len, reverse=True):
            if keyword in address_lower:
                found_keyword = keyword
                break

        if found_keyword:
            info = _CITY_INDEX[found_keyword]
            canonical = info["canonical"]
            days = [WEEKDAYS_RU[d] for d in sorted(info["days"])]
            days_str = ", ".join(days)
            await message.reply_text(
                f"🚛 *{canonical}*\n📅 Дни доставки: *{days_str}*",
                parse_mode="Markdown"
            )
            return

        # Московский адрес?
        if "москва" in address_lower or "moscow" in address_lower:
            await message.reply_text("🚛 *Москва* — доставляем в любой рабочий день.", parse_mode="Markdown")
            return

        # Геокодируем
        coords = await geocode_address(address)
        if not coords:
            await message.reply_text(f"😕 Не удалось определить направление для адреса: {address}")
            return

        lat, lon = coords
        dist_from_moscow = _haversine(lat, lon, 55.7558, 37.6173)
        if dist_from_moscow < 35:
            await message.reply_text("🚛 Адрес в московской агломерации — доставляем в любой рабочий день.", parse_mode="Markdown")
            return

        # Ищем ближайший город
        nearest_city = None
        nearest_dist = float("inf")
        for city, (clat, clon) in DELIVERY_CITIES_COORDS.items():
            dist = _haversine(lat, lon, clat, clon)
            if dist < nearest_dist:
                nearest_dist = dist
                nearest_city = city

        if nearest_dist > 25 or not nearest_city:
            await message.reply_text(
                f"😕 Адрес *{address}* не входит ни в одно наше направление МО.\n"
                f"Уточни у руководителя.",
                parse_mode="Markdown"
            )
            return

        # Нашли ближайший город — берём его дни
        days = []
        for keyword, info in _CITY_INDEX.items():
            if info["canonical"] == nearest_city:
                days = [WEEKDAYS_RU[d] for d in sorted(info["days"])]
                break

        days_str = ", ".join(days) if days else "уточни у руководителя"
        await message.reply_text(
            f"🚛 Адрес близко к *{nearest_city}* ({round(nearest_dist)} км)\n"
            f"📅 Дни доставки: *{days_str}*",
            parse_mode="Markdown"
        )

    elif action == "send_message_to_client":
        client_query = params.get("client", "")
        msg_text = params.get("message", "")

        if not client_query:
            await message.reply_text("❌ Укажи клиента.")
            return

        await message.reply_chat_action("typing")

        # Находим контрагента в МойСклад
        from moysklad import get_counterparty_phones
        counterparties = await get_counterparty_balance(client_query)
        if not counterparties:
            await message.reply_text(f"❌ Клиент *{client_query}* не найден в МойСклад.", parse_mode="Markdown")
            return

        cp = counterparties[0]
        cp_name = cp.get("name", client_query)

        # Берём телефон
        phones = await get_counterparty_phones([{"id": cp.get("id",""), "name": cp_name, "href": cp.get("href","")}])
        phone = phones[0].get("phone") if phones else None

        if not phone:
            await message.reply_text(f"❌ У клиента *{cp_name}* нет телефона в МойСклад.", parse_mode="Markdown")
            return

        # Если текст не задан — формируем напоминание об оплате
        if not msg_text:
            balance = cp.get("balance", 0)
            debt = abs(balance) if balance < 0 else 0
            if debt > 0:
                from moysklad import fmt_money
                msg_text = f"Добрый день! Напоминаем о задолженности перед компанией F2B в размере {fmt_money(debt)}. Просьба произвести оплату. Спасибо!"
            else:
                await message.reply_text(f"❌ У *{cp_name}* нет долга. Укажи текст сообщения явно.", parse_mode="Markdown")
                return

        # Определяем каналы в порядке приоритета TG → Max → WhatsApp
        CHANNEL_MAP = {
            "telegram": "ddd24a95-9304-4098-a320-3e47fcd1020a",
            "tgapi":    "ddd24a95-9304-4098-a320-3e47fcd1020a",
            "max":      "1d5bc70a-7ca6-4895-8d1f-9690cf448214",
            "whatsapp": "e180aa1d-dc48-4d0a-bec3-fc0afc53cf03",
        }
        PRIORITY = ["telegram", "tgapi", "max", "whatsapp"]

        # Ищем известные каналы клиента из вебхуков — по имени или телефону
        known = db.get_wazzup_contacts(cp_name)
        # Также ищем по номеру телефона
        if phone and not known:
            known = db.get_wazzup_contacts(phone[-10:])  # последние 10 цифр
        channels_to_try = []
        for p in PRIORITY:
            for k in known:
                if k.get("chat_type") in (p,):
                    channels_to_try.append({
                        "channel_id": k["channel_id"],
                        "chat_type": k["chat_type"],
                        "chat_id": k["chat_id"],
                    })
                    break

        # Fallback — WhatsApp по номеру телефона если нет известных каналов
        if not any(c["chat_type"] in ("whatsapp",) for c in channels_to_try):
            channels_to_try.append({
                "channel_id": CHANNEL_MAP["whatsapp"],
                "chat_type": "whatsapp",
                "chat_id": phone,
            })

        # Показываем превью с кнопками — ждём подтверждения
        import uuid as _uuid
        msg_key = str(_uuid.uuid4())[:8]
        _pending_sends[msg_key] = {
            "channels": channels_to_try,
            "name": cp_name,
            "text": msg_text,
        }

        group_chat_id = get_group_chat_id()
        target_chat = group_chat_id or chat_id

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Отправить", callback_data=f"send_confirm|{msg_key}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"send_cancel|{msg_key}"),
        ]])
        await context.bot.send_message(
            chat_id=target_chat,
            text=(
                f"📤 *Сообщение клиенту*\n\n"
                f"👤 *{cp_name}*\n"
                f"📱 {phone}\n\n"
                f"💬 _{msg_text}_"
            ),
            parse_mode="Markdown",
            reply_markup=keyboard
        )

    elif action == "search_mentions":
        product = params.get("product", "")
        days = int(params.get("days", 7))
        manager_filter = params.get("manager", "")

        if not product:
            await message.reply_text("❌ Укажи товар для поиска.")
            return

        await message.reply_chat_action("typing")

        # Разбиваем на несколько товаров если через запятую
        keywords = [p.strip().lower() for p in product.replace(" и ", ",").split(",") if p.strip()]

        rows = db.search_wazzup_mentions(keywords, days=days, manager_name=manager_filter or None)

        if not rows:
            await message.reply_text(
                f"😕 Упоминаний *{product}* за последние {days} дней не найдено.\n"
                f"_Данные накапливаются с момента подключения Wazzup._",
                parse_mode="Markdown"
            )
            return

        # Группируем по менеджеру
        by_manager = {}
        for row in rows:
            mgr = row.get("manager_name") or "Неизвестно"
            by_manager.setdefault(mgr, []).append(row)

        lines = [f"🔍 *Упоминания «{product}»* за {days} дней\n"]
        for mgr, msgs in sorted(by_manager.items()):
            clients = list({r.get("contact_name", "") for r in msgs if r.get("contact_name")})
            lines.append(f"👤 *{mgr}* — {len(msgs)} сообщений, {len(clients)} клиентов:")
            for c in clients[:10]:
                lines.append(f"  • {c}")
            if len(clients) > 10:
                lines.append(f"  _...и ещё {len(clients)-10}_")
            lines.append("")

        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:3900] + "\n\n_...уточни запрос_"
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "broadcast":
        product = params.get("product", "")
        broadcast_text = params.get("message", "")
        manager_filter = params.get("manager", "")

        if not product or not broadcast_text:
            await message.reply_text("❌ Не указан товар или текст сообщения.")
            return

        await message.reply_chat_action("typing")
        period_days = params.get("period_days", 180)
        await message.reply_text(
            f"🔍 Ищу клиентов которые покупали *{product}* за последние {period_days} дней...",
            parse_mode="Markdown"
        )

        # 1. Находим покупателей через МойСклад
        from moysklad import get_buyers_by_product, get_counterparty_phones
        result = await get_buyers_by_product(product, period_days=period_days)
        buyers = result.get("buyers", []) if isinstance(result, dict) else result
        found_name = result.get("product_name", product) if isinstance(result, dict) else product

        if not buyers:
            await message.reply_text(f"❌ Не найдено покупателей *{found_name}* за последние {period_days} дней.", parse_mode="Markdown")
            return

        await message.reply_text(f"📋 Найдено {len(buyers)} покупателей. Получаю телефоны...", parse_mode="Markdown")

        # 2. Получаем телефоны из МойСклад
        contacts = await get_counterparty_phones(buyers)
        with_phone = [c for c in contacts if c.get("phone")]
        no_phone = [c for c in contacts if not c.get("phone")]

        if not with_phone:
            await message.reply_text("❌ Ни у одного клиента нет телефона в МойСклад.")
            return

        # 3. Показываем список и просим подтверждение
        duration_min = len(with_phone)
        names_preview = "\n".join(f"• {c['name']} ({c['phone']})" for c in with_phone[:10])
        if len(with_phone) > 10:
            names_preview += f"\n_...и ещё {len(with_phone) - 10}_"

        no_phone_note = f"\n⚠️ Без телефона ({len(no_phone)}): {', '.join(c['name'] for c in no_phone[:5])}" if no_phone else ""

        confirm_text = (
            f"📣 *Рассылка готова*\n\n"
            f"*Товар:* {found_name}\n"
            f"*Текст:* _{broadcast_text}_\n\n"
            f"*Получатели ({len(with_phone)}):*\n{names_preview}{no_phone_note}\n\n"
            f"⏱ Рассылка займёт ~{duration_min} мин (1 сообщение в минуту)\n\n"
            f"Для подтверждения напиши: *да, рассылай*"
        )
        await message.reply_text(confirm_text, parse_mode="Markdown")

        # Сохраняем и показываем кнопку подтверждения
        import uuid as _uuid
        broadcast_key = str(_uuid.uuid4())[:8]
        _pending_sends[f"broadcast_{broadcast_key}"] = {
            "contacts": with_phone,
            "text": broadcast_text,
            "product": found_name,
            "is_broadcast": True,
        }

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Начать рассылку", callback_data=f"send_confirm|broadcast_{broadcast_key}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"send_cancel|broadcast_{broadcast_key}"),
        ]])
        await message.reply_text(confirm_text, parse_mode="Markdown", reply_markup=keyboard)

    elif action == "find_contact":
        contact_query = params.get("query", "")
        contacts = db.search_contacts(contact_query)
        if contacts:
            lines = [f"📞 *{c['name']}* — {c['phone']} ({c.get('company', '')})" for c in contacts]
            await message.reply_text("\n".join(lines), parse_mode="Markdown")
        else:
            await message.reply_text(f"Контакт '{contact_query}' не найден в базе.")

    elif action == "answer":
        text = params.get("text")
        if text:
            await message.reply_text(text)
        else:
            # Claude не дал готовый ответ — спрашиваем отдельно
            response = await smart_answer(query, user.full_name, context_data)
            await message.reply_text(response)

    else:
        # Неизвестное действие — текстовый ответ
        response = await smart_answer(query, user.full_name, context_data)
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


async def search_photo_in_content_channel(context: ContextTypes.DEFAULT_TYPE, query: str) -> list:
    """Ищет фото в канале Контент F2B по ключевым словам в подписи.
    Возвращает список (file_id, caption) подходящих фото.
    """
    content_chat_id = int(os.getenv("CONTENT_CHAT_ID", "-1001433042091"))
    query_lower = query.lower()
    results = []

    # Ищем в локальной БД (фото из канала сохраняются при поступлении)
    photos = db.search_media(query_lower, media_type="photo")
    for p in photos:
        if p.get("chat_id") == content_chat_id:
            results.append({"file_id": p["file_id"], "caption": p.get("caption", "")})

    return results


async def search_and_send_photo(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str):
    """Ищет фото товара: сначала в канале Контент, потом в МойСклад."""
    import io as _io
    await update.message.reply_chat_action("upload_photo")

    # 1. Ищем в канале Контент F2B
    content_photos = await search_photo_in_content_channel(context, query)
    if content_photos:
        sent = 0
        for p in content_photos[:3]:
            try:
                await update.message.reply_photo(
                    photo=p["file_id"],
                    caption=f"📸 {p['caption']}" if p["caption"] else f"📸 {query}"
                )
                sent += 1
            except Exception as e:
                logger.warning(f"Не удалось отправить фото из Контент: {e}")
        if sent > 0:
            return

    # 2. Fallback — ищем в МойСклад
    products = await search_products(query)
    with_photo = [p for p in products if p.get("image_href")]

    if not with_photo:
        if products:
            text = format_products(products, query)
            await update.message.reply_text(
                f"😕 Фото не найдено, но вот что есть:\n\n{text}",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(f"😕 Фото '{query}' не найдено ни в канале Контент, ни в МойСклад.")
        return

    sent = 0
    for product in with_photo[:3]:
        try:
            img_bytes = await download_image(product["image_href"])
            if img_bytes:
                name = product.get("name", query)
                price = product.get("sale_price", 0)
                stock = product.get("stock", 0)
                caption = f"📸 {name}"
                if price:
                    caption += f"\n💰 {price} руб/кг"
                if stock and stock > 0:
                    caption += f"\n📦 В наличии: {stock} кг"
                await update.message.reply_photo(
                    photo=_io.BytesIO(img_bytes),
                    caption=caption
                )
                sent += 1
        except Exception as e:
            logger.warning(f"Не удалось отправить фото {product.get('name')}: {e}")

    if sent == 0:
        await update.message.reply_text(f"😕 Фото '{query}' есть в МойСклад, но не удалось загрузить.")


async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает посты из канала Контент F2B — сохраняет фото в БД."""
    message = update.channel_post
    if not message:
        return

    # Логируем chat_id для диагностики (можно удалить после настройки)
    logger.info(f"channel_post from chat_id={message.chat_id}, title='{message.chat.title}', caption='{message.caption or message.text or ''}'")

    content_chat_id = int(os.getenv("CONTENT_CHAT_ID", "-1001433042091"))
    if message.chat_id != content_chat_id:
        logger.info(f"channel_post: chat_id {message.chat_id} != CONTENT_CHAT_ID {content_chat_id}, пропускаем")
        return

    caption = message.caption or message.text or ""

    if message.photo:
        file_id = message.photo[-1].file_id
        db.save_media(
            file_id=file_id,
            media_type="photo",
            caption=caption,
            chat_id=message.chat_id,
            uploader="Контент F2B",
            date=datetime.now().isoformat()
        )
        logger.info(f"Сохранено фото из канала Контент: '{caption}' file_id={file_id}")

    elif message.document and message.document.mime_type and message.document.mime_type.startswith("image/"):
        file_id = message.document.file_id
        db.save_media(
            file_id=file_id,
            media_type="photo",
            caption=caption or message.document.file_name or "",
            chat_id=message.chat_id,
            uploader="Контент F2B",
            date=datetime.now().isoformat()
        )
        logger.info(f"Сохранено фото-документ из канала Контент: '{caption}'")


# ─── Запуск ──────────────────────────────────────────────────────────────────

async def cmd_memory(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает что Эф помнит."""
    memories = db.get_all_memories()
    if not memories:
        await update.message.reply_text("🧠 Долгосрочная память пуста.")
        return
    lines = ["🧠 *Что я помню:*\n"]
    for m in memories[:20]:
        lines.append(f"• *{m['key']}*: {m['value']}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_remember(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Явно запомнить факт: /remember ключ: значение"""
    args = " ".join(context.args) if context.args else ""
    if ":" not in args:
        await update.message.reply_text(
            "Формат: /remember ключ: значение\n"
            "Например: /remember скидка Иванову: 5%"
        )
        return
    key, value = args.split(":", 1)
    db.remember(key.strip(), value.strip())
    await update.message.reply_text(f"✅ Запомнил: *{key.strip()}* → {value.strip()}", parse_mode="Markdown")


async def cmd_add_webhook(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Создаёт вебхуки в МойСклад. /add_webhook"""
    user = update.message.from_user
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]
    if user.id not in manager_ids:
        await update.message.reply_text("⛔ Только для руководителей.")
        return

    import aiohttp
    token = os.getenv("MOYSKLAD_TOKEN")
    webhook_url = "https://f2b-production.up.railway.app/webhook/moysklad"
    api_url = "https://api.moysklad.ru/api/remap/1.2/entity/webhook"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    results = []
    async with aiohttp.ClientSession() as session:
        for action, extra in [("CREATE", {}), ("UPDATE", {"diffType": "NONE"})]:
            payload = {"url": webhook_url, "action": action, "entityType": "customerorder", **extra}
            async with session.post(api_url, headers=headers, json=payload) as resp:
                data = await resp.json()
                if resp.status in (200, 201):
                    results.append(f"✅ {action}: id={data.get('id')}")
                else:
                    results.append(f"❌ {action}: {data}")

    await update.message.reply_text("Вебхуки МойСклад:\n" + "\n".join(results))
    """Тестовый запуск утренних задач ПДЗ. /pdz_test [имя|all]"""
    user = update.message.from_user
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]
    if user.id not in manager_ids:
        await update.message.reply_text("⛔ Только для руководителей.")
        return

    arg = (context.args[0].lower() if context.args else "all")

    from scheduler import pdz_morning_task, PDZ_MANAGERS
    app = update.get_bot()  # используем контекст

    targets = PDZ_MANAGERS if arg == "all" else [
        m for m in PDZ_MANAGERS if m["name"].lower() == arg or m["tag"] == arg
    ]

    if not targets:
        names = ", ".join(m["name"].lower() for m in PDZ_MANAGERS)
        await update.message.reply_text(f"Не найдено. Варианты: all, {names}")
        return

    await update.message.reply_text(
        f"🧪 Запускаю тест ПДЗ для: {', '.join(m['name'] for m in targets)}..."
    )

    for mgr in targets:
        try:
            await pdz_morning_task(context.application, mgr)
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка для {mgr['name']}: {e}")


# Хранилище ожидающих отправки сообщений — message_key → {phone, name, text, chat_type}
_pending_sends: dict = {}


async def handle_send_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатие кнопки Отправить / Отменить для сообщений клиентам."""
    query = update.callback_query
    await query.answer()

    user = query.from_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        await query.answer("⛔ Только для руководителей.", show_alert=True)
        return

    parts = query.data.split("|")
    action = parts[0]
    msg_key = parts[1] if len(parts) > 1 else ""

    if action == "send_cancel":
        _pending_sends.pop(msg_key, None)
        await query.message.edit_text("❌ Отправка отменена.")
        return

    if action != "send_confirm":
        return

    pending = _pending_sends.pop(msg_key, None)
    if not pending:
        await query.message.edit_text("❌ Сообщение устарело — попробуй снова.")
        return

    api_key = os.getenv("WAZZUP_API_KEY", "")
    import aiohttp, uuid as _uuid

    # Каналы в порядке приоритета: WhatsApp → Max → Telegram
    CHANNEL_PRIORITY = [
        {"id": "e180aa1d-dc48-4d0a-bec3-fc0afc53cf03", "type": "whatsapp"},
        {"id": "1d5bc70a-7ca6-4895-8d1f-9690cf448214", "type": "max"},
        {"id": "ddd24a95-9304-4098-a320-3e47fcd1020a", "type": "telegram"},
    ]

    # Рассылка (несколько клиентов)
    if pending.get("is_broadcast"):
        contacts = pending["contacts"]
        product = pending["product"]
        broadcast_text = pending["text"]
        count = len(contacts)
        await query.message.edit_text(
            f"🚀 Начинаю рассылку по *{product}*\n📨 {count} получателей · ~{count} мин",
            parse_mode="Markdown"
        )

        async def run_wazzup_broadcast():
            sent, failed = 0, 0
            async with aiohttp.ClientSession() as session:
                for c in contacts:
                    phone = c.get("phone", "")
                    if not phone:
                        failed += 1
                        continue
                    try:
                        async with session.post(
                            "https://api.wazzup24.com/v3/message",
                            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                            json={
                                "channelId": channel_id,
                                "chatType": c.get("chat_type", "whatsapp"),
                                "chatId": phone,
                                "crmMessageId": str(_uuid.uuid4()),
                                "text": broadcast_text,
                            }
                        ) as resp:
                            if resp.status in (200, 201):
                                sent += 1
                            else:
                                failed += 1
                    except Exception:
                        failed += 1
                    await asyncio.sleep(60)

            result_text = f"✅ *Рассылка завершена!*\n📨 Отправлено: {sent}/{count}\n"
            if failed:
                result_text += f"❌ Не отправлено: {failed}\n"
            group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
            await context.bot.send_message(chat_id=group_chat_id or query.message.chat_id, text=result_text, parse_mode="Markdown")

        asyncio.create_task(run_wazzup_broadcast())
        return

    try:
        async with aiohttp.ClientSession() as session:
            sent_channel = None
            last_error = ""
            channels = pending.get("channels") or []
            for ch in channels:
                try:
                    async with session.post(
                        "https://api.wazzup24.com/v3/message",
                        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                        json={
                            "channelId": ch["channel_id"],
                            "chatType": ch["chat_type"],
                            "chatId": ch["chat_id"],
                            "crmMessageId": str(_uuid.uuid4()),
                            "text": pending["text"],
                        }
                    ) as resp:
                        if resp.status in (200, 201):
                            sent_channel = ch["chat_type"]
                            break
                        else:
                            body = await resp.text()
                            last_error = f"{resp.status}: {body[:100]}"
                            logger.warning(f"Wazzup {ch['chat_type']} failed: {last_error}")
                except Exception as e:
                    last_error = str(e)
                    logger.warning(f"Wazzup {ch['chat_type']} exception: {e}")

            if sent_channel:
                await query.message.edit_text(
                    f"✅ Сообщение отправлено *{pending['name']}* через {sent_channel}",
                    parse_mode="Markdown"
                )
                logger.info(f"Wazzup: отправлено {pending['name']} ({pending['phone']}) через {sent_channel}")
            else:
                await query.message.edit_text(f"❌ Не удалось отправить ни через один канал.\nПоследняя ошибка: {last_error}")
    except Exception as e:
        await query.message.edit_text(f"❌ Ошибка: {e}")


async def cmd_pdz_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестовый запуск утренних задач ПДЗ. /pdz_test [имя|all]"""
    user = update.message.from_user
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]
    if user.id not in manager_ids:
        await update.message.reply_text("⛔ Только для руководителей.")
        return

    arg = (context.args[0].lower() if context.args else "all")

    from scheduler import pdz_morning_task, PDZ_MANAGERS
    app = update.get_bot()

    targets = PDZ_MANAGERS if arg == "all" else [
        m for m in PDZ_MANAGERS if m["name"].lower() == arg or m["tag"] == arg
    ]

    if not targets:
        names = ", ".join(m["name"].lower() for m in PDZ_MANAGERS)
        await update.message.reply_text(f"Не найдено. Варианты: all, {names}")
        return

    await update.message.reply_text(
        f"🧪 Запускаю тест ПДЗ для: {', '.join(m['name'] for m in targets)}..."
    )

    for mgr in targets:
        try:
            await pdz_morning_task(context.application, mgr)
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка для {mgr['name']}: {e}")


async def cmd_pdz_evening_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестовый запуск вечерней сводки ПДЗ. /pdz_evening"""
    user = update.message.from_user
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]
    if user.id not in manager_ids:
        await update.message.reply_text("⛔ Только для руководителей.")
        return

    await update.message.reply_text("🧪 Запускаю тест вечерней сводки ПДЗ...")
    from scheduler import pdz_evening_summary
    try:
        await pdz_evening_summary(context.application)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def handle_price_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатия кнопок на алерте о цене."""
    query = update.callback_query
    await query.answer()

    user = query.from_user

    # Только руководители могут нажимать
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]
    if user.id not in manager_ids:
        await query.answer("⛔ Только для руководителей.", show_alert=True)
        return

    parts = query.data.split("|")
    action = parts[0]
    order_href = parts[1] if len(parts) > 1 else ""

    # Имя менеджера берём из текста сообщения (строка "Менеджер: ...")
    manager_name = ""
    for line in query.message.text.split("\n"):
        if line.startswith("Менеджер:"):
            manager_name = line.replace("Менеджер:", "").strip()
            break

    group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))

    MS_STATE_AGREED = "005f3651-9a9a-11f0-0a80-03a900027474"

    if action == "price_ok":
        from moysklad import set_order_state
        order_id = parts[1] if len(parts) > 1 else ""
        if order_id:
            await set_order_state(order_id, MS_STATE_AGREED)
        await query.message.delete()

    elif action == "price_comment":
        new_text = query.message.text + f"\n\n💬 *{user.first_name} ждёт комментарий менеджера*"
        await query.edit_message_text(new_text, parse_mode="Markdown")
        if group_chat_id:
            contact = MANAGERS_CONTACTS.get(manager_name)
            mgr_mention = contact if contact else f"*{manager_name}*" if manager_name else "Менеджер"
            await context.bot.send_message(
                chat_id=group_chat_id,
                text=f"{mgr_mention}, дай комментарий по занижению цены.",
                parse_mode="Markdown"
            )

    elif action == "pdz_ok":
        from moysklad import set_order_state
        order_id = parts[1] if len(parts) > 1 else ""
        logger.info(f"pdz_ok: order_id={order_id}")
        if order_id:
            success = await set_order_state(order_id, MS_STATE_AGREED)
            logger.info(f"pdz_ok: set_order_state result={success}")
        await query.answer("✅ Принято")
        await query.message.delete()

    elif action == "pdz_comment":
        order_id = parts[1] if len(parts) > 1 else ""
        pdz_data = _pdz_alert_data.get(order_id, {})
        client = pdz_data.get("client", "")
        manager_name_pdz = pdz_data.get("manager", manager_name)
        order_name_pdz = pdz_data.get("order_name", "")
        debt_amount = pdz_data.get("debt_amount", 0)
        debt_days = pdz_data.get("debt_days", 0)

        # Сохраняем в БД
        db.save_pdz_comment(
            client=client,
            manager=manager_name_pdz,
            order_name=order_name_pdz,
            debt_amount=debt_amount,
            debt_days=debt_days,
            comment="Запрошен комментарий руководителем",
            commented_by=user.first_name,
        )

        new_text = query.message.text + f"\n\n💬 *{user.first_name} ждёт комментарий менеджера*"
        await query.edit_message_text(new_text, parse_mode="Markdown")

        if group_chat_id:
            contact = MANAGERS_CONTACTS.get(manager_name_pdz)
            mgr_mention = contact if contact else f"*{manager_name_pdz}*" if manager_name_pdz else "Менеджер"
            await context.bot.send_message(
                chat_id=group_chat_id,
                text=f"{mgr_mention}, дай комментарий по заказу *{order_name_pdz}* — у клиента просрочка {debt_days} дней.",
                parse_mode="Markdown"
            )


def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("Не задан TELEGRAM_BOT_TOKEN в переменных окружения!")

    app = Application.builder().token(token).build()

    # Команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("clearwazzup", cmd_clear_wazzup))
    app.add_handler(CommandHandler("wazzup_reset", cmd_wazzup_reset))
    app.add_handler(CommandHandler("wazzup_channels", cmd_wazzup_channels))
    app.add_handler(CommandHandler("wazzup_setup", cmd_wazzup_setup))
    app.add_handler(CommandHandler("clearall", cmd_clear_all))
    app.add_handler(CommandHandler("test", cmd_test))
    app.add_handler(CommandHandler("cleartasks", cmd_clear_tasks))
    app.add_handler(CommandHandler("all_tasks", cmd_all_tasks))
    app.add_handler(CommandHandler("overdue", cmd_overdue))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("debts", cmd_debtors))
    app.add_handler(CommandHandler("photo", cmd_photo))
    app.add_handler(CommandHandler("price", cmd_price))
    app.add_handler(CommandHandler("contact", cmd_contact))
    app.add_handler(CommandHandler("memory", cmd_memory))
    app.add_handler(CommandHandler("remember", cmd_remember))
    app.add_handler(CommandHandler("add_webhook", cmd_add_webhook))
    app.add_handler(CommandHandler("pdz_test", cmd_pdz_test))
    app.add_handler(CommandHandler("pdz_evening", cmd_pdz_evening_test))
    app.add_handler(CallbackQueryHandler(handle_price_callback, pattern="^(price_|pdz_)"))
    app.add_handler(CallbackQueryHandler(handle_send_callback, pattern="^send_"))
    app.add_handler(CallbackQueryHandler(handle_wazzup_link_callback, pattern="^(wazzup_link|wazzup_role|wazzup_pick)"))
    app.add_handler(CallbackQueryHandler(handle_wazzup_ignore_callback, pattern="^wazzup_ignore"))
    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POSTS, handle_channel_post))
    app.add_handler(MessageHandler(filters.ALL & ~filters.UpdateType.CHANNEL_POSTS, handle_message))

    # Планировщик
    setup_scheduler(app, db)

    # Запускаем webhook-сервер и polling параллельно
    import aiohttp.web as web

    async def handle_wazzup_webhook(request):
        """Принимает webhook от Wazzup — сохраняет сообщения и chatId клиентов."""
        try:
            data = await request.json()
            messages = data.get("messages", [])
            saved = 0
            for msg in messages:
                text = msg.get("text", "")
                chat_type = msg.get("chatType", "")
                chat_id_val = msg.get("chatId", "")
                channel_id_val = msg.get("channelId", "")
                contact = msg.get("contact", {})
                contact_name = contact.get("name", chat_id_val)
                is_outbound = msg.get("isEcho", False)
                manager_id = msg.get("crmUserId", "")
                manager_name = WAZZUP_MANAGERS.get(manager_id, manager_id)
                sent_at = msg.get("dateTime", "")

                logger.info(f"Wazzup msg: isEcho={is_outbound} channel={channel_id_val} chatType={chat_type} chatId={chat_id_val} contact='{contact_name}' text='{text[:60]}'")

                # Сохраняем маппинг контакта → chatId/channel для последующей отправки
                if chat_id_val and contact_name and not is_outbound:
                    db.save_wazzup_contact(
                        contact_name=contact_name,
                        chat_id=chat_id_val,
                        chat_type=chat_type,
                        channel_id=channel_id_val,
                    )
                    # Для Telegram — уведомляем руководителя если контакт неизвестен
                    is_known = db.is_wazzup_contact_known(chat_id_val)
                    logger.info(f"Wazzup: chat_id={chat_id_val} is_known={is_known}")
                    if chat_type in ("telegram", "tgapi") and not is_known:
                        # Проверяем что контакт не помечен как игнорируемый
                        ignored = db._fetchone(
                            "SELECT id FROM wazzup_contact_map WHERE chat_id=%s AND company_name='__ignore__'",
                            (chat_id_val,)
                        )
                        if ignored:
                            continue
                        group_chat_id = int(os.getenv("WAZZUP_ID_CHAT_ID", "0"))
                        logger.info(f"Wazzup: отправляю уведомление в группу {group_chat_id}")
                        if group_chat_id:
                            try:
                                import uuid as _uuid2
                                link_key = str(_uuid2.uuid4())[:8]
                                _pending_links[link_key] = {
                                    "chat_id": chat_id_val,
                                    "channel_id": channel_id_val,
                                    "wazzup_name": contact_name,
                                    "chat_type": chat_type,
                                }
                                keyboard = InlineKeyboardMarkup([[
                                    InlineKeyboardButton("🏢 Привязать компанию", callback_data=f"wazzup_link|{link_key}"),
                                    InlineKeyboardButton("🚫 Не привязывать", callback_data=f"wazzup_ignore|{chat_id_val}")
                                ]])
                                preview = (text or "").replace("\n", " ").strip()
                                if len(preview) > 120:
                                    preview = preview[:120] + "..."
                                await app.bot.send_message(
                                    chat_id=group_chat_id,
                                    text=(
                                        f"📩 *Новый неизвестный контакт в Telegram*\n\n"
                                        f"👤 Имя в TG: *{contact_name}*\n"
                                        f"💬 _{preview}_\n\n"
                                        f"Чей клиент? Нажми и напиши как он называется в МойСклад"
                                    ),
                                    parse_mode="Markdown",
                                    reply_markup=keyboard
                                )
                            except Exception as e:
                                logger.error(f"Не удалось отправить уведомление в группу: {e}", exc_info=True)

                if not text:
                    continue
                ok = db.save_wazzup_message(
                    message_id=msg.get("messageId", ""),
                    channel_id=channel_id_val,
                    chat_type=chat_type,
                    chat_id=chat_id_val,
                    contact_name=contact_name,
                    manager_id=manager_id,
                    manager_name=manager_name,
                    text=text,
                    is_outbound=is_outbound,
                    sent_at=sent_at,
                )
                if ok:
                    saved += 1
            logger.info(f"Wazzup webhook: получено {len(messages)} сообщений, сохранено {saved}")
            return web.Response(text="ok")
        except Exception as e:
            logger.error(f"Wazzup webhook error: {e}")
            return web.Response(text="error", status=500)

    async def handle_ms_webhook(request):
        """Принимает webhook от МойСклад — новые/обновлённые заказы."""
        try:
            data = await request.json()
            asyncio.create_task(process_ms_webhook(data, app.bot))
            return web.Response(text="ok")
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            return web.Response(text="error", status=500)

    async def handle_health(request):
        return web.Response(text="ok")

    async def run_web():
        web_app = web.Application()
        web_app.router.add_post("/webhook/moysklad", handle_ms_webhook)
        web_app.router.add_post("/webhook/wazzup", handle_wazzup_webhook)
        web_app.router.add_get("/health", handle_health)
        port = int(os.getenv("PORT", "8080"))
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logger.info(f"🌐 Webhook сервер запущен на порту {port}")

    async def run_all():
        await run_web()
        await app.initialize()
        await app.start()
        await app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=["message", "channel_post", "edited_message", "edited_channel_post", "callback_query"]
        )
        logger.info("🤖 Бот запущен!")
        # Держим бота запущенным
        try:
            import signal
            loop = asyncio.get_event_loop()
            stop = loop.create_future()
            loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
            loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
            await stop
        finally:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()

    asyncio.run(run_all())


# Маппинг менеджеров МойСклад → Telegram (username или телефон)
MANAGERS_CONTACTS = {
    "Леонтьев Алексей Вадимович":      "@EL_Aliexbox",
    "Мерзлякова Елена Владимировна":   "+79920035102",
    "Баласанян Карина Владимировна":   "@fatbob183",
    "Скляр Инесса Ионасовна":          "+79622522903",
    "Голубева Татьяна":                "@tanya_keratin14",
}

# Маппинг crmUserId Wazzup → имя менеджера (заполним после первых вебхуков)
WAZZUP_MANAGERS: dict = {}
# Кэш для дедупликации webhook — order_id → timestamp последней проверки
_price_check_cache: dict = {}
# Хранилище данных алертов ПДЗ — order_id → {client, manager, debt_amount, debt_days, order_name}
_pdz_alert_data: dict = {}


async def check_debtor_alert(order_href: str, bot, group_chat_id: int):
    """Проверяет есть ли у клиента просрочка > 5 дней при новом заказе."""
    try:
        import aiohttp
        from moysklad import get_headers, MS_BASE
        from datetime import date

        async with aiohttp.ClientSession() as session:
            async with session.get(
                order_href, headers=get_headers(),
                params={"expand": "agent,owner"}
            ) as resp:
                if resp.status != 200:
                    return
                order = await resp.json()

        agent = order.get("agent", {})
        agent_meta = agent.get("meta", {})
        agent_href = agent_meta.get("href", "")
        agent_id = agent.get("id") or (agent_href.split("/")[-1] if agent_href else "")
        agent_name = agent.get("name", "")
        order_name = order.get("name", "")
        owner = order.get("owner", {})
        manager_name = owner.get("name", "не указан")

        logger.info(f"check_debtor_alert: agent_id={agent_id} agent_name={agent_name} order={order_name}")

        if not agent_id:
            logger.warning("check_debtor_alert: agent_id пустой, пропускаем")
            return

        # Проверяем долг и просрочку через заказы контрагента
        from moysklad import get_counterparty_debt
        logger.info(f"check_debtor_alert: запрашиваю долг для {agent_id}")
        debt_info = await get_counterparty_debt(agent_id)
        logger.info(f"check_debtor_alert: debt_info={debt_info}")

        if not debt_info:
            logger.info("check_debtor_alert: debt_info пустой — нет долга или ошибка")
            return

        debt_amount = debt_info.get("debt", 0)
        debt_days = debt_info.get("overdue_days", 0)
        logger.info(f"check_debtor_alert: debt={debt_amount} days={debt_days}")

        if debt_days <= 5 or debt_amount <= 0:
            logger.info(f"check_debtor_alert: просрочка {debt_days} дней — ниже порога или долга нет")
            return

        order_id = order_href.split("/")[-1]
        _pdz_alert_data[order_id] = {
            "client": agent_name,
            "manager": manager_name,
            "order_name": order_name,
            "debt_amount": debt_amount,
            "debt_days": debt_days,
        }

        text = (
            f"🔴 *Новый заказ от клиента с просрочкой!*\n\n"
            f"*{agent_name}* | Заказ *{order_name}*\n"
            f"Менеджер: {manager_name}\n\n"
            f"Просрочка: *{debt_days} дней* | Сумма: *{debt_amount:,.0f} руб*"
        )
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Согласовано", callback_data=f"pdz_ok|{order_id}"),
                InlineKeyboardButton("💬 Требуется комментарий", callback_data=f"pdz_comment|{order_id}"),
            ]
        ])
        await bot.send_message(
            chat_id=group_chat_id,
            text=text,
            parse_mode="Markdown",
            reply_markup=keyboard
        )
        logger.info(f"ПДЗ алерт: {agent_name}, просрочка {debt_days} дней, заказ {order_name}")

    except Exception as e:
        logger.error(f"check_debtor_alert: {e}")
# Кэш позиций заказа — order_id → frozenset(позиций) для отслеживания изменений цен/номенклатуры
_order_positions_cache: dict = {}


async def process_ms_webhook(data: dict, bot):
    """Обрабатывает webhook от МойСклад — проверяет цены в заказе."""
    import time
    try:
        from moysklad import check_order_prices
        group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
        if not group_chat_id:
            return

        events = data.get("events", [])
        for event in events:
            meta = event.get("meta", {})
            entity_type = meta.get("type", "")
            if entity_type != "customerorder":
                continue

            order_href = meta.get("href", "")
            if not order_href:
                continue

            # Дедупликация — один заказ не чаще раза в 10 секунд
            order_id = order_href.split("/")[-1]
            now = time.time()
            last_check = _price_check_cache.get(order_id, 0)
            already_checked = now - last_check < 10
            _price_check_cache[order_id] = now

            action = event.get("action", "")
            logger.info(f"Webhook: заказ {order_id} action={action} already_checked={already_checked}")

            # ПДЗ алерт — только для новых заказов, только один раз
            if action == "CREATE" and not already_checked:
                await check_debtor_alert(order_href, bot, group_chat_id)

            if already_checked:
                logger.info(f"Webhook: заказ {order_id} уже проверялся, пропускаем цены/логистику")
                continue

            # Получаем снапшот позиций (товар + цена) и сравниваем с предыдущим
            from moysklad import get_order_positions_snapshot
            snapshot = await get_order_positions_snapshot(order_href)
            prev_snapshot = _order_positions_cache.get(order_id)
            _order_positions_cache[order_id] = snapshot

            if prev_snapshot is not None and snapshot == prev_snapshot:
                logger.info(f"Webhook: заказ {order_id} — цены/номенклатура не изменились, пропускаем")
                continue

            logger.info(f"Webhook: проверяю цены заказа {order_id}")
            alerts = await check_order_prices(order_href)

            if alerts:
                text = "⚠️ *Цена ниже минимальной!*\n\n" + "\n\n".join(alerts)
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("✅ Согласовано", callback_data=f"price_ok|{order_id}"),
                        InlineKeyboardButton("💬 Требуется комментарий", callback_data=f"price_comment|{order_id}"),
                    ]
                ])
                await bot.send_message(
                    chat_id=group_chat_id,
                    text=text,
                    parse_mode="Markdown",
                    reply_markup=keyboard
                )

            # Проверяем логистику — адрес vs день недели
            await check_logistics_alert(order_href, bot, group_chat_id)

            # ПДЗ алерт для новых заказов уже отправлен выше

    except Exception as e:
        logger.error(f"process_ms_webhook: {e}")


async def check_logistics_alert(order_href: str, bot, group_chat_id: int):
    """Проверяет адрес доставки заказа на соответствие расписанию логистики."""
    try:
        from moysklad import check_delivery_schedule, get_headers, MS_BASE
        import aiohttp

        async with aiohttp.ClientSession() as session:
            url = order_href.split("?")[0]
            async with session.get(url, headers=get_headers()) as resp:
                if resp.status != 200:
                    return
                order = await resp.json()

        address = order.get("shipmentAddress", "")
        delivery_date = order.get("deliveryPlannedMoment", "")
        order_name = order.get("name", "")

        if not address or not delivery_date:
            return

        result = await check_delivery_schedule(address, delivery_date)
        if result.get("ok"):
            return

        # Получаем имя клиента и менеджера
        agent_href = order.get("agent", {}).get("meta", {}).get("href", "")
        owner_href = order.get("owner", {}).get("meta", {}).get("href", "")
        client_name = ""
        manager_name = ""

        async with aiohttp.ClientSession() as session:
            from moysklad import get_headers
            if agent_href:
                async with session.get(agent_href, headers=get_headers()) as r:
                    if r.status == 200:
                        d = await r.json()
                        client_name = d.get("name", "")
            if owner_href:
                async with session.get(owner_href, headers=get_headers()) as r:
                    if r.status == 200:
                        d = await r.json()
                        manager_name = d.get("name", "")

        city = result["city"].capitalize()
        weekday = result["weekday"]  # строка: "среда", "пятница" и т.д.
        allowed = ", ".join(result["allowed_days"]) or "не запланирован"

        # Винительный падеж для "не едем в ..."
        WEEKDAY_ACCUSATIVE = {
            "понедельник": "понедельник",
            "вторник": "вторник",
            "среда": "среду",
            "четверг": "четверг",
            "пятница": "пятницу",
            "суббота": "субботу",
            "воскресенье": "воскресенье",
        }
        weekday_acc = WEEKDAY_ACCUSATIVE.get(weekday.lower(), weekday)
        from datetime import date
        MONTHS = ["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"]
        try:
            d = date.fromisoformat(result["date"])
            date_str = f"{d.day} {MONTHS[d.month-1]}"
        except Exception:
            date_str = result["date"]

        text = (
            f"🚛 *Несоответствие логистики*\n\n"
            f"👤 {client_name} | Заказ №{order_name}\n"
            f"👔 Менеджер: {manager_name}\n"
            f"📍 Адрес: {address}\n\n"
            f"📅 Дата отгрузки: *{date_str} ({weekday})*\n"
            f"❌ В {city} мы не едем в {weekday_acc}\n"
            f"✅ {city} доступен: *{allowed}*"
        )

        await bot.send_message(
            chat_id=group_chat_id,
            text=text,
            parse_mode="Markdown"
        )
        logger.info(f"Логистика алерт: заказ {order_name}, {city}, {weekday}")

    except Exception as e:
        logger.error(f"check_logistics_alert: {e}", exc_info=True)


if __name__ == "__main__":
    main()
