"""
F2B PRO — Telegram Bot
Ассистент отдела продаж: фото, прайсы, задачи
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
    BusinessConnectionHandler,
    filters,
)

from database import Database
from notifier import check_order_agreed  # рассылка при согласовании — не трогать!
from scheduler import setup_scheduler, get_group_chat_id, pdz_catch_up_missed_jobs
from claude_ai import dispatch, smart_answer, parse_product_query
from amocrm import check_connection as amo_check  # оставляем для совместимости
from amo_alarms import (
    handle_amo_webhook,
    handle_take_callback,
    handle_amo_link_callback,
    cmd_myamoid,
    cmd_amo_setup,
)
from moysklad import (search_products, search_products_filtered, get_price_list, format_products,
    format_price_list, get_product_image, download_image, get_image_download_url,
    get_counterparty_balance, get_all_debtors, format_debtors_ms, format_counterparty_balance,
    find_counterparty_info, format_counterparty_info,
    get_debtors_by_tag, get_clients_by_tag, resolve_tag,
    format_debtors_by_tag, format_clients_by_tag,
    get_overdue_demands, format_overdue_demands, format_overdue_summary,
    format_reminders_for_manager, format_debt_reminder, fmt_money,
    list_employees, create_task, invalidate_employees_cache)
from claude_ai import parse_task_draft

# ─── Владелец бота — TG-id, гейтит админские команды и адрес утренних сводок ─
_owner_chat_id_raw = os.getenv("OWNER_CHAT_ID")
if not _owner_chat_id_raw:
    raise RuntimeError(
        "OWNER_CHAT_ID env not set. "
        "Задай числовой Telegram-id владельца в Railway → Variables."
    )
OWNER_CHAT_ID = int(_owner_chat_id_raw)

# ─── Партнёр — TG-id, whitelist для /задача (второй из двух постановщиков) ───
_partner_chat_id_raw = os.getenv("PARTNER_CHAT_ID")
if not _partner_chat_id_raw:
    raise RuntimeError(
        "PARTNER_CHAT_ID env not set. "
        "Задай TG-id партнёра (Александр) в Railway → Variables."
    )
PARTNER_CHAT_ID = int(_partner_chat_id_raw)

# ─── Словарь сотрудников — варианты имён и склонений ─────────────────────────
EMPLOYEES = {
    "Белякова Александра": [
        "александра", "александры", "александре", "александру",
        "белякова", "беляковой", "белякову",
        "саша", "саши", "саше", "сашу",
    ],
    "Ирина Дьяченко": [
        "ирина", "ирины", "ирине", "ирину", "ириной",
        "дьяченко",
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
    "Денис Коликов": [
        "денис", "дениса", "денису", "денисом",
        "коликов", "коликова", "коликову",
    ],
}

# Менеджеры отдела продаж — "всем менеджерам"
MOP_MANAGERS = [
    "Карина Баласанян",
    "Елена Мерзлякова",
    "Инесса Скляр",
    "Ирина Дьяченко",
    "Денис Коликов",
]

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

# ─── Квиз — URL сервиса викторины ────────────────────────────────────────────
QUIZ_BASE_URL = os.getenv("QUIZ_BASE_URL", "")  # https://викторина-xxxx.up.railway.app

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
    user = update.effective_user
    chat_id = update.effective_chat.id
    if user and chat_id == user.id:
        db.save_manager_chat_id(user.id, user.full_name)
        logger.info(f"cmd_start: сохранён chat_id={chat_id} name={user.full_name}")
    is_author = user is not None and _is_task_author(user.id)
    await update.message.reply_text(
        f"👋 Привет, *{user.full_name if user else 'друг'}*! Я Эф — ассистент F2B PRO.\n\n"
        f"Используй меню ниже или обращайся: *Эф, [вопрос]*",
        parse_mode="Markdown",
        reply_markup=_user_menu_keyboard(include_task_button=is_author)
    )

def _user_menu_keyboard(include_task_button: bool = False) -> InlineKeyboardMarkup:
    """Общее меню для всех пользователей.

    Если include_task_button=True (только для whitelist Виктор/Александр) —
    добавляется кнопка «📋 Поставить задачу».
    """
    rows = [
        [
            InlineKeyboardButton("📸 Запросить фото товара", callback_data="user_photo"),
        ],
        [
            InlineKeyboardButton("📄 Сформировать договор", callback_data="user_contract"),
        ],
        [
            InlineKeyboardButton("📊 Отчёт ОП", callback_data="user_op_report"),
        ],
        [
            InlineKeyboardButton("📝 Новая заявка на закупку", callback_data="user_req_new"),
        ],
    ]
    if include_task_button:
        rows.append([InlineKeyboardButton("📋 Поставить задачу", callback_data="menu_task")])
    return InlineKeyboardMarkup(rows)

async def cmd_user_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/usermenu — общее меню."""
    await update.message.reply_text(
        "Выбери действие:",
        reply_markup=_user_menu_keyboard()
    )

async def handle_user_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик общего меню пользователей."""
    query = update.callback_query
    await query.answer()
    user = query.from_user
    action = query.data

    if action == "user_photo":
        await query.message.reply_text(
            "📸 Напиши название товара — пришлю фото.\n"
            "Например: _форель охл трим С_",
            parse_mode="Markdown"
        )
        _user_awaiting[query.from_user.id] = "photo"

    elif action == "user_contract":
        await query.message.reply_text(
            "📄 Напиши название компании — сформирую договор поставки.\n"
            "Например: _Атмосфера_ или _ИТФИШ_",
            parse_mode="Markdown"
        )
        _user_awaiting[query.from_user.id] = "contract"

    elif action == "user_op_report":
        await cmd_op_report(update, context)

    elif action == "user_req_new":
        await query.message.reply_text(
            "📝 Вставь запрос клиента *как есть* — я распарсу и покажу превью.\n\n"
            "*Минимум для заявки:* вид/категория, объём, цена для клиента.\n\n"
            "_Пример (рыба): «Лосось ПБГ 5-6 охл 200 кг, клиенту по 720 ₽, к четвергу»._\n"
            "_Пример (сопутка): «Сыр творожный 30 кг, клиенту по 410 ₽, к субботе. Филадельфия»._",
            parse_mode="Markdown"
        )
        _user_awaiting[query.from_user.id] = "request_text"

    elif action == "user_reconciliation":
        await query.message.reply_text(
            "📊 Напиши название компании — сформирую акт сверки.\n"
            "Например: _Атмосфера_ или _ИТФИШ_",
            parse_mode="Markdown"
        )
        _user_awaiting[query.from_user.id] = "reconciliation"


async def handle_request_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение / дополнение / отмена черновика заявки на закупку (Phase 3.2)."""
    query = update.callback_query
    await query.answer()
    user = query.from_user
    action = query.data

    draft = _draft_requests.get(user.id)
    if draft is None:
        await query.message.reply_text(
            "⚠️ Черновик заявки не найден (возможно, бот перезапускался). "
            "Создай заявку заново через меню.",
        )
        return

    parsed, assigned_to = draft

    if action == "req_cancel":
        _draft_requests.pop(user.id, None)
        _user_awaiting.pop(user.id, None)
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(
            "❌ Заявка отменена.",
            reply_markup=_user_menu_keyboard()
        )
        return

    if action == "req_amend":
        _user_awaiting[user.id] = "request_text_amend"
        # Считаем что не хватает — покажем менеджеру конкретно.
        from request_handler import validate_request
        missing_req, missing_rec = validate_request(parsed)
        gaps_line = ""
        if missing_req:
            gaps_line = f"\n\n*Не хватает обязательного:* {', '.join(missing_req)}."
        elif missing_rec:
            gaps_line = f"\n\n*Желательно добавить:* {', '.join(missing_rec)}."
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(
            "➕ Пришли ещё одно сообщение с недостающими данными — я склею с тем, "
            "что уже прислал, и покажу превью заново."
            f"{gaps_line}"
            "\n\n_Пример: «200 кг, продаём клиенту по 720 ₽/кг, к четвергу»_",
            parse_mode="Markdown",
        )
        return

    if action == "req_confirm":
        from request_handler import (
            insert_request, format_assignee_notification,
            validate_request, ASSIGNEE_TG, UNCLASSIFIED_NOTIFY_TG,
        )
        # Защита: пересчитать обязательные на момент confirm.
        missing_req, _ = validate_request(parsed)
        if missing_req:
            await query.message.reply_text(
                f"❌ Всё ещё не хватает обязательного: {', '.join(missing_req)}.\n"
                "Жми «➕ Дополнить» и допиши.",
            )
            return

        _draft_requests.pop(user.id, None)
        _user_awaiting.pop(user.id, None)

        try:
            request_id = insert_request(
                db, parsed, user.id, user.full_name or "", assigned_to,
            )
        except Exception as e:
            logger.exception(f"insert_request failed: {e}")
            await query.message.reply_text(f"⚠️ Не удалось сохранить заявку: {e}")
            return

        # Кому слать TG: если assigned_to=None — обоим закупщикам + Виктору.
        if assigned_to is None:
            recipients = list(UNCLASSIFIED_NOTIFY_TG)
        else:
            recipients = [ASSIGNEE_TG.get(assigned_to)]
        recipients = [r for r in recipients if r]

        notif = format_assignee_notification(request_id, parsed, user.full_name or "")
        for chat_id in recipients:
            try:
                await context.bot.send_message(
                    chat_id=chat_id, text=notif,
                    parse_mode="Markdown", disable_web_page_preview=True,
                )
            except Exception as e:
                logger.error(f"send to assignee {chat_id} failed: {e}")

        assignee_disp = {
            "belyakova": "Александре Беляковой",
            "kristina":  "Кристине Павленко",
            "victor":    "Виктору",
            None:        "обоим закупщикам + Виктору (не определён вид)",
        }.get(assigned_to)
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(
            f"✅ Заявка №{request_id} создана и ушла {assignee_disp}.",
            reply_markup=_user_menu_keyboard()
        )


async def cmd_mychatid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает chat_id пользователя."""
    user = update.effective_user
    chat_id = update.effective_chat.id
    if user and chat_id == user.id:
        db.save_manager_chat_id(user.id, user.full_name)
    await update.message.reply_text(
        f"👤 *{user.full_name if user else 'Неизвестный'}*\n"
        f"Твой chat_id: `{chat_id}`",
        parse_mode="Markdown"
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📋 *Все команды:*\n\n"
        "*Отчёты:*\n"
        "/report — недельный отчёт\n\n"
        "*Управление:*\n"
        "/menu — панель управления\n"
        "/mychatid — мой chat ID",
        parse_mode="Markdown"
    )

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Панель управления.

    - Виктор (OWNER): полное меню + секция «Постановка задач».
    - Александр (PARTNER): общее меню + кнопка «Поставить задачу».
    - Остальные: общее меню.
    """
    user = update.effective_user
    if not user:
        return

    # Не-руководитель — общее меню. Кнопка задач только для whitelist (Александр).
    if user.id != OWNER_CHAT_ID:
        await update.message.reply_text(
            "Выбери действие:",
            reply_markup=_user_menu_keyboard(include_task_button=_is_task_author(user.id))
        )
        return

    # OWNER — расширенное меню с кнопкой задач в персональном блоке.
    keyboard = InlineKeyboardMarkup([
        # ── Доступно всем ──────────────────────────────────────────
        [InlineKeyboardButton("── Общие функции ──", callback_data="menu_noop")],
        [
            InlineKeyboardButton("📸 Фото товара", callback_data="user_photo"),
        ],
        [
            InlineKeyboardButton("📄 Сформировать договор", callback_data="user_contract"),
        ],
        [
            InlineKeyboardButton("📊 Отчёт ОП", callback_data="menu_evening"),
        ],
        [
            InlineKeyboardButton("📝 Новая заявка на закупку", callback_data="user_req_new"),
        ],
        # ── Только руководитель ────────────────────────────────────
        [InlineKeyboardButton("── Только для меня ──", callback_data="menu_noop")],
        [
            InlineKeyboardButton("⏳ Стареющие", callback_data="menu_aging"),
        ],
        # ── Постановка задач в МойСклад ────────────────────────────
        [InlineKeyboardButton("── Постановка задач ──", callback_data="menu_noop")],
        [InlineKeyboardButton("📋 Поставить задачу", callback_data="menu_task")],
    ])

    await update.message.reply_text(
        "🎛 *Панель управления Эф*\n\nВыбери действие:",
        parse_mode="Markdown",
        reply_markup=keyboard
    )

async def handle_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопок панели управления."""
    query = update.callback_query
    await query.answer()

    if not query.from_user or query.from_user.id != OWNER_CHAT_ID:
        await query.answer("⛔ Только для руководителя.", show_alert=True)
        return

    action = query.data

    if action == "menu_noop":
        return  # разделитель — ничего не делаем

    elif action == "user_photo":
        await query.message.reply_text(
            "📸 Напиши название товара — пришлю фото.\nНапример: _форель охл трим С_",
            parse_mode="Markdown"
        )
        _user_awaiting[query.from_user.id] = "photo"

    elif action == "menu_evening":
        await cmd_op_report(update, context)

    elif action == "menu_aging":
        await cmd_aging(update, context)

    elif action == "menu_activity":
        await query.message.reply_text(
            "📊 Активность менеджеров:\nНапиши *Эф, активность за 7 дней*",
            parse_mode="Markdown"
        )

    elif action == "menu_report":
        await query.message.reply_text("⏳ Формирую отчёт...")
        context2 = db.get_context_summary()
        from claude_ai import smart_answer
        text = await smart_answer("Дай краткий недельный отчёт по задачам команды", context2)
        await query.message.reply_text(text, parse_mode="Markdown")

    elif action == "menu_test":
        await query.message.reply_text("🔍 Запускаю диагностику...")
        fake_update = update
        await cmd_test(fake_update, context)

    elif action == "menu_contacts":
        lines = ["📞 *Контакты менеджеров:*\n"]
        for name, contact in MANAGERS_CONTACTS.items():
            lines.append(f"• {name}: {contact}")
        await query.message.reply_text("\n".join(lines), parse_mode="Markdown")

    elif action == "menu_stats":
        stats = db.get_usage_stats()
        if not stats:
            await query.message.reply_text("📭 Статистики пока нет.")
        else:
            lines = ["📊 *Статистика использования бота*\n"]
            for s in stats:
                blocked = " 🔒" if s.get("is_blocked") else ""
                last = s.get("last_seen")
                last_str = last.strftime("%d.%m %H:%M") if last else "—"
                lines.append(
                    f"👤 *{s.get('full_name','?')}*{blocked}\n"
                    f"   `{s.get('user_id')}` · {s.get('request_count',0)} зап. · {last_str}"
                )
            await query.message.reply_text("\n".join(lines), parse_mode="Markdown")

    elif action == "menu_block":
        stats = db.get_usage_stats()
        if not stats:
            await query.message.reply_text("Нет пользователей.")
            return
        buttons = []
        for s in stats:
            if s.get("user_id") == OWNER_CHAT_ID:
                continue
            status = "🔒" if s.get("is_blocked") else "✅"
            cb = f"menu_toggleblock|{s['user_id']}"
            buttons.append([InlineKeyboardButton(
                f"{status} {s.get('full_name','?')} ({s.get('request_count',0)} зап.)",
                callback_data=cb
            )])
        buttons.append([InlineKeyboardButton("◀️ Назад", callback_data="menu_cancel")])
        await query.message.reply_text(
            "🔒 *Управление доступом*\nНажми на пользователя чтобы заблокировать/разблокировать:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif action.startswith("menu_toggleblock|"):
        uid = int(action.split("|")[1])
        row = db._fetchone("SELECT full_name, is_blocked FROM manager_chats WHERE user_id=%s", (uid,))
        if row:
            if row.get("is_blocked"):
                db.unblock_user(uid)
                await query.answer(f"🔓 {row['full_name']} разблокирован")
            else:
                db.block_user(uid)
                await query.answer(f"🔒 {row['full_name']} заблокирован")
        await query.message.delete()

    elif action == "menu_cancel":
        await query.message.delete()

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📋 *Все команды:*\n\n"
        "*Отчёты:*\n"
        "/report — недельный отчёт\n\n"
        "*База знаний:*\n"
        "/фото [товар] — найти фото\n"
        "/прайс — актуальный прайс\n"
        "/контакт [имя] — найти контакт\n\n"
        "*Обращение в свободной форме:*\n"
        "бот, [любой вопрос]",
        parse_mode="Markdown"
    )

# Кэш уже отправленных уведомлений об идентификации — chat_id → True
_wazzup_notified: set = set()

_pending_links: dict = {}
_user_awaiting: dict = {}  # user_id → "photo" | "contract" | "reconciliation" | "request_text"
_draft_requests: dict = {}  # user_id → (ParsedRequest, assigned_to). Phase 3.2.
_pending_price_comments: dict = {}  # manager_user_id → {alert_id, order_id, mgr_name, alert_type?, approver_chat_id?}
_pending_approver_input: dict = {}  # approver_chat_id → {alert_id, order_name, client_name, manager_name, manager_user_id}

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

# UUID полей контрагентов в МойСклад для рассылок
MS_ATTR_TELEGRAM = "15052610-34d7-11f1-0a80-1489000ec44a"
MS_ATTR_WHATSAPP = "1505270f-34d7-11f1-0a80-1489000ec44b"
MS_ATTR_MAX      = "1505236e-34d7-11f1-0a80-1489000ec449"
MS_ATTR_BY_TYPE  = {
    "telegram": MS_ATTR_TELEGRAM,
    "tgapi":    MS_ATTR_TELEGRAM,
    "whatsapp": MS_ATTR_WHATSAPP,
    "max":      MS_ATTR_MAX,
}

async def _write_contact_to_ms(agent_href: str, chat_type: str, chat_id: str) -> bool:
    """Записывает chat_id в поле контрагента в МойСклад."""
    import aiohttp
    from moysklad import get_headers
    attr_id = MS_ATTR_BY_TYPE.get(chat_type.lower())
    if not attr_id:
        return False
    MS_BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
    payload = {"attributes": [{"meta": {
        "href": f"{MS_BASE_URL}/entity/counterparty/metadata/attributes/{attr_id}",
        "type": "attributemetadata",
        "mediaType": "application/json"
    }, "value": str(chat_id)}]}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(agent_href, headers=get_headers(), json=payload) as r:
                return r.status in (200, 201)
    except Exception as e:
        logger.warning(f"_write_contact_to_ms: {e}")
        return False


async def handle_wazzup_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатие кнопки привязки Telegram контакта к компании."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split("|")

    # Новый контакт для рассылок: wazzup_mailing|link_key
    if parts[0] == "wazzup_mailing":
        if len(parts) < 2:
            return
        link_key = parts[1]
        pending = _pending_links.get(link_key)
        if not pending:
            await query.message.edit_text("❌ Сессия истекла, попробуй снова.")
            return
        # Помечаем как "для рассылок"
        pending["for_mailing"] = True
        _pending_links[link_key] = pending
        # Добавляем в стек пользователя
        if query.from_user.id not in _pending_links:
            _pending_links[query.from_user.id] = []
        existing_keys = [p.get("link_key") for p in _pending_links[query.from_user.id]] if isinstance(_pending_links.get(query.from_user.id), list) else []
        if link_key not in existing_keys:
            entry = {**pending, "link_key": link_key}
            if isinstance(_pending_links[query.from_user.id], list):
                _pending_links[query.from_user.id].append(entry)
            else:
                _pending_links[query.from_user.id] = [entry]
        CHANNEL_NAMES = {"telegram": "Telegram", "tgapi": "Telegram", "max": "Max", "whatsapp": "WhatsApp"}
        channel_label = CHANNEL_NAMES.get(pending.get("chat_type", ""), "")
        await query.message.edit_text(
            f"✅ Контакт для рассылок ({channel_label})\n\n"
            f"👤 *{pending['wazzup_name']}*\n\n"
            f"Как называется компания в МойСклад?\n"
            f"_(напиши название или часть)_",
            parse_mode="Markdown"
        )
        return

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
        _pending_links.pop(link_key, None)
        db.delete_pending_link(link_key)
        _pending_links.pop(query.from_user.id, None)
        ok = db.link_wazzup_contact(
            chat_id=pending["chat_id"],
            chat_type=pending["chat_type"],
            channel_id=pending["channel_id"],
            company_name=cp_name,
            wazzup_name=pending["wazzup_name"],
            role="рассылка",
        )
        if ok:
            _wazzup_notified.discard(pending["chat_id"])
            try:
                from moysklad import find_counterparty_info
                cp_list = await find_counterparty_info(cp_name)
                if cp_list:
                    cp_data = cp_list[0]
                    db.update_wazzup_contact_tags(
                        chat_id=pending["chat_id"],
                        tags=cp_data.get("tags", []),
                        manager=cp_data.get("manager", ""),
                        segment=cp_data.get("buyer_type", ""),
                    )
                    # Записываем chat_id в МойСклад
                    agent_href = cp_data.get("href", "")
                    if agent_href and pending.get("chat_type"):
                        ms_ok = await _write_contact_to_ms(
                            agent_href=agent_href,
                            chat_type=pending["chat_type"],
                            chat_id=pending["chat_id"],
                        )
                        if ms_ok:
                            logger.info(f"_write_contact_to_ms: {pending['chat_id']} ({pending['chat_type']}) → {cp_name}")
                        else:
                            logger.warning(f"_write_contact_to_ms: не удалось записать {pending['chat_id']} → {cp_name}")
            except Exception as e:
                logger.warning(f"Теги МойСклад: {e}")
            await query.message.edit_text(
                f"✅ *{pending['wazzup_name']}* → *{cp_name}*\nЭф запомнил!",
                parse_mode="Markdown"
            )
            await asyncio.sleep(3)
            try:
                await query.message.delete()
            except Exception:
                pass
        return

    # Выбор сегмента: wazzup_seg|сегмент|link_key
    if parts[0] == "wazzup_seg":
        segment = parts[1]
        link_key = parts[2]
        pending = _pending_links.get(link_key)
        if not pending:
            for uid, v in _pending_links.items():
                if isinstance(uid, int) and v.get("link_key") == link_key:
                    pending = v
                    break
        if not pending:
            await query.message.edit_text("❌ Сессия истекла, попробуй снова.")
            return
        pending["segment"] = segment
        # Спрашиваем менеджера
        MANAGERS = ["Баласанян К.", "Дьяченко И.", "Мерзлякова Е.", "Скляр И.", "Иванов А."]
        buttons = [[InlineKeyboardButton(m, callback_data=f"wazzup_mgr|{m}|{link_key}")] for m in MANAGERS]
        await query.message.edit_text(
            f"✅ Сегмент: *{segment}*\n\nОтветственный менеджер?",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

    # Выбор менеджера: wazzup_mgr|менеджер|link_key
    if parts[0] == "wazzup_mgr":
        manager = parts[1]
        link_key = parts[2]
        pending = _pending_links.get(link_key)
        if not pending:
            for uid, v in _pending_links.items():
                if isinstance(uid, int) and v.get("link_key") == link_key:
                    pending = v
                    break
        if not pending or "company_name" not in pending:
            await query.message.edit_text("❌ Сессия истекла, попробуй снова.")
            return
        # Очищаем
        _pending_links.pop(link_key, None)
        db.delete_pending_link(link_key)
        for uid in [k for k, v in list(_pending_links.items()) if isinstance(k, int) and v.get("link_key") == link_key]:
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
            # Подтягиваем теги из МойСклад
            try:
                from moysklad import find_counterparty_info
                cp_list = await find_counterparty_info(pending["company_name"])
                if cp_list:
                    cp = cp_list[0]
                    tags = cp.get("tags", [])
                    manager_tag = cp.get("manager", "")
                    buyer_type = cp.get("buyer_type", "")
                    db.update_wazzup_contact_tags(
                        chat_id=pending["chat_id"],
                        tags=tags,
                        manager=manager_tag,
                        segment=buyer_type,
                    )
            except Exception as e:
                logger.warning(f"Не удалось подтянуть теги из МойСклад: {e}")
        if ok:
            _wazzup_notified.discard(pending["chat_id"])
            await query.message.edit_text(
                f"✅ *{pending['wazzup_name']}* → *{pending['company_name']}* ({role})\nЭф запомнил!",
                parse_mode="Markdown"
            )
            await asyncio.sleep(3)
            try:
                await query.message.delete()
            except Exception:
                pass
        return

    # Выбор роли: wazzup_role|роль|link_key
    if parts[0] == "wazzup_role":
        role = parts[1]
        link_key = parts[2]

        # Отмена
        if role == "отмена":
            pending_data = _pending_links.get(link_key, {})
            _wazzup_notified.discard(pending_data.get("chat_id", ""))
            _pending_links.pop(link_key, None)
            db.delete_pending_link(link_key)
            for uid in [k for k, v in list(_pending_links.items()) if v.get("link_key") == link_key]:
                _pending_links.pop(uid, None)
            await query.message.delete()
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
        # После выбора роли — спрашиваем сегмент
        pending["role"] = role
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🍣 Хорека", callback_data=f"wazzup_seg|хорека|{link_key}"),
            InlineKeyboardButton("📦 Опт", callback_data=f"wazzup_seg|опт|{link_key}"),
            InlineKeyboardButton("🚚 Поставщик", callback_data=f"wazzup_seg|поставщик|{link_key}"),
        ]])
        await query.message.edit_text(
            f"✅ Роль: *{role}*\n\nСегмент клиента?",
            parse_mode="Markdown",
            reply_markup=keyboard
        )
        return

    # Откладываем идентификацию на 24 часа: wazzup_later|link_key
    if parts[0] == "wazzup_later":
        link_key = parts[1] if len(parts) > 1 else ""
        if link_key:
            db.postpone_pending_ident(link_key, hours=24)
            _wazzup_notified.discard(_pending_links.get(link_key, {}).get("chat_id", ""))
        await query.message.edit_text("⏰ Напомню через 24 часа.")
        return

    # Первое нажатие — запрашиваем название компании: wazzup_link|link_key
    if len(parts) < 2:
        return
    link_key = parts[1]
    pending = _pending_links.get(link_key)
    if not pending:
        await query.message.edit_text("❌ Сессия истекла, попробуй снова.")
        return

    # Помечаем что ждём ввода от этого пользователя — добавляем в стек
    if query.from_user.id not in _pending_links:
        _pending_links[query.from_user.id] = []
    # Если уже есть такой link_key — не дублируем
    existing_keys = [p.get("link_key") for p in _pending_links[query.from_user.id]] if isinstance(_pending_links.get(query.from_user.id), list) else []
    if link_key not in existing_keys:
        entry = {**pending, "link_key": link_key}
        if isinstance(_pending_links[query.from_user.id], list):
            _pending_links[query.from_user.id].append(entry)
        else:
            _pending_links[query.from_user.id] = [entry]
    _pending_links[link_key] = {**pending, "link_key": link_key}

    # Берём последний ожидающий контакт
    current = _pending_links[query.from_user.id][-1] if isinstance(_pending_links[query.from_user.id], list) else _pending_links[query.from_user.id]

    await query.message.edit_text(
        f"👤 Контакт: *{current['wazzup_name']}*\n\n"
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
        await update.message.reply_text("Укажи chat_id: /clearwazzup <id>")
        return
    try:
        with db.conn.cursor() as cur:
            cur.execute("DELETE FROM wazzup_contact_map WHERE chat_id=%s", (chat_id_val,))
            cur.execute("DELETE FROM wazzup_contacts WHERE chat_id=%s", (chat_id_val,))
        db.conn.commit()
        await update.message.reply_text(f"✅ Запись {chat_id_val} удалена.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

# Ожидающие данные для создания договора — user_id → {data, missing, missing_idx}
_pending_contracts: dict = {}

async def _create_and_send_contract(contract_data: dict, created_by: str,
                                    message, context, force_number: str = None):
    """Генерирует договор PDF и отправляет в группу."""
    import io, sys, os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    try:
        from contract_generator import generate_contract_pdf, get_contract_number
        from datetime import datetime

        today = datetime.now()

        if force_number:
            contract_number = force_number
        else:
            contract_number = get_contract_number(today, db)

        MONTHS_RU = ["января","февраля","марта","апреля","мая","июня",
                     "июля","августа","сентября","октября","ноября","декабря"]
        contract_data["contract_number"] = contract_number
        contract_data["contract_date"] = f"{today.day} {MONTHS_RU[today.month-1]} {today.year} г."

        pdf_bytes = generate_contract_pdf(contract_data)

        # Сохраняем в БД с полными реквизитами
        db.save_contract(contract_number, contract_data["buyer_name"], created_by,
                         buyer_data=contract_data)

        # Отправляем в группу
        group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
        target = group_chat_id or message.chat_id
        caption = (
            f"📄 *Договор поставки № {contract_number}*\n"
            f"📅 {contract_data['contract_date']}\n"
            f"🏢 {contract_data['buyer_name']}\n"
            f"👤 Создал: {created_by}"
        )
        await context.bot.send_document(
            chat_id=target,
            document=io.BytesIO(pdf_bytes),
            filename=f"Договор_{contract_number}_{contract_data['buyer_name'][:30]}.pdf",
            caption=caption,
            parse_mode="Markdown"
        )
        if target != message.chat_id:
            await message.reply_text(f"✅ Договор № {contract_number} отправлен в группу.", parse_mode="Markdown")

    except Exception as e:
        logger.error(f"_create_and_send_contract: {e}", exc_info=True)
        await message.reply_text(f"❌ Ошибка генерации договора: {e}")

async def cmd_wazzup_enrich(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обогащает базу контактов тегами из МойСклад. /wazzup_enrich"""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return

    rows = db._fetchall("SELECT chat_id, company_name FROM wazzup_contact_map WHERE company_name IS NOT NULL AND company_name != '__ignore__'")
    if not rows:
        await update.message.reply_text("База контактов пуста.")
        return

    await update.message.reply_text(f"🔍 Обогащаю {len(rows)} контактов из МойСклад...")
    from moysklad import find_counterparty_info
    updated = 0
    for r in rows:
        try:
            cp_list = await find_counterparty_info(r["company_name"])
            if cp_list:
                cp = cp_list[0]
                db.update_wazzup_contact_tags(
                    chat_id=r["chat_id"],
                    tags=cp.get("tags", []),
                    manager=cp.get("manager", ""),
                    segment=cp.get("buyer_type", ""),
                )
                updated += 1
        except Exception as e:
            logger.warning(f"wazzup_enrich {r['company_name']}: {e}")

    await update.message.reply_text(f"✅ Обновлено: {updated}/{len(rows)} контактов.")

async def cmd_wazzup_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выгружает базу идентифицированных контактов в Excel. /wazzup_export"""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return

    rows = db._fetchall("SELECT company_name, wazzup_name, chat_type, manager, segment, tags, created_at FROM wazzup_contact_map WHERE company_name != '__ignore__' ORDER BY company_name")

    if not rows:
        await update.message.reply_text("База контактов пуста.")
        return

    import io, csv
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Компания", "Имя в мессенджере", "Канал", "Менеджер", "Сегмент", "Теги", "Дата"])
    for r in rows:
        writer.writerow([
            r.get("company_name", ""),
            r.get("wazzup_name", ""),
            r.get("chat_type", ""),
            r.get("manager", ""),
            r.get("segment", ""),
            r.get("tags", ""),
            str(r.get("created_at", ""))[:10],
        ])

    output.seek(0)
    await update.message.reply_document(
        document=io.BytesIO(output.getvalue().encode("utf-8-sig")),
        filename="wazzup_contacts.csv",
        caption=f"📋 База контактов — {len(rows)} записей"
    )

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
    _public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "f2b-production.up.railway.app")
    webhook_url = f"https://{_public_domain}/webhook/wazzup"

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

    # 1. БД — базовая проверка
    try:
        db._ensure_connection()
        results.append("✅ База данных — соединение OK")
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
            _public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "f2b-production.up.railway.app")
            async with session.get(f"https://{_public_domain}/health") as resp:
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

async def handle_contract_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопок подтверждения создания договора."""
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "contract_cancel":
        await query.message.edit_text("❌ Создание договора отменено.")
        return

    if data.startswith("contract_force|"):
        cp_id = data.split("|")[1]
        from moysklad import get_counterparty_requisites
        await query.message.edit_text("🔍 Читаю реквизиты...")
        reqs = await get_counterparty_requisites(cp_id)
        contract_data = {
            "buyer_name": reqs.get("buyer_legal_title") or reqs.get("buyer_name", ""),
            "buyer_inn": reqs.get("buyer_inn", ""),
            "buyer_ogrn": reqs.get("buyer_ogrn", ""),
            "buyer_address": reqs.get("buyer_address", ""),
            "buyer_bank": reqs.get("buyer_bank", ""),
            "buyer_rs": reqs.get("buyer_rs", ""),
            "buyer_bik": reqs.get("buyer_bik", ""),
            "buyer_ks": reqs.get("buyer_ks", ""),
            "buyer_phone": reqs.get("buyer_phone", ""),
            "buyer_email": reqs.get("buyer_email", ""),
            "buyer_representative": reqs.get("buyer_representative", ""),
            "buyer_director_name": reqs.get("buyer_director_name", ""),
            "buyer_basis": "Устава",
        }
        # Для ИП: собираем полное ФИО если в buyer_name нет фамилии
        _ip_last = reqs.get("buyer_last_name", "")
        _ip_first = reqs.get("buyer_first_name", "")
        _ip_mid = reqs.get("buyer_middle_name", "")
        if _ip_last and _ip_first and _ip_last not in contract_data["buyer_name"]:
            _full_fio = " ".join(filter(None, [_ip_last, _ip_first, _ip_mid]))
            if contract_data["buyer_name"]:
                contract_data["buyer_name"] = contract_data["buyer_name"] + " " + _full_fio
            else:
                contract_data["buyer_name"] = _full_fio
        # Проверяем недостающие поля
        ASK_REQUIRED = {
            "buyer_representative": "должность и полное ФИО директора (напр. 'генерального директора Иванову Марию Алексеевну' — обязательно с фамилией!)",
            "buyer_basis": "основание полномочий",
        }
        INFO_REQUIRED = {
            "buyer_inn": "ИНН", "buyer_ogrn": "ОГРН",
            "buyer_address": "юридический адрес",
            "buyer_rs": "расчётный счёт (р/с)", "buyer_bik": "БИК банка",
            "buyer_bank": "название банка", "buyer_ks": "корреспондентский счёт (к/с)",
        }
        missing_ask = [(k, v) for k, v in ASK_REQUIRED.items() if not contract_data.get(k)]
        missing_info = [(k, v) for k, v in INFO_REQUIRED.items() if not contract_data.get(k)]
        user = query.from_user

        # Если не хватает данных из МойСклад — останавливаемся
        if missing_info:
            names = "\n".join(f"   • {v}" for _, v in missing_info)
            await query.message.edit_text(
                f"📄 *{contract_data['buyer_name']}*\n\n"
                f"❌ *Договор не сформирован* — в МойСклад отсутствуют:\n{names}\n\n"
                f"Попроси клиента предоставить данные и внеси их в карточку МойСклад, "
                f"затем запроси договор повторно.",
                parse_mode="Markdown"
            )
            return

        msg = f"📄 *{contract_data['buyer_name']}*\n"

        if missing_ask:
            _pending_contracts[user.id] = {
                "data": contract_data,
                "missing_keys": [m[0] for m in missing_ask],
                "missing_labels": [m[1] for m in missing_ask],
                "missing_idx": 0,
            }
            try:
                import json as _j
                db._execute("INSERT INTO pending_contracts (user_id, data) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET data=%s,created_at=NOW()", (user.id, _j.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str), _j.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str)))
            except Exception: pass
            msg += f"*{missing_ask[0][1]}*?"
            await query.message.edit_text(msg, parse_mode="Markdown")
        else:
            await _create_and_send_contract(
                contract_data, user.full_name, query.message, context
            )

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

async def process_sipuni_call(call_id: str, src_num: str, dst_num: str,
                              short_dst: str, tree_name: str, record_link: str,
                              call_start: str, call_answer: str, bot):
    """Скачивает запись звонка и транскрибирует через Whisper. Сохраняет в БД."""
    import aiohttp, tempfile, os as _os

    openai_key = _os.getenv("OPENAI_API_KEY", "")
    if not openai_key:
        logger.warning("process_sipuni_call: OPENAI_API_KEY не задан")
        return

    try:
        manager_name = tree_name or short_dst or dst_num

        # Длительность
        duration_sec = 0
        try:
            if call_start and call_answer and call_answer != "0":
                duration_sec = int(call_answer) - int(call_start)
                if duration_sec < 0:
                    duration_sec = 0
        except Exception:
            pass

        logger.info(f"Sipuni: транскрибирую звонок {call_id} от {src_num} ({duration_sec}с)")

        # 1. Скачиваем запись
        async with aiohttp.ClientSession() as session:
            async with session.get(record_link) as resp:
                if resp.status != 200:
                    logger.warning(f"Sipuni: не удалось скачать запись {resp.status}")
                    return
                audio_data = await resp.read()

        if len(audio_data) < 1000:
            logger.warning(f"Sipuni: запись слишком короткая ({len(audio_data)} байт)")
            return

        # 2. Транскрипция через OpenAI Whisper
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
            tmp.write(audio_data)
            tmp_path = tmp.name

        try:
            async with aiohttp.ClientSession() as session:
                with open(tmp_path, "rb") as f:
                    form = aiohttp.FormData()
                    form.add_field("file", f, filename="call.mp3", content_type="audio/mpeg")
                    form.add_field("model", "whisper-1")
                    form.add_field("language", "ru")
                    async with session.post(
                        "https://api.openai.com/v1/audio/transcriptions",
                        headers={"Authorization": f"Bearer {openai_key}"},
                        data=form
                    ) as resp:
                        if resp.status != 200:
                            body = await resp.text()
                            logger.warning(f"Whisper error {resp.status}: {body[:200]}")
                            return
                        result = await resp.json()
                        transcript = result.get("text", "")
        finally:
            _os.unlink(tmp_path)

        if not transcript:
            logger.warning("Sipuni: транскрипция пустая")
            return

        # 3. Сохраняем в БД
        saved = db.save_call_transcript(
            call_id=call_id,
            src_num=src_num,
            dst_num=dst_num,
            manager_name=manager_name,
            tree_name=tree_name,
            transcript=transcript,
            duration_sec=duration_sec,
        )
        logger.info(f"Sipuni: транскрипция сохранена call_id={call_id} saved={saved} ({len(transcript)} символов)")

    except Exception as e:
        logger.error(f"process_sipuni_call: {e}", exc_info=True)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главный обработчик всех сообщений."""
    message = update.message
    if not message:
        return

    # Диагностика входящих апдейтов: тип + chat + user
    _u = message.from_user
    _mt = ("video" if message.video else "video-doc" if (message.document and (message.document.mime_type or "").startswith("video/")) else "photo" if message.photo else "doc" if message.document else "text" if message.text else "other")
    logger.info(f"HM-IN: chat={message.chat_id} type={_mt} user={_u.id if _u else None} user_name={_u.full_name if _u else '-'}")

    # Видео от собственника/партнёра в личке → сохраняем в media до всех остальных return'ов
    if _u and _u.id in {OWNER_CHAT_ID, PARTNER_CHAT_ID}:
        if message.video:
            await save_media(message, "video")
            return
        if message.document and (message.document.mime_type or "").startswith("video/"):
            # видео отправленное как файл (большое или несжатое)
            file_id = message.document.file_id
            caption = message.caption or message.document.file_name or ""
            db.save_media(
                file_id=file_id,
                media_type="video",
                caption=caption,
                chat_id=message.chat_id,
                uploader=_u.full_name,
                date=datetime.now().isoformat()
            )
            await message.reply_text("Видео-файл сохранён в базу.")
            return

    async def safe_reply(text, **kwargs):
        """Отправляет ответ, при ошибке цитаты — без неё."""
        try:
            return await message.reply_text(text, **kwargs)
        except Exception:
            try:
                return await context.bot.send_message(
                    chat_id=message.chat_id, text=text,
                    parse_mode=kwargs.get("parse_mode"),
                    reply_markup=kwargs.get("reply_markup")
                )
            except Exception as e:
                logger.warning(f"safe_reply failed: {e}")

    # Команды обрабатываются отдельными CommandHandler — пропускаем
    if message.text and message.text.startswith("/"):
        return

    chat_id = message.chat_id
    user = message.from_user
    text = message.text or message.caption or ""

    # Проверяем блокировку
    if user and db.is_user_blocked(user.id):
        await message.reply_text("⛔ Доступ ограничен. Обратитесь к руководителю.")
        return

    # Логируем запрос
    if user and text:
        db.log_usage(user.id, user.full_name, text[:100], chat_id)

    # Обработка ожидаемого ввода из меню (фото / ПДЗ клиента)
    awaiting = _user_awaiting.get(user.id) if user else None
    if awaiting and user and text and not text.startswith("/"):
        _user_awaiting.pop(user.id, None)

        if awaiting == "photo":
            await message.reply_chat_action("upload_photo")
            await search_and_send_photo(update, context, text)
            await message.reply_text("Выбери действие:", reply_markup=_user_menu_keyboard())
            return

        elif awaiting in ("request_text", "request_text_amend"):
            await message.reply_chat_action("typing")
            try:
                from request_handler import (
                    parse_request_text, route_request, format_preview, validate_request,
                )
                # Amend — соединяем с предыдущим raw_text, чтобы LLM видел всё вместе.
                if awaiting == "request_text_amend":
                    prev = _draft_requests.get(user.id)
                    base_text = prev[0].raw_text if prev else ""
                    combined = (base_text + "\n" + text).strip() if base_text else text
                else:
                    combined = text
                parsed = await parse_request_text(combined)
            except Exception as e:
                logger.exception(f"request_text parse failed: {e}")
                await message.reply_text(
                    f"⚠️ Не удалось разобрать заявку: {e}\n\nПопробуй ещё раз через меню.",
                )
                await message.reply_text("Выбери действие:", reply_markup=_user_menu_keyboard())
                return
            assigned_to = route_request(parsed.species)
            _draft_requests[user.id] = (parsed, assigned_to)
            missing_req, missing_rec = validate_request(parsed)
            preview = format_preview(parsed, assigned_to, missing_req, missing_rec)

            # Без обязательных полей — нет «Подтвердить», только «Дополнить»+«Отменить».
            if missing_req:
                kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton("➕ Дополнить", callback_data="req_amend"),
                    InlineKeyboardButton("❌ Отменить",  callback_data="req_cancel"),
                ]])
            else:
                kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Подтвердить", callback_data="req_confirm"),
                    InlineKeyboardButton("➕ Дополнить",  callback_data="req_amend"),
                    InlineKeyboardButton("❌ Отменить",   callback_data="req_cancel"),
                ]])
            await message.reply_text(preview, parse_mode="Markdown", reply_markup=kb)
            return


        elif awaiting == "reconciliation":
            await message.reply_chat_action("typing")
            from datetime import datetime as _datetime
            buyer_query = text
            counterparties = await get_counterparty_balance(buyer_query)
            if not counterparties:
                await message.reply_text(
                    f"❌ Компания *{buyer_query}* не найдена в МойСклад.",
                    parse_mode="Markdown"
                )
                await message.reply_text("Выбери действие:", reply_markup=_user_menu_keyboard())
                return
            cp = counterparties[0]
            cp_id = cp.get("id", "")
            cp_name = cp.get("name", buyer_query)
            date_from = f"{_datetime.now().year}-01-01"
            date_to = _datetime.now().strftime("%Y-%m-%d")
            await message.reply_text(
                f"📊 Формирую акт сверки *{cp_name}*\n"
                f"📅 Период: {date_from} — {date_to}\n"
                f"⏳ Запрашиваю данные из МойСклад...",
                parse_mode="Markdown"
            )
            try:
                import io as _io
                from reconciliation_generator import generate_reconciliation_pdf
                from moysklad import get_reconciliation_data
                rec_data = await get_reconciliation_data(cp_id, date_from, date_to)
                if not rec_data or not rec_data.get("rows"):
                    await message.reply_text(
                        f"😕 За период {date_from} — {date_to} операций с *{cp_name}* не найдено.",
                        parse_mode="Markdown"
                    )
                    return
                pdf_bytes = generate_reconciliation_pdf(rec_data)
                closing = rec_data.get("closing_balance", 0)
                if closing > 0:
                    balance_str = f"💰 Долг клиента: *{closing:,.2f} руб.*"
                elif closing < 0:
                    balance_str = f"💰 Переплата: *{abs(closing):,.2f} руб.*"
                else:
                    balance_str = "✅ Взаиморасчёты согласованы"
                caption = (
                    f"📊 *Акт сверки — {cp_name}*\n"
                    f"📅 {date_from} — {date_to}\n"
                    f"{balance_str}"
                )
                await message.reply_document(
                    document=_io.BytesIO(pdf_bytes),
                    filename=f"Акт_сверки_{cp_name[:30]}_{date_to}.pdf",
                    caption=caption,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"reconciliation awaiting error: {e}", exc_info=True)
                await message.reply_text(f"❌ Ошибка формирования акта: {e}")
            await message.reply_text("Выбери действие:", reply_markup=_user_menu_keyboard())
            return

        elif awaiting == "contract":
            await message.reply_chat_action("typing")
            buyer_query = text
            from moysklad import get_counterparty_requisites
            from datetime import date as _date, datetime as _datetime
            counterparties = await get_counterparty_balance(buyer_query)
            if not counterparties:
                await message.reply_text(
                    f"❌ Компания *{buyer_query}* не найдена в МойСклад.",
                    parse_mode="Markdown"
                )
                await message.reply_text("Выбери действие:", reply_markup=_user_menu_keyboard())
                return
            cp = counterparties[0]
            cp_id = cp.get("id", "")
            cp_name = cp.get("name", buyer_query)

            # Проверяем дату создания клиента — если до 20.03.2026, договор не создаём
            created_str = cp.get("created", "")
            CUTOFF = _datetime(2026, 3, 20)
            is_old_client = False
            if created_str:
                try:
                    created_dt = _datetime.fromisoformat(created_str[:19])
                    if created_dt < CUTOFF:
                        is_old_client = True
                except Exception:
                    pass

            if is_old_client:
                await message.reply_text(
                    f"⚠️ *{cp_name}* — действующий клиент.\n\n"
                    f"Договор по этому клиенту уже существует или ведётся в работе.\n"
                    f"По вопросам договора обратитесь к *Юлии Гераськиной*.",
                    parse_mode="Markdown",
                    reply_markup=_user_menu_keyboard()
                )
                return

            existing = db.find_contract_by_buyer(cp_name)
            if existing:
                await message.reply_text(
                    f"📄 Договор с *{cp_name}* уже был сформирован.\n"
                    f"Номер: *{existing['contract_number']}* от {existing['created_at'].strftime('%d.%m.%Y')}\n\n"
                    f"Повторное формирование невозможно.\n"
                    f"По вопросам договора обратитесь к *Юлии Гераськиной*.",
                    parse_mode="Markdown"
                )
                await message.reply_text("Выбери действие:", reply_markup=_user_menu_keyboard())
                return
            # (проверка по дате создания убрана — достаточно проверки CUTOFF выше)
            await message.reply_text(f"🔍 Читаю реквизиты *{cp_name}*...", parse_mode="Markdown")
            reqs = await get_counterparty_requisites(cp_id)
            contract_data = {
                "buyer_name": reqs.get("buyer_legal_title") or reqs.get("buyer_name", buyer_query),
                "buyer_inn": reqs.get("buyer_inn", ""),
                "buyer_ogrn": reqs.get("buyer_ogrn", ""),
                "buyer_address": reqs.get("buyer_address", ""),
                "buyer_bank": reqs.get("buyer_bank", ""),
                "buyer_rs": reqs.get("buyer_rs", ""),
                "buyer_bik": reqs.get("buyer_bik", ""),
                "buyer_ks": reqs.get("buyer_ks", ""),
                "buyer_phone": reqs.get("buyer_phone", ""),
                "buyer_email": reqs.get("buyer_email", ""),
                "buyer_representative": reqs.get("buyer_representative", ""),
                "buyer_director_name": reqs.get("buyer_director_name", ""),
                "buyer_basis": "Устава",
            }
            # Для ИП: собираем полное ФИО если в buyer_name нет фамилии
            _ip_last = reqs.get("buyer_last_name", "")
            _ip_first = reqs.get("buyer_first_name", "")
            _ip_mid = reqs.get("buyer_middle_name", "")
            if _ip_last and _ip_first and _ip_last not in contract_data["buyer_name"]:
                _full_fio = " ".join(filter(None, [_ip_last, _ip_first, _ip_mid]))
                if contract_data["buyer_name"]:
                    contract_data["buyer_name"] = contract_data["buyer_name"] + " " + _full_fio
                else:
                    contract_data["buyer_name"] = _full_fio
            INFO_REQUIRED = {
                "buyer_inn": "ИНН", "buyer_ogrn": "ОГРН",
                "buyer_address": "юридический адрес",
                "buyer_rs": "расчётный счёт (р/с)", "buyer_bik": "БИК банка",
                "buyer_bank": "название банка", "buyer_ks": "корреспондентский счёт (к/с)",
            }
            ASK_REQUIRED = {
                "buyer_representative": "должность и полное ФИО директора (напр. 'генерального директора Иванову Марию Алексеевну' — обязательно с фамилией!)",
                "buyer_basis": "основание полномочий",
            }
            missing_info = [(k, v) for k, v in INFO_REQUIRED.items() if not contract_data.get(k)]
            missing_ask = [(k, v) for k, v in ASK_REQUIRED.items() if not contract_data.get(k)]

            if missing_info:
                names = "\n".join(f"   • {v}" for _, v in missing_info)
                await message.reply_text(
                    f"📄 *{contract_data['buyer_name']}*\n\n"
                    f"❌ *Договор не сформирован* — в МойСклад отсутствуют:\n{names}\n\n"
                    f"Внеси данные в карточку МойСклад и запроси договор повторно.",
                    parse_mode="Markdown",
                    reply_markup=_user_menu_keyboard()
                )
                return
            if missing_ask:
                _pending_contracts[user.id] = {
                    "data": contract_data,
                    "missing_keys": [m[0] for m in missing_ask],
                    "missing_labels": [m[1] for m in missing_ask],
                    "missing_idx": 0,
                }
                try:
                    import json as _j
                    db._execute("INSERT INTO pending_contracts (user_id, data) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET data=%s,created_at=NOW()", (user.id, _j.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str), _j.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str)))
                except Exception: pass
                await message.reply_text(
                    f"📄 *{contract_data['buyer_name']}*\n\nУточни:\n*{missing_ask[0][1]}*?",
                    parse_mode="Markdown"
                )
            else:
                await _create_and_send_contract(contract_data, user.full_name, message, context)
            return

    # Ответ менеджера на алерт цены в личке — пересылаем Виктору
    if user and chat_id == user.id and chat_id != OWNER_CHAT_ID and text:

        # 1. Approver написал текст комментария для менеджера (approval-алерт)
        if user.id in _pending_approver_input:
            pending = _pending_approver_input.pop(user.id)
            mgr_uid = pending.get("manager_user_id") or 0
            alert_id = pending["alert_id"]
            order_name = pending.get("order_name", "")
            client_name = pending.get("client_name", "")
            mgr_name = pending.get("manager_name", "?")

            # Fallback: если в БД остался mgr_user_id=0 (из-за старого бага
            # db.get_manager_chat_id), пробуем дорезолвить через
            # PDZ_MANAGER_TG_IDS на лету.
            if not mgr_uid and mgr_name and mgr_name != "?":
                from moysklad import PDZ_MANAGER_TG_IDS
                for part in mgr_name.split():
                    key = part.lower().strip(".,").rstrip()
                    if key in PDZ_MANAGER_TG_IDS:
                        mgr_uid = PDZ_MANAGER_TG_IDS[key]
                        logger.info(
                            f"appr_comment fallback: {mgr_name} → {mgr_uid} "
                            f"через PDZ_MANAGER_TG_IDS"
                        )
                        break

            if mgr_uid:
                try:
                    await context.bot.send_message(
                        chat_id=mgr_uid,
                        text=(
                            f"💬 *Вопрос по заказу {order_name} ({client_name})*\n\n"
                            f"{text}\n\n"
                            f"_Ответь в этот чат — ответ уйдёт обратно._"
                        ),
                        parse_mode="Markdown",
                    )
                    # Ждём reply менеджера → отправим обратно approver'у через _pending_price_comments
                    _pending_price_comments[mgr_uid] = {
                        "alert_id": alert_id,
                        "alert_type": "approval",
                        "approver_chat_id": user.id,
                        "order_id": pending.get("order_name", ""),
                        "order_name": order_name,
                        "client_name": client_name,
                        "mgr_name": mgr_name,
                    }
                    await message.reply_text("✅ Уйдёт менеджеру; жду его ответ.")
                except Exception as e:
                    logger.error(f"appr_comment forward: {e}")
                    await message.reply_text(f"❌ Не удалось отправить: {e}")
            else:
                await message.reply_text(
                    f"❌ Chat_id менеджера {mgr_name} не известен боту. "
                    f"Напишите ему напрямую."
                )
            return

        # 2. Ответ на запрос комментария по цене (price_*) ИЛИ от менеджера на approval
        if user.id in _pending_price_comments:
            pending = _pending_price_comments.pop(user.id)
            alert_id = pending.get("alert_id", 0)
            order_id = pending.get("order_id", "")
            order_name = pending.get("order_name", "")
            client_name = pending.get("client_name", "")
            mgr_name = pending.get("mgr_name", user.full_name)
            alert_type = pending.get("alert_type", "price")

            if alert_type == "approval":
                # Reply менеджера на approval-комментарий → шлём approver'у с шапкой заказа
                approver_chat_id = pending.get("approver_chat_id", OWNER_CHAT_ID)
                if alert_id:
                    db.close_approval_alert(alert_id, closed_by=user.id, comment=text)
                await context.bot.send_message(
                    chat_id=approver_chat_id,
                    text=(
                        f"💬 *Ответ по заказу {order_name} ({client_name})*\n"
                        f"👤 Менеджер: *{mgr_name}*\n\n"
                        f"{text}"
                    ),
                    parse_mode="Markdown",
                )
                await message.reply_text("✅ Ответ передан.")
                return

            # Старый flow для price_alerts
            if alert_id:
                db.close_price_alert(alert_id, text)
            await context.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=(
                    f"💬 *Комментарий по занижению цены*\n"
                    f"👤 Менеджер: *{mgr_name}*\n\n"
                    f"{text}"
                ),
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Согласовано", callback_data=f"price_ok|{order_id}"),
                ]])
            )
            await message.reply_text("✅ Комментарий отправлен руководителю.")
            return

        # 3. Ответ на алерт цены через reply
        replied = message.reply_to_message
        if replied and replied.text and "#price_alert_" in replied.text:
            import re
            m = re.search(r"#price_alert_(\d+)", replied.text)
            if m:
                alert_id = int(m.group(1))
                alert_data = db.get_price_alert(alert_id)
                if alert_data:
                    db.close_price_alert(alert_id, text)
                    order_name = alert_data.get("order_name", "")
                    mgr_name = alert_data.get("manager_name", user.full_name)
                    await context.bot.send_message(
                        chat_id=OWNER_CHAT_ID,
                        text=(
                            f"💬 *Ответ по алерту цены*\n"
                            f"👤 Менеджер: *{mgr_name}*\n"
                            f"📋 Заказ: {order_name or 'см. выше'}\n\n"
                            f"{text}"
                        ),
                        parse_mode="Markdown"
                    )
                    await message.reply_text("✅ Ответ отправлен руководителю.")
                    return

        # Прочие сообщения в личке — бот не реагирует
        return

    if message.forward_origin and chat_id == (user.id if user else None):
        origin = message.forward_origin
        logger.info(f"forward_origin type={type(origin).__name__} data={origin}")

        fwd_id = None
        fwd_name = None
        chat_type = "telegram"

        # Обычный пользователь с открытым профилем
        fwd_user = getattr(origin, "sender_user", None)
        if fwd_user:
            fwd_id = str(fwd_user.id)
            fwd_name = fwd_user.full_name or str(fwd_user.id)

        # Скрытый пользователь — нет ID, писать нельзя
        if not fwd_id:
            fwd_name = getattr(origin, "sender_user_name", None)
            if fwd_name:
                await message.reply_text(
                    f"😕 У контакта *{fwd_name}* закрыт профиль в Telegram.\n"
                    f"Написать ему через бота не получится.",
                    parse_mode="Markdown"
                )
            return

        # Канал или чат
        fwd_chat = getattr(origin, "sender_chat", None) or getattr(origin, "chat", None)
        if not fwd_id and fwd_chat:
            fwd_id = str(fwd_chat.id)
            fwd_name = fwd_chat.title or fwd_chat.username or str(fwd_chat.id)

        if not fwd_id or not fwd_name:
            await message.reply_text("😕 Не удалось определить контакт — возможно у него закрыта приватность.")
            return

        if db.is_wazzup_contact_known(fwd_id):
            rows = db._fetchall("SELECT company_name FROM wazzup_contact_map WHERE chat_id=%s", (fwd_id,))
            if rows:
                await message.reply_text(f"✅ Контакт *{fwd_name}* уже привязан к *{rows[0]['company_name']}*", parse_mode="Markdown")
            return

        import uuid as _uuid_fwd
        link_key = str(_uuid_fwd.uuid4())[:8]
        entry = {
            "chat_id": fwd_id,
            "channel_id": "ddd24a95-9304-4098-a320-3e47fcd1020a",
            "wazzup_name": fwd_name,
            "chat_type": chat_type,
            "link_key": link_key,
        }
        _pending_links[link_key] = entry
        # Используем стек как и везде
        if user.id not in _pending_links or not isinstance(_pending_links.get(user.id), list):
            _pending_links[user.id] = []
        _pending_links[user.id].append(entry)
        await message.reply_text(
            f"👤 Контакт: *{fwd_name}*\n\nКак этот клиент называется в МойСклад?\n_(напиши название или часть названия)_",
            parse_mode="Markdown"
        )
        return

    # В группе ИДЕНТИФИКАЦИЯ — обрабатываем ввод названия компании
    wazzup_id_chat = int(os.getenv("WAZZUP_ID_CHAT_ID", "0"))
    if wazzup_id_chat and chat_id == wazzup_id_chat:
        if not text or text.startswith("/"):
            return
        # Удаляем сообщение менеджера чтобы не засорять группу
        try:
            await message.delete()
        except Exception:
            pass
        # Если у этого пользователя нет pending — ищем любой активный pending в группе
        if user and user.id not in _pending_links:
            for uid, pl in list(_pending_links.items()):
                if isinstance(uid, int) and isinstance(pl, list) and pl:
                    _pending_links[user.id] = pl
                    break
                elif isinstance(uid, int) and isinstance(pl, dict) and "wazzup_name" in pl:
                    _pending_links[user.id] = [pl]
                    break
        logger.info(f"ИДЕНТИФИКАЦИЯ: user={user.id if user else None} pending={user.id in _pending_links if user else False} all_keys={[k for k in _pending_links.keys() if isinstance(k, int)]}")
        if user and user.id not in _pending_links:
            return

    # Сохраняем все сообщения в историю чата
    if text and user:
        db.save_message(
            chat_id=chat_id,
            user_id=user.id,
            user_name=user.full_name,
            text=text[:1000],  # обрезаем очень длинные
        )

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

    # Видео-форварды от собственника/партнёра → сохраняем как контент
    if message.video and user and user.id in {OWNER_CHAT_ID, PARTNER_CHAT_ID}:
        await save_media(message, "video")

    if not text:
        return

    # 2. Автоматическое извлечение задач (анализируем ВСЕ сообщения руководителя)
    # Список ID руководителей — добавь в .env
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]

    # Логируем ID для диагностики
    logger.info(f"Message from user.id={user.id}, name={user.full_name}, chat_id={message.chat_id}, manager_ids={manager_ids}")

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

    # Проверяем ожидание данных для договора (ПОСЛЕ идентификации)
    if user and user.id in _pending_contracts:
        pending_c = _pending_contracts[user.id]
        keys = pending_c["missing_keys"]
        labels = pending_c["missing_labels"]
        idx = pending_c["missing_idx"]
        data = pending_c["data"]

        # Чистим текст от обращения "Эф," если есть
        answer_text = text.strip()
        for prefix in ["эф,", "эф ", "ef,", "ef "]:
            if answer_text.lower().startswith(prefix):
                answer_text = answer_text[len(prefix):].strip()
                break

        field_key = keys[idx]
        data[field_key] = answer_text

        if field_key == "buyer_representative":
            parts = answer_text.split()
            # Слова которые являются частью должности (даже с заглавной буквы)
            POSITION_WORDS = {
                "генерального", "генеральный", "директора", "директор",
                "индивидуального", "индивидуальный", "предпринимателя", "предприниматель",
                "исполнительного", "исполнительный", "коммерческого", "коммерческий",
                "финансового", "финансовый", "управляющего", "управляющий",
                "президента", "президент", "председателя", "председатель",
            }
            # Ищем первое слово с заглавной буквы которое НЕ является словом должности
            fio_start = 0
            for i, w in enumerate(parts):
                cleaned = w.strip(".,").lower()
                if w.strip(".,") and w.strip(".,")[0].isupper() and i > 0 and cleaned not in POSITION_WORDS:
                    fio_start = i
                    break
            if fio_start > 0:
                data["buyer_director_name"] = " ".join(parts[fio_start:])
            elif len(parts) >= 2:
                data["buyer_director_name"] = " ".join(parts[-3:]) if len(parts) >= 3 else " ".join(parts)

        idx += 1
        pending_c["missing_idx"] = idx

        if idx < len(keys):
            try:
                import json as _j
                db._execute("INSERT INTO pending_contracts (user_id, data) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET data=%s,created_at=NOW()", (user.id, _j.dumps(pending_c, ensure_ascii=False, default=str), _j.dumps(pending_c, ensure_ascii=False, default=str)))
            except Exception: pass
            await message.reply_text(f"✅ Принято.\n\n*{labels[idx]}*?", parse_mode="Markdown")
        else:
            _pending_contracts.pop(user.id, None)
            db._execute("DELETE FROM pending_contracts WHERE user_id=%s", (user.id,))

            # Режим refresh — генерируем с сохранением номера и даты
            if pending_c.get("mode") == "refresh":
                await message.reply_text("✅ Данные получены. Генерирую договор...")
                try:
                    from contract_generator import generate_contract_pdf
                    import io as _io_refresh
                    pdf_bytes = generate_contract_pdf(data)
                    db.save_contract(data["contract_number"], data["buyer_name"], user.full_name, buyer_data=data)
                    group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
                    target = group_chat_id or message.chat_id
                    await context.bot.send_document(
                        chat_id=target,
                        document=_io_refresh.BytesIO(pdf_bytes),
                        filename=f"Договор_{data['contract_number']}_{data['buyer_name'][:30]}.pdf",
                        caption=(
                            f"📄 *Договор поставки № {data['contract_number']}* (переиздан, данные обновлены)\n"
                            f"📅 {data['contract_date']}\n"
                            f"🏢 {data['buyer_name']}\n"
                            f"👤 {user.full_name}"
                        ),
                        parse_mode="Markdown"
                    )
                    if target != message.chat_id:
                        await message.reply_text(f"✅ Договор № {data['contract_number']} переиздан и отправлен в группу.")
                except Exception as _e:
                    logger.error(f"refresh pending generate: {_e}", exc_info=True)
                    await message.reply_text(f"❌ Ошибка генерации: {_e}")
            else:
                await message.reply_text("✅ Все данные получены. Генерирую договор...")
                await _create_and_send_contract(data, user.full_name, message, context)
        return

    if not is_bot_addressed(text):
        return

    query = clean_query(text)

    # ── Всё через Claude — он сам разбирается что нужно ──
    # typing — косметика; сетевой дёрг к api.telegram.org не должен ронять весь handler
    # (24.06.2026 — ConnectTimeout здесь убил ответ на «Эф, когда доставка в Красногорск»)
    try:
        await message.reply_chat_action("typing")
    except Exception as e:
        logger.warning(f"reply_chat_action(typing) failed (non-fatal): {e}")
    context_data = db.get_context_summary()
    chat_history = db.format_history(chat_id, limit=40)
    memories = db.format_memories()

    logger.info(f"Dispatching query='{query}' from '{user.full_name}'")
    result = await dispatch(query, user.full_name, context_data,
                            chat_history=chat_history, memories=memories)
    logger.info(f"Dispatch result: {result}")
    action = result.get("action", "answer")
    params = result.get("params", {})

    if action == "get_report":
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

        # None означает что клиент не найден в МойСклад
        if items is None:
            await message.reply_text(
                f"❌ Клиент *{raw_query}* не найден в МойСклад.\n"
                f"Уточни название — например, часть названия компании.",
                parse_mode="Markdown"
            )
            return

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
                        "ирина": "дьяченко", "дьяченко": "дьяченко",
        }
        USER_MANAGER_DISPLAY = {
            "баласанян": "Карина Баласанян",
            "скляр": "Инесса Скляр",
            "мерзлякова": "Елена Мерзлякова",
            "дьяченко": "Ирина Дьяченко",
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

        MANAGER_TAG_MAP = {
            "баласанян": "Карина Баласанян", "мерзлякова": "Елена Мерзлякова",
            "скляр": "Инесса Скляр", "дьяченко": "Ирина Дьяченко",
        }
        SPEC_TAGS = {"опт", "хорека", "розница"}

        for b in buyers:
            tags = b.get("tags", [])
            tags_lower = [t.lower() for t in tags]
            spec = next((t.capitalize() for t in tags_lower if t in SPEC_TAGS), "—")
            manager = next((MANAGER_TAG_MAP[t] for t in tags_lower if t in MANAGER_TAG_MAP), "Не назначен")
            lines.append(
                f"👤 *{b['name']}*\n"
                f"   👔 Менеджер: {manager}\n"
                f"   🏷 Категория: {spec}"
            )
        text = "\n\n".join(lines)
        if len(text) > 4000:
            text = text[:3900] + "\n\n_...и ещё_"
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "get_delivery_days":
        address = params.get("address", "")
        if not address:
            await message.reply_text("❌ Укажи адрес или город.")
            return
        await message.reply_chat_action("typing")
        from moysklad import check_delivery_schedule, DELIVERY_CITIES_COORDS, _CITY_INDEX, WEEKDAYS_RU, geocode_address, _haversine, MOSCOW_AGGLOMERATION_KEYWORDS

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

        # Ближняя агломерация (Красногорск, Реутов, Балашиха и т.п.) — до геокодера,
        # т.к. Яндекс по голому названию города часто отдаёт пустой результат
        for kw, canonical in MOSCOW_AGGLOMERATION_KEYWORDS.items():
            if kw in address_lower:
                await message.reply_text(
                    f"🚛 *{canonical}* — московская агломерация, доставляем в любой рабочий день.",
                    parse_mode="Markdown"
                )
                return

        # Геокодируем
        coords = await geocode_address(address)
        if not coords:
            await message.reply_text(f"😕 Не удалось определить направление для адреса: {address}")
            return

        lat, lon = coords
        dist_from_moscow = _haversine(lat, lon, 55.7558, 37.6173)

        # Ищем ближайший маршрутный город (из расписания)
        nearest_city = None
        nearest_dist = float("inf")
        for city, (clat, clon) in DELIVERY_CITIES_COORDS.items():
            dist = _haversine(lat, lon, clat, clon)
            if dist < nearest_dist:
                nearest_dist = dist
                nearest_city = city

        def _days_for(city_name):
            for _kw, _info in _CITY_INDEX.items():
                if _info["canonical"] == city_name:
                    return [WEEKDAYS_RU[d] for d in sorted(_info["days"])]
            return []

        # 1) Очень близко (≤15 км) к маршрутному городу → это его маршрут
        if nearest_city and nearest_dist <= 15:
            days = _days_for(nearest_city)
            days_str = ", ".join(days) if days else "уточни у руководителя"
            await message.reply_text(
                f"🚛 *{address.split(',')[0].strip()}* — направление *{nearest_city}* ({round(nearest_dist)} км)\n"
                f"📅 Дни доставки: *{days_str}*",
                parse_mode="Markdown"
            )
            return

        # 2) В пределах московской агломерации (<35 км от центра) → любой рабочий день
        if dist_from_moscow < 35:
            await message.reply_text("🚛 Адрес в московской агломерации — доставляем в любой рабочий день.", parse_mode="Markdown")
            return

        # 3) Дальний матч до 25 км → выдаём с пометкой «близко к»
        if nearest_city and nearest_dist <= 25:
            days = _days_for(nearest_city)
            days_str = ", ".join(days) if days else "уточни у руководителя"
            await message.reply_text(
                f"🚛 Адрес близко к *{nearest_city}* ({round(nearest_dist)} км)\n"
                f"📅 Дни доставки: *{days_str}*",
                parse_mode="Markdown"
            )
            return

        # 4) Не входит ни в одно направление МО
        await message.reply_text(
            f"😕 Адрес *{address}* не входит ни в одно наше направление МО.\n"
            f"Уточни у руководителя.",
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

    elif action == "reconciliation":
        buyer_query = params.get("buyer", "")
        date_from = params.get("date_from", f"{datetime.now().year}-01-01")
        date_to = params.get("date_to", datetime.now().strftime("%Y-%m-%d"))

        await message.reply_chat_action("typing")

        # Ищем контрагента
        counterparties = await get_counterparty_balance(buyer_query)
        if not counterparties:
            await message.reply_text(
                f"❌ Компания *{buyer_query}* не найдена в МойСклад.",
                parse_mode="Markdown"
            )
            return

        cp = counterparties[0]
        cp_id = cp.get("id", "")
        cp_name = cp.get("name", buyer_query)

        await message.reply_text(
            f"📊 Формирую акт сверки *{cp_name}*\n"
            f"📅 Период: {date_from} — {date_to}\n"
            f"⏳ Запрашиваю данные из МойСклад...",
            parse_mode="Markdown"
        )

        try:
            import io as _io
            from reconciliation_generator import generate_reconciliation_pdf
            from moysklad import get_reconciliation_data

            # Получаем данные
            rec_data = await get_reconciliation_data(cp_id, date_from, date_to)

            if not rec_data or not rec_data.get("rows"):
                await message.reply_text(
                    f"😕 За период {date_from} — {date_to} операций с *{cp_name}* не найдено.",
                    parse_mode="Markdown"
                )
                return

            pdf_bytes = generate_reconciliation_pdf(rec_data)

            # Отправляем в группу
            group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
            target = group_chat_id or message.chat_id
            closing = rec_data.get("closing_balance", 0)
            if closing > 0:
                balance_str = f"💰 Долг клиента: *{closing:,.2f} руб.*"
            elif closing < 0:
                balance_str = f"💰 Переплата: *{abs(closing):,.2f} руб.*"
            else:
                balance_str = "✅ Взаиморасчёты согласованы"
            caption = (
                f"📊 *Акт сверки — {cp_name}*\n"
                f"📅 {date_from} — {date_to}\n"
                f"{balance_str}"
            )
            await context.bot.send_document(
                chat_id=target,
                document=_io.BytesIO(pdf_bytes),
                filename=f"Акт_сверки_{cp_name[:30]}_{date_to}.pdf",
                caption=caption,
                parse_mode="Markdown"
            )
            if target != message.chat_id:
                await message.reply_text("✅ Акт сверки отправлен в группу.")

        except Exception as e:
            logger.error(f"reconciliation error: {e}", exc_info=True)
            await message.reply_text(f"❌ Ошибка формирования акта: {e}")
        return

    elif action == "generate_contract":
        buyer_query = params.get("buyer", "")
        await message.reply_chat_action("typing")

        from moysklad import get_counterparty_requisites
        from datetime import date as _date, datetime as _datetime

        counterparties = await get_counterparty_balance(buyer_query)
        if not counterparties:
            await message.reply_text(
                f"❌ Компания *{buyer_query}* не найдена в МойСклад.",
                parse_mode="Markdown"
            )
            return

        cp = counterparties[0]
        cp_id = cp.get("id", "")
        cp_name = cp.get("name", buyer_query)

        # Запрет на договор для клиентов созданных до 20.03.2026
        created_str = cp.get("created", "")
        CUTOFF = _datetime(2026, 3, 20)
        if created_str:
            try:
                if _datetime.fromisoformat(created_str[:19]) < CUTOFF:
                    await message.reply_text(
                        f"⚠️ *{cp_name}* — действующий клиент.\n\n"
                        f"Договор по этому клиенту уже существует или ведётся в работе.\n"
                        f"По вопросам договора обратитесь к *Юлии Гераськиной*.",
                        parse_mode="Markdown"
                    )
                    return
            except Exception:
                pass

        # Проверяем — есть ли уже договор созданный Эфом
        existing = db.find_contract_by_buyer(cp_name)
        if existing:
            await message.reply_text(
                f"📄 Договор с *{cp_name}* уже был сформирован.\n"
                f"Номер: *{existing['contract_number']}* от {existing['created_at'].strftime('%d.%m.%Y')}\n\n"
                f"Повторное формирование договора невозможно.\n"
                f"По вопросам договора обратитесь к *Юлии Гераськиной*.",
                parse_mode="Markdown"
            )
            return

        # (проверка по дате создания убрана — достаточно проверки CUTOFF выше)
        if False:
            await message.reply_text("placeholder")
            return

        # Читаем полные реквизиты
        await message.reply_text(f"🔍 Читаю реквизиты *{cp.get('name','')}*...", parse_mode="Markdown")
        reqs = await get_counterparty_requisites(cp_id)

        contract_data = {
            "buyer_name": reqs.get("buyer_legal_title") or reqs.get("buyer_name", buyer_query),
            "buyer_inn": reqs.get("buyer_inn", ""),
            "buyer_ogrn": reqs.get("buyer_ogrn", ""),
            "buyer_address": reqs.get("buyer_address", ""),
            "buyer_bank": reqs.get("buyer_bank", ""),
            "buyer_rs": reqs.get("buyer_rs", ""),
            "buyer_bik": reqs.get("buyer_bik", ""),
            "buyer_ks": reqs.get("buyer_ks", ""),
            "buyer_phone": reqs.get("buyer_phone", ""),
            "buyer_email": reqs.get("buyer_email", ""),
            "buyer_representative": reqs.get("buyer_representative", ""),
            "buyer_director_name": reqs.get("buyer_director_name", ""),
            "buyer_basis": "Устава",  # по умолчанию
        }
        # Для ИП: собираем полное ФИО если в buyer_name нет фамилии
        _ip_last = reqs.get("buyer_last_name", "")
        _ip_first = reqs.get("buyer_first_name", "")
        _ip_mid = reqs.get("buyer_middle_name", "")
        if _ip_last and _ip_first and _ip_last not in contract_data["buyer_name"]:
            _full_fio = " ".join(filter(None, [_ip_last, _ip_first, _ip_mid]))
            if contract_data["buyer_name"]:
                contract_data["buyer_name"] = contract_data["buyer_name"] + " " + _full_fio
            else:
                contract_data["buyer_name"] = _full_fio

        # Проверяем чего не хватает
        # Разделяем: что спрашиваем у менеджера, что просто сообщаем как отсутствующее
        ASK_REQUIRED = {
            "buyer_representative": "должность и полное ФИО директора (напр. 'генерального директора Иванову Марию Алексеевну' — обязательно с фамилией!)",
            "buyer_basis": "основание полномочий (напр. 'Устава' или 'доверенности № 1 от 01.01.2026')",
        }
        INFO_REQUIRED = {
            "buyer_inn": "ИНН",
            "buyer_ogrn": "ОГРН",
            "buyer_address": "юридический адрес",
            "buyer_rs": "расчётный счёт (р/с)",
            "buyer_bik": "БИК банка",
            "buyer_bank": "название банка",
            "buyer_ks": "корреспондентский счёт (к/с)",
        }

        missing_ask = [(k, v) for k, v in ASK_REQUIRED.items() if not contract_data.get(k)]
        missing_info = [(k, v) for k, v in INFO_REQUIRED.items() if not contract_data.get(k)]

        if missing_ask or missing_info:
            _pending_contracts[user.id] = {
                "data": contract_data,
                "missing_keys": [m[0] for m in missing_ask],
                "missing_labels": [m[1] for m in missing_ask],
                "missing_idx": 0,
            }
            try:
                import json as _j
                db._execute("INSERT INTO pending_contracts (user_id, data) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET data=%s,created_at=NOW()", (user.id, _j.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str), _j.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str)))
            except Exception: pass
            found_info = []
            if contract_data["buyer_inn"]: found_info.append(f"ИНН: {contract_data['buyer_inn']}")
            if contract_data["buyer_ogrn"]: found_info.append(f"ОГРН: {contract_data['buyer_ogrn']}")
            if contract_data["buyer_bank"]: found_info.append(f"Банк: {contract_data['buyer_bank']}")
            found_str = " · ".join(found_info) if found_info else "реквизиты не найдены"

            msg = f"📄 *{contract_data['buyer_name']}*\n_{found_str}_\n\n"

            if missing_info:
                names = "\n".join(f"   • {v}" for _, v in missing_info)
                await message.reply_text(
                    f"📄 *{contract_data['buyer_name']}*\n\n"
                    f"❌ *Договор не сформирован* — в МойСклад отсутствуют:\n{names}\n\n"
                    f"Внеси данные в карточку МойСклад и запроси договор повторно.",
                    parse_mode="Markdown"
                )
                return

            if missing_ask:
                msg += f"*{missing_ask[0][1]}*?"
                await message.reply_text(msg, parse_mode="Markdown")
                return

        # Все данные есть — генерируем сразу
        await message.reply_text("✅ Все реквизиты найдены. Генерирую договор...")
        await _create_and_send_contract(contract_data, user.full_name, message, context)

    elif action == "manager_activity":
        days = int(params.get("days", 7))
        manager_filter = params.get("manager", "")
        if manager_filter.lower() in ("все", "all", ""):
            manager_filter = None

        # Маппинг имён → фамилий для поиска в БД
        NAME_MAP = {
            "инесса": "скляр", "скляр": "скляр", "скляр инесса ионасовна": "скляр",
            "карина": "баласанян", "баласанян": "баласанян", "баласанян карина владимировна": "баласанян",
                        "ирина": "дьяченко", "дьяченко": "дьяченко",
            "елена": "мерзлякова", "лена": "мерзлякова", "мерзлякова": "мерзлякова", "мерзлякова елена владимировна": "мерзлякова",
        }
        if manager_filter:
            manager_filter = NAME_MAP.get(manager_filter.lower(), manager_filter.split()[0].lower() if manager_filter else manager_filter)

        await message.reply_chat_action("typing")
        rows = db.get_manager_activity(days=days, manager_name=manager_filter)

        if not rows:
            await message.reply_text(f"😕 Нет данных за последние {days} дней.")
            return

        from moysklad import get_manager_stats_ms, MANAGER_TAGS

        lines = [f"📊 *Активность менеджеров за {days} дней*\n"]
        for r in rows:
            mgr = r["manager"]
            msg_count = r.get("msg_count", 0)
            msg_clients = r.get("msg_clients", 0)
            call_count = r.get("call_count", 0)
            call_clients = r.get("call_clients", 0)
            avg_dur = r.get("avg_duration", 0)
            avg_str = f" · ср. {avg_dur//60}:{avg_dur%60:02d}" if avg_dur else ""

            # Уникальные клиенты по всем каналам
            total_unique = len(set(list(range(msg_clients)) + list(range(msg_clients, msg_clients + call_clients))))
            # Упрощённо: берём максимум из двух (точнее не посчитать без JOIN)
            total_unique = max(msg_clients, call_clients)

            # Ищем тег менеджера для запроса в МойСклад
            ms_tag = None
            for tag_key, tag_name in MANAGER_TAGS.items():
                if tag_key.lower() in mgr.lower() or mgr.lower() in tag_name.lower():
                    ms_tag = tag_key
                    break

            lines.append(f"👤 *{mgr}*")
            if msg_count:
                lines.append(f"  💬 Сообщений: {msg_count} ({msg_clients} клиентов)")
            if call_count:
                lines.append(f"  📞 Звонков: {call_count} ({call_clients} клиентов){avg_str}")
            lines.append(f"  🤝 Всего контактов за период: {total_unique}")

            # Данные из МойСклад
            if ms_tag:
                try:
                    ms_stats = await get_manager_stats_ms(ms_tag)
                    lines.append(f"  📋 База МойСклад: {ms_stats['total']} компаний")
                    lines.append(f"  🔥 Активных (60 дн): {ms_stats['active']} компаний")
                except Exception:
                    pass
            lines.append("")

        await message.reply_text("\n".join(lines), parse_mode="Markdown")

    elif action == "op_report":
        await cmd_op_report(update, context)
        return

    elif action == "aging":
        await cmd_aging(update, context)
        return

    elif action == "search_mentions":
        product = params.get("product", "")
        days = int(params.get("days", 7))
        manager_filter = params.get("manager", "")

        if not product:
            await message.reply_text("❌ Укажи товар для поиска.")
            return

        await message.reply_chat_action("typing")

        # Разбиваем на несколько товаров если через запятую
        # Добавляем словоформы — обрезаем до основы (первые 5+ символов)
        raw_keywords = [p.strip().lower() for p in product.replace(" и ", ",").split(",") if p.strip()]
        keywords = []
        for kw in raw_keywords:
            keywords.append(kw)
            # Добавляем основу слова если длиннее 5 символов
            if len(kw) > 5:
                keywords.append(kw[:5])
        keywords = list(set(keywords))

        rows = db.search_wazzup_mentions(keywords, days=days, manager_name=manager_filter or None)
        call_rows = db.search_call_mentions(keywords, days=days, manager_name=manager_filter or None)

        if not rows and not call_rows:
            await message.reply_text(
                f"😕 Упоминаний *{product}* за последние {days} дней не найдено.\n"
                f"_Данные накапливаются с момента подключения._",
                parse_mode="Markdown"
            )
            return

        lines = [f"🔍 *Упоминания «{product}»* за {days} дней\n"]

        # Переписки
        if rows:
            by_manager = {}
            unknown = []
            for row in rows:
                mgr = row.get("manager_name") or "Неизвестно"
                if mgr == "Неизвестно":
                    unknown.append(row)
                else:
                    by_manager.setdefault(mgr, []).append(row)

            lines.append("💬 *Переписки:*")
            for mgr, msgs in sorted(by_manager.items()):
                clients = list({r.get("client_name") or r.get("contact_name", "") for r in msgs if r.get("client_name") or r.get("contact_name")})
                lines.append(f"👤 *{mgr}* — {len(msgs)} сообщений, {len(clients)} клиентов:")
                for c in clients[:10]:
                    lines.append(f"  • {c}")
                if len(clients) > 10:
                    lines.append(f"  _...и ещё {len(clients)-10}_")
                lines.append("")

            if unknown:
                clients_unk = list({r.get("client_name") or r.get("contact_name", "") for r in unknown if r.get("client_name") or r.get("contact_name")})
                lines.append(f"❓ *Не идентифицированы* — {len(unknown)} сообщений, {len(clients_unk)} контактов:")
                for c in clients_unk[:10]:
                    lines.append(f"  • {c}")
                if len(clients_unk) > 10:
                    lines.append(f"  _...и ещё {len(clients_unk)-10}_")
                lines.append("")

        # Звонки
        if call_rows:
            by_manager_calls = {}
            for row in call_rows:
                mgr = row.get("manager_name") or "Неизвестно"
                by_manager_calls.setdefault(mgr, []).append(row)

            lines.append("📞 *Звонки:*")
            for mgr, calls in sorted(by_manager_calls.items()):
                clients = list({r.get("src_num", "") for r in calls if r.get("src_num")})
                lines.append(f"👤 *{mgr}* — {len(calls)} звонков, {len(clients)} клиентов:")
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

# ─── Канал «Мониторинг» — закупочные прайсы поставщиков ────────────────────
# План фичи: F2B второй мозг/plans/2026-05-15-tg-канал-мониторинг-цен-поставщиков.md
# Обработка vision'ом — на стороне Claude Code (скилл update-market-intel),
# бот только сохраняет сырьё в БД и медиа на persistent volume Amvera.
MARKET_INTEL_CHAT_ID = int(os.getenv("MARKET_INTEL_CHAT_ID", "-1002964644525"))
MARKET_INTEL_DIR = os.getenv("MARKET_INTEL_DIR", "/data/market-intel")


async def handle_market_intel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохраняет channel_post из канала «Мониторинг»: текст + медиа."""
    msg = update.channel_post or update.edited_channel_post
    if not msg:
        return
    # Diagnostic: всегда логируем chat_id, чтобы видеть, какие каналы вообще достигают handler'а.
    # При несовпадении с whitelist — выходим, но запись в логе остаётся.
    logger.info(
        f"market_intel: channel_post received chat_id={msg.chat_id} "
        f"chat_title={getattr(msg.chat, 'title', '?')} "
        f"msg_id={msg.message_id} expected_chat_id={MARKET_INTEL_CHAT_ID}"
    )
    if msg.chat_id != MARKET_INTEL_CHAT_ID:
        return  # whitelist по chat_id

    # Тип сообщения и file_id (если есть)
    file_id = None
    file_ext = None
    original_filename = None  # 29.05: сохраняем оригинальное имя файла из TG как контекст для Sonnet
    if msg.photo:
        file_id = msg.photo[-1].file_id  # наибольшее разрешение
        file_ext = "jpg"
        msg_type = "photo"
    elif msg.document:
        file_id = msg.document.file_id
        # выбираем расширение по mime/имени
        fname = msg.document.file_name or ""
        original_filename = fname or None
        if "." in fname:
            file_ext = fname.rsplit(".", 1)[-1].lower()[:8]
        else:
            mime = (msg.document.mime_type or "").lower()
            file_ext = "pdf" if "pdf" in mime else "bin"
        msg_type = "document"
    else:
        msg_type = "text"

    text_raw = msg.text or msg.caption or ""
    # python-telegram-bot 21+: forward_from_chat / forward_sender_name удалены,
    # вместо них единое поле forward_origin (MessageOrigin*).
    forward_from = None
    origin = getattr(msg, "forward_origin", None)
    if origin is not None:
        chat = getattr(origin, "chat", None)
        if chat is not None:
            forward_from = chat.title or chat.username or ""
        else:
            sender_user = getattr(origin, "sender_user", None)
            if sender_user is not None:
                forward_from = sender_user.full_name or sender_user.username or ""
            else:
                forward_from = getattr(origin, "sender_user_name", None) or getattr(origin, "sender_chat", None)
                if hasattr(forward_from, "title"):
                    forward_from = forward_from.title

    # Скачиваем медиа на persistent volume (если есть)
    file_path = None
    if file_id:
        try:
            ym = msg.date.strftime("%Y-%m") if msg.date else datetime.utcnow().strftime("%Y-%m")
            month_dir = os.path.join(MARKET_INTEL_DIR, ym)
            os.makedirs(month_dir, exist_ok=True)
            file_path = os.path.join(month_dir, f"{msg.message_id}.{file_ext}")
            tg_file = await context.bot.get_file(file_id)
            await tg_file.download_to_drive(file_path)
        except Exception as e:
            logger.error(f"market_intel: download failed for msg {msg.message_id}: {e}")
            file_path = None  # запись в БД всё равно создаём, но без файла

    saved_id = db.save_market_intel_message(
        tg_msg_id=msg.message_id,
        chat_id=msg.chat_id,
        posted_at=msg.date or datetime.utcnow(),
        msg_type=msg_type,
        text_raw=text_raw,
        file_path=file_path,
        file_ext=file_ext,
        forward_from=forward_from,
        original_filename=original_filename,
    )
    if saved_id:
        logger.info(
            f"market_intel: saved id={saved_id} tg_msg={msg.message_id} type={msg_type} "
            f"file={'yes' if file_path else 'no'} fwd={forward_from or '-'}"
        )
    else:
        logger.info(f"market_intel: duplicate tg_msg={msg.message_id} skipped")


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
    elif media_type == "video":
        file_id = message.video.file_id
        if not caption:
            await message.reply_text(
                "Видео сохранено в базу без подписи.\n"
                "Чтобы его можно было найти, напиши следующим сообщением название товара.",
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
    """Ищет фото в канале Контент F2B по ключевым словам."""
    content_chat_id = int(os.getenv("CONTENT_CHAT_ID", "-1001433042091"))
    query_lower = query.lower()
    results = []
    seen = set()

    # Сначала ищем по полному запросу
    photos = db.search_media(query_lower, media_type="photo")
    logger.info(f"search_photo: query='{query_lower}' total_in_db={len(photos)}")
    for p in photos:
        logger.info(f"search_photo: chat_id={p.get('chat_id')} expected={content_chat_id} caption='{(p.get('caption') or '')[:40]}'")
        if p.get("chat_id") == content_chat_id and p["file_id"] not in seen:
            results.append({"file_id": p["file_id"], "caption": p.get("caption", "")})
            seen.add(p["file_id"])

    # Если не нашли — ищем по каждому значимому слову
    if not results:
        words = [w for w in query_lower.split() if len(w) >= 4]
        for word in words:
            photos = db.search_media(word, media_type="photo")
            logger.info(f"search_photo: word='{word}' found={len(photos)}")
            for p in photos:
                if p.get("chat_id") == content_chat_id and p["file_id"] not in seen:
                    results.append({"file_id": p["file_id"], "caption": p.get("caption", "")})
                    seen.add(p["file_id"])
            if results:
                break

    logger.info(f"search_photo: returning {len(results)} for '{query}'")
    return results

async def search_and_send_photo(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str):
    """Ищет фото товара в канале Контент F2B."""
    await update.message.reply_chat_action("upload_photo")

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

    await update.message.reply_text(f"😕 Фото *{query}* не найдено в канале Контент.", parse_mode="Markdown")

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

    elif message.video:
        file_id = message.video.file_id
        db.save_media(
            file_id=file_id,
            media_type="video",
            caption=caption,
            chat_id=message.chat_id,
            uploader="Контент F2B",
            date=datetime.now().isoformat()
        )
        logger.info(f"Сохранено видео из канала Контент: '{caption}' file_id={file_id}")

    elif message.document and message.document.mime_type and message.document.mime_type.startswith("video/"):
        file_id = message.document.file_id
        db.save_media(
            file_id=file_id,
            media_type="video",
            caption=caption or message.document.file_name or "",
            chat_id=message.chat_id,
            uploader="Контент F2B",
            date=datetime.now().isoformat()
        )
        logger.info(f"Сохранено видео-документ из канала Контент: '{caption}'")

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
    _public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "f2b-production.up.railway.app")
    webhook_url = f"https://{_public_domain}/webhook/moysklad"
    api_url = "https://api.moysklad.ru/api/remap/1.2/entity/webhook"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    results = []
    async with aiohttp.ClientSession() as session:
        for action, extra in [("CREATE", {}), ("UPDATE", {"diffType": "NONE"})]:
            payload = {"url": webhook_url, "action": action, "entityType": "customerorder", **extra}
            async with session.post(api_url, headers=headers, json=payload) as resp:
                data = await resp.json()
                if resp.status in (200, 201):
                    results.append(f"✅ customerorder.{action}: id={data.get('id')}")
                else:
                    results.append(f"❌ customerorder.{action}: {data}")

        # counterparty.UPDATE — для алерта об изменении «Дней отсрочки»
        payload_cp = {"url": webhook_url, "action": "UPDATE", "entityType": "counterparty", "diffType": "NONE"}
        async with session.post(api_url, headers=headers, json=payload_cp) as resp:
            data = await resp.json()
            if resp.status in (200, 201):
                results.append(f"✅ counterparty.UPDATE: id={data.get('id')}")
            else:
                results.append(f"❌ counterparty.UPDATE: {data}")

    await update.message.reply_text("Вебхуки МойСклад:\n" + "\n".join(results))

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

async def cmd_pdz_disabled(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Заглушка для всех старых команд по дебиторке (/pdz, /pdz_results, /pdz_test,
    /pdz_evening, /дебиторка, /debtors). Новая механика — без команд, см. объявление в группе ОП."""
    if update.message:
        await update.message.reply_text(
            "Команда временно отключена. Скоро запустим новую механику работы с дебиторкой — "
            "регламент будет в группе ОП."
        )


async def cmd_pdz_token_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Диагностика: проверка какой токен МС в env + жив ли он + есть ли права."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    tok = os.getenv("MOYSKLAD_TOKEN", "")
    if not tok:
        await update.message.reply_text("❌ MOYSKLAD_TOKEN в env не задан!")
        return

    lines = [
        f"🔑 Токен в env: len={len(tok)}",
        f"   prefix={tok[:4]}  suffix={tok[-4:]}",
        "",
    ]

    import aiohttp
    from moysklad import MS_BASE, get_headers
    try:
        async with aiohttp.ClientSession() as session:
            # 1) /context/employee — проверка живого токена
            async with session.get(f"{MS_BASE}/context/employee", headers=get_headers()) as r:
                lines.append(f"/context/employee → {r.status}")
                if r.status == 200:
                    data = await r.json()
                    lines.append(f"   me: {data.get('name', '?')} ({data.get('email', '?')})")

            # 2) /report/counterparty — берём один agent_id и тестируем
            async with session.get(f"{MS_BASE}/entity/customerorder?limit=1", headers=get_headers()) as r:
                if r.status == 200:
                    d = await r.json()
                    if d.get("rows"):
                        href = d["rows"][0]["agent"]["meta"]["href"]
                        aid = href.split("/")[-1]
                        async with session.get(f"{MS_BASE}/report/counterparty/{aid}", headers=get_headers()) as r2:
                            lines.append(f"/report/counterparty/{aid[:8]} → {r2.status}")
                            if r2.status == 200:
                                rd = await r2.json()
                                lines.append(f"   balance: {rd.get('balance')}")
                            else:
                                body = await r2.text()
                                lines.append(f"   body: {body[:200]}")
    except Exception as e:
        lines.append(f"❌ {type(e).__name__}: {e}")

    await update.message.reply_text("\n".join(lines))


async def cmd_pdz_snapshot_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Диагностика Фазы 2 ПДЗ-автоматики: руками запустить снимок и вернуть
    в ЛС количество записей + 3 примера строк. Доступ — только собственник."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    await update.message.reply_text("⏳ Тяну снимок customerorder из МойСклад...")
    try:
        from moysklad import pdz_take_snapshot
        rows = await pdz_take_snapshot()
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка снимка: {e}")
        return

    try:
        inserted = db.save_pdz_snapshot(rows)
    except Exception as e:
        await update.message.reply_text(
            f"⚠️ Снимок получен ({len(rows)} строк), но запись в БД упала: {e}"
        )
        return

    lines = [f"📸 *Снимок ПДЗ*", f"Получено: {len(rows)} · Записано: {inserted}", ""]
    if rows:
        lines.append("*Примеры (первые 3):*")
        for r in rows[:3]:
            bal_raw = r.get("agent_balance")
            bal_str = "—" if bal_raw is None else f"{bal_raw}"
            lines.append(
                f"• `{r.get('order_name','?')}` · {r.get('agent_name','?')} · "
                f"тег={r.get('manager_tag') or '—'} · "
                f"исх={r.get('ppm_initial')} · нов={r.get('ppm_new') or '—'} · "
                f"reason={r.get('reason_id') or '—'} · "
                f"{r.get('payed_sum')}/{r.get('total_sum')} · "
                f"balance={bal_str}"
            )
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_pdz_events_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Диагностика Фазы 3 ПДЗ-автоматики: руками запускает pdz_process_events_job.
    Отвечает короткой сводкой по событиям и аудиту. Доступ — только собственник."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    await update.message.reply_text("⏳ Запускаю обработку событий обещаний...")
    try:
        from scheduler import pdz_process_events_job
        result = await pdz_process_events_job(context.application, db)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    if result is None:
        await update.message.reply_text("⚠️ pdz_process_events_job вернула None")
        return

    err = result.get("error")
    if err:
        await update.message.reply_text(f"⚠️ Job упала: {err}")
        return

    text = (
        f"✅ События: {result.get('events_total', 0)} "
        f"(set:{result.get('set', 0)}, moved:{result.get('moved', 0)}, broken:{result.get('broken', 0)}). "
        f"Аудит ppm_initial: {result.get('initial_changes', 0)} алертов"
    )
    await update.message.reply_text(text)


async def cmd_pdz_html(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/pdz_html` — ручной запуск регенерации HTML-отчёта «Дебиторка» (Фаза 5).

    Только собственник (OWNER_CHAT_ID). Запускает pdz_generate_html_job —
    тот рендерит HTML из БД (snapshot + promise_log, без МС API), кладёт в
    кэш и шлёт собственнику ссылку с токеном TTL 24ч. Команда отвечает
    краткой сводкой результата."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        if update.message:
            await update.message.reply_text("Нет доступа")
        return
    if not update.message:
        return

    await update.message.reply_text("⏳ Регенерирую HTML-отчёт «Дебиторка»...")
    try:
        from scheduler import pdz_generate_html_job
        result = await pdz_generate_html_job(context.application, db)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    status = (result or {}).get("status")
    if status == "ok":
        url = result.get("url") or "—"
        size_kb = round(int(result.get("html_size") or 0) / 1024, 1)
        await update.message.reply_text(
            f"✅ Отчёт обновлён ({size_kb} KB)\n{url}",
            disable_web_page_preview=True,
        )
    else:
        err = (result or {}).get("error") or "—"
        await update.message.reply_text(
            f"⚠️ Статус: {status}. Ошибка: {err}"
        )


async def cmd_pdz_overdue_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/pdz_overdue_test <тег>` — печатает первые 5 строк просрочки менеджера.
    Тег — фамилия в нижнем регистре (скляр, баласанян, мерзлякова, дьяченко, коликов).
    Доступ — только собственник."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    if not context.args:
        await update.message.reply_text(
            "Укажи тег менеджера: `/pdz_overdue_test скляр`",
            parse_mode="Markdown",
        )
        return
    tag = context.args[0].strip().lower()

    await update.message.reply_text(f"⏳ Считаю просрочки для тега «{tag}»...")
    try:
        from moysklad import pdz_overdue_for_manager, fmt_money
        items = await pdz_overdue_for_manager(tag, db=db)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    if not items:
        await update.message.reply_text(f"✅ По тегу «{tag}» просрочек нет.")
        return

    # items — список dict, сгруппированный по контрагенту (group_by_agent=True
    # по умолчанию). Показываем ВСЕХ клиентов, разбивая на несколько TG-сообщений
    # по лимиту 3500 символов (запас от лимита Telegram 4096).
    header = f"📋 *Просрочки {tag}* — клиентов: {len(items)}"
    chunks: list[list[str]] = [[header, ""]]
    current_len = len(header) + 2

    def order_word(cnt: int) -> str:
        if cnt % 10 == 1 and cnt % 100 != 11:
            return "заказ"
        if cnt % 10 in (2, 3, 4) and cnt % 100 not in (12, 13, 14):
            return "заказа"
        return "заказов"

    def breaks_word(cnt: int) -> str:
        if cnt % 10 == 1 and cnt % 100 != 11:
            return "срыв"
        if cnt % 10 in (2, 3, 4) and cnt % 100 not in (12, 13, 14):
            return "срыва"
        return "срывов"

    for it in items:
        name = (it.get("agent_name") or "—").replace("*", "").replace("_", "")
        url = it.get("ms_url_first_order") or "#"
        cnt = it.get("orders_count", 0)
        breaks = int(it.get("breaks_count", 0) or 0)
        prefix = "🔴 " if breaks > 0 else ""
        suffix = f" ({breaks} {breaks_word(breaks)} за 90д)" if breaks > 0 else ""
        line = (
            f"{prefix}[{name}]({url}) · {cnt} {order_word(cnt)} · "
            f"{it.get('max_days_overdue', 0)} дн · "
            f"{fmt_money(it.get('total_unpaid', 0))}{suffix}"
        )
        if current_len + len(line) + 1 > 3500:
            chunks.append([])
            current_len = 0
        chunks[-1].append(line)
        current_len += len(line) + 1

    for chunk in chunks:
        if not chunk:
            continue
        await update.message.reply_text(
            "\n".join(chunk),
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )


# ─── ПДЗ Фаза 4: тестовые команды ────────────────────────────────────────

# In-memory токен подтверждения для «боевой» рассылки дайджестов.
# Проставляется в cmd_pdz_send_digests_test, сбрасывается через 10 минут
# либо после успешного запуска /pdz_send_digests_test_now.
_PDZ_DIGESTS_CONFIRM_TS = {"ts": None}


async def cmd_pdz_send_digests_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Диагностика Фазы 4: подтверждает намерение разослать боевые дайджесты.
    Ничего не шлёт — только включает окно подтверждения на 10 минут.
    Реальная рассылка — /pdz_send_digests_test_now. Доступ — только собственник."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    from datetime import datetime as _dt
    _PDZ_DIGESTS_CONFIRM_TS["ts"] = _dt.utcnow()
    await update.message.reply_text(
        "⚠️ Сейчас разошлю боевые дайджесты 5 менеджерам. "
        "Подтверди — отправь ещё раз `/pdz_send_digests_test_now`",
        parse_mode="Markdown",
    )


async def cmd_pdz_send_digests_test_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Реально запускает pdz_send_digests_job — рассылает боевые дайджесты
    менеджерам в личку. Требует, чтобы перед этим был вызван
    /pdz_send_digests_test (окно подтверждения 10 минут).
    Доступ — только собственник."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    from datetime import datetime as _dt, timedelta as _td
    ts = _PDZ_DIGESTS_CONFIRM_TS.get("ts")
    if not ts or (_dt.utcnow() - ts) > _td(minutes=10):
        await update.message.reply_text(
            "❌ Нет действующего подтверждения. Сначала отправь `/pdz_send_digests_test`",
            parse_mode="Markdown",
        )
        return
    # Сбрасываем токен — повторный вызов потребует нового /pdz_send_digests_test.
    _PDZ_DIGESTS_CONFIRM_TS["ts"] = None

    await update.message.reply_text("⏳ Шлю боевые дайджесты менеджерам...")
    try:
        from scheduler import pdz_send_digests_job
        report = await pdz_send_digests_job(context.application, db)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    if not report:
        await update.message.reply_text("⚠️ pdz_send_digests_job вернула пусто")
        return

    lines = ["✅ Дайджесты разосланы. Сводка:"]
    for tag, info in report.items():
        status = info.get("status")
        if status == "ok":
            lines.append(
                f"• {tag} ({info.get('manager','?')}) — {info.get('clients',0)} клиентов, "
                f"{info.get('messages_sent',0)}/{info.get('messages_total',0)} сообщений"
            )
        elif status == "no_overdue":
            lines.append(f"• {tag} — просрочек нет")
        elif status == "no_chat_id":
            lines.append(f"• {tag} ({info.get('manager','?')}) — ⚠️ chat_id не найден ({info.get('clients',0)} клиентов)")
        elif status == "error_fetch":
            lines.append(f"• {tag} — ❌ ошибка получения: {info.get('error','?')}")
        else:
            lines.append(f"• {tag} — {status}")
    await update.message.reply_text("\n".join(lines))


async def cmd_pdz_send_owner_pending_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Диагностика Фазы 4: руками запускает pdz_send_owner_pending_job.
    Результат собственник видит у себя же (job шлёт OWNER_CHAT_ID).
    Доступ — только собственник."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    await update.message.reply_text("⏳ Считаю необработанных для пинга собственнику...")
    try:
        from scheduler import pdz_send_owner_pending_job
        result = await pdz_send_owner_pending_job(context.application, db)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    if not result:
        await update.message.reply_text("⚠️ pdz_send_owner_pending_job вернула пусто")
        return

    status = result.get("status")
    if status == "all_clear":
        await update.message.reply_text("ℹ️ Сводка: все менеджеры обработали (сообщение пошло собственнику).")
    elif status == "sent":
        await update.message.reply_text(
            f"ℹ️ Сводка: {result.get('managers',0)} менеджеров с необработанными, "
            f"{result.get('messages_sent',0)}/{result.get('messages_total',0)} сообщений отправлено."
        )
    elif status == "no_owner_chat_id":
        await update.message.reply_text("⚠️ OWNER_CHAT_ID не задан в env.")
    elif status == "error":
        await update.message.reply_text(f"❌ Job упал: {result.get('error','?')}")
    else:
        await update.message.reply_text(f"ℹ️ status={status}")


async def cmd_pdz_breaks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/pdz_breaks` — топ-30 клиентов по числу срывов обещаний за 90 дней.

    Источник — таблица `promise_log` (event_type='broken'). Если в окне нет
    зафиксированных срывов (а так будет в первые дни после старта Фазы 4) —
    выводим «📭 За 90 дней нет зафиксированных срывов». Доступ — только
    собственник (OWNER_CHAT_ID).
    """
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    try:
        rows = db.get_promise_breaks_top(limit=30, days_window=90)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    if not rows:
        await update.message.reply_text("📭 За 90 дней нет зафиксированных срывов")
        return

    def breaks_word(cnt: int) -> str:
        if cnt % 10 == 1 and cnt % 100 != 11:
            return "срыв"
        if cnt % 10 in (2, 3, 4) and cnt % 100 not in (12, 13, 14):
            return "срыва"
        return "срывов"

    # URL первого заказа клиента — берём любой свежий заказ из последнего
    # snapshot по этому agent_id (используем уже хранящийся в БД ms_url).
    try:
        latest_snap = db.get_latest_snapshot() or []
    except Exception as e:
        latest_snap = []
        logger.warning(f"cmd_pdz_breaks: get_latest_snapshot failed: {e}")
    agent_url_map: dict = {}
    for r in latest_snap:
        aid = r.get("agent_id") or ""
        if not aid or aid in agent_url_map:
            continue
        order_id = r.get("order_id") or ""
        if order_id:
            agent_url_map[aid] = f"https://online.moysklad.ru/app/#customerorder/edit?id={order_id}"

    from datetime import datetime as _dt, timezone as _tz
    now_utc = _dt.now(_tz.utc)

    header = "🔴 *Срывы обещаний за 90 дней — топ-30*"
    chunks: list[list[str]] = [[header, ""]]
    cur_len = len(header) + 2

    for r in rows:
        aid = r.get("agent_id") or ""
        name = (r.get("agent_name") or "—").replace("*", "").replace("_", "")
        cnt = int(r.get("breaks_count", 0) or 0)
        tag = r.get("manager_tag") or "—"
        url = agent_url_map.get(aid)

        last_at = r.get("last_break_at")
        days_ago_str = "?"
        if last_at is not None:
            try:
                if isinstance(last_at, _dt):
                    if last_at.tzinfo is None:
                        last_at = last_at.replace(tzinfo=_tz.utc)
                    delta_days = (now_utc - last_at).days
                    days_ago_str = str(max(0, delta_days))
            except Exception:
                days_ago_str = "?"

        client_label = f"[{name}]({url})" if url else name
        line = (
            f"{client_label} · {cnt} {breaks_word(cnt)} · "
            f"последний {days_ago_str} дн назад · менеджер: {tag}"
        )
        if cur_len + len(line) + 1 > 3500:
            chunks.append([])
            cur_len = 0
        chunks[-1].append(line)
        cur_len += len(line) + 1

    for chunk in chunks:
        if not chunk:
            continue
        await update.message.reply_text(
            "\n".join(chunk),
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )


async def cmd_snimi_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/snimi_stop <agent_id | часть_имени>` — снять стоп-флаг (Фаза 6).

    Принимает либо UUID контрагента, либо подстроку имени. Если по подстроке
    найдено несколько активных флагов — список выводится и просим уточнить.
    Доступ — только собственник (OWNER_CHAT_ID).
    """
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        if update.message:
            await update.message.reply_text("Нет доступа")
        return
    if not update.message:
        return

    if not context.args:
        await update.message.reply_text(
            "Укажи agent\\_id или часть имени:\n`/snimi_stop <id|имя>`",
            parse_mode="Markdown",
        )
        return

    arg = " ".join(context.args).strip()
    # UUID-эвристика: 4 дефиса, длина 36.
    is_uuid_like = arg.count("-") >= 4 and len(arg) >= 30 and " " not in arg

    target_agent_id = None
    target_name = None
    if is_uuid_like:
        flag = db.get_client_stop_flag(arg)
        if not flag:
            await update.message.reply_text(
                f"❌ Активного стоп-флага по `{arg}` не найдено",
                parse_mode="Markdown",
            )
            return
        target_agent_id = arg
        target_name = flag.get("agent_name") or "—"
    else:
        candidates = db.find_stop_flag_by_name(arg)
        if not candidates:
            await update.message.reply_text(
                f"❌ Активных стоп-флагов по подстроке «{arg}» не найдено"
            )
            return
        if len(candidates) > 1:
            lines = [f"⚠️ Найдено {len(candidates)} активных флагов — уточни:"]
            for c in candidates:
                lines.append(
                    f"• `{c.get('agent_id')}` — {c.get('agent_name') or '—'}"
                    f" [{c.get('status')}]"
                )
            lines.append("\nПовтори с agent\\_id или более точным именем.")
            await update.message.reply_text(
                "\n".join(lines), parse_mode="Markdown"
            )
            return
        target_agent_id = candidates[0].get("agent_id")
        target_name = candidates[0].get("agent_name") or "—"

    username = user.username or str(user.id)
    ok = db.remove_client_stop_flag(target_agent_id, removed_by=f"tg:{username}")
    if not ok:
        await update.message.reply_text(
            f"⚠️ Не удалось снять флаг по `{target_agent_id}`",
            parse_mode="Markdown",
        )
        return

    safe_name = (target_name or "—").replace("*", "").replace("_", "")
    await update.message.reply_text(
        f"✅ Стоп-флаг снят: {safe_name}",
    )


async def cmd_list_stops(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/list_stops` — список активных стоп-флагов (Фаза 6).

    Доступ — только собственник.
    """
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        if update.message:
            await update.message.reply_text("Нет доступа")
        return
    if not update.message:
        return

    try:
        flags = db.get_active_stop_flags()
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    if not flags:
        await update.message.reply_text("✅ Активных стоп-флагов нет")
        return

    def _status_label(s: str) -> str:
        if s == "stop_shipments":
            return "🚫 СТОП"
        if s == "prepayment_only":
            return "🚫 ПРЕДОПЛАТА"
        return s or "—"

    lines = [f"📋 *Активные стоп-флаги* — {len(flags)}", ""]
    for f in flags:
        name = (f.get("agent_name") or "—").replace("*", "").replace("_", "")
        aid = f.get("agent_id") or "—"
        status = _status_label(f.get("status") or "")
        reason = (f.get("reason") or "—").replace("*", "").replace("_", "")
        set_by = (f.get("set_by") or "—").replace("*", "").replace("_", "")
        set_at = f.get("set_at")
        try:
            set_at_str = set_at.strftime("%Y-%m-%d %H:%M") if set_at else "—"
        except Exception:
            set_at_str = str(set_at) if set_at else "—"
        lines.append(f"• {status} *{name}*")
        lines.append(f"   `{aid}`")
        lines.append(f"   причина: {reason}")
        lines.append(f"   поставил: {set_by} · {set_at_str}")
        lines.append("")

    text = "\n".join(lines).rstrip()
    await update.message.reply_text(
        text, parse_mode="Markdown", disable_web_page_preview=True
    )


async def cmd_test_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/test_group [название группы] — сумма продаж по группе товаров за текущий месяц по менеджерам."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return

    import aiohttp
    from datetime import date
    from moysklad import get_headers
    MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

    group_name_query = " ".join(context.args) if context.args else "ПРИВЛЕЧЕННЫЕ ТОВАРЫ"
    await update.message.reply_text(f"🔍 Ищу группу товаров {group_name_query}...")

    month_start = date.today().replace(day=1).isoformat()
    month_end   = date.today().isoformat()

    TAGS = {
        "скляр":      "Инесса",
        "мерзлякова": "Елена",
        "баласанян":  "Карина",
        "дьяченко":  "Ирина",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/productfolder",
                headers=get_headers(),
                params={"filter": f"name={group_name_query}", "limit": 10}
            ) as r:
                data = await r.json()

            folders = data.get("rows", [])
            if not folders:
                async with session.get(f"{MS_BASE}/entity/productfolder", headers=get_headers(), params={"limit": 100}) as r:
                    data = await r.json()
                all_folders = [f.get("name","") for f in data.get("rows",[])]
                await update.message.reply_text(f"Группа не найдена. Все группы:\n" + "\n".join(all_folders[:20]))
                return

            folder = folders[0]
            folder_href = folder.get("meta", {}).get("href", "")
            folder_name = folder.get("name", "")
            await update.message.reply_text(f"Группа найдена: {folder_name}\nСчитаю по менеджерам...")

            tag_to_ids = {}
            for tag in TAGS:
                ids = set()
                off = 0
                while True:
                    async with session.get(f"{MS_BASE}/entity/counterparty", headers=get_headers(),
                                           params={"filter": f"tags={tag}", "limit": 100, "offset": off}) as r:
                        d = await r.json()
                    rows = d.get("rows", [])
                    for cp in rows:
                        ids.add(cp.get("id", ""))
                    if len(rows) < 100:
                        break
                    off += 100
                tag_to_ids[tag] = ids

            mgr_sums = {tag: 0.0 for tag in TAGS}
            total_sum = 0.0
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/report/profit/bycounterparty",
                    headers=get_headers(),
                    params={"momentFrom": f"{month_start} 00:00:00", "momentTo": f"{month_end} 23:59:59",
                            "filter": f"productFolder={folder_href}", "limit": 200, "offset": offset}
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    cp_href = row.get("counterparty", {}).get("meta", {}).get("href", "")
                    cp_id = cp_href.split("/")[-1] if cp_href else ""
                    sell_sum = (row.get("sellSum", 0) or 0) / 100
                    total_sum += sell_sum
                    for tag, ids in tag_to_ids.items():
                        if cp_id in ids:
                            mgr_sums[tag] += sell_sum
                            break
                total = data.get("meta", {}).get("size", 0)
                offset += len(rows)
                if offset >= total or len(rows) < 200:
                    break

            lines = [f"📊 *{folder_name}* · март\n", f"Итого: *{total_sum:,.0f} руб.*\n"]
            for tag, short in TAGS.items():
                lines.append(f"  {short}: *{mgr_sums[tag]:,.0f}* руб.")
            await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def cmd_op_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/op_report — временная ссылка на интерактивный отчёт ОП."""
    token = db.create_report_link(mgr_filter=None, ttl_minutes=60)
    base = os.getenv("RAILWAY_PUBLIC_DOMAIN", "f2b-production.up.railway.app")
    url = f"https://{base}/report?token={token}"
    msg = update.effective_message
    if msg:
        await msg.reply_text(f"📊 Отчёт ОП (действует 1 час):\n{url}")
    # Обновляем кэш в фоне если он устарел
    if not db.get_report_cache():
        import asyncio
        asyncio.ensure_future(_refresh_report_cache())

async def cmd_test_fact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/test_fact [тег] — тест получения отгрузок по тегу за текущий месяц."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    tag = " ".join(context.args).lower() if context.args else "скляр"
    await update.message.reply_text(f"🔍 Считаю отгрузки за март по тегу {tag}...")

    import aiohttp
    from datetime import date
    from moysklad import get_headers
    MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

    month_start = date.today().replace(day=1).isoformat()
    month_end = date.today().isoformat()

    try:
        async with aiohttp.ClientSession() as session:
            cp_ids = set()
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/counterparty",
                    headers=get_headers(),
                    params={"filter": f"tags={tag}", "limit": 100, "offset": offset}
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for cp in rows:
                    cp_ids.add(cp.get("id", ""))
                if len(rows) < 100:
                    break
                offset += 100

            await update.message.reply_text(f"Контрагентов с тегом {tag}: {len(cp_ids)}")

            shipments = 0
            revenue = 0.0
            clients = set()
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"moment>={month_start} 00:00:00;moment<={month_end} 23:59:59",
                        "expand": "agent",
                        "limit": 200,
                        "offset": offset,
                    }
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    agent_href = row.get("agent", {}).get("meta", {}).get("href", "")
                    agent_id = agent_href.split("/")[-1] if agent_href else ""
                    if agent_id in cp_ids:
                        shipments += 1
                        revenue += (row.get("sum", 0) or 0) / 100
                        clients.add(agent_id)
                if len(rows) < 200:
                    break
                offset += 200

            # Имена клиентов
            client_names = []
            for cid in clients:
                async with session.get(
                    f"{MS_BASE}/entity/counterparty/{cid}",
                    headers=get_headers()
                ) as r:
                    cp = await r.json()
                    client_names.append(cp.get("name", cid))
            client_names.sort()

            await update.message.reply_text(
                f"Итог {tag} за март:\nОтгрузок: {shipments}\nВыручка: {revenue:,.0f} руб.\nКлиентов: {len(clients)}\n\n" +
                "\n".join(f"{i+1}. {n}" for i, n in enumerate(client_names))
            )
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

    import aiohttp
    from datetime import date
    from moysklad import get_headers
    MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

    month_start = date.today().replace(day=1).isoformat()
    month_end = date.today().isoformat()

    try:
        async with aiohttp.ClientSession() as session:
            # 1. Все контрагенты с тегом менеджера
            cp_ids = set()
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/counterparty",
                    headers=get_headers(),
                    params={"filter": f"tags={tag}", "limit": 100, "offset": offset}
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for cp in rows:
                    cp_ids.add(cp.get("id", ""))
                if len(rows) < 100:
                    break
                offset += 100

            await update.message.reply_text(f"Контрагентов с тегом *{tag}*: {len(cp_ids)}", parse_mode="Markdown")

            # 2. Все отгрузки за период — фильтруем по agent из cp_ids
            shipments = 0
            revenue = 0.0
            clients = set()
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"moment>={month_start} 00:00:00;moment<={month_end} 23:59:59",
                        "expand": "agent",
                        "limit": 200,
                        "offset": offset,
                    }
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    agent = row.get("agent", {})
                    agent_href = agent.get("meta", {}).get("href", "")
                    agent_id = agent_href.split("/")[-1] if agent_href else ""
                    if agent_id in cp_ids:
                        shipments += 1
                        revenue += (row.get("sum", 0) or 0) / 100
                        clients.add(agent_id)
                if len(rows) < 200:
                    break
                offset += 200

            await update.message.reply_text(
                f"📊 *Итог по тегу {tag} за март:*\n"
                f"🚚 Отгрузок: *{shipments}*\n"
                f"💰 Выручка: *{revenue:,.0f} руб.*\n"
                f"👥 Клиентов: *{len(clients)}*",
                parse_mode="Markdown"
            )
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


# Временные токены для веб-отчёта: token → {expires, user_id, mgr_filter}
_report_tokens: dict = {}


async def cmd_refresh_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/refresh_history — принудительно обновить историю менеджеров."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text("🔄 Собираю историю по месяцам, это займёт несколько минут...")
    try:
        from moysklad import get_manager_monthly_history
        TAGS = {
            "скляр": "Инесса Скляр",
            "мерзлякова": "Елена Мерзлякова",
            "баласанян": "Карина Баласанян",
            "дьяченко": "Ирина Дьяченко",
        }
        for tag, mgr_name in TAGS.items():
            hist = await get_manager_monthly_history(tag, mgr_name)
            db.set_mgr_history_cache(tag, hist)
            await update.message.reply_text(f"✅ {mgr_name}: {len(hist)} месяцев")
        # Сбрасываем кэш отчёта чтобы следующий запрос подхватил историю
        db._execute("DELETE FROM report_cache")
        await update.message.reply_text("✅ История обновлена. Запроси /web_report для нового отчёта.")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def cmd_reissue_contract(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/reissue_contract [название компании] — перегенерировать договор с теми же номером и датой."""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    allowed_ids = manager_ids + [OWNER_CHAT_ID]
    if not user or user.id not in allowed_ids:
        await update.message.reply_text("❌ У вас нет доступа к этой команде.")
        return
    if not context.args:
        rows = db._fetchall(
            "SELECT buyer_name, contract_number, created_at FROM contracts ORDER BY created_at DESC LIMIT 10"
        )
        if not rows:
            await update.message.reply_text("📭 В базе нет сохранённых договоров.")
            return
        lines = ["📄 *Последние договоры в базе:*\n"]
        for r in rows:
            dt = r["created_at"].strftime("%d.%m.%Y") if r.get("created_at") else "—"
            lines.append(f"• {r['buyer_name']} №{r['contract_number']} от {dt}")
        lines.append("\nИспользование: /reissue_contract [название компании]")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    buyer_query = " ".join(context.args)
    existing = db.find_contract_by_buyer(buyer_query)
    if not existing:
        # Пробуем частичный поиск через LIKE
        rows = db._fetchall(
            "SELECT * FROM contracts WHERE LOWER(buyer_name) LIKE LOWER(%s) ORDER BY created_at DESC LIMIT 5",
            (f"%{buyer_query}%",)
        )
        if not rows:
            await update.message.reply_text(
                f"❌ Договор с '{buyer_query}' не найден в базе.\n"
                f"Попробуй часть названия, например: /reissue_contract ПРОДУКТЫ"
            )
            return
        if len(rows) == 1:
            existing = rows[0]
        else:
            lines = ["🔍 Найдено несколько договоров — уточни название:\n"]
            for r in rows:
                lines.append(f"• {r['buyer_name']} (№{r['contract_number']})")
            await update.message.reply_text("\n".join(lines))
            return

    contract_number = existing["contract_number"]
    created_at = existing["created_at"]
    buyer_data = existing.get("buyer_data") or {}

    await update.message.reply_text(
        f"📄 Найден договор №{contract_number} от {created_at.strftime('%d.%m.%Y')}\n"
        f"🏢 {existing['buyer_name']}\n\n"
        f"Перегенерирую PDF с теми же номером и датой..."
    )

    try:
        from contract_generator import generate_contract_pdf
        import io

        # Восстанавливаем данные из БД
        if isinstance(buyer_data, str):
            import json
            buyer_data = json.loads(buyer_data)

        MONTHS_RU = ["января","февраля","марта","апреля","мая","июня",
                     "июля","августа","сентября","октября","ноября","декабря"]
        buyer_data["contract_number"] = contract_number
        buyer_data["contract_date"] = f"{created_at.day} {MONTHS_RU[created_at.month-1]} {created_at.year} г."

        pdf_bytes = generate_contract_pdf(buyer_data)

        group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
        target = group_chat_id or update.message.chat_id
        await context.bot.send_document(
            chat_id=target,
            document=io.BytesIO(pdf_bytes),
            filename=f"Договор_{contract_number}_{existing['buyer_name'][:30]}.pdf",
            caption=(
                f"📄 *Договор поставки № {contract_number}* (переизданный)\n"
                f"📅 {buyer_data['contract_date']}\n"
                f"🏢 {existing['buyer_name']}"
            ),
            parse_mode="Markdown"
        )
        if target != update.message.chat_id:
            await update.message.reply_text(f"✅ Договор № {contract_number} переиздан и отправлен в группу.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_refresh_contract(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/refresh_contract [название компании] — обновить реквизиты из МойСклад и переиздать договор с тем же номером и датой."""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    allowed_ids = manager_ids + [OWNER_CHAT_ID]
    if not user or user.id not in allowed_ids:
        await update.message.reply_text("❌ У вас нет доступа к этой команде.")
        return
    if not context.args:
        await update.message.reply_text(
            "Использование: /refresh_contract [название компании]\n"
            "Команда заново запросит реквизиты из МойСклад и переиздаст договор с сохранением номера и даты."
        )
        return

    buyer_query = " ".join(context.args)
    existing = db.find_contract_by_buyer(buyer_query)
    if not existing:
        await update.message.reply_text(f"❌ Договор с '{buyer_query}' не найден в базе.")
        return

    contract_number = existing["contract_number"]
    created_at = existing["created_at"]

    await update.message.reply_text(
        f"🔍 Найден договор №{contract_number} от {created_at.strftime('%d.%m.%Y')}\n"
        f"🏢 {existing['buyer_name']}\n\n"
        f"Запрашиваю актуальные реквизиты из МойСклад..."
    )

    try:
        from moysklad import get_counterparty_requisites, get_counterparty_balance
        # Используем get_counterparty_balance для поиска контрагента по имени
        _found_list = await get_counterparty_balance(buyer_query)
        if not _found_list:
            await update.message.reply_text(f"❌ Компания '{buyer_query}' не найдена в МойСклад.")
            return
        cp_id = _found_list[0]["id"]
        reqs = await get_counterparty_requisites(cp_id)

        MONTHS_RU_RC = ["января","февраля","марта","апреля","мая","июня",
                        "июля","августа","сентября","октября","ноября","декабря"]

        # Старые данные из БД — используем как fallback для полей которых нет в МойСклад
        old_data = existing.get("buyer_data") or {}
        if isinstance(old_data, str):
            import json as _j2
            old_data = _j2.loads(old_data)

        contract_data = {
            "buyer_name": reqs.get("buyer_legal_title") or reqs.get("buyer_name", buyer_query),
            "buyer_legal_title": reqs.get("buyer_legal_title") or reqs.get("buyer_name", buyer_query),
            "buyer_inn": reqs.get("buyer_inn", "") or old_data.get("buyer_inn", ""),
            "buyer_ogrn": reqs.get("buyer_ogrn", "") or old_data.get("buyer_ogrn", ""),
            "buyer_address": reqs.get("buyer_address", "") or old_data.get("buyer_address", ""),
            "buyer_bank": reqs.get("buyer_bank", "") or old_data.get("buyer_bank", ""),
            "buyer_rs": reqs.get("buyer_rs", "") or old_data.get("buyer_rs", ""),
            "buyer_bik": reqs.get("buyer_bik", "") or old_data.get("buyer_bik", ""),
            "buyer_ks": reqs.get("buyer_ks", "") or old_data.get("buyer_ks", ""),
            "buyer_phone": reqs.get("buyer_phone", "") or old_data.get("buyer_phone", ""),
            "buyer_email": reqs.get("buyer_email", "") or old_data.get("buyer_email", ""),
            # ФИО директора — ТОЛЬКО из МойСклад, не из старых данных
            # (старые данные могут содержать неполное ФИО без фамилии)
            "buyer_representative": reqs.get("buyer_representative", ""),
            "buyer_director_name": reqs.get("buyer_director_name", ""),
            "buyer_basis": reqs.get("buyer_basis", "") or old_data.get("buyer_basis", "Устава"),
            # Сохраняем ОРИГИНАЛЬНЫЕ номер и дату
            "contract_number": contract_number,
            "contract_date": f"{created_at.day} {MONTHS_RU_RC[created_at.month-1]} {created_at.year} г.",
        }

        # Для ИП: собираем полное ФИО если в buyer_name нет фамилии
        _ip_last = reqs.get("buyer_last_name", "")
        _ip_first = reqs.get("buyer_first_name", "")
        _ip_mid = reqs.get("buyer_middle_name", "")
        if _ip_last and _ip_first and _ip_last not in contract_data["buyer_name"]:
            _full_fio = " ".join(filter(None, [_ip_last, _ip_first, _ip_mid]))
            contract_data["buyer_name"] = (contract_data["buyer_name"] + " " + _full_fio).strip() if contract_data["buyer_name"] else _full_fio

        # Если ФИО директора не нашли или нет фамилии — спрашиваем вручную
        _director_ok = (
            contract_data.get("buyer_director_name") and
            len(contract_data["buyer_director_name"].split()) >= 2 and
            contract_data.get("buyer_representative")
        )
        if not _director_ok:
            # Сначала очищаем любую старую запись
            _pending_contracts.pop(user.id, None)
            db._execute("DELETE FROM pending_contracts WHERE user_id=%s", (user.id,))

            _pending_contracts[user.id] = {
                "data": contract_data,
                "missing_keys": ["buyer_representative"],
                "missing_labels": ["должность и полное ФИО директора (напр. 'генерального директора Иванову Марию Алексеевну' — обязательно с фамилией!)"],
                "missing_idx": 0,
                "mode": "refresh",
            }
            import json as _j_rc
            db._execute(
                "INSERT INTO pending_contracts (user_id, data) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET data=%s,created_at=NOW()",
                (user.id, _j_rc.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str),
                 _j_rc.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str))
            )
            await update.message.reply_text(
                f"📄 *{contract_data['buyer_name']}*\n\n"
                f"В МойСклад не заполнено ФИО директора.\n\n"
                f"*Напиши должность и полное ФИО директора*\n"
                f"_(напр. 'генерального директора Иванову Марию Алексеевну')_",
                parse_mode="Markdown"
            )
            return

        from contract_generator import generate_contract_pdf
        import io as _io_rc

        pdf_bytes = generate_contract_pdf(contract_data)

        # Обновляем данные в БД
        import json as _j3
        db.save_contract(contract_number, contract_data["buyer_name"], user.full_name, buyer_data=contract_data)

        group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
        target = group_chat_id or update.message.chat_id
        await context.bot.send_document(
            chat_id=target,
            document=_io_rc.BytesIO(pdf_bytes),
            filename=f"Договор_{contract_number}_{contract_data['buyer_name'][:30]}.pdf",
            caption=(
                f"📄 *Договор поставки № {contract_number}* (переиздан, данные обновлены)\n"
                f"📅 {contract_data['contract_date']}\n"
                f"🏢 {contract_data['buyer_name']}\n"
                f"👤 {user.full_name}"
            ),
            parse_mode="Markdown"
        )
        if target != update.message.chat_id:
            await update.message.reply_text(f"✅ Договор № {contract_number} переиздан с актуальными данными и отправлен в группу.")
    except Exception as e:
        logger.error(f"cmd_refresh_contract: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_refresh_cache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/refresh_cache — принудительно обновить кэш отчёта ОП."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text("🔄 Собираю данные из МойСклад... (3-5 мин)")
    try:
        # Сначала удаляем старый кэш
        db._execute("DELETE FROM report_cache")
        # Собираем синхронно — ждём результата
        data = await _build_report_data()
        db.set_report_cache(data)
        from datetime import datetime
        ts = datetime.now().strftime("%d.%m %H:%M")
        await update.message.reply_text(f"✅ Кэш обновлён · {ts}")
    except Exception as e:
        logger.error(f"cmd_refresh_cache: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_web_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/web_report — временная ссылка на интерактивный отчёт ОП (1 час)."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    token = db.create_report_link(mgr_filter=None, ttl_minutes=60)
    base = os.getenv("RAILWAY_PUBLIC_DOMAIN", "f2b-production.up.railway.app")
    url = f"https://{base}/report?token={token}"
    await update.effective_message.reply_text(
        f"📊 Отчёт ОП (ссылка действует 1 час):\n{url}"
    )


async def cmd_lost_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/lost_clients — клиенты которые грузились в прошлом месяце но не грузились в этом."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return

    await update.message.reply_text("🔍 Ищу выбывших клиентов...")

    import aiohttp
    from datetime import date
    from moysklad import get_headers, MS_BASE

    today = date.today()
    # Текущий месяц
    month_start = today.replace(day=1).isoformat()
    # Прошлый месяц
    if today.month == 1:
        prev_month_start = date(today.year - 1, 12, 1).isoformat()
        prev_month_end = date(today.year, 1, 1).isoformat()
    else:
        prev_month_start = date(today.year, today.month - 1, 1).isoformat()
        prev_month_end = month_start

    MANAGERS = {
        "скляр":      "Инесса",
        "мерзлякова": "Елена",
        "баласанян":  "Карина",
        "дьяченко":  "Ирина",
    }

    try:
        async with aiohttp.ClientSession() as session:
            # Контрагенты каждого менеджера
            tag_to_ids = {}
            for tag in MANAGERS:
                ids = set()
                off = 0
                while True:
                    async with session.get(
                        f"{MS_BASE}/entity/counterparty",
                        headers=get_headers(),
                        params={"filter": f"tags={tag}", "limit": 100, "offset": off}
                    ) as r:
                        data = await r.json()
                    rows = data.get("rows", [])
                    for cp in rows:
                        ids.add(cp.get("id", ""))
                    if len(rows) < 100:
                        break
                    off += 100
                tag_to_ids[tag] = ids

            all_mgr_ids = set().union(*tag_to_ids.values())

            # Клиенты у кого были отгрузки в ПРОШЛОМ месяце
            prev_clients = {}  # agent_id → href
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"moment>={prev_month_start} 00:00:00;moment<{prev_month_end} 00:00:00",
                        "expand": "agent",
                        "limit": 200,
                        "offset": offset,
                    }
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    agent_href = row.get("agent", {}).get("meta", {}).get("href", "")
                    agent_id = agent_href.split("/")[-1] if agent_href else ""
                    if agent_id and agent_id in all_mgr_ids:
                        prev_clients[agent_id] = agent_href
                if len(rows) < 200:
                    break
                offset += 200

            # Клиенты у кого были отгрузки в ТЕКУЩЕМ месяце
            curr_clients = set()
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"moment>={month_start} 00:00:00;moment<={today.isoformat()} 23:59:59",
                        "expand": "agent",
                        "limit": 200,
                        "offset": offset,
                    }
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    agent_href = row.get("agent", {}).get("meta", {}).get("href", "")
                    agent_id = agent_href.split("/")[-1] if agent_href else ""
                    if agent_id:
                        curr_clients.add(agent_id)
                if len(rows) < 200:
                    break
                offset += 200

            # Выбывшие = были в прошлом месяце но нет в текущем
            lost_ids = {aid: href for aid, href in prev_clients.items() if aid not in curr_clients}

            if not lost_ids:
                await update.message.reply_text("✅ Выбывших клиентов нет.")
                return

            # Получаем имена и группируем по менеджеру
            by_mgr = {}
            for agent_id, agent_href in lost_ids.items():
                mgr = "Без менеджера"
                for tag, short in MANAGERS.items():
                    if agent_id in tag_to_ids.get(tag, set()):
                        mgr = short
                        break
                async with session.get(agent_href, headers=get_headers()) as r:
                    cp = await r.json()
                name = cp.get("name", agent_id)
                by_mgr.setdefault(mgr, []).append(name)

            total = sum(len(v) for v in by_mgr.values())
            prev_label = date(today.year, today.month - 1 if today.month > 1 else 12, 1).strftime("%B")
            lines = [f"📉 *Выбывшие клиенты* (грузились в {prev_label}, нет в марте) — {total} чел.\n"]
            for mgr, names in sorted(by_mgr.items()):
                lines.append(f"*{mgr}* ({len(names)}):")
                for n in sorted(names):
                    lines.append(f"  • {n}")
                lines.append("")

            await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def cmd_new_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/new_clients — список новых клиентов текущего месяца."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return

    await update.message.reply_text("🔍 Ищу новых клиентов за текущий месяц...")

    import aiohttp
    from datetime import date
    from moysklad import get_headers, MS_BASE

    today = date.today()
    month_start = today.replace(day=1).isoformat()

    MANAGERS = {
        "скляр":      "Инесса",
        "мерзлякова": "Елена",
        "баласанян":  "Карина",
        "дьяченко":  "Ирина",
    }

    try:
        async with aiohttp.ClientSession() as session:
            # Собираем контрагентов каждого менеджера
            tag_to_ids = {}
            for tag in MANAGERS:
                ids = set()
                off = 0
                while True:
                    async with session.get(
                        f"{MS_BASE}/entity/counterparty",
                        headers=get_headers(),
                        params={"filter": f"tags={tag}", "limit": 100, "offset": off}
                    ) as r:
                        data = await r.json()
                    rows = data.get("rows", [])
                    for cp in rows:
                        ids.add(cp.get("id", ""))
                    if len(rows) < 100:
                        break
                    off += 100
                tag_to_ids[tag] = ids

            # Клиенты у кого были отгрузки в текущем месяце
            month_clients = {}  # agent_id → agent_href
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"moment>={month_start} 00:00:00;moment<={today.isoformat()} 23:59:59",
                        "expand": "agent",
                        "limit": 200,
                        "offset": offset,
                    }
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    agent_href = row.get("agent", {}).get("meta", {}).get("href", "")
                    agent_id = agent_href.split("/")[-1] if agent_href else ""
                    if agent_id:
                        month_clients[agent_id] = agent_href
                if len(rows) < 200:
                    break
                offset += 200

            # Проверяем у кого не было отгрузок до месяца
            by_mgr = {}
            for agent_id, agent_href in month_clients.items():
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"agent={agent_href};moment<{month_start} 00:00:00",
                        "limit": 1,
                    }
                ) as r:
                    prev = await r.json()
                if prev.get("meta", {}).get("size", 0) == 0:
                    # Новый клиент — определяем менеджера
                    mgr = "Без менеджера"
                    for tag, short in MANAGERS.items():
                        if agent_id in tag_to_ids.get(tag, set()):
                            mgr = short
                            break
                    # Получаем имя
                    async with session.get(agent_href, headers=get_headers()) as r2:
                        cp = await r2.json()
                    name = cp.get("name", agent_id)
                    by_mgr.setdefault(mgr, []).append(name)

            if not by_mgr:
                await update.message.reply_text("Новых клиентов не найдено.")
                return

            total = sum(len(v) for v in by_mgr.values())
            lines = [f"🆕 *Новые клиенты за {today.strftime('%B')}* — {total} чел.\n"]
            for mgr, names in sorted(by_mgr.items()):
                lines.append(f"*{mgr}* ({len(names)}):")
                for n in sorted(names):
                    lines.append(f"  • {n}")
                lines.append("")

            await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def cmd_test_publink(update, context):
    """/test_publink — создаём публичную ссылку на PDF заказа."""
    import aiohttp
    from moysklad import get_headers, MS_BASE
    from telegram.ext import ContextTypes
    from telegram import Update

    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return

    order_id = context.args[0] if context.args else "ff0588e7-2766-11f1-0a80-01a1000aa0eb"
    await update.message.reply_text("🔍 Создаю публичную ссылку на PDF заказа...")

    try:
        async with aiohttp.ClientSession() as session:
            tmpl_href = f"{MS_BASE}/entity/customerorder/metadata/embeddedtemplate/ff0ad2ff-1883-4bc2-ba2f-6fe91686fc1b"

            async with session.post(
                f"{MS_BASE}/entity/customerorder/{order_id}/publications",
                headers=get_headers(),
                json={"template": {"meta": {
                    "href": tmpl_href,
                    "type": "embeddedtemplate",
                    "mediaType": "application/json"
                }}}
            ) as r:
                status = r.status
                body = await r.json() if r.status in (200, 201) else await r.text()

            await update.message.reply_text(f"Статус: {status}\n{str(body)[:500]}")

            if status in (200, 201) and isinstance(body, dict):
                pub_href = body.get("href", "")
                await update.message.reply_text(f"Публичная ссылка:\n{pub_href}")

    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def cmd_unlink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/unlink [chat_id или имя контакта] — удаляет контакт из базы идентификации."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /unlink [chat_id]\nПример: /unlink 1293122998")
        return

    query_val = " ".join(context.args)

    # Ищем по chat_id, contact_name, wazzup_name, company_name
    rows = db._fetchall(
        """SELECT chat_id, wazzup_name, company_name 
           FROM wazzup_contact_map 
           WHERE chat_id::text = %s 
              OR LOWER(wazzup_name) LIKE LOWER(%s)
              OR LOWER(company_name) LIKE LOWER(%s)""",
        (query_val, f"%{query_val}%", f"%{query_val}%")
    )
    if not rows:
        # Показываем последние 5 записей для диагностики
        recent = db._fetchall(
            "SELECT chat_id, wazzup_name, company_name FROM wazzup_contact_map ORDER BY created_at DESC LIMIT 5"
        )
        hint = "\n".join([f"• {r['wazzup_name']} / {r['company_name']} ({r['chat_id']})" for r in recent]) if recent else "—"
        await update.message.reply_text(
            f"❌ Контакт не найден: {query_val}\n\n"
            f"Последние 5 записей в базе:\n{hint}"
        )
        return

    for r in rows:
        db._execute("DELETE FROM wazzup_contact_map WHERE chat_id=%s", (r["chat_id"],))
        await update.message.reply_text(
            f"✅ Удалён: {r['wazzup_name']} / {r['company_name']} (chat_id: {r['chat_id']})"
        )

async def cmd_relink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/relink [chat_id] [новое имя компании] — перепривязывает контакт к другой компании."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if len(context.args) < 2:
        await update.message.reply_text(
            "Использование: /relink [chat_id] [название компании]\n"
            "Пример: /relink 1293122998 ИП Махнев"
        )
        return

    chat_id = context.args[0]
    company_name = " ".join(context.args[1:])

    # Обновляем company_name в wazzup_contact_map
    db._execute(
        "UPDATE wazzup_contact_map SET company_name=%s WHERE chat_id=%s",
        (company_name, chat_id)
    )
    rows = db._fetchall(
        "SELECT chat_id, contact_name, company_name, manager FROM wazzup_contact_map WHERE chat_id=%s",
        (chat_id,)
    )
    if rows:
        r = rows[0]
        await update.message.reply_text(
            f"✅ Контакт перепривязан:\n"
            f"chat_id: {r['chat_id']}\n"
            f"Контакт: {r['wazzup_name']}\n"
            f"Компания: {r['company_name']}\n"
            f"Менеджер: {r['manager']}"
        )
    else:
        await update.message.reply_text(f"❌ Контакт с chat_id={chat_id} не найден")

async def cmd_sync_managers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/sync_managers — обновляет менеджеров в базе по тегам МойСклад."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return

    await update.message.reply_text("🔄 Синхронизирую менеджеров...")
    updated = await sync_contact_managers()
    await update.message.reply_text(f"✅ Обновлено контактов: {updated}")

async def sync_contact_managers() -> int:
    """Обновляет поле manager в wazzup_contact_map по тегам из МойСклад."""
    import aiohttp
    from moysklad import get_headers, MS_BASE

    TAG_TO_NAME = {
        "скляр":      "Инесса Скляр",
        "мерзлякова": "Елена Мерзлякова",
        "баласанян":  "Карина Баласанян",
        "дьяченко":   "Ирина Дьяченко",
    }

    updated = 0
    try:
        # Берём все контакты из нашей БД
        contacts = db.get_all_contacts_with_company()
        if not contacts:
            return 0

        async with aiohttp.ClientSession() as session:
            for contact in contacts:
                company_name = contact.get("company_name", "")
                if not company_name:
                    continue
                # Ищем в МойСклад
                async with session.get(
                    f"{MS_BASE}/entity/counterparty",
                    headers=get_headers(),
                    params={"search": company_name, "limit": 1}
                ) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
                rows = data.get("rows", [])
                if not rows:
                    continue
                tags = [t.lower() for t in rows[0].get("tags", [])]
                mgr = next((TAG_TO_NAME[t] for t in tags if t in TAG_TO_NAME), None)
                if mgr and mgr != contact.get("manager"):
                    db.update_contact_manager(company_name, mgr)
                    updated += 1
                    logger.info(f"sync_managers: {company_name} → {mgr}")

    except Exception as e:
        logger.error(f"sync_contact_managers: {e}", exc_info=True)

    return updated

async def cmd_search_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/search_msg [слово] — ищет сообщения в БД без ограничения по дате."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    query = " ".join(context.args).lower() if context.args else ""
    if not query:
        await update.message.reply_text("Использование: /search_msg [слово]")
        return

    rows = db._fetchall(
        """SELECT manager_name, contact_name, text, sent_at, is_outbound
           FROM wazzup_messages
           WHERE LOWER(text) LIKE %s
           ORDER BY sent_at DESC LIMIT 10""",
        (f"%{query}%",)
    )
    if not rows:
        await update.message.reply_text(f"В БД нет сообщений со словом '{query}'")
        return

    lines = [f"Найдено {len(rows)} сообщений со словом '{query}':\n"]
    for r in rows:
        direction = "→" if r["is_outbound"] else "←"
        lines.append(f"{direction} {r['manager_name'] or '?'} / {r['wazzup_name']} [{r['sent_at']}]\n  {r['text'][:80]}")
    await update.message.reply_text("\n".join(lines))

async def cmd_aging(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/aging — показать список стареющих клиентов (40+ дней без отгрузок)."""
    msg = update.effective_message
    if not msg:
        return

    await msg.reply_text("🔍 Ищу стареющих клиентов...")

    from moysklad import get_aging_clients
    MANAGER_TAG_MAP = {
        "баласанян": "Карина", "мерзлякова": "Елена",
        "скляр": "Инесса", "дьяченко": "Ирина",
    }

    try:
        clients = await get_aging_clients(days=40)
        if not clients:
            await msg.reply_text("✅ Стареющих клиентов нет.")
            return

        # Группируем по менеджеру
        by_mgr = {}
        for c in clients:
            mgr = "Без менеджера"
            for tag in c.get("tags", []):
                if tag.lower() in MANAGER_TAG_MAP:
                    mgr = MANAGER_TAG_MAP[tag.lower()]
                    break
            by_mgr.setdefault(mgr, []).append(c)

        lines = [f"⚠️ *Стареющие клиенты (40+ дней)* — {len(clients)} чел.\n"]
        for mgr, cl_list in sorted(by_mgr.items()):
            lines.append(f"*{mgr}* ({len(cl_list)}):")
            for c in sorted(cl_list, key=lambda x: -x.get("days", 0)):
                days = c.get("days", c.get("days_ago", "?"))
                lines.append(f"  • {c['name']} — {days} дн.")
            lines.append("")

        text = "\n".join(lines)
        if len(text) > 4000:
            for i in range(0, len(text), 4000):
                await msg.reply_text(text[i:i+4000], parse_mode="Markdown")
        else:
            await msg.reply_text(text, parse_mode="Markdown")

    except Exception as e:
        await msg.reply_text(f"Ошибка: {e}")

async def cmd_block_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/block [user_id] — заблокировать пользователя."""
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /block [user_id]")
        return
    try:
        uid = int(context.args[0])
        db.block_user(uid)
        await update.message.reply_text(f"🔒 Пользователь {uid} заблокирован.")
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")

async def cmd_unblock_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/unblock [user_id] — разблокировать пользователя."""
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /unblock [user_id]")
        return
    try:
        uid = int(context.args[0])
        db.unblock_user(uid)
        await update.message.reply_text(f"🔓 Пользователь {uid} разблокирован.")
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/stats — статистика использования бота."""
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
        return
    stats = db.get_usage_stats()
    if not stats:
        await update.message.reply_text("📭 Статистики пока нет.")
        return
    lines = ["📊 *Статистика использования бота*\n"]
    for s in stats:
        blocked = " 🔒" if s.get("is_blocked") else ""
        last = s.get("last_seen")
        last_str = last.strftime("%d.%m %H:%M") if last else "—"
        lines.append(
            f"👤 *{s.get('full_name','?')}*{blocked}\n"
            f"   ID: `{s.get('user_id')}` · Запросов: {s.get('request_count',0)} · "
            f"Был: {last_str}"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

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
        order_id_val = parts[1] if len(parts) > 1 else ""
        alert_id = int(parts[2]) if len(parts) > 2 else 0
        alert_data = db.get_price_alert(alert_id) if alert_id else {}
        mgr_name = alert_data.get("manager_name", "") if alert_data else manager_name

        # Ищем chat_id менеджера
        mgr_chat_id = None
        if mgr_name:
            # Пробуем найти по фамилии
            for part in mgr_name.split():
                cid = db.get_manager_chat_id(part)
                if cid:
                    mgr_chat_id = cid
                    break

        alert_text = query.message.text

        if mgr_chat_id:
            await context.bot.send_message(
                chat_id=mgr_chat_id,
                text=(
                    f"⚠️ *Виктор просит пояснить занижение цены:*\n\n"
                    f"{alert_text}\n\n"
                    f"Напиши пояснение — оно уйдёт Виктору."
                ),
                parse_mode="Markdown"
            )
            # Сохраняем ожидание ответа
            _pending_price_comments[mgr_chat_id] = {
                "alert_id": alert_id,
                "order_id": order_id_val,
                "mgr_name": mgr_name,
            }
            await query.edit_message_text(
                query.message.text + f"\n\n💬 *Запрошен комментарий у {mgr_name}*",
                parse_mode="Markdown"
            )
        else:
            # Нет chat_id — пишем в группу
            group_chat_id_val = int(os.getenv("GROUP_CHAT_ID", "0"))
            if group_chat_id_val:
                contact = MANAGERS_CONTACTS.get(mgr_name, f"*{mgr_name}*")
                await context.bot.send_message(
                    chat_id=group_chat_id_val,
                    text=f"{contact}, Виктор просит пояснить занижение цены по заказу.",
                    parse_mode="Markdown"
                )
            await query.edit_message_text(
                query.message.text + f"\n\n💬 *Запрошен комментарий у {mgr_name}*",
                parse_mode="Markdown"
            )

async def handle_approval_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает нажатия на кнопки объединённого алерта «На согласовании / ЗА ЛИМИТОМ».
    Callbacks: appr_ok | appr_confirm | appr_cancel | appr_comment.
    План: 2026-05-21-объединённый-алерт-на-согласование.md, Фаза 3.
    """
    query = update.callback_query
    await query.answer()
    user = query.from_user

    parts = query.data.split("|")
    action = parts[0]
    try:
        alert_id = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, IndexError):
        alert_id = 0
    if not alert_id:
        return

    # Авторизация: пользователь должен быть в APPROVERS_CHAT_IDS (или дефолтный OWNER)
    raw_approvers = os.getenv("APPROVERS_CHAT_IDS", "").strip()
    if raw_approvers:
        approvers = [int(x.strip()) for x in raw_approvers.split(",")
                     if x.strip().lstrip("-").isdigit()]
    else:
        approvers = [OWNER_CHAT_ID]
    if user.id not in approvers:
        await query.answer("⛔ Только для согласующих.", show_alert=True)
        return

    alert_data = db.get_approval_alert(alert_id)
    if not alert_data:
        await query.answer("Алерт не найден или закрыт.", show_alert=True)
        return

    order_id = alert_data["order_id"]
    colors = alert_data.get("colors_json") or {}
    if isinstance(colors, str):
        import json as _json
        try:
            colors = _json.loads(colors)
        except Exception:
            colors = {}

    MS_STATE_AGREED = "005f3651-9a9a-11f0-0a80-03a900027474"

    # --- appr_ok ---
    if action == "appr_ok":
        # Если уже закрыт — молча подтверждаем
        if alert_data.get("closed_at"):
            await query.answer("Уже согласовано ✓", show_alert=False)
            return
        # Confirmation-step убран по запросу Виктора 2026-05-25 — раз нажал
        # «✅ Согласовано», значит решение принял; цвета светофора уже в самом
        # сообщении, лишний шаг «Точно?» только тормозит.
        action = "appr_confirm"

    # --- appr_confirm ---
    if action == "appr_confirm":
        from moysklad import set_order_state, get_headers, MS_BASE
        import aiohttp

        # Idempotency: GET state перед PATCH
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{MS_BASE}/entity/customerorder/{order_id}",
                    headers=get_headers(),
                    params={"expand": "state"},
                ) as r:
                    if r.status == 200:
                        ord_data = await r.json()
                        cur_state_id = ord_data.get("state", {}).get("meta", {}).get(
                            "href", "").split("/")[-1]
                        if cur_state_id == MS_STATE_AGREED:
                            db.close_approval_alert(alert_id, closed_by=user.id)
                            await query.answer("Уже согласовано ✓", show_alert=False)
                            return
        except Exception as e:
            logger.warning(f"appr_confirm idempotency check: {e}")

        ok = await set_order_state(order_id, MS_STATE_AGREED)
        if ok:
            from datetime import datetime, timezone, timedelta
            now_msk = datetime.now(timezone(timedelta(hours=3)))
            db.close_approval_alert(alert_id, closed_by=user.id)
            try:
                await query.edit_message_text(
                    query.message.text + f"\n\n✅ Согласовал: {user.full_name} в {now_msk.strftime('%H:%M')}",
                    parse_mode="Markdown",
                )
            except Exception:
                # Если это confirmation-сообщение (без кнопок ниже) — просто отвечаем
                await query.message.reply_text("✅ Согласовано")
        else:
            await query.answer("❌ Ошибка смены статуса в МС", show_alert=True)
        return

    # --- appr_cancel ---
    if action == "appr_cancel":
        try:
            await query.message.delete()
        except Exception:
            pass
        return

    # --- appr_comment ---
    if action == "appr_comment":
        if alert_data.get("closed_at"):
            await query.answer("Алерт уже закрыт.", show_alert=True)
            return
        _pending_approver_input[user.id] = {
            "alert_id": alert_id,
            "order_name": alert_data.get("order_name", ""),
            "client_name": alert_data.get("client_name", ""),
            "manager_name": alert_data.get("manager_name", ""),
            "manager_user_id": alert_data.get("manager_user_id", 0),
        }
        mgr_name = alert_data.get("manager_name", "?")
        await query.message.reply_text(
            f"💬 Напишите вопрос/инструкцию менеджеру *{mgr_name}* одним сообщением. "
            f"Уйдёт ему в личку.",
            parse_mode="Markdown",
        )
        return


async def _refresh_report_cache():
    """Фоновое обновление кэша отчёта."""
    try:
        logger.info("_refresh_report_cache: начинаю сбор данных...")
        data = await _build_report_data()
        db.set_report_cache(data)
        logger.info("_refresh_report_cache: кэш обновлён")
    except Exception as e:
        logger.error(f"_refresh_report_cache: {e}", exc_info=True)


async def _build_report_data() -> dict:
    """Собирает все данные для отчёта ОП из МойСклад."""
    from datetime import date
    from moysklad import get_manager_shipments, get_attracted_goods_by_manager, get_lost_clients_by_manager, get_headers, MS_BASE
    import aiohttp

    today = date.today()
    month_start = today.replace(day=1).isoformat()
    month_end = today.isoformat()

    facts = await get_manager_shipments(month_start, month_end)
    attracted = await get_attracted_goods_by_manager(month_start, month_end)
    lost = await get_lost_clients_by_manager(month_start, month_end)

    for mgr_name in facts:
        facts[mgr_name]["attracted"] = attracted.get(mgr_name, 0.0)
        facts[mgr_name]["lost_clients"] = lost.get(mgr_name, 0)

    TAGS = {"скляр":"Инесса Скляр","мерзлякова":"Елена Мерзлякова","баласанян":"Карина Баласанян","дьяченко":"Ирина Дьяченко","коликов":"Денис Коликов"}

    # История по месяцам (кэшируется раз в месяц)
    from moysklad import get_manager_monthly_history
    mgr_history = {}
    for tag, mgr_name in TAGS.items():
        cached_hist = db.get_mgr_history_cache(tag)
        if cached_hist is not None:
            mgr_history[mgr_name] = cached_hist
            logger.info(f"_build_report_data: история {mgr_name} из кэша")
        else:
            logger.info(f"_build_report_data: считаю историю {mgr_name}...")
            hist = await get_manager_monthly_history(tag, mgr_name)
            db.set_mgr_history_cache(tag, hist)
            mgr_history[mgr_name] = hist

    tag_to_ids = {}
    async with aiohttp.ClientSession() as session:
        for tag, mgr in TAGS.items():
            ids = set()
            off = 0
            while True:
                async with session.get(f"{MS_BASE}/entity/counterparty", headers=get_headers(), params={"filter":f"tags={tag}","limit":100,"offset":off}) as r:
                    d = await r.json()
                rows = d.get("rows",[])
                for cp in rows: ids.add(cp.get("id",""))
                if len(rows)<100: break
                off+=100
            tag_to_ids[mgr] = ids

        curr_ids = {}
        off = 0
        while True:
            async with session.get(f"{MS_BASE}/entity/demand", headers=get_headers(), params={"filter":f"moment>={month_start} 00:00:00;moment<={month_end} 23:59:59","expand":"agent","limit":200,"offset":off}) as r:
                d = await r.json()
            rows = d.get("rows",[])
            for row in rows:
                href = row.get("agent",{}).get("meta",{}).get("href","")
                aid = href.split("/")[-1] if href else ""
                if aid: curr_ids[aid] = href
            if len(rows)<200: break
            off+=200

        # «Новые» клиенты — есть demand в окне (aid in curr_ids), нет demand до month_start.
        # КРИТИЧНО: МС API при limit=1 нестабильно возвращает meta.size — иногда 0
        # даже когда demand реально есть (ловили на Инессе: 12 из 13 «новых» оказались
        # старыми клиентами с 8–116 demand до месяца). Надёжно — len(rows)+retry.
        new_client_names = {}
        for mgr, ids in tag_to_ids.items():
            for aid in ids:
                if aid not in curr_ids: continue
                has_before = False
                for attempt in range(3):
                    async with session.get(f"{MS_BASE}/entity/demand", headers=get_headers(), params={"filter":f"agent={MS_BASE}/entity/counterparty/{aid};moment<{month_start} 00:00:00","limit":1}) as r:
                        prev = await r.json()
                    if prev.get("rows"):
                        has_before = True
                        break
                    if attempt < 2:
                        await asyncio.sleep(0.4 * (attempt + 1))
                if has_before:
                    continue
                # Получаем имя контрагента (тоже с retry — ответ counterparty
                # иногда приходит без name, тогда тултип показывает UUID).
                name = None
                for attempt in range(3):
                    async with session.get(f"{MS_BASE}/entity/counterparty/{aid}", headers=get_headers()) as r2:
                        cp = await r2.json()
                    name = cp.get("name")
                    if name:
                        break
                    if attempt < 2:
                        await asyncio.sleep(0.4 * (attempt + 1))
                new_client_names.setdefault(mgr,[]).append(name or f"?({aid[:8]}…)")

        if today.month==1:
            prev_start = f"{today.year-1}-12-01"
        else:
            prev_start = f"{today.year}-{today.month-1:02d}-01"
        prev_ids = set()
        all_mgr_ids = set().union(*tag_to_ids.values())
        off = 0
        while True:
            async with session.get(f"{MS_BASE}/entity/demand", headers=get_headers(), params={"filter":f"moment>={prev_start} 00:00:00;moment<{month_start} 00:00:00","expand":"agent","limit":200,"offset":off}) as r:
                d = await r.json()
            rows = d.get("rows",[])
            for row in rows:
                href = row.get("agent",{}).get("meta",{}).get("href","")
                aid = href.split("/")[-1] if href else ""
                if aid and aid in all_mgr_ids: prev_ids.add(aid)
            if len(rows)<200: break
            off+=200

        EXCLUDED_STATUSES = {"закрылся", "переименован"}
        lost_client_names = {}
        for mgr, ids in tag_to_ids.items():
            for aid in (ids & prev_ids - set(curr_ids.keys())):
                async with session.get(f"{MS_BASE}/entity/counterparty/{aid}", headers=get_headers()) as r:
                    cp = await r.json()
                # Исключаем закрытых и переименованных
                cp_status = (cp.get("state") or {}).get("name", "").lower().strip()
                if cp_status in EXCLUDED_STATUSES:
                    logger.info(f"lost_clients: исключён {cp.get('name')} — статус '{cp_status}'")
                    continue
                # Проверяем что клиент всё ещё принадлежит этому менеджеру по тегу
                # Если тег сменился — клиент не выбывший, просто перешёл к другому менеджеру
                cp_tags = [t.lower() for t in cp.get("tags", [])]
                mgr_tag = next((t for t, m in TAGS.items() if m == mgr), None)
                if mgr_tag and mgr_tag not in cp_tags:
                    logger.info(f"Skipping lost client {cp.get('name')} for {mgr} — tag changed to {cp_tags}")
                    continue
                lost_client_names.setdefault(mgr, []).append(cp.get("name", aid))

    MONTHLY_PLANS = {
        "2026-05": {
            "Инесса Скляр":     {"shipments": 210, "revenue": 22_000_000, "clients": 28, "new_clients": 5, "attracted": 1_000_000},
            "Карина Баласанян": {"shipments": 180, "revenue": 6_000_000,  "clients": 44, "new_clients": 5, "attracted": 1_100_000},
            "Елена Мерзлякова": {"shipments": 67,  "revenue": 6_500_000,  "clients": 23, "new_clients": 5, "attracted": 300_000},
            "Ирина Дьяченко":   {"shipments": 4,   "revenue": 500_000,    "clients": 2,  "new_clients": 1, "attracted": 12_500},
            "Денис Коликов":    {"shipments": 15,  "revenue": 1_000_000,  "clients": 12, "new_clients": 5, "attracted": 50_000},
        },
        # 2026-06 — из БД-оверрайдов (revenue/shipments/clients), new/attracted перенесены с мая
        "2026-06": {
            "Инесса Скляр":     {"shipments": 170, "revenue": 18_000_000, "clients": 28, "new_clients": 5, "attracted": 1_000_000},
            "Карина Баласанян": {"shipments": 170, "revenue": 6_000_000,  "clients": 44, "new_clients": 5, "attracted": 1_100_000},
            "Елена Мерзлякова": {"shipments": 55,  "revenue": 5_000_000,  "clients": 20, "new_clients": 5, "attracted": 300_000},
            "Ирина Дьяченко":   {"shipments": 10,  "revenue": 1_000_000,  "clients": 5,  "new_clients": 1, "attracted": 12_500},
            "Денис Коликов":    {"shipments": 30,  "revenue": 2_500_000,  "clients": 15, "new_clients": 5, "attracted": 50_000},
        },
        # 2026-07..12 — собственник продиктовал ТОЛЬКО выручку (сессия 2026-06-29).
        # Прочие метрики несены флэтом с июня (shipments/clients) и мая (new/attracted).
        "2026-07": {
            "Инесса Скляр":     {"shipments": 170, "revenue": 19_000_000, "clients": 28, "new_clients": 5, "attracted": 1_000_000},
            "Карина Баласанян": {"shipments": 170, "revenue": 5_000_000,  "clients": 44, "new_clients": 5, "attracted": 1_100_000},
            "Елена Мерзлякова": {"shipments": 55,  "revenue": 5_000_000,  "clients": 20, "new_clients": 5, "attracted": 300_000},
            "Ирина Дьяченко":   {"shipments": 10,  "revenue": 1_500_000,  "clients": 5,  "new_clients": 1, "attracted": 12_500},
            "Денис Коликов":    {"shipments": 30,  "revenue": 4_800_000,  "clients": 15, "new_clients": 5, "attracted": 50_000},
        },
        "2026-08": {
            "Инесса Скляр":     {"shipments": 170, "revenue": 20_000_000, "clients": 28, "new_clients": 5, "attracted": 1_000_000},
            "Карина Баласанян": {"shipments": 170, "revenue": 5_500_000,  "clients": 44, "new_clients": 5, "attracted": 1_100_000},
            "Елена Мерзлякова": {"shipments": 55,  "revenue": 6_000_000,  "clients": 20, "new_clients": 5, "attracted": 300_000},
            "Ирина Дьяченко":   {"shipments": 10,  "revenue": 2_700_000,  "clients": 5,  "new_clients": 1, "attracted": 12_500},
            "Денис Коликов":    {"shipments": 30,  "revenue": 7_000_000,  "clients": 15, "new_clients": 5, "attracted": 50_000},
        },
        "2026-09": {
            "Инесса Скляр":     {"shipments": 170, "revenue": 21_000_000, "clients": 28, "new_clients": 5, "attracted": 1_000_000},
            "Карина Баласанян": {"shipments": 170, "revenue": 6_000_000,  "clients": 44, "new_clients": 5, "attracted": 1_100_000},
            "Елена Мерзлякова": {"shipments": 55,  "revenue": 6_700_000,  "clients": 20, "new_clients": 5, "attracted": 300_000},
            "Ирина Дьяченко":   {"shipments": 10,  "revenue": 3_500_000,  "clients": 5,  "new_clients": 1, "attracted": 12_500},
            "Денис Коликов":    {"shipments": 30,  "revenue": 10_000_000, "clients": 15, "new_clients": 5, "attracted": 50_000},
        },
        "2026-10": {
            "Инесса Скляр":     {"shipments": 170, "revenue": 22_000_000, "clients": 28, "new_clients": 5, "attracted": 1_000_000},
            "Карина Баласанян": {"shipments": 170, "revenue": 6_500_000,  "clients": 44, "new_clients": 5, "attracted": 1_100_000},
            "Елена Мерзлякова": {"shipments": 55,  "revenue": 8_000_000,  "clients": 20, "new_clients": 5, "attracted": 300_000},
            "Ирина Дьяченко":   {"shipments": 10,  "revenue": 4_500_000,  "clients": 5,  "new_clients": 1, "attracted": 12_500},
            "Денис Коликов":    {"shipments": 30,  "revenue": 11_000_000, "clients": 15, "new_clients": 5, "attracted": 50_000},
        },
        "2026-11": {
            "Инесса Скляр":     {"shipments": 170, "revenue": 23_000_000, "clients": 28, "new_clients": 5, "attracted": 1_000_000},
            "Карина Баласанян": {"shipments": 170, "revenue": 7_000_000,  "clients": 44, "new_clients": 5, "attracted": 1_100_000},
            "Елена Мерзлякова": {"shipments": 55,  "revenue": 10_000_000, "clients": 20, "new_clients": 5, "attracted": 300_000},
            "Ирина Дьяченко":   {"shipments": 10,  "revenue": 5_500_000,  "clients": 5,  "new_clients": 1, "attracted": 12_500},
            "Денис Коликов":    {"shipments": 30,  "revenue": 12_000_000, "clients": 15, "new_clients": 5, "attracted": 50_000},
        },
        "2026-12": {
            "Инесса Скляр":     {"shipments": 170, "revenue": 24_000_000, "clients": 28, "new_clients": 5, "attracted": 1_000_000},
            "Карина Баласанян": {"shipments": 170, "revenue": 7_500_000,  "clients": 44, "new_clients": 5, "attracted": 1_100_000},
            "Елена Мерзлякова": {"shipments": 55,  "revenue": 12_000_000, "clients": 20, "new_clients": 5, "attracted": 300_000},
            "Ирина Дьяченко":   {"shipments": 10,  "revenue": 6_500_000,  "clients": 5,  "new_clients": 1, "attracted": 12_500},
            "Денис Коликов":    {"shipments": 30,  "revenue": 13_000_000, "clients": 15, "new_clients": 5, "attracted": 50_000},
        },
    }
    current_month_key = today.strftime("%Y-%m")
    if current_month_key in MONTHLY_PLANS:
        PLANS = {mgr: dict(v) for mgr, v in MONTHLY_PLANS[current_month_key].items()}
    else:
        latest_month = max(MONTHLY_PLANS.keys())
        logger.warning(f"План на {current_month_key} не задан в MONTHLY_PLANS, использую {latest_month} как fallback (override из БД ниже)")
        PLANS = {mgr: dict(v) for mgr, v in MONTHLY_PLANS[latest_month].items()}
    # Перекрытие из БД: monthly_target_{period}_{mgr}_{metric} (set_monthly / set_monthly_bulk)
    for mgr_name in list(PLANS.keys()):
        for metric in ("revenue", "shipments", "clients", "new_clients", "attracted"):
            try:
                row = db._fetchone(
                    "SELECT value FROM bot_settings WHERE key=%s",
                    (f"monthly_target_{current_month_key}_{mgr_name}_{metric}",)
                )
                if row:
                    PLANS[mgr_name][metric] = float(row["value"])
            except Exception:
                pass
    WEEKLY_PLANS = {
        "Инесса Скляр":     {"shipments": 25,  "revenue": 2_000_000,  "clients": 10, "new_clients": 1, "attracted": 250_000},
        "Карина Баласанян": {"shipments": 40,  "revenue": 1_200_000,  "clients": 16, "new_clients": 1, "attracted": 275_000},
        "Елена Мерзлякова": {"shipments": 10,  "revenue": 1_000_000,  "clients": 5,  "new_clients": 1, "attracted": 75_000},
        "Ирина Дьяченко":   {"shipments": 1,   "revenue": 50_000,     "clients": 1,  "new_clients": 1, "attracted": 12_500},
        "Денис Коликов":    {"shipments": 1,   "revenue": 50_000,     "clients": 1,  "new_clients": 1, "attracted": 12_500},
    }
    SHORT_NAMES = {"Инесса Скляр":"Инесса","Карина Баласанян":"Карина","Елена Мерзлякова":"Елена","Ирина Дьяченко":"Ирина","Денис Коликов":"Денис"}

    # Загружаем накопительные недельные цели из БД (set_weekly) — перекрывают WEEKLY_PLANS
    weekly_targets = {}
    METRICS_KEYS = ["revenue", "shipments", "clients", "new_clients", "attracted"]
    all_mgr_names = list(PLANS.keys())
    for mgr_name in all_mgr_names:
        targets = {}
        for metric in METRICS_KEYS:
            try:
                row = db._fetchone(
                    "SELECT value FROM bot_settings WHERE key=%s",
                    (f"weekly_target_{mgr_name}_{metric}",)
                )
                if row:
                    targets[metric] = float(row["value"])
            except Exception:
                pass
        # Fallback на WEEKLY_PLANS если в БД нет данных
        if targets:
            weekly_targets[mgr_name] = targets
        else:
            weekly_targets[mgr_name] = WEEKLY_PLANS.get(mgr_name, {})

    # Аттестация из БД
    attestation = {}
    for mgr_name in all_mgr_names:
        att = {}
        for kind in ["general", "akb"]:
            try:
                row = db._fetchone(
                    "SELECT value FROM bot_settings WHERE key=%s",
                    (f"attestation_{kind}_{mgr_name}",)
                )
                if row:
                    att[kind] = int(row["value"])
            except Exception:
                pass
        if att:
            attestation[mgr_name] = att

    # Синхронизируем счётчики с реальными списками имён
    for mgr_name in facts:
        facts[mgr_name]["new_clients"] = len(new_client_names.get(mgr_name, []))
        facts[mgr_name]["lost_clients"] = len(lost_client_names.get(mgr_name, []))

    # Ссылки на персональные дашборды мотивации (quiz-game).
    # mgr_name (Карина Баласанян) → {url, tag}. Используется в отчёте ОП
    # на вкладке менеджера как кнопка «→ Дашборд мотивации».
    # Уточнено собственником 2026-06-09.
    motivation_links = {}
    try:
        tok_rows = db._fetchall(
            "SELECT manager_tag, token FROM manager_dashboard_tokens", None
        )
        tag_to_token = {r["manager_tag"]: r["token"] for r in tok_rows}
        for tag, mgr_name in TAGS.items():
            tok = tag_to_token.get(tag)
            if tok:
                motivation_links[mgr_name] = (
                    f"https://f2b-fishki-victor03.amvera.io/manager/{tag}/dashboard?token={tok}"
                )
    except Exception as e:
        logger.warning(f"_build_report_data: motivation_links не подгружен: {e}")

    return {
        "date": today.strftime("%d.%m.%Y"),
        "facts": facts,
        "plans": PLANS,
        "weekly_plans": WEEKLY_PLANS,
        "weekly_targets": weekly_targets,
        "attestation": attestation,
        "short_names": SHORT_NAMES,
        "new_client_names": new_client_names,
        "lost_client_names": lost_client_names,
        "mgr_history": mgr_history,
        "motivation_links": motivation_links,
    }


async def cmd_reset_agreed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/reset_agreed [номер заказа] — сбросить флаг отправки уведомления."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /reset_agreed [номер заказа, например 01645]")
        return
    query_val = " ".join(context.args)
    if len(query_val) > 20 and "-" in query_val:
        db._execute("DELETE FROM agreed_notifications WHERE order_id=%s", (query_val,))
        await update.message.reply_text(f"✅ Флаг сброшен для `{query_val}`", parse_mode="Markdown")
        return
    try:
        import aiohttp
        from moysklad import get_headers, MS_BASE
        await update.message.reply_text(f"🔍 Ищу заказ №{query_val}...")
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/customerorder",
                headers=get_headers(),
                params={"filter": f"name~{query_val}", "limit": 5}
            ) as resp:
                data = await resp.json()
        rows = data.get("rows", [])
        if not rows:
            await update.message.reply_text(f"❌ Заказ №{query_val} не найден в МойСклад.")
            return
        results = []
        for row in rows:
            order_id = row["id"]
            order_name = row.get("name", "")
            agent_name = row.get("agent", {}).get("name", "")
            db._execute("DELETE FROM agreed_notifications WHERE order_id=%s", (order_id,))
            results.append(f"✅ №{order_name} — {agent_name}\n   ID: `{order_id}`")
        await update.message.reply_text(
            "Флаги сброшены:\n\n" + "\n".join(results) +
            "\n\nТеперь смени статус заказа на «Согласовано» — уведомление отправится.",
            parse_mode="Markdown"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_fishki_remind_dry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Собрать preview FISHки-reminder в таблицу fishki_reminders."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text("🔍 Собираю превью FISHки-reminder…")
    try:
        from fishki_reminder import build_preview
        result = await build_preview(db)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка превью: {e}")
        return

    ready = result["ready"]
    skipped = result["skipped"]
    header = f"📋 Превью готово: {len(ready)} к отправке, {len(skipped)} пропущено.\n"
    lines = [
        f"• {x['company']} — № {x['order_name']} ({x['days_left']} дн.) → {x['chat_type']}"
        for x in ready
    ]
    body = "\n".join(lines)
    skipped_txt = ""
    if skipped:
        skipped_txt = "\n\n⏭ Пропущены:\n" + "\n".join(
            f"• {x['company']} — {x['reason']}" for x in skipped
        )
    footer = "\n\nОдобрить → /fishki_remind_send\nОтменить/прервать → /fishki_remind_stop"
    full = header + "\n" + body + skipped_txt + footer

    for start in range(0, len(full), 3500):
        await update.message.reply_text(full[start:start + 3500])

    if ready:
        await update.message.reply_text(
            f"Пример полного сообщения ({ready[0]['company']}):\n\n{ready[0]['msg']}"
        )


async def cmd_fishki_remind_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запустить отправку preview-сообщений через Wazzup, плавно."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text("🚀 Запускаю плавную рассылку (90–120 сек/сообщение).")
    from fishki_reminder import send_burst

    owner_id = user.id
    tg_bot = context.bot

    async def progress_cb(idx, total, ok, failed):
        try:
            await tg_bot.send_message(
                owner_id, f"📤 {idx}/{total} (✅ {ok} / ❌ {failed})"
            )
        except Exception:
            pass

    try:
        result = await send_burst(db, progress_cb=progress_cb)
    except Exception as e:
        await update.message.reply_text(f"❌ Сбой рассылки: {e}")
        return

    if "error" in result:
        await update.message.reply_text(f"❌ {result['error']}")
        return
    await update.message.reply_text(
        f"🏁 Готово. Всего {result['total']} | ✅ {result['sent']} | ❌ {result['failed']}"
        + (" | прервано" if result.get("stopped") else "")
    )


async def cmd_fishki_remind_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Прервать текущую рассылку (после очередного сообщения)."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    from fishki_reminder import STOP_EVENT
    STOP_EVENT.set()
    await update.message.reply_text("🛑 Stop-флаг выставлен. Текущая рассылка завершится.")


async def cmd_notifier_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/notifier_status — заказы за сегодня которым не ушла рассылка."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text("🔍 Проверяю заказы за сегодня...")
    try:
        import aiohttp
        from moysklad import get_headers, MS_BASE
        from datetime import date
        today = date.today().isoformat()
        MS_STATE_AGREED = "005f3651-9a9a-11f0-0a80-03a900027474"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/customerorder",
                headers=get_headers(),
                params={
                    "filter": f"state={MS_BASE}/entity/customerorderstatus/{MS_STATE_AGREED};moment>={today} 00:00:00",
                    "expand": "agent,state",
                    "limit": 100,
                }
            ) as resp:
                data = await resp.json()
        rows = data.get("rows", [])
        if not rows:
            await update.message.reply_text("✅ Сегодня согласованных заказов нет.")
            return
        sent, not_sent = [], []
        for row in rows:
            order_id = row["id"]
            order_name = row.get("name", "")
            agent_name = row.get("agent", {}).get("name", "")
            notified = db._fetchone("SELECT order_id FROM agreed_notifications WHERE order_id=%s", (order_id,))
            if notified:
                sent.append(f"  ✅ №{order_name} — {agent_name}")
            else:
                contact = db._fetchone(
                    "SELECT chat_id FROM wazzup_contact_map WHERE LOWER(company_name) LIKE LOWER(%s) LIMIT 1",
                    (f"%{agent_name}%",)
                )
                reason = "нет контакта в базе" if not contact else "неизвестно"
                not_sent.append(f"  ❌ №{order_name} — {agent_name} ({reason})")
        lines = [f"📊 Рассылка за {today} — всего {len(rows)} заказов\n"]
        if not_sent:
            lines.append(f"Не получили ({len(not_sent)}):")
            lines.extend(not_sent)
            lines.append("\nЧтобы дослать: /reset_agreed [номер заказа]")
        else:
            lines.append("✅ Все получили рассылку!")
        if sent:
            lines.append(f"\nПолучили ({len(sent)}):")
            lines.extend(sent)
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        logger.error(f"cmd_notifier_status: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_set_attestation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_attestation [имя] [общая|акб] [процент] — задать аттестацию менеджера."""
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
        return
    if len(context.args) < 3:
        await update.message.reply_text(
            "Использование: /set_attestation [имя] [общая|акб] [процент]\n"
            "Пример: /set_attestation Карина общая 35"
        )
        return
    name_part = context.args[0].lower()
    kind = context.args[1].lower()
    try:
        value = int(context.args[2])
    except ValueError:
        await update.message.reply_text("❌ Процент должен быть целым числом")
        return
    NAME_MAP = {
        "инесса": "Инесса Скляр", "скляр": "Инесса Скляр",
        "карина": "Карина Баласанян", "баласанян": "Карина Баласанян",
        "елена": "Елена Мерзлякова", "лена": "Елена Мерзлякова", "мерзлякова": "Елена Мерзлякова",
        "ирина": "Ирина Дьяченко", "дьяченко": "Ирина Дьяченко",
        "денис": "Денис Коликов", "коликов": "Денис Коликов",
    }
    mgr_name = NAME_MAP.get(name_part)
    if not mgr_name:
        await update.message.reply_text(f"❌ Менеджер '{context.args[0]}' не найден.")
        return
    if kind in ("общая", "general"):
        key = f"attestation_general_{mgr_name}"
        label = "общая"
    elif kind in ("акб", "akb"):
        key = f"attestation_akb_{mgr_name}"
        label = "АКБ"
    else:
        await update.message.reply_text("❌ Тип должен быть 'общая' или 'акб'")
        return
    db._execute(
        "INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=%s",
        (key, str(value), str(value))
    )
    await update.message.reply_text(f"✅ Аттестация {label} для {mgr_name}: {value}%")


async def cmd_attestation_cta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/attestation_cta on|off - открыть/закрыть окно одноразовой аттестации для всех 5 менеджеров ОП.

    `on`: сжигает все предыдущие токены (status='expired') и выставляет флаг,
    при следующем открытии дашборда менеджер получит свежий issued-токен и CTA.
    `off`: убирает флаг + сжигает issued/opened токены (completed не трогаем — там
    результат уже зафиксирован). Сохранённая менеджером ссылка перестаёт работать."""
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
        return
    if not context.args or context.args[0].lower() not in ("on", "off"):
        await update.message.reply_text(
            "Использование: /attestation_cta on|off\n"
            "on  - открыть окно (новые токены всем 5 менеджерам, CTA появится на дашбордах)\n"
            "off - закрыть окно (CTA скроется + issued/opened токены сжигаются)"
        )
        return
    mode = context.args[0].lower()
    db._execute(
        "INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=%s",
        ("attestation_cta_open", mode, mode),
    )
    if mode == "on":
        db._execute(
            "UPDATE manager_attestations SET status='expired' "
            "WHERE status IN ('issued','opened','completed')"
        )
        await update.message.reply_text(
            "✅ Окно аттестации открыто.\n"
            "При следующем открытии дашборда у 5 менеджеров появится кнопка «Пройти аттестацию».\n"
            "Напомни им: открыл - проходи сразу до конца, закрытие вкладки = использованная попытка."
        )
    else:
        # Сжигаем issued/opened, чтобы сохранённая менеджером ссылка перестала работать.
        # completed не трогаем - результат уже зафиксирован.
        db._execute(
            "UPDATE manager_attestations SET status='expired' "
            "WHERE status IN ('issued','opened')"
        )
        await update.message.reply_text("✅ Окно аттестации закрыто. CTA скрыта у всех + неиспользованные токены сожжены.")


def get_weekly_targets(mgr_name: str) -> dict:
    """Возвращает накопительные недельные цели менеджера из БД."""
    result = {}
    for metric in ["revenue", "shipments", "clients", "new_clients", "attracted"]:
        try:
            row = db._fetchone("SELECT value FROM bot_settings WHERE key=%s", (f"weekly_target_{mgr_name}_{metric}",))
            if row:
                result[metric] = float(row["value"])
        except Exception:
            pass
    return result


# ─── Карты менеджеров и метрик для /set_weekly и /set_weekly_bulk ────────────
_MGR_NAME_MAP = {
    "инесса": "Инесса Скляр", "скляр": "Инесса Скляр",
    "карина": "Карина Баласанян", "баласанян": "Карина Баласанян",
    "елена": "Елена Мерзлякова", "лена": "Елена Мерзлякова", "мерзлякова": "Елена Мерзлякова",
    "ирина": "Ирина Дьяченко", "дьяченко": "Ирина Дьяченко",
    "денис": "Денис Коликов", "коликов": "Денис Коликов",
}
_WEEKLY_METRIC_MAP = {
    "выручка": "revenue", "выручки": "revenue", "выручку": "revenue", "revenue": "revenue",
    "отгрузки": "shipments", "отгрузка": "shipments", "отгрузок": "shipments", "отгрузке": "shipments", "shipments": "shipments",
    "акб": "clients", "клиенты": "clients", "клиент": "clients", "клиента": "clients", "клиентов": "clients", "clients": "clients",
    "новые": "new_clients", "новых": "new_clients", "новый": "new_clients", "новых_клиентов": "new_clients", "new_clients": "new_clients",
    "привл": "attracted", "привлеченные": "attracted", "привлеч": "attracted", "привлеченных": "attracted", "attracted": "attracted",
}
_WEEKLY_METRIC_LABELS = {
    "revenue": "Выручка",
    "shipments": "Отгрузки",
    "clients": "АКБ",
    "new_clients": "Новые",
    "attracted": "Привл. товары",
}


def _parse_weekly_value(s: str) -> float:
    """Парсит '12500000', '12 500 000', '12,5 млн', '860 тыс', '500к' → float."""
    s = s.strip().lower().replace('₽', '').replace('руб', '').replace('р.', '').strip()
    s = s.replace(' ', '').replace(' ', '').replace(',', '.')
    m = re.match(r'^([\d.]+)(млн|млрд|тыс|m|k|кк)?$', s)
    if not m:
        raise ValueError(f"некорректное число «{s}»")
    v = float(m.group(1))
    suffix = m.group(2)
    if suffix in ('млн', 'm', 'кк'):
        v *= 1_000_000
    elif suffix == 'млрд':
        v *= 1_000_000_000
    elif suffix in ('тыс', 'k'):
        v *= 1_000
    return v


def _parse_weekly_chunk(chunk: str) -> tuple[str, float]:
    """Парсит 'выручка 12,5 млн' либо '12,5 млн выручка' → (metric_key, value)."""
    tokens = chunk.split()
    if len(tokens) < 2:
        raise ValueError(f"ожидается «метрика значение», получено «{chunk}»")
    metric_idx = None
    for i, tok in enumerate(tokens):
        if tok.lower() in _WEEKLY_METRIC_MAP:
            metric_idx = i
            break
    if metric_idx is None:
        raise ValueError(f"метрика не найдена в «{chunk}»")
    metric_key = _WEEKLY_METRIC_MAP[tokens[metric_idx].lower()]
    value_tokens = tokens[:metric_idx] + tokens[metric_idx + 1:]
    if not value_tokens:
        raise ValueError(f"нет числа в «{chunk}»")
    return metric_key, _parse_weekly_value(' '.join(value_tokens))


def _parse_weekly_bulk(text: str) -> tuple[list, list]:
    """Разбирает многострочный ввод. Возвращает (успехи, ошибки).

    Успехи: [(mgr_name, metric_key, value), ...]
    Ошибки: [(контекст, сообщение), ...]

    Поддерживает:
    - «Имя:» или просто «Имя» в начале строки — старт блока менеджера.
    - В блоке: пары «метрика значение» через запятую, точку с запятой или « — ».
    - Числа: «12500000», «12 500 000», «12,5 млн», «860 тыс», «500к».
    - Склонения: «отгрузок», «клиентов» и т.п.
    - Пустые строки и строки с «#» в начале игнорируются.
    """
    successes: list = []
    errors: list = []
    blocks: list = []
    current_mgr = None
    current_data: list = []

    for raw_line in text.split('\n'):
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        before, sep, after = line.partition(':')
        if sep and _MGR_NAME_MAP.get(before.strip().lower()):
            if current_mgr is not None:
                blocks.append((current_mgr, ' , '.join(current_data)))
            current_mgr = _MGR_NAME_MAP[before.strip().lower()]
            current_data = [after.strip()] if after.strip() else []
            continue
        if _MGR_NAME_MAP.get(line.lower()):
            if current_mgr is not None:
                blocks.append((current_mgr, ' , '.join(current_data)))
            current_mgr = _MGR_NAME_MAP[line.lower()]
            current_data = []
            continue
        if current_mgr is None:
            errors.append((line, "ожидался «Имя:» или строка только с именем менеджера"))
            continue
        current_data.append(line)
    if current_mgr is not None:
        blocks.append((current_mgr, ' , '.join(current_data)))

    for mgr_name, data_str in blocks:
        if not data_str.strip():
            errors.append((mgr_name, "нет данных по менеджеру"))
            continue
        normalized = re.sub(r'\s+[-—–;]\s+', ' , ', data_str)
        normalized = re.sub(r'(\d),(\d)', r'\1.\2', normalized)
        for chunk in normalized.split(','):
            chunk = chunk.strip()
            if not chunk:
                continue
            try:
                metric_key, value = _parse_weekly_chunk(chunk)
                successes.append((mgr_name, metric_key, value))
            except ValueError as e:
                errors.append((f"{mgr_name}: {chunk}", str(e)))

    return successes, errors


async def cmd_set_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_weekly [имя] [показатель] [значение] — задать накопительный недельный план."""
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
        return
    if len(context.args) < 3:
        await update.message.reply_text(
            "Использование: /set_weekly [имя] [показатель] [значение]\n"
            "Показатели: выручка, отгрузки, акб, новые, привл\n"
            "Пример: /set_weekly Инесса выручка 4000000\n\n"
            "Для нескольких значений сразу — /set_weekly_bulk"
        )
        return
    name_part = context.args[0].lower()
    metric_part = context.args[1].lower()
    try:
        value = float(context.args[2].replace(',', '.'))
    except ValueError:
        await update.message.reply_text("❌ Значение должно быть числом")
        return
    mgr_name = _MGR_NAME_MAP.get(name_part)
    if not mgr_name:
        await update.message.reply_text(f"❌ Менеджер '{context.args[0]}' не найден.")
        return
    metric_key = _WEEKLY_METRIC_MAP.get(metric_part)
    if not metric_key:
        await update.message.reply_text(f"❌ Показатель '{context.args[1]}' не найден.\nДоступные: выручка, отгрузки, акб, новые, привл")
        return
    key = f"weekly_target_{mgr_name}_{metric_key}"
    db._execute(
        "INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=%s",
        (key, str(value), str(value))
    )
    label = _WEEKLY_METRIC_LABELS.get(metric_key, metric_key)
    await update.message.reply_text(f"✅ Недельный план *{label}* для *{mgr_name}*: {value:,.0f}", parse_mode="Markdown")


async def cmd_set_weekly_bulk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_weekly_bulk — задать накопительные недельные планы списком (многострочно).

    Форматы (можно смешивать):
        /set_weekly_bulk
        Инесса: выручка 12,5 млн, отгрузки 110, акб 26
        Лена: 3 млн выручка, 32 отгрузок, 20 клиентов

    или с именем на отдельной строке:
        /set_weekly_bulk
        Инесса
        12,5 млн выручка — 110 отгрузок — 26 клиентов

    Числа: «12500000», «12 500 000», «12,5 млн», «860 тыс», «500к».
    Метрики: выручка, отгрузки, акб, новые, привл (склонения распознаются).
    """
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
        return
    raw_text = update.message.text or ''
    lines = raw_text.split('\n')
    first_stripped = re.sub(r'^/\S+\s*', '', lines[0]) if lines else ''
    body = '\n'.join(([first_stripped] if first_stripped.strip() else []) + lines[1:])

    if not body.strip():
        await update.message.reply_text(
            "Использование: пришли команду + список менеджеров (одна строка на менеджера).\n\n"
            "Пример:\n"
            "/set_weekly_bulk\n"
            "Инесса: выручка 12,5 млн, отгрузки 110, акб 26\n"
            "Лена: выручка 3 млн, отгрузки 32, акб 20\n"
            "Карина: выручка 3,8 млн, отгрузки 100, акб 36\n"
            "Ирина: выручка 500 тыс, отгрузки 2, акб 2\n"
            "Денис: выручка 860 тыс, отгрузки 6, акб 6\n\n"
            "Метрики: выручка, отгрузки, акб, новые, привл.\n"
            "Числа: 12500000, 12,5 млн, 860 тыс — все ок."
        )
        return

    successes, errors = _parse_weekly_bulk(body)

    applied: list = []
    for mgr_name, metric_key, value in successes:
        try:
            db._execute(
                "INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=%s",
                (f"weekly_target_{mgr_name}_{metric_key}", str(value), str(value))
            )
            applied.append((mgr_name, metric_key, value))
        except Exception as e:
            errors.append((f"{mgr_name} {metric_key}={value}", f"DB-ошибка: {e}"))

    msg_parts: list = []
    if applied:
        from collections import OrderedDict
        by_mgr: "OrderedDict[str, list]" = OrderedDict()
        for mgr, metric, val in applied:
            by_mgr.setdefault(mgr, []).append((metric, val))
        msg_parts.append(f"✅ Обновлено целей: {len(applied)}")
        for mgr, items in by_mgr.items():
            parts = []
            for metric, val in items:
                label = _WEEKLY_METRIC_LABELS.get(metric, metric)
                num = f"{val:,.0f}".replace(',', ' ')
                parts.append(f"{label} {num}")
            msg_parts.append(f"• {mgr}: {', '.join(parts)}")
    if errors:
        msg_parts.append(f"\n❌ Ошибок: {len(errors)}")
        for ctx_str, err in errors:
            msg_parts.append(f"• {ctx_str} → {err}")
    if not msg_parts:
        msg_parts = ["Нечего обновлять."]
    await update.message.reply_text('\n'.join(msg_parts))


_PERIOD_RE = re.compile(r'^(20\d{2})-(0[1-9]|1[0-2])$')


def _parse_monthly_period(token: str) -> str | None:
    """'2026-06' → '2026-06'. Иначе None."""
    return token if _PERIOD_RE.match(token) else None


def _parse_monthly_bulk(text: str) -> tuple[str | None, list, list]:
    """Как _parse_weekly_bulk, но первая значимая строка может быть периодом YYYY-MM.

    Возвращает (period_or_None, успехи, ошибки).
    Если период не указан — period_or_None=None, вызывающий проставляет текущий месяц.
    """
    lines = text.split('\n')
    period: str | None = None
    rest_lines = lines
    for i, raw in enumerate(lines):
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        parsed = _parse_monthly_period(line)
        if parsed:
            period = parsed
            rest_lines = lines[:i] + lines[i + 1:]
        break
    successes, errors = _parse_weekly_bulk('\n'.join(rest_lines))
    return period, successes, errors


async def cmd_set_monthly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_monthly [имя] [показатель] [значение] [YYYY-MM?] — задать месячный план.

    Без 4-го аргумента — текущий месяц.
    Показатели: выручка, отгрузки, акб, новые, привл.
    """
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
        return
    if len(context.args) < 3:
        await update.message.reply_text(
            "Использование: /set_monthly [имя] [показатель] [значение] [YYYY-MM]\n"
            "Показатели: выручка, отгрузки, акб, новые, привл\n"
            "Пример: /set_monthly Карина выручка 6000000\n"
            "Пример с месяцем: /set_monthly Карина выручка 6000000 2026-07\n\n"
            "Для нескольких значений сразу — /set_monthly_bulk"
        )
        return
    name_part = context.args[0].lower()
    metric_part = context.args[1].lower()
    try:
        value = float(context.args[2].replace(',', '.'))
    except ValueError:
        await update.message.reply_text("❌ Значение должно быть числом")
        return
    period = datetime.now().strftime("%Y-%m")
    if len(context.args) >= 4:
        parsed = _parse_monthly_period(context.args[3])
        if not parsed:
            await update.message.reply_text(f"❌ Период '{context.args[3]}' не похож на YYYY-MM")
            return
        period = parsed
    mgr_name = _MGR_NAME_MAP.get(name_part)
    if not mgr_name:
        await update.message.reply_text(f"❌ Менеджер '{context.args[0]}' не найден.")
        return
    metric_key = _WEEKLY_METRIC_MAP.get(metric_part)
    if not metric_key:
        await update.message.reply_text(f"❌ Показатель '{context.args[1]}' не найден.\nДоступные: выручка, отгрузки, акб, новые, привл")
        return
    key = f"monthly_target_{period}_{mgr_name}_{metric_key}"
    db._execute(
        "INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=%s",
        (key, str(value), str(value))
    )
    label = _WEEKLY_METRIC_LABELS.get(metric_key, metric_key)
    await update.message.reply_text(f"✅ Месячный план *{label}* {period} для *{mgr_name}*: {value:,.0f}", parse_mode="Markdown")


async def cmd_set_monthly_bulk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_monthly_bulk — задать месячные планы списком (многострочно).

    Первая строка может быть периодом YYYY-MM (по умолчанию — текущий месяц).

    Пример:
        /set_monthly_bulk
        2026-06
        Карина: выручка 6 млн, отгрузки 170, акб 44
        Лена: выручка 5 млн, отгрузки 55, акб 20

    Метрики и формат чисел — как в /set_weekly_bulk.
    """
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
        return
    raw_text = update.message.text or ''
    lines = raw_text.split('\n')
    first_stripped = re.sub(r'^/\S+\s*', '', lines[0]) if lines else ''
    body = '\n'.join(([first_stripped] if first_stripped.strip() else []) + lines[1:])

    if not body.strip():
        await update.message.reply_text(
            "Использование: пришли команду + (опц.) период YYYY-MM + список менеджеров.\n\n"
            "Пример:\n"
            "/set_monthly_bulk\n"
            "2026-06\n"
            "Инесса: выручка 22 млн, отгрузки 210, акб 28\n"
            "Карина: выручка 6 млн, отгрузки 170, акб 44\n"
            "Лена: выручка 5 млн, отгрузки 55, акб 20\n"
            "Ирина: выручка 1 млн, отгрузки 10, акб 5\n"
            "Денис: выручка 2,5 млн, отгрузки 30, акб 15\n\n"
            "Без строки YYYY-MM — применится к текущему месяцу.\n"
            "Метрики: выручка, отгрузки, акб, новые, привл."
        )
        return

    period, successes, errors = _parse_monthly_bulk(body)
    if period is None:
        period = datetime.now().strftime("%Y-%m")

    applied: list = []
    for mgr_name, metric_key, value in successes:
        try:
            db._execute(
                "INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=%s",
                (f"monthly_target_{period}_{mgr_name}_{metric_key}", str(value), str(value))
            )
            applied.append((mgr_name, metric_key, value))
        except Exception as e:
            errors.append((f"{mgr_name} {metric_key}={value}", f"DB-ошибка: {e}"))

    msg_parts: list = [f"📅 Период: *{period}*"]
    if applied:
        from collections import OrderedDict
        by_mgr: "OrderedDict[str, list]" = OrderedDict()
        for mgr, metric, val in applied:
            by_mgr.setdefault(mgr, []).append((metric, val))
        msg_parts.append(f"✅ Обновлено целей: {len(applied)}")
        for mgr, items in by_mgr.items():
            parts = []
            for metric, val in items:
                label = _WEEKLY_METRIC_LABELS.get(metric, metric)
                num = f"{val:,.0f}".replace(',', ' ')
                parts.append(f"{label} {num}")
            msg_parts.append(f"• {mgr}: {', '.join(parts)}")
    if errors:
        msg_parts.append(f"\n❌ Ошибок: {len(errors)}")
        for ctx_str, err in errors:
            msg_parts.append(f"• {ctx_str} → {err}")
    if not applied and not errors:
        msg_parts.append("Нечего обновлять.")
    await update.message.reply_text('\n'.join(msg_parts), parse_mode="Markdown")


async def cmd_managers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/managers — показать всех зарегистрированных пользователей и их chat_id."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    rows = db._fetchall(
        "SELECT user_id, full_name, is_blocked FROM manager_chats ORDER BY full_name"
    )
    if not rows:
        await update.message.reply_text("❌ Пользователи не найдены.")
        return
    lines = ["👥 *Зарегистрированные пользователи:*\n"]
    for r in rows:
        blocked = " 🚫" if r.get("is_blocked") else ""
        lines.append(f"• {r['full_name']}{blocked}\n  ID: `{r['user_id']}`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_ms_attributes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/ms_attributes — показать UUID дополнительных полей контрагентов."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    import aiohttp
    from moysklad import get_headers
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://api.moysklad.ru/api/remap/1.2/entity/counterparty/metadata/attributes",
            headers=get_headers()
        ) as r:
            data = await r.json()
    rows = data.get("rows", [])
    lines = ["Дополнительные поля контрагентов:\n"]
    for row in rows:
        lines.append(f"{row.get('name')} — {row.get('id')}")
    await update.message.reply_text("\n".join(lines))


async def cmd_del_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/del_user [user_id] — полностью удалить пользователя из базы."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /del_user [user_id]\nПример: /del_user 1337598287")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ user_id должен быть числом.")
        return
    try:
        row = db._fetchone("SELECT full_name FROM manager_chats WHERE user_id=%s", (target_id,))
        if not row:
            await update.message.reply_text(f"❌ Пользователь {target_id} не найден в базе.")
            return
        name = row["full_name"]
        db._execute("DELETE FROM manager_chats WHERE user_id=%s", (target_id,))
        await update.message.reply_text(f"✅ Удалён: {name} (ID: {target_id})")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


# ============================================================================
# Постановка задач в МойСклад через /задача (whitelist: Виктор + Александр)
# ============================================================================

from zoneinfo import ZoneInfo
_MSK = ZoneInfo("Europe/Moscow")

_TASK_CONV_TIMEOUT_SEC = 600  # 10 минут

# Стадии conversation для /задача
_STAGE_WAITING_DESCRIPTION = "WAITING_DESCRIPTION"       # ждём первое сообщение с задачей
_STAGE_DRAFT = "DRAFT"                                   # показан драфт, ждём ✅/✏️/❌
_STAGE_WAITING_CLARIFICATION = "WAITING_CLARIFICATION"   # ждём уточнения исполнителя
_STAGE_WAITING_REWRITE = "WAITING_REWRITE"               # ждём новое сообщение после ✏️

# In-memory state: {chat_id: {"stage", "draft", "draft_message_id", "started_at"}}
_task_conversations: dict = {}


def _is_task_author(chat_id: int) -> bool:
    """Whitelist для /задача — только Виктор и Александр."""
    return chat_id in {OWNER_CHAT_ID, PARTNER_CHAT_ID}


def _task_actor_name(chat_id: int) -> str:
    """Имя инициатора для логов."""
    if chat_id == OWNER_CHAT_ID:
        return "Виктор"
    if chat_id == PARTNER_CHAT_ID:
        return "Александр"
    return f"chat_id={chat_id}"


def _task_conv_expired(conv: dict) -> bool:
    """True, если с момента started_at прошло больше таймаута."""
    started = conv.get("started_at")
    if not started:
        return True
    return (datetime.now(_MSK) - started).total_seconds() > _TASK_CONV_TIMEOUT_SEC


async def _task_strip_buttons(chat_id: int, message_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Снять inline-кнопки у драфт-сообщения (если ещё живо)."""
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=None,
        )
    except Exception as e:
        logger.debug(f"_task_strip_buttons: {e}")


async def _task_cancel_existing(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Закрыть активную conversation: снять кнопки, очистить state."""
    conv = _task_conversations.pop(chat_id, None)
    if conv:
        msg_id = conv.get("draft_message_id")
        if msg_id:
            await _task_strip_buttons(chat_id, msg_id, context)


async def cmd_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/задача /поставить /task — старт постановки задачи в МойСклад.

    Доступно только Виктору и Александру в личном чате с ботом.
    В группе от whitelist → подсказка «работает только в личке».
    Не-whitelist → тихое игнорирование.
    """
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        return

    # Группа + whitelist → подсказка. Группа + не-whitelist → молчим.
    if chat.type != "private":
        if _is_task_author(user.id):
            await update.message.reply_text(
                "ℹ️ Постановка задач работает только в личном чате со мной."
            )
        return

    # Личка + не-whitelist → молчим.
    if not _is_task_author(user.id):
        return

    # Если уже есть активная conversation у этого пользователя — отменяем старую.
    if user.id in _task_conversations:
        await _task_cancel_existing(user.id, context)
        await update.message.reply_text("⚠️ Предыдущая задача отменена.")

    _task_conversations[user.id] = {
        "stage": _STAGE_WAITING_DESCRIPTION,
        "draft": None,
        "draft_message_id": None,
        "started_at": datetime.now(_MSK),
    }
    logger.info(
        f"metric.task_bot.conv_started chat_id={user.id} actor={_task_actor_name(user.id)}"
    )
    await update.message.reply_text(
        "✍️ Опиши задачу одним сообщением — кому, что сделать, к какому сроку."
    )


async def handle_menu_task_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Кнопка «📋 Поставить задачу» из /menu.

    Эквивалент /задача, но стартует через callback. Доступна только whitelist.
    """
    query = update.callback_query
    user = query.from_user
    if not user:
        return
    # Гейт: только Виктор и Александр
    if not _is_task_author(user.id):
        await query.answer("⛔ Доступно только Виктору и Александру", show_alert=True)
        return
    chat = query.message.chat if query.message else None
    if not chat or chat.type != "private":
        await query.answer("ℹ️ Работает только в личном чате со мной", show_alert=True)
        return
    await query.answer()
    # Если активная conv — закрываем старую, начинаем новую
    if user.id in _task_conversations:
        await _task_cancel_existing(user.id, context)
    _task_conversations[user.id] = {
        "stage": _STAGE_WAITING_DESCRIPTION,
        "draft": None,
        "draft_message_id": None,
        "started_at": datetime.now(_MSK),
    }
    logger.info(
        f"metric.task_bot.conv_started chat_id={user.id} "
        f"actor={_task_actor_name(user.id)} via=menu"
    )
    await context.bot.send_message(
        chat_id=user.id,
        text="✍️ Опиши задачу одним сообщением — кому, что сделать, к какому сроку.",
    )


async def cmd_task_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/отмена — отменить активную постановку задачи."""
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    if not _is_task_author(user.id):
        return
    if user.id not in _task_conversations:
        # Нечего отменять — тихо игнорируем, чтобы не реагировать на /отмена вне контекста.
        return
    await _task_cancel_existing(user.id, context)
    logger.info(f"metric.task_bot.task_cancelled chat_id={user.id}")
    await update.message.reply_text(
        "❌ Отменил. Чтобы начать заново — /задача."
    )


# ─── Рендеринг драфта и клавиатур ───────────────────────────────────────────

def _render_task_draft_text(draft: dict) -> str:
    assignee_name = draft.get("assignee_name") or "—"
    description = draft.get("description") or "—"
    due_str = draft.get("due_msk")
    due_pretty = f"{due_str} МСК" if due_str else "не указан"
    return (
        "📋 Задача в МойСклад:\n"
        f"• Исполнитель: {assignee_name}\n"
        f"• Описание: {description}\n"
        f"• Дедлайн: {due_pretty}"
    )


def _task_draft_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Создать", callback_data="task_confirm"),
        InlineKeyboardButton("✏️ Переписать", callback_data="task_rewrite"),
        InlineKeyboardButton("❌ Отмена", callback_data="task_draft_cancel"),
    ]])


def _task_candidates_keyboard(candidates: list) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(c["name"], callback_data=f"task_cand:{c['id']}")]
            for c in candidates]
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="task_draft_cancel")])
    return InlineKeyboardMarkup(rows)


async def _task_send_draft(chat_id: int, context: ContextTypes.DEFAULT_TYPE, draft: dict) -> None:
    """Отправляет новое сообщение с драфтом + кнопками, обновляет state."""
    text = _render_task_draft_text(draft)
    msg = await context.bot.send_message(
        chat_id=chat_id, text=text, reply_markup=_task_draft_keyboard()
    )
    conv = _task_conversations.get(chat_id)
    if conv is not None:
        conv["stage"] = _STAGE_DRAFT
        conv["draft"] = draft
        conv["draft_message_id"] = msg.message_id
        conv["started_at"] = datetime.now(_MSK)


# ─── Парсинг и маршрутизация драфта ─────────────────────────────────────────

async def _task_parse_and_route(chat_id: int, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    """Парсит текст через Claude, валидирует id, решает: драфт / кандидаты / переспрос."""
    conv = _task_conversations.get(chat_id)
    if not conv:
        return
    try:
        employees = await list_employees()
    except Exception as e:
        logger.error(f"list_employees failed: {e}")
        logger.info(f"metric.task_bot.task_error_ms_employees chat_id={chat_id}")
        await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ МойСклад временно недоступен. /отмена и попробуй позже.",
        )
        return
    valid_ids = {e["id"] for e in employees}
    id_to_name = {e["id"]: e["name"] for e in employees}
    now_msk = datetime.now(_MSK)
    try:
        draft = await parse_task_draft(text, employees, now_msk)
    except Exception as e:
        logger.error(f"parse_task_draft failed: {e}", exc_info=True)
        logger.info(f"metric.task_bot.task_error_claude chat_id={chat_id}")
        await context.bot.send_message(
            chat_id=chat_id, text="⚠️ Не получилось распарсить, попробуй ещё раз."
        )
        return
    # Валидация assignee_id — отсекаем галлюцинации Claude
    aid = draft.get("assignee_id")
    if aid and aid not in valid_ids:
        logger.warning(f"parse_task_draft: assignee_id={aid} не в списке — отбрасываем")
        aid = None
    cands = [c for c in (draft.get("assignee_candidates") or []) if c.get("id") in valid_ids]
    desc = (draft.get("description") or "").strip()
    if len(desc) > 200:
        desc = desc[:199] + "…"

    draft = {
        "assignee_id": aid,
        "assignee_name": id_to_name.get(aid) if aid else None,
        "assignee_candidates": cands if cands else None,
        "description": desc,
        "due_msk": draft.get("due_msk"),
    }

    # Есть однозначный исполнитель → сразу драфт
    if aid:
        await _task_send_draft(chat_id, context, draft)
        return
    # 2–5 кандидатов → кнопки выбора
    if cands and 2 <= len(cands) <= 5:
        conv["stage"] = _STAGE_WAITING_CLARIFICATION
        conv["draft"] = draft
        conv["draft_message_id"] = None
        conv["started_at"] = datetime.now(_MSK)
        await context.bot.send_message(
            chat_id=chat_id,
            text="🤔 Кого из них ты имел в виду?",
            reply_markup=_task_candidates_keyboard(cands),
        )
        return
    # 0 или >5 кандидатов → переспрос фамилией
    conv["stage"] = _STAGE_WAITING_CLARIFICATION
    conv["draft"] = draft
    conv["draft_message_id"] = None
    conv["started_at"] = datetime.now(_MSK)
    await context.bot.send_message(
        chat_id=chat_id, text="🤔 Кому ставим? Напиши фамилию."
    )


# ─── MessageHandler для текста в активной conversation ──────────────────────

class _TaskConvActiveFilter(filters.MessageFilter):
    """Срабатывает только для whitelist-пользователей в личке с активной conv."""
    def filter(self, message):
        user = getattr(message, "from_user", None)
        chat = getattr(message, "chat", None)
        if not user or not chat or chat.type != "private":
            return False
        active = user.id in _task_conversations
        # Диагностика: логируем попытку для whitelist-юзеров, чтобы увидеть,
        # почему filter не совпадает, если пользователь в conv ожидает.
        if _is_task_author(user.id):
            logger.info(
                f"metric.task_bot.filter_check user_id={user.id} "
                f"active={active} conv_keys={list(_task_conversations.keys())} "
                f"text={(message.text or '')[:60]!r}"
            )
        return active


_task_conv_active_filter = _TaskConvActiveFilter()


async def handle_task_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Текстовое сообщение пользователя в активной /задача-conversation."""
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    logger.info(
        f"metric.task_bot.message_received user_id={user.id} "
        f"in_conv={user.id in _task_conversations} "
        f"stage={_task_conversations.get(user.id, {}).get('stage')}"
    )
    if user.id not in _task_conversations:
        return
    conv = _task_conversations[user.id]
    if _task_conv_expired(conv):
        msg_id = conv.get("draft_message_id")
        if msg_id:
            await _task_strip_buttons(user.id, msg_id, context)
        _task_conversations.pop(user.id, None)
        logger.info(f"metric.task_bot.task_timeout chat_id={user.id}")
        await update.message.reply_text("⏳ Сессия устарела, нажми /задача заново.")
        return

    text = (update.message.text or "").strip()
    if not text:
        return
    stage = conv["stage"]
    if stage == _STAGE_WAITING_DESCRIPTION:
        await _task_parse_and_route(user.id, context, text)
    elif stage == _STAGE_WAITING_CLARIFICATION:
        # Склеиваем старое описание с уточнением-фамилией, чтобы парсер увидел оба факта.
        old_desc = (conv.get("draft") or {}).get("description") or ""
        combined = f"{text}, {old_desc}" if old_desc else text
        await _task_parse_and_route(user.id, context, combined)
    elif stage == _STAGE_WAITING_REWRITE:
        await _task_parse_and_route(user.id, context, text)
    # DRAFT — пользователь пишет текст, не нажимая кнопку. Игнорируем (ждём кнопку или /отмена).


# ─── Callback кнопок драфта и кандидатов ────────────────────────────────────

# Идемпотентность ✅ Создать: набор draft_message_id, по которым уже создаём задачу.
_task_creating: set = set()


async def handle_task_draft_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """✅ Создать / ✏️ Переписать / ❌ Отмена."""
    query = update.callback_query
    user = query.from_user
    if not _is_task_author(user.id):
        await query.answer("Недоступно", show_alert=False)
        return
    conv = _task_conversations.get(user.id)
    if not conv or _task_conv_expired(conv):
        await query.answer("⏳ Сессия устарела, нажми /задача заново.", show_alert=True)
        if conv:
            await _task_cancel_existing(user.id, context)
        return
    data = query.data
    if data == "task_draft_cancel":
        await query.answer("Отменил")
        await _task_cancel_existing(user.id, context)
        logger.info(f"metric.task_bot.task_cancelled chat_id={user.id}")
        await context.bot.send_message(
            chat_id=user.id,
            text="❌ Отменил. Чтобы начать заново — /задача.",
        )
        return
    if data == "task_rewrite":
        await query.answer()
        await _task_strip_buttons(user.id, query.message.message_id, context)
        conv["stage"] = _STAGE_WAITING_REWRITE
        conv["draft_message_id"] = None
        conv["started_at"] = datetime.now(_MSK)
        await context.bot.send_message(
            chat_id=user.id, text="✍️ Опиши задачу заново одним сообщением."
        )
        return
    if data == "task_confirm":
        await _task_handle_confirm(update, context)
        return


async def handle_task_candidate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор кандидата исполнителя из inline-кнопок."""
    query = update.callback_query
    user = query.from_user
    if not _is_task_author(user.id):
        await query.answer("Недоступно", show_alert=False)
        return
    conv = _task_conversations.get(user.id)
    if not conv or _task_conv_expired(conv):
        await query.answer("⏳ Сессия устарела, нажми /задача заново.", show_alert=True)
        if conv:
            await _task_cancel_existing(user.id, context)
        return
    _, _, emp_id = query.data.partition(":")
    try:
        employees = await list_employees()
    except Exception as e:
        logger.error(f"list_employees failed: {e}")
        await query.answer("⚠️ МойСклад временно недоступен", show_alert=True)
        return
    id_to_name = {e["id"]: e["name"] for e in employees}
    if emp_id not in id_to_name:
        await query.answer("Сотрудник не найден, начни заново /задача", show_alert=True)
        await _task_cancel_existing(user.id, context)
        return
    draft = dict(conv.get("draft") or {})
    draft["assignee_id"] = emp_id
    draft["assignee_name"] = id_to_name[emp_id]
    draft["assignee_candidates"] = None
    await _task_strip_buttons(user.id, query.message.message_id, context)
    await query.answer()
    await _task_send_draft(user.id, context, draft)


async def _task_handle_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """✅ Создать: создаём задачу в МС с идемпотентностью и обработкой ошибок."""
    import aiohttp as _aiohttp
    query = update.callback_query
    user_id = query.from_user.id
    conv = _task_conversations.get(user_id)
    if not conv:
        await query.answer("⏳ Сессия устарела, нажми /задача заново.", show_alert=True)
        return
    msg_id = query.message.message_id

    # Идемпотентность: второй клик по той же кнопке — отвечаем «уже создаю».
    if msg_id in _task_creating:
        await query.answer("Уже создаю…")
        return
    _task_creating.add(msg_id)

    try:
        await query.answer("Создаю…")
        await _task_strip_buttons(user_id, msg_id, context)

        draft = conv.get("draft") or {}
        aid = draft.get("assignee_id")
        desc = draft.get("description") or ""
        due_str = draft.get("due_msk")
        due_dt = None
        if due_str:
            try:
                due_dt = datetime.strptime(due_str, "%Y-%m-%d %H:%M")
            except ValueError:
                logger.warning(f"task_confirm: не распарсил due_msk='{due_str}', создаю без дедлайна")

        async def _call_create():
            return await create_task(aid, desc, due_dt)

        try:
            result = await _call_create()
        except _aiohttp.ClientResponseError as e:
            status = e.status
            logger.warning(f"create_task HTTP {status}: {e}")
            logger.info(f"metric.task_bot.task_error_{status} chat_id={user_id}")
            if status == 404:
                invalidate_employees_cache()
                try:
                    result = await _call_create()
                except Exception as e2:
                    logger.warning(f"create_task retry after 404 failed: {e2}")
                    await context.bot.send_message(
                        chat_id=user_id,
                        text="⚠️ Сотрудник не найден, возможно уволен. Выбери через ✏️ Переписать или ❌ Отмена.",
                    )
                    await _task_send_draft(user_id, context, draft)
                    return
            elif status in (401, 403):
                await context.bot.send_message(
                    chat_id=user_id,
                    text="⚠️ У «Эф» недостаточно прав в МойСкладе. Сообщи Виктору.",
                )
                await _task_send_draft(user_id, context, draft)
                return
            elif status >= 500:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="⚠️ МойСклад временно недоступен. Жми ✅ ещё раз или ❌ Отмена.",
                )
                await _task_send_draft(user_id, context, draft)
                return
            else:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"⚠️ Ошибка МС ({status}). Жми ✅ ещё раз или ❌ Отмена.",
                )
                await _task_send_draft(user_id, context, draft)
                return
        except Exception as e:
            logger.exception(f"create_task unexpected error: {e}")
            logger.info(f"metric.task_bot.task_error_exception chat_id={user_id}")
            await context.bot.send_message(
                chat_id=user_id,
                text="⚠️ Не удалось создать задачу. Жми ✅ ещё раз или ❌ Отмена.",
            )
            await _task_send_draft(user_id, context, draft)
            return

        # Успех
        task_id = result["id"]
        task_url = result["url"]
        assignee_name = draft.get("assignee_name") or "—"
        due_pretty = f"{due_str} МСК" if due_str else "не указан"
        logger.info(f"metric.task_bot.task_created chat_id={user_id} task_id={task_id}")
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                "✅ Задача создана\n"
                f"Исполнитель: {assignee_name}\n"
                f"Дедлайн: {due_pretty}\n"
                f"→ [Открыть в МС]({task_url})"
            ),
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
        _task_conversations.pop(user_id, None)
    finally:
        _task_creating.discard(msg_id)


# ─── Telegram Business: приём BusinessConnection (для TG Stories Publisher) ───
async def handle_business_connection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Ловит подключение/отключение бота в Telegram Business → Chatbots.
    Сохраняет business_connection_id в bot_settings и шлёт алерт собственнику.
    План: plans/2026-05-14-tg-stories-publisher-mvp.md (Фаза 0/1).
    """
    bc = update.business_connection
    if bc is None:
        return

    rights_repr = None
    try:
        if getattr(bc, "rights", None) is not None:
            rights_repr = str(bc.rights.to_dict()) if hasattr(bc.rights, "to_dict") else str(bc.rights)
    except Exception:
        rights_repr = "<unparseable>"

    payload = {
        "id": bc.id,
        "user_chat_id": bc.user_chat_id,
        "user_id": bc.user.id if bc.user else None,
        "is_enabled": bc.is_enabled,
        "rights": rights_repr,
        "date": bc.date.isoformat() if bc.date else None,
    }
    logger.info(f"📣 BusinessConnection update: {payload}")

    try:
        db._execute(
            """INSERT INTO bot_settings (key, value) VALUES (%s, %s)
               ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""",
            ("business_connection_id_ftb_mob", bc.id if bc.is_enabled else "")
        )
        db._execute(
            """INSERT INTO bot_settings (key, value) VALUES (%s, %s)
               ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""",
            ("business_connection_payload_ftb_mob", str(payload))
        )
    except Exception as e:
        logger.error(f"Не удалось сохранить business_connection в bot_settings: {e}")

    try:
        status_emoji = "🟢" if bc.is_enabled else "🔴"
        text = (
            f"{status_emoji} Business Connection {'подключён' if bc.is_enabled else 'отключён'}\n"
            f"business_connection_id: {bc.id}\n"
            f"От user: {payload['user_id']}\n"
            f"Права: {rights_repr}"
        )
        await context.bot.send_message(chat_id=OWNER_CHAT_ID, text=text)
    except Exception as e:
        logger.error(f"Не удалось отправить алерт собственнику: {e}")


def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("Не задан TELEGRAM_BOT_TOKEN в переменных окружения!")

    app = Application.builder().token(token).build()

    # Восстанавливаем ожидающие идентификации из БД после рестарта
    try:
        pending_rows = db.get_pending_idents()
        for row in pending_rows:
            lk = row["link_key"]
            _pending_links[lk] = {
                "chat_id": row["chat_id"],
                "channel_id": row["channel_id"],
                "wazzup_name": row["wazzup_name"],
                "chat_type": row["chat_type"],
                "link_key": lk,
            }
        if pending_rows:
            logger.info(f"Восстановлено {len(pending_rows)} ожидающих идентификаций из БД")
    except Exception as e:
        logger.warning(f"Не удалось восстановить pending_idents: {e}")

    # Telegram Business: подключение бота к бизнес-аккаунту (Stories Publisher)
    app.add_handler(BusinessConnectionHandler(handle_business_connection))

    # ─── Канал «Мониторинг» — закупочные прайсы поставщиков ─────────────────
    # Регистрируем ДО любых других handler'ов на channel_post, чтобы whitelist
    # по chat_id отработал первым. В функции стоит ранний return, если чат не наш.
    app.add_handler(MessageHandler(
        filters.UpdateType.CHANNEL_POST | filters.UpdateType.EDITED_CHANNEL_POST,
        handle_market_intel_post,
    ))

    # Команды
    app.add_handler(CommandHandler("op_report", cmd_op_report))
    app.add_handler(CommandHandler("test_group", cmd_test_group))
    app.add_handler(CommandHandler("test_fact", cmd_test_fact))
    app.add_handler(CommandHandler("refresh_history", cmd_refresh_history))
    app.add_handler(CommandHandler("reissue_contract", cmd_reissue_contract))
    app.add_handler(CommandHandler("refresh_contract", cmd_refresh_contract))
    app.add_handler(CommandHandler("refresh_cache", cmd_refresh_cache))
    app.add_handler(CommandHandler("web_report", cmd_web_report))
    app.add_handler(CommandHandler("lost_clients", cmd_lost_clients))
    app.add_handler(CommandHandler("new_clients", cmd_new_clients))
    app.add_handler(CommandHandler("test_publink", cmd_test_publink))
    app.add_handler(CommandHandler("unlink", cmd_unlink))
    app.add_handler(CommandHandler("relink", cmd_relink))
    app.add_handler(CommandHandler("sync_managers", cmd_sync_managers))
    app.add_handler(CommandHandler("managers", cmd_managers))
    app.add_handler(CommandHandler("search_msg", cmd_search_msg))
    app.add_handler(CommandHandler("aging", cmd_aging))
    app.add_handler(CommandHandler("block", cmd_block_user))
    app.add_handler(CommandHandler("unblock", cmd_unblock_user))
    app.add_handler(CommandHandler("del_user", cmd_del_user))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("usermenu", cmd_user_menu))
    app.add_handler(CallbackQueryHandler(handle_user_menu_callback, pattern="^user_"))
    app.add_handler(CallbackQueryHandler(handle_request_callback, pattern="^req_"))
    app.add_handler(CommandHandler("menu", cmd_menu))
    # Кнопка «📋 Поставить задачу» в /menu — whitelist-гейт внутри; регистрируется ДО общего handle_menu_callback
    app.add_handler(CallbackQueryHandler(handle_menu_task_button, pattern=r"^menu_task$"))
    app.add_handler(CallbackQueryHandler(handle_menu_callback, pattern="^menu_"))
    app.add_handler(CommandHandler("mychatid", cmd_mychatid))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("clearwazzup", cmd_clear_wazzup))
    app.add_handler(CommandHandler("wazzup_enrich", cmd_wazzup_enrich))
    app.add_handler(CommandHandler("wazzup_export", cmd_wazzup_export))
    app.add_handler(CommandHandler("wazzup_reset", cmd_wazzup_reset))
    app.add_handler(CommandHandler("wazzup_channels", cmd_wazzup_channels))
    app.add_handler(CommandHandler("wazzup_setup", cmd_wazzup_setup))
    app.add_handler(CommandHandler("test", cmd_test))
    app.add_handler(MessageHandler(filters.StatusUpdate.MESSAGE_AUTO_DELETE_TIMER_CHANGED, handle_message))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("photo", cmd_photo))
    app.add_handler(CommandHandler("price", cmd_price))
    app.add_handler(CommandHandler("contact", cmd_contact))
    app.add_handler(CommandHandler("memory", cmd_memory))
    app.add_handler(CommandHandler("remember", cmd_remember))
    app.add_handler(CommandHandler("add_webhook", cmd_add_webhook))
    # ── Заглушки для старых ПДЗ-команд (Фаза 1: чистка старой механики) ──
    # Команды отключены, новая механика дебиторки описывается в группе ОП.
    app.add_handler(CommandHandler("pdz", cmd_pdz_disabled))
    app.add_handler(CommandHandler("pdz_results", cmd_pdz_disabled))
    app.add_handler(CommandHandler("pdz_test", cmd_pdz_disabled))
    app.add_handler(CommandHandler("pdz_evening", cmd_pdz_disabled))
    app.add_handler(CommandHandler("debtors", cmd_pdz_disabled))
    app.add_handler(CommandHandler("debts", cmd_pdz_disabled))
    app.add_handler(MessageHandler(
        filters.Regex(r"^/дебиторка(@\w+)?(\s|$)"),
        cmd_pdz_disabled,
    ))
    # Диагностика Фазы 2 ПДЗ-автоматики (только собственник).
    app.add_handler(CommandHandler("pdz_snapshot_test", cmd_pdz_snapshot_test))
    app.add_handler(CommandHandler("pdz_token_check", cmd_pdz_token_check))
    # Диагностика Фазы 3 (только собственник).
    app.add_handler(CommandHandler("pdz_events_test", cmd_pdz_events_test))
    app.add_handler(CommandHandler("pdz_overdue_test", cmd_pdz_overdue_test))
    # Диагностика Фазы 4: TG-дайджесты менеджерам + пинг собственнику.
    app.add_handler(CommandHandler("pdz_send_digests_test", cmd_pdz_send_digests_test))
    app.add_handler(CommandHandler("pdz_send_digests_test_now", cmd_pdz_send_digests_test_now))
    app.add_handler(CommandHandler("pdz_send_owner_pending_test", cmd_pdz_send_owner_pending_test))
    # Фаза 4.5: топ-30 клиентов по срывам обещаний за 90 дней (только собственник).
    app.add_handler(CommandHandler("pdz_breaks", cmd_pdz_breaks))
    # ПДЗ Фаза 6 — управление стоп-флагами (только OWNER_CHAT_ID).
    app.add_handler(CommandHandler("snimi_stop", cmd_snimi_stop))
    app.add_handler(CommandHandler("list_stops", cmd_list_stops))
    # Фаза 5: HTML-отчёт «Дебиторка» — ручной запуск регенерации (только собственник).
    app.add_handler(CommandHandler("pdz_html", cmd_pdz_html))
    app.add_handler(CommandHandler("set_attestation", cmd_set_attestation))
    app.add_handler(CommandHandler("attestation_cta", cmd_attestation_cta))
    app.add_handler(CommandHandler("set_weekly", cmd_set_weekly))
    app.add_handler(CommandHandler("set_weekly_bulk", cmd_set_weekly_bulk))
    app.add_handler(CommandHandler("set_monthly", cmd_set_monthly))
    app.add_handler(CommandHandler("set_monthly_bulk", cmd_set_monthly_bulk))
    app.add_handler(CommandHandler("reset_agreed", cmd_reset_agreed))
    app.add_handler(CommandHandler("ms_attributes", cmd_ms_attributes))
    app.add_handler(CommandHandler("notifier_status", cmd_notifier_status))
    app.add_handler(CommandHandler("fishki_remind_dry", cmd_fishki_remind_dry))
    app.add_handler(CommandHandler("fishki_remind_send", cmd_fishki_remind_send))
    app.add_handler(CommandHandler("fishki_remind_stop", cmd_fishki_remind_stop))
    # План 2026-05-20-автоподстановка-исходной-даты-оплаты, Фазы 2-3
    app.add_handler(CommandHandler("payment_planned_autofill_test", cmd_payment_planned_autofill_test))
    app.add_handler(CommandHandler("payment_planned_history", cmd_payment_planned_history))
    app.add_handler(CallbackQueryHandler(handle_contract_callback, pattern="^contract_"))
    app.add_handler(CallbackQueryHandler(handle_price_callback, pattern="^(price_|pdz_)"))
    app.add_handler(CallbackQueryHandler(handle_approval_callback, pattern="^appr_"))
    app.add_handler(CallbackQueryHandler(handle_send_callback, pattern="^send_"))
    app.add_handler(CallbackQueryHandler(handle_wazzup_link_callback, pattern="^(wazzup_link|wazzup_role|wazzup_pick|wazzup_seg|wazzup_mgr|wazzup_mailing|wazzup_later)"))
    app.add_handler(CallbackQueryHandler(handle_wazzup_ignore_callback, pattern="^wazzup_ignore"))

    # ─── Wazzup classifier — кнопки на TG-алертах запроса по номенклатуре ──
    async def handle_wzc_callback(update, context):
        q = update.callback_query
        await q.answer()
        try:
            _, action, message_id = q.data.split(":", 2)
        except ValueError:
            return
        if action == "fp":
            # Ложно-позитивный → пишем feedback для re-train
            db._execute(
                """UPDATE wazzup_classifications
                   SET feedback = 'false_positive'
                   WHERE message_id = %s""",
                (message_id,),
            )
            await q.edit_message_text(
                q.message.text + "\n\n👎 Помечено как ложный — пойдёт в re-train.",
                parse_mode=None,
            )
        elif action in ("ok", "req"):
            # «✅ В работу» = подтверждение классификатора + создание заявки
            # закупщику в одном клике. `req` — legacy-алиас для старых сообщений
            # в чате (до 2026-06-18 кнопка была отдельной). См. план
            # plans/2026-06-18-wzc-merge-ok-req.md.
            #
            # Идемпотентность: assortment_requests.status='converted' блокирует
            # повторное создание; на повторный клик показываем номер существующей.
            db._execute(
                """UPDATE wazzup_classifications
                   SET feedback = 'confirmed'
                   WHERE message_id = %s""",
                (message_id,),
            )
            try:
                ar = db._fetchone(
                    """SELECT id, chat_id, contact_name, raw_text, species_normalized,
                              sku_or_description, urgency, llm_confidence, status,
                              converted_request_id
                       FROM procurement.assortment_requests
                       WHERE wazzup_message_id = %s""",
                    (message_id,),
                )
                if not ar:
                    await q.edit_message_text(
                        q.message.text + "\n\n⚠️ Запрос не найден в sink.",
                        parse_mode=None,
                    )
                    return
                if ar["status"] == "converted":
                    rid = ar["converted_request_id"]
                    await q.edit_message_text(
                        q.message.text + f"\n\n📋 Уже создана заявка #{rid}.",
                        parse_mode=None,
                    )
                    return
                row = db._fetchone(
                    """INSERT INTO procurement.requests
                       (created_by_tg, created_by_name, raw_text,
                        species, llm_confidence, client_name, status, comment)
                       VALUES (%s,%s,%s,%s,%s,%s,'новая',%s)
                       RETURNING request_id""",
                    (
                        q.from_user.id,
                        q.from_user.full_name or "owner",
                        ar["raw_text"],
                        ar["species_normalized"],
                        ar["llm_confidence"],
                        ar["contact_name"],
                        f"Авто из Wazzup-классификатора. {ar['sku_or_description'] or ''}",
                    ),
                )
                rid = row["request_id"]
                db._execute(
                    """UPDATE procurement.assortment_requests
                       SET status='converted', converted_request_id=%s,
                           status_changed_at=NOW(), status_changed_by=%s
                       WHERE id=%s""",
                    (rid, q.from_user.full_name or "owner", ar["id"]),
                )
                db._execute(
                    """INSERT INTO procurement.request_events
                       (request_id, actor, event_type, payload)
                       VALUES (%s, %s, 'created_from_wazzup', %s::jsonb)""",
                    (
                        rid, q.from_user.full_name or "owner",
                        f'{{"assortment_request_id": {ar["id"]}, "source": "wazzup_classifier"}}',
                    ),
                )
                await q.edit_message_text(
                    q.message.text + f"\n\n✅ Заявка #{rid} создана, закупщик увидит в дашборде.",
                    parse_mode=None,
                )
            except Exception as e:
                logger.error(f"handle_wzc_callback ok: {e}", exc_info=True)
                await q.edit_message_text(
                    q.message.text + f"\n\n⚠️ Ошибка создания заявки: {type(e).__name__}",
                    parse_mode=None,
                )

    app.add_handler(CallbackQueryHandler(handle_wzc_callback, pattern="^wzc:"))

    # ─── DashaMail weekly: «Запланировать» из cron-уведомления ──────────────
    async def handle_dashamail_callback(update, context):
        q = update.callback_query
        await q.answer()
        if not q.from_user or q.from_user.id != OWNER_CHAT_ID:
            await q.answer("⛔ Только для собственника.", show_alert=True)
            return
        try:
            _, action, cid_str = q.data.split(":", 2)
            cid = int(cid_str)
        except Exception:
            await q.edit_message_text(q.message.text + "\n\n⚠️ Неверный callback_data")
            return
        if action != "schedule":
            return
        from dashamail_scheduler import schedule_campaign
        await q.edit_message_text(q.message.text + f"\n\n⏳ Планирую CID={cid}…")
        try:
            res = await asyncio.to_thread(schedule_campaign, cid)
        except Exception as e:
            logger.error(f"dashamail schedule_campaign exception: {e}", exc_info=True)
            await q.edit_message_text(q.message.text + f"\n\n❌ Исключение: {type(e).__name__}: {e}")
            return
        if res.get("ok"):
            await q.edit_message_text(
                q.message.text + f"\n\n✅ Запланировано на {res['scheduled_at']}"
            )
        else:
            await q.edit_message_text(
                q.message.text + (
                    f"\n\n⚠️ Не удалось запланировать: {res.get('err')}"
                    f"\nДобей вручную: {res.get('wizard_url')}"
                )
            )

    app.add_handler(CallbackQueryHandler(handle_dashamail_callback, pattern="^dashamail:"))

    # ─── Алармы amoCRM ───────────────────────────────────────────────────────
    app.add_handler(CommandHandler("myamoid", lambda u, c: cmd_myamoid(u, c, db)))
    app.add_handler(CommandHandler("amo_setup", cmd_amo_setup))
    app.add_handler(CallbackQueryHandler(
        lambda u, c: handle_take_callback(u.callback_query, db),
        pattern="^amo_alarm_take\\|"
    ))
    app.add_handler(CallbackQueryHandler(
        lambda u, c: handle_amo_link_callback(u.callback_query, db),
        pattern="^amo_link\\|"
    ))
    # ─── Постановка задач в МойСклад (/задача) ───────────────────────────────
    # PTB не принимает кириллицу в CommandHandler (ValueError: not a valid bot command),
    # поэтому маршрутизируем /задача /поставить /отмена через MessageHandler + Regex.
    # /task — ASCII alias, можно зарегистрировать BotFather'ом в меню команд.
    app.add_handler(CommandHandler("task", cmd_task))
    app.add_handler(MessageHandler(
        filters.Regex(r"^/(задача|поставить)(@\w+)?(\s|$)"),
        cmd_task,
    ))

    # ─── /needs_review: подтверждение needs-review лотов (план 2026-05-28 Под-фаза 2) ──
    from needs_review_handler import cmd_needs_review, cb_needs_review

    async def _nr_guard_and_cmd(u, c):
        if not u.effective_user or u.effective_user.id != OWNER_CHAT_ID:
            await u.message.reply_text("⛔ Команда доступна только владельцу (bench-режим).")
            return
        await cmd_needs_review(u, c, db)

    async def _nr_guard_and_cb(u, c):
        if not u.callback_query or not u.callback_query.from_user \
                or u.callback_query.from_user.id != OWNER_CHAT_ID:
            await u.callback_query.answer("⛔ Нет доступа.", show_alert=True)
            return
        await cb_needs_review(u, c, db)

    app.add_handler(CommandHandler("needs_review", _nr_guard_and_cmd))
    app.add_handler(CallbackQueryHandler(_nr_guard_and_cb, pattern=r"^nr_"))
    app.add_handler(MessageHandler(
        filters.Regex(r"^/отмена(@\w+)?(\s|$)"),
        cmd_task_cancel,
    ))
    # Текст в активной /задача-conversation (только для whitelist в личке) — ДО общего handle_message
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & _task_conv_active_filter,
        handle_task_message,
    ))
    app.add_handler(CallbackQueryHandler(
        handle_task_draft_callback,
        pattern=r"^task_(confirm|rewrite|draft_cancel)$",
    ))
    app.add_handler(CallbackQueryHandler(
        handle_task_candidate_callback,
        pattern=r"^task_cand:",
    ))

    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POSTS, handle_channel_post))
    app.add_handler(MessageHandler(filters.ALL & ~filters.UpdateType.CHANNEL_POSTS, handle_message))

    # Планировщик
    setup_scheduler(app, db)

    # Алерт собственнику при приближении к авто-блоку JSON API МС.
    # Инциденты 29.05 и 01.06.2026: МС блокирует при >200 ответов 429/мин или
    # >400 ответов 429/час. Шлём TG-алерт заранее (порог 50/мин или 100/час).
    try:
        from moysklad import set_429_alert_callback

        async def _ms_429_owner_alert(cnt_min: int, cnt_hour: int):
            text = (
                f"⚠️ МС API: 429-шторм\n"
                f"• {cnt_min}/мин (авто-блок при 200)\n"
                f"• {cnt_hour}/час (авто-блок при 400)\n\n"
                f"Бот сам ушёл в long-sleep 30 с после каждой 429. "
                f"Если шторм не утихнет — поможет рестарт бота или временно "
                f"остановить тяжёлые отчёты."
            )
            try:
                await app.bot.send_message(chat_id=OWNER_CHAT_ID, text=text)
            except Exception as _e:
                logger.warning(f"_ms_429_owner_alert send_message: {_e}")

        set_429_alert_callback(_ms_429_owner_alert)
        logger.info("✅ МС 429-алерт собственнику зарегистрирован")
    except Exception as _e:
        logger.warning(f"Не удалось зарегистрировать MS 429-алерт: {_e}")

    async def retry_pending_idents(context):
        """Каждый час проверяем отложенные идентификации и повторяем запрос."""
        try:
            rows = db.get_retry_idents()
            if not rows:
                return
            group_chat_id = int(os.getenv("WAZZUP_ID_CHAT_ID", "0"))
            if not group_chat_id:
                return
            import uuid as _uuid3
            for row in rows:
                old_link_key = row["link_key"]
                chat_id_val = row["chat_id"]
                channel_id_val = row.get("channel_id", "")
                wazzup_name = row.get("wazzup_name", "")
                chat_type = row.get("chat_type", "telegram")

                # Создаём новый link_key
                link_key = str(_uuid3.uuid4())[:8]
                _pending_links[link_key] = {
                    "chat_id": chat_id_val,
                    "channel_id": channel_id_val,
                    "wazzup_name": wazzup_name,
                    "chat_type": chat_type,
                }
                db.save_pending_link(link_key, chat_id_val, channel_id_val, wazzup_name, chat_type)
                db.delete_pending_ident(old_link_key)

                CHANNEL_NAMES = {"telegram": "Telegram", "tgapi": "Telegram", "max": "Max", "whatsapp": "WhatsApp"}
                channel_label = CHANNEL_NAMES.get(chat_type, chat_type)
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("✅ Для рассылок", callback_data=f"wazzup_mailing|{link_key}"),
                        InlineKeyboardButton("👤 Просто контакт", callback_data=f"wazzup_link|{link_key}"),
                    ],
                    [
                        InlineKeyboardButton("⏰ Привязать позже", callback_data=f"wazzup_later|{link_key}"),
                    ]
                ])
                await context.bot.send_message(
                    chat_id=group_chat_id,
                    text=(
                        f"🔔 *Напоминание — {channel_label}*\n\n"
                        f"👤 *{wazzup_name}* ещё не привязан к компании.\n\n"
                        f"Этот контакт для рассылок при согласовании заказов?"
                    ),
                    parse_mode="Markdown",
                    reply_markup=keyboard,
                )
                logger.info(f"retry_pending_idents: повторный запрос для {wazzup_name} ({chat_id_val})")
        except Exception as e:
            logger.error(f"retry_pending_idents: {e}", exc_info=True)

    app.job_queue.run_repeating(retry_pending_idents, interval=3600, first=300)

    # market_intel — через PTB JobQueue (а не AsyncIOScheduler, который 28-29.05
    # дважды зависал после первого tick'а). PTB JobQueue работает стабильно
    # (retry_pending_idents подтверждён в логах каждый час).
    from market_intel_processor import market_intel_cron_job as _market_intel_cron

    async def _market_intel_job_wrapper(context):
        try:
            await _market_intel_cron(app, db)
        except Exception as e:
            logger.error(f"market_intel job wrapper: {e}", exc_info=True)

    app.job_queue.run_repeating(_market_intel_job_wrapper, interval=1800, first=30)

    # Автоподстановка «Дата планируемой оплаты» — каждые 10 мин. Перенесена из
    # AsyncIOScheduler 16.06.2026: tick 15:56 МСК пропустился без ошибок (тот же
    # паттерн, что с market_intel 28-29.05). Сам job стартует только при
    # PAYMENT_PLANNED_AUTOFILL_ENABLED=1 (env-gating внутри функции).
    from scheduler import payment_planned_autofill_job as _payment_planned_autofill

    async def _payment_planned_autofill_wrapper(context):
        try:
            await _payment_planned_autofill(app, db)
        except Exception as e:
            logger.error(f"payment_planned_autofill job wrapper: {e}", exc_info=True)

    app.job_queue.run_repeating(_payment_planned_autofill_wrapper, interval=600, first=60)

    # ────────────────────────────────────────────────────────────────────
    # Wazzup AI-классификатор: запросы клиентов по номенклатуре. Фаза 3
    # плана 2026-05-25. Раз в 15 мин в окне 09-19 МСК Пн-Пт. Сейчас только
    # копит данные в wazzup_classifications, без TG-алертов (Фазы 4-6 —
    # после юр-проверки 152-ФЗ + согласования с командой ОП).
    # Offline F1=0.97 (эксперимент 2026-06-04).
    # ────────────────────────────────────────────────────────────────────
    async def _wazzup_classifier_job(context):
        try:
            from wazzup_classifier import run_classification_batch
            stats = await run_classification_batch(db, bot_app=app)
            if stats.get("processed", 0) > 0:
                logger.info(f"wazzup_classifier job: {stats}")
        except Exception as e:
            logger.error(f"wazzup_classifier job: {e}", exc_info=True)

    app.job_queue.run_repeating(_wazzup_classifier_job, interval=900, first=120)

    # ────────────────────────────────────────────────────────────────────
    # Wazzup classifier — дневная сводка собственнику 17:00 МСК.
    # Счётчик за день + топ-5 срочных. Если 0 — «0 запросов, всё тихо».
    # ────────────────────────────────────────────────────────────────────
    from wazzup_classifier import URGENCY_EMOJI

    async def _wazzup_daily_summary(context):
        from datetime import datetime, timezone, timedelta
        now_msk = datetime.now(timezone(timedelta(hours=3)))
        if now_msk.weekday() >= 5:
            return
        try:
            total = db._fetchone("""
                SELECT COUNT(*) AS n
                FROM wazzup_classifications c
                JOIN wazzup_messages m ON m.message_id=c.message_id
                WHERE c.is_nomenclature_request = TRUE
                  AND m.sent_at::date = (NOW() AT TIME ZONE 'Europe/Moscow')::date
            """)["n"]
            urgent = db._fetchall("""
                SELECT m.contact_name, c.sku_or_description, c.urgency,
                       c.species_normalized
                FROM wazzup_classifications c
                JOIN wazzup_messages m ON m.message_id=c.message_id
                WHERE c.is_nomenclature_request = TRUE
                  AND m.sent_at::date = (NOW() AT TIME ZONE 'Europe/Moscow')::date
                ORDER BY CASE c.urgency
                            WHEN 'срочно' THEN 1
                            WHEN 'уточнение' THEN 2
                            ELSE 3 END,
                         m.sent_at DESC
                LIMIT 5
            """)
            if total == 0:
                text = "📊 *Wazzup-сводка за день*\n\n0 запросов по номенклатуре — всё тихо."
            else:
                lines = [f"📊 *Wazzup-сводка за день*\n\nВсего запросов: *{total}*\n\nТоп-5:"]
                for r in urgent:
                    em = URGENCY_EMOJI.get(r["urgency"], "⚪")
                    sku = (r["sku_or_description"] or "—")[:80]
                    contact = (r["contact_name"] or "?")[:25]
                    lines.append(f"{em} {contact}: {sku}")
                text = "\n".join(lines)
            await context.bot.send_message(
                OWNER_CHAT_ID, text, parse_mode="Markdown",
            )
        except Exception as e:
            logger.error(f"_wazzup_daily_summary: {e}", exc_info=True)

    # 17:00 МСК = 14:00 UTC
    from datetime import time as _dt_time, timezone as _tz_for_job
    app.job_queue.run_daily(
        _wazzup_daily_summary,
        time=_dt_time(hour=14, minute=0, tzinfo=_tz_for_job.utc),
    )

    # ────────────────────────────────────────────────────────────────────
    # Wazzup freshness watchdog: алерт собственнику если БД молчит >2ч в
    # рабочее время. Защита от повторного перехвата webhook AMGROUP-style
    # (см. retrospectives/2026-06-03-аудит-инфраструктуры-wazzup-amgbp.md).
    # Бьёт раз в час 09-19 МСК Пн-Пт. Анти-спам: 1 алерт за 6 часов.
    # ────────────────────────────────────────────────────────────────────
    _wazzup_alert_last_ts = [0]  # mutable closure cell

    async def _wazzup_freshness_check(context):
        from datetime import datetime, timezone, timedelta
        import time as _time
        now_msk = datetime.now(timezone(timedelta(hours=3)))
        if now_msk.weekday() >= 5:
            return
        if not (9 <= now_msk.hour < 19):
            return
        try:
            row = db._fetchone(
                "SELECT MAX(sent_at) AS last FROM wazzup_messages"
            )
            last = row.get("last") if row else None
            if last is None:
                gap_h = 999
            else:
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                gap_h = (datetime.now(timezone.utc) - last).total_seconds() / 3600
            if gap_h < 2:
                return
            if _time.time() - _wazzup_alert_last_ts[0] < 6 * 3600:
                return
            _wazzup_alert_last_ts[0] = _time.time()
            await context.bot.send_message(
                OWNER_CHAT_ID,
                f"⚠️ Wazzup БД молчит {gap_h:.1f}ч (последнее: {last}).\n\n"
                f"Возможные причины:\n"
                f"• AMGROUP опять перехватили webhook (cms.amgbp.ru).\n"
                f"• Сам Wazzup24 отвалил канал/пайплайн.\n\n"
                f"Команда для верификации в этом чате:\n"
                f"«проверь Wazzup webhook»",
            )
            logger.warning(
                f"wazzup_freshness: gap={gap_h:.1f}h, alert sent to OWNER"
            )
        except Exception as e:
            logger.error(f"wazzup_freshness_check: {e}", exc_info=True)

    app.job_queue.run_repeating(_wazzup_freshness_check, interval=3600, first=600)

    # Запускаем webhook-сервер и polling параллельно
    import aiohttp.web as web

    async def handle_sipuni_webhook(request):
        """Принимает события от Sipuni АТС."""
        try:
            params = dict(request.rel_url.query)
            event = params.get("event", "")
            call_id = params.get("call_id", "")
            src_num = params.get("src_num", "")
            dst_num = params.get("dst_num", "")
            short_dst = params.get("short_dst_num", "")
            tree_name = params.get("treeName", "")
            status = params.get("status", "")
            record_link = params.get("call_record_link", "")
            call_start = params.get("call_start_timestamp", "")
            call_answer = params.get("call_answer_timestamp", "0")

            logger.info(f"Sipuni event={event} call_id={call_id} src={src_num} dst={dst_num} tree={tree_name} status={status}")

            # event=2 — звонок завершён
            if event == "2" and record_link and status == "ANSWER":
                asyncio.create_task(process_sipuni_call(
                    call_id=call_id,
                    src_num=src_num,
                    dst_num=dst_num,
                    short_dst=short_dst,
                    tree_name=tree_name,
                    record_link=record_link,
                    call_start=call_start,
                    call_answer=call_answer,
                    bot=app.bot,
                ))

            return web.Response(text="ok")
        except Exception as e:
            logger.error(f"Sipuni webhook error: {e}")
            return web.Response(text="error", status=500)

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
                raw_dt = msg.get("dateTime", "")
                try:
                    from datetime import datetime, timezone
                    if raw_dt and str(raw_dt).isdigit():
                        sent_at = datetime.fromtimestamp(int(raw_dt), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        sent_at = str(raw_dt)
                except Exception:
                    sent_at = str(raw_dt)

                logger.info(f"Wazzup msg: isEcho={is_outbound} channel={channel_id_val} chatType={chat_type} chatId={chat_id_val} contact='{contact_name}' text='{text[:60]}'")

                # Сохраняем маппинг контакта → chatId/channel для последующей отправки
                if chat_id_val and contact_name and not is_outbound:
                    db.save_wazzup_contact(
                        contact_name=contact_name,
                        chat_id=chat_id_val,
                        chat_type=chat_type,
                        channel_id=channel_id_val,
                    )

                # Для исходящих — тоже проверяем идентификацию
                if chat_id_val and contact_name and is_outbound:
                    db.save_wazzup_contact(
                        contact_name=contact_name,
                        chat_id=chat_id_val,
                        chat_type=chat_type,
                        channel_id=channel_id_val,
                    )

                # Уведомляем если контакт неизвестен (для любых сообщений)
                if chat_id_val and contact_name:
                    is_known = db.is_wazzup_contact_known(chat_id_val)
                    logger.info(f"Wazzup: chat_id={chat_id_val} is_known={is_known}")
                    if chat_type in ("telegram", "tgapi", "max") and not is_known and chat_id_val not in _wazzup_notified:
                        # Проверяем что контакт не помечен как игнорируемый
                        ignored = db._fetchone(
                            "SELECT id FROM wazzup_contact_map WHERE chat_id=%s AND company_name='__ignore__'",
                            (chat_id_val,)
                        )
                        if ignored:
                            continue
                        # Проверяем есть ли уже chat_id в МойСклад — если да, идентификация не нужна
                        try:
                            from moysklad import get_headers
                            import aiohttp as _aiohttp
                            _attr_id = MS_ATTR_BY_TYPE.get(chat_type.lower(), "")
                            if _attr_id:
                                _ms_url = f"https://api.moysklad.ru/api/remap/1.2/entity/counterparty"
                                async with _aiohttp.ClientSession() as _s:
                                    async with _s.get(
                                        _ms_url,
                                        headers=get_headers(),
                                        params={"filter": f"attributes.{_attr_id}={chat_id_val}", "limit": 1}
                                    ) as _r:
                                        if _r.status == 200:
                                            _data = await _r.json()
                                            if _data.get("rows"):
                                                _cp_name = _data["rows"][0].get("name", "")
                                                logger.info(f"Wazzup: {chat_id_val} уже в МойСклад ({_cp_name}), идентификация не нужна")
                                                db.link_wazzup_contact(
                                                    chat_id=chat_id_val,
                                                    chat_type=chat_type,
                                                    channel_id=channel_id_val,
                                                    company_name=_cp_name,
                                                    wazzup_name=contact_name,
                                                    role="рассылка",
                                                )
                                                continue
                        except Exception as _e:
                            logger.warning(f"Wazzup: проверка МойСклад не удалась: {_e}")
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
                                # Сохраняем в БД чтобы пережить рестарт
                                db.save_pending_link(
                                    link_key=link_key,
                                    chat_id=chat_id_val,
                                    channel_id=channel_id_val,
                                    wazzup_name=contact_name,
                                    chat_type=chat_type,
                                )
                                keyboard = InlineKeyboardMarkup([
                                    [
                                        InlineKeyboardButton("✅ Для рассылок", callback_data=f"wazzup_mailing|{link_key}"),
                                        InlineKeyboardButton("👤 Просто контакт", callback_data=f"wazzup_link|{link_key}"),
                                    ],
                                    [
                                        InlineKeyboardButton("⏰ Привязать позже", callback_data=f"wazzup_later|{link_key}"),
                                    ]
                                ])
                                preview = (text or "").replace("\n", " ").strip()
                                if len(preview) > 120:
                                    preview = preview[:120] + "..."
                                CHANNEL_NAMES = {"telegram": "Telegram", "tgapi": "Telegram", "max": "Max", "whatsapp": "WhatsApp"}
                                channel_label = CHANNEL_NAMES.get(chat_type, chat_type)
                                await app.bot.send_message(
                                    chat_id=group_chat_id,
                                    text=(
                                        f"📩 *Новый контакт — {channel_label}*\n\n"
                                        f"👤 Имя: *{contact_name}*\n"
                                        f"💬 _{preview}_\n\n"
                                        f"Этот контакт для рассылок при согласовании заказов?"
                                    ),
                                    parse_mode="Markdown",
                                    reply_markup=keyboard
                                )
                                _wazzup_notified.add(chat_id_val)
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

    async def handle_web_report(request):
        """Интерактивный веб-отчёт ОП."""
        token = request.query.get("token", "")
        link = db.get_report_link(token)
        if not link:
            return web.Response(
                text="<html><body style='font-family:sans-serif;padding:40px;color:#6b7280'><h2>Ссылка истекла или недействительна</h2><p>Запроси новую через /web_report в боте.</p></body></html>",
                content_type="text/html", status=403
            )

        try:
            import json, pathlib
            from datetime import date

            # Проверяем кэш
            cached = db.get_report_cache()
            if cached:
                report_data = cached
                logger.info("handle_web_report: отдаём из кэша")
            else:
                report_data = await _build_report_data()
                db.set_report_cache(report_data)

            # Снимок «% работы на новых» (op_new_share) читается ОТДЕЛЬНО,
            # вне report_cache (TTL 5 ч). План: 2026-05-21-виджет-..., Фаза 3.
            # Пишется cron-job пятница 08:00 МСК + CLI --backfill.
            report_data["new_share"] = db.get_new_share_snapshot()

            tpl_path = pathlib.Path(__file__).parent / "report_template.html"
            if tpl_path.exists():
                html = tpl_path.read_text(encoding="utf-8")
            else:
                from report_html import REPORT_HTML_TEMPLATE
                html = REPORT_HTML_TEMPLATE
            html = html.replace("__REPORT_DATA__", json.dumps(report_data, ensure_ascii=False))
            return web.Response(text=html, content_type="text/html", charset="utf-8")

        except Exception as e:
            logger.error(f"handle_web_report: {e}", exc_info=True)
            return web.Response(text=f"Ошибка: {e}", status=500)

    async def handle_pdz_html(request):
        """HTML-отчёт «Дебиторка» (Фаза 5 плана 2026-05-20-пдз-автоматика).

        Доступ — только по токену из `report_links` с mgr_filter='pdz'.
        Отдаёт закэшированный HTML (cron 14:15 МСК пишет в bot_settings.pdz_html_cache).
        Если кэш пуст — рендерит на лету из последнего snapshot + promise_log.
        НЕ дёргает МойСклад API.
        """
        token = request.query.get("token", "")
        link = db.get_report_link(token)
        if not link or (link.get("mgr_filter") or "") != "pdz":
            return web.Response(
                text="<html><body style='font-family:sans-serif;padding:40px;color:#6b7280'>"
                     "<h2>Ссылка истекла или недействительна</h2>"
                     "<p>Запроси новую через /pdz_html в боте.</p></body></html>",
                content_type="text/html", status=403, charset="utf-8",
            )
        try:
            cached = db.get_pdz_html_cache()
            if cached:
                html_text = cached
                logger.info("handle_pdz_html: отдаём из кэша")
            else:
                from pdz_report_html import render_pdz_html_from_db
                html_text = render_pdz_html_from_db(db)
                try:
                    db.set_pdz_html_cache(html_text)
                except Exception as e:
                    logger.warning(f"handle_pdz_html: set_pdz_html_cache: {e}")
            return web.Response(text=html_text, content_type="text/html", charset="utf-8")
        except Exception as e:
            logger.error(f"handle_pdz_html: {e}", exc_info=True)
            return web.Response(text=f"Ошибка: {e}", status=500, charset="utf-8")

    async def handle_pdz_embed(request):
        """Embed-версия PDZ HTML для единого дашборда F2B (plans/2026-05-21-…).

        Отличия от handle_pdz_html:
          - аутентификация через постоянный shared-secret из env DASHBOARD_PDZ_SECRET
            (не через report_links с TTL — иначе дашборд должен бы каждые 24ч
            получать новый токен от собственника);
          - возвращает «голый» HTML-фрагмент (то же содержимое), который
            quiz-game встроит в свою вкладку «Дебиторка».
        """
        # Захардкоженный default — синхронизирован с quiz-game main.py (там тот же
        # default). Env DASHBOARD_PDZ_SECRET переопределяет, если нужно ротировать.
        embed_secret = os.getenv("DASHBOARD_PDZ_SECRET", "UY-2J7VujDgbFVEg26WJvqCpS1qY_5pm8x56qZ-O_uE")
        if request.query.get("secret", "") != embed_secret:
            return web.Response(text="forbidden", status=403, charset="utf-8")
        try:
            cached = db.get_pdz_html_cache()
            if cached:
                html_text = cached
            else:
                from pdz_report_html import render_pdz_html_from_db
                html_text = render_pdz_html_from_db(db)
                try:
                    db.set_pdz_html_cache(html_text)
                except Exception as e:
                    logger.warning(f"handle_pdz_embed: set_pdz_html_cache: {e}")
            return web.Response(text=html_text, content_type="text/html", charset="utf-8")
        except Exception as e:
            logger.error(f"handle_pdz_embed: {e}", exc_info=True)
            return web.Response(text=f"Ошибка: {e}", status=500, charset="utf-8")

    async def handle_pdz_manager_json(request):
        """JSON-список просрочки конкретного менеджера для дашборда мотивации.
        Тот же shared-secret что и /pdz/embed."""
        embed_secret = os.getenv("DASHBOARD_PDZ_SECRET", "UY-2J7VujDgbFVEg26WJvqCpS1qY_5pm8x56qZ-O_uE")
        if request.query.get("secret", "") != embed_secret:
            return web.json_response({"error": "forbidden"}, status=403)
        manager_tag = (request.match_info.get("tag") or "").strip().lower()
        if not manager_tag:
            return web.json_response({"error": "tag required"}, status=400)
        try:
            from moysklad import pdz_overdue_for_manager
            items = await pdz_overdue_for_manager(manager_tag, db=db, group_by_agent=True)
            pdz_list = [{
                "name":           x.get("agent_name") or "—",
                "days_overdue":   int(x.get("max_days_overdue") or 0),
                "amount_rub":     round(float(x.get("total_unpaid") or 0), 2),
                "orders_count":   int(x.get("orders_count") or 0),
                "breaks_count":   int(x.get("breaks_count") or 0),
                "ms_url":         x.get("ms_url_first_order"),
            } for x in items]
            return web.json_response({"manager_tag": manager_tag, "pdz_list": pdz_list})
        except Exception as e:
            logger.error(f"handle_pdz_manager_json: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def run_web():
        web_app = web.Application()
        web_app.router.add_post("/webhook/moysklad", handle_ms_webhook)
        web_app.router.add_post("/webhook/wazzup", handle_wazzup_webhook)
        web_app.router.add_get("/webhook/sipuni", handle_sipuni_webhook)
        web_app.router.add_post("/webhook/sipuni", handle_sipuni_webhook)
        web_app.router.add_get("/health", handle_health)
        web_app.router.add_get("/report", handle_web_report)
        web_app.router.add_get("/pdz", handle_pdz_html)
        web_app.router.add_get("/pdz/embed", handle_pdz_embed)
        web_app.router.add_get("/pdz/manager-json/{tag}", handle_pdz_manager_json)
        # ─── amoCRM алармы ───────────────────────────────────────────────────
        async def handle_amo_webhook_route(request):
            await handle_amo_webhook(request, app, db)
            return web.Response(text="ok")
        web_app.router.add_post("/webhook/amocrm", handle_amo_webhook_route)

        # ─── Market Intel: эндпоинты для скилла update-market-intel ──────────
        market_intel_token = os.getenv("MARKET_INTEL_TOKEN", "")

        def _check_market_intel_auth(request) -> bool:
            if not market_intel_token:
                return False
            auth = request.headers.get("Authorization", "")
            return auth == f"Bearer {market_intel_token}"

        async def handle_market_intel_file(request):
            if not _check_market_intel_auth(request):
                return web.Response(text="forbidden", status=403)
            try:
                msg_id = int(request.match_info["id"])
            except (KeyError, ValueError):
                return web.Response(text="bad id", status=400)
            row = db.get_market_intel_message(msg_id)
            if not row or not row.get("file_path"):
                return web.Response(text="not found", status=404)
            path = row["file_path"]
            if not os.path.exists(path):
                return web.Response(text="file missing on disk", status=410)
            return web.FileResponse(path)

        async def handle_market_intel_processed(request):
            if not _check_market_intel_auth(request):
                return web.Response(text="forbidden", status=403)
            try:
                data = await request.json()
                msg_id = int(data["id"])
            except Exception:
                return web.Response(text="bad body", status=400)
            db.mark_market_intel_processed(msg_id)
            return web.json_response({"ok": True, "id": msg_id})

        async def handle_market_intel_unprocessed(request):
            """Возвращает список необработанных сообщений из канала «Мониторинг»
            для скилла update-market-intel и cloud-routine."""
            if not _check_market_intel_auth(request):
                return web.Response(text="forbidden", status=403)
            try:
                limit = int(request.query.get("limit", "100"))
            except ValueError:
                limit = 100
            rows = db.get_unprocessed_market_intel(limit=limit)
            # сериализуем datetime в isoformat для JSON
            for r in rows:
                for k in ("posted_at", "created_at"):
                    v = r.get(k)
                    if v is not None and hasattr(v, "isoformat"):
                        r[k] = v.isoformat()
            counts = db.get_market_intel_count()
            return web.json_response({
                "ok": True,
                "limit": limit,
                "returned": len(rows),
                "total": counts.get("total", 0),
                "unprocessed_total": counts.get("unprocessed", 0),
                "messages": rows,
            })

        async def handle_market_intel_alert(request):
            """Принимает {"text": "..."} и шлёт его в OWNER_CHAT_ID (Виктор)
            от лица бота «Эф» — для алертов «кандидат на замену сырья»."""
            if not _check_market_intel_auth(request):
                return web.Response(text="forbidden", status=403)
            try:
                data = await request.json()
                text = (data.get("text") or "").strip()
            except Exception:
                return web.Response(text="bad body", status=400)
            if not text:
                return web.Response(text="empty text", status=400)
            try:
                sent = await app.bot.send_message(
                    chat_id=OWNER_CHAT_ID,
                    text=text,
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
            except Exception as e:
                logger.error(f"market_intel_alert send failed: {e}")
                # fallback без Markdown — в случае ошибки парсинга
                try:
                    sent = await app.bot.send_message(
                        chat_id=OWNER_CHAT_ID,
                        text=text,
                        disable_web_page_preview=True,
                    )
                except Exception as e2:
                    return web.json_response({"ok": False, "error": str(e2)}, status=500)
            return web.json_response({"ok": True, "message_id": sent.message_id})

        web_app.router.add_get("/market-intel/files/{id}", handle_market_intel_file)
        web_app.router.add_get("/market-intel/unprocessed", handle_market_intel_unprocessed)
        web_app.router.add_post("/market-intel/processed", handle_market_intel_processed)
        web_app.router.add_post("/market-intel/alert", handle_market_intel_alert)

        # ─── Internal notify_manager (procurement webapp → бот) ───────────────
        # План: F2B второй мозг/plans/2026-05-28-дашборд-закупщика-с-матчером.md, Фаза 4.
        # Webapp procurement_app пишет в outbox и POSTит сюда. Мы пересылаем
        # сообщение менеджеру в TG. Защита: X-Internal-Secret = BOT_INTERNAL_NOTIFY_SECRET.
        # Идемпотентность: in-process set {(request_id, text_hash)} переживает retry,
        # но сбрасывается при рестарте контейнера — это OK, webapp ставит delivered_at
        # только на ответ 200, дубль доставляется максимум один раз.
        import hashlib as _hashlib
        _notify_delivered_keys: set = set()
        _notify_delivered_keys_limit = 5000  # защита от бесконечного роста памяти

        async def handle_internal_notify_manager(request):
            import secrets as _secrets
            expected = os.getenv("BOT_INTERNAL_NOTIFY_SECRET", "")
            got = request.headers.get("X-Internal-Secret", "")
            # constant-time сравнение, как в procurement_app/main.py verify_csrf
            if not expected or not _secrets.compare_digest(got, expected):
                return web.Response(text="forbidden", status=403)
            try:
                data = await request.json()
                request_id = int(data["request_id"])
                manager_tg_id = int(data["manager_tg_id"])
                text = str(data["text"])
            except Exception as e:
                return web.json_response({"ok": False, "error": f"bad body: {e}"}, status=400)
            if not text.strip():
                return web.json_response({"ok": False, "error": "empty text"}, status=400)

            text_hash = _hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
            key = (request_id, text_hash)
            if key in _notify_delivered_keys:
                # уже доставляли — webapp получит 200 и не будет ретраить.
                return web.json_response({"ok": True, "idempotent": True})

            try:
                sent = await app.bot.send_message(
                    chat_id=manager_tg_id,
                    text=text,
                    disable_web_page_preview=True,
                )
            except Exception as e:
                logger.error(f"internal_notify_manager send failed: req={request_id} mgr={manager_tg_id} err={e}")
                return web.json_response({"ok": False, "error": str(e)[:300]}, status=500)

            # успех — фиксируем идемпотентность; чистим самые старые если превысили лимит.
            if len(_notify_delivered_keys) >= _notify_delivered_keys_limit:
                _notify_delivered_keys.clear()
            _notify_delivered_keys.add(key)
            return web.json_response({"ok": True, "message_id": sent.message_id})

        web_app.router.add_post("/internal/notify_manager", handle_internal_notify_manager)

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

        # Catch-up пропущенных PDZ-cron'ов: fire-and-forget в фон. Иначе
        # snapshot тянет МС API ~3 мин и блокирует start_polling — бот не
        # отвечает на команды до окончания catch-up (наблюдалось 2026-05-27
        # 17:50, когда я повторно ронял контейнер push'ами и polling никак
        # не стартовал). Catch-up догонит в фоне, polling работает сразу.
        import asyncio as _asyncio
        async def _catchup_in_bg():
            try:
                await pdz_catch_up_missed_jobs(app, db)
            except Exception as e:
                logger.error(f"pdz_catch_up_missed_jobs failed: {e}", exc_info=True)
        _asyncio.create_task(_catchup_in_bg())

        # Catch-up для market_intel: при rebuild'ах чаще 30 мин IntervalTrigger
        # никогда не успевает дёрнуться. Прогоняем разбор сразу на старте.
        try:
            from market_intel_processor import market_intel_cron_job
            await market_intel_cron_job(app, db)
        except Exception as e:
            logger.error(f"market_intel startup catch-up failed: {e}", exc_info=True)

        # Ждём завершения старого инстанса и сбрасываем webhook.
        # drop_pending_updates=False - чтобы Telegram отдал накопленное
        # за время rebuild'а (иначе теряем channel_post канала «Мониторинг»,
        # подтверждено окнами потерь 28.05 13:01 и 29.05 15:02-15:03).
        import asyncio as _asyncio
        for attempt in range(5):
            try:
                await app.bot.delete_webhook(drop_pending_updates=False)
                break
            except Exception as e:
                logger.warning(f"delete_webhook attempt {attempt+1}: {e}")
                await _asyncio.sleep(2)

        await app.updater.start_polling(
            drop_pending_updates=False,
            allowed_updates=["message", "channel_post", "edited_message", "edited_channel_post", "callback_query", "business_connection"]
        )
        logger.info("🤖 Бот запущен!")
        # Загружаем менеджеров Wazzup
        await load_wazzup_managers()
        # Восстанавливаем pending_contracts из БД
        try:
            import json as _json
            rows = db._fetchall("SELECT user_id, data FROM pending_contracts WHERE created_at > NOW() - INTERVAL '24 hours'")
            for row in rows:
                _pending_contracts[row["user_id"]] = _json.loads(row["data"])
            if rows:
                logger.info(f"Восстановлено {len(rows)} pending_contracts из БД")
        except Exception as e:
            logger.warning(f"Не удалось восстановить pending_contracts: {e}")
        # Восстанавливаем pending_links из БД после рестарта
        try:
            pending_rows = db.load_pending_links()
            for row in pending_rows:
                lk = row["link_key"]
                entry = {
                    "link_key": lk,
                    "chat_id": row["chat_id"],
                    "channel_id": row["channel_id"],
                    "wazzup_name": row["wazzup_name"],
                    "chat_type": row["chat_type"],
                }
                _pending_links[lk] = entry
            if pending_rows:
                logger.info(f"Восстановлено {len(pending_rows)} pending_links из БД")
        except Exception as e:
            logger.warning(f"load_pending_links: {e}")
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
    "Мерзлякова Елена Владимировна":   "+79920035102",
    "Баласанян Карина Владимировна":   "@fatbob183",
    "Скляр Инесса Ионасовна":          "+79622522903",
}

# Маппинг crmUserId Wazzup → имя менеджера (заполним после первых вебхуков)
WAZZUP_MANAGERS: dict = {}

async def load_wazzup_managers():
    """Загружает список менеджеров из Wazzup API."""
    import aiohttp
    api_key = os.getenv("WAZZUP_API_KEY", "")
    if not api_key:
        return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.wazzup24.com/v3/users",
                headers={"Authorization": f"Bearer {api_key}"}
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    for u in data.get("users", []):
                        uid = u.get("userId") or u.get("id", "")
                        name = u.get("name", "") or f"{u.get('firstName','')} {u.get('lastName','')}".strip()
                        if uid and name:
                            WAZZUP_MANAGERS[str(uid)] = name
                    logger.info(f"Wazzup managers loaded: {len(WAZZUP_MANAGERS)} — {list(WAZZUP_MANAGERS.values())}")
    except Exception as e:
        logger.warning(f"load_wazzup_managers: {e}")
# Кэш для дедупликации webhook — order_id → timestamp последней проверки
_price_check_cache: dict = {}


# Функция check_debtor_alert удалена в Фазе 5 (план 2026-05-21).
# Её роль (алертить о новом заказе клиента с просрочкой) перенесена в
# объединённый approval-алерт через notifier.check_approval_needed,
# который собирает 6-блочный светофор включая просрочку и шлёт в личку
# собственнику, а не в групповой чат.

async def process_ms_webhook(data: dict, bot):
    """Обрабатывает webhook от МойСклад — триггерит approval-алерт и логистику."""
    import time
    try:
        group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
        if not group_chat_id:
            return

        events = data.get("events", [])
        for event in events:
            meta = event.get("meta", {})
            entity_type = meta.get("type", "")

            # counterparty.UPDATE — отдельная ветка: алерт собственнику об
            # изменении «Дней отсрочки» (не связан со светофором заказов).
            if entity_type == "counterparty":
                cp_href = meta.get("href", "")
                if cp_href and event.get("action") == "UPDATE":
                    try:
                        await check_counterparty_delay_change(cp_href, bot, db)
                    except Exception as ex_cd:
                        logger.warning(f"check_counterparty_delay_change: {ex_cd}")
                continue

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

            # Смена статуса на "Согласовано" (UPDATE) или создание сразу
            # в этом статусе (CREATE — например, через API/импорт/Salesbot)
            # → отправляем клиенту. Notifier проверяет state внутри и делает
            # атомарный claim через agreed_notifications.
            if action in ("UPDATE", "CREATE"):
                await check_order_agreed(order_href, bot, db)

            # Объединённый алерт «На согласовании» / «ЗА ЛИМИТОМ» — собственнику
            # (план 2026-05-21, Фаза 3). Notifier сам проверяет state и делает
            # атомарный дедуп через pending_approval_alerts (UNIQUE order_id+sum_hash).
            # Заменил собой старые check_debtor_alert (в группу PRO) и self-standing
            # check_order_prices alert (в личку) — оба выпилены в Фазе 5.
            # 16.06.2026: задержка 60 сек, чтобы webhook-autofill «Даты планируемой
            # оплаты» успел отработать, и светофор уходил собственнику с уже
            # заполненной датой. Дубли при CREATE+UPDATE гасит UNIQUE pending_approval_alerts.
            if action in ("UPDATE", "CREATE"):
                from notifier import check_approval_needed
                async def _delayed_approval(href=order_href):
                    try:
                        await asyncio.sleep(60)
                        await check_approval_needed(href, bot, db)
                    except Exception as ex_app:
                        logger.warning(f"delayed check_approval_needed({order_id}): {ex_app}")
                asyncio.create_task(_delayed_approval())

            # Проверяем логистику — только при создании заказа
            if action == "CREATE":
                await check_logistics_alert(order_href, bot, group_chat_id)

            # Реактивная автоподстановка «Даты планируемой оплаты» — сразу
            # после сохранения заказа (16.06.2026). Cron в JobQueue остаётся
            # safety-net на 10 мин против потерь webhook'а.
            if action in ("UPDATE", "CREATE") and os.getenv("PAYMENT_PLANNED_AUTOFILL_ENABLED", "").strip().lower() in {"1","true","yes"}:
                try:
                    from moysklad import payment_planned_autofill_tick
                    res_af = await payment_planned_autofill_tick(db, order_id=order_id)
                    if res_af.get("patched", 0) > 0:
                        logger.info(f"payment_planned_autofill webhook({order_id}): {res_af}")
                except Exception as ex_af:
                    logger.warning(f"payment_planned_autofill webhook({order_id}): {ex_af}")

            # Аудит «Дата планируемой оплаты» (план 2026-05-20-автоподстановка, Фаза 3).
            # Включается env-флагом PAYMENT_PLANNED_AUTOFILL_ENABLED. Слой 1 — self-write
            # маркер не алертит на собственный PATCH. Слой 2 — запись в audit. Слой 3 — TG.
            if action == "UPDATE" and os.getenv("PAYMENT_PLANNED_AUTOFILL_ENABLED", "").strip().lower() in {"1","true","yes"}:
                try:
                    await check_payment_planned_audit(order_href, bot, db)
                except Exception as ex_aud:
                    logger.warning(f"check_payment_planned_audit({order_id}): {ex_aud}")

    except Exception as e:
        logger.error(f"process_ms_webhook: {e}")


async def check_counterparty_delay_change(cp_href: str, bot, db):
    """При UPDATE контрагента: если «Дней отсрочки» сменилось vs snapshot — TG-алерт.

    Первый encounter (snapshot отсутствует) = baseline без алерта.
    Кто менял — берём из /entity/counterparty/{id}/audit.
    """
    from moysklad import get_headers, MS_BASE, _DAYS_DELAY_ATTR_NAME
    import aiohttp

    cp_url = cp_href.split("?")[0]
    agent_id = cp_url.rstrip("/").split("/")[-1]

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{cp_url}?expand=attributes", headers=get_headers()) as resp:
            if resp.status != 200:
                return
            cp = await resp.json()

        agent_name = cp.get("name") or ""
        new_delay = None
        for a in cp.get("attributes", []) or []:
            if a.get("name") == _DAYS_DELAY_ATTR_NAME:
                v = a.get("value")
                if isinstance(v, (int, float)):
                    new_delay = int(v)
                break

        if new_delay is None:
            return  # отсрочка не задана — не трекаем

        try:
            old_delay = db.get_counterparty_delay_snapshot(agent_id)
        except Exception as ex:
            logger.warning(f"get_counterparty_delay_snapshot({agent_id}): {ex}")
            return

        # Baseline: первый раз видим контрагента — записываем без алерта
        if old_delay is None:
            try:
                db.upsert_counterparty_delay_snapshot(agent_id, agent_name, new_delay)
            except Exception as ex:
                logger.warning(f"upsert_counterparty_delay_snapshot baseline({agent_id}): {ex}")
            return

        if old_delay == new_delay:
            return  # ничего не изменилось

        # Изменение → определяем кто менял (best-effort)
        changed_by = "unknown"
        try:
            async with session.get(f"{MS_BASE}/entity/counterparty/{agent_id}/audit", headers=get_headers()) as resp_a:
                if resp_a.status == 200:
                    adata = await resp_a.json()
                    rows = adata.get("rows", []) or []
                    if rows:
                        emp = (rows[0].get("employee") or {})
                        changed_by = emp.get("name") or "unknown"
        except Exception:
            pass

        # TG-алерт собственнику
        try:
            owner_id = int(os.getenv("OWNER_CHAT_ID", "0") or 0)
            if owner_id:
                text = (
                    f"📋 *Контрагент:* [{agent_name}]"
                    f"(https://online.moysklad.ru/app/#company/edit?id={agent_id})\n"
                    f"«Дней отсрочки» изменено: *{old_delay} → {new_delay}* дн.\n"
                    f"Изменил: {changed_by}"
                )
                await bot.send_message(chat_id=owner_id, text=text, parse_mode="Markdown", disable_web_page_preview=True)
        except Exception as ex:
            logger.warning(f"counterparty_delay alert send: {ex}")

        # Обновляем snapshot
        try:
            db.upsert_counterparty_delay_snapshot(agent_id, agent_name, new_delay)
        except Exception as ex:
            logger.warning(f"upsert_counterparty_delay_snapshot after-alert({agent_id}): {ex}")


async def check_payment_planned_audit(order_href: str, bot, db):
    """Проверяет «Дату планируемой оплаты» после UPDATE-webhook'а.

    Если значение отличается от расчётного (counterparty.days_delay + order.moment),
    и это не самопатч бота (см. слой 1) — алерт собственнику + запись в audit log.
    Без авто-revert — alert-only (план Фаза 3, решение #8).
    """
    from moysklad import get_headers, MS_BASE, _PPM_INITIAL_ATTR_NAME, _DAYS_DELAY_ATTR_NAME, _autofill_fmt_ms_dt
    import aiohttp
    from datetime import datetime, timedelta

    order_url = order_href.split("?")[0]
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{order_url}?expand=agent,attributes", headers=get_headers()) as resp:
            if resp.status != 200:
                return
            order = await resp.json()

        order_id_v = order.get("id")
        order_name = order.get("name")
        agent = order.get("agent") or {}
        agent_id = agent.get("id")
        if not agent_id:
            href = (agent.get("meta") or {}).get("href") or ""
            if href:
                agent_id = href.rstrip("/").split("/")[-1] or None
        if not agent_id:
            return

        current_raw = None
        for a in order.get("attributes", []) or []:
            if a.get("name") == _PPM_INITIAL_ATTR_NAME:
                current_raw = a.get("value")
                break
        if not current_raw:
            return  # поле пустое, ничего сверять

        # Исторические заказы — бот не имел контроля над ними до 16.06.2026.
        # Если в payment_planned_audit нет ни одной записи cron/webhook_autofill
        # по этому order_id — менеджер когда-то поставил дату руками, и сверять
        # её с расчётной по сегодняшней отсрочке бессмысленно. Поймали на
        # ООО Фелиса (заказ от апреля).
        try:
            if not db.has_bot_autofill_for_order(order_id_v):
                return
        except Exception as ex:
            logger.warning(f"has_bot_autofill_for_order({order_id_v}): {ex}")

        # Слой 1: если это наш самопатч — гасим без алерта
        try:
            if db.consume_bot_self_write(order_id_v, "ppm_initial", str(current_raw), ttl_seconds=60):
                return
        except Exception as ex:
            logger.warning(f"consume_self_write({order_id_v}): {ex}")

        # Считаем expected
        async with session.get(f"{MS_BASE}/entity/counterparty/{agent_id}?expand=attributes", headers=get_headers()) as resp_cp:
            if resp_cp.status != 200:
                return
            cp = await resp_cp.json()
        delay = 0
        for a in cp.get("attributes", []) or []:
            if a.get("name") == _DAYS_DELAY_ATTR_NAME:
                v = a.get("value")
                if isinstance(v, (int, float)):
                    delay = int(v)
                break

        # База расчёта — План.дата отгрузки (deliveryPlannedMoment); фолбэк на moment.
        # Решено 16.06.2026: отсрочка считается от факта отгрузки.
        base_raw = order.get("deliveryPlannedMoment") or order.get("moment") or ""
        try:
            base_dt = datetime.strptime(base_raw[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return
        expected_dt = base_dt + timedelta(days=delay)
        expected_value = _autofill_fmt_ms_dt(expected_dt)

        # Сравниваем по дате, не по строке — иначе любой UPDATE заказа (позиции,
        # комментарий, статус) на котором МС перепишет time-компонент даты с
        # «20:00:00.000» на «14:43:00.000» поднимает ложный алерт. 16.06.2026
        # поймали на заказе 02571 (Скляр→Дубинин редактировали позиции).
        current_date_cmp = None
        try:
            current_dt_cmp = datetime.strptime(str(current_raw)[:19], "%Y-%m-%d %H:%M:%S")
            current_date_cmp = current_dt_cmp.date()
            if current_date_cmp == expected_dt.date():
                return  # дата совпадает — нет повода алертить
        except Exception:
            pass

        # Дата отличается от expected, но если бот сам когда-либо ставил это
        # значение — значит у контрагента позже изменилась отсрочка, реальной
        # правки от менеджера не было. Не алертим (история есть в audit-логе).
        # Поймали 16.06.2026 на ООО Печи: бот поставил 01.07 при отсрочке 14
        # дн, потом отсрочка стала 21 дн, expected стал 08.07 — но менеджер
        # дату не трогал.
        if current_date_cmp is not None:
            try:
                if db.was_payment_planned_set_by_bot(order_id_v, current_date_cmp):
                    return
            except Exception as ex:
                logger.warning(f"was_payment_planned_set_by_bot({order_id_v}): {ex}")

        # Кто менял
        changed_by = "unknown"
        try:
            async with session.get(f"{MS_BASE}/entity/customerorder/{order_id_v}/audit", headers=get_headers()) as resp_a:
                if resp_a.status == 200:
                    adata = await resp_a.json()
                    rows = adata.get("rows", []) or []
                    if rows:
                        last = rows[0]
                        emp = last.get("employee") or {}
                        changed_by = emp.get("name") or "unknown"
        except Exception:
            pass

        # Парсим current → date
        from datetime import datetime as _dt2
        try:
            current_date = _dt2.strptime(str(current_raw)[:19], "%Y-%m-%d %H:%M:%S").date()
        except Exception:
            current_date = None
        agent_name = agent.get("name") or ""

        try:
            db.log_payment_planned_audit(
                order_id=order_id_v,
                order_name=order_name,
                agent_id=agent_id,
                agent_name=agent_name,
                old_date=None,
                new_date=current_date,
                expected_date=expected_dt.date(),
                changed_by=changed_by,
                source="webhook_update",
            )
        except Exception as ex:
            logger.warning(f"audit log({order_id_v}): {ex}")

        # TG-алерт собственнику
        try:
            owner_id_raw = os.getenv("OWNER_CHAT_ID", "")
            owner_id = int(owner_id_raw) if owner_id_raw else None
        except ValueError:
            owner_id = None
        if not owner_id:
            return

        href_order = f"https://online.moysklad.ru/app/#customerorder/edit?id={order_id_v}"
        text = (
            f"⚠️ Заказ [{agent_name or order_name or '—'}]({href_order}): "
            f"«Дата планируемой оплаты» изменена менеджером {changed_by}\n"
            f"Сейчас: {current_date.strftime('%d.%m.%Y') if current_date else '—'}\n"
            f"Ожидалось по договору: {expected_dt.date().strftime('%d.%m.%Y')} (отсрочка {delay} дн.)\n"
            f"История: /payment_planned_history {order_id_v}"
        )
        try:
            await bot.send_message(
                chat_id=owner_id, text=text, parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        except Exception as ex:
            logger.warning(f"check_payment_planned_audit: TG send failed: {ex}")


async def cmd_payment_planned_autofill_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """OWNER-only. Тест cron-tick'а автоподстановки «Дата планируемой оплаты».

    /payment_planned_autofill_test                — dry-run за 24ч (без PATCH)
    /payment_planned_autofill_test <order_id>     — dry-run одного заказа
    /payment_planned_autofill_test live           — реальный PATCH за 24ч
    /payment_planned_autofill_test live <order_id> — реальный PATCH одного заказа
    """
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
        await update.message.reply_text("⛔ Только для собственника.")
        return

    args = list(context.args or [])
    live = False
    target_order_id = None
    if args and args[0].lower() == "live":
        live = True
        args.pop(0)
    if args:
        target_order_id = args[0]

    from moysklad import payment_planned_autofill_tick
    await update.message.reply_text(
        f"⏳ Запускаю autofill_tick {'(LIVE PATCH)' if live else '(dry-run)'}"
        + (f" для заказа {target_order_id}" if target_order_id else " за 24ч окно")
    )
    try:
        res = await payment_planned_autofill_tick(
            db, hours_back=24, order_id=target_order_id, dry_run=not live,
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    lines = [
        f"Результат autofill_tick {'(LIVE)' if live else '(dry-run)'}:",
        f"  обработано: {res.get('processed')}",
        f"  PATCH: {res.get('patched')}",
        f"  skip filled: {res.get('skipped_filled')}",
        f"  skip no agent: {res.get('skipped_no_agent')}",
        f"  PATCH failed: {res.get('skipped_patch_failed')}",
        f"  zero-delay alerts: {len(res.get('zero_alerts') or [])}",
    ]
    if res.get("errors"):
        lines.append("Errors:")
        for e in (res.get("errors") or [])[:5]:
            lines.append(f"  {e}")
    alerts = res.get("zero_alerts") or []
    if alerts:
        lines.append("")
        lines.append("Заказы без отсрочки (топ 10):")
        for a in alerts[:10]:
            lines.append(
                f"  • {a.get('agent_name') or '—'} — {a.get('sum_rub'):,.0f} ₽  ({a.get('order_id')})".replace(",", " ")
            )
    await update.message.reply_text("\n".join(lines))


async def cmd_payment_planned_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """OWNER-only. История изменений «Дата планируемой оплаты» по заказу.

    /payment_planned_history <order_id>
    """
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
        await update.message.reply_text("⛔ Только для собственника.")
        return
    if not context.args:
        await update.message.reply_text("Формат: /payment_planned_history <order_id>")
        return
    order_id_q = context.args[0].strip()
    rows = db.get_payment_planned_history(order_id_q, limit=50)
    if not rows:
        await update.message.reply_text(f"По заказу {order_id_q} истории нет.")
        return
    lines = [f"История «Даты планируемой оплаты» — заказ {order_id_q}"]
    name_set = False
    for r in rows:
        if not name_set and (r.get("agent_name") or r.get("order_name")):
            lines.append(f"Клиент: {r.get('agent_name') or '—'} · заказ: {r.get('order_name') or '—'}")
            name_set = True
        ts = r.get("ts")
        ts_s = ts.strftime("%d.%m.%Y %H:%M") if ts else "—"
        nd = r.get("new_date")
        nd_s = nd.strftime("%d.%m.%Y") if nd else "—"
        exp = r.get("expected_date")
        exp_s = exp.strftime("%d.%m.%Y") if exp else "—"
        lines.append(
            f"{ts_s} · {r.get('source') or '—'} · {r.get('changed_by') or '—'} · "
            f"new={nd_s} (ожид={exp_s})"
        )
    await update.message.reply_text("\n".join(lines))

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

        logger.info(f"check_logistics_alert: заказ={order_name} address='{address}' delivery_date='{delivery_date}'")

        if not address or not delivery_date:
            logger.info(f"check_logistics_alert: заказ {order_name} — нет адреса или даты, пропускаем (address={bool(address)}, date={bool(delivery_date)})")
            return

        # Не алертим старые заказы (старше 3 дней)
        from datetime import datetime, timezone, timedelta
        try:
            delivery_dt = datetime.fromisoformat(delivery_date.replace("Z", "+00:00"))
            if delivery_dt.tzinfo is None:
                delivery_dt = delivery_dt.replace(tzinfo=timezone.utc)
            if delivery_dt < datetime.now(timezone.utc) - timedelta(days=3):
                logger.info(f"check_logistics_alert: заказ {order_name} слишком старый ({delivery_date}), пропускаем")
                return
        except Exception:
            pass

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
