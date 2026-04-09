"""
amo_alarms.py — Алармы по воронке РЕКЛАМА в amoCRM
Логика:
  1. Webhook от amoCRM → сделка попала на этап "Новая заявка" → мгновенный алерт в группу PRO
  2. Через 5 минут проверяем:
     - Этап не сменился + нет звонка/сообщения → ⏰ "Висит без ответа"
     - Этап сменился + нет звонка/сообщения   → ⚠️ "Этап сменён без контакта"
     - Был звонок ИЛИ сообщение               → молчим
  3. Кнопка "Взял в работу":
     - Удаляет алерт из группы
     - Отменяет таймер
     - Меняет ответственного в amoCRM (если есть привязка TG → AMO)
"""

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiohttp
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application

logger = logging.getLogger(__name__)

AMO_SUBDOMAIN = os.getenv("AMO_SUBDOMAIN", "victorfishtobiz")
AMO_API_DOMAIN = os.getenv("AMO_API_DOMAIN", "api-b.amocrm.ru")
AMO_BASE_URL = f"https://{AMO_API_DOMAIN}/api/v4"

# ─── Кэш для хранения активных таймеров и алертов ────────────────────────────
# lead_id → {
#   "task": asyncio.Task,          # таймер 5 минут
#   "msg_id": int,                 # message_id алерта в группе
#   "chat_id": int,                # GROUP_CHAT_ID
#   "pipeline_id": int,
#   "stage_id": int,               # ID этапа "Новая заявка"
#   "triggered_at": datetime,      # когда пришёл вебхук
# }
_active_alarms: dict = {}

# Кэш ID воронки и этапа — заполняется при первом запросе
_pipeline_cache: dict = {}  # {"pipeline_id": int, "stage_id": int, "stage_name": str}


# ─── amoCRM API helpers ───────────────────────────────────────────────────────

def _amo_headers() -> dict:
    token = os.getenv("AMO_ACCESS_TOKEN", "")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


async def _amo_get(path: str, params: dict = None) -> Optional[dict]:
    url = f"{AMO_BASE_URL}{path}"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(url, headers=_amo_headers(), params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                logger.warning(f"AMO GET {path}: {resp.status}")
                return None
    except Exception as e:
        logger.error(f"AMO GET {path}: {e}")
        return None


async def _amo_patch(path: str, data: dict) -> Optional[dict]:
    url = f"{AMO_BASE_URL}{path}"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.patch(url, headers=_amo_headers(), json=data) as resp:
                if resp.status in (200, 201):
                    return await resp.json()
                text = await resp.text()
                logger.warning(f"AMO PATCH {path}: {resp.status} {text[:200]}")
                return None
    except Exception as e:
        logger.error(f"AMO PATCH {path}: {e}")
        return None


# ─── Поиск воронки и этапа ────────────────────────────────────────────────────

async def get_reklama_stage() -> dict:
    """
    Находит воронку РЕКЛАМА и этап НОВАЯ ЗАЯВКА через API.
    Кэширует результат чтобы не дёргать API каждый раз.
    Возвращает {"pipeline_id": int, "stage_id": int, "stage_name": str}
    """
    global _pipeline_cache
    if _pipeline_cache:
        return _pipeline_cache

    data = await _amo_get("/leads/pipelines")
    if not data:
        logger.error("get_reklama_stage: не удалось получить воронки")
        return {}

    pipelines = data.get("_embedded", {}).get("pipelines", [])
    for pipeline in pipelines:
        pipeline_name = pipeline.get("name", "").lower()
        if "реклам" in pipeline_name:
            pipeline_id = pipeline["id"]
            stages = pipeline.get("_embedded", {}).get("statuses", [])
            for stage in stages:
                stage_name = stage.get("name", "").lower()
                if "новая" in stage_name and "заявка" in stage_name:
                    _pipeline_cache = {
                        "pipeline_id": pipeline_id,
                        "stage_id": stage["id"],
                        "stage_name": stage["name"],
                    }
                    logger.info(
                        f"Воронка РЕКЛАМА: pipeline_id={pipeline_id}, "
                        f"stage_id={stage['id']}, stage='{stage['name']}'"
                    )
                    return _pipeline_cache

    logger.error("get_reklama_stage: воронка РЕКЛАМА или этап НОВАЯ ЗАЯВКА не найдены")
    return {}


async def get_all_stage_names(pipeline_id: int) -> dict:
    """Возвращает словарь stage_id → stage_name для воронки."""
    data = await _amo_get(f"/leads/pipelines/{pipeline_id}")
    if not data:
        return {}
    stages = data.get("_embedded", {}).get("statuses", [])
    return {s["id"]: s["name"] for s in stages}


# ─── Получение данных сделки ──────────────────────────────────────────────────

async def get_lead_info(lead_id: int) -> Optional[dict]:
    """Получает данные сделки с контактом и ответственным."""
    data = await _amo_get(
        f"/leads/{lead_id}",
        params={"with": "contacts"}
    )
    return data


async def get_lead_contact_name(lead_id: int) -> str:
    """Возвращает имя основного контакта сделки."""
    data = await _amo_get(
        f"/leads/{lead_id}",
        params={"with": "contacts"}
    )
    if not data:
        return "Неизвестный клиент"

    contacts = data.get("_embedded", {}).get("contacts", [])
    if contacts:
        contact_id = contacts[0]["id"]
        contact_data = await _amo_get(f"/contacts/{contact_id}")
        if contact_data:
            return contact_data.get("name", "Неизвестный клиент")
    return "Неизвестный клиент"


async def get_responsible_user_name(user_id: int) -> str:
    """Возвращает имя ответственного пользователя."""
    data = await _amo_get(f"/users/{user_id}")
    if data:
        return data.get("name", "Неизвестный менеджер")
    return "Неизвестный менеджер"


# ─── Проверка активности по сделке ───────────────────────────────────────────

async def check_lead_activity(lead_id: int, since: datetime) -> dict:
    """
    Проверяет была ли активность по сделке с момента since.
    Возвращает:
    {
        "has_call": bool,
        "has_message": bool,
        "current_stage_id": int,
        "is_closed": bool,       # закрыта в нереализованные
        "current_stage_name": str,
    }
    """
    result = {
        "has_call": False,
        "has_message": False,
        "current_stage_id": None,
        "is_closed": False,
        "current_stage_name": "",
    }

    since_ts = int(since.timestamp())

    # 1. Текущий статус сделки
    lead_data = await _amo_get(f"/leads/{lead_id}")
    if lead_data:
        result["current_stage_id"] = lead_data.get("status_id")
        result["is_closed"] = lead_data.get("closed_at") is not None or lead_data.get("loss_reason_id") is not None

        # Получаем название текущего этапа
        pipeline_id = lead_data.get("pipeline_id")
        if pipeline_id:
            stage_names = await get_all_stage_names(pipeline_id)
            result["current_stage_name"] = stage_names.get(result["current_stage_id"], "")

    # 2. События по сделке — звонки и сообщения
    events_data = await _amo_get(
        "/events",
        params={
            "filter[entity]": "lead",
            "filter[entity_id]": lead_id,
            "filter[created_at][from]": since_ts,
            "limit": 50,
        }
    )
    if events_data:
        events = events_data.get("_embedded", {}).get("events", [])
        for event in events:
            event_type = event.get("type", "")
            # Звонки
            if event_type in ("incoming_call", "outgoing_call", "call"):
                result["has_call"] = True
            # Сообщения — входящие и исходящие через amoCRM чат / Wazzup
            if event_type in (
                "incoming_chat_message", "outgoing_chat_message",
                "message_received", "message_sent",
                "chat_message",
            ):
                result["has_message"] = True

    logger.info(
        f"check_lead_activity lead_id={lead_id}: "
        f"call={result['has_call']} msg={result['has_message']} "
        f"stage_id={result['current_stage_id']} closed={result['is_closed']}"
    )
    return result


# ─── Смена ответственного ─────────────────────────────────────────────────────

async def set_lead_responsible(lead_id: int, amo_user_id: int) -> bool:
    """Меняет ответственного в сделке."""
    result = await _amo_patch(
        f"/leads/{lead_id}",
        {"responsible_user_id": amo_user_id}
    )
    return result is not None


async def get_amo_user_id_by_tg(tg_user_id: int, db) -> Optional[int]:
    """
    Ищет amoCRM user_id по Telegram user_id в БД.
    Таблица manager_chats должна иметь колонку amo_user_id.
    """
    try:
        row = db._fetchone(
            "SELECT amo_user_id FROM manager_chats WHERE user_id = %s",
            (tg_user_id,)
        )
        if row and row.get("amo_user_id"):
            return int(row["amo_user_id"])
    except Exception as e:
        logger.warning(f"get_amo_user_id_by_tg: {e}")
    return None


# ─── Формирование сообщений ───────────────────────────────────────────────────

def _lead_url(lead_id: int) -> str:
    return f"https://{AMO_SUBDOMAIN}.amocrm.ru/leads/detail/{lead_id}"


def _make_keyboard(lead_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "✅ Взял в работу",
            callback_data=f"amo_alarm_take|{lead_id}"
        )
    ]])


# ─── Основные функции алармов ─────────────────────────────────────────────────

async def send_first_alarm(
    lead_id: int,
    client_name: str,
    manager_name: str,
    app: Application,
    group_chat_id: int,
):
    """Отправляет первый мгновенный алерт в группу PRO."""
    text = (
        f"⚡️ *Новая заявка!*\n\n"
        f"👤 Клиент: *{client_name}*\n"
        f"👔 Менеджер: *{manager_name}*\n"
        f"🔗 {_lead_url(lead_id)}"
    )
    try:
        msg = await app.bot.send_message(
            chat_id=group_chat_id,
            text=text,
            parse_mode="Markdown",
            reply_markup=_make_keyboard(lead_id),
        )
        logger.info(f"Первый алерт отправлен: lead_id={lead_id} msg_id={msg.message_id}")
        return msg.message_id
    except Exception as e:
        logger.error(f"send_first_alarm: {e}")
        return None


async def _run_second_alarm_timer(
    lead_id: int,
    triggered_at: datetime,
    app: Application,
    group_chat_id: int,
    db,
):
    """
    Ждёт 5 минут, затем проверяет активность и отправляет второй алерт если нужно.
    """
    await asyncio.sleep(5 * 60)  # 5 минут

    # Если таймер был отменён через "Взял в работу" — выходим
    alarm = _active_alarms.get(lead_id)
    if not alarm:
        logger.info(f"Таймер lead_id={lead_id}: алерт уже закрыт, выходим")
        return

    activity = await check_lead_activity(lead_id, since=triggered_at)
    stage_info = _pipeline_cache  # {"stage_id": int, ...}
    new_stage_id = activity["current_stage_id"]
    original_stage_id = stage_info.get("stage_id")
    has_contact = activity["has_call"] or activity["has_message"]

    # Получаем имена для алерта
    lead_data = await _amo_get(f"/leads/{lead_id}", params={"with": "contacts"})
    client_name = "Неизвестный клиент"
    manager_name = "Неизвестный менеджер"
    if lead_data:
        contacts = lead_data.get("_embedded", {}).get("contacts", [])
        if contacts:
            c = await _amo_get(f"/contacts/{contacts[0]['id']}")
            if c:
                client_name = c.get("name", client_name)
        resp_id = lead_data.get("responsible_user_id")
        if resp_id:
            manager_name = await get_responsible_user_name(resp_id)

    text = None

    if has_contact:
        # Был звонок или сообщение — молчим
        logger.info(f"Таймер lead_id={lead_id}: был контакт, второй алерт не нужен")
        _active_alarms.pop(lead_id, None)
        return

    elif new_stage_id == original_stage_id:
        # Этап не сменился, нет контакта
        text = (
            f"⏰ *Заявка висит без ответа уже 5 минут!*\n\n"
            f"👤 Клиент: *{client_name}*\n"
            f"👔 Менеджер: *{manager_name}*\n"
            f"🔗 {_lead_url(lead_id)}"
        )

    else:
        # Этап сменился без контакта
        new_stage_name = activity.get("current_stage_name", "")
        text = (
            f"⚠️ *Этап сменён без контакта с клиентом!*\n\n"
            f"👤 Клиент: *{client_name}*\n"
            f"👔 Менеджер: *{manager_name}*\n"
            f"📋 Новый этап: *{new_stage_name}*\n"
            f"🔗 {_lead_url(lead_id)}"
        )

    if text:
        try:
            msg = await app.bot.send_message(
                chat_id=group_chat_id,
                text=text,
                parse_mode="Markdown",
                reply_markup=_make_keyboard(lead_id),
            )
            # Обновляем msg_id в кэше чтобы кнопка "Взял в работу" удаляла новое сообщение
            if lead_id in _active_alarms:
                _active_alarms[lead_id]["msg_id"] = msg.message_id
            logger.info(f"Второй алерт отправлен: lead_id={lead_id} msg_id={msg.message_id}")
        except Exception as e:
            logger.error(f"_run_second_alarm_timer send: {e}")

    _active_alarms.pop(lead_id, None)


def start_alarm_timer(
    lead_id: int,
    msg_id: int,
    triggered_at: datetime,
    app: Application,
    group_chat_id: int,
    db,
):
    """Запускает таймер 5 минут для второго алерта."""
    # Отменяем предыдущий таймер если был
    cancel_alarm_timer(lead_id)

    task = asyncio.create_task(
        _run_second_alarm_timer(lead_id, triggered_at, app, group_chat_id, db)
    )
    _active_alarms[lead_id] = {
        "task": task,
        "msg_id": msg_id,
        "chat_id": group_chat_id,
        "triggered_at": triggered_at,
    }
    logger.info(f"Таймер запущен: lead_id={lead_id}")


def cancel_alarm_timer(lead_id: int):
    """Отменяет таймер и удаляет из кэша."""
    alarm = _active_alarms.pop(lead_id, None)
    if alarm and alarm.get("task"):
        alarm["task"].cancel()
        logger.info(f"Таймер отменён: lead_id={lead_id}")


# ─── Обработчик webhook от amoCRM ─────────────────────────────────────────────

async def handle_amo_webhook(request, app: Application, db):
    """
    Принимает webhook от amoCRM.
    В amoCRM настрой webhook на события: leads[status] (смена этапа).
    URL: https://f2b-production.up.railway.app/webhook/amocrm
    """
    try:
        # amoCRM шлёт form-data (не JSON)
        data = await request.post()

        group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
        if not group_chat_id:
            logger.error("handle_amo_webhook: GROUP_CHAT_ID не задан")
            return

        # Получаем ID воронки/этапа "Новая заявка" (с кэшем)
        stage_info = await get_reklama_stage()
        if not stage_info:
            logger.error("handle_amo_webhook: не удалось получить stage_info")
            return

        target_pipeline_id = stage_info["pipeline_id"]
        target_stage_id = stage_info["stage_id"]

        # amoCRM шлёт данные в формате leads[status][0][id] и т.д.
        # Парсим все leads из формы
        leads_raw = {}
        for key, value in data.items():
            # Пример ключа: leads[status][0][id]
            if key.startswith("leads[status]"):
                import re
                m = re.match(r"leads\[status\]\[(\d+)\]\[(.+)\]", key)
                if m:
                    idx = int(m.group(1))
                    field = m.group(2)
                    if idx not in leads_raw:
                        leads_raw[idx] = {}
                    leads_raw[idx][field] = value

        logger.info(f"AMO webhook: получено {len(leads_raw)} событий")

        for idx, lead_fields in leads_raw.items():
            lead_id = int(lead_fields.get("id", 0))
            pipeline_id = int(lead_fields.get("pipeline_id", 0))
            status_id = int(lead_fields.get("status_id", 0))

            if not lead_id:
                continue

            logger.info(
                f"AMO webhook lead_id={lead_id}: "
                f"pipeline={pipeline_id} stage={status_id} "
                f"target_pipeline={target_pipeline_id} target_stage={target_stage_id}"
            )

            # Проверяем что это именно "Новая заявка" в воронке "Реклама"
            if pipeline_id == target_pipeline_id and status_id == target_stage_id:
                # Уже обрабатываем эту сделку — игнорируем дубль
                if lead_id in _active_alarms:
                    logger.info(f"AMO webhook: lead_id={lead_id} уже в обработке, пропускаем")
                    continue

                # Получаем данные сделки
                lead_data = await _amo_get(
                    f"/leads/{lead_id}",
                    params={"with": "contacts"}
                )
                if not lead_data:
                    logger.error(f"AMO webhook: не удалось получить сделку {lead_id}")
                    continue

                # Имя клиента
                client_name = "Неизвестный клиент"
                contacts = lead_data.get("_embedded", {}).get("contacts", [])
                if contacts:
                    c = await _amo_get(f"/contacts/{contacts[0]['id']}")
                    if c:
                        client_name = c.get("name", client_name)

                # Имя менеджера
                manager_name = "Неизвестный менеджер"
                resp_id = lead_data.get("responsible_user_id")
                if resp_id:
                    manager_name = await get_responsible_user_name(resp_id)

                triggered_at = datetime.now(timezone.utc)

                # Отправляем первый алерт
                msg_id = await send_first_alarm(
                    lead_id=lead_id,
                    client_name=client_name,
                    manager_name=manager_name,
                    app=app,
                    group_chat_id=group_chat_id,
                )

                # Запускаем таймер 5 минут
                if msg_id:
                    start_alarm_timer(
                        lead_id=lead_id,
                        msg_id=msg_id,
                        triggered_at=triggered_at,
                        app=app,
                        group_chat_id=group_chat_id,
                        db=db,
                    )

    except Exception as e:
        logger.error(f"handle_amo_webhook: {e}", exc_info=True)


# ─── Обработчик кнопки "Взял в работу" ───────────────────────────────────────

async def handle_take_callback(query, db):
    """
    Обрабатывает нажатие кнопки "Взял в работу".
    query — CallbackQuery из python-telegram-bot.
    """
    await query.answer()

    parts = query.data.split("|")
    lead_id = int(parts[1]) if len(parts) > 1 else 0
    if not lead_id:
        return

    tg_user = query.from_user
    logger.info(f"handle_take_callback: lead_id={lead_id} tg_user={tg_user.id} ({tg_user.full_name})")

    # Отменяем таймер
    cancel_alarm_timer(lead_id)

    # Меняем ответственного в amoCRM если есть привязка
    amo_user_id = await get_amo_user_id_by_tg(tg_user.id, db)
    if amo_user_id:
        success = await set_lead_responsible(lead_id, amo_user_id)
        if success:
            logger.info(f"Ответственный сменён: lead_id={lead_id} → amo_user_id={amo_user_id}")
        else:
            logger.warning(f"Не удалось сменить ответственного: lead_id={lead_id}")

    # Удаляем сообщение алерта
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"handle_take_callback delete: {e}")

    # Если нет привязки — напоминаем настроить
    if not amo_user_id:
        try:
            await query.message.reply_text(
                f"✅ *{tg_user.full_name}* взял заявку в работу.\n\n"
                f"💡 Чтобы автоматически становиться ответственным в amoCRM — "
                f"напиши боту /myamoid",
                parse_mode="Markdown"
            )
        except Exception:
            pass


# ─── Команда /myamoid — привязка TG → amoCRM ─────────────────────────────────

async def cmd_myamoid(update, context, db):
    """
    /myamoid — показывает список пользователей amoCRM,
    просит выбрать себя и сохраняет привязку TG user_id → amo_user_id.
    """
    user = update.effective_user
    if not user:
        return

    await update.message.reply_text("🔍 Загружаю список пользователей amoCRM...")

    users_data = await _amo_get("/users", params={"limit": 50})
    if not users_data:
        await update.message.reply_text("❌ Не удалось получить список пользователей amoCRM.")
        return

    amo_users = users_data.get("_embedded", {}).get("users", [])
    if not amo_users:
        await update.message.reply_text("❌ Пользователи amoCRM не найдены.")
        return

    buttons = []
    for u in amo_users:
        amo_id = u["id"]
        name = u.get("name", f"ID {amo_id}")
        buttons.append([InlineKeyboardButton(
            name,
            callback_data=f"amo_link|{user.id}|{amo_id}"
        )])

    await update.message.reply_text(
        f"👤 *{user.full_name}*, выбери себя в amoCRM:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def handle_amo_link_callback(query, db):
    """Сохраняет привязку TG user_id → amo_user_id."""
    await query.answer()
    parts = query.data.split("|")
    tg_user_id = int(parts[1])
    amo_user_id = int(parts[2])

    # Получаем имя amoCRM пользователя
    u_data = await _amo_get(f"/users/{amo_user_id}")
    amo_name = u_data.get("name", str(amo_user_id)) if u_data else str(amo_user_id)

    try:
        db._execute(
            """
            INSERT INTO manager_chats (user_id, amo_user_id)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET amo_user_id = %s
            """,
            (tg_user_id, amo_user_id, amo_user_id)
        )
        await query.message.edit_text(
            f"✅ Привязка сохранена!\n"
            f"Теперь при нажатии «Взял в работу» ты автоматически "
            f"станешь ответственным в amoCRM как *{amo_name}*.",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"handle_amo_link_callback: {e}")
        await query.message.edit_text(f"❌ Ошибка сохранения: {e}")


# ─── Команда /amo_setup — проверка и настройка webhook ───────────────────────

async def cmd_amo_setup(update, context):
    """
    /amo_setup — проверяет подключение к amoCRM и показывает ID воронки/этапа.
    Только для руководителя.
    """
    user = update.effective_user
    if not user or user.id != 360092495:
        return

    await update.message.reply_text("🔍 Проверяю подключение к amoCRM...")

    # 1. Проверяем токен
    account = await _amo_get("/account")
    if not account:
        await update.message.reply_text(
            "❌ Не удалось подключиться к amoCRM.\n"
            "Проверь AMO_ACCESS_TOKEN в Railway."
        )
        return

    # 2. Находим воронку и этап
    stage_info = await get_reklama_stage()

    # 3. Проверяем webhooks
    webhooks_data = await _amo_get("/webhooks")
    webhooks = webhooks_data.get("_embedded", {}).get("webhooks", []) if webhooks_data else []
    webhook_url = "https://f2b-production.up.railway.app/webhook/amocrm"
    webhook_ok = any(w.get("destination", "") == webhook_url for w in webhooks)

    lines = [
        f"✅ amoCRM подключён: *{account.get('name')}*\n",
        f"🔑 Subdomain: `{AMO_SUBDOMAIN}`\n",
    ]

    if stage_info:
        lines.append(
            f"🎯 Воронка РЕКЛАМА:\n"
            f"   pipeline_id: `{stage_info['pipeline_id']}`\n"
            f"   stage_id: `{stage_info['stage_id']}`\n"
            f"   Этап: *{stage_info['stage_name']}*\n"
        )
    else:
        lines.append("⚠️ Воронка РЕКЛАМА или этап НОВАЯ ЗАЯВКА не найдены!\n")

    if webhook_ok:
        lines.append(f"✅ Webhook настроен: `{webhook_url}`")
    else:
        lines.append(
            f"⚠️ Webhook *не найден*!\n"
            f"Настрой в amoCRM:\n"
            f"Настройки → Интеграции → Webhooks\n"
            f"URL: `{webhook_url}`\n"
            f"Событие: *Смена этапа сделки*"
        )

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
