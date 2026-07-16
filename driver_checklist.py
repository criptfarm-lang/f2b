"""
Чеклист водителя на точке доставки (Telegram, бот «Эф»).

План: F2B второй мозг/plans/2026-07-14-чеклист-водителя-приёмка-на-точке.md
Память: project_f2b_driver_checklist

Поток (MVP):
  /рейс → список отгрузок дня (кнопки, пагинация) → «Прибыл» на точке →
  чеклист: [деньги, если розничная касса и не образец] → документ → сдано/претензия.
  «Нет» на деньгах/документе и претензия → сигнал логисту (Белякова).
  Претензия → текст + фото → алерт логисту + собственнику + ответственному менеджеру.

Состояние чеклиста живёт в Postgres (public.delivery_checklist), НЕ в памяти —
чтобы переживать обрывы связи и рестарт процесса. Результат НЕ пишется в МойСклад
(read-only), а питает клиентский трекинг FISHек.
"""

import os
import io
import logging
from datetime import datetime, timezone, timedelta

import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ContextTypes, filters,
)

from moysklad import MS_BASE, get_headers, PDZ_MANAGER_TG_IDS

logger = logging.getLogger(__name__)

_MSK = timezone(timedelta(hours=3))

# Порог образца: если максимальная цена позиции ≤ 1 ₽ — это образец (маркер собственника).
_SAMPLE_MAX_PRICE_RUB = 1.0
# Розничная касса: имя контрагента начинается с этой строки.
_RETAIL_PREFIX = "Розничный покупатель"
# Сколько точек на странице списка.
_PAGE_SIZE = 8


# ─── Конфиг получателей (env, с дефолтами) ──────────────────────────────────

def _owner_chat_id() -> int:
    return int(os.getenv("OWNER_CHAT_ID", "0") or 0)


def _logist_chat_id() -> int:
    # Логист = Александра Белякова (решение собственника 2026-07-14).
    return int(os.getenv("LOGIST_CHAT_ID", "8267564735") or 0)


_DB = None  # ссылка на БД для whitelist водителей (ставится в register)


def _driver_chat_ids() -> set:
    """Whitelist водителей: таблица `drivers` (active) + env DRIVER_CHAT_IDS + владелец.
    Основной способ завести водителя — строка в `drivers` (без ребилда). env — легаси/бэкап."""
    ids = set()
    raw = os.getenv("DRIVER_CHAT_IDS", "")
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            ids.add(int(part))
    if _DB is not None:
        try:
            for r in (_DB._fetchall("SELECT chat_id FROM drivers WHERE active") or []):
                ids.add(int(r["chat_id"]))
        except Exception as e:
            logger.warning("drivers whitelist read: %s", e)
    owner = _owner_chat_id()
    if owner:
        ids.add(owner)
    return ids


def _is_driver(chat_id: int) -> bool:
    return chat_id in _driver_chat_ids()


def _driver_unit_id(chat_id: int):
    """Юнит Wialon, закреплённый за водителем (drivers.unit_id). None → фильтра нет."""
    if _DB is None or not chat_id:
        return None
    try:
        r = _DB._fetchone("SELECT unit_id FROM drivers WHERE chat_id=%s AND active", (chat_id,))
        return int(r["unit_id"]) if r and r.get("unit_id") else None
    except Exception as e:
        logger.warning("driver unit_id read: %s", e)
        return None


# ─── Схема БД ────────────────────────────────────────────────────────────────

def ensure_schema(db):
    """Создаёт таблицу чеклиста (идемпотентно). Вызывать один раз при старте."""
    db._execute("""
        CREATE TABLE IF NOT EXISTS delivery_checklist (
            demand_id        TEXT PRIMARY KEY,
            demand_name      TEXT,
            agent_id         TEXT,
            agent_name       TEXT,
            address          TEXT,
            sum_rub          NUMERIC,
            is_retail        BOOLEAN,
            is_sample        BOOLEAN,
            money_required   BOOLEAN,
            manager_tag      TEXT,
            driver_chat_id   BIGINT,
            snap_date        DATE,
            stage            TEXT,
            money_received   BOOLEAN,
            doc_signed       BOOLEAN,
            accepted_ok      BOOLEAN,
            claim_text       TEXT,
            claim_photo_file_id TEXT,
            status           TEXT,
            arrived_at       TIMESTAMPTZ DEFAULT now(),
            completed_at     TIMESTAMPTZ,
            updated_at       TIMESTAMPTZ DEFAULT now()
        )
    """)
    db._execute("""
        CREATE TABLE IF NOT EXISTS drivers (
            chat_id   BIGINT PRIMARY KEY,
            name      TEXT,
            active    BOOLEAN DEFAULT TRUE,
            added_at  TIMESTAMPTZ DEFAULT now()
        )
    """)
    # unit_id — юнит Wialon (26209/26210), чтобы /рейс показывал водителю ТОЛЬКО его машину.
    # NULL → водитель видит полный список дня (фолбэк).
    db._execute("ALTER TABLE drivers ADD COLUMN IF NOT EXISTS unit_id BIGINT")
    logger.info("delivery_checklist: схема готова")


def _get_row(db, demand_id: str):
    return db._fetchone(
        "SELECT * FROM delivery_checklist WHERE demand_id = %s", (demand_id,)
    )


def _set_fields(db, demand_id: str, **fields):
    if not fields:
        return
    cols = ", ".join(f"{k} = %s" for k in fields)
    params = list(fields.values()) + [demand_id]
    db._execute(
        f"UPDATE delivery_checklist SET {cols}, updated_at = now() WHERE demand_id = %s",
        params,
    )


# ─── МойСклад (read-only) ────────────────────────────────────────────────────

def _msk_today_bounds():
    now = datetime.now(_MSK)
    d = now.strftime("%Y-%m-%d")
    return f"{d} 00:00:00", f"{d} 23:59:59", now.date()


async def _fetch_today_demands() -> list:
    """Отгрузки за сегодня (МСК): [{id, name, agent_name, address, sum_rub}]."""
    lo, hi, _ = _msk_today_bounds()
    headers = get_headers()
    out = []
    offset = 0
    async with aiohttp.ClientSession() as session:
        while True:
            params = {
                "filter": f"moment>={lo};moment<={hi}",
                "expand": "agent",
                "order": "moment,asc",
                "limit": 100,
                "offset": offset,
            }
            async with session.get(f"{MS_BASE}/entity/demand", headers=headers, params=params) as r:
                if r.status != 200:
                    logger.warning("_fetch_today_demands: HTTP %s", r.status)
                    break
                data = await r.json()
            rows = data.get("rows", [])
            for x in rows:
                ag = x.get("agent") or {}
                out.append({
                    "id": x.get("id"),
                    "name": x.get("name"),
                    "agent_name": ag.get("name") if isinstance(ag, dict) else None,
                    "agent_id": (ag.get("id") if isinstance(ag, dict) else None),
                    "address": x.get("shipmentAddress"),
                    "sum_rub": (x.get("sum", 0) or 0) / 100,
                })
            if len(rows) < 100:
                break
            offset += 100
    return out


async def _fetch_demand_detail(demand_id: str) -> dict:
    """Один demand + позиции: считает is_sample и собирает состав."""
    headers = get_headers()
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{MS_BASE}/entity/demand/{demand_id}",
            headers=headers, params={"expand": "agent"},
        ) as r:
            if r.status != 200:
                return {}
            d = await r.json()
        async with session.get(
            f"{MS_BASE}/entity/demand/{demand_id}/positions",
            headers=headers, params={"expand": "assortment", "limit": 100},
        ) as r:
            pos = (await r.json()).get("rows", []) if r.status == 200 else []

    ag = d.get("agent") or {}
    prices = [(p.get("price", 0) or 0) / 100 for p in pos]
    lines = []
    for p in pos:
        nm = (p.get("assortment") or {}).get("name", "?")
        qty = p.get("quantity", 0) or 0
        pr = (p.get("price", 0) or 0) / 100
        lines.append(f"• {nm} — {qty:g} × {pr:,.0f}".replace(",", " "))
    return {
        "agent_id": ag.get("id"),
        "agent_name": ag.get("name"),
        "address": d.get("shipmentAddress"),
        "sum_rub": (d.get("sum", 0) or 0) / 100,
        "name": d.get("name"),
        "max_price_rub": max(prices) if prices else 0,
        "positions_text": "\n".join(lines),
    }


async def _fetch_agent_tags(agent_id: str) -> list:
    if not agent_id:
        return []
    headers = get_headers()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{MS_BASE}/entity/counterparty/{agent_id}", headers=headers) as r:
                if r.status == 200:
                    return (await r.json()).get("tags", []) or []
    except Exception as e:
        logger.warning("_fetch_agent_tags: %s", e)
    return []


# ─── Хелперы ─────────────────────────────────────────────────────────────────

def _is_retail(agent_name: str) -> bool:
    return bool(agent_name) and agent_name.strip().startswith(_RETAIL_PREFIX)


def _fmt_rub(v) -> str:
    return f"{float(v or 0):,.0f}".replace(",", " ") + " ₽"


async def _resolve_manager_chat(agent_id: str) -> tuple:
    """Возвращает (tag, chat_id|None) ответственного менеджера по тегам контрагента."""
    tags = await _fetch_agent_tags(agent_id)
    for t in tags:
        key = (t or "").strip().lower()
        if key in PDZ_MANAGER_TG_IDS:
            return t, PDZ_MANAGER_TG_IDS[key]
    return None, None


async def _send_alert(context, chat_ids, text, photo_file_id=None):
    seen = set()
    for cid in chat_ids:
        if not cid or cid in seen:
            continue
        seen.add(cid)
        try:
            if photo_file_id:
                await context.bot.send_photo(chat_id=cid, photo=photo_file_id, caption=text, parse_mode="Markdown")
            else:
                await context.bot.send_message(chat_id=cid, text=text, parse_mode="Markdown")
        except Exception as e:
            logger.warning("_send_alert → %s: %s", cid, e)


# ─── Экран: список точек ─────────────────────────────────────────────────────

async def cmd_reis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    if not _is_driver(user.id):
        await update.message.reply_text("⛔ Команда доступна водителям развозки.")
        return
    await _render_points(update.message.reply_text, page=0, driver_id=user.id)


def _nav_row(page: int, pages: int):
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("« Назад", callback_data=f"drv:pg:{page-1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Далее »", callback_data=f"drv:pg:{page+1}"))
    return nav


async def _render_points(send, page: int, driver_id: int = None):
    # Если за водителем закреплён юнит (drivers.unit_id) и на сегодня есть маршрут —
    # показываем ТОЛЬКО его машину в порядке выгрузки. Иначе — полный список дня (фолбэк).
    unit_id = _driver_unit_id(driver_id) if driver_id else None
    # Гейт: пока логист не подтвердил маршрут этой машины на сегодня — точки не отдаём.
    if unit_id:
        try:
            import route_dispatch
            if not route_dispatch.is_confirmed_today(unit_id):
                await send("🕓 Маршрут ещё не подтверждён логистом. Как подтвердит — точки придут сюда.")
                return
        except Exception as e:
            logger.warning("gate check упал, пропускаю гейт: %s", e)
    route_stops = None
    if unit_id:
        try:
            import route_registry as rr
            routes = await rr.fetch_routes()
            today = datetime.now(_MSK).date()
            def _stop_today(s):
                ref = s.get("tf") or s.get("vt") or s.get("tt")
                if not ref:
                    return True
                return datetime.fromtimestamp(ref, _MSK).date() == today
            route_stops = sorted(
                [s for s in (routes.get(unit_id) or []) if _stop_today(s)],
                key=lambda s: (s.get("seq") if s.get("seq") is not None else 999))
        except Exception as e:
            logger.warning("route filter упал, фолбэк на полный список: %s", e)
            route_stops = None

    if route_stops:
        import route_registry as rr
        total = len(route_stops)
        pages = (total + _PAGE_SIZE - 1) // _PAGE_SIZE
        page = max(0, min(page, pages - 1))
        chunk = route_stops[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE]
        buttons = []
        for i, s in enumerate(chunk, start=page * _PAGE_SIZE + 1):
            title = (s.get("client") or s.get("order_no") or "?")[:40]
            buttons.append([InlineKeyboardButton(
                f"📍 {i}. {title}", callback_data=f"drv:rp:{s['order_no']}")])
        nav = _nav_row(page, pages)
        if nav:
            buttons.append(nav)
        unit_name = rr.UNITS.get(unit_id, "")
        text = (f"🚚 Твой маршрут ({unit_name}) — {total} точек, порядок выгрузки. "
                f"Стр. {page+1}/{pages}.\nВыбери точку, куда приехал:")
        await send(text, reply_markup=InlineKeyboardMarkup(buttons))
        return

    demands = await _fetch_today_demands()
    if not demands:
        await send("На сегодня отгрузок нет.")
        return
    total = len(demands)
    pages = (total + _PAGE_SIZE - 1) // _PAGE_SIZE
    page = max(0, min(page, pages - 1))
    chunk = demands[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE]

    buttons = []
    for d in chunk:
        title = (d["agent_name"] or d["name"] or "?")[:45]
        buttons.append([InlineKeyboardButton(f"📍 {title}", callback_data=f"drv:pick:{d['id']}")])

    nav = _nav_row(page, pages)
    if nav:
        buttons.append(nav)

    text = f"🚚 Точки на сегодня ({total}). Стр. {page+1}/{pages}.\nВыбери точку, куда приехал:"
    await send(text, reply_markup=InlineKeyboardMarkup(buttons))


async def cb_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    page = int(q.data.split(":")[2])
    await _render_points(lambda *a, **k: q.edit_message_text(*a, **k),
                         page=page, driver_id=q.from_user.id)


# ─── Экран: карточка точки ───────────────────────────────────────────────────

async def _render_card(send, demand_id: str, db, with_back: bool = True):
    row = _get_row(db, demand_id)
    if row and row.get("status"):
        await send(f"✅ Точка уже закрыта: {row.get('agent_name')} — {row['status']}.")
        return
    det = await _fetch_demand_detail(demand_id)
    if not det:
        await send("Не удалось загрузить отгрузку. Попробуй ещё раз.")
        return
    lines = [
        f"*{det.get('agent_name') or '?'}*",
        f"Отгрузка № {det.get('name') or '?'}",
        f"Адрес: {det.get('address') or '—'}",
        f"Сумма: {_fmt_rub(det.get('sum_rub'))}",
    ]
    if det.get("positions_text"):
        lines.append("\n" + det["positions_text"])
    rows = [
        [InlineKeyboardButton("📍 Прибыл — начать сдачу", callback_data=f"drv:arrive:{demand_id}")],
        [InlineKeyboardButton("🔳 QR точки", callback_data=f"drv:qr:{demand_id}")],
    ]
    if with_back:
        rows.append([InlineKeyboardButton("« К списку", callback_data="drv:pg:0")])
    await send("\n".join(lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(rows))


async def cb_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    demand_id = q.data.split(":", 2)[2]
    await _render_card(q.edit_message_text, demand_id, context.bot_data["db"])


async def cb_route_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор точки из маршрута машины: callback несёт № заказа → резолвим в отгрузку."""
    q = update.callback_query
    await q.answer()
    order_no = q.data.split(":", 2)[2]
    demand_id = await _resolve_deeplink_payload(order_no)
    if not demand_id:
        await q.edit_message_text(
            f"По точке №{order_no} отгрузка ещё не создана. Открой список: /рейс")
        return
    await _render_card(q.edit_message_text, demand_id, context.bot_data["db"])


import re as _re
_UUID_RE = _re.compile(r"^[0-9a-fA-F-]{36}$")


async def _resolve_deeplink_payload(payload: str):
    """QR может кодировать что угодно: id отгрузки, id заказа или номер документа.
    Возвращает demand_id для карточки сдачи (или None)."""
    payload = (payload or "").strip()
    if not payload:
        return None
    headers = get_headers()
    async with aiohttp.ClientSession() as session:
        if _UUID_RE.match(payload):
            # 1) это id отгрузки?
            async with session.get(f"{MS_BASE}/entity/demand/{payload}", headers=headers) as r:
                if r.status == 200:
                    return payload
            # 2) это id заказа → берём его отгрузку
            async with session.get(
                f"{MS_BASE}/entity/customerorder/{payload}", headers=headers
            ) as r:
                if r.status == 200:
                    o = await r.json()
                    for dm in o.get("demands", []) or []:
                        href = (dm.get("meta") or {}).get("href", "")
                        if href:
                            return href.rstrip("/").split("/")[-1]
            return None
        # 3) это номер документа. Сначала как номер ОТГРУЗКИ, потом как номер ЗАКАЗА → его отгрузка.
        import urllib.parse as _up
        f = _up.quote(f"name={payload}")
        async with session.get(
            f"{MS_BASE}/entity/demand?filter={f}&order=moment,desc&limit=1", headers=headers
        ) as r:
            if r.status == 200:
                rows = (await r.json()).get("rows", [])
                if rows:
                    return rows[0].get("id")
        # номер заказа (реестр кодирует № заказа) → берём его отгрузку
        async with session.get(
            f"{MS_BASE}/entity/customerorder?filter={f}&limit=1", headers=headers
        ) as r:
            if r.status == 200:
                rows = (await r.json()).get("rows", [])
                if rows:
                    for dm in rows[0].get("demands", []) or []:
                        href = (dm.get("meta") or {}).get("href", "")
                        if href:
                            return href.rstrip("/").split("/")[-1]
    return None


async def open_from_deeplink(update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str):
    """Вход по QR-диплинку: /start chk_<id|номер> → карточка сдачи сразу, без списка.
    payload — id отгрузки, id заказа или номер документа (резолвим к отгрузке)."""
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    if not _is_driver(user.id):
        await update.message.reply_text("⛔ Сдача груза доступна водителям развозки.")
        return
    demand_id = await _resolve_deeplink_payload(payload)
    if not demand_id:
        await update.message.reply_text("Не удалось найти отгрузку по коду. Открой список: /рейс")
        return
    await _render_card(update.message.reply_text, demand_id, context.bot_data["db"], with_back=False)


def _qr_png(data: str) -> bytes:
    import qrcode
    img = qrcode.make(data)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


async def cb_qr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    demand_id = q.data.split(":", 2)[2]
    me = await context.bot.get_me()
    link = f"https://t.me/{me.username}?start=chk_{demand_id}"
    try:
        png = _qr_png(link)
        await context.bot.send_photo(
            chat_id=q.from_user.id, photo=io.BytesIO(png),
            caption=f"QR точки. Скан любой камерой → откроется сдача груза.\n{link}",
        )
    except Exception as e:
        logger.warning("cb_qr: %s", e)
        await context.bot.send_message(chat_id=q.from_user.id, text=f"Ссылка точки:\n{link}")


# ─── Чеклист ─────────────────────────────────────────────────────────────────

async def cb_arrive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    demand_id = q.data.split(":", 2)[2]
    db = context.bot_data["db"]
    driver_id = q.from_user.id

    det = await _fetch_demand_detail(demand_id)
    if not det:
        await q.edit_message_text("Не удалось загрузить отгрузку.")
        return

    is_retail = _is_retail(det.get("agent_name"))
    is_sample = det.get("max_price_rub", 0) <= _SAMPLE_MAX_PRICE_RUB
    money_required = is_retail and (not is_sample) and det.get("sum_rub", 0) > 0
    mtag, _mchat = await _resolve_manager_chat(det.get("agent_id"))
    _, _, snap = _msk_today_bounds()

    # upsert
    db._execute("""
        INSERT INTO delivery_checklist
          (demand_id, demand_name, agent_id, agent_name, address, sum_rub,
           is_retail, is_sample, money_required, manager_tag, driver_chat_id,
           snap_date, stage, arrived_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
        ON CONFLICT (demand_id) DO UPDATE SET
           driver_chat_id = EXCLUDED.driver_chat_id,
           money_required = EXCLUDED.money_required,
           stage = EXCLUDED.stage,
           updated_at = now()
    """, (
        demand_id, det.get("name"), det.get("agent_id"), det.get("agent_name"),
        det.get("address"), det.get("sum_rub"), is_retail, is_sample, money_required,
        mtag, driver_id, snap, "money" if money_required else "doc",
    ))

    if money_required:
        await _ask_money(q, demand_id, det.get("sum_rub"))
    else:
        await _ask_doc(q, demand_id)


async def _ask_money(q, demand_id, sum_rub):
    kb = [[InlineKeyboardButton("✅ Да", callback_data=f"drv:money:yes:{demand_id}"),
           InlineKeyboardButton("❌ Нет", callback_data=f"drv:money:no:{demand_id}")]]
    await q.edit_message_text(
        f"💵 Деньги приняты? ({_fmt_rub(sum_rub)})",
        reply_markup=InlineKeyboardMarkup(kb),
    )


async def _ask_doc(q, demand_id):
    kb = [[InlineKeyboardButton("✅ Да", callback_data=f"drv:doc:yes:{demand_id}"),
           InlineKeyboardButton("❌ Нет", callback_data=f"drv:doc:no:{demand_id}")]]
    await q.edit_message_text("✍️ Документ подписан?", reply_markup=InlineKeyboardMarkup(kb))


async def _ask_accept(q, demand_id):
    kb = [[InlineKeyboardButton("✅ Сдано", callback_data=f"drv:acc:ok:{demand_id}")],
          [InlineKeyboardButton("⚠️ Есть претензия", callback_data=f"drv:acc:claim:{demand_id}")]]
    await q.edit_message_text("📦 Сдано без претензий?", reply_markup=InlineKeyboardMarkup(kb))


async def cb_money(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, _, val, demand_id = q.data.split(":", 3)
    db = context.bot_data["db"]
    received = (val == "yes")
    _set_fields(db, demand_id, money_received=received, stage="doc")
    if not received:
        row = _get_row(db, demand_id)
        await _signal_logist(context, row, "💵 деньги НЕ приняты")
    await _ask_doc(q, demand_id)


async def cb_doc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, _, val, demand_id = q.data.split(":", 3)
    db = context.bot_data["db"]
    signed = (val == "yes")
    _set_fields(db, demand_id, doc_signed=signed, stage="accept")
    if not signed:
        row = _get_row(db, demand_id)
        await _signal_logist(context, row, "✍️ документ НЕ подписан")
    await _ask_accept(q, demand_id)


async def cb_accept(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, _, val, demand_id = q.data.split(":", 3)
    db = context.bot_data["db"]
    if val == "ok":
        _set_fields(db, demand_id, accepted_ok=True, status="сдан",
                    stage="done", completed_at=datetime.now(_MSK))
        try:
            import delivery_statuses as _dsx
            await _dsx.write_ms_status(demand_id, "Сдан")
        except Exception as e:
            logger.warning("МС статус Сдан: %s", e)
        try:
            import route_dispatch
            await route_dispatch.mark_done_and_refresh(context, q.from_user.id, demand_id)
        except Exception as e:
            logger.warning("route refresh (accept): %s", e)
        await q.edit_message_text("✅ Точка закрыта: сдано без претензий. Спасибо!")
    else:
        _set_fields(db, demand_id, accepted_ok=False, stage="claim_text")
        await q.edit_message_text("⚠️ Опиши претензию одним сообщением (текст).")


# ─── Ветка «претензия»: текст → фото → алерт ────────────────────────────────

def _driver_awaiting(db, chat_id, stage):
    return db._fetchone(
        "SELECT demand_id FROM delivery_checklist "
        "WHERE driver_chat_id = %s AND stage = %s ORDER BY updated_at DESC LIMIT 1",
        (chat_id, stage),
    )


async def handle_claim_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = context.bot_data["db"]
    row = _driver_awaiting(db, update.effective_user.id, "claim_text")
    if not row:
        return
    demand_id = row["demand_id"]
    _set_fields(db, demand_id, claim_text=(update.message.text or "").strip(), stage="claim_photo")
    kb = [[InlineKeyboardButton("Без фото", callback_data=f"drv:claimnophoto:{demand_id}")]]
    await update.message.reply_text("📷 Пришли фото претензии (или «Без фото»).",
                                    reply_markup=InlineKeyboardMarkup(kb))


async def handle_claim_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = context.bot_data["db"]
    row = _driver_awaiting(db, update.effective_user.id, "claim_photo")
    if not row:
        return
    demand_id = row["demand_id"]
    file_id = update.message.photo[-1].file_id
    _set_fields(db, demand_id, claim_photo_file_id=file_id)
    await _finish_claim(context, demand_id, update.message.reply_text)


async def cb_claim_nophoto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    demand_id = q.data.split(":", 2)[2]
    await _finish_claim(context, demand_id, q.edit_message_text)


async def _finish_claim(context, demand_id, reply):
    db = context.bot_data["db"]
    _set_fields(db, demand_id, status="сдан с проблемой", stage="done",
                completed_at=datetime.now(_MSK))
    try:
        import delivery_statuses as _dsx
        await _dsx.write_ms_status(demand_id, "Сдан с проблемой")
    except Exception as e:
        logger.warning("МС статус Сдан с проблемой: %s", e)
    row = _get_row(db, demand_id)
    try:
        import route_dispatch
        drv = (row or {}).get("driver_chat_id")
        if drv:
            await route_dispatch.mark_done_and_refresh(context, int(drv), demand_id)
    except Exception as e:
        logger.warning("route refresh (claim): %s", e)
    await _alert_claim(context, row)
    await reply("⚠️ Претензия зафиксирована. Логист и менеджер уведомлены. Спасибо!")


# ─── Алерты ──────────────────────────────────────────────────────────────────

def _point_head(row) -> str:
    return (f"*{row.get('agent_name') or '?'}*\n"
            f"Отгрузка № {row.get('demand_name') or '?'}\n"
            f"Адрес: {row.get('address') or '—'}")


async def _signal_logist(context, row, reason: str):
    if not row:
        return
    text = f"🚚 Сдача груза — сигнал\n{_point_head(row)}\n\n⚠️ {reason}"
    await _send_alert(context, [_logist_chat_id()], text)


async def _alert_claim(context, row):
    if not row:
        return
    tag = (row.get("manager_tag") or "").strip().lower()
    mgr_chat = PDZ_MANAGER_TG_IDS.get(tag)
    text = (f"⚠️ ПРЕТЕНЗИЯ на доставке\n{_point_head(row)}\n\n"
            f"Описание: {row.get('claim_text') or '—'}")
    if row.get("manager_tag"):
        text += f"\nМенеджер: {row.get('manager_tag')}"
    recipients = [_logist_chat_id(), _owner_chat_id(), mgr_chat]
    await _send_alert(context, recipients, text, photo_file_id=row.get("claim_photo_file_id"))


# ─── Лист отгрузок дня с QR (стопгэп до реестра, план Фаза 5) ────────────────

async def _fetch_shipments_for_list() -> list:
    """Отгрузки за сегодня + данные заказа (окно/места/комментарий) одним запросом
    (expand=agent,customerOrder инлайнит атрибуты заказа)."""
    lo, hi, _ = _msk_today_bounds()
    headers = get_headers()
    out = []
    offset = 0
    async with aiohttp.ClientSession() as session:
        while True:
            params = {
                "filter": f"moment>={lo};moment<={hi}",
                "expand": "agent,customerOrder",
                "order": "moment,asc",
                "limit": 100, "offset": offset,
            }
            async with session.get(f"{MS_BASE}/entity/demand", headers=headers, params=params) as r:
                if r.status != 200:
                    logger.warning("_fetch_shipments_for_list: HTTP %s", r.status)
                    break
                data = await r.json()
            rows = data.get("rows", [])
            for x in rows:
                ag = x.get("agent") or {}
                co = x.get("customerOrder") or {}
                wfrom = wto = places = None
                for a in co.get("attributes", []) or []:
                    n = a.get("name")
                    if n == "Окно доставки с (время)":
                        wfrom = a.get("value")
                    elif n == "Окно доставки до (время)":
                        wto = a.get("value")
                    elif n == "Количество мест":
                        places = a.get("value")
                out.append({
                    "demand_id": x.get("id"),
                    "demand_name": x.get("name"),
                    "agent_name": ag.get("name") if isinstance(ag, dict) else None,
                    "address": x.get("shipmentAddress"),
                    "sum_rub": (x.get("sum", 0) or 0) / 100,
                    "window_from": wfrom, "window_to": wto,
                    "places": places, "comment": co.get("description"),
                })
            if len(rows) < 100:
                break
            offset += 100
    return out


def _time_hm(v):
    if not v:
        return None
    try:
        return v.split(" ")[1][:5]
    except Exception:
        return None


def _build_shipment_list_pdf(shipments, bot_username, date_str) -> bytes:
    from contract_generator import FONT_NORMAL, FONT_BOLD
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.graphics.barcode.qr import QrCodeWidget
    from reportlab.graphics.shapes import Drawing

    def qr_flow(data, size=24 * mm):
        qr = QrCodeWidget(data)
        b = qr.getBounds()
        w = b[2] - b[0]
        h = b[3] - b[1]
        d = Drawing(size, size, transform=[size / w, 0, 0, size / h, 0, 0])
        d.add(qr)
        return d

    body = ParagraphStyle("b", fontName=FONT_NORMAL, fontSize=9, leading=12)
    title = ParagraphStyle("t", fontName=FONT_BOLD, fontSize=14, leading=18, spaceAfter=8)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=14 * mm, bottomMargin=12 * mm,
                            leftMargin=12 * mm, rightMargin=12 * mm)
    flow = [Paragraph(f"Лист отгрузок — {date_str} ({len(shipments)})", title)]
    rows = []
    for i, s in enumerate(shipments, 1):
        wf, wt = _time_hm(s.get("window_from")), _time_hm(s.get("window_to"))
        win = f"Окно: {wf}–{wt}   " if (wf and wt) else ""
        places = f"Мест: {s.get('places')}" if s.get("places") not in (None, "") else ""
        comment = (s.get("comment") or "").strip().replace("\n", " ")
        if len(comment) > 90:
            comment = comment[:90] + "…"
        info = (f"<b>{i}. {s.get('agent_name') or '?'}</b>  (№ {s.get('demand_name') or '?'})<br/>"
                f"{s.get('address') or '—'}<br/>{win}{places}")
        if comment:
            info += f"<br/><font size=8 color='#555555'>{comment}</font>"
        link = f"https://t.me/{bot_username}?start=chk_{s['demand_id']}"
        rows.append([Paragraph(info, body), qr_flow(link)])
    t = Table(rows, colWidths=[150 * mm, 26 * mm])
    t.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LINEBELOW", (0, 0), (-1, -2), 0.4, colors.HexColor("#cccccc")),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    flow.append(t)
    doc.build(flow)
    return buf.getvalue()


async def cmd_shipment_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    if not (_is_driver(user.id) or user.id == _owner_chat_id() or user.id == _logist_chat_id()):
        await update.message.reply_text("⛔ Лист отгрузок доступен логисту и водителям.")
        return
    await update.message.reply_text("Собираю лист отгрузок за сегодня…")
    try:
        shipments = await _fetch_shipments_for_list()
        if not shipments:
            await update.message.reply_text("На сегодня отгрузок нет.")
            return
        me = await context.bot.get_me()
        _, _, snap = _msk_today_bounds()
        pdf = _build_shipment_list_pdf(shipments, me.username, snap.strftime("%d.%m.%Y"))
        await context.bot.send_document(
            chat_id=user.id, document=io.BytesIO(pdf),
            filename=f"shipments_{snap.isoformat()}.pdf",
            caption=(f"Лист отгрузок за {snap.strftime('%d.%m.%Y')} — {len(shipments)} точек.\n"
                     "QR по каждой отгрузке → скан открывает сдачу груза."),
        )
    except Exception as e:
        logger.exception("cmd_shipment_list: %s", e)
        await update.message.reply_text("Не удалось собрать лист. Попробуй позже.")


# ─── Фильтры для текста/фото претензии ──────────────────────────────────────

class _ClaimTextFilter(filters.MessageFilter):
    def __init__(self, db):
        super().__init__()
        self._db = db

    def filter(self, message):
        u = getattr(message, "from_user", None)
        c = getattr(message, "chat", None)
        if not u or not c or c.type != "private":
            return False
        return bool(_driver_awaiting(self._db, u.id, "claim_text"))


class _ClaimPhotoFilter(filters.MessageFilter):
    def __init__(self, db):
        super().__init__()
        self._db = db

    def filter(self, message):
        u = getattr(message, "from_user", None)
        c = getattr(message, "chat", None)
        if not u or not c or c.type != "private":
            return False
        return bool(_driver_awaiting(self._db, u.id, "claim_photo"))


# ─── Регистрация ─────────────────────────────────────────────────────────────

def register(app: Application, db):
    """Подключить чеклист водителя. Вызывать в main() ДО catch-all handle_message."""
    global _DB
    _DB = db
    ensure_schema(db)
    app.bot_data["db"] = db

    # /рейс (кириллица → через Regex) + ASCII-alias /reis
    app.add_handler(CommandHandler("reis", cmd_reis))
    app.add_handler(MessageHandler(filters.Regex(r"^/рейс(@\w+)?(\s|$)"), cmd_reis))

    # /лист — лист отгрузок дня с QR (для логиста/склада/водителей) + alias /shipmentlist
    app.add_handler(CommandHandler("shipmentlist", cmd_shipment_list))
    app.add_handler(MessageHandler(filters.Regex(r"^/лист(@\w+)?(\s|$)"), cmd_shipment_list))

    app.add_handler(CallbackQueryHandler(cb_page, pattern=r"^drv:pg:"))
    app.add_handler(CallbackQueryHandler(cb_pick, pattern=r"^drv:pick:"))
    app.add_handler(CallbackQueryHandler(cb_route_pick, pattern=r"^drv:rp:"))
    app.add_handler(CallbackQueryHandler(cb_qr, pattern=r"^drv:qr:"))
    app.add_handler(CallbackQueryHandler(cb_arrive, pattern=r"^drv:arrive:"))
    app.add_handler(CallbackQueryHandler(cb_money, pattern=r"^drv:money:"))
    app.add_handler(CallbackQueryHandler(cb_doc, pattern=r"^drv:doc:"))
    app.add_handler(CallbackQueryHandler(cb_accept, pattern=r"^drv:acc:"))
    app.add_handler(CallbackQueryHandler(cb_claim_nophoto, pattern=r"^drv:claimnophoto:"))

    # Текст/фото претензии — ДО общего handle_message
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & _ClaimTextFilter(db), handle_claim_text))
    app.add_handler(MessageHandler(
        filters.PHOTO & _ClaimPhotoFilter(db), handle_claim_photo))

    logger.info("driver_checklist: хендлеры зарегистрированы")
