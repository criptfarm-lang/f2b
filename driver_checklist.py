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
import re as _re
import json
import asyncio
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
# Доп.поле заказа МС с пунктами-поручениями водителю (Фаза 7). Тип text, одна задача в строке.
_CHECKLIST_ATTR_NAME = "Чек-лист водителя"
# Максимум пунктов на точку (защита от мусорного ввода) и длина одного пункта.
_MAX_ITEMS = 12
_MAX_ITEM_LEN = 200


# ─── Конфиг получателей (env, с дефолтами) ──────────────────────────────────

def _owner_chat_id() -> int:
    return int(os.getenv("OWNER_CHAT_ID", "0") or 0)


# Логисты (равный доступ + рассылки): 8267564735 Белякова, 1689203038 Петровский.
def _logist_chat_ids() -> list:
    raw = os.getenv("LOGIST_CHAT_IDS") or "8267564735,1689203038"
    out = []
    for p in raw.split(","):
        p = p.strip()
        if p:
            try:
                out.append(int(p))
            except ValueError:
                pass
    return out


def _logist_chat_id() -> int:
    ids = _logist_chat_ids()
    return ids[0] if ids else 0


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


def is_registered_driver(chat_id: int) -> bool:
    """Водитель «по должности» — строка в `drivers` (active) или env DRIVER_CHAT_IDS.

    В отличие от `_is_driver` НЕ включает владельца (у него whitelist-доступ ко всему,
    но меню /start должно оставаться руководительским)."""
    if not chat_id:
        return False
    raw = os.getenv("DRIVER_CHAT_IDS", "")
    for part in raw.split(","):
        if part.strip().isdigit() and int(part.strip()) == chat_id:
            return True
    if _DB is None:
        return False
    try:
        r = _DB._fetchone("SELECT 1 AS x FROM drivers WHERE chat_id=%s AND active", (chat_id,))
        return bool(r)
    except Exception as e:
        logger.warning("is_registered_driver: %s", e)
        return False


def driver_menu_keyboard() -> InlineKeyboardMarkup:
    """Меню водителя (вместо меню ОП): рейс дня + реестр развоза."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚚 Мой рейс сегодня", callback_data="drv:menu:reis")],
        [InlineKeyboardButton("📋 Реестр развоза (PDF)", callback_data="drv:menu:registry")],
    ])


async def cb_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Кнопки меню водителя из /start."""
    q = update.callback_query
    await q.answer()
    what = q.data.split(":")[2]
    if not _is_driver(q.from_user.id):
        await q.message.reply_text("⛔ Доступно водителям развозки.")
        return
    if what == "reis":
        await _render_points(q.message.reply_text, page=0, driver_id=q.from_user.id)
    elif what == "registry":
        import route_registry as rr
        await rr.send_registry(context.bot, q.from_user.id, q.message.reply_text)


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
    # custom_items — пункты чек-листа из доп.поля заказа «Чек-лист водителя» (Фаза 7).
    # Массив [{idx, text, answer}]; answer: true/false/null (ещё не отвечено).
    db._execute("ALTER TABLE delivery_checklist ADD COLUMN IF NOT EXISTS custom_items JSONB DEFAULT '[]'::jsonb")
    # order_name — номер ЗАКАЗА покупателя (по нему сверяет логист и реестр развозки).
    # demand_name — номер расходной/отгрузки; логист по нему не сверяет → показываем оба.
    db._execute("ALTER TABLE delivery_checklist ADD COLUMN IF NOT EXISTS order_name TEXT")
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
            headers=headers, params={"expand": "agent,customerOrder"},
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
    # Пункты чек-листа лежат на ЗАКАЗЕ (customerorder.attributes), не на отгрузке.
    co = d.get("customerOrder") or {}
    checklist_raw = None
    win_from = win_to = ""
    for a in co.get("attributes", []) or []:
        nm = a.get("name")
        if nm == _CHECKLIST_ATTR_NAME:
            checklist_raw = a.get("value")
        elif nm == "Окно доставки с (время)":
            win_from = _attr_time_hm(a.get("value"))
        elif nm == "Окно доставки до (время)":
            win_to = _attr_time_hm(a.get("value"))
    return {
        "agent_id": ag.get("id"),
        "agent_name": ag.get("name"),
        "address": d.get("shipmentAddress"),
        "sum_rub": (d.get("sum", 0) or 0) / 100,
        "name": d.get("name"),
        "order_name": co.get("name"),  # номер ЗАКАЗА — по нему сверяет логист
        "max_price_rub": max(prices) if prices else 0,
        "positions_text": "\n".join(lines),
        "checklist_raw": checklist_raw,
        # Контакт/условия приёмки для водителя — из комментария ПОД адресом доставки
        # (shipmentAddressFull.comment). Стандартный «Комментарий» заказа (description) —
        # производственный (партии/разделка), водителю не показываем.
        "comment": ((co.get("shipmentAddressFull") or {}).get("comment") or "").strip(),
        "win_from": win_from,   # «Окно доставки с (время)» → HH:MM
        "win_to": win_to,       # «Окно доставки до (время)» → HH:MM
    }


async def _resolve_stop_doc(doc_no: str):
    """№ точки реестра → («demand»|«move», id) — документ, по которому сдаём точку.

    Точка в реестре кодируется номером ЗАКАЗА, поэтому идём от заказа, а не от отгрузки:

    1. заказ по номеру → его отгрузка (`demands`);
    2. отгрузки нет → ПЕРЕМЕЩЕНИЕ (`moves`). Так уходит ГФС: товар едет на их склад
       ответственного хранения, в МС это `entity/move` со ссылкой на заказ, demand не создаётся —
       водитель такую точку сдать не мог (собственник, 13.08.2026);
    3. заказа с таким номером нет → трактуем номер как номер отгрузки (старые QR).

    Порядок важен: у заказов и отгрузок в МС независимая сквозная нумерация, и номера
    пересекаются (на 13.08.2026 — 819 совпадений с 01.06). Поиск отгрузки ПО НОМЕРУ ТОЧКИ,
    как было раньше, на старом заказе даёт ЧУЖУЮ отгрузку: заказ ГФС 03150 → отгрузка 03150
    ООО «БРЭД ФУД». Тогда водитель закрыл бы чужую доставку, а свою оставил висеть.
    """
    doc_no = (doc_no or "").strip()
    if not doc_no:
        return None, None
    import urllib.parse as _up
    f = _up.quote(f"name={doc_no}")
    headers = get_headers()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/customerorder?filter={f}&limit=1", headers=headers
            ) as r:
                rows = (await r.json()).get("rows", []) if r.status == 200 else []
            if rows:
                o = rows[0]
                for dm in o.get("demands") or []:
                    href = (dm.get("meta") or {}).get("href", "")
                    if href:
                        return "demand", href.rstrip("/").split("/")[-1]
                # Перемещений по заказу обычно одно; если их несколько — берём последнее
                # (МС отдаёт связи в порядке создания, последнее = актуальная отгрузка со склада).
                for mv in reversed(o.get("moves") or []):
                    href = (mv.get("meta") or {}).get("href", "")
                    if href:
                        return "move", href.rstrip("/").split("/")[-1]
                return None, None
            # Заказа с таким номером нет — значит на точке номер отгрузки (печатные QR).
            async with session.get(
                f"{MS_BASE}/entity/demand?filter={f}&order=moment,desc&limit=1", headers=headers
            ) as r:
                rows = (await r.json()).get("rows", []) if r.status == 200 else []
            if rows:
                return "demand", rows[0].get("id")
    except Exception as e:
        logger.warning("_resolve_stop_doc %s: %s", doc_no, e)
    return None, None


async def _fetch_move_detail(move_id: str) -> dict:
    """Перемещение + позиции в том же формате, что `_fetch_demand_detail`.

    У перемещения нет контрагента и адреса доставки — их, как и чек-лист с окном,
    берём из связанного заказа покупателя."""
    headers = get_headers()
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{MS_BASE}/entity/move/{move_id}",
            headers=headers, params={"expand": "customerOrder.agent"},
        ) as r:
            if r.status != 200:
                return {}
            m = await r.json()
        async with session.get(
            f"{MS_BASE}/entity/move/{move_id}/positions",
            headers=headers, params={"expand": "assortment", "limit": 100},
        ) as r:
            pos = (await r.json()).get("rows", []) if r.status == 200 else []

    co = m.get("customerOrder") or {}
    ag = co.get("agent") or {}
    prices = [(p.get("price", 0) or 0) / 100 for p in pos]
    lines = []
    for p in pos:
        nm = (p.get("assortment") or {}).get("name", "?")
        qty = p.get("quantity", 0) or 0
        pr = (p.get("price", 0) or 0) / 100
        lines.append(f"• {nm} — {qty:g} × {pr:,.0f}".replace(",", " "))
    checklist_raw = None
    win_from = win_to = ""
    for a in co.get("attributes", []) or []:
        nm = a.get("name")
        if nm == _CHECKLIST_ATTR_NAME:
            checklist_raw = a.get("value")
        elif nm == "Окно доставки с (время)":
            win_from = _attr_time_hm(a.get("value"))
        elif nm == "Окно доставки до (время)":
            win_to = _attr_time_hm(a.get("value"))
    return {
        "is_move": True,        # статус доставки писать некуда: у move статусов «Сдан» нет
        "agent_id": ag.get("id"),
        "agent_name": ag.get("name"),
        "address": co.get("shipmentAddress"),
        "sum_rub": (m.get("sum", 0) or 0) / 100,
        "name": m.get("name"),           # № перемещения
        "order_name": co.get("name"),    # № заказа — по нему сверяет логист
        "max_price_rub": max(prices) if prices else 0,
        "positions_text": "\n".join(lines),
        "checklist_raw": checklist_raw,
        "comment": ((co.get("shipmentAddressFull") or {}).get("comment") or "").strip(),
        "win_from": win_from,
        "win_to": win_to,
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


def _attr_time_hm(val) -> str:
    """«2026-07-29 09:30:00.000» → «09:30» (дата в поле ненадёжна, берём только время)."""
    if not val:
        return ""
    m = _re.search(r"\b(\d{1,2}):(\d{2})\b", str(val))
    return f"{int(m.group(1)):02d}:{m.group(2)}" if m else ""


def _fmt_window(win_from: str, win_to: str) -> str:
    """Окно приёмки для карточки: обе стороны → «09:00–09:30»; только до → «до 09:30»;
    только с → «с 14:00»; ни одной → '' (строку не показываем)."""
    wf, wt = (win_from or "").strip(), (win_to or "").strip()
    if wf and wt:
        return f"{wf}–{wt}"
    if wt:
        return f"до {wt}"
    if wf:
        return f"с {wf}"
    return ""


# Ведущая нумерация пункта: «1. », «2) », «3 -» → срезаем, оставляем текст задачи.
_NUM_PREFIX_RE = _re.compile(r"^\s*\d+\s*[.)\-]\s*")
# Денежный пункт: для розницы деньги уже спрашиваем авто-шагом → не дублируем вопросом.
_MONEY_RE = _re.compile(r"налич|деньг", _re.IGNORECASE)


def _parse_checklist_items(raw, money_required: bool = False) -> list:
    """Доп.поле «Чек-лист водителя» → список пунктов (по строке на пункт).
    Срезает ведущую нумерацию и лишние пробелы. Для розницы (money_required)
    отбрасывает «наличные/деньги» — их закрывает авто-шаг про деньги."""
    items = []
    for ln in (raw or "").splitlines():
        t = _NUM_PREFIX_RE.sub("", ln).strip()
        t = _re.sub(r"\s{2,}", " ", t)
        if not t:
            continue
        if money_required and _MONEY_RE.search(t):
            continue
        items.append(t[:_MAX_ITEM_LEN])
        if len(items) >= _MAX_ITEMS:
            break
    return items


def _money_required(det) -> bool:
    """Розничная касса, не образец, сумма > 0 → водитель забирает деньги (авто-шаг)."""
    is_sample = (det.get("max_price_rub", 0) or 0) <= _SAMPLE_MAX_PRICE_RUB
    return _is_retail(det.get("agent_name")) and (not is_sample) and (det.get("sum_rub", 0) or 0) > 0


def _load_items(row) -> list:
    """custom_items из строки БД (psycopg2 может вернуть list или str)."""
    items = (row or {}).get("custom_items") or []
    if isinstance(items, str):
        try:
            items = json.loads(items)
        except Exception:
            items = []
    return items


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
            # Без parse_mode: текст содержит имя контрагента из МС (напр. «Розничный
            # покупатель*** (Инесса)»), Markdown падал бы 400 и алерт о проблеме тихо
            # не доходил бы до логиста/менеджера. Форматирование тут — только эмодзи.
            if photo_file_id:
                await context.bot.send_photo(chat_id=cid, photo=photo_file_id, caption=text)
            else:
                await context.bot.send_message(chat_id=cid, text=text)
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
                # rr.stop_days — день визита в раскладке ИЛИ день окна заявки; точка
                # без времени вовсе остаётся (как и раньше), чтобы не терять её у водителя.
                days = rr.stop_days(s)
                return today in days if days else True
            route_stops = sorted(
                [s for s in (routes.get(unit_id) or []) if _stop_today(s)],
                key=lambda s: (s.get("seq") if s.get("seq") is not None else 999))
            # Заборы: подменяем имя точки на поставщика из заказа поставщику — в Логистике
            # логист заводит точку забора поверх старой клиентской и имя остаётся чужое.
            # Своим try: упавший МС не должен ронять уже собранный маршрут в фолбэк.
            try:
                await rr.enrich_pickups(route_stops)
            except Exception as e:
                logger.warning("enrich_pickups упал, имена точек как в Логистике: %s", e)
        except Exception as e:
            logger.warning("route filter упал, фолбэк на полный список: %s", e)
            route_stops = None

    if route_stops:
        import route_registry as rr
        import route_dispatch
        # Кнопки-точки водителю убраны (собственник, 2026-08-04): в Telegram они не
        # нажимались/подвисали. Отдаём ссылку на веб-чеклист — он всегда актуален,
        # там же закрытие точек. Список точек оставляем текстом, чтобы было видно
        # порядок выгрузки, не открывая страницу.
        total = len(route_stops)
        unit_name = rr.UNITS.get(unit_id, "")
        today = datetime.now(_MSK).date()
        lines = [f"🚚 Твой маршрут ({unit_name}) — {total} точек, порядок выгрузки:"]
        # Лимит сообщения Telegram — 4096 символов; на «густой» день список режем,
        # полный всегда есть на странице чеклиста.
        _MAX_LISTED = 30
        for i, s in enumerate(route_stops[:_MAX_LISTED], start=1):
            lines.append(f"{i}. {route_dispatch._stop_label(s)}")
        if total > _MAX_LISTED:
            lines.append(f"…и ещё {total - _MAX_LISTED} — весь список в чеклисте.")
        link = route_dispatch._route_link(unit_id, today.isoformat())
        if link:
            lines.append(f"\n👉 Открой чеклист и закрывай точки там: {link}")
        await send("\n".join(lines), disable_web_page_preview=True)
        return

    demands = await _fetch_today_demands()
    if not demands:
        await send("На сегодня отгрузок нет.")
        return
    total = len(demands)
    pages = (total + _PAGE_SIZE - 1) // _PAGE_SIZE
    page = max(0, min(page, pages - 1))
    chunk = demands[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE]

    import route_dispatch  # общий помощник подписи: имя клиента + № (розницу сжимает)
    buttons = []
    for d in chunk:
        label = route_dispatch._btn_label(d["agent_name"] or d["name"], d.get("name"))
        buttons.append([InlineKeyboardButton(f"📍 {label}", callback_data=f"drv:pick:{d['id']}")])

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
        f"{det.get('agent_name') or '?'}",
        f"{_doc_ref(det.get('order_name'), det.get('name'))}",
        f"Адрес: {det.get('address') or '—'}",
        f"Сумма: {_fmt_rub(det.get('sum_rub'))}",
    ]
    # Окно приёмки — из полей заказа МС «Окно доставки с/до (время)».
    _win = _fmt_window(det.get("win_from"), det.get("win_to"))
    if _win:
        lines.append(f"🕒 Окно приёмки: {_win}")
    # Контакт/условия приёмки менеджер оставляет в Комментарии заказа — показываем водителю.
    if det.get("comment"):
        lines.append(f"💬 Приёмка: {det['comment']}")
    if det.get("positions_text"):
        lines.append("\n" + det["positions_text"])
    preview = _parse_checklist_items(det.get("checklist_raw"), _money_required(det))
    if preview:
        lines.append("\n📋 Чек-лист водителю:")
        lines.extend(f"• {t}" for t in preview)
    rows = [
        [InlineKeyboardButton("📍 Прибыл — начать сдачу", callback_data=f"drv:arrive:{demand_id}")],
        [InlineKeyboardButton("🔳 QR точки", callback_data=f"drv:qr:{demand_id}")],
    ]
    if with_back:
        rows.append([InlineKeyboardButton("« К списку", callback_data="drv:pg:0")])
    # Без parse_mode: имена контрагентов из МС содержат «*» (напр. «Розничный
    # покупатель*** (Инесса)»), а Markdown на них падает 400 «can't parse entities» →
    # карточка не открывалась, у водителя «ничего не происходит». Простой текст надёжен.
    await send("\n".join(lines), reply_markup=InlineKeyboardMarkup(rows))


async def cb_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    demand_id = q.data.split(":", 2)[2]
    await _render_card(q.edit_message_text, demand_id, context.bot_data["db"])


async def cb_route_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Нажатие точки в СТАРОМ сообщении маршрута (кнопки убраны 2026-08-04).

    Новые пуши приходят без клавиатуры, но у водителей в чатах остались прежние
    сообщения. Карточку сдачи в боте больше не открываем — отвечаем ссылкой на
    веб-чеклист с якорем на эту точку.
    """
    q = update.callback_query
    await q.answer()
    order_no = q.data.split(":", 2)[2]
    import route_dispatch
    unit_id = _driver_unit_id(q.from_user.id)
    link = (route_dispatch._route_link(unit_id, datetime.now(_MSK).date().isoformat())
            if unit_id else "")
    if not link:
        await q.message.reply_text(
            "Сдача груза переехала на страницу чеклиста — открой ссылку из сообщения "
            "с маршрутом или напиши логисту.")
        return
    doc = route_dispatch._doc_no(order_no)
    await q.message.reply_text(
        f"👉 Сдача груза теперь здесь: {link}#o{doc}",
        disable_web_page_preview=True)


async def open_from_deeplink(update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str):
    """Вход по СТАРОМУ QR-диплинку `/start chk_<...>` из уже напечатанных реестров.

    Карточку сдачи в боте больше не открываем (собственник, 2026-08-04: кнопки в
    Telegram убраны целиком). Отвечаем ссылкой на веб-чеклист машины водителя —
    с якорем на точку, если payload похож на № документа. Новые QR ведут в веб сразу.
    """
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    if not _is_driver(user.id):
        await update.message.reply_text("⛔ Сдача груза доступна водителям развозки.")
        return
    import route_dispatch
    unit_id = _driver_unit_id(user.id)
    if not unit_id:
        await update.message.reply_text(
            "За тобой не закреплена машина — попроси логиста привязать, "
            "тогда придёт ссылка на чеклист.")
        return
    link = route_dispatch._route_link(unit_id, datetime.now(_MSK).date().isoformat())
    if not link:
        await update.message.reply_text("Чеклист сейчас недоступен, напиши логисту.")
        return
    doc = route_dispatch._doc_no(payload)
    if doc and doc.isdigit():
        link += f"#o{doc}"
    await update.message.reply_text(
        f"👉 Сдача груза теперь на странице чеклиста: {link}",
        disable_web_page_preview=True)


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
        png = await asyncio.to_thread(_qr_png, link)  # reportlab CPU-sync → в поток, не блокируем loop
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

    # Пункты чек-листа из доп.поля заказа (Фаза 7). Фиксируем на момент прибытия.
    items = [
        {"idx": i, "text": t, "answer": None}
        for i, t in enumerate(_parse_checklist_items(det.get("checklist_raw"), money_required))
    ]
    items_json = json.dumps(items, ensure_ascii=False)

    # upsert
    db._execute("""
        INSERT INTO delivery_checklist
          (demand_id, demand_name, order_name, agent_id, agent_name, address, sum_rub,
           is_retail, is_sample, money_required, manager_tag, driver_chat_id,
           snap_date, stage, custom_items, arrived_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
        ON CONFLICT (demand_id) DO UPDATE SET
           order_name = EXCLUDED.order_name,
           driver_chat_id = EXCLUDED.driver_chat_id,
           money_required = EXCLUDED.money_required,
           stage = EXCLUDED.stage,
           custom_items = EXCLUDED.custom_items,
           updated_at = now()
    """, (
        demand_id, det.get("name"), det.get("order_name"), det.get("agent_id"), det.get("agent_name"),
        det.get("address"), det.get("sum_rub"), is_retail, is_sample, money_required,
        mtag, driver_id, snap, "money" if money_required else "doc", items_json,
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
    _set_fields(db, demand_id, doc_signed=signed)
    if not signed:
        row = _get_row(db, demand_id)
        await _signal_logist(context, row, "✍️ документ НЕ подписан")
    await _ask_next_item_or_accept(q, db, demand_id)


# ─── Пункты чек-листа из заказа (Фаза 7) ─────────────────────────────────────

async def _ask_item(q, demand_id, item):
    kb = [[InlineKeyboardButton("✅ Да", callback_data=f"drv:item:yes:{item['idx']}:{demand_id}"),
           InlineKeyboardButton("❌ Нет", callback_data=f"drv:item:no:{item['idx']}:{demand_id}")]]
    await q.edit_message_text(
        f"📋 {item['text']}\n\nВыполнено?",
        reply_markup=InlineKeyboardMarkup(kb),
    )


async def _ask_next_item_or_accept(q, db, demand_id):
    """Первый неотвеченный пункт → вопрос; если пунктов не осталось → приёмка."""
    row = _get_row(db, demand_id)
    for it in _load_items(row):
        if it.get("answer") is None:
            _set_fields(db, demand_id, stage=f"item:{it['idx']}")
            await _ask_item(q, demand_id, it)
            return
    _set_fields(db, demand_id, stage="accept")
    await _ask_accept(q, demand_id)


async def _signal_item_not_done(context, row, text: str):
    """«Нет» по пункту → сигнал логисту + ответственному менеджеру (по тегу)."""
    if not row:
        return
    tag = (row.get("manager_tag") or "").strip().lower()
    mgr_chat = PDZ_MANAGER_TG_IDS.get(tag)
    msg = f"🚚 Сдача груза — пункт НЕ выполнен\n{_point_head(row)}\n\n📋 {text}"
    if row.get("manager_tag"):
        msg += f"\nМенеджер: {row.get('manager_tag')}"
    await _send_alert(context, [*_logist_chat_ids(), mgr_chat], msg)


async def cb_item(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, _, val, idx, demand_id = q.data.split(":", 4)
    idx = int(idx)
    db = context.bot_data["db"]
    row = _get_row(db, demand_id)
    items = _load_items(row)
    done_ok = (val == "yes")
    text = None
    for it in items:
        if it.get("idx") == idx:
            it["answer"] = done_ok
            text = it.get("text")
            break
    _set_fields(db, demand_id, custom_items=json.dumps(items, ensure_ascii=False))
    if not done_ok:
        await _signal_item_not_done(context, row, text or "(пункт)")
    await _ask_next_item_or_accept(q, db, demand_id)


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

def _doc_ref(order_name, demand_name) -> str:
    """Идентификатор точки: номер ЗАКАЗА (по нему сверяет логист и реестр развозки)
    + номер отгрузки в скобках (напечатан на накладной у водителя). Заказ отсутствует
    (старые строки без колонки) → фолбэк на прежний «Отгрузка № …»."""
    o = (order_name or "").strip()
    d = (demand_name or "").strip()
    if o and d:
        return f"Заказ № {o} (отгр. {d})"
    if o:
        return f"Заказ № {o}"
    return f"Отгрузка № {d or '?'}"


def _point_head(row) -> str:
    return (f"*{row.get('agent_name') or '?'}*\n"
            f"{_doc_ref(row.get('order_name'), row.get('demand_name'))}\n"
            f"Адрес: {row.get('address') or '—'}")


async def _signal_logist(context, row, reason: str):
    if not row:
        return
    text = f"🚚 Сдача груза — сигнал\n{_point_head(row)}\n\n⚠️ {reason}"
    await _send_alert(context, [*_logist_chat_ids()], text)


async def _alert_claim(context, row):
    if not row:
        return
    tag = (row.get("manager_tag") or "").strip().lower()
    mgr_chat = PDZ_MANAGER_TG_IDS.get(tag)
    text = (f"⚠️ ПРЕТЕНЗИЯ на доставке\n{_point_head(row)}\n\n"
            f"Описание: {row.get('claim_text') or '—'}")
    if row.get("manager_tag"):
        text += f"\nМенеджер: {row.get('manager_tag')}"
    recipients = [*_logist_chat_ids(), _owner_chat_id(), mgr_chat]
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
                    "order_name": co.get("name"),  # номер ЗАКАЗА — по нему сверяет логист
                    "agent_name": ag.get("name") if isinstance(ag, dict) else None,
                    "address": x.get("shipmentAddress"),
                    "sum_rub": (x.get("sum", 0) or 0) / 100,
                    "window_from": wfrom, "window_to": wto,
                    "places": places,
                    # комментарий под адресом доставки (контакт/приёмка), не производственный description
                    "comment": (co.get("shipmentAddressFull") or {}).get("comment"),
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
        info = (f"<b>{i}. {s.get('agent_name') or '?'}</b>  ({_doc_ref(s.get('order_name'), s.get('demand_name'))})<br/>"
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
    if not (_is_driver(user.id) or user.id == _owner_chat_id() or user.id in _logist_chat_ids()):
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
        pdf = await asyncio.to_thread(_build_shipment_list_pdf, shipments, me.username,
                                      snap.strftime("%d.%m.%Y"))  # reportlab CPU-sync → в поток
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

def _point_head_plain(row) -> str:
    """Шапка точки без Markdown — имена контрагентов из МС содержат '*'/'(' (падает parse)."""
    return (f"{row.get('agent_name') or '?'}\n"
            f"{_doc_ref(row.get('order_name'), row.get('demand_name'))}\n"
            f"Адрес: {row.get('address') or '—'}")


async def _web_alert_claim(bot, row):
    """Претензия с веб-приёмки → логистам (best-effort; TG может тупить, но это канал эскалации)."""
    if not row:
        return
    text = (f"⚠️ Претензия при сдаче (веб-приёмка)\n{_point_head_plain(row)}\n"
            f"Коммент: {row.get('claim_text') or '—'}")
    for cid in _logist_chat_ids():
        try:
            await bot.send_message(chat_id=cid, text=text)
        except Exception as e:
            logger.warning("_web_alert_claim → %s: %s", cid, e)


async def web_pickup_submit(db, bot, uid, order_no, *, doc, accepted, claim_text, title="", day=None):
    """Закрытие точки ЗАБОРА товара (заказ поставщику или ручная точка Логистики).

    У забора нет отгрузки в МС → резолвить demand и писать статус отгрузки нечего.
    Делаем две вещи: гасим точку в route_dispatch.done и уведомляем логистов
    (собственник, 2026-08-04: по забору логисты хотят видеть каждое закрытие)."""
    import route_dispatch
    doc_no = route_dispatch._doc_no(order_no)
    ok_taken = (accepted == "ok")
    try:
        route_dispatch.mark_done_by_unit(uid, doc_no, day)
    except Exception as e:
        logger.warning("web_pickup_submit done: %s", e)
        return False, "Не удалось отметить точку, повтори."

    import route_registry as rr
    head = f"{title or doc_no} ({rr.UNITS.get(uid, uid)})"
    if ok_taken:
        text = (f"📦 Забор выполнен — {head}\n"
                f"№ {doc_no}\nДокументы: {'забрал' if doc == 'yes' else 'НЕ забрал'}")
    else:
        text = (f"⚠️ Проблема на заборе — {head}\n"
                f"№ {doc_no}\nДокументы: {'забрал' if doc == 'yes' else 'НЕ забрал'}\n"
                f"Коммент: {(claim_text or '').strip() or '—'}")
    for cid in _logist_chat_ids():
        try:
            await bot.send_message(chat_id=cid, text=text)
        except Exception as e:
            logger.warning("web_pickup_submit → %s: %s", cid, e)

    return True, ("✅ Забор закрыт, логист уведомлён." if ok_taken
                  else "⚠️ Проблема зафиксирована, логист уведомлён.")


# Сколько водитель ждёт МойСклад на сдаче, прежде чем сдача уходит в отложенную дозапись.
_WEB_SUBMIT_MS_TIMEOUT = 12


async def web_submit(db, bot, uid, order_no, *, money, doc, items, accepted, claim_text,
                     day=None, deferred=False):
    """Приёмка точки С ВЕБ-СТРАНИЦЫ, одним экраном, без telegram-контекста.
    money/doc/item — строки 'yes'/'no' (или None). accepted — 'ok'/'claim'.
    Пишет статус в МС (Сдан / Сдан с проблемой), помечает точку закрытой в
    route_dispatch.done по машине. Возвращает (ok: bool, msg: str). Бот — запаска,
    поэтому логика записи зеркалит cb_accept/_finish_claim.

    Документ точки — отгрузка (demand), а для ГФС — ПЕРЕМЕЩЕНИЕ на склад их
    ответхранения (move): у такой точки чеклист пишется, статус в МС — нет.

    deferred=True — это уже фоновая дозапись (МС ждём сколько нужно, второй раз не откладываем)."""
    import route_dispatch
    # № документа держим ОТДЕЛЬНОЙ переменной: раньше он затирал `doc` — ответ водителя
    # «Подписал документ?», и в `delivery_checklist.doc_signed` всегда уезжал null.
    doc_no = route_dispatch._doc_no(order_no)  # чистим заметки логиста из № (как кнопки маршрута)
    try:
        # Резолв отгрузки — два запроса в МС. При 429-шторме они висят десятками секунд
        # (троттл спит 30 с), телефон рвёт соединение и сдача выглядит несработавшей.
        # Поэтому ждём ограниченно, а дальше принимаем точку и дописываем фоном.
        async def _resolve():
            kind, did = await _resolve_stop_doc(doc_no)
            if kind == "demand":
                return did, await _fetch_demand_detail(did)
            if kind == "move":
                return did, await _fetch_move_detail(did)
            return None, None
        if deferred:
            demand_id, det = await _resolve()
        else:
            demand_id, det = await asyncio.wait_for(_resolve(), timeout=_WEB_SUBMIT_MS_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning("web_submit %s: МойСклад не ответил за %d с — принимаю точку отложенно",
                       doc_no, _WEB_SUBMIT_MS_TIMEOUT)
        try:
            route_dispatch.mark_done_by_unit(uid, doc_no, day)
        except Exception as e:
            logger.warning("web_submit done (отложенно): %s", e)
        asyncio.create_task(web_submit(db, bot, uid, order_no, money=money, doc=doc, items=items,
                                       accepted=accepted, claim_text=claim_text, day=day,
                                       deferred=True))
        return True, "✅ Точка закрыта. МойСклад тормозит — данные допишутся сами."
    if not demand_id:
        return False, "Точка ещё не отгружена складом — попробуй позже."
    if not det:
        return False, "Не удалось загрузить отгрузку. Попробуй ещё раз."

    is_retail = _is_retail(det.get("agent_name"))
    is_sample = det.get("max_price_rub", 0) <= _SAMPLE_MAX_PRICE_RUB
    money_required = is_retail and (not is_sample) and det.get("sum_rub", 0) > 0
    mtag, _ = await _resolve_manager_chat(det.get("agent_id"))
    _, _, snap = _msk_today_bounds()

    def _yn(v):
        return True if v == "yes" else (False if v == "no" else None)

    base_items = _parse_checklist_items(det.get("checklist_raw"), money_required)
    ci = [{"idx": i, "text": t, "answer": _yn((items or {}).get(str(i)))}
          for i, t in enumerate(base_items)]

    drow = db._fetchone("SELECT chat_id FROM drivers WHERE unit_id=%s AND active LIMIT 1", (uid,))
    driver_chat = (drow or {}).get("chat_id")
    accepted_ok = (accepted == "ok")
    status = "сдан" if accepted_ok else "сдан с проблемой"
    money_val = _yn(money) if money_required else None

    db._execute("""
        INSERT INTO delivery_checklist
          (demand_id, demand_name, order_name, agent_id, agent_name, address, sum_rub,
           is_retail, is_sample, money_required, manager_tag, driver_chat_id,
           snap_date, stage, custom_items, money_received, doc_signed,
           accepted_ok, claim_text, status, arrived_at, completed_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'done',%s,%s,%s,%s,%s,%s, now(), now(), now())
        ON CONFLICT (demand_id) DO UPDATE SET
           order_name=EXCLUDED.order_name,
           driver_chat_id=EXCLUDED.driver_chat_id, money_required=EXCLUDED.money_required,
           stage='done', custom_items=EXCLUDED.custom_items,
           money_received=EXCLUDED.money_received, doc_signed=EXCLUDED.doc_signed,
           accepted_ok=EXCLUDED.accepted_ok, claim_text=EXCLUDED.claim_text,
           status=EXCLUDED.status, completed_at=now(), updated_at=now()
    """, (demand_id, det.get("name"), det.get("order_name"), det.get("agent_id"), det.get("agent_name"),
          det.get("address"), det.get("sum_rub"), is_retail, is_sample, money_required,
          mtag, driver_chat, snap, json.dumps(ci, ensure_ascii=False),
          money_val, _yn(doc), accepted_ok,
          ((claim_text or "").strip() or None) if not accepted_ok else None, status))

    try:
        route_dispatch.mark_done_by_unit(uid, doc_no, day)
    except Exception as e:
        logger.warning("web_submit done: %s", e)

    # Запись статуса в МС и алерт по претензии — ФОНОМ. При 429 глобальный троттл МС
    # уходит в 30-секундный sleep, и водитель на телефоне видел сетевую ошибку, хотя
    # сдача уже была принята (план 2026-08-07). Точка гаснет сразу, статус доезжает сам.
    ms_target = "Сдан" if accepted_ok else "Сдан с проблемой"
    # Перемещение (ГФС, ответхранение) — статус доставки писать некуда: в справочнике
    # статусов move только Новый / Внутреннее / Внешнее. Решение собственника 13.08.2026:
    # статусы в МС под это не заводим, точка живёт в чеклисте и гаснет в реестре.
    is_move = bool(det.get("is_move"))
    asyncio.create_task(_web_finalize_bg(db, bot, demand_id, ms_target, accepted_ok,
                                         write_status=not is_move))

    base = ("✅ Точка закрыта: сдано." if accepted_ok
            else "⚠️ Претензия зафиксирована, логист уведомлён.")
    if is_move:
        return True, base
    return True, f"{base} Статус в МС → «{ms_target}» проставляется."


async def _web_finalize_bg(db, bot, demand_id, ms_target, accepted_ok, write_status=True):
    """Хвост веб-сдачи, который водителю ждать незачем: статус в МС (с ретраями) и
    алерт логистам по претензии. Если статус так и не записался — говорим логистам,
    иначе отгрузка молча останется в старом статусе.

    write_status=False — документ не отгрузка (перемещение на ответхранение): статуса
    «Сдан» у него в МС не бывает, пишем только чеклист и, если надо, алерт по претензии."""
    try:
        import delivery_statuses as _dsx
        write_on = _dsx._write_enabled() and write_status
        wrote = False
        if write_on:
            for attempt in range(3):
                try:
                    wrote = await _dsx.write_ms_status(demand_id, ms_target)
                except Exception as e:
                    logger.warning("web_submit МС-статус (попытка %d): %s", attempt + 1, e)
                    wrote = False
                if wrote:
                    break
                await asyncio.sleep(5 * (attempt + 1))
        if not accepted_ok:
            await _web_alert_claim(bot, _get_row(db, demand_id))
        if write_on and not wrote:
            row = _get_row(db, demand_id) or {}
            text = (f"⚠️ Статус «{ms_target}» в МойСклад НЕ записался\n"
                    f"Отгрузка {row.get('demand_name') or demand_id}"
                    f"{' — ' + row.get('agent_name') if row.get('agent_name') else ''}\n"
                    f"Водитель точку сдал, поставьте статус руками.")
            for cid in _logist_chat_ids():
                try:
                    await bot.send_message(chat_id=cid, text=text)
                except Exception as e:
                    logger.warning("_web_finalize_bg → %s: %s", cid, e)
    except Exception as e:
        logger.error("_web_finalize_bg %s: %s", demand_id, e, exc_info=True)


def register(app: Application, db):
    """Подключить чеклист водителя. Вызывать в main() ДО catch-all handle_message."""
    global _DB
    _DB = db
    app.bot_data["db"] = db

    # /рейс (кириллица → через Regex) + ASCII-alias /reis
    app.add_handler(CommandHandler("reis", cmd_reis))
    app.add_handler(MessageHandler(filters.Regex(r"^/рейс(@\w+)?(\s|$)"), cmd_reis))

    # /лист — лист отгрузок дня с QR (для логиста/склада/водителей) + alias /shipmentlist
    app.add_handler(CommandHandler("shipmentlist", cmd_shipment_list))
    app.add_handler(MessageHandler(filters.Regex(r"^/лист(@\w+)?(\s|$)"), cmd_shipment_list))

    app.add_handler(CallbackQueryHandler(cb_menu, pattern=r"^drv:menu:"))
    app.add_handler(CallbackQueryHandler(cb_page, pattern=r"^drv:pg:"))
    app.add_handler(CallbackQueryHandler(cb_pick, pattern=r"^drv:pick:"))
    app.add_handler(CallbackQueryHandler(cb_route_pick, pattern=r"^drv:rp:"))
    app.add_handler(CallbackQueryHandler(cb_qr, pattern=r"^drv:qr:"))
    app.add_handler(CallbackQueryHandler(cb_arrive, pattern=r"^drv:arrive:"))
    app.add_handler(CallbackQueryHandler(cb_money, pattern=r"^drv:money:"))
    app.add_handler(CallbackQueryHandler(cb_doc, pattern=r"^drv:doc:"))
    app.add_handler(CallbackQueryHandler(cb_item, pattern=r"^drv:item:"))
    app.add_handler(CallbackQueryHandler(cb_accept, pattern=r"^drv:acc:"))
    app.add_handler(CallbackQueryHandler(cb_claim_nophoto, pattern=r"^drv:claimnophoto:"))

    # Текст/фото претензии — ДО общего handle_message
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & _ClaimTextFilter(db), handle_claim_text))
    app.add_handler(MessageHandler(
        filters.PHOTO & _ClaimPhotoFilter(db), handle_claim_photo))

    # ensure_schema — ПОСЛЕ регистрации хендлеров и best-effort: транзиентный сбой БД на
    # старте (БД не готова) НЕ должен ронять весь register и оставлять кнопки водителя
    # мёртвыми (симптом 22.07: и Мага, и Сиро — «кнопки не нажимаются»). Таблицы и так
    # уже существуют с прошлых стартов, CREATE IF NOT EXISTS идемпотентен.
    try:
        ensure_schema(db)
    except Exception as e:
        logger.exception("driver_checklist.ensure_schema отложено (БД не готова?): %s", e)

    logger.info("driver_checklist: хендлеры зарегистрированы")
