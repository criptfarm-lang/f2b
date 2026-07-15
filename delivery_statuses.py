"""
Авто-статусы отгрузки (Фаза 2 плана логистической оркестрации 2026-07-09).

По маршруту Wialon + live-позициям машин + чеклисту водителя автоматически
проставляет статус отгрузки в МойСклад:
  - «В пути»            — машина выехала из геозоны базы (Ильинский);
  - «Задержка в пути»   — сейчас > план (p.r.vt) + 30 мин, а машина ещё не доехала
                          (по GPS) → + алерт логисту/менеджеру/собственнику;
  - «Сдан»/«Сдан с проблемой» — из чеклиста водителя (событийно, см. driver_checklist).

⚠️ ЗАПИСЬ В МОЙСКЛАД. По умолчанию СУХОЙ режим (только лог): включается env
DELIVERY_STATUS_WRITE=1. Санкция собственника на автозапись статусов — 2026-07-15.

Команда /статусы (владелец/логист) — превью: что бот проставил бы сейчас (read-only).
Крон — раз в 10 мин в часы развоза.
"""

import os
import math
import json
import logging
import urllib.parse
from datetime import datetime, timezone, timedelta

import aiohttp
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

from moysklad import MS_BASE, get_headers, PDZ_MANAGER_TG_IDS

logger = logging.getLogger(__name__)
_MSK = timezone(timedelta(hours=3))

# База (загрузка утром) — Ильинский, Раменское
BASE_LAT, BASE_LON = 55.6323, 38.1038
R_BASE_M = 500       # «В пути» — удаление транспорта на 500 м от склада (спека собственника)
R_STOP_M = 300       # радиус «прибыл на точку»
DELAY_BUFFER_MIN = 30
DELIVERY_HOURS = (7, 21)   # МСК, в эти часы крон активен
JOB_INTERVAL_SEC = 600

# Статусы, которые авто-движок ИМЕЕТ ПРАВО менять (логистические, в процессе развоза).
# Всё остальное — ручное/бухгалтерское (Отгружен ставит оператор; УПД подписан / Долг /
# Оплачен — оператор; Сдан / Сдан с проблемой / Едет возврат — терминальные) — НЕ трогаем.
MANAGED = {"Отгружен", "В пути", "Задержка в пути"}


def _write_enabled() -> bool:
    return os.getenv("DELIVERY_STATUS_WRITE", "0") == "1"


def _haversine(a_lat, a_lon, b_lat, b_lon) -> float:
    R = 6371000.0
    p1, p2 = math.radians(a_lat), math.radians(b_lat)
    dp = math.radians(b_lat - a_lat)
    dl = math.radians(b_lon - a_lon)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


# ─── Схема состояния (чтобы не дублировать записи/алерты) ─────────────────────

def ensure_schema(db):
    db._execute("""
        CREATE TABLE IF NOT EXISTS route_status_state (
            order_no       TEXT PRIMARY KEY,
            demand_id      TEXT,
            unit_id        BIGINT,
            arrived        BOOLEAN DEFAULT FALSE,
            ms_status      TEXT,
            delay_alerted  BOOLEAN DEFAULT FALSE,
            updated_at     TIMESTAMPTZ DEFAULT now()
        )
    """)


def _st_get(db, order_no):
    return db._fetchone("SELECT * FROM route_status_state WHERE order_no=%s", (order_no,))


def _st_upsert(db, order_no, **f):
    row = _st_get(db, order_no)
    if row is None:
        cols = ["order_no"] + list(f.keys())
        vals = [order_no] + list(f.values())
        ph = ",".join(["%s"] * len(vals))
        db._execute(f"INSERT INTO route_status_state ({','.join(cols)}) VALUES ({ph})", vals)
    elif f:
        sets = ", ".join(f"{k}=%s" for k in f)
        db._execute(f"UPDATE route_status_state SET {sets}, updated_at=now() WHERE order_no=%s",
                    list(f.values()) + [order_no])


# ─── Wialon позиции ──────────────────────────────────────────────────────────

async def _wialon_positions(session, sid):
    import route_registry as rr
    out = {}
    for uid in rr.UNITS:
        r = await rr._wialon_call(session, "core/search_item", {"id": uid, "flags": 1024}, sid)
        pos = (r.get("item") or {}).get("pos") or {}
        if pos:
            out[uid] = {"lat": pos.get("y"), "lon": pos.get("x"),
                        "speed": pos.get("s"), "ts": pos.get("t")}
    return out


# ─── МойСклад ────────────────────────────────────────────────────────────────

async def _ms_states(session) -> dict:
    async with session.get(f"{MS_BASE}/entity/demand/metadata", headers=get_headers()) as r:
        d = await r.json()
    return {s["name"]: s["meta"] for s in d.get("states", [])}


async def _ms_resolve(session, order_no) -> dict:
    """order_no → {demand_id, state_name, agent_tags}. None если отгрузки нет."""
    f = urllib.parse.quote(f"name={order_no}")
    url = f"{MS_BASE}/entity/customerorder?filter={f}&expand=agent,demands.state&limit=1"
    async with session.get(url, headers=get_headers()) as r:
        if r.status != 200:
            return None
        rows = (await r.json()).get("rows", [])
    if not rows:
        return None
    o = rows[0]
    demands = o.get("demands") or []
    if not demands:
        return None
    dem = demands[0]
    tags = (o.get("agent") or {}).get("tags", []) or []
    return {
        "demand_id": dem.get("id"),
        "state_name": (dem.get("state") or {}).get("name"),
        "agent_tags": tags,
    }


async def _ms_set_state(session, demand_id, state_meta) -> bool:
    body = json.dumps({"state": {"meta": state_meta}}).encode()
    async with session.put(f"{MS_BASE}/entity/demand/{demand_id}",
                           headers=get_headers(), data=body) as r:
        ok = r.status == 200
        if not ok:
            logger.warning("_ms_set_state %s → HTTP %s", demand_id, r.status)
        return ok


async def write_ms_status(demand_id: str, status_name: str) -> bool:
    """Событийная запись статуса отгрузки в МС (из чеклиста водителя — работает БЕЗ GPS,
    в т.ч. для наёмной машины). Уважает сухой режим (DELIVERY_STATUS_WRITE)."""
    if not demand_id:
        return False
    if not _write_enabled():
        logger.info("delivery_statuses(dry): demand %s → %s (запись выкл)", demand_id, status_name)
        return False
    try:
        async with aiohttp.ClientSession() as session:
            states = await _ms_states(session)
            meta = states.get(status_name)
            if not meta:
                logger.warning("write_ms_status: нет статуса %r в МС", status_name)
                return False
            return await _ms_set_state(session, demand_id, meta)
    except Exception as e:
        logger.warning("write_ms_status %s → %s: %s", demand_id, status_name, e)
        return False


def _manager_chat(agent_tags):
    for t in agent_tags:
        key = (t or "").strip().lower()
        if key in PDZ_MANAGER_TG_IDS:
            return t, PDZ_MANAGER_TG_IDS[key]
    return None, None


# ─── Ядро: вычислить целевой статус для точки ────────────────────────────────

def _target_status(*, left_base, arrived, vt, now_ts, chk_status):
    """chk_status — статус из delivery_checklist (сдан/сдан с проблемой) или None."""
    if chk_status == "сдан с проблемой":
        return "Сдан с проблемой"
    if chk_status == "сдан":
        return "Сдан"
    if not left_base:
        return None                      # ещё на базе — не трогаем «Отгружен»
    if arrived:
        return "В пути"                  # приехал, ждём чеклист
    if vt and now_ts > vt + DELAY_BUFFER_MIN * 60:
        return "Задержка в пути"
    return "В пути"


# ─── Прогон (dry / live) ─────────────────────────────────────────────────────

async def run_check(db, bot=None, preview=False) -> list:
    """Возвращает список строк-решений. preview/сухой режим — без записи и алертов.
    Реальная запись — только если _write_enabled() и not preview."""
    import route_registry as rr
    lines = []
    write = _write_enabled() and not preview
    now_ts = int(datetime.now(_MSK).timestamp())

    token = os.getenv("WIALON_TOKEN")
    async with aiohttp.ClientSession() as session:
        login = await rr._wialon_call(session, "token/login", {"token": token})
        sid = login.get("eid")
        if not sid:
            return ["Wialon login fail"]
        routes = await _fetch_routes_via(session, sid)
        positions = await _wialon_positions(session, sid)
        states = await _ms_states(session)

        for uid, name in rr.UNITS.items():
            stops = routes.get(uid) or []
            pos = positions.get(uid)
            left_base = bool(pos) and _haversine(pos["lat"], pos["lon"], BASE_LAT, BASE_LON) > R_BASE_M
            for s in stops:
                order_no = s["order_no"]
                info = await _ms_resolve(session, order_no)
                if not info or not info["demand_id"]:
                    continue
                cur = info["state_name"]
                st = _st_get(db, order_no) or {}
                # прибытие
                arrived = bool(st.get("arrived"))
                if not arrived and pos and s.get("lat") and s.get("lon"):
                    if _haversine(pos["lat"], pos["lon"], s["lat"], s["lon"]) < R_STOP_M:
                        arrived = True
                        if not preview:
                            _st_upsert(db, order_no, arrived=True, demand_id=info["demand_id"], unit_id=uid)
                # чеклист
                chk = db._fetchone("SELECT status FROM delivery_checklist WHERE demand_id=%s",
                                   (info["demand_id"],))
                chk_status = (chk or {}).get("status")
                target = _target_status(left_base=left_base, arrived=arrived, vt=s.get("vt"),
                                        now_ts=now_ts, chk_status=chk_status)
                # трогаем только логистические статусы; ручные/бухгалтерские/терминальные — нет
                if not target or cur not in MANAGED:
                    continue
                if cur == "Задержка в пути" and target == "В пути":
                    continue  # не понижаем
                if target == cur:
                    continue
                tag = f"{name} №{order_no} {s['client'][:24]}: {cur} → {target}"
                lines.append(tag)
                if write:
                    meta = states.get(target)
                    if meta and await _ms_set_state(session, info["demand_id"], meta):
                        _st_upsert(db, order_no, ms_status=target, demand_id=info["demand_id"], unit_id=uid)
                # алерт по задержке (один раз)
                if target == "Задержка в пути" and not st.get("delay_alerted"):
                    if bot and not preview:
                        await _delay_alert(bot, name, s, info["agent_tags"])
                        _st_upsert(db, order_no, delay_alerted=True, demand_id=info["demand_id"], unit_id=uid)
    return lines


async def _fetch_routes_via(session, sid):
    import route_registry as rr
    res = await rr._wialon_call(session, "core/search_item",
                                {"id": rr.RESOURCE_ID, "flags": 0xffffff}, sid)
    orders = (res.get("item") or {}).get("orders") or {}
    routes = {uid: [] for uid in rr.UNITS}
    for o in orders.values():
        uid = o.get("u")
        if uid not in rr.UNITS:
            continue
        p = o.get("p") or {}
        r = p.get("r") or {}
        routes[uid].append({
            "seq": r.get("i"), "vt": r.get("vt"),
            "client": p.get("n") or "?", "order_no": o.get("n"),
            "lat": o.get("y"), "lon": o.get("x"),
        })
    for uid in routes:
        routes[uid].sort(key=lambda s: (s["seq"] if s["seq"] is not None else 999))
    return routes


async def _delay_alert(bot, unit_name, stop, agent_tags):
    import driver_checklist as dc
    vt = stop.get("vt")
    plan = datetime.fromtimestamp(vt, _MSK).strftime("%H:%M") if vt else "—"
    tag, mgr_chat = _manager_chat(agent_tags)
    text = (f"⏱ ЗАДЕРЖКА в пути\n{unit_name}\n"
            f"{stop['client']} (№{stop['order_no']})\n"
            f"План прибытия {plan} + 30 мин — машина ещё не на точке.")
    if tag:
        text += f"\nМенеджер: {tag}"
    recipients = [dc._logist_chat_id(), dc._owner_chat_id(), mgr_chat]
    seen = set()
    for cid in recipients:
        if not cid or cid in seen:
            continue
        seen.add(cid)
        try:
            await bot.send_message(chat_id=cid, text=text)
        except Exception as e:
            logger.warning("_delay_alert → %s: %s", cid, e)


# ─── Команда превью + крон ───────────────────────────────────────────────────

async def cmd_statuses(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import driver_checklist as dc
    user = update.effective_user
    if not user or update.effective_chat.type != "private":
        return
    if not (user.id == dc._owner_chat_id() or user.id == dc._logist_chat_id()):
        await update.message.reply_text("⛔ Доступно логисту и владельцу.")
        return
    mode = "ЗАПИСЬ ВКЛючена" if _write_enabled() else "СУХОЙ режим (запись выкл)"
    await update.message.reply_text(f"Считаю статусы… ({mode})")
    try:
        lines = await run_check(context.bot_data["db"], bot=context.bot, preview=True)
        if not lines:
            await update.message.reply_text("Изменений статусов нет — всё актуально или машины ещё на базе.")
            return
        await update.message.reply_text("Бот проставил бы:\n" + "\n".join(f"• {l}" for l in lines))
    except Exception as e:
        logger.exception("cmd_statuses: %s", e)
        await update.message.reply_text("Ошибка расчёта статусов. Проверь WIALON_TOKEN.")


async def _job(context: ContextTypes.DEFAULT_TYPE):
    h = datetime.now(_MSK).hour
    if not (DELIVERY_HOURS[0] <= h < DELIVERY_HOURS[1]):
        return
    db = context.application.bot_data.get("db")
    if not db:
        return
    try:
        lines = await run_check(db, bot=context.bot, preview=False)
        if lines:
            logger.info("delivery_statuses: %s", "; ".join(lines))
    except Exception as e:
        logger.exception("delivery_statuses job: %s", e)


def register(app: Application, db):
    ensure_schema(db)
    app.bot_data.setdefault("db", db)
    app.add_handler(CommandHandler("statuses", cmd_statuses))
    app.add_handler(MessageHandler(filters.Regex(r"^/статусы(@\w+)?(\s|$)"), cmd_statuses))
    if app.job_queue:
        app.job_queue.run_repeating(_job, interval=JOB_INTERVAL_SEC, first=90, name="delivery_statuses")
    logger.info("delivery_statuses: register (write=%s)", _write_enabled())
