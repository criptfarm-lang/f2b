"""
Авто-статусы отгрузки (Фаза 2 плана логистической оркестрации 2026-07-09).

По маршруту Wialon + live-позициям машин + чеклисту водителя автоматически
проставляет статус отгрузки в МойСклад:
  - «В пути»            — машина выехала из геозоны базы (Ильинский);
  - «Задержка в пути»   — по РЕАЛЬНОМУ окну приёмки, в двух случаях:
                          • ФАКТ: конец окна уже прошёл, а машина не доехала (по GPS);
                          • ПРОГНОЗ: план точки + текущее отставание машины перепрыгивают
                            конец окна (знаем о задержке заранее, до конца окна).
                          При смене статуса на «Задержка» → уведомление владелец+логисты
                          (Белякова, Петровский)+Маланчук (одно на машину за прогон,
                          список точек, прогноз ИЛИ факт).
                          Менеджеру — отдельно, по ФАКТУ опоздания > 60 мин.
  - «Сдан»/«Сдан с проблемой» — из чеклиста водителя (событийно, см. driver_checklist).

ВЫПОЛНЕНИЕ ТОЧКИ = СТАТУС ОТГРУЗКИ В МС (источник истины). Любой статус, кроме
активных на маршруте (Отгружен / В пути / Задержка в пути), = доставка закрыта
(Сдан / Сдан с проблемой / Едет возврат / УПД подписан / Долг / Оплачен). По таким
точкам НЕ считаем отставание и НЕ шлём алерты — даже если заявка ещё висит в Wialon
(вчерашние заявки нельзя удалить, они остаются). Так фейковое «отставание ~сутки»
на закрытых вчерашних точках исключается по факту в МС, а не только по дате.

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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

from moysklad import MS_BASE, get_headers, PDZ_MANAGER_TG_IDS

logger = logging.getLogger(__name__)
_MSK = timezone(timedelta(hours=3))

# База (загрузка утром) — Ильинский, Раменское
BASE_LAT, BASE_LON = 55.6323, 38.1038

# Точки ночёвки/гаражи водителей: простой здесь = не проблема, а «конец маршрута».
# 26209 (В 970 СВ 797, Мага) → гараж Химки. 26210 (К 459 ХК) ночует на базе (см. _near_base).
HOME_POINTS = {26209: (55.9033, 37.4062)}
R_HOME_M = 300
R_BASE_M = 500       # «В пути» — удаление транспорта на 500 м от склада (спека собственника)
R_STOP_M = 300       # радиус «прибыл на точку»
DELAY_BUFFER_MIN = 30
LAG_ALERT_MIN = 120        # отставание от плана ≥ этого (даже при плавающих окнах) → тревога
LAG_ALERT_POINTS = 2       # ...и хотя бы столько непосещённых точек позади плана
IDLE_MIN = 60              # простой: машина стоит на месте ≥ этого → тревога логисту
ROUTE_END_MIN = 10         # на точке ночёвки ≥ этого → фиксируем «конец маршрута» в моменте
IDLE_RADIUS_M = 150        # «на месте»: не отходит дальше этого от точки стоянки
DELIVERY_HOURS = (7, 21)   # МСК, в эти часы крон активен
JOB_INTERVAL_SEC = 600            # дневной опрос МС↔логистика — каждые 10 мин
JOB_INTERVAL_EVENING_SEC = 300    # вечерний (активный развоз) — каждые 5 мин
EVENING_FAST_HOUR = 17            # МСК: с этого часа переходим на 5-мин опрос
DWELL_UNLOAD_MIN = 5       # стоянка у точки клиента ≥ этого = вероятная выгрузка (нижний порог, верхнего нет)
UNLOAD_SPEED_KMH = 5       # «стоит»: мгновенная скорость ≤ этого (иначе — проезд мимо, не выгрузка)

# Статусы, которые авто-движок ИМЕЕТ ПРАВО менять (логистические, в процессе развоза).
# Всё остальное — ручное/бухгалтерское (Отгружен ставит оператор; УПД подписан / Долг /
# Оплачен — оператор; Сдан / Сдан с проблемой / Едет возврат — терминальные) — НЕ трогаем.
MANAGED = {"Отгружен", "В пути", "Задержка в пути"}


def _write_enabled() -> bool:
    return os.getenv("DELIVERY_STATUS_WRITE", "0") == "1"


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
    return ids[0] if ids else 0  # Белякова (первичный)


def _partner_chat_id() -> int:
    return int(os.getenv("PARTNER_CHAT_ID", "772630562") or 0)  # Маланчук


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
    db._execute("ALTER TABLE route_status_state ADD COLUMN IF NOT EXISTS delay_mgr_alerted BOOLEAN DEFAULT FALSE")
    db._execute("ALTER TABLE route_status_state ADD COLUMN IF NOT EXISTS write_fail_target TEXT")
    # Детект «выгрузил, но не отметил»: копим стоянку у точки (near_since..near_last), дедуп unload_alerted.
    db._execute("ALTER TABLE route_status_state ADD COLUMN IF NOT EXISTS near_since BIGINT")
    db._execute("ALTER TABLE route_status_state ADD COLUMN IF NOT EXISTS near_last BIGINT")
    db._execute("ALTER TABLE route_status_state ADD COLUMN IF NOT EXISTS unload_alerted BOOLEAN DEFAULT FALSE")
    db._execute("""
        CREATE TABLE IF NOT EXISTS unit_dwell (
            unit_id     BIGINT PRIMARY KEY,
            lat         DOUBLE PRECISION,
            lon         DOUBLE PRECISION,
            since_ts    BIGINT,
            alerted     BOOLEAN DEFAULT FALSE,
            updated_at  TIMESTAMPTZ DEFAULT now()
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


# ─── Простой машины (dwell watchdog) ─────────────────────────────────────────

def _dwell_get(db, uid):
    return db._fetchone("SELECT * FROM unit_dwell WHERE unit_id=%s", (uid,))


def _dwell_set(db, uid, lat, lon, since_ts, alerted):
    if _dwell_get(db, uid) is None:
        db._execute("INSERT INTO unit_dwell (unit_id,lat,lon,since_ts,alerted) VALUES (%s,%s,%s,%s,%s)",
                    (uid, lat, lon, since_ts, alerted))
    else:
        db._execute("UPDATE unit_dwell SET lat=%s,lon=%s,since_ts=%s,alerted=%s,updated_at=now() WHERE unit_id=%s",
                    (lat, lon, since_ts, alerted, uid))


def _near_base(lat, lon) -> bool:
    return _haversine(lat, lon, BASE_LAT, BASE_LON) <= R_BASE_M


def _near_home(uid, lat, lon) -> bool:
    h = HOME_POINTS.get(uid)
    return bool(h) and _haversine(lat, lon, h[0], h[1]) <= R_HOME_M


def _near_any_client(lat, lon, stops) -> bool:
    for s in stops:
        if s.get("lat") and s.get("lon") and _haversine(lat, lon, s["lat"], s["lon"]) <= R_STOP_M:
            return True
    return False


def _check_idle(db, uid, name, pos, stops, now_ts, preview):
    """Сторож простоя: машина стоит на месте ≥ IDLE_MIN, не на базе и не у клиента → тревога.
    Возвращает строку-описание, если простой обнаружен (для превью/лога), иначе None."""
    if not pos or not pos.get("lat"):
        return None
    lat, lon = pos["lat"], pos["lon"]
    row = _dwell_get(db, uid) or {}
    anchored = row.get("since_ts") and row.get("lat") is not None \
        and _haversine(lat, lon, row["lat"], row["lon"]) <= IDLE_RADIUS_M
    if not anchored:
        # машина сдвинулась (или первое наблюдение) — новый якорь, счётчик с нуля
        if not preview:
            _dwell_set(db, uid, lat, lon, now_ts, False)
        return None
    since = row["since_ts"]
    idle_min = int((now_ts - since) / 60)
    if _near_base(lat, lon) or _near_any_client(lat, lon, stops):
        return None
    on_home = _near_home(uid, lat, lon)
    # Дом/гараж → «конец маршрута» с коротким порогом (фиксируем время финиша в моменте);
    # прочие точки → «ПРОСТОЙ» с длинным порогом.
    threshold = ROUTE_END_MIN if on_home else IDLE_MIN
    if idle_min >= threshold:
        kind = "route_end" if on_home else "idle"
        if kind == "route_end":
            end_hm = datetime.fromtimestamp(since, _MSK).strftime("%H:%M")
            line = (f"{name}: конец маршрута в {end_hm} (на точке ночёвки) "
                    f"[{lat:.4f},{lon:.4f}]")
        else:
            line = (f"{name}: ПРОСТОЙ ~{idle_min} мин на месте (не база, не клиент) "
                    f"[{lat:.4f},{lon:.4f}]")
        fire = (not preview) and (not row.get("alerted"))
        return {"line": line, "fire": fire, "since": since, "idle_min": idle_min,
                "lat": lat, "lon": lon, "anchor": (row["lat"], row["lon"]), "kind": kind}
    return None


async def _idle_alert(bot, unit_name, info):
    since = datetime.fromtimestamp(info["since"], _MSK).strftime("%H:%M")
    link = f"https://yandex.ru/maps/?pt={info['lon']:.5f},{info['lat']:.5f}&z=17&l=map"
    text = (f"🅿️ ПРОСТОЙ машины\n{unit_name}\n"
            f"Стоит на месте ~{info['idle_min']} мин (с {since}) — не база и не адрес клиента.\n"
            f"Точка: {info['lat']:.4f},{info['lon']:.4f}\n{link}")
    seen = set()
    for cid in [_owner_chat_id(), *_logist_chat_ids(), _partner_chat_id()]:
        if not cid or cid in seen:
            continue
        seen.add(cid)
        try:
            await bot.send_message(chat_id=cid, text=text)
        except Exception as e:
            logger.warning("_idle_alert → %s: %s", cid, e)


async def _route_end_alert(bot, unit_name, info):
    since = datetime.fromtimestamp(info["since"], _MSK).strftime("%H:%M")
    link = f"https://yandex.ru/maps/?pt={info['lon']:.5f},{info['lat']:.5f}&z=17&l=map"
    text = (f"🏁 Конец маршрута\n{unit_name}\n"
            f"Вернулась на точку ночёвки (с {since}) — развоз на сегодня завершён.\n"
            f"Точка: {info['lat']:.4f},{info['lon']:.4f}\n{link}")
    seen = set()
    for cid in [_owner_chat_id(), *_logist_chat_ids(), _partner_chat_id()]:
        if not cid or cid in seen:
            continue
        seen.add(cid)
        try:
            await bot.send_message(chat_id=cid, text=text)
        except Exception as e:
            logger.warning("_route_end_alert → %s: %s", cid, e)


def _unload_recipients() -> list:
    """Контролёры выгрузки: Саша(Белякова)+Виктор+Александр(Маланчук)+Володя(Петровский)
    = owner + логисты + партнёр, с дедупом (Белякова уже среди логистов)."""
    seen = []
    for cid in [_owner_chat_id(), *_logist_chat_ids(), _partner_chat_id()]:
        if cid and cid not in seen:
            seen.append(cid)
    return seen


async def _unload_driver_alert(bot, driver_chat, unit_name, uf):
    """Персональный алерт водителю: похоже, выгрузил, но не отметил. Кнопка → полный чек-лист точки."""
    if not driver_chat:
        return
    import route_dispatch
    s = uf["s"]
    label = route_dispatch._stop_label(s)
    doc = route_dispatch._doc_no(s.get("order_no"))
    text = (f"🔔 Похоже, выгрузка состоялась, но сдача не отмечена.\n"
            f"{label} — стоянка ~{uf['dwell_min']} мин, точка не закрыта.\n"
            f"Открой чек-лист и заверши сдачу:")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(
        f"📋 Заполнить чек-лист: {label}", callback_data=f"drv:rp:{doc}")]])
    try:
        await bot.send_message(chat_id=driver_chat, text=text, reply_markup=kb)
    except Exception as e:
        logger.warning("_unload_driver_alert → %s: %s", driver_chat, e)


async def _unload_control_alert(bot, unit_name, uf, recipients):
    """Эскалация контролёрам: машина простояла и уехала, а выгрузка не подтверждена."""
    import route_dispatch
    label = route_dispatch._stop_label(uf["s"])
    text = (f"⚠️ Контроль выгрузки — {unit_name}\n"
            f"Простояла у точки ~{uf['dwell_min']} мин и уехала, сдача не подтверждена.\n"
            f"{label}\nПроконтролируйте, что выгрузка состоялась и водитель закрыл точку.")
    for cid in recipients:
        try:
            await bot.send_message(chat_id=cid, text=text)
        except Exception as e:
            logger.warning("_unload_control_alert → %s: %s", cid, e)


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
    agent = o.get("agent") or {}
    return {
        "demand_id": dem.get("id"),
        "state_name": (dem.get("state") or {}).get("name"),
        "agent_tags": agent.get("tags", []) or [],
        "agent_name": agent.get("name"),
        "sum": dem.get("sum") or 0,
        "payedSum": dem.get("payedSum") or 0,
    }


def _is_retail(name) -> bool:
    return bool(name) and name.strip().startswith("Розничный покупатель")


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

def _has_real_window(tf, tt) -> bool:
    """Реальное окно приёмки (клиент ждёт в интервал), а не плавающее 09:00–18:00 (дефолт моста)."""
    if not tf or not tt:
        return False
    a = datetime.fromtimestamp(tf, _MSK)
    b = datetime.fromtimestamp(tt, _MSK)
    if (a.hour, a.minute, b.hour, b.minute) == (9, 0, 18, 0):
        return False
    return True


def _stop_is_today(s, now_ts) -> bool:
    """Точка относится к СЕГОДНЯШНЕМУ дню развоза. Якорь времени: vt (план маршрута) →
    tt (конец окна) → tf (начало окна). Нет якоря — не режем (датировать нечем).
    Защита от вчерашнего маршрута, ещё висящего в Wialon до вечерней перестройки."""
    ref = s.get("vt") or s.get("tt") or s.get("tf")
    if not ref:
        return True
    today = datetime.fromtimestamp(now_ts, _MSK).date()
    return datetime.fromtimestamp(ref, _MSK).date() == today


def _target_status(*, left_base, arrived, tt, vt, now_ts, lag_sec, chk_status, real_window):
    """chk_status — статус из delivery_checklist (сдан/сдан с проблемой) или None.
    «Задержка в пути» — по РЕАЛЬНОМУ окну, в двух случаях:
      • ФАКТ: конец окна `tt` уже прошёл (now_ts > tt);
      • ПРОГНОЗ: план точки `vt` + текущее отставание машины `lag_sec` перепрыгивает
        конец окна — знаем о задержке заранее, не дожидаясь конца окна.
    Плавающее окно (09:00–18:00, дефолт моста) — не опоздание."""
    if chk_status == "сдан с проблемой":
        return "Сдан с проблемой"
    if chk_status == "сдан":
        return "Сдан"
    if not left_base:
        return None                      # ещё на базе — не трогаем «Отгружен»
    if arrived:
        return "В пути"                  # приехал, ждём чеклист
    if real_window and tt:
        if now_ts > tt:
            return "Задержка в пути"      # реальное окно нарушено (факт)
        if vt and (vt + lag_sec) > tt:
            return "Задержка в пути"      # прогноз прибытия за концом окна
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
            # Только СЕГОДНЯШНИЙ маршрут. Вчерашняя раскладка висит в Wialon до вечерней
            # перестройки — её прошедший план дал бы фейковое отставание в ~сутки (лаг-алерт,
            # прогноз «Задержки», факт по окну). Датируем точку по vt→tt→tf.
            stops = [s for s in (routes.get(uid) or []) if _stop_is_today(s, now_ts)]
            pos = positions.get(uid)
            left_base = bool(pos) and _haversine(pos["lat"], pos["lon"], BASE_LAT, BASE_LON) > R_BASE_M
            machine = []   # для расчёта отставания / риска смещения
            recs = []      # разрешённые точки (МС + прибытие + чеклист) — решаем статус вторым проходом
            unload_flips = []  # точки, где машина простояла и уехала, а сдача не закрыта
            for s in stops:
                order_no = s["order_no"]
                info = await _ms_resolve(session, order_no)
                if not info or not info["demand_id"]:
                    continue
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
                real_window = _has_real_window(s.get("tf"), s.get("tt"))
                cur = info["state_name"]
                # Источник истины «точка выполнена» — статус отгрузки в МС. Всё, кроме активных
                # на маршруте (Отгружен/В пути/Задержка), = доставка закрыта (Сдан / Сдан с
                # проблемой / Едет возврат / УПД подписан / Долг / Оплачен). По таким точкам не
                # считаем отставание и не алертим — даже если в Wialon заявка ещё висит.
                done = bool(cur) and cur not in MANAGED

                # ── Детект «выгрузил, но не отметил» ──
                # Копим стоянку у точки: пока машина СТОИТ (speed≤порог) в радиусе R_STOP_M —
                # ведём near_since (первый раз) и near_last (каждый прогон). Когда уехала из
                # радиуса, а точка НЕ закрыта и стоянка была ≥ DWELL_UNLOAD_MIN — кандидат на
                # алерт (дедуп unload_alerted). Крон 10 мин: реальная выгрузка 5–30 мин почти
                # всегда ловится на ≥2 прогонах → near_last−near_since ≥ порога.
                if not preview and pos and s.get("lat") and s.get("lon"):
                    near_stopped = (_haversine(pos["lat"], pos["lon"], s["lat"], s["lon"]) <= R_STOP_M
                                    and (pos.get("speed") or 0) <= UNLOAD_SPEED_KMH)
                    if near_stopped:
                        if not st.get("near_since"):
                            _st_upsert(db, order_no, near_since=now_ts, near_last=now_ts,
                                       demand_id=info["demand_id"], unit_id=uid)
                        else:
                            _st_upsert(db, order_no, near_last=now_ts,
                                       demand_id=info["demand_id"], unit_id=uid)
                    else:
                        ns, nl = st.get("near_since"), st.get("near_last")
                        if (ns and not done and not st.get("unload_alerted")
                                and ((nl or ns) - ns) >= DWELL_UNLOAD_MIN * 60):
                            unload_flips.append({"s": s, "order_no": order_no,
                                                 "demand_id": info["demand_id"],
                                                 "dwell_min": int(((nl or ns) - ns) / 60)})

                machine.append({"s": s, "arrived": arrived, "done": done, "real_window": real_window})
                recs.append({"s": s, "order_no": order_no, "info": info, "cur": cur,
                             "st": st, "arrived": arrived, "chk_status": chk_status,
                             "real_window": real_window})

            # Отставание машины считаем ПО ВСЕМ точкам — нужно для прогнозной «Задержки в пути»
            # (точка получает статус до конца окна, если план + отставание перепрыгивают окно).
            lag_sec = _machine_lag(machine, now_ts)
            delay_flips = []   # точки, чей статус в этом прогоне стал «Задержка в пути»

            for rec in recs:
                s, order_no, info = rec["s"], rec["order_no"], rec["info"]
                cur, st = rec["cur"], rec["st"]
                target = _target_status(left_base=left_base, arrived=rec["arrived"],
                                        tt=s.get("tt"), vt=s.get("vt"), now_ts=now_ts,
                                        lag_sec=lag_sec, chk_status=rec["chk_status"],
                                        real_window=rec["real_window"])
                # Розничная касса: после «Сдан» + полная оплата → «Оплачен» (наличные привязаны
                # к заказу, payedSum достоверен только для розницы; банк-клиентов не трогаем).
                if (cur == "Сдан" and _is_retail(info.get("agent_name"))
                        and (info.get("sum") or 0) > 0
                        and (info.get("payedSum") or 0) >= (info.get("sum") or 0)):
                    lines.append(f"{name} №{order_no} {(info.get('agent_name') or '')[:20]}: Сдан → Оплачен")
                    if write:
                        meta = states.get("Оплачен")
                        if meta and await _ms_set_state(session, info["demand_id"], meta):
                            _st_upsert(db, order_no, ms_status="Оплачен",
                                       demand_id=info["demand_id"], unit_id=uid)
                    continue
                # трогаем только логистические статусы; ручные/бухгалтерские/терминальные — нет
                if not target or cur not in MANAGED:
                    continue
                if cur == "Задержка в пути" and target == "В пути":
                    continue  # не понижаем
                if target == cur:
                    continue
                lines.append(f"{name} №{order_no} {s['client'][:24]}: {cur} → {target}")
                wrote_ok = None   # None — не писали (сухой режим); True/False — итог PUT
                if write:
                    meta = states.get(target)
                    wrote_ok = bool(meta) and await _ms_set_state(session, info["demand_id"], meta)
                    if wrote_ok:
                        _st_upsert(db, order_no, ms_status=target, write_fail_target=None,
                                   demand_id=info["demand_id"], unit_id=uid)
                    else:
                        logger.warning("МС-запись №%s %s→%s не прошла (meta=%s)", order_no, cur, target, bool(meta))
                        if bot and st.get("write_fail_target") != target:
                            await _write_fail_alert(bot, name, order_no, s.get("client"), cur, target, bool(meta))
                            _st_upsert(db, order_no, write_fail_target=target,
                                       demand_id=info["demand_id"], unit_id=uid)
                # Смена статуса на «Задержка в пути» → уведомление владелец+логисты (Белякова, Петровский)+Маланчук.
                # Копим по машине, шлём одним сообщением ниже. Триггер — сам факт смены статуса
                # (прогноз ИЛИ факт), один раз на заказ (дедуп delay_alerted).
                tt = s.get("tt") or 0
                if target == "Задержка в пути" and not st.get("delay_alerted"):
                    delay_flips.append({"s": s, "order_no": order_no, "demand_id": info["demand_id"]})
                # Опоздание > 60 мин ПО ФАКТУ — дополнительно ответственному менеджеру.
                if (target == "Задержка в пути" and now_ts > tt and (now_ts - tt) / 60 >= 60
                        and not st.get("delay_mgr_alerted")):
                    if bot and not preview:
                        _tag, mgr = _manager_chat(info["agent_tags"])
                        if mgr:
                            await _delay_alert(bot, name, s, [mgr], over60=True)
                        _st_upsert(db, order_no, delay_mgr_alerted=True,
                                   demand_id=info["demand_id"], unit_id=uid)

            # ── Уведомление о смене статуса на «Задержка в пути» ──
            # Владелец + логисты (Белякова, Петровский) + Маланчук: одно сообщение на машину за прогон, список точек.
            if delay_flips:
                lines.append(f"{name}: уведомление о задержке — точек: {len(delay_flips)}")
                if bot and not preview:
                    await _delay_flip_alert(bot, name, [d["s"] for d in delay_flips], lag_sec, now_ts,
                                            [_owner_chat_id(), *_logist_chat_ids(), _partner_chat_id()])
                if not preview:
                    for d in delay_flips:
                        _st_upsert(db, d["order_no"], delay_alerted=True,
                                   demand_id=d["demand_id"], unit_id=uid)

            # ── Алерт «выгрузка состоялась, кнопка не нажата» ──
            # Машина простояла у точки ≥5 мин и уехала, а сдача не закрыта. Водителю —
            # персонально с кнопкой на полный чек-лист; контролёрам (Саша/Виктор/Александр/
            # Володя = owner+логисты+партнёр) — эскалация. Дедуп на заказ (unload_alerted).
            if unload_flips:
                lines.append(f"{name}: выгрузка без подтверждения — точек: {len(unload_flips)}")
                if bot and not preview:
                    drow = db._fetchone(
                        "SELECT chat_id FROM drivers WHERE unit_id=%s AND active LIMIT 1", (uid,))
                    driver_chat = (drow or {}).get("chat_id")
                    controllers = _unload_recipients()
                    for uf in unload_flips:
                        await _unload_driver_alert(bot, driver_chat, name, uf)
                        await _unload_control_alert(bot, name, uf, controllers)
                if not preview:
                    for uf in unload_flips:
                        _st_upsert(db, uf["order_no"], unload_alerted=True,
                                   demand_id=uf["demand_id"], unit_id=uid)

            # ── Отставание от плана (для лаг-алерта ниже; риск-алерт заменён уведомлением выше) ──
            risk = _compute_risk(machine, now_ts)

            # ── Отставание от плана ≥2ч по ≥2 точкам (ловит коллапс дня и при плавающих окнах) ──
            behind = risk.get("behind") or []
            if risk["lag_min"] >= LAG_ALERT_MIN and len(behind) >= LAG_ALERT_POINTS:
                lines.append(f"{name}: ОТСТАВАНИЕ ~{risk['lag_min']} мин, точек позади плана: {len(behind)}")
                if bot and not preview:
                    lkey = f"lag:{uid}:{datetime.now(_MSK).date().isoformat()}"
                    if not (_st_get(db, lkey) or {}).get("delay_alerted"):
                        await _lag_alert(bot, name, risk["lag_min"], behind, now_ts)
                        _st_upsert(db, lkey, delay_alerted=True, unit_id=uid)

            # ── Простой машины (стоит на месте ≥ IDLE_MIN, не база и не клиент) ──
            idle = _check_idle(db, uid, name, pos, stops, now_ts, preview)
            if idle:
                lines.append(idle["line"])
                if idle["fire"] and bot:
                    if idle["kind"] == "route_end":
                        await _route_end_alert(bot, name, idle)
                    else:
                        await _idle_alert(bot, name, idle)
                    _dwell_set(db, uid, idle["anchor"][0], idle["anchor"][1], idle["since"], True)
    return lines


def _machine_lag(machine, now_ts) -> int:
    """Отставание машины, сек = макс. по НЕзакрытым точкам, чей план (`vt`) уже прошёл.
    Закрытая (`done` по статусу МС) или прибывшая точка в отставание не идёт."""
    lag_sec = 0
    for m in machine:
        vt = m["s"].get("vt")
        if not m["arrived"] and not m.get("done") and vt and now_ts > vt:
            lag_sec = max(lag_sec, now_ts - vt)
    return lag_sec


def _compute_risk(machine, now_ts):
    """lag = насколько машина отстаёт (макс. по незакрытым точкам с прошедшим планом);
    at_risk = будущие точки с РЕАЛЬНЫМ окном, чьё окно ещё открыто, но сдвиг его срывает.
    Точки, закрытые по статусу МС (`done`), в расчёт не берём."""
    lag_sec = _machine_lag(machine, now_ts)
    behind = []   # непосещённые НЕзакрытые точки, чьё плановое время уже прошло
    for m in machine:
        vt = m["s"].get("vt")
        if not m["arrived"] and not m.get("done") and vt and now_ts > vt:
            behind.append(m["s"])
    at_risk = []
    for m in machine:
        s = m["s"]
        tt, vt = s.get("tt"), s.get("vt")
        if m["arrived"] or m.get("done") or not m["real_window"] or not tt or not vt:
            continue
        if now_ts < tt and (vt + lag_sec) > tt:
            at_risk.append(s)
    return {"lag_min": int(lag_sec / 60), "lag_sec": lag_sec,
            "at_risk": at_risk, "behind": behind}


async def _delay_flip_alert(bot, unit_name, stops, lag_sec, now_ts, recipients):
    """Уведомление владелец+логисты (Белякова, Петровский)+Маланчук: у этих точек статус стал «Задержка в пути»
    (прогноз ИЛИ факт). Одно сообщение на машину за прогон."""
    def _hm(t):
        return datetime.fromtimestamp(t, _MSK).strftime("%H:%M") if t else "—"

    def line(s):
        tf, tt, vt = s.get("tf"), s.get("tt"), s.get("vt")
        win = f"{_hm(tf)}–{_hm(tt)}" if tf and tt else "—"
        proj = _hm(vt + lag_sec) if vt else "—"
        tail = ""
        if tt and now_ts > tt:                    # окно уже прошло (факт)
            tail = f" — уже просрочено ~{int((now_ts - tt) / 60)} мин"
        return (f"• {s['client'][:30]} (№{s['order_no']}) окно {win}\n"
                f"   план {_hm(vt)} → ожидается ~{proj}{tail}")
    rows = "\n".join(line(s) for s in stops)
    text = (f"⏱ Статус → «Задержка в пути»\n{unit_name} отстаёт ~{int(lag_sec / 60)} мин от плана.\n"
            f"Не укладываемся в окно приёмки:\n{rows}")
    seen = set()
    for cid in recipients:
        if not cid or cid in seen:
            continue
        seen.add(cid)
        try:
            await bot.send_message(chat_id=cid, text=text)
        except Exception as e:
            logger.warning("_delay_flip_alert → %s: %s", cid, e)


async def _write_fail_alert(bot, unit_name, order_no, client, cur, target, has_meta):
    """Статус в МС не записался (PUT не 200 или статуса нет в метаданных). Сигнал владельцу+логисту."""
    reason = "PUT отклонён МойСкладом (не 200)" if has_meta else f"статуса «{target}» нет в метаданных МС"
    text = (f"🛑 Статус в МойСклад НЕ записан\n{unit_name} №{order_no} {client or ''}\n"
            f"{cur} → {target}: {reason}.\nПроверьте отгрузку вручную.")
    seen = set()
    for cid in [_owner_chat_id(), *_logist_chat_ids()]:
        if not cid or cid in seen:
            continue
        seen.add(cid)
        try:
            await bot.send_message(chat_id=cid, text=text)
        except Exception as e:
            logger.warning("_write_fail_alert → %s: %s", cid, e)


async def _lag_alert(bot, unit_name, lag_min, behind, now_ts):
    """Машина системно отстаёт от плана (даже при плавающих окнах) — день под угрозой."""
    def line(s):
        vt = s.get("vt")
        plan = datetime.fromtimestamp(vt, _MSK).strftime("%H:%M") if vt else "—"
        over = int((now_ts - vt) / 60) if vt else 0
        return f"• {s['client'][:30]} (№{s['order_no']}) план {plan} — просрочено ~{over} мин"
    rows = "\n".join(line(s) for s in behind)
    text = (f"⏳ ОТСТАВАНИЕ ОТ ГРАФИКА\n{unit_name}\n"
            f"Машина отстаёт от плана ~{lag_min} мин (≈{lag_min // 60} ч). "
            f"Даже с плавающими окнами день под угрозой — все точки можно не успеть.\n"
            f"Позади плана ({len(behind)}):\n{rows}")
    seen = set()
    for cid in [_owner_chat_id(), *_logist_chat_ids(), _partner_chat_id()]:
        if not cid or cid in seen:
            continue
        seen.add(cid)
        try:
            await bot.send_message(chat_id=cid, text=text)
        except Exception as e:
            logger.warning("_lag_alert → %s: %s", cid, e)


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
            "tf": o.get("tf"), "tt": o.get("tt"),
            "client": p.get("n") or "?", "order_no": o.get("n"),
            "lat": o.get("y"), "lon": o.get("x"),
        })
    for uid in routes:
        routes[uid].sort(key=lambda s: (s["seq"] if s["seq"] is not None else 999))
    return routes


async def _delay_alert(bot, unit_name, stop, recipients, over60=False, status_written=True):
    vt = stop.get("vt")
    plan = datetime.fromtimestamp(vt, _MSK).strftime("%H:%M") if vt else "—"
    if over60:
        text = (f"⏱ ЗАДЕРЖКА больше 60 мин\n{unit_name}\n"
                f"{stop['client']} (№{stop['order_no']})\n"
                f"План прибытия {plan} — опоздание больше часа.")
    else:
        text = (f"⏱ ЗАДЕРЖКА в пути\n{unit_name}\n"
                f"{stop['client']} (№{stop['order_no']})\n"
                f"План прибытия {plan} + 30 мин — машина ещё не на точке.")
    if status_written is False:
        text += "\n⚠️ Статус «Задержка в пути» в МойСклад выставить НЕ удалось — проверьте отгрузку вручную."
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
    if not (user.id == dc._owner_chat_id() or user.id in dc._logist_chat_ids()):
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


_last_run_ts = 0.0  # факт. время прошлого прогона — для гейта дневной/вечерний интервал


async def _job(context: ContextTypes.DEFAULT_TYPE):
    global _last_run_ts
    now = datetime.now(_MSK)
    h = now.hour
    if not (DELIVERY_HOURS[0] <= h < DELIVERY_HOURS[1]):
        return
    # Тик каждые 5 мин, но ДО вечернего часа пропускаем каждый второй тик → 10 мин;
    # с EVENING_FAST_HOUR (17:00) прогоняем каждый тик → 5 мин. Гейт по факт. паузе
    # (устойчив к джиттеру тика; глобал сбрасывается при рестарте — не страшно).
    now_ts = now.timestamp()
    min_gap = JOB_INTERVAL_EVENING_SEC if h >= EVENING_FAST_HOUR else JOB_INTERVAL_SEC
    if now_ts - _last_run_ts < min_gap - 45:
        return
    _last_run_ts = now_ts
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
        app.job_queue.run_repeating(_job, interval=JOB_INTERVAL_EVENING_SEC, first=90, name="delivery_statuses")
    logger.info("delivery_statuses: register (write=%s)", _write_enabled())
