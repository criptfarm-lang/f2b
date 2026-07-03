"""
Светофор техопераций (бот «Эф»). План (второй мозг):
plans/2026-07-03-светофор-техопераций-эф.md — Фаза 2 (read-only).

По каждой техоперации МС бот шлёт Виктору (OWNER_CHAT_ID) и Маланчуку (PARTNER_CHAT_ID)
оценку себестоимости (₽/кг) и выхода (%) относительно нормы — медианы по проверенным
операциям того же SKU+типа сырья (view production.processing_stats).

Триггеры (polling каждые 30 мин, вебхука у МС нет):
  - новая техоперация (нет в логе отправок);
  - state операции стал «Анализ сделан» (повторный светофор).

Себестоимость — через ОБОРОТ /report/turnover/all (FIFO-выбытие), НЕ через остатки
(остатки дают 0 для партии, списанной в ноль). Кеш карты {товар→₽/ед} по дню.

Независим от bot.py: свой psycopg2-коннект в schema production, MS через httpx
(глобальный throttle бота применяется к AsyncClient автоматически). Кнопок нет (Фаза 2).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import httpx
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# MS_BASE/get_headers определены локально (не импортируем moysklad.py — у него import-time
# зависимость от YANDEX_GEOCODER_KEY). Глобальный httpx-throttle бота патчит AsyncClient
# в процессе (moysklad импортируется в bot.py), поэтому наши вызовы тоже троттлятся.
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"


def get_headers() -> dict:
    return {
        "Authorization": f"Bearer {os.environ['MOYSKLAD_TOKEN']}",
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json",
    }

TOL = 0.02              # ±2% от медианы — жёлтая зона
COST_FLOOR = 200        # ₽/кг: ниже — битая себест. выбытия (near-zero партия)
YIELD_MIN, YIELD_MAX = 30, 120  # % выхода вне диапазона — мусор состава
ANALIZ_STATE = "Анализ сделан"
POLL_DAYS = 7

LOG_DDL = """
create schema if not exists production;
create table if not exists production.processing_svetofor_log (
    processing_id  uuid primary key,
    name           text,
    last_state     text,
    first_sent_at  timestamptz,
    analiz_sent_at timestamptz,
    updated_at     timestamptz default now()
);
"""


# ── DB (свой коннект, autocommit) ──────────────────────────────────────────
_conn = None


def _db():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(os.environ["DATABASE_URL"],
                                 cursor_factory=psycopg2.extras.RealDictCursor)
        _conn.autocommit = True
        with _conn.cursor() as cur:
            cur.execute(LOG_DDL)
    return _conn


# ── MS ─────────────────────────────────────────────────────────────────────
async def _ms_get(path: str, params: dict | None = None) -> dict:
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.get(f"{MS_BASE}{path}", headers=get_headers(), params=params)
        r.raise_for_status()
        return r.json()


def _norm(h: str | None) -> str | None:
    return h.split("?")[0].rstrip("/") if h else None


# ── расчёт ──────────────────────────────────────────────────────────────────
_day_cache: dict[str, dict[str, float]] = {}


async def _day_turnover(day: str) -> dict[str, float]:
    if day in _day_cache:
        return _day_cache[day]
    out, offset = {}, 0
    while True:
        d = await _ms_get("/report/turnover/all", {
            "momentFrom": f"{day} 00:00:00.000", "momentTo": f"{day} 23:59:59.999",
            "limit": 1000, "offset": offset,
        })
        rows = d.get("rows") or []
        for r in rows:
            href = _norm((r.get("assortment") or {}).get("meta", {}).get("href"))
            oc = r.get("outcome") or {}
            q = oc.get("quantity") or 0
            s = (oc.get("sum") or 0) / 100.0
            if href and q:
                out[href] = s / q
        if len(rows) < 1000:
            break
        offset += 1000
    _day_cache[day] = out
    return out


def _classify_fish(name: str) -> str | None:
    up = (name or "").upper()
    return "ПБГ" if "ПБГ" in up else ("ПСГ" if "ПСГ" in up else None)


async def _positions(pid: str, kind: str) -> list[dict]:
    d = await _ms_get(f"/entity/processing/{pid}/{kind}", {"expand": "assortment", "limit": 1000})
    return d.get("rows") or []


async def compute(pid: str, name: str, moment: str, state: str | None) -> dict | None:
    prods = await _positions(pid, "products")
    if not prods:
        return None
    mats = await _positions(pid, "materials")

    out_pos = [((p.get("assortment") or {}).get("code"),
                (p.get("assortment") or {}).get("name"),
                float(p.get("quantity") or 0)) for p in prods]
    out_qty = sum(q for _, _, q in out_pos)
    out_multi = len(out_pos) > 1
    main = max(out_pos, key=lambda t: t[2])
    out_sku_code, out_sku_name, _ = main

    mat_rows, fish = [], None
    for m in mats:
        a = m.get("assortment") or {}
        href = (a.get("meta") or {}).get("href")
        qty = float(m.get("quantity") or 0)
        if href:
            mat_rows.append((href, qty))
        ft = _classify_fish(a.get("name"))
        if ft and (fish is None or qty > fish[1]):
            fish = (a.get("name"), qty, ft)

    if fish is None:
        fish_name, fish_qty, fish_type, yield_pct = None, None, "NONE", None
    else:
        fish_name, fish_qty, fish_type = fish
        yield_pct = round(out_qty / fish_qty * 100, 2) if fish_qty else None

    perunit = await _day_turnover(moment[:10])
    cost_total, missing = 0.0, 0
    for h, q in mat_rows:
        pu = perunit.get(_norm(h))
        if pu is None:
            missing += 1
        else:
            cost_total += pu * q
    cost_per_kg = round(cost_total / out_qty, 2) if (out_qty and missing == 0) else None

    return {
        "name": name, "moment": moment, "check_status": state,
        "out_sku_code": out_sku_code, "out_sku_name": out_sku_name,
        "out_qty": out_qty, "out_multi": out_multi,
        "fish_type": fish_type, "fish_qty": fish_qty,
        "cost_per_kg": cost_per_kg, "yield_pct": yield_pct,
    }


def _color_cost(v, m):
    if m is None or v is None:
        return "⚪"
    return "🟢" if v <= m * (1 - TOL) else ("🔴" if v >= m * (1 + TOL) else "🟡")


def _color_yield(v, m):
    if m is None or v is None:
        return "⚪"
    return "🟢" if v >= m * (1 + TOL) else ("🔴" if v <= m * (1 - TOL) else "🟡")


def _overall(*cs):
    rank = {"🔴": 3, "🟡": 2, "🟢": 1, "⚪": 0}
    present = [c for c in cs if c != "⚪"]
    return max(present, key=lambda c: rank[c]) if present else "⚪"


def _pct(v, m):
    return None if (m is None or v is None or m == 0) else (v / m - 1) * 100


def _norm_row(sku_code, fish_type):
    with _db().cursor() as cur:
        cur.execute("""select n_cost, med_cost_per_kg, n_yield, med_yield_pct
                       from production.processing_stats
                       where out_sku_code=%s and fish_type=%s""", (sku_code, fish_type))
        return cur.fetchone()


def render(snap: dict) -> str:
    norm = _norm_row(snap["out_sku_code"], snap["fish_type"])
    cost = snap["cost_per_kg"]
    yld = snap["yield_pct"]
    cost_broken = cost is not None and cost < COST_FLOOR
    yield_broken = yld is not None and not (YIELD_MIN <= yld <= YIELD_MAX)
    if cost_broken:
        cost = None
    if yield_broken:
        yld = None

    med_cost = float(norm["med_cost_per_kg"]) if norm and norm["med_cost_per_kg"] is not None else None
    med_yld = float(norm["med_yield_pct"]) if norm and norm["med_yield_pct"] is not None else None
    n_cost = norm["n_cost"] if norm else 0
    n_yld = norm["n_yield"] if norm else 0

    c_cost = _color_cost(cost, med_cost)
    c_yld = _color_yield(yld, med_yld)
    sku_name = (snap["out_sku_name"] or "").split(",")[0]
    moment = datetime.strptime(snap["moment"], "%Y-%m-%d %H:%M:%S.%f").strftime("%d.%m")

    lines = [f"{_overall(c_cost, c_yld)} Техоперация №{snap['name']} · {moment}",
             f"{sku_name} ({snap['out_sku_code']}) · сырьё {snap['fish_type']}", ""]

    if cost is None:
        lines.append("Себестоимость: н/д (битая себест. выбытия — проверь)"
                     if cost_broken else "Себестоимость: н/д")
    elif med_cost is None:
        lines.append(f"Себестоимость: {cost:.0f} ₽/кг  ⚪ нет нормы")
    else:
        thin = " · мало данных" if n_cost and n_cost <= 2 else ""
        lines.append(f"Себестоимость: {cost:.0f} ₽/кг  {c_cost} "
                     f"{_pct(cost, med_cost):+.1f}% к норме {med_cost:.0f} (n={n_cost}){thin}")

    if yld is None:
        lines.append(f"Выход: {snap['yield_pct']:.0f}% ⚪ вне диапазона — проверь состав"
                     if yield_broken else "Выход: н/д")
    elif med_yld is None:
        lines.append(f"Выход: {yld:.1f}%  ⚪ нет нормы")
    else:
        thin = " · мало данных" if n_yld and n_yld <= 2 else ""
        lines.append(f"Выход: {yld:.1f}%  {c_yld} "
                     f"{_pct(yld, med_yld):+.1f}% к норме {med_yld:.1f}% (n={n_yld}){thin}")

    if snap.get("check_status"):
        lines += ["", f"статус в МС: {snap['check_status']}"]
    return "\n".join(lines)


# ── детект + отправка ────────────────────────────────────────────────────────
def _recipients() -> list[int]:
    ids = []
    for env in ("OWNER_CHAT_ID", "PARTNER_CHAT_ID"):
        v = (os.getenv(env) or "").strip()
        if v.lstrip("-").isdigit():
            ids.append(int(v))
    return ids


def _log_get_all() -> dict[str, dict]:
    with _db().cursor() as cur:
        cur.execute("select processing_id, analiz_sent_at from production.processing_svetofor_log")
        return {str(r["processing_id"]): r for r in cur.fetchall()}


def _log_upsert(pid, name, state, analiz: bool):
    with _db().cursor() as cur:
        cur.execute("""
            insert into production.processing_svetofor_log
              (processing_id,name,last_state,first_sent_at,analiz_sent_at,updated_at)
            values (%s,%s,%s,now(),case when %s then now() else null end,now())
            on conflict (processing_id) do update set
              last_state=excluded.last_state,
              analiz_sent_at=coalesce(production.processing_svetofor_log.analiz_sent_at,
                             case when %s then now() else null end),
              updated_at=now()
        """, (pid, name, state, analiz, analiz))


async def _fetch_recent() -> list[dict]:
    since = (datetime.now() - timedelta(days=POLL_DAYS)).strftime("%Y-%m-%d 00:00:00.000")
    rows, offset = [], 0
    while True:
        d = await _ms_get("/entity/processing", {
            "limit": 100, "offset": offset, "order": "moment,desc",
            "expand": "state", "filter": f"moment>={since}",
        })
        chunk = d.get("rows") or []
        rows.extend(chunk)
        if len(chunk) < 100:
            break
        offset += 100
    return rows


async def poll_job(app, db=None):
    """APScheduler-джоба: детект новых/«Анализ сделан» → светофор Виктору и Маланчуку.

    Защита от «потопа»: на самом первом поллинге (лог пуст) операции за окно только
    помечаются как отправленные, без рассылки — светофоры идут лишь по новым с этого момента.
    """
    try:
        rows = await _fetch_recent()
    except Exception as e:  # noqa: BLE001
        logger.error(f"svetofor poll: ошибка МС: {e}")
        return
    log = _log_get_all()

    if not log and rows:
        for r in rows:
            state = (r.get("state") or {}).get("name")
            _log_upsert(r["id"], r.get("name"), state, analiz=(state == ANALIZ_STATE))
        logger.info(f"svetofor: первичный сид лога — {len(rows)} операций, рассылки нет")
        return

    recipients = _recipients()
    sent = 0
    for r in rows:
        pid = r["id"]
        state = (r.get("state") or {}).get("name")
        prev = log.get(pid)
        if prev is None:
            reason = "новая"
        elif state == ANALIZ_STATE and prev["analiz_sent_at"] is None:
            reason = "анализ сделан"
        else:
            continue
        try:
            snap = await compute(pid, r.get("name"), r["moment"], state)
            if snap is None:
                _log_upsert(pid, r.get("name"), state, analiz=(state == ANALIZ_STATE))
                continue
            text = render(snap)
            for chat_id in recipients:
                await app.bot.send_message(chat_id=chat_id, text=text)
            _log_upsert(pid, r.get("name"), state, analiz=(state == ANALIZ_STATE))
            sent += 1
            logger.info(f"svetofor: №{r.get('name')} [{reason}] отправлен ({len(recipients)} получат.)")
        except Exception as e:  # noqa: BLE001
            logger.error(f"svetofor: №{r.get('name')} ошибка: {e}")
    if sent:
        logger.info(f"svetofor poll: отправлено {sent}")
