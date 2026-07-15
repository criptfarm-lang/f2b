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

import html
import logging
import os
import re
from datetime import datetime, timedelta

import httpx
import psycopg2
import psycopg2.extras
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

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
    """Тип сырья по наименованию — только для метки нормы (не для поиска рыбы).
    ПБГ/ПСГ — разделка филе; Н/Р — целая рыба (копчение). None — не распознали."""
    up = (name or "").upper()
    if "ПБГ" in up:
        return "ПБГ"
    if "ПСГ" in up:
        return "ПСГ"
    if "Н/Р" in up:
        return "Н/Р"
    return None


SYRYE_ROOT = "СЫРЬЕ"  # корневая папка сырья в МС (выход считаем от позиций из неё)


def _folder(assortment: dict) -> str:
    """Папка позиции (pathName). Для модификации (variant) — с родит. product."""
    p = assortment.get("pathName")
    if p is None:
        p = (assortment.get("product") or {}).get("pathName")
    return p or ""


def _in_syrye(assortment: dict) -> bool:
    """Позиция относится к папке СЫРЬЕ (или её подгруппе). pathName берём с товара,
    а для модификации (variant) — с родительского product (expand=assortment.product)."""
    p = _folder(assortment)
    return p == SYRYE_ROOT or p.startswith(SYRYE_ROOT + "/")


# Вес в наименовании: "500 гр.", "250 г", "0,5 кг", "1 кг" → кг
_WEIGHT_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(кг|гр|г)\b", re.IGNORECASE)


def _is_piece(assortment: dict) -> bool:
    """Штучная позиция (uom = шт), а не весовая (кг)."""
    nm = ((assortment.get("uom") or {}).get("name") or "").lower()
    return "шт" in nm


def _unit_kg(assortment: dict) -> float | None:
    """Вес одной штуки в кг: сперва из наименования, иначе из поля weight МС."""
    m = _WEIGHT_RE.search(assortment.get("name") or "")
    if m:
        val = float(m.group(1).replace(",", "."))
        return val if m.group(2).lower() == "кг" else val / 1000.0
    w = assortment.get("weight")
    return float(w) if w and float(w) > 0 else None


def _out_kg(assortment: dict, qty: float) -> float | None:
    """Объём выпуска позиции в кг. Весовая — как есть; штучная — qty × вес штуки."""
    if not _is_piece(assortment):
        return qty
    uk = _unit_kg(assortment)
    return qty * uk if uk else None


async def _positions(pid: str, kind: str, expand: str = "assortment") -> list[dict]:
    d = await _ms_get(f"/entity/processing/{pid}/{kind}", {"expand": expand, "limit": 1000})
    return d.get("rows") or []


# Рентабельность считаем от ЖИВОЙ карточки ГП (тип «Цена опт» → рент. ОПТ, «Цена продажи»
# → рент. ХОР), а не от снимка цены внутри техоперации: светофор приходит на «Анализ
# сделан» и должен отражать текущую цену продажи, а не ту, что была при выпуске.
PRICE_OPT = "Цена опт"
PRICE_HOR = "Цена продажи"


async def _card_prices(sku_code: str | None) -> dict[str, float]:
    """{имя_типа_цены: ₽} по актуальной карточке ГП. Пусто, если код не найден."""
    if not sku_code:
        return {}
    d = await _ms_get("/entity/product", {"filter": f"code={sku_code}", "limit": 1})
    rows = d.get("rows") or []
    if not rows:
        return {}
    out: dict[str, float] = {}
    for sp in rows[0].get("salePrices") or []:
        nm = (sp.get("priceType") or {}).get("name")
        if nm:
            out[nm] = (sp.get("value") or 0) / 100.0
    return out


async def compute(pid: str, name: str, moment: str, state: str | None) -> dict | None:
    prods = await _positions(pid, "products", expand="assortment.uom")
    if not prods:
        return None
    mats = await _positions(pid, "materials", expand="assortment.product")

    # (код, наименование, qty в базовой ед., объём в кг, папка). Для штучных qty×вес; None — не перевести.
    out_pos = []
    for p in prods:
        a = p.get("assortment") or {}
        qty = float(p.get("quantity") or 0)
        out_pos.append((a.get("code"), a.get("name"), qty, _out_kg(a, qty), _folder(a)))
    out_multi = len(out_pos) > 1
    main = max(out_pos, key=lambda t: (t[3] if t[3] is not None else t[2]))
    out_sku_code, out_sku_name, main_folder = main[0], main[1], main[4]
    # Метрики (выход, ₽/кг) считаем на СО-ПРОДУКТЫ основного выпуска — все продукты из
    # ТОЙ ЖЕ папки, что и основной (кусок+ломтики одной филе → 'ГОТОВАЯ ПРОДУКЦИЯ/КУСОК,
    # ЛОМТИК, СТЕЙК'). Побочный выпуск (суп.набор/фарш → '…/ПРОЧЕЕ ПРОИЗВОДСТВО') лежит
    # в ДРУГОЙ папке и в числитель не идёт — иначе он раздувал бы выход и занижал ₽/кг.
    # out_qty (сумма всех продуктов) остаётся только для снимка.
    coprod = [t for t in out_pos if t[4] == main_folder]
    # если хоть один со-продукт не перевели в кг (штучное без веса) — не врём: main_kg=None
    main_kg = None if any(t[3] is None for t in coprod) else sum(t[3] for t in coprod)
    out_qty = None if any(t[3] is None for t in out_pos) else sum(t[3] for t in out_pos)

    # Знаменатель выхода — позиции из папки СЫРЬЕ (решение собственника): любой вид
    # рыбы-сырья (ПБГ/ПСГ филейное, н/р для копчения) лежит в этой папке. Так надёжнее
    # токенов в наименовании — копчёные операции из целой рыбы «н/р» теперь считаются.
    mat_rows, syrye_rows, syrye_outside = [], [], []
    for m in mats:
        a = m.get("assortment") or {}
        href = (a.get("meta") or {}).get("href")
        qty = float(m.get("quantity") or 0)
        if href:
            mat_rows.append((href, qty))
        if _in_syrye(a):
            syrye_rows.append((a.get("name"), qty))
        elif _classify_fish(a.get("name")) and not _is_piece(a):
            # Рыба-сырьё ВНЕ папки СЫРЬЕ: продаваемый SKU (напр. 71011 ПСГ 5-6 кг живёт
            # в ГОТОВОЙ ПРОДУКЦИИ), но в разделке это сырьё. Засчитываем в знаменатель
            # выхода по токену ПБГ/ПСГ/Н/Р (весовая) и помечаем в карточке.
            syrye_outside.append((a.get("name"), qty))

    # Знаменатель выхода = папка СЫРЬЕ ∪ рыба-по-токену вне неё (продаваемые SKU типа
    # 71011, идущие в разделку). Сырьё часто разбито на строки-размерки — СУММИРУЕМ все.
    all_raw = syrye_rows + syrye_outside
    if not all_raw:
        fish_name, fish_qty, fish_type, yield_pct = None, None, "NONE", None
    else:
        fish_qty = sum(q for _, q in all_raw)
        fish_name = max(all_raw, key=lambda t: t[1])[0]  # тип нормы — по доминирующей строке
        fish_type = _classify_fish(fish_name) or "СЫРЬЁ"
        yield_pct = round(main_kg / fish_qty * 100, 2) if (fish_qty and main_kg) else None

    perunit = await _day_turnover(moment[:10])
    cost_total, missing = 0.0, 0
    for h, q in mat_rows:
        pu = perunit.get(_norm(h))
        if pu is None:
            missing += 1
        else:
            cost_total += pu * q
    cost_per_kg = round(cost_total / main_kg, 2) if (main_kg and missing == 0) else None

    return {
        "name": name, "moment": moment, "check_status": state,
        "out_sku_code": out_sku_code, "out_sku_name": out_sku_name,
        "out_qty": out_qty, "out_multi": out_multi,
        "fish_type": fish_type, "fish_qty": fish_qty,
        "cost_per_kg": cost_per_kg, "yield_pct": yield_pct,
        "syrye_outside": syrye_outside,
        "card_prices": await _card_prices(out_sku_code),
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


def render(snap: dict) -> tuple[str, str | None]:
    norm = _norm_row(snap["out_sku_code"], snap["fish_type"])
    cost = snap["cost_per_kg"]
    yld = snap["yield_pct"]
    # «Битая себест. выбытия» (near-zero списание партии) — реальный риск ТОЛЬКО
    # для филе ПБГ/ПСГ (норма 1200–1600 ₽/кг). Дешёвые продукты (суповой набор,
    # фарш из обрези ~27 ₽/кг) законно ниже COST_FLOOR — не флагаем, показываем как есть.
    cost_broken = (cost is not None and cost < COST_FLOOR
                   and snap["fish_type"] in ("ПБГ", "ПСГ"))
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
    sku_name = snap["out_sku_name"] or ""   # полное имя готовой продукции, без обрезки по запятой
    moment = datetime.strptime(snap["moment"], "%Y-%m-%d %H:%M:%S.%f").strftime("%d.%m")

    if cost is None:
        cost_line = ("Себестоимость: н/д (битая себест. выбытия — проверь)"
                     if cost_broken else "Себестоимость: н/д")
    elif med_cost is None:
        cost_line = f"Себестоимость: {cost:.0f} ₽/кг  ⚪ нет нормы"
    else:
        thin = " · мало данных" if n_cost and n_cost <= 2 else ""
        cost_line = (f"Себестоимость: {cost:.0f} ₽/кг  {c_cost} "
                     f"{_pct(cost, med_cost):+.1f}% к норме {med_cost:.0f} (n={n_cost}){thin}")

    if yld is None:
        yld_line = (f"Выход: {snap['yield_pct']:.0f}% ⚪ вне диапазона — проверь состав"
                    if yield_broken else "Выход: н/д")
    elif med_yld is None:
        yld_line = f"Выход: {yld:.1f}%  ⚪ нет нормы"
    else:
        thin = " · мало данных" if n_yld and n_yld <= 2 else ""
        yld_line = (f"Выход: {yld:.1f}%  {c_yld} "
                    f"{_pct(yld, med_yld):+.1f}% к норме {med_yld:.1f}% (n={n_yld}){thin}")

    lines = [f"{_overall(c_cost, c_yld)} Техоперация №{snap['name']} · {moment}",
             f"{sku_name} ({snap['out_sku_code']})",
             yld_line]
    # Рыба-сырьё вне папки СЫРЬЕ: засчитана в выход по токену — помечаем, чтобы было
    # видно, что зачёт по fallback, а не по папке.
    outside = snap.get("syrye_outside") or []
    if outside:
        names = ", ".join(f"{n} ({f'{q:g}'.replace('.', ',')} кг)" for n, q in outside)
        lines.append(f"ℹ учтено в выходе по токену (вне папки СЫРЬЕ): {names}")
    lines += ["", cost_line]

    # Рентабельность к цене продажи (как в отчёте МС «Прибыльность»):
    # (цена − себест) / цена. Только при достоверной себестоимости.
    card_prices = snap.get("card_prices") or {}
    for label, ptype in (("ОПТ", PRICE_OPT), ("ХОР", PRICE_HOR)):
        price = card_prices.get(ptype)
        if price and cost:
            r = (price - cost) / price * 100
            lines.append(f"Рентабельность {label}: {r:.1f}% (цена {price:.0f} / себест {cost:.0f})")

    if snap.get("check_status"):
        lines += ["", "", f"статус в МС: {snap['check_status']}"]

    body = "\n".join(lines)
    # Комментарий техоперации (поле description МС) — под чертой курсивом.
    # Курсив требует HTML parse_mode → экранируем весь текст.
    comment = (snap.get("comment") or "").strip()
    if not comment:
        return body, None
    return (f"{html.escape(body, quote=False)}\n"
            f"──────────\n"
            f"<i>{html.escape(comment, quote=False)}</i>"), "HTML"


# ── Фаза 3: кнопки (только Виктору) ──────────────────────────────────────────
ACTION_STATE = {"ok": "Проверено", "vy": "Проверить выход", "rz": "Разобраться"}
_states_cache: dict[str, str] = {}


async def _states_meta() -> dict[str, str]:
    """{name: href state} техопераций (кеш метаданных)."""
    if not _states_cache:
        d = await _ms_get("/entity/processing/metadata")
        for s in d.get("states") or []:
            href = (s.get("meta") or {}).get("href")
            if s.get("name") and href:
                _states_cache[s["name"]] = href
    return _states_cache


async def _patch_state(pid: str, state_name: str):
    href = (await _states_meta()).get(state_name)
    if not href:
        raise RuntimeError(f"нет state '{state_name}' в метаданных processing")
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.put(
            f"{MS_BASE}/entity/processing/{pid}", headers=get_headers(),
            json={"state": {"meta": {"href": href, "type": "state",
                                     "mediaType": "application/json"}}})
        r.raise_for_status()


def keyboard(pid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Проверено", callback_data=f"svf:ok:{pid}"),
        InlineKeyboardButton("Проверить выход", callback_data=f"svf:vy:{pid}"),
        InlineKeyboardButton("Разобраться", callback_data=f"svf:rz:{pid}"),
    ]])


def _dec(x):
    from decimal import Decimal
    return None if x is None else Decimal(str(x))


async def _upsert_snapshot(pid, name, moment, state):
    """При «Проверено» записываем снимок в норму (view processing_stats)."""
    snap = await compute(pid, name, moment, state)
    if snap is None:
        return
    with _db().cursor() as cur:
        cur.execute("""
            insert into production.processing_snapshots
              (processing_id,name,moment,out_sku_code,out_sku_name,out_qty,out_multi,
               fish_type,fish_qty,cost_per_kg,yield_pct,check_status,computed_at)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
            on conflict (processing_id) do update set
               check_status=excluded.check_status, cost_per_kg=excluded.cost_per_kg,
               yield_pct=excluded.yield_pct, out_qty=excluded.out_qty,
               out_multi=excluded.out_multi, fish_type=excluded.fish_type,
               fish_qty=excluded.fish_qty, computed_at=now()
        """, (pid, name, datetime.strptime(moment, "%Y-%m-%d %H:%M:%S.%f"),
              snap["out_sku_code"], snap["out_sku_name"], _dec(snap["out_qty"]), snap["out_multi"],
              snap["fish_type"], _dec(snap["fish_qty"]), _dec(snap["cost_per_kg"]),
              _dec(snap["yield_pct"]), state))


async def handle_svetofor_callback(update, context):
    """Callback кнопок светофора (pattern ^svf:). Только Виктор."""
    q = update.callback_query
    try:
        _, action, pid = (q.data or "").split(":", 2)
    except ValueError:
        await q.answer()
        return
    owner = (os.getenv("OWNER_CHAT_ID") or "").strip()
    if owner.isdigit() and update.effective_user and update.effective_user.id != int(owner):
        await q.answer("Нет прав")
        return
    state_name = ACTION_STATE.get(action)
    if not state_name:
        await q.answer()
        return
    # Ack СРАЗУ: PUT статуса + пересчёт снимка («Проверено» тянет оборот дня при
    # холодном кеше) могут занять >15с, иначе Telegram показывает «Timed out».
    # Результат отдаём правкой сообщения, а не toast'ом.
    await q.answer("Отмечаю…")
    try:
        await _patch_state(pid, state_name)
        if action == "ok":
            doc = await _ms_get(f"/entity/processing/{pid}", {"expand": "state"})
            await _upsert_snapshot(pid, doc.get("name"), doc["moment"], "Проверено")
        _log_upsert(pid, None, state_name, analiz=False)
        base = q.message.text or ""
        await q.edit_message_text(f"{base}\n\n→ отмечено: {state_name}", reply_markup=None)
    except Exception as e:  # noqa: BLE001
        logger.error(f"svetofor callback {action} {pid}: {e}")
        # callback уже отвечен — кнопки оставляем для повтора, ошибку отдельным сообщением
        try:
            await context.bot.send_message(
                chat_id=q.message.chat_id,
                text=f"⚠️ Светофор: не удалось отметить «{state_name}» — {e}. "
                     f"Кнопки на месте, попробуй ещё раз.")
        except Exception:  # noqa: BLE001
            pass


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
              name=coalesce(excluded.name, production.processing_svetofor_log.name),
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
    owner = (os.getenv("OWNER_CHAT_ID") or "").strip()
    owner_id = int(owner) if owner.isdigit() else None
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
            snap["comment"] = r.get("description")  # комментарий техоперации → под чертой курсивом
            text, parse_mode = render(snap)
            for chat_id in recipients:
                kb = keyboard(pid) if chat_id == owner_id else None  # кнопки только Виктору
                await app.bot.send_message(chat_id=chat_id, text=text,
                                           reply_markup=kb, parse_mode=parse_mode)
            _log_upsert(pid, r.get("name"), state, analiz=(state == ANALIZ_STATE))
            sent += 1
            logger.info(f"svetofor: №{r.get('name')} [{reason}] отправлен ({len(recipients)} получат.)")
        except Exception as e:  # noqa: BLE001
            logger.error(f"svetofor: №{r.get('name')} ошибка: {e}")
    if sent:
        logger.info(f"svetofor poll: отправлено {sent}")
