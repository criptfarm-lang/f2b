"""Алерт складу: приход по заказу поставщику не заведён на остатки.

План (второй мозг): plans/2026-09-07-алерт-склад-приход-не-заведён-на-остатки.md

В заказе поставщику закупщик ставит плановую дату прихода (deliveryPlannedMoment).
Если к 20:00 МСК этой даты товар не оприходован (нет проведённой приёмки), заказ
молча зависает: закупщик считает поставку состоявшейся, склад её не видит, остатки
не сходятся. Джоба раз в день в 20:00 МСК шлёт в группу «F2B СКЛАД» список таких
заказов — чтобы закупщик проверил заведение товара на остатки.

Только чтение МС: ничего не создаёт и не правит (правило «no writes to MoySklad» —
приёмку заводит человек).

Решения собственника 07.09.2026:
  • частичный приход алертим, только если принято меньше 50% суммы заказа;
  • просроченные заказы напоминаем каждый день, пока не закрыты;
  • товарные заказы без плановой даты — отдельным блоком «дата не проставлена».
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, datetime, timedelta, timezone
from html import escape

import aiohttp
import psycopg2
import psycopg2.extras

from database import CONNECT_TIMEOUT_SEC, STATEMENT_TIMEOUT_MS
from moysklad import MS_BASE, get_headers

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))

# Группа «F2B СКЛАД» (getChat 07.09.2026). Переопределяется env.
DEFAULT_CHAT_ID = -4750423130

# Насколько глубоко в прошлое смотрим просроченные заказы.
LOOKBACK_DAYS = int(os.getenv("ARRIVAL_LOOKBACK_DAYS", "90"))
# Приход считается частичным и попадает в алерт, если принято меньше этой доли суммы.
PARTIAL_THRESHOLD = float(os.getenv("ARRIVAL_PARTIAL_THRESHOLD", "0.5"))

_GOODS_TYPES = ("product", "variant")

# Контролируем приход только по этим корневым группам товаров МС (решение
# собственника 07.09.2026): сырьё, пищевые добавки, упаковка, привлечённые товары.
# Всё остальное — «яПРОЧЕЕ» (топливо, перчатки, инвентарь), «ГОТОВАЯ ПРОДУКЦИЯ» —
# в алерт не идёт. Матчим по корню `assortment.pathName`; список переопределяется
# env ARRIVAL_PRODUCT_FOLDERS (через запятую).
DEFAULT_FOLDERS = "СЫРЬЕ,ПИЩЕВЫЕ ДОБАВКИ,УПАКОВКА,ПРИВЛЕЧЕННЫЕ ТОВАРЫ"

# Статусы purchaseorder, которые вне контроля прихода (metadata 07.09.2026):
#   «Отменен» — поставки не будет; «Возврат» — товар уехал обратно;
#   «Новый» — черновик, закупщик ещё пишет заказ.
STATE_CANCELLED = "9a57f52b-a5df-11f0-0a80-163f00106cef"
STATE_RETURN = "9a57f47a-a5df-11f0-0a80-163f00106cee"
STATE_DRAFT = "18018a74-7c2c-11f1-0a80-03bb0004e405"
SKIP_STATES = {STATE_CANCELLED, STATE_RETURN, STATE_DRAFT}

_PAGE = 100
_ID_CHUNK = 40              # сколько id заказов кладём в один OR-фильтр
_MAX_ROWS_PER_BLOCK = 15  # сверх этого — счётчик «и ещё N»

LOG_DDL = """
create table if not exists public.arrival_check_log (
    day         date primary key,
    fingerprint text,
    orders      int,
    sent_at     timestamptz default now()
);
"""

_conn = None


def _db():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(os.environ["DATABASE_URL"],
                                 cursor_factory=psycopg2.extras.RealDictCursor,
                                 connect_timeout=CONNECT_TIMEOUT_SEC,
                                 options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS}")
        _conn.autocommit = True
        with _conn.cursor() as cur:
            cur.execute(LOG_DDL)
    return _conn


def _already_sent(day: date, fingerprint: str) -> bool:
    """Тот же состав в тот же день уже уходил? Защита от рестарта бота в 20:00."""
    with _db().cursor() as cur:
        cur.execute("select fingerprint from public.arrival_check_log where day=%s", (day,))
        row = cur.fetchone()
        return bool(row and row.get("fingerprint") == fingerprint)


def _mark_sent(day: date, fingerprint: str, orders: int):
    with _db().cursor() as cur:
        cur.execute("""
            insert into public.arrival_check_log (day, fingerprint, orders, sent_at)
            values (%s, %s, %s, now())
            on conflict (day) do update set
              fingerprint = excluded.fingerprint,
              orders      = excluded.orders,
              sent_at     = now()
        """, (day, fingerprint, orders))


def chat_id() -> int:
    v = (os.getenv("ARRIVAL_ALERT_CHAT_ID") or "").strip()
    return int(v) if v.lstrip("-").isdigit() else DEFAULT_CHAT_ID


# ── МойСклад (чтение) ────────────────────────────────────────────────────────
async def _fetch_page(session: aiohttp.ClientSession, entity: str, params: dict) -> dict:
    """Одна страница с ретраями на 5xx: МС отдаёт 502 на тяжёлых expand-запросах."""
    last = None
    for attempt in range(3):
        async with session.get(
            f"{MS_BASE}/entity/{entity}",
            headers=get_headers(),
            params=params,
            timeout=aiohttp.ClientTimeout(total=90),
        ) as r:
            if r.status == 200:
                return await r.json()
            last = f"{r.status}: {(await r.text())[:200]}"
        if attempt < 2:
            await asyncio.sleep(2 * (attempt + 1))
    raise RuntimeError(f"МС {entity} вернул {last}")


async def _fetch_all(session: aiohttp.ClientSession, entity: str, params: dict,
                     page: int = _PAGE) -> list[dict]:
    """Постранично тянет сущность МС. Ошибку пробрасывает наверх — джоба молчит,
    а не шлёт складу неполный список как полный."""
    rows, offset = [], 0
    while True:
        data = await _fetch_page(session, entity,
                                 {**params, "limit": page, "offset": offset})
        chunk = data.get("rows", [])
        rows.extend(chunk)
        if len(chunk) < page:
            break
        offset += page
    return rows


async def fetch_orders_with_date(session, today: date) -> list[dict]:
    """Заказы поставщику с плановой датой приёмки от today−LOOKBACK по сегодня включительно."""
    lo = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d 00:00:00")
    hi = (today + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    return await _fetch_all(session, "purchaseorder", {
        "filter": f"deliveryPlannedMoment>={lo};deliveryPlannedMoment<{hi}",
        "order": "deliveryPlannedMoment,asc",
        "expand": "agent,state,owner",
    })


async def fetch_orders_without_date(session, today: date) -> list[dict]:
    """Заказы, созданные за окно, — из них берём те, где плановая дата не проставлена."""
    lo = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d 00:00:00")
    rows = await _fetch_all(session, "purchaseorder", {
        "filter": f"moment>={lo}",
        "order": "moment,desc",
        "expand": "agent,state,owner",
    })
    return [r for r in rows if not r.get("deliveryPlannedMoment")]


async def fetch_received_sums(session, today: date) -> dict[str, float]:
    """{id заказа поставщику: сумма проведённых приёмок, руб}.

    Считаем только applicable=true: непроведённая приёмка на остатки товар не ставит.
    Окно приёмок берём с запасом в 30 дней вглубь относительно окна заказов — приёмка
    не может быть раньше своего заказа, запас на документы задним числом.
    """
    lo = (today - timedelta(days=LOOKBACK_DAYS + 30)).strftime("%Y-%m-%d 00:00:00")
    rows = await _fetch_all(session, "supply", {
        "filter": f"moment>={lo}",
        "order": "moment,asc",
    })
    sums: dict[str, float] = {}
    for s in rows:
        if not s.get("applicable"):
            continue
        po = s.get("purchaseOrder")
        if not po:
            continue
        po_id = (po.get("meta") or {}).get("href", "").rsplit("/", 1)[-1]
        if not po_id:
            continue
        sums[po_id] = sums.get(po_id, 0.0) + (s.get("sum") or 0) / 100
    return sums


async def fetch_goods_ids(session, order_ids: list[str]) -> set[str]:
    """Из переданных заказов оставляет те, где есть позиция из контролируемых групп.

    Позиции раскрываем только для кандидатов на алерт (их единицы), а не для всей
    выборки: expand positions.assortment на сотне документов МС отдаёт 502 и съедает
    лимит запросов. Фильтр вида `id=A;id=B` по одному полю МС трактует как OR
    (проверено 07.09.2026 на трёх заказах).
    """
    goods: set[str] = set()
    for i in range(0, len(order_ids), _ID_CHUNK):
        chunk = order_ids[i:i + _ID_CHUNK]
        data = await _fetch_page(session, "purchaseorder", {
            "filter": ";".join(f"id={oid}" for oid in chunk),
            "limit": _PAGE,
            "expand": "positions.assortment",
        })
        for row in data.get("rows", []):
            if _has_goods(row):
                goods.add(row.get("id"))
    return goods


# ── классификация ────────────────────────────────────────────────────────────
def tracked_folders() -> set[str]:
    raw = os.getenv("ARRIVAL_PRODUCT_FOLDERS") or DEFAULT_FOLDERS
    return {name.strip().upper() for name in raw.split(",") if name.strip()}


def _has_goods(order: dict) -> bool:
    """Есть ли в заказе позиция из контролируемых групп товаров.

    Услуги перевозчиков, топливо, инвентарь и прочее из «яПРОЧЕЕ» приходом на склад
    не считаем — контроль только по сырью, добавкам, упаковке и привлечённым товарам.
    """
    folders = tracked_folders()
    for p in (order.get("positions") or {}).get("rows", []) or []:
        a = p.get("assortment") or {}
        if ((a.get("meta") or {}).get("type")) not in _GOODS_TYPES:
            continue
        root = (a.get("pathName") or "").split("/", 1)[0].strip().upper()
        if root in folders:
            return True
    return False


def _skip_by_state(order: dict) -> bool:
    return ((order.get("state") or {}).get("id")) in SKIP_STATES


def _card(order: dict, received: float) -> dict:
    return {
        "id": order.get("id"),
        "name": order.get("name") or "?",
        "supplier": (order.get("agent") or {}).get("name") or "—",
        "buyer": (order.get("owner") or {}).get("name") or "—",
        "state": (order.get("state") or {}).get("name") or "—",
        "sum": (order.get("sum") or 0) / 100,
        "received": received,
        "planned": (order.get("deliveryPlannedMoment") or "")[:10] or None,
        "created": (order.get("moment") or "")[:10],
        "url": ((order.get("meta") or {}).get("uuidHref")) or "",
    }


def classify(orders_dated: list[dict], orders_undated: list[dict],
             received: dict[str, float], today: date) -> dict:
    """Раскладывает заказы по блокам сообщения.

    Возвращает {"today": [...], "overdue": [...], "partial": [...], "nodate": [...]}.
    """
    result = {"today": [], "overdue": [], "partial": [], "nodate": []}

    for o in orders_dated:
        if _skip_by_state(o):
            continue
        planned_raw = o.get("deliveryPlannedMoment")
        if not planned_raw:
            continue
        planned = date.fromisoformat(planned_raw[:10])
        got = received.get(o.get("id"), 0.0)
        total = (o.get("sum") or 0) / 100
        card = _card(o, got)
        if got <= 0:
            card["overdue_days"] = (today - planned).days
            result["today" if planned == today else "overdue"].append(card)
        elif total > 0 and got < total * PARTIAL_THRESHOLD:
            card["overdue_days"] = (today - planned).days
            card["share"] = got / total
            result["partial"].append(card)

    for o in orders_undated:
        if _skip_by_state(o):
            continue
        if received.get(o.get("id"), 0.0) > 0:
            continue
        result["nodate"].append(_card(o, 0.0))

    result["overdue"].sort(key=lambda c: c["overdue_days"], reverse=True)
    result["partial"].sort(key=lambda c: c["overdue_days"], reverse=True)
    result["nodate"].sort(key=lambda c: c["created"])
    return result


# ── рендер ───────────────────────────────────────────────────────────────────
def _money(amount: float) -> str:
    return f"{amount:,.0f}".replace(",", " ") + " ₽"


def _plural_days(n: int) -> str:
    if 11 <= n % 100 <= 14:
        return "дней"
    return {1: "день", 2: "дня", 3: "дня", 4: "дня"}.get(n % 10, "дней")


def _link(card: dict) -> str:
    label = f"{card['name']} {card['supplier']}"
    if card["url"]:
        return f'<a href="{card["url"]}">{escape(label)}</a>'
    return escape(label)


def _block(title: str, cards: list[dict], line_fn) -> list[str]:
    if not cards:
        return []
    lines = [f"\n{title} — {len(cards)}"]
    for c in cards[:_MAX_ROWS_PER_BLOCK]:
        lines.append(line_fn(c))
    hidden = len(cards) - _MAX_ROWS_PER_BLOCK
    if hidden > 0:
        lines.append(f"… и ещё {hidden}")
    return lines


def render(buckets: dict, today: date) -> str:
    parts = [f"📦 <b>Приход не заведён на остатки</b> — проверка на 20:00, "
             f"{today.strftime('%d.%m')}",
             "<i>сырьё, пищевые добавки, упаковка, привлечённые товары</i>"]

    parts += _block(
        "🔴 Плановый приход сегодня, приёмки нет",
        buckets["today"],
        lambda c: f"• {_link(c)}\n  {_money(c['sum'])} · {c['state']} · закупка: {escape(c['buyer'])}",
    )
    parts += _block(
        "🟠 Висят с прошлых дней",
        buckets["overdue"],
        lambda c: (f"• {_link(c)}\n  план {date.fromisoformat(c['planned']).strftime('%d.%m')}, "
                   f"просрочка {c['overdue_days']} {_plural_days(c['overdue_days'])} · "
                   f"{_money(c['sum'])} · закупка: {escape(c['buyer'])}"),
    )
    parts += _block(
        "🟡 Принято меньше половины заказа",
        buckets["partial"],
        lambda c: (f"• {_link(c)}\n  принято {_money(c['received'])} из {_money(c['sum'])} "
                   f"({c['share'] * 100:.0f}%) · план "
                   f"{date.fromisoformat(c['planned']).strftime('%d.%m')}"),
    )
    parts += _block(
        "⚪ Плановая дата приёмки не проставлена",
        buckets["nodate"],
        lambda c: (f"• {_link(c)}\n  создан "
                   f"{date.fromisoformat(c['created']).strftime('%d.%m')} · "
                   f"{_money(c['sum'])} · {c['state']} · закупка: {escape(c['buyer'])}"),
    )

    parts.append("\nЗаведите приёмку в МойСклад или поправьте плановую дату в заказе — "
                 "тогда напоминание уйдёт.")
    return "\n".join(parts)


def fingerprint(buckets: dict) -> str:
    """Отпечаток состава — чтобы не отправить тот же список дважды за день."""
    keys = []
    for bucket in ("today", "overdue", "partial", "nodate"):
        keys += [f"{bucket}:{c['name']}" for c in buckets[bucket]]
    return "|".join(sorted(keys))


# ── прогон ───────────────────────────────────────────────────────────────────
async def collect(today: date | None = None) -> dict:
    """Собирает блоки сообщения из МойСклад. Исключения пробрасывает наверх."""
    today = today or datetime.now(MSK).date()
    async with aiohttp.ClientSession() as session:
        dated = await fetch_orders_with_date(session, today)
        undated = await fetch_orders_without_date(session, today)
        received = await fetch_received_sums(session, today)
        buckets = classify(dated, undated, received, today)

        candidate_ids = [c["id"] for cards in buckets.values() for c in cards if c.get("id")]
        goods = await fetch_goods_ids(session, candidate_ids)
    return {k: [c for c in v if c.get("id") in goods] for k, v in buckets.items()}


async def run(app, dry_run: bool = False) -> dict:
    """Один прогон: собрать, отрендерить, отправить в группу склада.

    Возвращает сводку по блокам. При dry_run сообщение только логируется.
    """
    today = datetime.now(MSK).date()
    buckets = await collect(today)
    stats = {k: len(v) for k, v in buckets.items()}
    total = sum(stats.values())
    if not total:
        logger.info("arrival_check: нечего слать — все приходы заведены")
        return {**stats, "sent": False}

    text = render(buckets, today)
    fp = fingerprint(buckets)

    if dry_run:
        logger.info("arrival_check DRY-RUN → chat %s:\n%s", chat_id(), text)
        return {**stats, "sent": False, "dry_run": True}

    if _already_sent(today, fp):
        logger.info("arrival_check: тот же состав уже отправлен сегодня, пропуск")
        return {**stats, "sent": False, "duplicate": True}

    await app.bot.send_message(
        chat_id=chat_id(), text=text, parse_mode="HTML",
        disable_web_page_preview=True,
    )
    _mark_sent(today, fp, total)
    logger.info("arrival_check: отправлено, %s заказов", total)
    return {**stats, "sent": True}


async def poll_job(app) -> dict:
    """Обёртка для JobQueue: ошибки логируем, джобу не роняем."""
    try:
        return await run(app)
    except Exception as e:
        logger.error("arrival_check poll_job: %s", e, exc_info=True)
        return {}
