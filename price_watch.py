"""Сторож: заказ ниже прайса ушёл в работу без согласования собственника.

План: «F2B второй мозг»/plans/2026-09-22-сторож-заказ-ниже-прайса-без-согласования.md.

Статус «На согласовании» ставит менеджер, а отдельный алерт «заказ ниже прайса» выключен
в мае (план 2026-05-21, Фаза 5). За 08–22.09 из 213 заказов ниже прайса 4 ушли без
светофора. Сторож ловит такие заказы, когда они уже в работе (Согласован / Собирается /
Собран / Документы готовы / Отгружен), и пишет собственнику одно сообщение на заказ.

Не обход:
  • заказ согласован кнопкой светофора (`pending_approval_alerts.owner_approved_at` или
    запись закрыта без комментария);
  • позиция покрыта ценой из «ЗАПРОС ЦЕНЫ» в дашборде (price_requests).
Цены прайса – из развёрнутого товара заказа (expand), без запроса на каждую позицию.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import aiohttp
import psycopg2
import psycopg2.extras

from database import CONNECT_TIMEOUT_SEC, STATEMENT_TIMEOUT_MS
from moysklad import MS_BASE, get_headers, is_attracted_path

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))
# Заказ уже в работе – согласование должно было случиться раньше
WORK_STATES = {
    "005f3651-9a9a-11f0-0a80-03a900027474",  # Согласован
    "267fdfbc-a2a7-11f0-0a80-0f640047fcaa",  # Собирается
    "70999fb0-a2b6-11f0-0a80-1c830049f367",  # Собран без охл
    "005f376a-9a9a-11f0-0a80-03a900027475",  # Собран
    "ee088f23-df45-11f0-0a80-1670003a954a",  # ИЗМЕНЕН
    "6edbfa00-dfdb-11f0-0a80-104e0008a4d4",  # Документы готовы
    "005f383a-9a9a-11f0-0a80-03a900027476",  # Отгружен
}
SWEEP_HOURS = 26
STATE_AGREED = "Согласован"
# Кто согласует: собственник в МС (admin@vicpure) и бот «Эф» по кнопке светофора (f@vicpure).
# Статус «Согласован», поставленный кем-то ещё, согласованием не считается.
APPROVER_MS_UIDS = {u.strip().lower() for u in
                    os.getenv("PRICE_WATCH_APPROVER_UIDS", "admin@vicpure,f@vicpure").split(",") if u.strip()}
DDL = """
create table if not exists public.price_bypass_alerts (
    order_id   text primary key,
    order_name text,
    state      text,
    items      jsonb,
    silent     boolean not null default false,   -- размечен первым прогоном, не отправлялся
    sent_at    timestamptz not null default now()
);
create table if not exists public.price_bypass_state (
    id        int primary key default 1,
    seeded_at timestamptz
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
            cur.execute(DDL)
    return _conn


def _q(sql: str, params=(), fetch: str = "one"):
    for attempt in (1, 2):
        try:
            with _db().cursor() as cur:
                cur.execute(sql, params)
                if not cur.description:
                    return None
                return cur.fetchone() if fetch == "one" else cur.fetchall()
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            global _conn
            _conn = None
            if attempt == 2:
                raise


def _seeded() -> bool:
    r = _q("select seeded_at from public.price_bypass_state where id=1")
    return bool(r and r["seeded_at"])


def _owner_approved(order_id: str) -> bool:
    r = _q("""select 1 from pending_approval_alerts
              where order_id=%s and (owner_approved_at is not null or (closed_at is not null and comment is null))
              limit 1""", (order_id,))
    return bool(r)


def _fmt(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ")


def below_price_items(order: dict) -> list[dict]:
    """Позиции-товары ниже прайса по типу клиента (хорека – «Цена продажи», опт – «Цена опт»)."""
    tags = [t.lower() for t in ((order.get("agent") or {}).get("tags") or [])]
    if "хорека" in tags:
        price_type = "Цена продажи"
    elif "опт" in tags:
        price_type = "Цена опт"
    else:
        return []
    out = []
    for p in ((order.get("positions") or {}).get("rows") or []):
        a = p.get("assortment") or {}
        if (a.get("meta") or {}).get("type") != "product":
            continue
        list_price = next(((sp.get("value") or 0) / 100 for sp in a.get("salePrices") or []
                           if (sp.get("priceType") or {}).get("name") == price_type), 0)
        price = (p.get("price") or 0) / 100 * (1 - float(p.get("discount") or 0) / 100)
        if list_price and 0 < price < list_price - 0.5:
            out.append({"code": a.get("code"), "name": a.get("name") or "", "qty": float(p.get("quantity") or 0),
                        "order_price": price, "min_price": list_price,
                        "diff_pct": (list_price - price) / list_price * 100,
                        "attracted": is_attracted_path(a.get("pathName"))})
    return out


async def check_order(order: dict, bot, silent: bool = False) -> bool:
    """True – обход найден (и отправлен, если не silent)."""
    state = order.get("state") or {}
    if state.get("id") not in WORK_STATES:
        return False
    order_id, order_name = order.get("id"), order.get("name")
    if _q("select 1 from public.price_bypass_alerts where order_id=%s", (order_id,)):
        return False
    items = below_price_items(order)
    if not items or _owner_approved(order_id) or await _agreed_in_ms(order_id):
        return False
    try:
        from price_requests import apply_dashboard_approvals
        agent = order.get("agent") or {}
        items = apply_dashboard_approvals({"items": items, "attracted_items": [], "agent_id": agent.get("id"),
                                           "agent_inn": agent.get("inn")}, order_name)["items"]
    except Exception as e:
        logger.warning("price_watch %s: согласования из дашборда → %r", order_name, e)
    if not items:
        return False
    row = _q("""insert into public.price_bypass_alerts (order_id, order_name, state, items, silent)
                values (%s,%s,%s,%s::jsonb,%s) on conflict (order_id) do nothing returning order_id""",
             (order_id, order_name, state.get("name"), json.dumps(items, ensure_ascii=False), silent))
    if not row or silent:
        return bool(row)

    from notifier import _md
    agent = order.get("agent") or {}
    owner = (order.get("owner") or {}).get("name") or "—"
    lines = [
        "⚠️ *Заказ ниже прайса ушёл без согласования*",
        f"Заказ {_md(order_name or '')} · {_md(agent.get('name') or '')} · {_fmt((order.get('sum') or 0) / 100)} ₽",
        f"👔 {_md(owner)} · статус «{_md(state.get('name') or '')}»",
        "",
    ]
    for it in items[:8]:
        mark = " (привлечённые)" if it["attracted"] else ""
        pct = f"{it['diff_pct']:.1f}".replace(".", ",")
        lines.append(f"• {_md((it['code'] or '') + ' ' + it['name'][:46])}{mark}: {_fmt(it['order_price'])} ₽ "
                     f"при прайсе {_fmt(it['min_price'])} ₽ (−{pct} %)")
    if len(items) > 8:
        lines.append(f"• … и ещё {len(items) - 8}")
    url = (order.get("meta") or {}).get("uuidHref")
    if url:
        lines += ["", f"[Открыть заказ в МойСкладе]({url})"]
    owner_chat = int(os.getenv("OWNER_CHAT_ID", "0") or 0)
    try:
        await bot.send_message(chat_id=owner_chat, text="\n".join(lines), parse_mode="Markdown",
                               disable_web_page_preview=True)
        logger.info("price_watch: обход согласования %s отправлен", order_name)
        return True
    except Exception as e:
        # не доставлено – снимаем отметку, добор повторит
        _q("delete from public.price_bypass_alerts where order_id=%s", (order_id,))
        logger.warning("price_watch %s: не отправлено (%r)", order_name, e)
        return False


async def _agreed_in_ms(order_id: str) -> bool:
    """Статус «Согласован» ставил согласующий (собственник руками в МС или бот по кнопке).
    Кейс 21.09: заказы 04240, 04252, 04272 собственник согласовал в МС, минуя кнопку;
    04241 кладовщик перевёл из «На согласовании» сразу в «Отгружен» – это обход."""
    async with aiohttp.ClientSession() as session:
        d = await _fetch(session, f"{MS_BASE}/entity/customerorder/{order_id}/audit", {"limit": 100})
    for r in d.get("rows") or []:
        st = (r.get("diff") or {}).get("state") or {}
        new = (st.get("newValue") or {}).get("name")
        if new == STATE_AGREED and (r.get("uid") or "").lower() in APPROVER_MS_UIDS:
            return True
    return False


async def _fetch(session, url: str, params: dict | None = None) -> dict:
    async with session.get(url, headers=get_headers(), params=params,
                           timeout=aiohttp.ClientTimeout(total=60)) as r:
        r.raise_for_status()
        return await r.json()


EXPAND = "positions.assortment,agent,state,owner"


async def check_order_href(order_href: str, bot) -> None:
    """Из вебхука заказа: после первого прогона добора, заказ целиком одним запросом."""
    if not _seeded():
        return
    async with aiohttp.ClientSession() as session:
        order = await _fetch(session, order_href.split("?")[0], {"expand": EXPAND})
    await check_order(order, bot)


async def sweep(bot) -> dict:
    """Добор: заказы, изменённые за сутки. Первый прогон – тихая разметка текущих."""
    silent = not _seeded()
    since = (datetime.now(MSK) - timedelta(hours=SWEEP_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
    orders, offset = [], 0
    async with aiohttp.ClientSession() as session:
        while True:
            d = await _fetch(session, f"{MS_BASE}/entity/customerorder", {
                "filter": f"updated>={since};applicable=true", "expand": EXPAND, "limit": 100, "offset": offset})
            rows = d.get("rows") or []
            orders += rows
            if len(rows) < 100:
                break
            offset += 100
    found = 0
    for o in orders:
        try:
            if await check_order(o, bot, silent=silent):
                found += 1
        except Exception as e:
            logger.warning("price_watch sweep %s: %r", o.get("name"), e)
    if silent:
        _q("""insert into public.price_bypass_state (id, seeded_at) values (1, now())
              on conflict (id) do update set seeded_at = excluded.seeded_at""")
        logger.info("price_watch: первый прогон, размечено без отправки: %d из %d заказов", found, len(orders))
    elif found:
        logger.info("price_watch sweep: отправлено %d", found)
    return {"orders": len(orders), "found": found, "silent": silent}
