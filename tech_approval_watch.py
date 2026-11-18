"""Сторож одобрения технологом прихода сырья и привлечённых товаров.

План (второй мозг): plans/2026-10-23-одобрение-технологом-приход-сырья.md

В каждой приёмке МС есть флажок «Одобрено технологом». Технолог осматривает
приход и ставит галочку. Если галочки нет дольше порога (охлаждённое – 1 день,
замороженное – 3 дня, календарные дни от даты приёмки), раз в день в 10:00 МСК
бот шлёт список в личку закупщикам: Беляковой – сырьё, Павленко –
привлечённые; каждый день в списке – штраф 5 000 ₽ за приёмку.

Правило собственника 16.11.2026: одобрено, только если флажок включил технолог
(сейчас Маланчук, uid `alexandr@vicpure`). Проверяем по аудиту приёмки: последняя
правка поля «Одобрено технологом» – его, newValue=true. Склад ставил галочку при
заведении приёмки – такая галочка не считается (в аудите создания атрибутов не
видно, значит и автора нет).

Только чтение МС. Кому шлём – см. BUYERS (16.11.2026).
"""
from __future__ import annotations

import asyncio
import hashlib
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

ATTR_ID = "a8d905d3-cef5-11f1-0a80-0c62001b7a68"   # «Одобрено технологом», supply
ATTR_NAME = "Одобрено ответственным"
# 18.11.2026 собственник переименовал флажок; в старых записях аудита – прежнее имя.
ATTR_NAMES = ("Одобрено ответственным", "Одобрено технологом")

# Контроль только приёмок с этой даты (решение 23.10.2026).
START = date.fromisoformat(os.getenv("TECH_APPROVAL_START", "2026-10-24"))
CHILLED_DAYS = 1
FROZEN_DAYS = 3

# Группы товара → ответственный закупщик.
GROUP_RAW = "СЫРЬЕ"
GROUP_ATTR = "ПРИВЛЕЧЕННЫЕ ТОВАРЫ"
_GOODS_TYPES = ("product", "variant")

# Адресаты (Telegram id, сверено с memory reference_f2b_telegram_chat_ids).
BELYAKOVA_ID = 8267564735
PAVLENKO_ID = 8185545246


def technologist_uids() -> set[str]:
    raw = os.getenv("TECH_APPROVAL_UIDS") or "alexandr@vicpure"
    return {u.strip() for u in raw.split(",") if u.strip()}


# Кому шлём (решение собственника 16.11.2026): только закупщикам, каждой – её
# приёмки; собственнику и Маланчуку не шлём. Штраф 5 000 ₽ за каждый день,
# когда приёмка попала в список, – запись в public.tech_approval_penalty,
# её читает мотивация закупщиков (f2b-publisher buyer_kpi / buyer_kpi_moz).
BUYERS = {
    "belyakova": {"chat": BELYAKOVA_ID, "group": GROUP_RAW},
    "kristina": {"chat": PAVLENKO_ID, "group": GROUP_ATTR},
}


LOG_DDL = """
create table if not exists public.tech_approval_log (
    day         date not null,
    chat_id     bigint not null,
    fingerprint text,
    supplies    int,
    sent_at     timestamptz default now(),
    primary key (day, chat_id)
);
create table if not exists public.tech_approval_penalty (
    day         date not null,
    buyer       text not null,
    supply_id   text not null,
    supply_name text,
    supplier    text,
    supply_day  date,
    marked_by   text,
    created_at  timestamptz default now(),
    primary key (day, buyer, supply_id)
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


def _already_sent(day: date, chat: int, fp: str) -> bool:
    with _db().cursor() as cur:
        cur.execute("select fingerprint from public.tech_approval_log "
                    "where day=%s and chat_id=%s", (day, chat))
        row = cur.fetchone()
        return bool(row and row.get("fingerprint") == fp)


def _mark_sent(day: date, chat: int, fp: str, n: int):
    with _db().cursor() as cur:
        cur.execute("""
            insert into public.tech_approval_log (day, chat_id, fingerprint, supplies)
            values (%s, %s, %s, %s)
            on conflict (day, chat_id) do update set
              fingerprint = excluded.fingerprint, supplies = excluded.supplies,
              sent_at = now()
        """, (day, chat, fp, n))


def _record_penalties(day: date, buyer: str, cards: list[dict]):
    with _db().cursor() as cur:
        for c in cards:
            cur.execute("""
                insert into public.tech_approval_penalty
                  (day, buyer, supply_id, supply_name, supplier, supply_day, marked_by)
                values (%s, %s, %s, %s, %s, %s, %s)
                on conflict do nothing
            """, (day, buyer, c["id"], c["name"], c["supplier"], c["day"], c["marked_by"]))


# ── МойСклад (чтение) ────────────────────────────────────────────────────────
async def _get(session: aiohttp.ClientSession, url: str, params: dict | None = None) -> dict:
    """GET с ретраями на 429/5xx."""
    last = None
    for attempt in range(4):
        async with session.get(url, headers=get_headers(), params=params or {},
                               timeout=aiohttp.ClientTimeout(total=90)) as r:
            if r.status == 200:
                return await r.json()
            last = f"{r.status}: {(await r.text())[:200]}"
            if r.status not in (429, 500, 502, 503, 504):
                break
        await asyncio.sleep(3 * (attempt + 1))
    raise RuntimeError(f"МС {url} вернул {last}")


async def fetch_supplies(session, since: date) -> list[dict]:
    rows, offset = [], 0
    while True:
        data = await _get(session, f"{MS_BASE}/entity/supply", {
            "filter": f"moment>={since.strftime('%Y-%m-%d')} 00:00:00",
            "order": "moment,asc",
            "expand": "agent,positions.assortment.uom",
            "limit": 100, "offset": offset,
        })
        chunk = data.get("rows", [])
        rows.extend(chunk)
        if len(chunk) < 100:
            return rows
        offset += 100


async def approval_by_audit(session, supply_id: str) -> tuple[bool, str | None]:
    """(одобрено технологом?, uid последнего, кто трогал флажок)."""
    data = await _get(session, f"{MS_BASE}/entity/supply/{supply_id}/audit", {"limit": 100})
    for row in data.get("rows", []):          # МС отдаёт от новых к старым
        diff = row.get("diff") or {}
        name = next((n for n in ATTR_NAMES if n in diff), None)
        if not name:
            continue
        v = diff[name]
        new = v.get("newValue") if isinstance(v, dict) else v
        uid = row.get("uid")
        return (new is True and uid in technologist_uids()), uid
    return False, None


# ── разбор ───────────────────────────────────────────────────────────────────
def _flag(supply: dict):
    for a in supply.get("attributes") or []:
        if a.get("id") == ATTR_ID:
            return a.get("value")
    return None


def analyze(supply: dict) -> dict | None:
    """Карточка приёмки, если в ней есть сырьё/привлечённые; иначе None."""
    groups, chilled, items, qty = set(), False, [], {}
    for p in (supply.get("positions") or {}).get("rows", []) or []:
        a = p.get("assortment") or {}
        if (a.get("meta") or {}).get("type") not in _GOODS_TYPES:
            continue
        root = (a.get("pathName") or "").split("/", 1)[0].strip().upper()
        if root not in (GROUP_RAW, GROUP_ATTR):
            continue
        groups.add(root)
        name = a.get("name") or "?"
        if "ОХЛ" in name.upper():
            chilled = True
        items.append(name)
        uom = ((a.get("uom") or {}).get("name") or "кг").strip()
        qty[uom] = qty.get(uom, 0.0) + float(p.get("quantity") or 0)
    if not groups:
        return None
    day = date.fromisoformat((supply.get("moment") or "")[:10])
    return {
        "id": supply.get("id"),
        "name": supply.get("name") or "?",
        "supplier": (supply.get("agent") or {}).get("name") or "—",
        "upd": supply.get("incomingNumber") or "",
        "day": day,
        "groups": groups,
        "chilled": chilled,
        "items": items,
        "qty": qty,
        "flag": _flag(supply),
        "url": (supply.get("meta") or {}).get("uuidHref") or "",
    }


def buyers(card: dict) -> str:
    who = []
    if GROUP_RAW in card["groups"]:
        who.append("Белякова")
    if GROUP_ATTR in card["groups"]:
        who.append("Павленко")
    return ", ".join(who)


async def _employee_names(session) -> dict[str, str]:
    data = await _get(session, f"{MS_BASE}/entity/employee", {"limit": 1000})
    return {e.get("uid"): e.get("name") for e in data.get("rows", []) if e.get("uid")}


async def collect(today: date | None = None) -> list[dict]:
    today = today or datetime.now(MSK).date()
    out = []
    async with aiohttp.ClientSession() as session:
        supplies = await fetch_supplies(session, START)
        names = None
        for s in supplies:
            card = analyze(s)
            if not card:
                continue
            limit = CHILLED_DAYS if card["chilled"] else FROZEN_DAYS
            age = (today - card["day"]).days
            if age <= limit:
                continue
            card["age"], card["overdue"] = age, age - limit
            card["marked_by"] = None
            # С 18.11.2026 галочку «Одобрено ответственным» может ставить любой (собственник) –
            # стоит галочка, значит одобрено; кто ставил, больше не проверяем.
            if card["flag"] is True:
                continue
            out.append(card)
    out.sort(key=lambda c: c["age"], reverse=True)
    return out


# ── рендер ───────────────────────────────────────────────────────────────────
def _plural_days(n: int) -> str:
    if 11 <= n % 100 <= 14:
        return "дней"
    return {1: "день", 2: "дня", 3: "дня", 4: "дня"}.get(n % 10, "дней")


def _num(x: float) -> str:
    s = f"{x:,.3f}".replace(",", " ")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s.replace(".", ",")


def _qty(q: dict) -> str:
    return " + ".join(f"{_num(v)} {u}" for u, v in q.items())


def _plural_supply(n: int) -> str:
    if 11 <= n % 100 <= 14:
        return "приёмок"
    return {1: "приёмка", 2: "приёмки", 3: "приёмки", 4: "приёмки"}.get(n % 10, "приёмок")


def render(cards: list[dict], today: date) -> str:
    lines = [f"<b>Приход без одобрения технолога</b> – {today.strftime('%d.%m')}, "
             f"{len(cards)} {_plural_supply(len(cards))}",
             "<i>охл. дольше 1 дня, с/м дольше 3 дней; галочку в приёмке ставит "
             "Маланчук, отметка склада не считается. Штраф 5 000 ₽ за каждый день "
             "в этом списке по каждой приёмке</i>"]
    for c in cards:
        label = f"{c['name']} {c['supplier']}"
        link = f'<a href="{c["url"]}">{escape(label)}</a>' if c["url"] else escape(label)
        items = ", ".join(dict.fromkeys(c["items"]))
        if len(items) > 120:
            items = items[:117] + "…"
        upd = f" · УПД {escape(c['upd'])}" if c["upd"] else ""
        kind = "охл." if c["chilled"] else "с/м"
        lines.append(
            f"\n• {link}{upd} от {c['day'].strftime('%d.%m')}\n"
            f"  {escape(items)} – {_qty(c['qty'])} ({kind})\n"
            f"  без осмотра {c['age']} {_plural_days(c['age'])}, просрочка "
            f"{c['overdue']} {_plural_days(c['overdue'])} · закупка: {buyers(c)}"
            + (f"\n  галочка стоит, но отметил не технолог: {escape(c['marked_by'])}"
               if c["marked_by"] else "")
        )
    return "\n".join(lines)


def fingerprint(cards: list[dict]) -> str:
    raw = "|".join(f"{c['name']}:{c['age']}:{c['marked_by']}" for c in cards)
    return hashlib.sha1(raw.encode()).hexdigest()


async def _send_long(app, chat: int, text: str):
    """Телеграм режет на 4096 символов – делим по пустым строкам."""
    chunk = ""
    for part in text.split("\n\n"):
        if len(chunk) + len(part) + 2 > 3900 and chunk:
            await app.bot.send_message(chat_id=chat, text=chunk, parse_mode="HTML",
                                       disable_web_page_preview=True)
            chunk = ""
        chunk = f"{chunk}\n\n{part}" if chunk else part
    if chunk:
        await app.bot.send_message(chat_id=chat, text=chunk, parse_mode="HTML",
                                   disable_web_page_preview=True)


async def run(app, dry_run: bool = False) -> dict:
    today = datetime.now(MSK).date()
    cards = await collect(today)
    stats = {"supplies": len(cards)}
    for buyer, cfg in BUYERS.items():
        mine = [c for c in cards if cfg["group"] in c["groups"]]
        stats[buyer] = len(mine)
        if not mine:
            continue
        text = render(mine, today)
        if dry_run:
            logger.info("tech_approval DRY-RUN %s:\n%s", buyer, text)
            continue
        _record_penalties(today, buyer, mine)
        fp = fingerprint(mine)
        if _already_sent(today, cfg["chat"], fp):
            continue
        try:
            await _send_long(app, cfg["chat"], text)
            _mark_sent(today, cfg["chat"], fp, len(mine))
        except Exception as e:
            logger.error("tech_approval: не ушло %s: %s", buyer, e)
    return stats


async def poll_job(app) -> dict:
    try:
        return await run(app)
    except Exception as e:
        logger.error("tech_approval poll_job: %s", e, exc_info=True)
        return {}
