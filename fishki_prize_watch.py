"""FISHки: автоотметка «Приз отправлен» по отгрузке + просьба об отзыве на следующий день.

План: «F2B второй мозг»/plans/2026-09-29-fishki-автоотметка-приза-и-просьба-об-отзыве.md.

Приз (пласт 12040 по цене 0) отгружают на «Розничный покупатель*** (менеджер)», а победителя
пишут только текстом в комментарии заказа («для Брэд Фуд за фишки») и адресом доставки.
Поэтому сторож ищет отгрузки с 12040 по 0 и сопоставляет их с открытыми запросами на приз
(`redeem_requests.status='pending'`, таблица quiz-game) сначала по названию, потом по адресу.
Совпал один запрос → отмечаем «выдан» и ставим просьбу об отзыве на завтра 12:00 МСК.
Образцы уходят так же, но без открытого запроса и без слов «фишки/выигрыш/приз» – молчим.

Ручная кнопка в дашборде (quiz-game /api/redeem-requests/{id}/resolve) тоже ставит
`review_ask_due_at`, отправку в обоих случаях делает `send_due_review_asks`.
"""
from __future__ import annotations

import html
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import aiohttp
import psycopg2
import psycopg2.extras

from database import CONNECT_TIMEOUT_SEC, STATEMENT_TIMEOUT_MS
from moysklad import MS_BASE, get_headers
from notifier import WAZZUP_API_URL, _get_contacts_from_ms

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))
PRIZE_PRODUCT_ID = "6c6a6355-a11f-11f0-0a80-11aa003658b9"  # 12040 Форель филе, с/с, ЗАМОРОЖ., Трим С 0.9-1.3 кг.
LOOKBACK_DAYS = 2           # обычный тик: отгрузки, изменённые за 2 суток
BACKFILL_DAYS = 60          # первый прогон: хвост неотмеченных призов
ADDR_ORDERS_DAYS = 180      # адреса победителя – из его заказов за полгода
SEND_FROM_HOUR, SEND_TO_HOUR = 12, 18   # окно просьбы об отзыве, МСК
MAX_SEND_ATTEMPTS = 3
ALERT_MAX_AGE_DAYS = 7     # «не понял кому» – только по свежим отгрузкам
PRIZE_WORDS = ("фишк", "фишек", "выигр", "приз")
REVIEW_URL = "https://yandex.ru/maps/org/fish_tu_biznes/213891389392/reviews/"
REVIEW_TEXT = (
    "Спасибо, что играете в FISHки! Если приз понравился – оставьте, пожалуйста, "
    f"пару слов о нас:\n{REVIEW_URL}\n\nНам это очень помогает"
)

DDL = """
alter table public.redeem_requests add column if not exists resolved_by text;
alter table public.redeem_requests add column if not exists prize_demand_id text;
alter table public.redeem_requests add column if not exists prize_demand_name text;
alter table public.redeem_requests add column if not exists review_ask_due_at timestamptz;
alter table public.redeem_requests add column if not exists review_ask_sent_at timestamptz;
alter table public.redeem_requests add column if not exists review_ask_attempts integer not null default 0;
alter table public.redeem_requests add column if not exists review_ask_error text;
create table if not exists public.fishki_prize_state (
    id            int primary key default 1,
    backfilled_at timestamptz
);
create table if not exists public.fishki_prize_alerts (
    key     text primary key,           -- unmatched:<demand_id> | nocontact:<req_id> | fail:<req_id>
    sent_at timestamptz not null default now()
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


# ─── Сопоставление ────────────────────────────────────────────────────────────

_LEGAL = {"ооо", "ип", "ао", "зао", "пао", "оао", "нао", "ано", "ооо "}
_ADDR_STOP = {"россия", "рф", "г", "город", "ул", "улица", "д", "дом", "стр", "строение", "к", "корп",
              "корпус", "обл", "область", "р", "н", "район", "п", "пос", "поселок", "ш", "шоссе",
              "пр", "проспект", "пер", "переулок", "офис", "оф", "помещ", "пом", "эт", "этаж", "мо"}


def _norm(text: str) -> str:
    t = (text or "").lower().replace("ё", "е")
    return re.sub(r"[^0-9a-zа-я]+", " ", t).strip()


def _name_tokens(company: str) -> list[str]:
    words = _norm(company).split()
    is_ip = bool(words) and words[0] == "ип" or company.strip().lower().startswith("индивидуальный")
    words = [w for w in words if w not in _LEGAL and w != "индивидуальный" and w != "предприниматель"]
    if is_ip:
        return words[:1]          # у ИП в комментарии пишут фамилию
    return words


def _stem(tok: str) -> str:
    n = len(tok)
    if n <= 3:
        return tok
    if n == 4:
        return tok[:3]
    if n <= 6:
        return tok[:n - 1]
    return tok[:n - 2]


def _name_score(company: str, comment: str) -> tuple[float, int]:
    """Доля слов названия, найденных в комментарии (по основам), и длина самого длинного совпадения."""
    toks = _name_tokens(company)
    words = _norm(comment).split()
    if not toks or not words:
        return 0.0, 0
    hit, longest = 0, 0
    for t in toks:
        st = _stem(t)
        ok = any(w == t for w in words) if len(t) <= 3 else any(w.startswith(st) for w in words)
        if ok:
            hit += 1
            longest = max(longest, len(t))
    return hit / len(toks), longest


def _addr_tokens(addr: str) -> set[str]:
    toks = {w for w in _norm(addr).split() if w not in _ADDR_STOP}
    return {w for w in toks if not re.fullmatch(r"\d{6}", w)}   # без почтового индекса


def _addr_match(a: str, b: str) -> bool:
    ta, tb = _addr_tokens(a), _addr_tokens(b)
    if len(ta) < 2 or len(tb) < 2:
        return False
    if not any(re.search(r"\d", w) for w in ta & tb):   # номер дома должен совпасть
        return False
    small, big = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    return len(small & big) / len(small) >= 0.8


def _parse_ms(ts: str) -> datetime:
    return datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)


async def _ms_get(session, url: str, params: dict | None = None) -> dict | None:
    async with session.get(url, headers=get_headers(), params=params) as r:
        if r.status != 200:
            logger.warning("fishki_prize: МС %s → %s", url[-80:], r.status)
            return None
        return await r.json()


async def _prize_demands(session, since: datetime) -> list[dict]:
    """Отгрузки, изменённые после `since`, где есть 12040 по цене 0."""
    out, offset = [], 0
    flt = f"updated>={since.astimezone(MSK):%Y-%m-%d %H:%M:%S}"
    while True:
        d = await _ms_get(session, f"{MS_BASE}/entity/demand",
                          {"filter": flt, "limit": 100, "offset": offset, "expand": "positions,customerOrder"})
        if not d:
            break
        for doc in d.get("rows", []):
            for p in (doc.get("positions") or {}).get("rows", []):
                pid = p["assortment"]["meta"]["href"].split("/")[-1]
                if pid == PRIZE_PRODUCT_ID and not p.get("price"):
                    out.append(doc)
                    break
        offset += 100
        if offset >= d.get("meta", {}).get("size", 0):
            break
    return out


async def _client_addresses(session, client_id: str, cache: dict) -> list[str]:
    if client_id in cache:
        return cache[client_id]
    addrs = []
    cp = await _ms_get(session, f"{MS_BASE}/entity/counterparty/{client_id}")
    if cp and cp.get("actualAddress"):
        addrs.append(cp["actualAddress"])
    since = datetime.now(MSK) - timedelta(days=ADDR_ORDERS_DAYS)
    orders = await _ms_get(session, f"{MS_BASE}/entity/customerorder", {
        "filter": f"agent={MS_BASE}/entity/counterparty/{client_id};moment>={since:%Y-%m-%d %H:%M:%S}",
        "limit": 100})
    for o in (orders or {}).get("rows", []):
        if o.get("shipmentAddress") and o["shipmentAddress"] not in addrs:
            addrs.append(o["shipmentAddress"])
    cache[client_id] = addrs
    return addrs


async def match_demand(session, demand: dict, pending: list[dict], addr_cache: dict) -> tuple[str, dict | None, str]:
    """→ (вердикт, запрос, как нашли). Вердикты: match | ambiguous | unmatched_prize | sample."""
    order = demand.get("customerOrder") or {}
    comment = " ".join(x for x in (order.get("description"), demand.get("description")) if x)
    created = _parse_ms(demand.get("created") or demand["moment"])
    cands = [r for r in pending if r["created_at"] <= created]

    # Один клиент может иметь несколько открытых запросов – берём самый ранний
    by_client: dict[str, dict] = {}
    for r in sorted(cands, key=lambda r: r["created_at"]):
        by_client.setdefault(r["client_id"], r)

    scored = []
    for r in by_client.values():
        score, longest = _name_score(r["company_name"] or "", comment)
        if score == 1.0 or (score >= 0.5 and longest >= 5):
            scored.append((score, r))
    if scored:
        best = max(s for s, _ in scored)
        top = [r for s, r in scored if s == best]
        if len(top) == 1:
            return "match", top[0], "по названию"
        return "ambiguous", None, ", ".join(r["company_name"] for r in top)

    addr = demand.get("shipmentAddress") or (order.get("shipmentAddress") if order else "") or ""
    if addr:
        hits = []
        for r in by_client.values():
            for a in await _client_addresses(session, r["client_id"], addr_cache):
                if _addr_match(addr, a):
                    hits.append(r)
                    break
        if len(hits) == 1:
            return "match", hits[0], "по адресу"
        if len(hits) > 1:
            return "ambiguous", None, ", ".join(r["company_name"] for r in hits)

    if any(w in _norm(comment) for w in PRIZE_WORDS):
        return "unmatched_prize", None, ""
    return "sample", None, ""


# ─── Сторож отгрузок ──────────────────────────────────────────────────────────

def _tomorrow_noon() -> datetime:
    d = datetime.now(MSK).date() + timedelta(days=1)
    return datetime(d.year, d.month, d.day, 12, 0, tzinfo=MSK)


async def _alert_owner_once(bot, key: str, text: str) -> None:
    if _q("insert into public.fishki_prize_alerts(key) values (%s) on conflict do nothing returning key", (key,)) is None:
        return
    owner_chat = int(os.getenv("OWNER_CHAT_ID", "0") or 0)
    try:
        await bot.send_message(chat_id=owner_chat, text=text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        _q("delete from public.fishki_prize_alerts where key=%s", (key,))   # повторит следующий тик
        logger.warning("fishki_prize: алерт %s не отправлен (%r)", key, e)


async def sweep(bot) -> dict:
    pending = _q("""select id, client_id, company_name, created_at from public.redeem_requests
                    where status='pending' order by created_at""", fetch="all") or []
    stats = {"pending": len(pending), "demands": 0, "matched": 0, "alerts": 0}
    if not pending:
        return stats
    state = _q("select backfilled_at from public.fishki_prize_state where id=1")
    backfill = not (state and state["backfilled_at"])
    since = datetime.now(MSK) - timedelta(days=BACKFILL_DAYS if backfill else LOOKBACK_DAYS)

    used = {r["prize_demand_id"] for r in (_q(
        "select prize_demand_id from public.redeem_requests where prize_demand_id is not null", fetch="all") or [])}
    addr_cache: dict = {}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        demands = [d for d in await _prize_demands(session, since) if d["id"] not in used]
        stats["demands"] = len(demands)
        for d in sorted(demands, key=lambda x: x.get("created") or x["moment"]):
            verdict, req, how = await match_demand(session, d, pending, addr_cache)
            order = d.get("customerOrder") or {}
            link = (d.get("meta") or {}).get("uuidHref", "")
            if verdict == "match":
                row = _q("""update public.redeem_requests
                            set status='resolved', resolved_at=%s, resolved_by='auto',
                                prize_demand_id=%s, prize_demand_name=%s, review_ask_due_at=%s
                            where id=%s and status='pending' returning id""",
                         (_parse_ms(d["moment"]), d["id"], d["name"], _tomorrow_noon(), req["id"]))
                if row:
                    stats["matched"] += 1
                    pending = [p for p in pending if p["id"] != req["id"]]
                    logger.info("fishki_prize: приз %s → %s (%s), запрос #%s",
                                d["name"], req["company_name"], how, req["id"])
            elif verdict in ("ambiguous", "unmatched_prize") and not backfill \
                    and _parse_ms(d["moment"]) >= datetime.now(MSK) - timedelta(days=ALERT_MAX_AGE_DAYS):
                # первый прогон и старые отгрузки – это призы по уже отмеченным запросам, молчим
                why = (f"подходят несколько: {html.escape(how)}" if verdict == "ambiguous"
                       else "в комментарии не нашёл клиента из «Ожидают выдачи»")
                text = (f"🐟 Приз FISHки отгружен, но не понял кому – {why}.\n"
                        f"Отгрузка {d['name']}, заказ {order.get('name', '—')}: "
                        f"«{html.escape((order.get('description') or '').strip()[:200])}»\n"
                        f"Отметьте вручную в дашборде (FISHки → Ожидают выдачи).")
                if link:
                    text += f"\n<a href=\"{link}\">Открыть отгрузку</a>"
                await _alert_owner_once(bot, f"unmatched:{d['id']}", text)
                stats["alerts"] += 1
    if backfill:
        _q("""insert into public.fishki_prize_state(id, backfilled_at) values (1, now())
              on conflict (id) do update set backfilled_at=excluded.backfilled_at""")
    return stats


# ─── Просьба об отзыве ────────────────────────────────────────────────────────

async def _send_wazzup(contacts: list[dict], text: str) -> tuple[bool, str]:
    api_key = os.getenv("WAZZUP_API_KEY", "")
    last_err = "no_channels"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        for c in contacts:
            payload = {"channelId": c["channel_id"], "chatType": c["chat_type"], "text": text}
            if c["chat_id"].startswith("@"):
                payload["username"] = c["chat_id"].lstrip("@")
            else:
                payload["chatId"] = c["chat_id"]
            try:
                async with session.post(WAZZUP_API_URL, json=payload, headers={
                        "Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}) as r:
                    if r.status in (200, 201):
                        return True, c["chat_type"]
                    last_err = f"wazzup_err:{r.status}:{(await r.text())[:200]}"
            except Exception as e:
                last_err = f"net_err:{type(e).__name__}:{str(e)[:200]}"
    return False, last_err


async def send_due_review_asks(bot) -> dict:
    stats = {"sent": 0, "failed": 0}
    if not (SEND_FROM_HOUR <= datetime.now(MSK).hour < SEND_TO_HOUR):
        return stats
    due = _q("""select id, client_id, company_name, review_ask_attempts from public.redeem_requests
                where status='resolved' and review_ask_due_at <= now() and review_ask_sent_at is null
                  and review_ask_attempts < %s and coalesce(review_ask_error, '') <> 'no_contact'
                order by review_ask_due_at""", (MAX_SEND_ATTEMPTS,), fetch="all") or []
    headers = get_headers()
    for r in due:
        # claim до отправки: строго одно сообщение на запрос
        if not _q("""update public.redeem_requests set review_ask_sent_at=now()
                     where id=%s and review_ask_sent_at is null returning id""", (r["id"],)):
            continue
        contacts = await _get_contacts_from_ms(r["client_id"], headers)
        if not contacts:
            _q("update public.redeem_requests set review_ask_sent_at=null, review_ask_error='no_contact' where id=%s",
               (r["id"],))
            await _alert_owner_once(bot, f"nocontact:{r['id']}",
                                    f"🐟 Просьбу об отзыве за приз FISHки не отправил: у «{html.escape(r['company_name'] or '')}» "
                                    f"в карточке МойСклад нет Telegram/Max/WhatsApp.")
            stats["failed"] += 1
            continue
        ok, info = await _send_wazzup(contacts, REVIEW_TEXT)
        if ok:
            _q("update public.redeem_requests set review_ask_error=null where id=%s", (r["id"],))
            stats["sent"] += 1
            logger.info("fishki_prize: просьба об отзыве → %s (%s)", r["company_name"], info)
            continue
        attempts = r["review_ask_attempts"] + 1
        _q("""update public.redeem_requests set review_ask_sent_at=null, review_ask_attempts=%s,
                     review_ask_error=%s where id=%s""", (attempts, info, r["id"]))
        stats["failed"] += 1
        logger.warning("fishki_prize: просьба об отзыве %s не ушла (%s)", r["company_name"], info)
        if attempts >= MAX_SEND_ATTEMPTS:
            await _alert_owner_once(bot, f"fail:{r['id']}",
                                    f"🐟 Просьба об отзыве за приз FISHки не дошла до «{html.escape(r['company_name'] or '')}» "
                                    f"после {attempts} попыток: {html.escape(info[:150])}")
    return stats
