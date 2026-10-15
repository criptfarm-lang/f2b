"""
Честный знак: GTIN из входящих УПД поставщиков (Диадок) → штрихкод в карточке товара МС.
План (второй мозг): plans/2026-10-15-чз-gtin-из-входящих-упд.md.

GTIN в УПД бывает в двух местах: атрибут ДопСведТов/@ГТИН (учёт по коду товара и
количеству – масло, бакалея) и внутри кода маркировки <КИЗ>01<GTIN 14>21… (поштучный).
Строку УПД сопоставляем с позицией приёмки МС: приёмка того же поставщика с
incomingNumber = номер УПД, позиция – по количеству и словам названия.

Запись в МС – ТОЛЬКО добавление штрихкода GTIN в карточку товара (разрешение
собственника 15.10.2026, memory feedback_no_writes_to_moysklad): существующие
штрихкоды сохраняются, товары своих нужд не трогаем, каждая запись – в edo_gtin_log.
"""
from __future__ import annotations

import html
import logging
import os
import re
from datetime import date, datetime, timedelta

import aiohttp
import psycopg2.extras

import edo_unsigned as edo

logger = logging.getLogger(__name__)

_OWN_NEEDS = ("яПРОЧЕЕ", "УПАКОВКА", "ПИЩЕВЫЕ ДОБАВКИ")
_FIRST_FROM = "01.01.2026"   # первый прогон – с начала года, дальше – окно 30 дней

DDL = """
create table if not exists public.edo_gtin_seen (
    entity_id    text primary key,
    doc_number   text,
    doc_date     date,
    processed_at timestamptz default now()
);
create table if not exists public.edo_gtin_log (
    id          bigserial primary key,
    entity_id   text,
    doc_number  text,
    doc_date    date,
    supplier    text,
    line_name   text,
    gtin        text,
    product_id  uuid,
    product     text,
    action      text,      -- written / exists / own_needs / unmatched
    reason      text,
    created_at  timestamptz default now()
);
"""


def _db():
    conn = edo._db()
    with conn.cursor() as cur:
        cur.execute(DDL)
    return conn


# ── разбор УПД ───────────────────────────────────────────────────────────────
def _decode(raw: bytes) -> str:
    m = re.search(rb'encoding="([^"]+)"', raw[:200])
    return raw.decode(m.group(1).decode() if m else "utf-8", "replace")


def parse_lines(xml: str) -> list[dict]:
    """Строки УПД, где есть GTIN: {name, qty, gtins}."""
    out = []
    for st in re.findall(r"<СведТов .*?</СведТов>", xml, re.S):
        gt = set(re.findall(r'ГТИН="(\d{14})"', st))
        for k in re.findall(r"<КИЗ>([^<]+)</КИЗ>", st):
            k = html.unescape(k)
            if k.startswith("01") and k[2:16].isdigit():
                gt.add(k[2:16])
        if not gt:
            continue
        attr = lambda a: (re.search(a + r'="([^"]*)"', st) or [None, None])[1]
        try:
            qty = float((attr("КолТов") or "0").replace(",", "."))
        except ValueError:
            qty = 0.0
        out.append({"name": html.unescape(attr("НаимТов") or ""), "qty": qty, "gtins": sorted(gt)})
    return out


# ── сопоставление с приёмкой ─────────────────────────────────────────────────
def _words(s: str) -> set[str]:
    return {w for w in re.findall(r"[a-zа-яё0-9]+", (s or "").lower().replace("ё", "е")) if len(w) > 2}


def match_position(line: dict, positions: list[dict]) -> tuple[dict | None, str]:
    """Позиция приёмки для строки УПД. Сначала равное количество, при нескольких –
    по совпадению слов; без равного количества – только уверенное совпадение слов."""
    lw = _words(line["name"])
    score = lambda p: len(lw & _words(p["assortment"].get("name", ""))) / max(1, len(_words(p["assortment"].get("name", ""))))
    same_qty = [p for p in positions if abs((p.get("quantity") or 0) - line["qty"]) < 1e-6]
    if len(same_qty) == 1 and score(same_qty[0]) >= 0.3:
        return same_qty[0], "количество и название"
    pool = same_qty or positions
    best = sorted(pool, key=score, reverse=True)
    if best and score(best[0]) >= 0.6 and (len(best) == 1 or score(best[0]) > score(best[1])):
        return best[0], "по названию"
    return None, "позиция в приёмке не найдена однозначно"


async def _ms_get(session, path: str, params: dict | None = None) -> dict:
    from moysklad import MS_BASE, get_headers
    url = path if path.startswith("http") else f"{MS_BASE}{path}"
    async with session.get(url, headers=get_headers(), params=params or {},
                           timeout=aiohttp.ClientTimeout(total=60)) as r:
        if r.status != 200:
            raise RuntimeError(f"МС {path} {r.status}: {(await r.text())[:200]}")
        return await r.json()


async def find_supply(session, supplier_inn: str, number: str, doc_date: date) -> dict | None:
    """Приёмка поставщика с incomingNumber = номер УПД (фильтр МС по этому полю не
    работает со «/» в номере – сравниваем сами в окне дат)."""
    from moysklad import MS_BASE
    cps = (await _ms_get(session, "/entity/counterparty", {"filter": f"inn={supplier_inn}", "limit": 20})).get("rows", [])
    lo = (doc_date - timedelta(days=10)).strftime("%Y-%m-%d 00:00:00")
    hi = (doc_date + timedelta(days=40)).strftime("%Y-%m-%d 23:59:59")
    for cp in cps:
        rows = (await _ms_get(session, "/entity/supply", {
            "filter": f"agent={MS_BASE}/entity/counterparty/{cp['id']};moment>={lo};moment<={hi}",
            "expand": "positions.assortment", "limit": 100})).get("rows", [])
        for s in rows:
            if (s.get("incomingNumber") or "").strip() == number.strip():
                return s
    return None


async def _add_gtin(session, product: dict, gtin: str) -> str:
    """Добавить штрихкод GTIN в карточку товара. 'exists' – уже есть, 'written' – добавлен."""
    from moysklad import get_headers
    fresh = await _ms_get(session, product["meta"]["href"])
    barcodes = fresh.get("barcodes") or []
    if any(b.get("gtin") == gtin for b in barcodes):
        return "exists"
    async with session.put(product["meta"]["href"], headers=get_headers(),
                           json={"barcodes": barcodes + [{"gtin": gtin}]},
                           timeout=aiohttp.ClientTimeout(total=60)) as r:
        if r.status != 200:
            raise RuntimeError(f"PUT штрихкода {r.status}: {(await r.text())[:200]}")
    check = await _ms_get(session, product["meta"]["href"])  # контрольное чтение
    if not any(b.get("gtin") == gtin for b in check.get("barcodes") or []):
        raise RuntimeError("после записи GTIN в карточке нет")
    return "written"


# ── прогон ───────────────────────────────────────────────────────────────────
async def run(write: bool = True) -> list[dict]:
    """Новые входящие УПД → GTIN в МС. Возвращает записи журнала этого прогона."""
    conn = _db()
    with conn.cursor() as cur:
        cur.execute("select entity_id from public.edo_gtin_seen")
        seen = {r["entity_id"] for r in cur.fetchall()}
    frm = (date.today() - timedelta(days=30)).strftime("%d.%m.%Y") if seen else _FIRST_FROM
    log: list[dict] = []
    async with aiohttp.ClientSession() as s:
        docs, after = {}, None
        for _ in range(50):
            q = {"boxId": edo.BOX_ID, "filterCategory": "UniversalTransferDocument.Inbound",
                 "fromDocumentDate": frm}
            if after:
                q["afterIndexKey"] = after
            page = (await edo._get(s, "/V3/GetDocuments", q)).get("Documents") or []
            new = [d for d in page if d["EntityId"] not in docs]
            for d in new:
                docs[d["EntityId"]] = d
            if len(page) < 100 or not new:
                break
            after = page[-1].get("IndexKey")

        tok = await edo._access_token()
        for d in docs.values():
            if d["EntityId"] in seen:
                continue
            async with s.get(f"{edo.API}/V4/GetEntityContent",
                             params={"boxId": edo.BOX_ID, "messageId": d["MessageId"], "entityId": d["EntityId"]},
                             headers={"Authorization": f"Bearer {tok}"},
                             timeout=aiohttp.ClientTimeout(total=60)) as r:
                if r.status != 200:
                    logger.warning(f"edo_gtin: контент {d.get('DocumentNumber')} {r.status}")
                    continue
                lines = parse_lines(_decode(await r.read()))
            number, ddate = d.get("DocumentNumber") or "", d.get("DocumentDate")
            try:
                dd = datetime.strptime(ddate, "%d.%m.%Y").date()
            except Exception:
                dd = date.today()
            supplier = None
            if lines:
                org = await edo._get(s, "/GetOrganization", {"boxId": d["CounteragentBoxId"]})
                supplier = org.get("ShortName") or org.get("FullName")
                supply = await find_supply(s, org.get("Inn") or "", number, dd)
                positions = ((supply or {}).get("positions") or {}).get("rows", []) if supply else []
                for ln in lines:
                    base = {"entity_id": d["EntityId"], "doc_number": number, "doc_date": dd,
                            "supplier": supplier, "line_name": ln["name"]}
                    pos, why = match_position(ln, positions) if positions else (None, "приёмка по УПД не найдена")
                    a = (pos or {}).get("assortment") or {}
                    for g in ln["gtins"]:
                        rec = {**base, "gtin": g, "product_id": a.get("id"), "product": a.get("name")}
                        own_supply = positions and all(
                            ((p.get("assortment") or {}).get("pathName") or "").startswith(_OWN_NEEDS)
                            for p in positions)
                        if not pos and own_supply:
                            log.append({**rec, "action": "own_needs", "reason": "приёмка своих нужд"})
                        elif not pos or a.get("meta", {}).get("type") != "product":
                            log.append({**rec, "action": "unmatched", "reason": why})
                        elif (a.get("pathName") or "").startswith(_OWN_NEEDS):
                            log.append({**rec, "action": "own_needs", "reason": "товар своих нужд"})
                        elif not write:
                            log.append({**rec, "action": "dry_run", "reason": why})
                        else:
                            try:
                                log.append({**rec, "action": await _add_gtin(s, a, g), "reason": why})
                            except Exception as e:
                                log.append({**rec, "action": "unmatched", "reason": f"ошибка записи: {e}"[:200]})
            if write:
                with conn.cursor() as cur:
                    cur.execute("insert into public.edo_gtin_seen (entity_id, doc_number, doc_date) "
                                "values (%s, %s, %s) on conflict do nothing", (d["EntityId"], number, dd))
    if write and log:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """insert into public.edo_gtin_log
                (entity_id, doc_number, doc_date, supplier, line_name, gtin, product_id, product, action, reason)
                values %s""", [(x["entity_id"], x["doc_number"], x["doc_date"], x["supplier"], x["line_name"],
                                x["gtin"], x["product_id"], x["product"], x["action"], x["reason"]) for x in log])
    return log


def missing_gtin_products() -> list[str]:
    """Товары обязательных групп ЧЗ (по классификатору), у которых в карточке нет GTIN –
    считаем по кэшу классификатора и последней записи GTIN; карточки читаются при отчёте."""
    conn = _db()
    with conn.cursor() as cur:
        cur.execute("""select c.product_id, c.name from public.chz_product_class c
                       join public.chz_groups g on g.key = c.group_key
                       where g.status = 'mandatory' and c.group_key <> 'unknown'""")
        return [(str(r["product_id"]), r["name"]) for r in cur.fetchall()]


def _accountant_chat_id() -> int | None:
    v = (os.getenv("CHZ_ACCOUNTANT_CHAT_ID") or "96477973").strip()
    return int(v) if v.isdigit() else None


async def job(app, weekly_missing: bool = False):
    """Ежедневно: прогон + сообщение главбуху, если что-то записали или не сопоставили.
    По понедельникам – ещё список маркируемых товаров без GTIN в карточке."""
    try:
        log = await run(write=True)
    except Exception as e:
        logger.error(f"edo_gtin: прогон упал: {e}", exc_info=True)
        return
    written = [x for x in log if x["action"] == "written"]
    unmatched = [x for x in log if x["action"] == "unmatched"]
    lines = []
    if written:
        lines.append("Честный знак: GTIN из УПД записан в карточки МойСклада")
        for x in written:
            lines.append(f"• {x['product']} – {x['gtin']} (УПД {x['doc_number']}, {x['supplier']})")
    if unmatched:
        lines.append("" if lines else "Честный знак: GTIN из УПД")
        lines.append("Не смог сопоставить с товаром МС – внесите вручную:")
        for x in unmatched:
            lines.append(f"• {x['line_name'][:60]} – GTIN {x['gtin']} (УПД {x['doc_number']} от "
                         f"{x['doc_date']:%d.%m.%Y}, {x['supplier']}): {x['reason']}")
    if weekly_missing:
        miss = []
        async with aiohttp.ClientSession() as s:
            from moysklad import MS_BASE
            for pid, name in missing_gtin_products():
                try:
                    p = await _ms_get(s, f"{MS_BASE}/entity/product/{pid}")
                except Exception:
                    continue
                if not p.get("archived") and not any(b.get("gtin") for b in p.get("barcodes") or []):
                    miss.append(name)
        if miss:
            lines += ["", "Маркируемые товары без GTIN в карточке (из УПД пока не пришёл):"]
            lines += [f"• {n}" for n in sorted(miss)]
    chat = _accountant_chat_id()
    if lines and chat:
        text = "\n".join(lines)
        for i in range(0, len(text), 3800):
            await app.bot.send_message(chat, text[i:i + 3800])
