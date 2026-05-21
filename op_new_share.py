"""F2B — async-расчёт % операций ОП на новых клиентах.

Canonical-классификатор. См. план
`.../F2B второй мозг/plans/2026-05-21-виджет-процент-новых-в-отчете-оп.md`.

Изначально жил в repo «второй мозг» `scripts/op_new_client_share.py` — после
2026-05-21 canonical = этот модуль (бот). При правках алгоритма — сначала
здесь, затем синхронизировать со скриптом «второго мозга».

CLI:
    python op_new_share.py                     # посчитать MTD, вывести JSON
    python op_new_share.py --backfill          # посчитать MTD и записать в БД
    python op_new_share.py --start ... --end ...  # явное окно
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
from collections import Counter
from datetime import date, datetime
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import aiohttp

logger = logging.getLogger(__name__)

MSK = ZoneInfo("Europe/Moscow")

# amoCRM user_id → ФИО (5 менеджеров ОП).
# СИНХРОНИЗИРОВАТЬ с bot.py TAGS (фамилия → ФИО) и с canonical скриптом
# scripts/op_new_client_share.py в repo «второй мозг».
MANAGERS: dict[int, str] = {
    11544494: "Инесса Скляр",
    12625622: "Карина Баласанян",
    12788698: "Елена Мерзлякова",
    13665786: "Денис Коликов",
    13746010: "Ирина Дьяченко",
}

# Whitelist типов событий amoCRM, считающихся «операцией» менеджера.
OP_WHITELIST = {
    "outgoing_chat_message", "conversation_answered",
    "outgoing_call", "incoming_call", "outgoing_mail", "incoming_mail",
    "task_added", "task_completed", "task_result_added",
    "lead_status_changed", "customer_status_changed",
    "common_note_added", "attachment_note_added",
    "lead_added", "contact_added", "company_added", "customer_added",
}

OPF = {"ооо", "оао", "зао", "пао", "нпп", "ип", "ао", "нко", "чп", "тд", "тк",
       "ук", "сро", "фгуп", "гуп", "муп", "снт", "кфх", "спк", "пк", "фл"}
TRASH = {"повтор", "заказ", "заказа", "заявка", "заявки", "франшиза", "сделка",
         "от", "для", "переписк", "переписка", "автосделка", "рассылка",
         "ча", "на", "в", "и"}

PLURAL = {"leads": "lead", "contacts": "contact",
          "companies": "company", "customers": "customer"}

AMO_SUBDOMAIN = os.getenv("AMO_SUBDOMAIN", "victorfishtobiz")
AMO_BASE = f"https://{AMO_SUBDOMAIN}.amocrm.ru/api/v4"
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

SLEEP_AMO = 0.15
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=60)


def _amo_headers() -> dict:
    return {"Authorization": f"Bearer {os.getenv('AMO_ACCESS_TOKEN', '')}"}


def _ms_headers() -> dict:
    return {
        "Authorization": f"Bearer {os.getenv('MOYSKLAD_TOKEN', '')}",
        "Accept-Encoding": "gzip",
    }


def _is_op(t: str) -> bool:
    return t in OP_WHITELIST or (
        t.startswith("custom_field_") and t.endswith("_value_changed")
    )


async def _amo_get(session: aiohttp.ClientSession, path: str, params=None):
    url = AMO_BASE + path
    if params is not None:
        url += "?" + urlencode(params, doseq=True, safe="[]")
    for _ in range(3):
        try:
            async with session.get(url, headers=_amo_headers(), timeout=HTTP_TIMEOUT) as r:
                if r.status in (204, 404):
                    return None
                if r.status == 429:
                    await asyncio.sleep(2)
                    continue
                if r.status != 200:
                    logger.warning(f"amoCRM GET {path}: {r.status}")
                    return None
                return await r.json()
        except asyncio.TimeoutError:
            logger.warning(f"amoCRM GET {path}: timeout")
            return None
        finally:
            await asyncio.sleep(SLEEP_AMO)
    return None


async def _amo_get_all(session, path, params):
    out = []
    page = 1
    while True:
        if isinstance(params, list):
            p = list(params) + [("page", page)]
            if not any(k == "limit" for k, _ in p):
                p.append(("limit", 250))
        else:
            p = dict(params)
            p["page"] = page
            p.setdefault("limit", 250)
        d = await _amo_get(session, path, p)
        if not d or "_embedded" not in d:
            break
        items = next(iter(d["_embedded"].values()), [])
        if not items:
            break
        out.extend(items)
        if "next" not in d.get("_links", {}):
            break
        page += 1
    return out


async def _amo_batch(session, path, ids, batch=50):
    out = {}
    ids = list(set(ids))
    for i in range(0, len(ids), batch):
        chunk = ids[i:i + batch]
        params = [("filter[id][]", str(x)) for x in chunk] + [("limit", 250)]
        d = await _amo_get(session, path, params)
        if not d or "_embedded" not in d:
            continue
        items = next(iter(d["_embedded"].values()), [])
        for it in items:
            key = it.get("id") if "id" in it else it.get("talk_id")
            if key is not None:
                out[key] = it
    return out


async def _ms_get(session, path, params=None):
    url = MS_BASE + path
    if params is not None:
        url += "?" + urlencode(params)
    async with session.get(url, headers=_ms_headers(), timeout=HTTP_TIMEOUT) as r:
        if r.status != 200:
            logger.warning(f"МС GET {path}: {r.status}")
            return None
        return await r.json()


def _norm_name(s: str):
    s = (s or "").lower()
    s = re.sub(r'["«»\']', " ", s)
    s = re.sub(r"[^a-zа-я0-9ё ]+", " ", s)
    toks = [t for t in s.split()
            if t and t not in OPF and t not in TRASH and not t.isdigit()]
    return " ".join(toks), toks


async def compute_new_client_share(start: date, end: date) -> dict:
    """Считает % операций ОП на новых клиентах за окно [start..end] (МСК).

    Возвращает dict:
      {
        "computed_at": iso (МСК),
        "window": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"},
        "company_pct": float (взвешено по объёму операций),
        "by_manager": {full_name: float}
      }
    """
    ts_from = int(datetime(start.year, start.month, start.day,
                           0, 0, 0, tzinfo=MSK).timestamp())
    ts_to = int(datetime(end.year, end.month, end.day,
                         23, 59, 59, tzinfo=MSK).timestamp())

    async with aiohttp.ClientSession() as session:
        # 1. Events по 5 менеджерам
        all_events: dict[str, list] = {}
        for uid, name in MANAGERS.items():
            evs = await _amo_get_all(session, "/events", {
                "filter[created_by]": uid,
                "filter[created_at][from]": ts_from,
                "filter[created_at][to]": ts_to,
            })
            all_events[name] = evs
            logger.info(f"compute_new_client_share: {name}: {len(evs)} events")

        op_events = {n: [e for e in evs if _is_op(e.get("type", ""))]
                     for n, evs in all_events.items()}

        # 2. Резолв task → parent
        task_ids = set()
        for evs in op_events.values():
            for e in evs:
                if e.get("entity_type") == "task":
                    task_ids.add(e.get("entity_id"))
        tasks_map = (await _amo_batch(session, "/tasks", list(task_ids))
                     if task_ids else {})
        logger.info(f"compute_new_client_share: tasks resolved "
                    f"{len(tasks_map)}/{len(task_ids)}")

        # /talks fetch с расширенным окном (как в canonical)
        talks_map: dict = {}
        page = 1
        while True:
            d = await _amo_get(session, "/talks", {
                "filter[updated_at][from]": ts_from - 60 * 86400,
                "filter[updated_at][to]": ts_to + 86400,
                "page": page, "limit": 250,
            })
            if not d or "_embedded" not in d:
                break
            items = d["_embedded"].get("talks", [])
            if not items:
                break
            for t in items:
                talks_map[t.get("talk_id")] = t
            if "next" not in d.get("_links", {}):
                break
            page += 1
        logger.info(f"compute_new_client_share: talks fetched {len(talks_map)}")

        def resolve(e):
            et, eid = e.get("entity_type"), e.get("entity_id")
            if et == "task" and eid in tasks_map:
                t = tasks_map[eid]
                return t.get("entity_id"), PLURAL.get(
                    t.get("entity_type"), t.get("entity_type"))
            if et == "talk" and eid in talks_map:
                t = talks_map[eid]
                return t.get("entity_id"), PLURAL.get(
                    t.get("entity_type"), t.get("entity_type"))
            return eid, PLURAL.get(et, et)

        # 3. Сбор уникальных entities
        customer_ids: set = set()
        lead_ids: set = set()
        contact_ids: set = set()
        company_ids: set = set()
        for evs in op_events.values():
            for e in evs:
                pid, ptype = resolve(e)
                if not pid:
                    continue
                if ptype == "customer":
                    customer_ids.add(pid)
                elif ptype == "lead":
                    lead_ids.add(pid)
                elif ptype == "contact":
                    contact_ids.add(pid)
                elif ptype == "company":
                    company_ids.add(pid)

        leads_map = (await _amo_batch(session, "/leads", list(lead_ids))
                     if lead_ids else {})
        contacts_map = (await _amo_batch(session, "/contacts", list(contact_ids))
                        if contact_ids else {})

        # 4. customers — purchases_count напрямую
        customers_full: dict = {}
        for cid in customer_ids:
            customers_full[cid] = await _amo_get(session, f"/customers/{cid}")

        # 5. companies — embedded из leads/contacts + явные из событий
        company_set = set(company_ids)
        for ld in leads_map.values():
            for c in (ld.get("_embedded") or {}).get("companies") or []:
                company_set.add(c["id"])
        for ct in contacts_map.values():
            for co in (ct.get("_embedded") or {}).get("companies") or []:
                company_set.add(co["id"])
        companies_map = (await _amo_batch(session, "/companies", list(company_set))
                         if company_set else {})

        def get_inn(co):
            for f in (co.get("custom_fields_values") or []):
                code = f.get("field_code") or ""
                name_l = (f.get("field_name") or "").lower()
                if code == "INN" or "инн" in name_l:
                    for v in f.get("values") or []:
                        val = re.sub(r"\D", "", str(v.get("value") or ""))
                        if val:
                            return val
            return None

        company_inn = {cid: get_inn(c) for cid, c in companies_map.items()}

        # 6. МС /report/counterparty снимок
        logger.info("compute_new_client_share: загружаю МС /report/counterparty")
        ms_counts: list = []
        offset = 0
        while True:
            d = await _ms_get(session, "/report/counterparty",
                              {"limit": 1000, "offset": offset})
            if not d:
                break
            rows = d.get("rows", [])
            if not rows:
                break
            for r in rows:
                cp = r.get("counterparty", {})
                inn = re.sub(r"\D", "", cp.get("inn") or "")
                ms_name = cp.get("name", "")
                n_norm, n_toks = _norm_name(ms_name)
                ms_counts.append((inn, ms_name, n_norm, n_toks,
                                  r.get("demandsCount") or 0))
            if len(rows) < 1000:
                break
            offset += 1000

        inn_to_demands: dict[str, int] = {}
        for inn, _, _, _, dc in ms_counts:
            if inn:
                inn_to_demands[inn] = inn_to_demands.get(inn, 0) + dc

        # 7. Fuzzy lookup
        def fuzzy(amo_name):
            n, toks = _norm_name(amo_name)
            if not toks:
                return None
            for inn, ms_name, ms_norm, ms_toks, dem in ms_counts:
                if ms_norm and ms_norm == n:
                    return (ms_name, dem)
            if len(n) >= 5:
                for inn, ms_name, ms_norm, ms_toks, dem in ms_counts:
                    if not ms_norm or len(ms_norm) < 5:
                        continue
                    if ms_norm in n or n in ms_norm:
                        return (ms_name, dem)
            amo_set = set(toks)
            best = None
            for inn, ms_name, ms_norm, ms_toks, dem in ms_counts:
                if not ms_toks:
                    continue
                if not (len(ms_toks) >= 2 or any(len(t) >= 5 for t in ms_toks)):
                    continue
                if all(t in amo_set for t in ms_toks):
                    score = sum(len(t) for t in ms_toks)
                    if best is None or score > best[2]:
                        best = (ms_name, dem, score)
            if best:
                return (best[0], best[1])
            return None

        def loyal_by_name(name):
            m = fuzzy(name)
            return m and m[1] >= 3

        def classify(pid, ptype):
            if ptype == "customer":
                c = customers_full.get(pid)
                if not c:
                    return "unknown"
                return "loyal" if (c.get("purchases_count") or 0) >= 3 else "new"
            if ptype == "lead":
                ld = leads_map.get(pid)
                if not ld:
                    return "unknown"
                comps = (ld.get("_embedded") or {}).get("companies") or []
                for c in comps:
                    inn = company_inn.get(c["id"])
                    if inn and inn_to_demands.get(inn, 0) >= 3:
                        return "loyal"
                    co = companies_map.get(c["id"])
                    if co and loyal_by_name(co.get("name", "")):
                        return "loyal"
                if not comps and loyal_by_name(ld.get("name", "")):
                    return "loyal"
                return "new"
            if ptype == "contact":
                ct = contacts_map.get(pid)
                if not ct:
                    return "unknown"
                comps = (ct.get("_embedded") or {}).get("companies") or []
                for co in comps:
                    inn = company_inn.get(co["id"])
                    if inn and inn_to_demands.get(inn, 0) >= 3:
                        return "loyal"
                    comp = companies_map.get(co["id"])
                    if comp and loyal_by_name(comp.get("name", "")):
                        return "loyal"
                if not comps and loyal_by_name(ct.get("name", "")):
                    return "loyal"
                return "new"
            if ptype == "company":
                inn = company_inn.get(pid)
                if inn and inn_to_demands.get(inn, 0) >= 3:
                    return "loyal"
                co = companies_map.get(pid)
                if co and loyal_by_name(co.get("name", "")):
                    return "loyal"
                return "new"
            return "unknown"

        # 8. Подсчёт
        by_manager: dict[str, float] = {}
        total_new = 0
        total_resolved = 0
        for mgr, evs in op_events.items():
            ctr: Counter = Counter()
            for e in evs:
                pid, ptype = resolve(e)
                klass = classify(pid, ptype) if pid else "unknown"
                ctr[klass] += 1
            denom = ctr["new"] + ctr["loyal"]
            pct = (100.0 * ctr["new"] / denom) if denom else 0.0
            by_manager[mgr] = round(pct, 1)
            total_new += ctr["new"]
            total_resolved += denom

        company_pct = (100.0 * total_new / total_resolved) if total_resolved else 0.0

        return {
            "computed_at": datetime.now(MSK).isoformat(),
            "window": {"start": start.isoformat(), "end": end.isoformat()},
            "company_pct": round(company_pct, 1),
            "by_manager": by_manager,
        }


async def _backfill_mtd() -> dict:
    """Расчёт MTD + запись снимка в БД бота. Используется cron-job и CLI."""
    from database import Database
    today = datetime.now(MSK).date()
    start = today.replace(day=1)
    logger.info(f"backfill: окно {start.isoformat()} .. {today.isoformat()}")
    result = await compute_new_client_share(start, today)
    db = Database()
    db.set_new_share_snapshot(result)
    logger.info(f"backfill OK: company={result['company_pct']}% "
                f"by_manager={result['by_manager']}")
    return result


def main():
    parser = argparse.ArgumentParser(
        description="F2B — расчёт % операций ОП на новых клиентах")
    parser.add_argument("--backfill", action="store_true",
                        help="записать снимок в БД (bot_settings.op_new_share_snapshot)")
    parser.add_argument("--start", help="YYYY-MM-DD (явное окно)")
    parser.add_argument("--end", help="YYYY-MM-DD (явное окно)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.start and args.end:
        s = date.fromisoformat(args.start)
        e = date.fromisoformat(args.end)
        result = asyncio.run(compute_new_client_share(s, e))
    else:
        today = datetime.now(MSK).date()
        result = asyncio.run(compute_new_client_share(today.replace(day=1), today))

    print(json.dumps(result, ensure_ascii=False, indent=2))

    if args.backfill:
        from database import Database
        Database().set_new_share_snapshot(result)
        print("✓ Snapshot saved to bot_settings.op_new_share_snapshot")


if __name__ == "__main__":
    main()
