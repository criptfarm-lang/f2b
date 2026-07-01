"""
Сверка «Наш ас-т».

По нажатиям кнопки «🐟 Наш ас-т» в карточке запроса по номенклатуре
(status='our_assortment' в procurement.assortment_requests) проверяем:
отгрузили ли этому клиенту запрошенную позицию ПОСЛЕ нажатия.

Пайплайн:
  имя клиента (contact_name) → amoCRM компания по имени → ИНН
  → отгрузки МС этого ИНН за окно [дата нажатия … конец периода]
  → матч по species → shipped / not shipped.

Результат складывается в procurement.assortment_hit_results (upsert).
Считает бот (у него есть клиенты amoCRM/МС); вкладка в дашборде закупок
только читает эту таблицу.

План: plans/2026-07-01-кнопка-наш-ас-т-запрос-номенклатуры.md (Фаза B).
"""

import re
import logging
from datetime import date
from typing import Optional

import aiohttp

import amocrm
from moysklad import MS_BASE, get_headers as ms_headers

logger = logging.getLogger(__name__)


# ─── amoCRM: имя клиента → ИНН ────────────────────────────────────────────────

def _parse_inn(company: dict) -> Optional[str]:
    """Достаёт ИНН из custom_fields_values компании amoCRM."""
    for f in (company.get("custom_fields_values") or []):
        code = f.get("field_code") or ""
        name_l = (f.get("field_name") or "").lower()
        if code == "INN" or "инн" in name_l:
            for v in (f.get("values") or []):
                digits = re.sub(r"\D", "", str(v.get("value") or ""))
                if digits:
                    return digits
    return None


async def resolve_client(name: str) -> dict:
    """
    Имя клиента → {inn, company_name, confidence}.
    confidence:
      matched   — компания найдена по названию + ИНН достан
      low       — ИНН достан через контакт → его компанию (нечёткий путь)
      unmatched — ИНН не найден, отгрузку проверить нельзя
    """
    empty = {"inn": None, "company_name": None, "confidence": "unmatched"}
    if not name or name.strip() in ("", "?"):
        return empty

    # 1) Прямой поиск компании по имени
    companies = await amocrm.find_company_by_name(name)
    for co in companies:
        inn = _parse_inn(co)
        if inn:
            return {"inn": inn, "company_name": co.get("name"), "confidence": "matched"}

    # 2) Через контакт → его компанию
    contacts = await amocrm.find_contact_by_name(name)
    for ct in contacts:
        emb = ct.get("_embedded") or {}
        for co_ref in (emb.get("companies") or []):
            co_id = co_ref.get("id")
            if not co_id:
                continue
            co = await amocrm.amo_get(f"/companies/{co_id}")
            if co:
                inn = _parse_inn(co)
                if inn:
                    return {"inn": inn, "company_name": co.get("name"),
                            "confidence": "low"}

    return {"inn": None,
            "company_name": (companies[0].get("name") if companies else None),
            "confidence": "unmatched"}


# ─── МойСклад: отгрузки по ИНН за окно ────────────────────────────────────────

async def fetch_positions_by_inn(target_inns: set, date_from: str, date_to: str) -> tuple:
    """
    Возвращает (positions_by_inn, name_by_inn):
      positions_by_inn = {inn: [ {date, name, qty, sum_rub}, ... ]}
      name_by_inn      = {inn: имя контрагента МС}
    Только по контрагентам, чей ИНН есть в target_inns.
    """
    positions_by_inn = {inn: [] for inn in target_inns}
    name_by_inn = {}
    if not target_inns:
        return positions_by_inn, name_by_inn

    async with aiohttp.ClientSession() as session:
        offset = 0
        while True:
            params = {
                "filter": f"moment>={date_from} 00:00:00;moment<={date_to} 23:59:59",
                "expand": "agent,positions.assortment",
                "limit": 100,
                "offset": offset,
            }
            async with session.get(
                f"{MS_BASE}/entity/demand", headers=ms_headers(), params=params
            ) as r:
                if r.status != 200:
                    body = await r.text()
                    logger.error(f"assortment_hits: demand fetch {r.status} {body[:200]}")
                    break
                data = await r.json()

            rows = data.get("rows", [])
            for d in rows:
                agent = d.get("agent") or {}
                inn = re.sub(r"\D", "", str(agent.get("inn") or ""))
                if not inn or inn not in target_inns:
                    continue
                name_by_inn.setdefault(inn, agent.get("name") or "")
                moment = (d.get("moment") or "")[:10]
                pos_rows = ((d.get("positions") or {}).get("rows")) or []
                for p in pos_rows:
                    assort = p.get("assortment") or {}
                    positions_by_inn[inn].append({
                        "date": moment,
                        "name": assort.get("name") or "",
                        "qty": p.get("quantity") or 0,
                        "sum_rub": (p.get("sum") or 0) / 100,
                    })
            if len(rows) < 100:
                break
            offset += 100

    return positions_by_inn, name_by_inn


# ─── Матч species ↔ позиция отгрузки ──────────────────────────────────────────

def _species_matches(species: Optional[str], position_name: str) -> bool:
    """
    Грубый матч по species_normalized: ядро (первое значимое слово ≥4 букв)
    как подстрока в названии позиции. MVP — при спорных случаях добиваем глазами
    (в отчёте видна позиция и дата отгрузки).
    """
    if not species:
        return False
    pn = position_name.lower()
    for token in re.split(r"[\s,()]+", species.lower()):
        token = token.strip()
        if len(token) >= 4 and token in pn:
            return True
    return False


# ─── Основной расчёт ──────────────────────────────────────────────────────────

async def compute_assortment_hits(db, period_from: date, period_to: date) -> list:
    """
    Считает и сохраняет результаты сверки за окно [period_from, period_to].
    Возвращает список dict'ов (для печати в /assortment_hits и вкладки).
    """
    rows = db._fetchall(
        """SELECT id, contact_name, species_normalized, sku_or_description,
                  status_changed_at
           FROM procurement.assortment_requests
           WHERE status = 'our_assortment'
             AND status_changed_at::date >= %s
             AND status_changed_at::date <= %s
           ORDER BY status_changed_at""",
        (period_from, period_to),
    )
    if not rows:
        return []

    # 1) Резолвим ИНН по каждому уникальному имени клиента
    resolve_cache = {}
    for r in rows:
        name = r["contact_name"]
        if name not in resolve_cache:
            resolve_cache[name] = await resolve_client(name)

    # 2) Тянем отгрузки для всех целевых ИНН одним окном
    target_inns = {resolve_cache[r["contact_name"]]["inn"]
                   for r in rows if resolve_cache[r["contact_name"]]["inn"]}
    window_from = min((r["status_changed_at"].date() for r in rows), default=period_from)
    positions_by_inn, name_by_inn = await fetch_positions_by_inn(
        target_inns, window_from.isoformat(), period_to.isoformat()
    )

    # 3) По каждому запросу — отгрузили ли species после даты нажатия
    results = []
    for r in rows:
        res = resolve_cache[r["contact_name"]]
        inn = res["inn"]
        clicked = r["status_changed_at"]
        clicked_iso = clicked.date().isoformat()

        shipped = False
        qty = 0.0
        ssum = 0.0
        first_date = None
        if inn:
            for pos in positions_by_inn.get(inn, []):
                if pos["date"] < clicked_iso:
                    continue
                if _species_matches(r["species_normalized"], pos["name"]):
                    shipped = True
                    qty += float(pos["qty"] or 0)
                    ssum += float(pos["sum_rub"] or 0)
                    if first_date is None or pos["date"] < first_date:
                        first_date = pos["date"]

        ms_name = name_by_inn.get(inn) if inn else None
        db._execute(
            """INSERT INTO procurement.assortment_hit_results
               (assortment_request_id, contact_name, species_normalized,
                sku_or_description, clicked_at, amo_company_name, inn,
                ms_counterparty, match_confidence, shipped, shipped_qty,
                shipped_sum, first_shipment_date, period_from, period_to, computed_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
               ON CONFLICT (assortment_request_id, period_from, period_to)
               DO UPDATE SET
                   contact_name=EXCLUDED.contact_name,
                   species_normalized=EXCLUDED.species_normalized,
                   sku_or_description=EXCLUDED.sku_or_description,
                   clicked_at=EXCLUDED.clicked_at,
                   amo_company_name=EXCLUDED.amo_company_name,
                   inn=EXCLUDED.inn,
                   ms_counterparty=EXCLUDED.ms_counterparty,
                   match_confidence=EXCLUDED.match_confidence,
                   shipped=EXCLUDED.shipped,
                   shipped_qty=EXCLUDED.shipped_qty,
                   shipped_sum=EXCLUDED.shipped_sum,
                   first_shipment_date=EXCLUDED.first_shipment_date,
                   computed_at=NOW()""",
            (r["id"], r["contact_name"], r["species_normalized"],
             r["sku_or_description"], clicked, res["company_name"], inn,
             ms_name, res["confidence"], shipped, qty or None, ssum or None,
             first_date, period_from, period_to),
        )

        results.append({
            "contact_name": r["contact_name"],
            "species_normalized": r["species_normalized"],
            "sku_or_description": r["sku_or_description"],
            "clicked_at": clicked,
            "amo_company_name": res["company_name"],
            "inn": inn,
            "ms_counterparty": ms_name,
            "match_confidence": res["confidence"],
            "shipped": shipped,
            "shipped_qty": qty,
            "shipped_sum": ssum,
            "first_shipment_date": first_date,
        })

    return results


def format_hits_report(results: list, period_from: date, period_to: date) -> str:
    """Текстовый отчёт для команды /assortment_hits в TG."""
    if not results:
        return (f"🔎 *Контроль* за {period_from:%d.%m}–{period_to:%d.%m}\n\n"
                "Нет нажатий кнопки «Контроль» за период.")

    shipped = [r for r in results if r["shipped"]]
    not_shipped = [r for r in results if not r["shipped"]
                   and r["match_confidence"] != "unmatched"]
    unmatched = [r for r in results if r["match_confidence"] == "unmatched"]

    lines = [f"🔎 *Контроль* за {period_from:%d.%m}–{period_to:%d.%m}",
             f"Всего: {len(results)} · отгрузили: {len(shipped)} · "
             f"не отгрузили: {len(not_shipped)} · не сматчено: {len(unmatched)}", ""]

    def _rub(v):
        return f"{v:,.0f}".replace(",", " ")

    def _row(r, mark):
        who = r["contact_name"] or "?"
        sp = r["species_normalized"] or r["sku_or_description"] or "?"
        d = r["clicked_at"].strftime("%d.%m")
        s = f"{mark} {who} — {sp} (нажал {d})"
        if r["shipped"]:
            s += (f"\n     отгрузили {r['shipped_qty']:.0f} на "
                  f"{_rub(r['shipped_sum'])} ₽ с {r['first_shipment_date']}")
        return s

    if shipped:
        lines.append("*✅ Отгрузили:*")
        lines += [_row(r, "✅") for r in shipped]
        lines.append("")
    if not_shipped:
        lines.append("*❌ Не отгрузили:*")
        lines += [_row(r, "❌") for r in not_shipped]
        lines.append("")
    if unmatched:
        lines.append("*❓ Не сматчено (нет ИНН в amoCRM):*")
        lines += [_row(r, "❓") for r in unmatched]

    return "\n".join(lines)
