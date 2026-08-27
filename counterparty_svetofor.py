"""
Светофор надёжности контрагента по ИНН (бот «Эф»).
План (второй мозг): plans/2026-07-29-svetofor-nadezhnosti-kontragenta.md — Фаза 2.

По ИНН считает цвет надёжности из показателей:
  №1 стоп-флаги ЕГРЮЛ  — DaData free (findById/party): статус, ликвидация, дисквалификация руководителя.
  №4 финансы           — ГИР БО ФНС (bo.nalog.gov.ru): выручка (2110), чистая прибыль (2400),
                          чистые активы / капитал (1300) по годам.
  №3 иски-ответчик     — kad.arbitr: бесплатного API нет → пока «не проверено» (заглушка, best-effort).
  №2 долги ФССП        — источник мёртв (api-ip.fssp.gov.ru отдаёт 410) → «не проверено», ручная досверка.
  №5 суды конкурентов  — таблица public.competitor_court_debtors: контрагенты, на которых НАВАФИШ/АНФИШ
                          подали иски о взыскании (kad.arbitr, собрано 26.08.2026). Особое предупреждение:
                          не платил конкуренту — не заплатит и нам, работать только по предоплате.

Правило цвета:
  🔴 red    — статус ЕГРЮЛ не ACTIVE (ликвидация/банкротство/реорг) ИЛИ руководитель дисквалифицирован
              ИЛИ чистые активы < 0.
  🟡 yellow — (не red) и: убыток/прибыль≈0 (маржа < 1%) ИЛИ падение выручки год-к-году ≥ 15%
              ИЛИ контрагент — ответчик по иску конкурента о взыскании долга.
  🟢 green  — всё чисто.
  ⚪ unknown — ЕГРЮЛ не отдал данные (нет ИНН/не найдено).

ВАЖНО (проверено live 2026-07-29): DaData free блок finance НЕ отдаёт → финансы только через ГИР БО.
Гос-источники (bo.nalog.gov.ru) отвечают только с РФ-IP → модуль работает на Amvera, не из dev-среды.
Секрет DADATA_TOKEN — в env Amvera.

Точная форма JSON ГИР БО из dev-среды не проверялась (гео-блок): парсер финотчётности
_parse_bfo написан по документированным путям, при первом прогоне на Amvera логирует сырой
ответ (logger.info FINANCE_RAW) — сверить и, если ключи иные, поправить _parse_bfo.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import aiohttp
import requests
import psycopg2
import psycopg2.extras

from database import CONNECT_TIMEOUT_SEC, STATEMENT_TIMEOUT_MS

logger = logging.getLogger(__name__)

# ── пороги правила (тюнятся) ──────────────────────────────────────────────────
REVENUE_DROP_YELLOW = 0.15   # падение выручки год-к-году ≥ 15% → жёлтый
MARGIN_ZERO_YELLOW = 0.01    # маржа чистой прибыли < 1% → «прибыль ≈ 0» → жёлтый

DADATA_PARTY_URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party"
BO_SEARCH_URL = "https://bo.nalog.gov.ru/advanced-search/organizations/search"
BO_BFO_URL = "https://bo.nalog.gov.ru/nbo/organizations/{org_id}/bfo/"
BO_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "ru,en;q=0.9",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=12, connect=6, sock_connect=6, sock_read=10)
PER_CHECK_TIMEOUT = 25  # hard-таймаут на одну проверку в батче (backstop против зависаний ГИР БО)

# ── DDL / DB (свой коннект, autocommit — как в supply_svetofor) ────────────────
DDL = """
create table if not exists public.counterparty_svetofor (
    inn        text primary key,
    name       text,
    color      text not null,
    flags      jsonb not null default '{}'::jsonb,
    checked_at timestamptz not null default now()
);
"""
DDL_DEBTORS = """
create table if not exists public.competitor_court_debtors (
    inn        text primary key,
    name       text,
    competitor text not null,
    case_no    text,
    case_date  date,
    added_at   timestamptz not null default now()
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
            cur.execute(DDL_DEBTORS)
    return _conn


def upsert_svetofor(inn: str, name: str | None, color: str, flags: dict):
    with _db().cursor() as cur:
        cur.execute("""
            insert into public.counterparty_svetofor (inn, name, color, flags, checked_at)
            values (%s, %s, %s, %s::jsonb, now())
            on conflict (inn) do update set
              name = excluded.name,
              color = excluded.color,
              flags = excluded.flags,
              checked_at = now()
        """, (inn, name, color, json.dumps(flags, ensure_ascii=False)))


def get_court_debtor(inn: str) -> dict | None:
    """Есть ли контрагент среди ответчиков по искам конкурентов (НАВАФИШ/АНФИШ).
    Источник — kad.arbitr, справочник public.competitor_court_debtors.
    Возвращает {competitor, case_no, case_date} или None. Сбой БД → None (мягкая деградация)."""
    inn = (inn or "").strip()
    if not inn:
        return None
    try:
        with _db().cursor() as cur:
            cur.execute("""select name, competitor, case_no, case_date
                             from public.competitor_court_debtors where inn = %s""", (inn,))
            return cur.fetchone()
    except Exception as e:
        logger.warning("competitor_court_debtors: чтение %s → %s", inn, e)
        return None


_EGRUL_STATUS_RU = {
    "LIQUIDATING": "в стадии ликвидации",
    "LIQUIDATED": "ликвидирована",
    "BANKRUPT": "банкротство",
    "REORGANIZING": "реорганизация",
}


def _lawsuit_warning(flags: dict) -> str:
    """Особое предупреждение: контрагент – ответчик по иску конкурента о взыскании долга.

    Отдельной строкой, потому что смысл другой, чем у цвета: не «нельзя работать»,
    а «только по предоплате». Источник – kad.arbitr, справочник competitor_court_debtors.

    Ни конкурента, ни номер дела, ни вывод про предоплату в алерт не выводим
    (решение собственника 27.08.2026): в согласовании нужен сам факт, детали –
    в справочнике. Флаг competitor_lawsuit при этом хранит их целиком.
    """
    if not flags.get("competitor_lawsuit"):
        return ""
    return "\n⛔ *Судится с конкурентом*"


def format_reliability_line(res: dict | None) -> tuple[str, str]:
    """Короткая строка «Надёжность» для алертов согласования (заказ/договор).
    Возвращает (color, markdown-строка). color ∈ green|yellow|red|unknown.
    При наличии иска конкурента добавляет отдельную строку-предупреждение."""
    icon_map = {"green": "🟢", "yellow": "🟡", "red": "🔴", "unknown": "⚪"}
    if not res or res.get("color") == "unknown":
        return "unknown", "⚪ *Надёжность:* не проверено (нет ИНН / источник недоступен)"
    color = res.get("color", "unknown")
    flags = res.get("flags") or {}
    icon = icon_map.get(color, "⚪")
    warn = _lawsuit_warning(flags)
    if color == "red":
        reasons = []
        for r in (flags.get("red_reasons") or []):
            # «статус ЕГРЮЛ: LIQUIDATING» → человеческое
            if r.startswith("статус ЕГРЮЛ:"):
                code = r.split(":", 1)[1].strip()
                reasons.append(_EGRUL_STATUS_RU.get(code, code))
            else:
                reasons.append(r)
        return color, f"{icon} *Надёжность:* " + "; ".join(reasons[:2] or ["стоп-флаг ЕГРЮЛ"]) + warn
    if color == "yellow":
        prefix = "ЕГРЮЛ действующая, но " if flags.get("egrul_status") == "ACTIVE" else ""
        ys = flags.get("yellow_reasons") or ["есть тревожные признаки"]
        return color, f"{icon} *Надёжность:* {prefix}" + "; ".join(ys[:2]) + warn
    return color, f"{icon} *Надёжность:* ЕГРЮЛ действующая, финансы ОК" + warn


def _bulk_upsert(rows: list):
    """Пишет пачку (inn, name, color, flags) ОДНИМ свежим коннектом (не общий _conn) —
    чтобы массовая запись в конце батча не зависела от состояния общего соединения."""
    if not rows:
        return
    # Батчевая запись длиннее обычной — statement_timeout шире, но не безлимитный.
    conn = psycopg2.connect(os.environ["DATABASE_URL"],
                            connect_timeout=CONNECT_TIMEOUT_SEC,
                            options="-c statement_timeout=120000")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
            for inn, name, color, flags in rows:
                cur.execute("""
                    insert into public.counterparty_svetofor (inn, name, color, flags, checked_at)
                    values (%s, %s, %s, %s::jsonb, now())
                    on conflict (inn) do update set
                      name = excluded.name, color = excluded.color,
                      flags = excluded.flags, checked_at = now()
                """, (inn, name, color, json.dumps(flags, ensure_ascii=False)))
    finally:
        conn.close()


def get_svetofor(inn: str) -> dict | None:
    with _db().cursor() as cur:
        cur.execute("select inn, name, color, flags, checked_at "
                    "from public.counterparty_svetofor where inn = %s", (inn,))
        row = cur.fetchone()
        return dict(row) if row else None


# ── №1 DaData: статус ЕГРЮЛ + дисквалификация руководителя ─────────────────────
async def _dadata_party(session: aiohttp.ClientSession, inn: str) -> dict | None:
    token = os.getenv("DADATA_TOKEN")
    if not token:
        logger.error("counterparty_svetofor: DADATA_TOKEN не задан")
        return None
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {token}",
    }
    try:
        async with session.post(DADATA_PARTY_URL, json={"query": inn},
                                headers=headers, timeout=HTTP_TIMEOUT) as r:
            if r.status != 200:
                logger.warning("DaData %s → HTTP %s", inn, r.status)
                return None
            data = await r.json()
    except Exception as e:
        logger.warning("DaData %s → %s", inn, e)
        return None
    sugg = (data.get("suggestions") or [])
    if not sugg:
        return None
    d = sugg[0].get("data") or {}
    state = d.get("state") or {}
    mgmt = d.get("management") or {}
    return {
        "name": sugg[0].get("value"),
        "status": state.get("status"),                 # ACTIVE / LIQUIDATING / LIQUIDATED / BANKRUPT / REORGANIZING
        "liquidation_date": state.get("liquidation_date"),
        "disqualified": bool(mgmt.get("disqualified")),
        "manager": mgmt.get("name"),
        "address": (d.get("address") or {}).get("value"),
    }


# ── №4 ГИР БО: финансовая отчётность ──────────────────────────────────────────
async def _bo_org_id(session: aiohttp.ClientSession, inn: str) -> str | None:
    try:
        async with session.get(BO_SEARCH_URL, params={"query": inn, "page": "0"},
                               headers=BO_HEADERS, timeout=HTTP_TIMEOUT) as r:
            if r.status != 200:
                logger.warning("ГИР БО search %s → HTTP %s", inn, r.status)
                return None
            data = await r.json()
    except Exception as e:
        logger.warning("ГИР БО search %s → %s", inn, e)
        return None
    content = data.get("content") if isinstance(data, dict) else data
    if not content:
        return None
    return str(content[0].get("id"))


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _num_rub(v):
    """ГИР БО отдаёт суммы в ТЫСЯЧАХ рублей (проверено live: gainSum=148393 = 148.4 млн ₽).
    Приводим к рублям."""
    n = _num(v)
    return n * 1000 if n is not None else None


def _parse_bfo(reports: list) -> dict:
    """Из списка годовых отчётов ГИР БО достаёт выручку (2110), чистую прибыль (2400),
    капитал/чистые активы (1300) за последние 2 года.
    Форма ответа сверяется на Amvera (см. FINANCE_RAW в логе) — при иных ключах поправить здесь."""
    years = []
    for rep in reports or []:
        period = rep.get("period") or rep.get("year")
        corr = None
        tc = rep.get("typeCorrections") or []
        if tc:
            corr = (tc[0] or {}).get("correction") or {}
        corr = corr or rep.get("correction") or rep
        fin = corr.get("financialResult") or {}
        bal = corr.get("balance") or {}
        # Fallback на топ-левел gainSum (выручка), если financialResult пуст.
        revenue = _num_rub(fin.get("current2110"))
        if revenue is None:
            revenue = _num_rub(rep.get("gainSum"))
        years.append({
            "year": period,
            "revenue": revenue,
            "profit": _num_rub(fin.get("current2400")),
            "equity": _num_rub(bal.get("current1300")),
        })
    years = [y for y in years if y["year"] is not None]
    years.sort(key=lambda y: y["year"], reverse=True)
    latest = years[0] if years else {}
    prev = years[1] if len(years) > 1 else {}
    return {
        "year": latest.get("year"),
        "revenue": latest.get("revenue"),
        "revenue_prev": prev.get("revenue"),
        "profit": latest.get("profit"),
        "equity": latest.get("equity"),
    }


async def _bo_finance(session: aiohttp.ClientSession, inn: str) -> dict | None:
    org_id = await _bo_org_id(session, inn)
    if not org_id:
        return None
    try:
        async with session.get(BO_BFO_URL.format(org_id=org_id),
                               headers=BO_HEADERS, timeout=HTTP_TIMEOUT) as r:
            if r.status != 200:
                logger.warning("ГИР БО bfo %s → HTTP %s", inn, r.status)
                return None
            reports = await r.json()
    except Exception as e:
        logger.warning("ГИР БО bfo %s → %s", inn, e)
        return None
    logger.info("FINANCE_RAW inn=%s org_id=%s body=%s", inn, org_id,
                json.dumps(reports, ensure_ascii=False)[:2000])
    return _parse_bfo(reports if isinstance(reports, list) else reports.get("content", []))


# ── расчёт цвета ──────────────────────────────────────────────────────────────
def _compute_color(egrul: dict | None, finance: dict | None,
                   debtor: dict | None = None) -> tuple[str, dict]:
    flags: dict = {}
    if not egrul:
        return "unknown", {"egrul": "не найдено"}

    status = (egrul.get("status") or "").upper()
    flags["egrul_status"] = status or None
    flags["disqualified"] = egrul.get("disqualified")

    red = []
    if status and status != "ACTIVE":
        red.append(f"статус ЕГРЮЛ: {status}")
    if egrul.get("disqualified"):
        red.append("руководитель дисквалифицирован")

    yellow = []
    if finance:
        flags["finance"] = {
            "year": finance.get("year"),
            "revenue": finance.get("revenue"),
            "revenue_prev": finance.get("revenue_prev"),
            "profit": finance.get("profit"),
            "equity": finance.get("equity"),
        }
        equity = finance.get("equity")
        if equity is not None and equity < 0:
            red.append("отрицательные чистые активы")
        profit, revenue = finance.get("profit"), finance.get("revenue")
        if profit is not None and revenue and revenue > 0:
            if profit <= 0:
                yellow.append("убыток")
            elif profit / revenue < MARGIN_ZERO_YELLOW:
                yellow.append("прибыль ≈ 0 (маржа < 1%)")
        rev_prev = finance.get("revenue_prev")
        if revenue is not None and rev_prev and rev_prev > 0:
            drop = (rev_prev - revenue) / rev_prev
            if drop >= REVENUE_DROP_YELLOW:
                yellow.append(f"падение выручки {round(drop * 100)}%")
    else:
        flags["finance"] = "не проверено"

    # №2/№3 — вне бесплатной автоматизации
    flags["fssp"] = "не проверено (источник недоступен)"
    flags["arbitrage_defendant"] = "не проверено"

    # №5 — иск конкурента о взыскании: не платил им, не заплатит и нам
    if debtor:
        case = (debtor.get("case_no") or "").strip()
        dt = debtor.get("case_date")
        flags["competitor_lawsuit"] = {
            "competitor": debtor.get("competitor"),
            "case_no": case or None,
            "case_date": dt.isoformat() if hasattr(dt, "isoformat") else dt,
        }
        tail = f" ({case} от {dt:%d.%m.%Y})" if case and hasattr(dt, "strftime") else ""
        yellow.append(f"иск от конкурента {debtor.get('competitor')} о взыскании долга{tail}")

    if red:
        flags["red_reasons"] = red
        return "red", flags
    if yellow:
        flags["yellow_reasons"] = yellow
        return "yellow", flags
    return "green", flags


def _dadata_party_sync(inn: str) -> dict | None:
    """Синхронная версия DaData (requests) — для батча через asyncio.to_thread.
    aiohttp-сессия в фоновой create_task-задаче PTB залипает намертво (ни таймаут aiohttp,
    ни wait_for не срабатывают), а requests с timeout отрабатывает надёжно в отдельном потоке."""
    token = os.getenv("DADATA_TOKEN")
    if not token:
        return None
    try:
        r = requests.post(
            DADATA_PARTY_URL, json={"query": inn},
            headers={"Content-Type": "application/json", "Accept": "application/json",
                     "Authorization": f"Token {token}"},
            timeout=12)
        if r.status_code != 200:
            logger.warning("DaData(sync) %s → HTTP %s", inn, r.status_code)
            return None
        data = r.json()
    except Exception as e:
        logger.warning("DaData(sync) %s → %s", inn, e)
        return None
    sugg = data.get("suggestions") or []
    if not sugg:
        return None
    d = sugg[0].get("data") or {}
    state = d.get("state") or {}
    mgmt = d.get("management") or {}
    return {
        "name": sugg[0].get("value"),
        "status": state.get("status"),
        "liquidation_date": state.get("liquidation_date"),
        "disqualified": bool(mgmt.get("disqualified")),
        "manager": mgmt.get("name"),
        "address": (d.get("address") or {}).get("value"),
    }


def check_counterparty_sync(inn: str) -> dict:
    """Синхронная проверка для батча: только DaData (стоп-флаги ЕГРЮЛ), без финансов.
    Запускать через asyncio.to_thread — не блокирует event loop, requests-таймаут надёжен."""
    inn = (inn or "").strip()
    egrul = _dadata_party_sync(inn)
    color, flags = _compute_color(egrul, None)
    name = egrul.get("name") if egrul else None
    return {"inn": inn, "name": name, "color": color, "flags": flags}


async def fetch_shipped_counterparties(months: int = 3) -> tuple[list[dict], list[str]]:
    """Контрагенты, которым были отгрузки (demand) за последние `months` месяцев.
    Дедуп по ИНН. Возвращает (список {inn, name}, список имён без ИНН)."""
    from moysklad import MS_BASE, get_headers
    from datetime import datetime, timedelta
    since = (datetime.now() - timedelta(days=months * 31)).strftime("%Y-%m-%d 00:00:00")
    url = f"{MS_BASE}/entity/demand"
    by_inn: dict[str, str] = {}
    no_inn: dict[str, int] = {}
    offset = 0
    async with aiohttp.ClientSession() as session:
        while True:
            params = {
                "limit": 100, "offset": offset, "expand": "agent",
                "filter": f"moment>={since}", "order": "moment,asc",
            }
            try:
                async with session.get(url, headers=get_headers(), params=params,
                                       timeout=HTTP_TIMEOUT) as r:
                    if r.status != 200:
                        logger.error("demand fetch %s: %s", r.status, (await r.text())[:200])
                        break
                    data = await r.json()
            except Exception as e:
                logger.error("demand fetch → %s", e)
                break
            rows = data.get("rows", [])
            for d in rows:
                ag = d.get("agent") or {}
                name = (ag.get("name") or "").strip()
                if not name or "розничный покупатель" in name.lower():
                    continue
                inn = (ag.get("inn") or "").strip()
                if inn:
                    by_inn.setdefault(inn, name)
                else:
                    no_inn[name] = no_inn.get(name, 0) + 1
            if len(rows) < 100:
                break
            offset += 100
    counterparties = [{"inn": inn, "name": name} for inn, name in by_inn.items()]
    return counterparties, list(no_inn.keys())


async def run_batch(months: int = 3) -> dict:
    """Прогоняет светофор по всем контрагентам с отгрузкой за `months` мес.
    ПОСЛЕДОВАТЕЛЬНО (массовый asyncio.gather зависал наглухо), save=False в цикле —
    БД пишем одним bulk в конце через СВЕЖИЙ коннект (не общий _conn). Возвращает
    сводку по цветам + список «не проверено» (без ИНН)."""
    counterparties, no_inn = await fetch_shipped_counterparties(months)
    total = len(counterparties)
    logger.info("run_batch: контрагентов к проверке %s (без ИНН %s)", total, len(no_inn))
    summary = {"checked": 0, "green": 0, "yellow": 0, "red": 0, "unknown": 0,
               "reds": [], "yellows": [], "no_inn": no_inn}
    rows = []  # (inn, name, color, flags) — для bulk-записи в конце
    logger.info("DIAG старт цикла по %s", total)
    for i, cp in enumerate(counterparties, 1):
        inn = cp["inn"]
        try:
            res = await asyncio.wait_for(
                asyncio.to_thread(check_counterparty_sync, inn),
                timeout=PER_CHECK_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning("run_batch %s → таймаут проверки", inn)
            res = None
        except Exception as e:
            logger.error("run_batch %s → %s", inn, e)
            res = None
        if res:
            color = res.get("color", "unknown")
            summary["checked"] += 1
            summary[color] = summary.get(color, 0) + 1
            label = f"{res.get('name') or cp['name']} (ИНН {inn})"
            if color == "red":
                summary["reds"].append(label)
            elif color == "yellow":
                summary["yellows"].append(label)
            rows.append((inn, res.get("name"), color, res.get("flags") or {}))
        if i % 25 == 0:
            logger.info("run_batch: %s/%s", i, total)
    try:
        _bulk_upsert(rows)
    except Exception as e:
        logger.error("run_batch bulk upsert → %s", e)
    logger.info("run_batch: готово %s", {k: summary[k] for k in ('checked','green','yellow','red','unknown')})
    return summary


async def weekly_batch_job(app=None, db=None):
    """PTB/APScheduler-джоба: недельный прогон светофора по всей активной базе,
    сводка собственнику (OWNER_CHAT_ID)."""
    logger.info("counterparty_svetofor: старт недельного батча")
    summary = await run_batch()
    owner = (os.getenv("OWNER_CHAT_ID") or "").strip()
    if not (app and owner.lstrip("-").isdigit()):
        logger.info("counterparty_svetofor батч готов: %s", summary)
        return summary
    lines = [
        "🚦 Светофор контрагентов (отгрузки за 3 мес)",
        f"Проверено: {summary['checked']}  🟢 {summary['green']}  🟡 {summary['yellow']}  🔴 {summary['red']}  ⚪ {summary['unknown']}",
    ]
    if summary["reds"]:
        lines.append("\n🔴 Красные:")
        lines += [f"• {x}" for x in summary["reds"][:15]]
    if summary["no_inn"]:
        lines.append(f"\n⚪ Без ИНН (не проверено): {len(summary['no_inn'])}")
        lines += [f"• {x}" for x in summary["no_inn"][:15]]
    try:
        await app.bot.send_message(chat_id=int(owner), text="\n".join(lines))
    except Exception as e:
        logger.error("counterparty_svetofor: не отправил сводку → %s", e)
    return summary


async def check_counterparty(inn: str, save: bool = True, with_finance: bool = True) -> dict:
    """Считает светофор по ИНН, при save=True кладёт/обновляет в БД. Возвращает
    {inn, name, color, flags, checked_at}.

    with_finance=False — пропустить ГИР БО (для батча: гос-сайт тарпитит параллельные
    запросы и подвешивает воркеры; в массовом прогоне берём только стоп-флаги ЕГРЮЛ из
    DaData, финансы доступны в одиночном /svetofor)."""
    inn = (inn or "").strip()
    async with aiohttp.ClientSession() as session:
        egrul = await _dadata_party(session, inn)
        logger.info("DIAG check DaData ok %s egrul=%s", inn, bool(egrul))
        finance = await _bo_finance(session, inn) if (egrul and with_finance) else None
    debtor = get_court_debtor(inn)
    color, flags = _compute_color(egrul, finance, debtor)
    name = egrul.get("name") if egrul else None
    if save:
        try:
            upsert_svetofor(inn, name, color, flags)
        except Exception as e:
            logger.error("counterparty_svetofor: upsert %s → %s", inn, e)
    return {
        "inn": inn,
        "name": name,
        "color": color,
        "flags": flags,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
