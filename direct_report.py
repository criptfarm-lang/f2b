"""F2B — сводка по рекламным кампаниям Я.Директ для бота «Эф».

Canonical-расчёт еженедельной сводки. Перенесён из repo «второй мозг»
(`scripts/yandex_direct/{api,balance,weekly_compare,site_leads_real,full_report}.py`)
по плану `plans/2026-08-10-еженедельная-сводка-директа-в-боте.md`.
Локальные скрипты остаются для ручного глубокого разбора; при правках
алгоритма — сначала здесь, затем синхронизировать со «вторым мозгом».

Отличия от локальной версии:
- токен Я.Директ из env `YANDEX_DIRECT_TOKEN` (в macOS Keychain лезем только
  при локальном CLI-прогоне), amoCRM — из `AMO_ACCESS_TOKEN` как весь бот;
- всё сетевое на aiohttp, чтобы не блокировать event loop бота;
- на выходе не markdown-файл, а plain-text для Telegram.

CLI (локальная сверка с full_report.py):
    python direct_report.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiohttp

logger = logging.getLogger(__name__)

MSK = ZoneInfo("Europe/Moscow")

# ─── Константы Я.Директ ───────────────────────────────────────────────────────
# Performance-кампании. Синхронизировать с SKILL.md скилла f2b-direct-analyze.
CAMPAIGN_IDS = ["710295507", "710295591"]
CAMPAIGN_NAMES = {"710295507": "ОПТ", "710295591": "ХоРеКа"}
API_REPORTS = "https://api.direct.yandex.com/json/v5/reports"
# Баланс отдаёт только Live v4 (метод AccountManagement) — в JSON API v5
# такого сервиса нет. Проверено на живом аккаунте dr-fishwork 2026-08-10:
# Amount совпал с балансом в веб-интерфейсе до копейки.
API_LIVE_V4 = "https://api.direct.yandex.ru/live/v4/json/"
PLAN_CPA_RUB = 1000

# ─── Константы amoCRM ─────────────────────────────────────────────────────────
AMO_SUBDOMAIN = os.getenv("AMO_SUBDOMAIN", "victorfishtobiz")
AMO_BASE = f"https://{AMO_SUBDOMAIN}.amocrm.ru/api/v4"
PIPELINE_ATTRACT = 10873622
TAG_IDS = {782551: "сайт", 782507: "сайт заявка"}
STATUS_WON, STATUS_LOST = 142, 143
LEADS_WINDOW_DAYS = 13
RATE_DELAY = 0.2

# Порог простоя лида до пинга менеджера, часы.
IDLE_FIRST_CONTACT_H = 2
IDLE_OTHER_H = 48

TELEGRAM_LIMIT = 4000


class DirectError(RuntimeError):
    """Ошибка, которую нужно показать собственнику текстом, а не спрятать в лог."""


def get_direct_token() -> str:
    """Токен Я.Директ: env на проде, Keychain — при локальном прогоне."""
    token = os.getenv("YANDEX_DIRECT_TOKEN", "").strip()
    if token:
        return token
    try:
        return subprocess.check_output(
            ["security", "find-generic-password", "-w",
             "-s", "f2b-yandex-direct-token", "-a", "viktorvasilev"]
        ).decode().strip()
    except Exception:
        raise DirectError(
            "Нет токена Я.Директ: переменная YANDEX_DIRECT_TOKEN не задана.")


# ─── Отчёты Я.Директ ──────────────────────────────────────────────────────────

async def fetch_report_tsv(session: aiohttp.ClientSession, date_from: str,
                           date_to: str, field_names: list[str],
                           report_name: str) -> str:
    """Забирает CAMPAIGN_PERFORMANCE_REPORT в TSV.

    Директ готовит отчёт асинхронно: 201/202 значит «ещё считается», надо
    повторить тот же запрос. Ждём максимум ~100 с, дальше это уже не «долго»,
    а сломалось.
    """
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to,
                "Filter": [
                    {"Field": "CampaignId", "Operator": "IN",
                     "Values": CAMPAIGN_IDS},
                ],
            },
            "FieldNames": field_names,
            "ReportName": f"{report_name}_{int(time.time())}",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO",
        }
    }
    headers = {
        "Authorization": f"Bearer {get_direct_token()}",
        "Accept-Language": "ru",
        "Content-Type": "application/json; charset=utf-8",
        "processingMode": "auto",
        "returnMoneyInMicros": "false",
        "skipReportHeader": "true",
        "skipColumnHeader": "false",
        "skipReportSummary": "true",
    }
    payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
    timeout = aiohttp.ClientTimeout(total=120)

    for _ in range(20):
        async with session.post(API_REPORTS, data=payload, headers=headers,
                                timeout=timeout) as resp:
            if resp.status == 200:
                return await resp.text()
            if resp.status in (201, 202):
                await asyncio.sleep(5)
                continue
            text = await resp.text()
            if resp.status in (401, 403):
                raise DirectError(
                    "Я.Директ не принял токен (HTTP %s). Нужен свежий "
                    "YANDEX_DIRECT_TOKEN." % resp.status)
            raise DirectError(f"Я.Директ HTTP {resp.status}: {text[:200]}")
    raise DirectError("Я.Директ не собрал отчёт за 100 секунд.")


def aggregate(tsv: str) -> dict[str, dict]:
    """TSV → {campaign_id: {imp, clk, cost, conv}}."""
    out: dict[str, dict] = defaultdict(
        lambda: {"imp": 0, "clk": 0, "cost": 0.0, "conv": 0})
    lines = tsv.splitlines()
    if not lines:
        return {}
    header = lines[0].split("\t")
    try:
        idx = {name: header.index(name) for name in
               ("CampaignId", "Impressions", "Clicks", "Cost", "Conversions")}
    except ValueError:
        return {}
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) <= max(idx.values()):
            continue
        cid = parts[idx["CampaignId"]]
        try:
            out[cid]["imp"] += int(parts[idx["Impressions"]])
            out[cid]["clk"] += int(parts[idx["Clicks"]])
            out[cid]["cost"] += float(parts[idx["Cost"]])
            conv = parts[idx["Conversions"]]
            out[cid]["conv"] += int(conv) if conv not in ("--", "") else 0
        except (ValueError, IndexError):
            continue
    return dict(out)


def derive(metrics: dict) -> dict:
    imp, clk, cost, conv = (metrics["imp"], metrics["clk"],
                            metrics["cost"], metrics["conv"])
    return {
        **metrics,
        "ctr": (100 * clk / imp) if imp else 0,
        "avg_cpc": (cost / clk) if clk else 0,
        "cpa": (cost / conv) if conv else None,
    }


def delta(cur: float, prev: float) -> str:
    if prev == 0:
        return "+∞" if cur > 0 else "0"
    pct = 100 * (cur - prev) / prev
    return f"{'+' if pct >= 0 else ''}{pct:.0f}%"


async def weekly_compare(session: aiohttp.ClientSession, today: date) -> dict:
    """Последние 7 дней (без сегодня) против предыдущих 7."""
    cur_to = today - timedelta(days=1)
    cur_from = today - timedelta(days=7)
    prev_to = cur_from - timedelta(days=1)
    prev_from = cur_from - timedelta(days=7)

    fields = ["Date", "CampaignId", "Impressions", "Clicks", "Cost",
              "Conversions"]
    cur_tsv = await fetch_report_tsv(session, cur_from.isoformat(),
                                     cur_to.isoformat(), fields, "wc_cur")
    prev_tsv = await fetch_report_tsv(session, prev_from.isoformat(),
                                      prev_to.isoformat(), fields, "wc_prev")

    cur, prev = aggregate(cur_tsv), aggregate(prev_tsv)
    empty = {"imp": 0, "clk": 0, "cost": 0.0, "conv": 0}
    return {
        "period_current": [cur_from.isoformat(), cur_to.isoformat()],
        "period_previous": [prev_from.isoformat(), prev_to.isoformat()],
        "campaigns": {
            cid: {
                "name": CAMPAIGN_NAMES[cid],
                "current": derive(cur.get(cid, dict(empty))),
                "previous": derive(prev.get(cid, dict(empty))),
            }
            for cid in CAMPAIGN_IDS
        },
    }


async def fetch_balance(session: aiohttp.ClientSession) -> dict:
    """Остаток на счёте Я.Директ через Live v4 AccountManagement.

    Возвращает {login, amount, available_for_transfer, currency}.
    """
    body = {
        "method": "AccountManagement",
        "param": {"Action": "Get", "SelectionCriteria": {}},
        "token": get_direct_token(),
    }
    timeout = aiohttp.ClientTimeout(total=60)
    async with session.post(
            API_LIVE_V4,
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=timeout) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise DirectError(f"Я.Директ баланс HTTP {resp.status}: "
                              f"{text[:200]}")
        data = await resp.json(content_type=None)

    if "error_str" in data or "error_code" in data:
        raise DirectError(
            f"Я.Директ баланс: {data.get('error_str')} "
            f"{data.get('error_detail', '')}".strip())

    accounts = data.get("data", {}).get("Accounts") or []
    if not accounts:
        raise DirectError("Я.Директ не вернул ни одного счёта.")
    acc = accounts[0]
    return {
        "login": acc.get("Login", "?"),
        "amount": float(acc.get("Amount") or 0),
        "available_for_transfer": float(
            acc.get("AmountAvailableForTransfer") or 0),
        "currency": acc.get("Currency", "RUB"),
    }


# ─── Сайт-лиды amoCRM ─────────────────────────────────────────────────────────
# Фильтр filter[tags][i][id] в amoCRM API v4 не работает — сервер молча
# отдаёт все лиды. Поэтому теги фильтруем строго на клиенте.

async def amo_get(session: aiohttp.ClientSession, path: str,
                  params: dict) -> dict:
    url = AMO_BASE + path
    headers = {
        "Authorization": f"Bearer {os.getenv('AMO_ACCESS_TOKEN', '')}",
        "Accept": "application/json",
    }
    timeout = aiohttp.ClientTimeout(total=30)
    for attempt in range(5):
        async with session.get(url, params=params, headers=headers,
                               timeout=timeout) as resp:
            if resp.status == 200:
                return await resp.json(content_type=None)
            if resp.status in (204, 404):
                return {}
            if resp.status == 429:
                await asyncio.sleep(2 ** attempt)
                continue
            if resp.status == 401:
                raise DirectError(
                    "amoCRM не принял токен: нужен свежий AMO_ACCESS_TOKEN.")
            text = await resp.text()
            raise DirectError(f"amoCRM HTTP {resp.status}: {text[:200]}")
    raise DirectError(f"amoCRM не ответил после 5 попыток: {path}")


def is_site_lead(lead: dict) -> bool:
    tags = lead.get("_embedded", {}).get("tags") or []
    return any(t.get("id") in TAG_IDS for t in tags)


async def fetch_site_leads(session: aiohttp.ClientSession,
                           days: int = LEADS_WINDOW_DAYS) -> dict:
    now = datetime.now(timezone.utc)
    ts_from = int((now - timedelta(days=days)).timestamp())

    users_raw = await amo_get(session, "/users", {"limit": 250})
    users = {u["id"]: u["name"]
             for u in users_raw.get("_embedded", {}).get("users", [])}

    st_raw = await amo_get(session, f"/leads/pipelines/{PIPELINE_ATTRACT}", {})
    statuses = {s["id"]: s["name"]
                for s in st_raw.get("_embedded", {}).get("statuses", [])}

    all_leads: list[dict] = []
    page = 1
    while True:
        d = await amo_get(session, "/leads", {
            "filter[pipeline_id]": PIPELINE_ATTRACT,
            "filter[created_at][from]": ts_from,
            "limit": 250,
            "page": page,
        })
        leads = d.get("_embedded", {}).get("leads", [])
        if not leads:
            break
        all_leads.extend(leads)
        if len(leads) < 250:
            break
        page += 1
        await asyncio.sleep(RATE_DELAY)

    site = [l for l in all_leads if is_site_lead(l)]

    by_status: Counter = Counter()
    by_resp: Counter = Counter()
    won = lost = open_ = 0
    for l in site:
        st = l["status_id"]
        if st == STATUS_WON:
            label, won = "Реализовано", won + 1
        elif st == STATUS_LOST:
            label, lost = "Не реализовано", lost + 1
        else:
            label, open_ = statuses.get(st, f"status={st}"), open_ + 1
        by_status[label] += 1
        by_resp[users.get(l["responsible_user_id"],
                          f"id={l['responsible_user_id']}")] += 1

    stale = []
    for l in site:
        if l["status_id"] in (STATUS_WON, STATUS_LOST):
            continue
        last_note = await fetch_last_note_ts(session, l["id"])
        await asyncio.sleep(RATE_DELAY)
        candidates = [t for t in (last_note, l.get("updated_at"),
                                  l.get("created_at")) if t]
        idle_h = (now.timestamp() - max(candidates)) / 3600 if candidates else 0
        st_name = statuses.get(l["status_id"], "?")
        threshold = (IDLE_FIRST_CONTACT_H if "Первичный" in st_name
                     else IDLE_OTHER_H)
        if idle_h > threshold:
            stale.append({
                "id": l["id"],
                "name": l.get("name") or f"Сделка {l['id']}",
                "status": st_name,
                "responsible": users.get(l["responsible_user_id"], "?"),
                "idle_hours": round(idle_h, 1),
            })
    stale.sort(key=lambda x: -x["idle_hours"])

    return {
        "days": days,
        "total_in_pipeline": len(all_leads),
        "total_site": len(site),
        "won": won,
        "lost": lost,
        "open": open_,
        "by_status": dict(by_status.most_common()),
        "by_responsible": dict(by_resp.most_common()),
        "stale": stale,
    }


async def fetch_last_note_ts(session: aiohttp.ClientSession,
                             lead_id: int) -> int | None:
    d = await amo_get(session, f"/leads/{lead_id}/notes",
                      {"limit": 1, "order[updated_at]": "desc"})
    notes = d.get("_embedded", {}).get("notes", [])
    if not notes:
        return None
    return max(notes[0].get("created_at") or 0,
               notes[0].get("updated_at") or 0)


# ─── Сборка текста ────────────────────────────────────────────────────────────

def rub(x: float) -> str:
    return f"{x:,.0f} ₽".replace(",", " ")


def plural(n: int, one: str, few: str, many: str) -> str:
    """«1 заявка / 2 заявки / 5 заявок» — иначе отчёт читается коряво."""
    n = abs(n)
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def block_balance(bal: dict, avg_daily: float) -> tuple[list[str], dict]:
    days_left = bal["amount"] / avg_daily if avg_daily else float("inf")
    tail = (f"хватит на ~{days_left:.0f} дн."
            if days_left != float("inf") else "расхода за неделю не было")
    line = f"Баланс {rub(bal['amount'])} · {tail}"
    if days_left < 7:
        line += " · пополнить"
    return [line], {"amount": bal["amount"], "days_left": days_left}


def block_weekly(weekly: dict) -> list[str]:
    """Одна строка на кампанию: клики, заявки, цена заявки, расход."""
    cf, ct = weekly["period_current"]
    lines = [f"Неделя {cf[8:10]}.{cf[5:7]}–{ct[8:10]}.{ct[5:7]} "
             f"(в скобках – к прошлой)"]
    for blk in weekly["campaigns"].values():
        c, p = blk["current"], blk["previous"]
        lines.append(
            f"{blk['name']}: {c['clk']} "
            f"{plural(c['clk'], 'клик', 'клика', 'кликов')} "
            f"({delta(c['clk'], p['clk'])}), {c['conv']} "
            f"{plural(c['conv'], 'заявка', 'заявки', 'заявок')} "
            f"({delta(c['conv'], p['conv'])}), "
            f"цена заявки {rub(c['cpa']) if c['cpa'] else '–'}, "
            f"расход {rub(c['cost'])}")
    return lines


def block_recommendations(weekly: dict, leads: dict,
                          balance_info: dict) -> list[str]:
    recs: list[str] = []
    for cid, blk in weekly["campaigns"].items():
        c, name = blk["current"], blk["name"]
        if c["ctr"] < 4 and c["imp"] > 100:
            recs.append(f"{name}: CTR {c['ctr']:.1f}% ниже 4%")
        if c["cpa"] and c["cpa"] > 1.5 * PLAN_CPA_RUB:
            recs.append(f"{name}: заявка {c['cpa']:.0f} ₽ при плане "
                        f"{PLAN_CPA_RUB} ₽")
        if c["conv"] == 0 and c["clk"] >= 100:
            recs.append(f"{name}: 0 заявок при {c['clk']} кликах – "
                        f"проверить цель в Метрике")
        if cid == "710295591" and c["conv"] >= 10:
            recs.append(f"{name}: {c['conv']} заявок – пора на среднюю цену "
                        f"конверсии")
    if balance_info["days_left"] < 7:
        recs.append(f"Баланс кончается через "
                    f"{balance_info['days_left']:.0f} дн.")
    if leads["total_site"] > 0 and leads["won"] == 0:
        recs.append(f"0 продаж из {leads['total_site']} "
                    f"{plural(leads['total_site'], 'заявки', 'заявок', 'заявок')} "
                    f"за {leads['days']} дн.")
    if not recs:
        return ["Проблем нет."]
    return ["Главное:"] + [f"– {r}" for r in recs]


def block_leads(leads: dict, weekly: dict) -> list[str]:
    """Заявки с сайта, продажи, зависшие — три числа и цена заявки."""
    total_cost = sum(b["current"]["cost"] for b in weekly["campaigns"].values())
    cpl = (f", по ~{rub(total_cost / leads['total_site'])}"
           if leads["total_site"] else "")
    lines = [f"Заявки с сайта за {leads['days']} дн.: "
             f"{leads['total_site']}{cpl} · продаж {leads['won']} · "
             f"отказов {leads['lost']}"]

    stale = leads["stale"]
    if stale:
        worst = stale[0]
        top = Counter(s["responsible"] for s in stale).most_common(1)[0]
        lines.append(
            f"Без движения: {len(stale)} · дольше всех "
            f"{worst['idle_hours']:.0f} ч ({worst['responsible']}) · "
            f"больше всех у одного: {top[0]} – {top[1]}")
    return lines


async def build_report() -> str:
    """Собирает готовый текст сводки. Кидает DirectError с внятным текстом."""
    today = datetime.now(MSK).date()
    async with aiohttp.ClientSession() as session:
        bal = await fetch_balance(session)
        weekly = await weekly_compare(session, today)
        # Средний расход за неделю считаем по тем же отчётам, что и блок
        # эффективности: сумма current-периода по обеим кампаниям / 7.
        avg_daily = sum(b["current"]["cost"]
                        for b in weekly["campaigns"].values()) / 7
        leads = await fetch_site_leads(session)

    bal_lines, bal_info = block_balance(bal, avg_daily)
    parts = [
        [f"Я.Директ · {today.strftime('%d.%m')}"] + bal_lines,
        block_weekly(weekly),
        block_leads(leads, weekly),
        block_recommendations(weekly, leads, bal_info),
    ]
    return "\n\n".join("\n".join(p) for p in parts)


def split_for_telegram(text: str, limit: int = TELEGRAM_LIMIT) -> list[str]:
    """Режет длинный текст по границам блоков, не по середине строки."""
    if len(text) <= limit:
        return [text]
    chunks, cur = [], ""
    for block in text.split("\n\n"):
        if len(cur) + len(block) + 2 > limit and cur:
            chunks.append(cur)
            cur = block
        else:
            cur = f"{cur}\n\n{block}" if cur else block
    if cur:
        chunks.append(cur)
    return chunks


async def _main() -> None:
    logging.basicConfig(level=logging.INFO)
    print(await build_report())


if __name__ == "__main__":
    asyncio.run(_main())
