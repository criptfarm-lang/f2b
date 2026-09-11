"""Алерт менеджеру: в заказе покупателя не проставлено время приёмки.

План (второй мозг): plans/2026-09-11-алерт-менеджеру-время-приёмки-не-проставлено.md

В заказе есть пара атрибутов «Окно доставки с (время)» и «Окно доставки до (время)» —
часы, в которые точка готова принять товар. Менеджеры регулярно оставляют границу «с»
пустой (замер 28.08–11.09.2026: 109 заказов из 305, 36%). Мост логистики пустую границу
не угадывает, а разворачивает окно на край дня (`DAY_BOUNDS = (6, 21)` в
f2b-logistics-bridge/config.py) — логист видит «принимают с 06:00», водитель приезжает к
открытию, а точка работает с 11. Дальше едет весь маршрут.

Правим только пингом: писать в МойСклад агенту нельзя (правило «no writes to MoySklad»),
окно проставляет менеджер руками.

Два триггера, одна логика и один дедуп:
  1. webhook МС — при переводе заказа в «Согласован» (менеджер закончил ввод);
  2. крон каждые 30 мин в окне 10:00–20:00 МСК — добивка на потерянные webhook'и
     и на заказы, согласованные раньше горизонта.

На каждое сохранение заказа не вешаемся: при создании поля ещё пустые по определению,
менеджер получал бы пинг во время ввода.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timedelta, timezone

import aiohttp

from moysklad import (
    MOSCOW_AGGLOMERATION_KEYWORDS,
    MS_BASE,
    PDZ_MANAGER_TG_IDS,
    _CITY_INDEX,
    get_headers,
)

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))

ATTR_FROM = "Окно доставки с (время)"
ATTR_TO = "Окно доставки до (время)"

# Статус «Согласован» — момент, когда заказ считается сформированным (metadata МС).
STATE_AGREED = "005f3651-9a9a-11f0-0a80-03a900027474"

# Статусы, при которых окно уже не поправить или заказ вне развозки (metadata МС 11.09.2026).
SKIP_STATES = {"Отгружен", "Возврат", "Отменен", "НЕ СОГЛАСОВАН"}

# Окно работы крон-джобы. Раньше 10:00 смысла нет — заказы на завтра ещё собираются
# (замер 28.08–11.09.2026: пик создания 15:00–17:00 МСК). Верхняя граница 20:00, а не
# 18:00: за две недели 11 заказов «на завтра» завели после 18:00, крон их иначе теряет.
FROM_HOUR = int(os.getenv("DELIVERY_WINDOW_FROM_HOUR", "10"))
TO_HOUR = int(os.getenv("DELIVERY_WINDOW_TO_HOUR", "20"))

# Горизонт контроля: сегодня + следующие N дней по дате отгрузки. Конец горизонта
# дотягивается до ближайшего дня развозки (см. horizon_end): по субботам не возим
# (замер 28.08–11.09.2026: сб — 0 отгрузок), а в пятницу менеджеры уже заводят заказы
# на понедельник — с голым «сегодня+1» они выпадали бы из контроля до выходных.
HORIZON_DAYS = int(os.getenv("DELIVERY_WINDOW_HORIZON_DAYS", "1"))

_PAGE = 100
_MAX_ROWS = 12          # сверх этого в сообщении — счётчик «и ещё N»

MONTHS = ["янв", "фев", "мар", "апр", "май", "июн",
          "июл", "авг", "сен", "окт", "ноя", "дек"]

DDL = """
CREATE TABLE IF NOT EXISTS delivery_window_alerts (
    order_id    TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    sent_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (order_id, fingerprint)
)
"""


def ensure_schema(db) -> None:
    db._execute(DDL)
    logger.info("delivery_window_check: схема готова")


def _claim(db, order_id: str, fingerprint: str, on_error: bool = False) -> bool:
    """True — по этому заказу с такой нехваткой ещё не писали, шлём.

    fingerprint = дата отгрузки + чего именно не хватает: менеджер перенёс заказ на
    другой день или потерял вторую границу — придёт новый пинг, повторные прогоны
    крона по тем же данным молчат.

    on_error — что делать при недоступной БД. Крон передаёт False (промолчим, следующий
    прогон через 30 мин повторит), webhook — True: событие приходит один раз, потерять
    его хуже, чем продублировать.
    """
    if db is None:
        return True
    try:
        row = db._fetchone(
            "INSERT INTO delivery_window_alerts (order_id, fingerprint) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING RETURNING order_id",
            (order_id, fingerprint))
        return row is not None
    except Exception as e:
        logger.warning("delivery_window_check claim(%s): %s", order_id, e)
        return on_error


def _fallback_chat_id() -> int:
    """Кому уходит алерт, если менеджер не резолвится в telegram-id."""
    for key in ("DELIVERY_WINDOW_FALLBACK_CHAT_ID", "OWNER_CHAT_ID"):
        v = (os.getenv(key) or "").strip()
        if v.lstrip("-").isdigit():
            return int(v)
    return 0


def manager_chat_id(manager_name: str, db=None) -> int:
    """«Баласанян К.» → telegram user_id. 0 — не нашли.

    Primary — PDZ_MANAGER_TG_IDS (канон состава ОП), fallback — таблица managers
    (вдруг новый менеджер ещё не в словаре). Тот же порядок, что в notifier.
    """
    for part in (manager_name or "").split():
        key = part.lower().strip(".,")
        if key in PDZ_MANAGER_TG_IDS:
            return PDZ_MANAGER_TG_IDS[key]
    if db is not None:
        for part in (manager_name or "").split():
            try:
                cid = db.get_manager_chat_id(part)
            except Exception:
                cid = None
            if cid:
                return int(cid)
    return 0


def is_our_delivery(address: str) -> bool:
    """Везём ли мы этот заказ своей развозкой (Москва / ближнее МО).

    Регионы уезжают транспортной компанией — окно приёмки там не наше дело, а
    самовывоз менеджеры помечают словом «самовывоз» прямо в адресе (та же примета,
    что в мосту логистики и manual_route).
    """
    low = (address or "").strip().lower()
    if not low or "самовывоз" in low:
        return False
    if "москва" in low or "московск" in low or "moscow" in low:
        return True
    if any(kw in low for kw in MOSCOW_AGGLOMERATION_KEYWORDS):
        return True
    return any(kw in low for kw in _CITY_INDEX)


def window_gap(order: dict) -> list[str]:
    """Каких границ окна не хватает: ['с'], ['до'], ['с', 'до'] или [].

    Граница считается отсутствующей, если поле пустое ИЛИ стоит на дату, отличную от
    даты отгрузки: такую мост игнорирует ровно так же, как пустую (значение осталось
    от прошлого заказа клиента). О протухшей границе отдельно предупреждает проверка
    'window' в check_order_logistics_validity — здесь важен сам факт, что времени нет.
    """
    deliv_day = (order.get("deliveryPlannedMoment") or "")[:10]
    if not deliv_day:
        return []
    raw = {}
    for a in order.get("attributes", []) or []:
        nm = a.get("name")
        if nm == ATTR_FROM:
            raw["с"] = (a.get("value") or "").strip()
        elif nm == ATTR_TO:
            raw["до"] = (a.get("value") or "").strip()
    missing = []
    for label in ("с", "до"):
        val = raw.get(label) or ""
        if not val or val[:10] != deliv_day:
            missing.append(label)
    return missing


def horizon_end(today: date) -> date:
    """Дата, до которой смотрим заказы. Хвост горизонта, упавший на выходной, тянем
    до ближайшего буднего дня: в пятницу менеджер уже заводит заказы на понедельник,
    и проверить их надо тогда же. Редкие воскресные отгрузки попадают внутрь
    получившегося диапазона сами."""
    end = today + timedelta(days=HORIZON_DAYS)
    while end.weekday() >= 5:          # сб/вс — развозки, как правило, нет
        end += timedelta(days=1)
    return end


def _fmt_day(iso_day: str) -> str:
    try:
        d = date.fromisoformat(iso_day)
        return f"{d.day} {MONTHS[d.month - 1]}"
    except Exception:
        return iso_day


def _fmt_time(value: str) -> str:
    """«2026-09-11 16:00:00.000» → «16:00». Дата в поле ненадёжна, берём только время —
    тот же разбор, что в реестре водителя (route_registry._attr_time)."""
    m = re.search(r"\b(\d{1,2}):(\d{2})\b", str(value or ""))
    return f"{int(m.group(1)):02d}:{m.group(2)}" if m else ""


def _order_line(row: dict) -> str:
    """Строка заказа в сообщении: что стоит и чего не хватает."""
    missing = row["missing"]
    if missing == ["с"]:
        tail = f"нет «с», стоит только «до {_fmt_time(row['win_to'])}»"
    elif missing == ["до"]:
        tail = f"нет «до», стоит только «с {_fmt_time(row['win_from'])}»"
    else:
        tail = "время не проставлено совсем"
    return (f"№{row['order_name']} · {row['agent_name']} · отгрузка {_fmt_day(row['deliv_day'])}\n"
            f"   {tail}")


def format_message(rows: list[dict]) -> str:
    """Текст пинга менеджеру: заголовок и список заказов, без объяснений и призывов —
    короткий формат по решению собственника 11.09.2026. Без Markdown: в названиях
    клиентов живут кавычки и звёздочки."""
    body = "\n".join(_order_line(r) for r in rows[:_MAX_ROWS])
    tail = ""
    if len(rows) > _MAX_ROWS:
        tail = f"\n\n…и ещё {len(rows) - _MAX_ROWS} заказ(ов)."
    return f"⏰ Не проставлено время приёмки\n\n{body}{tail}"


async def _fetch_orders(session: aiohttp.ClientSession, lo: date, hi: date) -> list[dict]:
    """Заказы покупателя с плановой отгрузкой в [lo, hi] включительно."""
    params_base = {
        "filter": (f"deliveryPlannedMoment>={lo:%Y-%m-%d} 00:00:00;"
                   f"deliveryPlannedMoment<{hi + timedelta(days=1):%Y-%m-%d} 00:00:00"),
        "order": "deliveryPlannedMoment,asc",
        "expand": "agent,state,owner",
    }
    rows, offset = [], 0
    while True:
        async with session.get(f"{MS_BASE}/entity/customerorder",
                               headers=get_headers(),
                               params={**params_base, "limit": _PAGE, "offset": offset},
                               timeout=aiohttp.ClientTimeout(total=90)) as r:
            if r.status != 200:
                raise RuntimeError(f"МС customerorder вернул {r.status}: {(await r.text())[:200]}")
            data = await r.json()
        chunk = data.get("rows", [])
        rows.extend(chunk)
        if len(chunk) < _PAGE:
            break
        offset += _PAGE
    return rows


def _as_row(order: dict) -> dict | None:
    """Заказ → строка для алерта, либо None, если нарушения нет / заказ вне контроля."""
    state_name = ((order.get("state") or {}).get("name") or "").strip()
    if state_name in SKIP_STATES:
        return None
    address = order.get("shipmentAddress") or ""
    if not is_our_delivery(address):
        return None
    missing = window_gap(order)
    if not missing:
        return None
    raw = {a.get("name"): (a.get("value") or "")
           for a in (order.get("attributes") or [])}
    return {
        "order_id": order.get("id") or "",
        "order_name": order.get("name") or "",
        "agent_name": ((order.get("agent") or {}).get("name") or "—"),
        "manager_name": ((order.get("owner") or {}).get("name") or ""),
        "deliv_day": (order.get("deliveryPlannedMoment") or "")[:10],
        "missing": missing,
        "win_from": raw.get(ATTR_FROM, ""),
        "win_to": raw.get(ATTR_TO, ""),
        "state": state_name,
    }


async def collect(lo: date, hi: date) -> list[dict]:
    """Все заказы окна, у которых не хватает границ окна приёмки."""
    async with aiohttp.ClientSession() as session:
        orders = await _fetch_orders(session, lo, hi)
    out = []
    for o in orders:
        row = _as_row(o)
        if row:
            out.append(row)
    return out


async def _send(bot, rows: list[dict], db) -> dict:
    """Группирует по менеджеру и шлёт в личку. Возвращает статистику."""
    by_mgr: dict[int, list[dict]] = {}
    fallback: list[dict] = []
    for row in rows:
        chat = manager_chat_id(row["manager_name"], db)
        if chat:
            by_mgr.setdefault(chat, []).append(row)
        else:
            fallback.append(row)
            logger.warning("delivery_window_check: не нашёл tg_id менеджера '%s' (заказ %s)",
                           row["manager_name"], row["order_name"])

    sent, failed = 0, 0
    for chat, items in by_mgr.items():
        try:
            await bot.send_message(chat_id=chat, text=format_message(items))
            sent += len(items)
        except Exception as e:
            failed += len(items)
            logger.warning("delivery_window_check: не отправил менеджеру %s: %s", chat, e)

    if fallback:
        chat = _fallback_chat_id()
        if chat:
            text = ("⏰ Время приёмки не проставлено, менеджер не определился по боту:\n\n"
                    + format_message(fallback))
            try:
                await bot.send_message(chat_id=chat, text=text)
                sent += len(fallback)
            except Exception as e:
                failed += len(fallback)
                logger.warning("delivery_window_check: фолбэк не ушёл: %s", e)

    return {"orders": len(rows), "sent": sent, "failed": failed,
            "managers": len(by_mgr), "fallback": len(fallback)}


def _fingerprint(row: dict) -> str:
    return f"{row['deliv_day']}|{'+'.join(row['missing'])}"


async def poll_job(app, db=None) -> dict | None:
    """Крон-добивка: раз в 30 мин в окне 10:00–18:00 МСК.

    Берёт заказы с отгрузкой на сегодня и на завтра, оставляет те, где нет границ окна,
    клеймит каждый заказ и шлёт ответственному менеджеру одним сообщением.
    """
    now = datetime.now(MSK)
    if not (FROM_HOUR <= now.hour < TO_HOUR):
        return None

    today = now.date()
    try:
        rows = await collect(today, horizon_end(today))
    except Exception as e:
        logger.warning("delivery_window_check: чтение МС не удалось: %s", e)
        return None

    fresh = [r for r in rows if _claim(db, r["order_id"], _fingerprint(r))]
    if not fresh:
        return None

    stats = await _send(app.bot, fresh, db)
    logger.info("delivery_window_check poll: %s", stats)
    return stats


async def check_one(order_href: str, bot, db=None) -> bool:
    """Webhook МС: заказ перевели в «Согласован» — проверяем окно сразу.

    Возвращает True, если алерт ушёл. Дедуп общий с крон-джобой, так что менеджер
    получит пинг один раз, каким бы путём нарушение ни нашлось.
    """
    try:
        async with aiohttp.ClientSession() as session:
            url = order_href.split("?")[0]
            async with session.get(url, headers=get_headers(),
                                   params={"expand": "agent,state,owner"},
                                   timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    return False
                order = await r.json()

        state_id = ((order.get("state") or {}).get("meta") or {}).get("href", "").rsplit("/", 1)[-1]
        if state_id != STATE_AGREED:
            return False

        row = _as_row(order)
        if not row:
            return False

        # Горизонт тот же, что у крона: заказ на послезавтра ещё успеют поправить,
        # дёргать менеджера заранее незачем.
        today = datetime.now(MSK).date()
        try:
            deliv = date.fromisoformat(row["deliv_day"])
        except Exception:
            return False
        if not (today <= deliv <= horizon_end(today)):
            return False

        if not _claim(db, row["order_id"], _fingerprint(row), on_error=True):
            return False

        stats = await _send(bot, [row], db)
        logger.info("delivery_window_check webhook(%s): %s", row["order_name"], stats)
        return bool(stats.get("sent"))
    except Exception as e:
        logger.error("delivery_window_check.check_one: %s", e, exc_info=True)
        return False
