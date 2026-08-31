"""Утренняя проверка логистики: заявки без машины и неподтверждённый маршрут.

План: plans/2026-08-31-алерт-логисту-заявки-без-машины.md (репо «второй мозг»).

Повод — 31.08.2026: мост создал в Логистике заявки по заказам 03818 и 03819, но они
остались без назначения на машину (`u = 0`), логист подтвердила маршруты без них, а
заказы всё равно отгрузили. Такая потеря нигде не всплывает: реестр водителя строится
по машинам, и заявка без машины просто невидима.

Проверяем два условия и пишем логистам в личку:
  1. заявка на сегодня есть, но машина не назначена;
  2. у машины есть точки на сегодня, а маршрут за сегодня не подтверждён.

Только чтение: ни Логистику, ни МойСклад не правим.
"""
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import aiohttp

import route_registry as rr

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))

# Окно проверки. Раньше 08:00 смысла нет: часть раскладок логист подтверждает утром
# (замеры за август — 07:11, 07:39, 07:49 МСК), до этого «без машины» — норма.
FROM_HOUR = int(os.getenv("LOGI_CHECK_FROM_HOUR", "8"))
TO_HOUR = int(os.getenv("LOGI_CHECK_TO_HOUR", "13"))

_DB = None


def ensure_schema(db):
    db._execute("""
        CREATE TABLE IF NOT EXISTS logi_morning_alerts (
            day         DATE,
            kind        TEXT,          -- 'unassigned' | 'unconfirmed'
            fingerprint TEXT,          -- состав проблемы; меняется → шлём заново
            alerted_at  TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (day, kind, fingerprint)
        )
    """)
    logger.info("logi_morning_check: схема готова")


def _claim(day, kind, fingerprint) -> bool:
    """True — этот состав проблемы ещё не отправляли за сегодня."""
    if _DB is None:
        return True
    try:
        r = _DB._fetchone(
            "SELECT 1 FROM logi_morning_alerts WHERE day=%s AND kind=%s AND fingerprint=%s",
            (day, kind, fingerprint))
        if r:
            return False
        _DB._execute(
            "INSERT INTO logi_morning_alerts (day, kind, fingerprint) VALUES (%s,%s,%s) "
            "ON CONFLICT DO NOTHING", (day, kind, fingerprint))
        return True
    except Exception as e:
        # БД недоступна — лучше продублировать сообщение, чем промолчать о потерянной точке.
        logger.warning("logi_morning_check._claim: %s", e)
        return True


def _confirmed_units(day) -> set:
    """Машины с подтверждённым маршрутом на день."""
    if _DB is None:
        return set()
    try:
        rows = _DB._fetchall(
            "SELECT unit_id FROM route_dispatch WHERE snap_date=%s AND status='confirmed'", (day,))
        return {int(r["unit_id"]) for r in (rows or [])}
    except Exception as e:
        logger.warning("logi_morning_check._confirmed_units: %s", e)
        return set()


async def collect(target_date=None) -> dict:
    """{'unassigned': [заявка,...], 'assigned': {unit_id: N точек}} на дату.

    Заявка = {order_no, client, address, has_cid}. Читаем ресурс Логистики целиком
    (тот же вызов, что route_registry.fetch_routes), но берём и точки без машины —
    fetch_routes их молча отбрасывает, потому что строит маршруты по юнитам.
    """
    token = os.getenv("WIALON_TOKEN")
    if not token:
        raise RuntimeError("WIALON_TOKEN не задан")
    day = target_date or datetime.now(MSK).date()

    async with aiohttp.ClientSession() as session:
        login = await rr._wialon_call(session, "token/login", {"token": token})
        sid = login.get("eid")
        if not sid:
            raise RuntimeError(f"Wialon login fail: {login}")
        res = await rr._wialon_call(session, "core/search_item",
                                    {"id": rr.RESOURCE_ID, "flags": 0xffffff}, sid)
    orders = (res.get("item") or {}).get("orders") or {}

    unassigned, assigned = [], {}
    for o in orders.values():
        if not isinstance(o, dict) or not o.get("n"):
            continue
        p = o.get("p") or {}
        r = p.get("r") or {}
        s = {"vt": r.get("vt"), "tf": o.get("tf"), "tt": o.get("tt")}
        if day not in rr.stop_days(s):
            continue
        uid = o.get("u")
        if uid in rr.UNITS:
            assigned[uid] = assigned.get(uid, 0) + 1
            continue
        unassigned.append({
            "order_no": o.get("n"),
            "client": p.get("n") or "",
            "address": p.get("a") or "",
            "has_cid": bool(p.get("cid")),
        })
    unassigned.sort(key=lambda x: str(x["order_no"]))
    return {"unassigned": unassigned, "assigned": assigned}


def _fmt(day, unassigned, unconfirmed) -> str:
    lines = [f"🚚 *Логистика на {day.strftime('%d.%m')}*"]
    if unassigned:
        lines.append(f"\n⚠️ *Без машины в раскладке — {len(unassigned)}:*")
        for x in unassigned:
            head = f"• {x['order_no']}"
            if x["client"]:
                head += f" · {x['client']}"
            lines.append(head)
            if x["address"]:
                lines.append(f"  {x['address']}")
            if not x["has_cid"]:
                lines.append("  _заявка заведена вручную в Логистике_")
        lines.append("Заявка без машины в реестр водителя не попадает.")
    if unconfirmed:
        lines.append("\n⚠️ *Маршрут не подтверждён:*")
        for uid, n in unconfirmed:
            lines.append(f"• {rr.UNITS.get(uid, uid)} — точек: {n}")
        lines.append("Пока не подтверждён, водителю ничего не ушло: /маршруты → Подтвердить.")
    return "\n".join(lines)


async def poll_job(app, db=None) -> dict | None:
    """Один прогон проверки. Возвращает статистику или None, если ничего не слали."""
    now = datetime.now(MSK)
    if not (FROM_HOUR <= now.hour < TO_HOUR):
        return None
    day = now.date()

    try:
        data = await collect(day)
    except Exception as e:
        logger.warning("logi_morning_check: чтение Логистики не удалось: %s", e)
        return None

    unassigned = data["unassigned"]
    confirmed = _confirmed_units(day)
    unconfirmed = sorted((uid, n) for uid, n in data["assigned"].items() if uid not in confirmed)

    if not unassigned and not unconfirmed:
        return None

    # Дедуп раздельный: состав «без машины» меняется в течение утра чаще, чем статус
    # подтверждения, и повторять из-за него весь блок не нужно.
    send_unassigned = bool(unassigned) and _claim(
        day, "unassigned", ",".join(str(x["order_no"]) for x in unassigned))
    send_unconfirmed = bool(unconfirmed) and _claim(
        day, "unconfirmed", ",".join(str(uid) for uid, _ in unconfirmed))
    if not send_unassigned and not send_unconfirmed:
        return None

    text = _fmt(day,
                unassigned if send_unassigned else [],
                unconfirmed if send_unconfirmed else [])

    import route_dispatch as rd
    sent = 0
    for chat_id in rd._logist_chat_ids():
        try:
            await app.bot.send_message(chat_id, text, parse_mode="Markdown")
            sent += 1
        except Exception as e:
            logger.warning("logi_morning_check: не отправилось в %s: %s", chat_id, e)
    stats = {"unassigned": len(unassigned) if send_unassigned else 0,
             "unconfirmed": len(unconfirmed) if send_unconfirmed else 0,
             "sent": sent}
    logger.info("logi_morning_check: %s", json.dumps(stats, ensure_ascii=False))
    return stats


def register(db):
    global _DB
    _DB = db
    try:
        ensure_schema(db)
    except Exception as e:
        logger.exception("logi_morning_check.ensure_schema отложено (БД не готова?): %s", e)
