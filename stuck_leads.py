"""
stuck_leads.py — Пинг ответственного при зависании лида на «Неразобранном» > N часов.

Воронка ПРИВЛЕЧЕНИЕ (pipeline_id=10873622). Первый рабочий столбец — «Неразобранное»
type=0 (status_id=85554790): именно сюда лид попадает при создании (подтверждено данными
2026-07-14: 48/50 недавних сделок). Системные «Входящие» type=1 (85554786) и /leads/unsorted
в аккаунте не используются — НЕ мониторим.

Механика:
  - Polling через PTB JobQueue run_repeating (AsyncIOScheduler на Amvera молча теряет
    interval-тики — см. market_intel/payment_planned/светофоры).
  - «Возраст в статусе» = now − последний lead_status_changed в статус 85554790; если
    переходов ещё не было (лид создан в этом статусе) — по created_at сделки.
  - Пинг при возрасте ≥ STUCK_LEAD_HOURS (по умолч. 5 ч).
  - Пинги только в окне 09:00–20:00 МСК (любой день недели). Вне окна — тихо, первый
    пинг откладывается до открытия окна.
  - Повтор не чаще STUCK_LEAD_REPEAT_HOURS (по умолч. 3 ч), пока лид не уйдёт со статуса.
  - Адресат — личка ответственного (reverse manager_chats.amo_user_id → user_id). Если
    связки нет — fallback на OWNER_CHAT_ID с пометкой.

План: plans/2026-07-14-пинг-зависших-лидов-неразобранное.md
"""

import os
import logging
from datetime import datetime, timezone, timedelta

from amocrm import amo_get

logger = logging.getLogger(__name__)

PIPELINE_ID = 10873622
STAGE_ID = 85554790  # «Неразобранное» type=0 — первый рабочий столбец воронки ПРИВЛЕЧЕНИЕ

STUCK_LEAD_HOURS = float(os.getenv("STUCK_LEAD_HOURS", "5"))
STUCK_LEAD_REPEAT_HOURS = float(os.getenv("STUCK_LEAD_REPEAT_HOURS", "3"))
WINDOW_START_H = int(os.getenv("STUCK_LEAD_WINDOW_START", "9"))   # включительно
WINDOW_END_H = int(os.getenv("STUCK_LEAD_WINDOW_END", "20"))       # не включая (последний пинг в 19:xx)

AMO_SUBDOMAIN = os.getenv("AMO_SUBDOMAIN", "victorfishtobiz")
MSK = timezone(timedelta(hours=3))

_owner_raw = os.getenv("OWNER_CHAT_ID")
OWNER_CHAT_ID = int(_owner_raw) if _owner_raw else None

# Кэш имён пользователей amoCRM (id → имя), чтобы не дёргать /users на каждом тике
_user_name_cache: dict = {}


def _lead_url(lead_id: int) -> str:
    return f"https://{AMO_SUBDOMAIN}.amocrm.ru/leads/detail/{lead_id}"


def _in_window(now_msk: datetime) -> bool:
    return WINDOW_START_H <= now_msk.hour < WINDOW_END_H


async def _get_user_name(user_id: int) -> str:
    if user_id in _user_name_cache:
        return _user_name_cache[user_id]
    data = await amo_get(f"/users/{user_id}")
    name = (data or {}).get("name", f"ID {user_id}")
    _user_name_cache[user_id] = name
    return name


async def _get_client_name(lead: dict) -> str:
    """Имя основного контакта сделки (lead уже с with=contacts)."""
    contacts = lead.get("_embedded", {}).get("contacts", [])
    if contacts:
        c = await amo_get(f"/contacts/{contacts[0]['id']}")
        if c and c.get("name"):
            return c["name"]
    # запасной вариант — название самой сделки
    return lead.get("name") or "Без контакта"


async def _stage_entered_at(lead: dict) -> datetime:
    """
    Момент входа в текущий статус STAGE_ID (aware UTC).
    Берём последний lead_status_changed с value_after == STAGE_ID; если переходов нет
    (лид создан прямо в статусе) — created_at сделки.
    """
    lead_id = lead["id"]
    ev = await amo_get(
        "/events",
        {
            "filter[entity]": "lead",
            "filter[entity_id]": lead_id,
            "filter[type]": "lead_status_changed",
            "limit": 100,
        },
    )
    best_ts = None
    if ev:
        for e in ev.get("_embedded", {}).get("events", []):
            va = e.get("value_after", [])
            if va and isinstance(va, list):
                st = va[0].get("lead_status", {})
                if st.get("id") == STAGE_ID and st.get("pipeline_id") == PIPELINE_ID:
                    ts = e.get("created_at", 0)
                    if best_ts is None or ts > best_ts:
                        best_ts = ts
    if best_ts is None:
        best_ts = lead.get("created_at", 0)
    return datetime.fromtimestamp(best_ts, tz=timezone.utc)


async def _fetch_stage_leads() -> list:
    """Все сделки, лежащие сейчас на STAGE_ID (с контактами)."""
    leads = []
    page = 1
    while True:
        d = await amo_get(
            "/leads",
            {
                "filter[statuses][0][pipeline_id]": PIPELINE_ID,
                "filter[statuses][0][status_id]": STAGE_ID,
                "with": "contacts",
                "limit": 250,
                "page": page,
            },
        )
        if not d:
            break
        batch = d.get("_embedded", {}).get("leads", [])
        if not batch:
            break
        leads.extend(batch)
        if d.get("_links", {}).get("next"):
            page += 1
        else:
            break
    return leads


def _resolve_target(db, responsible_id):
    """
    Возвращает (chat_id, is_fallback). Реверс manager_chats.amo_user_id → user_id.
    Если связки нет / чат заблокирован — fallback на OWNER_CHAT_ID.
    """
    if responsible_id:
        row = db._fetchone(
            "SELECT user_id, is_blocked FROM manager_chats WHERE amo_user_id=%s",
            (responsible_id,),
        )
        if row and row.get("user_id") and not row.get("is_blocked"):
            return int(row["user_id"]), False
    return OWNER_CHAT_ID, True


async def poll_job(app, db):
    """Один прогон: найти зависшие лиды и разослать/повторить пинги."""
    now_utc = datetime.now(timezone.utc)
    now_msk = now_utc.astimezone(MSK)

    try:
        leads = await _fetch_stage_leads()
    except Exception as e:
        logger.error(f"stuck_leads: не смог получить сделки: {e}", exc_info=True)
        return

    current_ids = [l["id"] for l in leads]

    # Чистим записи по лидам, ушедшим со статуса (при возврате начнём заново)
    try:
        if current_ids:
            db._execute(
                "DELETE FROM lead_stuck_pings WHERE lead_id <> ALL(%s)",
                (current_ids,),
            )
        else:
            db._execute("DELETE FROM lead_stuck_pings", None)
    except Exception as e:
        logger.warning(f"stuck_leads: очистка ушедших не удалась: {e}")

    if not leads:
        return

    for lead in leads:
        lead_id = lead["id"]
        try:
            entered_at = await _stage_entered_at(lead)
            hours = (now_utc - entered_at).total_seconds() / 3600.0
            if hours < STUCK_LEAD_HOURS:
                continue

            rec = db._fetchone(
                "SELECT stage_entered_at, last_ping_at, pings_count FROM lead_stuck_pings WHERE lead_id=%s",
                (lead_id,),
            )
            # Если лид перезашёл в статус (новый entered_at) — сбрасываем историю пингов
            reentered = False
            if rec and rec.get("stage_entered_at"):
                prev = rec["stage_entered_at"]
                if prev.tzinfo is None:
                    prev = prev.replace(tzinfo=timezone.utc)
                if abs((prev - entered_at).total_seconds()) > 120:
                    reentered = True

            last_ping = None if (rec is None or reentered) else rec.get("last_ping_at")

            # Вне окна 09–20 МСК — не шлём (первый пинг откладывается)
            if not _in_window(now_msk):
                continue

            # Анти-спам: не чаще REPEAT_HOURS
            if last_ping is not None:
                lp = last_ping if last_ping.tzinfo else last_ping.replace(tzinfo=timezone.utc)
                if (now_utc - lp).total_seconds() / 3600.0 < STUCK_LEAD_REPEAT_HOURS:
                    continue

            responsible_id = lead.get("responsible_user_id")
            chat_id, is_fallback = _resolve_target(db, responsible_id)
            if not chat_id:
                logger.warning(f"stuck_leads: нет адресата для lead={lead_id}, пропуск")
                continue

            client = await _get_client_name(lead)
            hours_txt = f"{int(hours)} ч" if hours >= 2 else f"{hours:.1f} ч"

            text = (
                f"⏰ Лид завис на «Неразобранном» уже {hours_txt}\n\n"
                f"👤 {client}\n"
                f"🔗 {_lead_url(lead_id)}\n\n"
                f"Возьми в работу — переведи на следующий этап."
            )
            if is_fallback:
                mgr = await _get_user_name(responsible_id) if responsible_id else "не назначен"
                text = (
                    f"⚠️ Ответственный *{mgr}* — нет личного чата с ботом, пингую тебя.\n\n"
                    + text
                )

            try:
                await app.bot.send_message(chat_id=chat_id, text=text)
            except Exception as e:
                logger.error(f"stuck_leads: send lead={lead_id} chat={chat_id}: {e}")
                continue

            db._execute(
                """
                INSERT INTO lead_stuck_pings (lead_id, stage_entered_at, last_ping_at, pings_count)
                VALUES (%s, %s, %s, 1)
                ON CONFLICT (lead_id) DO UPDATE
                    SET stage_entered_at = EXCLUDED.stage_entered_at,
                        last_ping_at = EXCLUDED.last_ping_at,
                        pings_count = CASE
                            WHEN %s THEN 1
                            ELSE lead_stuck_pings.pings_count + 1 END
                """,
                (lead_id, entered_at, now_utc, reentered),
            )
            logger.info(
                f"stuck_leads: пинг lead={lead_id} chat={chat_id} "
                f"fallback={is_fallback} hours={hours:.1f}"
            )
        except Exception as e:
            logger.error(f"stuck_leads: обработка lead={lead_id}: {e}", exc_info=True)
