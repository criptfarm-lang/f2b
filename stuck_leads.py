"""
stuck_leads.py — Пинг ответственного при зависании лида на «Неразобранном» > N часов.

Воронка ПРИВЛЕЧЕНИЕ (pipeline_id=10873622). Первый рабочий столбец — «Неразобранное»
type=0 (status_id=85554790): именно сюда лид попадает при создании (подтверждено данными
2026-07-14: 48/50 недавних сделок). Системные «Входящие» type=1 (85554786) и /leads/unsorted
в аккаунте не используются — НЕ мониторим.

Механика (v2, 2026-07-28 — трёхступенчатая эскалация со штрафом):
  - Polling через PTB JobQueue run_repeating (AsyncIOScheduler на Amvera молча теряет
    interval-тики — см. market_intel/payment_planned/светофоры).
  - «Возраст в статусе» = now − последний lead_status_changed в статус 85554790; если
    переходов ещё не было (лид создан в этом статусе) — по created_at сделки.
  - Считаем РАБОЧЕЕ время в стадии: только окно 09:00–20:00 МСК в рабочие дни
    (ПН-ПТ + ВС; суббота — выходной). Вне окна время не идёт, пинги на паузе.
  - «Лид из паузы = горячий»: если лид вошёл в статус вне рабочего окна (ночь/суббота),
    добавляем кредит 1 раб.ч, чтобы на открытии окна сразу шёл пинг №1.
  - Уровни по effective = раб.часы_в_стадии + кредит:
      ≥ L1 (1 ч) → пинг-напоминание;
      ≥ L2 (2 ч) → пинг с предупреждением о штрафе 500 ₽;
      ≥ L3 (3 ч) → пинг «штраф начислен» + запись 500 ₽ в lead_stuck_penalties
                    (ТОЛЬКО менеджерам ОП; закупщиков/не-ОП штраф не касается).
    Каждый уровень шлётся один раз. После L3 — тишина (один штраф на вход в статус).
  - Адресат — личка ответственного (reverse manager_chats.amo_user_id → user_id). Если
    связки нет — fallback на OWNER_CHAT_ID с пометкой.

Планы: plans/2026-07-14-пинг-зависших-лидов-неразобранное.md,
       plans/2026-07-28-штраф-зависание-неразобранное.md
"""

import os
import logging
from datetime import datetime, timezone, timedelta

from amocrm import amo_get

logger = logging.getLogger(__name__)

PIPELINE_ID = 10873622
STAGE_ID = 85554790  # «Неразобранное» type=0 — первый рабочий столбец воронки ПРИВЛЕЧЕНИЕ

# Пороги эскалации в РАБОЧИХ часах (effective = раб.часы + кредит паузы)
NERAZ_L1_H = float(os.getenv("NERAZ_L1_H", "1"))  # напоминание
NERAZ_L2_H = float(os.getenv("NERAZ_L2_H", "2"))  # предупреждение о штрафе
NERAZ_L3_H = float(os.getenv("NERAZ_L3_H", "3"))  # штраф начислен
NERAZ_PENALTY_RUB = int(os.getenv("NERAZ_PENALTY_RUB", "500"))
NERAZ_PAUSE_CREDIT_H = float(os.getenv("NERAZ_PAUSE_CREDIT_H", "1"))  # кредит «лид из паузы»

WINDOW_START_H = int(os.getenv("STUCK_LEAD_WINDOW_START", "9"))   # включительно
WINDOW_END_H = int(os.getenv("STUCK_LEAD_WINDOW_END", "20"))       # не включая (последний пинг в 19:xx)
# Рабочие дни недели (Пн=0 .. Вс=6). По умолчанию ПН-ПТ + ВС, суббота (5) исключена.
WORKDAYS = {
    int(x) for x in os.getenv("STUCK_LEAD_WORKDAYS", "0,1,2,3,4,6").split(",") if x.strip() != ""
}

# amo_user_id пяти менеджеров ОП — только им начисляется денежный штраф.
# Источник правды — MANAGER_TAG_TO_AMO_USER_ID в f2b-publisher/src/manager_dashboard_data.py
# (кросс-репо, импортировать нельзя; при смене состава ОП синхронизировать оба места).
# баласанян/дьяченко/коликов/мерзлякова/скляр.
_op_raw = os.getenv("STUCK_LEAD_OP_AMO_IDS", "12625622,13746010,13665786,12788698,11544494")
OP_AMO_IDS = {int(x) for x in _op_raw.split(",") if x.strip() != ""}

AMO_SUBDOMAIN = os.getenv("AMO_SUBDOMAIN", "victorfishtobiz")
MSK = timezone(timedelta(hours=3))

_owner_raw = os.getenv("OWNER_CHAT_ID")
OWNER_CHAT_ID = int(_owner_raw) if _owner_raw else None

# Кэш имён пользователей amoCRM (id → имя), чтобы не дёргать /users на каждом тике
_user_name_cache: dict = {}


def _lead_url(lead_id: int) -> str:
    return f"https://{AMO_SUBDOMAIN}.amocrm.ru/leads/detail/{lead_id}"


def _in_window(now_msk: datetime) -> bool:
    """True, если сейчас рабочее окно: рабочий день И час в [START, END)."""
    return now_msk.weekday() in WORKDAYS and WINDOW_START_H <= now_msk.hour < WINDOW_END_H


def _working_hours(entered_utc: datetime, now_utc: datetime) -> float:
    """Рабочее время (в часах) между entered и now: сумма пересечений с окнами
    09–20 МСК в рабочие дни (ПН-ПТ + ВС). Суббота и ночи не считаются."""
    start = entered_utc.astimezone(MSK)
    end = now_utc.astimezone(MSK)
    if end <= start:
        return 0.0
    total = 0.0
    day = start.date()
    last = end.date()
    while day <= last:
        if day.weekday() in WORKDAYS:
            win_s = datetime(day.year, day.month, day.day, WINDOW_START_H, tzinfo=MSK)
            win_e = datetime(day.year, day.month, day.day, WINDOW_END_H, tzinfo=MSK)
            s = max(start, win_s)
            e = min(end, win_e)
            if e > s:
                total += (e - s).total_seconds()
        day += timedelta(days=1)
    return total / 3600.0


def _effective_hours(entered_utc: datetime, now_utc: datetime) -> float:
    """effective = рабочие часы в стадии + кредит паузы (1 ч, если лид вошёл в
    статус вне рабочего окна — «лид из паузы = горячий»)."""
    w = _working_hours(entered_utc, now_utc)
    entered_msk = entered_utc.astimezone(MSK)
    credit = 0.0 if _in_window(entered_msk) else NERAZ_PAUSE_CREDIT_H
    return w + credit


def _level_for(effective: float) -> int:
    """Уровень эскалации 0..3 по effective рабочим часам."""
    if effective >= NERAZ_L3_H:
        return 3
    if effective >= NERAZ_L2_H:
        return 2
    if effective >= NERAZ_L1_H:
        return 1
    return 0


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
            effective = _effective_hours(entered_at, now_utc)
            target_level = _level_for(effective)
            if target_level == 0:
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

            sent_level = 0 if (rec is None or reentered) else int(rec.get("pings_count") or 0)

            # Вне рабочего окна (ночь/суббота/до 9/после 20) — пауза, откладываем
            if not _in_window(now_msk):
                continue

            # Каждый уровень шлём один раз; после L3 — тишина
            if target_level <= sent_level:
                continue

            responsible_id = lead.get("responsible_user_id")
            chat_id, is_fallback = _resolve_target(db, responsible_id)
            if not chat_id:
                logger.warning(f"stuck_leads: нет адресата для lead={lead_id}, пропуск")
                continue

            client = await _get_client_name(lead)
            url = _lead_url(lead_id)
            is_op = responsible_id in OP_AMO_IDS

            if target_level >= 3:
                penalty_note = (
                    f"Начислен штраф {NERAZ_PENALTY_RUB} ₽ (виден в дашборде)."
                    if is_op else
                    "Разбери сделку — она провисела 3 рабочих часа."
                )
                text = (
                    f"⛔ Лид висел 3 рабочих часа на «Неразобранном» без движения.\n"
                    f"{penalty_note}\n\n"
                    f"👤 {client}\n🔗 {url}\n\nРазбери сделку."
                )
            elif target_level == 2:
                warn = (
                    f"Ещё час без движения — штраф {NERAZ_PENALTY_RUB} ₽."
                    if is_op else
                    "Разбери сделку, чтобы не висела."
                )
                text = (
                    f"⚠️ Лид висит ~2 часа на «Неразобранном». {warn}\n\n"
                    f"👤 {client}\n🔗 {url}\n\nРазбери сделку сейчас."
                )
            else:  # level 1
                text = (
                    f"⏰ Лид завис на «Неразобранном» ~1 час.\n\n"
                    f"👤 {client}\n🔗 {url}\n\n"
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

            # На уровне 3 (впервые) — начисляем штраф, только менеджерам ОП
            if target_level >= 3 and sent_level < 3 and is_op:
                try:
                    db._execute(
                        """
                        INSERT INTO lead_stuck_penalties
                            (lead_id, stage_entered_at, responsible_id, amount_rub, charged_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (lead_id, stage_entered_at) DO NOTHING
                        """,
                        (lead_id, entered_at, responsible_id, NERAZ_PENALTY_RUB, now_utc),
                    )
                    logger.info(
                        f"stuck_leads: ШТРАФ {NERAZ_PENALTY_RUB}₽ lead={lead_id} "
                        f"responsible={responsible_id}"
                    )
                except Exception as e:
                    logger.error(f"stuck_leads: не записал штраф lead={lead_id}: {e}")

            # pings_count = максимальный отправленный уровень (анти-спам по уровням)
            db._execute(
                """
                INSERT INTO lead_stuck_pings (lead_id, stage_entered_at, last_ping_at, pings_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (lead_id) DO UPDATE
                    SET stage_entered_at = EXCLUDED.stage_entered_at,
                        last_ping_at = EXCLUDED.last_ping_at,
                        pings_count = EXCLUDED.pings_count
                """,
                (lead_id, entered_at, now_utc, target_level),
            )
            logger.info(
                f"stuck_leads: пинг L{target_level} lead={lead_id} chat={chat_id} "
                f"fallback={is_fallback} eff={effective:.2f}ч op={is_op}"
            )
        except Exception as e:
            logger.error(f"stuck_leads: обработка lead={lead_id}: {e}", exc_info=True)
