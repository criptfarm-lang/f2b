"""
fishki_reminder.py — разовый ремайндер по FISHки-ссылкам, которые скоро протухнут.

Используется через TG-команды собственника:
- /fishki_remind_dry  — собрать preview в таблицу fishki_reminders
- /fishki_remind_send — отправить с интервалом 90-120 сек + jitter
- /fishki_remind_stop — мягкая остановка
"""

import asyncio
import hashlib
import logging
import os
import random

import aiohttp

from notifier import (
    WAZZUP_API_URL,
    QUIZ_BASE_URL,
    MS_BASE,
    _get_contact_from_ms,
)
from moysklad import get_headers

logger = logging.getLogger(__name__)

WINDOW_MIN_DAYS = 4
WINDOW_MAX_DAYS = 7
DELAY_MIN_S = 90
DELAY_MAX_S = 120

STOP_EVENT = asyncio.Event()


def _make_short_url(order_id: str, agent_id: str) -> str:
    short_code = hashlib.md5(f"{order_id}{agent_id}".encode()).hexdigest()[:8]
    return f"{QUIZ_BASE_URL}/q/{short_code}"


def _plural_days(n: int) -> str:
    if n == 1:
        return "1 день"
    if 2 <= n <= 4:
        return f"{n} дня"
    return f"{n} дней"


def _make_message(order_name: str, days_left: int, quiz_url: str) -> str:
    return (
        f"🎣 Заказ № {order_name} — ваша FISHки-викторина ещё открыта.\n\n"
        f"Ссылка действует 7 дней, осталось {_plural_days(days_left)}. "
        f"Дальше она закроется, и FISHки за этот заказ сгорят безвозвратно.\n\n"
        f"50 FISHек = пласт форели в подарок.\n\n"
        f"Сыграть → {quiz_url}"
    )


async def _fetch_order_name(order_id: str, headers: dict) -> str | None:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"{MS_BASE}/entity/customerorder/{order_id}",
                headers=headers,
            ) as r:
                if r.status != 200:
                    return None
                data = await r.json()
                return data.get("name")
    except Exception as e:
        logger.warning(f"_fetch_order_name {order_id}: {e}")
        return None


def _select_burning(db) -> list:
    """Один заказ на клиента (самый свежий) из окна 4-7 дней, без quiz_results."""
    return db._fetchall(f"""
        WITH burning AS (
          SELECT m.client_id, m.company_name, m.order_id, m.sent_at,
                 ROW_NUMBER() OVER (
                   PARTITION BY m.client_id ORDER BY m.sent_at DESC
                 ) AS rn,
                 (7 - EXTRACT(day FROM NOW() - m.sent_at)::int) AS days_left
          FROM mailings m
          LEFT JOIN quiz_results q ON q.order_id = m.order_id
          WHERE m.sent_at >= NOW() - INTERVAL '{WINDOW_MAX_DAYS} days'
            AND m.sent_at <  NOW() - INTERVAL '{WINDOW_MIN_DAYS} days'
            AND q.order_id IS NULL
            AND m.client_id IS NOT NULL
        )
        SELECT client_id, company_name, order_id, sent_at, days_left
        FROM burning
        WHERE rn = 1
        ORDER BY days_left, company_name
    """)


async def build_preview(db) -> dict:
    """Собирает preview-список (тянет МС-контакты), пишет в fishki_reminders status='preview'."""
    db._execute("DELETE FROM fishki_reminders WHERE status = 'preview'")

    rows = _select_burning(db)
    headers = get_headers()
    ready = []
    skipped = []

    for row in rows:
        order_id = row["order_id"]
        client_id = row["client_id"]
        company = row["company_name"] or "?"
        days_left = int(row["days_left"])

        try:
            contact = await _get_contact_from_ms(client_id, headers)
        except Exception as e:
            logger.warning(f"fishki_reminder build_preview contact {company}: {e}")
            contact = None
        if not contact:
            skipped.append({"company": company, "reason": "нет контакта в МС"})
            continue

        order_name = await _fetch_order_name(order_id, headers) or order_id[:8]
        quiz_url = _make_short_url(order_id, client_id)
        msg = _make_message(order_name, days_left, quiz_url)

        db._execute("""
            INSERT INTO fishki_reminders
              (order_id, client_id, company_name, chat_type, chat_id_value, status, sent_at)
            VALUES (%s, %s, %s, %s, %s, 'preview', NULL)
            ON CONFLICT (order_id) DO UPDATE
              SET client_id = EXCLUDED.client_id,
                  company_name = EXCLUDED.company_name,
                  chat_type = EXCLUDED.chat_type,
                  chat_id_value = EXCLUDED.chat_id_value,
                  status = 'preview',
                  sent_at = NULL,
                  error = NULL
        """, (order_id, client_id, company, contact["chat_type"], contact["chat_id"]))

        ready.append({
            "company": company,
            "order_name": order_name,
            "days_left": days_left,
            "chat_type": contact["chat_type"],
            "msg": msg,
        })

    return {"ready": ready, "skipped": skipped}


async def send_burst(db, progress_cb=None) -> dict:
    """Шлёт preview-сообщения через Wazzup с интервалом 90-120 сек + jitter."""
    STOP_EVENT.clear()
    api_key = os.getenv("WAZZUP_API_KEY", "")
    if not api_key:
        return {"error": "WAZZUP_API_KEY env not set"}

    headers_ms = get_headers()
    rows = db._fetchall("""
        SELECT order_id, client_id, company_name
        FROM fishki_reminders
        WHERE status = 'preview'
    """)
    total = len(rows)
    if total == 0:
        return {"error": "preview пуст; запусти /fishki_remind_dry"}

    random.shuffle(rows)
    sent_ok = 0
    failed = 0

    for idx, row in enumerate(rows, 1):
        if STOP_EVENT.is_set():
            break

        order_id = row["order_id"]
        client_id = row["client_id"]
        company = row["company_name"]

        try:
            contact = await _get_contact_from_ms(client_id, headers_ms)
        except Exception as e:
            db._execute(
                "UPDATE fishki_reminders SET status='error', error=%s WHERE order_id=%s",
                (f"contact fetch: {e}", order_id),
            )
            failed += 1
            continue
        if not contact:
            db._execute(
                "UPDATE fishki_reminders SET status='error', error=%s WHERE order_id=%s",
                ("no contact", order_id),
            )
            failed += 1
            continue

        order_name = await _fetch_order_name(order_id, headers_ms) or order_id[:8]
        mailrow = db._fetchone(
            "SELECT (7 - EXTRACT(day FROM NOW() - sent_at)::int) AS days_left FROM mailings WHERE order_id=%s",
            (order_id,),
        )
        days_left = int(mailrow["days_left"]) if mailrow else 1
        if days_left < 1:
            days_left = 1

        quiz_url = _make_short_url(order_id, client_id)
        msg = _make_message(order_name, days_left, quiz_url)

        chat_id_final = contact["chat_id"]
        payload = {
            "channelId": contact["channel_id"],
            "chatType": contact["chat_type"],
            "text": msg,
        }
        if chat_id_final.startswith("@"):
            payload["username"] = chat_id_final.lstrip("@")
        else:
            payload["chatId"] = chat_id_final

        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    WAZZUP_API_URL,
                    headers={"Authorization": f"Bearer {api_key}",
                             "Content-Type": "application/json"},
                    json=payload,
                ) as r:
                    if r.status in (200, 201):
                        db._execute(
                            "UPDATE fishki_reminders SET status='sent', sent_at=NOW() WHERE order_id=%s",
                            (order_id,),
                        )
                        sent_ok += 1
                        logger.info(f"fishki_reminder ✅ {company} ({order_name})")
                    else:
                        body = await r.text()
                        db._execute(
                            "UPDATE fishki_reminders SET status='error', error=%s WHERE order_id=%s",
                            (f"HTTP {r.status}: {body[:200]}", order_id),
                        )
                        failed += 1
                        logger.error(f"fishki_reminder ❌ {company}: HTTP {r.status} {body[:200]}")
        except Exception as e:
            db._execute(
                "UPDATE fishki_reminders SET status='error', error=%s WHERE order_id=%s",
                (f"exception: {e}", order_id),
            )
            failed += 1
            logger.error(f"fishki_reminder ❌ {company}: {e}")

        if progress_cb and idx % 5 == 0:
            try:
                await progress_cb(idx, total, sent_ok, failed)
            except Exception as _e:
                logger.warning(f"fishki_reminder progress_cb: {_e}")

        if idx < total and not STOP_EVENT.is_set():
            delay = random.uniform(DELAY_MIN_S, DELAY_MAX_S)
            try:
                await asyncio.wait_for(STOP_EVENT.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    return {
        "total": total,
        "sent": sent_ok,
        "failed": failed,
        "stopped": STOP_EVENT.is_set(),
    }
