"""Реактивация сайт-лидов: медленная отправка предложения в MAX/Telegram через Wazzup.

План: plans/2026-09-17-реактивация-сайт-лидов-спеццена-мессенджеры.md (репо «второй мозг»).

Очередь (`reactivation_queue`), тексты (`reactivation_texts`) и настройки кампании
(`bot_settings`, ключ `reactivation:<кампания>`) заливает скрипт из «второго мозга».
Бот только отправляет: тик раз в минуту шлёт не больше одного сообщения.

Порядок защиты на каждую отправку:
  1. кампания включена, канал не на паузе;
  2. окно пн–пт 10:00–17:00 МСК, дневной лимит, случайный интервал между отправками;
  3. раз в день цены «Спец.» и «Цена опт» в МойСклад совпадают с ценами в текстах;
  4. сделка не реализована и не закрыта с причиной «Не целевой»/«Логистика невозможна»;
  5. за 7 дней в чате нет переписки, а в amoCRM нет звонков и сообщений.
Если amoCRM или МойСклад не ответили — отправка откладывается, а не идёт вслепую.

Строка переводится в `sending` ДО запроса в Wazzup: при падении или таймауте между
запросом и ответом сообщение не уйдёт клиенту второй раз. Такая строка становится
`sent`, когда в `wazzup_messages` появляется исходящее эхо, иначе остаётся в сводке
«без подтверждения». Повтор — только если соединение с Wazzup не установилось.
"""
import asyncio
import json
import logging
import os
import random
from datetime import datetime, timedelta, timezone

import aiohttp

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))
SETTINGS_PREFIX = "reactivation:"
WAZZUP_API_URL = "https://api.wazzup24.com/v3/message"
AMO_BASE = f"https://{os.getenv('AMO_SUBDOMAIN', 'victorfishtobiz')}.amocrm.ru/api/v4"
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

CHANNEL_IDS = {
    "max": "1d5bc70a-7ca6-4895-8d1f-9690cf448214",
    "telegram": "ddd24a95-9304-4098-a320-3e47fcd1020a",
}
ACTIVITY_EVENT_TYPES = ["incoming_call", "outgoing_call", "entity_direct_message",
                        "incoming_chat_message", "outgoing_chat_message"]
EXCLUDED_LOSS_REASONS = {23426482, 23426478}   # Не целевой, Логистика невозможна
OBJECTION_FIELD_ID = 2246177                    # «Тип возражения»
NETWORK_ATTEMPTS = 3


class Postpone(Exception):
    """Внешняя система не ответила — отправку откладываем, строку не трогаем."""


# ─── чистые функции (покрыты тестами) ────────────────────────────────────────

def in_window(now_msk: datetime, cfg: dict) -> bool:
    """Рабочий день из cfg['days'] (0=пн) и время в [start, end)."""
    if now_msk.weekday() not in cfg.get("days", [0, 1, 2, 3, 4]):
        return False
    hm = now_msk.strftime("%H:%M")
    return cfg.get("start", "10:00") <= hm < cfg.get("end", "17:00")


def block_index(sent_total: int, block_size: int, n_templates: int) -> int:
    """Номер текста: меняется каждые block_size фактических отправок."""
    return (sent_total // max(block_size, 1)) % max(n_templates, 1)


def render(template: str, manager_first_name: str) -> str:
    return template.replace("{manager}", manager_first_name)


def build_payload(row: dict, text: str, crm_user_mode: str) -> dict:
    payload = {
        "channelId": CHANNEL_IDS[row["chat_type"]],
        "chatType": row["chat_type"],
        "chatId": str(row["chat_id"]),
        "text": text,
    }
    if crm_user_mode == "responsible" and row.get("responsible_user_id"):
        payload["crmUserId"] = str(row["responsible_user_id"])
    return payload


def price_mismatches(expected: dict, actual: dict) -> list:
    """expected/actual: {код: {"spec": 1490, "opt": 1630}} → список расхождений."""
    out = []
    for code, exp in expected.items():
        act = actual.get(code)
        if act is None:
            out.append(f"{code}: не найден в МойСклад")
            continue
        for kind in ("spec", "opt"):
            if exp.get(kind) is not None and round(act.get(kind) or 0) != round(exp[kind]):
                out.append(f"{code} {kind}: в текстах {exp[kind]}, в МойСклад {act.get(kind)}")
    return out


def lead_blocked(lead: dict) -> str | None:
    """Причина не писать по статусу сделки или None."""
    if lead.get("status_id") == 142:
        return "сделка реализована"
    if lead.get("status_id") == 143 and lead.get("loss_reason_id") in EXCLUDED_LOSS_REASONS:
        return "закрыта: нецелевой/логистика"
    for f in lead.get("custom_fields_values") or []:
        if f.get("field_id") == OBJECTION_FIELD_ID and any(
                v.get("value") == "Логистика невозможна" for v in f.get("values") or []):
            return "возражение: логистика невозможна"
    return None


# ─── БД ──────────────────────────────────────────────────────────────────────

def ensure_tables(db) -> None:
    db._execute("""CREATE TABLE IF NOT EXISTS reactivation_queue (
        id SERIAL PRIMARY KEY,
        campaign TEXT NOT NULL,
        seq INT NOT NULL,
        lead_id BIGINT,
        contact_id BIGINT,
        manager_first_name TEXT NOT NULL,
        responsible_user_id BIGINT,
        chat_type TEXT NOT NULL,
        chat_id TEXT NOT NULL,
        all_chat_ids TEXT[] NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'pending',
        block INT,
        text TEXT,
        attempts INT NOT NULL DEFAULT 0,
        sent_at TIMESTAMPTZ,
        result TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (campaign, chat_type, chat_id)
    )""")
    db._execute("""CREATE TABLE IF NOT EXISTS reactivation_texts (
        campaign TEXT NOT NULL,
        block INT NOT NULL,
        template TEXT NOT NULL,
        PRIMARY KEY (campaign, block)
    )""")


def _load_cfg(db, campaign: str) -> dict:
    row = db._fetchone("SELECT value FROM bot_settings WHERE key=%s", (SETTINGS_PREFIX + campaign,))
    return json.loads(row["value"]) if row and row.get("value") else {}


def _save_cfg(db, campaign: str, cfg: dict) -> None:
    v = json.dumps(cfg, ensure_ascii=False)
    db._execute("""INSERT INTO bot_settings (key, value) VALUES (%s, %s)
                   ON CONFLICT (key) DO UPDATE SET value=%s""",
                (SETTINGS_PREFIX + campaign, v, v))


def _campaigns(db) -> list:
    rows = db._fetchall("SELECT key FROM bot_settings WHERE key LIKE %s", (SETTINGS_PREFIX + "%",))
    return [r["key"][len(SETTINGS_PREFIX):] for r in rows]


# ─── внешние системы ─────────────────────────────────────────────────────────

async def _amo_get(session, path: str, params) -> dict:
    """204 = пусто ({}), 404 = нет сущности (None); любая другая неудача → Postpone."""
    headers = {"Authorization": f"Bearer {os.getenv('AMO_ACCESS_TOKEN', '')}"}
    try:
        async with session.get(AMO_BASE + path, headers=headers, params=params) as r:
            if r.status == 204:
                return {}
            if r.status == 404:
                return None
            if r.status == 200:
                return await r.json(content_type=None)
            raise Postpone(f"amoCRM {path}: HTTP {r.status}")
    except Postpone:
        raise
    except Exception as e:
        raise Postpone(f"amoCRM {path}: {type(e).__name__}: {e}")


async def _amo_activity_7d(session, lead_id: int, contact_id: int | None) -> bool:
    since = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp())
    for entity, eid in (("lead", lead_id), ("contact", contact_id)):
        if not eid:
            continue
        params = [("filter[entity]", entity), ("filter[entity_id][]", str(eid)),
                  ("filter[created_at][from]", str(since)), ("limit", "1")]
        params += [("filter[type][]", t) for t in ACTIVITY_EVENT_TYPES]
        d = await _amo_get(session, "/events", params) or {}
        if (d.get("_embedded") or {}).get("events"):
            return True
    return False


async def _ms_prices(session, codes: list) -> dict:
    headers = {"Authorization": f"Bearer {os.getenv('MOYSKLAD_TOKEN', '')}",
               "Accept-Encoding": "gzip"}
    out = {}
    for code in codes:
        try:
            async with session.get(f"{MS_BASE}/entity/product", headers=headers,
                                   params={"filter": f"code={code}"}) as r:
                if r.status != 200:
                    raise Postpone(f"МойСклад code={code}: HTTP {r.status}")
                d = await r.json(content_type=None)
        except Postpone:
            raise
        except Exception as e:
            raise Postpone(f"МойСклад code={code}: {type(e).__name__}: {e}")
        for p in d.get("rows", []):
            sp = {(x.get("priceType") or {}).get("name"): x.get("value", 0) / 100
                  for x in p.get("salePrices", [])}
            out[code] = {"spec": sp.get("Спец."), "opt": sp.get("Цена опт")}
    return out


async def _notify_owner(app, text: str) -> None:
    owner = os.getenv("OWNER_CHAT_ID", "").strip()
    if not owner:
        logger.warning("reactivation: OWNER_CHAT_ID не задан, сообщение: %s", text)
        return
    try:
        await app.bot.send_message(chat_id=int(owner), text=text)   # без parse_mode: имена из CRM
    except Exception as e:
        logger.error("reactivation: не отправил собственнику: %s", e)


# ─── тик ─────────────────────────────────────────────────────────────────────

_tables_ready = False


def _confirm_by_echo(db, campaign: str) -> None:
    """'sending' старше 10 мин с исходящим эхо в чате после отправки → 'sent'."""
    db._execute(
        """UPDATE reactivation_queue q SET status='sent', result=COALESCE(q.result,'') || ' | эхо найдено'
           WHERE q.campaign=%s AND q.status='sending' AND q.sent_at < now() - interval '10 minutes'
           AND EXISTS (SELECT 1 FROM wazzup_messages m WHERE m.chat_id = q.chat_id AND m.is_outbound
                       AND m.sent_at >= (q.sent_at AT TIME ZONE 'UTC') - interval '1 minute')""",
        (campaign,))


async def tick(app, db) -> None:
    global _tables_ready
    if not _tables_ready:
        ensure_tables(db)
        _tables_ready = True
    for campaign in _campaigns(db):
        try:
            await _tick_campaign(app, db, campaign)
        except Exception as e:
            logger.error("reactivation[%s]: %s", campaign, e, exc_info=True)


async def _tick_campaign(app, db, campaign: str) -> None:
    cfg = _load_cfg(db, campaign)
    if not cfg.get("enabled"):
        return
    _confirm_by_echo(db, campaign)
    now = datetime.now(MSK)
    today = now.date().isoformat()

    if not in_window(now, cfg):
        if now.weekday() in cfg.get("days", [0, 1, 2, 3, 4]) and now.strftime("%H:%M") >= cfg.get("end", "17:00") \
                and cfg.get("summary_date") != today:
            await _daily_summary(app, db, campaign, cfg, today)
        return

    if cfg.get("next_send_at") and now < datetime.fromisoformat(cfg["next_send_at"]):
        return

    sent_today = db._fetchone(
        """SELECT count(*) AS n FROM reactivation_queue WHERE campaign=%s AND status IN ('sent','sending')
           AND (sent_at AT TIME ZONE 'Europe/Moscow')::date = %s""", (campaign, today))["n"]
    if sent_today >= cfg.get("daily_cap", 25):
        return

    paused = cfg.get("paused_channels", [])
    templates = db._fetchall("SELECT block, template FROM reactivation_texts WHERE campaign=%s ORDER BY block",
                             (campaign,))
    if not templates:
        logger.error("reactivation[%s]: нет текстов", campaign)
        return

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            if cfg.get("price_check_date") != today:
                actual = await _ms_prices(session, list(cfg.get("expected_prices", {}).keys()))
                bad = price_mismatches(cfg.get("expected_prices", {}), actual)
                if bad:
                    cfg["enabled"] = False
                    _save_cfg(db, campaign, cfg)
                    await _notify_owner(app, "Реактивация сайт-лидов остановлена: цены в МойСклад "
                                             "не совпадают с текстами.\n" + "\n".join(bad))
                    return
                cfg["price_check_date"] = today
                _save_cfg(db, campaign, cfg)

            for _ in range(5):   # до 5 пропусков за тик, отправка — максимум одна
                row = db._fetchone(
                    """SELECT * FROM reactivation_queue WHERE campaign=%s AND status='pending'
                       AND NOT (chat_type = ANY(%s)) ORDER BY seq LIMIT 1""", (campaign, paused))
                if not row:
                    if not db._fetchone("SELECT 1 AS x FROM reactivation_queue WHERE campaign=%s "
                                        "AND status='pending' LIMIT 1", (campaign,)):
                        cfg["enabled"] = False
                        _save_cfg(db, campaign, cfg)
                        await _daily_summary(app, db, campaign, cfg, today, final=True)
                    return
                reason = await _skip_reason(session, db, row)
                if reason:
                    db._execute("UPDATE reactivation_queue SET status='skipped', result=%s WHERE id=%s",
                                (reason, row["id"]))
                    continue
                await _send(app, db, session, campaign, cfg, row, templates)
                return
        except Postpone as e:
            logger.warning("reactivation[%s]: отложено: %s", campaign, e)
            cfg["next_send_at"] = (now + timedelta(minutes=10)).isoformat()
            _save_cfg(db, campaign, cfg)


async def _skip_reason(session, db, row: dict) -> str | None:
    chats = list(row.get("all_chat_ids") or []) or [row["chat_id"]]
    recent = db._fetchone(
        """SELECT 1 AS x FROM wazzup_messages WHERE chat_id = ANY(%s)
           AND sent_at > (now() AT TIME ZONE 'UTC') - interval '7 days' LIMIT 1""",
        (chats,))
    if recent:
        return "переписка за 7 дней"
    lead = await _amo_get(session, f"/leads/{row['lead_id']}", None)
    if lead is None:
        return "сделка не найдена в amoCRM"
    blocked = lead_blocked(lead)
    if blocked:
        return blocked
    if await _amo_activity_7d(session, row["lead_id"], row.get("contact_id")):
        return "звонок/сообщение в amoCRM за 7 дней"
    return None


async def _send(app, db, session, campaign: str, cfg: dict, row: dict, templates: list) -> None:
    sent_total = db._fetchone(
        "SELECT count(*) AS n FROM reactivation_queue WHERE campaign=%s AND status IN ('sent','sending')",
        (campaign,))["n"]
    blk = block_index(sent_total, cfg.get("block_size", 5), len(templates))
    text = render(templates[blk]["template"], row["manager_first_name"])
    payload = build_payload(row, text, cfg.get("crm_user_mode", "none"))

    db._execute("""UPDATE reactivation_queue SET status='sending', block=%s, text=%s, sent_at=now(),
                   attempts=attempts+1 WHERE id=%s""", (blk, text, row["id"]))
    lo, hi = cfg.get("interval_min", [8, 15])
    now = datetime.now(MSK)
    try:
        async with session.post(WAZZUP_API_URL, json=payload, headers={
                "Authorization": f"Bearer {os.getenv('WAZZUP_API_KEY', '')}",
                "Content-Type": "application/json"}) as r:
            body = (await r.text())[:300]
            status = r.status
    except aiohttp.ClientConnectorError as e:
        # Соединение с Wazzup не установилось — запрос точно не ушёл, можно повторить.
        attempts = row["attempts"] + 1
        final = attempts >= NETWORK_ATTEMPTS
        db._execute("UPDATE reactivation_queue SET status=%s, result=%s WHERE id=%s",
                    ("error" if final else "pending", f"connect:{e}"[:300], row["id"]))
        cfg["next_send_at"] = (now + timedelta(minutes=10)).isoformat()
        _save_cfg(db, campaign, cfg)
        if final:
            await _notify_owner(app, f"Реактивация: Wazzup недоступен, сделка {row['lead_id']} "
                                     f"({row['chat_type']}) не отправлена после {attempts} попыток: {e}")
        return
    except Exception as e:
        # Таймаут/обрыв после отправки: дошло ли сообщение — неизвестно. Повторно НЕ шлём,
        # строка остаётся в 'sending'; _confirm_by_echo подтвердит её по эхо-вебхуку.
        db._execute("UPDATE reactivation_queue SET result=%s WHERE id=%s",
                    (f"unknown:{type(e).__name__}:{e}"[:300], row["id"]))
        cfg["next_send_at"] = (now + timedelta(minutes=random.uniform(lo, hi))).isoformat()
        _save_cfg(db, campaign, cfg)
        return

    if status in (200, 201):
        db._execute("UPDATE reactivation_queue SET status='sent', result=%s WHERE id=%s", (body, row["id"]))
        cfg["next_send_at"] = (now + timedelta(minutes=random.uniform(lo, hi))).isoformat()
        _save_cfg(db, campaign, cfg)
        logger.info("reactivation[%s]: отправлено lead=%s %s блок=%s", campaign, row["lead_id"], row["chat_type"], blk)
        return

    db._execute("UPDATE reactivation_queue SET status='error', result=%s WHERE id=%s",
                (f"http:{status}:{body}", row["id"]))
    paused = set(cfg.get("paused_channels", [])) | {row["chat_type"]}
    cfg["paused_channels"] = sorted(paused)
    cfg["next_send_at"] = (now + timedelta(minutes=random.uniform(lo, hi))).isoformat()
    _save_cfg(db, campaign, cfg)
    await _notify_owner(app, f"Реактивация: канал {row['chat_type']} поставлен на паузу.\n"
                             f"Wazzup ответил {status} на сделку {row['lead_id']}: {body[:200]}")


async def _daily_summary(app, db, campaign: str, cfg: dict, today: str, final: bool = False) -> None:
    day = db._fetchall(
        """SELECT status, chat_type, count(*) AS n FROM reactivation_queue WHERE campaign=%s
           AND (COALESCE(sent_at, created_at) AT TIME ZONE 'Europe/Moscow')::date = %s
           AND status IN ('sent','error','sending') GROUP BY 1,2""", (campaign, today))
    total = {r["status"]: r["n"] for r in db._fetchall(
        "SELECT status, count(*) AS n FROM reactivation_queue WHERE campaign=%s GROUP BY 1", (campaign,))}
    replied = db._fetchone(
        """SELECT count(DISTINCT q.id) AS n FROM reactivation_queue q JOIN wazzup_messages m
           ON m.chat_id = q.chat_id AND NOT m.is_outbound AND m.sent_at > (q.sent_at AT TIME ZONE 'UTC')
           WHERE q.campaign=%s AND q.status='sent'""", (campaign,))["n"]
    cfg["summary_date"] = today
    _save_cfg(db, campaign, cfg)
    sent_day = sum(r["n"] for r in day if r["status"] == "sent")
    err_day = sum(r["n"] for r in day if r["status"] != "sent")
    if not final and not day:
        return
    by_ch = ", ".join(f"{r['chat_type']} {r['n']}" for r in day if r["status"] == "sent") or "—"
    head = "Реактивация сайт-лидов завершена." if final else "Реактивация сайт-лидов за сегодня."
    lines = [head,
             f"Отправлено сегодня: {sent_day} ({by_ch}), ошибок: {err_day}.",
             f"Всего: отправлено {total.get('sent', 0)}, пропущено {total.get('skipped', 0)}, "
             f"в очереди {total.get('pending', 0)}, ошибок {total.get('error', 0)}.",
             f"Ответили после сообщения: {replied}."]
    if cfg.get("paused_channels"):
        lines.append("На паузе: " + ", ".join(cfg["paused_channels"]) + ".")
    if total.get("sending"):
        lines.append(f"Без подтверждения отправки: {total['sending']} — проверить вручную.")
    await _notify_owner(app, "\n".join(lines))
