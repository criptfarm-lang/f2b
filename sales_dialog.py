"""Агент ведёт переписку с лидом и доводит до заказа.

План: `plans/2026-09-23-агент-ведёт-переписку-с-лидом.md` во «втором мозге».

Фаза 2–3: слушаем входящие по пилотным чатам, собираем контекст (переписка +
карточка amoCRM + живые цены и остатки МойСклад + база возражений), обезличиваем
и генерируем черновик ответа от имени Инессы Скляр. **Ничего не отправляем** —
отправка по кнопке собственника делается на Фазе 5, черновики копятся в
`sales_dialog_messages`.

Устройство повторяет соседние модули: конфиг в `bot_settings`, тик раз в минуту
из JobQueue (как `reactivation_campaign`), идемпотентность по
`UNIQUE (inbound_message_id, prompt_version)` (как `wazzup_classifier`).
"""
from __future__ import annotations

import base64
import json
import logging
import os
import random
import re
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp

from chat_anonymizer import anonymize, find_leaks
# Каналы и эндпоинт Wazzup общие с рассылкой реактивации — держим в одном месте.
from reactivation_campaign import CHANNEL_IDS, WAZZUP_API_URL

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))
# Раскачанный лид возвращается менеджеру, который вёл его до агента; если тот
# уволен (в amoCRM `rights.is_active = false`) — Инессе (собственник 24.09.2026).
INESSA_AMO_USER = 11544494
INESSA_TG_CHAT = 1435133158        # moysklad.PDZ_MANAGER_TG_IDS["скляр"]
# amoCRM id → Telegram, канон состава — moysklad.PDZ_MANAGER_TG_IDS.
MANAGER_TG = {
    11544494: 1435133158,          # Инесса Скляр
    12625622: 595181729,           # Карина Баласанян
    12788698: 8021969241,          # Елена Мерзлякова
    13665786: 683079752,           # Денис Коликов
    13746010: 649712597,           # Ирина Дьяченко
}
# Кто подписывает черновик. Карточка уходит менеджеру партии, и писать он должен
# от своего имени: 28.09.2026 Денису пришли сообщения от имени Инессы. Пол нужен
# не для вежливости, а для сверки: глаголы о себе должны быть в его роде.
PERSONAS = {
    11544494: {"first": "Инесса", "full": "Инесса Скляр", "gender": "f"},
    13665786: {"first": "Денис", "full": "Денис Коликов", "gender": "m"},
    12625622: {"first": "Карина", "full": "Карина Баласанян", "gender": "f"},
    12788698: {"first": "Елена", "full": "Елена Мерзлякова", "gender": "f"},
    13746010: {"first": "Ирина", "full": "Ирина Дьяченко", "gender": "f"},
}
# Карточку без адресата (партия собственника) по-прежнему подписывает Инесса.
DEFAULT_PERSONA = PERSONAS[11544494]

GENDER_RULE = {
    "f": ("Ты женщина. Все глаголы о себе — в женском роде: «поняла», «прошла», «уточнила», "
          "«посмотрела», «отправила». Мужской род выдаёт, что пишет не {first}."),
    "m": ("Ты мужчина. Все глаголы о себе — в мужском роде: «понял», «прошёл», «уточнил», "
          "«посмотрел», «отправил». Женский род выдаёт, что пишет не {first}."),
}


def persona_for(amo_id) -> dict:
    return PERSONAS.get(amo_id) or DEFAULT_PERSONA


def system_prompt(persona: dict) -> str:
    """Системный промпт под конкретного менеджера: имя, подпись и род глаголов."""
    raw = (PROMPTS_DIR / "sales_dialog_system.md").read_text(encoding="utf-8")
    rule = GENDER_RULE[persona["gender"]].format(first=persona["first"])
    return (raw.replace("{{AGENT_FULL}}", persona["full"])
               .replace("{{AGENT_FIRST}}", persona["first"])
               .replace("{{GENDER_RULE}}", rule))


ATTRACT_PIPELINE = 10873622        # воронка ПРИВЛЕЧЕНИЕ
ATTRACT_FIRST_STATUS = 85554794    # этап «Первичный контакт»
# Новые сайт-лиды с 29.09.2026 сразу уходят агенту (план
# `plans/2026-09-29-новые-сайт-лиды-ведёт-агент.md`): воронка «Эф», её
# «Первичный контакт» и пользователь «Эф» – там же, где пилот 23.09.2026.
AGENT_PIPELINE = 11331778
AGENT_FIRST_STATUS = 88823510
AGENT_AMO_USER = 13548094
AGENT_TAG_ID = 786137              # «ведёт агент»
SITE_TAG_IDS = {782551, 782507}    # «сайт», «сайт заявка»
AMO_FIELD_MAX_ID = 2244321         # MaxId_WZ
AMO_FIELD_TG_ID = 2224427          # TelegramId_WZ
# Ответ отправки по номеру, когда Telegram-аккаунта у номера нет: по нему
# кнопка отдаёт лид Инессе на звонок.
NO_TG = "номер не нашёлся в Telegram"
MODEL = "claude-opus-5"
PROMPT_VERSION = "sales-dialog-v7"
# Версия кода — отдельно от версии промпта: менять PROMPT_VERSION ради
# наблюдаемости деплоя нельзя, он входит в ключ идемпотентности.
CODE_VERSION = "antispam-3009"
SETTINGS_PREFIX = "sales_dialog:"
PROMPTS_DIR = Path(__file__).parent / "prompts"

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
AMO_BASE = f"https://{os.getenv('AMO_SUBDOMAIN', 'victorfishtobiz')}.amocrm.ru/api/v4"

# Сколько последних сообщений отдаём модели. 25 было мало: в переговорах
# на два месяца требования клиента оставались за пределом окна.
HISTORY_LIMIT = 60
# Прайс обновляем не чаще раза в час — он меняется редко, а позиций под две сотни.
PRICE_TTL_SEC = 3600

_tables_ready = False
_price_cache: dict = {"at": None, "rows": []}
_tariffs_cache: dict | None = None


# ─── схема ────────────────────────────────────────────────────────────────────
def ensure_tables(db) -> None:
    db._execute("""CREATE TABLE IF NOT EXISTS sales_dialog_leads (
        id                  SERIAL PRIMARY KEY,
        campaign            TEXT NOT NULL,
        lead_id             BIGINT NOT NULL,
        contact_id          BIGINT,
        lead_name           TEXT,
        contact_name        TEXT,
        manager_first_name  TEXT,
        responsible_user_id BIGINT,
        chat_type           TEXT,
        chat_id             TEXT,
        all_chat_ids        TEXT[],
        status              TEXT NOT NULL DEFAULT 'candidate',
        replies_sent        INT NOT NULL DEFAULT 0,
        last_inbound_at     TIMESTAMPTZ,
        last_outbound_at    TIMESTAMPTZ,
        note                TEXT,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (campaign, lead_id)
    )""")
    db._execute("""CREATE TABLE IF NOT EXISTS sales_dialog_messages (
        id                 SERIAL PRIMARY KEY,
        campaign           TEXT NOT NULL,
        lead_id            BIGINT NOT NULL,
        chat_id            TEXT,
        inbound_message_id TEXT,
        inbound_text       TEXT,
        draft_text         TEXT,
        action             TEXT,
        reason             TEXT,
        need_check         TEXT,
        price_claims       JSONB,
        model              TEXT,
        prompt_version     TEXT,
        verdict            TEXT,
        final_text         TEXT,
        sent_at            TIMESTAMPTZ,
        created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (inbound_message_id, prompt_version)
    )""")
    # Чат, в который отвечаем, может отличаться от основного чата лида: клиент
    # пишет в тот мессенджер, в котором ему удобно. Канал храним рядом с чатом.
    db._execute("""ALTER TABLE sales_dialog_messages
                   ADD COLUMN IF NOT EXISTS chat_type TEXT""")
    # id карточки в Telegram: по нему находим черновик, когда собственник
    # отвечает на неё своим текстом. Без этого правка жила в одной ячейке на
    # пользователя и терялась, стоило прийти следующей карточке.
    db._execute("""ALTER TABLE sales_dialog_messages
                   ADD COLUMN IF NOT EXISTS tg_message_id BIGINT""")
    # Уступки по цене: что предложено, прайс и порог — чтобы собственник видел
    # цифры на карточке и мог судить о торге, а не только о тексте.
    db._execute("""ALTER TABLE sales_dialog_messages
                   ADD COLUMN IF NOT EXISTS bargain JSONB""")
    # Кому вернуть лид, когда агент его раскачает.
    db._execute("""ALTER TABLE sales_dialog_leads
                   ADD COLUMN IF NOT EXISTS prev_responsible_user_id BIGINT""")
    # Дата, раньше которой лид в работу не берём: пул набирается впрок, а
    # разбирается по столько в день, сколько человек успевает утвердить.
    db._execute("""ALTER TABLE sales_dialog_leads
                   ADD COLUMN IF NOT EXISTS activate_on DATE""")
    # Чья это карточка. NULL — собственника, иначе amoCRM id менеджера: с
    # 29.09.2026 партии спящих разбирают Коликов и Скляр, каждый свою.
    db._execute("""ALTER TABLE sales_dialog_leads
                   ADD COLUMN IF NOT EXISTS assignee_amo_id BIGINT""")
    # Кнопка «Забрать»: когда менеджер взял диалог себе и какая сделка создана.
    db._execute("""ALTER TABLE sales_dialog_leads
                   ADD COLUMN IF NOT EXISTS taken_at TIMESTAMPTZ""")
    db._execute("""ALTER TABLE sales_dialog_leads
                   ADD COLUMN IF NOT EXISTS new_lead_id BIGINT""")
    # Мягкий отказ («сейчас не актуально»): отвечать можно, первым – больше никогда.
    db._execute("""ALTER TABLE sales_dialog_leads
                   ADD COLUMN IF NOT EXISTS no_initiative BOOLEAN NOT NULL DEFAULT false""")
    # Адресат карточки фиксируется в самой карточке: лид могут передать другому,
    # а решение по уже отправленной карточке должен принимать тот, кому её послали.
    db._execute("""ALTER TABLE sales_dialog_messages
                   ADD COLUMN IF NOT EXISTS assignee_amo_id BIGINT""")
    # Сколько раз уже напоминали по этой карточке и когда в последний раз.
    db._execute("""ALTER TABLE sales_dialog_messages
                   ADD COLUMN IF NOT EXISTS ping_count INT NOT NULL DEFAULT 0""")
    db._execute("""ALTER TABLE sales_dialog_messages
                   ADD COLUMN IF NOT EXISTS pinged_at TIMESTAMPTZ""")
    # Телефон из заявки: у трети сайт-лидов чата в мессенджере нет, и первое
    # сообщение уходит в Telegram по номеру (проверено 29.09.2026).
    db._execute("""ALTER TABLE sales_dialog_leads
                   ADD COLUMN IF NOT EXISTS phone TEXT""")
    db._execute("""CREATE INDEX IF NOT EXISTS sales_dialog_leads_status_idx
                   ON sales_dialog_leads (campaign, status)""")
    db._execute("""CREATE INDEX IF NOT EXISTS sales_dialog_messages_lead_idx
                   ON sales_dialog_messages (campaign, lead_id, created_at)""")
    # Отбор очереди ищет переписку лида по chat_id. Без индекса каждый тик
    # перебирал всю wazzup_messages по нескольку раз на лид, упирался в
    # statement_timeout и держал event loop бота по 50 с – водители не могли
    # открыть чеклист маршрута (29.09–01.10.2026). На бою создан CONCURRENTLY.
    db._execute("""CREATE INDEX IF NOT EXISTS idx_wazzup_messages_chat_sent
                   ON wazzup_messages (chat_id, sent_at DESC)""")


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


# ─── окно работы ──────────────────────────────────────────────────────────────
def in_window(now_msk: datetime, cfg: dict) -> bool:
    """Пишем клиентам только в рабочее время и только по будням."""
    if now_msk.weekday() >= 5 and not cfg.get("weekends"):
        return False
    return cfg.get("start_hour", 9) <= now_msk.hour < cfg.get("end_hour", 19)


def workdays_ago(now: datetime, days: int) -> datetime:
    """Момент, отстоящий на `days` рабочих дней назад (выходные не считаются).

    Нужен для паузы между двумя сообщениями в молчащий диалог: «пара рабочих
    дней» в пятницу означает среду, а не воскресенье.
    """
    d = now
    left = max(0, days)
    while left > 0:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            left -= 1
    return d


# ─── антиспам: правила по каналам ─────────────────────────────────────────────
# План `plans/2026-09-30-антиспам-правила-переписки-агента.md`. 30.09.2026 MAX
# заблокировал номер F2B за подозрение на спам: накануне ушло 117 сообщений в чаты
# без единого входящего. Инициатива (агент пишет первым: `silent:` и `first:`)
# теперь идёт через общий для всех кампаний бюджет канала. Ответы клиенту, который
# написал сам, бюджетом не ограничены. Настройки – `bot_settings`, ключ
# `sales_dialog:_channels`, меняются без выкатки.
CHANNELS_KEY = "_channels"
DEFAULT_CHANNELS = {
    "limits": {"telegram": 10, "max": 0, "whatsapp": 0},   # инициатив в день на канал
    "window": [10, 17],                  # часы МСК, пн–пт
    "interval_min": [20, 45],            # случайная пауза между инициативами в канале
    "first_touch_channels": ["telegram"],  # куда первым пишем новому сайт-лиду
    "first_touch_max_days": 7,           # заявка старше – менеджеру, а не агенту
    "paused": {},                        # канал -> причина; снимает только собственник
    "next_at": {},                       # канал -> ISO-время, раньше которого не пишем
}
LINK_RE = re.compile(r"https?://|www\.|t\.me/|\b[\w-]+\.(ru|рф|com|group|store)\b", re.I)
HARD_STOP_RE = re.compile(
    r"не\s+пиш(и|ите)|больше\s+не\s+пиш|не\s+беспоко|отпиш(и|ите)|удалите|"
    r"хватит\s+писать|прекратите|не\s+присылайте|\bспам", re.I)
SOFT_STOP_RE = re.compile(
    r"не\s*актуальн|не\s+интересн|не\s+интересует|не\s+нужн|не\s+требуется|не\s+надо", re.I)
SPAM_RE = re.compile(r"\bспам", re.I)
WAZZUP_TRANSPORT = {"tgapi": "telegram", "telegram": "telegram", "max": "max",
                    "whatsapp": "whatsapp"}


def channel_policy(db) -> dict:
    """Правила каналов: значения по умолчанию, поверх – то, что лежит в настройках."""
    saved = _load_cfg(db, CHANNELS_KEY)
    p = json.loads(json.dumps(DEFAULT_CHANNELS))
    for k, v in saved.items():
        if isinstance(v, dict) and isinstance(p.get(k), dict):
            p[k].update(v)
        else:
            p[k] = v
    return p


def is_initiative(row: dict) -> bool:
    """Агент пишет первым: оживление молчащего диалога или первое касание лида."""
    key = str(row.get("inbound_message_id") or row.get("message_id") or "")
    return key.startswith(("silent:", "first:"))


def row_channel(row: dict) -> str:
    # Лид без чата получает первое сообщение по номеру – это Telegram.
    return row.get("chat_type") or "telegram"


def refusal_kind(text: str | None) -> str | None:
    """«hard» – просят не писать; «soft» – сейчас не нужно; None – не отказ."""
    t = text or ""
    if HARD_STOP_RE.search(t):
        return "hard"
    if SOFT_STOP_RE.search(t):
        return "soft"
    return None


def initiatives_used(db) -> dict:
    """Сколько инициатив по каналам уже выдано сегодня (черновики и отправленные)."""
    rows = db._fetchall("""
        SELECT coalesce(chat_type, 'telegram') AS ch, count(*) AS n
        FROM sales_dialog_messages
        WHERE (inbound_message_id LIKE %s OR inbound_message_id LIKE %s)
          AND verdict IN ('draft', 'edited', 'sent')
          AND created_at >= date_trunc('day', now() AT TIME ZONE 'Europe/Moscow')
                            AT TIME ZONE 'Europe/Moscow'
        GROUP BY 1""", ("silent:%", "first:%"))
    return {r["ch"]: r["n"] for r in rows}


def initiative_block(row: dict, policy: dict, used: dict, now: datetime) -> str | None:
    """Почему агенту нельзя сейчас написать первым; None – можно."""
    ch = row_channel(row)
    if ch in (policy.get("paused") or {}):
        return f"канал {ch} на паузе: {policy['paused'][ch]}"
    lo, hi = policy.get("window") or [10, 17]
    if now.weekday() >= 5 or not (lo <= now.hour < hi):
        return "вне окна инициативы"
    if used.get(ch, 0) >= int((policy.get("limits") or {}).get(ch, 0)):
        return f"дневной лимит канала {ch}"
    nxt = (policy.get("next_at") or {}).get(ch)
    if nxt and now < datetime.fromisoformat(nxt):
        return f"пауза между сообщениями в {ch}"
    if row.get("source") == "first":
        if ch not in (policy.get("first_touch_channels") or []):
            return f"первым в {ch} новому лиду не пишем"
        taken = row.get("taken_at_lead")
        days = int(policy.get("first_touch_max_days", 7))
        if taken and now - taken > timedelta(days=days):
            return f"заявка старше {days} дней"
    return None


def note_initiative(db, policy: dict, ch: str, now: datetime) -> None:
    """После инициативы канал молчит случайные 20–45 минут: без пачек."""
    lo, hi = policy.get("interval_min") or [20, 45]
    at = (now + timedelta(minutes=random.randint(lo, hi))).isoformat()
    policy.setdefault("next_at", {})[ch] = at
    saved = _load_cfg(db, CHANNELS_KEY)
    saved.setdefault("next_at", {})[ch] = at
    _save_cfg(db, CHANNELS_KEY, saved)


async def pause_channel(app, db, ch: str, reason: str) -> None:
    """Ставит инициативу в канале на паузу и один раз говорит об этом собственнику."""
    saved = _load_cfg(db, CHANNELS_KEY)
    paused = saved.setdefault("paused", {})
    if ch in paused:
        return
    paused[ch] = f"{datetime.now(MSK):%d.%m %H:%M} {reason}"[:200]
    _save_cfg(db, CHANNELS_KEY, saved)
    logger.warning("sales_dialog: канал %s на паузе: %s", ch, reason)
    if app:
        await notify_owner(app, f"Агент: инициатива в {ch} на паузе – {reason}. "
                                f"Ответы клиентам идут. Снять паузу – команда собственника.")


_channels_cache: dict = {"at": None, "states": {}}


async def channel_states(session) -> dict:
    """Состояние каналов Wazzup (кэш 5 минут): {'telegram': 'active', …}."""
    now = datetime.now(timezone.utc)
    if _channels_cache["at"] and (now - _channels_cache["at"]).total_seconds() < 300:
        return _channels_cache["states"]
    states = {}
    try:
        async with session.get("https://api.wazzup24.com/v3/channels", headers={
                "Authorization": f"Bearer {os.getenv('WAZZUP_API_KEY', '')}"},
                timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status == 200:
                for c in await r.json():
                    ch = WAZZUP_TRANSPORT.get(c.get("transport"))
                    if ch:
                        states[ch] = c.get("state")
    except Exception as e:
        logger.warning("sales_dialog: состояние каналов Wazzup не прочиталось: %s", e)
    _channels_cache.update(at=now, states=states)
    return states


async def gate_initiatives(app, db, session, rows: list, now: datetime) -> list:
    """Пропускает ответы как есть, инициативы – только в пределах правил каналов.

    Первое касание, которое правила не пускают насовсем (канал не тот, заявка
    старая), отдаёт лид менеджеру: агент по нему больше не пишет.
    """
    if not any(is_initiative(r) for r in rows):
        return rows
    policy = channel_policy(db)
    states = await channel_states(session)
    for ch, limit in (policy.get("limits") or {}).items():
        st = states.get(ch)
        if int(limit) > 0 and st and st != "active" and ch not in (policy.get("paused") or {}):
            await pause_channel(app, db, ch, f"канал в Wazzup в состоянии {st}")
            policy = channel_policy(db)
    used = initiatives_used(db)
    out, took = [], False
    for r in rows:
        if not is_initiative(r):
            out.append(r)
            continue
        if took:
            continue                     # одна инициатива за тик: интервал решает дальше
        why = initiative_block(r, policy, used, now)
        if why is None:
            out.append(r)
            took = True
        elif why.startswith(("первым в", "заявка старше")):
            db._execute("""UPDATE sales_dialog_leads SET status='handed', note=%s
                           WHERE campaign=%s AND lead_id=%s AND status='active'""",
                        (f"антиспам: {why}", r["campaign"], r["lead_id"]))
            if app:
                await hand_to_manager(app, db, {"campaign": r["campaign"], "lead_id": r["lead_id"]},
                                      task_text=f"Связаться с клиентом: заявка с сайта, агент "
                                                f"первым не пишет ({why})")
    return out


# ─── внешние системы ──────────────────────────────────────────────────────────
MRM_DISCOUNT = 100.0


def is_mrm(name: str) -> bool:
    u = (name or "").upper()
    return "ОХЛ" in u and "МУРМАНСК" in u


def mrm_price(name: str, price: float | None) -> float | None:
    """Охлаждённая мурманская рыба идёт клиенту на 100 ₽ дешевле, чем в МойСкладе.

    Правило собственника от 23.09.2026. Скидку применяем прямо в справочнике,
    который уходит и в промпт, и в сверку цен, — иначе агент назовёт одну цифру,
    а проверка потребует другую и завернёт черновик.
    """
    if price is None:
        return None
    if is_mrm(name) and price > MRM_DISCOUNT:
        return round(price - MRM_DISCOUNT, 2)
    return price


_mrm_guard: dict = {"at": None, "blocked": set()}
MRM_GUARD_TTL_SEC = 3600


async def mrm_guard(session, db, rows: list) -> list:
    """Снимает скидку МРМ там, где она уводит цену ниже порога ценообразования.

    Скидка применяется в справочнике до сверки цен, поэтому «прайс минус 100»
    проходил как прайсовая цена, а не как уступка: 25.09.2026 ПСГ Мурманск 5–6
    ушёл клиенту по 1690 при безубыточности 1729. Закупка мурманки меняется
    каждую неделю, поэтому решение не зашито в правило, а считается по текущим
    порогам — и кэшируется на час, чтобы не дёргать дашборд на каждый черновик.
    """
    now = datetime.now(timezone.utc)
    fresh = (_mrm_guard["at"] and (now - _mrm_guard["at"]).total_seconds() < MRM_GUARD_TTL_SEC)
    if not fresh:
        blocked = set()
        for p in [r for r in rows if r.get("mrm")]:
            base = (p.get("list") or {}).get("opt")
            if not base:
                continue
            fl = await price_floor(session, db, p["code"])
            floor = fl.get("floor_pay") or fl.get("floor")
            if fl.get("error") or floor is None:
                # Порог не посчитался — скидку не даём: дешевле отказаться от 100 ₽,
                # чем продать ниже себестоимости.
                blocked.add(p["code"])
                continue
            if float(base) - MRM_DISCOUNT < float(floor):
                blocked.add(p["code"])
        _mrm_guard.update({"at": now, "blocked": blocked})
        if blocked:
            logger.info("sales_dialog: скидка МРМ снята по %s", sorted(blocked))
    out = []
    for p in rows:
        if p.get("code") in _mrm_guard["blocked"]:
            listed = p.get("list") or {}
            out.append({**p, "opt": listed.get("opt"), "horeca": listed.get("horeca"),
                        "spec": listed.get("spec"), "mrm_discount_off": True})
        else:
            out.append(p)
    return out


async def _ms_price_rows(session: aiohttp.ClientSession) -> list:
    """Ассортимент МойСклад: код, имя, три типа цены, остаток. С часовым кэшем.

    Тип цены под направление: ОПТ — «Цена опт», HoReCa — «Цена продажи»
    (зафиксировано собственником 05.06.2026), «Спец.» — цена первого заказа.
    """
    now = datetime.now(timezone.utc)
    if _price_cache["at"] and (now - _price_cache["at"]).total_seconds() < PRICE_TTL_SEC:
        return _price_cache["rows"]

    token = os.getenv("MOYSKLAD_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}", "Accept-Encoding": "gzip"}
    rows, offset = [], 0
    while True:
        url = f"{MS_BASE}/entity/assortment?limit=1000&offset={offset}"
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=120)) as r:
            if r.status != 200:
                logger.warning("sales_dialog: МойСклад ассортимент %s", r.status)
                return _price_cache["rows"]
            data = await r.json()
        batch = data.get("rows", [])
        rows += batch
        if len(batch) < 1000:
            break
        offset += 1000

    out = []
    for r in rows:
        if (r.get("meta") or {}).get("type") not in ("product", "variant") or r.get("archived"):
            continue
        prices = {(p.get("priceType") or {}).get("name"): p.get("value", 0) / 100
                  for p in r.get("salePrices") or []}
        if not prices.get("Цена опт"):
            continue
        path = r.get("pathName") or ""
        name = r.get("name")
        out.append({"code": r.get("code"), "name": name,
                    "opt": mrm_price(name, prices.get("Цена опт")),
                    "horeca": mrm_price(name, prices.get("Цена продажи")),
                    "spec": mrm_price(name, prices.get("Спец.")),
                    # Цены до скидки — чтобы её можно было снять, если она уводит
                    # ниже порога: закупка мурманки меняется каждую неделю.
                    "mrm": is_mrm(name),
                    "list": {"opt": prices.get("Цена опт"), "horeca": prices.get("Цена продажи"),
                             "spec": prices.get("Спец.")},
                    "stock": round(r.get("stock") or 0, 1),
                    # Собственное производство делаем под заказ, поэтому нулевой остаток
                    # по нему — не «нет», а «сделаем». Привлечённые товары так нельзя:
                    # там ноль означает, что позицию надо закупить.
                    "own": path.startswith("ГОТОВАЯ ПРОДУКЦИЯ")})
    _price_cache.update({"at": now, "rows": out})
    logger.info("sales_dialog: прайс обновлён, позиций %s", len(out))
    return out


async def _amo_write(session: aiohttp.ClientSession, path: str, payload, method: str = "PATCH"):
    token = os.getenv("AMO_ACCESS_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        async with session.request(method, f"{AMO_BASE}{path}", headers=headers, json=payload,
                                   timeout=aiohttp.ClientTimeout(total=40)) as r:
            body = (await r.text())[:300]
            if r.status not in (200, 201):
                logger.warning("sales_dialog: amo %s %s → %s %s", method, path, r.status, body)
            return r.status in (200, 201)
    except Exception as e:
        logger.warning("sales_dialog: amo %s %s: %s", method, path, e)
        return False


async def _amo_user_active(session: aiohttp.ClientSession, user_id: int) -> bool:
    """Работает ли ещё этот менеджер: у уволенных в amoCRM `rights.is_active = false`."""
    token = os.getenv("AMO_ACCESS_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        async with session.get(f"{AMO_BASE}/users/{user_id}", headers=headers,
                               timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                return False
            d = await r.json()
            return bool((d.get("rights") or {}).get("is_active"))
    except Exception as e:
        logger.warning("sales_dialog: проверка пользователя %s: %s", user_id, e)
        return False


async def hand_to_manager(app, db, msg: dict,
                          task_text: str = "Ответить клиенту: диалог передан от агента") -> str:
    """Возврат раскачанного лида человеку: сделка, контакты, задача и сообщение в Telegram.

    Возвращаем тому менеджеру, который вёл лид до агента, — он помнит клиента и
    его историю. Если менеджер уже не работает, лид идёт Инессе (собственник,
    24.09.2026). До этого дня кнопка вообще ничего не передавала: ответственным
    оставался пользователь «Эф», задачи не было, человек ничего не знал.
    """
    lead_id = msg["lead_id"]
    lead = db._fetchone("""SELECT lead_name, contact_name, chat_type, prev_responsible_user_id
                           FROM sales_dialog_leads WHERE campaign=%s AND lead_id=%s""",
                        (msg["campaign"], lead_id))
    prev = (lead or {}).get("prev_responsible_user_id")
    done = []
    async with aiohttp.ClientSession() as session:
        target = prev if prev and await _amo_user_active(session, prev) else INESSA_AMO_USER
        if prev and target != prev:
            done.append("прежний менеджер не работает, отдаём Инессе")
        ok = await _amo_write(session, f"/leads/{lead_id}",
                              {"pipeline_id": ATTRACT_PIPELINE, "status_id": ATTRACT_FIRST_STATUS,
                               "responsible_user_id": target})
        done.append("сделка передана" if ok else "сделку передать не вышло")

        full = await _amo_lead(session, lead_id)
        contacts = [c["id"] for c in ((full.get("_embedded") or {}).get("contacts") or [])]
        if contacts:
            ok_c = await _amo_write(session, "/contacts",
                                    [{"id": c, "responsible_user_id": target} for c in contacts])
            done.append("контакты переданы" if ok_c else "контакты передать не вышло")

        till = int((datetime.now(MSK) + timedelta(hours=3)).timestamp())
        ok_t = await _amo_write(session, "/tasks", [{
            "entity_id": lead_id, "entity_type": "leads", "responsible_user_id": target,
            "task_type_id": 1, "complete_till": till,
            "text": task_text}], method="POST")
        done.append("задача поставлена" if ok_t else "задачу поставить не вышло")

    who = (lead or {}).get("lead_name") or f"сделка {lead_id}"
    text = (f"Передаю тебе диалог: {who}\n"
            + (f"{task_text}\n" if not task_text.startswith("Ответить клиенту") else "")
            + f"https://{os.getenv('AMO_SUBDOMAIN', 'victorfishtobiz')}.amocrm.ru/leads/detail/{lead_id}\n\n"
            f"Последнее от клиента: {(msg.get('inbound_text') or '—')[:300]}\n\n"
            f"Что готовил агент (не отправлено):\n{(msg.get('draft_text') or '—')[:600]}")
    chat = MANAGER_TG.get(target)
    if chat:
        try:
            await app.bot.send_message(chat, text)
            done.append("менеджеру написали")
        except Exception as e:
            logger.warning("sales_dialog: сообщение менеджеру не ушло lead=%s: %s", lead_id, e)
            done.append("сообщение в Telegram не ушло")
    else:
        done.append("Telegram менеджера неизвестен, не писали")
    return ", ".join(done)


AGENT_TAG = "ведёт агент"        # создавать теги через API в этом аккаунте нельзя, ставим готовый


async def take_by_manager(app, db, msg: dict, amo_user: int) -> str:
    """Менеджер забрал диалог себе: новая сделка, контакты, задача, агент молчит.

    Спящие лиды приходят из закрытых сделок — закрытую сделку не переоткрываем,
    её история остаётся историей, а работа продолжается в новой сделке воронки
    ПРИВЛЕЧЕНИЕ на том менеджере, который нажал кнопку (собственник 25.09.2026).
    """
    lead_id = msg["lead_id"]
    lead = db._fetchone("""SELECT lead_name, contact_name, chat_type, contact_id
                           FROM sales_dialog_leads WHERE campaign=%s AND lead_id=%s""",
                        (msg["campaign"], lead_id))
    name = (lead or {}).get("lead_name") or f"сделка {lead_id}"
    done, new_id = [], None
    async with aiohttp.ClientSession() as session:
        full = await _amo_lead(session, lead_id)
        contacts = [c["id"] for c in ((full.get("_embedded") or {}).get("contacts") or [])]
        if not contacts and (lead or {}).get("contact_id"):
            contacts = [lead["contact_id"]]
        payload = [{
            "name": f"{name} – агент разбудил",
            "pipeline_id": ATTRACT_PIPELINE, "status_id": ATTRACT_FIRST_STATUS,
            "responsible_user_id": amo_user,
            "_embedded": {"tags": [{"name": AGENT_TAG}],
                          **({"contacts": [{"id": c} for c in contacts]} if contacts else {})},
        }]
        created = await _amo_json(session, "/leads", payload, method="POST")
        new_id = (((created or {}).get("_embedded") or {}).get("leads") or [{}])[0].get("id")
        done.append(f"сделка {new_id} создана" if new_id else "сделку создать не вышло")

        if contacts:
            ok_c = await _amo_write(session, "/contacts",
                                    [{"id": c, "responsible_user_id": amo_user} for c in contacts])
            done.append("контакты переданы" if ok_c else "контакты передать не вышло")

        if new_id:
            note = (f"Диалог забран у агента.\n\nПоследнее от клиента: "
                    f"{(msg.get('inbound_text') or '—')[:500]}\n\n"
                    f"Черновик агента (не отправлен):\n{(msg.get('draft_text') or '—')[:1000]}\n\n"
                    f"Прежняя сделка: {lead_id}")
            await _amo_write(session, f"/leads/{new_id}/notes",
                             [{"note_type": "common", "params": {"text": note}}], method="POST")
            till = int((datetime.now(MSK) + timedelta(hours=3)).timestamp())
            ok_t = await _amo_write(session, "/tasks", [{
                "entity_id": new_id, "entity_type": "leads", "responsible_user_id": amo_user,
                "task_type_id": 1, "complete_till": till,
                "text": "Ответить клиенту: диалог забран у агента"}], method="POST")
            done.append("задача поставлена" if ok_t else "задачу поставить не вышло")

    db._execute("""UPDATE sales_dialog_leads SET status='taken', taken_at=now(), new_lead_id=%s
                   WHERE campaign=%s AND lead_id=%s""", (new_id, msg["campaign"], lead_id))
    if app and new_id:
        sub = os.getenv("AMO_SUBDOMAIN", "victorfishtobiz")
        await notify_owner(app, f"{name}: диалог забрал менеджер, сделка "
                                f"https://{sub}.amocrm.ru/leads/detail/{new_id}")
    return ", ".join(done)


async def _amo_json(session: aiohttp.ClientSession, path: str, payload, method: str = "POST"):
    """Как `_amo_write`, но возвращает тело ответа: при создании нужен id."""
    token = os.getenv("AMO_ACCESS_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        async with session.request(method, f"{AMO_BASE}{path}", headers=headers, json=payload,
                                   timeout=aiohttp.ClientTimeout(total=40)) as r:
            body = await r.text()
            if r.status not in (200, 201):
                logger.warning("sales_dialog: amo %s %s → %s %s", method, path, r.status, body[:300])
                return None
            return json.loads(body) if body else {}
    except Exception as e:
        logger.warning("sales_dialog: amo %s %s: %s", method, path, e)
        return None


async def _amo_lead(session: aiohttp.ClientSession, lead_id: int) -> dict:
    token = os.getenv("AMO_ACCESS_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = f"{AMO_BASE}/leads/{lead_id}?with=contacts"
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as r:
            return await r.json() if r.status == 200 else {}
    except Exception as e:
        logger.warning("sales_dialog: amoCRM лид %s: %s", lead_id, e)
        return {}


# Поля карточки, из которых видно, с чем человек пришёл. Остальное (метрики,
# идентификаторы Яндекса, IP) в промпт не идёт — это шум и персональные данные.
LEAD_FIELDS_USEFUL = {"Комментарий", "Специализация", "Тип возражения", "Текущая блокировка",
                      "Бюджет", "Регион", "Город"}
SEARCH_RE = re.compile(r"[?&]text=([^&]+)")


def lead_brief(lead: dict) -> str:
    """С чем клиент пришёл: поисковый запрос, комментарий менеджера, направление.

    У холодного сайт-лида переписки нет, и без этого блока модель писала всем
    одно и то же «по охлаждёнке сейчас актуально…» (24.09.2026, 12 одинаковых
    сообщений подряд). Здесь лежит то, что делает первое сообщение конкретным.
    """
    out = []
    for f in (lead.get("custom_fields_values") or []):
        name = f.get("field_name") or ""
        vals = [str(v.get("value")) for v in (f.get("values") or []) if v.get("value")]
        if not vals:
            continue
        if name in LEAD_FIELDS_USEFUL:
            out.append(f"{name}: {', '.join(vals)[:200]}")
        elif name == "referrer":
            m = SEARCH_RE.search(vals[0])
            if m:
                q = urllib.parse.unquote_plus(m.group(1))
                out.append(f"пришёл по поисковому запросу: «{q[:80]}»")
    return "\n".join(out)


def site_request(notes: list) -> str:
    """Текст заявки с сайта из примечаний: что человек смотрел и откуда он."""
    out = []
    for n in notes:
        p = n.get("params") or {}
        txt = str(p.get("text") or "")
        if "Данные с сайта" in txt or "посещенные страницы" in txt:
            for line in txt.splitlines():
                if line.startswith(("последняя страница", "посещенные страницы")):
                    out.append(line.strip())
        elif "Дополнительная информация" in txt:
            for line in txt.splitlines():
                if line.strip().startswith("Дата и время"):
                    zone = re.search(r"\(([^)]+)\)", line)
                    if zone:
                        out.append(f"часовой пояс заявки: {zone.group(1)}")
    return "\n".join(dict.fromkeys(out))[:400]


async def _amo_notes(session: aiohttp.ClientSession, lead_id: int) -> list:
    token = os.getenv("AMO_ACCESS_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = f"{AMO_BASE}/leads/{lead_id}/notes?limit=20"
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                return []
            return ((await r.json()).get("_embedded") or {}).get("notes") or []
    except Exception as e:
        logger.warning("sales_dialog: примечания лида %s: %s", lead_id, e)
        return []


# ─── приём новых сайт-лидов ───────────────────────────────────────────────────
def norm_phone(raw: str | None) -> str | None:
    """Телефон в виде 7XXXXXXXXXX или None, если это не российский мобильный номер."""
    d = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if len(d) == 10:
        d = "7" + d
    elif len(d) == 11 and d.startswith("8"):
        d = "7" + d[1:]
    return d if len(d) == 11 and d.startswith("7") else None


def contact_channels(contact: dict) -> dict:
    """Чаты и телефон из карточки контакта: {'max': id, 'telegram': id, 'phone': 7…}."""
    out = {}
    for f in contact.get("custom_fields_values") or []:
        vals = [str(v.get("value")).strip() for v in (f.get("values") or []) if v.get("value")]
        if not vals:
            continue
        if f.get("field_id") == AMO_FIELD_MAX_ID:
            out["max"] = vals[0]
        elif f.get("field_id") == AMO_FIELD_TG_ID:
            out["telegram"] = vals[0]
        elif f.get("field_code") == "PHONE" and "phone" not in out:
            for v in vals:
                if norm_phone(v):
                    out["phone"] = norm_phone(v)
                    break
    return out


def pick_chat(channels: dict, last_inbound_chat: str | None = None) -> tuple:
    """Куда писать первым: `(chat_type, chat_id, all_chat_ids)`.

    Если клиент уже писал в один из чатов – туда, он там и читает. Иначе
    Telegram раньше MAX: по Telegram отправку проверили 29.09.2026. Нет чатов –
    `('telegram', None, [])`: первое сообщение уйдёт по номеру телефона.
    """
    chats = [(k, channels[k]) for k in ("telegram", "max") if channels.get(k)]
    ids = [cid for _, cid in chats]
    for kind, cid in chats:
        if last_inbound_chat and cid == last_inbound_chat:
            return kind, cid, ids
    if chats:
        return chats[0][0], chats[0][1], ids
    return "telegram", None, []


def is_site_lead(lead: dict) -> bool:
    tags = {t.get("id") for t in (lead.get("_embedded") or {}).get("tags") or []}
    return bool(tags & SITE_TAG_IDS) and lead.get("status_id") not in (142, 143)


async def _amo_get(session: aiohttp.ClientSession, path: str, params: dict | None = None) -> dict:
    token = os.getenv("AMO_ACCESS_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        async with session.get(f"{AMO_BASE}{path}", headers=headers, params=params or {},
                               timeout=aiohttp.ClientTimeout(total=30)) as r:
            return await r.json() if r.status == 200 else {}
    except Exception as e:
        logger.warning("sales_dialog: amo GET %s: %s", path, e)
        return {}


async def _is_customer(session: aiohttp.ClientSession, contact: dict) -> bool:
    """Контакт из компании, которая уже есть в «Покупателях».

    Покупатели в amoCRM привязаны к компаниям, а не к контактам (проверено
    29.09.2026), поэтому смотрим компанию контакта. Такой клиент уже чей-то –
    заявку с сайта от него ведёт менеджер, не агент.
    """
    for co in (contact.get("_embedded") or {}).get("companies") or []:
        full = await _amo_get(session, f"/companies/{co['id']}", {"with": "customers"})
        if (full.get("_embedded") or {}).get("customers"):
            return True
    return False


async def intake_new(app, db, session: aiohttp.ClientSession, campaign: str, cfg: dict) -> int:
    """Забирает агенту сайт-лиды, пришедшие после `intake_from`.

    Раньше их вручную раздавал собственник, и лид ждал менеджера часами (разбор
    29.09.2026: 4 из 5 открытых сайт-лидов без движения от 96 до 259 часов).
    Берём любой открытый лид воронки ПРИВЛЕЧЕНИЕ с тегом «сайт» / «сайт заявка»,
    кто бы ни был ответственным. Не берём действующих покупателей и контакты,
    которые агент уже ведёт в другой кампании. Возвращает число взятых.
    """
    since = cfg.get("intake_from")
    if not since:
        return 0
    # Окно скользит: за неделю лид либо взят, либо записан как исключённый, а
    # старт от `intake_from` через месяц упрётся в лимит 250 сделок на запрос.
    ts = max(int(datetime.fromisoformat(since).timestamp()),
             int((datetime.now(MSK) - timedelta(days=7)).timestamp()))
    data = await _amo_get(session, "/leads", {
        "filter[pipeline_id][]": ATTRACT_PIPELINE, "filter[created_at][from]": ts,
        "with": "contacts", "limit": 250})
    leads = [l for l in ((data.get("_embedded") or {}).get("leads") or []) if is_site_lead(l)]
    if not leads:
        return 0
    known = {r["lead_id"] for r in db._fetchall(
        "SELECT lead_id FROM sales_dialog_leads WHERE lead_id = ANY(%s)", ([l["id"] for l in leads],))}
    taken = 0
    for lead in leads:
        if lead["id"] in known:
            continue
        try:
            if await _take_new_lead(app, db, session, campaign, lead):
                taken += 1
        except Exception as e:
            _heartbeat(db, f"{campaign}: приём лида {lead['id']}: {type(e).__name__}: {e}", problem=True)
            logger.error("sales_dialog: приём лида %s: %s", lead["id"], e, exc_info=True)
    return taken


def _remember(db, campaign: str, lead: dict, status: str, note: str, contact: dict | None = None,
              chat: tuple = ("telegram", None, []), phone: str | None = None,
              activate_on=None) -> None:
    # Прежний ответственный – тот, кому «Вернуть менеджеру» отдаст раскачанный
    # лид (собственник 29.09.2026); уволенного заменит Инесса в `hand_to_manager`.
    prev = lead.get("responsible_user_id")
    db._execute("""INSERT INTO sales_dialog_leads
        (campaign, lead_id, contact_id, lead_name, contact_name, responsible_user_id,
         chat_type, chat_id, all_chat_ids, status, note, phone, prev_responsible_user_id,
         activate_on)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (campaign, lead_id) DO NOTHING""",
        (campaign, lead["id"], (contact or {}).get("id"), lead.get("name"),
         (contact or {}).get("name"), AGENT_AMO_USER if status == "active" else prev,
         chat[0], chat[1], chat[2] or None, status, note, phone,
         prev if prev != AGENT_AMO_USER else None, activate_on))


async def _take_new_lead(app, db, session, campaign: str, lead: dict) -> bool:
    lead_id = lead["id"]
    contacts = (lead.get("_embedded") or {}).get("contacts") or []
    main = next((c for c in contacts if c.get("is_main")), contacts[0] if contacts else None)
    if not main:
        _remember(db, campaign, lead, "excluded", "нет контакта")
        return False
    contact = await _amo_get(session, f"/contacts/{main['id']}")
    if not contact:
        return False                      # amoCRM не ответил – попробуем на следующем тике
    sub = os.getenv("AMO_SUBDOMAIN", "victorfishtobiz")
    link = f"https://{sub}.amocrm.ru/leads/detail/{lead_id}"
    if await _is_customer(session, contact):
        _remember(db, campaign, lead, "excluded", "действующий покупатель", contact)
        if app:
            await notify_owner(app, f"Сайт-лид от действующего покупателя – агенту не отдаю, "
                                    f"распредели менеджеру: {lead.get('name')}\n{link}")
        return False
    busy = db._fetchone("""SELECT campaign FROM sales_dialog_leads
                           WHERE contact_id=%s AND status='active' LIMIT 1""", (contact["id"],))
    if busy:
        _remember(db, campaign, lead, "excluded", f"контакт уже ведёт агент: {busy['campaign']}", contact)
        return False
    ch = contact_channels(contact)
    ids = [v for k, v in ch.items() if k in ("telegram", "max")]
    last = db._fetchone("""SELECT chat_id FROM wazzup_messages
                           WHERE chat_id = ANY(%s) AND is_outbound = false
                           ORDER BY sent_at DESC LIMIT 1""", (ids,)) if ids else None
    chat = pick_chat(ch, (last or {}).get("chat_id"))
    if not chat[1] and not ch.get("phone"):
        _remember(db, campaign, lead, "excluded", "ни чата, ни телефона", contact)
        if app:
            await notify_owner(app, f"Сайт-лид без чата и без телефона – агенту писать некуда: "
                                    f"{lead.get('name')}\n{link}")
        return False

    tags = [{"id": t["id"]} for t in (lead.get("_embedded") or {}).get("tags") or []]
    ok = await _amo_write(session, f"/leads/{lead_id}", {
        "pipeline_id": AGENT_PIPELINE, "status_id": AGENT_FIRST_STATUS,
        "responsible_user_id": AGENT_AMO_USER,
        "_embedded": {"tags": tags + [{"id": AGENT_TAG_ID}]}})
    if not ok:
        return False                      # не перенесли – не берём, повторим на следующем тике
    await _amo_write(session, "/contacts", [{"id": c["id"], "responsible_user_id": AGENT_AMO_USER}
                                            for c in contacts])
    _remember(db, campaign, lead, "active",
              f"новый сайт-лид; до агента был на {lead.get('responsible_user_id')}",
              contact, chat, ch.get("phone"))
    logger.info("sales_dialog: новый сайт-лид %s взят агентом (%s %s)", lead_id, chat[0],
                chat[1] or "по номеру")
    return True


def _pending_first(db, campaign: str) -> list:
    """Новые лиды, которым агент ещё не написал: первое касание.

    Идут вне очереди, как ответы клиентам: заявка с сайта остывает за часы.
    Черновик на лид – не больше одного в день (ключ `first:<лид>:<дата>`); если
    собственник его не разобрал и он протух, завтра будет новый. Не пишем, если
    по лиду уже что-то ушло, собственник нажал «Не отвечать» или «Вернуть
    менеджеру», или менеджер успел написать в чат сам.
    """
    return db._fetchall("""
        SELECT l.campaign, l.lead_id, l.contact_id, l.assignee_amo_id,
               l.chat_id, l.chat_type, l.lead_name, l.contact_name, l.replies_sent,
               'first:' || l.lead_id || ':' ||
                 to_char(now() AT TIME ZONE 'Europe/Moscow', 'YYYYMMDD') AS message_id,
               NULL AS inbound_text, l.last_inbound_at AS sent_at, 'first' AS source,
               l.activate_on, l.created_at AS taken_at_lead
        FROM sales_dialog_leads l
        WHERE l.campaign = %s AND l.status = 'active'
          AND (l.activate_on IS NULL
               OR l.activate_on <= (now() AT TIME ZONE 'Europe/Moscow')::date)
          AND NOT EXISTS (
              SELECT 1 FROM sales_dialog_messages d
              WHERE d.campaign = l.campaign AND d.lead_id = l.lead_id
                AND (d.verdict IN ('sent', 'draft', 'edited', 'skipped', 'handed', 'taken')
                     OR d.created_at >= date_trunc('day', now() AT TIME ZONE 'Europe/Moscow')
                                        AT TIME ZONE 'Europe/Moscow'))
          AND NOT EXISTS (
              SELECT 1 FROM wazzup_messages o
              WHERE o.chat_id = ANY(coalesce(l.all_chat_ids, ARRAY[l.chat_id]))
                AND o.is_outbound = true AND o.sent_at AT TIME ZONE 'UTC' > l.created_at)
        ORDER BY l.id
    """, (campaign,))


CARD_HEADER_RE = re.compile(
    r"^\s*(Ответ от имени [^:]*:|Клиент:|Его черновик:|Агент не бер[её]тся[^\n]*|"
    r"Обещал[аи]? уточнить:[^\n]*|Уступка:[^\n]*|Диалог затих[^\n]*|"
    # Первая строка карточки: «Имя клиента · max · сделка 41740283».
    r"[^\n]*·[^\n]*сделка\s*\d+[^\n]*)\s*|"
    # Статус карточки отдельной строкой в квадратных скобках: «[жду твой текст — …]»
    # ушёл клиентке 29.09.2026 вместе с правкой, скопированной из карточки.
    r"^\s*\[[^\]\n]*\]\s*$", re.I | re.M)
# Если после чистки в тексте всё ещё видны следы карточки — отправлять нельзя.
CARD_TRACE_RE = re.compile(r"(Ответ от имени|·\s*сделка\s*\d+|Диалог затих|"
                           r"жду твой текст|ответом на карточку)", re.I)


def strip_card_header(text: str) -> str:
    """Человек правит текст, копируя его из карточки — служебные строки убираем.

    24.09.2026 клиенту ушло сообщение, начинавшееся с «Ответ от имени Инессы:».
    28.09.2026 повторилось хуже: в чат «ФАРШ» уехала вся шапка карточки вместе с
    строкой «Диалог затих, агент пишет первым» — фильтр существовал, но его никто
    не вызывал. Теперь он стоит на пути правки, а `_do_send` дополнительно
    отказывается отправлять текст, в котором следы карточки остались.
    """
    return CARD_HEADER_RE.sub("", text or "").strip()


def winning_openers(db, campaign: str, hours: int = 48, limit: int = 5) -> str:
    """Зачины, после которых клиент отвечал — материал для следующих сообщений.

    Считаем по факту: есть ли входящее в чате в течение `hours` после отправки.
    Пока данных мало, список будет коротким — это нормально, он растёт сам.
    """
    rows = db._fetchall("""
        SELECT coalesce(m.final_text, m.draft_text) AS t
        FROM sales_dialog_messages m
        JOIN sales_dialog_leads l ON l.campaign = m.campaign AND l.lead_id = m.lead_id
        WHERE m.campaign = %s AND m.verdict = 'sent' AND m.sent_at IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM wazzup_messages w
              WHERE w.chat_id = ANY(coalesce(l.all_chat_ids, ARRAY[l.chat_id]))
                AND w.is_outbound = false
                AND w.sent_at AT TIME ZONE 'UTC' > m.sent_at
                AND w.sent_at AT TIME ZONE 'UTC' < m.sent_at + (%s || ' hours')::interval)
        ORDER BY m.id DESC LIMIT %s""", (campaign, str(hours), limit))
    return "\n".join("— " + (r["t"] or "").strip().split("\n")[0][:110] for r in rows if r["t"])


MAX_IMAGE_BYTES = 4 * 1024 * 1024
IMAGE_TYPES = {"image/jpeg", "image/png", "image/webp", "image/gif"}


async def fetch_client_images(session: aiohttp.ClientSession, history: list, limit: int = 2) -> list:
    """Фото, которые прислал клиент, — модели они нужны глазами.

    Собственник 24.09.2026: когда клиент говорит «беру Трим С дешевле», дело
    почти всегда в фактической разделке и размере пласта, а не в цене как
    таковой. Правильный ход — попросить фото и предложить аналог по нему,
    поэтому присланные фото уходят в модель вместе с перепиской.
    """
    out = []
    for h in reversed(history):
        if len(out) >= limit:
            break
        if h.get("is_outbound") or not h.get("content_uri"):
            continue
        try:
            async with session.get(h["content_uri"], timeout=aiohttp.ClientTimeout(total=40)) as r:
                if r.status != 200:
                    continue
                ctype = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
                if ctype not in IMAGE_TYPES:
                    continue
                raw = await r.content.read(MAX_IMAGE_BYTES + 1)
                if len(raw) > MAX_IMAGE_BYTES:
                    continue
        except Exception as e:
            logger.warning("sales_dialog: вложение не скачалось: %s", e)
            continue
        out.append({"type": "image", "source": {"type": "base64", "media_type": ctype,
                                                "data": base64.b64encode(raw).decode()}})
    return out


def recent_openers(db, campaign: str, limit: int = 12) -> str:
    """Первые строки последних отправленных сообщений — чтобы не повторяться.

    Без этого агент писал холодным лидам один и тот же зачин десяток раз подряд.
    """
    rows = db._fetchall("""SELECT coalesce(final_text, draft_text) AS t FROM sales_dialog_messages
                           WHERE campaign=%s AND verdict='sent'
                           ORDER BY id DESC LIMIT %s""", (campaign, limit))
    outs = []
    for r in rows:
        first = (r["t"] or "").strip().split("\n")[0]
        if first:
            outs.append("— " + first[:110])
    return "\n".join(outs)


# ─── сбор контекста ───────────────────────────────────────────────────────────
def _known_names(db, row: dict, lead: dict) -> list:
    """Все имена, которые надо замаскировать.

    Мало передать `contact_name` из карточки: в чате человек часто называет себя
    иначе (23.09.2026 в карточке была «Викулова», а в переписке клиент назвался
    Анастасией — имя ушло в модель). Поэтому собираем имена всех контактов сделки
    плюс имена собеседников из истории Wazzup.
    """
    names = [row.get("contact_name"), row.get("lead_name")]
    for c in (lead.get("_embedded") or {}).get("contacts") or []:
        names.append(c.get("name"))
    for r in db._fetchall("""SELECT DISTINCT contact_name FROM wazzup_messages
                             WHERE chat_id=%s AND contact_name IS NOT NULL""", (row["chat_id"],)):
        names.append(r["contact_name"])
    seen, out = set(), []
    for n in names:
        n = (n or "").strip()
        if len(n) > 2 and n.lower() not in seen:
            seen.add(n.lower())
            out.append(n)
    return out


def _history(db, chats: list) -> list:
    """Переписка по ВСЕМ чатам лида, слитая в одну ленту по времени.

    У части клиентов разговор идёт в двух чатах сразу – с разными людьми одной
    компании. Айс Фиш 23.09.2026: в одном чате технолог месяц объясняла, что ей
    нужен тузлучный посол и потолок 1500 ₽, в другом закупщик отказывался от
    временного сотрудничества. Агент видел один чат из двух и написал мимо.
    """
    rows = db._fetchall("""SELECT sent_at, is_outbound, chat_id, COALESCE(text,'') AS text,
                                  content_uri
                           FROM wazzup_messages WHERE chat_id = ANY(%s)
                           ORDER BY sent_at DESC LIMIT %s""",
                        (list(chats), HISTORY_LIMIT))
    return list(reversed(rows))


def _delivery_tariffs() -> dict:
    """Тарифы доставки Москва→регионы из калькулятора менеджеров.

    Источник — `delivery_calc.html` приложения FISHки (fishki.f2b.group/delivery-calc),
    КП «Джи Эф Си Логистикс», кросс-док терминал-дверь, 691 город. Цены с НДС 22%.
    """
    global _tariffs_cache
    if _tariffs_cache is None:
        try:
            _tariffs_cache = json.loads((PROMPTS_DIR / "delivery_tariffs.json").read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("sales_dialog: тарифы доставки не прочитались: %s", e)
            _tariffs_cache = {}
    return _tariffs_cache


def find_city(text: str) -> str | None:
    """Ищет в переписке город из справочника тарифов.

    Берём самое длинное совпадение: «Нижний Новгород» важнее, чем «Новгород».
    Совпадение — по границе слова, иначе «Орел» ловится внутри «орел» в любом тексте.
    """
    best = None
    low = (text or "").lower()
    for city in _delivery_tariffs():
        name = city.split(",")[0].strip().lower()
        if len(name) < 4:
            continue
        if re.search(rf"\b{re.escape(name)}\w{{0,3}}\b", low):
            if best is None or len(name) > len(best.split(",")[0]):
                best = city
    return best


def delivery_note(city: str | None) -> str:
    """Блок про доставку для промпта. Без выдуманных цифр: чего нет — того нет."""
    base = ("Москва и МО – бесплатно от 7 000 ₽. "
            "В регионы возим до адреса клиента через партнёра-перевозчика, доставка платная.")
    if not city:
        return base + " Город клиента не определён – цену доставки не называй, спроси город."
    rows = _delivery_tariffs().get(city) or []
    if not rows:
        return base
    parts = []
    for r in rows:
        t100 = f"до 100 кг – {int(r['t100'])} ₽" if r.get("t100") else "тарифа до 100 кг нет, считается от паллеты"
        parts.append(f"{r['rg']}: {t100}, паллета (до 600 кг) – {int(r['pal'])} ₽")
    return (f"{base}\nТариф до города {city} (с НДС, терминал-дверь): " + "; ".join(parts) +
            ". Это ориентир по прайсу перевозчика: можешь назвать его как примерную стоимость, "
            "точный расчёт под вес и объём делает менеджер.")


def offer_prices(db) -> dict:
    """Цены предложения: `{код: цена}` из `bot_settings`, ключ `sales_dialog:price_overrides`.

    Собственник задаёт их вручную под волну (25.09.2026), и они главнее прайса
    МойСклада: там цена может быть и выше, и ниже, а клиенту в этой волне
    называем именно эту. В МойСклад агент не пишет — правило read-only.
    """
    row = db._fetchone("SELECT value FROM bot_settings WHERE key=%s",
                       (SETTINGS_PREFIX + "price_overrides",))
    try:
        raw = json.loads((row or {}).get("value") or "{}")
    except Exception:
        logger.warning("sales_dialog: price_overrides не разобрались")
        return {}
    out = {}
    for code, val in raw.items():
        try:
            out[str(code)] = float(val)
        except (TypeError, ValueError):
            logger.warning("sales_dialog: цена предложения %s не число: %r", code, val)
    return out


def apply_offers(rows: list, offers: dict) -> list:
    """Накладывает цены предложения на справочник, не портя кэш прайса."""
    if not offers:
        return rows
    out = []
    for p in rows:
        price = offers.get(str(p.get("code")))
        if price is None:
            out.append(p)
            continue
        # Все три типа равны цене предложения: какой бы тип модель ни выбрала,
        # клиенту уйдёт одна и та же согласованная цифра.
        out.append({**p, "opt": price, "horeca": price, "spec": price, "offer": price})
    return out


def _format_prices(rows: list) -> str:
    """В промпт идут позиции с остатком, все спеццены и вся своя готовая продукция."""
    def line(p):
        own = "наше производство" if p.get("own") else "привлечённый товар"
        if p.get("offer"):
            return (f"{p['code']} | {p['name']} | ЦЕНА ПРЕДЛОЖЕНИЯ {p['offer']} "
                    f"(ниже не опускаться, торг — только через руководителя) | "
                    f"остаток {p['stock']} | {own}")
        return (f"{p['code']} | {p['name']} | опт {p['opt']} | horeca {p['horeca'] or '-'} | "
                f"спец {p['spec'] or '-'} | остаток {p['stock']} | {own}")
    return "\n".join(line(p) for p in rows
                     if p["stock"] > 0 or p["spec"] or p.get("own") or p.get("offer"))


async def build_context(db, session: aiohttp.ClientSession, row: dict) -> dict:
    """Готовит всё, что уходит в модель. Переписка — обезличенная."""
    lead = await _amo_lead(session, row["lead_id"])
    lead_row = db._fetchone("""SELECT all_chat_ids FROM sales_dialog_leads
                               WHERE campaign=%s AND lead_id=%s""",
                            (row["campaign"], row["lead_id"]))
    chats = (lead_row or {}).get("all_chat_ids") or [c for c in [row["chat_id"]] if c]
    history = _history(db, chats)
    # Ветки помечаем, только когда их правда несколько: иначе лишний шум в промпте.
    branches = {h.get("chat_id") for h in history if h.get("chat_id")}
    order = {cid: i + 1 for i, cid in enumerate(sorted(branches))}
    raw = "\n".join(
        (f"[{h['sent_at']:%d.%m %H:%M}]"
         + (f"[ветка {order.get(h.get('chat_id'), 1)}]" if len(branches) > 1 else "")
         + f" {'МЕНЕДЖЕР' if h['is_outbound'] else 'КЛИЕНТ'}: {h['text']}")
        for h in history)
    names = _known_names(db, row, lead)
    safe = anonymize(raw, contact_name=names[0] if names else None, extra_names=names[1:])
    leaks = find_leaks(safe)
    if leaks:
        logger.warning("sales_dialog: в обезличенной переписке остались ПДн lead=%s %s",
                       row["lead_id"], list(leaks))

    # Город ищем ТОЛЬКО в словах клиента и в названии сделки. В наших исходящих
    # почти всегда есть «доставка по Москве и МО» — по ним город определялся как
    # Москва даже у клиента, который писал «мы не в МСК находимся» (23.09.2026).
    client_text = " ".join(h["text"] for h in history if not h["is_outbound"])
    city = find_city(client_text + " " + (lead.get("name") or ""))
    prices = await mrm_guard(session, db, await _ms_price_rows(session))
    prices = apply_offers(prices, offer_prices(db))
    objections = (PROMPTS_DIR / "objections.md").read_text(encoding="utf-8")[:4000]
    last_in = next((h for h in reversed(history) if not h["is_outbound"]), None)
    already = db._fetchone("""SELECT count(*) AS n FROM sales_dialog_messages
                              WHERE campaign=%s AND lead_id=%s AND verdict='sent'""",
                           (row["campaign"], row["lead_id"]))
    first_time = not (already or {}).get("n")
    days = (datetime.now() - history[-1]["sent_at"]).days if history else 0

    notes = await _amo_notes(session, row["lead_id"])
    brief = lead_brief(lead)
    site = site_request(notes)
    openers = recent_openers(db, row["campaign"])
    worked = winning_openers(db, row["campaign"])
    persona = persona_for(row.get("assignee_amo_id"))
    wrote = "писала" if persona["gender"] == "f" else "писал"
    if row.get("source") == "first":
        created = lead.get("created_at")
        when = (datetime.fromtimestamp(created, MSK).strftime("%d.%m в %H:%M")
                if created else "недавно")
        if history:
            first_touch_note = (f"Клиент оставил заявку на сайте {when}. Менеджер уже писал ему "
                                "(см. переписку), клиент не ответил. Ты пишешь в этот чат впервые: "
                                "не повторяй прайс и то, что уже отправлено, зайди от его заявки "
                                "и одним вопросом, который двигает к заказу.")
        else:
            first_touch_note = (f"Клиент САМ оставил заявку на сайте {when}, до тебя ему никто не "
                                "писал. Это первое сообщение: ответь на его заявку – коротко, от "
                                "того, с чем он пришёл (страницы, запрос, комментарий), и одним "
                                "вопросом, который двигает к заказу.")
    elif first_time:
        first_touch_note = "Ты пишешь в этот чат ВПЕРВЫЕ — до тебя его вёл другой менеджер."
    else:
        first_touch_note = f"Ты уже {wrote} в этот чат, представляться повторно не нужно."
    if row.get("source") in ("silent", "first"):
        first_touch_note += (
            "\nТы пишешь ПЕРВЫМ. Без ссылок и файлов. Закончи мягким выходом своими словами, "
            "например «если сейчас неактуально – скажите, не буду беспокоить»; формулировку "
            "каждый раз меняй, одинаковый хвост во многих чатах мессенджер считает спамом.")
    user = f"""ПЕРЕПИСКА (последнее сообщение {days} дн. назад):
{safe if safe.strip() else "— переписки нет, это первое обращение к клиенту"}

КАРТОЧКА: «{lead.get('name', '')}», лид с сайта f2b.group.
{brief if brief else "дополнительных данных по заявке нет"}
{site}

ТАК НАЧИНАЛИСЬ ПОСЛЕДНИЕ ОТПРАВЛЕННЫЕ СООБЩЕНИЯ ДРУГИМ КЛИЕНТАМ
(повторять эти зачины и структуру нельзя, найди свой заход под этого клиента):
{openers if openers else "— пока ничего не отправляли"}

НА ЭТИ ЗАХОДЫ КЛИЕНТЫ ОТВЕЧАЛИ (копировать дословно нельзя, но приём рабочий):
{worked if worked else "— статистики пока нет"}
{first_touch_note}

ДОСТАВКА:
{delivery_note(city)}

СПРАВОЧНИК ЦЕН И ОСТАТКОВ (единственный источник цифр):
{_format_prices(prices)}

БАЗА ВОЗРАЖЕНИЙ:
{objections}

Напиши сообщение клиенту."""
    images = await fetch_client_images(session, history)
    return {"system": system_prompt(persona), "persona": persona,
            "user": user, "images": images, "leaks": leaks,
            "prices": {p["code"]: p for p in prices},
            "last_inbound": last_in, "names": names, "city": city}


# ─── генерация ────────────────────────────────────────────────────────────────
SCHEMA = {
    "type": "object",
    "properties": {
        "action": {"type": "string", "enum": ["reply", "escalate", "close"]},
        "text": {"type": "string"},
        "reason": {"type": "string"},
        "price_claims": {"type": "array", "items": {
            "type": "object",
            "properties": {"code": {"type": "string"}, "name": {"type": "string"},
                           "price": {"type": "number"},
                           "price_type": {"type": "string", "enum": ["opt", "horeca", "spec"]}},
            "required": ["code", "name", "price", "price_type"], "additionalProperties": False}},
        "escalate_to_owner": {"type": "string"},
    },
    "required": ["action", "text", "reason", "price_claims", "escalate_to_owner"],
    "additionalProperties": False,
}


# На Amvera msk0 связь с api.anthropic.com держится только на anthropic==0.40.0 +
# httpx==0.27.2 (memory feedback_pin_anthropic_httpx_on_amvera): новые версии SDK
# в этом контейнере не устанавливают соединение. Поэтому ни `thinking`, ни
# `output_config` использовать нельзя — этот SDK их не знает. Схему просим
# текстом и парсим сами, как это делает wazzup_classifier.
JSON_RULE = """

ФОРМАТ ОТВЕТА. Верни ТОЛЬКО JSON, без markdown-обёртки и без пояснений:
{"action": "reply|escalate|close",
 "text": "текст сообщения клиенту",
 "reason": "почему так решила, одна фраза",
 "price_claims": [{"code": "код из справочника", "name": "позиция",
                   "price": число, "price_type": "opt|horeca|spec"}],
 "escalate_to_owner": "что передать собственнику, если action=escalate, иначе пустая строка",
 "need_check": "что ты обещала уточнить, если обещала; иначе пустая строка"}
Если цен в сообщении нет — price_claims пустой список."""


def parse_draft(raw: str) -> dict | None:
    """Достаёт JSON из ответа модели. Терпим к ```json-обёртке и болтовне вокруг."""
    txt = (raw or "").strip()
    if txt.startswith("```"):
        txt = txt.split("\n", 1)[1] if "\n" in txt else txt
        txt = txt.rsplit("```", 1)[0].strip()
        if txt.startswith("json"):
            txt = txt[4:].lstrip()
    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        start, end = txt.find("{"), txt.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(txt[start:end + 1])
            except json.JSONDecodeError:
                return None
        return None


def answer_text(data: dict) -> str:
    """Текст ответа из блоков `content`.

    Брать `content[0]` нельзя: у моделей с размышлением первым идёт блок
    `thinking`, текст — вторым.
    """
    for b in (data or {}).get("content") or []:
        if b.get("type") == "text" and b.get("text"):
            return b["text"]
    return ""


async def generate_draft(ctx: dict, db=None) -> dict | None:
    """Запрос к Claude напрямую по HTTP, в обход SDK.

    На Amvera стоит anthropic==0.40.0 (новее в этом контейнере не соединяется с
    api.anthropic.com), и этот SDK не умеет разбирать ответ Opus 5: падает с
    `AttributeError: 'typing.Union' object has no attribute '__discriminator__'`
    ещё до того, как мы увидим текст. Сам запрос при этом проходит. Поэтому
    ходим aiohttp'ом и разбираем JSON сами — версия SDK перестаёт что-либо решать.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("sales_dialog: ANTHROPIC_API_KEY не задан")
        return None
    content = [{"type": "text", "text": ctx["user"]}] + list(ctx.get("images") or [])
    payload = {"model": MODEL, "max_tokens": 8000,
               "system": ctx["system"] + JSON_RULE,
               "messages": [{"role": "user", "content": content}]}
    headers = {"x-api-key": api_key, "anthropic-version": ANTHROPIC_VERSION,
               "content-type": "application/json"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(ANTHROPIC_URL, json=payload, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=180)) as r:
                if r.status != 200:
                    body = (await r.text())[:300]
                    logger.warning("sales_dialog: Anthropic %s: %s", r.status, body)
                    if db is not None:
                        _heartbeat(db, f"Anthropic http {r.status}: {body}", problem=True)
                    return None
                data = await r.json()
        raw = answer_text(data)
        draft = parse_draft(raw)
        if not draft or "text" not in draft:
            logger.warning("sales_dialog: ответ модели не разобран: %r", raw[:200])
            if db is not None:
                _heartbeat(db, f"ответ не разобран: {raw[:200]!r}", problem=True)
            return None
        draft.setdefault("action", "reply")
        draft.setdefault("price_claims", [])
        draft.setdefault("reason", "")
        draft.setdefault("escalate_to_owner", "")
        draft.setdefault("need_check", "")
        return draft
    except Exception as e:
        logger.warning("sales_dialog: генерация не удалась: %s: %s", type(e).__name__, e)
        if db is not None:
            _heartbeat(db, f"вызов Anthropic упал: {type(e).__name__}: {e}"[:400], problem=True)
        return None


def allowed_numbers(city: str | None) -> set:
    """Числа, которые законно стоят в тексте помимо цен на товар.

    Порог бесплатной доставки и тарифы перевозчика по городу клиента — не цены
    из прайса, но появляться в сообщении имеют право.
    """
    out = {"7000"}
    for r in _delivery_tariffs().get(city or "", []):
        for key in ("t100", "pal"):
            if r.get(key):
                out.add(str(int(r[key])))
                out.add(str(round(r[key])))
    return out


def check_prices(draft: dict, prices: dict, allowed: set | None = None) -> list:
    """Сверка каждой названной цены со справочником. Непустой список = не отправлять.

    Отдельно ловим числа, которых нет в `price_claims`: на прогонах 23.09.2026 модель
    таких не выдумывала, но проверка дешёвая, а цена ошибки — неверная цена клиенту.
    """
    problems = []
    for c in draft.get("price_claims") or []:
        p = prices.get(c["code"])
        if not p:
            problems.append(f"кода {c['code']} нет в справочнике")
            continue
        # Цена предложения главнее типа: она согласована собственником под волну,
        # и назвать её можно, даже если модель выбрала тип, которого в прайсе нет.
        actual = p.get("offer") or p.get(c["price_type"])
        if not actual:
            problems.append(f"{c['code']}: нет цены типа «{c['price_type']}»")
            continue
        said, listed = float(c["price"]), float(actual)
        if said > listed + 0.01:
            problems.append(f"{c['code']}: сказано {said}, в МойСклад {listed}")
        elif said < listed - 0.01:
            # Ниже прайса — это торг. Допустим он или нет, решает пол из дашборда,
            # проверка асинхронная, поэтому здесь только помечаем.
            draft.setdefault("bargain", []).append(
                {"code": c["code"], "price": said, "list_price": listed,
                 "price_type": c["price_type"]})
    declared = {str(int(c["price"])) for c in draft.get("price_claims") or []}
    # Тариф перевозчика человек называет округлённо («около 14 400» вместо 14 417),
    # и это нормально. Цены на товар округлять нельзя — они сверены точно выше.
    loose = sorted(float(x) for x in (allowed or set()))
    for num in re.findall(r"\b(\d[\d\s ]{2,})\s*₽", draft.get("text") or ""):
        plain = num.replace(" ", "").replace(" ", "")
        if plain in declared:
            continue
        val = float(plain)
        if any(abs(val - a) <= max(a * 0.03, 1) for a in loose):
            continue
        problems.append(f"в тексте число {num.strip()} ₽, не объявленное в price_claims")
    return problems


DASHBOARD_URL = os.getenv("FISHKI_URL", "https://fishki.f2b.group")
# На сколько процентов агенту разрешено уступать от прайса за один шаг торга.
BARGAIN_STEP_PCT = 3


async def price_floor(session: aiohttp.ClientSession, db, sku_code: str) -> dict:
    """Пороги цены по позиции из дашборда менеджера («согласование цены»).

    Считает их `price_approval.evaluate` — тот же расчёт, что видит менеджер,
    поэтому агент торгуется по тем же правилам и ничего не дублирует. Токен
    лежит в общей таблице `bot_settings`.
    """
    row = db._fetchone("SELECT value FROM bot_settings WHERE key='price_floors_token'")
    token = (row or {}).get("value")
    if not token:
        return {"error": "нет токена price_floors_token"}
    url = f"{DASHBOARD_URL}/api/price/floors"
    try:
        async with session.get(url, params={"sku": sku_code, "token": token},
                               timeout=aiohttp.ClientTimeout(total=40)) as r:
            if r.status != 200:
                return {"error": f"http {r.status}"}
            return await r.json()
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


async def check_bargain(session, db, draft: dict, prices: dict | None = None) -> list:
    """Торг в пределах правил ценообразования: ниже пола — только через собственника.

    Клиенты, особенно новые, почти всегда просят ниже прайса. Агент вправе
    уступать, но нижняя граница — порог из дашборда (для новых и спящих это
    «минимум», то есть безубыточность плюс вклад). Всё, что ниже, — эскалация.

    У позиций с ценой предложения торга нет вовсе: эту цифру собственник задал
    вручную, и часть таких цен сама лежит около порога (25.09.2026: 40089 по
    2450 при безубыточности 2491). Ниже неё — сразу к собственнику.
    """
    problems = []
    for b in draft.get("bargain") or []:
        offer = ((prices or {}).get(b["code"]) or {}).get("offer")
        if offer and b["price"] < offer - 0.5:
            problems.append(f"{b['code']}: предложено {b['price']:.0f}, "
                            f"ниже согласованной цены предложения {offer:.0f}")
            continue
        fl = await price_floor(session, db, b["code"])
        if fl.get("error") or fl.get("floor") is None:
            problems.append(f"{b['code']}: цена {b['price']} ниже прайса {b['list_price']}, "
                            f"порог посчитать не удалось ({fl.get('error', 'нет данных')})")
            continue
        floor = float(fl.get("floor_pay") or fl["floor"])
        b["floor"] = round(floor)
        if b["price"] < floor - 0.5:
            problems.append(f"{b['code']}: предложено {b['price']:.0f}, ниже порога "
                            f"{floor:.0f} ({fl.get('status_label') or fl.get('status') or ''})")
    return problems


MASC_RE = re.compile(r"\b(понял|прошёл|прошел|посмотрел|уточнил|написал|сделал|проверил|"
                     r"добавил|отправил|подготовил|связался|рад|готов)\b")
FEM_RE = re.compile(r"\b(поняла|прошла|посмотрела|уточнила|написала|сделала|проверила|"
                    r"добавила|отправила|подготовила|связалась|рада|готова)\b")


def check_style(text: str, gender: str = "f") -> list:
    """Стилевые запреты, которые нельзя доверять одному промпту.

    Род от первого лица (23.09.2026 модель написала «Понял, торопить не буду» от
    имени Инессы) и любое упоминание звонка — собственник запретил звонки
    отдельно, пилот только переписка. Род сверяем с полом того менеджера, чьё имя
    стоит под сообщением: у Дениса правильный род как раз мужской.
    """
    problems = []
    low = (text or "").lower()
    # Женские формы вычитаем первыми: «поняла» содержит в себе «понял».
    fem = set(FEM_RE.findall(low))
    masc = set(MASC_RE.findall(FEM_RE.sub(" ", low)))
    wrong = masc if gender == "f" else fem
    if wrong:
        label = "мужской" if gender == "f" else "женский"
        problems.append(f"{label} род от первого лица: " + ", ".join(sorted(wrong)))
    call = re.findall(r"\b(позвон\w*|созвон\w*|наберу|набер[её]м|перезвон\w*|звонок|"
                      r"телефон\w* для связи)\b", (text or "").lower())
    if call:
        problems.append("упоминание звонка: " + ", ".join(sorted(set(call))))
    # Инъект клиенту не называем никогда (собственник 28.09.2026).
    inject = re.findall(r"\bинъе[кц]\w*", low)
    if inject:
        problems.append("упоминание инъекта: " + ", ".join(sorted(set(inject))))
    return problems


INTRO_MARKERS = re.compile(
    r"(меня зовут|теперь я веду|дальше (ваш вопрос )?веду|дальше я веду|"
    r"я теперь ваш менеджер|перед[аё]ли мне ваш вопрос|на связи инесса|инесса, f2b)",
    re.I)


def polish(text: str) -> str:
    """Правки собственника, которые дешевле сделать кодом, чем ждать от модели.

    1. Представляться не надо (23.09.2026): клиент уже в переписке, а лишняя
       строка «меня зовут, теперь я веду ваш вопрос» только добавляет шума.
       Вырезаем предложение с представлением, остальное сообщение не трогаем.
    2. Размер рыбы называем «размер», а не «развес». Слова однокоренные по
       склонению, поэтому замена корня сохраняет падеж: развесом → размером.
    """
    if not text:
        return text
    out = []
    for para in text.split("\n"):
        parts = re.split(r"(?<=[.!?])\s+", para)
        kept = [x for x in parts if not INTRO_MARKERS.search(x)]
        out.append(" ".join(kept).strip() if len(kept) != len(parts) else para)
    text = "\n".join(out)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return re.sub(r"(?i)развес", lambda m: "Размер" if m.group(0)[0].isupper() else "размер", text)


# ─── тик ──────────────────────────────────────────────────────────────────────
def _pending_inbound(db, campaign: str) -> list:
    """Клиент написал, и после его сообщения мы ещё не отвечали.

    Потолка на число ответов здесь нет сознательно (правило собственника от
    23.09.2026): пока клиент задаёт вопросы, на них отвечают, сколько бы их ни
    было. Ограничение касается только инициативы в молчащий диалог, см.
    `_pending_silent`.

    Смотрим во ВСЕ чаты лида (`all_chat_ids`), а не только в основной: у части
    лидов контакт заведён в двух мессенджерах, и написать клиент может в любой.
    Отвечаем в тот чат, откуда пришло сообщение, — его `chat_id` и канал и
    возвращаем, они же уедут в черновик.

    Условие про исходящие важно: если менеджер успел ответить руками, агент в
    разговор не лезет. Идемпотентность — по `message_id` входящего.
    """
    return db._fetchall("""
        SELECT l.campaign, l.lead_id, l.contact_id, l.assignee_amo_id,
               m.chat_id, coalesce(m.chat_type, l.chat_type) AS chat_type,
               l.lead_name, l.contact_name, l.replies_sent,
               m.message_id, m.text AS inbound_text, m.sent_at, 'inbound' AS source
        FROM sales_dialog_leads l
        CROSS JOIN LATERAL (SELECT coalesce(l.all_chat_ids, ARRAY[l.chat_id]) AS ids) c
        JOIN LATERAL (
            SELECT w.message_id, w.text, w.sent_at, w.chat_id, w.chat_type
            FROM wazzup_messages w
            WHERE w.chat_id = ANY(c.ids) AND w.is_outbound = false
            ORDER BY w.sent_at DESC LIMIT 1
        ) m ON true
        WHERE l.campaign = %s AND l.status = 'active'
          -- Лид партии до своего дня не трогаем, а его старое неотвеченное
          -- сообщение – не «клиент ждёт», а спящий диалог: им занимается очередь
          -- оживления по квоте. 29.09.2026 партию на 30.09 записали днём, и агент
          -- тут же понёс менеджерам ответы на сообщения месячной давности.
          AND (l.activate_on IS NULL
               OR (l.activate_on <= (now() AT TIME ZONE 'Europe/Moscow')::date
                   AND m.sent_at AT TIME ZONE 'UTC' > l.created_at))
          AND NOT EXISTS (
              SELECT 1 FROM sales_dialog_messages d
              WHERE d.inbound_message_id = m.message_id AND d.prompt_version = %s)
          AND NOT EXISTS (
              SELECT 1 FROM wazzup_messages o
              WHERE o.chat_id = ANY(c.ids) AND o.is_outbound = true AND o.sent_at > m.sent_at)
    """, (campaign, PROMPT_VERSION))


def _pending_silent(db, campaign: str, silent_days: int,
                    max_followups: int, not_before: datetime) -> list:
    """Диалог затих — агент пишет в него сам, но не больше пары раз.

    Правило собственника (23.09.2026): на вопросы клиента отвечаем всегда, а
    молчащего трогаем не более `max_followups` раз подряд с паузой в пару
    рабочих дней. Дальше лид ждёт — статус остаётся `active`, и как только
    клиент напишет, разговор подхватит `_pending_inbound`, а счётчик
    обнулится сам, потому что считается от последнего входящего.

    Пауза отмеряется от последнего исходящего, включая ручное сообщение
    менеджера: если человек только что написал сам, агент сверху не пишет.

    Тишину и касания считаем по всем чатам лида, а пишем в тот чат, где клиент
    писал последний раз (если не писал нигде — в основной чат карточки): именно
    там он и читает.

    Ключ идемпотентности — лид плюс сегодняшняя дата, и ровно то же условие
    стоит в отборе («сегодня по лиду черновиков ещё не было»). Если отбор и
    ключ расходятся, `ON CONFLICT DO NOTHING` молча гасит вставку и тик
    крутится вхолостую без единой ошибки — уже обжигались.
    """
    return db._fetchall("""
        SELECT l.campaign, l.lead_id, l.contact_id, l.assignee_amo_id,
               coalesce(lastin.chat_id, l.chat_id) AS chat_id,
               coalesce(lastin.chat_type, l.chat_type) AS chat_type,
               l.lead_name, l.contact_name, l.replies_sent,
               'silent:' || l.lead_id || ':' ||
                 to_char(now() AT TIME ZONE 'Europe/Moscow', 'YYYYMMDD') AS message_id,
               NULL AS inbound_text,
               (SELECT max(w.sent_at) AT TIME ZONE 'UTC' FROM wazzup_messages w
                 WHERE w.chat_id = ANY(c.ids)) AS sent_at,
               'silent' AS source
        FROM sales_dialog_leads l
        CROSS JOIN LATERAL (SELECT coalesce(l.all_chat_ids, ARRAY[l.chat_id]) AS ids) c
        LEFT JOIN LATERAL (
            SELECT w.chat_id, w.chat_type, w.sent_at FROM wazzup_messages w
            WHERE w.chat_id = ANY(c.ids) AND w.is_outbound = false
            ORDER BY w.sent_at DESC LIMIT 1
        ) lastin ON true
        WHERE l.campaign = %s AND l.status = 'active'
          -- Первым пишем только туда, где клиент хоть раз писал сам (собственник
          -- 30.09.2026, «нас блокируют из-за подозрений на спам»): 29.09 агент дал
          -- 79 из 117 сообщений в чаты без единого входящего, и MAX-канал отключили.
          AND lastin.chat_id IS NOT NULL
          AND NOT l.no_initiative
          AND (l.activate_on IS NULL
               OR l.activate_on <= (now() AT TIME ZONE 'Europe/Moscow')::date)
          AND (SELECT max(w.sent_at) AT TIME ZONE 'UTC' FROM wazzup_messages w
                WHERE w.chat_id = ANY(c.ids)) < now() - (%s || ' days')::interval
          AND (SELECT count(*) FROM sales_dialog_messages d
                WHERE d.campaign = l.campaign AND d.lead_id = l.lead_id
                  AND d.verdict = 'sent'
                  AND d.sent_at > coalesce(lastin.sent_at AT TIME ZONE 'UTC',
                                           '-infinity'::timestamptz)) < %s
          AND coalesce((SELECT max(w.sent_at) AT TIME ZONE 'UTC' FROM wazzup_messages w
                         WHERE w.chat_id = ANY(c.ids) AND w.is_outbound = true),
                       '-infinity'::timestamptz) <= %s
          AND NOT EXISTS (
              SELECT 1 FROM sales_dialog_messages d
              WHERE d.campaign = l.campaign AND d.lead_id = l.lead_id
                AND d.created_at >= date_trunc('day', now() AT TIME ZONE 'Europe/Moscow')
                                    AT TIME ZONE 'Europe/Moscow')
        -- Кого не успели разобрать в свой день, тот идёт первым на следующий:
        -- партия на день ограничена квотой, а не составом пула (собственник
        -- 28.09.2026 – «тех, кого забыли сегодня, в тот же список»).
        -- Неохваченные – раньше повторных касаний (собственник 29.09.2026:
        -- «всех неохваченных всегда перетягивай на завтра»): иначе повторы по
        -- позавчерашней партии съедают дневную квоту, и новые лиды стоят.
        ORDER BY (coalesce(l.replies_sent, 0) > 0 OR EXISTS (
                     SELECT 1 FROM sales_dialog_messages d
                     WHERE d.campaign = l.campaign AND d.lead_id = l.lead_id
                       AND d.verdict = 'sent')),
                 l.activate_on NULLS FIRST, l.id
    """, (campaign, str(silent_days), max_followups, not_before))


def _heartbeat(db, status: str, problem: bool = False) -> None:
    """Отметка живости тика прямо в БД.

    Логи Amvera снимаются только из TTY, поэтому диагностику держим там, где её
    видно снаружи: ключи `sales_dialog:_heartbeat` и `_lasterror` в `bot_settings`.
    Проблемы пишем отдельным ключом: иначе следующий успешный тик затирает их
    своим «ok» и причина молчания теряется.
    """
    try:
        payload = {"at": datetime.now(MSK).isoformat(timespec="seconds"),
                   "status": status[:400], "code": PROMPT_VERSION,
                   "build": CODE_VERSION}
        v = json.dumps(payload, ensure_ascii=False)
        key = SETTINGS_PREFIX + ("_lasterror" if problem else "_heartbeat")
        db._execute("""INSERT INTO bot_settings (key, value) VALUES (%s, %s)
                       ON CONFLICT (key) DO UPDATE SET value=%s""", (key, v, v))
    except Exception:
        pass


async def tick(app, db) -> None:
    global _tables_ready
    try:
        if not _tables_ready:
            ensure_tables(db)
            _tables_ready = True
        campaigns = [c for c in _campaigns(db) if not c.startswith("_")]
    except Exception as e:
        _heartbeat(db, f"старт тика упал: {type(e).__name__}: {e}", problem=True)
        logger.error("sales_dialog: старт тика: %s", e, exc_info=True)
        return
    for campaign in campaigns:
        try:
            await _tick_campaign(app, db, campaign)
        except Exception as e:
            _heartbeat(db, f"{campaign}: {type(e).__name__}: {e}", problem=True)
            logger.error("sales_dialog[%s]: %s", campaign, e, exc_info=True)


async def _apply_refusals(app, db, rows: list) -> list:
    """Отказ клиента во входящем: «не пишите» – стоп навсегда и без ответа,
    «сейчас не актуально» – ответить можно, первым больше не пишем. Слово
    «спам» ещё и ставит инициативу в канале на паузу: жалоба – прямой путь к бану.
    """
    out = []
    for r in rows:
        kind = refusal_kind(r.get("inbound_text"))
        if kind == "hard":
            db._execute("""UPDATE sales_dialog_leads SET status='stop', no_initiative=true, note=%s
                           WHERE campaign=%s AND lead_id=%s""",
                        (f"клиент просит не писать: «{(r.get('inbound_text') or '')[:120]}»",
                         r["campaign"], r["lead_id"]))
            if SPAM_RE.search(r.get("inbound_text") or ""):
                await pause_channel(app, db, row_channel(r),
                                    f"клиент {r.get('lead_name') or r['lead_id']} написал «спам»")
            continue
        if kind == "soft":
            db._execute("""UPDATE sales_dialog_leads SET no_initiative=true
                           WHERE campaign=%s AND lead_id=%s""", (r["campaign"], r["lead_id"]))
        out.append(r)
    return out


async def _tick_campaign(app, db, campaign: str) -> None:
    cfg = _load_cfg(db, campaign)
    if not cfg.get("enabled"):
        return
    now = datetime.now(MSK)
    # Забираем новые сайт-лиды круглосуточно: ночная заявка не должна утром
    # уйти менеджеру раньше, чем агенту. Пишет агент всё равно только в окно.
    if cfg.get("intake"):
        async with aiohttp.ClientSession() as session:
            await intake_new(app, db, session, campaign, cfg)
    if not in_window(now, cfg):
        return
    # Отложенный старт: кампанию можно держать включённой, но не давать ей
    # работать до нужного дня — пересборка промпта закончилась вечером, а
    # начинать договорились утром.
    not_before = cfg.get("not_before_date")
    if not_before and now.date().isoformat() < not_before:
        return

    # Протухшие карточки гасим до отбора: иначе одна неразобранная блокирует
    # очередь до конца дня (25.09.2026 так встали все партии после 11:28).
    await expire_stale(app, db, campaign)
    await nudge(app, db, campaign, cfg, now)
    await daily_digest(app, db, campaign, cfg, now)

    # Ответ клиента идёт вне очереди: он ждать не должен (собственник 24.09.2026).
    # Придержать можно только инициативу в молчащий диалог — чтобы карточки не
    # сыпались быстрее, чем человек успевает по ним решать.
    rows = await _apply_refusals(app, db, _pending_inbound(db, campaign))
    n_inbound = len(rows)
    # Первое сообщение новому лиду – тоже вне очереди: заявка остывает за часы.
    # Лиды, перенесённые партией (`activate_on`), идут общей очередью по одной
    # карточке: 29.09.2026 разом переносили 17 открытых сайт-лидов.
    batch_first = []
    if cfg.get("first_touch"):
        seen = {r["lead_id"] for r in rows}
        for r in _pending_first(db, campaign):
            if r["lead_id"] in seen:
                continue
            (batch_first if r.get("activate_on") else rows).append(r)
        n_inbound = len(rows)

    pending = db._fetchone("""SELECT count(*) AS n FROM sales_dialog_messages
                              WHERE campaign=%s AND verdict IN ('draft','edited')""", (campaign,))
    n_pending = (pending or {}).get("n", 0)

    # Суточный потолок: защита от лавины, если очередь вдруг окажется большой.
    sent_today = db._fetchone("""SELECT count(*) AS n FROM sales_dialog_messages
                                 WHERE campaign=%s AND verdict='sent'
                                   AND sent_at >= date_trunc('day', now() AT TIME ZONE 'Europe/Moscow')
                                                  AT TIME ZONE 'Europe/Moscow'""",
                              (campaign,))
    if (sent_today or {}).get("n", 0) >= cfg.get("daily_cap", 20):
        _heartbeat(db, f"дневной потолок отправок исчерпан: {(sent_today or {}).get('n')}")
        return

    if cfg.get("revive", True):
        seen = {r["lead_id"] for r in rows}
        # Пауза между двумя касаниями молчащего диалога — в рабочих днях, чтобы
        # пятничное сообщение не превращалось в воскресное.
        not_before = workdays_ago(now, cfg.get("followup_workdays", 2))
        silent = [r for r in _pending_silent(db, campaign,
                                             cfg.get("silent_days", 3),
                                             cfg.get("max_followups", 2),
                                             not_before)
                  if r["lead_id"] not in seen]
        first_ids = {r["lead_id"] for r in batch_first}
        silent = [r for r in silent if r["lead_id"] not in first_ids]
        rows += _allowed_silent(db, campaign, batch_first + silent, cfg, now)
    _heartbeat(db, f"очередь: входящих {n_inbound}, первых и оживления {len(rows) - n_inbound}, "
                   f"ждут решения {n_pending}")
    if not rows:
        return
    # Потолок на тик — на каждого адресата свой: у Коликова и Скляр партии идут
    # параллельно, и одна висящая карточка не должна тормозить второго.
    cap = cfg.get("drafts_per_tick", 1)
    taken: dict = {}
    async with aiohttp.ClientSession() as session:
        rows = await gate_initiatives(app, db, session, rows, now)
        for row in rows:
            key = row.get("assignee_amo_id")
            if taken.get(key, 0) >= cap:
                continue
            taken[key] = taken.get(key, 0) + 1
            await _handle_one(app, db, session, campaign, row, cfg)
            if is_initiative(row):
                note_initiative(db, channel_policy(db), row_channel(row), now)


async def _tell(app, chat: int, text: str) -> None:
    try:
        await app.bot.send_message(chat, text)
    except Exception as e:
        logger.warning("sales_dialog: сообщение в %s не ушло: %s", chat, e)


async def expire_stale(app, db, campaign: str) -> None:
    """Черновик старше TTL снимаем сами, не дожидаясь кнопки.

    До 25.09.2026 срок проверялся только в момент нажатия «Отправить»: карточка
    висела в `draft`, `max_pending` держал очередь, и агент стоял с 11:28 до
    вечера. Теперь тик гасит её сам и говорит адресату, что ответ пересоберётся.
    """
    stale = db._fetchall("""SELECT id, lead_id, assignee_amo_id, campaign
                            FROM sales_dialog_messages
                            WHERE campaign=%s AND verdict IN ('draft','edited')
                              AND created_at < now() - (%s || ' minutes')::interval""",
                         (campaign, str(APPROVAL_TTL_MIN)))
    for row in stale:
        db._execute("UPDATE sales_dialog_messages SET verdict='expired' WHERE id=%s", (row["id"],))
        lead = db._fetchone("""SELECT lead_name FROM sales_dialog_leads
                               WHERE campaign=%s AND lead_id=%s""", (campaign, row["lead_id"]))
        who = (lead or {}).get("lead_name") or f"сделка {row['lead_id']}"
        if app:
            chat, _ = card_recipient(db, row)
            await _tell(app, chat, f"{who}: черновик пролежал больше "
                                   f"{APPROVAL_TTL_MIN // 60} часов и снят. "
                                   f"Если диалог ещё нужен — соберу заново.")


def _nudge_state(db, key: str) -> dict:
    row = db._fetchone("SELECT value FROM bot_settings WHERE key=%s", (SETTINGS_PREFIX + key,))
    try:
        return json.loads((row or {}).get("value") or "{}")
    except Exception:
        return {}


def _save_nudge_state(db, key: str, state: dict) -> None:
    v = json.dumps(state, ensure_ascii=False)
    db._execute("""INSERT INTO bot_settings (key, value) VALUES (%s,%s)
                   ON CONFLICT (key) DO UPDATE SET value=%s""",
                (SETTINGS_PREFIX + key, v, v))


async def nudge(app, db, campaign: str, cfg: dict, now: datetime) -> None:
    """Пинги: карточка лежит без решения и клиент ждёт в забранном диалоге.

    Задача собственника (25.09.2026) — чтобы партия из 30 была отработана за
    день. Замершую работу видно по двум признакам: карточка висит и никто её не
    трогает; клиент написал в диалог, который менеджер забрал себе, и молчание.
    """
    if not app:
        return
    first = int(cfg.get("ping_after_min", 20))
    hang = db._fetchall("""SELECT id, lead_id, campaign, assignee_amo_id, ping_count,
                                  round(extract(epoch from now() - created_at) / 60) AS age_min
                           FROM sales_dialog_messages
                           WHERE campaign=%s AND verdict IN ('draft','edited')
                             AND created_at < now() - (%s || ' minutes')::interval
                             AND (pinged_at IS NULL
                                  OR pinged_at < now() - (%s || ' minutes')::interval)""",
                        (campaign, str(first), str(first)))
    for row in hang:
        if (row["ping_count"] or 0) >= 2:
            continue
        lead = db._fetchone("""SELECT lead_name FROM sales_dialog_leads
                               WHERE campaign=%s AND lead_id=%s""", (campaign, row["lead_id"]))
        who = (lead or {}).get("lead_name") or f"сделка {row['lead_id']}"
        chat, amo = card_recipient(db, row)
        age = int(row["age_min"] or 0)
        # Пинг = сама карточка заново внизу чата (собственник 29.09.2026): текст
        # «ждёт решения» заставлял листать вверх. Старую убираем, чтобы не было двух.
        old = db._fetchone("SELECT tg_message_id FROM sales_dialog_messages WHERE id=%s",
                           (row["id"],))
        if old and old.get("tg_message_id"):
            try:
                await app.bot.delete_message(chat, old["tg_message_id"])
            except Exception as e:
                logger.info("sales_dialog: старую карточку %s не удалить: %s", row["id"], e)
        await send_for_approval(app, db, row["id"])
        # Второй пинг дублируем собственнику: значит человек не в работе.
        if (row["ping_count"] or 0) >= 1 and amo:
            await notify_owner(app, f"{who}: менеджер не разбирает карточку {age} мин, "
                                    f"это второе напоминание.")
        db._execute("""UPDATE sales_dialog_messages
                       SET ping_count=ping_count+1, pinged_at=now() WHERE id=%s""", (row["id"],))

    # Забранные диалоги: клиент ответил, а менеджер молчит.
    wait = int(cfg.get("taken_wait_min", 30))
    state = _nudge_state(db, "_taken_pings")
    changed = False
    rows = db._fetchall("""
        SELECT l.lead_id, l.lead_name, l.assignee_amo_id, l.new_lead_id,
               m.text, round(extract(epoch from now() - m.sent_at AT TIME ZONE 'UTC') / 60) AS age_min
        FROM sales_dialog_leads l
        CROSS JOIN LATERAL (SELECT coalesce(l.all_chat_ids, ARRAY[l.chat_id]) AS ids) c
        JOIN LATERAL (
            SELECT w.text, w.sent_at FROM wazzup_messages w
            WHERE w.chat_id = ANY(c.ids) AND w.is_outbound = false
            ORDER BY w.sent_at DESC LIMIT 1
        ) m ON true
        WHERE l.campaign=%s AND l.status='taken' AND l.assignee_amo_id IS NOT NULL
          AND m.sent_at AT TIME ZONE 'UTC' < now() - (%s || ' minutes')::interval
          AND NOT EXISTS (SELECT 1 FROM wazzup_messages o
                          WHERE o.chat_id = ANY(c.ids) AND o.is_outbound = true
                            AND o.sent_at > m.sent_at)""", (campaign, str(wait)))
    for r in rows:
        age = int(r["age_min"] or 0)
        seen = state.get(str(r["lead_id"])) or {}
        stage = int(seen.get("stage", 0))
        # Первый пинг менеджеру, через час — собственнику, дальше молчим.
        if stage == 0:
            chat = MANAGER_TG.get(r["assignee_amo_id"])
            if chat:
                sub = os.getenv("AMO_SUBDOMAIN", "victorfishtobiz")
                link = (f"\nhttps://{sub}.amocrm.ru/leads/detail/{r['new_lead_id']}"
                        if r.get("new_lead_id") else "")
                await _tell(app, chat, f"{r['lead_name'] or r['lead_id']}: клиент ждёт ответа "
                                       f"{age} мин.{link}\nПоследнее от него: "
                                       f"«{(r['text'] or '')[:200]}»")
            state[str(r["lead_id"])] = {"stage": 1, "at": now.isoformat(timespec="minutes")}
            changed = True
        elif stage == 1 and age >= int(cfg.get("taken_escalate_min", 60)):
            await notify_owner(app, f"{r['lead_name'] or r['lead_id']}: клиент ждёт ответа "
                                    f"{age} мин, менеджер забрал диалог и не отвечает.")
            state[str(r["lead_id"])] = {"stage": 2, "at": now.isoformat(timespec="minutes")}
            changed = True
    if changed:
        _save_nudge_state(db, "_taken_pings", state)


def batch_progress(db, campaign: str) -> list:
    """Прогресс дня по каждому адресату: что выдано и что с этим сделали."""
    return db._fetchall("""
        SELECT assignee_amo_id,
               count(*) AS issued,
               count(*) FILTER (WHERE verdict='sent') AS sent,
               count(*) FILTER (WHERE verdict='taken') AS taken,
               count(*) FILTER (WHERE verdict IN ('skipped','handed')) AS refused,
               count(*) FILTER (WHERE verdict IN ('draft','edited')) AS pending,
               count(*) FILTER (WHERE verdict IN ('expired','stale')) AS lost
        FROM sales_dialog_messages
        WHERE campaign=%s
          AND created_at >= date_trunc('day', now() AT TIME ZONE 'Europe/Moscow')
                            AT TIME ZONE 'Europe/Moscow'
        GROUP BY assignee_amo_id ORDER BY assignee_amo_id""", (campaign,))


AMO_NAMES = {11544494: "Скляр", 12625622: "Баласанян", 12788698: "Мерзлякова",
             13665786: "Коликов", 13746010: "Дьяченко"}


async def daily_digest(app, db, campaign: str, cfg: dict, now: datetime) -> None:
    """Сводка собственнику в заданные часы: кто сколько разобрал из своих 30."""
    if not app:
        return
    hours = cfg.get("digest_hours") or [13, 18]
    if now.hour not in [int(h) for h in hours]:
        return
    state = _nudge_state(db, "_digest")
    mark = f"{now.date().isoformat()}:{now.hour}"
    if state.get("last") == mark:
        return
    rows = batch_progress(db, campaign)
    if not rows:
        return
    target = int(cfg.get("daily_target", 30))
    lines = []
    for r in rows:
        amo = r["assignee_amo_id"]
        name = AMO_NAMES.get(amo, "собственник" if not amo else f"amo {amo}")
        done = (r["sent"] or 0) + (r["taken"] or 0) + (r["refused"] or 0)
        lines.append(f"{name}: разобрано {done} из {target} – отправлено {r['sent']}, "
                     f"забрано {r['taken']}, отклонено {r['refused']}, "
                     f"висит {r['pending']}, протухло {r['lost']}")
    await notify_owner(app, f"Партии на {now:%d.%m} в {now.hour}:00\n" + "\n".join(lines))
    _save_nudge_state(db, "_digest", {"last": mark})


def slot_quota(now: datetime, cfg: dict) -> int:
    """Сколько карточек на оживление положено выдать к этому моменту дня.

    Собственник 25.09.2026: отправка с 9:00, одна карточка раз в 7 минут. Значит
    к 9:00 положена первая, к 9:07 — вторая и так далее, но не больше дневной
    цели. Так менеджер получает работу ровным потоком, а не пачкой из 30 писем.
    """
    start_hour = int(cfg.get("slot_start_hour", cfg.get("start_hour", 9)))
    step = max(1, int(cfg.get("slot_minutes", 7)))
    target = int(cfg.get("daily_target", 30))
    start = now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    if now < start:
        return 0
    return min(target, int((now - start).total_seconds() // 60 // step) + 1)


def _allowed_silent(db, campaign: str, silent: list, cfg: dict, now: datetime) -> list:
    """Фильтр очереди оживления по расписанию и по нагрузке каждого адресата.

    Без адресата (карточки собственника) работает прежнее правило `max_pending`.
    С адресатом добавляется дневная цель и слоты: сколько карточек этому
    менеджеру уже выдано за сегодня против того, сколько положено ко времени.
    """
    max_pending = int(cfg.get("max_pending", 1))
    quota = slot_quota(now, cfg)
    stats = {r["assignee_amo_id"]: r for r in db._fetchall("""
        SELECT assignee_amo_id,
               count(*) FILTER (WHERE verdict IN ('draft','edited')) AS pending,
               count(*) FILTER (WHERE created_at >= date_trunc('day', now() AT TIME ZONE 'Europe/Moscow')
                                                   AT TIME ZONE 'Europe/Moscow') AS today
        FROM sales_dialog_messages WHERE campaign=%s GROUP BY assignee_amo_id""", (campaign,))}
    out = []
    room: dict = {}
    for r in silent:
        amo = r.get("assignee_amo_id")
        st = stats.get(amo) or {}
        pending, today = st.get("pending") or 0, st.get("today") or 0
        planned = room.get(amo, 0)
        if pending + planned >= max_pending:
            continue
        if amo and today + planned >= quota:
            continue
        room[amo] = planned + 1
        out.append(r)
    return out


async def _handle_one(app, db, session, campaign: str, row: dict, cfg: dict) -> None:
    # Клиент написал, пока прежний черновик ждал кнопки, – снимаем его и говорим
    # об этом собственнику, чтобы он не отправил ответ на уже неактуальную реплику.
    if row.get("source") == "inbound":
        stale = db._fetchall("""SELECT id FROM sales_dialog_messages
                                WHERE campaign=%s AND lead_id=%s AND verdict IN ('draft','edited')
                                  AND created_at < %s::timestamp AT TIME ZONE 'UTC'""",
                             (campaign, row["lead_id"], row["sent_at"]))
        if stale:
            db._execute("UPDATE sales_dialog_messages SET verdict='stale' WHERE id = ANY(%s)",
                        ([x["id"] for x in stale],))
            if app:
                await notify_owner(app, f"{row.get('lead_name') or row['lead_id']} · сделка {row['lead_id']}\n"
                                        f"Клиент ответил, пока черновик ждал: «{(row.get('inbound_text') or '')[:150]}»\n"
                                        f"Прежний черновик снят, готовлю новый.")
    ctx = await build_context(db, session, row)
    draft = await generate_draft(ctx, db)
    if not draft:
        _heartbeat(db, f"генерация не дала результата, lead={row['lead_id']}", problem=True)
        return
    draft["text"] = polish(draft.get("text") or "")
    # Пустой текст при action=reply — брак генерации: 28.09.2026 на «Спасибо,
    # большое)» пришла карточка без ответа, и её пришлось отклонять.
    if draft.get("action") == "reply" and not draft["text"].strip():
        draft["action"] = "escalate"
        draft["reason"] = "модель не написала текст ответа – нужен человек"
    problems = (check_prices(draft, ctx["prices"], allowed_numbers(ctx.get("city")))
                + check_style(draft["text"], ctx["persona"]["gender"]))
    # Ссылка в сообщении первым – один из признаков спама для мессенджеров.
    if is_initiative(row) and LINK_RE.search(draft["text"]):
        problems.append("в сообщении первым есть ссылка")
    # Уступка ниже прайса проверяется порогами дашборда, а не на глаз.
    problems += await check_bargain(session, db, draft, ctx["prices"])
    if draft.get("bargain") and not problems:
        logger.info("sales_dialog: торг lead=%s %s", row["lead_id"], draft["bargain"])
    action = draft["action"]
    if problems:
        # Цена разошлась со справочником или текст нарушает запреты — не отправляем.
        action = "escalate"
        draft["reason"] = "проверка не пройдена: " + "; ".join(problems)
        logger.warning("sales_dialog: проверка не пройдена lead=%s %s", row["lead_id"], problems)

    saved = db._fetchone("""INSERT INTO sales_dialog_messages
        (campaign, lead_id, chat_id, chat_type, inbound_message_id, inbound_text, draft_text,
         action, reason, need_check, price_claims, bargain, model, prompt_version, verdict,
         assignee_amo_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'draft',%s)
        ON CONFLICT (inbound_message_id, prompt_version) DO NOTHING
        RETURNING id""",
        (campaign, row["lead_id"], row["chat_id"], row.get("chat_type"),
         row["message_id"], row["inbound_text"],
         draft.get("text"), action, draft.get("reason"), draft.get("need_check"),
         json.dumps(draft.get("price_claims"), ensure_ascii=False),
         json.dumps(draft.get("bargain"), ensure_ascii=False) if draft.get("bargain") else None,
         MODEL, PROMPT_VERSION, row.get("assignee_amo_id")))
    db._execute("""UPDATE sales_dialog_leads SET last_inbound_at=%s
                   WHERE campaign=%s AND lead_id=%s""", (row["sent_at"], campaign, row["lead_id"]))
    logger.info("sales_dialog: черновик lead=%s action=%s", row["lead_id"], action)
    # Карточка собственнику. Без его кнопки клиенту ничего не уходит.
    if saved and app:
        await send_for_approval(app, db, saved["id"])


# ─── подтверждение собственником и отправка ───────────────────────────────────
# Отправка клиенту возможна ТОЛЬКО после нажатия кнопки: автономного режима в
# модуле нет вообще. Механика карточки повторяет `protocol_approval`.
_CB = re.compile(r"^sd:(send|edit|skip|hand|take):(\d+)$")
# Сколько черновик живёт до протухания: ответ на утреннее сообщение, ушедший
# вечером, хуже молчания.
APPROVAL_TTL_MIN = 180
# Сколько ждём текст после нажатия «Правка», если он пришёл не ответом на карточку.
EDIT_WAIT_SEC = 1800


def _owner_id() -> int:
    return int(os.environ["OWNER_CHAT_ID"])


def _card_text(db, msg: dict) -> str:
    lead = db._fetchone("""SELECT lead_name, contact_name, chat_type, chat_id FROM sales_dialog_leads
                           WHERE campaign=%s AND lead_id=%s""", (msg["campaign"], msg["lead_id"]))
    who = (lead or {}).get("lead_name") or f"сделка {msg['lead_id']}"
    chat_id, chat_type = delivery_target(msg, lead)
    channel = (chat_type or "") + ("" if chat_id else " по номеру")
    head = f"{who} · {channel} · сделка {msg['lead_id']}"
    if msg.get("inbound_text"):
        head += f"\n\nКлиент: {msg['inbound_text'][:300]}"
    elif str(msg.get("inbound_message_id") or "").startswith("first:"):
        head += "\n\nНовая заявка с сайта, агент пишет первым."
    else:
        head += "\n\nДиалог затих, агент пишет первым."
    if msg.get("action") == "escalate":
        return (f"{head}\n\nАгент не берётся отвечать сам: {msg.get('reason', '')}"
                f"\n\nЕго черновик:\n{msg.get('draft_text', '')}")
    tail = ""
    bargain = msg.get("bargain")
    if isinstance(bargain, str):
        bargain = json.loads(bargain)
    for b in bargain or []:
        tail += (f"\n\nУступка: {b['code']} – {b['price']:.0f} ₽ при прайсе {b['list_price']:.0f}"
                 + (f", порог {b['floor']}" if b.get("floor") else ""))
    if msg.get("need_check"):
        tail += f"\n\nОбещала уточнить: {msg['need_check']}"
    who_signs = persona_for(msg.get("assignee_amo_id"))["first"]
    return f"{head}\n\nОтвет от имени {who_signs}:\n{msg.get('draft_text', '')}{tail}"


def _keyboard(row_id: int, action: str, to_manager: bool = False):
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    rows = []
    if action != "escalate":
        rows.append([InlineKeyboardButton("Отправить", callback_data=f"sd:send:{row_id}")])
    rows.append([InlineKeyboardButton("Правка", callback_data=f"sd:edit:{row_id}"),
                 InlineKeyboardButton("Не отвечать", callback_data=f"sd:skip:{row_id}")])
    # Менеджеру возвращать лид некому — он и есть менеджер. Ему нужна обратная
    # кнопка: забрать диалог себе, со своей сделкой и своей задачей.
    if to_manager:
        rows.append([InlineKeyboardButton("Забрать", callback_data=f"sd:take:{row_id}")])
    else:
        rows.append([InlineKeyboardButton("Вернуть менеджеру", callback_data=f"sd:hand:{row_id}")])
    return InlineKeyboardMarkup(rows)


def card_recipient(db, msg: dict) -> tuple[int, int | None]:
    """Кому уходит карточка: `(telegram chat, amoCRM id менеджера или None)`.

    Партии спящих с 29.09.2026 разбирают сами менеджеры, каждый свою. Если у
    карточки адресата нет или его Telegram неизвестен — карточка идёт
    собственнику, чтобы диалог не остался без человека.
    """
    amo = msg.get("assignee_amo_id")
    if not amo:
        lead = db._fetchone("""SELECT assignee_amo_id FROM sales_dialog_leads
                               WHERE campaign=%s AND lead_id=%s""",
                            (msg["campaign"], msg["lead_id"]))
        amo = (lead or {}).get("assignee_amo_id")
    chat = MANAGER_TG.get(amo) if amo else None
    if not chat:
        if amo:
            logger.warning("sales_dialog: Telegram менеджера %s неизвестен, карточка собственнику", amo)
        return _owner_id(), None
    return chat, amo


async def send_for_approval(app, db, row_id: int) -> None:
    msg = db._fetchone("SELECT * FROM sales_dialog_messages WHERE id=%s", (row_id,))
    if not msg:
        return
    chat, amo = card_recipient(db, msg)
    try:
        sent = await app.bot.send_message(chat, _card_text(db, msg),
                                          reply_markup=_keyboard(row_id, msg.get("action"),
                                                                 to_manager=bool(amo)))
        db._execute("UPDATE sales_dialog_messages SET tg_message_id=%s WHERE id=%s",
                    (sent.message_id, row_id))
    except Exception as e:
        logger.warning("sales_dialog: карточка не ушла lead=%s: %s", msg["lead_id"], e)


def delivery_target(msg: dict, lead: dict | None) -> tuple:
    """Куда отвечать: чат черновика первичен, карточка лида — запасной вариант.

    У части лидов контакт заведён в двух мессенджерах, и отвечать надо в тот,
    откуда пришло сообщение, а не в основной чат карточки. Фоллбэк нужен для
    старых черновиков, созданных до того, как канал стали записывать.
    """
    lead = lead or {}
    return (msg.get("chat_id") or lead.get("chat_id"),
            msg.get("chat_type") or lead.get("chat_type"))


async def _deliver(db, session, msg: dict, text: str) -> tuple[bool, str]:
    """Отправка клиенту. Канал и чат берём из карточки лида, не из вольного ввода."""
    lead = db._fetchone("""SELECT chat_type, chat_id, phone, contact_id FROM sales_dialog_leads
                           WHERE campaign=%s AND lead_id=%s""", (msg["campaign"], msg["lead_id"]))
    chat_id, chat_type = delivery_target(msg, lead)
    by_phone = not chat_id and chat_type == "telegram" and bool((lead or {}).get("phone"))
    if chat_type not in CHANNEL_IDS or not (chat_id or by_phone):
        return False, f"неизвестный канал {chat_type}"
    payload = {"channelId": CHANNEL_IDS[chat_type], "chatType": chat_type, "text": text}
    if by_phone:
        # Wazzup сам находит Telegram-аккаунт по номеру и возвращает его chatId;
        # нет chatId в ответе – аккаунта нет (проверено 29.09.2026).
        payload["phone"] = lead["phone"]
    else:
        payload["chatId"] = str(chat_id)
    async with session.post(WAZZUP_API_URL, json=payload, headers={
            "Authorization": f"Bearer {os.getenv('WAZZUP_API_KEY', '')}",
            "Content-Type": "application/json"}) as r:
        body = (await r.text())[:300]
        ok = r.status in (200, 201)
    if not by_phone:
        return (True, body) if ok else (False, f"http {r.status}: {body}")
    try:
        found = str((json.loads(body) or {}).get("chatId") or "") if ok else ""
    except ValueError:
        found = ""
    if not found:
        return False, f"{NO_TG}: {body[:150]}"
    await _bind_telegram(db, session, msg, lead, found)
    return True, body


async def _bind_telegram(db, session, msg: dict, lead: dict, chat_id: str) -> None:
    """Номер нашёлся в Telegram: дальше разговор идёт в этом чате.

    Пишем chatId в лид агента, в черновик и в поле TelegramId_WZ контакта –
    по этому полю интеграция Wazzup находит контакт и не заводит дубль сделки.
    """
    db._execute("""UPDATE sales_dialog_leads SET chat_id=%s, all_chat_ids=ARRAY[%s]
                   WHERE campaign=%s AND lead_id=%s""",
                (chat_id, chat_id, msg["campaign"], msg["lead_id"]))
    db._execute("UPDATE sales_dialog_messages SET chat_id=%s WHERE id=%s", (chat_id, msg["id"]))
    if lead.get("contact_id"):
        await _amo_write(session, "/contacts", [{
            "id": lead["contact_id"],
            "custom_fields_values": [{"field_id": AMO_FIELD_TG_ID, "values": [{"value": chat_id}]}]}])


async def _after_send_failure(app, db, row_id: int, info: str) -> str:
    """Номер не нашёлся в Telegram – писать клиенту некуда, лид идёт Инессе на звонок.

    Ошибка самого Wazzup (http 4xx/5xx) – признак проблемы с каналом, в том числе
    бана: инициатива в канале встаёт на паузу до решения собственника.
    """
    if info.startswith("http ") and app:
        msg = db._fetchone("SELECT chat_type FROM sales_dialog_messages WHERE id=%s", (row_id,))
        await pause_channel(app, db, row_channel(msg or {}), f"ошибка отправки: {info[:120]}")
        return ". Инициатива в канале на паузе"
    if not info.startswith(NO_TG) or not app:
        return ""
    msg = db._fetchone("SELECT * FROM sales_dialog_messages WHERE id=%s", (row_id,))
    if not msg:
        return ""
    db._execute("UPDATE sales_dialog_messages SET verdict='handed' WHERE id=%s", (row_id,))
    db._execute("""UPDATE sales_dialog_leads SET status='handed'
                   WHERE campaign=%s AND lead_id=%s""", (msg["campaign"], msg["lead_id"]))
    what = await hand_to_manager(app, db, msg, task_text=(
        "Позвонить клиенту: заявка с сайта, чата в мессенджерах нет, "
        "в Telegram по номеру не нашёлся"))
    return f". Лид передан на звонок: {what}"


def newer_inbound(db, msg: dict) -> dict | None:
    """Входящее, пришедшее уже после того, как черновик был написан.

    Собственник держит карточку в руках не мгновенно, и за это время клиент
    успевает ответить – так было с Кибер домом 23.09.2026: на вопрос «какую из
    двух берём» он написал «только Мурманск», а карточка с тем же вопросом всё
    ещё ждала кнопки. Отправлять такой черновик нельзя: разговор разъедется.
    """
    lead = db._fetchone("""SELECT all_chat_ids, chat_id FROM sales_dialog_leads
                           WHERE campaign=%s AND lead_id=%s""", (msg["campaign"], msg["lead_id"]))
    chats = (lead or {}).get("all_chat_ids") or [msg.get("chat_id")]
    return db._fetchone("""SELECT text, sent_at FROM wazzup_messages
                           WHERE chat_id = ANY(%s) AND is_outbound = false
                             AND sent_at AT TIME ZONE 'UTC' > %s
                           ORDER BY sent_at DESC LIMIT 1""", (list(chats), msg["created_at"]))


async def notify_owner(app, text: str) -> None:
    try:
        await app.bot.send_message(_owner_id(), text)
    except Exception as e:
        logger.warning("sales_dialog: уведомление не ушло: %s", e)


async def _do_send(db, row_id: int, text: str) -> tuple[bool, str]:
    msg = db._fetchone("SELECT * FROM sales_dialog_messages WHERE id=%s", (row_id,))
    if not msg:
        return False, "черновик не найден"
    if msg.get("verdict") not in ("draft", "edited"):
        return False, f"черновик уже обработан ({msg.get('verdict')})"
    age_min = (datetime.now(timezone.utc) - msg["created_at"]).total_seconds() / 60
    if age_min > APPROVAL_TTL_MIN:
        # Ответ на утреннее сообщение, ушедший вечером, хуже молчания.
        db._execute("UPDATE sales_dialog_messages SET verdict='expired' WHERE id=%s", (row_id,))
        return False, f"черновик устарел ({int(age_min)} мин), не отправлен"
    if not (text or "").strip():
        return False, "текст пустой"
    if CARD_TRACE_RE.search(text):
        # 28.09.2026 в чат клиента уехала шапка карточки: «ФАРШ · max · сделка …».
        return False, "в тексте остались служебные строки карточки, не отправлено"
    if is_initiative(msg):
        policy = channel_policy(db)
        ch = row_channel(msg)
        if ch in (policy.get("paused") or {}):
            return False, f"инициатива в {ch} на паузе: {policy['paused'][ch]}"
        sent_today = db._fetchone("""SELECT count(*) AS n FROM sales_dialog_messages
            WHERE (inbound_message_id LIKE %s OR inbound_message_id LIKE %s)
              AND verdict='sent' AND coalesce(chat_type, 'telegram')=%s
              AND sent_at >= date_trunc('day', now() AT TIME ZONE 'Europe/Moscow')
                             AT TIME ZONE 'Europe/Moscow'""", ("silent:%", "first:%", ch))
        if (sent_today or {}).get("n", 0) >= int((policy.get("limits") or {}).get(ch, 0)):
            return False, f"дневной лимит сообщений первым в {ch} исчерпан"
    fresh = newer_inbound(db, msg)
    if fresh:
        db._execute("UPDATE sales_dialog_messages SET verdict='stale' WHERE id=%s", (row_id,))
        return False, ("клиент ответил после черновика: «"
                       + (fresh.get("text") or "")[:120] + "» – ответ пересобираю")
    async with aiohttp.ClientSession() as session:
        ok, info = await _deliver(db, session, msg, text)
    if ok:
        db._execute("""UPDATE sales_dialog_messages SET verdict='sent', final_text=%s, sent_at=now()
                       WHERE id=%s""", (text, row_id))
        db._execute("""UPDATE sales_dialog_leads SET replies_sent=replies_sent+1, last_outbound_at=now()
                       WHERE campaign=%s AND lead_id=%s""", (msg["campaign"], msg["lead_id"]))
        logger.info("sales_dialog: отправлено клиенту lead=%s", msg["lead_id"])
    else:
        db._execute("UPDATE sales_dialog_messages SET verdict='send_failed', reason=%s WHERE id=%s",
                    (info[:500], row_id))
    return ok, info


def register(app, db) -> None:
    """Кнопки под карточкой и приём отредактированного текста ответом на неё."""
    from telegram.ext import CallbackQueryHandler, MessageHandler, filters

    async def on_button(update, context):
        q = update.callback_query
        m = _CB.match(q.data or "")
        if not m:
            await q.answer("Не разобрал кнопку")
            return
        action, row_id = m.group(1), int(m.group(2))
        # Решает тот, кому карточку послали: собственник — по своим, менеджер —
        # по своей партии. Чужую карточку нажать нельзя.
        card = db._fetchone("SELECT assignee_amo_id FROM sales_dialog_messages WHERE id=%s",
                            (row_id,)) or {}
        allowed = {_owner_id()}
        card_amo = card.get("assignee_amo_id")
        if card_amo and MANAGER_TG.get(card_amo):
            allowed.add(MANAGER_TG[card_amo])
        if not q.from_user or q.from_user.id not in allowed:
            await q.answer("Эта карточка не твоя.", show_alert=True)
            return
        base = (q.message.text or "").split("\n\n[")[0]

        if action == "skip":
            db._execute("UPDATE sales_dialog_messages SET verdict='skipped' WHERE id=%s", (row_id,))
            await q.edit_message_text(base + "\n\n[не отвечаем]")
        elif action == "hand":
            msg = db._fetchone("SELECT * FROM sales_dialog_messages WHERE id=%s", (row_id,))
            db._execute("UPDATE sales_dialog_messages SET verdict='handed' WHERE id=%s", (row_id,))
            if msg:
                db._execute("""UPDATE sales_dialog_leads SET status='handed'
                               WHERE campaign=%s AND lead_id=%s""", (msg["campaign"], msg["lead_id"]))
                await q.answer("Передаю…")
                what = await hand_to_manager(app, db, msg)
            else:
                what = "черновик не найден"
            await q.edit_message_text(base + f"\n\n[возвращено менеджеру: {what}. Агент по этому лиду молчит]")
        elif action == "take":
            msg = db._fetchone("SELECT * FROM sales_dialog_messages WHERE id=%s", (row_id,))
            if not msg:
                await q.edit_message_text(base + "\n\n[черновик не найден]")
            elif not card_amo:
                await q.edit_message_text(base + "\n\n[у карточки нет менеджера, "
                                                 "забрать может только он]")
            else:
                db._execute("UPDATE sales_dialog_messages SET verdict='taken' WHERE id=%s",
                            (row_id,))
                await q.answer("Забираю…")
                what = await take_by_manager(app, db, msg, card_amo)
                await q.edit_message_text(base + f"\n\n[забрано: {what}. Агент по этому лиду молчит]")
        elif action == "edit":
            context.user_data["sd_edit_row"] = (row_id, datetime.now(timezone.utc))
            await q.edit_message_text(base + "\n\n[жду твой текст — ответом на карточку "
                                             "или просто следующим сообщением]")
        elif action == "send":
            msg = db._fetchone("SELECT draft_text FROM sales_dialog_messages WHERE id=%s", (row_id,))
            ok, info = await _do_send(db, row_id, (msg or {}).get("draft_text") or "")
            if not ok:
                info += await _after_send_failure(app, db, row_id, info)
            await q.edit_message_text(base + ("\n\n[отправлено клиенту]" if ok else f"\n\n[не отправлено: {info}]"))
        await q.answer()

    async def on_edit_reply(update, context):
        """Текст собственника после «Правки» — это правка черновика.

        Ловим любое его сообщение, не только ответ-реплай: 24.09.2026 правка
        «Готовы предложить 2210р» была написана обычным сообщением и уехала в
        общий гейт «бот только оповещает». Порядок такой:
        1) ответ на карточку — правим её черновик, по `tg_message_id`;
        2) иначе — черновик, по которому недавно нажали «Правка»;
        3) иначе молчим и пропускаем сообщение дальше.
        """
        from telegram.ext import ApplicationHandlerStop

        if not update.effective_message:
            return
        reply_to = update.effective_message.reply_to_message
        row_id = None
        who = update.effective_user.id if update.effective_user else None
        if reply_to:
            row = db._fetchone("""SELECT id, assignee_amo_id FROM sales_dialog_messages
                                  WHERE tg_message_id=%s""", (reply_to.message_id,))
            # Правит тот, кому карточка адресована: у менеджеров партии свои.
            amo = (row or {}).get("assignee_amo_id")
            mine = who == _owner_id() or (bool(amo) and MANAGER_TG.get(amo) == who)
            row_id = (row or {}).get("id") if mine else None
        if not row_id:
            pending = context.user_data.get("sd_edit_row")
            if pending:
                candidate, at = pending
                fresh = (datetime.now(timezone.utc) - at).total_seconds() <= EDIT_WAIT_SEC
                context.user_data.pop("sd_edit_row", None)
                if fresh:
                    row_id = candidate
        if not row_id:
            return
        context.user_data.pop("sd_edit_row", None)
        text = strip_card_header(update.effective_message.text or "")
        db._execute("UPDATE sales_dialog_messages SET verdict='edited', final_text=%s WHERE id=%s",
                    (text, row_id))
        ok, info = await _do_send(db, row_id, text)
        if not ok:
            info += await _after_send_failure(app, db, row_id, info)
        await update.effective_message.reply_text(
            "Отправлено клиенту." if ok else f"Не отправлено: {info}")
        # Иначе следом ответит общий гейт «я только присылаю уведомления».
        raise ApplicationHandlerStop

    app.add_handler(CallbackQueryHandler(on_button, pattern=r"^sd:"))
    # Группа -1: общий гейт «бот только оповещает» стоит там же и ловит только
    # сообщения, начинающиеся с «/», поэтому обычный ответ до нас доходит.
    # Правку пишет тот, кому пришла карточка: собственник или менеджер партии.
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND
        & filters.User([_owner_id(), *MANAGER_TG.values()]), on_edit_reply), group=-1)
