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

import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp

from chat_anonymizer import anonymize, find_leaks
# Каналы и эндпоинт Wazzup общие с рассылкой реактивации — держим в одном месте.
from reactivation_campaign import CHANNEL_IDS, WAZZUP_API_URL

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))
MODEL = "claude-opus-5"
PROMPT_VERSION = "sales-dialog-v7"
# Версия кода — отдельно от версии промпта: менять PROMPT_VERSION ради
# наблюдаемости деплоя нельзя, он входит в ключ идемпотентности.
CODE_VERSION = "followups-2x2"
SETTINGS_PREFIX = "sales_dialog:"
PROMPTS_DIR = Path(__file__).parent / "prompts"

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
AMO_BASE = f"https://{os.getenv('AMO_SUBDOMAIN', 'victorfishtobiz')}.amocrm.ru/api/v4"

# Сколько последних сообщений чата отдаём модели.
HISTORY_LIMIT = 25
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
    db._execute("""CREATE INDEX IF NOT EXISTS sales_dialog_leads_status_idx
                   ON sales_dialog_leads (campaign, status)""")
    db._execute("""CREATE INDEX IF NOT EXISTS sales_dialog_messages_lead_idx
                   ON sales_dialog_messages (campaign, lead_id, created_at)""")


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


# ─── внешние системы ──────────────────────────────────────────────────────────
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
        out.append({"code": r.get("code"), "name": r.get("name"),
                    "opt": prices.get("Цена опт"), "horeca": prices.get("Цена продажи"),
                    "spec": prices.get("Спец."), "stock": round(r.get("stock") or 0, 1),
                    # Собственное производство делаем под заказ, поэтому нулевой остаток
                    # по нему — не «нет», а «сделаем». Привлечённые товары так нельзя:
                    # там ноль означает, что позицию надо закупить.
                    "own": path.startswith("ГОТОВАЯ ПРОДУКЦИЯ")})
    _price_cache.update({"at": now, "rows": out})
    logger.info("sales_dialog: прайс обновлён, позиций %s", len(out))
    return out


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


def _history(db, chat_id: str) -> list:
    rows = db._fetchall("""SELECT sent_at, is_outbound, COALESCE(text,'') AS text
                           FROM wazzup_messages WHERE chat_id=%s
                           ORDER BY sent_at DESC LIMIT %s""", (chat_id, HISTORY_LIMIT))
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


def _format_prices(rows: list) -> str:
    """В промпт идут позиции с остатком, все спеццены и вся своя готовая продукция."""
    return "\n".join(
        f"{p['code']} | {p['name']} | опт {p['opt']} | horeca {p['horeca'] or '-'} | "
        f"спец {p['spec'] or '-'} | остаток {p['stock']} | "
        f"{'наше производство' if p.get('own') else 'привлечённый товар'}"
        for p in rows if p["stock"] > 0 or p["spec"] or p.get("own"))


async def build_context(db, session: aiohttp.ClientSession, row: dict) -> dict:
    """Готовит всё, что уходит в модель. Переписка — обезличенная."""
    lead = await _amo_lead(session, row["lead_id"])
    history = _history(db, row["chat_id"])
    raw = "\n".join(
        f"[{h['sent_at']:%d.%m %H:%M}] {'МЕНЕДЖЕР' if h['is_outbound'] else 'КЛИЕНТ'}: {h['text']}"
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
    prices = await _ms_price_rows(session)
    objections = (PROMPTS_DIR / "objections.md").read_text(encoding="utf-8")[:4000]
    last_in = next((h for h in reversed(history) if not h["is_outbound"]), None)
    already = db._fetchone("""SELECT count(*) AS n FROM sales_dialog_messages
                              WHERE campaign=%s AND lead_id=%s AND verdict='sent'""",
                           (row["campaign"], row["lead_id"]))
    first_time = not (already or {}).get("n")
    days = (datetime.now() - history[-1]["sent_at"]).days if history else 0

    user = f"""ПЕРЕПИСКА (последнее сообщение {days} дн. назад):
{safe}

КАРТОЧКА: «{lead.get('name', '')}», лид с сайта f2b.group.
{"Ты пишешь в этот чат ВПЕРВЫЕ — до тебя его вёл другой менеджер." if first_time else "Ты уже писала в этот чат, представляться повторно не нужно."}

ДОСТАВКА:
{delivery_note(city)}

СПРАВОЧНИК ЦЕН И ОСТАТКОВ (единственный источник цифр):
{_format_prices(prices)}

БАЗА ВОЗРАЖЕНИЙ:
{objections}

Напиши сообщение клиенту."""
    return {"system": (PROMPTS_DIR / "sales_dialog_system.md").read_text(encoding="utf-8"),
            "user": user, "leaks": leaks, "prices": {p["code"]: p for p in prices},
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
    payload = {"model": MODEL, "max_tokens": 8000,
               "system": ctx["system"] + JSON_RULE,
               "messages": [{"role": "user", "content": ctx["user"]}]}
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
        actual = p.get(c["price_type"])
        if not actual or abs(float(actual) - float(c["price"])) > 0.01:
            problems.append(f"{c['code']}: сказано {c['price']}, в МойСклад {actual}")
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


def check_style(text: str) -> list:
    """Стилевые запреты, которые нельзя доверять одному промпту.

    Мужской род от первого лица (23.09.2026 модель написала «Понял, торопить не
    буду» от имени Инессы) и любое упоминание звонка — собственник запретил
    звонки отдельно, пилот только переписка.
    """
    problems = []
    masc = re.findall(r"\b(понял|прошёл|прошел|посмотрел|уточнил|написал|сделал|проверил|"
                      r"добавил|отправил|подготовил|связался|уточню-ка|рад|готов)\b",
                      (text or "").lower())
    if masc:
        problems.append("мужской род от первого лица: " + ", ".join(sorted(set(masc))))
    call = re.findall(r"\b(позвон\w*|созвон\w*|наберу|набер[её]м|перезвон\w*|звонок|"
                      r"телефон\w* для связи)\b", (text or "").lower())
    if call:
        problems.append("упоминание звонка: " + ", ".join(sorted(set(call))))
    return problems


# ─── тик ──────────────────────────────────────────────────────────────────────
def _pending_inbound(db, campaign: str) -> list:
    """Клиент написал, и после его сообщения мы ещё не отвечали.

    Потолка на число ответов здесь нет сознательно (правило собственника от
    23.09.2026): пока клиент задаёт вопросы, на них отвечают, сколько бы их ни
    было. Ограничение касается только инициативы в молчащий диалог, см.
    `_pending_silent`.

    Условие про исходящие важно: если менеджер успел ответить руками, агент в
    разговор не лезет. Идемпотентность — по `message_id` входящего.
    """
    return db._fetchall("""
        SELECT l.campaign, l.lead_id, l.contact_id, l.chat_id, l.chat_type,
               l.lead_name, l.contact_name, l.replies_sent,
               m.message_id, m.text AS inbound_text, m.sent_at, 'inbound' AS source
        FROM sales_dialog_leads l
        JOIN LATERAL (
            SELECT message_id, text, sent_at FROM wazzup_messages w
            WHERE w.chat_id = l.chat_id AND w.is_outbound = false
            ORDER BY w.sent_at DESC LIMIT 1
        ) m ON true
        WHERE l.campaign = %s AND l.status = 'active'
          AND NOT EXISTS (
              SELECT 1 FROM sales_dialog_messages d
              WHERE d.inbound_message_id = m.message_id AND d.prompt_version = %s)
          AND NOT EXISTS (
              SELECT 1 FROM wazzup_messages o
              WHERE o.chat_id = l.chat_id AND o.is_outbound = true AND o.sent_at > m.sent_at)
    """, (campaign, PROMPT_VERSION))


def _pending_silent(db, campaign: str, silent_days: int,
                    max_followups: int, not_before: datetime) -> list:
    """Диалог затих — агент пишет в него сам, но не больше пары раз.

    Правило собственника (23.09.2026): на вопросы клиента отвечаем всегда, а
    молчащего трогаем не более `max_followups` раз подряд с паузой в пару
    рабочих дней. Дальше лид ждёт — статус остаётся `active`, и как только
    клиент напишет, разговор подхватит `_pending_inbound`, а счётчик
    обнулится сам, потому что считается от последнего входящего.

    Пауза отмеряется от последнего исходящего в чате, включая ручное сообщение
    менеджера: если человек только что написал сам, агент сверху не пишет.

    Ключ идемпотентности — лид плюс сегодняшняя дата, и ровно то же условие
    стоит в отборе («сегодня по лиду черновиков ещё не было»). Если отбор и
    ключ расходятся, `ON CONFLICT DO NOTHING` молча гасит вставку и тик
    крутится вхолостую без единой ошибки — уже обжигались.
    """
    return db._fetchall("""
        SELECT l.campaign, l.lead_id, l.contact_id, l.chat_id, l.chat_type,
               l.lead_name, l.contact_name, l.replies_sent,
               'silent:' || l.lead_id || ':' ||
                 to_char(now() AT TIME ZONE 'Europe/Moscow', 'YYYYMMDD') AS message_id,
               NULL AS inbound_text,
               (SELECT max(w.sent_at) AT TIME ZONE 'UTC' FROM wazzup_messages w
                 WHERE w.chat_id = l.chat_id) AS sent_at,
               'silent' AS source
        FROM sales_dialog_leads l
        WHERE l.campaign = %s AND l.status = 'active'
          AND (SELECT max(w.sent_at) AT TIME ZONE 'UTC' FROM wazzup_messages w
                WHERE w.chat_id = l.chat_id) < now() - (%s || ' days')::interval
          AND (SELECT count(*) FROM sales_dialog_messages d
                WHERE d.campaign = l.campaign AND d.lead_id = l.lead_id
                  AND d.verdict = 'sent'
                  AND d.sent_at > coalesce(
                      (SELECT max(w.sent_at) AT TIME ZONE 'UTC' FROM wazzup_messages w
                        WHERE w.chat_id = l.chat_id AND w.is_outbound = false),
                      '-infinity'::timestamptz)) < %s
          AND coalesce((SELECT max(w.sent_at) AT TIME ZONE 'UTC' FROM wazzup_messages w
                         WHERE w.chat_id = l.chat_id AND w.is_outbound = true),
                       '-infinity'::timestamptz) <= %s
          AND NOT EXISTS (
              SELECT 1 FROM sales_dialog_messages d
              WHERE d.campaign = l.campaign AND d.lead_id = l.lead_id
                AND d.created_at >= date_trunc('day', now() AT TIME ZONE 'Europe/Moscow')
                                    AT TIME ZONE 'Europe/Moscow')
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


async def _tick_campaign(app, db, campaign: str) -> None:
    cfg = _load_cfg(db, campaign)
    if not cfg.get("enabled"):
        return
    now = datetime.now(MSK)
    if not in_window(now, cfg):
        return

    # Суточный потолок: защита от лавины, если очередь вдруг окажется большой.
    sent_today = db._fetchone("""SELECT count(*) AS n FROM sales_dialog_messages
                                 WHERE campaign=%s AND verdict='sent'
                                   AND sent_at >= date_trunc('day', now() AT TIME ZONE 'Europe/Moscow')
                                                  AT TIME ZONE 'Europe/Moscow'""",
                              (campaign,))
    if (sent_today or {}).get("n", 0) >= cfg.get("daily_cap", 20):
        _heartbeat(db, f"дневной потолок отправок исчерпан: {(sent_today or {}).get('n')}")
        return

    # Ответ живому клиенту важнее, чем оживление молчащего диалога.
    rows = _pending_inbound(db, campaign)
    n_inbound = len(rows)
    if cfg.get("revive", True):
        seen = {r["lead_id"] for r in rows}
        # Пауза между двумя касаниями молчащего диалога — в рабочих днях, чтобы
        # пятничное сообщение не превращалось в воскресное.
        not_before = workdays_ago(now, cfg.get("followup_workdays", 2))
        rows += [r for r in _pending_silent(db, campaign,
                                            cfg.get("silent_days", 3),
                                            cfg.get("max_followups", 2),
                                            not_before)
                 if r["lead_id"] not in seen]
    _heartbeat(db, f"очередь: входящих {n_inbound}, оживление {len(rows) - n_inbound}")
    if not rows:
        return
    cap = cfg.get("drafts_per_tick", 1)
    async with aiohttp.ClientSession() as session:
        for row in rows[:cap]:
            await _handle_one(app, db, session, campaign, row, cfg)


async def _handle_one(app, db, session, campaign: str, row: dict, cfg: dict) -> None:
    ctx = await build_context(db, session, row)
    draft = await generate_draft(ctx, db)
    if not draft:
        _heartbeat(db, f"генерация не дала результата, lead={row['lead_id']}", problem=True)
        return
    problems = (check_prices(draft, ctx["prices"], allowed_numbers(ctx.get("city")))
                + check_style(draft.get("text") or ""))
    action = draft["action"]
    if problems:
        # Цена разошлась со справочником или текст нарушает запреты — не отправляем.
        action = "escalate"
        draft["reason"] = "проверка не пройдена: " + "; ".join(problems)
        logger.warning("sales_dialog: проверка не пройдена lead=%s %s", row["lead_id"], problems)

    saved = db._fetchone("""INSERT INTO sales_dialog_messages
        (campaign, lead_id, chat_id, inbound_message_id, inbound_text, draft_text,
         action, reason, need_check, price_claims, model, prompt_version, verdict)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'draft')
        ON CONFLICT (inbound_message_id, prompt_version) DO NOTHING
        RETURNING id""",
        (campaign, row["lead_id"], row["chat_id"], row["message_id"], row["inbound_text"],
         draft.get("text"), action, draft.get("reason"), draft.get("need_check"),
         json.dumps(draft.get("price_claims"), ensure_ascii=False), MODEL, PROMPT_VERSION))
    db._execute("""UPDATE sales_dialog_leads SET last_inbound_at=%s
                   WHERE campaign=%s AND lead_id=%s""", (row["sent_at"], campaign, row["lead_id"]))
    logger.info("sales_dialog: черновик lead=%s action=%s", row["lead_id"], action)
    # Карточка собственнику. Без его кнопки клиенту ничего не уходит.
    if saved and app:
        await send_for_approval(app, db, saved["id"])


# ─── подтверждение собственником и отправка ───────────────────────────────────
# Отправка клиенту возможна ТОЛЬКО после нажатия кнопки: автономного режима в
# модуле нет вообще. Механика карточки повторяет `protocol_approval`.
_CB = re.compile(r"^sd:(send|edit|skip|hand):(\d+)$")
# Сколько черновик живёт до протухания: ответ на утреннее сообщение, ушедший
# вечером, хуже молчания.
APPROVAL_TTL_MIN = 90


def _owner_id() -> int:
    return int(os.environ["OWNER_CHAT_ID"])


def _card_text(db, msg: dict) -> str:
    lead = db._fetchone("""SELECT lead_name, contact_name, chat_type FROM sales_dialog_leads
                           WHERE campaign=%s AND lead_id=%s""", (msg["campaign"], msg["lead_id"]))
    who = (lead or {}).get("lead_name") or f"сделка {msg['lead_id']}"
    head = f"{who} · {(lead or {}).get('chat_type', '')} · сделка {msg['lead_id']}"
    if msg.get("inbound_text"):
        head += f"\n\nКлиент: {msg['inbound_text'][:300]}"
    else:
        head += "\n\nДиалог затих, агент пишет первым."
    if msg.get("action") == "escalate":
        return (f"{head}\n\nАгент не берётся отвечать сам: {msg.get('reason', '')}"
                f"\n\nЕго черновик:\n{msg.get('draft_text', '')}")
    tail = ""
    if msg.get("need_check"):
        tail = f"\n\nОбещала уточнить: {msg['need_check']}"
    return f"{head}\n\nОтвет от имени Инессы:\n{msg.get('draft_text', '')}{tail}"


def _keyboard(row_id: int, action: str):
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    rows = []
    if action != "escalate":
        rows.append([InlineKeyboardButton("Отправить", callback_data=f"sd:send:{row_id}")])
    rows.append([InlineKeyboardButton("Правка", callback_data=f"sd:edit:{row_id}"),
                 InlineKeyboardButton("Не отвечать", callback_data=f"sd:skip:{row_id}")])
    rows.append([InlineKeyboardButton("Передать Инессе", callback_data=f"sd:hand:{row_id}")])
    return InlineKeyboardMarkup(rows)


async def send_for_approval(app, db, row_id: int) -> None:
    msg = db._fetchone("SELECT * FROM sales_dialog_messages WHERE id=%s", (row_id,))
    if not msg:
        return
    try:
        await app.bot.send_message(_owner_id(), _card_text(db, msg),
                                   reply_markup=_keyboard(row_id, msg.get("action")))
    except Exception as e:
        logger.warning("sales_dialog: карточка не ушла lead=%s: %s", msg["lead_id"], e)


async def _deliver(db, session, msg: dict, text: str) -> tuple[bool, str]:
    """Отправка клиенту. Канал и чат берём из карточки лида, не из вольного ввода."""
    lead = db._fetchone("""SELECT chat_type, chat_id FROM sales_dialog_leads
                           WHERE campaign=%s AND lead_id=%s""", (msg["campaign"], msg["lead_id"]))
    if not lead or lead["chat_type"] not in CHANNEL_IDS:
        return False, f"неизвестный канал {(lead or {}).get('chat_type')}"
    payload = {"channelId": CHANNEL_IDS[lead["chat_type"]], "chatType": lead["chat_type"],
               "chatId": str(lead["chat_id"]), "text": text}
    async with session.post(WAZZUP_API_URL, json=payload, headers={
            "Authorization": f"Bearer {os.getenv('WAZZUP_API_KEY', '')}",
            "Content-Type": "application/json"}) as r:
        body = (await r.text())[:300]
        if r.status in (200, 201):
            return True, body
        return False, f"http {r.status}: {body}"


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
        if not q.from_user or q.from_user.id != _owner_id():
            await q.answer("Только для руководителя.", show_alert=True)
            return
        m = _CB.match(q.data or "")
        if not m:
            await q.answer("Не разобрал кнопку")
            return
        action, row_id = m.group(1), int(m.group(2))
        base = (q.message.text or "").split("\n\n[")[0]

        if action == "skip":
            db._execute("UPDATE sales_dialog_messages SET verdict='skipped' WHERE id=%s", (row_id,))
            await q.edit_message_text(base + "\n\n[не отвечаем]")
        elif action == "hand":
            msg = db._fetchone("SELECT campaign, lead_id FROM sales_dialog_messages WHERE id=%s", (row_id,))
            db._execute("UPDATE sales_dialog_messages SET verdict='handed' WHERE id=%s", (row_id,))
            if msg:
                db._execute("""UPDATE sales_dialog_leads SET status='handed'
                               WHERE campaign=%s AND lead_id=%s""", (msg["campaign"], msg["lead_id"]))
            await q.edit_message_text(base + "\n\n[передано Инессе, агент по этому лиду молчит]")
        elif action == "edit":
            context.user_data["sd_edit_row"] = row_id
            await q.edit_message_text(base + "\n\n[жду твой текст ответом на это сообщение]")
        elif action == "send":
            msg = db._fetchone("SELECT draft_text FROM sales_dialog_messages WHERE id=%s", (row_id,))
            ok, info = await _do_send(db, row_id, (msg or {}).get("draft_text") or "")
            await q.edit_message_text(base + ("\n\n[отправлено клиенту]" if ok else f"\n\n[не отправлено: {info}]"))
        await q.answer()

    async def on_edit_reply(update, context):
        row_id = context.user_data.pop("sd_edit_row", None)
        if not row_id or not update.effective_message:
            return
        text = update.effective_message.text or ""
        db._execute("UPDATE sales_dialog_messages SET verdict='edited', final_text=%s WHERE id=%s",
                    (text, row_id))
        ok, info = await _do_send(db, row_id, text)
        await update.effective_message.reply_text(
            "Отправлено клиенту." if ok else f"Не отправлено: {info}")

    app.add_handler(CallbackQueryHandler(on_button, pattern=r"^sd:"))
    # Группа -1: общий гейт «бот только оповещает» стоит там же и ловит только
    # сообщения, начинающиеся с «/», поэтому обычный ответ до нас доходит.
    app.add_handler(MessageHandler(
        filters.REPLY & filters.TEXT & filters.User(_owner_id()), on_edit_reply), group=-1)
