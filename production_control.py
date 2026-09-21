"""Контроль производства: температурные замеры по расписанию в Telegram.

План: plans/2026-08-25-сбор-ручных-замеров-температуры-в-тг.md (репо «второй мозг»).

Зачем: до установки стационарных датчиков (план 2026-08-21) единственный источник
температуры продукта — руки мастера. Раньше цифры жили в переписке и терялись.
Джобы шлют в группу вопрос по окну, мастер отвечает reply, бот парсит строки и
кладёт в quality.temp_readings — дальше ряд разбирается запросом, а не глазами.

Регламент с 18.09.2026 (решение собственника): три одинаковых напоминания в день
— 09:00, 13:00, 17:00 МСК. В каждом весь перечень: сырьё до начала работ и при
смене, тузлук, цех и камеры хранения. Прежние окна (тушка перед порезкой, филе на
этапах, подготовка к обеду, толща на дефросте) сняты.
"""
import logging
import os
import re

import aiohttp
from datetime import datetime, timedelta, timezone

from telegram import Update
from telegram.ext import MessageHandler, filters

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))

# Группа по умолчанию — «F2B САНКОНТРОЛЬ» (собственник 26.08.2026 переносит сюда
# контроль производства и позже переименует группу в «Контроль производства»).
DEFAULT_CHAT_ID = -5432509607

# Физически осмысленный диапазон для продукта и тузлука в цеху.
T_MIN, T_MAX = -40.0, 40.0

# Дни недели, когда окна работают: 0=Пн … 6=Вс.
# Пн–Пт и Вс: цех работает по воскресеньям, но без технолога — в этот день
# на замеры отвечает мастер смены. Суббота выходная.
WORKDAYS = {int(x) for x in os.getenv("QC_WORKDAYS", "0,1,2,3,4,6").split(",") if x.strip()}


def chat_id() -> int:
    raw = os.getenv("QUALITY_CHAT_ID", "").strip()
    return int(raw) if raw else DEFAULT_CHAT_ID


# ─────────────────────────────────────────────────────────────────────────────
# Окна регламента
# ─────────────────────────────────────────────────────────────────────────────

# Перечень один и тот же во всех трёх напоминаниях: технолог сам решает, что из
# этого стало актуально к моменту напоминания (решение собственника 18.09.2026).
CHECKS = [
    "температура сырья — до начала работ и при каждой смене сырья",
    "температура тузлука",
    "температура в цехе и в камерах хранения",
]

EXAMPLE = ("00614 / лосось Чили / сырьё / -1,5\n"
           "тузлук / 6,0\n"
           "цех / 11,0\n"
           "камера 2 / -18")

WINDOWS = [
    {
        "key": key,
        "utc": utc,
        "kind": "measure",
        # Точка по умолчанию: строка без названия точки — это замер сырья.
        "point": "сырьё",
        "title": "Замеры температуры",
        "checks": CHECKS,
        "fields": "партия / продукт / точка / температура — по строке на каждый замер",
        "example": EXAMPLE,
    }
    for key, utc in (("09:00", (6, 0)), ("13:00", (10, 0)), ("17:00", (14, 0)))
]

WINDOW_BY_KEY = {w["key"]: w for w in WINDOWS}


# ─────────────────────────────────────────────────────────────────────────────
# Схема
# ─────────────────────────────────────────────────────────────────────────────

def ensure_tables(db):
    db._execute("CREATE SCHEMA IF NOT EXISTS quality")
    db._execute(
        """
        CREATE TABLE IF NOT EXISTS quality.control_windows (
            id            BIGSERIAL PRIMARY KEY,
            window_key    TEXT        NOT NULL,
            asked_on      DATE        NOT NULL,
            asked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            chat_id       BIGINT      NOT NULL,
            message_id    BIGINT,
            answered_at   TIMESTAMPTZ,
            reminded      BOOLEAN     NOT NULL DEFAULT FALSE,
            status        TEXT        NOT NULL DEFAULT 'open',
            UNIQUE (window_key, asked_on, chat_id)
        )
        """
    )
    db._execute(
        """
        CREATE TABLE IF NOT EXISTS quality.temp_readings (
            id             BIGSERIAL PRIMARY KEY,
            window_id      BIGINT REFERENCES quality.control_windows(id) ON DELETE SET NULL,
            window_key     TEXT,
            point_key      TEXT        NOT NULL,
            batch_no       TEXT,
            descr          TEXT,
            value_c        NUMERIC(5,2) NOT NULL,
            measured_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            author_tg_id   BIGINT,
            author_name    TEXT,
            chat_id        BIGINT,
            answer_msg_id  BIGINT,
            raw_line       TEXT,
            device         TEXT        NOT NULL DEFAULT 'thermopro',
            is_preliminary BOOLEAN     NOT NULL DEFAULT TRUE,
            sign_pending   BOOLEAN     NOT NULL DEFAULT FALSE,
            superseded     BOOLEAN     NOT NULL DEFAULT FALSE,
            ms_product     TEXT,
            ms_state       TEXT,
            stage          TEXT,
            reported_at    TIMESTAMPTZ,
            cut_note       TEXT,
            stage_source   TEXT
        )
        """
    )
    for col, ddl in (
        ("sign_pending", "BOOLEAN NOT NULL DEFAULT FALSE"),
        ("superseded", "BOOLEAN NOT NULL DEFAULT FALSE"),
        ("ms_product", "TEXT"),
        ("ms_state", "TEXT"),
        ("stage", "TEXT"),
        ("reported_at", "TIMESTAMPTZ"),
        ("cut_note", "TEXT"),
        ("stage_source", "TEXT"),
    ):
        db._execute(
            f"ALTER TABLE quality.temp_readings ADD COLUMN IF NOT EXISTS {col} {ddl}"
        )
    db._execute(
        """
        CREATE TABLE IF NOT EXISTS quality.batches (
            batch_no    TEXT PRIMARY KEY,
            moment      TIMESTAMPTZ,
            description TEXT,
            products    TEXT,
            state       TEXT,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    db._execute(
        """
        CREATE TABLE IF NOT EXISTS quality.chat_log (
            chat_id        BIGINT      NOT NULL,
            message_id     BIGINT      NOT NULL,
            sent_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            author_tg_id   BIGINT,
            author_name    TEXT,
            is_bot         BOOLEAN     NOT NULL DEFAULT FALSE,
            text           TEXT,
            reply_to       BIGINT,
            window_id      BIGINT,
            window_key     TEXT,
            parsed_count   INT         NOT NULL DEFAULT 0,
            PRIMARY KEY (chat_id, message_id)
        )
        """
    )
    db._execute(
        "CREATE INDEX IF NOT EXISTS chat_log_sent_at_idx ON quality.chat_log (sent_at DESC)"
    )
    db._execute(
        "CREATE INDEX IF NOT EXISTS temp_readings_measured_at_idx "
        "ON quality.temp_readings (measured_at DESC)"
    )
    db._execute(
        "CREATE INDEX IF NOT EXISTS temp_readings_batch_idx "
        "ON quality.temp_readings (batch_no)"
    )
    db.conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Разбор ответа
# ─────────────────────────────────────────────────────────────────────────────

_NUM_RE = re.compile(r"^[+-]?\d{1,3}(?:[.,]\d{1,2})?$")
# Число в конце строки. Знак засчитывается ТОЛЬКО если прилеплен к цифрам и сам
# стоит после пробела или в начале: «-12» — знак, «тушка- 6» и «00620- 12» — дефис
# как разделитель, а не минус. Ошибка стоила бы инверсии значений: 26.08 «Лосось
# тушка- 6» разбиралось как −6.
_UNIT = r"(?:°\s*[cCсС]?|[cCсС]\b|градус\w*|град\w*|гр\b)"
_TAIL_NUM_RE = re.compile(
    r"(?:(?<=\s)|^)([-+−])?(\d{1,3}(?:[.,]\d{1,2})?)\s*" + _UNIT + r"?\s*[.!]?\s*$"
)
# Номер партии МойСклад: 5 цифр (00615, 00618, 00620).
_BATCH_RE = re.compile(r"\b(\d{5})\b")
# Ответ на переспрос знака.
# Время замера, если названо: «09:20», «9.20». Даёт reported_at.
_TIME_RE = re.compile(r"^\s*([01]?\d|2[0-3])[:.]([0-5]\d)\s*$")
_SIGN_RE = re.compile(r"^\s*(минус|плюс|[-+−])\s*$", re.IGNORECASE)

# Точки, где значение может быть минусовым: число без знака двусмысленно (Инна
# 26.08 написала «12», имея в виду −12). Переспрашиваем. Сырьё приходит и
# мороженым, камера хранения бывает морозильной.
SIGN_STRICT_POINTS = {"сырьё", "камера хранения", "дефрост-толща"}


def sign_answer(text: str):
    """«минус» / «плюс» / «-» / «+» → -1 / +1, иначе None."""
    m = _SIGN_RE.match(text or "")
    if not m:
        return None
    return -1 if m.group(1).lower() in ("минус", "-", "−") else 1


def _to_float(token: str):
    token = (token or "").strip().replace(",", ".").replace("−", "-")
    # «+ 7.5» внутри поля — знак с пробелом. 27.08 такая строка потерялась целиком.
    token = re.sub(r"^([-+])\s+", r"\1", token)
    token = re.sub(_UNIT + r"\s*[.!]?\s*$", "", token, flags=re.IGNORECASE).strip()
    token = token.rstrip("°cCсС ").strip()
    if not _NUM_RE.match(token):
        return None
    try:
        return float(token)
    except ValueError:
        return None


# Словарь этапов. Люди пишут по-разному — «финишная зачистка» и «стол финишной
# зачистки» это один этап (замечание собственника 26.08). Без сведения к канону
# ряд рассыпается на псевдоэтапы и по нему нельзя посчитать ни одну кривую.
STAGE_CANON = [
    ("дефрост", ("дефрост", "дефростац", "разморозк", "оттаив")),
    ("перед порезкой", ("перед порезк", "перед разделк", "до порезк", "до разделк")),
    ("разделка", ("разделк", "порезк", "филетир")),
    ("финишная зачистка", ("финиш", "зачистк")),
    ("порционирование", ("порцион", "нарезк", "ломтик")),
    ("посол", ("посол", "инъект", "инъекц", "тузлук", "рассол")),
    ("вакуум", ("вакуум",)),
    ("упаковка", ("упаковк", "фасовк")),
    ("охл. камера", ("охл камер", "охл. камер", "охлажд камер", "холодильник", "в камере")),
]


# Как пошёл нож. Ставить эксперимент в производственном режиме нельзя (собственник,
# 26.08), поэтому норматив «при какой температуре брать в разделку» выводится не
# пробами каждые три часа, а одной пометкой к замеру 09:00, который и так делается.
CUT_CANON = [
    ("легко", ("легко", "нормальн", "хорошо", "отлично", "как обычно")),
    ("тяжело", ("тяжело", "туго", "трудно", "плохо", "с усилием")),
    ("не идёт", ("не идет", "не идёт", "не режет", "не порезал", "не удалось", "твёрд", "тверд", "крошит", "рвёт", "рвет")),
]


def cut_note(text: str):
    """Оценка разделки из свободного слова в строке."""
    if not text:
        return None
    low = text.lower()
    for canon, keys in CUT_CANON:
        if any(k in low for k in keys):
            return canon
    return None


def _num_and_note(token: str):
    """«-0,5 тяжело» → (-0.5, 'тяжело'). Число впереди, оценка словом сзади."""
    m = re.match(r"^\s*([-+−]?\s*\d{1,3}(?:[.,]\d{1,2})?)\s*" + _UNIT + r"?\s*(.*)$",
                 token or "", flags=re.IGNORECASE)
    if not m:
        return None, None
    return _to_float(m.group(1)), cut_note(m.group(2))


# Точки замера, которые человек называет словом в начале строки. Без партии и без
# этапа: это среда, а не продукт. Сначала длинные ключи, иначе «камера» перебьёт
# «камера шоковой заморозки».
POINT_HEADS = [
    ("сырьё", ("сырьё", "сырье")),
    ("тузлук", ("тузлук", "рассол")),
    ("цех", ("цех", "в цеху", "в цехе", "воздух в цеху", "воздух в цехе", "помещение")),
    ("камера хранения", ("камера хранения", "камере хранения", "камера", "камере",
                         "камеры", "холодильная камера", "холодильник", "морозильная камера",
                         "морозилка", "склад")),
]

# К этим точкам этап производства не относится: замеряется среда, а не рыба.
POINT_NO_STAGE = {"тузлук", "цех", "камера хранения"}

# Канонические этапы производства. Если этап назван, продукт уже не сырьё, и
# писать такой замер в точку «сырьё» нельзя — ряд по сырью поедет.
STAGE_KNOWN = {canon for canon, _ in STAGE_CANON}


def _point_of(head: str):
    """«камера 2» → ('камера хранения', '2'). Не узнали — (None, head)."""
    low = re.sub(r"\s+", " ", (head or "").strip().lower())
    if not low:
        return None, head
    for canon, keys in POINT_HEADS:
        for k in sorted(keys, key=len, reverse=True):
            if low.startswith(k):
                rest = (head or "").strip()[len(k):].strip(" .,:;-–—")
                return canon, (rest or None)
    return None, head


def norm_stage(text: str):
    """Свободное название этапа → канонический. Неизвестное возвращаем как есть."""
    if not text:
        return None
    low = re.sub(r"\s+", " ", text.strip().lower())
    for canon, keys in STAGE_CANON:
        if any(k in low for k in keys):
            return canon
    return text.strip()


def _hhmm(token: str):
    m = _TIME_RE.match(token or "")
    return (int(m.group(1)), int(m.group(2))) if m else None


def _clean(text: str) -> str:
    text = re.sub(r"[()\[\]]", " ", text or "")
    text = re.sub(r"\s+", " ", text)
    return text.strip(" .,;:-–").strip()


def _parse_free_line(line: str):
    """«Форель Иран 00620- 12», «Форель Кар. 00618 ( пласт) 4» → (batch, descr, value, explicit_sign)."""
    m = _TAIL_NUM_RE.search(line)
    if not m:
        return None
    value = _to_float((m.group(1) or "") + m.group(2))
    if value is None:
        return None
    head = line[: m.start()]
    b = _BATCH_RE.search(head)
    batch = b.group(1) if b else None
    if b:
        head = head[: b.start()] + " " + head[b.end():]
    return batch, _clean(head) or None, value, bool(m.group(1))


def parse_lines(text: str, window: dict):
    """Разбирает сообщение мастера. Возвращает (readings, errors).

    Понимает три вида записи, потому что человек пишет по-разному:
      1) «00615 / филе на шкуре / 6,0» — формат из подсказки;
      2) карточка из строк: «00620», «Форель Иран», «4»;
      3) свободная строка: «Форель Иран 00620- 12», «Лосось тушка- 6».
    """
    raw_lines = [l.strip() for l in (text or "").replace(";", "\n").splitlines()]
    lines = [l for l in raw_lines if l]
    if not lines:
        return [], []

    point = window.get("point") or "филе-этап"

    default_stage = window.get("stage")

    def _mk(batch, descr, value, explicit_sign, raw, stage=None, hhmm=None):
        cut = cut_note(raw)
        named_point, rest = _point_of(descr)
        # Точку человек называет и в поле этапа: «00614 / лосось / сырьё / -1,5».
        stage_point, _ = _point_of(stage) if stage else (None, None)
        pt = named_point or stage_point or point
        if named_point and batch is None:
            descr = rest
        named_stage = None if stage_point else norm_stage(stage)
        if not named_point and not stage_point:
            if named_stage == "дефрост":
                pt = "дефрост-толща"
            elif named_stage in STAGE_KNOWN:
                pt = "продукт-этап"
        no_stage = pt in POINT_NO_STAGE
        return {
            "point_key": pt,
            "batch_no": batch,
            "descr": descr,
            "stage": named_stage or (None if no_stage else default_stage),
            "stage_source": ("строка" if named_stage
                             else (None if no_stage or not default_stage else "окно")),
            "hhmm": hhmm,
            "cut_note": cut,
            "value_c": value,
            "raw_line": raw,
            "sign_pending": (not explicit_sign) and pt in SIGN_STRICT_POINTS and value != 0,  # у нуля знака нет
        }

    # Карточка: 2–3 строки, последняя — голое число, ни одна не содержит «/».
    if 2 <= len(lines) <= 3 and not any("/" in l for l in lines):
        tail = _to_float(lines[-1])
        if tail is None:
            m_tail = _TAIL_NUM_RE.search(lines[-1])
            tail = _to_float((m_tail.group(1) or "") + m_tail.group(2)) if m_tail else None
        if tail is not None and T_MIN <= tail <= T_MAX:
            batch = None
            descr_parts = []
            for l in lines[:-1]:
                b = _BATCH_RE.fullmatch(l.strip())
                if b and batch is None:
                    batch = b.group(1)
                else:
                    descr_parts.append(l)
            explicit = bool(re.match(r"^[-+−]\s*\d", lines[-1].strip()))
            return [_mk(batch, _clean(" ".join(descr_parts)) or None, tail, explicit, text.strip())], []

    readings, errors = [], []
    for line in lines:
        if "/" in line:
            parts = [p.strip() for p in line.split("/") if p.strip() != ""]
            if len(parts) < 2:
                errors.append(line)
                continue
            hhmm = _hhmm(parts[-1])
            if hhmm:
                parts = parts[:-1]
            # Хвостовое слово-оценка отдельным полем: «… / -1,0 / не идёт»
            if (len(parts) >= 3 and _num_and_note(parts[-1])[0] is None
                    and cut_note(parts[-1])):
                parts = parts[:-1]
            if len(parts) < 2:
                errors.append(line)
                continue
            value, _ = _num_and_note(parts[-1])
            if value is None or not (T_MIN <= value <= T_MAX):
                errors.append(line)
                continue
            explicit = bool(re.match(r"^[-+−]\s*\d", parts[-1].strip()))
            head = parts[0]
            b = _BATCH_RE.search(head)
            batch = b.group(1) if b else (head if head and head[0].isdigit() else None)
            mid = parts[1:-1]
            if batch is None and not b:
                mid = parts[:-1]
            # «партия / продукт / этап / температура» — этап последним из середины
            stage = mid[-1] if len(mid) >= 2 else None
            descr = " / ".join(mid[:-1]) if len(mid) >= 2 else (" / ".join(mid) or None)
            readings.append(_mk(batch, descr or None, value, explicit, line, stage, hhmm))
            continue

        parsed = _parse_free_line(line)
        if not parsed:
            errors.append(line)
            continue
        batch, descr, value, explicit = parsed
        if not (T_MIN <= value <= T_MAX):
            errors.append(line)
            continue
        readings.append(_mk(batch, descr, value, explicit, line))
    return readings, errors


# ─────────────────────────────────────────────────────────────────────────────
# Отправка окна
# ─────────────────────────────────────────────────────────────────────────────

def _question_text(window: dict) -> str:
    if window["kind"] == "checklist":
        items = "\n".join(f"— {i}" for i in window["items"])
        return (
            f"<b>{window['key']} · {window['title']}</b>\n\n{items}\n\n"
            "Ответьте «готово» в ответ на это сообщение."
        )
    strict = (window.get("point") in SIGN_STRICT_POINTS)
    tail = (
        "\n\n<b>Температуру пишите со знаком</b>: <code>-12</code> или <code>+4</code>."
        if strict else ""
    )
    checks = "".join(f"— {c}\n" for c in window.get("checks") or [])
    return (
        f"<b>{window['key']} · {window['title']}</b>\n\n"
        + (f"{checks}\n" if checks else "")
        + f"Формат: <code>{window['fields']}</code>\n"
        f"Например:\n<code>{window['example']}</code>\n\n"
        "Если замер сделан раньше — допишите время последним полем: "
        "<code>… / -1,5 / 09:20</code>.\n"
        f"Ответьте в ответ на это сообщение.{tail}"
    )


async def send_window(app, db, window_key: str):
    window = WINDOW_BY_KEY[window_key]
    now = datetime.now(MSK)
    if now.weekday() not in WORKDAYS:
        return None
    ensure_tables(db)
    cid = chat_id()
    row = db._fetchone(
        "SELECT id FROM quality.control_windows "
        "WHERE window_key=%s AND asked_on=%s AND chat_id=%s",
        (window_key, now.date(), cid),
    )
    if row:
        return row["id"]
    msg = await app.bot.send_message(
        chat_id=cid, text=_question_text(window), parse_mode="HTML"
    )
    rec = db._fetchone(
        "INSERT INTO quality.control_windows (window_key, asked_on, chat_id, message_id) "
        "VALUES (%s,%s,%s,%s) RETURNING id",
        (window_key, now.date(), cid, msg.message_id),
    )
    db.conn.commit()
    logger.info("production_control: окно %s отправлено, msg=%s", window_key, msg.message_id)
    return rec["id"] if rec else None


async def close_open(app, db, window_key: str):
    """Закрывает неотвеченное окно пропуском по истечении срока приёма ответов.

    Повтора «замер ещё не получен» с 18.09.2026 нет: собственник просил ровно три
    сообщения в день, а не три плюс догоняющие.
    """
    ensure_tables(db)
    now = datetime.now(MSK)
    db._execute(
        "UPDATE quality.control_windows SET status='missed' "
        "WHERE window_key=%s AND asked_on=%s AND chat_id=%s AND status='open'",
        (window_key, now.date(), chat_id()),
    )
    db.conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Приём ответа
# ─────────────────────────────────────────────────────────────────────────────

MS_MOVE_URL = "https://api.moysklad.ru/api/remap/1.2/entity/move"


def _state_of(names) -> str:
    """Состояние сырья по наименованию МойСклад: ОХЛ против СМ/ЗАМОРОЖ.

    Различать обязательно: 0 °C у охлаждённого во льду сырья и 0 °C после
    дефроста — разные величины, и смешивать их в одном ряду нельзя
    (замечание собственника 26.08 по партии 00626, лосось Мурманск ОХЛ).
    """
    up = " ".join(names).upper()
    chilled = "ОХЛ" in up
    frozen = bool(
        re.search(r"(?<![А-ЯЁA-Z])(СМ|С/М|С\\М)(?![А-ЯЁA-Z])", up) or "ЗАМОРОЖ" in up
    )
    if chilled and frozen:
        return "смешанное"
    if chilled:
        return "охл"
    if frozen:
        return "мороженое"
    return "не определено"


async def resolve_batch(db, batch_no: str):
    """Наименования из перемещения МойСклад по номеру партии. Кэш в quality.batches."""
    if not batch_no:
        return None, None
    row = db._fetchone(
        "SELECT products, state FROM quality.batches WHERE batch_no=%s", (batch_no,)
    )
    if row:
        return row["products"], row["state"]
    try:
        import moysklad
        params = {"filter": f"name={batch_no}", "expand": "positions.assortment", "limit": "5"}
        async with aiohttp.ClientSession() as sess:
            async with sess.get(MS_MOVE_URL, headers=moysklad.get_headers(),
                                params=params, timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    logger.warning("resolve_batch %s: HTTP %s", batch_no, r.status)
                    return None, None
                data = await r.json()
        rows = data.get("rows") or []
        if not rows:
            db._execute(
                "INSERT INTO quality.batches (batch_no, state) VALUES (%s,'нет перемещения') "
                "ON CONFLICT (batch_no) DO NOTHING", (batch_no,)
            )
            db.conn.commit()
            return None, "нет перемещения"
        m = rows[0]
        names = [
            (p.get("assortment") or {}).get("name", "")
            for p in ((m.get("positions") or {}).get("rows") or [])
        ]
        names = [n for n in names if n]
        products = "; ".join(names)
        state = _state_of(names)
        db._execute(
            "INSERT INTO quality.batches (batch_no, moment, description, products, state) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (batch_no) DO UPDATE "
            "SET products=EXCLUDED.products, state=EXCLUDED.state, updated_at=NOW()",
            (batch_no, m.get("moment"), (m.get("description") or "")[:500], products, state),
        )
        db.conn.commit()
        return products, state
    except Exception as e:
        logger.warning("resolve_batch %s: %s", batch_no, e)
        return None, None


# Сколько минут после отправки окна считаем, что ответы относятся к нему.
WINDOW_TTL_MIN = int(os.getenv("QC_WINDOW_TTL_MIN", "90"))


def _active_window(db, msg):
    """Окно, к которому относится сообщение: по reply — точно, иначе по времени."""
    if msg.reply_to_message:
        row = db._fetchone(
            "SELECT id, window_key FROM quality.control_windows "
            "WHERE chat_id=%s AND message_id=%s",
            (msg.chat_id, msg.reply_to_message.message_id),
        )
        if row:
            return row
    return db._fetchone(
        "SELECT id, window_key FROM quality.control_windows "
        "WHERE chat_id=%s AND asked_at >= NOW() - make_interval(mins => %s) "
        "ORDER BY asked_at DESC LIMIT 1",
        (msg.chat_id, WINDOW_TTL_MIN),
    )


def _make_reply_handler(db):
    async def handler(update: Update, context):
        msg = update.effective_message
        if not msg or msg.chat_id != chat_id():
            return
        text = (msg.text or msg.caption or "").strip()
        if not text:
            # Фото без подписи: машина его не разберёт, подсказываем сразу —
            # 26.08 технолог прислала «Фото с верху!», и цифру пришлось диктовать.
            if msg.photo and _active_window(db, msg):
                await msg.reply_text("Фото я не разберу. Напишите числом: партия / сырьё / температура.")
            return
        ensure_tables(db)

        author = update.effective_user
        author_name = " ".join(
            x for x in [getattr(author, "first_name", None), getattr(author, "last_name", None)] if x
        ) or getattr(author, "username", None) or str(getattr(author, "id", ""))
        author_id = getattr(author, "id", None)

        # Сырой лог. Пишем ДО всякого разбора и независимо от того, распознали мы
        # что-нибудь или нет: пока технолог не привыкла к формату, правда живёт в
        # переписке — в поправках собственника, уточнениях и переспросах. Парсер
        # берёт что может, остальное восстанавливается по этому логу.
        try:
            db._execute(
                """
                INSERT INTO quality.chat_log
                    (chat_id, message_id, author_tg_id, author_name, is_bot, text, reply_to)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (chat_id, message_id) DO NOTHING
                """,
                (msg.chat_id, msg.message_id, author_id, author_name,
                 bool(getattr(author, "is_bot", False)), text,
                 msg.reply_to_message.message_id if msg.reply_to_message else None),
            )
            db.conn.commit()
        except Exception as e:
            logger.warning("production_control: сырой лог не записан: %s", e)

        if text.startswith("/"):
            return

        # Ответ на переспрос знака: «минус» / «плюс» / «-» / «+».
        sign = sign_answer(text)
        if sign is not None:
            rows = db._fetchall(
                "SELECT id, value_c FROM quality.temp_readings "
                "WHERE sign_pending AND author_tg_id=%s AND chat_id=%s "
                "AND measured_at >= NOW() - INTERVAL '2 hours'",
                (author_id, msg.chat_id),
            )
            if not rows:
                return
            for r in rows:
                db._execute(
                    "UPDATE quality.temp_readings SET value_c=%s, sign_pending=FALSE WHERE id=%s",
                    (abs(float(r["value_c"])) * sign, r["id"]),
                )
            db.conn.commit()
            await msg.reply_text(f"Уточнил знак, поправил записей: {len(rows)}.")
            return

        row = _active_window(db, msg)
        if row:
            window = WINDOW_BY_KEY.get(row["window_key"])
            if not window:
                return
        else:
            # Вне окна замер тоже принимаем (решение собственника 21.09.2026: запросы
            # системы не считаем, считаем, сколько раз производство прислало замеры).
            # Мерят и пишут, когда удобно, — с 10 по 17.09 так пропало ~10 сообщений.
            # Разбираем только строки в формате «… / … / температура»: без окна в
            # группу пишут и о другом, и «цех 1 убрать до 15» замером не считаем.
            if "/" not in text:
                return
            row = {"id": None, "window_key": None}
            window = WINDOWS[0]   # перечень во всех окнах один и тот же

        if window["kind"] == "checklist" and row["id"]:
            db._execute(
                "UPDATE quality.control_windows SET answered_at=NOW(), status='done' WHERE id=%s",
                (row["id"],),
            )
            db.conn.commit()
            await msg.reply_text("Принято.")
            return

        if row["id"]:
            db._execute(
                "UPDATE quality.chat_log SET window_id=%s, window_key=%s "
                "WHERE chat_id=%s AND message_id=%s",
                (row["id"], row["window_key"], msg.chat_id, msg.message_id),
            )
            db.conn.commit()

        readings, errors = parse_lines(text, window)
        if not row["id"]:
            # Без окна «Не разобрал» не пишем: реплика со слэшем могла быть не замером.
            errors = []
        if not readings:
            return  # реплика без чисел — молчим, чтобы не шуметь; текст уже в логе

        if (row["id"] and len(readings) == 1 and not readings[0]["batch_no"]
                and not readings[0]["descr"]):
            dup = db._fetchone(
                "SELECT id FROM quality.temp_readings "
                "WHERE window_id=%s AND author_tg_id=%s AND NOT superseded "
                "AND ABS(value_c) = ABS(%s::numeric) "
                "AND measured_at >= NOW() - INTERVAL '15 minutes' "
                "ORDER BY id DESC LIMIT 1",
                (row["id"], author_id, readings[0]["value_c"]),
            )
            if dup:
                db._execute(
                    "UPDATE quality.temp_readings "
                    "SET value_c=%s, sign_pending=FALSE WHERE id=%s",
                    (readings[0]["value_c"], dup["id"]),
                )
                db.conn.commit()
                await msg.reply_text("Принял как уточнение знака к предыдущему замеру.")
                return

        pending = 0
        for r in readings:
            r["ms_product"], r["ms_state"] = await resolve_batch(db, r["batch_no"])
            # Повторный замер по той же партии в том же окне — исправление.
            if r["batch_no"] and row["id"]:
                db._execute(
                    "UPDATE quality.temp_readings SET superseded=TRUE "
                    "WHERE window_id=%s AND batch_no=%s AND NOT superseded",
                    (row["id"], r["batch_no"]),
                )
            db._execute(
                """
                INSERT INTO quality.temp_readings
                    (window_id, window_key, point_key, batch_no, descr, value_c,
                     author_tg_id, author_name, chat_id, answer_msg_id, raw_line,
                     sign_pending, ms_product, ms_state, stage, stage_source,
                     cut_note, reported_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        CASE WHEN %s::int IS NULL THEN NULL
                             ELSE (((NOW() AT TIME ZONE 'Europe/Moscow')::date
                                    + make_time(%s::int, %s::int, 0))
                                   AT TIME ZONE 'Europe/Moscow') END)
                """,
                (row["id"], row["window_key"], r["point_key"], r["batch_no"], r["descr"],
                 r["value_c"], author_id, author_name, msg.chat_id, msg.message_id,
                 r["raw_line"], r["sign_pending"], r.get("ms_product"), r.get("ms_state"),
                 r.get("stage"), r.get("stage_source"), r.get("cut_note"),
                 (r["hhmm"][0] if r.get("hhmm") else None),
                 (r["hhmm"][0] if r.get("hhmm") else None),
                 (r["hhmm"][1] if r.get("hhmm") else None)),
            )
            if r["sign_pending"]:
                pending += 1
        if row["id"]:
            db._execute(
                "UPDATE quality.control_windows SET answered_at=NOW(), status='done' WHERE id=%s",
                (row["id"],),
            )
        db._execute(
            "UPDATE quality.chat_log SET parsed_count=%s WHERE chat_id=%s AND message_id=%s",
            (len(readings), msg.chat_id, msg.message_id),
        )
        db.conn.commit()

        named = [r for r in readings if r.get("ms_product")]
        if named:
            parts = ["Записал: " + "; ".join(
                f"{r['batch_no']} {r['ms_product'][:34]}"
                + (f", {r['stage']}" if r.get("stage") else "")
                + f" — {r['value_c']:g} °C"
                + (f" в {r['hhmm'][0]:02d}:{r['hhmm'][1]:02d}" if r.get("hhmm") else "")
                for r in named[:3]
            ) + "."]
            unknown = [r for r in readings if not r.get("ms_product") and r.get("batch_no")]
            if unknown:
                parts.append(
                    "Не нашёл перемещения: " + ", ".join(sorted({r["batch_no"] for r in unknown}))
                    + " — проверьте номер партии."
                )
        else:
            parts = [f"Записал: {len(readings)}."]
            unknown = [r for r in readings if r.get("batch_no")]
            if unknown:
                parts.append(
                    "Перемещения " + ", ".join(sorted({r["batch_no"] for r in unknown}))
                    + " в МойСкладе нет — проверьте номер."
                )
        if pending:
            vals = ", ".join(
                f"{r['descr'] or r['batch_no'] or 'замер'} {r['value_c']:g}"
                for r in readings if r["sign_pending"]
            )
            parts.append(f"Уточните знак — минус или плюс? ({vals})")
        if errors:
            parts.append("Не разобрал: " + "; ".join(errors[:3]))
        await msg.reply_text(" ".join(parts))

    return handler


def register(app, db):
    """Хендлер ответов на замерные окна. Вызывать ДО catch-all MessageHandler.

    Команды сводки нет: 28.08.2026 собственник решил, что замеры смотрим в
    рабочих сессиях по `quality.temp_readings`, а через бот — не планируем.
    """
    app.add_handler(
        MessageHandler(
            # ~COMMAND обязателен: хендлер зарегистрирован раньше остальных и без
            # фильтра проглотит любые команды, набранные в этой группе.
            filters.Chat(chat_id()) & (filters.TEXT | filters.CAPTION) & ~filters.COMMAND,
            _make_reply_handler(db),
        )
    )
    # Таблицы — ПОСЛЕ хендлера и без права уронить регистрацию: с 09.09 по 17.09.2026
    # замеры из группы в базу не шли, а сообщения забирал общий handle_message —
    # по всей видимости, DDL на старте падал (таймауты базы видны в логах тех дней),
    # и хендлер не подключался. Та же ловушка, что у чек-листа водителя 22.07.
    # Таблицы давно существуют; обработчик сам зовёт ensure_tables на каждом сообщении.
    try:
        ensure_tables(db)
    except Exception as e:
        logger.exception("production_control: ensure_tables на старте отложено: %s", e)
    logger.info("production_control: зарегистрирован, чат %s", chat_id())


def schedule(app, db):
    """Джобы по окнам регламента. Времена в UTC, МСК = UTC+3."""
    from datetime import time as _time

    for w in WINDOWS:
        h, m = w["utc"]
        key = w["key"]

        def _mk(window_key):
            async def _job(context):
                try:
                    await send_window(app, db, window_key)
                except Exception as e:
                    logger.error("production_control %s: %s", window_key, e, exc_info=True)

            async def _close(context):
                try:
                    await close_open(app, db, window_key)
                except Exception as e:
                    logger.error("production_control close %s: %s", window_key, e, exc_info=True)

            return _job, _close

        job, close = _mk(key)
        app.job_queue.run_daily(job, time=_time(hour=h, minute=m, tzinfo=timezone.utc))
        ch, cm = divmod(h * 60 + m + WINDOW_TTL_MIN, 60)
        app.job_queue.run_daily(
            close, time=_time(hour=ch % 24, minute=cm, tzinfo=timezone.utc)
        )
    logger.info("production_control: расписание поставлено, окон %d", len(WINDOWS))
