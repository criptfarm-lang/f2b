"""Контроль производства: температурные замеры по расписанию в Telegram.

План: plans/2026-08-25-сбор-ручных-замеров-температуры-в-тг.md (репо «второй мозг»).

Зачем: до установки стационарных датчиков (план 2026-08-21) единственный источник
температуры продукта — руки мастера. Раньше цифры жили в переписке и терялись.
Джобы шлют в группу вопрос по окну, мастер отвечает reply, бот парсит строки и
кладёт в quality.temp_readings — дальше ряд разбирается запросом, а не глазами.

Регламент задан собственником 26.08.2026. Окна: 09:00, 11:00, 12:50 (регламент
подготовки к обеду, без замера), 14:30, 16:00, 17:30 МСК.
"""
import logging
import os
import re
from datetime import datetime, timedelta, timezone

from telegram import Update
from telegram.ext import MessageHandler, CommandHandler, filters

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))

# Группа по умолчанию — «F2B САНКОНТРОЛЬ» (собственник 26.08.2026 переносит сюда
# контроль производства и позже переименует группу в «Контроль производства»).
DEFAULT_CHAT_ID = -5432509607

# Физически осмысленный диапазон для продукта и тузлука в цеху.
T_MIN, T_MAX = -40.0, 40.0

# Через сколько минут напомнить, если на окно не ответили. Один раз.
REMIND_AFTER_MIN = int(os.getenv("QC_REMIND_AFTER_MIN", "30"))

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

WINDOWS = [
    {
        "key": "09:00",
        "utc": (6, 0),
        "kind": "measure",
        "point": "тушка-перед-порезкой",
        "title": "Температура в тушке перед началом порезки",
        "fields": "номер партии / вид сырья / температура",
        "example": "00615 / форель ПСГ Карелия 3,5+ / -1,5",
    },
    {
        "key": "11:00",
        "utc": (8, 0),
        "kind": "measure",
        "point": "филе-финиш-зачистка",
        "title": "Температура филе на столе финишной зачистки",
        "fields": "номер партии / вид разделки / температура",
        "example": "00615 / филе на шкуре / 6,0",
    },
    {
        "key": "12:50",
        "utc": (9, 50),
        "kind": "checklist",
        "title": "Подготовка к обеду",
        "items": [
            "всё филе убрать в охлаждаемую камеру вместе с паспортами",
            "ножи и доски замочить",
            "полы и поверхности протереть",
        ],
    },
    {
        "key": "14:30",
        "utc": (11, 30),
        "kind": "measure",
        "point": "филе-этап",
        "with_brine": True,
        "title": "Температура филе на столе финишной зачистки и на прочих этапах, где идут работы, плюс тузлук",
        "fields": "номер партии / этап / температура — по строке на каждый этап",
        "example": "00615 / финишная зачистка / 8,0\n00615 / порционирование / 11,5\nтузлук / 6,0",
    },
    {
        "key": "16:00",
        "utc": (13, 0),
        "kind": "measure",
        "point": "филе-этап",
        "with_brine": True,
        "title": "Температура филе на этапах, где идут работы, плюс тузлук",
        "fields": "номер партии / этап / температура — по строке на каждый этап",
        "example": "00615 / финишная зачистка / 9,0\nтузлук / 6,5",
    },
    {
        "key": "17:30",
        "utc": (14, 30),
        "kind": "measure",
        "point": "филе-этап",
        "title": "Температура филе на этапах, где идут работы",
        "fields": "номер партии / этап / температура — по строке на каждый этап",
        "example": "00615 / упаковка / 10,0",
    },
    {
        "key": "18:00",
        "utc": (15, 0),
        "kind": "measure",
        "point": "дефрост-толща",
        "title": "Температура в толще рыбы, лежащей на дефросте",
        "fields": "номер партии / вид сырья / температура — по строке на каждую рыбу",
        "example": "00615 / форель Карелия 3,5+ / -2,5\n00616 / лосось / -2,0",
    },
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
            superseded     BOOLEAN     NOT NULL DEFAULT FALSE
        )
        """
    )
    for col, ddl in (
        ("sign_pending", "BOOLEAN NOT NULL DEFAULT FALSE"),
        ("superseded", "BOOLEAN NOT NULL DEFAULT FALSE"),
    ):
        db._execute(
            f"ALTER TABLE quality.temp_readings ADD COLUMN IF NOT EXISTS {col} {ddl}"
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
_SIGN_RE = re.compile(r"^\s*(минус|плюс|[-+−])\s*$", re.IGNORECASE)

# Окна, где продукт может быть ещё мороженым: число без знака двусмысленно
# (Инна 26.08 написала «12», имея в виду −12). Переспрашиваем.
SIGN_STRICT_POINTS = {"тушка-перед-порезкой", "дефрост-толща"}


def sign_answer(text: str):
    """«минус» / «плюс» / «-» / «+» → -1 / +1, иначе None."""
    m = _SIGN_RE.match(text or "")
    if not m:
        return None
    return -1 if m.group(1).lower() in ("минус", "-", "−") else 1


def _to_float(token: str):
    token = (token or "").strip().replace(",", ".").replace("−", "-")
    token = re.sub(_UNIT + r"\s*[.!]?\s*$", "", token, flags=re.IGNORECASE).strip()
    token = token.rstrip("°cCсС ").strip()
    if not _NUM_RE.match(token):
        return None
    try:
        return float(token)
    except ValueError:
        return None


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

    def _mk(batch, descr, value, explicit_sign, raw):
        head = (descr or "").lower()
        pt = "тузлук" if head.startswith("тузлук") or head.startswith("рассол") else point
        if batch is None and (head.startswith("тузлук") or head.startswith("рассол")):
            descr = None
        return {
            "point_key": pt,
            "batch_no": batch,
            "descr": descr,
            "value_c": value,
            "raw_line": raw,
            "sign_pending": (not explicit_sign) and pt in SIGN_STRICT_POINTS,
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
            explicit = bool(re.match(r"^[-+−]\d", lines[-1].strip()))
            return [_mk(batch, _clean(" ".join(descr_parts)) or None, tail, explicit, text.strip())], []

    readings, errors = [], []
    for line in lines:
        if "/" in line:
            parts = [p.strip() for p in line.split("/")]
            if len(parts) < 2:
                errors.append(line)
                continue
            value = _to_float(parts[-1])
            if value is None or not (T_MIN <= value <= T_MAX):
                errors.append(line)
                continue
            explicit = bool(re.match(r"^[-+−]\d", parts[-1].strip()))
            head = parts[0]
            b = _BATCH_RE.search(head)
            batch = b.group(1) if b else (head if head and head[0].isdigit() else None)
            descr = " / ".join(parts[1:-1]) or None
            if not b and batch is None:
                descr = " / ".join(parts[:-1]) or None
            readings.append(_mk(batch, descr, value, explicit, line))
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
    return (
        f"<b>{window['key']} · {window['title']}</b>\n\n"
        f"Формат: <code>{window['fields']}</code>\n"
        f"Например:\n<code>{window['example']}</code>\n\n"
        "Можно писать и просто текстом, например "
        f"<code>Форель Иран 00620 -12</code> — я разберу.{tail}"
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


async def remind_open(app, db, window_key: str):
    """Один повтор по неотвеченному окну. Закрывает окно отдельная джоба close_open."""
    ensure_tables(db)
    now = datetime.now(MSK)
    row = db._fetchone(
        "SELECT id, message_id, reminded FROM quality.control_windows "
        "WHERE window_key=%s AND asked_on=%s AND chat_id=%s AND status='open'",
        (window_key, now.date(), chat_id()),
    )
    if not row or row["reminded"]:
        return
    try:
        await app.bot.send_message(
            chat_id=chat_id(),
            text=f"Напоминание: замер {window_key} ещё не получен.",
            reply_to_message_id=row["message_id"],
        )
    except Exception as e:
        logger.warning("production_control: напоминание %s не ушло: %s", window_key, e)
    db._execute("UPDATE quality.control_windows SET reminded=TRUE WHERE id=%s", (row["id"],))
    db.conn.commit()


async def close_open(app, db, window_key: str):
    """Закрывает неотвеченное окно пропуском.

    Отдельная джоба, а не второй заход remind_open: повтор ставится один раз в
    сутки, поэтому внутри него окно никогда бы не перешло в missed и висело бы
    open вечно (замечено на первом же окне 26.08.2026).
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
        if not row:
            return  # вне окна — в сыром логе уже сохранено, разбирать нечего
        window = WINDOW_BY_KEY.get(row["window_key"])
        if not window:
            return

        if window["kind"] == "checklist":
            db._execute(
                "UPDATE quality.control_windows SET answered_at=NOW(), status='done' WHERE id=%s",
                (row["id"],),
            )
            db.conn.commit()
            await msg.reply_text("Принято.")
            return

        db._execute(
            "UPDATE quality.chat_log SET window_id=%s, window_key=%s "
            "WHERE chat_id=%s AND message_id=%s",
            (row["id"], row["window_key"], msg.chat_id, msg.message_id),
        )
        db.conn.commit()

        readings, errors = parse_lines(text, window)
        if not readings:
            return  # реплика без чисел — молчим, чтобы не шуметь; текст уже в логе

        pending = 0
        for r in readings:
            # Повторный замер по той же партии в том же окне — исправление.
            if r["batch_no"]:
                db._execute(
                    "UPDATE quality.temp_readings SET superseded=TRUE "
                    "WHERE window_id=%s AND batch_no=%s AND NOT superseded",
                    (row["id"], r["batch_no"]),
                )
            db._execute(
                """
                INSERT INTO quality.temp_readings
                    (window_id, window_key, point_key, batch_no, descr, value_c,
                     author_tg_id, author_name, chat_id, answer_msg_id, raw_line, sign_pending)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (row["id"], row["window_key"], r["point_key"], r["batch_no"], r["descr"],
                 r["value_c"], author_id, author_name, msg.chat_id, msg.message_id,
                 r["raw_line"], r["sign_pending"]),
            )
            if r["sign_pending"]:
                pending += 1
        db._execute(
            "UPDATE quality.control_windows SET answered_at=NOW(), status='done' WHERE id=%s",
            (row["id"],),
        )
        db._execute(
            "UPDATE quality.chat_log SET parsed_count=%s WHERE chat_id=%s AND message_id=%s",
            (len(readings), msg.chat_id, msg.message_id),
        )
        db.conn.commit()

        parts = [f"Записал: {len(readings)}."]
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


def _make_status_cmd(db):
    async def cmd(update: Update, context):
        ensure_tables(db)
        rows = db._fetchall(
            """
            SELECT window_key, point_key, batch_no, descr, value_c,
                   measured_at AT TIME ZONE 'Europe/Moscow' AS t, author_name
            FROM quality.temp_readings
            WHERE measured_at >= NOW() - INTERVAL '2 days' AND NOT superseded
            ORDER BY measured_at DESC LIMIT 40
            """
        )
        if not rows:
            await update.effective_message.reply_text("Замеров за двое суток нет.")
            return
        lines = [
            f"{r['t']:%d.%m %H:%M} · {r['point_key']}"
            f"{' · ' + r['batch_no'] if r['batch_no'] else ''}"
            f"{' · ' + r['descr'] if r['descr'] else ''} — {r['value_c']} °C"
            for r in rows
        ]
        await update.effective_message.reply_text(
            "Замеры за двое суток:\n" + "\n".join(lines)
        )
    return cmd


def register(app, db):
    """Хендлер ответов и команда сводки. Вызывать ДО catch-all MessageHandler."""
    ensure_tables(db)
    app.add_handler(
        MessageHandler(
            # ~COMMAND обязателен: хендлер зарегистрирован раньше CommandHandler,
            # и без фильтра он проглотит /qc и остальные команды в этой группе.
            filters.Chat(chat_id()) & (filters.TEXT | filters.CAPTION) & ~filters.COMMAND,
            _make_reply_handler(db),
        )
    )
    app.add_handler(CommandHandler("qc", _make_status_cmd(db)))
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

            async def _remind(context):
                try:
                    await remind_open(app, db, window_key)
                except Exception as e:
                    logger.error("production_control remind %s: %s", window_key, e, exc_info=True)

            async def _close(context):
                try:
                    await close_open(app, db, window_key)
                except Exception as e:
                    logger.error("production_control close %s: %s", window_key, e, exc_info=True)

            return _job, _remind, _close

        job, remind, close = _mk(key)
        app.job_queue.run_daily(job, time=_time(hour=h, minute=m, tzinfo=timezone.utc))
        rh, rm = divmod(h * 60 + m + REMIND_AFTER_MIN, 60)
        app.job_queue.run_daily(
            remind, time=_time(hour=rh % 24, minute=rm, tzinfo=timezone.utc)
        )
        ch, cm = divmod(h * 60 + m + REMIND_AFTER_MIN * 2, 60)
        app.job_queue.run_daily(
            close, time=_time(hour=ch % 24, minute=cm, tzinfo=timezone.utc)
        )
    logger.info("production_control: расписание поставлено, окон %d", len(WINDOWS))
