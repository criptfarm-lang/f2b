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
            is_preliminary BOOLEAN     NOT NULL DEFAULT TRUE
        )
        """
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

_NUM_RE = re.compile(r"^-?\d{1,3}(?:[.,]\d{1,2})?$")


def _to_float(token: str):
    token = token.strip().replace(",", ".").replace("−", "-").rstrip("°cCсС ").strip()
    if not _NUM_RE.match(token):
        return None
    try:
        return float(token)
    except ValueError:
        return None


def parse_lines(text: str, window: dict):
    """Разбирает ответ мастера. Возвращает (readings, errors).

    Формат строки: «партия / описание / температура» либо «тузлук / температура».
    Терпим к лишним пробелам и к разделителю десятых.
    """
    readings, errors = [], []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split("/")]
        if len(parts) < 2:
            errors.append(line)
            continue
        value = _to_float(parts[-1])
        if value is None or not (T_MIN <= value <= T_MAX):
            errors.append(line)
            continue
        head = parts[0].lower()
        if head.startswith("тузлук") or head.startswith("рассол"):
            readings.append({
                "point_key": "тузлук",
                "batch_no": None,
                "descr": " / ".join(parts[1:-1]) or None,
                "value_c": value,
                "raw_line": line,
            })
            continue
        readings.append({
            "point_key": window.get("point") or "филе-этап",
            "batch_no": parts[0] or None,
            "descr": " / ".join(parts[1:-1]) or None,
            "value_c": value,
            "raw_line": line,
        })
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
    return (
        f"<b>{window['key']} · {window['title']}</b>\n\n"
        f"Формат: <code>{window['fields']}</code>\n"
        f"Например:\n<code>{window['example']}</code>\n\n"
        "Ответьте в ответ на это сообщение."
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
    """Один повтор по неотвеченному окну, дальше окно закрывается пропуском."""
    ensure_tables(db)
    now = datetime.now(MSK)
    row = db._fetchone(
        "SELECT id, message_id, reminded FROM quality.control_windows "
        "WHERE window_key=%s AND asked_on=%s AND chat_id=%s AND status='open'",
        (window_key, now.date(), chat_id()),
    )
    if not row:
        return
    if row["reminded"]:
        db._execute(
            "UPDATE quality.control_windows SET status='missed' WHERE id=%s", (row["id"],)
        )
        db.conn.commit()
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


# ─────────────────────────────────────────────────────────────────────────────
# Приём ответа
# ─────────────────────────────────────────────────────────────────────────────

def _make_reply_handler(db):
    async def handler(update: Update, context):
        msg = update.effective_message
        if not msg or not msg.reply_to_message:
            return
        if msg.chat_id != chat_id():
            return
        parent_id = msg.reply_to_message.message_id
        row = db._fetchone(
            "SELECT id, window_key FROM quality.control_windows "
            "WHERE chat_id=%s AND message_id=%s",
            (msg.chat_id, parent_id),
        )
        if not row:
            return  # reply не на наше сообщение — не наше дело
        window = WINDOW_BY_KEY.get(row["window_key"])
        if not window:
            return
        author = update.effective_user
        author_name = " ".join(
            x for x in [getattr(author, "first_name", None), getattr(author, "last_name", None)] if x
        ) or getattr(author, "username", None) or str(getattr(author, "id", ""))

        if window["kind"] == "checklist":
            db._execute(
                "UPDATE quality.control_windows SET answered_at=NOW(), status='done' WHERE id=%s",
                (row["id"],),
            )
            db.conn.commit()
            await msg.reply_text("Принято.")
            return

        readings, errors = parse_lines(msg.text or msg.caption or "", window)
        for r in readings:
            db._execute(
                """
                INSERT INTO quality.temp_readings
                    (window_id, window_key, point_key, batch_no, descr, value_c,
                     author_tg_id, author_name, chat_id, answer_msg_id, raw_line)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (row["id"], row["window_key"], r["point_key"], r["batch_no"], r["descr"],
                 r["value_c"], getattr(author, "id", None), author_name,
                 msg.chat_id, msg.message_id, r["raw_line"]),
            )
        if readings:
            db._execute(
                "UPDATE quality.control_windows SET answered_at=NOW(), status='done' WHERE id=%s",
                (row["id"],),
            )
        db.conn.commit()

        if readings and not errors:
            await msg.reply_text(f"Записал: {len(readings)}.")
        elif readings and errors:
            bad = "\n".join(f"• {e}" for e in errors[:5])
            await msg.reply_text(
                f"Записал: {len(readings)}. Не разобрал строки:\n{bad}\n\n"
                f"Формат: {window['fields']}"
            )
        else:
            await msg.reply_text(
                "Не разобрал ни одной строки.\n"
                f"Формат: {window['fields']}\nНапример: {window['example'].splitlines()[0]}"
            )

    return handler


def _make_status_cmd(db):
    async def cmd(update: Update, context):
        ensure_tables(db)
        rows = db._fetchall(
            """
            SELECT window_key, point_key, batch_no, descr, value_c,
                   measured_at AT TIME ZONE 'Europe/Moscow' AS t, author_name
            FROM quality.temp_readings
            WHERE measured_at >= NOW() - INTERVAL '2 days'
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
        MessageHandler(filters.REPLY & filters.Chat(chat_id()), _make_reply_handler(db))
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

            return _job, _remind

        job, remind = _mk(key)
        app.job_queue.run_daily(job, time=_time(hour=h, minute=m, tzinfo=timezone.utc))
        rh, rm = divmod(h * 60 + m + REMIND_AFTER_MIN, 60)
        app.job_queue.run_daily(
            remind, time=_time(hour=rh % 24, minute=rm, tzinfo=timezone.utc)
        )
    logger.info("production_control: расписание поставлено, окон %d", len(WINDOWS))
