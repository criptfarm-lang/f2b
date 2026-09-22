"""Сторис-студия: приём материала, кнопки под превью, правки собственника.

Материал для сторис – посты канала «Контент F2B» (CONTENT_CHAT_ID, решение собственника
22.09.2026: «буду слать не в бота, а в группу»): фото, видео, альбом, с подписью или без,
в том числе пересланное. Бот ставит на пост отметку 👀 и кладёт задачу в очередь
`publishing.story_jobs`. Превью с кнопками присылает собственнику в личку сборщик
`f2b-stories` тем же токеном бота; нажатия и правки обрабатываются здесь.

- Работает, только если в `bot_settings` ключ `stories_studio_enabled` = '1'.
- Обработчик канала стоит в своей группе (STUDIO_GROUP): старый handle_channel_post
  (сохранение материала в media) отрабатывает как раньше.
- Альбом Telegram присылает отдельными апдейтами с общим media_group_id – копим в одну
  задачу со статусом `collecting`, через ALBUM_WAIT_SEC без новых частей – `queued`.
  Если бот перезапустился в этом окне, сборщик берёт `collecting` старше минуты сам.
- Бот скачивает файлы только до 20 МБ (ограничение Bot API) – больше не берём,
  собственнику в личку – как прислать.
- «В расписание» не публикует сразу: сборщик ставит сторис в ближайший свободный день.

План: plans/2026-09-22-сторис-студия-в-боте.md (репо «второй мозг»).
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time

import psycopg2.extras
from telegram import ForceReply
from telegram.ext import CallbackQueryHandler, MessageHandler, filters

logger = logging.getLogger(__name__)

SETTING_KEY = "stories_studio_enabled"
MAX_DOWNLOAD = 20 * 1024 * 1024      # getFile в Bot API отдаёт файлы до 20 МБ
MAX_STORY_SEC = 60                   # сторис в Telegram и Instagram – до 60 с
ALBUM_WAIT_SEC = 4                   # части альбома приходят подряд за 1–2 с
STUDIO_GROUP = 1                     # своя группа обработчиков: handle_channel_post в группе 0 тоже отрабатывает
ACK_REACTION = "👀"
_ENABLED_TTL = 30                    # кеш выключателя, чтобы не ходить в БД на каждое фото

_enabled_cache = {"at": 0.0, "value": False}
_album_tasks: set = set()   # ссылки на фоновые задачи альбомов, иначе их может собрать сборщик мусора

SCHEMA = """
CREATE SCHEMA IF NOT EXISTS publishing;
CREATE TABLE IF NOT EXISTS publishing.story_jobs (
    id              bigserial PRIMARY KEY,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    chat_id         bigint NOT NULL,
    author_id       bigint NOT NULL,
    media_group_id  text,
    media           jsonb NOT NULL DEFAULT '[]'::jsonb,
    caption         text,
    status          text NOT NULL DEFAULT 'queued',
    error           text
);
CREATE UNIQUE INDEX IF NOT EXISTS story_jobs_album_uq
    ON publishing.story_jobs (chat_id, media_group_id) WHERE media_group_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS story_jobs_status_idx
    ON publishing.story_jobs (status, created_at);
-- колонки и статусы цикла сборки; то же самое – MIGRATION в f2b-stories/stories/db.py, менять оба
ALTER TABLE publishing.story_jobs
    ADD COLUMN IF NOT EXISTS revision int NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS note text,
    ADD COLUMN IF NOT EXISTS plan jsonb,
    ADD COLUMN IF NOT EXISTS kind text,
    ADD COLUMN IF NOT EXISTS preview_message_id bigint,
    ADD COLUMN IF NOT EXISTS tg_story_id bigint,
    ADD COLUMN IF NOT EXISTS ig_media_id text,
    ADD COLUMN IF NOT EXISTS published_at timestamptz,
    ADD COLUMN IF NOT EXISTS source_chat_id bigint,
    ADD COLUMN IF NOT EXISTS scheduled_at timestamptz;
ALTER TABLE publishing.story_jobs DROP CONSTRAINT IF EXISTS story_jobs_status_chk;
ALTER TABLE publishing.story_jobs ADD CONSTRAINT story_jobs_status_chk CHECK (status IN (
    'collecting', 'queued', 'building', 'ready', 'awaiting_note', 'approved', 'scheduled',
    'publish_requested', 'publishing', 'published', 'cancelled', 'failed'));
"""


def _owner_id() -> int:
    return int(os.environ["OWNER_CHAT_ID"])


def ensure_schema(db) -> None:
    db._execute(SCHEMA)


def _enabled(db) -> bool:
    now = time.monotonic()
    if now - _enabled_cache["at"] > _ENABLED_TTL:
        try:
            row = db._fetchone("SELECT value FROM bot_settings WHERE key = %s", (SETTING_KEY,))
            _enabled_cache["value"] = bool(row and (row["value"] or "").strip() == "1")
        except Exception:
            logger.exception("stories_studio: не прочитал выключатель, считаю выключенным")
            _enabled_cache["value"] = False
        _enabled_cache["at"] = now
    return _enabled_cache["value"]


def _content_chat_id() -> int:
    return int(os.getenv("CONTENT_CHAT_ID", "-1001433042091"))   # тот же ключ, что у handle_channel_post


class _StudioFilter(filters.MessageFilter):
    """Фото или видео в канале «Контент F2B» при включённой студии."""

    def __init__(self, db):
        super().__init__(name="StoriesStudio")
        self.db = db

    def filter(self, message) -> bool:
        if message.chat_id != _content_chat_id():
            return False
        doc = message.document
        is_media = bool(message.photo or message.video or (
            doc and (doc.mime_type or "").startswith(("image/", "video/"))))
        return is_media and _enabled(self.db)


def _media_item(message) -> dict:
    """Что лежит в сообщении – для очереди. Размер и длительность – из метаданных Telegram."""
    if message.photo:
        p = message.photo[-1]  # самое большое разрешение
        return {"kind": "photo", "file_id": p.file_id, "file_unique_id": p.file_unique_id,
                "file_size": p.file_size, "width": p.width, "height": p.height,
                "message_id": message.message_id, "as_document": False}
    if message.video:
        v = message.video
        return {"kind": "video", "file_id": v.file_id, "file_unique_id": v.file_unique_id,
                "file_size": v.file_size, "width": v.width, "height": v.height,
                "duration": v.duration, "mime_type": v.mime_type,
                "message_id": message.message_id, "as_document": False}
    d = message.document
    kind = "video" if (d.mime_type or "").startswith("video/") else "photo"
    return {"kind": kind, "file_id": d.file_id, "file_unique_id": d.file_unique_id,
            "file_size": d.file_size, "mime_type": d.mime_type, "file_name": d.file_name,
            "message_id": message.message_id, "as_document": True}


def _describe(media: list[dict]) -> str:
    photos = sum(1 for m in media if m["kind"] == "photo")
    videos = len(media) - photos
    parts = []
    if videos:
        parts.append(f"{videos} видео")
    if photos:
        parts.append(f"{photos} фото")
    return " и ".join(parts)


def _mb(size) -> str:
    # десятичные мегабайты – как показывают Finder и Telegram (57 468 099 байт = «57 МБ»)
    return f"{size / 1_000_000:.0f} МБ"


def _make_handler(db):
    async def finalize_album(chat_id: int, group_id: str):
        try:
            await asyncio.sleep(ALBUM_WAIT_SEC)
            db._fetchone(
                "UPDATE publishing.story_jobs SET status = 'queued', updated_at = now() "
                "WHERE chat_id = %s AND media_group_id = %s AND status = 'collecting' "
                "AND updated_at <= now() - make_interval(secs => %s) RETURNING id",
                (chat_id, group_id, ALBUM_WAIT_SEC - 0.5))
        except Exception:
            logger.exception("stories_studio: альбом %s не финализирован", group_id)

    async def handle(update, context):
        message = update.effective_message
        owner = _owner_id()
        item = _media_item(message)
        caption = (message.caption or "").strip() or None
        author = message.sender_chat.id if message.sender_chat else message.chat_id

        if item.get("file_size") and item["file_size"] > MAX_DOWNLOAD:
            what = "Видео" if item["kind"] == "video" else "Фото"
            await context.bot.send_message(
                owner, f"{what} {_mb(item['file_size'])} из «Контент F2B» не возьму в сторис – бот может скачать "
                       "только до 20 МБ. Пришли его в канал обычным видео или фото, не файлом: Telegram сожмёт.")
            return

        try:
            if message.media_group_id:
                db._execute(
                    "INSERT INTO publishing.story_jobs "
                    "(chat_id, author_id, source_chat_id, media_group_id, media, caption, status) "
                    "VALUES (%s, %s, %s, %s, %s, %s, 'collecting') "
                    "ON CONFLICT (chat_id, media_group_id) WHERE media_group_id IS NOT NULL "
                    "DO UPDATE SET media = publishing.story_jobs.media || EXCLUDED.media, "
                    "caption = COALESCE(publishing.story_jobs.caption, EXCLUDED.caption), "
                    "updated_at = now()",
                    (owner, author, message.chat_id, message.media_group_id,
                     psycopg2.extras.Json([item]), caption),
                )
                task = asyncio.create_task(finalize_album(owner, message.media_group_id))
                _album_tasks.add(task)
                task.add_done_callback(_album_tasks.discard)
            else:
                db._execute(
                    "INSERT INTO publishing.story_jobs (chat_id, author_id, source_chat_id, media, caption, status) "
                    "VALUES (%s, %s, %s, %s, %s, 'queued')",
                    (owner, author, message.chat_id, psycopg2.extras.Json([item]), caption),
                )
        except Exception as exc:
            logger.exception("stories_studio: не поставил в очередь")
            await context.bot.send_message(owner, f"Не смог взять пост из «Контент F2B» в сторис: {exc}")
            return
        try:
            await message.set_reaction(ACK_REACTION)   # «принял» – без сообщений в канал
        except Exception:
            logger.info("stories_studio: реакцию на пост не поставил (в канале её могут не разрешать)")

    return handle


# ── Кнопки под превью и правки ────────────────────────────────────────────────
# Превью с кнопками присылает сборщик f2b-stories тем же токеном бота; нажатия приходят сюда.
# «Эф» только меняет статус задачи (атомарно, по номеру версии), сборку и публикацию делает сборщик.
_CB = re.compile(r"^st:(pub|now|note|ideas|cancel):(\d+):(\d+)$")
OPEN = ("ready", "awaiting_note")          # версия на руках у собственника, кнопки живые
NOTE_WINDOW_MIN = 30                       # «Другой текст» ждёт правку не дольше
IDEAS_NOTE = "Предложи совсем другую подачу: другой угол, другие слова, другое настроение."
_NUMBER = re.compile(r"^[\d\s.,₽рруб]+$")


def _make_callback(db):
    async def handle(update, context):
        q = update.callback_query
        if not q.from_user or q.from_user.id != _owner_id():
            await q.answer("⛔ Только для руководителя.", show_alert=True)
            return
        m = _CB.match(q.data or "")
        if not m or not q.message:
            await q.answer("Не разобрал кнопку")
            return
        action, job_id, rev = m.group(1), int(m.group(2)), int(m.group(3))
        base = q.message.caption or ""
        ents = q.message.caption_entities

        async def mark(text, keep_keys=False):
            try:
                await q.edit_message_caption(caption=(base + "\n\n" + text)[:1024], caption_entities=ents,
                                             reply_markup=q.message.reply_markup if keep_keys else None)
            except Exception:
                logger.exception("stories_studio: не поправил подпись превью №%s", job_id)

        if action == "note":
            row = db._fetchone(
                "UPDATE publishing.story_jobs SET status = 'awaiting_note', updated_at = now() "
                "WHERE id = %s AND revision = %s AND status IN ('ready', 'awaiting_note') RETURNING id",
                (job_id, rev))
            if not row:
                await _stale(db, q, job_id)
                return
            await q.answer()
            await q.message.reply_text(
                f"Напиши, что поменять в сторис №{job_id}. Можно прислать готовый текст подписи – "
                "поставлю его дословно.", reply_markup=ForceReply(input_field_placeholder="Что поменять?"))
            return

        sql = {
            # «В расписание»: сборщик сам поставит в ближайший свободный день и пришлёт дату
            "pub": ("UPDATE publishing.story_jobs SET status = 'approved', updated_at = now() "
                    "WHERE id = %s AND revision = %s AND status IN ('ready', 'awaiting_note') RETURNING id", ()),
            "now": ("UPDATE publishing.story_jobs SET status = 'publish_requested', updated_at = now() "
                    "WHERE id = %s AND revision = %s AND status IN ('ready', 'awaiting_note', 'approved', 'scheduled') "
                    "RETURNING id", ()),
            "ideas": ("UPDATE publishing.story_jobs SET status = 'queued', note = %s, updated_at = now() "
                      "WHERE id = %s AND revision = %s AND status IN ('ready', 'awaiting_note') RETURNING id",
                      (IDEAS_NOTE,)),
            "cancel": ("UPDATE publishing.story_jobs SET status = 'cancelled', scheduled_at = NULL, updated_at = now() "
                       "WHERE id = %s AND revision = %s "
                       "AND status IN ('ready', 'awaiting_note', 'approved', 'scheduled') RETURNING id", ()),
        }[action]
        row = db._fetchone(sql[0], sql[1] + (job_id, rev))
        if not row:
            await _stale(db, q, job_id)
            return
        text = {"pub": "Ставлю в расписание…", "now": "Публикую сейчас…",
                "ideas": "Ищу другую подачу – пришлю новую версию.", "cancel": "Отменено."}[action]
        await q.answer(text)
        await mark(text)

    return handle


async def _stale(db, q, job_id):
    """Кнопка от старой версии или уже обработанной задачи – объясняем и снимаем кнопки."""
    row = db._fetchone("SELECT status, revision FROM publishing.story_jobs WHERE id = %s", (job_id,))
    why = {"published": "Эта сторис уже опубликована.", "publish_requested": "Уже публикую.",
           "approved": "Уже ставлю в расписание.",
           "publishing": "Уже публикую.", "cancelled": "Эта сторис отменена.",
           "queued": "Уже пересобираю – дождись новой версии.", "building": "Уже пересобираю – дождись новой версии.",
           }.get(row["status"] if row else "", "Эта версия устарела – смотри последнюю.")
    await q.answer(why, show_alert=True)
    try:
        await q.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass


def _note_target(db, message):
    """К какой сторис правка: ответ на превью (или на вопрос «Что поменять?») либо задача, где нажали
    «Другой текст» в последние 30 минут. Голое число правкой не считаем – это может быть ввод цены."""
    user = message.from_user
    if not user or user.id != _owner_id() or message.chat_id != user.id or not message.text:
        return None
    if message.text.startswith("/"):
        return None
    reply = message.reply_to_message
    if reply:
        row = db._fetchone(
            "SELECT id FROM publishing.story_jobs WHERE chat_id = %s AND preview_message_id = %s "
            "AND status IN ('ready', 'awaiting_note')", (message.chat_id, reply.message_id))
        if row:
            return row["id"]
    if _NUMBER.match(message.text.strip()) and not reply:
        return None
    row = db._fetchone(
        "SELECT id FROM publishing.story_jobs WHERE chat_id = %s AND status = 'awaiting_note' "
        "AND updated_at > now() - make_interval(mins => %s) ORDER BY updated_at DESC LIMIT 1",
        (message.chat_id, NOTE_WINDOW_MIN))
    return row["id"] if row else None


class _NoteFilter(filters.MessageFilter):
    def __init__(self, db):
        super().__init__(name="StoriesNote")
        self.db = db

    def filter(self, message) -> bool:
        if not message.text or not message.from_user or message.from_user.id != _owner_id():
            return False
        if not _enabled(self.db):
            return False
        try:
            return _note_target(self.db, message) is not None
        except Exception:
            logger.exception("stories_studio: не проверил правку")
            return False


def _make_note_handler(db):
    async def handle(update, context):
        message = update.effective_message
        job_id = _note_target(db, message)
        if not job_id:
            return
        row = db._fetchone(
            "UPDATE publishing.story_jobs SET note = %s, status = 'queued', updated_at = now() "
            "WHERE id = %s AND status IN ('ready', 'awaiting_note') RETURNING id",
            (message.text.strip()[:1000], job_id))
        if row:
            await message.reply_text(f"Принял правку к сторис №{job_id}, пересобираю.")
        else:
            await message.reply_text(f"Сторис №{job_id} уже в работе или опубликована – правку не применил.")

    return handle


def register(app, db):
    # только новые посты канала: правка подписи под старым постом не должна создавать сторис
    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POST & _StudioFilter(db), _make_handler(db)),
                    group=STUDIO_GROUP)
    app.add_handler(MessageHandler(filters.UpdateType.MESSAGE & filters.TEXT & _NoteFilter(db),
                                   _make_note_handler(db)))
    app.add_handler(CallbackQueryHandler(_make_callback(db), pattern=r"^st:(pub|now|note|ideas|cancel):"))
    try:
        ensure_schema(db)
    except Exception as e:
        logger.exception("stories_studio.ensure_schema отложено (БД не готова?): %s", e)
    logger.info("stories_studio: зарегистрирован")
