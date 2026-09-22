"""Сторис-студия, этап 1.1: приём материала для сторис в личке собственника.

Собственник шлёт боту фото, видео или альбом (с подписью или без) – материал
ложится в очередь `publishing.story_jobs`, бот отвечает «Принял». Сборку,
превью с кнопками и публикацию делает отдельное приложение `f2b-stories`
(этапы 1.2–1.5), здесь только вход.

- Работает, только если в `bot_settings` ключ `stories_studio_enabled` = '1'.
  Выключено – фильтр не срабатывает, фото и видео идут в общий handle_message
  как раньше (видео – в таблицу media).
- Пересланное не берём: собственник пересылает боту чужие сообщения и для других целей.
- Альбом (несколько фото или видео одним сообщением) Telegram присылает отдельными
  апдейтами с общим media_group_id – копим в одну задачу со статусом `collecting`
  и через ALBUM_WAIT_SEC без новых частей переводим в `queued`. Если бот
  перезапустился в этом окне, задача остаётся `collecting` – сборщик берёт
  такие старше минуты сам.
- Бот скачивает файлы только до 20 МБ (ограничение Bot API) – больше не берём
  и просим прислать обычным видео, не файлом.
- Материал заодно сохраняется в таблицу media, как раньше делал handle_message.

План: plans/2026-09-22-сторис-студия-в-боте.md (репо «второй мозг»).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime

import psycopg2.extras
from telegram.ext import MessageHandler, filters

logger = logging.getLogger(__name__)

SETTING_KEY = "stories_studio_enabled"
MAX_DOWNLOAD = 20 * 1024 * 1024      # getFile в Bot API отдаёт файлы до 20 МБ
MAX_STORY_SEC = 60                   # сторис в Telegram и Instagram – до 60 с
ALBUM_WAIT_SEC = 4                   # части альбома приходят подряд за 1–2 с
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
    error           text,
    CONSTRAINT story_jobs_status_chk CHECK (status IN (
        'collecting', 'queued', 'building', 'ready', 'publishing',
        'published', 'cancelled', 'failed'))
);
CREATE UNIQUE INDEX IF NOT EXISTS story_jobs_album_uq
    ON publishing.story_jobs (chat_id, media_group_id) WHERE media_group_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS story_jobs_status_idx
    ON publishing.story_jobs (status, created_at);
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


class _StudioFilter(filters.MessageFilter):
    """Своё фото/видео собственника в личке при включённой студии."""

    def __init__(self, db):
        super().__init__(name="StoriesStudio")
        self.db = db

    def filter(self, message) -> bool:
        user = message.from_user
        if not user or user.id != _owner_id() or message.chat_id != user.id:
            return False
        if message.forward_origin is not None:
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
    async def finalize_album(bot, chat_id: int, group_id: str):
        try:
            await _finalize_album(bot, chat_id, group_id)
        except Exception:
            logger.exception("stories_studio: альбом %s не финализирован", group_id)

    async def _finalize_album(bot, chat_id: int, group_id: str):
        await asyncio.sleep(ALBUM_WAIT_SEC)
        row = db._fetchone(
            "UPDATE publishing.story_jobs SET status = 'queued', updated_at = now() "
            "WHERE chat_id = %s AND media_group_id = %s AND status = 'collecting' "
            "AND updated_at <= now() - make_interval(secs => %s) "
            "RETURNING id, media",
            (chat_id, group_id, ALBUM_WAIT_SEC - 0.5),
        )
        if not row:
            return  # пришла ещё часть – финализирует её задача
        media = row["media"]
        long_note = _long_note(media)
        await bot.send_message(
            chat_id,
            f"Принял {_describe(media)} – соберу одну сторис (№{row['id']}).{long_note}")

    async def handle(update, context):
        message = update.effective_message
        user = message.from_user
        item = _media_item(message)
        caption = (message.caption or "").strip() or None

        # как раньше делал handle_message: материал собственника – в общую базу media
        try:
            db.save_media(file_id=item["file_id"], media_type=item["kind"], caption=caption or "",
                          chat_id=message.chat_id, uploader=user.full_name,
                          date=datetime.now().isoformat())
        except Exception:
            logger.exception("stories_studio: не сохранил в media")

        if item.get("file_size") and item["file_size"] > MAX_DOWNLOAD:
            what = "Видео" if item["kind"] == "video" else "Фото"
            await message.reply_text(
                f"{what} {_mb(item['file_size'])} – бот может скачать только до 20 МБ. "
                "Пришли его обычным видео или фото, не файлом: Telegram сожмёт.")
            return

        try:
            if message.media_group_id:
                db._execute(
                    "INSERT INTO publishing.story_jobs "
                    "(chat_id, author_id, media_group_id, media, caption, status) "
                    "VALUES (%s, %s, %s, %s, %s, 'collecting') "
                    "ON CONFLICT (chat_id, media_group_id) WHERE media_group_id IS NOT NULL "
                    "DO UPDATE SET media = publishing.story_jobs.media || EXCLUDED.media, "
                    "caption = COALESCE(publishing.story_jobs.caption, EXCLUDED.caption), "
                    "updated_at = now()",
                    (message.chat_id, user.id, message.media_group_id,
                     psycopg2.extras.Json([item]), caption),
                )
                task = asyncio.create_task(finalize_album(context.bot, message.chat_id, message.media_group_id))
                _album_tasks.add(task)
                task.add_done_callback(_album_tasks.discard)
                return
            row = db._fetchone(
                "INSERT INTO publishing.story_jobs (chat_id, author_id, media, caption, status) "
                "VALUES (%s, %s, %s, %s, 'queued') RETURNING id",
                (message.chat_id, user.id, psycopg2.extras.Json([item]), caption),
            )
        except Exception as exc:
            logger.exception("stories_studio: не поставил в очередь")
            await message.reply_text(f"Не смог принять материал для сторис: {exc}")
            return

        await message.reply_text(
            f"Принял {_describe([item])} для сторис (№{row['id']}). "
            f"Соберу и пришлю варианты.{_long_note([item])}")

    return handle


def _long_note(media: list[dict]) -> str:
    longest = max((m.get("duration") or 0 for m in media), default=0)
    if longest > MAX_STORY_SEC:
        return f"\nВидео {longest} с, в сторис влезает до {MAX_STORY_SEC} с – возьму лучший отрезок."
    return ""


def register(app, db):
    # только новые сообщения: правка подписи под старым фото не должна создавать сторис
    app.add_handler(MessageHandler(filters.UpdateType.MESSAGE & _StudioFilter(db), _make_handler(db)))
    try:
        ensure_schema(db)
    except Exception as e:
        logger.exception("stories_studio.ensure_schema отложено (БД не готова?): %s", e)
    logger.info("stories_studio: зарегистрирован")
