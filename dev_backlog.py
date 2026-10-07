"""
Накопитель доработок системы (07.10.2026).

Собственник пишет боту в личку сообщение, начинающееся со слова «ДОРАБОТКА»
(текст или подпись к фото/файлу). Бот кладёт его в таблицу dev_backlog и
отвечает номером. Если это ответ на сообщение бота – цитируемый текст
сохраняется рядом: так видно, к какому алерту/отчёту относится замечание.

Разбирает очередь сессия Claude на ноутбуке по слову «ДОРАБОТКА»
(скилл f2b-dorabotka: список открытых → починка → закрытие с итогом).

Обработчик стоит в group=-1: иначе подпись к фото или текст могут съесть
модули с собственными MessageHandler (сторис-студия, согласования).
"""

import json
import logging
import re

from telegram import Update
from telegram.ext import ApplicationHandlerStop, ContextTypes, MessageHandler, filters

logger = logging.getLogger(__name__)

KEYWORD_RE = re.compile(r"^\s*доработк[аи]\b[\s:.,–—-]*", re.IGNORECASE)

# media_group_id → id доработки: в альбоме подпись есть только у первого фото,
# остальные приходят отдельными апдейтами следом. Живёт в памяти – альбом
# прилетает за секунды, рестарт в эту секунду допустимый риск.
_album_items: dict[str, int] = {}

SCHEMA = """
CREATE TABLE IF NOT EXISTS dev_backlog (
    id          SERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    text        TEXT NOT NULL DEFAULT '',
    quoted      TEXT,
    files       JSONB NOT NULL DEFAULT '[]'::jsonb,
    tg_message_id BIGINT,
    status      TEXT NOT NULL DEFAULT 'open',
    closed_at   TIMESTAMPTZ,
    resolution  TEXT
)
"""


def ensure_schema(db):
    db._execute(SCHEMA)


def _files_of(msg) -> list[dict]:
    if msg.photo:
        return [{"kind": "photo", "file_id": msg.photo[-1].file_id}]
    if msg.document:
        return [{"kind": "document", "file_id": msg.document.file_id,
                 "name": msg.document.file_name or ""}]
    if msg.voice:
        return [{"kind": "voice", "file_id": msg.voice.file_id}]
    return []


def _quoted(msg) -> str | None:
    r = msg.reply_to_message
    if not r:
        return None
    return (r.text or r.caption or "")[:4000] or None


def make_handlers(db, owner_id: int):
    async def on_item(update: Update, context: ContextTypes.DEFAULT_TYPE):
        msg = update.effective_message
        raw = msg.text or msg.caption or ""
        body = KEYWORD_RE.sub("", raw, count=1).strip()
        files = _files_of(msg)
        quoted = _quoted(msg)
        if not body and not files and not quoted:
            await msg.reply_text("Пустая доработка. Напишите после слова ДОРАБОТКА, что не так.")
            raise ApplicationHandlerStop
        try:
            row = db._fetchone(
                "INSERT INTO dev_backlog (text, quoted, files, tg_message_id) "
                "VALUES (%s, %s, %s::jsonb, %s) RETURNING id",
                (body, quoted, json.dumps(files, ensure_ascii=False), msg.message_id),
            )
            n_open = db._fetchone("SELECT count(*) AS n FROM dev_backlog WHERE status='open'")["n"]
        except Exception as e:
            logger.exception(f"dev_backlog: не записал: {e}")
            await msg.reply_text("Не удалось записать доработку, база недоступна. Повторите позже.")
            raise ApplicationHandlerStop
        if msg.media_group_id:
            _album_items[msg.media_group_id] = row["id"]
        await msg.reply_text(f"Записал доработку №{row['id']}. Открытых: {n_open}.")
        raise ApplicationHandlerStop

    async def on_album_tail(update: Update, context: ContextTypes.DEFAULT_TYPE):
        msg = update.effective_message
        item_id = _album_items.get(msg.media_group_id)
        if not item_id:
            return
        try:
            db._execute(
                "UPDATE dev_backlog SET files = files || %s::jsonb WHERE id=%s",
                (json.dumps(_files_of(msg), ensure_ascii=False), item_id),
            )
        except Exception as e:
            logger.exception(f"dev_backlog: фото альбома не дописано: {e}")
        raise ApplicationHandlerStop

    owner_dm = filters.ChatType.PRIVATE & filters.User(user_id=owner_id)
    keyword = filters.Regex(KEYWORD_RE) | filters.CaptionRegex(KEYWORD_RE)
    return [
        MessageHandler(owner_dm & keyword, on_item),
        MessageHandler(owner_dm & (filters.PHOTO | filters.Document.ALL), on_album_tail),
    ]


def _check(request, token) -> bool:
    return bool(token) and request.headers.get("Authorization", "") == f"Bearer {token}"


def add_routes(web_app, db, bot, token: str):
    """HTTP для скилла f2b-dorabotka: с ноутбука прямой Postgres Amvera часто
    недоступен (TLS-обрыв), а HTTPS бота доступен. Токен – MARKET_INTEL_TOKEN."""
    from aiohttp import web

    def _row(r):
        r = dict(r)
        for k in ("created_at", "closed_at"):
            if r.get(k):
                r[k] = r[k].isoformat()
        if isinstance(r.get("files"), str):
            r["files"] = json.loads(r["files"])
        return r

    async def list_items(request):
        if not _check(request, token):
            return web.Response(text="forbidden", status=403)
        if request.query.get("all"):
            rows = db._fetchall("SELECT * FROM dev_backlog ORDER BY id DESC LIMIT 30")
        else:
            rows = db._fetchall("SELECT * FROM dev_backlog WHERE status='open' ORDER BY id")
        return web.json_response([_row(r) for r in rows])

    async def set_status(request):
        if not _check(request, token):
            return web.Response(text="forbidden", status=403)
        try:
            item_id = int(request.match_info["id"])
            data = await request.json()
            status = data["status"]
            assert status in ("open", "done", "skipped")
        except Exception:
            return web.Response(text="bad request", status=400)
        if status == "open":
            row = db._fetchone("UPDATE dev_backlog SET status='open', closed_at=NULL, "
                               "resolution=NULL WHERE id=%s RETURNING id", (item_id,))
        else:
            row = db._fetchone("UPDATE dev_backlog SET status=%s, closed_at=now(), "
                               "resolution=%s WHERE id=%s RETURNING id",
                               (status, data.get("resolution") or "", item_id))
        if not row:
            return web.Response(text="not found", status=404)
        n = db._fetchone("SELECT count(*) AS n FROM dev_backlog WHERE status='open'")["n"]
        return web.json_response({"ok": True, "id": item_id, "open_left": n})

    async def get_file(request):
        if not _check(request, token):
            return web.Response(text="forbidden", status=403)
        try:
            f = await bot.get_file(request.match_info["file_id"])
            data = await f.download_as_bytearray()
        except Exception as e:
            return web.Response(text=f"telegram: {e}", status=502)
        ext = (f.file_path or "").rsplit(".", 1)[-1] if "." in (f.file_path or "") else "bin"
        return web.Response(body=bytes(data), headers={"X-File-Ext": ext},
                            content_type="application/octet-stream")

    web_app.router.add_get("/dev-backlog", list_items)
    web_app.router.add_post("/dev-backlog/{id}", set_status)
    web_app.router.add_get("/dev-backlog/file/{file_id}", get_file)


def register(app, db, owner_id: int):
    # Хендлеры первыми, схема после и best-effort (см. feedback_bot_dev_pitfalls).
    for h in make_handlers(db, owner_id):
        app.add_handler(h, group=-1)
    try:
        ensure_schema(db)
    except Exception as e:
        logger.exception(f"dev_backlog: ensure_schema упал: {e}")
    logger.info("dev_backlog: хендлеры зарегистрированы")
