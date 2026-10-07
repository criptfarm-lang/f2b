"""Уборка обработанных сообщений с кнопками в личке собственника (07.10.2026).

Решение собственника: в чате с ботом должны висеть только необработанные
сообщения с кнопками. Нажал «Одобрить» / «Отклонить» / «Опубликовать» и т.п. –
сообщение с кнопками удаляется, остаётся только ответ бота с итогом.

Как понимаем, что кнопка «обработана»: все решающие обработчики после действия
снимают клавиатуру (edit_message_reply_markup(None) или правка текста/подписи
без reply_markup). Навигационные кнопки клавиатуру оставляют – такие сообщения
не трогаем. Поэтому правило общее, без списка обработчиков:

  нажатие в личке собственника → обработчик снял кнопки с этого сообщения →
  после обработчика сообщение удаляется.

Исключение – итог с ошибкой (⚠️, «ошибка», «не удалось», «исключение»):
такое сообщение оставляем, чтобы ошибку было видно.

Техника: правки сообщений ловим обёрткой методов ExtBot на уровне класса
(экземпляр Bot заморожен), удаление – обработчиком в последней группе, PTB
проходит группы одного апдейта последовательно. Правки из фоновых задач,
запущенных обработчиком, сюда не попадают – такие сообщения остаются.
"""
import inspect
import logging

from telegram import Update
from telegram.ext import CallbackQueryHandler, ContextTypes, ExtBot

logger = logging.getLogger(__name__)

PRE_GROUP = -50
POST_GROUP = 50

_ERROR_MARKS = ("⚠️", "ошибк", "не удалось", "исключение")

# (chat_id, message_id) → True, если кнопки сняты успешной правкой,
# False – сняты, но итог с ошибкой (не удаляем).
_kb_removed: dict = {}
_watched: set = set()


def _has_kb(markup) -> bool:
    return bool(markup and getattr(markup, "inline_keyboard", None))


def _wrap(name: str):
    orig = getattr(ExtBot, name)
    sig = inspect.signature(orig)

    async def wrapper(self, *args, **kwargs):
        res = await orig(self, *args, **kwargs)
        try:
            a = sig.bind_partial(self, *args, **kwargs).arguments
            key = (a.get("chat_id"), a.get("message_id"))
            if key in _watched:
                if _has_kb(a.get("reply_markup")):
                    _kb_removed.pop(key, None)
                else:
                    text = str(a.get("text") or a.get("caption") or "").lower()
                    _kb_removed[key] = not any(m in text for m in _ERROR_MARKS)
        except Exception as e:
            logger.warning(f"button_cleanup {name}: {e!r}")
        return res

    setattr(ExtBot, name, wrapper)


def register(app, owner_id: int) -> None:
    for name in ("edit_message_text", "edit_message_caption",
                 "edit_message_reply_markup", "edit_message_media"):
        _wrap(name)

    def _key(update: Update):
        q = update.callback_query
        m = q.message if q else None
        if not m or m.chat.type != "private" or m.chat.id != owner_id:
            return None
        return (m.chat.id, m.message_id)

    async def pre(update: Update, context: ContextTypes.DEFAULT_TYPE):
        key = _key(update)
        if key:
            _watched.add(key)
            _kb_removed.pop(key, None)

    async def post(update: Update, context: ContextTypes.DEFAULT_TYPE):
        key = _key(update)
        if not key:
            return
        _watched.discard(key)
        if _kb_removed.pop(key, False):
            try:
                await context.bot.delete_message(chat_id=key[0], message_id=key[1])
            except Exception as e:
                logger.warning(f"button_cleanup: не удалить {key}: {e!r}")

    app.add_handler(CallbackQueryHandler(pre), group=PRE_GROUP)
    app.add_handler(CallbackQueryHandler(post), group=POST_GROUP)
