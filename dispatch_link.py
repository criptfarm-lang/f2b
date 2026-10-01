"""Именная ссылка на дашборд развозки (автораспределение по машинам) — /razvozka, /развозка.

Дашборд живёт отдельным сервисом Amvera `f2b-logi-dispatch`. Вход — подписанный токен
«<exp>.<who>.<hmac_sha256(DISPATCH_SSO_SECRET, '<exp>.<who>')>» в ?k=, 12 часов.
По имени из токена дашборд пишет, кто утвердил раскладку. Ссылку получают логисты
(route_dispatch._logist_chat_ids) и владелец; остальным команда молчит.
"""
import hashlib
import hmac
import logging
import os
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

import route_dispatch as rd

logger = logging.getLogger(__name__)

TTL_SEC = 12 * 3600
# chat_id → кто (латиницей; дашборд знает имена для этих ключей)
_WHO = {8267564735: "belyakova", 1296942948: "boeva"}


def _base_url() -> str:
    return os.getenv("DISPATCH_BASE_URL", "https://f2b-logi-dispatch-victor03.amvera.io")


def who_for(chat_id: int) -> str | None:
    if chat_id == rd._owner_chat_id():
        return "owner"
    if chat_id in rd._logist_chat_ids():
        return _WHO.get(chat_id, f"tg{chat_id}")
    return None


def make_link(secret: str, who: str, now: float | None = None) -> str:
    expires = int(now if now is not None else time.time()) + TTL_SEC
    msg = f"{expires}.{who}"
    sig = hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return f"{_base_url()}/?k={msg}.{sig}"


async def cmd_razvozka(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    who = who_for(chat_id)
    if not who:
        return
    secret = os.getenv("DISPATCH_SSO_SECRET", "")
    if not secret:
        await update.message.reply_text("Развозка временно недоступна: не настроен ключ входа.")
        logger.error("dispatch_link: нет DISPATCH_SSO_SECRET")
        return
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Открыть развозку", url=make_link(secret, who))]])
    await update.message.reply_text(
        "Развозка по машинам: предложение раскладки на карте. Поправь перетаскиванием "
        "и нажми «Утвердить». Ссылка личная, действует 12 часов — не пересылай её.",
        reply_markup=kb)


def register(app: Application):
    app.add_handler(CommandHandler("razvozka", cmd_razvozka))
    app.add_handler(MessageHandler(filters.Regex(r"^/развозка(@\w+)?(\s|$)"), cmd_razvozka))
    logger.info("dispatch_link: хендлеры зарегистрированы")
