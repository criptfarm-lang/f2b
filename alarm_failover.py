"""«авария» от собственника в личке бота «Эф» → всё на офисную коробочку (план «второго мозга»
plans/2026-11-18-аварийный-протокол-отказ-сервера.md).

На боевом сервере: свежая копия базы → коробочка поднимает службы, Амстердам принимает сайты,
адреса в nic.ru переключаются → службы боевого останавливаются (бот последним, иначе два бота «Эф»).
Делает это отдельный процесс `/opt/f2b-reserve/failover_to_box.sh` (systemd-run), бот только запускает.
На самой коробочке (файл /opt/f2b/ON_BOX) – отвечает, что уже переключено.
Если боевой лежит целиком, «авария» ловит сторож на коробочке (f2b-alarm-watch).
"""
import logging
import os
import re
import subprocess

from telegram import Update
from telegram.ext import ApplicationHandlerStop, ContextTypes, MessageHandler, filters

logger = logging.getLogger(__name__)
KEYWORD_RE = re.compile(r"^\s*авария[.!]*\s*$", re.IGNORECASE)
SCRIPT = "/opt/f2b-reserve/failover_to_box.sh"


def register(app, owner_id: int):
    async def on_alarm(update: Update, context: ContextTypes.DEFAULT_TYPE):
        msg = update.effective_message
        if os.path.exists("/opt/f2b/ON_BOX"):
            await msg.reply_text("Уже работаем на офисной коробочке.")
            raise ApplicationHandlerStop
        if not os.path.exists(SCRIPT):
            await msg.reply_text("Скрипт переключения не найден на сервере – переключить не могу.")
            raise ApplicationHandlerStop
        await msg.reply_text("Авария принята: снимаю свежую копию базы и переключаю всё на офисную коробочку. "
                             "Минут через 5–10 придёт отчёт, адреса дойдут за 5–30 мин.")
        try:
            subprocess.Popen(["systemd-run", "--unit", "f2b-to-box", "--collect", SCRIPT],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            logger.exception("авария: не запустил переключение")
            await msg.reply_text(f"Не запустилось: {e}")
        raise ApplicationHandlerStop

    owner_dm = filters.ChatType.PRIVATE & filters.User(user_id=owner_id)
    app.add_handler(MessageHandler(owner_dm & filters.Regex(KEYWORD_RE), on_alarm), group=-1)
    logger.info("alarm_failover: «авария» зарегистрирована")
