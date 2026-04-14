"""
notifier.py — Рассылка клиентам при согласовании заказа в МойСклад.

Этот файл полностью независим от bot.py.
Любые правки в bot.py НЕ затрагивают рассылку.

Используется в bot.py только через один импорт:
    from notifier import check_order_agreed
"""

import logging
import os

logger = logging.getLogger(__name__)

QUIZ_BASE_URL = os.getenv("QUIZ_BASE_URL", "")

# In-memory кэш отправленных уведомлений — защита от дублей при параллельных вызовах
_notified_orders: set = set()
MS_STATE_AGREED = "005f3651-9a9a-11f0-0a80-03a900027474"
WAZZUP_API_URL = "https://api.wazzup24.com/v3/message"
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
# UUID дополнительных полей контрагентов в МойСклад
ATTR_TELEGRAM = "15052610-34d7-11f1-0a80-1489000ec44a"
ATTR_WHATSAPP = "1505270f-34d7-11f1-0a80-1489000ec44b"
ATTR_MAX      = "1505236e-34d7-11f1-0a80-1489000ec449"

# Каналы Wazzup
CHANNEL_TELEGRAM = "ddd24a95-9304-4098-a320-3e47fcd1020a"
CHANNEL_WHATSAPP = "e180aa1d-dc48-4d0a-bec3-fc0afc53cf03"
CHANNEL_MAX      = "1d5bc70a-7ca6-4895-8d1f-9690cf448214"


MONTHS_RU = [
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря"
]


async def _is_company_excluded(company_name: str) -> bool:
    """Проверяет находится ли компания в списке исключений квиза.
    При недоступном API — возвращает False (квиз отправляем, не блокируем)."""
    import aiohttp
    if not QUIZ_BASE_URL:
        return False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{QUIZ_BASE_URL}/api/check-exclusion",
                params={"company_name": company_name},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    return data.get("excluded", False)
    except Exception as e:
        logger.warning(f"_is_company_excluded: API недоступен ({e}), квиз отправляем")
    return False


async def _load_order_positions(order_id: str, headers: dict) -> str:
    """Загружает позиции заказа и возвращает отформатированный текст."""
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/customerorder/{order_id}/positions",
                headers=headers,
                params={"expand": "assortment", "limit": 100}
            ) as r:
                if r.status != 200:
                    return ""
                data = await r.json()
                lines = []
                for pos in data.get("rows", []):
                    name = pos.get("assortment", {}).get("name", "?")
                    qty = int(pos.get("quantity", 0))
                    price = (pos.get("price", 0) or 0) / 100
                    total = qty * price
                    lines.append(f"  • {name} — {qty} кг × {price:,.0f} руб/кг = {total:,.0f} руб.")
                return "\n".join(lines)
    except Exception as e:
        logger.warning(f"_load_order_positions: {e}")
        return ""


async def _get_agent_tags(agent_id: str, headers: dict) -> list:
    """Загружает теги контрагента из МойСклад."""
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/counterparty/{agent_id}",
                headers=headers
            ) as r:
                if r.status == 200:
                    cp = await r.json()
                    return cp.get("tags", [])
    except Exception as e:
        logger.warning(f"_get_agent_tags: {e}")
    return []


async def _get_contact_from_ms(agent_id: str, headers: dict) -> dict | None:
    """Читает контакт для рассылки из дополнительных полей контрагента в МойСклад.
    Возвращает {chat_id, chat_type, channel_id} или None."""
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/counterparty/{agent_id}",
                headers=headers,
                params={"expand": "attributes"}
            ) as r:
                if r.status != 200:
                    return None
                cp = await r.json()

        attributes = cp.get("attributes", [])
        telegram_id = None
        whatsapp_id = None
        max_id = None

        for attr in attributes:
            attr_href = attr.get("meta", {}).get("href", "")
            value = attr.get("value", "")
            if not value:
                continue
            if ATTR_TELEGRAM in attr_href:
                telegram_id = str(value)
            elif ATTR_WHATSAPP in attr_href:
                whatsapp_id = str(value)
            elif ATTR_MAX in attr_href:
                max_id = str(value)

        # Приоритет: Telegram → Max → WhatsApp
        if telegram_id:
            return {"chat_id": telegram_id, "chat_type": "telegram", "channel_id": CHANNEL_TELEGRAM}
        if max_id:
            return {"chat_id": max_id, "chat_type": "max", "channel_id": CHANNEL_MAX}
        if whatsapp_id:
            return {"chat_id": whatsapp_id, "chat_type": "whatsapp", "channel_id": CHANNEL_WHATSAPP}

    except Exception as e:
        logger.warning(f"_get_contact_from_ms: {e}")
    return None


async def check_order_agreed(order_href: str, bot, db):
    """При смене статуса заказа на Согласовано — отправляем клиенту в мессенджер.

    Аргументы:
        order_href — ссылка на заказ из МойСклад webhook
        bot        — telegram bot instance (не используется напрямую, для совместимости)
        db         — экземпляр Database
    """
    try:
        import aiohttp
        from moysklad import get_headers

        headers = get_headers()

        # ── 1. Загружаем заказ ────────────────────────────────────────────
        async with aiohttp.ClientSession() as session:
            async with session.get(
                order_href,
                headers=headers,
                params={"expand": "agent,state,owner"}
            ) as r:
                if r.status != 200:
                    return
                order = await r.json()

        # ── 2. Проверяем статус "Согласовано" ─────────────────────────────
        state_id = order.get("state", {}).get("meta", {}).get("href", "").split("/")[-1]
        if state_id != MS_STATE_AGREED:
            return

        order_id = order_href.split("/")[-1].split("?")[0]

        # ── 3. Дедупликация — не отправлять дважды ───────────────────────
        if order_id in _notified_orders or db.is_agreed_notified(order_id):
            logger.info(f"notifier: заказ {order_id} уже уведомлялся, пропускаем")
            return
        # Сразу помечаем в памяти чтобы параллельный вызов не прошёл
        _notified_orders.add(order_id)

        # ── 4. Данные заказа ──────────────────────────────────────────────
        order_name = order.get("name", "")
        order_sum  = (order.get("sum", 0) or 0) / 100
        agent      = order.get("agent", {})
        agent_name = agent.get("name", "")
        agent_id   = agent.get("meta", {}).get("href", "").split("/")[-1]

        # Пропускаем розничного покупателя
        if agent_name.lower().strip() == "розничный покупатель":
            db.save_agreed_notification(order_id)
            return

        logger.info(f"notifier: заказ {order_name} клиент={agent_name}")

        # ── 5. Проверка исключений — исключённые не получают ничего ──────
        excluded = await _is_company_excluded(agent_name) if QUIZ_BASE_URL else False
        if excluded:
            logger.info(f"notifier: {agent_name} в исключениях, рассылка не отправляется")
            db.save_agreed_notification(order_id)
            return

        # ── 6. Ищем контакт в МойСклад ──────────────────────────────────
        contact = await _get_contact_from_ms(agent_id, headers)
        if contact:
            logger.info(f"notifier: контакт {agent_name} найден в МойСклад → {contact['chat_type']}")
        else:
            logger.info(f"notifier: контакт {agent_name} не найден в МойСклад, пропускаем")
            # НЕ сохраняем флаг — заполни поля Telegram/Max/WhatsApp в карточке контрагента
            return

        # ── 7. Позиции заказа ─────────────────────────────────────────────
        positions_text = await _load_order_positions(order_id, headers)

        # ── 8. Дата отгрузки ──────────────────────────────────────────────
        delivery_raw = order.get("deliveryPlannedMoment", "")
        delivery_fmt = ""
        if delivery_raw:
            try:
                from datetime import date as _d
                d = _d.fromisoformat(delivery_raw[:10])
                delivery_fmt = f"{d.day} {MONTHS_RU[d.month - 1]}"
            except Exception:
                delivery_fmt = delivery_raw[:10]

        # ── 9. Формируем сообщение ────────────────────────────────────────
        msg = f"📋 Пожалуйста, проверьте заказ {order_name}\n\n"
        if positions_text:
            msg += f"📦 Состав:\n{positions_text}\n\n"
        msg += f"💰 Итого: {order_sum:,.0f} руб.\n"
        if delivery_fmt:
            msg += f"📅 Плановая дата отгрузки: {delivery_fmt}\n"

        # ── 10. Квиз (всем кроме исключённых, исключённые уже отфильтрованы) ──
        if QUIZ_BASE_URL:
            import urllib.parse, hashlib, aiohttp as _ah
            # Создаём короткую ссылку через квиз-сервер
            short_code = hashlib.md5(f"{order_id}{agent_id}".encode()).hexdigest()[:8]
            quiz_url = f"{QUIZ_BASE_URL}/q/{short_code}"
            try:
                async with _ah.ClientSession() as _qs:
                    await _qs.post(
                        f"{QUIZ_BASE_URL}/api/short-link",
                        params={
                            "order_id": order_id,
                            "client_id": agent_id,
                            "amount": int(order_sum),
                            "company_name": agent_name,
                        },
                        timeout=_ah.ClientTimeout(total=5)
                    )
            except Exception as _e:
                # Если не удалось создать короткую ссылку — используем длинную
                quiz_url = (
                    f"{QUIZ_BASE_URL}"
                    f"/?order={order_id}"
                    f"&client_id={agent_id}"
                    f"&amount={int(order_sum)}"
                    f"&company={urllib.parse.quote(agent_name)}"
                )
                logger.warning(f"notifier: short-link failed ({_e}), используем длинную")
            msg += (
                f"\n\n🎣 Дарим 300 кг филе форели! Играйте в викторину, копите FISHки и обменивайте на филе!\n"
                f"{quiz_url}"
            )
            logger.info(f"notifier: квиз добавлен для {agent_name}")

        # ── 11. Отправляем через Wazzup ───────────────────────────────────
        api_key = os.getenv("WAZZUP_API_KEY", "")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                WAZZUP_API_URL,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={
                    "channelId": contact["channel_id"],
                    "chatType":  contact["chat_type"],
                    "chatId":    contact["chat_id"],
                    "text":      msg,
                }
            ) as r:
                if r.status in (200, 201):
                    logger.info(f"notifier: ✅ отправлено {agent_name} → {contact['chat_type']}")
                    db.save_agreed_notification(order_id)
                    # Логируем рассылку в квиз-сервис для статистики
                    logger.info(f"notifier: QUIZ_BASE_URL={QUIZ_BASE_URL!r}")
                    if QUIZ_BASE_URL:
                        try:
                            async with aiohttp.ClientSession() as _s:
                                await _s.post(
                                    f"{QUIZ_BASE_URL}/api/log-mailing",
                                    json={"order_id": order_id, "client_id": agent_id, "company_name": agent_name},
                                    timeout=aiohttp.ClientTimeout(total=5)
                                )
                        except Exception as _e:
                            logger.warning(f"notifier: log-mailing failed ({_e})")
                else:
                    body = await r.text()
                    logger.error(f"notifier: ❌ ошибка {r.status} для {agent_name}: {body[:200]}")

    except Exception as e:
        logger.error(f"notifier.check_order_agreed: {e}", exc_info=True)
