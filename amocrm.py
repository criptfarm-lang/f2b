"""
amoCRM API — интеграция для F2B PRO бота
Функции: поиск контактов, сделок, отправка сообщений в чат сделки
"""

import os
import logging
import asyncio
import aiohttp
from typing import Optional

logger = logging.getLogger(__name__)

AMO_SUBDOMAIN = os.getenv("AMO_SUBDOMAIN", "victorfishtobiz")
AMO_BASE_URL = f"https://{AMO_SUBDOMAIN}.amocrm.ru/api/v4"


def get_headers() -> dict:
    token = os.getenv("AMO_ACCESS_TOKEN", "")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


# ─── Базовый запрос ───────────────────────────────────────────────────────────

async def amo_get(path: str, params: dict = None) -> Optional[dict]:
    """GET запрос к amoCRM API."""
    url = f"{AMO_BASE_URL}{path}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=get_headers(), params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 401:
                    logger.error("amoCRM: токен истёк, нужно обновить AMO_ACCESS_TOKEN")
                    return None
                else:
                    text = await resp.text()
                    logger.error(f"amoCRM GET {path}: {resp.status} {text[:200]}")
                    return None
    except Exception as e:
        logger.error(f"amoCRM GET {path}: {e}")
        return None


async def amo_post(path: str, data: dict) -> Optional[dict]:
    """POST запрос к amoCRM API."""
    url = f"{AMO_BASE_URL}{path}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=get_headers(), json=data) as resp:
                if resp.status in (200, 201):
                    return await resp.json()
                else:
                    text = await resp.text()
                    logger.error(f"amoCRM POST {path}: {resp.status} {text[:200]}")
                    return None
    except Exception as e:
        logger.error(f"amoCRM POST {path}: {e}")
        return None


# ─── Контакты и компании ──────────────────────────────────────────────────────

async def find_contact_by_name(name: str) -> list:
    """Ищет контакты по имени или названию компании."""
    result = await amo_get("/contacts", params={"query": name, "limit": 10})
    if not result:
        return []
    return result.get("_embedded", {}).get("contacts", [])


async def find_company_by_name(name: str) -> list:
    """Ищет компании по названию."""
    result = await amo_get("/companies", params={"query": name, "limit": 10})
    if not result:
        return []
    return result.get("_embedded", {}).get("companies", [])


# ─── Сделки ──────────────────────────────────────────────────────────────────

async def get_leads_by_contact(contact_id: int) -> list:
    """Получает сделки контакта."""
    result = await amo_get(f"/contacts/{contact_id}/leads")
    if not result:
        return []
    return result.get("_embedded", {}).get("leads", [])


async def get_lead(lead_id: int) -> Optional[dict]:
    """Получает сделку по ID."""
    return await amo_get(f"/leads/{lead_id}")


async def get_active_leads(limit: int = 50, page: int = 1) -> list:
    """Получает активные сделки (не закрытые)."""
    result = await amo_get("/leads", params={
        "limit": limit,
        "page": page,
        "filter[statuses][0][pipeline_id]": "",  # все воронки
    })
    if not result:
        return []
    return result.get("_embedded", {}).get("leads", [])


# ─── Поиск контрагентов по товару (через МойСклад + amoCRM) ──────────────────

async def find_contacts_for_broadcast(counterparty_names: list) -> list:
    """
    По списку названий контрагентов из МойСклад находит контакты/компании в amoCRM.
    Возвращает список: [{"name": ..., "contact_id": ..., "lead_id": ...}]
    """
    found = []
    for name in counterparty_names:
        # Ищем сначала как компанию
        companies = await find_company_by_name(name)
        if companies:
            company = companies[0]
            company_id = company["id"]
            # Получаем сделки компании
            leads_data = await amo_get(f"/companies/{company_id}/leads")
            leads = leads_data.get("_embedded", {}).get("leads", []) if leads_data else []
            if leads:
                lead_id = leads[0]["id"]
                found.append({
                    "name": name,
                    "amo_name": company.get("name", name),
                    "company_id": company_id,
                    "lead_id": lead_id,
                })
                continue

        # Ищем как контакт
        contacts = await find_contact_by_name(name)
        if contacts:
            contact = contacts[0]
            contact_id = contact["id"]
            leads = await get_leads_by_contact(contact_id)
            if leads:
                lead_id = leads[0]["id"]
                found.append({
                    "name": name,
                    "amo_name": contact.get("name", name),
                    "contact_id": contact_id,
                    "lead_id": lead_id,
                })

        await asyncio.sleep(0.1)  # не спамим API

    return found


# ─── Отправка сообщений ───────────────────────────────────────────────────────

async def send_message_to_lead(lead_id: int, text: str) -> bool:
    """
    Отправляет сообщение в чат сделки (через встроенные мессенджеры amoCRM).
    Сообщение уйдёт клиенту через тот мессенджер который он использует.
    """
    # Получаем talks (чаты) сделки
    talks_data = await amo_get(f"/leads/{lead_id}/talks")
    if not talks_data:
        logger.warning(f"Нет чатов для сделки {lead_id}")
        return False

    talks = talks_data.get("_embedded", {}).get("talks", [])
    if not talks:
        logger.warning(f"Сделка {lead_id}: нет активных чатов")
        return False

    # Берём последний активный чат
    talk_id = talks[0]["id"]

    result = await amo_post(f"/talks/{talk_id}/messages", {
        "text": text
    })

    if result:
        logger.info(f"Сообщение отправлено в сделку {lead_id}, чат {talk_id}")
        return True
    return False


# ─── Рассылка ────────────────────────────────────────────────────────────────

async def broadcast_to_leads(lead_ids: list, text: str, delay_seconds: int = 60) -> dict:
    """
    Медленная рассылка по списку сделок.
    delay_seconds — пауза между сообщениями (по умолчанию 1 минута).
    Возвращает статистику: {"sent": N, "failed": N, "errors": [...]}
    """
    sent = 0
    failed = 0
    errors = []

    for i, lead_id in enumerate(lead_ids):
        try:
            success = await send_message_to_lead(lead_id, text)
            if success:
                sent += 1
                logger.info(f"Рассылка [{i+1}/{len(lead_ids)}]: ✅ сделка {lead_id}")
            else:
                failed += 1
                errors.append(lead_id)
                logger.warning(f"Рассылка [{i+1}/{len(lead_ids)}]: ❌ сделка {lead_id}")
        except Exception as e:
            failed += 1
            errors.append(lead_id)
            logger.error(f"Рассылка [{i+1}/{len(lead_ids)}]: ошибка {e}")

        # Пауза между сообщениями (кроме последнего)
        if i < len(lead_ids) - 1:
            await asyncio.sleep(delay_seconds)

    return {"sent": sent, "failed": failed, "errors": errors}


# ─── Проверка подключения ─────────────────────────────────────────────────────

async def check_connection() -> bool:
    """Проверяет что токен работает."""
    result = await amo_get("/account")
    if result:
        logger.info(f"amoCRM подключён: {result.get('name')} (id={result.get('id')})")
        return True
    return False
