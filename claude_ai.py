"""
Интеграция с Claude API
- Ответы на свободные вопросы
- Извлечение задач из сообщений руководителя
"""

import os
import json
import logging
import re
from anthropic import AsyncAnthropic

logger = logging.getLogger(__name__)

client = AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

SYSTEM_PROMPT = """Ты — корпоративный ассистент компании F2B PRO (оптовая торговля рыбой и морепродуктами).
Ты работаешь в Telegram-группах отдела продаж.

Твои задачи:
- Отвечать на вопросы сотрудников кратко и по делу
- Помогать с текстами для клиентов
- Давать справки по продуктам компании (лосось, форель, тунец, креветки, угорь и др.)
- Помогать с расчётами, ценами, логистикой

Стиль: профессиональный, но дружелюбный. Отвечай на русском языке.
Если не знаешь точного ответа — скажи честно и предложи как найти информацию.
Отвечай кратко — максимум 3-4 предложения если не просят подробно."""


async def ask_claude(query: str, context: str = "") -> str:
    """Отправляет вопрос Claude и возвращает ответ."""
    try:
        user_content = query
        if context:
            user_content = f"Контекст о компании:\n{context}\n\nВопрос: {query}"

        response = await client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_content}]
        )
        return response.content[0].text

    except Exception as e:
        logger.error(f"Ошибка Claude API: {e}")
        return "Извини, не смог обработать запрос. Попробуй позже."


async def extract_tasks_from_message(text: str, author: str) -> list:
    """
    Анализирует сообщение руководителя и извлекает задачи.
    Возвращает список: [{"task": "...", "executor": "...", "deadline": "YYYY-MM-DD"}]
    """
    try:
        prompt = f"""Проанализируй сообщение руководителя и извлеки из него задачи для сотрудников.

Сообщение от {author}:
\"\"\"{text}\"\"\"

Верни ТОЛЬКО JSON массив. Каждая задача:
{{
  "task": "краткое описание задачи",
  "executor": "имя исполнителя (если указано, иначе пустая строка)",
  "deadline": "дата в формате YYYY-MM-DD (если указана, иначе null)"
}}

Если задач нет — верни пустой массив [].
Не включай общие объявления и информационные сообщения — только конкретные поручения.
Только JSON, без пояснений."""

        response = await client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )

        raw = response.content[0].text.strip()
        # Убираем markdown-обёртку если есть
        raw = re.sub(r"```json|```", "", raw).strip()
        tasks = json.loads(raw)
        return tasks if isinstance(tasks, list) else []

    except Exception as e:
        logger.warning(f"Не удалось извлечь задачи: {e}")
        return []


async def generate_morning_summary(tasks_today: list, tasks_overdue: list) -> str:
    """Генерирует утреннюю сводку для отправки в группу."""
    try:
        tasks_str = "\n".join([f"- {t['executor']}: {t['text']}" for t in tasks_today]) or "нет"
        overdue_str = "\n".join([f"- {t['executor']}: {t['text']} [срок: {t['deadline']}]"
                                  for t in tasks_overdue]) or "нет"

        prompt = f"""Составь короткое утреннее сообщение для рабочей группы в Telegram.

Задачи на сегодня:
{tasks_str}

Просроченные задачи:
{overdue_str}

Стиль: деловой, краткий, мотивирующий. Используй эмодзи уместно.
Максимум 15 строк."""

        response = await client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text

    except Exception as e:
        logger.error(f"Ошибка генерации сводки: {e}")
        # Фолбек без Claude
        lines = ["🌅 *Доброе утро, команда F2B PRO!*\n"]
        if tasks_today:
            lines.append("📋 *На сегодня:*")
            for t in tasks_today:
                lines.append(f"• {t.get('executor', '?')}: {t['text']}")
        if tasks_overdue:
            lines.append("\n🔴 *Просрочено — требует внимания:*")
            for t in tasks_overdue:
                lines.append(f"• {t.get('executor', '?')}: {t['text']} [{t.get('deadline')}]")
        return "\n".join(lines)
