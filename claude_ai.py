"""
Claude AI — мозг бота F2B PRO
Все запросы проходят через Claude, который сам решает что делать
"""

import os
import json
import logging
import re
from anthropic import AsyncAnthropic

logger = logging.getLogger(__name__)

def get_client():
    """Lazy client init — ensures env vars are loaded."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set!")
    return AsyncAnthropic(api_key=api_key)

# ─── Список сотрудников для контекста ────────────────────────────────────────
EMPLOYEES_CONTEXT = """
Сотрудники компании F2B PRO:
- Белякова Александра (менеджер закупок) — Александра, Белякова, Саша
- Алексей Леонтьев — Алексей, Леонтьев, Лёша
- Ярослав — Ярослав, Ярик
- Андрей Иванов — Андрей, Иванов
- Инесса Скляр — Инесса, Скляр
- Маланчук Александр — Маланчук
- Карина Баласанян — Карина, Баласанян
- Елена Мерзлякова — Елена, Лена, Мерзлякова, Марзлякова
- Татьяна Голубева — Татьяна, Таня, Голубева
- Васильев Виктор (руководитель) — Виктор, Васильев
"""

# ─── Главный промпт-диспетчер ─────────────────────────────────────────────────
DISPATCHER_PROMPT = f"""Ты — умный диспетчер корпоративного бота компании F2B PRO (оптовая торговля рыбой и морепродуктами).

{EMPLOYEES_CONTEXT}

Продукция компании: лосось, форель, тунец, креветки, угорь, кальмар, треска, минтай, семга, нерка, кета и другие морепродукты.

Твоя задача — понять запрос пользователя и вернуть JSON с действием.

ДОСТУПНЫЕ ДЕЙСТВИЯ:
- get_tasks: показать задачи сотрудника
- get_all_tasks: показать все задачи команды
- get_overdue: показать просроченные задачи
- get_report: недельный отчёт
- get_debtors: дебиторка
- find_photo: найти фото товара из базы Telegram-чата
- get_price: получить полный прайс-лист из МойСклад
- ms_search: найти конкретный товар в МойСклад (остатки + цена + характеристики)
- find_contact: найти контакт
- answer: ответить на вопрос текстом (когда не подходит ни одно действие выше)

ВАЖНО — когда использовать ms_search vs find_photo:
- ms_search: "что есть из лосося", "остатки тунца", "цена на форель", "есть ли семга", "покажи товары"
- find_photo: "пришли фото тунца", "покажи как выглядит упаковка"

ФОРМАТ ОТВЕТА — только JSON, никакого текста вокруг:
{{
  "action": "название_действия",
  "params": {{
    // параметры зависят от действия:
    // get_tasks: "employee" — полное имя из списка сотрудников, или null если спрашивают про себя
    // find_photo: "query" — название товара для поиска
    // find_contact: "query" — имя или компания
    // answer: "text" — готовый ответ пользователю
  }},
  "confidence": 0.0-1.0  // уверенность в интерпретации
}}

ПРИМЕРЫ:
Запрос: "покажи что висит у Лены"
Ответ: {{"action": "get_tasks", "params": {{"employee": "Елена Мерзлякова"}}, "confidence": 0.95}}

Запрос: "пришли фото форель трим С"
Ответ: {{"action": "find_photo", "params": {{"query": "форель трим С"}}, "confidence": 0.99}}

Запрос: "что есть из лосося?"
Ответ: {{"action": "ms_search", "params": {{"query": "лосось"}}, "confidence": 0.99}}

Запрос: "есть ли тунец на складе?"
Ответ: {{"action": "ms_search", "params": {{"query": "тунец"}}, "confidence": 0.99}}

Запрос: "какая цена на форель?"
Ответ: {{"action": "ms_search", "params": {{"query": "форель"}}, "confidence": 0.97}}

Запрос: "кто должен нам деньги?"
Ответ: {{"action": "get_debtors", "params": {{}}, "confidence": 0.9}}

Запрос: "какой срок годности у замороженного лосося?"
Ответ: {{"action": "answer", "params": {{"text": "Срок годности замороженного лосося при температуре -18°C составляет обычно 6-9 месяцев. При -25°C — до 12 месяцев. После разморозки хранить не более 2 суток в холодильнике."}}, "confidence": 0.95}}

Запрос: "мои задачи"
Ответ: {{"action": "get_tasks", "params": {{"employee": null}}, "confidence": 1.0}}

Отвечай ТОЛЬКО валидным JSON. Никаких пояснений, никакого markdown."""



async def detect_task_completion(text: str, open_tasks: list) -> list:
    """
    Анализирует сообщение чата — возвращает список ID задач которые выполнены.
    open_tasks: список dict с полями id, text, executor
    """
    if not open_tasks or not text:
        return []

    tasks_str = "\n".join([
        f"ID={t['id']}: {t.get('executor','')} — {t.get('text','')}"
        for t in open_tasks[:20]
    ])

    prompt = f"""Ты анализируешь рабочий чат компании. Сообщение:
"{text}"

Открытые задачи:
{tasks_str}

Задача считается выполненной если сообщение ПРЯМО или КОСВЕННО говорит о её завершении.
Примеры выполнения:
- "я позвонила" → закрывает задачу "позвонить клиенту"
- "Орехов заплатит завтра" → закрывает задачу "ответить по ИП Орехов" (результат получен)
- "Карина задачу выполнила" → закрывает любую задачу Карины
- "готово" / "сделала" / "отправила" → закрывает релевантную задачу

НЕ закрывать если:
- это новое поручение ("Саша, сделай X")
- вопрос ("как дела с задачей?")
- обсуждение без результата

Отвечай ТОЛЬКО JSON массивом ID. Пример: [3, 7] или []"""

    try:
        client = get_client()
        resp = await client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=100,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = resp.content[0].text.strip()
        import json as _json
        # Извлекаем массив из ответа
        match = re.search(r'\[.*?\]', raw, re.DOTALL)
        if match:
            ids = _json.loads(match.group())
            return [int(i) for i in ids if isinstance(i, (int, float))]
    except Exception as e:
        logger.error(f"detect_task_completion error: {e}")
    return []

async def dispatch(query: str, user_name: str, context_data: str = "",
                   chat_history: str = "", memories: str = "") -> dict:
    """
    Главная функция — Claude разбирает запрос и возвращает действие.
    Возвращает dict: {"action": "...", "params": {...}, "confidence": 0.9}
    """
    try:
        history_block = f"\n\nИСТОРИЯ ЧАТА (последние сообщения):\n{chat_history}" if chat_history else ""
        memory_block = f"\n\nДОЛГОСРОЧНАЯ ПАМЯТЬ:\n{memories}" if memories else ""
        user_context = (
            f"Запрос от: {user_name}\n"
            f"Контекст системы: {context_data}"
            f"{history_block}"
            f"{memory_block}"
            f"\n\nЗапрос: {query}"
        )

        response = await get_client().messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            system=DISPATCHER_PROMPT,
            messages=[{"role": "user", "content": user_context}]
        )

        raw = response.content[0].text.strip()
        logger.info(f"Dispatch raw response: {raw!r}")

        # Убираем markdown обёртку
        raw = re.sub(r"```json|```", "", raw).strip()

        # Ищем JSON даже если вокруг есть текст
        json_match = re.search(r'[{].*[}]', raw, re.DOTALL)
        if json_match:
            raw = json_match.group(0)

        result = json.loads(raw)
        logger.info(f"Dispatch: '{query}' → {result['action']} (conf={result.get('confidence', '?')})")
        return result

    except json.JSONDecodeError as e:
        logger.error(f"Dispatch JSON error: {e}, raw: {raw!r}")
        # Фолбек на умный текстовый ответ
        return {"action": "answer", "params": {"text": None}, "confidence": 0.0}
    except Exception as e:
        logger.error(f"Dispatch error: {e}")
        return {"action": "answer", "params": {"text": None}, "confidence": 0.0}


async def smart_answer(query: str, user_name: str, context_data: str = "") -> str:
    """Умный ответ на вопрос — когда нужен текст, а не действие."""
    try:
        system = f"""Ты — корпоративный ассистент компании F2B PRO (оптовая торговля рыбой и морепродуктами).
{EMPLOYEES_CONTEXT}
Контекст системы: {context_data}

Отвечай кратко, по делу, на русском языке. Максимум 4-5 предложений если не просят подробнее.
Если вопрос про рыбу/морепродукты — отвечай профессионально как эксперт отрасли."""

        response = await get_client().messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=800,
            system=system,
            messages=[{"role": "user", "content": f"{user_name} спрашивает: {query}"}]
        )
        return response.content[0].text

    except Exception as e:
        logger.error(f"Smart answer error: {e}")
        return "Извини, не смог обработать запрос. Попробуй позже."


async def extract_tasks_from_message(text: str, author: str) -> list:
    """
    Анализирует сообщение руководителя и извлекает задачи.
    Возвращает список: [{"task": "...", "executor": "...", "deadline": "YYYY-MM-DD"}]
    """
    try:
        prompt = f"""Ты анализируешь сообщение руководителя компании F2B PRO и извлекаешь из него задачи.

{EMPLOYEES_CONTEXT}

Сообщение от {author}:
\"\"\"{text}\"\"\"

Верни ТОЛЬКО JSON массив задач. Каждая задача:
{{
  "task": "краткое чёткое описание задачи",
  "executor": "полное имя исполнителя из списка сотрудников (если упомянут), иначе пустая строка",
  "deadline": "дата в формате YYYY-MM-DD если указана (учти 'до конца марта' = 2026-03-31, 'до пятницы' = ближайшая пятница), иначе null"
}}

Правила:
- Извлекай только конкретные поручения, не общую информацию
- Если задача адресована нескольким людям — создай отдельную запись для каждого
- Если исполнитель не указан явно — попробуй определить по контексту
- Если задач нет — верни []

Только JSON, без пояснений."""

        response = await get_client().messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )

        raw = response.content[0].text.strip()
        raw = re.sub(r"```json|```", "", raw).strip()
        tasks = json.loads(raw)
        return tasks if isinstance(tasks, list) else []

    except Exception as e:
        logger.warning(f"Task extraction error: {e}")
        return []


async def generate_morning_summary(tasks_today: list, tasks_overdue: list) -> str:
    """Генерирует утреннюю сводку."""
    try:
        tasks_str = "\n".join([f"- {t.get('executor','?')}: {t['text']}" for t in tasks_today]) or "нет"
        overdue_str = "\n".join([f"- {t.get('executor','?')}: {t['text']} [срок: {t.get('deadline','?')}]"
                                  for t in tasks_overdue]) or "нет"

        response = await get_client().messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role": "user", "content":
                f"Составь короткое деловое утреннее сообщение для рабочей Telegram-группы.\n\n"
                f"Задачи на сегодня:\n{tasks_str}\n\n"
                f"Просроченные задачи:\n{overdue_str}\n\n"
                f"Стиль: деловой, краткий. Используй эмодзи уместно. Максимум 12 строк."}]
        )
        return response.content[0].text

    except Exception as e:
        logger.error(f"Morning summary error: {e}")
        lines = ["🌅 *Доброе утро, F2B PRO!*\n"]
        if tasks_today:
            lines.append("📋 *На сегодня:*")
            for t in tasks_today:
                lines.append(f"• {t.get('executor','?')}: {t['text']}")
        if tasks_overdue:
            lines.append("\n🔴 *Просрочено:*")
            for t in tasks_overdue:
                lines.append(f"• {t.get('executor','?')}: {t['text']}")
        return "\n".join(lines)
