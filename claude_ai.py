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

ПРАВИЛО №1 — ПДЗ / ПРОСРОЧКА:
Следующие слова и сокращения ВСЕГДА означают get_overdue_debt (просроченная дебиторка):
  "пдз", "ПДЗ" — просроченная дебиторская задолженность
  "просрочка", "просроченный долг", "просроченная задолженность"
  "просрочка у [компания]", "пдз по [менеджер]", "есть ли пдз"
→ Исключение: только если явно про задачи/дедлайны/поручения — get_overdue
Примеры:
  "пдз" → get_overdue_debt
  "есть пдз у Гефест?" → get_overdue_debt (query=Гефест)
  "пдз по Скляр" → get_overdue_debt (tag=скляр)
  "просрочка у Джи Эф" → get_overdue_debt (query=Джи Эф)
  "просроченные задачи" → get_overdue

ДОСТУПНЫЕ ДЕЙСТВИЯ:
- get_tasks: показать задачи сотрудника
- get_all_tasks: показать все задачи команды
- get_overdue: просроченные ЗАДАЧИ/ПОРУЧЕНИЯ (дедлайн прошёл) — НЕ для долгов и клиентов
- get_report: недельный отчёт
- get_debtors: общий список всех должников
- get_debt: долг конкретного контрагента/клиента (параметр: query — название компании)
- find_counterparty: найти контрагента — менеджер, тип (ХОРЕКА/ОПТ), долг, теги (параметр: query)
- get_group_debts: долги по группе/менеджеру/типу (параметр: tag — баласанян/скляр/хорека/опт и т.д.)
- get_group_clients: список клиентов группы/менеджера (параметр: tag)
- prepare_reminders: подготовить тексты напоминаний об оплате для клиентов (параметр: tag — если указывают конкретного менеджера)
- get_overdue_debt: просроченная ДЕБИТОРКА — клиент не заплатил вовремя (параметры: query=компания, tag=группа, brief=true если просят кратко/по менеджерам/сводку)
- find_photo: найти и прислать фото товара из МойСклад (карточки товаров)
- get_price: получить полный прайс-лист из МойСклад
- ms_search: найти конкретный товар в МойСклад (остатки + цена + характеристики)
- find_contact: найти контакт
- answer: ответить на вопрос текстом (когда не подходит ни одно действие выше)

ВАЖНО — когда использовать ms_search vs find_photo:
- ms_search: "что есть из лосося", "остатки тунца", "цена на форель", "есть ли семга", "покажи товары"
- find_photo: "пришли фото тунца", "покажи как выглядит упаковка", "как выглядит товар"
Фото берутся напрямую из карточек товаров МойСклад — они там есть.

ВАЖНО — когда использовать answer (а НЕ ms_search):
- Жалобы и замечания: "ты не написал цену", "ты не ответил", "неправильно понял"
- Благодарности: "спасибо", "окей", "понял"
- Вопросы не про товары: "как дела", "ты тут?"
- Общие вопросы без названия конкретного товара

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

Запрос: "какой долг у Белуги?"
Ответ: {{"action": "get_debt", "params": {{"query": "Белуга"}}, "confidence": 0.98}}

Запрос: "сколько должна Белуга Плюс?"
Ответ: {{"action": "get_debt", "params": {{"query": "Белуга Плюс"}}, "confidence": 0.98}}

Запрос: "чей ИП Орехов?"
Ответ: {{"action": "find_counterparty", "params": {{"query": "Орехов"}}, "confidence": 0.97}}

Запрос: "кто ведёт Атмосфера ООО?"
Ответ: {{"action": "find_counterparty", "params": {{"query": "Атмосфера"}}, "confidence": 0.97}}

Запрос: "какой тип у клиента Ромашка?"
Ответ: {{"action": "find_counterparty", "params": {{"query": "Ромашка"}}, "confidence": 0.95}}

Запрос: "долги по Баласанян"
Ответ: {{"action": "get_group_debts", "params": {{"tag": "баласанян"}}, "confidence": 0.98}}

Запрос: "кто должен из хореки?"
Ответ: {{"action": "get_group_debts", "params": {{"tag": "хорека"}}, "confidence": 0.97}}

Запрос: "долги по оптовым"
Ответ: {{"action": "get_group_debts", "params": {{"tag": "опт"}}, "confidence": 0.97}}

Запрос: "список клиентов Скляр"
Ответ: {{"action": "get_group_clients", "params": {{"tag": "скляр"}}, "confidence": 0.95}}

Запрос: "покажи всех клиентов Мерзляковой"
Ответ: {{"action": "get_group_clients", "params": {{"tag": "мерзлякова"}}, "confidence": 0.95}}

Запрос: "просроченная дебиторка"
Ответ: {{"action": "get_overdue_debt", "params": {{}}, "confidence": 0.98}}

Запрос: "просрочка по Скляр"
Ответ: {{"action": "get_overdue_debt", "params": {{"tag": "скляр"}}, "confidence": 0.97}}

Запрос: "кто из хореки просрочил оплату?"
Ответ: {{"action": "get_overdue_debt", "params": {{"tag": "хорека"}}, "confidence": 0.96}}

Запрос: "есть просроченная задолженность у Биг Мама?"
Ответ: {{"action": "get_overdue_debt", "params": {{"query": "Биг Мама"}}, "confidence": 0.97}}

Запрос: "просрочка по Атмосфера"
Ответ: {{"action": "get_overdue_debt", "params": {{"query": "Атмосфера"}}, "confidence": 0.97}}

Запрос: "Белуга просрочила?"
Ответ: {{"action": "get_overdue_debt", "params": {{"query": "Белуга"}}, "confidence": 0.95}}

Запрос: "есть просрочка у Биг Мама?"
Ответ: {{"action": "get_overdue_debt", "params": {{"query": "Биг Мама"}}, "confidence": 0.98}}

Запрос: "просрочка у Фугу"
Ответ: {{"action": "get_overdue_debt", "params": {{"query": "Фугу"}}, "confidence": 0.98}}

Запрос: "просрочка"
Ответ: {{"action": "get_overdue_debt", "params": {{}}, "confidence": 0.95}}

Запрос: "пдз"
Ответ: {{"action": "get_overdue_debt", "params": {{}}, "confidence": 0.99}}

Запрос: "есть пдз у Гефест?"
Ответ: {{"action": "get_overdue_debt", "params": {{"query": "Гефест"}}, "confidence": 0.99}}

Запрос: "пдз по Скляр"
Ответ: {{"action": "get_overdue_debt", "params": {{"tag": "скляр"}}, "confidence": 0.99}}

Запрос: "пдз по хореке"
Ответ: {{"action": "get_overdue_debt", "params": {{"tag": "хорека"}}, "confidence": 0.99}}

Запрос: "дай дебиторку кратко"
Ответ: {{"action": "get_overdue_debt", "params": {{"brief": true}}, "confidence": 0.99}}

Запрос: "пдз кратко"
Ответ: {{"action": "get_overdue_debt", "params": {{"brief": true}}, "confidence": 0.99}}

Запрос: "покажи пдз по менеджерам"
Ответ: {{"action": "get_overdue_debt", "params": {{"brief": true}}, "confidence": 0.99}}

Запрос: "какой срок годности у замороженного лосося?"
Ответ: {{"action": "answer", "params": {{"text": "Срок годности замороженного лосося при температуре -18°C составляет обычно 6-9 месяцев. При -25°C — до 12 месяцев. После разморозки хранить не более 2 суток в холодильнике."}}, "confidence": 0.95}}

Запрос: "мои задачи"
Ответ: {{"action": "get_tasks", "params": {{"employee": null}}, "confidence": 1.0}}

Запрос: "подготовь напоминания об оплате"
Ответ: {{"action": "prepare_reminders", "params": {{}}, "confidence": 0.99}}

Запрос: "напоминания для клиентов Карины"
Ответ: {{"action": "prepare_reminders", "params": {{"tag": "баласанян"}}, "confidence": 0.99}}

Запрос: "подготовь напоминалки по просрочке"
Ответ: {{"action": "prepare_reminders", "params": {{}}, "confidence": 0.99}}

Запрос: "напомни должникам об оплате"
Ответ: {{"action": "prepare_reminders", "params": {{}}, "confidence": 0.99}}

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

async def parse_product_query(query: str) -> dict:
    """Claude разбирает запрос на товар и возвращает структурированные фильтры.
    
    Возвращает:
    {
        "search_term": "основное слово для поиска в API",
        "filters": {
            "trim": "пр"|"а"|"б"|"д"|"е"|"с"|null,
            "processing": "хк"|"сс"|"см"|"охл"|null,
            "frozen": true|false|null,
            "region": "мурманск"|null,
            "caliber": "1.6-2.0"|null
        },
        "raw_tokens": ["слова", "для", "скоринга"]
    }
    """
    client = get_client()
    
    prompt = f"""Ты помощник для поиска рыбных товаров на складе.
Разбери запрос и верни JSON с параметрами поиска.

СПРАВОЧНИК СОКРАЩЕНИЙ:
- ПР, пр = вид разделки "Трим ПР"
- А, Б, Д, Е, С = вид разделки "Трим А/Б/Д/Е/С"
- ПСГ = потрошёный без головы (тушка)
- Тушка = ПСГ (потрошёный без головы)
- ХК, х/к = холодное копчение
- ГК, г/к = горячее копчение
- СС, с/с = слабосолёный
- СМ, с/м = сырой мороженый (нет копчения и засолки)
- ОХЛ = охлаждённый
- ЗАМ, ЗАМОРОЖ = замороженный
- МРМ = Мурманск (происхождение)
- Семга, сёмга = лосось атл.

ФОРМАТ ОТВЕТА — только JSON:
{{
  "search_term": "главное слово для поиска (название рыбы)",
  "filters": {{
    "trim": "пр" | "а" | "б" | "д" | "е" | "с" | null,
    "cut": "псг" | "филе" | null,
    "processing": "хк" | "гк" | "сс" | "см" | null,
    "state": "охл" | "заморож" | null,
    "region": "мурманск" | "чили" | null,
    "caliber": "диапазон кг если указан" | null,
    "in_stock": true | false | null
  }},
  "raw_tokens": ["все", "значимые", "слова", "для", "скоринга"]
// in_stock=true если пользователь спрашивает "что есть", "в наличии", "на складе", "есть ли"
}}

Запрос: "{query}"

Отвечай ТОЛЬКО валидным JSON."""

    try:
        response = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        text = re.sub(r'^```json|^```|```$', '', text, flags=re.MULTILINE).strip()
        result = json.loads(text)
        logger.info(f"parse_product_query: '{query}' → {result}")
        return result
    except Exception as e:
        logger.error(f"parse_product_query error: {e}")
        # Fallback — вернуть запрос как есть
        return {"search_term": query, "filters": {}, "raw_tokens": query.lower().split()}


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
        system = f"""Ты — Эф, корпоративный ассистент компании F2B PRO (оптовая торговля рыбой и морепродуктами).
{EMPLOYEES_CONTEXT}
Контекст системы: {context_data}

Твои возможности:
- Задачи и дедлайны команды
- Цены и остатки товаров через МойСклад
- Фото товаров из карточек МойСклад
- Дебиторская задолженность
- Контакты

Отвечай кратко, по делу, на русском языке. Максимум 4-5 предложений если не просят подробнее.
Если вопрос про рыбу/морепродукты — отвечай профессионально как эксперт отрасли.
Никогда не говори что у тебя нет доступа к фото — фото есть в МойСклад."""

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
