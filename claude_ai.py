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
СОСТАВ ГРУППЫ F2B PRO (только эти люди пишут в группу):
- Васильев Виктор Алексеевич — руководитель
- Маланчук Александр Владимирович — руководитель
- Леонтьев Алексей Вадимович — менеджер по продажам (единственный Алексей в группе)
- Мерзлякова Елена Владимировна — менеджер по продажам
- Иванов Андрей Валерьевич — менеджер по закупкам (единственный Андрей в группе)
- Скляр Инесса Ионасовна — менеджер по продажам
- Белякова Александра Александровна — менеджер по закупкам (единственная Александра/Саша в группе)
- Баласанян Карина Владимировна — менеджер по продажам
- Голубева Татьяна — менеджер по продажам

ВАЖНО при постановке задач:
- "Алексей" → всегда Леонтьев Алексей Вадимович
- "Андрей" → всегда Иванов Андрей Валерьевич
- "Саша" / "Александра" → всегда Белякова Александра Александровна
- "Карина" → Баласанян Карина Владимировна
- "Инесса" → Скляр Инесса Ионасовна
- "Елена" / "Лена" → Мерзлякова Елена Владимировна
- "Татьяна" / "Таня" → Голубева Татьяна
- "Виктор" → Васильев Виктор Алексеевич
- "Александр" / "Саня" (если руководитель) → Маланчук Александр Владимирович
Сотрудники компании АО "ФИШ ТУ БИЗНЕС" (F2B):
- Баласанян Карина Владимировна — менеджер по продажам — Карина, Баласанян
- Белякова Александра Александровна — менеджер по закупкам — Александра, Белякова, Саша
- Васильев Виктор Алексеевич — руководитель — Виктор, Васильев
- Велиев Исрафил Джабраил Оглы — водитель
- Гашимзаде Магаммед Аладдин Оглы — водитель
- Гераскина Юлия Игоревна — оператор — Юля, Гераскина
- Голубева Татьяна — менеджер по продажам — Татьяна, Голубева
- Дубинин Алексей Владимирович — руководитель склада — Алексей (склад), Дубинин
- Иванов Андрей Валерьевич — менеджер по закупкам — Андрей, Иванов
- Кормилицын Антон Александрович — технолог — Антон, Кормилицын
- Куревлева Екатерина Игоревна — бухгалтер — Катя, Куревлева
- Леонтьев Алексей Вадимович — менеджер по продажам — Алексей (продажи), Леонтьев, Лёша
- Маланчук Александр Владимирович — руководитель — Александр, Маланчук
- Малышкин Андрей Анатольевич — главный бухгалтер — Андрей (бухгалтерия), Малышкин
- Мерзлякова Елена Владимировна — менеджер по продажам — Елена, Лена, Мерзлякова
- Петровский Владимир Николаевич — кладовщик — Владимир, Петровский
- Садыгов Самир Мелик Оглы — водитель
- Сайгашкина Оксана Юрьевна — бухгалтер — Оксана, Сайгашкина
- Скляр Инесса Ионасовна — менеджер по продажам — Инесса, Скляр

Менеджеры по продажам (всего 5): Баласанян Карина, Мерзлякова Елена, Скляр Инесса, Голубева Татьяна, Леонтьев Алексей.

Расписание доставки по Московской области:
- Понедельник: Звенигород, Истра, Солнечногорск
- Вторник: Королёв, Мытищи, Одинцово, Подольск, Серпухов, Чехов, Щелково
- Среда: Домодедово, Королёв, Мытищи, Орехово-Зуево, Павловский Посад, Сергиев Посад, Щелково, Красноармейск, Пушкино
- Четверг: Апрелевка, Королёв, Мытищи, Наро-Фоминск, Щелково
- Пятница: Егорьевск, Воскресенск, Королёв, Мытищи, Щелково, Каширское шоссе
"""

# ─── Главный промпт-диспетчер ─────────────────────────────────────────────────
DISPATCHER_PROMPT = f"""Ты — умный диспетчер корпоративного бота компании АО "ФИШ ТУ БИЗНЕС" (оптовая торговля рыбой и морепродуктами). Сокращённое название — F2B, PRO — это название телеграм-группы продажников.

{EMPLOYEES_CONTEXT}

Продукция компании: лосось, форель, тунец, креветки, угорь, кальмар, треска, минтай, семга, нерка, кета и другие морепродукты.

ПСЕВДОНИМЫ КЛИЕНТОВ — всегда разворачивай перед поиском:
- "Джи" → "Джи Эф Си" (JFC)
- "ГФ" → "Глобал Фудс"

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

ПРАВИЛО №2 — IT-ВОПРОСЫ:
Если сообщение касается технических проблем с рабочими инструментами:
  телеграм, telegram — не отправляется, не загружается, не работает
  amoCRM, амо, CRM — не открывается, слетела интеграция, нет сообщений
  звонки, телефония, связь — не проходят звонки, нет сигнала, не дозвониться
  почта, email — не отправляется, не приходит
→ Используй action: "answer" и в ответе напиши:
"По техническим вопросам (Telegram, amoCRM, звонки, почта) пишите в группу **IT8 & ОП ФИШ ТУ БИЗНЕС** 🛠"

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
- find_buyers: найти покупателей которые покупали определённый товар за период (параметры: product — название товара, period_days — за сколько дней, по умолчанию 30)
- broadcast: разослать сообщение клиентам которые покупали определённый товар (параметры: product — название товара, message — текст сообщения, period_days — за сколько дней анализировать заказы, по умолчанию 180)
- get_delivery_days: узнать на какой день можно поставить заказ для определённого адреса или города (параметр: address — адрес или название города)
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

Запрос: "кто в феврале покупал кета филе х/к?"
Ответ: {{"action": "find_buyers", "params": {{"product": "кета филе х/к", "period_days": 28}}, "confidence": 0.99}}

Запрос: "кто покупал дорадо за последний месяц?"
Ответ: {{"action": "find_buyers", "params": {{"product": "дорадо", "period_days": 30}}, "confidence": 0.99}}

Запрос: "покажи клиентов которые брали форель в этом году"
Ответ: {{"action": "find_buyers", "params": {{"product": "форель", "period_days": 90}}, "confidence": 0.97}}


Ответ: {{"action": "broadcast", "params": {{"product": "дорадо", "message": "Дорадо 300-400 по спеццене 890 руб!", "period_days": 180}}, "confidence": 0.99}}

Запрос: "разошли всем кто брал форель за последний месяц: Форель доступна по цене 650 руб/кг"
Ответ: {{"action": "broadcast", "params": {{"product": "форель", "message": "Форель доступна по цене 650 руб/кг", "period_days": 30}}, "confidence": 0.99}}

Запрос: "разошли всем кто покупал семгу за год: Семга с/с — специальная цена!"
Ответ: {{"action": "broadcast", "params": {{"product": "семга", "message": "Семга с/с — специальная цена!", "period_days": 365}}, "confidence": 0.99}}


Ответ: {{"action": "ms_search", "params": {{"query": "тунец"}}, "confidence": 0.99}}

Запрос: "какая цена на форель?"
Ответ: {{"action": "ms_search", "params": {{"query": "форель"}}, "confidence": 0.97}}

Запрос: "кто должен нам деньги?"
Ответ: {{"action": "get_debtors", "params": {{}}, "confidence": 0.9}}

Запрос: "какой долг у Белуги?"
Ответ: {{"action": "get_debt", "params": {{"query": "Белуга"}}, "confidence": 0.98}}

Запрос: "какой долг у Честной рыбы?"
Ответ: {{"action": "get_debt", "params": {{"query": "Честная рыба"}}, "confidence": 0.98}}

Запрос: "долг у Честной рыбы"
Ответ: {{"action": "get_debt", "params": {{"query": "Честная рыба"}}, "confidence": 0.98}}

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

Запрос: "напиши сообщение о просрочке для Честной рыбы"
Ответ: {{"action": "prepare_reminders", "params": {{"query": "Честная рыба"}}, "confidence": 0.99}}

Запрос: "напиши напоминание об оплате для Гефест"
Ответ: {{"action": "prepare_reminders", "params": {{"query": "Гефест"}}, "confidence": 0.99}}

Запрос: "подготовь напоминалки по просрочке"
Ответ: {{"action": "prepare_reminders", "params": {{}}, "confidence": 0.99}}

Запрос: "напомни должникам об оплате"
Ответ: {{"action": "prepare_reminders", "params": {{}}, "confidence": 0.99}}

Отвечай ТОЛЬКО валидным JSON. Никаких пояснений, никакого markdown."""



async def detect_task_completion(text: str, open_tasks: list, author: str = "") -> list:
    """
    Анализирует сообщение чата — возвращает список {id, result} выполненных задач.
    author — имя сотрудника написавшего сообщение.
    """
    if not open_tasks or not text:
        return []

    tasks_str = "\n".join([
        f"ID={t['id']}: {t.get('executor','')} — {t.get('text','')}"
        for t in open_tasks[:20]
    ])

    author_note = f"\nСообщение написал(а): {author}" if author else ""

    prompt = f"""Ты анализируешь рабочий чат компании. Сообщение:
"{text}"{author_note}

Открытые задачи:
{tasks_str}

Задача считается выполненной если сообщение ПРЯМО или КОСВЕННО говорит о её завершении.
Примеры:
- "я позвонила Маркину, оплатит в пятницу" → закрывает задачу "позвонить Маркину"
- "Орехов заплатит завтра" → закрывает задачу "получить оплату от Орехова"
- "готово" / "сделала" / "отправила" → закрывает релевантную задачу

КРИТИЧЕСКИ ВАЖНО — закрывай задачу ТОЛЬКО если:
1. Исполнитель задачи совпадает с автором сообщения (или задача без исполнителя)
2. Сообщение говорит именно о ЗАВЕРШЕНИИ действия, не о частичном прогрессе
3. Связь между сообщением и задачей ПРЯМАЯ и ОЧЕВИДНАЯ — не через случайное совпадение слов

НЕ закрывать если:
- это новое поручение ("Саша, сделай X")
- вопрос ("как дела с задачей?")
- обсуждение без конкретного результата
- исполнитель задачи — другой человек, не автор сообщения
- сообщение упоминает похожее слово но не говорит о выполнении задачи ("приедет в понедельник" ≠ "определил день доставки")
- сообщение слишком короткое и неоднозначное ("ок", "понял", "в понедельник")

Поле result — краткое резюме итога (1 предложение от третьего лица).
Примеры result: "Позвонила, оплата ожидается в пятницу", "Отправлено КП", "Договорились на встречу 15.03"

Отвечай ТОЛЬКО JSON массивом объектов с полями id и result.
Пример: [{{"id": 3, "result": "Позвонила, оплатит в пятницу"}}, {{"id": 7, "result": "КП отправлено"}}] или []"""

    try:
        client = get_client()
        resp = await client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = resp.content[0].text.strip()
        import json as _json
        match = re.search(r'\[.*?\]', raw, re.DOTALL)
        if match:
            items = _json.loads(match.group())
            result = []
            for item in items:
                if isinstance(item, dict) and 'id' in item:
                    result.append({"id": int(item['id']), "result": item.get('result', '')})
                elif isinstance(item, (int, float)):
                    result.append({"id": int(item), "result": ""})
            return result
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
        system = f"""Ты — Эф, корпоративный ассистент компании АО "ФИШ ТУ БИЗНЕС" (оптовая торговля рыбой и морепродуктами). Сокращённое название компании — F2B, PRO — название телеграм-группы продажников, не часть названия компании.
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
    from datetime import date, timedelta
    today = date.today()
    tomorrow = today + timedelta(days=1)
    # Конец текущей недели (воскресенье)
    days_to_sunday = 6 - today.weekday()
    end_of_week = today + timedelta(days=days_to_sunday)
    # Конец следующей недели
    end_of_next_week = end_of_week + timedelta(days=7)

    try:
        prompt = f"""Ты анализируешь сообщение руководителя компании F2B PRO и извлекаешь из него задачи.

{EMPLOYEES_CONTEXT}

Сегодня: {today.strftime('%d.%m.%Y')} ({['понедельник','вторник','среда','четверг','пятница','суббота','воскресенье'][today.weekday()]})
Завтра: {tomorrow.strftime('%d.%m.%Y')}
Конец этой недели (воскресенье): {end_of_week.strftime('%d.%m.%Y')}
Конец следующей недели: {end_of_next_week.strftime('%d.%m.%Y')}

Сообщение от {author}:
\"\"\"{text}\"\"\"

Верни ТОЛЬКО JSON массив задач. Каждая задача:
{{
  "task": "краткое чёткое описание задачи",
  "executor": "полное имя исполнителя из списка сотрудников (если упомянут), иначе пустая строка",
  "deadline": "дата в формате YYYY-MM-DD если указана, иначе null"
}}

Перевод сроков в даты:
- "сегодня" → {today.isoformat()}
- "завтра" → {tomorrow.isoformat()}
- "до конца недели" / "до пятницы" → {(today + timedelta(days=4 - today.weekday() if today.weekday() <= 4 else 0)).isoformat()}
- "до конца следующей недели" → {end_of_next_week.isoformat()}
- "до конца месяца" → {today.replace(day=1).replace(month=today.month % 12 + 1) - timedelta(days=1) if today.month < 12 else today.replace(day=31)}
- конкретная дата "15 марта" → 2026-03-15

Правила:
- Извлекай только конкретные поручения, не общую информацию
- Если задача адресована нескольким людям — создай отдельную запись для каждого
- Если исполнитель не указан явно — попробуй определить по контексту
- Извлекай задачи даже если сообщение начинается с обращения к боту ("Эф, Андрей наполняй..." → задача для Андрея)
- НЕ создавай задачу если это вопрос боту без поручения сотруднику ("Эф, покажи остатки")
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


async def analyze_pdz_responses(results: dict) -> str:
    """
    Анализирует ответы менеджеров из переписки группы и формирует сводку по ПДЗ.

    results: {
        "Карина": {
            "items": [...клиенты с просрочкой...],
            "messages": ["Карина: ИП Орехов оплатит 12.03", ...]
        },
        ...
    }
    """
    try:
        sections = []
        for manager_name, data in results.items():
            items = data.get("items", [])
            messages = data.get("messages", [])

            if not items:
                continue

            clients = ", ".join(
                f"{c['name']} ({c['overdue_sum']:,.0f} руб.)".replace(",", " ")
                for c in items
            )
            msgs_text = "\n".join(messages) if messages else "(нет ответов)"
            sections.append(
                f"Менеджер: {manager_name}\n"
                f"Клиенты с ПДЗ: {clients}\n"
                f"Сообщения в группе:\n{msgs_text}"
            )

        if not sections:
            return "Сегодня активных клиентов с ПДЗ не найдено."

        prompt = f"""Ты анализируешь переписку менеджеров по работе с просроченной дебиторской задолженностью.

По каждому менеджеру дан список клиентов с ПДЗ и сообщения менеджера из группы за сегодня.

Составь сводку в формате:

*Карина:*
• ИП Орехов — оплатит 16.03
• ООО Атмосфера — недозвонились
• ИП Иванов — нет ответа от менеджера

*Елена:*
• ...

Правила:
- Если в сообщениях есть упоминание клиента и дата/обещание — пиши "оплатит ДД.ММ"
- Если менеджер написал что не дозвонился — "недозвонились"
- Если по клиенту нет ни одного упоминания — "нет ответа от менеджера"
- Используй только данные из сообщений, не придумывай
- В конце добавь итоговую строку: всего клиентов, получен ответ по X, нет ответа по Y

Данные:

{chr(10).join(sections)}

Только сводка, без вступления."""

        response = await get_client().messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text

    except Exception as e:
        logger.error(f"analyze_pdz_responses error: {e}", exc_info=True)
        # Fallback — простая сводка без AI
        lines = []
        for manager_name, data in results.items():
            if not data.get("items"):
                continue
            lines.append(f"*{manager_name}:*")
            for c in data["items"]:
                lines.append(f"• {c['name']} — нет ответа от менеджера")
        return "\n".join(lines) if lines else "Нет данных."
