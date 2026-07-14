"""Обезличивание клиентских переписок перед отправкой в Claude (152-ФЗ).

Ф1 плана `2026-04-29-анализ-переписок-фундамент.md` (реализация 2026-07-14).

Задача: убрать из текста всё, что позволяет реидентифицировать клиента через
amoCRM/МойСклад — ФИО, телефоны, email, ИНН, URL, конкретные тоннажи, суммы и
даты. Видовое название товара («судак», «лосось») СОХРАНЯЕМ — оно нужно для
пользы дайджеста и не является ПДн, а размерность/объём/цена маскируются.

Порядок важен: сначала «жадные» цифровые сущности (телефон), потом узкие
(ИНН/сумма/объём/дата), потом имена, иначе телефон разобьётся масками объёма.

Модуль самодостаточный: без импортов БД/телеграма, чтобы гоняться в юнит-тестах.
"""
from __future__ import annotations

import re
from typing import Iterable

# ── Полные имена менеджеров ОП (для маскировки в тексте) ──────────────────────
# Синхронизировать при смене состава ОП (memory `reference_f2b_op_managers_5`).
_MANAGER_FULL_NAMES = [
    "Баласанян Карина", "Карина Баласанян",
    "Дьяченко Ирина", "Ирина Дьяченко",
    "Мерзлякова Елена", "Елена Мерзлякова",
    "Скляр Инесса", "Инесса Скляр",
    "Коликов Денис", "Денис Коликов",
]

# ── Регэкспы сущностей ────────────────────────────────────────────────────────
_RE_URL = re.compile(r"https?://\S+|\bwww\.\S+", re.IGNORECASE)
_RE_EMAIL = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b", re.IGNORECASE)

# Телефон РФ: +7/8/7 + 10 цифр в любой разбивке скобками/дефисами/пробелами.
_RE_PHONE = re.compile(
    r"(?:\+7|\b8|\b7)[\s\-(]*\d{3}[\s\-)]*\d{3}[\s\-]*\d{2}[\s\-]*\d{2}\b"
)

# ИНН: отдельно стоящие 10 или 12 цифр (после того как телефоны уже съедены).
_RE_INN = re.compile(r"(?<!\d)(?:\d{10}|\d{12})(?!\d)")

# Сумма: число (с разделителями/дробной частью) + валюта.
_RE_AMOUNT = re.compile(
    r"\d[\d\s.,]*\s*(?:₽|руб(?:лей|ля|\.)?|р\.)",
    re.IGNORECASE,
)

# Объём/количество: число + единица (кг, т, тонн, пласт, шт, короб, паллет).
_RE_VOLUME = re.compile(
    r"\d[\d\s.,]*(?:[\-–]\d[\d\s.,]*)?\s*"
    r"(?:кг|килограмм\w*|тонн\w*|\bт\b|пласт\w*|шт\b|штук\w*|короб\w*|паллет\w*|мешк\w*)",
    re.IGNORECASE,
)

# Дата: дд.мм(.гггг) / дд/мм / дд число + месяц словом.
_RE_DATE_NUM = re.compile(r"\b\d{1,2}[.\/]\d{1,2}(?:[.\/]\d{2,4})?\b")
_RE_DATE_WORD = re.compile(
    r"\b\d{1,2}\s*(?:январ|феврал|март|апрел|ма[йя]|июн|июл|"
    r"август|сентябр|октябр|ноябр|декабр)\w*",
    re.IGNORECASE,
)

# Токены ПДн, наличие которых в результате = утечка (для проверки в тестах).
_LEAK_PATTERNS = {
    "email": _RE_EMAIL,
    "phone": _RE_PHONE,
    "inn": _RE_INN,
    "url": _RE_URL,
}


def _name_variants(full_name: str) -> list[str]:
    """«Инна Ухват» → ['Инна Ухват', 'Ухват Инна', 'Инна', 'Ухват'].

    Части короче 3 символов отбрасываем (инициалы/предлоги), чтобы не выкосить
    случайные буквы из текста.
    """
    parts = [p for p in re.split(r"[\s,]+", full_name.strip()) if len(p) >= 3]
    if not parts:
        return []
    variants = {full_name.strip()}
    if len(parts) >= 2:
        variants.add(" ".join(parts))
        variants.add(" ".join(reversed(parts)))
    variants.update(parts)
    # длинные варианты вперёд, чтобы «Инна Ухват» заменилось раньше «Инна»
    return sorted(variants, key=len, reverse=True)


def _mask_names(text: str, names: Iterable[str], placeholder: str) -> str:
    for full in names:
        for variant in _name_variants(full):
            text = re.sub(
                r"(?<!\w)" + re.escape(variant) + r"(?!\w)",
                placeholder,
                text,
                flags=re.IGNORECASE,
            )
    return text


def anonymize(
    text: str,
    contact_name: str | None = None,
    manager_name: str | None = None,
    company_name: str | None = None,
    extra_names: Iterable[str] | None = None,
) -> str:
    """Вернуть обезличенную версию `text`.

    Известные из БД сущности (`contact_name`, `manager_name`, `company_name`)
    передаём явно — они точнее любого NER. `extra_names` — дополнительные имена
    (например, другие контакты того же чата).
    """
    if not text:
        return text or ""

    out = text
    # 1) жадные структурные сущности
    out = _RE_URL.sub("[URL]", out)
    out = _RE_EMAIL.sub("[EMAIL]", out)
    out = _RE_PHONE.sub("[PHONE]", out)
    out = _RE_INN.sub("[INN]", out)
    # 2) деньги / объём / дата (число+хвост) — до имён, до generic
    out = _RE_AMOUNT.sub("[AMOUNT]", out)
    out = _RE_VOLUME.sub("[VOLUME]", out)
    out = _RE_DATE_NUM.sub("[DATE]", out)
    out = _RE_DATE_WORD.sub("[DATE]", out)
    # 3) имена: сначала известные из БД, потом весь состав ОП
    if company_name:
        out = _mask_names(out, [company_name], "[COMPANY]")
    if contact_name:
        out = _mask_names(out, [contact_name], "[CLIENT]")
    manager_names = list(_MANAGER_FULL_NAMES)
    if manager_name:
        manager_names.insert(0, manager_name)
    out = _mask_names(out, manager_names, "[MANAGER]")
    if extra_names:
        out = _mask_names(out, list(extra_names), "[CLIENT]")
    return out


def find_leaks(text: str) -> dict[str, list[str]]:
    """Найти в тексте остаточные ПДн-паттерны (email/phone/inn/url).

    Используется в тестах и как самопроверка: пустой словарь = чисто.
    Имена не проверяем здесь (их наличие зависит от переданных словарей).
    """
    leaks: dict[str, list[str]] = {}
    for name, pat in _LEAK_PATTERNS.items():
        hits = pat.findall(text)
        if hits:
            leaks[name] = hits
    return leaks
