"""Юнит-тесты анонимайзера переписок (Ф1 плана анализа переписок).

Гоняется: `python3 -m pytest test_chat_anonymizer.py -q` из ~/code/f2b.
Цель — 0 утечек ПДн (email/phone/inn/url) + корректная маскировка объёма/
суммы/даты/имён при сохранении видового названия товара.
"""
from chat_anonymizer import anonymize, find_leaks


# ── Структурные сущности ──────────────────────────────────────────────────────
def test_email_masked():
    r = anonymize("пишите на ivan.petrov@globalfoods.ru срочно")
    assert "[EMAIL]" in r
    assert not find_leaks(r)


def test_phone_formats():
    for raw in [
        "+7 (999) 123-45-67",
        "8 999 123 45 67",
        "89991234567",
        "тел: +79991234567,",
    ]:
        r = anonymize(raw)
        assert "[PHONE]" in r, raw
        assert not find_leaks(r), (raw, r)


def test_inn_masked():
    r = anonymize("ИНН 7701234567 и ещё 770112345678")
    assert r.count("[INN]") == 2
    assert not find_leaks(r)


def test_url_masked():
    r = anonymize("прайс тут https://f2b.group/price?id=5 смотрите")
    assert "[URL]" in r
    assert not find_leaks(r)


# ── Деньги / объём / дата ─────────────────────────────────────────────────────
def test_amount_masked():
    for raw in ["по 1200 руб", "1 200 000 ₽", "стоит 1200р.", "450 рублей"]:
        r = anonymize(raw)
        assert "[AMOUNT]" in r, raw


def test_volume_masked():
    for raw in ["нужно 100 кг", "2 тонны", "10 пластов", "5 коробов", "1.6-2.0 кг"]:
        r = anonymize(raw)
        assert "[VOLUME]" in r, raw


def test_date_masked():
    for raw in ["к 15.05", "до 3/06/2026", "15 мая привезите", "20 июня"]:
        r = anonymize(raw)
        assert "[DATE]" in r, raw


# ── Имена ─────────────────────────────────────────────────────────────────────
def test_contact_and_manager_names():
    r = anonymize(
        "Инна Ухват спросила, ответила Мерзлякова Елена",
        contact_name="Инна Ухват",
    )
    assert "[CLIENT]" in r
    assert "[MANAGER]" in r
    assert "Ухват" not in r
    assert "Мерзлякова" not in r


def test_company_masked():
    r = anonymize(
        'заказ от ООО "Глобал Фудс" готов',
        company_name='ООО "Глобал Фудс"',
    )
    assert "[COMPANY]" in r
    assert "Глобал" not in r


def test_species_preserved():
    # видовое название товара НЕ должно теряться — оно нужно дайджесту
    r = anonymize("нужен судак 100 кг срочно")
    assert "судак" in r.lower()
    assert "[VOLUME]" in r


# ── Реалистичные комбинированные сообщения ────────────────────────────────────
def test_realistic_combined_no_leaks():
    msg = (
        "Добрый день! Это Инна Ухват, ООО \"РыбаОпт\", ИНН 7701234567. "
        "Нужно 460 кг креветки 5/6 супер к 15.05, бюджет до 1 200 000 ₽. "
        "Мой номер +7 (999) 123-45-67, почта inna@ryba.ru"
    )
    r = anonymize(msg, contact_name="Инна Ухват", company_name='ООО "РыбаОпт"')
    assert not find_leaks(r), r
    # ключевое: вид товара сохранён, ПДн вычищены
    assert "креветк" in r.lower()
    assert "Ухват" not in r
    assert "7701234567" not in r


def test_empty_and_none():
    assert anonymize("") == ""
    assert anonymize(None) == ""


def test_idempotent_on_clean_text():
    clean = "Спасибо, всё устроило, будем брать ещё"
    assert anonymize(clean) == clean
