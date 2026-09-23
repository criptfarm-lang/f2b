"""Юнит-тесты агента-продавца (план 2026-09-23).

Гоняется: `python3 -m pytest test_sales_dialog.py -q` из ~/code/f2b.
"""
import asyncio
from datetime import date, datetime, timedelta, timezone

import sales_dialog
from sales_dialog import check_prices, delivery_target, in_window, mrm_price, polish, workdays_ago

MSK = timezone(timedelta(hours=3))
PRICES = {
    "14001": {"code": "14001", "opt": 1050.0, "horeca": 1100.0, "spec": 950.0, "stock": 808.2},
    "40139": {"code": "40139", "opt": 780.0, "horeca": 780.0, "spec": None, "stock": 50.0},
}


def test_price_ok():
    draft = {"text": "масляная филе – 950 ₽/кг", "price_claims": [
        {"code": "14001", "name": "Масляная", "price": 950.0, "price_type": "spec"}]}
    assert check_prices(draft, PRICES) == []


def test_price_mismatch_caught():
    draft = {"text": "масляная филе – 890 ₽/кг", "price_claims": [
        {"code": "14001", "name": "Масляная", "price": 890.0, "price_type": "spec"}]}
    problems = check_prices(draft, PRICES)
    assert problems and "950" in problems[0]


def test_unknown_code_caught():
    draft = {"text": "тилапия – 500 ₽/кг", "price_claims": [
        {"code": "99999", "name": "Тилапия", "price": 500.0, "price_type": "opt"}]}
    assert "99999" in check_prices(draft, PRICES)[0]


def test_missing_price_type_caught():
    """У позиции нет спеццены, а агент её назвал."""
    draft = {"text": "судак – 780 ₽/кг", "price_claims": [
        {"code": "40139", "name": "Судак", "price": 780.0, "price_type": "spec"}]}
    assert check_prices(draft, PRICES)


def test_undeclared_number_in_text():
    """Число в тексте, которого нет в price_claims, — тоже проблема."""
    draft = {"text": "масляная 950 ₽/кг, а тунец 1 200 ₽/кг", "price_claims": [
        {"code": "14001", "name": "Масляная", "price": 950.0, "price_type": "spec"}]}
    problems = check_prices(draft, PRICES)
    assert any("1 200" in p or "1200" in p for p in problems), problems


def test_nonbreaking_space_number_is_seen():
    """Цена с неразрывным пробелом тоже должна ловиться."""
    draft = {"text": "тунец 1 200 ₽/кг", "price_claims": []}
    assert check_prices(draft, PRICES)


def test_window_workday():
    assert in_window(datetime(2026, 9, 23, 10, 0, tzinfo=MSK), {})
    assert not in_window(datetime(2026, 9, 23, 8, 0, tzinfo=MSK), {})
    assert not in_window(datetime(2026, 9, 23, 19, 0, tzinfo=MSK), {})


def test_window_weekend_closed_by_default():
    assert not in_window(datetime(2026, 9, 26, 12, 0, tzinfo=MSK), {})   # суббота
    assert in_window(datetime(2026, 9, 26, 12, 0, tzinfo=MSK), {"weekends": True})


def test_window_custom_hours():
    cfg = {"start_hour": 7, "end_hour": 12}
    assert in_window(datetime(2026, 9, 23, 7, 30, tzinfo=MSK), cfg)
    assert not in_window(datetime(2026, 9, 23, 12, 30, tzinfo=MSK), cfg)


# ── стилевые запреты ──────────────────────────────────────────────────────────
def test_workdays_ago_skips_weekend():
    # Пятница 25.09.2026 минус два рабочих дня — среда 23-го, а не воскресенье.
    fri = datetime(2026, 9, 25, 16, 0, tzinfo=MSK)
    assert workdays_ago(fri, 2).date() == date(2026, 9, 23)


def test_workdays_ago_over_weekend():
    # Понедельник минус два рабочих дня — четверг прошлой недели.
    mon = datetime(2026, 9, 28, 10, 0, tzinfo=MSK)
    assert workdays_ago(mon, 2).date() == date(2026, 9, 24)


def test_workdays_ago_zero():
    now = datetime(2026, 9, 23, 12, 0, tzinfo=MSK)
    assert workdays_ago(now, 0) == now


def test_style_masculine_caught():
    """Инесса — женщина; мужской род выдаёт, что пишет не она."""
    from sales_dialog import check_style
    assert check_style("Понял, торопить не буду")
    assert check_style("Прошел по вашему списку")
    assert not check_style("Поняла, торопить не буду")
    assert not check_style("Прошла по вашему списку, цены ₽/кг")


def test_style_calls_caught():
    from sales_dialog import check_style
    for bad in ["Давайте созвонимся", "Наберу вас завтра", "Позвоните мне", "Перезвоним в течение дня"]:
        assert check_style(bad), bad
    assert not check_style("Напишите, какой объём нужен – посчитаю")


def test_style_clean_text():
    from sales_dialog import check_style
    assert check_style("Охлаждёнку делаем под заказ: заказ сегодня – привезём завтра.") == []


# ── определение города для доставки ───────────────────────────────────────────
def test_find_city_basic():
    from sales_dialog import find_city
    assert find_city("Добрый день, мы находимся в Омске") == "Омск"
    assert find_city("поставки в Екатеринбург, потребность 700 кг") == "Екатеринбург"
    assert find_city("доставка в Нижний Новгород возможна?") == "Нижний Новгород"


def test_find_city_none_when_unknown():
    from sales_dialog import find_city
    assert find_city("мы не в МСК находимся") is None
    assert find_city("здравствуйте, нужна рыба") is None
    assert find_city("") is None


def test_delivery_note_without_city_forbids_numbers():
    """Город неизвестен — агенту прямо сказано не называть цену доставки."""
    from sales_dialog import delivery_note
    note = delivery_note(None)
    assert "спроси город" in note
    assert "7 000" in note          # порог по Москве остаётся


def test_delivery_note_with_city_has_tariff():
    from sales_dialog import delivery_note
    note = delivery_note("Омск")
    assert "Омск" in note and "паллета" in note


def test_delivery_note_two_modes():
    """У дальних городов заморозка и охлаждение стоят по-разному."""
    from sales_dialog import delivery_note
    note = delivery_note("Екатеринбург")
    assert "зам" in note and "охл" in note


def test_delivery_threshold_not_flagged():
    """7 000 ₽ — порог бесплатной доставки, а не цена товара: тревоги быть не должно."""
    from sales_dialog import allowed_numbers, check_prices
    draft = {"text": "По Москве и МО бесплатно от 7 000 ₽.", "price_claims": []}
    assert check_prices(draft, PRICES, allowed_numbers(None)) == []


def test_delivery_tariff_not_flagged():
    from sales_dialog import allowed_numbers, check_prices
    draft = {"text": "Доставка до Омска паллетой – 30036 ₽.", "price_claims": []}
    assert check_prices(draft, PRICES, allowed_numbers("Омск")) == []


def test_foreign_number_still_flagged():
    """Чужой тариф всё равно ловится: Омск разрешён, а питерский тариф — нет."""
    from sales_dialog import allowed_numbers, check_prices
    draft = {"text": "Доставка – 10950 ₽.", "price_claims": []}
    assert check_prices(draft, PRICES, allowed_numbers("Омск"))


def test_rounded_delivery_tariff_allowed():
    """«Около 14 400 ₽» при тарифе 14 417 — нормальная человеческая речь."""
    from sales_dialog import allowed_numbers, check_prices
    draft = {"text": "до 100 кг – около 14 400 ₽, паллета – около 30 000 ₽", "price_claims": []}
    assert check_prices(draft, PRICES, allowed_numbers("Омск")) == []


def test_product_price_rounding_still_flagged():
    """А вот цену товара округлять нельзя: 950 названо как 900."""
    from sales_dialog import check_prices
    draft = {"text": "масляная – 900 ₽/кг", "price_claims": [
        {"code": "14001", "name": "Масляная", "price": 900.0, "price_type": "spec"}]}
    assert check_prices(draft, PRICES)


# ── карточка подтверждения и отправка ─────────────────────────────────────────
class FakeDB:
    """Заглушка БД: отдаёт заранее заданные строки, запоминает UPDATE-и."""

    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def _fetchone(self, sql, params=None):
        if "sales_dialog_leads" in sql:
            return self.rows.get("lead")
        if "wazzup_messages" in sql:
            return self.rows.get("fresh")
        return self.rows.get("msg")

    def _fetchall(self, sql, params=None):
        return []

    def _execute(self, sql, params=None):
        self.executed.append((sql, params))


def _msg(**over):
    from datetime import datetime, timezone
    base = {"id": 7, "campaign": "c", "lead_id": 44721919, "inbound_text": "а треска есть?",
            "draft_text": "Треска лойн – 1630 ₽/кг, есть.", "action": "reply", "reason": "",
            "verdict": "draft", "created_at": datetime.now(timezone.utc)}
    base.update(over)
    return base


def test_card_shows_client_question_and_draft():
    from sales_dialog import _card_text
    db = FakeDB({"lead": {"lead_name": "ИП Чебан", "chat_type": "max"}, "msg": _msg()})
    card = _card_text(db, _msg())
    assert "ИП Чебан" in card and "44721919" in card
    assert "а треска есть?" in card
    assert "Треска лойн" in card


def test_card_for_silent_dialog():
    """У оживления затихшего диалога входящего сообщения нет."""
    from sales_dialog import _card_text
    db = FakeDB({"lead": {"lead_name": "ИП Волков", "chat_type": "max"}, "msg": _msg()})
    card = _card_text(db, _msg(inbound_text=None))
    assert "Диалог затих" in card


def test_escalated_card_has_no_send_button():
    """Если агент сам не берётся отвечать — кнопки «Отправить» быть не должно."""
    from sales_dialog import _keyboard
    kb = _keyboard(7, "escalate")
    labels = [b.text for row in kb.inline_keyboard for b in row]
    assert "Отправить" not in labels
    assert "Передать Инессе" in labels


def test_normal_card_has_send_button():
    from sales_dialog import _keyboard
    labels = [b.text for row in _keyboard(7, "reply").inline_keyboard for b in row]
    assert labels[0] == "Отправить"


def test_mrm_price_minus_100():
    assert mrm_price("Лосось (сёмга) филе, ОХЛ., Трим Д 1.6-2.0 кг. Мурманск", 2750.0) == 2650.0
    assert mrm_price("Лосось атл. (сёмга), ПСГ, ОХЛ., 4-5 кг. Мурманск", 1690.0) == 1590.0


def test_mrm_discount_only_for_chilled_murmansk():
    # заморозка из Мурманска и охлаждёнка не из Мурманска идут по цене МойСклада
    assert mrm_price("Лосось (сёмга) филе, ЗАМОРОЖ., Трим Д 1.6-2.0 кг. Мурманск", 2750.0) == 2750.0
    assert mrm_price("Лосось (сёмга) филе, ОХЛ., Трим Д 1.6-2.0 кг.", 2290.0) == 2290.0
    assert mrm_price("Форель филе, ОХЛ., Трим С 1.4-2.0 кг.", 2020.0) == 2020.0


def test_mrm_price_handles_missing():
    assert mrm_price("Лосось филе, ОХЛ. Мурманск", None) is None


def test_polish_drops_introduction():
    t = ("Добрый день. Меня зовут Инесса, компания F2B – теперь я веду ваш вопрос.\n\n"
         "По форели Трим ПР: 1420 ₽/кг.")
    out = polish(t)
    assert "Меня зовут" not in out
    assert out.startswith("Добрый день.")
    assert "1420" in out


def test_polish_keeps_normal_text():
    t = "Добрый день. По треске цена 620 ₽/кг, привезём завтра."
    assert polish(t) == t


def test_polish_renames_razves_keeping_case():
    assert polish("Развес 1.3–1.5 кг") == "Размер 1.3–1.5 кг"
    assert polish("два развеса: 0.6–1.0 и 1.3–1.5") == "два размера: 0.6–1.0 и 1.3–1.5"
    assert polish("по развесу подберём") == "по размеру подберём"


def test_delivery_goes_to_chat_of_the_draft():
    # Клиент написал во второй мессенджер — отвечаем туда, а не в основной чат.
    msg = {"chat_id": "672974160", "chat_type": "telegram"}
    lead = {"chat_id": "18973580", "chat_type": "max"}
    assert delivery_target(msg, lead) == ("672974160", "telegram")


def test_delivery_falls_back_to_lead_card():
    # Старый черновик без канала — берём чат карточки лида.
    assert delivery_target({"chat_id": None, "chat_type": None},
                           {"chat_id": "18973580", "chat_type": "max"}) == ("18973580", "max")


def test_callback_data_fits_telegram_limit():
    """Telegram режет callback_data длиннее 64 байт."""
    from sales_dialog import _CB, _keyboard
    for row in _keyboard(999999999, "reply").inline_keyboard:
        for b in row:
            assert len(b.callback_data.encode()) <= 64
            assert _CB.match(b.callback_data)


def test_draft_not_sent_if_client_replied_meanwhile():
    """Клиент ответил после черновика – отправка блокируется, черновик stale."""
    db = FakeDB({"msg": _msg(), "lead": {"all_chat_ids": ["1"], "chat_id": "1"},
                 "fresh": {"text": "Только Мурманск", "sent_at": None}})
    ok, info = asyncio.run(sales_dialog._do_send(db, 7, "текст"))
    assert ok is False
    assert "клиент ответил" in info.lower() and "Мурманск" in info
    assert any("verdict='stale'" in sql for sql, _ in db.executed)


def test_draft_sent_when_no_new_inbound():
    """Нового входящего нет – отправка идёт как обычно."""
    db = FakeDB({"msg": _msg(), "lead": {"all_chat_ids": ["1"], "chat_id": "1"}, "fresh": None})
    sent = {}

    async def fake_deliver(db_, session, msg, text):
        sent["text"] = text
        return True, "ok"

    orig = sales_dialog._deliver
    sales_dialog._deliver = fake_deliver
    try:
        ok, _ = asyncio.run(sales_dialog._do_send(db, 7, "текст"))
    finally:
        sales_dialog._deliver = orig
    assert ok is True and sent["text"] == "текст"


def test_expired_draft_is_not_sent():
    """Черновик старше TTL клиенту не уходит."""
    import asyncio
    from datetime import datetime, timedelta, timezone
    from sales_dialog import APPROVAL_TTL_MIN, _do_send
    old = _msg(created_at=datetime.now(timezone.utc) - timedelta(minutes=APPROVAL_TTL_MIN + 5))
    db = FakeDB({"msg": old})
    ok, info = asyncio.run(_do_send(db, 7, "текст"))
    assert not ok and "устарел" in info
    assert any("expired" in sql for sql, _ in db.executed)


def test_already_handled_draft_is_not_sent_twice():
    import asyncio
    from sales_dialog import _do_send
    db = FakeDB({"msg": _msg(verdict="sent")})
    ok, info = asyncio.run(_do_send(db, 7, "текст"))
    assert not ok and "уже обработан" in info


# ── разбор ответа модели (SDK 0.40.0 без structured outputs) ─────────────────
def test_parse_plain_json():
    from sales_dialog import parse_draft
    assert parse_draft('{"action":"reply","text":"привет"}')["text"] == "привет"


def test_parse_markdown_wrapped():
    from sales_dialog import parse_draft
    assert parse_draft('```json\n{"action":"reply","text":"привет"}\n```')["action"] == "reply"


def test_parse_with_surrounding_chatter():
    from sales_dialog import parse_draft
    assert parse_draft('Вот ответ: {"action":"reply","text":"привет"} — готово')["text"] == "привет"


def test_parse_garbage_returns_none():
    from sales_dialog import parse_draft
    assert parse_draft("совсем не json") is None
    assert parse_draft("") is None


def test_answer_text_skips_thinking_block():
    """У моделей с размышлением content[0] — блок thinking, текст идёт вторым."""
    from sales_dialog import answer_text
    data = {"content": [{"type": "thinking", "thinking": "..."},
                        {"type": "text", "text": '{"action":"reply","text":"ок"}'}]}
    assert answer_text(data) == '{"action":"reply","text":"ок"}'


def test_answer_text_empty_content():
    from sales_dialog import answer_text
    assert answer_text({"content": []}) == ""
    assert answer_text({}) == ""
