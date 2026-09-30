"""Юнит-тесты агента-продавца (план 2026-09-23).

Гоняется: `python3 -m pytest test_sales_dialog.py -q` из ~/code/f2b.
"""
import asyncio
from datetime import date, datetime, timedelta, timezone

import sales_dialog
from sales_dialog import (check_prices, delivery_target, in_window, lead_brief, mrm_price,
                          polish, strip_card_header, workdays_ago)

MSK = timezone(timedelta(hours=3))
PRICES = {
    "14001": {"code": "14001", "opt": 1050.0, "horeca": 1100.0, "spec": 950.0, "stock": 808.2},
    "40139": {"code": "40139", "opt": 780.0, "horeca": 780.0, "spec": None, "stock": 50.0},
}


def test_price_ok():
    draft = {"text": "масляная филе – 950 ₽/кг", "price_claims": [
        {"code": "14001", "name": "Масляная", "price": 950.0, "price_type": "spec"}]}
    assert check_prices(draft, PRICES) == []


def test_price_above_list_caught():
    """Выше прайса — ошибка: такую цену клиенту называть нельзя."""
    draft = {"text": "масляная филе – 1010 ₽/кг", "price_claims": [
        {"code": "14001", "name": "Масляная", "price": 1010.0, "price_type": "spec"}]}
    problems = check_prices(draft, PRICES)
    assert problems and "950" in problems[0]


def test_price_below_list_is_bargain_not_error():
    """Ниже прайса — это торг: сверка его не заворачивает, решает порог из дашборда."""
    draft = {"text": "масляная филе – 890 ₽/кг", "price_claims": [
        {"code": "14001", "name": "Масляная", "price": 890.0, "price_type": "spec"}]}
    assert check_prices(draft, PRICES) == []
    assert draft["bargain"] == [{"code": "14001", "price": 890.0,
                                 "list_price": 950.0, "price_type": "spec"}]


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


def test_product_price_rounding_up_still_flagged():
    """Округлить цену товара ВВЕРХ нельзя: 950 названо как 990."""
    from sales_dialog import check_prices
    draft = {"text": "масляная – 990 ₽/кг", "price_claims": [
        {"code": "14001", "name": "Масляная", "price": 990.0, "price_type": "spec"}]}
    assert check_prices(draft, PRICES)


def test_bargain_below_floor_blocked(monkeypatch):
    """Уступка ниже порога ценообразования не уходит клиенту."""
    draft = {"bargain": [{"code": "14001", "price": 700.0, "list_price": 950.0}]}

    async def fake_floor(session, db, sku):
        return {"floor": 820.0, "floor_pay": 820.0, "status_label": "новый"}

    monkeypatch.setattr(sales_dialog, "price_floor", fake_floor)
    problems = asyncio.run(sales_dialog.check_bargain(None, None, draft))
    assert problems and "820" in problems[0]


def test_bargain_above_floor_passes(monkeypatch):
    """Уступка в пределах порога проходит, порог запоминается для карточки."""
    draft = {"bargain": [{"code": "14001", "price": 900.0, "list_price": 950.0}]}

    async def fake_floor(session, db, sku):
        return {"floor": 820.0, "floor_pay": 820.0, "status_label": "новый"}

    monkeypatch.setattr(sales_dialog, "price_floor", fake_floor)
    assert asyncio.run(sales_dialog.check_bargain(None, None, draft)) == []
    assert draft["bargain"][0]["floor"] == 820


def test_bargain_without_floor_is_escalated(monkeypatch):
    """Порог не посчитался — молча уступать нельзя."""
    draft = {"bargain": [{"code": "14001", "price": 900.0, "list_price": 950.0}]}

    async def fake_floor(session, db, sku):
        return {"error": "http 500"}

    monkeypatch.setattr(sales_dialog, "price_floor", fake_floor)
    problems = asyncio.run(sales_dialog.check_bargain(None, None, draft))
    assert problems and "http 500" in problems[0]


class FakeBot:
    def __init__(self):
        self.sent = []

    async def send_message(self, chat_id, text, **kw):
        self.sent.append((chat_id, text))


class FakeApp:
    def __init__(self):
        self.bot = FakeBot()


def test_hand_to_manager_writes_and_pings(monkeypatch):
    """Передача лида: правки в amoCRM плюс сообщение менеджеру с контекстом."""
    calls = []

    async def fake_write(session, path, payload, method="PATCH"):
        calls.append((method, path))
        return True

    async def fake_lead(session, lead_id):
        return {"_embedded": {"contacts": [{"id": 555}]}}

    monkeypatch.setattr(sales_dialog, "_amo_write", fake_write)
    monkeypatch.setattr(sales_dialog, "_amo_lead", fake_lead)
    app = FakeApp()
    async def fake_active(session, uid):
        return True

    monkeypatch.setattr(sales_dialog, "_amo_user_active", fake_active)
    db = FakeDB({"lead": {"lead_name": "ИП Волков", "contact_name": "Пётр", "chat_type": "max",
                          "prev_responsible_user_id": None}})
    msg = _msg(lead_id=44548679, inbound_text="дайте цену на треску",
               draft_text="Треска лойн – 1630 ₽/кг.")
    what = asyncio.run(sales_dialog.hand_to_manager(app, db, msg))

    assert ("PATCH", "/leads/44548679") in calls
    assert ("PATCH", "/contacts") in calls
    assert ("POST", "/tasks") in calls
    chat_id, text = app.bot.sent[0]
    assert chat_id == sales_dialog.INESSA_TG_CHAT
    assert "44548679" in text and "дайте цену на треску" in text
    assert "менеджеру написали" in what


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
    assert "Вернуть менеджеру" in labels


def test_normal_card_has_send_button():
    from sales_dialog import _keyboard
    labels = [b.text for row in _keyboard(7, "reply").inline_keyboard for b in row]
    assert labels[0] == "Отправить"


def test_strip_card_header_removes_service_lines():
    t = "Ответ от имени Инессы: Адреса приняла, по цене сориентируйте пожалуйста"
    assert strip_card_header(t) == "Адреса приняла, по цене сориентируйте пожалуйста"


def test_strip_card_header_keeps_normal_text():
    t = "Добрый день. По треске 1630 ₽/кг, есть."
    assert strip_card_header(t) == t


def test_lead_brief_picks_useful_fields():
    lead = {"custom_fields_values": [
        {"field_name": "referrer", "values": [{"value": "https://yandex.ru/search/?text=купить+форель+оптом&clid=1"}]},
        {"field_name": "Комментарий", "values": [{"value": "Якутск контакт Афанасий"}]},
        {"field_name": "Специализация", "values": [{"value": "ОПТ"}]},
        {"field_name": "_ym_uid", "values": [{"value": "1780965298943707632"}]},
    ]}
    b = lead_brief(lead)
    assert "купить форель оптом" in b
    assert "Якутск" in b and "ОПТ" in b
    assert "1780965298943707632" not in b        # метрики в промпт не идут


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


def test_hand_returns_to_previous_manager(monkeypatch):
    """Раскачанный лид возвращается тому менеджеру, который вёл его до агента."""
    calls = []

    async def fake_write(session, path, payload, method="PATCH"):
        calls.append((method, path, payload))
        return True

    async def fake_lead(session, lead_id):
        return {"_embedded": {"contacts": []}}

    async def fake_active(session, uid):
        return True

    monkeypatch.setattr(sales_dialog, "_amo_write", fake_write)
    monkeypatch.setattr(sales_dialog, "_amo_lead", fake_lead)
    monkeypatch.setattr(sales_dialog, "_amo_user_active", fake_active)
    app = FakeApp()
    db = FakeDB({"lead": {"lead_name": "ИП Волков", "chat_type": "max",
                          "prev_responsible_user_id": 12625622}})      # Баласанян
    asyncio.run(sales_dialog.hand_to_manager(app, db, _msg()))
    lead_patch = next(p for m, path, p in calls if path.startswith("/leads/"))
    assert lead_patch["responsible_user_id"] == 12625622
    assert app.bot.sent[0][0] == sales_dialog.MANAGER_TG[12625622]


def test_hand_falls_back_to_inessa_if_manager_left(monkeypatch):
    """Менеджер уволен — лид уходит Инессе, а не в пустоту."""
    calls = []

    async def fake_write(session, path, payload, method="PATCH"):
        calls.append((method, path, payload))
        return True

    async def fake_lead(session, lead_id):
        return {"_embedded": {"contacts": []}}

    async def fake_active(session, uid):
        return False

    monkeypatch.setattr(sales_dialog, "_amo_write", fake_write)
    monkeypatch.setattr(sales_dialog, "_amo_lead", fake_lead)
    monkeypatch.setattr(sales_dialog, "_amo_user_active", fake_active)
    app = FakeApp()
    db = FakeDB({"lead": {"lead_name": "ИП Волков", "chat_type": "max",
                          "prev_responsible_user_id": 13553106}})      # уволенный
    what = asyncio.run(sales_dialog.hand_to_manager(app, db, _msg()))
    lead_patch = next(p for m, path, p in calls if path.startswith("/leads/"))
    assert lead_patch["responsible_user_id"] == sales_dialog.INESSA_AMO_USER
    assert "не работает" in what


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


# ── партии менеджеров (план 2026-09-25) ───────────────────────────────────────
def test_slot_quota_grows_every_seven_minutes():
    """Отправка с 9:00 по одной карточке в 7 минут (собственник 25.09.2026)."""
    from sales_dialog import slot_quota
    cfg = {"slot_start_hour": 9, "slot_minutes": 7, "daily_target": 30}
    assert slot_quota(datetime(2026, 9, 29, 8, 59, tzinfo=MSK), cfg) == 0
    assert slot_quota(datetime(2026, 9, 29, 9, 0, tzinfo=MSK), cfg) == 1
    assert slot_quota(datetime(2026, 9, 29, 9, 6, tzinfo=MSK), cfg) == 1
    assert slot_quota(datetime(2026, 9, 29, 9, 7, tzinfo=MSK), cfg) == 2
    # К 12:30 прошло 210 минут — ровно 30 слотов, потолок дня.
    assert slot_quota(datetime(2026, 9, 29, 12, 30, tzinfo=MSK), cfg) == 30
    assert slot_quota(datetime(2026, 9, 29, 18, 0, tzinfo=MSK), cfg) == 30


class QueueDB:
    """Заглушка под отбор очереди: отдаёт статистику по адресатам."""

    def __init__(self, stats):
        self.stats = stats

    def _fetchall(self, sql, params=None):
        return self.stats

    def _fetchone(self, sql, params=None):
        return None

    def _execute(self, sql, params=None):
        pass


def _silent_row(lead_id, amo):
    return {"lead_id": lead_id, "assignee_amo_id": amo, "campaign": "c"}


def test_allowed_silent_respects_slots_per_manager():
    """Каждому менеджеру своя квота: у кого выдано по норме — тому пока не даём."""
    from sales_dialog import _allowed_silent
    db = QueueDB([{"assignee_amo_id": 13665786, "pending": 0, "today": 2},
                  {"assignee_amo_id": 11544494, "pending": 0, "today": 0}])
    cfg = {"slot_start_hour": 9, "slot_minutes": 7, "daily_target": 30, "max_pending": 1}
    now = datetime(2026, 9, 29, 9, 8, tzinfo=MSK)          # квота 2
    out = _allowed_silent(db, "c", [_silent_row(1, 13665786), _silent_row(2, 11544494)], cfg, now)
    assert [r["lead_id"] for r in out] == [2]


def test_allowed_silent_holds_when_card_pending():
    """Пока карточка у менеджера не решена, вторую не выдаём."""
    from sales_dialog import _allowed_silent
    db = QueueDB([{"assignee_amo_id": 13665786, "pending": 1, "today": 1}])
    cfg = {"slot_start_hour": 9, "slot_minutes": 7, "daily_target": 30, "max_pending": 1}
    now = datetime(2026, 9, 29, 11, 0, tzinfo=MSK)
    assert _allowed_silent(db, "c", [_silent_row(1, 13665786)], cfg, now) == []


def test_allowed_silent_one_hanging_card_does_not_block_other_manager():
    """Висящая карточка Коликова не должна останавливать партию Скляр."""
    from sales_dialog import _allowed_silent
    db = QueueDB([{"assignee_amo_id": 13665786, "pending": 1, "today": 5},
                  {"assignee_amo_id": 11544494, "pending": 0, "today": 5}])
    cfg = {"slot_start_hour": 9, "slot_minutes": 7, "daily_target": 30, "max_pending": 1}
    now = datetime(2026, 9, 29, 11, 0, tzinfo=MSK)
    out = _allowed_silent(db, "c", [_silent_row(1, 13665786), _silent_row(2, 11544494)], cfg, now)
    assert [r["lead_id"] for r in out] == [2]


def test_offer_price_replaces_all_types():
    from sales_dialog import apply_offers
    rows = [{"code": "11043", "name": "Лосось ОХЛ Трим Д Мурманск", "opt": 2750.0,
             "horeca": 2750.0, "spec": None, "stock": 0, "own": True}]
    out = apply_offers(rows, {"11043": 2490.0})[0]
    assert out["opt"] == out["horeca"] == out["spec"] == out["offer"] == 2490.0


def test_offer_price_accepted_by_check_prices():
    """Цена предложения проходит сверку, даже если типа «спец» в прайсе нет."""
    prices = {"11043": {"code": "11043", "opt": 2490.0, "horeca": 2490.0,
                        "spec": 2490.0, "offer": 2490.0, "stock": 0}}
    draft = {"text": "лосось охл. Трим Д – 2490 ₽/кг", "price_claims": [
        {"code": "11043", "name": "Лосось", "price": 2490.0, "price_type": "spec"}]}
    assert check_prices(draft, prices) == []


def test_below_offer_price_is_escalation():
    """Ниже согласованной цены предложения агент не торгуется вообще."""
    from sales_dialog import check_bargain
    prices = {"11043": {"code": "11043", "offer": 2490.0, "opt": 2490.0,
                        "horeca": 2490.0, "spec": 2490.0}}
    draft = {"bargain": [{"code": "11043", "price": 2400.0, "list_price": 2490.0,
                          "price_type": "opt"}]}
    problems = asyncio.run(check_bargain(None, None, draft, prices))
    assert problems and "ниже согласованной цены предложения" in problems[0]


def test_manager_card_has_take_button():
    from sales_dialog import _keyboard
    kb = _keyboard(7, "reply", to_manager=True)
    labels = [b.text for row in kb.inline_keyboard for b in row]
    assert "Забрать" in labels and "Вернуть менеджеру" not in labels


def test_owner_card_keeps_hand_button():
    from sales_dialog import _keyboard
    kb = _keyboard(7, "reply")
    labels = [b.text for row in kb.inline_keyboard for b in row]
    assert "Вернуть менеджеру" in labels and "Забрать" not in labels


def test_card_goes_to_assigned_manager():
    from sales_dialog import card_recipient
    db = FakeDB({"lead": {"assignee_amo_id": 13665786}})
    chat, amo = card_recipient(db, _msg(assignee_amo_id=13665786))
    assert chat == sales_dialog.MANAGER_TG[13665786] and amo == 13665786


def test_card_without_assignee_goes_to_owner(monkeypatch):
    from sales_dialog import card_recipient
    monkeypatch.setenv("OWNER_CHAT_ID", "777")
    db = FakeDB({"lead": {"assignee_amo_id": None}})
    chat, amo = card_recipient(db, _msg(assignee_amo_id=None))
    assert chat == 777 and amo is None


def test_progress_line_counts_done(monkeypatch):
    """Разобрано = отправлено + забрано + отклонено, висящие не считаются."""
    from sales_dialog import batch_progress

    class DB:
        def _fetchall(self, sql, params=None):
            return [{"assignee_amo_id": 13665786, "issued": 12, "sent": 7, "taken": 2,
                     "refused": 1, "pending": 1, "lost": 1}]
    row = batch_progress(DB(), "c")[0]
    assert row["sent"] + row["taken"] + row["refused"] == 10


# ── подпись по менеджеру (28.09.2026: Денису шли сообщения от имени Инессы) ────
def test_persona_by_manager():
    from sales_dialog import persona_for
    assert persona_for(13665786)["first"] == "Денис"
    assert persona_for(11544494)["first"] == "Инесса"
    assert persona_for(None)["first"] == "Инесса"          # карточка собственника


def test_system_prompt_has_manager_name_and_gender():
    from sales_dialog import persona_for, system_prompt
    denis = system_prompt(persona_for(13665786))
    assert "Денис Коликов" in denis and "Инесса Скляр" not in denis
    assert "в мужском роде" in denis
    inessa = system_prompt(persona_for(11544494))
    assert "Инесса Скляр" in inessa and "в женском роде" in inessa
    for ph in ("{{AGENT_FULL}}", "{{AGENT_FIRST}}", "{{GENDER_RULE}}"):
        assert ph not in denis and ph not in inessa


def test_style_check_gender_aware():
    from sales_dialog import check_style
    # У Инессы мужской род — ошибка, у Дениса он правильный.
    assert check_style("Понял, уточнил и отправил", "f")
    assert check_style("Понял, уточнил и отправил", "m") == []
    assert check_style("Поняла, уточнила и отправила", "m")
    assert check_style("Поняла, уточнила и отправила", "f") == []


def test_style_check_still_catches_calls():
    from sales_dialog import check_style
    assert any("звонк" in p for p in check_style("Давайте созвонимся, наберу вас", "m"))


def test_card_signed_by_assignee():
    from sales_dialog import _card_text
    db = FakeDB({"lead": {"lead_name": "Олива", "chat_type": "max"}, "msg": _msg()})
    card = _card_text(db, _msg(assignee_amo_id=13665786))
    assert "Ответ от имени Денис" in card


# ── скидка МРМ не должна уводить ниже порога (28.09.2026) ─────────────────────
def test_is_mrm_only_chilled_murmansk():
    from sales_dialog import is_mrm
    assert is_mrm("Лосось атл. (сёмга), ПСГ, ОХЛ., 5-6 кг. Мурманск")
    assert not is_mrm("Лосось (сёмга) филе, МРМ, ЗАМОРОЖ., Трим Д 1.6-2.0 кг.")
    assert not is_mrm("Форель ПСГ, ОХЛ, 4.0 + кг. Карелия")


def _mrm_rows():
    return [{"code": "71011", "name": "Лосось атл. (сёмга), ПСГ, ОХЛ., 5-6 кг. Мурманск",
             "opt": 1690.0, "horeca": 1690.0, "spec": None, "mrm": True,
             "list": {"opt": 1790.0, "horeca": 1790.0, "spec": None}, "stock": 882.1}]


def test_mrm_discount_kept_when_above_floor(monkeypatch):
    """Закупка упала – скидка проходит, цена со скидкой выше порога."""
    import sales_dialog
    sales_dialog._mrm_guard.update({"at": None, "blocked": set()})

    async def fake_floor(session, db, code):
        return {"floor": 1651.0, "floor_pay": 1651.0}
    monkeypatch.setattr(sales_dialog, "price_floor", fake_floor)
    out = asyncio.run(sales_dialog.mrm_guard(None, None, _mrm_rows()))
    assert out[0]["opt"] == 1690.0 and not out[0].get("mrm_discount_off")


def test_mrm_discount_removed_when_below_floor(monkeypatch):
    """Сырьё подорожало – скидка снимается, агент называет прайсовую цену."""
    import sales_dialog
    sales_dialog._mrm_guard.update({"at": None, "blocked": set()})

    async def fake_floor(session, db, code):
        return {"floor": 1779.0, "floor_pay": 1779.0}
    monkeypatch.setattr(sales_dialog, "price_floor", fake_floor)
    out = asyncio.run(sales_dialog.mrm_guard(None, None, _mrm_rows()))
    assert out[0]["opt"] == 1790.0 and out[0]["mrm_discount_off"]


def test_mrm_discount_removed_when_floor_unknown(monkeypatch):
    """Порог не посчитался – скидку не даём, это дешевле ошибки."""
    import sales_dialog
    sales_dialog._mrm_guard.update({"at": None, "blocked": set()})

    async def fake_floor(session, db, code):
        return {"error": "http 500"}
    monkeypatch.setattr(sales_dialog, "price_floor", fake_floor)
    out = asyncio.run(sales_dialog.mrm_guard(None, None, _mrm_rows()))
    assert out[0]["opt"] == 1790.0 and out[0]["mrm_discount_off"]


# ── шапка карточки не должна уезжать клиенту (28.09.2026, чат «ФАРШ») ──────────
def test_strip_card_header_removes_whole_card_head():
    from sales_dialog import strip_card_header
    raw = ("ФАРШ · max · сделка 41740283\n\n"
           "Диалог затих, агент пишет первым.\n\n"
           "Ответ от имени Инессы:\n"
           "У нас охлаждённое филе Трим С – 1890 ₽/кг.\nПривезти пласт на пробу?")
    out = strip_card_header(raw)
    assert out.startswith("У нас охлаждённое филе")
    assert "сделка 41740283" not in out and "Диалог затих" not in out
    assert "Ответ от имени" not in out


def test_card_trace_blocks_send():
    from sales_dialog import CARD_TRACE_RE
    assert CARD_TRACE_RE.search("ФАРШ · max · сделка 41740283")
    assert CARD_TRACE_RE.search("Ответ от имени Дениса:")
    assert not CARD_TRACE_RE.search("Форель филе Трим С – 1890 ₽/кг, привезти пробу?")


def test_strip_keeps_plain_text():
    from sales_dialog import strip_card_header
    t = "Клиенту: форель 1890 ₽/кг"          # «Клиент:» с двоеточием — служебная, а это нет
    assert strip_card_header(t) == t


def test_inject_word_banned():
    """Инъект клиенту не называем никогда (собственник 28.09.2026)."""
    from sales_dialog import check_style
    assert any("инъект" in p for p in check_style("Слоение шло от инъекта", "f"))
    assert any("инъект" in p for p in check_style("там инъекцию делают", "f"))
    assert check_style("Слоение шло от сырья", "f") == []


# ─── новые сайт-лиды ведёт агент (план 2026-09-29) ────────────────────────────
from sales_dialog import contact_channels, is_site_lead, norm_phone, pick_chat


def test_norm_phone():
    assert norm_phone("8 (909) 909-84-51") == "79099098451"
    assert norm_phone("+7 909 909 84 51") == "79099098451"
    assert norm_phone("9099098451") == "79099098451"
    assert norm_phone("12345") is None
    assert norm_phone(None) is None


def test_contact_channels_reads_wz_fields_and_phone():
    contact = {"custom_fields_values": [
        {"field_id": 2244321, "values": [{"value": "116796554"}]},
        {"field_id": 2224427, "values": [{"value": "360092495"}]},
        {"field_id": 1, "field_code": "PHONE", "values": [{"value": "8 909 909-84-51"}]},
    ]}
    assert contact_channels(contact) == {"max": "116796554", "telegram": "360092495",
                                         "phone": "79099098451"}


def test_pick_chat_prefers_where_client_wrote():
    ch = {"telegram": "t1", "max": "m1"}
    assert pick_chat(ch, "m1") == ("max", "m1", ["t1", "m1"])
    assert pick_chat(ch) == ("telegram", "t1", ["t1", "m1"])
    assert pick_chat({"max": "m1"}) == ("max", "m1", ["m1"])


def test_pick_chat_without_chats_goes_by_phone():
    assert pick_chat({"phone": "79099098451"}) == ("telegram", None, [])


def test_is_site_lead():
    lead = {"status_id": 85554794, "_embedded": {"tags": [{"id": 782551}]}}
    assert is_site_lead(lead)
    assert not is_site_lead({**lead, "status_id": 143})
    assert not is_site_lead({"status_id": 85554794, "_embedded": {"tags": [{"id": 1}]}})


class _CardDB:
    def __init__(self, lead):
        self.lead = lead

    def _fetchone(self, sql, params=()):
        return self.lead


def test_card_marks_new_site_lead_by_phone():
    db = _CardDB({"lead_name": "Новый", "contact_name": None, "chat_type": "telegram",
                  "chat_id": None})
    msg = {"campaign": "site-leads-new", "lead_id": 1, "chat_id": None, "chat_type": "telegram",
           "inbound_message_id": "first:1:20260929", "inbound_text": None,
           "draft_text": "Добрый день", "action": "reply", "assignee_amo_id": None}
    text = sales_dialog._card_text(db, msg)
    assert "telegram по номеру" in text
    assert "Новая заявка с сайта" in text


class _Resp:
    def __init__(self, status, body):
        self.status, self._body = status, body

    async def text(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    def __init__(self, status, body):
        self.resp, self.payloads = _Resp(status, body), []

    def post(self, url, json=None, headers=None):
        self.payloads.append(json)
        return self.resp


class _PhoneDB:
    def __init__(self):
        self.executed = []

    def _fetchone(self, sql, params=()):
        return {"chat_type": "telegram", "chat_id": None, "phone": "79099098451", "contact_id": 5}

    def _execute(self, sql, params=()):
        self.executed.append((sql, params))


def _phone_msg():
    return {"id": 7, "campaign": "site-leads-new", "lead_id": 1, "chat_id": None,
            "chat_type": "telegram"}


def test_deliver_by_phone_binds_found_chat(monkeypatch):
    writes = []

    async def fake_write(session, path, payload, method="PATCH"):
        writes.append((path, payload))
        return True
    monkeypatch.setattr(sales_dialog, "_amo_write", fake_write)
    db, s = _PhoneDB(), _Session(201, '{"messageId":"x","chatId":"360092495"}')
    ok, _ = asyncio.run(sales_dialog._deliver(db, s, _phone_msg(), "Добрый день"))
    assert ok
    assert s.payloads[0]["phone"] == "79099098451" and "chatId" not in s.payloads[0]
    assert any("360092495" in str(p) for _, p in db.executed)
    assert writes and writes[0][1][0]["custom_fields_values"][0]["values"][0]["value"] == "360092495"


def test_deliver_by_phone_not_found():
    db, s = _PhoneDB(), _Session(201, '{"messageId":"x"}')
    ok, info = asyncio.run(sales_dialog._deliver(db, s, _phone_msg(), "Добрый день"))
    assert not ok and info.startswith(sales_dialog.NO_TG)
    db, s = _PhoneDB(), _Session(400, '{"error":"CHAT_NOT_FOUND"}')
    ok, info = asyncio.run(sales_dialog._deliver(db, s, _phone_msg(), "Добрый день"))
    assert not ok and info.startswith(sales_dialog.NO_TG)


def test_remember_keeps_prev_manager():
    calls = []

    class DB:
        def _execute(self, sql, params=()):
            calls.append(params)
    lead = {"id": 1, "name": "Чуева", "responsible_user_id": 13746010}
    sales_dialog._remember(DB(), "site-leads-new", lead, "active", "перенос", {"id": 5, "name": "Ч"},
                           ("max", "m1", ["m1"]), "79000000000")
    p = calls[0]
    assert p[5] == sales_dialog.AGENT_AMO_USER      # ответственный – «Эф»
    assert p[12] == 13746010                         # вернуть – прежнему менеджеру


# ── статус карточки «[жду твой текст …]» не должен уезжать клиенту (29.09.2026) ─
def test_strip_card_header_removes_bracket_status():
    from sales_dialog import strip_card_header, CARD_TRACE_RE
    raw = ("Ольга, цена уже с доставкой.\nСобрать один пласт на пробу?\n\n"
           "[жду твой текст — ответом на карточку или просто следующим сообщением]")
    out = strip_card_header(raw)
    assert out == "Ольга, цена уже с доставкой.\nСобрать один пласт на пробу?"
    assert not CARD_TRACE_RE.search(out)
    assert CARD_TRACE_RE.search(raw)


# ── антиспам (план 2026-09-30-антиспам-правила-переписки-агента) ──────────────
POLICY = {"limits": {"telegram": 10, "max": 0, "whatsapp": 0}, "window": [10, 17],
          "interval_min": [20, 45], "first_touch_channels": ["telegram"],
          "first_touch_max_days": 7, "paused": {}, "next_at": {}}
WED_11 = datetime(2026, 10, 7, 11, 0, tzinfo=MSK)


def _init_row(ch="telegram", source="silent", **kw):
    return {"lead_id": 1, "campaign": "c", "chat_type": ch, "source": source,
            "message_id": f"{source}:1:20261007", **kw}


def test_initiative_detected_by_key():
    from sales_dialog import is_initiative
    assert is_initiative({"message_id": "silent:1:20261007"})
    assert is_initiative({"inbound_message_id": "first:1:20261007"})
    assert not is_initiative({"inbound_message_id": "a1b2-c3d4"})


def test_initiative_allowed_within_rules():
    from sales_dialog import initiative_block
    assert initiative_block(_init_row(), POLICY, {"telegram": 3}, WED_11) is None


def test_initiative_blocked_by_channel_limit():
    from sales_dialog import initiative_block
    assert "лимит" in initiative_block(_init_row(), POLICY, {"telegram": 10}, WED_11)


def test_max_zero_limit_blocks_initiative():
    """Пока бан MAX не снят, первым в MAX не пишем вообще."""
    from sales_dialog import initiative_block
    assert "лимит" in initiative_block(_init_row("max"), POLICY, {}, WED_11)


def test_initiative_blocked_when_paused():
    from sales_dialog import initiative_block
    p = {**POLICY, "paused": {"telegram": "30.09 ошибка отправки"}}
    assert "пауз" in initiative_block(_init_row(), p, {}, WED_11)


def test_initiative_blocked_outside_window_and_weekend():
    from sales_dialog import initiative_block
    assert "окна" in initiative_block(_init_row(), POLICY, {}, WED_11.replace(hour=17, minute=5))
    assert "окна" in initiative_block(_init_row(), POLICY, {}, WED_11.replace(hour=9, minute=59))
    assert "окна" in initiative_block(_init_row(), POLICY, {}, datetime(2026, 10, 10, 12, tzinfo=MSK))


def test_initiative_waits_for_random_interval():
    from sales_dialog import initiative_block
    p = {**POLICY, "next_at": {"telegram": (WED_11 + timedelta(minutes=10)).isoformat()}}
    assert "пауза" in initiative_block(_init_row(), p, {}, WED_11)
    assert initiative_block(_init_row(), p, {}, WED_11 + timedelta(minutes=11)) is None


def test_first_touch_only_telegram_and_fresh():
    from sales_dialog import initiative_block
    p = {**POLICY, "limits": {"telegram": 10, "max": 5}}
    assert "первым в max" in initiative_block(_init_row("max", "first"), p, {}, WED_11)
    old = _init_row(source="first", taken_at_lead=WED_11 - timedelta(days=8))
    assert "старше" in initiative_block(old, p, {}, WED_11)
    fresh = _init_row(source="first", taken_at_lead=WED_11 - timedelta(days=2))
    assert initiative_block(fresh, p, {}, WED_11) is None


def test_phone_only_lead_counts_as_telegram():
    from sales_dialog import row_channel
    assert row_channel({"chat_type": None}) == "telegram"


def test_refusal_hard():
    from sales_dialog import refusal_kind
    for t in ("Не пишите мне больше", "больше не пишите", "Прошу не беспокоить", "это спам",
              "Удалите мой номер", "отпишите нас", "Хватит писать", "не присылайте ничего"):
        assert refusal_kind(t) == "hard", t


def test_refusal_soft():
    from sales_dialog import refusal_kind
    for t in ("Пока не актуально", "неактуально", "Нам не интересно", "не нужно, спасибо",
              "Сейчас не требуется"):
        assert refusal_kind(t) == "soft", t


def test_not_refusal():
    from sales_dialog import refusal_kind
    for t in ("Пришлите прайс", "Сколько стоит форель?", "Давайте 30 кг", None, ""):
        assert refusal_kind(t) is None, t


def test_link_detected_in_initiative_text():
    from sales_dialog import LINK_RE
    assert LINK_RE.search("Прайс тут: https://f2b.group/price")
    assert LINK_RE.search("смотрите f2b.group")
    assert LINK_RE.search("t.me/fishto_biz")
    assert not LINK_RE.search("Форель филе Трим С 1.4–2.0 кг – 1890 ₽/кг. Прислать образец?")


class PolicyDB:
    """Хранит bot_settings в памяти – для проверки паузы и интервала."""
    def __init__(self, value=None):
        self.value = value

    def _fetchone(self, sql, params=None):
        return {"value": self.value} if self.value is not None else None

    def _execute(self, sql, params=None):
        self.value = params[1]


def test_pause_channel_saved_once():
    from sales_dialog import channel_policy, pause_channel
    db = PolicyDB()
    asyncio.run(pause_channel(None, db, "telegram", "ошибка отправки"))
    first = channel_policy(db)["paused"]["telegram"]
    asyncio.run(pause_channel(None, db, "telegram", "другая причина"))
    assert channel_policy(db)["paused"]["telegram"] == first


def test_note_initiative_sets_interval_within_bounds():
    from sales_dialog import channel_policy, note_initiative
    db = PolicyDB()
    note_initiative(db, channel_policy(db), "telegram", WED_11)
    nxt = datetime.fromisoformat(channel_policy(db)["next_at"]["telegram"])
    assert WED_11 + timedelta(minutes=20) <= nxt <= WED_11 + timedelta(minutes=45)
    assert channel_policy(db)["limits"]["max"] == 0      # умолчания не затёрты
