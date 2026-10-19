"""Сверка 18:00 (Ф8): проблемы заказа."""
import os
from datetime import datetime, timedelta

os.environ.setdefault("YANDEX_GEOCODER_KEY", "test")

import razvozka_evening as ev

NOW = datetime(2026, 10, 19, 18, 0, tzinfo=ev._MSK)


def _o(state="Согласован", comment="тел +7 999 123-45-67", rest="Здрасте", win=True):
    attrs = []
    if win:
        attrs = [{"name": "Окно доставки с (время)", "value": "2026-10-20 10:00:00"},
                 {"name": "Окно доставки до (время)", "value": "2026-10-20 14:00:00"}]
    return {"name": "04821", "state": {"name": state}, "deliveryPlannedMoment": "2026-10-20 10:00:00",
            "attributes": attrs, "shipmentAddressFull": {"comment": comment},
            "agent": {"id": "a1", "name": "ООО Х",
                      "attributes": [{"name": "Название ресторана", "value": rest}] if rest else []}}


def test_clean_order_confirmed_recently():
    issues, btn = ev.order_issues(_o(), {}, {"a1": NOW - timedelta(days=3)}, NOW)
    assert issues == [] and btn is False


def test_all_problems():
    issues, btn = ev.order_issues(_o(state="На согласовании", comment="", rest="", win=False),
                                  {"04821": "нет точки на карте"}, {}, NOW)
    assert issues == ["не согласован (На согласовании)", "нет окна приёмки",
                      "нет контакта приёмки (телефона в комментарии к адресу)",
                      "нет «Названия ресторана»", "нет точки на карте"]
    assert btn is False


def test_contact_stale_needs_button():
    issues, btn = ev.order_issues(_o(), {}, {"a1": NOW - timedelta(days=30)}, NOW)
    assert issues == [] and btn is True


def test_retail_no_restaurant_no_confirm():
    o = _o(rest="")
    o["agent"]["name"] = "Розничный покупатель*** (Карина)"
    assert ev.order_issues(o, {}, {}, NOW) == ([], False)
    o2 = _o(rest="нет")
    assert ev.order_issues(o2, {}, {"a1": NOW}, NOW)[0] == ["нет «Названия ресторана»"]
