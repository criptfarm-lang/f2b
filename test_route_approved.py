"""Раскладка «Развозки» вместо Логистики на утверждённые дни (план 2026-10-16-логистика-всё-в-развозке)."""
import os
from datetime import date, datetime

os.environ.setdefault("YANDEX_GEOCODER_KEY", "test")

import route_registry as rr

D = date(2026, 10, 15)


def ts(h, day=D):
    return int(datetime(day.year, day.month, day.day, h, tzinfo=rr._MSK).timestamp())


PLAN = {"units": [
    {"unit_id": 26209, "stops": [
        {"seq": 2, "order_no": "04728", "client": "Б", "address": "адр Б", "lat": 55.8, "lon": 37.5,
         "win_from": 10, "win_to": 14, "kind": "delivery"},
        {"seq": 1, "order_no": "04631", "client": "ООО Фугу", "address": "адр А", "lat": 55.84, "lon": 37.52,
         "win_from": 9, "win_to": 10, "kind": "delivery"},
    ]},
    {"unit_id": 26695, "stops": [
        {"seq": 1, "order_no": "ЗАБОР 00123", "client": "ЗАБОР: Поставщик", "address": "склад",
         "lat": 55.7, "lon": 37.6, "win_from": 9, "win_to": 18, "kind": "pickup"},
    ]},
], "hired": [], "unassigned": []}


def _routes():
    return {
        26209: [
            {"order_no": "04631", "tf": ts(9), "tt": ts(10), "phone": "+7999", "note": "звонить",
             "oid": 111, "client": "ФУГУ", "has_cid": True, "vt": None},
            {"order_no": "99999", "tf": ts(11), "tt": ts(12), "client": "старая раскладка",
             "has_cid": True, "vt": ts(11)},
        ],
        26210: [{"order_no": "77777", "tf": ts(9, date(2026, 10, 16)), "tt": ts(10, date(2026, 10, 16)),
                 "client": "другой день", "has_cid": True}],
        26695: [{"order_no": "ЗАБОР 00123: Поставщик", "tf": ts(9), "tt": ts(18), "oid": 222,
                 "client": "Поставщик", "has_cid": True}],
    }


def test_approved_day_replaces_logistics_in_logist_order():
    out = rr.apply_approved(_routes(), {D: PLAN})
    assert [s["order_no"] for s in out[26209]] == ["04631", "04728"]   # порядок логиста, старая точка убрана
    assert [s["seq"] for s in out[26209]] == [0, 1]
    first = out[26209][0]
    assert first["phone"] == "+7999" and first["note"] == "звонить" and first["oid"] == 111
    assert first["tf"] == ts(9) and first["approved"] is True


def test_other_days_untouched():
    out = rr.apply_approved(_routes(), {D: PLAN})
    assert [s["order_no"] for s in out[26210]] == ["77777"]


def test_pickup_matched_to_bridge_order():
    out = rr.apply_approved(_routes(), {D: PLAN})
    pk = out[26695][0]
    assert pk["order_no"] == "ЗАБОР 00123: Поставщик" and pk["oid"] == 222 and pk["is_pickup"] is True


def test_unmatched_stop_gets_window_from_hours():
    out = rr.apply_approved(_routes(), {D: PLAN})
    b = out[26209][1]
    assert b["tf"] == ts(10) and b["tt"] == ts(14) and b["phone"] == ""


def test_no_plans_noop():
    r = _routes()
    assert rr.apply_approved(r, {}) is r


def test_plan_times_from_departure():
    """Выезд 08:00, плечи 20 и 10 мин без пробок (×1.21), окно второй точки с 09:00 – ждёт."""
    dep = ts(8)
    stops = [{"lat": 55.8, "lon": 37.5, "tf": ts(8)},
             {"lat": 55.7, "lon": 37.6, "tf": ts(9)},
             {"lat": None, "lon": None},
             {"lat": 55.6, "lon": 37.7}]
    out = rr.plan_times(stops, dep, [1200, 600, 300])
    assert out[0] == dep + int(1200 * 1.21)                 # 08:24
    assert out[1] == ts(9)                                   # приехал 08:39, ждёт окна
    assert out[2] is None                                    # без координат – без времени
    # после второй: 8 мин выгрузки, точка без гео тоже 8 мин, плечо 5 мин × 1.21
    assert out[3] == ts(9) + 16 * 60 + int(300 * 1.21)


def test_apply_approved_marks_plan_day():
    out = rr.apply_approved(_routes(), {D: PLAN})
    st = [s for s in out[26209] if s.get("approved")]
    assert {s["plan_day"] for s in st} == {D.isoformat()}
    assert all(s["vt"] is None for s in st)  # сигналы по `vt` до Ф7 не включаем
