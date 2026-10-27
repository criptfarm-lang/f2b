"""27.10.2026: «Маршруты → Сегодня» не снимает «подтверждено» с неизменённой машины."""
import json
import os

os.environ.setdefault("YANDEX_GEOCODER_KEY", "test")

import route_dispatch as rd  # noqa: E402


class FakeDB:
    def __init__(self, row):
        self.row, self.sql = row, []

    def _fetchone(self, q, a):
        return self.row

    def _execute(self, q, a):
        self.sql.append(q)


SNAP = [{"order_no": "04947"}, {"order_no": "ТОЧКА 4"}]


def test_same_route_stays_confirmed(monkeypatch):
    db = FakeDB({"status": "confirmed", "stops": json.dumps(SNAP)})
    monkeypatch.setattr(rd, "_DB", db)
    assert rd._upsert_draft("2026-10-27", 26210, 1, SNAP) is True
    assert "draft" not in db.sql[0]


def test_changed_route_goes_draft(monkeypatch):
    db = FakeDB({"status": "confirmed", "stops": json.dumps(SNAP)})
    monkeypatch.setattr(rd, "_DB", db)
    assert rd._upsert_draft("2026-10-27", 26209, 1, SNAP + [{"order_no": "ЗАБОР 00645"}]) is False
    assert "'draft'" in db.sql[0]
