"""Дневная сводка техопераций собственнику (05.10.2026)."""
import asyncio
import types

import processing_svetofor as ps


class FakeCur:
    def __init__(self, store):
        self.store = store
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        if "insert into production.processing_svetofor_digest" in sql:
            pid, name, color, text = params
            self.store[pid] = {"processing_id": pid, "name": name, "color": color,
                               "text_html": text, "sent_at": None}
        elif "select processing_id" in sql:
            self.rows = [dict(r) for r in self.store.values() if r["sent_at"] is None]
        elif "set sent_at=now()" in sql:
            for pid in params[0]:
                self.store[pid]["sent_at"] = "now"

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self):
        self.store = {}

    def cursor(self):
        return FakeCur(self.store)


class FakeBot:
    def __init__(self):
        self.sent = []

    async def send_message(self, **kw):
        self.sent.append(kw)


def test_digest_sorted_red_first_with_buttons(monkeypatch):
    conn = FakeConn()
    monkeypatch.setattr(ps, "_db", lambda: conn)
    monkeypatch.setenv("OWNER_CHAT_ID", "1")
    ps._digest_enqueue("a" * 36, "101", "🟢 Техоперация №101 · 05.10\nok", None)
    ps._digest_enqueue("b" * 36, "102", "🔴 Техоперация №102 · 05.10\n<плохо>", None)
    app = types.SimpleNamespace(bot=FakeBot())
    asyncio.run(ps.digest_job(app))
    assert len(app.bot.sent) == 1
    msg = app.bot.sent[0]
    assert msg["text"].index("№102") < msg["text"].index("№101")
    assert "&lt;плохо&gt;" in msg["text"]
    rows = msg["reply_markup"].inline_keyboard
    assert [r[0].text for r in rows] == ["№102 ✅ Проверено", "№101 ✅ Проверено"]
    assert all(len(b.callback_data.encode()) <= 64 for r in rows for b in r)
    # повторный прогон – тишина
    asyncio.run(ps.digest_job(app))
    assert len(app.bot.sent) == 1


def test_digest_empty_is_silent(monkeypatch):
    conn = FakeConn()
    monkeypatch.setattr(ps, "_db", lambda: conn)
    monkeypatch.setenv("OWNER_CHAT_ID", "1")
    app = types.SimpleNamespace(bot=FakeBot())
    asyncio.run(ps.digest_job(app))
    assert app.bot.sent == []


def test_partner_only_immediate(monkeypatch):
    monkeypatch.setenv("OWNER_CHAT_ID", "1")
    monkeypatch.setenv("PARTNER_CHAT_ID", "2")
    assert ps._recipients() == [2]
