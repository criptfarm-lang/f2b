"""
База данных бота F2B PRO
SQLite — хранит задачи, медиафайлы, контакты, прайсы
"""

import sqlite3
import os
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional


DB_PATH = os.getenv("DB_PATH", "f2b_bot.db")


class Database:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT NOT NULL,
                executor TEXT,
                deadline TEXT,
                status TEXT DEFAULT 'open',
                source_chat INTEGER,
                source_message_id INTEGER,
                created_by TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                completed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS media (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id TEXT NOT NULL,
                media_type TEXT,
                caption TEXT,
                chat_id INTEGER,
                uploader TEXT,
                date TEXT
            );

            CREATE TABLE IF NOT EXISTS prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id TEXT NOT NULL,
                filename TEXT,
                chat_id INTEGER,
                uploader TEXT,
                uploaded_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                phone TEXT,
                company TEXT,
                notes TEXT,
                added_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS debtors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client TEXT NOT NULL,
                manager TEXT,
                amount REAL,
                days INTEGER,
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                user_id INTEGER,
                user_name TEXT,
                text TEXT,
                message_type TEXT DEFAULT 'text',
                ts TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE NOT NULL,
                value TEXT,
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)
        self.conn.commit()

    # ─── ЗАДАЧИ ───────────────────────────────────────────────────────────────

    def save_task(self, text: str, executor: str = "", deadline: str = None,
                  source_chat: int = None, source_message_id: int = None,
                  created_by: str = "") -> int:
        cur = self.conn.execute(
            """INSERT INTO tasks (text, executor, deadline, source_chat, source_message_id, created_by)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (text, executor, deadline, source_chat, source_message_id, created_by)
        )
        self.conn.commit()
        return cur.lastrowid

    def complete_task(self, task_id: int):
        self.conn.execute(
            "UPDATE tasks SET status='done', completed_at=datetime('now') WHERE id=?",
            (task_id,)
        )
        self.conn.commit()

    def get_tasks_for_user(self, name: str) -> List[Dict]:
        """Задачи конкретного сотрудника (нечёткий поиск по имени)."""
        name_parts = name.lower().split()
        rows = self.conn.execute(
            "SELECT * FROM tasks WHERE status='open' ORDER BY deadline ASC NULLS LAST"
        ).fetchall()

        result = []
        today = date.today().isoformat()
        for row in rows:
            exe = (row['executor'] or "").lower()
            if any(p in exe for p in name_parts):
                d = dict(row)
                d['overdue'] = bool(d.get('deadline') and d['deadline'] < today)
                result.append(d)
        return result

    def get_all_open_tasks(self) -> List[Dict]:
        today = date.today().isoformat()
        rows = self.conn.execute(
            "SELECT * FROM tasks WHERE status='open' ORDER BY executor, deadline ASC NULLS LAST"
        ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d['overdue'] = bool(d.get('deadline') and d['deadline'] < today)
            result.append(d)
        return result

    def get_overdue_tasks(self) -> List[Dict]:
        today = date.today().isoformat()
        rows = self.conn.execute(
            "SELECT * FROM tasks WHERE status='open' AND deadline < ? ORDER BY deadline ASC",
            (today,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_tasks_due_today(self) -> List[Dict]:
        today = date.today().isoformat()
        rows = self.conn.execute(
            "SELECT * FROM tasks WHERE status='open' AND deadline = ?",
            (today,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_tasks_due_tomorrow(self) -> List[Dict]:
        tomorrow = (date.today() + timedelta(days=1)).isoformat()
        rows = self.conn.execute(
            "SELECT * FROM tasks WHERE status='open' AND deadline = ?",
            (tomorrow,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_weekly_stats(self) -> Dict[str, Dict]:
        week_ago = (date.today() - timedelta(days=7)).isoformat()
        rows = self.conn.execute("SELECT * FROM tasks WHERE created_at >= ?", (week_ago,)).fetchall()

        stats = {}
        today = date.today().isoformat()
        for row in rows:
            exe = row['executor'] or 'Без исполнителя'
            if exe not in stats:
                stats[exe] = {'total': 0, 'done': 0, 'overdue': 0}
            stats[exe]['total'] += 1
            if row['status'] == 'done':
                stats[exe]['done'] += 1
            elif row['deadline'] and row['deadline'] < today:
                stats[exe]['overdue'] += 1
        return stats

    # ─── МЕДИА ────────────────────────────────────────────────────────────────

    def save_media(self, file_id: str, media_type: str, caption: str,
                   chat_id: int, uploader: str, date: str):
        self.conn.execute(
            "INSERT INTO media (file_id, media_type, caption, chat_id, uploader, date) VALUES (?,?,?,?,?,?)",
            (file_id, media_type, caption, chat_id, uploader, date)
        )
        self.conn.commit()

    def search_media(self, query: str, media_type: str = None) -> List[Dict]:
        """Ищет медиафайлы по ключевым словам в подписи."""
        words = query.lower().split()
        rows = self.conn.execute("SELECT * FROM media ORDER BY date DESC").fetchall()

        results = []
        for row in rows:
            if media_type and row['media_type'] != media_type:
                continue
            caption = (row['caption'] or "").lower()
            if any(w in caption for w in words):
                results.append(dict(row))
        return results

    # ─── ПРАЙСЫ ───────────────────────────────────────────────────────────────

    def save_price(self, file_id: str, filename: str, chat_id: int, uploader: str):
        self.conn.execute(
            "INSERT INTO prices (file_id, filename, chat_id, uploader) VALUES (?,?,?,?)",
            (file_id, filename, chat_id, uploader)
        )
        self.conn.commit()

    def get_latest_price(self) -> Optional[Dict]:
        row = self.conn.execute(
            "SELECT *, uploaded_at as date FROM prices ORDER BY uploaded_at DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    # ─── КОНТАКТЫ ─────────────────────────────────────────────────────────────

    def save_contact(self, name: str, phone: str, company: str = "", notes: str = ""):
        self.conn.execute(
            "INSERT OR REPLACE INTO contacts (name, phone, company, notes) VALUES (?,?,?,?)",
            (name, phone, company, notes)
        )
        self.conn.commit()

    def search_contacts(self, query: str) -> List[Dict]:
        q = f"%{query.lower()}%"
        rows = self.conn.execute(
            "SELECT * FROM contacts WHERE lower(name) LIKE ? OR lower(company) LIKE ?",
            (q, q)
        ).fetchall()
        return [dict(r) for r in rows]

    # ─── ДЕБИТОРКА ────────────────────────────────────────────────────────────

    def save_debtor(self, client: str, manager: str, amount: float, days: int):
        self.conn.execute(
            """INSERT OR REPLACE INTO debtors (client, manager, amount, days, updated_at)
               VALUES (?, ?, ?, ?, datetime('now'))""",
            (client, manager, amount, days)
        )
        self.conn.commit()

    def get_debtors(self) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT * FROM debtors ORDER BY days DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    # ─── ИСТОРИЯ СООБЩЕНИЙ ───────────────────────────────────────────────────

    def save_message(self, chat_id: int, user_id: int, user_name: str,
                     text: str, message_type: str = 'text'):
        """Сохраняет сообщение из чата."""
        self.conn.execute(
            """INSERT INTO chat_messages (chat_id, user_id, user_name, text, message_type)
               VALUES (?, ?, ?, ?, ?)""",
            (chat_id, user_id, user_name, text, message_type)
        )
        # Оставляем только последние 500 сообщений на чат
        self.conn.execute(
            """DELETE FROM chat_messages WHERE chat_id = ? AND id NOT IN (
               SELECT id FROM chat_messages WHERE chat_id = ?
               ORDER BY id DESC LIMIT 500)""",
            (chat_id, chat_id)
        )
        self.conn.commit()

    def get_recent_messages(self, chat_id: int, limit: int = 50) -> List[Dict]:
        """Возвращает последние N сообщений из чата."""
        rows = self.conn.execute(
            """SELECT user_name, text, ts, message_type
               FROM chat_messages WHERE chat_id = ?
               ORDER BY id DESC LIMIT ?""",
            (chat_id, limit)
        ).fetchall()
        return [dict(r) for r in reversed(rows)]

    def format_history(self, chat_id: int, limit: int = 50) -> str:
        """Форматирует историю сообщений для промпта Claude."""
        messages = self.get_recent_messages(chat_id, limit)
        if not messages:
            return ""
        lines = []
        for m in messages:
            ts = m['ts'][11:16] if m.get('ts') else ""  # HH:MM
            lines.append(f"[{ts}] {m['user_name']}: {m['text']}")
        return "\n".join(lines)

    # ─── ДОЛГОСРОЧНАЯ ПАМЯТЬ ──────────────────────────────────────────────────

    def remember(self, key: str, value: str):
        """Сохраняет факт в долгосрочную память."""
        self.conn.execute(
            """INSERT OR REPLACE INTO memory (key, value, updated_at)
               VALUES (?, ?, datetime('now'))""",
            (key, value)
        )
        self.conn.commit()

    def recall(self, key: str) -> Optional[str]:
        """Извлекает факт из памяти."""
        row = self.conn.execute(
            "SELECT value FROM memory WHERE key = ?", (key,)
        ).fetchone()
        return row['value'] if row else None

    def get_all_memories(self) -> List[Dict]:
        """Все сохранённые факты."""
        rows = self.conn.execute(
            "SELECT key, value, updated_at FROM memory ORDER BY updated_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def format_memories(self) -> str:
        """Форматирует память для промпта Claude."""
        memories = self.get_all_memories()
        if not memories:
            return ""
        lines = [f"- {m['key']}: {m['value']}" for m in memories[:30]]
        return "\n".join(lines)

    # ─── КОНТЕКСТ ДЛЯ CLAUDE ──────────────────────────────────────────────────

    def get_context_summary(self) -> str:
        """Краткий контекст о компании для Claude."""
        open_tasks = len(self.get_all_open_tasks())
        overdue = len(self.get_overdue_tasks())
        debtors = self.get_debtors()
        debtor_list = ", ".join(d['client'] for d in debtors[:5]) if debtors else "нет"

        return (
            f"Компания: F2B PRO (рыба и морепродукты оптом).\n"
            f"Открытых задач: {open_tasks}, просрочено: {overdue}.\n"
            f"Клиенты с долгами: {debtor_list}.\n"
            f"Сотрудники: Белякова А. (закупки), Баласанян К. (продажи), "
            f"Скляр И. (продажи), Малышкин А. (финансы), Гераскина Ю. (CRM)."
        )
