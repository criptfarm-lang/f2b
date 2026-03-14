"""
База данных бота F2B PRO
PostgreSQL — хранит задачи, медиафайлы, контакты, прайсы, ПДЗ комментарии
"""

import os
import psycopg2
import psycopg2.extras
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:FOfGvpjobKJPQYPBcuefaHWwtGEmVyte@switchback.proxy.rlwy.net:44165/railway")


class Database:
    def __init__(self):
        self.conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        self.conn.autocommit = False
        self._create_tables()

    def _execute(self, sql: str, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
            self.conn.commit()
            return cur

    def _fetchall(self, sql: str, params=None) -> List[Dict]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
            return [dict(r) for r in cur.fetchall()]

    def _fetchone(self, sql: str, params=None) -> Optional[Dict]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            return dict(row) if row else None

    def _create_tables(self):
        sql = """
            CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                text TEXT NOT NULL,
                executor TEXT,
                deadline TEXT,
                status TEXT DEFAULT 'open',
                source_chat BIGINT,
                source_message_id BIGINT,
                created_by TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP,
                completed_by TEXT,
                result TEXT
            );

            CREATE TABLE IF NOT EXISTS media (
                id SERIAL PRIMARY KEY,
                file_id TEXT NOT NULL,
                media_type TEXT,
                caption TEXT,
                chat_id BIGINT,
                uploader TEXT,
                date TEXT
            );

            CREATE TABLE IF NOT EXISTS prices (
                id SERIAL PRIMARY KEY,
                file_id TEXT NOT NULL,
                filename TEXT,
                chat_id BIGINT,
                uploader TEXT,
                uploaded_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS contacts (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                phone TEXT,
                company TEXT,
                notes TEXT,
                added_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS debtors (
                id SERIAL PRIMARY KEY,
                client TEXT NOT NULL UNIQUE,
                manager TEXT,
                amount REAL,
                days INTEGER,
                updated_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS chat_messages (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT,
                user_id BIGINT,
                user_name TEXT,
                text TEXT,
                message_type TEXT DEFAULT 'text',
                ts TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS memory (
                id SERIAL PRIMARY KEY,
                key TEXT UNIQUE NOT NULL,
                value TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS pdz_comments (
                id SERIAL PRIMARY KEY,
                client TEXT NOT NULL,
                manager TEXT,
                order_name TEXT,
                debt_amount REAL,
                debt_days INTEGER,
                comment TEXT,
                commented_by TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS wazzup_messages (
                id SERIAL PRIMARY KEY,
                message_id TEXT UNIQUE,
                channel_id TEXT,
                chat_type TEXT,
                chat_id TEXT,
                contact_name TEXT,
                manager_id TEXT,
                manager_name TEXT,
                text TEXT,
                is_outbound BOOLEAN DEFAULT FALSE,
                sent_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
        self.conn.commit()
        self._migrate()

    def _migrate(self):
        """Добавляет новые колонки если их нет (для существующих БД)."""
        migrations = [
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS completed_by TEXT",
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS result TEXT",
        ]
        with self.conn.cursor() as cur:
            for m in migrations:
                try:
                    cur.execute(m)
                except Exception:
                    pass
        self.conn.commit()

    # ─── ЗАДАЧИ ───────────────────────────────────────────────────────────────

    def save_task(self, text: str, executor: str = "", deadline: str = None,
                  source_chat: int = None, source_message_id: int = None,
                  created_by: str = "") -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO tasks (text, executor, deadline, source_chat, source_message_id, created_by)
                   VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
                (text, executor, deadline, source_chat, source_message_id, created_by)
            )
            row = cur.fetchone()
            self.conn.commit()
            return row['id']

    def complete_task(self, task_id: int, result: str = "", completed_by: str = ""):
        self._execute(
            "UPDATE tasks SET status='done', completed_at=NOW(), result=%s, completed_by=%s WHERE id=%s",
            (result, completed_by, task_id)
        )

    def get_recently_done(self, hours: int = 24) -> List[Dict]:
        """Задачи выполненные за последние N часов."""
        return self._fetchall(
            """SELECT * FROM tasks WHERE status='done'
               AND completed_at >= NOW() - INTERVAL '%s hours'
               ORDER BY completed_at DESC""",
            (hours,)
        )

    def cleanup_done_tasks(self):
        """Удаляет выполненные задачи старше 24 часов."""
        self._execute(
            "DELETE FROM tasks WHERE status='done' AND completed_at < NOW() - INTERVAL '24 hours'"
        )

    def get_tasks_for_user(self, name: str) -> List[Dict]:
        name_parts = name.lower().split()
        rows = self._fetchall(
            "SELECT * FROM tasks WHERE status='open' ORDER BY deadline ASC NULLS LAST"
        )
        today = date.today().isoformat()
        result = []
        for row in rows:
            exe = (row.get('executor') or "").lower()
            if any(p in exe for p in name_parts):
                row['overdue'] = bool(row.get('deadline') and str(row['deadline'])[:10] < today)
                result.append(row)
        return result

    def get_all_open_tasks(self) -> List[Dict]:
        today = date.today().isoformat()
        rows = self._fetchall(
            "SELECT * FROM tasks WHERE status='open' ORDER BY executor, deadline ASC NULLS LAST"
        )
        for row in rows:
            row['overdue'] = bool(row.get('deadline') and str(row['deadline'])[:10] < today)
        return rows

    def get_overdue_tasks(self) -> List[Dict]:
        today = date.today().isoformat()
        return self._fetchall(
            "SELECT * FROM tasks WHERE status='open' AND deadline < %s ORDER BY deadline ASC",
            (today,)
        )

    def get_tasks_due_today(self) -> List[Dict]:
        today = date.today().isoformat()
        return self._fetchall(
            "SELECT * FROM tasks WHERE status='open' AND deadline::text = %s",
            (today,)
        )

    def get_tasks_due_tomorrow(self) -> List[Dict]:
        tomorrow = (date.today() + timedelta(days=1)).isoformat()
        return self._fetchall(
            "SELECT * FROM tasks WHERE status='open' AND deadline::text = %s",
            (tomorrow,)
        )

    def get_weekly_stats(self) -> Dict[str, Dict]:
        week_ago = (date.today() - timedelta(days=7)).isoformat()
        rows = self._fetchall(
            "SELECT * FROM tasks WHERE created_at >= %s", (week_ago,)
        )
        stats = {}
        today = date.today().isoformat()
        for row in rows:
            exe = row.get('executor') or 'Без исполнителя'
            if exe not in stats:
                stats[exe] = {'total': 0, 'done': 0, 'overdue': 0}
            stats[exe]['total'] += 1
            if row['status'] == 'done':
                stats[exe]['done'] += 1
            elif row.get('deadline') and str(row['deadline'])[:10] < today:
                stats[exe]['overdue'] += 1
        return stats

    # ─── МЕДИА ────────────────────────────────────────────────────────────────

    def save_media(self, file_id: str, media_type: str, caption: str,
                   chat_id: int, uploader: str, date: str):
        self._execute(
            "INSERT INTO media (file_id, media_type, caption, chat_id, uploader, date) VALUES (%s,%s,%s,%s,%s,%s)",
            (file_id, media_type, caption, chat_id, uploader, date)
        )

    def search_media(self, query: str, media_type: str = None) -> List[Dict]:
        words = query.lower().split()
        rows = self._fetchall("SELECT * FROM media ORDER BY date DESC")
        results = []
        for row in rows:
            if media_type and row.get('media_type') != media_type:
                continue
            caption = (row.get('caption') or "").lower()
            if all(w in caption for w in words):
                results.append(row)
        return results

    # ─── ПРАЙСЫ ───────────────────────────────────────────────────────────────

    def save_price(self, file_id: str, filename: str, chat_id: int, uploader: str):
        self._execute(
            "INSERT INTO prices (file_id, filename, chat_id, uploader) VALUES (%s,%s,%s,%s)",
            (file_id, filename, chat_id, uploader)
        )

    def get_latest_price(self) -> Optional[Dict]:
        return self._fetchone(
            "SELECT *, uploaded_at as date FROM prices ORDER BY uploaded_at DESC LIMIT 1"
        )

    # ─── КОНТАКТЫ ─────────────────────────────────────────────────────────────

    def save_contact(self, name: str, phone: str, company: str = "", notes: str = ""):
        self._execute(
            "INSERT INTO contacts (name, phone, company, notes) VALUES (%s,%s,%s,%s)",
            (name, phone, company, notes)
        )

    def search_contacts(self, query: str) -> List[Dict]:
        q = f"%{query.lower()}%"
        return self._fetchall(
            "SELECT * FROM contacts WHERE lower(name) LIKE %s OR lower(company) LIKE %s",
            (q, q)
        )

    # ─── ДЕБИТОРКА ────────────────────────────────────────────────────────────

    def save_debtor(self, client: str, manager: str, amount: float, days: int):
        self._execute(
            """INSERT INTO debtors (client, manager, amount, days, updated_at)
               VALUES (%s, %s, %s, %s, NOW())
               ON CONFLICT (client) DO UPDATE SET manager=%s, amount=%s, days=%s, updated_at=NOW()""",
            (client, manager, amount, days, manager, amount, days)
        )

    def get_debtors(self) -> List[Dict]:
        return self._fetchall("SELECT * FROM debtors ORDER BY days DESC")

    # ─── ИСТОРИЯ СООБЩЕНИЙ ───────────────────────────────────────────────────

    def save_message(self, chat_id: int, user_id: int, user_name: str,
                     text: str, message_type: str = 'text'):
        self._execute(
            """INSERT INTO chat_messages (chat_id, user_id, user_name, text, message_type)
               VALUES (%s, %s, %s, %s, %s)""",
            (chat_id, user_id, user_name, text, message_type)
        )
        self._execute(
            """DELETE FROM chat_messages WHERE chat_id = %s AND id NOT IN (
               SELECT id FROM chat_messages WHERE chat_id = %s
               ORDER BY id DESC LIMIT 500)""",
            (chat_id, chat_id)
        )

    def get_recent_messages(self, chat_id: int, limit: int = 50) -> List[Dict]:
        rows = self._fetchall(
            """SELECT user_name, text, ts, message_type
               FROM chat_messages WHERE chat_id = %s
               ORDER BY id DESC LIMIT %s""",
            (chat_id, limit)
        )
        return list(reversed(rows))

    def format_history(self, chat_id: int, limit: int = 50) -> str:
        messages = self.get_recent_messages(chat_id, limit)
        if not messages:
            return ""
        lines = []
        for m in messages:
            ts = str(m.get('ts', ''))
            ts = ts[11:16] if len(ts) > 11 else ""
            lines.append(f"[{ts}] {m['user_name']}: {m['text']}")
        return "\n".join(lines)

    # ─── ДОЛГОСРОЧНАЯ ПАМЯТЬ ──────────────────────────────────────────────────

    def remember(self, key: str, value: str):
        self._execute(
            """INSERT INTO memory (key, value, updated_at) VALUES (%s, %s, NOW())
               ON CONFLICT (key) DO UPDATE SET value=%s, updated_at=NOW()""",
            (key, value, value)
        )

    def recall(self, key: str) -> Optional[str]:
        row = self._fetchone("SELECT value FROM memory WHERE key = %s", (key,))
        return row['value'] if row else None

    def get_all_memories(self) -> List[Dict]:
        return self._fetchall(
            "SELECT key, value, updated_at FROM memory ORDER BY updated_at DESC"
        )

    def format_memories(self) -> str:
        memories = self.get_all_memories()
        if not memories:
            return ""
        lines = [f"- {m['key']}: {m['value']}" for m in memories[:30]]
        return "\n".join(lines)

    # ─── КОНТЕКСТ ДЛЯ CLAUDE ──────────────────────────────────────────────────

    def get_context_summary(self) -> str:
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

    # ─── ПДЗ КОММЕНТАРИИ ──────────────────────────────────────────────────────

    def save_pdz_comment(self, client: str, manager: str, order_name: str,
                         debt_amount: float, debt_days: int, comment: str,
                         commented_by: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO pdz_comments
                   (client, manager, order_name, debt_amount, debt_days, comment, commented_by)
                   VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (client, manager, order_name, debt_amount, debt_days, comment, commented_by)
            )
            row = cur.fetchone()
            self.conn.commit()
            return row['id']

    def get_pdz_comments(self, limit: int = 50) -> list:
        return self._fetchall(
            "SELECT * FROM pdz_comments ORDER BY created_at DESC LIMIT %s",
            (limit,)
        )

    # ─── WAZZUP СООБЩЕНИЯ ─────────────────────────────────────────────────────

    def save_wazzup_message(self, message_id: str, channel_id: str, chat_type: str,
                            chat_id: str, contact_name: str, manager_id: str,
                            manager_name: str, text: str, is_outbound: bool,
                            sent_at: str) -> bool:
        """Сохраняет сообщение из Wazzup. Возвращает True если новое."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO wazzup_messages
                       (message_id, channel_id, chat_type, chat_id, contact_name,
                        manager_id, manager_name, text, is_outbound, sent_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (message_id) DO NOTHING RETURNING id""",
                    (message_id, channel_id, chat_type, chat_id, contact_name,
                     manager_id, manager_name, text, is_outbound, sent_at)
                )
                row = cur.fetchone()
                self.conn.commit()
                return row is not None
        except Exception:
            self.conn.rollback()
            return False

    def search_wazzup_mentions(self, keywords: list, days: int = 7,
                               manager_name: str = None) -> list:
        """Ищет исходящие сообщения менеджеров с упоминанием ключевых слов."""
        from datetime import datetime, timedelta
        since = datetime.now() - timedelta(days=days)

        conditions = ["is_outbound = TRUE", "sent_at >= %s", "text IS NOT NULL"]
        params = [since]

        if manager_name:
            conditions.append("LOWER(manager_name) LIKE %s")
            params.append(f"%{manager_name.lower()}%")

        # Фильтр по ключевым словам (любое из них)
        kw_conditions = " OR ".join(["LOWER(text) LIKE %s"] * len(keywords))
        conditions.append(f"({kw_conditions})")
        for kw in keywords:
            params.append(f"%{kw.lower()}%")

        sql = f"""
            SELECT manager_name, contact_name, chat_type, text, sent_at
            FROM wazzup_messages
            WHERE {' AND '.join(conditions)}
            ORDER BY manager_name, sent_at DESC
        """
        return self._fetchall(sql, params)

    def get_wazzup_stats(self, days: int = 7) -> dict:
        """Статистика сообщений по менеджерам за период."""
        from datetime import datetime, timedelta
        since = datetime.now() - timedelta(days=days)
        rows = self._fetchall(
            """SELECT manager_name, COUNT(*) as msg_count,
               COUNT(DISTINCT chat_id) as client_count
               FROM wazzup_messages
               WHERE is_outbound = TRUE AND sent_at >= %s AND manager_name IS NOT NULL
               GROUP BY manager_name ORDER BY msg_count DESC""",
            (since,)
        )
        return {r['manager_name']: r for r in rows}
