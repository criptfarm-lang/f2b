"""
База данных бота F2B PRO
PostgreSQL — хранит задачи, медиафайлы, контакты, прайсы, ПДЗ комментарии
"""

import os
import psycopg2
import psycopg2.extras
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional


DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL env not set. "
        "Задай переменную в Railway → Variables (строка подключения Postgres-плагина)."
    )


class Database:
    def __init__(self):
        self._dsn = DATABASE_URL
        self.conn = self._connect()
        self._create_tables()

    def _connect(self):
        conn = psycopg2.connect(self._dsn, cursor_factory=psycopg2.extras.RealDictCursor)
        conn.autocommit = False
        return conn

    def _ensure_connection(self):
        """Переподключается если соединение закрыто или упало."""
        try:
            self.conn.cursor().execute("SELECT 1")
        except Exception:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = self._connect()

    def _execute(self, sql: str, params=None):
        self._ensure_connection()
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
            self.conn.commit()
            return cur

    def _fetchall(self, sql: str, params=None) -> List[Dict]:
        self._ensure_connection()
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
            return [dict(r) for r in cur.fetchall()]

    def _fetchone(self, sql: str, params=None) -> Optional[Dict]:
        self._ensure_connection()
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

            CREATE TABLE IF NOT EXISTS agreed_notifications (
                order_id TEXT PRIMARY KEY,
                sent_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS report_links (
                token TEXT PRIMARY KEY,
                mgr_filter TEXT,
                expires_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS report_cache (
                id INTEGER PRIMARY KEY DEFAULT 1,
                data TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS mgr_history_cache (
                mgr_tag TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS pending_contracts (
                user_id BIGINT PRIMARY KEY,
                data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS aging_alerts (
                counterparty_id TEXT PRIMARY KEY,
                name TEXT,
                alerted_at DATE DEFAULT CURRENT_DATE
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
            """CREATE TABLE IF NOT EXISTS wazzup_messages (
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
            )""",
            """CREATE TABLE IF NOT EXISTS wazzup_contact_map (
                id SERIAL PRIMARY KEY,
                chat_id TEXT UNIQUE NOT NULL,
                chat_type TEXT,
                channel_id TEXT,
                company_name TEXT,
                wazzup_name TEXT,
                role TEXT DEFAULT 'закупщик',
                created_at TIMESTAMP DEFAULT NOW()
            )""",
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS bot_message_id BIGINT",
            "ALTER TABLE wazzup_contact_map ADD COLUMN IF NOT EXISTS tags TEXT",
            "ALTER TABLE wazzup_contact_map ADD COLUMN IF NOT EXISTS manager TEXT",
            "ALTER TABLE wazzup_contact_map ADD COLUMN IF NOT EXISTS segment TEXT",
            """CREATE TABLE IF NOT EXISTS wazzup_pending (
                link_key TEXT PRIMARY KEY,
                chat_id TEXT,
                channel_id TEXT,
                wazzup_name TEXT,
                chat_type TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS contracts (
                id SERIAL PRIMARY KEY,
                contract_number TEXT UNIQUE NOT NULL,
                buyer_name TEXT,
                buyer_data JSONB,
                created_by TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )""",
            "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS buyer_data JSONB",
            """CREATE TABLE IF NOT EXISTS call_transcripts (
                id SERIAL PRIMARY KEY,
                call_id TEXT UNIQUE,
                src_num TEXT,
                dst_num TEXT,
                manager_name TEXT,
                tree_name TEXT,
                transcript TEXT,
                duration_sec INTEGER DEFAULT 0,
                called_at TIMESTAMP DEFAULT NOW()
            )""",
            "ALTER TABLE wazzup_contact_map ADD COLUMN IF NOT EXISTS segment TEXT",
            "ALTER TABLE wazzup_contact_map ADD COLUMN IF NOT EXISTS manager TEXT",
            """CREATE TABLE IF NOT EXISTS wazzup_pending_ident (
                id SERIAL PRIMARY KEY,
                link_key TEXT UNIQUE NOT NULL,
                chat_id TEXT NOT NULL,
                channel_id TEXT,
                wazzup_name TEXT,
                chat_type TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS wazzup_contacts (
                id SERIAL PRIMARY KEY,
                contact_name TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                chat_type TEXT,
                channel_id TEXT,
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(contact_name, chat_type)
            )""",
            """CREATE TABLE IF NOT EXISTS manager_chats (
                user_id BIGINT PRIMARY KEY,
                full_name TEXT,
                is_blocked BOOLEAN DEFAULT FALSE,
                request_count INT DEFAULT 0,
                last_seen TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )""",
            "ALTER TABLE manager_chats ADD COLUMN IF NOT EXISTS is_blocked BOOLEAN DEFAULT FALSE",
            "ALTER TABLE manager_chats ADD COLUMN IF NOT EXISTS request_count INT DEFAULT 0",
            "ALTER TABLE manager_chats ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP DEFAULT NOW()",
            """CREATE TABLE IF NOT EXISTS aging_alerts (
                counterparty_id TEXT PRIMARY KEY,
                name TEXT,
                alerted_at DATE DEFAULT CURRENT_DATE
            )""",
            """CREATE TABLE IF NOT EXISTS agreed_notifications (
                order_id TEXT PRIMARY KEY,
                sent_at TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS bot_usage_log (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                full_name TEXT,
                action TEXT,
                chat_id BIGINT,
                created_at TIMESTAMP DEFAULT NOW()
            )""",
            # Таблица для хранения контекста алертов цены (ожидание ответа менеджера)
            """CREATE TABLE IF NOT EXISTS price_alerts (
                id SERIAL PRIMARY KEY,
                order_id TEXT,
                order_name TEXT,
                client_name TEXT,
                manager_name TEXT,
                manager_user_id BIGINT,
                alert_text TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW()
            )""",
            # Объединённый алерт «На согласовании» / «ЗА ЛИМИТОМ»
            # (план 2026-05-21-объединённый-алерт-на-согласование.md, Фаза 4).
            # Дедуп: UNIQUE(order_id, sum_hash). Новый алерт триггерится только при
            # изменении суммы заказа > 5% от уже зафиксированной.
            """CREATE TABLE IF NOT EXISTS pending_approval_alerts (
                id BIGSERIAL PRIMARY KEY,
                order_id TEXT NOT NULL,
                sum_hash BIGINT NOT NULL,
                order_name TEXT,
                client_name TEXT,
                manager_name TEXT,
                manager_user_id BIGINT,
                alert_text TEXT,
                colors_json JSONB,
                sent_at TIMESTAMPTZ DEFAULT NOW(),
                closed_at TIMESTAMPTZ,
                closed_by BIGINT,
                comment TEXT,
                UNIQUE (order_id, sum_hash)
            )""",
            "CREATE INDEX IF NOT EXISTS idx_approval_alerts_order ON pending_approval_alerts (order_id)",
            "CREATE INDEX IF NOT EXISTS idx_approval_alerts_open ON pending_approval_alerts (closed_at) WHERE closed_at IS NULL",
            """CREATE TABLE IF NOT EXISTS pdz_results (
                id SERIAL PRIMARY KEY,
                manager_name TEXT,
                manager_user_id BIGINT,
                result_text TEXT,
                work_date DATE DEFAULT CURRENT_DATE,
                created_at TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS market_intel_messages (
                id SERIAL PRIMARY KEY,
                tg_msg_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                posted_at TIMESTAMPTZ NOT NULL,
                msg_type TEXT NOT NULL,
                text_raw TEXT,
                file_path TEXT,
                file_ext TEXT,
                forward_from TEXT,
                processed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (chat_id, tg_msg_id)
            )""",
            "CREATE INDEX IF NOT EXISTS idx_market_intel_unprocessed ON market_intel_messages (processed_at) WHERE processed_at IS NULL",
            # ── ПДЗ-автоматика (план 2026-05-20, Фаза 2) ───────────────────────
            # Снимок состояния всех customerorder с заполненным ppm_initial.
            # Cron 13:55 и 14:00 МСК пишет сюда. Источник правды для сравнения
            # «вчера vs сегодня» и для logic «срыв обещания».
            """CREATE TABLE IF NOT EXISTS pdz_snapshots (
                id SERIAL PRIMARY KEY,
                snap_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                snap_date DATE NOT NULL,
                order_id TEXT NOT NULL,
                order_name TEXT,
                agent_id TEXT,
                agent_name TEXT,
                manager_tag TEXT,
                ppm_initial DATE,
                ppm_new DATE,
                reason_id TEXT,
                payed_sum NUMERIC(14,2),
                total_sum NUMERIC(14,2)
            )""",
            "CREATE INDEX IF NOT EXISTS idx_pdz_snapshots_snap_date ON pdz_snapshots (snap_date)",
            "CREATE INDEX IF NOT EXISTS idx_pdz_snapshots_order_snap ON pdz_snapshots (order_id, snap_date)",
            # 2026-05-20: balance контрагента — фильтр против ложных PDZ
            # (payed_sum<total_sum, но клиент по факту ничего не должен — оплата
            # не разнесена бухгалтерией). См. moysklad.pdz_overdue_for_manager.
            "ALTER TABLE pdz_snapshots ADD COLUMN IF NOT EXISTS agent_balance NUMERIC(14,2)",
            # Журнал обещаний оплаты. event_type = 'set' | 'moved' | 'broken'.
            """CREATE TABLE IF NOT EXISTS promise_log (
                id SERIAL PRIMARY KEY,
                occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                order_id TEXT NOT NULL,
                order_name TEXT,
                agent_id TEXT,
                agent_name TEXT,
                manager_tag TEXT,
                event_type TEXT NOT NULL,
                old_ppm_new DATE,
                new_ppm_new DATE,
                reason_id TEXT
            )""",
            "CREATE INDEX IF NOT EXISTS idx_promise_log_agent_time ON promise_log (agent_id, occurred_at)",
            "CREATE INDEX IF NOT EXISTS idx_promise_log_order_time ON promise_log (order_id, occurred_at)",
        ]
        with self.conn.cursor() as cur:
            for m in migrations:
                try:
                    cur.execute(m)
                except Exception:
                    pass
        self.conn.commit()

    def save_pdz_result(self, manager_name: str, manager_user_id: int, result_text: str):
        self._execute(
            "INSERT INTO pdz_results (manager_name, manager_user_id, result_text) VALUES (%s,%s,%s)",
            (manager_name, manager_user_id, result_text)
        )

    # ─── ПДЗ-автоматика: снимки и журнал обещаний (Фаза 2) ────────────────
    def save_pdz_snapshot(self, rows: List[Dict]) -> int:
        """Batch insert строк снимка состояния заказов.

        Каждый row — dict с ключами: snap_date, order_id, order_name,
        agent_id, agent_name, manager_tag, ppm_initial, ppm_new, reason_id,
        payed_sum, total_sum, agent_balance (опционально, может быть None).
        Возвращает число вставленных записей.
        """
        if not rows:
            return 0
        self._ensure_connection()
        params = [
            (
                r.get("snap_date"),
                r.get("order_id"),
                r.get("order_name"),
                r.get("agent_id"),
                r.get("agent_name"),
                r.get("manager_tag"),
                r.get("ppm_initial"),
                r.get("ppm_new"),
                r.get("reason_id"),
                r.get("payed_sum"),
                r.get("total_sum"),
                r.get("agent_balance"),
            )
            for r in rows
        ]
        with self.conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO pdz_snapshots
                   (snap_date, order_id, order_name, agent_id, agent_name,
                    manager_tag, ppm_initial, ppm_new, reason_id, payed_sum,
                    total_sum, agent_balance)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                params,
            )
            self.conn.commit()
        return len(params)

    def get_pdz_snapshot(self, snap_date) -> List[Dict]:
        """Возвращает все строки снимка за указанную дату (для отладки)."""
        return self._fetchall(
            """SELECT id, snap_at, snap_date, order_id, order_name, agent_id,
                      agent_name, manager_tag, ppm_initial, ppm_new, reason_id,
                      payed_sum, total_sum, agent_balance
               FROM pdz_snapshots
               WHERE snap_date = %s
               ORDER BY snap_at DESC, id ASC""",
            (snap_date,),
        )

    def get_latest_snapshot(self) -> List[Dict]:
        """Возвращает последний snapshot — DISTINCT ON (order_id) за MAX(snap_date)
        с самым свежим snap_at. Используется TG-дайджестом, чтобы не дёргать МС API
        каждый раз."""
        self._ensure_connection()
        with self.conn.cursor() as cur:
            cur.execute("SELECT MAX(snap_date) AS d FROM pdz_snapshots")
            row = cur.fetchone()
            latest_date = row.get("d") if row else None
        if not latest_date:
            return []
        return self._fetchall(
            """SELECT DISTINCT ON (order_id)
                      id, snap_at, snap_date, order_id, order_name, agent_id,
                      agent_name, manager_tag, ppm_initial, ppm_new, reason_id,
                      payed_sum, total_sum, agent_balance
               FROM pdz_snapshots
               WHERE snap_date = %s
               ORDER BY order_id, snap_at DESC""",
            (latest_date,),
        )

    def get_last_snapshot_before(self, snap_date) -> List[Dict]:
        """Возвращает последний снимок ПЕРЕД snap_date (для сравнения «вчера vs сегодня»).

        Логика:
          1) Берём MAX(snap_date) с условием snap_date < %s.
          2) Если такой даты нет — возвращаем [].
          3) Иначе тянем все строки этого дня — берём ПОСЛЕДНИЙ snap_at
             (на случай нескольких запусков cron в один день — 13:55 и 14:00).
        """
        self._ensure_connection()
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(snap_date) AS d FROM pdz_snapshots WHERE snap_date < %s",
                (snap_date,),
            )
            row = cur.fetchone()
            prev_date = row.get("d") if row else None
        if not prev_date:
            return []
        # Берём строки последнего snap_at в этом дне (на каждый order_id — самая поздняя
        # запись). На случай редких дублей по (order_id, snap_at) — DISTINCT ON.
        return self._fetchall(
            """
            SELECT DISTINCT ON (order_id)
                   id, snap_at, snap_date, order_id, order_name, agent_id,
                   agent_name, manager_tag, ppm_initial, ppm_new, reason_id,
                   payed_sum, total_sum, agent_balance
            FROM pdz_snapshots
            WHERE snap_date = %s
            ORDER BY order_id, snap_at DESC, id DESC
            """,
            (prev_date,),
        )

    def save_promise_events(self, events: List[Dict]) -> int:
        """Batch insert событий в promise_log.

        Каждый event — dict с ключами: order_id, order_name, agent_id, agent_name,
        manager_tag, event_type ('set'|'moved'|'broken'), old_ppm_new, new_ppm_new,
        reason_id. Возвращает число вставленных записей.
        """
        if not events:
            return 0
        self._ensure_connection()
        params = [
            (
                e.get("order_id"),
                e.get("order_name"),
                e.get("agent_id"),
                e.get("agent_name"),
                e.get("manager_tag"),
                e.get("event_type"),
                e.get("old_ppm_new"),
                e.get("new_ppm_new"),
                e.get("reason_id"),
            )
            for e in events
        ]
        with self.conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO promise_log
                   (order_id, order_name, agent_id, agent_name, manager_tag,
                    event_type, old_ppm_new, new_ppm_new, reason_id)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                params,
            )
            self.conn.commit()
        return len(params)

    # ─── ПДЗ Фаза 4.5: счётчики срывов обещаний по клиентам ───────────────
    def get_promise_breaks_count(self, agent_ids: list, days_window: int = 90) -> dict:
        """Возвращает {agent_id: count} — число срывов (event_type='broken')
        по каждому agent_id за последние `days_window` дней.

        Batch SQL — один запрос на всех agent_ids. Если список пустой —
        возвращает {}. Если у agent_id нет срывов, его в результате не будет
        (вызывающий код может .get(aid, 0)).
        """
        if not agent_ids:
            return {}
        # Уникальный список (на случай дублей), фильтруем пустые.
        unique_ids = [a for a in {x for x in agent_ids if x}]
        if not unique_ids:
            return {}
        days = int(days_window)
        sql = (
            "SELECT agent_id, COUNT(*) AS cnt FROM promise_log "
            "WHERE agent_id = ANY(%s) AND event_type='broken' "
            f"AND occurred_at >= NOW() - INTERVAL '{days} days' "
            "GROUP BY agent_id"
        )
        rows = self._fetchall(sql, (unique_ids,))
        return {r["agent_id"]: int(r["cnt"]) for r in rows}

    def get_promise_breaks_top(self, limit: int = 30, days_window: int = 90) -> list:
        """Топ контрагентов по числу broken-событий за `days_window` дней.

        Возвращает список dict с полями:
            {agent_id, agent_name, manager_tag, breaks_count, last_break_at}

        Группировка по agent_id; agent_name и manager_tag берутся из
        последнего broken-события. Сортировка breaks_count DESC, далее
        last_break_at DESC. Лимит — `limit`.
        """
        days = int(days_window)
        lim = int(limit)
        sql = (
            "SELECT agent_id, "
            "       (SELECT agent_name FROM promise_log p2 "
            "          WHERE p2.agent_id = p.agent_id AND p2.event_type='broken' "
            f"          AND p2.occurred_at >= NOW() - INTERVAL '{days} days' "
            "          ORDER BY p2.occurred_at DESC LIMIT 1) AS agent_name, "
            "       (SELECT manager_tag FROM promise_log p3 "
            "          WHERE p3.agent_id = p.agent_id AND p3.event_type='broken' "
            f"          AND p3.occurred_at >= NOW() - INTERVAL '{days} days' "
            "          ORDER BY p3.occurred_at DESC LIMIT 1) AS manager_tag, "
            "       COUNT(*) AS breaks_count, "
            "       MAX(occurred_at) AS last_break_at "
            "FROM promise_log p "
            "WHERE event_type='broken' "
            f"  AND occurred_at >= NOW() - INTERVAL '{days} days' "
            "  AND agent_id IS NOT NULL AND agent_id <> '' "
            "GROUP BY agent_id "
            "ORDER BY breaks_count DESC, last_break_at DESC "
            f"LIMIT {lim}"
        )
        rows = self._fetchall(sql)
        out = []
        for r in rows:
            out.append({
                "agent_id": r.get("agent_id"),
                "agent_name": r.get("agent_name"),
                "manager_tag": r.get("manager_tag"),
                "breaks_count": int(r.get("breaks_count") or 0),
                "last_break_at": r.get("last_break_at"),
            })
        return out

    # ─── Market Intel (канал «Мониторинг» — закупочные прайсы) ──────────────
    def save_market_intel_message(
        self,
        tg_msg_id: int,
        chat_id: int,
        posted_at,
        msg_type: str,
        text_raw: Optional[str] = None,
        file_path: Optional[str] = None,
        file_ext: Optional[str] = None,
        forward_from: Optional[str] = None,
    ) -> Optional[int]:
        """Сохраняет сообщение из канала «Мониторинг». Возвращает id или None если уже есть (UNIQUE chat_id, tg_msg_id)."""
        self._ensure_connection()
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO market_intel_messages
                   (tg_msg_id, chat_id, posted_at, msg_type, text_raw, file_path, file_ext, forward_from)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (chat_id, tg_msg_id) DO NOTHING
                   RETURNING id""",
                (tg_msg_id, chat_id, posted_at, msg_type, text_raw, file_path, file_ext, forward_from),
            )
            row = cur.fetchone()
            self.conn.commit()
            return row["id"] if row else None

    def get_unprocessed_market_intel(self, limit: int = 100) -> List[Dict]:
        return self._fetchall(
            """SELECT id, tg_msg_id, chat_id, posted_at, msg_type, text_raw,
                      file_path, file_ext, forward_from, created_at
               FROM market_intel_messages
               WHERE processed_at IS NULL
               ORDER BY posted_at ASC
               LIMIT %s""",
            (limit,),
        )

    def mark_market_intel_processed(self, msg_id: int):
        self._execute(
            "UPDATE market_intel_messages SET processed_at = NOW() WHERE id = %s",
            (msg_id,),
        )

    def get_market_intel_message(self, msg_id: int) -> Optional[Dict]:
        return self._fetchone(
            """SELECT id, tg_msg_id, chat_id, posted_at, msg_type, text_raw,
                      file_path, file_ext, forward_from, processed_at, created_at
               FROM market_intel_messages
               WHERE id = %s""",
            (msg_id,),
        )

    def get_market_intel_count(self) -> Dict[str, int]:
        row = self._fetchone(
            """SELECT
                 COUNT(*) AS total,
                 COUNT(*) FILTER (WHERE processed_at IS NULL) AS unprocessed
               FROM market_intel_messages"""
        )
        return {
            "total": int(row["total"]) if row else 0,
            "unprocessed": int(row["unprocessed"]) if row else 0,
        }

    def get_activity_by_day(self, days: int = 7) -> list:
        """Активность менеджеров по дням: звонки + сообщения."""
        from datetime import datetime, timedelta
        since = datetime.now() - timedelta(days=days)
        msgs = self._fetchall(
            """SELECT
                DATE(sent_at) as day,
                COALESCE(NULLIF(m.manager_name,''), cm.manager, 'Неизвестно') AS manager,
                COUNT(*) as msg_count
               FROM wazzup_messages m
               LEFT JOIN wazzup_contact_map cm ON m.chat_id = cm.chat_id
               WHERE m.is_outbound = TRUE AND m.sent_at >= %s
               GROUP BY DATE(sent_at), COALESCE(NULLIF(m.manager_name,''), cm.manager, 'Неизвестно')""",
            (since,)
        )
        calls = self._fetchall(
            """SELECT DATE(called_at) as day, manager_name as manager, COUNT(*) as call_count
               FROM call_transcripts
               WHERE called_at >= %s AND manager_name IS NOT NULL AND manager_name != ''
               GROUP BY DATE(called_at), manager_name""",
            (since,)
        )
        # Объединяем
        data = {}
        for r in msgs:
            key = (str(r["day"]), r["manager"])
            data.setdefault(key, {"day": str(r["day"]), "manager": r["manager"], "msgs": 0, "calls": 0})
            data[key]["msgs"] += r["msg_count"]
        for r in calls:
            key = (str(r["day"]), r["manager"])
            data.setdefault(key, {"day": str(r["day"]), "manager": r["manager"], "msgs": 0, "calls": 0})
            data[key]["calls"] += r["call_count"]
        return list(data.values())

    def get_contracts_today(self) -> list:
        """Договоры созданные сегодня."""
        return self._fetchall(
            "SELECT * FROM contracts WHERE DATE(created_at) = CURRENT_DATE ORDER BY created_at"
        )

    def get_pdz_results_today(self) -> list:
        return self._fetchall(
            "SELECT * FROM pdz_results WHERE work_date = CURRENT_DATE ORDER BY created_at"
        )

    def get_pdz_results_last(self) -> tuple:
        """Возвращает (дата, результаты) последнего дня с результатами."""
        row = self._fetchone(
            "SELECT work_date FROM pdz_results ORDER BY work_date DESC, created_at DESC LIMIT 1"
        )
        if not row:
            return None, []
        last_date = row["work_date"]
        results = self._fetchall(
            "SELECT * FROM pdz_results WHERE work_date = %s ORDER BY created_at",
            (last_date,)
        )
        return last_date, results

    # ─── ПЛАНЫ ПРОДАЖ ────────────────────────────────────────────────────────

    def get_mgr_history_cache(self, mgr_tag: str) -> list:
        """Возвращает кэш истории менеджера (обновляется раз в месяц)."""
        import json
        row = self._fetchone(
            """SELECT data FROM mgr_history_cache
               WHERE mgr_tag=%s
               AND DATE_TRUNC('month', updated_at) = DATE_TRUNC('month', NOW())""",
            (mgr_tag,)
        )
        return json.loads(row["data"]) if row else None

    def set_mgr_history_cache(self, mgr_tag: str, data: list):
        """Сохраняет историю менеджера в кэш."""
        import json
        self._execute(
            """INSERT INTO mgr_history_cache (mgr_tag, data, updated_at) VALUES (%s, %s, NOW())
               ON CONFLICT (mgr_tag) DO UPDATE SET data=%s, updated_at=NOW()""",
            (mgr_tag, json.dumps(data, ensure_ascii=False), json.dumps(data, ensure_ascii=False))
        )

    def get_report_cache(self) -> dict:
        """Возвращает закэшированные данные отчёта если они не старше 1 часа."""
        import json
        row = self._fetchone(
            "SELECT data FROM report_cache WHERE updated_at > NOW() - INTERVAL '300 minutes' ORDER BY updated_at DESC LIMIT 1"
        )
        return json.loads(row["data"]) if row else None

    def set_report_cache(self, data: dict):
        """Сохраняет данные отчёта в кэш."""
        import json
        self._execute(
            """INSERT INTO report_cache (data, updated_at) VALUES (%s, NOW())
               ON CONFLICT (id) DO UPDATE SET data=%s, updated_at=NOW()""",
            (json.dumps(data, ensure_ascii=False), json.dumps(data, ensure_ascii=False))
        )

    def create_report_link(self, mgr_filter: str = None, ttl_minutes: int = 60) -> str:
        """Создаёт временную ссылку на отчёт. Возвращает токен."""
        import uuid
        token = str(uuid.uuid4()).replace('-', '')[:24]
        self._execute(
            """INSERT INTO report_links (token, mgr_filter, expires_at)
               VALUES (%s, %s, NOW() + INTERVAL '%s minutes')""",
            (token, mgr_filter, ttl_minutes)
        )
        return token

    def get_report_link(self, token: str) -> dict:
        """Возвращает данные ссылки если она действительна."""
        row = self._fetchone(
            "SELECT token, mgr_filter FROM report_links WHERE token=%s AND expires_at > NOW()",
            (token,)
        )
        return dict(row) if row else None

    def is_agreed_notified(self, order_id: str) -> bool:
        """Проверяет отправляли ли уже уведомление по этому заказу."""
        row = self._fetchone("SELECT order_id FROM agreed_notifications WHERE order_id=%s", (order_id,))
        return row is not None

    def save_agreed_notification(self, order_id: str):
        """Сохраняет факт отправки уведомления."""
        self._execute(
            "INSERT INTO agreed_notifications (order_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (order_id,)
        )

    def try_claim_agreed_notification(self, order_id: str) -> bool:
        """Атомарный claim: пытается отметить заказ как уведомлённый.

        True — запись вставлена этой транзакцией, мы первые → отправляем.
        False — запись уже была (другой webhook/процесс отметил раньше) → молчим.
        """
        self._ensure_connection()
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO agreed_notifications (order_id) VALUES (%s) "
                "ON CONFLICT DO NOTHING RETURNING order_id",
                (order_id,)
            )
            row = cur.fetchone()
            self.conn.commit()
            return row is not None

    def get_aging_alerted(self) -> set:
        """Возвращает ID контрагентов которым уже отправлен алерт."""
        rows = self._fetchall("SELECT counterparty_id FROM aging_alerts")
        return {r["counterparty_id"] for r in rows}

    def save_aging_alert(self, counterparty_id: str, name: str):
        """Сохраняет факт отправки алерта."""
        self._execute(
            """INSERT INTO aging_alerts (counterparty_id, name, alerted_at)
               VALUES (%s, %s, CURRENT_DATE)
               ON CONFLICT (counterparty_id) DO UPDATE SET alerted_at=CURRENT_DATE""",
            (counterparty_id, name)
        )

    def clear_aging_alerts(self):
        """Очищает алерты (вызывать в начале нового цикла если нужно)."""
        self._execute("DELETE FROM aging_alerts")

    def get_promo(self, segment: str = None) -> str:
        """Возвращает промо-текст для сегмента (хорека/опт) или общий."""
        if segment:
            key = f"promo_{segment.lower()}"
            row = self._fetchone("SELECT value FROM bot_settings WHERE key=%s", (key,))
            if row and row["value"]:
                return row["value"]
        row = self._fetchone("SELECT value FROM bot_settings WHERE key='promo_text'", ())
        return row["value"] if row else ""

    def set_promo(self, text: str, segment: str = None):
        """Сохраняет промо-текст для сегмента или общий."""
        key = f"promo_{segment.lower()}" if segment else "promo_text"
        self._execute(
            """INSERT INTO bot_settings (key, value) VALUES (%s, %s)
               ON CONFLICT (key) DO UPDATE SET value=%s""",
            (key, text, text)
        )

    # ── Снимок «% работы на новых» (op_new_share) ────────────────────────────
    # План: 2026-05-21-виджет-процент-новых-в-отчете-оп.md, Фаза 2.
    # Пишется cron-job в scheduler.py (пятница 08:00 МСК) и CLI --backfill.
    # Читается отдельно в handle_web_report, ВНЕ report_cache.

    def get_new_share_snapshot(self):
        """Возвращает dict снимка или None если ключ отсутствует."""
        import json
        row = self._fetchone(
            "SELECT value FROM bot_settings WHERE key='op_new_share_snapshot'", ()
        )
        if not row or not row.get("value"):
            return None
        try:
            return json.loads(row["value"])
        except Exception:
            return None

    def set_new_share_snapshot(self, snapshot: dict):
        """Идемпотентно перезаписывает снимок (JSON в text-value)."""
        import json
        payload = json.dumps(snapshot, ensure_ascii=False)
        self._execute(
            """INSERT INTO bot_settings (key, value) VALUES (%s, %s)
               ON CONFLICT (key) DO UPDATE SET value=%s""",
            ("op_new_share_snapshot", payload, payload)
        )

    def save_manager_chat_id(self, user_id: int, full_name: str):
        self._execute(
            """INSERT INTO manager_chats (user_id, full_name, updated_at)
               VALUES (%s, %s, NOW())
               ON CONFLICT (user_id) DO UPDATE SET full_name=%s, updated_at=NOW()""",
            (user_id, full_name, full_name)
        )

    def log_usage(self, user_id: int, full_name: str, action: str, chat_id: int):
        """Логирует запрос пользователя и обновляет счётчик."""
        try:
            self._execute(
                """INSERT INTO bot_usage_log (user_id, full_name, action, chat_id)
                   VALUES (%s, %s, %s, %s)""",
                (user_id, full_name, action[:200], chat_id)
            )
            self._execute(
                """INSERT INTO manager_chats (user_id, full_name, request_count, last_seen, updated_at)
                   VALUES (%s, %s, 1, NOW(), NOW())
                   ON CONFLICT (user_id) DO UPDATE SET
                     full_name=%s, request_count=manager_chats.request_count+1,
                     last_seen=NOW(), updated_at=NOW()""",
                (user_id, full_name, full_name)
            )
        except Exception as e:
            logger.warning(f"log_usage: {e}")

    def is_user_blocked(self, user_id: int) -> bool:
        row = self._fetchone(
            "SELECT is_blocked FROM manager_chats WHERE user_id=%s", (user_id,)
        )
        return bool(row and row.get("is_blocked"))

    def block_user(self, user_id: int):
        self._execute(
            """INSERT INTO manager_chats (user_id, full_name, is_blocked)
               VALUES (%s, '', TRUE)
               ON CONFLICT (user_id) DO UPDATE SET is_blocked=TRUE""",
            (user_id,)
        )

    def unblock_user(self, user_id: int):
        self._execute(
            "UPDATE manager_chats SET is_blocked=FALSE WHERE user_id=%s", (user_id,)
        )

    def get_usage_stats(self) -> list:
        return self._fetchall(
            """SELECT user_id, full_name, request_count, last_seen, is_blocked
               FROM manager_chats ORDER BY request_count DESC"""
        )

    def get_manager_chat_id(self, name_fragment: str) -> int:
        """Ищет chat_id менеджера по части имени."""
        row = self._fetchone(
            "SELECT user_id FROM manager_chats WHERE LOWER(full_name) LIKE LOWER(%s) LIMIT 1",
            (f"%{name_fragment}%",)
        )
        return row["user_id"] if row else None

    def save_price_alert(self, order_id: str, order_name: str, client_name: str,
                         manager_name: str, manager_user_id: int, alert_text: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO price_alerts (order_id, order_name, client_name, manager_name, manager_user_id, alert_text)
                   VALUES (%s,%s,%s,%s,%s,%s) RETURNING id""",
                (order_id, order_name, client_name, manager_name, manager_user_id, alert_text)
            )
            row = cur.fetchone()
            self.conn.commit()
            return row["id"]

    def get_price_alert(self, alert_id: int) -> dict:
        return self._fetchone("SELECT * FROM price_alerts WHERE id=%s", (alert_id,))

    def close_price_alert(self, alert_id: int, comment: str):
        self._execute(
            "UPDATE price_alerts SET status='answered', alert_text=alert_text||%s WHERE id=%s",
            (f"\n💬 Ответ: {comment}", alert_id)
        )

    # ─── ОБЪЕДИНЁННЫЙ АЛЕРТ «НА СОГЛАСОВАНИИ / ЗА ЛИМИТОМ» ──────────────────────
    # План: 2026-05-21-объединённый-алерт-на-согласование.md (Фаза 4).

    def try_insert_approval_alert(
        self,
        order_id: str,
        sum_hash: int,
        alert_text: str,
        colors_json: dict,
        order_name: str = "",
        client_name: str = "",
        manager_name: str = "",
        manager_user_id: int = None,
    ) -> Optional[int]:
        """
        Атомарная вставка с дедупом по (order_id, sum_hash).
        Возвращает id если алерт реально вставлен; None если запись уже была
        (значит, алерт уже отправлен в прошлый webhook).
        """
        import json
        self._ensure_connection()
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO pending_approval_alerts
                       (order_id, sum_hash, order_name, client_name,
                        manager_name, manager_user_id, alert_text, colors_json)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (order_id, sum_hash) DO NOTHING
                   RETURNING id""",
                (order_id, sum_hash, order_name, client_name,
                 manager_name, manager_user_id, alert_text, json.dumps(colors_json))
            )
            row = cur.fetchone()
            self.conn.commit()
            return row["id"] if row else None

    def get_approval_alert(self, alert_id: int) -> Optional[dict]:
        return self._fetchone(
            "SELECT * FROM pending_approval_alerts WHERE id=%s", (alert_id,)
        )

    def get_approval_alert_by_order(self, order_id: str) -> Optional[dict]:
        """
        Fallback lookup: если callback пришёл с alert_id, которого нет в БД
        (например, БД упала между отправкой и кликом), ищем самый свежий
        открытый алерт по order_id.
        """
        return self._fetchone(
            """SELECT * FROM pending_approval_alerts
               WHERE order_id=%s AND closed_at IS NULL
               ORDER BY id DESC LIMIT 1""",
            (order_id,)
        )

    def close_approval_alert(self, alert_id: int, closed_by: int, comment: str = None):
        if comment:
            self._execute(
                """UPDATE pending_approval_alerts
                   SET closed_at = NOW(), closed_by = %s, comment = %s
                   WHERE id = %s""",
                (closed_by, comment, alert_id)
            )
        else:
            self._execute(
                """UPDATE pending_approval_alerts
                   SET closed_at = NOW(), closed_by = %s
                   WHERE id = %s""",
                (closed_by, alert_id)
            )

    # ─── ЗАДАЧИ ─────────────────────────────────────────────────────────────────

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

    def set_task_bot_message_id(self, task_id: int, bot_message_id: int):
        """Сохраняет message_id сообщения бота о задаче."""
        self._execute(
            "UPDATE tasks SET bot_message_id=%s WHERE id=%s",
            (bot_message_id, task_id)
        )

    def delete_tasks_by_bot_message_id(self, bot_message_id: int, chat_id: int) -> list:
        """Удаляет задачи привязанные к сообщению бота. Возвращает удалённые задачи."""
        rows = self._fetchall(
            "SELECT * FROM tasks WHERE bot_message_id=%s AND source_chat=%s AND status='open'",
            (bot_message_id, chat_id)
        )
        if rows:
            self._execute(
                "DELETE FROM tasks WHERE bot_message_id=%s AND source_chat=%s AND status='open'",
                (bot_message_id, chat_id)
            )
        return rows

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

    def get_tasks_by_executor(self, executor_name: str) -> List[Dict]:
        """Возвращает открытые задачи по имени исполнителя."""
        return self._fetchall(
            """SELECT * FROM tasks WHERE status='open'
               AND LOWER(executor) LIKE LOWER(%s)
               ORDER BY deadline NULLS LAST, created_at""",
            (f"%{executor_name.split()[0]}%",)
        )

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
        """Ищет сообщения с упоминанием ключевых слов (входящие и исходящие)."""
        from datetime import datetime, timedelta
        since = datetime.utcnow() - timedelta(days=days)

        conditions = ["sent_at >= %s", "text IS NOT NULL"]
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
            SELECT
                COALESCE(NULLIF(m.manager_name, ''), cm.manager, 'Неизвестно') AS manager_name,
                m.contact_name,
                COALESCE(cm.company_name, m.contact_name) AS client_name,
                m.chat_type,
                m.text,
                m.sent_at
            FROM wazzup_messages m
            LEFT JOIN wazzup_contact_map cm ON m.chat_id = cm.chat_id
            WHERE {' AND '.join(conditions)}
            ORDER BY manager_name, m.sent_at DESC
        """
        result = self._fetchall(sql, params)
        import logging
        logging.getLogger(__name__).info(f"search_wazzup_mentions: days={days} since={since.date()} keywords={keywords} found={len(result)}")
        return result

    def save_pending_ident(self, link_key: str, chat_id: str, channel_id: str,
                           wazzup_name: str, chat_type: str):
        """Сохраняет ожидающую идентификацию в БД (выживает перезапуск)."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO wazzup_pending_ident (link_key, chat_id, channel_id, wazzup_name, chat_type)
                       VALUES (%s,%s,%s,%s,%s) ON CONFLICT (link_key) DO NOTHING""",
                    (link_key, chat_id, channel_id, wazzup_name, chat_type)
                )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"save_pending_ident: {e}")

    def get_pending_idents(self) -> list:
        """Возвращает все ожидающие идентификации (без отложенных)."""
        return self._fetchall(
            "SELECT * FROM wazzup_pending_ident WHERE retry_after IS NULL OR retry_after <= NOW() ORDER BY created_at",
            []
        )

    def get_retry_idents(self) -> list:
        """Возвращает отложенные идентификации у которых пришло время повтора."""
        return self._fetchall(
            "SELECT * FROM wazzup_pending_ident WHERE retry_after IS NOT NULL AND retry_after <= NOW()",
            []
        )

    def postpone_pending_ident(self, link_key: str, hours: int = 24):
        """Откладывает идентификацию на N часов."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE wazzup_pending_ident SET retry_after = NOW() + INTERVAL '%s hours' WHERE link_key=%s",
                    (hours, link_key)
                )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"postpone_pending_ident: {e}")

    def delete_pending_ident(self, link_key: str):
        """Удаляет идентификацию после завершения."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("DELETE FROM wazzup_pending_ident WHERE link_key=%s", (link_key,))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"delete_pending_ident: {e}")

    def update_contact_manager(self, company_name: str, manager: str):
        """Обновляет поле manager у контакта по имени компании."""
        self._execute(
            "UPDATE wazzup_contact_map SET manager=%s WHERE LOWER(company_name) LIKE LOWER(%s)",
            (manager, f"%{company_name}%")
        )

    def get_all_contacts_with_company(self) -> list:
        """Возвращает все контакты с заполненным company_name."""
        return self._fetchall(
            "SELECT DISTINCT company_name, manager FROM wazzup_contact_map WHERE company_name IS NOT NULL AND company_name != '' AND company_name != '__ignore__'"
        )

    def save_wazzup_contact(self, contact_name: str, chat_id: str,
                            chat_type: str, channel_id: str):
        """Сохраняет или обновляет chatId клиента по каналу."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO wazzup_contacts (contact_name, chat_id, chat_type, channel_id, updated_at)
                       VALUES (%s, %s, %s, %s, NOW())
                       ON CONFLICT (contact_name, chat_type)
                       DO UPDATE SET chat_id=EXCLUDED.chat_id, channel_id=EXCLUDED.channel_id, updated_at=NOW()""",
                    (contact_name, chat_id, chat_type, channel_id)
                )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"save_wazzup_contact error: {e}")

    def get_wazzup_contacts(self, contact_name: str) -> list:
        """Возвращает все известные каналы для контакта по имени или компании."""
        # Сначала ищем по привязке company_name
        rows = self._fetchall(
            "SELECT * FROM wazzup_contact_map WHERE LOWER(company_name) LIKE LOWER(%s) ORDER BY created_at DESC",
            (f"%{contact_name}%",)
        )
        if rows:
            return [{"chat_id": r["chat_id"], "chat_type": r["chat_type"],
                     "channel_id": r["channel_id"]} for r in rows]
        # Fallback — по имени в wazzup_contacts
        return self._fetchall(
            "SELECT * FROM wazzup_contacts WHERE LOWER(contact_name) LIKE LOWER(%s) ORDER BY updated_at DESC",
            (f"%{contact_name}%",)
        )

    def is_wazzup_contact_known(self, chat_id: str) -> bool:
        """Проверяет есть ли уже привязка для этого chatId."""
        row = self._fetchone(
            "SELECT id FROM wazzup_contact_map WHERE chat_id = %s", (chat_id,)
        )
        return row is not None

    def link_wazzup_contact(self, chat_id: str, chat_type: str,
                            channel_id: str, company_name: str,
                            wazzup_name: str = "", role: str = "закупщик",
                            segment: str = "", manager: str = "") -> bool:
        """Привязывает chatId к названию компании и роли."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO wazzup_contact_map
                       (chat_id, chat_type, channel_id, company_name, wazzup_name, role, segment, manager)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (chat_id) DO UPDATE
                       SET company_name=EXCLUDED.company_name, wazzup_name=EXCLUDED.wazzup_name,
                           role=EXCLUDED.role, segment=EXCLUDED.segment, manager=EXCLUDED.manager""",
                    (chat_id, chat_type, channel_id, company_name, wazzup_name, role, segment, manager)
                )
            self.conn.commit()
            # Обновляем и в wazzup_contacts
            with self.conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO wazzup_contacts (contact_name, chat_id, chat_type, channel_id, updated_at)
                       VALUES (%s, %s, %s, %s, NOW())
                       ON CONFLICT (contact_name, chat_type)
                       DO UPDATE SET chat_id=EXCLUDED.chat_id, channel_id=EXCLUDED.channel_id, updated_at=NOW()""",
                    (company_name, chat_id, chat_type, channel_id)
                )
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"link_wazzup_contact error: {e}")
            return False

    def update_wazzup_contact_tags(self, chat_id: str, tags: list,
                                    manager: str = "", segment: str = ""):
        """Обновляет теги, менеджера и сегмент контакта из МойСклад."""
        try:
            tags_str = ", ".join(tags) if tags else ""
            with self.conn.cursor() as cur:
                cur.execute(
                    """UPDATE wazzup_contact_map
                       SET tags=%s, manager=%s, segment=%s
                       WHERE chat_id=%s""",
                    (tags_str, manager, segment, chat_id)
                )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"update_wazzup_contact_tags error: {e}")

    def get_wazzup_broadcast_contacts(self, company_name: str,
                                       roles: list = None) -> list:
        """Возвращает контакты компании подходящие для рассылки (закупщики/директора)."""
        if roles is None:
            roles = ["рассылка"]
        placeholders = ",".join(["%s"] * len(roles))
        return self._fetchall(
            f"""SELECT * FROM wazzup_contact_map
               WHERE LOWER(company_name) LIKE LOWER(%s)
               AND LOWER(role) IN ({placeholders})
               ORDER BY created_at DESC""",
            [f"%{company_name}%"] + roles
        )

    def save_pending_link(self, link_key: str, chat_id: str, channel_id: str,
                          wazzup_name: str, chat_type: str):
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO wazzup_pending (link_key, chat_id, channel_id, wazzup_name, chat_type)
                       VALUES (%s,%s,%s,%s,%s) ON CONFLICT (link_key) DO NOTHING""",
                    (link_key, chat_id, channel_id, wazzup_name, chat_type)
                )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"save_pending_link: {e}")

    def load_pending_links(self) -> list:
        return self._fetchall(
            "SELECT link_key, chat_id, channel_id, wazzup_name, chat_type FROM wazzup_pending"
        )

    def delete_pending_link(self, link_key: str):
        try:
            with self.conn.cursor() as cur:
                cur.execute("DELETE FROM wazzup_pending WHERE link_key=%s", (link_key,))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()

    def save_contract(self, contract_number: str, buyer_name: str,
                      created_by: str, buyer_data: dict = None):
        import json
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO contracts (contract_number, buyer_name, buyer_data, created_by)
                       VALUES (%s, %s, %s, %s) ON CONFLICT (contract_number) DO NOTHING""",
                    (contract_number, buyer_name,
                     json.dumps(buyer_data, ensure_ascii=False) if buyer_data else None,
                     created_by)
                )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"save_contract error: {e}")

    def find_contract_by_buyer(self, buyer_name: str) -> dict:
        """Ищет существующий договор по имени покупателя."""
        import json
        rows = self._fetchall(
            "SELECT * FROM contracts WHERE LOWER(buyer_name) LIKE LOWER(%s) ORDER BY created_at DESC LIMIT 1",
            (f"%{buyer_name}%",)
        )
        if not rows:
            return None
        row = dict(rows[0])
        if row.get("buyer_data") and isinstance(row["buyer_data"], str):
            try:
                row["buyer_data"] = json.loads(row["buyer_data"])
            except Exception:
                pass
        return row

    def get_contracts_by_date(self, date_str: str) -> list:
        return self._fetchall(
            "SELECT * FROM contracts WHERE contract_number LIKE %s ORDER BY id",
            (f"{date_str}%",)
        )

    def save_call_transcript(self, call_id: str, src_num: str, dst_num: str,
                             manager_name: str, tree_name: str, transcript: str,
                             duration_sec: int = 0, called_at: str = None) -> bool:
        """Сохраняет транскрипцию звонка."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO call_transcripts
                       (call_id, src_num, dst_num, manager_name, tree_name, transcript, duration_sec, called_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, COALESCE(%s::timestamp, NOW()))
                       ON CONFLICT (call_id) DO NOTHING RETURNING id""",
                    (call_id, src_num, dst_num, manager_name, tree_name,
                     transcript, duration_sec, called_at)
                )
                row = cur.fetchone()
                self.conn.commit()
                return row is not None
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"save_call_transcript error: {e}")
            return False

    def search_call_mentions(self, keywords: list, days: int = 7,
                             manager_name: str = None) -> list:
        """Ищет упоминания ключевых слов в транскрипциях звонков."""
        from datetime import datetime, timedelta
        since = datetime.now() - timedelta(days=days)

        conditions = ["called_at >= %s", "transcript IS NOT NULL"]
        params = [since]

        if manager_name:
            conditions.append("LOWER(manager_name) LIKE %s")
            params.append(f"%{manager_name.lower()}%")

        kw_conditions = " OR ".join(["LOWER(transcript) LIKE %s"] * len(keywords))
        conditions.append(f"({kw_conditions})")
        for kw in keywords:
            params.append(f"%{kw.lower()}%")

        sql = f"""
            SELECT manager_name, src_num, tree_name, transcript, called_at
            FROM call_transcripts
            WHERE {' AND '.join(conditions)}
            ORDER BY manager_name, called_at DESC
        """
        return self._fetchall(sql, params)

    def get_manager_activity(self, days: int = 7, manager_name: str = None) -> list:
        """Отчёт по активности менеджеров: звонки + сообщения за период."""
        from datetime import datetime, timedelta
        since = datetime.now() - timedelta(days=days)

        # Сообщения — берём менеджера из contact_map если в сообщении пустой
        mgr_filter = "AND LOWER(COALESCE(NULLIF(m.manager_name,''), cm.manager, '')) LIKE %s" if manager_name else ""
        mgr_params = [since] + ([f"%{manager_name.lower()}%"] if manager_name else [])
        msg_rows = self._fetchall(
            f"""SELECT
                COALESCE(NULLIF(m.manager_name,''), cm.manager, 'Неизвестно') AS manager_name,
                COUNT(*) as msg_count,
                COUNT(DISTINCT m.chat_id) as client_count
                FROM wazzup_messages m
                LEFT JOIN wazzup_contact_map cm ON m.chat_id = cm.chat_id
                WHERE m.is_outbound = TRUE AND m.sent_at >= %s
                {mgr_filter}
                GROUP BY COALESCE(NULLIF(m.manager_name,''), cm.manager, 'Неизвестно')""",
            mgr_params
        )

        # Звонки
        call_filter = "AND LOWER(manager_name) LIKE %s" if manager_name else ""
        call_params = [since] + ([f"%{manager_name.lower()}%"] if manager_name else [])
        call_rows = self._fetchall(
            f"""SELECT manager_name,
                COUNT(*) as call_count,
                COUNT(DISTINCT src_num) as caller_count,
                ROUND(AVG(duration_sec)) as avg_duration
                FROM call_transcripts
                WHERE called_at >= %s
                AND manager_name IS NOT NULL AND manager_name != ''
                {call_filter}
                GROUP BY manager_name""",
            call_params
        )

        # Объединяем
        result = {}
        for r in msg_rows:
            mgr = r["manager_name"]
            if mgr == "Неизвестно":
                continue
            result[mgr] = {
                "manager": mgr,
                "msg_count": r["msg_count"],
                "msg_clients": r["client_count"],
                "call_count": 0,
                "call_clients": 0,
                "avg_duration": 0,
            }
        for r in call_rows:
            mgr = r["manager_name"]
            if mgr not in result:
                result[mgr] = {"manager": mgr, "msg_count": 0, "msg_clients": 0}
            result[mgr]["call_count"] = r["call_count"]
            result[mgr]["call_clients"] = r["caller_count"]
            result[mgr]["avg_duration"] = int(r["avg_duration"] or 0)

        return sorted(result.values(), key=lambda x: x.get("msg_count", 0) + x.get("call_count", 0), reverse=True)

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
