def _reconnect(self):
    try:
        self.conn.close()
    except Exception:
        pass
    self.conn = psycopg2.connect(
        DATABASE_URL, 
        cursor_factory=psycopg2.extras.RealDictCursor
    )
    self.conn.autocommit = False

def _execute(self, sql: str, params=None):
    for attempt in range(3):
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, params or ())
                self.conn.commit()
                return cur
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            self._reconnect()
    raise Exception("DB execute failed after 3 attempts")

def _fetchall(self, sql: str, params=None) -> List[Dict]:
    for attempt in range(3):
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, params or ())
                return [dict(r) for r in cur.fetchall()]
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            self._reconnect()
    return []

def _fetchone(self, sql: str, params=None) -> Optional[Dict]:
    for attempt in range(3):
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, params or ())
                row = cur.fetchone()
                return dict(row) if row else None
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            self._reconnect()
    return None
