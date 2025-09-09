# src/storage/state.py
import sqlite3
from pathlib import Path
from typing import Optional, Tuple

class StateDB:
    def __init__(self, db_path: Path):
        # Ensure parent directory exists BEFORE opening SQLite
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure()

    def _ensure(self):
        con = sqlite3.connect(self.db_path)
        try:
            cur = con.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS queue (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  url TEXT UNIQUE,
                  depth INTEGER,
                  source TEXT,
                  record_type TEXT,
                  added_ts REAL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS seen_url (
                  url_hash TEXT PRIMARY KEY
                )
            """)
            con.commit()
        finally:
            con.close()

    def push(self, url: str, depth: int, source: str, record_type: str):
        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                "INSERT OR IGNORE INTO queue(url, depth, source, record_type, added_ts) "
                "VALUES (?,?,?,?,strftime('%s','now'))",
                (url, depth, source, record_type),
            )
            con.commit()
        finally:
            con.close()

    def pop(self) -> Optional[Tuple[str, int, str, str]]:
        con = sqlite3.connect(self.db_path)
        try:
            cur = con.execute(
                "SELECT id, url, depth, source, record_type FROM queue ORDER BY id LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                return None
            qid, url, depth, source, record_type = row
            con.execute("DELETE FROM queue WHERE id=?", (qid,))
            con.commit()
            return url, depth, source, record_type
        finally:
            con.close()

    def mark_seen(self, url_hash: str):
        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                "INSERT OR IGNORE INTO seen_url(url_hash) VALUES (?)", (url_hash,)
            )
            con.commit()
        finally:
            con.close()

    def is_seen(self, url_hash: str) -> bool:
        con = sqlite3.connect(self.db_path)
        try:
            cur = con.execute(
                "SELECT 1 FROM seen_url WHERE url_hash=? LIMIT 1", (url_hash,)
            )
            return cur.fetchone() is not None
        finally:
            con.close()
