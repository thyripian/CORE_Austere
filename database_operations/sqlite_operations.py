# database_operations/sqlite_operations.py

import sqlite3
import threading
from typing import List, Dict, Any

class SQLiteDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: sqlite3.Connection = None
        self.cursor: sqlite3.Cursor = None
        # Lock to serialize access if multiple threads invoke methods
        self._lock = threading.Lock()

    def connect(self) -> None:
        # Allow this connection to be used across threads
        self.conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False
        )
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    def list_tables(self) -> List[str]:
        with self._lock:
            self.cursor.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%';"
            )
            return [row["name"] for row in self.cursor.fetchall()]

    def list_columns(self, table_name: str) -> List[str]:
        with self._lock:
            self.cursor.execute(f"PRAGMA table_info({table_name});")
            return [row["name"] for row in self.cursor.fetchall()]

    def search_table(
        self,
        table_name: str,
        query: str,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        with self._lock:
            # Identify text columns
            self.cursor.execute(f"PRAGMA table_info({table_name});")
            rows = self.cursor.fetchall()
            text_columns = [
                row["name"]
                for row in rows
                if row["type"].lower() in ("text", "varchar")
            ]
            if not text_columns:
                raise ValueError("No text columns available for search.")

            # Build and execute LIKE query
            where_clause = " OR ".join(f"{col} LIKE ?" for col in text_columns)
            values = [f"%{query}%"] * len(text_columns)
            sql = f"SELECT * FROM {table_name} WHERE {where_clause} LIMIT ?"
            self.cursor.execute(sql, (*values, limit))
            return [dict(row) for row in self.cursor.fetchall()]
