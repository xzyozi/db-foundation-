"""親プロジェクト固有の Adapter 実装例（SQLite FTS5 Projection・MaintenanceHooks）。"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
import sqlite3
import threading
from typing import Any

from storage_kernel import RecordEnvelope


class SqliteFts5Projection:
    """(partition_key, seq) 単位の事前削除＋再挿入で冪等性を担保する FTS5 Projection 例。"""

    def __init__(self, db_path: str | Path, consumer_id: str = "parent_fts5_v1") -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._consumer_id = consumer_id
        self._lock = threading.RLock()
        self._init_schema()

    @property
    def consumer_id(self) -> str:
        return self._consumer_id

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=15.0, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.executescript(
                    """
                    CREATE VIRTUAL TABLE IF NOT EXISTS records_fts USING fts5(
                        partition_key UNINDEXED,
                        seq UNINDEXED,
                        record_type,
                        content
                    );
                    """
                )
            finally:
                conn.close()

    def apply_batch(self, records: Sequence[RecordEnvelope[dict[str, Any]]]) -> None:
        """同一トランザクション内で (partition_key, seq) を事前削除してから挿入し冪等性を保つ。"""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                try:
                    for rec in records:
                        conn.execute(
                            "DELETE FROM records_fts WHERE partition_key = ? AND seq = ?",
                            (rec.partition_key, rec.seq),
                        )
                        searchable = " ".join(str(v) for v in rec.payload.values())
                        conn.execute(
                            "INSERT INTO records_fts (partition_key, seq, record_type, content) VALUES (?, ?, ?, ?)",
                            (rec.partition_key, rec.seq, rec.record_type, searchable),
                        )
                    conn.execute("COMMIT;")
                except Exception:
                    conn.execute("ROLLBACK;")
                    raise
            finally:
                conn.close()

    def search(self, query: str, *, limit: int = 20) -> list[dict[str, Any]]:
        """FTS5 インデックスを用いて全文検索を実行する。"""
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    """
                    SELECT partition_key, seq, record_type, content
                    FROM records_fts
                    WHERE records_fts MATCH ?
                    ORDER BY rank LIMIT ?
                    """,
                    (query, limit),
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()


class ParentMaintenanceHooks:
    """メンテナンス整合点でバックアップ等の処理を行う親側フック例。"""

    def __init__(self) -> None:
        self.events: list[str] = []

    def on_enter_maintenance(self, reason: str) -> None:
        self.events.append(f"enter:{reason}")

    def on_exit_maintenance(self) -> None:
        self.events.append("exit")
