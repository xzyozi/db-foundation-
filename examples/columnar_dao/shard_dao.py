"""ペイロード（dict）を業務カラムへ展開して保存する ShardDAOPort の実装例。

`StorageKernel(store_raw_bytes=False)` と組み合わせ、DAO がペイロード `T`（ここでは dict）を直接受け取る。
参照実装（`storage_kernel.contrib.sqlite`）は、ペイロードを BLOB として保存する。この例は、親プロジェクトが
「業務カラムとして保存したい」場合に、DAO の自由度が実際に成立するかを確かめるためのもの。

- 既知のキー（`symbol`・`qty`・`price`）は、型が合うときだけ専用カラムへ保存する。
- それ以外のキー、および型が合わない値は、`extra`（JSON）に保存する。往復で値を変えないための扱い。
- 親プロジェクトが書く想定のため、`storage_kernel.contrib` の内部には依存しない。
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
import json
import logging
from pathlib import Path
import sqlite3
import threading
from typing import Any, Literal

from storage_kernel import DuplicateKeyError, RecordEnvelope
from storage_kernel.contracts.envelope import DurabilityLevel

logger = logging.getLogger(__name__)

# 専用カラムへ展開するキーと、受け付ける Python の型。bool は int のサブクラスなので除外する。
_COLUMNS: dict[str, tuple[type, ...]] = {
    "symbol": (str,),
    "qty": (int,),
    "price": (int, float),
}


def _split_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """ペイロードを (専用カラムの値, extra) に分ける。型が合わない値は extra に残す。"""
    columns: dict[str, Any] = {}
    extra: dict[str, Any] = {}
    for key, value in payload.items():
        accepted = _COLUMNS.get(key)
        if accepted is not None and isinstance(value, accepted) and not isinstance(value, bool):
            columns[key] = value
        else:
            extra[key] = value
    return columns, extra


def _classify_integrity_error(message: str) -> Literal["sequence", "idempotency_key", "unknown"]:
    """SQLite の一意制約違反のメッセージから、どの制約かを判定する。"""
    if "idempotency_key" in message:
        return "idempotency_key"
    if "records.seq" in message or "PRIMARY KEY" in message:
        return "sequence"
    return "unknown"


class _ColumnarTx:
    """単一シャードの排他書き込みトランザクション。"""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def find_by_idempotency(self, partition_key: str, idempotency_key: str) -> RecordEnvelope[dict[str, Any]] | None:
        row = self._conn.execute(
            f"{_SELECT} WHERE partition_key = ? AND idempotency_key = ?",
            (partition_key, idempotency_key),
        ).fetchone()
        return _row_to_envelope(row) if row is not None else None

    def next_sequence(self, partition_key: str) -> int:
        self._conn.execute(
            """
            INSERT INTO sequences (partition_key, last_seq) VALUES (?, 1)
            ON CONFLICT(partition_key) DO UPDATE SET last_seq = last_seq + 1
            """,
            (partition_key,),
        )
        row = self._conn.execute(
            "SELECT last_seq FROM sequences WHERE partition_key = ?",
            (partition_key,),
        ).fetchone()
        assert row is not None
        return int(row["last_seq"])

    def insert_record(self, envelope: RecordEnvelope[dict[str, Any]]) -> None:
        columns, extra = _split_payload(envelope.payload)
        try:
            self._conn.execute(
                """
                INSERT INTO records (
                    partition_key, seq, idempotency_key, record_type, schema_version, payload_hash,
                    created_at, covers_through_seq, symbol, qty, price, extra
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    envelope.partition_key,
                    envelope.seq,
                    envelope.idempotency_key,
                    envelope.record_type,
                    envelope.schema_version,
                    envelope.payload_hash,
                    envelope.created_at,
                    envelope.covers_through_seq,
                    columns.get("symbol"),
                    columns.get("qty"),
                    columns.get("price"),
                    json.dumps(extra, sort_keys=True, ensure_ascii=False),
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise DuplicateKeyError(str(exc), kind=_classify_integrity_error(str(exc))) from exc


_SELECT = """
    SELECT partition_key, seq, idempotency_key, record_type, schema_version, payload_hash,
           created_at, covers_through_seq, symbol, qty, price, extra
    FROM records
"""


def _row_to_envelope(row: sqlite3.Row) -> RecordEnvelope[dict[str, Any]]:
    """専用カラムと extra から、元の dict を復元する。"""
    payload: dict[str, Any] = json.loads(row["extra"])
    for name in _COLUMNS:
        if row[name] is not None:
            payload[name] = row[name]
    return RecordEnvelope(
        partition_key=str(row["partition_key"]),
        seq=int(row["seq"]),
        idempotency_key=str(row["idempotency_key"]),
        record_type=str(row["record_type"]),
        schema_version=int(row["schema_version"]),
        payload_hash=str(row["payload_hash"]),
        payload=payload,
        created_at=float(row["created_at"]),
        covers_through_seq=int(row["covers_through_seq"]) if row["covers_through_seq"] is not None else None,
    )


class ColumnarShardDAO:
    """業務カラム展開の ShardDAOPort 実装例（SQLite、シャードごとに 1 ファイル）。"""

    def __init__(self, base_dir: str | Path, *, durability: DurabilityLevel = "power_loss") -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._durability: DurabilityLevel = durability
        self._guard = threading.Lock()
        self._locks: dict[str, threading.RLock] = {}
        self._ready: set[str] = set()

    @property
    def durability(self) -> DurabilityLevel:
        return self._durability

    def _lock(self, shard_id: str) -> threading.RLock:
        with self._guard:
            return self._locks.setdefault(shard_id, threading.RLock())

    @contextmanager
    def _connect(self, shard_id: str) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(
            self._base_dir / f"{shard_id}.db",
            timeout=15.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(f"PRAGMA synchronous={'FULL' if self._durability == 'power_loss' else 'NORMAL'};")
            conn.execute("PRAGMA busy_timeout=5000;")
            yield conn
        finally:
            conn.close()

    def _ensure_schema(self, shard_id: str) -> None:
        if shard_id in self._ready:
            return
        with self._lock(shard_id), self._connect(shard_id) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sequences (
                    partition_key TEXT PRIMARY KEY,
                    last_seq INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS records (
                    partition_key TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    record_type TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    payload_hash TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    covers_through_seq INTEGER,
                    symbol TEXT,
                    qty INTEGER,
                    price REAL,
                    extra TEXT NOT NULL,
                    PRIMARY KEY (partition_key, seq),
                    UNIQUE (partition_key, idempotency_key)
                );
                CREATE INDEX IF NOT EXISTS idx_records_symbol ON records (symbol);
                """
            )
            self._ready.add(shard_id)

    @contextmanager
    def write_transaction(self, shard_id: str) -> Iterator[_ColumnarTx]:
        self._ensure_schema(shard_id)
        with self._lock(shard_id), self._connect(shard_id) as conn:
            conn.execute("BEGIN IMMEDIATE;")
            try:
                yield _ColumnarTx(conn)
                conn.execute("COMMIT;")
            except Exception:
                try:
                    conn.execute("ROLLBACK;")
                except sqlite3.Error:
                    logger.warning("ROLLBACK に失敗しました（元の例外を優先して送出します）", exc_info=True)
                raise

    def fetch_range(
        self, shard_id: str, partition_key: str, after_seq: int, limit: int
    ) -> Sequence[RecordEnvelope[dict[str, Any]]]:
        if limit <= 0:
            return ()
        self._ensure_schema(shard_id)
        with self._connect(shard_id) as conn:
            rows = conn.execute(
                f"{_SELECT} WHERE partition_key = ? AND seq > ? ORDER BY seq ASC LIMIT ?",
                (partition_key, after_seq, limit),
            ).fetchall()
            return tuple(_row_to_envelope(r) for r in rows)

    def count_by_symbol(self, shard_id: str, symbol: str) -> int:
        """業務カラムでの検索例。ペイロードを BLOB で持つ DAO では、全件を読み出さないとできない。"""
        self._ensure_schema(shard_id)
        with self._connect(shard_id) as conn:
            row = conn.execute("SELECT COUNT(*) AS n FROM records WHERE symbol = ?", (symbol,)).fetchone()
            return int(row["n"])
