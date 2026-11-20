"""SQLite による MetaDAOPort および DeadLetterPort の参照実装。"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Generic, TypeVar, cast

from ...contracts.envelope import (
    DeadLetterRecord,
    DurabilityLevel,
    PartitionInfo,
    PartitionStatus,
    RecordEnvelope,
)
from ...contracts.errors import PartitionStateConflictError
from ...ports.codec import PayloadCodec
from ._transaction import immediate_transaction

T = TypeVar("T")


def _synchronous_pragma(durability: DurabilityLevel) -> str:
    if durability == "power_loss":
        return "FULL"
    if durability == "process_crash":
        return "NORMAL"
    return "OFF"


class SqliteMetaDAO:
    """meta.db にパーティション台帳とカーソルを永続化する参照 MetaDAOPort 実装。"""

    def __init__(
        self,
        db_path: str | Path,
        *,
        durability: DurabilityLevel = "power_loss",
    ) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._durability: DurabilityLevel = durability
        self._lock = threading.RLock()
        self._init_schema()

    @property
    def durability(self) -> DurabilityLevel:
        return self._durability

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(
            self._db_path,
            timeout=15.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(f"PRAGMA synchronous={_synchronous_pragma(self._durability)};")
            conn.execute("PRAGMA busy_timeout=5000;")
            yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS partitions (
                    partition_key TEXT PRIMARY KEY,
                    shard_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'read_only')),
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cursors (
                    consumer_id TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    last_seq INTEGER NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (consumer_id, partition_key)
                );
                """
            )

    @staticmethod
    def _row_to_partition(row: sqlite3.Row) -> PartitionInfo:
        status = cast(PartitionStatus, str(row["status"]))
        return PartitionInfo(
            partition_key=str(row["partition_key"]),
            shard_id=str(row["shard_id"]),
            status=status,
            created_at=float(row["created_at"]),
        )

    def get_partition(self, partition_key: str) -> PartitionInfo | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT partition_key, shard_id, status, created_at FROM partitions WHERE partition_key = ?",
                (partition_key,),
            ).fetchone()
            return self._row_to_partition(row) if row is not None else None

    def bind_shard(self, partition_key: str, shard_id: str) -> PartitionInfo:
        with self._lock, self._connect() as conn, immediate_transaction(conn):
            conn.execute(
                """
                INSERT OR IGNORE INTO partitions (partition_key, shard_id, status, created_at)
                VALUES (?, ?, 'active', ?)
                """,
                (partition_key, shard_id, time.time()),
            )
            row = conn.execute(
                "SELECT partition_key, shard_id, status, created_at FROM partitions WHERE partition_key = ?",
                (partition_key,),
            ).fetchone()
            assert row is not None
            return self._row_to_partition(row)

    def set_partition_status(
        self,
        partition_key: str,
        status: PartitionStatus,
        *,
        expected_status: PartitionStatus | None = None,
    ) -> PartitionInfo:
        with self._lock, self._connect() as conn, immediate_transaction(conn):
            row = conn.execute(
                "SELECT partition_key, shard_id, status, created_at FROM partitions WHERE partition_key = ?",
                (partition_key,),
            ).fetchone()
            if row is None:
                raise KeyError(f"partition not registered: {partition_key}")
            current = self._row_to_partition(row)
            if expected_status is not None and current.status != expected_status:
                raise PartitionStateConflictError(
                    f"expected status {expected_status!r} for {partition_key}, got {current.status!r}"
                )
            conn.execute(
                "UPDATE partitions SET status = ? WHERE partition_key = ?",
                (status, partition_key),
            )
            updated_row = conn.execute(
                "SELECT partition_key, shard_id, status, created_at FROM partitions WHERE partition_key = ?",
                (partition_key,),
            ).fetchone()
            assert updated_row is not None
            return self._row_to_partition(updated_row)

    def list_partitions(self, *, status: PartitionStatus | None = None) -> Sequence[PartitionInfo]:
        with self._lock, self._connect() as conn:
            if status is None:
                rows = conn.execute(
                    "SELECT partition_key, shard_id, status, created_at FROM partitions ORDER BY partition_key ASC"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT partition_key, shard_id, status, created_at "
                    "FROM partitions WHERE status = ? ORDER BY partition_key ASC",
                    (status,),
                ).fetchall()
            return tuple(self._row_to_partition(r) for r in rows)

    def get_cursor(self, consumer_id: str, partition_key: str) -> int:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT last_seq FROM cursors WHERE consumer_id = ? AND partition_key = ?",
                (consumer_id, partition_key),
            ).fetchone()
            return int(row["last_seq"]) if row is not None else 0

    def advance_cursor(self, consumer_id: str, partition_key: str, seq: int) -> None:
        with self._lock, self._connect() as conn, immediate_transaction(conn):
            conn.execute(
                """
                INSERT INTO cursors (consumer_id, partition_key, last_seq, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(consumer_id, partition_key) DO UPDATE SET
                    last_seq = MAX(cursors.last_seq, excluded.last_seq),
                    updated_at = excluded.updated_at
                """,
                (consumer_id, partition_key, seq, time.time()),
            )


class SqliteDeadLetterDAO(Generic[T]):
    """meta.db 等に Poison Pill を隔離する参照 DeadLetterPort 実装。"""

    def __init__(
        self,
        db_path: str | Path,
        *,
        codec: PayloadCodec[T] | None = None,
        durability: DurabilityLevel = "power_loss",
    ) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._codec = codec
        self._durability: DurabilityLevel = durability
        self._lock = threading.RLock()
        self._init_schema()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(
            self._db_path,
            timeout=15.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(f"PRAGMA synchronous={_synchronous_pragma(self._durability)};")
            conn.execute("PRAGMA busy_timeout=5000;")
            yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS dead_letters (
                    consumer_id TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    record_type TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    payload_hash TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    created_at REAL NOT NULL,
                    covers_through_seq INTEGER,
                    error_type TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    failed_at REAL NOT NULL,
                    PRIMARY KEY (consumer_id, partition_key, seq)
                );
                """
            )

    def _encode_payload(self, payload: Any) -> bytes:
        if self._codec is not None:
            return self._codec.encode(payload)
        if isinstance(payload, bytes):
            return payload
        raise TypeError("codec is required when dead letter payload is not bytes")

    def _decode_payload(self, raw: bytes) -> T:
        if self._codec is not None:
            return self._codec.decode(raw)
        return cast(T, raw)

    def put_dead_letter(self, record: DeadLetterRecord[T]) -> None:
        env = record.envelope
        raw_payload = self._encode_payload(env.payload)
        with self._lock, self._connect() as conn, immediate_transaction(conn):
            conn.execute(
                """
                INSERT INTO dead_letters (
                    consumer_id, partition_key, seq, idempotency_key,
                    record_type, schema_version, payload_hash, payload,
                    created_at, covers_through_seq, error_type, error_message,
                    attempts, failed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(consumer_id, partition_key, seq) DO UPDATE SET
                    error_type = excluded.error_type,
                    error_message = excluded.error_message,
                    attempts = excluded.attempts,
                    failed_at = excluded.failed_at
                """,
                (
                    record.consumer_id,
                    env.partition_key,
                    env.seq,
                    env.idempotency_key,
                    env.record_type,
                    env.schema_version,
                    env.payload_hash,
                    raw_payload,
                    env.created_at,
                    env.covers_through_seq,
                    record.error_type,
                    record.error_message,
                    record.attempts,
                    record.failed_at,
                ),
            )

    def list_dead_letters(
        self,
        *,
        consumer_id: str | None = None,
        limit: int = 100,
    ) -> Sequence[DeadLetterRecord[T]]:
        if limit <= 0:
            return ()
        with self._lock, self._connect() as conn:
            if consumer_id is None:
                rows = conn.execute(
                    "SELECT * FROM dead_letters ORDER BY failed_at ASC, seq ASC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM dead_letters WHERE consumer_id = ? ORDER BY failed_at ASC, seq ASC LIMIT ?",
                    (consumer_id, limit),
                ).fetchall()
            result: list[DeadLetterRecord[T]] = []
            for r in rows:
                env = RecordEnvelope(
                    partition_key=str(r["partition_key"]),
                    seq=int(r["seq"]),
                    idempotency_key=str(r["idempotency_key"]),
                    record_type=str(r["record_type"]),
                    schema_version=int(r["schema_version"]),
                    payload_hash=str(r["payload_hash"]),
                    payload=self._decode_payload(bytes(r["payload"])),
                    created_at=float(r["created_at"]),
                    covers_through_seq=int(r["covers_through_seq"]) if r["covers_through_seq"] is not None else None,
                )
                result.append(
                    DeadLetterRecord(
                        consumer_id=str(r["consumer_id"]),
                        envelope=env,
                        error_type=str(r["error_type"]),
                        error_message=str(r["error_message"]),
                        attempts=int(r["attempts"]),
                        failed_at=float(r["failed_at"]),
                    )
                )
            return tuple(result)
