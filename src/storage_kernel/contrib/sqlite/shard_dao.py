"""SQLite による ShardDAOPort および ShardTxPort の参照実装。"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
import sqlite3
import threading
from typing import Any, Generic, Literal, TypeVar, cast

from ...contracts.envelope import DurabilityLevel, RecordEnvelope
from ...contracts.errors import DuplicateKeyError
from ...ports.codec import PayloadCodec
from ._transaction import immediate_transaction

T = TypeVar("T")


def _synchronous_pragma(durability: DurabilityLevel) -> str:
    if durability == "power_loss":
        return "FULL"
    if durability == "process_crash":
        return "NORMAL"
    return "OFF"


def _classify_integrity_error(message: str) -> Literal["sequence", "idempotency_key", "unknown"]:
    if "records.partition_key, records.seq" in message or "PRIMARY KEY" in message:
        return "sequence"
    if "records.partition_key, records.idempotency_key" in message or "idempotency_key" in message:
        return "idempotency_key"
    return "unknown"


class _SqliteShardTx(Generic[T]):
    """単一シャード DB コネクション上の排他書き込みトランザクション。"""

    def __init__(self, conn: sqlite3.Connection, codec: PayloadCodec[T] | None) -> None:
        self._conn = conn
        self._codec = codec

    def _encode(self, payload: Any) -> bytes:
        if self._codec is not None:
            return self._codec.encode(payload)
        if isinstance(payload, bytes):
            return payload
        raise TypeError("codec is required when shard payload is not bytes")

    def _decode(self, raw: bytes) -> T:
        if self._codec is not None:
            return self._codec.decode(raw)
        return cast(T, raw)

    def _row_to_envelope(self, row: sqlite3.Row) -> RecordEnvelope[T]:
        return RecordEnvelope(
            partition_key=str(row["partition_key"]),
            seq=int(row["seq"]),
            idempotency_key=str(row["idempotency_key"]),
            record_type=str(row["record_type"]),
            schema_version=int(row["schema_version"]),
            payload_hash=str(row["payload_hash"]),
            payload=self._decode(bytes(row["payload"])),
            created_at=float(row["created_at"]),
            covers_through_seq=int(row["covers_through_seq"]) if row["covers_through_seq"] is not None else None,
        )

    def find_by_idempotency(self, partition_key: str, idempotency_key: str) -> RecordEnvelope[T] | None:
        row = self._conn.execute(
            """
            SELECT partition_key, seq, idempotency_key, record_type, schema_version,
                   payload_hash, payload, created_at, covers_through_seq
            FROM records
            WHERE partition_key = ? AND idempotency_key = ?
            """,
            (partition_key, idempotency_key),
        ).fetchone()
        return self._row_to_envelope(row) if row is not None else None

    def next_sequence(self, partition_key: str) -> int:
        self._conn.execute(
            """
            INSERT INTO sequences (partition_key, last_seq)
            VALUES (?, 1)
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

    def insert_record(self, envelope: RecordEnvelope[T]) -> None:
        raw_payload = self._encode(envelope.payload)
        try:
            self._conn.execute(
                """
                INSERT INTO records (
                    partition_key, seq, idempotency_key, record_type, schema_version,
                    payload_hash, payload, created_at, covers_through_seq
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    envelope.partition_key,
                    envelope.seq,
                    envelope.idempotency_key,
                    envelope.record_type,
                    envelope.schema_version,
                    envelope.payload_hash,
                    raw_payload,
                    envelope.created_at,
                    envelope.covers_through_seq,
                ),
            )
        except sqlite3.IntegrityError as exc:
            kind = _classify_integrity_error(str(exc))
            raise DuplicateKeyError(str(exc), kind=kind) from exc


class SqliteShardDAO(Generic[T]):
    """shard_*.db にイミュータブル・レコードと連番を永続化する参照 ShardDAOPort 実装。"""

    def __init__(
        self,
        base_dir: str | Path,
        *,
        codec: PayloadCodec[T] | None = None,
        durability: DurabilityLevel = "power_loss",
    ) -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._codec = codec
        self._durability: DurabilityLevel = durability
        self._locks_guard = threading.Lock()
        self._shard_locks: dict[str, threading.RLock] = {}
        self._initialized_shards: set[str] = set()

    @property
    def durability(self) -> DurabilityLevel:
        return self._durability

    def _shard_path(self, shard_id: str) -> Path:
        return self._base_dir / f"{shard_id}.db"

    def _get_shard_lock(self, shard_id: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._shard_locks.get(shard_id)
            if lock is None:
                lock = threading.RLock()
                self._shard_locks[shard_id] = lock
            return lock

    @contextmanager
    def _connect(self, shard_id: str) -> Iterator[sqlite3.Connection]:
        db_path = self._shard_path(shard_id)
        conn = sqlite3.connect(
            db_path,
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

    def _ensure_shard_schema(self, shard_id: str) -> None:
        if shard_id in self._initialized_shards:
            return
        with self._get_shard_lock(shard_id):
            if shard_id in self._initialized_shards:
                return
            with self._connect(shard_id) as conn:
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
                        payload BLOB NOT NULL,
                        created_at REAL NOT NULL,
                        covers_through_seq INTEGER,
                        PRIMARY KEY (partition_key, seq),
                        UNIQUE (partition_key, idempotency_key)
                    );
                    """
                )
            self._initialized_shards.add(shard_id)

    @contextmanager
    def write_transaction(self, shard_id: str) -> Iterator[_SqliteShardTx[T]]:
        self._ensure_shard_schema(shard_id)
        with self._get_shard_lock(shard_id), self._connect(shard_id) as conn, immediate_transaction(conn):
            yield _SqliteShardTx(conn, self._codec)

    def fetch_range(
        self,
        shard_id: str,
        partition_key: str,
        after_seq: int,
        limit: int,
    ) -> Sequence[RecordEnvelope[T]]:
        if limit <= 0:
            return ()
        self._ensure_shard_schema(shard_id)
        with self._connect(shard_id) as conn:
            rows = conn.execute(
                """
                SELECT partition_key, seq, idempotency_key, record_type, schema_version,
                       payload_hash, payload, created_at, covers_through_seq
                FROM records
                WHERE partition_key = ? AND seq > ?
                ORDER BY seq ASC
                LIMIT ?
                """,
                (partition_key, after_seq, limit),
            ).fetchall()
            tx = _SqliteShardTx(conn, self._codec)
            return tuple(tx._row_to_envelope(r) for r in rows)
