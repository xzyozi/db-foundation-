"""PostgreSQL による ShardDAOPort および ShardTxPort の試作実装。"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
import threading
from typing import Any, Generic, Literal, TypeVar, cast

import psycopg
from psycopg import errors, sql

from storage_kernel import DuplicateKeyError, RecordEnvelope
from storage_kernel.contracts.envelope import DurabilityLevel
from storage_kernel.ports.codec import PayloadCodec

from ._db import ensure_schema_exists, open_connection, table_ident, write_tx

T = TypeVar("T")


def _classify_unique_violation(
    exc: errors.UniqueViolation,
) -> Literal["sequence", "idempotency_key", "unknown"]:
    """PostgreSQL の一意制約違反から DuplicateKeyError.kind を判定する。"""
    constraint = exc.diag.constraint_name or ""
    if constraint == "pk_records" or ("seq" in constraint and "idempotency" not in constraint):
        return "sequence"
    if constraint == "uq_records_idempotency" or "idempotency" in constraint:
        return "idempotency_key"
    message = str(exc)
    if "pk_records" in message or "(shard_id, partition_key, seq)" in message:
        return "sequence"
    if "uq_records_idempotency" in message or "idempotency_key" in message:
        return "idempotency_key"
    return "unknown"


def _normalize_bytes(raw: Any) -> bytes:
    """BYTEA 列の読み戻し値（bytes / memoryview 等）を bytes へ正規化する。"""
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, (bytearray, memoryview)):
        return bytes(raw)
    raise TypeError(f"expected bytes-like payload from BYTEA column, got {type(raw).__name__}")


class _PostgresShardTx(Generic[T]):
    """単一シャードの書き込みトランザクション。"""

    def __init__(
        self,
        conn: psycopg.Connection[tuple[Any, ...]],
        shard_id: str,
        codec: PayloadCodec[T] | None,
        schema: str,
    ) -> None:
        self._conn = conn
        self._shard_id = shard_id
        self._codec = codec
        self._sequences_table = table_ident(schema, "sequences")
        self._records_table = table_ident(schema, "records")

    def _encode(self, payload: Any) -> bytes:
        if self._codec is not None:
            return self._codec.encode(payload)
        if isinstance(payload, (bytes, bytearray, memoryview)):
            return bytes(payload)
        raise TypeError("codec is required when shard payload is not bytes")

    def _decode(self, raw: Any) -> T:
        raw_bytes = _normalize_bytes(raw)
        if self._codec is not None:
            return self._codec.decode(raw_bytes)
        return cast(T, raw_bytes)

    def _row_to_envelope(self, row: tuple[Any, ...]) -> RecordEnvelope[T]:
        return RecordEnvelope(
            partition_key=str(row[0]),
            seq=int(row[1]),
            idempotency_key=str(row[2]),
            record_type=str(row[3]),
            schema_version=int(row[4]),
            payload_hash=str(row[5]),
            payload=self._decode(row[6]),
            created_at=float(row[7]),
            covers_through_seq=int(row[8]) if row[8] is not None else None,
        )

    def find_by_idempotency(self, partition_key: str, idempotency_key: str) -> RecordEnvelope[T] | None:
        query = sql.SQL(
            """
            SELECT partition_key, seq, idempotency_key, record_type, schema_version,
                   payload_hash, payload, created_at, covers_through_seq
            FROM {}
            WHERE shard_id = %s AND partition_key = %s AND idempotency_key = %s
            """
        ).format(self._records_table)
        row = self._conn.execute(query, (self._shard_id, partition_key, idempotency_key)).fetchone()
        return self._row_to_envelope(row) if row is not None else None

    def next_sequence(self, partition_key: str) -> int:
        # CREATE SEQUENCE の nextval() は ROLLBACK 時に巻き戻らないため、
        # テーブル行の UPSERT + RETURNING で TR-IF-001 のロールバック原子性を担保する。
        query = sql.SQL(
            """
            INSERT INTO {} (shard_id, partition_key, last_seq)
            VALUES (%s, %s, 1)
            ON CONFLICT (shard_id, partition_key)
            DO UPDATE SET last_seq = sequences.last_seq + 1
            RETURNING last_seq
            """
        ).format(self._sequences_table)
        row = self._conn.execute(query, (self._shard_id, partition_key)).fetchone()
        assert row is not None
        return int(row[0])

    def insert_record(self, envelope: RecordEnvelope[T]) -> None:
        raw_payload = self._encode(envelope.payload)
        query = sql.SQL(
            """
            INSERT INTO {} (
                shard_id, partition_key, seq, idempotency_key, record_type,
                schema_version, payload_hash, payload, created_at, covers_through_seq
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        ).format(self._records_table)
        try:
            self._conn.execute(
                query,
                (
                    self._shard_id,
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
        except errors.UniqueViolation as exc:
            kind = _classify_unique_violation(exc)
            raise DuplicateKeyError(str(exc), kind=kind) from exc


class PostgresShardDAO(Generic[T]):
    """PostgreSQL にイミュータブル・レコードと連番を永続化する試作 ShardDAOPort 実装。"""

    def __init__(
        self,
        dsn: str,
        *,
        codec: PayloadCodec[T] | None = None,
        durability: DurabilityLevel = "power_loss",
        schema: str = "public",
    ) -> None:
        self._dsn = dsn
        self._codec = codec
        self._durability: DurabilityLevel = durability
        self._schema = schema
        self._sequences_table = table_ident(schema, "sequences")
        self._records_table = table_ident(schema, "records")
        self._init_lock = threading.Lock()
        self._init_schema()

    @property
    def durability(self) -> DurabilityLevel:
        return self._durability

    def _init_schema(self) -> None:
        with self._init_lock, write_tx(self._dsn, durability=self._durability) as conn:
            ensure_schema_exists(conn, self._schema)
            conn.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        shard_id TEXT NOT NULL,
                        partition_key TEXT NOT NULL,
                        last_seq BIGINT NOT NULL DEFAULT 0,
                        CONSTRAINT pk_sequences PRIMARY KEY (shard_id, partition_key)
                    )
                    """
                ).format(self._sequences_table)
            )
            conn.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        shard_id TEXT NOT NULL,
                        partition_key TEXT NOT NULL,
                        seq BIGINT NOT NULL,
                        idempotency_key TEXT NOT NULL,
                        record_type TEXT NOT NULL,
                        schema_version INTEGER NOT NULL,
                        payload_hash TEXT NOT NULL,
                        payload BYTEA NOT NULL,
                        created_at DOUBLE PRECISION NOT NULL,
                        covers_through_seq BIGINT,
                        CONSTRAINT pk_records PRIMARY KEY (shard_id, partition_key, seq),
                        CONSTRAINT uq_records_idempotency UNIQUE (shard_id, partition_key, idempotency_key)
                    )
                    """
                ).format(self._records_table)
            )

    @contextmanager
    def write_transaction(self, shard_id: str) -> Iterator[_PostgresShardTx[T]]:
        with write_tx(self._dsn, durability=self._durability) as conn:
            yield _PostgresShardTx(conn, shard_id, self._codec, self._schema)

    def fetch_range(
        self,
        shard_id: str,
        partition_key: str,
        after_seq: int,
        limit: int,
    ) -> Sequence[RecordEnvelope[T]]:
        if limit <= 0:
            return ()
        query = sql.SQL(
            """
            SELECT partition_key, seq, idempotency_key, record_type, schema_version,
                   payload_hash, payload, created_at, covers_through_seq
            FROM {}
            WHERE shard_id = %s AND partition_key = %s AND seq > %s
            ORDER BY seq ASC
            LIMIT %s
            """
        ).format(self._records_table)
        with open_connection(self._dsn) as conn:
            rows = conn.execute(query, (shard_id, partition_key, after_seq, limit)).fetchall()
            tx = _PostgresShardTx(conn, shard_id, self._codec, self._schema)
            return tuple(tx._row_to_envelope(row) for row in rows)
