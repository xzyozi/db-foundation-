"""PostgreSQL による MetaDAOPort の試作実装。"""

from __future__ import annotations

from collections.abc import Sequence
import threading
import time
from typing import Any, cast

from psycopg import sql

from storage_kernel import PartitionInfo, PartitionStateConflictError
from storage_kernel.contracts.envelope import DurabilityLevel, PartitionStatus

from ._db import ensure_schema_exists, open_connection, table_ident, write_tx


class PostgresMetaDAO:
    """PostgreSQL にパーティション台帳とカーソルを永続化する試作 MetaDAOPort 実装。"""

    def __init__(
        self,
        dsn: str,
        *,
        durability: DurabilityLevel = "power_loss",
        schema: str = "public",
    ) -> None:
        self._dsn = dsn
        self._durability: DurabilityLevel = durability
        self._schema = schema
        self._partitions_table = table_ident(schema, "partitions")
        self._cursors_table = table_ident(schema, "cursors")
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
                        partition_key TEXT PRIMARY KEY,
                        shard_id TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'read_only')),
                        created_at DOUBLE PRECISION NOT NULL
                    )
                    """
                ).format(self._partitions_table)
            )
            conn.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        consumer_id TEXT NOT NULL,
                        partition_key TEXT NOT NULL,
                        last_seq BIGINT NOT NULL,
                        updated_at DOUBLE PRECISION NOT NULL,
                        PRIMARY KEY (consumer_id, partition_key)
                    )
                    """
                ).format(self._cursors_table)
            )

    @staticmethod
    def _row_to_partition(row: tuple[Any, ...]) -> PartitionInfo:
        status = cast(PartitionStatus, str(row[2]))
        return PartitionInfo(
            partition_key=str(row[0]),
            shard_id=str(row[1]),
            status=status,
            created_at=float(row[3]),
        )

    def get_partition(self, partition_key: str) -> PartitionInfo | None:
        query = sql.SQL("SELECT partition_key, shard_id, status, created_at FROM {} WHERE partition_key = %s").format(
            self._partitions_table
        )
        with open_connection(self._dsn) as conn:
            row = conn.execute(query, (partition_key,)).fetchone()
            return self._row_to_partition(row) if row is not None else None

    def bind_shard(self, partition_key: str, shard_id: str) -> PartitionInfo:
        insert_query = sql.SQL(
            """
            INSERT INTO {} (partition_key, shard_id, status, created_at)
            VALUES (%s, %s, 'active', %s)
            ON CONFLICT (partition_key) DO NOTHING
            """
        ).format(self._partitions_table)
        select_query = sql.SQL(
            "SELECT partition_key, shard_id, status, created_at FROM {} WHERE partition_key = %s"
        ).format(self._partitions_table)
        with write_tx(self._dsn, durability=self._durability) as conn:
            conn.execute(insert_query, (partition_key, shard_id, time.time()))
            row = conn.execute(select_query, (partition_key,)).fetchone()
            assert row is not None
            return self._row_to_partition(row)

    def set_partition_status(
        self,
        partition_key: str,
        status: PartitionStatus,
        *,
        expected_status: PartitionStatus | None = None,
    ) -> PartitionInfo:
        select_query = sql.SQL(
            "SELECT partition_key, shard_id, status, created_at FROM {} WHERE partition_key = %s FOR UPDATE"
        ).format(self._partitions_table)
        update_query = sql.SQL(
            """
            UPDATE {} SET status = %s
            WHERE partition_key = %s
            RETURNING partition_key, shard_id, status, created_at
            """
        ).format(self._partitions_table)
        with write_tx(self._dsn, durability=self._durability) as conn:
            row = conn.execute(select_query, (partition_key,)).fetchone()
            if row is None:
                raise KeyError(f"partition not registered: {partition_key}")
            current = self._row_to_partition(row)
            if expected_status is not None and current.status != expected_status:
                raise PartitionStateConflictError(
                    f"expected status {expected_status!r} for {partition_key}, got {current.status!r}"
                )
            updated_row = conn.execute(update_query, (status, partition_key)).fetchone()
            assert updated_row is not None
            return self._row_to_partition(updated_row)

    def list_partitions(self, *, status: PartitionStatus | None = None) -> Sequence[PartitionInfo]:
        with open_connection(self._dsn) as conn:
            if status is None:
                query = sql.SQL(
                    "SELECT partition_key, shard_id, status, created_at FROM {} ORDER BY partition_key ASC"
                ).format(self._partitions_table)
                rows = conn.execute(query).fetchall()
            else:
                query = sql.SQL(
                    """
                    SELECT partition_key, shard_id, status, created_at
                    FROM {}
                    WHERE status = %s
                    ORDER BY partition_key ASC
                    """
                ).format(self._partitions_table)
                rows = conn.execute(query, (status,)).fetchall()
            return tuple(self._row_to_partition(row) for row in rows)

    def get_cursor(self, consumer_id: str, partition_key: str) -> int:
        query = sql.SQL("SELECT last_seq FROM {} WHERE consumer_id = %s AND partition_key = %s").format(
            self._cursors_table
        )
        with open_connection(self._dsn) as conn:
            row = conn.execute(query, (consumer_id, partition_key)).fetchone()
            return int(row[0]) if row is not None else 0

    def advance_cursor(self, consumer_id: str, partition_key: str, seq: int) -> None:
        query = sql.SQL(
            """
            INSERT INTO {} (consumer_id, partition_key, last_seq, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (consumer_id, partition_key) DO UPDATE SET
                last_seq = GREATEST(cursors.last_seq, EXCLUDED.last_seq),
                updated_at = EXCLUDED.updated_at
            """
        ).format(self._cursors_table)
        with write_tx(self._dsn, durability=self._durability) as conn:
            conn.execute(query, (consumer_id, partition_key, seq, time.time()))
