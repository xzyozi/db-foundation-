"""DB 不要で即動作する In-Memory Port 実装群（テスト・Contract Test 用）。"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import json
import threading
import time
from typing import Any, Generic, Iterator, TypeVar

from ..contracts.envelope import (
    DeadLetterRecord,
    DurabilityLevel,
    PartitionInfo,
    PartitionStatus,
    RecordEnvelope,
)
from ..contracts.errors import (
    DuplicateKeyError,
    PartitionStateConflictError,
    SchemaVersionError,
)

T = TypeVar("T")


@dataclass
class InMemoryStorageBackend(Generic[T]):
    """複数の In-Memory DAO インスタンス間で共有できるメモリ状態コンテナ。"""

    lock: threading.RLock = field(default_factory=threading.RLock)
    partitions: dict[str, PartitionInfo] = field(default_factory=dict)
    cursors: dict[tuple[str, str], int] = field(default_factory=dict)
    sequences: dict[tuple[str, str], int] = field(default_factory=dict)
    records: dict[tuple[str, str], list[RecordEnvelope[T]]] = field(default_factory=dict)
    dead_letters: dict[tuple[str, str, int], DeadLetterRecord[T]] = field(default_factory=dict)


class _InMemoryShardTx(Generic[T]):
    """単一シャード内のステージングバッファを用いたトランザクション実装。"""

    def __init__(self, backend: InMemoryStorageBackend[T], shard_id: str) -> None:
        self._backend = backend
        self._shard_id = shard_id
        self._staged_sequences: dict[str, int] = {}
        self._staged_records: list[RecordEnvelope[T]] = []

    def _effective_records(self, partition_key: str) -> list[RecordEnvelope[T]]:
        base = list(self._backend.records.get((self._shard_id, partition_key), []))
        base.extend(rec for rec in self._staged_records if rec.partition_key == partition_key)
        return base

    def find_by_idempotency(self, partition_key: str, idempotency_key: str) -> RecordEnvelope[T] | None:
        for rec in self._effective_records(partition_key):
            if rec.idempotency_key == idempotency_key:
                return rec
        return None

    def next_sequence(self, partition_key: str) -> int:
        if partition_key in self._staged_sequences:
            next_seq = self._staged_sequences[partition_key] + 1
        else:
            next_seq = self._backend.sequences.get((self._shard_id, partition_key), 0) + 1
        self._staged_sequences[partition_key] = next_seq
        return next_seq

    def insert_record(self, envelope: RecordEnvelope[T]) -> None:
        existing = self._effective_records(envelope.partition_key)
        for rec in existing:
            if rec.seq == envelope.seq:
                raise DuplicateKeyError(
                    f"duplicate seq ({envelope.partition_key}, {envelope.seq})",
                    kind="sequence",
                )
            if rec.idempotency_key == envelope.idempotency_key:
                raise DuplicateKeyError(
                    f"duplicate idempotency_key ({envelope.partition_key}, {envelope.idempotency_key})",
                    kind="idempotency_key",
                )
        self._staged_records.append(envelope)

    def commit(self) -> None:
        for partition_key, seq in self._staged_sequences.items():
            self._backend.sequences[(self._shard_id, partition_key)] = seq
        for rec in self._staged_records:
            bucket = self._backend.records.setdefault((self._shard_id, rec.partition_key), [])
            bucket.append(rec)


class InMemoryShardDAO(Generic[T]):
    """ShardDAOPort の In-Memory 実装（durability='none'）。"""

    def __init__(self, backend: InMemoryStorageBackend[T] | None = None) -> None:
        self.backend: InMemoryStorageBackend[T] = backend if backend is not None else InMemoryStorageBackend()

    @property
    def durability(self) -> DurabilityLevel:
        return "none"

    @contextmanager
    def write_transaction(self, shard_id: str) -> Iterator[_InMemoryShardTx[T]]:
        with self.backend.lock:
            tx = _InMemoryShardTx(self.backend, shard_id)
            yield tx
            tx.commit()

    def fetch_range(
        self,
        shard_id: str,
        partition_key: str,
        after_seq: int,
        limit: int,
    ) -> Sequence[RecordEnvelope[T]]:
        if limit <= 0:
            return ()
        with self.backend.lock:
            rows = [rec for rec in self.backend.records.get((shard_id, partition_key), []) if rec.seq > after_seq]
            rows.sort(key=lambda r: r.seq)
            return tuple(rows[:limit])


class InMemoryMetaDAO:
    """MetaDAOPort の In-Memory 実装（durability='none'）。"""

    def __init__(self, backend: InMemoryStorageBackend[Any] | None = None) -> None:
        self.backend: InMemoryStorageBackend[Any] = backend if backend is not None else InMemoryStorageBackend()

    @property
    def durability(self) -> DurabilityLevel:
        return "none"

    def get_partition(self, partition_key: str) -> PartitionInfo | None:
        with self.backend.lock:
            return self.backend.partitions.get(partition_key)

    def bind_shard(self, partition_key: str, shard_id: str) -> PartitionInfo:
        with self.backend.lock:
            existing = self.backend.partitions.get(partition_key)
            if existing is not None:
                return existing
            info = PartitionInfo(
                partition_key=partition_key,
                shard_id=shard_id,
                status="active",
                created_at=time.time(),
            )
            self.backend.partitions[partition_key] = info
            return info

    def set_partition_status(
        self,
        partition_key: str,
        status: PartitionStatus,
        *,
        expected_status: PartitionStatus | None = None,
    ) -> PartitionInfo:
        with self.backend.lock:
            current = self.backend.partitions.get(partition_key)
            if current is None:
                raise KeyError(f"partition not registered: {partition_key}")
            if expected_status is not None and current.status != expected_status:
                raise PartitionStateConflictError(
                    f"expected status {expected_status!r} for {partition_key}, got {current.status!r}"
                )
            updated = PartitionInfo(
                partition_key=current.partition_key,
                shard_id=current.shard_id,
                status=status,
                created_at=current.created_at,
            )
            self.backend.partitions[partition_key] = updated
            return updated

    def list_partitions(self, *, status: PartitionStatus | None = None) -> Sequence[PartitionInfo]:
        with self.backend.lock:
            items = list(self.backend.partitions.values())
            if status is not None:
                items = [item for item in items if item.status == status]
            return tuple(items)

    def get_cursor(self, consumer_id: str, partition_key: str) -> int:
        with self.backend.lock:
            return self.backend.cursors.get((consumer_id, partition_key), 0)

    def advance_cursor(self, consumer_id: str, partition_key: str, seq: int) -> None:
        with self.backend.lock:
            key = (consumer_id, partition_key)
            current = self.backend.cursors.get(key, 0)
            if seq > current:
                self.backend.cursors[key] = seq


class InMemoryProjection(Generic[T]):
    """(partition_key, seq) 単位で冪等性を保証する In-Memory Projection 実装。"""

    def __init__(self, consumer_id: str = "in_memory_projection") -> None:
        self._consumer_id = consumer_id
        self._lock = threading.RLock()
        self.applied: dict[tuple[str, int], RecordEnvelope[T]] = {}

    @property
    def consumer_id(self) -> str:
        return self._consumer_id

    def apply_batch(self, records: Sequence[RecordEnvelope[T]]) -> None:
        with self._lock:
            for rec in records:
                self.applied[(rec.partition_key, rec.seq)] = rec

    def records_for(self, partition_key: str) -> list[RecordEnvelope[T]]:
        """指定パーティションに適用済みのレコードを seq 昇順で返すテスト用ヘルパー。"""
        with self._lock:
            rows = [rec for (pk, _), rec in self.applied.items() if pk == partition_key]
            rows.sort(key=lambda r: r.seq)
            return rows


class InMemoryDeadLetter(Generic[T]):
    """DeadLetterPort の In-Memory 実装。"""

    def __init__(self, backend: InMemoryStorageBackend[T] | None = None) -> None:
        self.backend: InMemoryStorageBackend[T] = backend if backend is not None else InMemoryStorageBackend()

    def put_dead_letter(self, record: DeadLetterRecord[T]) -> None:
        with self.backend.lock:
            key = (record.consumer_id, record.envelope.partition_key, record.envelope.seq)
            self.backend.dead_letters[key] = record

    def list_dead_letters(
        self,
        *,
        consumer_id: str | None = None,
        limit: int = 100,
    ) -> Sequence[DeadLetterRecord[T]]:
        if limit <= 0:
            return ()
        with self.backend.lock:
            rows = list(self.backend.dead_letters.values())
            if consumer_id is not None:
                rows = [r for r in rows if r.consumer_id == consumer_id]
            return tuple(rows[:limit])


class JsonDeterministicCodec:
    """キー順序を正規化して決定論的バイト列を返す JSON PayloadCodec 実装。"""

    def encode(self, value: dict[str, Any]) -> bytes:
        return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    def decode(self, data: bytes) -> dict[str, Any]:
        loaded = json.loads(data.decode("utf-8"))
        if not isinstance(loaded, dict):
            raise TypeError(f"expected dict payload, got {type(loaded).__name__}")
        return loaded


class SimplePartitionRouter:
    """blake2b ハッシュで既定シャード数へ割り当てる純粋な PartitionRouterPort 実装。"""

    def __init__(self, default_shards: int = 4) -> None:
        if default_shards < 1:
            raise ValueError("default_shards must be >= 1")
        self._default_shards = default_shards

    def select_shard(self, partition_key: str, *, preferred_shard: str | None = None) -> str:
        if preferred_shard is not None:
            return preferred_shard
        digest = hashlib.blake2b(partition_key.encode("utf-8"), digest_size=4).digest()
        idx = int.from_bytes(digest, "big") % self._default_shards
        return f"shard_{idx}"


class MappingPayloadUpcaster:
    """(record_type, from_version) 単位の変換関数マップで動作する PayloadUpcaster 実装。"""

    def __init__(self, handlers: dict[tuple[str, int], Callable[[bytes], bytes]] | None = None) -> None:
        self._handlers: dict[tuple[str, int], Callable[[bytes], bytes]] = dict(handlers or {})

    def register(self, record_type: str, from_version: int, fn: Callable[[bytes], bytes]) -> None:
        self._handlers[(record_type, from_version)] = fn

    def upcast(self, record_type: str, from_version: int, raw_payload: bytes) -> bytes:
        handler = self._handlers.get((record_type, from_version))
        if handler is None:
            raise SchemaVersionError(f"missing upcaster path for ({record_type}, v{from_version})")
        return handler(raw_payload)
