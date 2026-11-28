"""書き込みパイプライン（検証・payload_hash算出・bind_shard先行・Write-Before-ACK）。"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
import hashlib
import time
from typing import Any, Generic, TypeVar, cast

from ..contracts.envelope import (
    SNAPSHOT_RECORD_TYPE,
    BatchAppendItem,
    KernelConfig,
    PartitionInfo,
    RecordEnvelope,
    WriteAck,
)
from ..contracts.errors import (
    IdempotencyConflict,
    PartitionReadOnlyError,
    PayloadTooLarge,
    ReservedRecordTypeError,
    SchemaVersionError,
)
from ..contracts.telemetry import TelemetryEvent, TelemetrySink
from ..ports.codec import PayloadCodec
from ..ports.dao import MetaDAOPort, ShardDAOPort, ShardTxPort
from ..ports.router import PartitionRouterPort
from ..runtime.concurrency import ShardLockManager
from .ordering import SequenceCoordinator

T = TypeVar("T")


def _length_prefixed(data: bytes) -> bytes:
    return f"{len(data)}:".encode("ascii") + data


def compute_payload_hash(record_type: str, schema_version: int, raw_bytes: bytes) -> str:
    """境界衝突を防ぐため長さプレフィックス付き連結の SHA-256（小文字16進64文字）を算出する。"""
    rt_bytes = record_type.encode("utf-8")
    ver_bytes = str(schema_version).encode("ascii")
    digest = hashlib.sha256()
    digest.update(_length_prefixed(rt_bytes))
    digest.update(_length_prefixed(ver_bytes))
    digest.update(_length_prefixed(raw_bytes))
    return digest.hexdigest()


@dataclass(frozen=True)
class PreparedWriteRequest(Generic[T]):
    """ロック獲得前の事前検証とハッシュ算出が完了した書き込み要求 DTO。"""

    partition_key: str
    record_type: str
    schema_version: int
    idempotency_key: str
    payload: T
    raw_payload: bytes
    payload_hash: str
    covers_through_seq: int | None = None
    preferred_shard: str | None = None
    is_snapshot: bool = False


@dataclass(frozen=True)
class WritePipelineResult(Generic[T]):
    """WritePipeline の実行結果 DTO。"""

    ack: WriteAck[T]
    should_dispatch_event: bool
    is_snapshot: bool


class WritePipeline(Generic[T]):
    """予約検査・サイズ検査・冪等性・採番・Write-Before-ACK を担うパイプライン。"""

    def __init__(
        self,
        *,
        config: KernelConfig,
        shard_dao: ShardDAOPort[Any],
        meta_dao: MetaDAOPort,
        codec: PayloadCodec[T],
        router: PartitionRouterPort,
        lock_manager: ShardLockManager,
        ordering: SequenceCoordinator | None = None,
        telemetry_sink: TelemetrySink | None = None,
        store_raw_bytes: bool = False,
        upcast_decode_fn: Callable[[RecordEnvelope[Any]], RecordEnvelope[T]] | None = None,
    ) -> None:
        self._config = config
        self._shard_dao = shard_dao
        self._meta_dao = meta_dao
        self._codec = codec
        self._router = router
        self._lock_manager = lock_manager
        self._ordering = ordering if ordering is not None else SequenceCoordinator()
        self._telemetry_sink = telemetry_sink
        self._store_raw_bytes = store_raw_bytes
        self._upcast_decode_fn = upcast_decode_fn

    def _emit(self, name: str, attributes: dict[str, str | int | float | bool]) -> None:
        if self._telemetry_sink is not None:
            self._telemetry_sink.emit(TelemetryEvent(name=name, attributes=attributes))

    def prepare_and_hash(
        self,
        *,
        partition_key: str,
        record_type: str,
        payload: T,
        idempotency_key: str,
        schema_version: int,
        covers_through_seq: int | None = None,
        preferred_shard: str | None = None,
        is_snapshot: bool = False,
    ) -> PreparedWriteRequest[T]:
        """ロック獲得前に実行する入力契約検証・エンコード・ハッシュ算出。"""
        if not partition_key:
            raise ValueError("partition_key must be non-empty")
        if not idempotency_key:
            raise ValueError("idempotency_key must be non-empty")
        if not record_type:
            raise ValueError("record_type must be non-empty")

        if is_snapshot:
            if record_type != SNAPSHOT_RECORD_TYPE:
                raise ValueError(f"snapshot write must use record_type={SNAPSHOT_RECORD_TYPE!r}")
            if covers_through_seq is None or covers_through_seq < 1:
                raise ValueError("covers_through_seq must be >= 1 for snapshots")
        else:
            if record_type.startswith("__"):
                raise ReservedRecordTypeError(f"record_type {record_type!r} is reserved for system use")

        expected_version = self._config.schema_registry.get(record_type)
        if expected_version is None or schema_version != expected_version:
            raise SchemaVersionError(
                f"schema_version {schema_version} for {record_type!r} "
                f"does not match registered latest {expected_version}"
            )

        raw_bytes = self._codec.encode(payload)
        if len(raw_bytes) > self._config.max_payload_bytes:
            raise PayloadTooLarge(len(raw_bytes), self._config.max_payload_bytes)

        payload_hash = compute_payload_hash(record_type, schema_version, raw_bytes)
        return PreparedWriteRequest(
            partition_key=partition_key,
            record_type=record_type,
            schema_version=schema_version,
            idempotency_key=idempotency_key,
            payload=payload,
            raw_payload=raw_bytes,
            payload_hash=payload_hash,
            covers_through_seq=covers_through_seq,
            preferred_shard=preferred_shard,
            is_snapshot=is_snapshot,
        )

    def resolve_and_bind_partition(
        self,
        partition_key: str,
        *,
        preferred_shard: str | None = None,
    ) -> PartitionInfo:
        """パーティションロック下で台帳参照・未登録時 bind_shard 先行確定・状態確認を行う。"""
        info = self._lock_manager.get_cached_partition(partition_key)
        if info is None:
            info = self._meta_dao.get_partition(partition_key)
            if info is None:
                shard_id = self._router.select_shard(partition_key, preferred_shard=preferred_shard)
                info = self._meta_dao.bind_shard(partition_key, shard_id)
                self._emit(
                    "partition.bound",
                    {"partition_key": partition_key, "shard_id": info.shard_id},
                )
            self._lock_manager.update_cached_partition(info)

        if info.status == "read_only":
            raise PartitionReadOnlyError(f"partition {partition_key!r} is read_only")
        return info

    def _to_public_envelope(self, stored: RecordEnvelope[Any]) -> RecordEnvelope[T]:
        if self._upcast_decode_fn is not None:
            return self._upcast_decode_fn(stored)
        if self._store_raw_bytes and isinstance(stored.payload, bytes):
            decoded = self._codec.decode(stored.payload)
            return RecordEnvelope(
                partition_key=stored.partition_key,
                seq=stored.seq,
                idempotency_key=stored.idempotency_key,
                record_type=stored.record_type,
                schema_version=stored.schema_version,
                payload_hash=stored.payload_hash,
                payload=decoded,
                created_at=stored.created_at,
                covers_through_seq=stored.covers_through_seq,
            )
        return cast(RecordEnvelope[T], stored)

    def execute_append(
        self,
        partition_key: str,
        record_type: str,
        payload: T,
        idempotency_key: str,
        *,
        schema_version: int,
        preferred_shard: str | None = None,
    ) -> WritePipelineResult[T]:
        """通常レコードの書き込みを実行する（F-101）。"""
        self._lock_manager.check_mutable_state()
        req = self.prepare_and_hash(
            partition_key=partition_key,
            record_type=record_type,
            payload=payload,
            idempotency_key=idempotency_key,
            schema_version=schema_version,
            preferred_shard=preferred_shard,
            is_snapshot=False,
        )
        return self._execute_request(req)

    def execute_snapshot(
        self,
        partition_key: str,
        payload: T,
        idempotency_key: str,
        *,
        covers_through_seq: int,
        schema_version: int,
    ) -> WritePipelineResult[T]:
        """手動スナップショットの書き込みを実行する（F-102）。"""
        self._lock_manager.check_mutable_state()
        req = self.prepare_and_hash(
            partition_key=partition_key,
            record_type=SNAPSHOT_RECORD_TYPE,
            payload=payload,
            idempotency_key=idempotency_key,
            schema_version=schema_version,
            covers_through_seq=covers_through_seq,
            is_snapshot=True,
        )
        return self._execute_request(req)

    def execute_append_batch(
        self,
        partition_key: str,
        items: Sequence[BatchAppendItem[T]],
        *,
        preferred_shard: str | None = None,
    ) -> tuple[WritePipelineResult[T], ...]:
        """同一パーティションへの一括レコード書き込みを実行する（IF-011 / F-107）。"""
        self._lock_manager.check_mutable_state()
        if not partition_key:
            raise ValueError("partition_key must be non-empty")
        if not items:
            return ()

        reqs = [
            self.prepare_and_hash(
                partition_key=partition_key,
                record_type=item.record_type,
                payload=item.payload,
                idempotency_key=item.idempotency_key,
                schema_version=item.schema_version,
                preferred_shard=preferred_shard,
                is_snapshot=False,
            )
            for item in items
        ]

        started_at = time.monotonic()
        with self._lock_manager.partition_write_guard(partition_key):
            info = self.resolve_and_bind_partition(
                partition_key,
                preferred_shard=preferred_shard,
            )
            seen_in_batch: dict[str, RecordEnvelope[T]] = {}
            results: list[WritePipelineResult[T]] = []
            with self._shard_dao.write_transaction(info.shard_id) as tx:
                for req in reqs:
                    results.append(
                        self._process_request_in_tx(
                            tx,
                            info.shard_id,
                            req,
                            seen_in_batch=seen_in_batch,
                        )
                    )

        elapsed_ms = (time.monotonic() - started_at) * 1000.0
        for result in results:
            self._emit_write_telemetry(result, elapsed_ms)
        return tuple(results)

    def _emit_write_telemetry(self, result: WritePipelineResult[T], elapsed_ms: float) -> None:
        ack = result.ack
        if ack.status == "duplicate":
            self._emit(
                "write.duplicate",
                {
                    "partition_key": ack.envelope.partition_key,
                    "shard_id": ack.shard_id,
                    "seq": ack.envelope.seq,
                    "elapsed_ms": elapsed_ms,
                },
            )
        else:
            self._emit(
                "write.committed",
                {
                    "partition_key": ack.envelope.partition_key,
                    "shard_id": ack.shard_id,
                    "seq": ack.envelope.seq,
                    "is_snapshot": result.is_snapshot,
                    "elapsed_ms": elapsed_ms,
                },
            )

    def _process_request_in_tx(
        self,
        tx: ShardTxPort[Any],
        shard_id: str,
        req: PreparedWriteRequest[T],
        *,
        seen_in_batch: dict[str, RecordEnvelope[T]] | None = None,
    ) -> WritePipelineResult[T]:
        seen_env = seen_in_batch.get(req.idempotency_key) if seen_in_batch is not None else None
        if seen_env is not None:
            if seen_env.payload_hash != req.payload_hash:
                raise IdempotencyConflict(
                    req.partition_key,
                    req.idempotency_key,
                    seen_env.payload_hash,
                    req.payload_hash,
                )
            return WritePipelineResult(
                ack=WriteAck(status="duplicate", shard_id=shard_id, envelope=seen_env),
                should_dispatch_event=False,
                is_snapshot=req.is_snapshot,
            )

        existing = tx.find_by_idempotency(req.partition_key, req.idempotency_key)
        if existing is not None:
            if existing.payload_hash != req.payload_hash:
                raise IdempotencyConflict(
                    req.partition_key,
                    req.idempotency_key,
                    existing.payload_hash,
                    req.payload_hash,
                )
            public_existing = self._to_public_envelope(existing)
            if seen_in_batch is not None:
                seen_in_batch[req.idempotency_key] = public_existing
            return WritePipelineResult(
                ack=WriteAck(status="duplicate", shard_id=shard_id, envelope=public_existing),
                should_dispatch_event=False,
                is_snapshot=req.is_snapshot,
            )

        next_seq = self._ordering.allocate_and_verify(
            tx,
            req.partition_key,
            covers_through_seq=req.covers_through_seq,
        )
        now = time.time()
        stored_payload: Any = req.raw_payload if self._store_raw_bytes else req.payload
        stored_envelope: RecordEnvelope[Any] = RecordEnvelope(
            partition_key=req.partition_key,
            seq=next_seq,
            idempotency_key=req.idempotency_key,
            record_type=req.record_type,
            schema_version=req.schema_version,
            payload_hash=req.payload_hash,
            payload=stored_payload,
            created_at=now,
            covers_through_seq=req.covers_through_seq,
        )
        tx.insert_record(stored_envelope)

        public_envelope = RecordEnvelope(
            partition_key=req.partition_key,
            seq=next_seq,
            idempotency_key=req.idempotency_key,
            record_type=req.record_type,
            schema_version=req.schema_version,
            payload_hash=req.payload_hash,
            payload=req.payload,
            created_at=now,
            covers_through_seq=req.covers_through_seq,
        )
        if seen_in_batch is not None:
            seen_in_batch[req.idempotency_key] = public_envelope
        return WritePipelineResult(
            ack=WriteAck(status="committed", shard_id=shard_id, envelope=public_envelope),
            should_dispatch_event=True,
            is_snapshot=req.is_snapshot,
        )

    def _execute_request(self, req: PreparedWriteRequest[T]) -> WritePipelineResult[T]:
        started_at = time.monotonic()
        with self._lock_manager.partition_write_guard(req.partition_key):
            info = self.resolve_and_bind_partition(
                req.partition_key,
                preferred_shard=req.preferred_shard,
            )

            with self._shard_dao.write_transaction(info.shard_id) as tx:
                result = self._process_request_in_tx(tx, info.shard_id, req)

        elapsed_ms = (time.monotonic() - started_at) * 1000.0
        self._emit_write_telemetry(result, elapsed_ms)
        return result
