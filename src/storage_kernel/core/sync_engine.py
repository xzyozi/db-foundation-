"""複数シャード横断のカーソル差分同期（Scatter-Gather）と読み取り時バイト列 Upcast。"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
import logging
from typing import Any, Generic, TypeVar, cast

from ..contracts.envelope import KernelConfig, RecordEnvelope
from ..contracts.errors import SchemaVersionError
from ..contracts.telemetry import TelemetryEvent, TelemetrySink
from ..ports.codec import PayloadCodec, PayloadUpcaster
from ..ports.dao import MetaDAOPort, ShardDAOPort
from ..runtime.concurrency import ShardLockManager

T = TypeVar("T")

logger = logging.getLogger("storage_kernel.sync_engine")


class UpcastPipeline(Generic[T]):
    """読み取り時に旧 schema_version のバイト列を 1 版ずつ最新版へ変換し、最後に 1 回 decode する。"""

    def __init__(
        self,
        *,
        config: KernelConfig,
        codec: PayloadCodec[T],
        upcaster: PayloadUpcaster | None = None,
        store_raw_bytes: bool = False,
        telemetry_sink: TelemetrySink | None = None,
    ) -> None:
        self._config = config
        self._codec = codec
        self._upcaster = upcaster
        self._store_raw_bytes = store_raw_bytes
        self._telemetry_sink = telemetry_sink

    def upcast_and_decode_envelope(self, envelope: RecordEnvelope[Any]) -> RecordEnvelope[T]:
        """RecordEnvelope を最新版 schema_version へ Upcast/デコードする（元ハッシュ・保存データは不変）。"""
        target_version = self._config.schema_registry.get(envelope.record_type)
        if target_version is None:
            raise SchemaVersionError(f"record_type {envelope.record_type!r} is not registered in schema_registry")

        if envelope.schema_version > target_version:
            raise SchemaVersionError(
                f"record schema_version {envelope.schema_version} for {envelope.record_type!r} "
                f"is newer than registered latest {target_version}"
            )

        if envelope.schema_version == target_version:
            if self._store_raw_bytes and isinstance(envelope.payload, bytes):
                decoded = self._codec.decode(envelope.payload)
                return RecordEnvelope(
                    partition_key=envelope.partition_key,
                    seq=envelope.seq,
                    idempotency_key=envelope.idempotency_key,
                    record_type=envelope.record_type,
                    schema_version=envelope.schema_version,
                    payload_hash=envelope.payload_hash,
                    payload=decoded,
                    created_at=envelope.created_at,
                    covers_through_seq=envelope.covers_through_seq,
                )
            return cast(RecordEnvelope[T], envelope)

        if self._upcaster is None:
            raise SchemaVersionError(
                f"upcaster is required to convert {envelope.record_type!r} "
                f"from v{envelope.schema_version} to v{target_version}"
            )

        raw_bytes: bytes = (
            envelope.payload if isinstance(envelope.payload, bytes) else self._codec.encode(envelope.payload)
        )
        steps = 0
        for ver in range(envelope.schema_version, target_version):
            try:
                raw_bytes = self._upcaster.upcast(envelope.record_type, ver, raw_bytes)
            except SchemaVersionError:
                raise
            except Exception as exc:
                raise SchemaVersionError(f"upcast failed for {envelope.record_type!r} at v{ver} -> v{ver + 1}") from exc
            steps += 1

        decoded_payload = self._codec.decode(raw_bytes)
        if self._telemetry_sink is not None:
            self._telemetry_sink.emit(
                TelemetryEvent(
                    name="sync.upcast_applied",
                    attributes={
                        "record_type": envelope.record_type,
                        "from_version": envelope.schema_version,
                        "to_version": target_version,
                        "steps": steps,
                    },
                )
            )

        return RecordEnvelope(
            partition_key=envelope.partition_key,
            seq=envelope.seq,
            idempotency_key=envelope.idempotency_key,
            record_type=envelope.record_type,
            schema_version=target_version,
            payload_hash=envelope.payload_hash,
            payload=decoded_payload,
            created_at=envelope.created_at,
            covers_through_seq=envelope.covers_through_seq,
        )


class CursorSyncEngine(Generic[T]):
    """{partition_key: last_seen_seq} に基づき複数シャードを横断して未処理差分を取得する。"""

    def __init__(
        self,
        *,
        shard_dao: ShardDAOPort[Any],
        meta_dao: MetaDAOPort,
        lock_manager: ShardLockManager,
        upcast_pipeline: UpcastPipeline[T],
        telemetry_sink: TelemetrySink | None = None,
    ) -> None:
        self._shard_dao = shard_dao
        self._meta_dao = meta_dao
        self._lock_manager = lock_manager
        self._upcast_pipeline = upcast_pipeline
        self._telemetry_sink = telemetry_sink

    def sync(
        self,
        cursors: Mapping[str, int],
        *,
        limit_per_partition: int = 100,
    ) -> dict[str, list[RecordEnvelope[T]]]:
        """複数パーティションの差分をシャード横断で取得し、最新版へ Upcast して返す（F-301）。"""
        if limit_per_partition < 1:
            raise ValueError("limit_per_partition must be >= 1")

        shard_batches: dict[str, list[tuple[str, int]]] = defaultdict(list)
        for partition_key, after_seq in cursors.items():
            if after_seq < 0:
                raise ValueError(f"cursor seq must be >= 0 for {partition_key!r}")
            info = self._lock_manager.get_cached_partition(partition_key)
            if info is None:
                info = self._meta_dao.get_partition(partition_key)
                if info is not None:
                    self._lock_manager.update_cached_partition(info)
            if info is None:
                continue
            shard_batches[info.shard_id].append((partition_key, after_seq))

        synced: dict[str, list[RecordEnvelope[T]]] = {}
        total_records = 0
        for shard_id, items in shard_batches.items():
            for partition_key, after_seq in items:
                raw_rows = self._shard_dao.fetch_range(
                    shard_id,
                    partition_key,
                    after_seq,
                    limit_per_partition,
                )
                if raw_rows:
                    converted = [self._upcast_pipeline.upcast_and_decode_envelope(r) for r in raw_rows]
                    synced[partition_key] = converted
                    total_records += len(converted)

        if self._telemetry_sink is not None:
            self._telemetry_sink.emit(
                TelemetryEvent(
                    name="sync.completed",
                    attributes={
                        "requested_partitions": len(cursors),
                        "returned_partitions": len(synced),
                        "returned_records": total_records,
                    },
                )
            )
        return synced
