"""StorageKernel: 親プロジェクトから呼び出す公開 Facade。"""

from __future__ import annotations

from collections.abc import Callable
import logging
from typing import Any, Generic, TypeVar

from ..contracts.envelope import (
    KernelConfig,
    PartitionInfo,
    PartitionStatus,
    RecordEnvelope,
    WriteAck,
)
from ..contracts.telemetry import TelemetryEvent, TelemetrySink
from ..ports.codec import PayloadCodec, PayloadUpcaster
from ..ports.dao import DeadLetterPort, MetaDAOPort, ShardDAOPort
from ..ports.maintenance import MaintenanceHooks
from ..ports.router import PartitionRouterPort
from ..runtime.concurrency import ShardLockManager
from .ingestion import WritePipeline

T = TypeVar("T")

logger = logging.getLogger("storage_kernel")


class StorageKernel(Generic[T]):
    """storage_kernel v1 の公開 Facade クラス。"""

    def __init__(
        self,
        *,
        config: KernelConfig,
        shard_dao: ShardDAOPort[Any],
        meta_dao: MetaDAOPort,
        codec: PayloadCodec[T],
        router: PartitionRouterPort,
        upcaster: PayloadUpcaster | None = None,
        dead_letter_port: DeadLetterPort[T] | None = None,
        maintenance_hooks: MaintenanceHooks | None = None,
        telemetry_sink: TelemetrySink | None = None,
        store_raw_bytes: bool = False,
    ) -> None:
        self._config = config
        self._shard_dao = shard_dao
        self._meta_dao = meta_dao
        self._codec = codec
        self._router = router
        self._upcaster = upcaster
        self._dead_letter_port = dead_letter_port
        self._maintenance_hooks = maintenance_hooks
        self._telemetry_sink = telemetry_sink
        self._store_raw_bytes = store_raw_bytes

        self._lock_manager = ShardLockManager()
        self._post_commit_listeners: list[Callable[[RecordEnvelope[T], bool], None]] = []
        self._write_pipeline = WritePipeline(
            config=config,
            shard_dao=shard_dao,
            meta_dao=meta_dao,
            codec=codec,
            router=router,
            lock_manager=self._lock_manager,
            telemetry_sink=telemetry_sink,
            store_raw_bytes=store_raw_bytes,
        )

        logger.info(
            "StorageKernel initialized (shard_durability=%s, meta_durability=%s)",
            shard_dao.durability,
            meta_dao.durability,
        )
        if telemetry_sink is not None:
            telemetry_sink.emit(
                TelemetryEvent(
                    name="kernel.started",
                    attributes={
                        "shard_durability": shard_dao.durability,
                        "meta_durability": meta_dao.durability,
                    },
                )
            )

    def _notify_post_commit(self, envelope: RecordEnvelope[T], is_snapshot: bool) -> None:
        for listener in self._post_commit_listeners:
            listener(envelope, is_snapshot)

    def append(
        self,
        partition_key: str,
        record_type: str,
        payload: T,
        idempotency_key: str,
        *,
        schema_version: int,
        preferred_shard: str | None = None,
    ) -> WriteAck[T]:
        """指定パーティションへ通常レコードを冪等に追記する（IF-001 / F-101）。"""
        result = self._write_pipeline.execute_append(
            partition_key,
            record_type,
            payload,
            idempotency_key,
            schema_version=schema_version,
            preferred_shard=preferred_shard,
        )
        if result.should_dispatch_event:
            self._notify_post_commit(result.ack.envelope, result.is_snapshot)
        return result.ack

    def write_snapshot(
        self,
        partition_key: str,
        payload: T,
        idempotency_key: str,
        *,
        covers_through_seq: int,
        schema_version: int,
    ) -> WriteAck[T]:
        """予約種別 '__snapshot__' として手動スナップショットを書き込む（IF-002 / F-102）。"""
        result = self._write_pipeline.execute_snapshot(
            partition_key,
            payload,
            idempotency_key,
            covers_through_seq=covers_through_seq,
            schema_version=schema_version,
        )
        if result.should_dispatch_event:
            self._notify_post_commit(result.ack.envelope, result.is_snapshot)
        return result.ack

    def set_partition_status(
        self,
        partition_key: str,
        status: PartitionStatus,
        *,
        expected_status: PartitionStatus | None = None,
    ) -> PartitionInfo:
        """パーティションロック下で状態を遷移させ、キャッシュを更新する（IF-006 / F-103）。"""
        with self._lock_manager.partition_write_guard(partition_key):
            updated = self._meta_dao.set_partition_status(
                partition_key,
                status,
                expected_status=expected_status,
            )
            self._lock_manager.update_cached_partition(updated)
            logger.info("Partition %s status updated to %s", partition_key, updated.status)
            if self._telemetry_sink is not None:
                self._telemetry_sink.emit(
                    TelemetryEvent(
                        name="partition.status_changed",
                        attributes={"partition_key": partition_key, "status": updated.status},
                    )
                )
            return updated
