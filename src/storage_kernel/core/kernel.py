"""StorageKernel: 親プロジェクトから呼び出す公開 Facade。"""

from __future__ import annotations

from collections.abc import Callable, Sequence
import logging
from typing import Any, Generic, TypeVar, cast

from ..contracts.envelope import (
    DropPolicy,
    KernelConfig,
    PartitionInfo,
    PartitionStatus,
    ProjectionFailurePolicy,
    RecordEnvelope,
    RetryPolicy,
    SubscriptionHandle,
    WriteAck,
)
from ..contracts.telemetry import TelemetryEvent, TelemetrySink
from ..ports.codec import PayloadCodec, PayloadUpcaster
from ..ports.dao import DeadLetterPort, MetaDAOPort, ProjectionPort, ShardDAOPort
from ..ports.maintenance import MaintenanceHooks
from ..ports.router import PartitionRouterPort
from ..runtime.concurrency import ShardLockManager
from .event_bus import EventDispatcher
from .ingestion import WritePipeline
from .projection import ProjectionRunner

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
        self._event_dispatcher = EventDispatcher(telemetry_sink=telemetry_sink)
        self._write_pipeline = WritePipeline(
            config=config,
            shard_dao=shard_dao,
            meta_dao=meta_dao,
            codec=codec,
            router=router,
            lock_manager=self._lock_manager,
            telemetry_sink=telemetry_sink,
            store_raw_bytes=store_raw_bytes,
            upcast_decode_fn=self._decode_stored_record,
        )
        self._projection_runner = ProjectionRunner(
            shard_dao=shard_dao,
            meta_dao=meta_dao,
            dead_letter_port=dead_letter_port,
            default_retry_policy=config.default_retry_policy,
            decode_record_fn=self._decode_stored_record,
            telemetry_sink=telemetry_sink,
        )
        self._post_commit_listeners.append(self._dispatch_post_commit)

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

    def _decode_stored_record(self, stored: RecordEnvelope[Any]) -> RecordEnvelope[T]:
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

    def _dispatch_post_commit(self, envelope: RecordEnvelope[T], is_snapshot: bool) -> None:
        self._event_dispatcher.publish(cast(RecordEnvelope[object], envelope), is_snapshot=is_snapshot)
        self._projection_runner.notify(envelope)

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

    def subscribe(
        self,
        pattern: str,
        callback: Callable[[RecordEnvelope[T]], None],
        *,
        max_queue_size: int = 256,
        drop_policy: DropPolicy = "drop_newest",
        on_error: Callable[[RecordEnvelope[T], Exception], None] | None = None,
    ) -> SubscriptionHandle:
        """At-Most-Once の非同期購読を登録する（IF-003 / F-201）。"""
        self._lock_manager.check_mutable_state()
        return self._event_dispatcher.subscribe(
            pattern,
            cast(Callable[[RecordEnvelope[object]], None], callback),
            max_queue_size=max_queue_size,
            drop_policy=drop_policy,
            on_error=cast(Callable[[RecordEnvelope[object], Exception], None] | None, on_error),
        )

    def register_projection(
        self,
        projection: ProjectionPort[T],
        *,
        partition_patterns: Sequence[str] = ("*",),
        batch_size: int = 100,
        retry_policy: RetryPolicy | None = None,
        failure_policy: ProjectionFailurePolicy | None = None,
        include_snapshots: bool = False,
    ) -> None:
        """At-Least-Once の非同期投影ワーカーを登録する（IF-004 / F-202）。"""
        self._lock_manager.check_mutable_state()
        self._projection_runner.register(
            projection,
            partition_patterns=partition_patterns,
            batch_size=batch_size,
            retry_policy=retry_policy,
            failure_policy=failure_policy,
            include_snapshots=include_snapshots,
        )

    def resume_projection(
        self,
        consumer_id: str,
        partition_key: str,
        *,
        skip_current: bool = False,
    ) -> None:
        """pause_partition で一時停止した投影を再開する（IF-005 / F-203）。"""
        if skip_current:
            self._lock_manager.check_mutable_state()
        self._projection_runner.resume_partition(
            consumer_id,
            partition_key,
            skip_current=skip_current,
        )

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
