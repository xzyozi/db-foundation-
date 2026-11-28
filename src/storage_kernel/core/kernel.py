"""StorageKernel: 親プロジェクトから呼び出す公開 Facade。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import logging
from typing import Any, Generic, TypeVar, cast

from ..contracts.envelope import (
    BatchAppendItem,
    DropPolicy,
    KernelConfig,
    PartitionInfo,
    PartitionStatus,
    ProjectionFailurePolicy,
    RecordEnvelope,
    RetryPolicy,
    ShutdownReport,
    SubscriptionHandle,
    WriteAck,
)
from ..contracts.errors import KernelClosedError
from ..contracts.telemetry import TelemetryEvent, TelemetrySink
from ..ports.codec import PayloadCodec, PayloadUpcaster
from ..ports.dao import DeadLetterPort, MetaDAOPort, ProjectionPort, ShardDAOPort
from ..ports.maintenance import MaintenanceHooks
from ..ports.router import PartitionRouterPort
from ..runtime.concurrency import ShardLockManager
from ..runtime.lifecycle import LifecycleManager
from ..runtime.maintenance import MaintenanceCoordinator
from .event_bus import EventDispatcher
from .ingestion import WritePipeline
from .projection import ProjectionRunner
from .sync_engine import CursorSyncEngine, UpcastPipeline

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
        self._upcast_pipeline = UpcastPipeline(
            config=config,
            codec=codec,
            upcaster=upcaster,
            store_raw_bytes=store_raw_bytes,
            telemetry_sink=telemetry_sink,
        )
        self._write_pipeline = WritePipeline(
            config=config,
            shard_dao=shard_dao,
            meta_dao=meta_dao,
            codec=codec,
            router=router,
            lock_manager=self._lock_manager,
            telemetry_sink=telemetry_sink,
            store_raw_bytes=store_raw_bytes,
            upcast_decode_fn=self._upcast_pipeline.upcast_and_decode_envelope,
        )
        self._projection_runner = ProjectionRunner(
            shard_dao=shard_dao,
            meta_dao=meta_dao,
            dead_letter_port=dead_letter_port,
            default_retry_policy=config.default_retry_policy,
            decode_record_fn=self._upcast_pipeline.upcast_and_decode_envelope,
            telemetry_sink=telemetry_sink,
            lock_manager=self._lock_manager,
        )
        self._sync_engine = CursorSyncEngine(
            shard_dao=shard_dao,
            meta_dao=meta_dao,
            lock_manager=self._lock_manager,
            upcast_pipeline=self._upcast_pipeline,
            telemetry_sink=telemetry_sink,
        )
        self._maintenance = MaintenanceCoordinator(
            lock_manager=self._lock_manager,
            projection_runner=self._projection_runner,
            maintenance_hooks=maintenance_hooks,
            telemetry_sink=telemetry_sink,
        )
        self._lifecycle = LifecycleManager(
            lock_manager=self._lock_manager,
            projection_runner=self._projection_runner,
            event_dispatcher=self._event_dispatcher,
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

    def _check_not_closed(self) -> None:
        """close() 開始後なら KernelClosedError を送出する（メンテナンス中は許容する）。"""
        if self._lock_manager.lifecycle_phase in ("CLOSING", "CLOSED"):
            raise KernelClosedError("storage_kernel is closed")

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

    def append_batch(
        self,
        partition_key: str,
        items: Sequence[BatchAppendItem[T]],
        *,
        preferred_shard: str | None = None,
    ) -> tuple[WriteAck[T], ...]:
        """同一パーティションへ複数レコードを単一トランザクションで追記する（IF-011 / F-107）。"""
        results = self._write_pipeline.execute_append_batch(
            partition_key,
            items,
            preferred_shard=preferred_shard,
        )
        for result in results:
            if result.should_dispatch_event:
                self._notify_post_commit(result.ack.envelope, result.is_snapshot)
        return tuple(result.ack for result in results)

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
        """At-Most-Once の非同期購読を登録する（IF-003 / F-201）。

        購読は永続状態を変えないため、メンテナンス中でも登録できる（close 後は拒否する）。
        """
        self._check_not_closed()
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
        self._check_not_closed()
        if skip_current:
            # 早期拒否。確認から更新までの競合は ProjectionRunner 側の partition_write_guard が防ぐ。
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

    def enter_maintenance(
        self,
        reason: str,
        *,
        drain_projections: bool = False,
        timeout: float = 30.0,
    ) -> None:
        """メンテナンスモードへ移行し、バックアップ整合点を確立する（IF-007 / F-402）。"""
        self._maintenance.enter(reason, drain_projections=drain_projections, timeout=timeout)

    def exit_maintenance(self) -> None:
        """メンテナンスモードを解除し、通常運転を再開する（IF-008 / F-403）。"""
        self._maintenance.exit()

    def sync(
        self,
        cursors: Mapping[str, int],
        *,
        limit_per_partition: int = 100,
    ) -> dict[str, list[RecordEnvelope[T]]]:
        """複数シャードを横断して未処理差分を取得し、読み取り時 Upcast を適用して返す（IF-009 / F-301）。"""
        return self._sync_engine.sync(cursors, limit_per_partition=limit_per_partition)

    def close(
        self,
        *,
        timeout: float = 5.0,
        drain_projections: bool = False,
        drain_subscribers: bool = False,
    ) -> ShutdownReport:
        """新規書き込みを拒否し、進行中の書き込み完了後にワーカーを停止する（IF-010 / F-404）。"""
        return self._lifecycle.shutdown(
            timeout=timeout,
            drain_projections=drain_projections,
            drain_subscribers=drain_subscribers,
        )
