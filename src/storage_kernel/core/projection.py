"""Dirty Set 追いつきと At-Least-Once 非同期投影ワーカー（ProjectionRunner）。"""

from __future__ import annotations

from collections.abc import Callable, Sequence
import logging
import threading
import time
from typing import Any, Generic, TypeVar

from ..contracts.envelope import (
    ProjectionFailurePolicy,
    RecordEnvelope,
    RetryPolicy,
)
from ..contracts.errors import SchemaVersionError
from ..contracts.telemetry import TelemetryEvent, TelemetrySink
from ..ports.dao import DeadLetterPort, MetaDAOPort, ProjectionPort, ShardDAOPort
from .event_bus import PatternMatcher
from .reliability import DeadLetterCoordinator

T = TypeVar("T")

logger = logging.getLogger("storage_kernel.projection")

DEFAULT_CATCHUP_INTERVAL_SEC = 5.0
"""定期追いつき（Dirty Set への全パーティション再投入）の既定間隔（秒）。0 以下で無効。"""


class _ProjectionWorker(Generic[T]):
    """単一 ProjectionPort を駆動する独立ワーカースレッド。"""

    def __init__(
        self,
        *,
        projection: ProjectionPort[T],
        matchers: tuple[PatternMatcher, ...],
        batch_size: int,
        retry_policy: RetryPolicy,
        failure_policy: ProjectionFailurePolicy,
        include_snapshots: bool,
        shard_dao: ShardDAOPort[Any],
        meta_dao: MetaDAOPort,
        dead_letter_port: DeadLetterPort[T] | None,
        coordinator: DeadLetterCoordinator[T],
        decode_record_fn: Callable[[RecordEnvelope[Any]], RecordEnvelope[T]],
        telemetry_sink: TelemetrySink | None,
        poll_interval_sec: float = 0.25,
        catchup_interval_sec: float = 0.0,
    ) -> None:
        self.projection = projection
        self.consumer_id = projection.consumer_id
        self._matchers = matchers
        self._batch_size = batch_size
        self._retry_policy = retry_policy
        self._failure_policy = failure_policy
        self._include_snapshots = include_snapshots
        self._shard_dao = shard_dao
        self._meta_dao = meta_dao
        self._dead_letter_port = dead_letter_port
        self._coordinator = coordinator
        self._decode_record_fn = decode_record_fn
        self._telemetry_sink = telemetry_sink
        self._poll_interval_sec = poll_interval_sec

        self._cond = threading.Condition()
        self._dirty_partitions: set[str] = set()
        self._paused_partitions: set[str] = set()
        self._in_cycle = False
        self._maintenance_hold = False
        self._maintenance_drain = False
        self._stop_requested = False
        self._stop_drain = False
        self._stop_event = threading.Event()
        self._catchup_interval_sec = catchup_interval_sec
        self._catchup_stop = threading.Event()

        # 起動時キャッチアップ：既登録パーティションを Dirty Set へ初期投入
        self._dirty_partitions.update(self._list_matching_partition_keys())

        self._thread = threading.Thread(
            target=self._run,
            name=f"sk-proj-{self.consumer_id[:12]}",
            daemon=True,
        )
        self._thread.start()

        self._catchup_thread: threading.Thread | None = None
        if catchup_interval_sec > 0.0:
            self._catchup_thread = threading.Thread(
                target=self._catchup_loop,
                name=f"sk-proj-catchup-{self.consumer_id[:12]}",
                daemon=True,
            )
            self._catchup_thread.start()

    def _list_matching_partition_keys(self) -> list[str]:
        """登録パターンに一致する全パーティションキーを取得する（ロックを持たずに呼ぶこと）。"""
        return [p.partition_key for p in self._meta_dao.list_partitions() if self.matches(p.partition_key)]

    def _catchup_loop(self) -> None:
        """catchup_interval_sec ごとに全一致パーティションを Dirty Set へ再投入する（F-206 定期ポーリング）。"""
        while not self._catchup_stop.wait(timeout=self._catchup_interval_sec):
            try:
                keys = self._list_matching_partition_keys()
            except Exception:
                logger.exception("Periodic catch-up listing failed: consumer=%s", self.consumer_id)
                continue
            with self._cond:
                if self._stop_requested or self._maintenance_hold:
                    continue
                self._dirty_partitions.update(k for k in keys if k not in self._paused_partitions)
                self._cond.notify_all()

    def matches(self, partition_key: str) -> bool:
        return any(m.matches(partition_key) for m in self._matchers)

    def notify(self, partition_key: str) -> None:
        """書き込み確定やギャップ検知に伴い partition_key を Dirty Set に積んで起床する。"""
        if not self.matches(partition_key):
            return
        with self._cond:
            if self._stop_requested or partition_key in self._paused_partitions:
                return
            self._dirty_partitions.add(partition_key)
            self._cond.notify_all()

    def resume_partition(self, partition_key: str, *, skip_current: bool) -> None:
        """一時停止中のパーティションを再開する（skip_current=True で先頭 1 件をスキップ）。"""
        with self._cond:
            if skip_current:
                info = self._meta_dao.get_partition(partition_key)
                if info is not None:
                    current_seq = self._meta_dao.get_cursor(self.consumer_id, partition_key)
                    next_rows = self._shard_dao.fetch_range(
                        info.shard_id,
                        partition_key,
                        after_seq=current_seq,
                        limit=1,
                    )
                    if next_rows:
                        self._meta_dao.advance_cursor(self.consumer_id, partition_key, next_rows[0].seq)
            self._paused_partitions.discard(partition_key)
            if self.matches(partition_key):
                self._dirty_partitions.add(partition_key)
            self._cond.notify_all()

    def enter_maintenance_hold(self, *, drain: bool, deadline: float) -> bool:
        """メンテナンス用にカーソル更新を静止させる。"""
        with self._cond:
            self._maintenance_hold = True
            self._maintenance_drain = drain
            if drain:
                for p in self._meta_dao.list_partitions():
                    if self.matches(p.partition_key) and p.partition_key not in self._paused_partitions:
                        self._dirty_partitions.add(p.partition_key)
            self._cond.notify_all()

            while True:
                if not drain:
                    if not self._in_cycle:
                        return True
                else:
                    if not self._in_cycle and not self._dirty_partitions:
                        return True
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    return False
                self._cond.wait(timeout=min(remaining, 0.05))

    def exit_maintenance_hold(self) -> None:
        """メンテナンス静止を解除して投影を再開する。"""
        with self._cond:
            self._maintenance_hold = False
            self._maintenance_drain = False
            self._cond.notify_all()

    def stop(self, *, drain: bool, deadline: float) -> tuple[tuple[str, ...], bool]:
        """ワーカーを停止し、(未処理パーティションタプル, タイムアウト内に完了したか) を返す。"""
        self._catchup_stop.set()
        with self._cond:
            self._stop_requested = True
            self._stop_drain = drain
            if drain:
                for p in self._meta_dao.list_partitions():
                    if self.matches(p.partition_key) and p.partition_key not in self._paused_partitions:
                        self._dirty_partitions.add(p.partition_key)
            else:
                self._stop_event.set()
            self._cond.notify_all()

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                self._stop_event.set()
                with self._cond:
                    self._cond.notify_all()
                    pending = tuple(sorted(self._dirty_partitions))
                self._thread.join(timeout=0.05)
                return pending, False

            self._thread.join(timeout=min(remaining, 0.05))
            if not self._thread.is_alive():
                with self._cond:
                    pending = tuple(sorted(self._dirty_partitions))
                return pending, True

    def _emit_telemetry(self, name: str, attributes: dict[str, str | int | float | bool]) -> None:
        if self._telemetry_sink is not None:
            self._telemetry_sink.emit(TelemetryEvent(name=name, attributes=attributes))

    def _pick_next_partition(self) -> str | None:
        with self._cond:
            while True:
                if self._stop_requested:
                    if not self._stop_drain or not self._dirty_partitions:
                        return None
                if self._maintenance_hold:
                    if not self._maintenance_drain or not self._dirty_partitions:
                        self._cond.wait(timeout=self._poll_interval_sec)
                        continue

                candidates = sorted(self._dirty_partitions - self._paused_partitions)
                if candidates:
                    pk = candidates[0]
                    self._dirty_partitions.discard(pk)
                    self._in_cycle = True
                    return pk

                if self._stop_requested and self._stop_drain:
                    return None
                self._cond.wait(timeout=self._poll_interval_sec)

    def _run(self) -> None:
        while True:
            pk = self._pick_next_partition()
            if pk is None:
                return

            requeue = False
            try:
                info = self._meta_dao.get_partition(pk)
                if info is not None:
                    last_seq = self._meta_dao.get_cursor(self.consumer_id, pk)
                    raw_records = self._shard_dao.fetch_range(
                        info.shard_id,
                        pk,
                        after_seq=last_seq,
                        limit=self._batch_size,
                    )
                    if raw_records:
                        decoded_records = [self._decode_record_fn(r) for r in raw_records]
                        outcome = self._coordinator.apply_with_retry_and_isolate(
                            partition_key=pk,
                            records=decoded_records,
                            projection=self.projection,
                            meta_dao=self._meta_dao,
                            dead_letter_port=self._dead_letter_port,
                            retry_policy=self._retry_policy,
                            failure_policy=self._failure_policy,
                            include_snapshots=self._include_snapshots,
                            stop_event=self._stop_event,
                        )
                        if outcome == "paused":
                            with self._cond:
                                self._paused_partitions.add(pk)
                        elif outcome == "applied" and len(raw_records) >= self._batch_size:
                            requeue = True
                        elif outcome == "stopped":
                            requeue = True
            except SchemaVersionError:
                logger.exception(
                    "SchemaVersionError during projection catch-up: consumer=%s partition=%s",
                    self.consumer_id,
                    pk,
                )
                with self._cond:
                    self._paused_partitions.add(pk)
                self._emit_telemetry(
                    "projection.schema_version_error",
                    {"consumer_id": self.consumer_id, "partition_key": pk},
                )
            except Exception:
                logger.exception(
                    "Unexpected error in projection worker: consumer=%s partition=%s",
                    self.consumer_id,
                    pk,
                )
                requeue = True
            finally:
                with self._cond:
                    if requeue and pk not in self._paused_partitions:
                        self._dirty_partitions.add(pk)
                    self._in_cycle = False
                    self._cond.notify_all()


class ProjectionRunner(Generic[T]):
    """登録された全 ProjectionPort ワーカーを統括するランナー。"""

    def __init__(
        self,
        *,
        shard_dao: ShardDAOPort[Any],
        meta_dao: MetaDAOPort,
        dead_letter_port: DeadLetterPort[T] | None,
        default_retry_policy: RetryPolicy,
        decode_record_fn: Callable[[RecordEnvelope[Any]], RecordEnvelope[T]],
        telemetry_sink: TelemetrySink | None = None,
        catchup_interval_sec: float = DEFAULT_CATCHUP_INTERVAL_SEC,
    ) -> None:
        self._catchup_interval_sec = catchup_interval_sec
        self._shard_dao = shard_dao
        self._meta_dao = meta_dao
        self._dead_letter_port = dead_letter_port
        self._default_retry_policy = default_retry_policy
        self._decode_record_fn = decode_record_fn
        self._telemetry_sink = telemetry_sink
        self._coordinator: DeadLetterCoordinator[T] = DeadLetterCoordinator(telemetry_sink=telemetry_sink)

        self._lock = threading.RLock()
        self._workers: dict[str, _ProjectionWorker[T]] = {}

    def register(
        self,
        projection: ProjectionPort[T],
        *,
        partition_patterns: Sequence[str] = ("*",),
        batch_size: int = 100,
        retry_policy: RetryPolicy | None = None,
        failure_policy: ProjectionFailurePolicy | None = None,
        include_snapshots: bool = False,
    ) -> None:
        """Projection を登録し、専用バックグラウンドワーカーを起動する。"""
        if not partition_patterns:
            raise ValueError("partition_patterns must contain at least one pattern")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        matchers = tuple(PatternMatcher(p) for p in partition_patterns)

        if failure_policy is None:
            resolved_policy: ProjectionFailurePolicy = (
                "dlq_and_skip" if self._dead_letter_port is not None else "pause_partition"
            )
        else:
            if failure_policy == "dlq_and_skip" and self._dead_letter_port is None:
                raise ValueError("failure_policy='dlq_and_skip' requires DeadLetterPort")
            resolved_policy = failure_policy

        effective_retry = retry_policy if retry_policy is not None else self._default_retry_policy

        with self._lock:
            if projection.consumer_id in self._workers:
                raise ValueError(f"projection consumer_id {projection.consumer_id!r} is already registered")
            worker = _ProjectionWorker(
                projection=projection,
                matchers=matchers,
                batch_size=batch_size,
                retry_policy=effective_retry,
                failure_policy=resolved_policy,
                include_snapshots=include_snapshots,
                shard_dao=self._shard_dao,
                meta_dao=self._meta_dao,
                dead_letter_port=self._dead_letter_port,
                coordinator=self._coordinator,
                decode_record_fn=self._decode_record_fn,
                telemetry_sink=self._telemetry_sink,
                catchup_interval_sec=self._catchup_interval_sec,
            )
            self._workers[projection.consumer_id] = worker

    def notify(self, envelope: RecordEnvelope[T]) -> None:
        """書き込み確定イベントを一致する全 Projection ワーカーの Dirty Set へ通知する。"""
        with self._lock:
            workers = list(self._workers.values())
        for worker in workers:
            worker.notify(envelope.partition_key)

    def resume_partition(
        self,
        consumer_id: str,
        partition_key: str,
        *,
        skip_current: bool = False,
    ) -> None:
        """一時停止中のパーティション投影を再開する。"""
        with self._lock:
            worker = self._workers.get(consumer_id)
        if worker is None:
            raise ValueError(f"unknown projection consumer_id: {consumer_id!r}")
        worker.resume_partition(partition_key, skip_current=skip_current)

    def enter_maintenance(self, *, drain: bool, deadline: float) -> bool:
        """全 Projection ワーカーをメンテナンス静止状態へ移行する。"""
        with self._lock:
            workers = list(self._workers.values())
        for worker in workers:
            if not worker.enter_maintenance_hold(drain=drain, deadline=deadline):
                return False
        return True

    def exit_maintenance(self) -> None:
        """全 Projection ワーカーのメンテナンス静止を解除する。"""
        with self._lock:
            workers = list(self._workers.values())
        for worker in workers:
            worker.exit_maintenance_hold()

    def stop(self, *, drain: bool = False, deadline: float | None = None) -> tuple[tuple[str, ...], bool]:
        """全 Projection ワーカーを停止し、(未完了パーティション一覧, 全件クリーン完了) を返す。"""
        effective_deadline = deadline if deadline is not None else time.monotonic() + 5.0
        with self._lock:
            workers = list(self._workers.values())
        pending_all: set[str] = set()
        all_clean = True
        for worker in workers:
            pending, clean = worker.stop(drain=drain, deadline=effective_deadline)
            pending_all.update(pending)
            if not clean:
                all_clean = False
        return tuple(sorted(pending_all)), all_clean
