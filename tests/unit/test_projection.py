"""ProjectionRunner と DeadLetterCoordinator（DBF-DD-002 TR-DD2-001, 002, 004, 005）のユニットテスト。"""

from __future__ import annotations

from collections.abc import Sequence
import threading
import time
from typing import Any
import unittest

from storage_kernel.contracts.envelope import (
    SNAPSHOT_RECORD_TYPE,
    KernelConfig,
    RecordEnvelope,
    RetryPolicy,
)
from storage_kernel.core.kernel import StorageKernel
from storage_kernel.core.projection import ProjectionRunner
from storage_kernel.testing.memory_ports import (
    InMemoryDeadLetter,
    InMemoryMetaDAO,
    InMemoryProjection,
    InMemoryShardDAO,
    InMemoryStorageBackend,
    JsonDeterministicCodec,
    SimplePartitionRouter,
)


def _wait_until(predicate: Any, timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


class TestProjection(unittest.TestCase):
    """TR-DD2-001, TR-DD2-002, TR-DD2-004, TR-DD2-005 の検証。"""

    def setUp(self) -> None:
        self.store: InMemoryStorageBackend[dict[str, Any]] = InMemoryStorageBackend()
        self.shard_dao = InMemoryShardDAO(self.store)
        self.meta_dao = InMemoryMetaDAO(self.store)
        self.dlq = InMemoryDeadLetter(self.store)
        self.kernels: list[StorageKernel[dict[str, Any]]] = []

    def tearDown(self) -> None:
        for k in self.kernels:
            k._projection_runner.stop(drain=False)
            k._event_dispatcher.stop(drain=False)

    def _new_kernel(self, *, with_dlq: bool = True) -> StorageKernel[dict[str, Any]]:
        k = StorageKernel(
            config=KernelConfig(
                schema_registry={"tick": 1, SNAPSHOT_RECORD_TYPE: 1},
                default_retry_policy=RetryPolicy(max_attempts=2, initial_delay_sec=0.005, max_delay_sec=0.01),
            ),
            shard_dao=self.shard_dao,
            meta_dao=self.meta_dao,
            codec=JsonDeterministicCodec(),
            router=SimplePartitionRouter(),
            dead_letter_port=self.dlq if with_dlq else None,
        )
        self.kernels.append(k)
        return k

    def test_projection_catches_up_via_dirty_set_on_queue_overflow(self) -> None:
        """TR-DD2-001: 通知が落ちても Dirty Set + fetch_range 経由で自動的に全件追いつくこと。"""
        k1 = self._new_kernel()
        for i in range(1, 6):
            k1.append("chat:1", "tick", {"n": i}, f"idem-{i}", schema_version=1)

        # 再起動後、notify なしの既登録状態から起動時キャッチアップで追いつく
        k2 = self._new_kernel()
        proj = InMemoryProjection[dict[str, Any]]("proj_catchup")
        k2.register_projection(proj, batch_size=2)

        self.assertTrue(_wait_until(lambda: len(proj.records_for("chat:1")) == 5))
        self.assertEqual([r.seq for r in proj.records_for("chat:1")], [1, 2, 3, 4, 5])
        self.assertEqual(self.meta_dao.get_cursor("proj_catchup", "chat:1"), 5)

        # Event Bus への notify 呼び出しを差し替えても Dirty Set 登録だけで追いつける
        k2._projection_runner.notify(
            RecordEnvelope(
                partition_key="chat:1",
                seq=99,
                idempotency_key="gap",
                record_type="tick",
                schema_version=1,
                payload_hash="h",
                payload={"n": 6},
                created_at=time.time(),
            )
        )
        k2.append("chat:1", "tick", {"n": 6}, "idem-6", schema_version=1)
        self.assertTrue(_wait_until(lambda: len(proj.records_for("chat:1")) == 6))

    def test_cursor_not_advanced_on_apply_batch_failure_and_retried(self) -> None:
        """TR-DD2-002: apply_batch 例外時は advance_cursor が呼ばれず、再試行で回復すること。"""
        kernel = self._new_kernel(with_dlq=False)
        attempts = {"count": 0}
        applied: list[int] = []

        class _FlakyProjection:
            consumer_id = "flaky_proj"

            def apply_batch(self, records: Sequence[RecordEnvelope[dict[str, Any]]]) -> None:
                attempts["count"] += 1
                if attempts["count"] == 1:
                    raise RuntimeError("transient error")
                applied.extend(r.seq for r in records)

        kernel.register_projection(
            _FlakyProjection(),
            retry_policy=RetryPolicy(max_attempts=3, initial_delay_sec=0.005, max_delay_sec=0.01),
        )
        kernel.append("job:1", "tick", {"v": 1}, "idem-1", schema_version=1)

        self.assertTrue(_wait_until(lambda: self.meta_dao.get_cursor("flaky_proj", "job:1") == 1))
        self.assertEqual(applied, [1])
        self.assertGreaterEqual(attempts["count"], 2)

    def test_poison_pill_single_record_isolation_and_resume(self) -> None:
        """TR-DD2-004: Poison Pill 1件のみ DLQ 隔離され正常レコードが適用されること、及び pause/resume。"""
        # 1) dlq_and_skip: seq=2 だけ毒レコード、seq=1 と seq=3 は適用される
        k1 = self._new_kernel(with_dlq=True)
        for i in (1, 2, 3):
            k1.append("dlq:1", "tick", {"poison": i == 2, "n": i}, f"idem-{i}", schema_version=1)

        applied_dlq: list[int] = []
        lock = threading.Lock()

        class _PoisonRejectingProj:
            def __init__(self, cid: str) -> None:
                self._cid = cid

            @property
            def consumer_id(self) -> str:
                return self._cid

            def apply_batch(self, records: Sequence[RecordEnvelope[dict[str, Any]]]) -> None:
                for r in records:
                    if r.payload.get("poison"):
                        raise ValueError("poison pill encountered")
                with lock:
                    for r in records:
                        if r.seq not in applied_dlq:
                            applied_dlq.append(r.seq)

        k1.register_projection(
            _PoisonRejectingProj("proj_dlq"),
            retry_policy=RetryPolicy(max_attempts=2, initial_delay_sec=0.001, max_delay_sec=0.005),
            failure_policy="dlq_and_skip",
        )
        self.assertTrue(_wait_until(lambda: self.meta_dao.get_cursor("proj_dlq", "dlq:1") == 3))
        self.assertEqual(sorted(applied_dlq), [1, 3])
        dead = list(self.dlq.list_dead_letters(consumer_id="proj_dlq"))
        self.assertEqual(len(dead), 1)
        self.assertEqual(dead[0].envelope.seq, 2)

        # 2) pause_partition -> resume_projection(skip_current=True)
        applied_paused: list[int] = []

        class _PauseProj:
            consumer_id = "proj_pause"

            def apply_batch(self, records: Sequence[RecordEnvelope[dict[str, Any]]]) -> None:
                for r in records:
                    if r.payload.get("poison"):
                        raise ValueError("poison stops partition")
                for r in records:
                    if r.seq not in applied_paused:
                        applied_paused.append(r.seq)

        k1.register_projection(
            _PauseProj(),
            retry_policy=RetryPolicy(max_attempts=2, initial_delay_sec=0.001, max_delay_sec=0.005),
            failure_policy="pause_partition",
        )
        self.assertTrue(_wait_until(lambda: applied_paused == [1]))
        # seq=2 で一時停止しカーソルは 1 のまま
        time.sleep(0.05)
        self.assertEqual(self.meta_dao.get_cursor("proj_pause", "dlq:1"), 1)

        k1.resume_projection("proj_pause", "dlq:1", skip_current=True)
        self.assertTrue(_wait_until(lambda: self.meta_dao.get_cursor("proj_pause", "dlq:1") == 3))
        self.assertEqual(applied_paused, [1, 3])

    def test_snapshot_filtering_for_subscribers_and_projections(self) -> None:
        """TR-DD2-005: __snapshot__ は subscribe と既定 Projection に届かず include_snapshots=True にだけ届く。"""
        kernel = self._new_kernel()
        sub_types: list[str] = []
        kernel.subscribe("*", lambda env: sub_types.append(env.record_type))

        proj_default = InMemoryProjection[dict[str, Any]]("proj_no_snap")
        proj_with_snap = InMemoryProjection[dict[str, Any]]("proj_with_snap")
        kernel.register_projection(proj_default, include_snapshots=False)
        kernel.register_projection(proj_with_snap, include_snapshots=True)

        kernel.append("snap:1", "tick", {"v": 1}, "idem-1", schema_version=1)
        kernel.write_snapshot("snap:1", {"sum": 1}, "idem-s1", covers_through_seq=1, schema_version=1)
        kernel.append("snap:1", "tick", {"v": 2}, "idem-2", schema_version=1)

        self.assertTrue(_wait_until(lambda: self.meta_dao.get_cursor("proj_no_snap", "snap:1") == 3))
        self.assertTrue(_wait_until(lambda: self.meta_dao.get_cursor("proj_with_snap", "snap:1") == 3))
        self.assertTrue(_wait_until(lambda: len(sub_types) == 2))

        self.assertEqual(sub_types, ["tick", "tick"])
        self.assertEqual([r.record_type for r in proj_default.records_for("snap:1")], ["tick", "tick"])
        self.assertEqual(
            [r.record_type for r in proj_with_snap.records_for("snap:1")],
            ["tick", SNAPSHOT_RECORD_TYPE, "tick"],
        )


class _RunnerTestBase(unittest.TestCase):
    """ProjectionRunner を StorageKernel 経由の通知なしで直接組み立てるテストの共通土台。"""

    def setUp(self) -> None:
        self.store: InMemoryStorageBackend[dict[str, Any]] = InMemoryStorageBackend()
        self.shard_dao = InMemoryShardDAO(self.store)
        self.meta_dao = InMemoryMetaDAO(self.store)
        self.kernel: StorageKernel[dict[str, Any]] = StorageKernel(
            config=KernelConfig(schema_registry={"tick": 1}),
            shard_dao=self.shard_dao,
            meta_dao=self.meta_dao,
            codec=JsonDeterministicCodec(),
            router=SimplePartitionRouter(),
        )
        self.addCleanup(self.kernel.close)

    def _new_runner(
        self,
        *,
        catchup_interval_sec: float,
        meta_dao: Any = None,
        retry_policy: RetryPolicy | None = None,
        telemetry_sink: Any = None,
    ) -> ProjectionRunner[dict[str, Any]]:
        runner: ProjectionRunner[dict[str, Any]] = ProjectionRunner(
            telemetry_sink=telemetry_sink,
            shard_dao=self.shard_dao,
            meta_dao=meta_dao if meta_dao is not None else self.meta_dao,
            dead_letter_port=None,
            default_retry_policy=retry_policy or RetryPolicy(max_attempts=2, initial_delay_sec=0.005, max_delay_sec=0.01),
            decode_record_fn=self.kernel._upcast_pipeline.upcast_and_decode_envelope,
            catchup_interval_sec=catchup_interval_sec,
        )
        self.addCleanup(runner.stop, drain=False)
        return runner


class TestPeriodicCatchup(_RunnerTestBase):
    """TR-DD2-007: 通知を経由しない書き込みが定期ポーリングで Projection に届く。"""

    def test_periodic_catchup_delivers_records_without_notify(self) -> None:
        runner = self._new_runner(catchup_interval_sec=0.05)
        proj = InMemoryProjection[dict[str, Any]]("proj_periodic")
        runner.register(proj)

        # kernel の通知は別の ProjectionRunner に向かうため、runner は notify されない
        self.kernel.append("poll:1", "tick", {"n": 1}, "idem-1", schema_version=1)
        self.kernel.append("poll:1", "tick", {"n": 2}, "idem-2", schema_version=1)

        self.assertTrue(_wait_until(lambda: len(proj.records_for("poll:1")) == 2))
        self.assertEqual(self.meta_dao.get_cursor("proj_periodic", "poll:1"), 2)

    def test_catchup_disabled_when_interval_is_not_positive(self) -> None:
        runner = self._new_runner(catchup_interval_sec=0.0)
        proj = InMemoryProjection[dict[str, Any]]("proj_no_periodic")
        runner.register(proj)

        self.kernel.append("poll:1", "tick", {"n": 1}, "idem-1", schema_version=1)

        self.assertFalse(_wait_until(lambda: len(proj.records_for("poll:1")) == 1, timeout=0.4))


_SLOW_CALLER = "slow-caller"


class _SlowListMetaDAO(InMemoryMetaDAO):
    """スレッド名 `slow-caller` からの list_partitions だけを 0.3 秒遅延させる MetaDAO。"""

    def __init__(self, store: Any) -> None:
        super().__init__(store)
        self.slow_entered = threading.Event()

    def list_partitions(self) -> Any:
        if threading.current_thread().name == _SLOW_CALLER:
            self.slow_entered.set()
            time.sleep(0.3)
        return super().list_partitions()


class TestDaoCallsOutsideLock(_RunnerTestBase):
    """TR-DD2-008: 親 DAO（list_partitions）の呼び出し中も notify / register がブロックされない。"""

    def _assert_not_blocked_while(self, slow_call: Any) -> None:
        meta = _SlowListMetaDAO(self.store)
        runner = self._new_runner(catchup_interval_sec=0.0, meta_dao=meta)
        runner.register(InMemoryProjection[dict[str, Any]]("proj_a"))
        ack = self.kernel.append("lock:1", "tick", {"n": 1}, "idem-1", schema_version=1)

        slow = threading.Thread(target=lambda: slow_call(runner), name=_SLOW_CALLER, daemon=True)
        slow.start()
        self.addCleanup(slow.join, 2.0)
        self.assertTrue(meta.slow_entered.wait(timeout=1.0))

        started = time.monotonic()
        runner.notify(ack.envelope)
        runner.register(InMemoryProjection[dict[str, Any]]("proj_b"))
        elapsed = time.monotonic() - started

        self.assertLess(elapsed, 0.15, "list_partitions 実行中に notify/register がブロックされた")

    def test_stop_drain_does_not_hold_lock_while_listing_partitions(self) -> None:
        self._assert_not_blocked_while(lambda runner: runner.stop(drain=True, deadline=time.monotonic() + 3.0))

    def test_maintenance_drain_does_not_hold_lock_while_listing_partitions(self) -> None:
        self._assert_not_blocked_while(lambda runner: runner.enter_maintenance(drain=True, deadline=time.monotonic() + 3.0))


class _AlwaysFailingCursorMetaDAO(InMemoryMetaDAO):
    """fail_partition に対する get_cursor が常に例外を送出し、呼び出し回数を数える MetaDAO。"""

    def __init__(self, store: Any, fail_partition: str) -> None:
        super().__init__(store)
        self.fail_partition = fail_partition
        self.failing = True
        self.fail_calls = 0

    def get_cursor(self, consumer_id: str, partition_key: str) -> int:
        if self.failing and partition_key == self.fail_partition:
            self.fail_calls += 1
            raise RuntimeError("meta dao is down")
        return super().get_cursor(consumer_id, partition_key)


class _ListSink:
    """emit されたテレメトリイベントを保持するだけのシンク。"""

    def __init__(self) -> None:
        self.events: list[Any] = []

    def emit(self, event: Any) -> None:
        self.events.append(event)


class TestUnexpectedFailureBackoff(_RunnerTestBase):
    """TR-DD2-009: 想定外の例外はバックオフ付きで再投入され、上限到達で一時停止し、resume で再開できる。"""

    def test_backoff_pause_and_resume_after_repeated_unexpected_errors(self) -> None:
        meta = _AlwaysFailingCursorMetaDAO(self.store, "bad:1")
        sink = _ListSink()
        # 失敗 1〜3 回目の後の待機は 0.05 / 0.1 / 0.1 秒（4 回目の失敗で一時停止）
        policy = RetryPolicy(max_attempts=4, initial_delay_sec=0.05, backoff_multiplier=2.0, max_delay_sec=0.1)
        runner = self._new_runner(catchup_interval_sec=0.0, meta_dao=meta, retry_policy=policy, telemetry_sink=sink)
        self.kernel.append("bad:1", "tick", {"n": 1}, "idem-b", schema_version=1)
        self.kernel.append("good:1", "tick", {"n": 1}, "idem-g", schema_version=1)

        proj = InMemoryProjection[dict[str, Any]]("proj_backoff")
        started = time.monotonic()
        runner.register(proj)

        # 失敗中のパーティションがあっても、他のパーティションの投影は止まらない
        self.assertTrue(_wait_until(lambda: len(proj.records_for("good:1")) == 1))
        self.assertLess(time.monotonic() - started, 0.2)

        worker = runner._workers["proj_backoff"]
        self.assertTrue(_wait_until(lambda: "bad:1" in worker._paused_partitions))
        elapsed = time.monotonic() - started
        self.assertEqual(meta.fail_calls, 4)
        self.assertGreaterEqual(elapsed, 0.2)  # 0.05 + 0.1 + 0.1 のバックオフを待っている

        # 一時停止後は再選択されない（ビジーループにならない）
        time.sleep(0.3)
        self.assertEqual(meta.fail_calls, 4)
        paused_events = [e for e in sink.events if e.name == "projection.partition_paused"]
        self.assertEqual(len(paused_events), 1)
        self.assertEqual(paused_events[0].attributes["partition_key"], "bad:1")

        # 原因解消後に resume_projection 相当で再開できる
        meta.failing = False
        runner.resume_partition("proj_backoff", "bad:1")
        self.assertTrue(_wait_until(lambda: len(proj.records_for("bad:1")) == 1))
        self.assertEqual(meta.get_cursor("proj_backoff", "bad:1"), 1)

    def test_success_resets_consecutive_failure_counter(self) -> None:
        meta = _AlwaysFailingCursorMetaDAO(self.store, "bad:1")
        policy = RetryPolicy(max_attempts=3, initial_delay_sec=0.1, backoff_multiplier=1.0, max_delay_sec=0.1)
        runner = self._new_runner(catchup_interval_sec=0.0, meta_dao=meta, retry_policy=policy)
        self.kernel.append("bad:1", "tick", {"n": 1}, "idem-b", schema_version=1)
        proj = InMemoryProjection[dict[str, Any]]("proj_reset")
        runner.register(proj)
        worker = runner._workers["proj_reset"]

        self.assertTrue(_wait_until(lambda: meta.fail_calls >= 2))
        meta.failing = False  # 2 回失敗したところで回復（上限 3 には達しない）
        self.assertTrue(_wait_until(lambda: len(proj.records_for("bad:1")) == 1))
        self.assertTrue(_wait_until(lambda: "bad:1" not in worker._consecutive_failures))
        self.assertNotIn("bad:1", worker._paused_partitions)


if __name__ == "__main__":
    unittest.main()
