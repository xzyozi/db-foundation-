"""EventDispatcher（DBF-DD-002 TR-DD2-003, TR-DD2-006）のユニットテスト。"""

from __future__ import annotations

import threading
import time
from typing import Any
import unittest

from storage_kernel.contracts.envelope import KernelConfig, RecordEnvelope
from storage_kernel.core.event_bus import PatternMatcher
from storage_kernel.core.kernel import StorageKernel
from storage_kernel.testing.memory_ports import (
    InMemoryMetaDAO,
    InMemoryProjection,
    InMemoryShardDAO,
    InMemoryStorageBackend,
    JsonDeterministicCodec,
    SimplePartitionRouter,
)


def _wait_until(predicate: Any, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


class TestEventBus(unittest.TestCase):
    """TR-DD2-003, TR-DD2-006 の検証。"""

    def setUp(self) -> None:
        self.store: InMemoryStorageBackend[dict[str, Any]] = InMemoryStorageBackend()
        self.kernel = StorageKernel(
            config=KernelConfig(schema_registry={"tick": 1}),
            shard_dao=InMemoryShardDAO(self.store),
            meta_dao=InMemoryMetaDAO(self.store),
            codec=JsonDeterministicCodec(),
            router=SimplePartitionRouter(),
        )

    def tearDown(self) -> None:
        self.kernel._projection_runner.stop(drain=False)
        self.kernel._event_dispatcher.stop(drain=False)

    def test_subscription_pattern_matching_and_validation(self) -> None:
        """TR-DD2-006: 完全一致・*・末尾ワイルドカードが照合され、不正パターンは ValueError。"""
        self.assertTrue(PatternMatcher("*").matches("chat:general"))
        self.assertTrue(PatternMatcher("chat:general").matches("chat:general"))
        self.assertFalse(PatternMatcher("chat:general").matches("chat:random"))
        self.assertTrue(PatternMatcher("chat:*").matches("chat:general"))
        self.assertFalse(PatternMatcher("chat:*").matches("order:1"))

        for bad in ("", "*chat", "chat:*:sub", "**", "a*b"):
            with self.assertRaises(ValueError):
                PatternMatcher(bad)

        received_prefix: list[str] = []
        handle = self.kernel.subscribe("chat:*", lambda env: received_prefix.append(env.partition_key))
        self.kernel.append("chat:room1", "tick", {"m": 1}, "i1", schema_version=1)
        self.kernel.append("order:1", "tick", {"m": 2}, "i2", schema_version=1)
        self.assertTrue(_wait_until(lambda: len(received_prefix) == 1))
        self.assertEqual(received_prefix, ["chat:room1"])
        handle.unsubscribe()
        self.assertFalse(handle.is_active)

    def test_slow_subscriber_isolation_and_drop_policies(self) -> None:
        """TR-DD2-003: 遅い/例外購読者が他購読者と Projection を止めず、drop_policy ごとに dropped_count が出ること。"""
        gate = threading.Event()
        fast_received: list[int] = []
        error_events: list[tuple[int, str]] = []

        proj = InMemoryProjection[dict[str, Any]]("proj_fast")
        self.kernel.register_projection(proj)

        def _slow_cb(_env: RecordEnvelope[dict[str, Any]]) -> None:
            gate.wait(timeout=2.0)

        def _err_cb(_env: RecordEnvelope[dict[str, Any]]) -> None:
            raise RuntimeError("boom")

        h_drop_newest = self.kernel.subscribe(
            "stream:*",
            _slow_cb,
            max_queue_size=1,
            drop_policy="drop_newest",
        )
        h_drop_oldest = self.kernel.subscribe(
            "stream:*",
            _slow_cb,
            max_queue_size=1,
            drop_policy="drop_oldest",
        )
        h_disconnect = self.kernel.subscribe(
            "stream:*",
            _slow_cb,
            max_queue_size=1,
            drop_policy="disconnect",
        )
        self.kernel.subscribe("stream:*", lambda env: fast_received.append(env.seq))
        self.kernel.subscribe(
            "stream:*",
            _err_cb,
            on_error=lambda env, exc: error_events.append((env.seq, type(exc).__name__)),
        )

        # 最初の 1 件はワーカーが取り出して gate.wait でブロック、以降 3 件は満杯トリガー
        for i in range(1, 5):
            self.kernel.append("stream:1", "tick", {"n": i}, f"idem-{i}", schema_version=1)
            if i == 1:
                # 1 件目がワーカーに取り込まれるのを少し待つ
                time.sleep(0.03)

        self.assertTrue(_wait_until(lambda: len(fast_received) == 4))
        self.assertTrue(_wait_until(lambda: len(error_events) == 4))
        self.assertTrue(_wait_until(lambda: len(proj.records_for("stream:1")) == 4))

        self.assertGreaterEqual(h_drop_newest.dropped_count, 1)
        self.assertGreaterEqual(h_drop_oldest.dropped_count, 1)
        self.assertEqual(h_disconnect.dropped_count, 1)
        self.assertFalse(h_disconnect.is_active)

        gate.set()


if __name__ == "__main__":
    unittest.main()
