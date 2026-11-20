"""SQLite 参照実装と障害注入ハーネスを用いた結合障害シナリオテスト。"""

from __future__ import annotations

from pathlib import Path
import tempfile
import time
from typing import Any
import unittest

from storage_kernel import KernelConfig, RetryPolicy, StorageKernel
from storage_kernel.contrib.sqlite import SqliteDeadLetterDAO, SqliteMetaDAO, SqliteShardDAO
from storage_kernel.testing.fault_inject import (
    FaultInjectingProjection,
    FaultInjectingShardDAO,
    InjectedFaultError,
)
from storage_kernel.testing.memory_ports import (
    InMemoryProjection,
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


class TestFaultInjection(unittest.TestCase):
    """障害注入（Tx 失敗ロールバック・DLQ 隔離・再起動リカバリ）の結合テスト。"""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._base_dir = Path(self._tmpdir.name)
        self._codec = JsonDeterministicCodec()
        self._meta = SqliteMetaDAO(self._base_dir / "meta.db")
        self._dlq = SqliteDeadLetterDAO[dict[str, Any]](self._base_dir / "meta.db", codec=self._codec)
        self._base_shard = SqliteShardDAO[bytes](self._base_dir, codec=None)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_write_tx_failure_rolls_back_and_retry_succeeds(self) -> None:
        """1回目の write_transaction 失敗がロールバックされ、同じ idempotency_key の再試行が seq=1 で成功する。"""
        faulty_shard = FaultInjectingShardDAO(self._base_shard, fail_on_tx_calls={1})
        kernel = StorageKernel(
            config=KernelConfig(schema_registry={"tick": 1}),
            shard_dao=faulty_shard,
            meta_dao=self._meta,
            codec=self._codec,
            router=SimplePartitionRouter(),
            store_raw_bytes=True,
        )
        try:
            with self.assertRaises(InjectedFaultError):
                kernel.append("fi:1", "tick", {"v": 1}, "idem-retry", schema_version=1)

            ack = kernel.append("fi:1", "tick", {"v": 1}, "idem-retry", schema_version=1)
            self.assertEqual(ack.status, "committed")
            self.assertEqual(ack.envelope.seq, 1)
        finally:
            kernel.close()

    def test_sqlite_projection_fault_isolation_and_restart_recovery(self) -> None:
        """一時失敗のリトライ回復・Poison Pill の SQLite DLQ 隔離・再起動後追いつきを検証する。"""
        k1 = StorageKernel(
            config=KernelConfig(
                schema_registry={"tick": 1},
                default_retry_policy=RetryPolicy(max_attempts=2, initial_delay_sec=0.005, max_delay_sec=0.01),
            ),
            shard_dao=self._base_shard,
            meta_dao=self._meta,
            codec=self._codec,
            router=SimplePartitionRouter(),
            dead_letter_port=self._dlq,
            store_raw_bytes=True,
        )
        k1.append("fi:proj", "tick", {"n": 1}, "k1", schema_version=1)
        k1.append("fi:proj", "tick", {"n": 2, "bad": True}, "k2", schema_version=1)
        k1.append("fi:proj", "tick", {"n": 3}, "k3", schema_version=1)
        k1.close()

        # 再起動後に FaultInjectingProjection で 1回目失敗 + bad=True 失敗を注入
        k2 = StorageKernel(
            config=KernelConfig(
                schema_registry={"tick": 1},
                default_retry_policy=RetryPolicy(max_attempts=2, initial_delay_sec=0.005, max_delay_sec=0.01),
            ),
            shard_dao=self._base_shard,
            meta_dao=self._meta,
            codec=self._codec,
            router=SimplePartitionRouter(),
            dead_letter_port=self._dlq,
            store_raw_bytes=True,
        )
        try:
            inner_proj = InMemoryProjection[dict[str, Any]]("sqlite_fi_proj")
            faulty_proj = FaultInjectingProjection(
                inner_proj,
                fail_calls={1},
                fail_predicate=lambda rec: bool(rec.payload.get("bad")),
            )
            k2.register_projection(faulty_proj, failure_policy="dlq_and_skip")

            self.assertTrue(_wait_until(lambda: self._meta.get_cursor("sqlite_fi_proj", "fi:proj") == 3))
            self.assertEqual([r.seq for r in inner_proj.records_for("fi:proj")], [1, 3])
            dead = list(self._dlq.list_dead_letters(consumer_id="sqlite_fi_proj"))
            self.assertEqual(len(dead), 1)
            self.assertEqual(dead[0].envelope.seq, 2)
        finally:
            k2.close()


if __name__ == "__main__":
    unittest.main()
