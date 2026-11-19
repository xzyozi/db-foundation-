"""グレースフルシャットダウン（DBF-DD-004 TR-DD4-003, TR-DD4-004）のユニットテスト。"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import contextmanager
import threading
import time
from typing import Any, Iterator
import unittest

from storage_kernel import KernelClosedError, KernelConfig, RecordEnvelope, StorageKernel
from storage_kernel.testing.memory_ports import (
    InMemoryMetaDAO,
    InMemoryProjection,
    InMemoryShardDAO,
    InMemoryStorageBackend,
    JsonDeterministicCodec,
    SimplePartitionRouter,
)


class TestLifecycle(unittest.TestCase):
    """TR-DD4-003, TR-DD4-004 の検証。"""

    def test_close_rejects_new_writes_and_completes_in_flight_writes(self) -> None:
        """TR-DD4-003: close() 開始後の新規 append は KernelClosedError になり、進行中 Tx は完了する。"""
        store: InMemoryStorageBackend[dict[str, Any]] = InMemoryStorageBackend()
        base_shard = InMemoryShardDAO(store)
        meta_dao = InMemoryMetaDAO(store)

        tx_entered = threading.Event()
        release_tx = threading.Event()
        write_result: list[int] = []

        class _SlowTxShard:
            durability = base_shard.durability

            def __init__(self) -> None:
                self._first = True

            @contextmanager
            def write_transaction(self, shard_id: str) -> Iterator[Any]:
                with base_shard.write_transaction(shard_id) as tx:
                    if self._first:
                        self._first = False
                        tx_entered.set()
                        release_tx.wait(timeout=2.0)
                    yield tx

            def fetch_range(self, shard_id: str, partition_key: str, after_seq: int, limit: int) -> Any:
                return base_shard.fetch_range(shard_id, partition_key, after_seq, limit)

        kernel = StorageKernel(
            config=KernelConfig(schema_registry={"tick": 1}),
            shard_dao=_SlowTxShard(),
            meta_dao=meta_dao,
            codec=JsonDeterministicCodec(),
            router=SimplePartitionRouter(),
        )

        t_write = threading.Thread(
            target=lambda: write_result.append(
                kernel.append("life:1", "tick", {"v": 1}, "idem-1", schema_version=1).envelope.seq
            )
        )
        t_write.start()
        self.assertTrue(tx_entered.wait(timeout=1.0))

        reports = []
        t_close = threading.Thread(target=lambda: reports.append(kernel.close(timeout=3.0)))
        t_close.start()
        time.sleep(0.05)

        with self.assertRaises(KernelClosedError):
            kernel.append("life:2", "tick", {"v": 2}, "idem-2", schema_version=1)

        release_tx.set()
        t_write.join(timeout=2.0)
        t_close.join(timeout=2.0)

        self.assertEqual(write_result, [1])
        self.assertEqual(len(reports), 1)
        self.assertTrue(reports[0].completed_cleanly)
        # close() は冪等に同じレポートを返す
        self.assertEqual(kernel.close(), reports[0])

    def test_close_drain_options_and_timeout_report(self) -> None:
        """TR-DD4-004: drain オプションおよび timeout 超過時の ShutdownReport を検証する。"""
        # 1) drain_projections=True で最新 seq まで完了すること
        store1: InMemoryStorageBackend[dict[str, Any]] = InMemoryStorageBackend()
        k1 = StorageKernel(
            config=KernelConfig(schema_registry={"tick": 1}),
            shard_dao=InMemoryShardDAO(store1),
            meta_dao=InMemoryMetaDAO(store1),
            codec=JsonDeterministicCodec(),
            router=SimplePartitionRouter(),
        )
        proj = InMemoryProjection[dict[str, Any]]("drain_proj")
        k1.register_projection(proj)
        for i in range(1, 5):
            k1.append("drain:1", "tick", {"i": i}, f"i-{i}", schema_version=1)

        report_ok = k1.close(timeout=3.0, drain_projections=True, drain_subscribers=True)
        self.assertTrue(report_ok.completed_cleanly)
        self.assertEqual(len(proj.records_for("drain:1")), 4)

        # 2) Projection がブロック中で timeout 超過してもハングせず completed_cleanly=False を返す
        store2: InMemoryStorageBackend[dict[str, Any]] = InMemoryStorageBackend()
        k2 = StorageKernel(
            config=KernelConfig(schema_registry={"tick": 1}),
            shard_dao=InMemoryShardDAO(store2),
            meta_dao=InMemoryMetaDAO(store2),
            codec=JsonDeterministicCodec(),
            router=SimplePartitionRouter(),
        )
        proj_started = threading.Event()
        proj_release = threading.Event()

        class _BlockingProj:
            consumer_id = "blocking_proj"

            def apply_batch(self, records: Sequence[RecordEnvelope[dict[str, Any]]]) -> None:
                proj_started.set()
                proj_release.wait(timeout=2.0)

        k2.register_projection(_BlockingProj())
        k2.append("block:1", "tick", {"i": 1}, "b-1", schema_version=1)
        self.assertTrue(proj_started.wait(timeout=1.0))

        report_timeout = k2.close(timeout=0.05)
        self.assertFalse(report_timeout.completed_cleanly)
        proj_release.set()


if __name__ == "__main__":
    unittest.main()
