"""メンテナンスモード（DBF-DD-004 TR-DD4-002）のユニットテスト。"""

from __future__ import annotations

from contextlib import contextmanager
import threading
from typing import Any, Iterator
import unittest

from storage_kernel import KernelConfig, KernelMaintenanceError, StorageKernel
from storage_kernel.testing.memory_ports import (
    InMemoryMetaDAO,
    InMemoryShardDAO,
    InMemoryStorageBackend,
    JsonDeterministicCodec,
    SimplePartitionRouter,
)


class TestMaintenance(unittest.TestCase):
    """TR-DD4-002: メンテナンス遮断・進行中 Tx 待機・フック順序・解除の検証。"""

    def test_maintenance_mode_blocks_writes_and_waits_for_in_flight_transactions(self) -> None:
        store: InMemoryStorageBackend[dict[str, Any]] = InMemoryStorageBackend()
        base_shard = InMemoryShardDAO(store)
        meta_dao = InMemoryMetaDAO(store)

        tx_entered = threading.Event()
        release_tx = threading.Event()
        events: list[str] = []

        class _SlowFirstTxShard:
            durability = base_shard.durability

            def __init__(self) -> None:
                self._slow_once = True

            @contextmanager
            def write_transaction(self, shard_id: str) -> Iterator[Any]:
                with base_shard.write_transaction(shard_id) as tx:
                    if self._slow_once:
                        self._slow_once = False
                        tx_entered.set()
                        release_tx.wait(timeout=2.0)
                    yield tx
                    events.append("tx_committed")

            def fetch_range(self, shard_id: str, partition_key: str, after_seq: int, limit: int) -> Any:
                return base_shard.fetch_range(shard_id, partition_key, after_seq, limit)

        class _Hooks:
            def on_enter_maintenance(self, reason: str) -> None:
                events.append(f"enter:{reason}")

            def on_exit_maintenance(self) -> None:
                events.append("exit")

        kernel = StorageKernel(
            config=KernelConfig(schema_registry={"tick": 1}),
            shard_dao=_SlowFirstTxShard(),
            meta_dao=meta_dao,
            codec=JsonDeterministicCodec(),
            router=SimplePartitionRouter(),
            maintenance_hooks=_Hooks(),
        )
        try:
            t_write = threading.Thread(
                target=lambda: kernel.append("m:1", "tick", {"v": 1}, "idem-1", schema_version=1)
            )
            t_write.start()
            self.assertTrue(tx_entered.wait(timeout=1.0))

            t_maint = threading.Thread(
                target=lambda: kernel.enter_maintenance("backup", timeout=3.0)
            )
            t_maint.start()

            # enter_maintenance が進行中遮断に入った後、新規書き込みは KernelMaintenanceError になる
            # （tx_committed の前に enter が呼ばれないこと）
            import time

            time.sleep(0.05)
            with self.assertRaises(KernelMaintenanceError):
                kernel.append("m:2", "tick", {"v": 2}, "idem-2", schema_version=1)
            with self.assertRaises(KernelMaintenanceError):
                kernel.set_partition_status("m:1", "read_only")

            self.assertNotIn("enter:backup", events)
            release_tx.set()
            t_write.join(timeout=2.0)
            t_maint.join(timeout=2.0)

            self.assertEqual(events, ["tx_committed", "enter:backup"])

            # メンテナンス中は依然として書き込み不可
            with self.assertRaises(KernelMaintenanceError):
                kernel.append("m:1", "tick", {"v": 2}, "idem-2", schema_version=1)

            kernel.exit_maintenance()
            self.assertEqual(events[-1], "exit")

            # 解除後は通常どおり書き込み可能
            ack = kernel.append("m:1", "tick", {"v": 2}, "idem-2", schema_version=1)
            self.assertEqual(ack.envelope.seq, 2)
        finally:
            release_tx.set()
            kernel.close()


if __name__ == "__main__":
    unittest.main()
