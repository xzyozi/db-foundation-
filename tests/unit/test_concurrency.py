"""並行制御（DBF-DD-004 TR-DD4-001）のユニットテスト。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any
import unittest

from storage_kernel.contracts.envelope import KernelConfig
from storage_kernel.contracts.errors import PartitionReadOnlyError
from storage_kernel.core.kernel import StorageKernel
from storage_kernel.testing.memory_ports import (
    InMemoryMetaDAO,
    InMemoryShardDAO,
    InMemoryStorageBackend,
    JsonDeterministicCodec,
    SimplePartitionRouter,
)


class TestConcurrency(unittest.TestCase):
    """TR-DD4-001: 状態遷移と append の並行実行で read_only 遷移後の書き込みが成功しない。"""

    def test_concurrent_status_transition_to_read_only_prevents_subsequent_writes(self) -> None:
        store: InMemoryStorageBackend[dict[str, Any]] = InMemoryStorageBackend()
        shard_dao = InMemoryShardDAO(store)
        meta_dao = InMemoryMetaDAO(store)
        kernel = StorageKernel(
            config=KernelConfig(schema_registry={"tick": 1}),
            shard_dao=shard_dao,
            meta_dao=meta_dao,
            codec=JsonDeterministicCodec(),
            router=SimplePartitionRouter(),
        )
        pk = "conc:1"
        kernel.append(pk, "tick", {"n": 0}, "idem-init", schema_version=1)

        def _writer(idx: int) -> bool:
            try:
                kernel.append(pk, "tick", {"n": idx}, f"idem-{idx}", schema_version=1)
                return True
            except PartitionReadOnlyError:
                return False

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(_writer, i) for i in range(1, 30)]
            kernel.set_partition_status(pk, "read_only", expected_status="active")
            # 遷移完了後に追加された書き込みは必ず拒否されること
            after_transition = [pool.submit(_writer, i) for i in range(100, 120)]
            for fut in futures:
                fut.result()
            after_results = [fut.result() for fut in after_transition]

        self.assertEqual(after_results, [False] * 20)
        info = meta_dao.get_partition(pk)
        self.assertIsNotNone(info)
        assert info is not None
        self.assertEqual(info.status, "read_only")
        rows = list(shard_dao.fetch_range(info.shard_id, pk, after_seq=0, limit=200))
        self.assertEqual([r.seq for r in rows], list(range(1, len(rows) + 1)))


if __name__ == "__main__":
    unittest.main()
