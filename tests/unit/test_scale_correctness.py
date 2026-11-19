"""Issue #7 向けの CI 用小規模正確性テスト（seq 欠落・重複・逆転なし検証）。"""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any
import unittest

from storage_kernel import KernelConfig, StorageKernel
from storage_kernel.contrib.sqlite import SqliteMetaDAO, SqliteShardDAO
from storage_kernel.testing.memory_ports import JsonDeterministicCodec, SimplePartitionRouter


class TestScaleCorrectness(unittest.TestCase):
    """CI 向けに件数を絞って連続書き込み・分散書き込み・全件再生の seq 整合性を検証する。"""

    def test_single_and_multi_partition_writes_and_full_replay_have_no_seq_anomalies(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            codec = JsonDeterministicCodec()
            meta = SqliteMetaDAO(base / "meta.db", durability="power_loss")
            shard = SqliteShardDAO[bytes](base / "shards", codec=None, durability="power_loss")

            kernel = StorageKernel(
                config=KernelConfig(schema_registry={"bench": 1}),
                shard_dao=shard,
                meta_dao=meta,
                codec=codec,
                router=SimplePartitionRouter(default_shards=4),
                store_raw_bytes=True,
            )
            try:
                single_count = 600
                for idx in range(single_count):
                    ack = kernel.append(
                        "benchmark:single",
                        "bench",
                        {"n": idx, "pad": "x" * 128},
                        f"single-{idx}",
                        schema_version=1,
                    )
                    self.assertEqual(ack.envelope.seq, idx + 1)

                multi_partitions = 10
                multi_total = 400
                for idx in range(multi_total):
                    pk = f"benchmark:multi:{idx % multi_partitions:03d}"
                    kernel.append(pk, "bench", {"n": idx}, f"multi-{idx}", schema_version=1)
            finally:
                kernel.close()

            # 再起動（新インスタンス）後にカーソル 0 から全件再生し seq の欠落・重複・逆転がないこと
            kernel_restarted = StorageKernel(
                config=KernelConfig(schema_registry={"bench": 1}),
                shard_dao=shard,
                meta_dao=meta,
                codec=codec,
                router=SimplePartitionRouter(default_shards=4),
                store_raw_bytes=True,
            )
            try:
                cursor = 0
                replayed_seqs: list[int] = []
                while True:
                    batch = kernel_restarted.sync({"benchmark:single": cursor}, limit_per_partition=100)
                    records = batch.get("benchmark:single", [])
                    if not records:
                        break
                    replayed_seqs.extend(r.seq for r in records)
                    cursor = records[-1].seq
                self.assertEqual(replayed_seqs, list(range(1, single_count + 1)))

                multi_cursors = {f"benchmark:multi:{i:03d}": 0 for i in range(multi_partitions)}
                multi_seqs: dict[str, list[int]] = {pk: [] for pk in multi_cursors}
                while True:
                    m_batch: dict[str, list[Any]] = kernel_restarted.sync(multi_cursors, limit_per_partition=25)
                    if not m_batch:
                        break
                    for pk, recs in m_batch.items():
                        multi_seqs[pk].extend(r.seq for r in recs)
                        multi_cursors[pk] = recs[-1].seq
                for pk, seqs in multi_seqs.items():
                    self.assertEqual(seqs, list(range(1, (multi_total // multi_partitions) + 1)), pk)
            finally:
                kernel_restarted.close()


if __name__ == "__main__":
    unittest.main()
