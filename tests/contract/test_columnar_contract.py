"""業務カラム展開 DAO（examples/columnar_dao）に対する Contract Test 13 項目と、往復・結合の検証（Issue #53）。

`T`（dict）のまま DAO に渡し、DAO が業務カラムへ展開する形でも、基盤の契約を満たせるかを確かめる。
"""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
from typing import Any
import unittest

from examples.columnar_dao import ColumnarShardDAO
from storage_kernel import KernelConfig, RecordEnvelope, StorageKernel
from storage_kernel.contrib.sqlite import SqliteMetaDAO
from storage_kernel.ports.dao import ProjectionPort
from storage_kernel.testing import contract_test
from storage_kernel.testing.memory_ports import (
    InMemoryProjection,
    JsonDeterministicCodec,
    MappingPayloadUpcaster,
    SimplePartitionRouter,
)


def _order_payload(n: int) -> dict[str, Any]:
    """専用カラム（symbol・qty・price）と、extra へ入るキー、型が合わず extra へ残る値を、すべて含むペイロード。"""
    return {
        "symbol": f"SYM{n % 3}",
        "qty": n,
        "price": n + 0.5,
        "note": f"order-{n}",  # extra へ入る
        "tags": ["a", "b"],  # extra へ入る
    }


class TestColumnarContract(contract_test.StorageKernelContractTest[dict[str, Any]]):
    """業務カラム展開の DAO で、Contract Test 13 項目（再接続永続性を含む）を検証する。"""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._base_dir = Path(self._tmpdir.name)
        self._codec = JsonDeterministicCodec()
        self._projection: InMemoryProjection[dict[str, Any]] = InMemoryProjection("columnar_contract_proj")

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def projected_seqs(self, projection: ProjectionPort[dict[str, Any]], partition_key: str) -> list[int]:
        assert isinstance(projection, InMemoryProjection)
        return [rec.seq for rec in projection.records_for(partition_key)]

    def make_dao(self) -> contract_test.DAOContractBundle[dict[str, Any]]:
        upcaster = MappingPayloadUpcaster(
            {
                ("tick", 1): lambda raw: json.dumps(
                    {**json.loads(raw.decode("utf-8")), "v": 2},
                    sort_keys=True,
                ).encode("utf-8"),
            }
        )
        return contract_test.DAOContractBundle(
            shard_dao=ColumnarShardDAO(self._base_dir / "shards"),
            meta_dao=SqliteMetaDAO(self._base_dir / "meta.db"),
            projection=self._projection,
            codec=self._codec,
            upcaster=upcaster,
            make_payload=_order_payload,
            equivalent_payloads=({"symbol": "A", "qty": 1}, {"qty": 1, "symbol": "A"}),
            valid_upcast_input=("tick", 1, b'{"price":100}'),
            unknown_upcast_input=("tick", 999, b'{"price":100}'),
        )


class TestColumnarRoundTrip(unittest.TestCase):
    """契約の範囲外で、業務カラム展開の往復が値を変えないことと、業務カラムでの検索を確かめる。"""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._root = Path(self._tmpdir.name)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def _kernel(self) -> tuple[StorageKernel[dict[str, Any]], ColumnarShardDAO]:
        dao = ColumnarShardDAO(self._root / "shards")
        kernel: StorageKernel[dict[str, Any]] = StorageKernel(
            config=KernelConfig(schema_registry={"order": 1}),
            shard_dao=dao,
            meta_dao=SqliteMetaDAO(self._root / "meta.db"),
            codec=JsonDeterministicCodec(),
            router=SimplePartitionRouter(default_shards=1),
            store_raw_bytes=False,
        )
        self.addCleanup(kernel.close)
        return kernel, dao

    def test_kernel_with_store_raw_bytes_false_round_trips_payload(self) -> None:
        """`store_raw_bytes=False` で、dict がそのまま DAO に渡り、`sync` で同じ値が戻る。"""
        kernel, _ = self._kernel()
        original = _order_payload(7)
        ack = kernel.append("order:1", "order", original, "k1", schema_version=1)
        self.assertEqual(ack.status, "committed")

        synced = kernel.sync({"order:1": 0})
        self.assertEqual([r.payload for r in synced["order:1"]], [original])

        # 同じ内容の再送は、冪等に処理される。内容が違う再送は、IdempotencyConflict になる。
        dup = kernel.append("order:1", "order", dict(original), "k1", schema_version=1)
        self.assertEqual((dup.status, dup.envelope.seq), ("duplicate", 1))

    def test_values_that_do_not_fit_columns_are_kept_in_extra(self) -> None:
        """型が合わない値（qty が文字列、bool、price が文字列）や未知のキーが、変わらずに戻る。"""
        kernel, _ = self._kernel()
        odd: dict[str, Any] = {
            "symbol": "X",
            "qty": "10",  # 文字列なので専用カラムに入れず extra へ
            "price": True,  # bool は数値として扱わない
            "unknown": {"nested": [1, 2, 3]},
        }
        kernel.append("order:2", "order", odd, "k-odd", schema_version=1)
        got = kernel.sync({"order:2": 0})["order:2"][0].payload
        self.assertEqual(got, odd)
        self.assertIsInstance(got["qty"], str)
        self.assertIs(got["price"], True)

    def test_columns_can_be_queried_without_decoding_payloads(self) -> None:
        """業務カラムに展開した利点: ペイロードを全件読み出さずに、`symbol` で件数を数えられる。"""
        kernel, dao = self._kernel()
        for n in range(1, 7):
            kernel.append("order:3", "order", {"symbol": "AAA" if n % 2 else "BBB", "qty": n}, f"k{n}", schema_version=1)
        shard_id = SimplePartitionRouter(default_shards=1).select_shard("order:3")
        self.assertEqual(dao.count_by_symbol(shard_id, "AAA"), 3)
        self.assertEqual(dao.count_by_symbol(shard_id, "BBB"), 3)
        self.assertEqual(dao.count_by_symbol(shard_id, "CCC"), 0)

    def test_envelope_type_is_unchanged(self) -> None:
        """DAO が返すのは、基盤の `RecordEnvelope`（契約型）のままである。"""
        kernel, dao = self._kernel()
        kernel.append("order:4", "order", {"symbol": "Z", "qty": 1}, "k1", schema_version=1)
        shard_id = SimplePartitionRouter(default_shards=1).select_shard("order:4")
        rows = dao.fetch_range(shard_id, "order:4", 0, 10)
        self.assertEqual(len(rows), 1)
        self.assertIsInstance(rows[0], RecordEnvelope)


if __name__ == "__main__":
    unittest.main()
