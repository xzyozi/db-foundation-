"""同期と版管理（DBF-DD-003 TR-DD3-001〜TR-DD3-004）のユニットテスト。"""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import time
from typing import Any
import unittest

from storage_kernel import (
    SNAPSHOT_RECORD_TYPE,
    KernelConfig,
    SchemaVersionError,
    StorageKernel,
)
from storage_kernel.contrib.sqlite import SqliteMetaDAO, SqliteShardDAO
from storage_kernel.testing.memory_ports import (
    InMemoryProjection,
    JsonDeterministicCodec,
    MappingPayloadUpcaster,
    SimplePartitionRouter,
)


def _wait_until(predicate: Any, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


class TestSyncEngine(unittest.TestCase):
    """TR-DD3-001 〜 TR-DD3-004 の検証。"""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._base_dir = Path(self._tmpdir.name)
        self._codec = JsonDeterministicCodec()
        self._meta = SqliteMetaDAO(self._base_dir / "meta.db")
        self._shard = SqliteShardDAO[bytes](self._base_dir, codec=None)
        self._router = SimplePartitionRouter(default_shards=4)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_multi_step_upcast_preserves_payload_hash_and_stored_record(self) -> None:
        """TR-DD3-001: v1 -> v2 -> v3 の Upcast が sync/Projection で適用され元データと hash が不変。"""
        # 1) v1 として書き込み
        k_v1 = StorageKernel(
            config=KernelConfig(schema_registry={"order": 1}),
            shard_dao=self._shard,
            meta_dao=self._meta,
            codec=self._codec,
            router=self._router,
            store_raw_bytes=True,
        )
        ack_v1 = k_v1.append("order:1", "order", {"amount": 10}, "idem-v1", schema_version=1)
        k_v1.close()

        # 2) v3 へアップグレードしたカーネルで読み取り（v1 -> v2 -> v3）
        upcaster = MappingPayloadUpcaster(
            {
                ("order", 1): lambda raw: json.dumps(
                    {**json.loads(raw.decode("utf-8")), "currency": "JPY"},
                    sort_keys=True,
                ).encode("utf-8"),
                ("order", 2): lambda raw: json.dumps(
                    {**json.loads(raw.decode("utf-8")), "v": 3},
                    sort_keys=True,
                ).encode("utf-8"),
            }
        )
        k_v3 = StorageKernel(
            config=KernelConfig(schema_registry={"order": 3}),
            shard_dao=self._shard,
            meta_dao=self._meta,
            codec=self._codec,
            router=self._router,
            upcaster=upcaster,
            store_raw_bytes=True,
        )
        try:
            synced = k_v3.sync({"order:1": 0})
            rows = synced["order:1"]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].schema_version, 3)
            self.assertEqual(rows[0].payload, {"amount": 10, "currency": "JPY", "v": 3})
            self.assertEqual(rows[0].payload_hash, ack_v1.envelope.payload_hash)

            # DB 上の保存済みレコードが v1 のまま書き換えられていないこと
            raw_db_rows = list(self._shard.fetch_range(ack_v1.shard_id, "order:1", after_seq=0, limit=10))
            self.assertEqual(raw_db_rows[0].schema_version, 1)
            self.assertEqual(raw_db_rows[0].payload, self._codec.encode({"amount": 10}))

            # Projection でも同様に v3 へ Upcast されること
            proj = InMemoryProjection[dict[str, Any]]("proj_upcast")
            k_v3.register_projection(proj)
            self.assertTrue(_wait_until(lambda: len(proj.records_for("order:1")) == 1))
            projected = proj.records_for("order:1")[0]
            self.assertEqual(projected.schema_version, 3)
            self.assertEqual(projected.payload, {"amount": 10, "currency": "JPY", "v": 3})
            self.assertEqual(projected.payload_hash, ack_v1.envelope.payload_hash)
        finally:
            k_v3.close()

    def test_missing_upcast_path_or_future_version_raises_schema_version_error(self) -> None:
        """TR-DD3-002: Upcast 経路欠落または未知の未来版で SchemaVersionError になる。"""
        k_v2 = StorageKernel(
            config=KernelConfig(schema_registry={"order": 2}),
            shard_dao=self._shard,
            meta_dao=self._meta,
            codec=self._codec,
            router=self._router,
            store_raw_bytes=True,
        )
        k_v2.append("order:err", "order", {"amount": 10, "currency": "JPY"}, "idem-1", schema_version=2)
        k_v2.close()

        # 未来の未知版（登録済みが v1 なのに DB に v2 がある）
        k_v1_old = StorageKernel(
            config=KernelConfig(schema_registry={"order": 1}),
            shard_dao=self._shard,
            meta_dao=self._meta,
            codec=self._codec,
            router=self._router,
            store_raw_bytes=True,
        )
        try:
            with self.assertRaises(SchemaVersionError):
                k_v1_old.sync({"order:err": 0})
        finally:
            k_v1_old.close()

        # 経路欠落（登録済み v4 だが v2 -> v3 変換がない）
        k_v4_incomplete = StorageKernel(
            config=KernelConfig(schema_registry={"order": 4}),
            shard_dao=self._shard,
            meta_dao=self._meta,
            codec=self._codec,
            router=self._router,
            upcaster=MappingPayloadUpcaster({("order", 3): lambda raw: raw}),
            store_raw_bytes=True,
        )
        try:
            with self.assertRaises(SchemaVersionError):
                k_v4_incomplete.sync({"order:err": 0})
        finally:
            k_v4_incomplete.close()

    def test_scatter_gather_sync_across_multiple_shards(self) -> None:
        """TR-DD3-003: 複数シャード横断で差分取得でき、read_only も含み未登録は作られない。"""
        k = StorageKernel(
            config=KernelConfig(schema_registry={"tick": 1}),
            shard_dao=self._shard,
            meta_dao=self._meta,
            codec=self._codec,
            router=self._router,
            store_raw_bytes=True,
        )
        try:
            k.append("p:alpha", "tick", {"n": 1}, "a1", schema_version=1, preferred_shard="shard_a")
            k.append("p:alpha", "tick", {"n": 2}, "a2", schema_version=1, preferred_shard="shard_a")
            k.append("p:beta", "tick", {"n": 10}, "b1", schema_version=1, preferred_shard="shard_b")
            k.set_partition_status("p:beta", "read_only", expected_status="active")

            res = k.sync({"p:alpha": 1, "p:beta": 0, "p:unregistered": 0})
            self.assertEqual(set(res.keys()), {"p:alpha", "p:beta"})
            self.assertEqual([r.seq for r in res["p:alpha"]], [2])
            self.assertEqual([r.seq for r in res["p:beta"]], [1])
            self.assertIsNone(self._meta.get_partition("p:unregistered"))
        finally:
            k.close()

    def test_snapshot_record_upcast_preserves_covers_through_seq(self) -> None:
        """TR-DD3-004: __snapshot__ にも版管理と Upcast が適用され covers_through_seq が保持される。"""
        k_v1 = StorageKernel(
            config=KernelConfig(schema_registry={"tick": 1, SNAPSHOT_RECORD_TYPE: 1}),
            shard_dao=self._shard,
            meta_dao=self._meta,
            codec=self._codec,
            router=self._router,
            store_raw_bytes=True,
        )
        k_v1.append("snap:up", "tick", {"n": 1}, "t1", schema_version=1)
        k_v1.write_snapshot("snap:up", {"total": 1}, "s1", covers_through_seq=1, schema_version=1)
        k_v1.close()

        upcaster = MappingPayloadUpcaster(
            {
                (SNAPSHOT_RECORD_TYPE, 1): lambda raw: json.dumps(
                    {**json.loads(raw.decode("utf-8")), "migrated": True},
                    sort_keys=True,
                ).encode("utf-8"),
            }
        )
        k_v2 = StorageKernel(
            config=KernelConfig(schema_registry={"tick": 1, SNAPSHOT_RECORD_TYPE: 2}),
            shard_dao=self._shard,
            meta_dao=self._meta,
            codec=self._codec,
            router=self._router,
            upcaster=upcaster,
            store_raw_bytes=True,
        )
        try:
            rows = k_v2.sync({"snap:up": 1})["snap:up"]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].record_type, SNAPSHOT_RECORD_TYPE)
            self.assertEqual(rows[0].schema_version, 2)
            self.assertEqual(rows[0].covers_through_seq, 1)
            self.assertEqual(rows[0].payload, {"total": 1, "migrated": True})
        finally:
            k_v2.close()


if __name__ == "__main__":
    unittest.main()
