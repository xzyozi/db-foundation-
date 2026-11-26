"""PostgreSQL 試作 DAO（examples/postgres_dao）に対する Contract Test 13 項目と結合テスト（Issue #59）。

`POSTGRES_TEST_DSN` 環境変数が未設定、または `psycopg` が未導入の環境では自動スキップする。
"""

from __future__ import annotations

import importlib.util
import json
import os
from typing import Any
import unittest
import uuid

from storage_kernel import KernelConfig, RecordEnvelope, StorageKernel
from storage_kernel.ports.dao import ProjectionPort
from storage_kernel.testing import contract_test
from storage_kernel.testing.memory_ports import (
    InMemoryProjection,
    JsonDeterministicCodec,
    MappingPayloadUpcaster,
    SimplePartitionRouter,
)

_HAS_PSYCOPG = importlib.util.find_spec("psycopg") is not None
_POSTGRES_TEST_DSN = os.environ.get("POSTGRES_TEST_DSN")
_SKIP_REASON = "POSTGRES_TEST_DSN 未設定または psycopg 未導入のため PostgreSQL テストをスキップします"


def _create_isolated_schema(dsn: str) -> str:
    import psycopg
    from psycopg import sql

    schema = f"dbf_test_{uuid.uuid4().hex[:16]}"
    with psycopg.connect(dsn) as conn, conn.transaction():
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    return schema


def _drop_isolated_schema(dsn: str, schema: str) -> None:
    import psycopg
    from psycopg import sql

    with psycopg.connect(dsn) as conn, conn.transaction():
        conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


@unittest.skipUnless(_HAS_PSYCOPG and bool(_POSTGRES_TEST_DSN), _SKIP_REASON)
class TestPostgresContract(contract_test.StorageKernelContractTest[dict[str, Any]]):
    """実 PostgreSQL DB 上で Contract Test 13 項目（再接続永続性を含む）を検証する。"""

    def setUp(self) -> None:
        assert _POSTGRES_TEST_DSN is not None
        self._dsn = _POSTGRES_TEST_DSN
        self._schema = _create_isolated_schema(self._dsn)
        self._codec = JsonDeterministicCodec()
        self._projection: InMemoryProjection[dict[str, Any]] = InMemoryProjection("postgres_contract_proj")

    def tearDown(self) -> None:
        _drop_isolated_schema(self._dsn, self._schema)

    def projected_seqs(self, projection: ProjectionPort[dict[str, Any]], partition_key: str) -> list[int]:
        assert isinstance(projection, InMemoryProjection)
        return [rec.seq for rec in projection.records_for(partition_key)]

    def make_dao(self) -> contract_test.DAOContractBundle[dict[str, Any]]:
        from examples.postgres_dao import PostgresMetaDAO, PostgresShardDAO

        upcaster = MappingPayloadUpcaster(
            {
                ("tick", 1): lambda raw: json.dumps(
                    {**json.loads(raw.decode("utf-8")), "v": 2},
                    sort_keys=True,
                ).encode("utf-8"),
            }
        )
        return contract_test.DAOContractBundle(
            shard_dao=PostgresShardDAO(self._dsn, codec=self._codec, schema=self._schema),
            meta_dao=PostgresMetaDAO(self._dsn, schema=self._schema),
            projection=self._projection,
            codec=self._codec,
            upcaster=upcaster,
            make_payload=lambda n: {"seq_val": n},
            equivalent_payloads=({"a": 1, "b": 2}, {"b": 2, "a": 1}),
            valid_upcast_input=("tick", 1, b'{"price":100}'),
            unknown_upcast_input=("tick", 999, b'{"price":100}'),
        )


@unittest.skipUnless(_HAS_PSYCOPG and bool(_POSTGRES_TEST_DSN), _SKIP_REASON)
class TestPostgresKernelIntegration(unittest.TestCase):
    """StorageKernel と PostgreSQL 試作 DAO の結合（append → 冪等再送 → sync → close）を検証する。"""

    def setUp(self) -> None:
        assert _POSTGRES_TEST_DSN is not None
        self._dsn = _POSTGRES_TEST_DSN
        self._schema = _create_isolated_schema(self._dsn)

    def tearDown(self) -> None:
        _drop_isolated_schema(self._dsn, self._schema)

    def test_append_idempotent_sync_and_close_with_codec_in_dao(self) -> None:
        """store_raw_bytes=False で PostgresShardDAO + PostgresMetaDAO を使い往復と冪等再送が成立する。"""
        from examples.postgres_dao import PostgresMetaDAO, PostgresShardDAO

        codec = JsonDeterministicCodec()
        shard_dao: PostgresShardDAO[dict[str, Any]] = PostgresShardDAO(self._dsn, codec=codec, schema=self._schema)
        meta_dao = PostgresMetaDAO(self._dsn, schema=self._schema)
        kernel: StorageKernel[dict[str, Any]] = StorageKernel(
            config=KernelConfig(schema_registry={"order": 1}),
            shard_dao=shard_dao,
            meta_dao=meta_dao,
            codec=codec,
            router=SimplePartitionRouter(default_shards=2),
            store_raw_bytes=False,
        )
        try:
            p1 = {"order_id": "ord-1", "amount": 1200}
            p2 = {"order_id": "ord-2", "amount": 3400}

            ack1 = kernel.append("orders:tokyo", "order", p1, "idem-1", schema_version=1)
            self.assertEqual(ack1.status, "committed")
            self.assertEqual(ack1.envelope.seq, 1)

            # 冪等再送は seq を増やさず duplicate を返す
            dup1 = kernel.append("orders:tokyo", "order", dict(p1), "idem-1", schema_version=1)
            self.assertEqual(dup1.status, "duplicate")
            self.assertEqual(dup1.envelope.seq, 1)

            ack2 = kernel.append("orders:tokyo", "order", p2, "idem-2", schema_version=1)
            self.assertEqual(ack2.status, "committed")
            self.assertEqual(ack2.envelope.seq, 2)

            synced = kernel.sync({"orders:tokyo": 0})
            self.assertEqual([r.seq for r in synced["orders:tokyo"]], [1, 2])
            self.assertEqual([r.payload for r in synced["orders:tokyo"]], [p1, p2])
            self.assertTrue(all(isinstance(r, RecordEnvelope) for r in synced["orders:tokyo"]))
        finally:
            report = kernel.close(drain_projections=True)
            self.assertTrue(report.completed_cleanly)

    def test_append_idempotent_sync_and_close_with_raw_bytes(self) -> None:
        """store_raw_bytes=True でも PostgresShardDAO(codec=None) で同様に往復できる。"""
        from examples.postgres_dao import PostgresMetaDAO, PostgresShardDAO

        codec = JsonDeterministicCodec()
        shard_dao: PostgresShardDAO[bytes] = PostgresShardDAO(self._dsn, codec=None, schema=self._schema)
        meta_dao = PostgresMetaDAO(self._dsn, schema=self._schema)
        kernel: StorageKernel[dict[str, Any]] = StorageKernel(
            config=KernelConfig(schema_registry={"order": 1}),
            shard_dao=shard_dao,
            meta_dao=meta_dao,
            codec=codec,
            router=SimplePartitionRouter(default_shards=2),
            store_raw_bytes=True,
        )
        try:
            payload = {"order_id": "ord-raw", "qty": 5}
            ack = kernel.append("orders:osaka", "order", payload, "idem-raw-1", schema_version=1)
            self.assertEqual((ack.status, ack.envelope.seq), ("committed", 1))

            dup = kernel.append("orders:osaka", "order", dict(payload), "idem-raw-1", schema_version=1)
            self.assertEqual((dup.status, dup.envelope.seq), ("duplicate", 1))

            synced = kernel.sync({"orders:osaka": 0})
            self.assertEqual([r.payload for r in synced["orders:osaka"]], [payload])
        finally:
            report = kernel.close(drain_projections=True)
            self.assertTrue(report.completed_cleanly)
