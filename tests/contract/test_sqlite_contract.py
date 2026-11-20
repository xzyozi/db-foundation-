"""SQLite 参照 DAO 実装（contrib/sqlite/）に対する Contract Test 13 項目の実行スイート。"""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
from typing import Any
import unittest

from storage_kernel.contrib.sqlite import SqliteMetaDAO, SqliteShardDAO
from storage_kernel.ports.dao import ProjectionPort
from storage_kernel.testing import contract_test
from storage_kernel.testing.memory_ports import (
    InMemoryProjection,
    JsonDeterministicCodec,
    MappingPayloadUpcaster,
)


class TestSqliteContract(contract_test.StorageKernelContractTest[dict[str, Any]]):
    """実 SQLite DB 上で Contract Test 13 項目すべて（再接続永続性を含む）を検証する。"""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._base_dir = Path(self._tmpdir.name)
        self._codec = JsonDeterministicCodec()
        self._projection: InMemoryProjection[dict[str, Any]] = InMemoryProjection("sqlite_contract_proj")

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
            shard_dao=SqliteShardDAO(self._base_dir, codec=self._codec),
            meta_dao=SqliteMetaDAO(self._base_dir / "meta.db"),
            projection=self._projection,
            codec=self._codec,
            upcaster=upcaster,
            make_payload=lambda n: {"seq_val": n},
            equivalent_payloads=({"a": 1, "b": 2}, {"b": 2, "a": 1}),
            valid_upcast_input=("tick", 1, b'{"price":100}'),
            unknown_upcast_input=("tick", 999, b'{"price":100}'),
        )


if __name__ == "__main__":
    unittest.main()
