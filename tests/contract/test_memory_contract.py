"""In-Memory Port 実装に対する Contract Test 13 項目の実行スイート。"""

from __future__ import annotations

import json
from typing import Any
import unittest

from storage_kernel.testing import contract_test
from storage_kernel.testing.memory_ports import (
    InMemoryMetaDAO,
    InMemoryProjection,
    InMemoryShardDAO,
    InMemoryStorageBackend,
    JsonDeterministicCodec,
    MappingPayloadUpcaster,
)


class TestInMemoryContract(contract_test.StorageKernelContractTest[dict[str, Any]]):
    """In-Memory DAO 群が 13 項目の不変条件を満たすことを検証する。"""

    def setUp(self) -> None:
        self._backend: InMemoryStorageBackend[dict[str, Any]] = InMemoryStorageBackend()

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
            shard_dao=InMemoryShardDAO(self._backend),
            meta_dao=InMemoryMetaDAO(self._backend),
            projection=InMemoryProjection(),
            codec=JsonDeterministicCodec(),
            upcaster=upcaster,
            make_payload=lambda n: {"seq_val": n},
            equivalent_payloads=({"a": 1, "b": 2}, {"b": 2, "a": 1}),
            valid_upcast_input=("tick", 1, b'{"price":100}'),
            unknown_upcast_input=("tick", 999, b'{"price":100}'),
        )


if __name__ == "__main__":
    unittest.main()
