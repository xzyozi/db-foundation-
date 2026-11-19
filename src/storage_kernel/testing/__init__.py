"""testing: 親プロジェクト向け In-Memory 実装・Contract Test 基底・障害注入ハーネス。"""

from __future__ import annotations

from .contract_test import DAOContractBundle, ShardDAOContractTest, StorageKernelContractTest
from .memory_ports import (
    InMemoryDeadLetter,
    InMemoryMetaDAO,
    InMemoryProjection,
    InMemoryShardDAO,
    InMemoryStorageBackend,
    JsonDeterministicCodec,
    MappingPayloadUpcaster,
    SimplePartitionRouter,
)

__all__ = [
    "DAOContractBundle",
    "InMemoryDeadLetter",
    "InMemoryMetaDAO",
    "InMemoryProjection",
    "InMemoryShardDAO",
    "InMemoryStorageBackend",
    "JsonDeterministicCodec",
    "MappingPayloadUpcaster",
    "ShardDAOContractTest",
    "SimplePartitionRouter",
    "StorageKernelContractTest",
]
