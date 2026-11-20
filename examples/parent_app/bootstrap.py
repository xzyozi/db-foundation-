"""親プロジェクトの Composition Root（DI 組み立て例）。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from storage_kernel import KernelConfig, StorageKernel
from storage_kernel.contrib.sqlite import SqliteDeadLetterDAO, SqliteMetaDAO, SqliteShardDAO

from .adapters import ParentHashRouter, ParentJsonCodec, ParentMaintenanceHooks, SqliteFts5Projection


@dataclass(frozen=True)
class ParentAppRuntime:
    """親アプリケーションが保持するカーネルと投影アダプタの束。"""

    kernel: StorageKernel[dict[str, Any]]
    fts_projection: SqliteFts5Projection
    maintenance_hooks: ParentMaintenanceHooks


def build_parent_app(base_dir: str | Path) -> ParentAppRuntime:
    """StorageKernel に DAO / Codec / Projection / Hooks を DI 注入して起動する。"""
    root = Path(base_dir)
    codec = ParentJsonCodec()
    meta_dao = SqliteMetaDAO(root / "meta.db")
    shard_dao = SqliteShardDAO[bytes](root / "shards", codec=None)
    dlq_dao = SqliteDeadLetterDAO[dict[str, Any]](root / "meta.db", codec=codec)
    hooks = ParentMaintenanceHooks()

    config = KernelConfig(
        schema_registry={"chat_message": 1, "__snapshot__": 1},
    )
    kernel = StorageKernel(
        config=config,
        shard_dao=shard_dao,
        meta_dao=meta_dao,
        codec=codec,
        router=ParentHashRouter(shard_count=4),
        dead_letter_port=dlq_dao,
        maintenance_hooks=hooks,
        store_raw_bytes=True,
    )

    fts_projection = SqliteFts5Projection(root / "projection.db")
    kernel.register_projection(fts_projection, partition_patterns=("chat:*",))
    return ParentAppRuntime(
        kernel=kernel,
        fts_projection=fts_projection,
        maintenance_hooks=hooks,
    )
