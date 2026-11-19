"""新規パーティションの格納先シャード決定ポリシー契約。"""

from __future__ import annotations

from typing import Protocol


class PartitionRouterPort(Protocol):
    """新規パーティションの格納先シャードを決める純粋なポリシー（移動は扱わない）。"""

    def select_shard(self, partition_key: str, *, preferred_shard: str | None = None) -> str:
        """新規パーティションに割り当てる shard_id を返す。"""
        ...
