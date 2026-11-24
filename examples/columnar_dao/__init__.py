"""業務カラム展開 DAO の例（Issue #53 の検証用。storage_kernel 本体には含まれない）。"""

from __future__ import annotations

from .shard_dao import ColumnarShardDAO

__all__ = ["ColumnarShardDAO"]
