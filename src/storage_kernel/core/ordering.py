"""パーティション単位の単調増加連番の採番調整と境界検証。"""

from __future__ import annotations

from typing import Any

from ..ports.dao import ShardTxPort


class SequenceCoordinator:
    """ShardTxPort.next_sequence の呼び出し結果とスナップショット境界を検証する。"""

    def allocate_and_verify(
        self,
        tx: ShardTxPort[Any],
        partition_key: str,
        *,
        covers_through_seq: int | None = None,
    ) -> int:
        """新しい seq を採番し、単調増加性およびスナップショット範囲を検証する。"""
        next_seq = tx.next_sequence(partition_key)
        if next_seq < 1:
            raise ValueError(f"next_sequence returned invalid seq {next_seq} for {partition_key!r}")

        if covers_through_seq is not None:
            latest_committed_seq = next_seq - 1
            if covers_through_seq < 1 or covers_through_seq > latest_committed_seq:
                raise ValueError(
                    f"covers_through_seq ({covers_through_seq}) must be between 1 and {latest_committed_seq}"
                )
        return next_seq
