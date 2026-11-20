"""シャード・メタ台帳・Projection・DLQ の DAO Port 契約。"""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Protocol, Sequence, TypeVar

from ..contracts.envelope import (
    DeadLetterRecord,
    DurabilityLevel,
    PartitionInfo,
    PartitionStatus,
    RecordEnvelope,
)

T = TypeVar("T")


class ShardTxPort(Protocol[T]):
    """単一シャード内の排他書き込みトランザクション契約。"""

    def find_by_idempotency(self, partition_key: str, idempotency_key: str) -> RecordEnvelope[T] | None:
        """(partition_key, idempotency_key) に一致する payload_hash 等を含む完全な RecordEnvelope を返す。"""
        ...

    def next_sequence(self, partition_key: str) -> int:
        """パーティション単位の単調増加 seq を採番する（初回は 1、欠番・逆転禁止）。"""
        ...

    def insert_record(self, envelope: RecordEnvelope[T]) -> None:
        """レコードを永続化する。

        (partition_key, seq) / (partition_key, idempotency_key) の一意制約違反は
        DB 固有の例外を漏らさず DuplicateKeyError に変換して送出すること。
        """
        ...


class ShardDAOPort(Protocol[T]):
    """シャードストレージの契約。"""

    @property
    def durability(self) -> DurabilityLevel:
        """自己申告の耐久性レベル（参照実装は 'power_loss'、In-Memory は 'none'）。"""
        ...

    def write_transaction(self, shard_id: str) -> AbstractContextManager[ShardTxPort[T]]:
        """正常終了時に COMMIT、例外時に ROLLBACK するトランザクションを開始する。

        正常に抜けたことは、宣言した durability の水準で永続化が確定したことを意味する。
        """
        ...

    def fetch_range(
        self,
        shard_id: str,
        partition_key: str,
        after_seq: int,
        limit: int,
    ) -> Sequence[RecordEnvelope[T]]:
        """seq > after_seq のレコードを厳密な昇順で欠落なく最大 limit 件返す。"""
        ...


class MetaDAOPort(Protocol):
    """パーティション台帳とカーソル管理の契約。"""

    @property
    def durability(self) -> DurabilityLevel:
        """メタストレージが保証する自己申告の耐久性レベル。"""
        ...

    def get_partition(self, partition_key: str) -> PartitionInfo | None:
        """登録済みパーティション情報を返す。未登録なら None。"""
        ...

    def bind_shard(self, partition_key: str, shard_id: str) -> PartitionInfo:
        """未登録なら status='active' で新規登録する。

        登録済みなら上書きせず既存の PartitionInfo を返す（並行呼び出しに対し冪等かつ不変）。
        """
        ...

    def set_partition_status(
        self,
        partition_key: str,
        status: PartitionStatus,
        *,
        expected_status: PartitionStatus | None = None,
    ) -> PartitionInfo:
        """パーティション状態を遷移させる。

        expected_status 指定時に現在の状態と異なれば PartitionStateConflictError を送出する。
        未登録の partition_key に対しては KeyError を送出する。
        """
        ...

    def list_partitions(self, *, status: PartitionStatus | None = None) -> Sequence[PartitionInfo]:
        """登録済みパーティションの一覧を返す（status 指定時は絞り込み）。"""
        ...

    def get_cursor(self, consumer_id: str, partition_key: str) -> int:
        """(consumer_id, partition_key) の最終処理済み seq を返す。未記録なら 0。"""
        ...

    def advance_cursor(self, consumer_id: str, partition_key: str, seq: int) -> None:
        """カーソルを単調非減少 MAX(current, seq) で更新する。後退させてはならない。"""
        ...


class ProjectionPort(Protocol[T]):
    """取りこぼしてはいけない非同期投影（At-Least-Once）の契約。"""

    @property
    def consumer_id(self) -> str:
        """カーソル管理および DLQ 記録で用いる一意な Consumer 識別子。"""
        ...

    def apply_batch(self, records: Sequence[RecordEnvelope[T]]) -> None:
        """(partition_key, seq) に対して冪等にレコード群を投影する。

        失敗時は例外を送出し、基盤側の Retry / DLQ 制御に委ねる。
        """
        ...


class DeadLetterPort(Protocol[T]):
    """Projection のリトライ上限を超えたレコード（Poison Pill）を隔離する任意の契約。"""

    def put_dead_letter(self, record: DeadLetterRecord[T]) -> None:
        """失敗した単件レコードを永続化隔離する（同一キーの再隔離は冪等）。"""
        ...

    def list_dead_letters(
        self,
        *,
        consumer_id: str | None = None,
        limit: int = 100,
    ) -> Sequence[DeadLetterRecord[T]]:
        """隔離済みの DeadLetterRecord 一覧を返す。"""
        ...
