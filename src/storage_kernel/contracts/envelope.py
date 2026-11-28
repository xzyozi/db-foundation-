"""storage_kernel と親プロジェクト間で共有する不変契約データ型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, Literal, Protocol, TypeVar

T = TypeVar("T")

SNAPSHOT_RECORD_TYPE = "__snapshot__"
DurabilityLevel = Literal["none", "process_crash", "power_loss"]
PartitionStatus = Literal["active", "read_only"]
DropPolicy = Literal["drop_newest", "drop_oldest", "disconnect"]
ProjectionFailurePolicy = Literal["dlq_and_skip", "pause_partition"]


@dataclass(frozen=True)
class RetryPolicy:
    """Projection 適用失敗時の指数バックオフ再試行ポリシー。"""

    max_attempts: int = 3
    initial_delay_sec: float = 0.1
    backoff_multiplier: float = 2.0
    max_delay_sec: float = 5.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.initial_delay_sec < 0.0:
            raise ValueError("initial_delay_sec must be >= 0.0")
        if self.backoff_multiplier < 1.0:
            raise ValueError("backoff_multiplier must be >= 1.0")
        if self.max_delay_sec < self.initial_delay_sec:
            raise ValueError("max_delay_sec must be >= initial_delay_sec")

    def delay_for_attempt(self, attempt: int) -> float:
        """1始まりの失敗試行回数に対する待機秒数を算出する。"""
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        delay = self.initial_delay_sec * (self.backoff_multiplier ** (attempt - 1))
        return float(min(self.max_delay_sec, delay))


@dataclass(frozen=True)
class PartitionInfo:
    """パーティション台帳上のメタ情報。"""

    partition_key: str
    shard_id: str
    status: PartitionStatus
    created_at: float


@dataclass(frozen=True)
class RecordEnvelope(Generic[T]):
    """基盤がレコードの中身に依存せず扱うためのメタデータ付き封筒。"""

    partition_key: str
    seq: int
    idempotency_key: str
    record_type: str
    schema_version: int
    payload_hash: str
    payload: T
    created_at: float
    covers_through_seq: int | None = None


@dataclass(frozen=True)
class KernelConfig:
    """親プロジェクトの bootstrap から DI で注入する設定（環境変数は読まない）。"""

    schema_registry: dict[str, int]
    max_payload_bytes: int = 64 * 1024
    default_retry_policy: RetryPolicy = field(default_factory=RetryPolicy)

    def __post_init__(self) -> None:
        if self.max_payload_bytes < 1:
            raise ValueError("max_payload_bytes must be >= 1")


@dataclass(frozen=True)
class BatchAppendItem(Generic[T]):
    """append_batch() に渡す 1 件分の通常レコード書き込み要求。"""

    record_type: str
    payload: T
    idempotency_key: str
    schema_version: int


@dataclass(frozen=True)
class WriteAck(Generic[T]):
    """Write-Before-ACK 完了後に呼び出し元へ返す書き込み確認結果。"""

    status: Literal["committed", "duplicate"]
    shard_id: str
    envelope: RecordEnvelope[T]


@dataclass(frozen=True)
class DeadLetterRecord(Generic[T]):
    """Projection のリトライ上限を超えた Poison Pill レコードの隔離記録。"""

    consumer_id: str
    envelope: RecordEnvelope[T]
    error_type: str
    error_message: str
    attempts: int
    failed_at: float


@dataclass(frozen=True)
class ShutdownReport:
    """StorageKernel.close() 完了時の停止状態レポート。"""

    completed_cleanly: bool
    dropped_subscriber_events: int
    pending_projection_partitions: tuple[str, ...]
    elapsed_sec: float


class SubscriptionHandle(Protocol):
    """subscribe() が返す購読管理ハンドル。基盤が実装し、親は利用のみ行う。"""

    @property
    def subscription_id(self) -> str:
        """購読を一意に識別する ID。"""
        ...

    @property
    def is_active(self) -> bool:
        """購読が有効な間 True、解除または disconnect 後は False。"""
        ...

    @property
    def dropped_count(self) -> int:
        """キュー満杯により破棄された累積イベント件数。"""
        ...

    def unsubscribe(self) -> None:
        """購読を解除し、以降の新規イベント投入を停止する（冪等）。"""
        ...
