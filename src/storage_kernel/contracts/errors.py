"""storage_kernel 基盤が送出・規定する標準例外体系。"""

from __future__ import annotations

from typing import Literal


class StorageKernelError(Exception):
    """storage_kernel 基盤の基底例外。"""


class KernelClosedError(StorageKernelError):
    """close() 開始後に書き込みや状態変更などが呼ばれた。"""


class KernelMaintenanceError(StorageKernelError):
    """メンテナンスモード中に書き込み・状態変更・カーソル更新などが呼ばれた。"""


class PayloadTooLarge(StorageKernelError):
    """encode 後のペイロードが上限（既定 64 KB）を超えた。"""

    def __init__(self, size: int, limit: int) -> None:
        super().__init__(f"payload size {size} exceeds limit {limit}")
        self.size = size
        self.limit = limit


class ReservedRecordTypeError(StorageKernelError):
    """通常 append に '__' で始まる予約 record_type を渡した。"""


class DuplicateKeyError(StorageKernelError):
    """DAO が DB 固有の一意制約違反を変換して送出する標準例外。"""

    def __init__(
        self,
        message: str,
        *,
        kind: Literal["sequence", "idempotency_key", "unknown"] = "unknown",
    ) -> None:
        super().__init__(message)
        self.kind = kind


class IdempotencyConflict(StorageKernelError):
    """同じ (partition_key, idempotency_key) で payload_hash が一致しない。"""

    def __init__(
        self,
        partition_key: str,
        idempotency_key: str,
        existing_hash: str,
        incoming_hash: str,
    ) -> None:
        super().__init__(
            f"Idempotency conflict on ({partition_key}, {idempotency_key}): "
            f"existing={existing_hash} incoming={incoming_hash}"
        )
        self.partition_key = partition_key
        self.idempotency_key = idempotency_key
        self.existing_hash = existing_hash
        self.incoming_hash = incoming_hash


class PartitionReadOnlyError(StorageKernelError):
    """status='read_only' のパーティションへ書き込んだ。"""


class PartitionStateConflictError(StorageKernelError):
    """set_partition_status の expected_status が現在の状態と一致しない。"""


class CursorExpiredError(StorageKernelError):
    """保持範囲より古いカーソルで sync / fetch_range を実行した（v1 は契約のみ定義）。"""

    def __init__(self, partition_key: str, cursor_seq: int, oldest_retained_seq: int) -> None:
        super().__init__(f"cursor {cursor_seq} on {partition_key} is older than retained seq {oldest_retained_seq}")
        self.partition_key = partition_key
        self.cursor_seq = cursor_seq
        self.oldest_retained_seq = oldest_retained_seq


class SchemaVersionError(StorageKernelError):
    """未知の schema_version、最新版との不一致、または Upcast 変換経路が欠けている。"""
