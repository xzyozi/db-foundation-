"""contracts: 境界を越えて共有する不変 DTO・標準例外・テレメトリ型。"""

from __future__ import annotations

from .envelope import (
    SNAPSHOT_RECORD_TYPE,
    BatchAppendItem,
    DeadLetterRecord,
    DropPolicy,
    DurabilityLevel,
    KernelConfig,
    PartitionInfo,
    PartitionStatus,
    ProjectionFailurePolicy,
    RecordEnvelope,
    RetryPolicy,
    ShutdownReport,
    SubscriptionHandle,
    WriteAck,
)
from .errors import (
    CursorExpiredError,
    DuplicateKeyError,
    IdempotencyConflict,
    KernelClosedError,
    KernelMaintenanceError,
    PartitionReadOnlyError,
    PartitionStateConflictError,
    PayloadTooLarge,
    ReservedRecordTypeError,
    SchemaVersionError,
    StorageKernelError,
)
from .telemetry import TelemetryEvent, TelemetrySink

__all__ = [
    "BatchAppendItem",
    "CursorExpiredError",
    "DeadLetterRecord",
    "DropPolicy",
    "DuplicateKeyError",
    "DurabilityLevel",
    "IdempotencyConflict",
    "KernelClosedError",
    "KernelConfig",
    "KernelMaintenanceError",
    "PartitionInfo",
    "PartitionReadOnlyError",
    "PartitionStateConflictError",
    "PartitionStatus",
    "PayloadTooLarge",
    "ProjectionFailurePolicy",
    "RecordEnvelope",
    "ReservedRecordTypeError",
    "RetryPolicy",
    "SNAPSHOT_RECORD_TYPE",
    "SchemaVersionError",
    "ShutdownReport",
    "StorageKernelError",
    "SubscriptionHandle",
    "TelemetryEvent",
    "TelemetrySink",
    "WriteAck",
]
