"""Projection のリトライ・Poison Pill 単件特定・DLQ 隔離制御。"""

from __future__ import annotations

from collections.abc import Sequence
import logging
import threading
import time
from typing import Generic, Literal, TypeVar

from ..contracts.envelope import (
    SNAPSHOT_RECORD_TYPE,
    DeadLetterRecord,
    ProjectionFailurePolicy,
    RecordEnvelope,
    RetryPolicy,
)
from ..contracts.telemetry import TelemetryEvent, TelemetrySink
from ..ports.dao import DeadLetterPort, MetaDAOPort, ProjectionPort

T = TypeVar("T")

logger = logging.getLogger("storage_kernel.reliability")


class DeadLetterCoordinator(Generic[T]):
    """RetryPolicy に基づく指数バックオフ再試行と Poison Pill 単件特定・隔離を担う。"""

    def __init__(self, *, telemetry_sink: TelemetrySink | None = None) -> None:
        self._telemetry_sink = telemetry_sink

    def _emit(self, name: str, attributes: dict[str, str | int | float | bool]) -> None:
        if self._telemetry_sink is not None:
            self._telemetry_sink.emit(TelemetryEvent(name=name, attributes=attributes))

    def _apply_with_retries(
        self,
        *,
        projection: ProjectionPort[T],
        records: Sequence[RecordEnvelope[T]],
        retry_policy: RetryPolicy,
        stop_event: threading.Event,
    ) -> tuple[bool, Exception | None, int]:
        last_exc: Exception | None = None
        for attempt in range(1, retry_policy.max_attempts + 1):
            if stop_event.is_set():
                return False, last_exc, attempt - 1
            try:
                projection.apply_batch(records)
                return True, None, attempt
            except Exception as exc:
                last_exc = exc
                if attempt < retry_policy.max_attempts:
                    delay = retry_policy.delay_for_attempt(attempt)
                    if delay > 0.0 and stop_event.wait(delay):
                        return False, last_exc, attempt
        return False, last_exc, retry_policy.max_attempts

    def apply_with_retry_and_isolate(
        self,
        *,
        partition_key: str,
        records: Sequence[RecordEnvelope[T]],
        projection: ProjectionPort[T],
        meta_dao: MetaDAOPort,
        dead_letter_port: DeadLetterPort[T] | None,
        retry_policy: RetryPolicy,
        failure_policy: ProjectionFailurePolicy,
        include_snapshots: bool,
        stop_event: threading.Event,
    ) -> Literal["applied", "paused", "stopped"]:
        """バッチ適用を実行し、リトライ上限超過時は Poison Pill を 1 件ずつ特定して隔離/停止する。"""
        if not records:
            return "applied"

        filtered = [rec for rec in records if include_snapshots or rec.record_type != SNAPSHOT_RECORD_TYPE]
        if not filtered:
            meta_dao.advance_cursor(projection.consumer_id, partition_key, records[-1].seq)
            return "applied"

        ok, _, _ = self._apply_with_retries(
            projection=projection,
            records=filtered,
            retry_policy=retry_policy,
            stop_event=stop_event,
        )
        if stop_event.is_set() and not ok:
            return "stopped"
        if ok:
            meta_dao.advance_cursor(projection.consumer_id, partition_key, records[-1].seq)
            return "applied"

        # バッチ単位で上限超過 -> 1件ずつ再実行して Poison Pill を特定する
        for rec in records:
            if stop_event.is_set():
                return "stopped"
            if not include_snapshots and rec.record_type == SNAPSHOT_RECORD_TYPE:
                meta_dao.advance_cursor(projection.consumer_id, partition_key, rec.seq)
                continue

            single_ok, single_exc, attempts = self._apply_with_retries(
                projection=projection,
                records=(rec,),
                retry_policy=retry_policy,
                stop_event=stop_event,
            )
            if stop_event.is_set() and not single_ok:
                return "stopped"
            if single_ok:
                meta_dao.advance_cursor(projection.consumer_id, partition_key, rec.seq)
                continue

            assert single_exc is not None
            if failure_policy == "dlq_and_skip" and dead_letter_port is not None:
                dlq_record = DeadLetterRecord(
                    consumer_id=projection.consumer_id,
                    envelope=rec,
                    error_type=type(single_exc).__name__,
                    error_message=str(single_exc),
                    attempts=attempts,
                    failed_at=time.time(),
                )
                try:
                    dead_letter_port.put_dead_letter(dlq_record)
                except Exception:
                    logger.exception(
                        "Failed to store dead letter for consumer=%s partition=%s seq=%d; pausing partition",
                        projection.consumer_id,
                        partition_key,
                        rec.seq,
                    )
                    return "paused"

                meta_dao.advance_cursor(projection.consumer_id, partition_key, rec.seq)
                logger.warning(
                    "Isolated poison pill to DLQ: consumer=%s partition=%s seq=%d error=%s",
                    projection.consumer_id,
                    partition_key,
                    rec.seq,
                    type(single_exc).__name__,
                )
                self._emit(
                    "projection.dlq_isolated",
                    {
                        "consumer_id": projection.consumer_id,
                        "partition_key": partition_key,
                        "seq": rec.seq,
                        "error_type": type(single_exc).__name__,
                    },
                )
                continue

            logger.error(
                "Pausing partition on poison pill: consumer=%s partition=%s seq=%d error=%s",
                projection.consumer_id,
                partition_key,
                rec.seq,
                type(single_exc).__name__,
            )
            self._emit(
                "projection.partition_paused",
                {
                    "consumer_id": projection.consumer_id,
                    "partition_key": partition_key,
                    "seq": rec.seq,
                    "error_type": type(single_exc).__name__,
                },
            )
            return "paused"

        return "applied"
