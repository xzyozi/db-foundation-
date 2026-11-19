"""段階的グレースフルシャットダウン（close）を管理する LifecycleManager。"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from ..contracts.envelope import ShutdownReport
from ..contracts.telemetry import TelemetryEvent, TelemetrySink
from ..core.event_bus import EventDispatcher
from ..core.projection import ProjectionRunner
from .concurrency import ShardLockManager

logger = logging.getLogger("storage_kernel.lifecycle")


class LifecycleManager:
    """書き込み遮断・進行中完了待機・Projection停止・Subscriber停止の順序を制御する。"""

    def __init__(
        self,
        *,
        lock_manager: ShardLockManager,
        projection_runner: ProjectionRunner[Any],
        event_dispatcher: EventDispatcher,
        telemetry_sink: TelemetrySink | None = None,
    ) -> None:
        self._lock_manager = lock_manager
        self._projection_runner = projection_runner
        self._event_dispatcher = event_dispatcher
        self._telemetry_sink = telemetry_sink
        self._close_lock = threading.Lock()
        self._last_report: ShutdownReport | None = None

    def shutdown(
        self,
        *,
        timeout: float = 5.0,
        drain_projections: bool = False,
        drain_subscribers: bool = False,
    ) -> ShutdownReport:
        """カーネルを段階的に停止させ ShutdownReport を返す（冪等、F-404）。"""
        if timeout < 0.0:
            raise ValueError("timeout must be >= 0.0")

        with self._close_lock:
            if self._last_report is not None:
                return self._last_report

            started_at = time.monotonic()
            deadline = started_at + timeout
            self._lock_manager.begin_closing()

            mutators_ok = self._lock_manager.wait_for_mutators_idle(deadline)
            pending_partitions, proj_ok = self._projection_runner.stop(
                drain=drain_projections,
                deadline=deadline,
            )
            dropped_events, sub_ok = self._event_dispatcher.stop(
                drain=drain_subscribers,
                deadline=deadline,
            )

            self._lock_manager.mark_closed()
            elapsed = time.monotonic() - started_at
            completed_cleanly = mutators_ok and proj_ok and sub_ok

            report = ShutdownReport(
                completed_cleanly=completed_cleanly,
                dropped_subscriber_events=dropped_events,
                pending_projection_partitions=pending_partitions,
                elapsed_sec=elapsed,
            )
            self._last_report = report

            if not completed_cleanly:
                logger.warning(
                    "StorageKernel closed with timeout (pending_projections=%s, dropped_events=%d, elapsed=%.3fs)",
                    pending_partitions,
                    dropped_events,
                    elapsed,
                )
            else:
                logger.info("StorageKernel closed cleanly (elapsed=%.3fs)", elapsed)

            if self._telemetry_sink is not None:
                self._telemetry_sink.emit(
                    TelemetryEvent(
                        name="kernel.closed",
                        attributes={
                            "completed_cleanly": completed_cleanly,
                            "dropped_subscriber_events": dropped_events,
                            "pending_projection_count": len(pending_partitions),
                            "elapsed_sec": elapsed,
                        },
                    )
                )
            return report
