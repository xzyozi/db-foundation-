"""メンテナンスモードの静止点作成・解除を調停する MaintenanceCoordinator。"""

from __future__ import annotations

import logging
import time

from ..contracts.errors import KernelClosedError
from ..contracts.telemetry import TelemetryEvent, TelemetrySink
from ..ports.maintenance import MaintenanceHooks
from .collaborators import ProjectionControl
from .concurrency import ShardLockManager

logger = logging.getLogger("storage_kernel.maintenance")


class MaintenanceCoordinator:
    """新規更新遮断・進行中トランザクション待機・フック呼び出しを統括する。"""

    def __init__(
        self,
        *,
        lock_manager: ShardLockManager,
        projection_runner: ProjectionControl,
        maintenance_hooks: MaintenanceHooks | None = None,
        telemetry_sink: TelemetrySink | None = None,
    ) -> None:
        self._lock_manager = lock_manager
        self._projection_runner = projection_runner
        self._hooks = maintenance_hooks
        self._telemetry_sink = telemetry_sink

    def _emit(self, name: str, attributes: dict[str, str | int | float | bool]) -> None:
        if self._telemetry_sink is not None:
            self._telemetry_sink.emit(TelemetryEvent(name=name, attributes=attributes))

    def enter(
        self,
        reason: str,
        *,
        drain_projections: bool = False,
        timeout: float = 30.0,
    ) -> None:
        """メンテナンスモードへ移行し、バックアップ取得可能な整合点を確立する（F-402）。"""
        if not reason:
            raise ValueError("maintenance reason must be non-empty")
        if timeout <= 0.0:
            raise ValueError("timeout must be > 0")

        started_at = time.monotonic()
        deadline = started_at + timeout
        self._lock_manager.begin_maintenance_entering()

        try:
            if not self._lock_manager.wait_for_mutators_idle(deadline):
                raise TimeoutError("timed out waiting for in-flight write transactions before maintenance")

            if not self._projection_runner.enter_maintenance(drain=drain_projections, deadline=deadline):
                raise TimeoutError("timed out waiting for projections before maintenance")

            self._lock_manager.commit_maintenance_entered()
            if self._hooks is not None:
                self._hooks.on_enter_maintenance(reason)
        except Exception:
            self._projection_runner.exit_maintenance()
            self._lock_manager.rollback_maintenance_to_running()
            raise

        elapsed = time.monotonic() - started_at
        logger.info("Entered maintenance mode (reason=%s, elapsed=%.3fs)", reason, elapsed)
        self._emit(
            "maintenance.entered",
            {
                "reason": reason,
                "drain_projections": drain_projections,
                "elapsed_sec": elapsed,
            },
        )

    def exit(self) -> None:
        """メンテナンスモードを解除し、通常運転へ戻す（F-403）。"""
        phase = self._lock_manager.lifecycle_phase
        if phase in ("CLOSING", "CLOSED"):
            raise KernelClosedError("storage_kernel is closed")
        if phase != "MAINTENANCE":
            return

        try:
            if self._hooks is not None:
                self._hooks.on_exit_maintenance()
        finally:
            self._lock_manager.rollback_maintenance_to_running()
            self._projection_runner.exit_maintenance()
            logger.info("Exited maintenance mode")
            self._emit("maintenance.exited", {})
