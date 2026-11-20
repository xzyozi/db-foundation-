"""パーティション単位の排他ロック・ルーティングキャッシュ・ライフサイクル状態管理。"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import threading
from typing import Literal

from ..contracts.envelope import PartitionInfo
from ..contracts.errors import KernelClosedError, KernelMaintenanceError

LifecyclePhase = Literal[
    "RUNNING",
    "MAINTENANCE_ENTERING",
    "MAINTENANCE",
    "CLOSING",
    "CLOSED",
]


class ShardLockManager:
    """同一パーティションに対する書き込みと状態遷移を直列化し、実行ライフサイクルを制御する。"""

    def __init__(self) -> None:
        self._state_cond = threading.Condition()
        self._lifecycle_phase: LifecyclePhase = "RUNNING"
        self._active_mutators = 0
        self._partition_locks: dict[str, threading.RLock] = {}
        self._routing_cache: dict[str, PartitionInfo] = {}

    @property
    def lifecycle_phase(self) -> LifecyclePhase:
        with self._state_cond:
            return self._lifecycle_phase

    @property
    def active_mutators(self) -> int:
        with self._state_cond:
            return self._active_mutators

    def _get_partition_lock(self, partition_key: str) -> threading.RLock:
        with self._state_cond:
            lock = self._partition_locks.get(partition_key)
            if lock is None:
                lock = threading.RLock()
                self._partition_locks[partition_key] = lock
            return lock

    def check_mutable_state(self) -> None:
        """現在のライフサイクルが更新系操作を受け付けられる状態か検査する。"""
        with self._state_cond:
            if self._lifecycle_phase in ("CLOSING", "CLOSED"):
                raise KernelClosedError("storage_kernel is closed")
            if self._lifecycle_phase in ("MAINTENANCE_ENTERING", "MAINTENANCE"):
                raise KernelMaintenanceError("storage_kernel is in maintenance mode")

    @contextmanager
    def partition_write_guard(self, partition_key: str) -> Iterator[None]:
        """パーティション単位の排他ロックとアクティブ更新カウンタを獲得するコンテキスト。"""
        with self._state_cond:
            if self._lifecycle_phase in ("CLOSING", "CLOSED"):
                raise KernelClosedError("storage_kernel is closed")
            if self._lifecycle_phase in ("MAINTENANCE_ENTERING", "MAINTENANCE"):
                raise KernelMaintenanceError("storage_kernel is in maintenance mode")
            self._active_mutators += 1
            lock = self._partition_locks.get(partition_key)
            if lock is None:
                lock = threading.RLock()
                self._partition_locks[partition_key] = lock

        lock.acquire()
        try:
            yield
        finally:
            lock.release()
            with self._state_cond:
                self._active_mutators -= 1
                if self._active_mutators == 0:
                    self._state_cond.notify_all()

    def get_cached_partition(self, partition_key: str) -> PartitionInfo | None:
        """ルーティングキャッシュからパーティション情報を取得する。"""
        with self._state_cond:
            return self._routing_cache.get(partition_key)

    def update_cached_partition(self, info: PartitionInfo) -> None:
        """ルーティングキャッシュのパーティション情報を更新する。"""
        with self._state_cond:
            self._routing_cache[info.partition_key] = info

    def clear_routing_cache(self) -> None:
        """ルーティングキャッシュをクリアする。"""
        with self._state_cond:
            self._routing_cache.clear()

    def begin_maintenance_entering(self) -> None:
        """新規更新を遮断するため MAINTENANCE_ENTERING へ遷移させる。"""
        with self._state_cond:
            if self._lifecycle_phase in ("CLOSING", "CLOSED"):
                raise KernelClosedError("storage_kernel is closed")
            if self._lifecycle_phase in ("MAINTENANCE_ENTERING", "MAINTENANCE"):
                raise KernelMaintenanceError("storage_kernel is already in maintenance mode")
            self._lifecycle_phase = "MAINTENANCE_ENTERING"

    def wait_for_mutators_idle(self, deadline: float) -> bool:
        """active_mutators == 0 になるまで monotonic deadline まで待機する。"""
        import time

        with self._state_cond:
            while self._active_mutators > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    return False
                self._state_cond.wait(timeout=remaining)
            return True

    def commit_maintenance_entered(self) -> None:
        """MAINTENANCE 状態を確定させる。"""
        with self._state_cond:
            if self._lifecycle_phase == "MAINTENANCE_ENTERING":
                self._lifecycle_phase = "MAINTENANCE"

    def rollback_maintenance_to_running(self) -> None:
        """メンテナンス移行失敗または解除時に RUNNING へ戻す。"""
        with self._state_cond:
            if self._lifecycle_phase in ("MAINTENANCE_ENTERING", "MAINTENANCE"):
                self._lifecycle_phase = "RUNNING"
                self._state_cond.notify_all()

    def begin_closing(self) -> LifecyclePhase:
        """CLOSING へ遷移させ、遷移前の状態を返す。"""
        with self._state_cond:
            prev = self._lifecycle_phase
            if prev != "CLOSED":
                self._lifecycle_phase = "CLOSING"
                self._state_cond.notify_all()
            return prev

    def mark_closed(self) -> None:
        """CLOSED 状態へ遷移させる。"""
        with self._state_cond:
            self._lifecycle_phase = "CLOSED"
            self._state_cond.notify_all()
