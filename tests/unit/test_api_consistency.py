"""公開 API の整合（DBF-DD-004 TR-DD4-005, TR-DD4-006 / DBF-IF-001 IF-003〜IF-005）のユニットテスト。"""

from __future__ import annotations

import threading
import time
from typing import Any
import unittest

from storage_kernel import KernelClosedError, KernelConfig, KernelMaintenanceError, StorageKernel
from storage_kernel.testing.memory_ports import (
    InMemoryMetaDAO,
    InMemoryProjection,
    InMemoryShardDAO,
    InMemoryStorageBackend,
    JsonDeterministicCodec,
    SimplePartitionRouter,
)


def _wait_until(predicate: Any, timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


class _BlockingAdvanceMetaDAO(InMemoryMetaDAO):
    """block=True の間、advance_cursor を release が立つまで止める MetaDAO。"""

    def __init__(self, store: Any) -> None:
        super().__init__(store)
        self.block = False
        self.advance_entered = threading.Event()
        self.release = threading.Event()

    def advance_cursor(self, consumer_id: str, partition_key: str, seq: int) -> None:
        if self.block:
            self.advance_entered.set()
            self.release.wait(timeout=3.0)
        super().advance_cursor(consumer_id, partition_key, seq)


class _RecordingHooks:
    def __init__(self) -> None:
        self.events: list[str] = []

    def on_enter_maintenance(self, reason: str) -> None:
        self.events.append(f"enter:{reason}")

    def on_exit_maintenance(self) -> None:
        self.events.append("exit")


class _ApiTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.store: InMemoryStorageBackend[dict[str, Any]] = InMemoryStorageBackend()
        self.meta = _BlockingAdvanceMetaDAO(self.store)
        self.hooks = _RecordingHooks()
        self.kernel: StorageKernel[dict[str, Any]] = StorageKernel(
            config=KernelConfig(schema_registry={"tick": 1}),
            shard_dao=InMemoryShardDAO(self.store),
            meta_dao=self.meta,
            codec=JsonDeterministicCodec(),
            router=SimplePartitionRouter(),
            maintenance_hooks=self.hooks,
        )
        self.addCleanup(self.kernel.close)
        # "other:*" にしか一致しない Projection を置き、"r:1" のカーソルは resume_projection だけが動かす
        self.kernel.register_projection(
            InMemoryProjection[dict[str, Any]]("c1"),
            partition_patterns=("other:*",),
        )
        self.kernel.append("r:1", "tick", {"n": 1}, "idem-1", schema_version=1)


class TestResumeProjectionConsistency(_ApiTestBase):
    """TR-DD4-005: resume_projection のメンテナンスとの競合・close 後の拒否。"""

    def test_skip_current_does_not_advance_cursor_after_maintenance_point(self) -> None:
        self.meta.block = True
        errors: list[BaseException] = []

        def _resume() -> None:
            try:
                self.kernel.resume_projection("c1", "r:1", skip_current=True)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        t_resume = threading.Thread(target=_resume, daemon=True)
        t_resume.start()
        self.assertTrue(self.meta.advance_entered.wait(timeout=1.0))

        # カーソル更新の途中でメンテナンスが始まっても、更新の完了までは整合点を確立しない
        t_maint = threading.Thread(
            target=lambda: self.kernel.enter_maintenance("backup", timeout=3.0),
            daemon=True,
        )
        t_maint.start()
        time.sleep(0.15)
        self.assertEqual(self.hooks.events, [])
        self.assertEqual(self.meta.get_cursor("c1", "r:1"), 0)

        self.meta.release.set()
        t_resume.join(timeout=2.0)
        t_maint.join(timeout=2.0)
        self.assertEqual(errors, [])

        # 整合点の確立後にカーソルが進んでいないこと（フック呼び出し時点で 1 に確定済み）
        self.assertEqual(self.hooks.events, ["enter:backup"])
        self.assertEqual(self.meta.get_cursor("c1", "r:1"), 1)

        # 確立後の再試行は拒否され、カーソルは動かない
        with self.assertRaises(KernelMaintenanceError):
            self.kernel.resume_projection("c1", "r:1", skip_current=True)
        self.assertEqual(self.meta.get_cursor("c1", "r:1"), 1)

    def test_resume_projection_after_close_raises_for_both_skip_modes(self) -> None:
        self.kernel.close()

        with self.assertRaises(KernelClosedError):
            self.kernel.resume_projection("c1", "r:1", skip_current=False)
        with self.assertRaises(KernelClosedError):
            self.kernel.resume_projection("c1", "r:1", skip_current=True)

    def test_resume_projection_without_skip_is_allowed_during_maintenance(self) -> None:
        self.kernel.enter_maintenance("backup", timeout=1.0)

        self.kernel.resume_projection("c1", "r:1", skip_current=False)
        self.assertEqual(self.meta.get_cursor("c1", "r:1"), 0)


if __name__ == "__main__":
    unittest.main()
