"""親プロジェクト接続例（examples/parent_app）の結合動作確認テスト。"""

from __future__ import annotations

import tempfile
import time
from typing import Any
import unittest

from examples.parent_app.bootstrap import build_parent_app


def _wait_until(predicate: Any, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


class TestParentAppExample(unittest.TestCase):
    """examples/parent_app の DI 組み立て・書き込み・FTS5 投影・同期・メンテナンス・停止を検証する。"""

    def test_end_to_end_parent_app_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = build_parent_app(tmpdir)
            try:
                ack1 = runtime.kernel.append(
                    "chat:general",
                    "chat_message",
                    {"user": "alice", "text": "hello storage kernel"},
                    "msg-1",
                    schema_version=1,
                )
                ack2 = runtime.kernel.append(
                    "chat:general",
                    "chat_message",
                    {"user": "bob", "text": "ports and adapters"},
                    "msg-2",
                    schema_version=1,
                )
                self.assertEqual(ack1.envelope.seq, 1)
                self.assertEqual(ack2.envelope.seq, 2)

                self.assertTrue(
                    _wait_until(lambda: len(runtime.fts_projection.search("kernel")) == 1)
                )
                hits = runtime.fts_projection.search("adapters")
                self.assertEqual(len(hits), 1)
                self.assertEqual(hits[0]["seq"], 2)

                synced = runtime.kernel.sync({"chat:general": 1})
                self.assertEqual([r.seq for r in synced["chat:general"]], [2])

                runtime.kernel.enter_maintenance("snapshot_backup", drain_projections=True)
                runtime.kernel.exit_maintenance()
                self.assertEqual(runtime.maintenance_hooks.events, ["enter:snapshot_backup", "exit"])
            finally:
                report = runtime.kernel.close(drain_projections=True)
                self.assertTrue(report.completed_cleanly)


if __name__ == "__main__":
    unittest.main()
