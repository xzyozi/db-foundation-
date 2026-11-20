"""SQLite のトランザクション補助が、ROLLBACK の失敗で元の例外を失わないことの検証。"""

from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
from typing import Any, cast
import unittest

from examples.parent_app.adapters import SqliteFts5Projection
from storage_kernel import RecordEnvelope
from storage_kernel.contracts.errors import PartitionStateConflictError
from storage_kernel.contrib.sqlite import SqliteMetaDAO
from storage_kernel.contrib.sqlite._transaction import immediate_transaction


class _FakeConn:
    """execute に渡された SQL を記録し、指定した文だけ失敗させる接続の代役。"""

    def __init__(self, failures: dict[str, Exception] | None = None) -> None:
        self.statements: list[str] = []
        self._failures = failures or {}

    def execute(self, sql: str, *args: Any) -> None:
        self.statements.append(sql)
        failure = self._failures.get(sql)
        if failure is not None:
            raise failure


def _conn(fake: _FakeConn) -> sqlite3.Connection:
    return cast(sqlite3.Connection, fake)


class TestImmediateTransaction(unittest.TestCase):
    def test_commits_when_block_succeeds(self) -> None:
        fake = _FakeConn()
        with immediate_transaction(_conn(fake)):
            pass
        self.assertEqual(fake.statements, ["BEGIN IMMEDIATE;", "COMMIT;"])

    def test_rolls_back_and_reraises_when_block_fails(self) -> None:
        fake = _FakeConn()
        with self.assertRaises(ValueError), immediate_transaction(_conn(fake)):
            raise ValueError("body failed")
        self.assertEqual(fake.statements, ["BEGIN IMMEDIATE;", "ROLLBACK;"])

    def test_keeps_original_exception_when_rollback_also_fails(self) -> None:
        fake = _FakeConn({"ROLLBACK;": sqlite3.OperationalError("rollback failed")})
        with self.assertLogs("storage_kernel.contrib.sqlite._transaction", level="WARNING"):
            with self.assertRaises(ValueError) as ctx, immediate_transaction(_conn(fake)):
                raise ValueError("body failed")
        self.assertEqual(str(ctx.exception), "body failed")

    def test_keeps_commit_error_when_commit_and_rollback_both_fail(self) -> None:
        fake = _FakeConn(
            {
                "COMMIT;": sqlite3.OperationalError("commit failed"),
                "ROLLBACK;": sqlite3.OperationalError("cannot rollback - no transaction is active"),
            }
        )
        with self.assertLogs("storage_kernel.contrib.sqlite._transaction", level="WARNING"):
            with self.assertRaises(sqlite3.OperationalError) as ctx, immediate_transaction(_conn(fake)):
                pass
        self.assertEqual(str(ctx.exception), "commit failed")


class TestSqliteMetaDaoRollback(unittest.TestCase):
    """実 DB で、失敗したトランザクションが巻き戻され、DAO が引き続き使えること。"""

    def test_failed_transaction_is_rolled_back_and_dao_stays_usable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            dao = SqliteMetaDAO(Path(tmpdir) / "meta.db")
            dao.bind_shard("chat:a", "shard_0")

            with self.assertRaises(KeyError):
                dao.set_partition_status("chat:unknown", "read_only")
            with self.assertRaises(PartitionStateConflictError):
                dao.set_partition_status("chat:a", "read_only", expected_status="read_only")

            info = dao.get_partition("chat:a")
            assert info is not None
            self.assertEqual(info.status, "active")
            self.assertEqual(dao.set_partition_status("chat:a", "read_only").status, "read_only")


class _RollbackFailingConn:
    """実接続に処理を委譲するが、ROLLBACK だけ失敗させる。"""

    def __init__(self, real: sqlite3.Connection) -> None:
        self._real = real

    def execute(self, sql: str, *args: Any) -> sqlite3.Cursor:
        if sql.startswith("ROLLBACK"):
            raise sqlite3.OperationalError("cannot rollback - no transaction is active")
        return self._real.execute(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


class _FtsWithFailingRollback(SqliteFts5Projection):
    def _connect(self) -> sqlite3.Connection:
        return cast(sqlite3.Connection, _RollbackFailingConn(super()._connect()))


class TestParentAppAdapterRollback(unittest.TestCase):
    def test_apply_batch_keeps_original_exception_when_rollback_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            projection = _FtsWithFailingRollback(Path(tmpdir) / "fts.db")
            broken: RecordEnvelope[dict[str, Any]] = RecordEnvelope(
                partition_key="chat:a",
                seq=1,
                idempotency_key="k1",
                record_type="chat_message",
                schema_version=1,
                payload_hash="h",
                payload=cast(Any, None),  # payload.values() が AttributeError になる
                created_at=0.0,
            )
            with self.assertLogs("examples.parent_app.adapters", level="WARNING"):
                with self.assertRaises(AttributeError):
                    projection.apply_batch([broken])


if __name__ == "__main__":
    unittest.main()
