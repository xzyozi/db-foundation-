"""書き込み経路（DBF-DD-001 TR-DD1-001〜TR-DD1-008）のユニットテスト。"""

from __future__ import annotations

from contextlib import contextmanager
import time
from typing import Any, Iterator
import unittest

from storage_kernel.contracts.envelope import KernelConfig, RecordEnvelope, SNAPSHOT_RECORD_TYPE
from storage_kernel.contracts.errors import (
    IdempotencyConflict,
    PartitionReadOnlyError,
    PayloadTooLarge,
    ReservedRecordTypeError,
    SchemaVersionError,
)
from storage_kernel.core.kernel import StorageKernel
from storage_kernel.testing.memory_ports import (
    InMemoryMetaDAO,
    InMemoryProjection,
    InMemoryShardDAO,
    InMemoryStorageBackend,
    JsonDeterministicCodec,
    SimplePartitionRouter,
)


def _make_kernel(
    *,
    max_payload_bytes: int = 64 * 1024,
    backend: InMemoryStorageBackend[dict[str, Any]] | None = None,
    shard_dao: Any = None,
    meta_dao: Any = None,
) -> tuple[StorageKernel[dict[str, Any]], InMemoryStorageBackend[dict[str, Any]]]:
    store = backend if backend is not None else InMemoryStorageBackend()
    s_dao = shard_dao if shard_dao is not None else InMemoryShardDAO(store)
    m_dao = meta_dao if meta_dao is not None else InMemoryMetaDAO(store)
    kernel = StorageKernel(
        config=KernelConfig(
            schema_registry={"tick": 1, SNAPSHOT_RECORD_TYPE: 1},
            max_payload_bytes=max_payload_bytes,
        ),
        shard_dao=s_dao,
        meta_dao=m_dao,
        codec=JsonDeterministicCodec(),
        router=SimplePartitionRouter(),
    )
    return kernel, store


class TestWriteIngestion(unittest.TestCase):
    """TR-DD1-001 〜 TR-DD1-008 の検証。"""

    def test_idempotency_conflict_on_hash_mismatch(self) -> None:
        """TR-DD1-001: 同一冪等キーでペイロードが異なる場合は IdempotencyConflict になり既存は不変。"""
        kernel, store = _make_kernel()
        ack = kernel.append("order:1", "tick", {"price": 100}, "idem-1", schema_version=1)
        self.assertEqual(ack.status, "committed")
        self.assertEqual(ack.envelope.seq, 1)

        with self.assertRaises(IdempotencyConflict) as ctx:
            kernel.append("order:1", "tick", {"price": 200}, "idem-1", schema_version=1)

        self.assertEqual(ctx.exception.partition_key, "order:1")
        self.assertEqual(ctx.exception.idempotency_key, "idem-1")
        self.assertEqual(ctx.exception.existing_hash, ack.envelope.payload_hash)
        rows = store.records[(ack.shard_id, "order:1")]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].payload, {"price": 100})

    def test_payload_too_large_rejected_without_advancing_seq(self) -> None:
        """TR-DD1-002: 64 KB 超過のペイロードは PayloadTooLarge になり seq が進まない。"""
        kernel, store = _make_kernel(max_payload_bytes=64)
        kernel.append("order:2", "tick", {"k": "ok"}, "idem-ok", schema_version=1)

        with self.assertRaises(PayloadTooLarge) as ctx:
            kernel.append("order:2", "tick", {"k": "x" * 100}, "idem-big", schema_version=1)

        self.assertGreater(ctx.exception.size, 64)
        self.assertEqual(ctx.exception.limit, 64)

        # write_snapshot にも同じ上限が適用され、拒否されても seq は進まない
        with self.assertRaises(PayloadTooLarge) as snap_ctx:
            kernel.write_snapshot(
                "order:2", {"k": "x" * 100}, "idem-snap-big", covers_through_seq=1, schema_version=1
            )
        self.assertGreater(snap_ctx.exception.size, 64)
        self.assertEqual(snap_ctx.exception.limit, 64)

        ack2 = kernel.append("order:2", "tick", {"k": "ok2"}, "idem-ok2", schema_version=1)
        self.assertEqual(ack2.envelope.seq, 2)
        self.assertEqual(len(store.records[(ack2.shard_id, "order:2")]), 2)

    def test_reserved_record_type_rejected_in_append(self) -> None:
        """TR-DD1-003: '__' 始まりの record_type は通常 append で ReservedRecordTypeError になる。"""
        kernel, _ = _make_kernel()
        for bad_type in (SNAPSHOT_RECORD_TYPE, "__internal__"):
            with self.assertRaises(ReservedRecordTypeError):
                kernel.append("order:3", bad_type, {"a": 1}, f"idem-{bad_type}", schema_version=1)

    def test_write_with_non_latest_schema_version_rejected(self) -> None:
        """TR-DD1-004: schema_registry の最新版と一致しない schema_version や未登録種別は拒否される。"""
        kernel, _ = _make_kernel()
        with self.assertRaises(SchemaVersionError):
            kernel.append("order:4", "tick", {"a": 1}, "idem-v2", schema_version=2)
        with self.assertRaises(SchemaVersionError):
            kernel.append("order:4", "unknown_type", {"a": 1}, "idem-unk", schema_version=1)

    def test_read_only_partition_rejects_writes_allows_reads(self) -> None:
        """TR-DD1-005: read_only パーティションへの append/write_snapshot は PartitionReadOnlyError になる。"""
        kernel, store = _make_kernel()
        ack = kernel.append("order:5", "tick", {"a": 1}, "idem-1", schema_version=1)
        info = kernel.set_partition_status("order:5", "read_only", expected_status="active")
        self.assertEqual(info.status, "read_only")

        with self.assertRaises(PartitionReadOnlyError):
            kernel.append("order:5", "tick", {"a": 2}, "idem-2", schema_version=1)
        with self.assertRaises(PartitionReadOnlyError):
            kernel.write_snapshot("order:5", {"sum": 1}, "idem-snap", covers_through_seq=1, schema_version=1)

        shard_dao = InMemoryShardDAO(store)
        rows = list(shard_dao.fetch_range(ack.shard_id, "order:5", after_seq=0, limit=10))
        self.assertEqual(len(rows), 1)

        # 読み取り（sync）は read_only でも動く
        synced = kernel.sync({"order:5": 0})
        self.assertEqual([r.seq for r in synced["order:5"]], [1])

        # Projection の追いつきも read_only のパーティションに対して動く
        projection = InMemoryProjection[dict[str, Any]]("proj_read_only")
        kernel.register_projection(projection)
        self.addCleanup(kernel.close)
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline and len(projection.records_for("order:5")) < 1:
            time.sleep(0.01)
        self.assertEqual([r.seq for r in projection.records_for("order:5")], [1])
        self.assertEqual(kernel._meta_dao.get_cursor("proj_read_only", "order:5"), 1)

    def test_write_snapshot_covers_through_seq_validation(self) -> None:
        """TR-DD1-006: covers_through_seq が最終 seq を超えると拒否され、正常範囲なら保存される。"""
        kernel, _ = _make_kernel()
        kernel.append("order:6", "tick", {"n": 1}, "idem-1", schema_version=1)
        kernel.append("order:6", "tick", {"n": 2}, "idem-2", schema_version=1)

        with self.assertRaises(ValueError):
            kernel.write_snapshot("order:6", {"sum": 3}, "idem-snap-bad", covers_through_seq=3, schema_version=1)

        snap_ack = kernel.write_snapshot("order:6", {"sum": 3}, "idem-snap-ok", covers_through_seq=2, schema_version=1)
        self.assertEqual(snap_ack.status, "committed")
        self.assertEqual(snap_ack.envelope.record_type, SNAPSHOT_RECORD_TYPE)
        self.assertEqual(snap_ack.envelope.covers_through_seq, 2)
        self.assertEqual(snap_ack.envelope.seq, 3)

    def test_bind_shard_precedes_write_and_write_before_ack(self) -> None:
        """TR-DD1-007: bind_shard が write_transaction より先に呼ばれ、COMMIT 失敗時は通知されない。"""
        store: InMemoryStorageBackend[dict[str, Any]] = InMemoryStorageBackend()
        call_log: list[str] = []
        base_meta = InMemoryMetaDAO(store)
        base_shard = InMemoryShardDAO(store)

        class _TracingMeta:
            durability = base_meta.durability

            def get_partition(self, pk: str) -> Any:
                return base_meta.get_partition(pk)

            def bind_shard(self, pk: str, sid: str) -> Any:
                call_log.append("bind_shard")
                return base_meta.bind_shard(pk, sid)

            def set_partition_status(self, pk: str, status: Any, *, expected_status: Any = None) -> Any:
                return base_meta.set_partition_status(pk, status, expected_status=expected_status)

            def list_partitions(self, *, status: Any = None) -> Any:
                return base_meta.list_partitions(status=status)

            def get_cursor(self, cid: str, pk: str) -> int:
                return base_meta.get_cursor(cid, pk)

            def advance_cursor(self, cid: str, pk: str, seq: int) -> None:
                base_meta.advance_cursor(cid, pk, seq)

        fail_commit = {"enabled": False}

        class _TracingShard:
            durability = base_shard.durability

            @contextmanager
            def write_transaction(self, sid: str) -> Iterator[Any]:
                call_log.append("write_transaction")
                with base_shard.write_transaction(sid) as tx:
                    yield tx
                    if fail_commit["enabled"]:
                        raise RuntimeError("disk failure on commit")

            def fetch_range(self, sid: str, pk: str, after_seq: int, limit: int) -> Any:
                return base_shard.fetch_range(sid, pk, after_seq, limit)

        kernel, _ = _make_kernel(backend=store, shard_dao=_TracingShard(), meta_dao=_TracingMeta())
        notified: list[RecordEnvelope[dict[str, Any]]] = []
        kernel._post_commit_listeners.append(lambda env, _is_snap: notified.append(env))

        kernel.append("order:7", "tick", {"v": 1}, "idem-1", schema_version=1)
        self.assertEqual(call_log, ["bind_shard", "write_transaction"])
        self.assertEqual(len(notified), 1)

        fail_commit["enabled"] = True
        with self.assertRaises(RuntimeError):
            kernel.append("order:7", "tick", {"v": 2}, "idem-2", schema_version=1)
        self.assertEqual(len(notified), 1)

    def test_duplicate_write_returns_existing_without_redispatch(self) -> None:
        """TR-DD1-008: 同一キー・同一ハッシュ再送時は duplicate を返しイベント再配信しない。"""
        kernel, _ = _make_kernel()
        dispatched: list[RecordEnvelope[dict[str, Any]]] = []
        kernel._post_commit_listeners.append(lambda env, _is_snap: dispatched.append(env))

        first = kernel.append("order:8", "tick", {"a": 1, "b": 2}, "idem-dup", schema_version=1)
        second = kernel.append("order:8", "tick", {"b": 2, "a": 1}, "idem-dup", schema_version=1)

        self.assertEqual(first.status, "committed")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(second.envelope.seq, first.envelope.seq)
        self.assertEqual(len(dispatched), 1)


if __name__ == "__main__":
    unittest.main()
