"""親プロジェクトの DAO / Projection / Codec / Upcaster 実装が基盤の不変条件（13項目）を満たすか検証する基底クラス。

サードパーティ依存を避けるため pytest を import せず標準ライブラリの unittest ベースで実装する。
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import time
from typing import Generic, TypeVar
import unittest

from ..contracts.envelope import RecordEnvelope
from ..contracts.errors import (
    DuplicateKeyError,
    PartitionStateConflictError,
    SchemaVersionError,
)
from ..ports.codec import PayloadCodec, PayloadUpcaster
from ..ports.dao import MetaDAOPort, ProjectionPort, ShardDAOPort

T = TypeVar("T")


@dataclass
class DAOContractBundle(Generic[T]):
    """Contract Test が対象バックエンドを検証するために必要な Port 群とテスト入力。"""

    shard_dao: ShardDAOPort[T]
    meta_dao: MetaDAOPort
    projection: ProjectionPort[T]
    codec: PayloadCodec[T]
    upcaster: PayloadUpcaster
    make_payload: Callable[[int], T]
    equivalent_payloads: tuple[T, T]
    valid_upcast_input: tuple[str, int, bytes]
    unknown_upcast_input: tuple[str, int, bytes]


class StorageKernelContractTest(unittest.TestCase, Generic[T]):
    """DBF-IF-001 §6（TR-IF-001〜TR-IF-013）に定める 13 項目の契約テスト基底クラス。

    親プロジェクトは本クラスを継承し、同じバックエンドを指す新しいインスタンス一式を返す
    ``make_dao()`` を実装する。
    """

    def make_dao(self) -> DAOContractBundle[T]:
        """同じストレージバックエンドを指す新しい Port インスタンス群を返すフック。"""
        raise unittest.SkipTest("Override make_dao() in a concrete ContractTest subclass.")

    def projected_seqs(self, projection: ProjectionPort[T], partition_key: str) -> Sequence[int]:
        """指定パーティションについて Projection が保持している ``seq`` を 1 件 1 要素で返すフック（10 番用）。

        ``ProjectionPort`` には状態の読み戻し口がないため、親プロジェクトが実装する。
        同じ ``seq`` を重複して保持している場合は、その件数分だけ同じ値を含めて返すこと。
        未実装（既定）のときは TR-IF-010 を黙って通さず ``NotImplementedError`` で失敗する。
        """
        raise NotImplementedError(
            "Override projected_seqs(projection, partition_key) in the ContractTest subclass: "
            "return the seq of every row the projection holds for partition_key "
            "(one element per stored row, duplicates included)."
        )

    @staticmethod
    def _envelope(
        *,
        partition_key: str,
        seq: int,
        idempotency_key: str,
        payload: T,
        payload_hash: str = "hash-1",
        record_type: str = "event",
        schema_version: int = 1,
    ) -> RecordEnvelope[T]:
        return RecordEnvelope(
            partition_key=partition_key,
            seq=seq,
            idempotency_key=idempotency_key,
            record_type=record_type,
            schema_version=schema_version,
            payload_hash=payload_hash,
            payload=payload,
            created_at=time.time(),
        )

    def test_01_rollback_atomicity(self) -> None:
        """TR-IF-001: 例外送出時に挿入レコードと採番シーケンスの両方が巻き戻ること。"""
        bundle = self.make_dao()
        pk = "contract:rollback"
        shard_id = "shard_0"

        class _SimulatedTxFailure(RuntimeError):
            pass

        with self.assertRaises(_SimulatedTxFailure):
            with bundle.shard_dao.write_transaction(shard_id) as tx:
                seq = tx.next_sequence(pk)
                self.assertEqual(seq, 1)
                tx.insert_record(
                    self._envelope(
                        partition_key=pk,
                        seq=seq,
                        idempotency_key="idem-rollback",
                        payload=bundle.make_payload(1),
                    )
                )
                raise _SimulatedTxFailure("abort transaction")

        self.assertEqual(list(bundle.shard_dao.fetch_range(shard_id, pk, after_seq=0, limit=10)), [])

        with bundle.shard_dao.write_transaction(shard_id) as tx:
            next_seq = tx.next_sequence(pk)
            self.assertEqual(next_seq, 1)

    def test_02_persistence_across_reconnect(self) -> None:
        """TR-IF-002: 再生成した DAO インスタンスから直前の書き込み・バインド・カーソルが読めること。"""
        bundle1 = self.make_dao()
        check_shard = bundle1.shard_dao.durability != "none"
        check_meta = bundle1.meta_dao.durability != "none"
        if not check_shard and not check_meta:
            self.skipTest("durability='none' のため再接続永続性テストをスキップします")

        pk = "contract:persist"
        shard_id = "shard_0"
        if check_meta:
            bundle1.meta_dao.bind_shard(pk, shard_id)
            bundle1.meta_dao.advance_cursor("consumer-1", pk, 1)

        if check_shard:
            with bundle1.shard_dao.write_transaction(shard_id) as tx:
                seq = tx.next_sequence(pk)
                tx.insert_record(
                    self._envelope(
                        partition_key=pk,
                        seq=seq,
                        idempotency_key="idem-persist",
                        payload=bundle1.make_payload(42),
                        payload_hash="hash-42",
                    )
                )

        bundle2 = self.make_dao()
        if check_meta:
            info = bundle2.meta_dao.get_partition(pk)
            self.assertIsNotNone(info)
            assert info is not None
            self.assertEqual(info.shard_id, shard_id)
            self.assertEqual(bundle2.meta_dao.get_cursor("consumer-1", pk), 1)
        if check_shard:
            rows = list(bundle2.shard_dao.fetch_range(shard_id, pk, after_seq=0, limit=10))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].seq, 1)
            self.assertEqual(rows[0].payload_hash, "hash-42")

    def test_03_monotonic_sequence_and_fetch_order(self) -> None:
        """TR-IF-003: 連続・並行書き込みで seq が 1, 2, ... と欠番なく採番され昇順取得できること。"""
        bundle = self.make_dao()
        pk = "contract:monotonic"
        shard_id = "shard_0"
        total = 20

        # 連続書き込み: 同一スレッドで N 件。seq は 1..N の欠番・重複なし。
        seq_pk = "contract:monotonic_seq"
        sequential: list[int] = []
        for idx in range(total):
            with bundle.shard_dao.write_transaction(shard_id) as tx:
                seq = tx.next_sequence(seq_pk)
                tx.insert_record(
                    self._envelope(
                        partition_key=seq_pk,
                        seq=seq,
                        idempotency_key=f"idem-seq-{idx}",
                        payload=bundle.make_payload(idx),
                    )
                )
                sequential.append(seq)
        self.assertEqual(sequential, list(range(1, total + 1)))
        seq_rows = list(bundle.shard_dao.fetch_range(shard_id, seq_pk, after_seq=0, limit=total + 5))
        self.assertEqual([r.seq for r in seq_rows], list(range(1, total + 1)))

        def _write_one(idx: int) -> int:
            with bundle.shard_dao.write_transaction(shard_id) as tx:
                seq = tx.next_sequence(pk)
                tx.insert_record(
                    self._envelope(
                        partition_key=pk,
                        seq=seq,
                        idempotency_key=f"idem-{idx}",
                        payload=bundle.make_payload(idx),
                    )
                )
                return seq

        with ThreadPoolExecutor(max_workers=4) as pool:
            assigned = list(pool.map(_write_one, range(total)))

        self.assertEqual(sorted(assigned), list(range(1, total + 1)))
        rows = list(bundle.shard_dao.fetch_range(shard_id, pk, after_seq=0, limit=total + 5))
        self.assertEqual([r.seq for r in rows], list(range(1, total + 1)))

    def test_04_idempotent_lookup_returns_original(self) -> None:
        """TR-IF-004: 同一 (partition_key, idempotency_key) で元の RecordEnvelope が返り seq が増えないこと。"""
        bundle = self.make_dao()
        pk = "contract:idem"
        shard_id = "shard_0"

        with bundle.shard_dao.write_transaction(shard_id) as tx:
            seq = tx.next_sequence(pk)
            tx.insert_record(
                self._envelope(
                    partition_key=pk,
                    seq=seq,
                    idempotency_key="idem-same",
                    payload=bundle.make_payload(100),
                    payload_hash="hash-100",
                )
            )

        # 再送: 既存があれば採番も挿入もしない（カーネルの冪等ヒット経路と同じ手順）。
        with bundle.shard_dao.write_transaction(shard_id) as tx:
            found = tx.find_by_idempotency(pk, "idem-same")
            self.assertIsNotNone(found)
            assert found is not None
            self.assertEqual(found.seq, 1)
            self.assertEqual(found.payload_hash, "hash-100")
            self.assertEqual(found.payload, bundle.make_payload(100))

        rows = list(bundle.shard_dao.fetch_range(shard_id, pk, after_seq=0, limit=10))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].seq, 1)
        self.assertEqual(rows[0].payload_hash, "hash-100")
        self.assertEqual(rows[0].payload, bundle.make_payload(100))

        # 再送後も最終 seq は 1 のまま。次の新規書き込みは seq=2 になる。
        with bundle.shard_dao.write_transaction(shard_id) as tx:
            self.assertEqual(tx.next_sequence(pk), 2)

    def test_05_unique_violation_raises_duplicate_key_error(self) -> None:
        """TR-IF-005: 一意制約違反時に DB 固有例外でなく DuplicateKeyError が送出されること。"""
        bundle = self.make_dao()
        pk = "contract:dup"
        shard_id = "shard_0"

        with bundle.shard_dao.write_transaction(shard_id) as tx:
            tx.insert_record(
                self._envelope(
                    partition_key=pk,
                    seq=1,
                    idempotency_key="idem-1",
                    payload=bundle.make_payload(1),
                )
            )

        with self.assertRaises(DuplicateKeyError) as seq_ctx:
            with bundle.shard_dao.write_transaction(shard_id) as tx:
                tx.insert_record(
                    self._envelope(
                        partition_key=pk,
                        seq=1,
                        idempotency_key="idem-2",
                        payload=bundle.make_payload(2),
                    )
                )
        self.assertEqual(seq_ctx.exception.kind, "sequence")

        with self.assertRaises(DuplicateKeyError) as idem_ctx:
            with bundle.shard_dao.write_transaction(shard_id) as tx:
                tx.insert_record(
                    self._envelope(
                        partition_key=pk,
                        seq=2,
                        idempotency_key="idem-1",
                        payload=bundle.make_payload(2),
                    )
                )
        self.assertEqual(idem_ctx.exception.kind, "idempotency_key")

    def test_06_idempotency_scope_isolated_by_partition(self) -> None:
        """TR-IF-006: partition_key が異なれば同じ idempotency_key でも独立して seq=1 から書き込めること。"""
        bundle = self.make_dao()
        shard_id = "shard_0"

        for pk in ("contract:part_a", "contract:part_b"):
            with bundle.shard_dao.write_transaction(shard_id) as tx:
                seq = tx.next_sequence(pk)
                self.assertEqual(seq, 1)
                tx.insert_record(
                    self._envelope(
                        partition_key=pk,
                        seq=seq,
                        idempotency_key="shared-idem-key",
                        payload=bundle.make_payload(1),
                    )
                )

    def test_07_bind_shard_idempotent_and_immutable(self) -> None:
        """TR-IF-007: 並行・複数回の bind_shard で最初に確定した shard_id が維持されること。"""
        bundle = self.make_dao()
        pk = "contract:bind"

        first = bundle.meta_dao.bind_shard(pk, "shard_alpha")
        self.assertEqual(first.shard_id, "shard_alpha")

        def _bind_attempt(candidate: str) -> str:
            return bundle.meta_dao.bind_shard(pk, candidate).shard_id

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(_bind_attempt, ["shard_beta", "shard_gamma", "shard_delta", "shard_epsilon"]))

        self.assertTrue(all(sid == "shard_alpha" for sid in results))
        fetched = bundle.meta_dao.get_partition(pk)
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched.shard_id, "shard_alpha")

    def test_08_empty_partition_allowed(self) -> None:
        """TR-IF-008: bind_shard 済みの空パーティションは fetch_range が空列を返し初回が seq=1 になること。"""
        bundle = self.make_dao()
        pk = "contract:empty"
        shard_id = "shard_0"

        info = bundle.meta_dao.bind_shard(pk, shard_id)
        self.assertEqual(info.status, "active")
        self.assertEqual(list(bundle.shard_dao.fetch_range(shard_id, pk, after_seq=0, limit=10)), [])

        with bundle.shard_dao.write_transaction(shard_id) as tx:
            seq = tx.next_sequence(pk)
            self.assertEqual(seq, 1)
            tx.insert_record(
                self._envelope(
                    partition_key=pk,
                    seq=seq,
                    idempotency_key="idem-first",
                    payload=bundle.make_payload(1),
                )
            )

    def test_09_cursor_monotonic_non_decreasing(self) -> None:
        """TR-IF-009: 過去の小さい値で advance_cursor を呼んでもカーソルが後退しないこと。"""
        bundle = self.make_dao()
        consumer = "consumer-mono"
        pk = "contract:cursor"

        self.assertEqual(bundle.meta_dao.get_cursor(consumer, pk), 0)
        bundle.meta_dao.advance_cursor(consumer, pk, 10)
        self.assertEqual(bundle.meta_dao.get_cursor(consumer, pk), 10)
        bundle.meta_dao.advance_cursor(consumer, pk, 4)
        self.assertEqual(bundle.meta_dao.get_cursor(consumer, pk), 10)
        bundle.meta_dao.advance_cursor(consumer, pk, 15)
        self.assertEqual(bundle.meta_dao.get_cursor(consumer, pk), 15)

    def test_10_projection_apply_batch_idempotent(self) -> None:
        """TR-IF-010: 同じバッチを 2 回 apply_batch しても重複や状態破壊が起きないこと。"""
        bundle = self.make_dao()
        batch = [
            self._envelope(
                partition_key="contract:proj",
                seq=1,
                idempotency_key="idem-p1",
                payload=bundle.make_payload(1),
            ),
            self._envelope(
                partition_key="contract:proj",
                seq=2,
                idempotency_key="idem-p2",
                payload=bundle.make_payload(2),
            ),
        ]
        bundle.projection.apply_batch(batch)
        bundle.projection.apply_batch(batch)
        self.assertEqual(
            sorted(self.projected_seqs(bundle.projection, "contract:proj")),
            [1, 2],
            "同じバッチの再適用で (partition_key, seq) が重複・欠落しています",
        )

    def test_11_payload_codec_deterministic_encode(self) -> None:
        """TR-IF-011: 論理的に等価な値を encode したとき常に同一バイト列を返すこと。"""
        bundle = self.make_dao()
        left, right = bundle.equivalent_payloads
        encoded_left = bundle.codec.encode(left)
        encoded_right = bundle.codec.encode(right)
        self.assertEqual(encoded_left, encoded_right)
        self.assertIsInstance(encoded_left, bytes)

    def test_12_partition_status_transition_and_listing(self) -> None:
        """TR-IF-012: bind_shard 直後は active、expected_status 不一致は例外、list_partitions が条件どおり返すこと。"""
        bundle = self.make_dao()
        info_a = bundle.meta_dao.bind_shard("contract:status_a", "shard_0")
        info_b = bundle.meta_dao.bind_shard("contract:status_b", "shard_1")
        self.assertEqual(info_a.status, "active")
        self.assertEqual(info_b.status, "active")

        with self.assertRaises(PartitionStateConflictError):
            bundle.meta_dao.set_partition_status("contract:status_a", "read_only", expected_status="read_only")

        updated = bundle.meta_dao.set_partition_status("contract:status_a", "read_only", expected_status="active")
        self.assertEqual(updated.status, "read_only")

        only_active = {p.partition_key for p in bundle.meta_dao.list_partitions(status="active")}
        only_ro = {p.partition_key for p in bundle.meta_dao.list_partitions(status="read_only")}
        self.assertIn("contract:status_b", only_active)
        self.assertNotIn("contract:status_a", only_active)
        self.assertIn("contract:status_a", only_ro)

    def test_13_payload_upcaster_deterministic_and_unknown_version(self) -> None:
        """TR-IF-013: Upcaster が同じ入力に同じバイト列を返し、未知版で SchemaVersionError を送出すること。"""
        bundle = self.make_dao()
        rec_type, ver, raw = bundle.valid_upcast_input
        out1 = bundle.upcaster.upcast(rec_type, ver, raw)
        out2 = bundle.upcaster.upcast(rec_type, ver, raw)
        self.assertEqual(out1, out2)
        self.assertIsInstance(out1, bytes)

        bad_type, bad_ver, bad_raw = bundle.unknown_upcast_input
        with self.assertRaises(SchemaVersionError):
            bundle.upcaster.upcast(bad_type, bad_ver, bad_raw)


ShardDAOContractTest = StorageKernelContractTest
