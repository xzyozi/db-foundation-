"""重複送信・遅延・トランザクション失敗・投影障害を再現する障害注入ハーネス。"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
import time
from typing import Generic, TypeVar

from ..contracts.envelope import DurabilityLevel, RecordEnvelope
from ..ports.dao import ProjectionPort, ShardDAOPort, ShardTxPort

T = TypeVar("T")


class InjectedFaultError(RuntimeError):
    """障害注入ハーネスが意図的に送出するテスト用例外。"""


class FaultInjectingShardDAO(Generic[T]):
    """ShardDAOPort をラップし、トランザクション遅延や途中失敗を注入するハーネス。"""

    def __init__(
        self,
        inner: ShardDAOPort[T],
        *,
        delay_before_commit_sec: float = 0.0,
        fail_on_tx_calls: set[int] | None = None,
        fail_on_fetch_calls: set[int] | None = None,
    ) -> None:
        self._inner = inner
        self._delay_before_commit_sec = delay_before_commit_sec
        self._fail_on_tx_calls = set(fail_on_tx_calls or ())
        self._fail_on_fetch_calls = set(fail_on_fetch_calls or ())
        self.tx_call_count = 0
        self.fetch_call_count = 0

    @property
    def durability(self) -> DurabilityLevel:
        return self._inner.durability

    @contextmanager
    def write_transaction(self, shard_id: str) -> Iterator[ShardTxPort[T]]:
        self.tx_call_count += 1
        call_no = self.tx_call_count
        with self._inner.write_transaction(shard_id) as tx:
            yield tx
            if self._delay_before_commit_sec > 0.0:
                time.sleep(self._delay_before_commit_sec)
            if call_no in self._fail_on_tx_calls:
                raise InjectedFaultError(f"injected write_transaction failure at call #{call_no}")

    def fetch_range(
        self,
        shard_id: str,
        partition_key: str,
        after_seq: int,
        limit: int,
    ) -> Sequence[RecordEnvelope[T]]:
        self.fetch_call_count += 1
        call_no = self.fetch_call_count
        if call_no in self._fail_on_fetch_calls:
            raise InjectedFaultError(f"injected fetch_range failure at call #{call_no}")
        return self._inner.fetch_range(shard_id, partition_key, after_seq, limit)


class FaultInjectingProjection(Generic[T]):
    """ProjectionPort をラップし、呼び出し回数・レコード条件に応じた遅延/障害を注入するハーネス。"""

    def __init__(
        self,
        inner: ProjectionPort[T],
        *,
        fail_calls: set[int] | None = None,
        fail_predicate: Callable[[RecordEnvelope[T]], bool] | None = None,
        delay_sec: float = 0.0,
    ) -> None:
        self._inner = inner
        self._fail_calls = set(fail_calls or ())
        self._fail_predicate = fail_predicate
        self._delay_sec = delay_sec
        self.apply_call_count = 0

    @property
    def consumer_id(self) -> str:
        return self._inner.consumer_id

    def apply_batch(self, records: Sequence[RecordEnvelope[T]]) -> None:
        self.apply_call_count += 1
        if self._delay_sec > 0.0:
            time.sleep(self._delay_sec)
        if self.apply_call_count in self._fail_calls:
            raise InjectedFaultError(f"injected apply_batch failure at call #{self.apply_call_count}")
        if self._fail_predicate is not None:
            for rec in records:
                if self._fail_predicate(rec):
                    raise InjectedFaultError(f"injected poison pill failure at ({rec.partition_key}, {rec.seq})")
        self._inner.apply_batch(records)
