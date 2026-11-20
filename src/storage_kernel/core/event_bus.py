"""パターン照合と購読者別有界キューによる At-Most-Once イベント配信。"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
import logging
import threading
import time
import uuid

from ..contracts.envelope import (
    SNAPSHOT_RECORD_TYPE,
    DropPolicy,
    RecordEnvelope,
    SubscriptionHandle,
)
from ..contracts.telemetry import TelemetryEvent, TelemetrySink

logger = logging.getLogger("storage_kernel.event_bus")


class PatternMatcher:
    """完全一致・全件('*')・末尾ワイルドカード('prefix:*')のパターン照合を行う。"""

    def __init__(self, pattern: str) -> None:
        if not pattern:
            raise ValueError("subscription pattern must be non-empty")
        if pattern == "*":
            self._kind = "all"
            self._prefix = ""
        elif "*" in pattern:
            if not pattern.endswith("*") or pattern.count("*") > 1 or len(pattern) < 2:
                raise ValueError(
                    f"invalid pattern {pattern!r}: '*' is only allowed as '*' or a single trailing wildcard"
                )
            self._kind = "prefix"
            self._prefix = pattern[:-1]
        else:
            self._kind = "exact"
            self._prefix = pattern
        self.pattern = pattern

    def matches(self, partition_key: str) -> bool:
        """partition_key がこのパターンに一致するか判定する。"""
        if self._kind == "all":
            return True
        if self._kind == "prefix":
            return partition_key.startswith(self._prefix)
        return partition_key == self._prefix


class _SubscriberSlot(SubscriptionHandle):
    """購読者1件ごとの有界キュー・配信スレッド・SubscriptionHandle 実装。"""

    def __init__(
        self,
        *,
        subscription_id: str,
        matcher: PatternMatcher,
        callback: Callable[[RecordEnvelope[object]], None],
        max_queue_size: int,
        drop_policy: DropPolicy,
        on_error: Callable[[RecordEnvelope[object], Exception], None] | None,
        telemetry_sink: TelemetrySink | None,
    ) -> None:
        self._subscription_id = subscription_id
        self._matcher = matcher
        self._callback = callback
        self._max_queue_size = max_queue_size
        self._drop_policy = drop_policy
        self._on_error = on_error
        self._telemetry_sink = telemetry_sink

        self._cond = threading.Condition()
        self._queue: deque[RecordEnvelope[object]] = deque()
        self._active = True
        self._closed = False
        self._in_flight = False
        self._dropped_count = 0

        self._thread = threading.Thread(
            target=self._run,
            name=f"sk-sub-{subscription_id[:8]}",
            daemon=True,
        )
        self._thread.start()

    @property
    def subscription_id(self) -> str:
        return self._subscription_id

    @property
    def is_active(self) -> bool:
        with self._cond:
            return self._active and not self._closed

    @property
    def dropped_count(self) -> int:
        with self._cond:
            return self._dropped_count

    def matches(self, partition_key: str) -> bool:
        return self._matcher.matches(partition_key)

    def _emit_telemetry(self, name: str, attributes: dict[str, str | int | float | bool]) -> None:
        if self._telemetry_sink is not None:
            self._telemetry_sink.emit(TelemetryEvent(name=name, attributes=attributes))

    def enqueue(self, envelope: RecordEnvelope[object]) -> None:
        """有界キューへイベントをノンブロッキング投入する。"""
        with self._cond:
            if not self._active or self._closed:
                return
            if len(self._queue) < self._max_queue_size:
                self._queue.append(envelope)
                self._cond.notify_all()
                return

            self._dropped_count += 1
            dropped_now = self._dropped_count
            # drop_newest は新規イベントを捨てるだけでキューを変更しないため、ここでは何もしない。
            if self._drop_policy == "drop_oldest":
                self._queue.popleft()
                self._queue.append(envelope)
            elif self._drop_policy == "disconnect":
                self._active = False
                self._cond.notify_all()

        logger.warning(
            "Subscriber %s queue full (policy=%s, dropped=%d)",
            self._subscription_id,
            self._drop_policy,
            dropped_now,
        )
        self._emit_telemetry(
            "subscriber.dropped",
            {
                "subscription_id": self._subscription_id,
                "drop_policy": self._drop_policy,
                "dropped_count": dropped_now,
            },
        )

    def unsubscribe(self) -> None:
        """購読を解除する（冪等）。"""
        with self._cond:
            if not self._active:
                return
            self._active = False
            self._queue.clear()
            self._cond.notify_all()

    def stop(self, *, drain: bool, deadline: float) -> tuple[int, bool]:
        """購読ワーカーを停止し、(破棄件数, タイムアウト内に完了したか) を返す。"""
        dropped_on_stop = 0
        with self._cond:
            self._closed = True
            if not drain:
                dropped_on_stop = len(self._queue)
                self._dropped_count += dropped_on_stop
                self._queue.clear()
                self._active = False
            self._cond.notify_all()

        while True:
            remaining = deadline - time.monotonic()
            with self._cond:
                if not drain:
                    if not self._in_flight:
                        break
                else:
                    if not self._queue and not self._in_flight:
                        self._active = False
                        self._cond.notify_all()
                        break
            if remaining <= 0.0:
                with self._cond:
                    leftover = len(self._queue)
                    if leftover > 0:
                        dropped_on_stop += leftover
                        self._dropped_count += leftover
                        self._queue.clear()
                    self._active = False
                    self._cond.notify_all()
                return dropped_on_stop, False
            self._thread.join(timeout=min(remaining, 0.05))
            if not self._thread.is_alive():
                return dropped_on_stop, True

        self._thread.join(timeout=max(0.0, deadline - time.monotonic()))
        return dropped_on_stop, not self._thread.is_alive()

    def _run(self) -> None:
        while True:
            with self._cond:
                while not self._queue:
                    if self._closed or not self._active:
                        return
                    self._cond.wait()
                envelope = self._queue.popleft()
                self._in_flight = True

            try:
                self._callback(envelope)
            except Exception as exc:
                logger.exception(
                    "Subscriber %s callback raised %s",
                    self._subscription_id,
                    type(exc).__name__,
                )
                self._emit_telemetry(
                    "subscriber.callback_error",
                    {
                        "subscription_id": self._subscription_id,
                        "partition_key": envelope.partition_key,
                        "seq": envelope.seq,
                        "error_type": type(exc).__name__,
                    },
                )
                if self._on_error is not None:
                    try:
                        self._on_error(envelope, exc)
                    except Exception:
                        logger.exception("Subscriber %s on_error handler raised", self._subscription_id)
            finally:
                with self._cond:
                    self._in_flight = False
                    self._cond.notify_all()


class EventDispatcher:
    """At-Most-Once の非同期 Pub/Sub ディスパッチャ。"""

    def __init__(self, *, telemetry_sink: TelemetrySink | None = None) -> None:
        self._telemetry_sink = telemetry_sink
        self._lock = threading.RLock()
        self._slots: list[_SubscriberSlot] = []

    def subscribe(
        self,
        pattern: str,
        callback: Callable[[RecordEnvelope[object]], None],
        *,
        max_queue_size: int = 256,
        drop_policy: DropPolicy = "drop_newest",
        on_error: Callable[[RecordEnvelope[object], Exception], None] | None = None,
    ) -> SubscriptionHandle:
        """購読者を登録し、独立した有界キューと SubscriptionHandle を返す。"""
        if max_queue_size < 1:
            raise ValueError("max_queue_size must be >= 1")
        matcher = PatternMatcher(pattern)
        sub_id = uuid.uuid4().hex
        slot = _SubscriberSlot(
            subscription_id=sub_id,
            matcher=matcher,
            callback=callback,
            max_queue_size=max_queue_size,
            drop_policy=drop_policy,
            on_error=on_error,
            telemetry_sink=self._telemetry_sink,
        )
        with self._lock:
            self._slots.append(slot)
        return slot

    def publish(self, envelope: RecordEnvelope[object], *, is_snapshot: bool = False) -> None:
        """確定済みイベントを一致する購読者へ配信する（__snapshot__ は除外）。"""
        if is_snapshot or envelope.record_type == SNAPSHOT_RECORD_TYPE:
            return
        with self._lock:
            targets = [slot for slot in self._slots if slot.is_active and slot.matches(envelope.partition_key)]
        for slot in targets:
            slot.enqueue(envelope)

    def stop(self, *, drain: bool = False, deadline: float | None = None) -> tuple[int, bool]:
        """全購読ワーカーを停止し、(破棄件数合計, 全件クリーン完了) を返す。"""
        effective_deadline = deadline if deadline is not None else time.monotonic() + 5.0
        with self._lock:
            slots = list(self._slots)
        total_dropped = 0
        all_clean = True
        for slot in slots:
            dropped, clean = slot.stop(drain=drain, deadline=effective_deadline)
            total_dropped += dropped
            if not clean:
                all_clean = False
        return total_dropped, all_clean
