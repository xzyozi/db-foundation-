"""テレメトリイベント型と計測シンクの契約。"""

from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Mapping, Protocol


@dataclass(frozen=True)
class TelemetryEvent:
    """基盤内部の状態遷移・処理結果・エラーを通知するための構造化イベント。"""

    name: str
    attributes: Mapping[str, str | int | float | bool] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


class TelemetrySink(Protocol):
    """親プロジェクト側でメトリクスや構造化ログを受け取る任意のシンク契約。"""

    def emit(self, event: TelemetryEvent) -> None:
        """構造化テレメトリイベントを 1 件記録する（例外を送出しないこと）。"""
        ...
