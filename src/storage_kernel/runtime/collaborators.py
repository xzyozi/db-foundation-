"""runtime 層が協調先に要求する最小インターフェース（構造的部分型）。

runtime は core に依存しない（DBF-BD-001 C-006）。core の ProjectionRunner / EventDispatcher は
本モジュールの Protocol を構造的に満たすため、core/kernel.py がそのまま注入できる。
"""

from __future__ import annotations

from typing import Protocol


class ProjectionControl(Protocol):
    """Projection 群の停止・メンテナンス静止を制御する操作。"""

    def stop(self, *, drain: bool, deadline: float | None) -> tuple[tuple[str, ...], bool]:
        """停止し、(未完了パーティション一覧, 全件クリーン完了) を返す。"""
        ...

    def enter_maintenance(self, *, drain: bool, deadline: float) -> bool:
        """メンテナンス静止へ移行し、期限内に完了したかを返す。"""
        ...

    def exit_maintenance(self) -> None:
        """メンテナンス静止を解除する。"""
        ...


class SubscriberControl(Protocol):
    """Subscriber 配信の停止操作。"""

    def stop(self, *, drain: bool, deadline: float | None) -> tuple[int, bool]:
        """停止し、(破棄件数合計, 全件クリーン完了) を返す。"""
        ...
