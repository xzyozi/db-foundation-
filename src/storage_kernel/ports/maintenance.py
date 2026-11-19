"""メンテナンスモード遷移時の拡張フック契約。"""

from __future__ import annotations

from typing import Protocol


class MaintenanceHooks(Protocol):
    """親プロジェクトが実装するメンテナンスの拡張口。

    v1 はメンテナンスモードの開始・終了の通知のみ定義する。
    スナップショット自動生成・Retention・VACUUM は v2 以降で追加する。
    """

    def on_enter_maintenance(self, reason: str) -> None:
        """書き込みと進行中トランザクションが静止した整合点で呼ばれる（バックアップはここで取得）。"""
        ...

    def on_exit_maintenance(self) -> None:
        """exit_maintenance() により通常運転へ戻る直前に呼ばれる。"""
        ...
