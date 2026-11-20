"""ペイロードの決定論的シリアライズとスキーマ版 Upcast の契約。"""

from __future__ import annotations

from typing import Protocol, TypeVar

T = TypeVar("T")


class PayloadCodec(Protocol[T]):
    """ペイロードのエンコード・デコード契約。

    encode は決定論的であること（論理的に等価な値から常に同じバイト列を返すこと）。
    core/ingestion はこの出力から payload_hash を算出し、サイズ上限（64 KB）も検査する。
    """

    def encode(self, value: T) -> bytes:
        """ペイロード値を決定論的なバイト列へシリアライズする。"""
        ...

    def decode(self, data: bytes) -> T:
        """最新 schema_version のバイト列からドメインペイロードを復元する。"""
        ...


class PayloadUpcaster(Protocol):
    """旧 schema_version のバイト列を 1 版ずつ進める変換契約。

    - 決定論的で副作用を持たないこと（保存済み元レコードは書き換えない）
    - 旧版は最新 decode で読めないことがあるためバイト列で受け渡し、decode は最後に 1 回だけ行う
    - 変換できない版は SchemaVersionError を送出すること（Downcast は提供しない）
    """

    def upcast(self, record_type: str, from_version: int, raw_payload: bytes) -> bytes:
        """from_version から from_version + 1 へ変換したバイト列を返す。"""
        ...
