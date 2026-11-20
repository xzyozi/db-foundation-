"""contrib.sqlite 内部で共有する BEGIN IMMEDIATE / COMMIT / ROLLBACK の補助。"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import logging
import sqlite3

logger = logging.getLogger(__name__)


@contextmanager
def immediate_transaction(conn: sqlite3.Connection) -> Iterator[None]:
    """`BEGIN IMMEDIATE` で排他書き込みトランザクションを開始し、正常終了時に COMMIT する。

    ブロック内または COMMIT で例外が出たときは ROLLBACK してから、元の例外をそのまま送出する。
    ROLLBACK 自体が失敗した場合（COMMIT 失敗で既に巻き戻されている等）は、ログに記録するだけで、
    元の例外を ROLLBACK の例外で置き換えない。
    """
    conn.execute("BEGIN IMMEDIATE;")
    try:
        yield
        conn.execute("COMMIT;")
    except Exception:
        try:
            conn.execute("ROLLBACK;")
        except sqlite3.Error:
            logger.warning("ROLLBACK に失敗しました（元の例外を優先して送出します）", exc_info=True)
        raise
