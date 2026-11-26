"""examples.postgres_dao 内部で共有する PostgreSQL 接続・トランザクション・識別子補助。"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg import sql

from storage_kernel.contracts.envelope import DurabilityLevel


def table_ident(schema: str, table: str) -> sql.Identifier:
    """スキーマ修飾されたテーブル識別子を返す。"""
    if not schema:
        raise ValueError("schema must be non-empty")
    return sql.Identifier(schema, table)


def ensure_schema_exists(conn: psycopg.Connection[tuple[Any, ...]], schema: str) -> None:
    """public 以外のスキーマが未作成なら作成する。"""
    if schema != "public":
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))


@contextmanager
def open_connection(dsn: str) -> Iterator[psycopg.Connection[tuple[Any, ...]]]:
    """PostgreSQL 接続を開き、抜けるときに確実にクローズする。"""
    conn = psycopg.connect(dsn)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def write_tx(
    dsn: str,
    *,
    durability: DurabilityLevel = "power_loss",
) -> Iterator[psycopg.Connection[tuple[Any, ...]]]:
    """トランザクション付きで接続を開き、正常終了時に COMMIT、例外時に ROLLBACK する。"""
    with open_connection(dsn) as conn, conn.transaction():
        if durability == "none":
            conn.execute("SET LOCAL synchronous_commit = off")
        yield conn
