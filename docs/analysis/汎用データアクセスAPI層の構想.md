---
title: "汎用データアクセスAPI層（Storage & Sync Engine API）の構想"
document_type: "analysis"
document_role: "rationale"
ssot_scope: "system_architecture（構想段階の判断材料）"
status: "draft"
version: "0.1"
created_at: "2026-11-19"
updated_at: "2026-11-19"
author: "xzyozi"
owner: "xzyozi"
purpose: "アプリケーション本体と物理DB群の間に汎用データアクセスAPI層を挟む構成の検討材料を記録するため"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "用語集（相対パス: ../../CONTEXT.md）"
---

# 汎用データアクセスAPI層（Storage & Sync Engine API）の構想

アプリケーション本体（ビジネスロジック）と物理DB群の間に汎用データアクセスAPI層（Storage & Sync Engine API）を挟む構成は、実質的に「パーティション分割された分散ログ（Kafka的機能）＋キーバリュー／ドキュメントストア＋差分同期エンジン」を単一ライブラリのAPIとして抽象化するアプローチに相当する。

このAPI層に先行のシステム設計のエッセンスを汎用化して組み込むことで、アプリケーション側のモジュールは「DBの分割数」「ロック競合」「再送による重複」「オフラインからの復旧」「非同期インデックス更新」を意識せず、統一されたメソッドを呼ぶだけで済むようになる。

> 本文書は構想段階の分析資料であり、現在の仕様の正本ではない。採用が決まった時点で、基本設計書（BD）・データ構造仕様書（DS）などの正本へ反映する。

---

## 1. 全体アーキテクチャ：3層分離とAPI層の責務

```text
+-------------------------------------------------------------------+
|  Application / Domain Modules (業務ロジック・UI・各種ワーカー)        |
|   - DBの物理分割やリトライ制御を意識せず、API層のインターフェースのみ利用 |
+---------------------------------+---------------------------------+
                                  |  Unified Storage & Event API
                                  v
+-------------------------------------------------------------------+
|  Universal DB API Layer (汎用データ基盤・ミドルウェア層)               |
|  +-----------------------+  +----------------------------------+  |
|  | 1. Idempotency Guard  |  | 6. Ephemeral State Store (TTL)   |  |
|  |    (UUID重複排除)      |  |    (高頻度・揮発データのメモリ隔離) |  |
|  +-----------+-----------+  +----------------------------------+  |
|              v                                                    |
|  +-----------------------+  +----------------------------------+  |
|  | 2. Partition Router   |  | 5. Internal Event Bus & Fan-out  |  |
|  |    (動的シャード解決)  |  |    (コミット後の非同期Pub/Sub配信) |  |
|  +-----------+-----------+  +----------------+-----------------+  |
|              v                               |                    |
|  +-----------------------+                   |                    |
|  | 3. Monotonic Sequencer|                   |                    |
|  |    (パーティション連番) |                   |                    |
|  +-----------+-----------+                   |                    |
|              v                               |                    |
|  +-----------------------+                   |                    |
|  | 4. Write-Before-ACK   |-------------------+                    |
|  |    (WAL永続化保証)     |                                        |
|  +-----------+-----------+                                        |
|              |              +----------------------------------+  |
|              |              | 7. Cursor Sync Engine (Scatter)  |  |
|              |              |    (since_seq による横断差分取得)  |  |
+--------------+--------------+----------------+--------------------+
               |                               |
               v                               v
+-------------------------------------------------------------------+
|  Physical Storage Layer (役割別・水平分割された複数DB)               |
|  [meta.db]          [shard_0.db ... shard_N.db]   [projection.db] |
|  ・パーティション台帳  ・パーティション別レコード本体   ・全文検索(FTS5)   |
|  ・購読者カーソル     ・単調増加シーケンス管理       ・集計マテビュー   |
+-------------------------------------------------------------------+
```

### API層が引き受ける7つの汎用機能（先行設計図との対応）

| API層コンポーネント | 汎用化された責務 | 元設計図の対応 |
| --- | --- | --- |
| **1. Idempotency Guard** | 呼び出し元が渡す `idempotency_key`（UUID等）で重複書き込みを検知し、二重登録を防ぎつつ既存の確定結果を返す。 | Section D (Idempotency) |
| **2. Partition Router** | データの論理単位である `partition_key`（例: `chat:123`, `job:abc`, `user:42`）から保存先のシャードDBを解決する。高負荷パーティションの専用DB隔離も可能。 | Section O-1 (Partitioning) |
| **3. Monotonic Sequencer** | 端末の時計ズレ（Clock Skew）を排除するため、API層がパーティション単位の厳密な単調増加連番 `seq` を採番する。 | Section D & O-2 (Ordering) |
| **4. Write-Before-ACK** | 対象シャードへの `COMMIT` 完了を保証してから呼び出し元へACK（確定 `seq`）を返し、その後にイベントバスへ流す。 | Section H & O-3 (Durability) |
| **5. Internal Event Bus** | 確定した書き込みイベントをメモリ上のPub/Subで配信。検索DBの更新、集計ビュー作成、UI通知などを本体処理から切り離す。 | Section F, L, M (Event Bus) |
| **6. Ephemeral Store** | 生存確認（Heartbeat）、一時ロック、進捗率などの「消えてもよい高頻度データ」をTTL付きメモリ辞書で捌き、ディスクDBを保護する。 | Section I & O-6 (Ephemeral) |
| **7. Cursor Sync Engine** | モジュールやクライアントが保持する `{partition_key: last_seen_seq}` を受け取り、複数シャードを横断して未処理差分を一括返却する。 | Section J (Offline Sync) |

---

## 2. 汎用DBスキーマ設計（Append-Only + Projection パターン）

汎用性を最大化するため、シャードDB側にはドメイン固有のカラムを持たせず、「パーティションキー ＋ 連番 ＋ JSONペイロード」という不変ログ（イミュータブル・レコード）形式で格納する。用途別のテーブルや全文検索は、Event Bus経由で別DB（`projection.db`）へ非同期に投影（Materialized View化）する。

- **`meta.db`（システム管理・カーソルDB）**
  - `partitions(partition_key PRIMARY KEY, shard_id, status, created_at)`: どのパーティションがどの物理DBファイルに属するかを管理する（動的シャード割り当て・ホットパーティションの隔離に対応）。
  - `cursors(consumer_id, partition_key, last_seq, updated_at)`: 各モジュールやクライアントが「どこまで処理・既読にしたか」を追跡する。
- **`shard_0.db` 〜 `shard_N.db`（水平分割データDB）**
  - `sequences(partition_key PRIMARY KEY, last_seq)`: パーティションごとの最新連番。
  - `records(partition_key, seq, idempotency_key, record_type, payload, created_at)`: `PRIMARY KEY (partition_key, seq)` および `UNIQUE (partition_key, idempotency_key)` を強制する。
- **`projection.db`（参照・検索特化DB ※任意）**
  - `fts_index`（SQLite FTS5仮想テーブル）や、特定用途向けに非正規化した高速検索用テーブルを保持する。壊れても `shard_*.db` の `seq=0` から再構築できる。

---

## 3. 汎用DB基盤API層の実装プロトタイプ（Python標準ライブラリ）

外部依存なし（標準ライブラリのみ）で、上記の7機能（冪等性、動的シャーディング、連番採番、Write-Before-ACK、内部Pub/Sub、非同期Projectionワーカー、揮発TTLストア、カーソル差分同期）を統合した基盤コアの実装例である。

```python
from __future__ import annotations

import hashlib
import json
import queue
import sqlite3
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


@dataclass(frozen=True)
class Record:
    partition_key: str
    seq: int
    idempotency_key: str
    record_type: str
    payload: dict[str, Any]
    created_at: float


@dataclass(frozen=True)
class WriteAck:
    status: str  # "committed" | "duplicate"
    shard_id: str
    record: Record


Subscriber = Callable[[Record], None]


class UniversalStorageAPI:
    """
    モジュール本体と複数DB層を仲介する汎用ストレージ・イベントAPI基盤
    """

    def __init__(self, base_dir: str | Path = "./universal_db", default_shards: int = 4) -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.default_shards = default_shards

        self.meta_path = self.base_dir / "meta.db"
        self.projection_path = self.base_dir / "projection.db"

        # 6. Ephemeral State Store (揮発データ用メモリTTLストア)
        self._ephemeral_lock = threading.Lock()
        self._ephemeral_store: dict[str, tuple[Any, float]] = {}

        # 5. Internal Event Bus & Fan-out (非同期配信キュー)
        self._subscribers_lock = threading.Lock()
        self._subscribers: dict[str, list[Subscriber]] = defaultdict(list)  # partition_key or "*"
        self._event_queue: queue.Queue[Record | None] = queue.Queue(maxsize=10_000)

        self._init_meta_and_projection()
        for i in range(default_shards):
            self._ensure_shard_db(f"shard_{i}")

        # バックグラウンドワーカー起動（Event Bus配信 & Projection DB更新）
        self._worker = threading.Thread(target=self._event_dispatch_loop, daemon=True)
        self._worker.start()

    # ------------------------------------------------------------------
    # DB接続・初期化ヘルパー
    # ------------------------------------------------------------------
    def _connect(self, db_path: Path) -> sqlite3.Connection:
        conn = sqlite3.connect(db_path, timeout=15.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _shard_path(self, shard_id: str) -> Path:
        return self.base_dir / f"{shard_id}.db"

    def _init_meta_and_projection(self) -> None:
        with self._connect(self.meta_path) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS partitions (
                    partition_key TEXT PRIMARY KEY,
                    shard_id TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cursors (
                    consumer_id TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    last_seq INTEGER NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (consumer_id, partition_key)
                );
                """
            )

        # 非同期プロジェクションDB（全文検索FTS5の例）
        with self._connect(self.projection_path) as conn:
            conn.executescript(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS records_fts USING fts5(
                    partition_key UNINDEXED,
                    seq UNINDEXED,
                    record_type,
                    content
                );
                """
            )

    def _ensure_shard_db(self, shard_id: str) -> Path:
        path = self._shard_path(shard_id)
        with self._connect(path) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sequences (
                    partition_key TEXT PRIMARY KEY,
                    last_seq INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS records (
                    partition_key TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    record_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (partition_key, seq),
                    UNIQUE (partition_key, idempotency_key)
                );
                """
            )
        return path

    # ------------------------------------------------------------------
    # 2. Partition Router (動的ルーティング & ホットパーティション隔離)
    # ------------------------------------------------------------------
    def resolve_shard(self, partition_key: str, dedicated_shard: str | None = None) -> str:
        """
        meta.db を参照してパーティションの格納先シャードを決定する。
        未登録の場合は自動割り当て（dedicated_shard指定時は専用DBファイルへ隔離）。
        """
        with self._connect(self.meta_path) as conn:
            row = conn.execute(
                "SELECT shard_id FROM partitions WHERE partition_key = ?",
                (partition_key,),
            ).fetchone()
            if row:
                return str(row["shard_id"])

            if dedicated_shard:
                shard_id = dedicated_shard
            else:
                digest = hashlib.blake2b(partition_key.encode("utf-8"), digest_size=4).digest()
                idx = int.from_bytes(digest, "big") % self.default_shards
                shard_id = f"shard_{idx}"

            self._ensure_shard_db(shard_id)
            conn.execute(
                """
                INSERT OR IGNORE INTO partitions (partition_key, shard_id, created_at)
                VALUES (?, ?, ?)
                """,
                (partition_key, shard_id, time.time()),
            )
            # 競合時の再取得
            final_row = conn.execute(
                "SELECT shard_id FROM partitions WHERE partition_key = ?",
                (partition_key,),
            ).fetchone()
            return str(final_row["shard_id"])

    # ------------------------------------------------------------------
    # 1, 3, 4. Append API (冪等性・連番採番・Write-Before-ACK)
    # ------------------------------------------------------------------
    def append(
        self,
        partition_key: str,
        record_type: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> WriteAck:
        shard_id = self.resolve_shard(partition_key)
        db_path = self._shard_path(shard_id)
        now = time.time()
        payload_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

        with self._connect(db_path) as conn:
            conn.execute("BEGIN IMMEDIATE;")
            try:
                # 1. Idempotency Guard
                dup = conn.execute(
                    """
                    SELECT seq, record_type, payload, created_at
                    FROM records
                    WHERE partition_key = ? AND idempotency_key = ?
                    """,
                    (partition_key, idempotency_key),
                ).fetchone()

                if dup:
                    conn.execute("COMMIT;")
                    rec = Record(
                        partition_key=partition_key,
                        seq=int(dup["seq"]),
                        idempotency_key=idempotency_key,
                        record_type=str(dup["record_type"]),
                        payload=json.loads(dup["payload"]),
                        created_at=float(dup["created_at"]),
                    )
                    return WriteAck(status="duplicate", shard_id=shard_id, record=rec)

                # 3. Monotonic Sequencer
                conn.execute(
                    """
                    INSERT INTO sequences (partition_key, last_seq)
                    VALUES (?, 1)
                    ON CONFLICT(partition_key) DO UPDATE SET last_seq = last_seq + 1
                    """,
                    (partition_key,),
                )
                seq_row = conn.execute(
                    "SELECT last_seq FROM sequences WHERE partition_key = ?",
                    (partition_key,),
                ).fetchone()
                next_seq = int(seq_row["last_seq"])

                # 4. Durable Write
                conn.execute(
                    """
                    INSERT INTO records (partition_key, seq, idempotency_key, record_type, payload, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (partition_key, next_seq, idempotency_key, record_type, payload_str, now),
                )
                conn.execute("COMMIT;")

                record = Record(
                    partition_key=partition_key,
                    seq=next_seq,
                    idempotency_key=idempotency_key,
                    record_type=record_type,
                    payload=payload,
                    created_at=now,
                )

                # コミット成功後に非同期イベントバスへ投入（Write-Before-ACK）
                try:
                    self._event_queue.put_nowait(record)
                except queue.Full:
                    # Backpressure: キュー溢れ時は購読者側のCursor Syncにフォールバックさせる
                    pass

                return WriteAck(status="committed", shard_id=shard_id, record=record)
            except Exception:
                conn.execute("ROLLBACK;")
                raise

    # ------------------------------------------------------------------
    # 5. Internal Event Bus & Asynchronous Projection
    # ------------------------------------------------------------------
    def subscribe(self, partition_pattern: str, callback: Subscriber) -> None:
        """特定の partition_key または '*' (全イベント) を購読"""
        with self._subscribers_lock:
            self._subscribers[partition_pattern].append(callback)

    def _event_dispatch_loop(self) -> None:
        while True:
            record = self._event_queue.get()
            if record is None:
                break
            try:
                # 1) 非同期で検索用Projection DB (FTS5) を更新
                self._update_search_projection(record)

                # 2) アプリケーション層の購読モジュールへFan-out配信
                with self._subscribers_lock:
                    targets = list(self._subscribers.get(record.partition_key, []))
                    targets.extend(self._subscribers.get("*", []))

                for cb in targets:
                    try:
                        cb(record)
                    except Exception:
                        # 購読者のエラーでディスパッチャ全体を止めない
                        pass
            finally:
                self._event_queue.task_done()

    def _update_search_projection(self, record: Record) -> None:
        searchable_text = " ".join(str(v) for v in record.payload.values())
        with self._connect(self.projection_path) as conn:
            conn.execute(
                """
                INSERT INTO records_fts (partition_key, seq, record_type, content)
                VALUES (?, ?, ?, ?)
                """,
                (record.partition_key, record.seq, record.record_type, searchable_text),
            )

    def search(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        """Projection DB (FTS5) を使った高速全文検索"""
        with self._connect(self.projection_path) as conn:
            rows = conn.execute(
                """
                SELECT partition_key, seq, record_type, content
                FROM records_fts
                WHERE records_fts MATCH ?
                ORDER BY rank LIMIT ?
                """,
                (query, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # 6. Ephemeral State Store (ディスクを書かない高頻度ステート管理)
    # ------------------------------------------------------------------
    def set_ephemeral(self, key: str, value: Any, ttl_sec: float = 15.0) -> None:
        with self._ephemeral_lock:
            self._ephemeral_store[key] = (value, time.monotonic() + ttl_sec)

    def get_ephemeral(self, key: str) -> Any | None:
        now = time.monotonic()
        with self._ephemeral_lock:
            item = self._ephemeral_store.get(key)
            if not item:
                return None
            val, expires_at = item
            if now > expires_at:
                del self._ephemeral_store[key]
                return None
            return val

    # ------------------------------------------------------------------
    # 7. Cursor-Based Sync Engine (複数シャード横断の差分取得)
    # ------------------------------------------------------------------
    def sync(self, cursors: dict[str, int], limit_per_partition: int = 100) -> dict[str, list[Record]]:
        shard_batches: dict[str, list[tuple[str, int]]] = defaultdict(list)
        for p_key, last_seq in cursors.items():
            shard_id = self.resolve_shard(p_key)
            shard_batches[shard_id].append((p_key, last_seq))

        synced: dict[str, list[Record]] = {}
        for shard_id, items in shard_batches.items():
            db_path = self._shard_path(shard_id)
            with self._connect(db_path) as conn:
                for p_key, last_seq in items:
                    rows = conn.execute(
                        """
                        SELECT partition_key, seq, idempotency_key, record_type, payload, created_at
                        FROM records
                        WHERE partition_key = ? AND seq > ?
                        ORDER BY seq ASC
                        LIMIT ?
                        """,
                        (p_key, last_seq, limit_per_partition),
                    ).fetchall()
                    if rows:
                        synced[p_key] = [
                            Record(
                                partition_key=str(r["partition_key"]),
                                seq=int(r["seq"]),
                                idempotency_key=str(r["idempotency_key"]),
                                record_type=str(r["record_type"]),
                                payload=json.loads(r["payload"]),
                                created_at=float(r["created_at"]),
                            )
                            for r in rows
                        ]
        return synced

    def commit_cursor(self, consumer_id: str, partition_key: str, seq: int) -> None:
        with self._connect(self.meta_path) as conn:
            conn.execute(
                """
                INSERT INTO cursors (consumer_id, partition_key, last_seq, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(consumer_id, partition_key) DO UPDATE SET
                    last_seq = MAX(cursors.last_seq, excluded.last_seq),
                    updated_at = excluded.updated_at
                """,
                (consumer_id, partition_key, seq, time.time()),
            )

    def close(self) -> None:
        self._event_queue.put(None)
        self._worker.join(timeout=2.0)
```

---

## 4. 汎用基盤として実用化する際の設計上の勘所

- **モジュール間の疎結合化（プラグイン構造）**
  本体モジュールは `db.append("order:usdjpy", "tick_signal", {...}, uuid)` のようにイベントを書き込むだけにし、別の集計モジュールや通知モジュールは `db.subscribe("order:usdjpy", handler)` で購読する形に揃えると、機能追加時に既存コードのトランザクションを汚さずに済む。
- **ホットパーティションの隔離（テナント分離）**
  `resolve_shard(partition_key, dedicated_shard="shard_heavy")` のように、`meta.db` にルーティング表を持たせておくことで、特定のパーティションだけデータ量や書き込み頻度が突出した場合に、そのパーティションだけを別ファイル（または将来の外部DBエンジン）へ無停止で逃がす余地を残せる。
- **スナップショット（チェックポイント）の導入**
  汎用イベントログ基盤として長期運用すると、`seq=0` からの再生コストが高くなる。`record_type="__snapshot__"` として現在の集約状態を定期保存する規約をAPI層に設けておけば、`sync` 時に最新スナップショット以降の差分だけを読み込む高速リカバリが可能になる。
- **プロセス境界の切り出し（IPC / RPC化）**
  最初は同一プロセス内のPythonクラス（インプロセスAPI）として組み込み、複数プロセスや別言語モジュールから同時に呼びたくなった段階で、この `UniversalStorageAPI` の外側に薄い Unix Domain Socket / HTTP+SSE ラッパーを被せることで、ローカルデータサーバーへ昇格できる。

---

## 5. 未決事項

- 想定用途（複数モジュールが協調するローカルアプリ、ストリーミングデータの収集・分析基盤、別言語からのIPC利用など）が未確定である。用途に応じてBD/DSでの方針を決める。
