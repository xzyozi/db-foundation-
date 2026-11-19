---
title: "storage_kernel サブモジュール構成案（Ports & Adapters）"
document_type: "analysis"
document_role: "rationale"
ssot_scope: "system_architecture（構想段階の判断材料）"
status: "draft"
version: "0.1"
created_at: "2026-11-19"
updated_at: "2026-11-19"
author: "xzyozi"
owner: "xzyozi"
purpose: "DAOを親プロジェクトへ委譲し、基盤をGitサブモジュールとして組み込める独立カーネルにする構成案を記録するため"
related_documents:
  - "汎用データアクセスAPI層の構想（相対パス: ./汎用データアクセスAPI層の構想.md）"
  - "ドキュメントガバナンス（相対パス: ../README.md）"
---

# storage_kernel サブモジュール構成案（Ports & Adapters）

DAO（テーブル構造や具体的なSQL・DBドライバー）を親プロジェクト側へ完全に委譲し、基盤側を **Gitサブモジュールとしてそのまま組み込める独立カーネル** にする場合、Ports & Adapters（ヘキサゴナルアーキテクチャ）を採用して「基盤コア」と「DAO差し替え口（Ports）」を明確に分離する。

基盤側はレコードの中身（ドメイン構造）には一切触れず、メタデータ付きの **`Envelope[T]`（封筒）** としてのみ扱うのが設計の核になる。

> 本文書は構想段階の分析資料であり、現在の仕様の正本ではない。採用が決まった時点で、基本設計書（BD）・IF/API仕様書（IF）などの正本へ反映する。
> 機能面の構想（7つの汎用機能、スキーマ例）は [汎用データアクセスAPI層の構想](./汎用データアクセスAPI層の構想.md) を参照する。

---

## 1. サブモジュール・ディレクトリ構成

サブモジュール単体で完結し、外部依存ゼロ（または最小）で動く構成とする（例：`storage_kernel/`）。

```text
storage_kernel/                  # Gitサブモジュールルート
├── pyproject.toml               # パッケージ定義（依存ゼロを基本）
├── src/
│   └── storage_kernel/
│       ├── __init__.py          # 公開Facade・主要型・Portsのみをre-export
│       │
│       ├── contracts/           # 【不変の契約型】DAOの中身に依存しない共通DTO・例外
│       │   ├── envelope.py      # RecordEnvelope[T], WriteAck, SyncBatch, Cursor
│       │   ├── errors.py        # 基盤標準例外 (IdempotencyConflict, BackpressureError等)
│       │   └── telemetry.py     # メトリクス・トレーシングイベント型
│       │
│       ├── ports/               # 【SPI / 差し替え口】親プロジェクト(DAO側)が実装するIF群
│       │   ├── dao.py           # ShardDAOPort, MetaDAOPort, ProjectionPort, BlobStorePort
│       │   ├── codec.py         # PayloadCodec (シリアライズ) & Upcaster (schema_version変換)
│       │   ├── router.py        # PartitionRouterPort (動的ルーティング・テナント隔離)
│       │   ├── guard.py         # AccessPolicyPort (ACL/名前空間検査), RateLimiterPort
│       │   └── transport.py     # DeliveryPort (In-Process / IPC / WS / SSE の配信口)
│       │
│       ├── core/                # 【基盤エンジン本体】分散システム設計パターンの純粋実装
│       │   ├── kernel.py        # StorageKernel (外部から叩く統合Facade)
│       │   ├── ingestion.py     # WritePipeline (認証→制限→冪等性→採番→Write-Before-ACK)
│       │   ├── ordering.py      # SequenceCoordinator (パーティション単位連番・ギャップ検知)
│       │   ├── event_bus.py     # EventDispatcher (非同期Fan-out・Slow Consumer切り離し)
│       │   ├── sync_engine.py   # CursorSyncEngine (Scatter-Gather横断差分同期)
│       │   ├── projection.py    # ProjectionRunner (カーソル追跡付き非同期投影・自動復旧)
│       │   ├── ephemeral.py     # EphemeralTTLStore (Heartbeat等・揮発データのメモリ隔離)
│       │   ├── blob_offload.py  # BlobOffloadInterceptor (閾値超過データの自動外部退避)
│       │   └── reliability.py   # RetryPolicy & DeadLetterCoordinator (DLQ隔離・再投入)
│       │
│       ├── runtime/             # 【実行制御・運用ライフサイクル】
│       │   ├── concurrency.py   # ShardLockManager (シャード毎のWriter直列化・Reader並行制御)
│       │   ├── maintenance.py   # MaintenanceHooks (Retention / Snapshot / Checkpoint 調停)
│       │   └── lifecycle.py     # GracefulShutdown (進行中Write完了・キュー排出制御)
│       │
│       └── testing/             # 【親プロジェクト向け検証キット】
│           ├── memory_ports.py  # DB不要で即動くIn-Memory実装群（親の単体テスト用）
│           ├── contract_test.py # 親が作ったDAOが基盤の不変条件を満たすか検証するテスト基底
│           └── fault_inject.py  # 重複送信・遅延・クラッシュを再現するテストハーネス
│
└── tests/                       # サブモジュール単体のユニットテスト
```

---

## 2. 各層の責務と境界ルール

依存の向きは常に次のとおり固定する。

- `親プロジェクト (DAO実装・業務ロジック)` → `ports/`・`contracts/`
- `core/`・`runtime/` → `ports/`・`contracts/`

`core/`・`runtime/` は親プロジェクトに依存せず、親プロジェクトも `core/`・`runtime/` の内部に依存しない（公開Facadeのみ利用する）。

| ディレクトリ | 責務 | 関知しないこと（境界） |
| --- | --- | --- |
| **`contracts/`** | 基盤と親がやり取りする最小のデータ構造（`partition_key`, `seq`, `idempotency_key`, `schema_version`, `payload: T`）と例外体系を定義する。 | ドメイン固有のフィールド、DBのカラム型、SQLエラーコード。 |
| **`ports/`** | 親プロジェクトが実装すべき抽象インターフェース（Protocol / Trait）を定義する。基盤側が必要とする **原子性（トランザクション）と順序の要件** をメソッド契約として明文化する。 | 実際のストレージ製品（SQLite / PostgreSQL / DuckDB / ファイル等）やテーブル設計。 |
| **`core/`** | 冪等性制御、Write-Before-ACK、シャード横断のScatter-Gather同期、Projectionの欠落リカバリ、DLQ、揮発TTL管理などの **制御フロー** を担う。 | 物理DBへの接続方法、SQL文、ネットワークプロトコルの詳細。 |
| **`runtime/`** | シャード単位の排他制御（Writerロック）、定期メンテナンスのフック、終了時のグレースフルドレインを管理する。 | OS固有のデーモン管理や親アプリのイベントループ詳細。 |
| **`testing/`** | 親プロジェクトが独自DAOを実装した際、「重複排除が壊れていないか」「連番が飛んだり逆転しないか」を自動検証する契約テスト（Contract Test）を提供する。 | 親プロジェクト固有のビジネス要件テスト。 |

---

## 3. DAOを関知しないための最重要インターフェース設計（`ports/dao.py`）

DAOを親側へ完全に逃がす際、最も事故が起きやすいのは **トランザクション境界** である。「重複チェック → 連番採番 → 書き込み」は同一シャード内で原子的（Atomic）に行う必要があるため、基盤側は **Unit of Work（トランザクション・コンテキスト）** または **アトミックな単一プリミティブ** を Port として要求する。

```python
from __future__ import annotations
from contextlib import AbstractContextManager
from typing import Generic, Protocol, Sequence, TypeVar

from ..contracts.envelope import RecordEnvelope, WriteCommitResult

T = TypeVar("T")


class ShardTxPort(Protocol[T]):
    """単一シャード内のトランザクションで親側DAOが提供すべき最小操作"""

    def find_by_idempotency(
        self, partition_key: str, idempotency_key: str
    ) -> RecordEnvelope[T] | None: ...

    def next_sequence(self, partition_key: str) -> int: ...

    def insert_record(self, envelope: RecordEnvelope[T]) -> None: ...


class ShardDAOPort(Protocol[T]):
    """親プロジェクトが要件に合わせて実装するシャードDAOインターフェース"""

    def write_transaction(
        self, shard_id: str
    ) -> AbstractContextManager[ShardTxPort[T]]:
        """対象シャードの排他書き込みトランザクションを開始・COMMIT/ROLLBACKする"""
        ...

    def fetch_range(
        self,
        shard_id: str,
        partition_key: str,
        after_seq: int,
        limit: int,
    ) -> Sequence[RecordEnvelope[T]]:
        """seq > after_seq を昇順で取得（Cursor Sync / Projection復旧で使用）"""
        ...


class MetaDAOPort(Protocol):
    """パーティション→シャード対応表と、Consumerカーソル管理のDAOインターフェース"""

    def get_shard_id(self, partition_key: str) -> str | None: ...
    def bind_shard(self, partition_key: str, shard_id: str) -> str: ...
    def get_cursor(self, consumer_id: str, partition_key: str) -> int: ...
    def advance_cursor(self, consumer_id: str, partition_key: str, seq: int) -> None: ...


class ProjectionPort(Protocol[T]):
    """検索インデックスや集計ビューなど、親側が自由に増設できる非同期プロジェクション口"""

    @property
    def consumer_id(self) -> str: ...

    def apply_batch(self, records: Sequence[RecordEnvelope[T]]) -> None:
        """バッチ適用（失敗時は例外を送出し、基盤側のRetry/DLQに委ねる）"""
        ...
```

この契約にしておくことで、親プロジェクト側は以下の自由度を得られる。

- あるプロジェクトでは `ShardDAOPort` の裏側を **SQLiteのJSONカラム** にし、別のプロジェクトでは **PostgreSQLの専用テーブル（業務カラム展開）** として実装できる。
- `ProjectionPort` を複数登録するだけで、FTS5全文検索・集計サマリー・外部通知などを `core/projection.py` のカーソル復旧機構（クラッシュ耐性＋DLQ付き）に乗せられる。

---

## 4. 親プロジェクト側との接続構成（サブモジュール運用イメージ）

親リポジトリでは `vendor/` または `libs/` 配下にサブモジュールとして配置し、薄い「Adapter（DAO実装）」と「Composition Root（DI組み立て）」だけを書く。

```text
my_app/                          # 親プロジェクト
├── libs/
│   └── storage_kernel/          # ← Gitサブモジュール（編集しない・タグ固定）
└── src/
    └── my_app/
        ├── domain/              # 業務モデル (Order, ChatMessage, TelemetryEvent等)
        ├── adapters/            # サブモジュールの ports/ を満たす要件固有実装
        │   ├── sqlite_shard_dao.py    # 要件に合わせたテーブル構造・SQLを実装
        │   ├── sqlite_meta_dao.py     # カーソル・シャード台帳の実装
        │   ├── fts_projection.py      # 要件固有の検索投影
        │   └── local_blob_store.py    # 大容量ファイル退避先
        ├── bootstrap.py         # AdapterをStorageKernelにDIして組み立てる
        └── tests/
            └── test_my_dao_contract.py  # サブモジュールのcontract_testを継承して実行
```

### サブモジュール化を成功させる3つの運用規約

1. **Contract Test の継承を必須にする**
   `storage_kernel.testing.contract_test.ShardDAOContractTest` を親側のテストで継承させ、自作DAOを渡して `pytest` を回すだけで「冪等性の保証」「連番の単調性」「トランザクションのロールバック」「範囲スキャンの昇順性」を自動保証する。
2. **基盤側の設定はすべてDI（コンストラクタ注入）で渡す**
   サブモジュール内部で環境変数や設定ファイルを直接読ませない。`KernelConfig`（キュー上限、リトライ回数、BLOB退避閾値、TTL等）を親の `bootstrap.py` から明示的に注入する。
3. **破壊的変更を防ぐセマンティックバージョニング**
   サブモジュールは `ports/` と `contracts/` の変更だけを Major バージョンアップ対象とし、`core/` や `runtime/` のバグ修正・性能改善は親側が `git submodule update` だけで取り込める状態を維持する。
