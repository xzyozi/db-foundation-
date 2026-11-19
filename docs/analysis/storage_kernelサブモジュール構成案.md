---
title: "storage_kernel サブモジュール構成案（Ports & Adapters）"
document_type: "analysis"
document_role: "rationale"
ssot_scope: "system_architecture（構想段階の判断材料）"
status: "draft"
version: "0.2"
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

| ディレクトリ     | 責務                                                                                                                                                                         | 関知しないこと（境界）                                                            |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| **`contracts/`** | 基盤と親がやり取りする最小のデータ構造（`partition_key`, `seq`, `idempotency_key`, `schema_version`, `payload: T`）と例外体系を定義する。                                    | ドメイン固有のフィールド、DBのカラム型、SQLエラーコード。                         |
| **`ports/`**     | 親プロジェクトが実装すべき抽象インターフェース（Protocol / Trait）を定義する。基盤側が必要とする **原子性（トランザクション）と順序の要件** をメソッド契約として明文化する。 | 実際のストレージ製品（SQLite / PostgreSQL / DuckDB / ファイル等）やテーブル設計。 |
| **`core/`**      | 冪等性制御、Write-Before-ACK、シャード横断のScatter-Gather同期、Projectionの欠落リカバリ、DLQ、揮発TTL管理などの **制御フロー** を担う。                                     | 物理DBへの接続方法、SQL文、ネットワークプロトコルの詳細。                         |
| **`runtime/`**   | シャード単位の排他制御（Writerロック）、定期メンテナンスのフック、終了時のグレースフルドレインを管理する。                                                                   | OS固有のデーモン管理や親アプリのイベントループ詳細。                              |
| **`testing/`**   | 親プロジェクトが独自DAOを実装した際、「重複排除が壊れていないか」「連番が飛んだり逆転しないか」を自動検証する契約テスト（Contract Test）を提供する。                         | 親プロジェクト固有のビジネス要件テスト。                                          |

---

## 3. DAOを関知しないための最重要インターフェース設計（`ports/dao.py`）

DAOを親側へ完全に逃がす際、最も事故が起きやすいのは **トランザクション境界** である。「重複チェック → 連番採番 → 書き込み」は同一シャード内で原子的（Atomic）に行う必要があるため、基盤側は **Unit of Work（トランザクション・コンテキスト）** または **アトミックな単一プリミティブ** を Port として要求する。

以下は v1 の定義案である。決定の根拠は Issue #1（前提・スコープ）、#2（整合性・耐久性）、#3（API・Port 契約）を参照する。v1 に含めないもの（パーティション移動、`migrating` 状態、`BlobStorePort`、`PayloadUpcaster`）は 3.6 にまとめる。

### 3.1 `contracts/envelope.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Literal, Protocol, TypeVar

T = TypeVar("T")

DurabilityLevel = Literal["none", "process_crash", "power_loss"]  # #2 B1
PartitionStatus = Literal["active", "read_only"]  # #3 C5（v2 で "migrating" を追加）
DropPolicy = Literal["drop_newest", "drop_oldest", "disconnect"]  # #3 C3
ProjectionFailurePolicy = Literal["dlq_and_skip", "pause_partition"]  # #3 C3


@dataclass(frozen=True)
class PartitionInfo:
    partition_key: str
    shard_id: str
    status: PartitionStatus
    created_at: float


@dataclass(frozen=True)
class RecordEnvelope(Generic[T]):
    partition_key: str
    seq: int
    idempotency_key: str
    record_type: str
    schema_version: int
    payload_hash: str  # core/ingestion が算出（#2 B5）
    payload: T
    created_at: float


@dataclass(frozen=True)
class WriteAck(Generic[T]):
    status: Literal["committed", "duplicate"]
    shard_id: str
    envelope: RecordEnvelope[T]


@dataclass(frozen=True)
class DeadLetterRecord(Generic[T]):
    consumer_id: str
    envelope: RecordEnvelope[T]
    error_type: str
    error_message: str
    attempts: int
    failed_at: float


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    initial_delay_sec: float = 0.1
    backoff_multiplier: float = 2.0
    max_delay_sec: float = 5.0


@dataclass(frozen=True)
class ShutdownReport:
    completed_cleanly: bool
    dropped_subscriber_events: int
    pending_projection_partitions: tuple[str, ...]
    elapsed_sec: float


class SubscriptionHandle(Protocol):
    """subscribe() が返す購読管理ハンドル。基盤が実装し、親は利用するだけ"""

    @property
    def subscription_id(self) -> str: ...

    @property
    def is_active(self) -> bool: ...

    @property
    def dropped_count(self) -> int: ...

    def unsubscribe(self) -> None: ...
```

### 3.2 `contracts/errors.py`

```python
from __future__ import annotations

from typing import Literal


class StorageKernelError(Exception):
    """storage_kernel 基盤の基底例外"""


class KernelClosedError(StorageKernelError):
    """close() 開始後に書き込みなどが呼ばれた（#3 C4）"""


class PayloadTooLarge(StorageKernelError):
    """encode 後のペイロードが上限（64 KB）を超えた（#1 A3）"""

    def __init__(self, size: int, limit: int) -> None:
        super().__init__(f"payload size {size} exceeds limit {limit}")
        self.size = size
        self.limit = limit


class DuplicateKeyError(StorageKernelError):
    """DAO が DB 固有の一意制約違反を変換して送出する標準例外（#2 B5）"""

    def __init__(
        self,
        message: str,
        *,
        kind: Literal["sequence", "idempotency_key", "unknown"] = "unknown",
    ) -> None:
        super().__init__(message)
        self.kind = kind


class IdempotencyConflict(StorageKernelError):
    """同じ (partition_key, idempotency_key) で payload_hash が一致しない（#2 B5）"""

    def __init__(
        self,
        partition_key: str,
        idempotency_key: str,
        existing_hash: str,
        incoming_hash: str,
    ) -> None:
        super().__init__(
            f"Idempotency conflict on ({partition_key}, {idempotency_key}): "
            f"existing={existing_hash} incoming={incoming_hash}"
        )
        self.partition_key = partition_key
        self.idempotency_key = idempotency_key
        self.existing_hash = existing_hash
        self.incoming_hash = incoming_hash


class PartitionReadOnlyError(StorageKernelError):
    """status='read_only' のパーティションへ書き込んだ（#3 C5）"""


class PartitionStateConflictError(StorageKernelError):
    """set_partition_status の expected_status が現在の状態と一致しない（#3 C5）"""
```

### 3.3 `ports/dao.py`

```python
from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Protocol, Sequence, TypeVar

from ..contracts.envelope import (
    DeadLetterRecord,
    DurabilityLevel,
    PartitionInfo,
    PartitionStatus,
    RecordEnvelope,
)

T = TypeVar("T")


class ShardTxPort(Protocol[T]):
    """単一シャード内の排他書き込みトランザクションの契約"""

    def find_by_idempotency(
        self, partition_key: str, idempotency_key: str
    ) -> RecordEnvelope[T] | None:
        """payload_hash を含む完全な RecordEnvelope を返す"""
        ...

    def next_sequence(self, partition_key: str) -> int:
        """パーティション単位の単調増加 seq を採番する（初回は 1）"""
        ...

    def insert_record(self, envelope: RecordEnvelope[T]) -> None:
        """
        レコードを永続化する。
        (partition_key, seq) / (partition_key, idempotency_key) の一意制約違反は、
        DB 固有の例外を漏らさず DuplicateKeyError に変換して送出する。
        """
        ...


class ShardDAOPort(Protocol[T]):
    """シャードストレージの契約"""

    @property
    def durability(self) -> DurabilityLevel:
        """自己申告の耐久性レベル（参照実装は 'power_loss'、In-Memory は 'none'）"""
        ...

    def write_transaction(self, shard_id: str) -> AbstractContextManager[ShardTxPort[T]]:
        """
        正常終了時に COMMIT、例外時に ROLLBACK する。
        正常に抜けたことは、宣言した durability の水準で永続化が確定したことを意味する。
        """
        ...

    def fetch_range(
        self,
        shard_id: str,
        partition_key: str,
        after_seq: int,
        limit: int,
    ) -> Sequence[RecordEnvelope[T]]:
        """seq > after_seq を厳密な昇順で、欠落なく返す"""
        ...


class MetaDAOPort(Protocol):
    """パーティション台帳とカーソル管理の契約"""

    @property
    def durability(self) -> DurabilityLevel: ...

    def get_partition(self, partition_key: str) -> PartitionInfo | None: ...

    def bind_shard(self, partition_key: str, shard_id: str) -> PartitionInfo:
        """
        未登録なら status='active' で登録する。
        登録済みなら上書きせず、既存の PartitionInfo を返す（冪等かつ不変。#2 B4）。
        """
        ...

    def set_partition_status(
        self,
        partition_key: str,
        status: PartitionStatus,
        *,
        expected_status: PartitionStatus | None = None,
    ) -> PartitionInfo:
        """
        状態を遷移させる。
        expected_status を指定し、現在の状態と異なれば PartitionStateConflictError を送出する。
        """
        ...

    def list_partitions(self, *, status: PartitionStatus | None = None) -> Sequence[PartitionInfo]:
        """登録済みパーティションの一覧（ProjectionRunner の起動時キャッチアップに使う）"""
        ...

    def get_cursor(self, consumer_id: str, partition_key: str) -> int:
        """未記録なら 0 を返す"""
        ...

    def advance_cursor(self, consumer_id: str, partition_key: str, seq: int) -> None:
        """単調非減少（MAX(current, seq)）で更新する。後退させてはならない"""
        ...


class ProjectionPort(Protocol[T]):
    """取りこぼしてはいけない非同期投影（At-Least-Once）の契約"""

    @property
    def consumer_id(self) -> str: ...

    def apply_batch(self, records: Sequence[RecordEnvelope[T]]) -> None:
        """
        (partition_key, seq) に対して冪等であること。
        失敗時は例外を送出し、基盤側の Retry / DLQ 制御に委ねる。
        """
        ...


class DeadLetterPort(Protocol[T]):
    """Projection のリトライ上限を超えたレコード（Poison Pill）を隔離する任意の契約"""

    def put_dead_letter(self, record: DeadLetterRecord[T]) -> None: ...

    def list_dead_letters(
        self, *, consumer_id: str | None = None, limit: int = 100
    ) -> Sequence[DeadLetterRecord[T]]: ...
```

### 3.4 `ports/codec.py`・`ports/router.py`

```python
# --- ports/codec.py ---
from __future__ import annotations

from typing import Protocol, TypeVar

T = TypeVar("T")


class PayloadCodec(Protocol[T]):
    """
    encode は決定論的であること（論理的に等価な値から常に同じバイト列を返す。#2 B5）。
    core/ingestion はこの出力から payload_hash を算出し、サイズ上限（#1 A3）も検査する。
    """

    def encode(self, value: T) -> bytes: ...

    def decode(self, data: bytes) -> T: ...


# --- ports/router.py ---
class PartitionRouterPort(Protocol):
    """新規パーティションの格納先シャードを決める純粋なポリシー（移動は扱わない。#3 C1）"""

    def select_shard(self, partition_key: str, *, preferred_shard: str | None = None) -> str: ...
```

`ports/transport.py` は、将来の IPC 配信口（#1 A1・A2）のために予約し、v1 では定義しない。

### 3.5 `StorageKernel` の公開 API（`core/kernel.py`、v1）

基盤が実装し、親プロジェクトが呼ぶメソッドの形状である（#3 C3・C4・C5）。

| メソッド                                                                                                                                 | 概要                                                                                                                  |
| ---------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `append(partition_key, record_type, payload, idempotency_key, *, schema_version) -> WriteAck[T]`                                         | 状態確認（`read_only` なら拒否）→ サイズ検査 → 冪等性 → 採番 → 書き込み                                               |
| `subscribe(pattern, callback, *, max_queue_size=256, drop_policy="drop_newest", on_error=None) -> SubscriptionHandle`                    | at-most-once の通知。購読者ごとの有界キュー                                                                           |
| `register_projection(projection, *, partition_patterns=("*",), batch_size=100, retry_policy=RetryPolicy(), failure_policy=None) -> None` | at-least-once の投影。`failure_policy` 省略時は、`DeadLetterPort` があれば `dlq_and_skip`、なければ `pause_partition` |
| `resume_projection(consumer_id, partition_key, *, skip_current=False) -> None`                                                           | `pause_partition` で止めた投影を再開する。`skip_current=True` で原因レコードを飛ばす                                  |
| `set_partition_status(partition_key, status, *, expected_status=None) -> PartitionInfo`                                                  | `MetaDAOPort` へ委譲し、ルーティングキャッシュを更新する                                                              |
| `sync(cursors, *, limit_per_partition=100)`                                                                                              | カーソルからの差分取得                                                                                                |
| `close(*, timeout=5.0, drain_projections=False, drain_subscribers=False) -> ShutdownReport`                                              | 新規書き込みを拒否し、進行中の書き込みを待ってから停止する                                                            |

### 3.6 v2 以降に回すもの

| 項目                                                                           | 理由                                                                                 | 決定           |
| ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ | -------------- |
| パーティション移動（`migrating` 状態、`Migratable*Port`、`migrate_partition`） | コピー検証、ルーティングキャッシュ無効化、CAS による参照切り替えを一体で設計するため | #3 C1          |
| `BlobStorePort`、`RecordEnvelope.blob_refs`                                    | v1 は 64 KB 超を `PayloadTooLarge` で拒否する                                        | #1 A3          |
| `PayloadUpcaster`                                                              | スキーマ移行の論点                                                                   | #4 D3 で決める |

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
