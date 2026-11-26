---
title: "IF/API仕様書"
document_type: "interface_specification"
document_number: "DBF-IF-001"
document_role: "normative"
ssot_scope: "external_interface"
status: "approved"
version: "1.4"
created_at: "2026-11-19"
updated_at: "2026-11-20"
author: "xzyozi"
owner: "xzyozi"
purpose: "親プロジェクトと storage_kernel のあいだで合意する外部インターフェース契約（公開API、Port/SPI、契約型・標準例外、互換性、および13項目のContract Test要件）を定義し、境界を越える受け渡しの不整合を防ぐため"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "用語集（相対パス: ../../CONTEXT.md）"
  - "DBF-BD-001 基本設計書（相対パス: ./DBF-BD-001_基本設計書.md）"
  - "DBF-ENV-001 環境・開発基盤仕様書（相対パス: ../setup/DBF-ENV-001_環境・開発基盤仕様書.md）"
  - "DBF-DS-001 データ構造仕様書（相対パス: ./DBF-DS-001_データ構造仕様書.md）"
  - "DBF-DD-001 書き込み経路詳細設計書（相対パス: ./DBF-DD-001_書き込み経路詳細設計書.md）"
  - "DBF-DD-002 配信とProjection詳細設計書（相対パス: ./DBF-DD-002_配信とProjection詳細設計書.md）"
  - "DBF-DD-003 同期と版管理詳細設計書（相対パス: ./DBF-DD-003_同期と版管理詳細設計書.md）"
  - "DBF-DD-004 実行制御詳細設計書（相対パス: ./DBF-DD-004_実行制御詳細設計書.md）"
---

# IF/API仕様書（外部インターフェース契約）
**storage_kernel 公開 API・Port（SPI）・契約型および Contract Test 仕様（文書番号: DBF-IF-001）**

---

## 1. 概要とSSOT境界

### 1.1 対象インターフェースの目的

- **提供者 / 利用者**:
  - **公開 API（`IF-001`〜`IF-010`）および契約型（`contracts/`）**: `storage_kernel` が提供し、親プロジェクト（別リポジトリのアプリケーション本体）が呼び出す。
  - **Port / SPI（`IF-101`〜`IF-109`、`ports/`）**: `storage_kernel` が Protocol 契約を定義して呼び出し、親プロジェクト（または参照実装 `contrib/sqlite/`・テスト実装 `testing/memory_ports.py`）が実装して DI で注入する。
- **連携の目的**: 親プロジェクト側のドメイン構造や物理 DB エンジン（SQLite / PostgreSQL / DuckDB 等）に一切依存せず、冪等書き込み、パーティション連番採番、Write-Before-ACK、2レーン配信（`subscribe` / `Projection`）、横断差分同期（`sync`）、スキーマ版移行（Upcast）、およびメンテナンス整合点制御を安全に連携するため。
- **対象範囲**:
  - `StorageKernel` が公開する 10 メソッドの契約（引数、戻り値、事前・事後条件、例外）。
  - 親プロジェクトが実装する 9 つの Port（`ShardDAOPort`, `ShardTxPort`, `MetaDAOPort`, `ProjectionPort`, `DeadLetterPort`, `PayloadCodec`, `PayloadUpcaster`, `PartitionRouterPort`, `MaintenanceHooks`）のメソッド契約と不変条件。
  - 境界を越えて受け渡す契約データ型（`RecordEnvelope[T]`, `WriteAck[T]`, `PartitionInfo`, `KernelConfig`, `RetryPolicy`, `DeadLetterRecord[T]`, `ShutdownReport`, `SubscriptionHandle`）および標準例外体系（`contracts/errors.py`）。
  - 親プロジェクトの DAO / Codec / Projection 実装が満たすべき 13 項目の外部契約テスト要件（`TR-IF-001`〜`TR-IF-013`）。
  - セマンティックバージョニング方針と将来予約済みの拡張口。
- **対象外**:
  - `core/`・`runtime/` 内部の制御フロー、スレッド・ロック管理、内部キュー・Dirty Set 構造（詳細設計書 `DBF-DD-001`〜`004` の正本）。
  - ストレージ上の論理・物理保存スキーマ、原子書き込み、バックアップ対象（データ構造仕様書 `DBF-DS-001` の正本）。
  - v2 以降に先送りした `append_batch`、パーティション移動（`Migratable*Port`）、大容量退避（`BlobStorePort`）、揮発ストア（`ephemeral`）、アクセス制御（`guard`）、IPC 配信口（`ports/transport.py`）の具象定義。

### 1.2 単一責任範囲 (SSOT) と文書境界

- **本書の正本範囲**: `external_interface`。親プロジェクトと `storage_kernel` サブモジュールの境界を越えて受け渡す公開 API、Port（SPI）、契約型、例外、互換性、および外部契約テスト要件の正本とする。
- **DDとの関係**: 内部のモジュール間 DTO、制御フロー、状態遷移は詳細設計書（`DBF-DD-001`〜`004`）を正本とする。本書で定義する各インターフェース（`IF-xxx`）は、各 DD の対象機能一覧（`F-xxx`）と 1 対 1 または多対 1 で対応付ける。
- **DSとの関係**: 保存後の論理スキーマと永続化不変条件はデータ構造仕様書（`DBF-DS-001`）を正本とする。親プロジェクトから受け取った入力（受信形式：本書）と、DAO が永続化する構造（保存形式：`DBF-DS-001`）を明確に区別する。
- **BDとの関係**: システム全体のアーキテクチャ境界と非機能方針は基本設計書（[`DBF-BD-001`](./DBF-BD-001_基本設計書.md)）を正本とし、本書はそれを接続契約へ具体化する。
- **他文書との関係**: 環境・品質ゲートは [`DBF-ENV-001`](../setup/DBF-ENV-001_環境・開発基盤仕様書.md)、設計判断理由は `ADR-002`〜`ADR-005` を正本とし、本書の契約を上書きしない。
- **責務の対象外**: 具象ソースコード（Python の `class` 構文等）の直接貼付は行わず、すべてメソッド・フィールド契約表で定義する。
- **矛盾時の扱い**: 文書の更新日ではなく、該当する `ssot_scope` の正本を更新し、関連文書を追従させる。

---

## 2. インターフェース共通仕様

### 2.1 インターフェース一覧

#### A. 公開 API（`StorageKernel` が提供し、親プロジェクトが呼び出す）

| IF-ID  | インターフェース名                   | 方向 | 方式                                               | 相手先                               | 対応するDD機能ID                                | 状態       |
| :----- | :----------------------------------- | :--- | :------------------------------------------------- | :----------------------------------- | :---------------------------------------------- | :--------- |
| IF-001 | `StorageKernel.append`               | 提供 | インプロセス関数呼び出し（同期）                   | 親プロジェクト業務モジュール         | `DBF-DD-001` (`F-101`)                          | 計画（v1） |
| IF-002 | `StorageKernel.write_snapshot`       | 提供 | インプロセス関数呼び出し（同期）                   | 親プロジェクト業務モジュール         | `DBF-DD-001` (`F-102`)                          | 計画（v1） |
| IF-003 | `StorageKernel.subscribe`            | 提供 | インプロセス関数呼び出し（非同期コールバック登録） | 親プロジェクト購読モジュール         | `DBF-DD-002` (`F-201`)                          | 計画（v1） |
| IF-004 | `StorageKernel.register_projection`  | 提供 | インプロセス関数呼び出し（非同期ワーカー登録）     | 親プロジェクト Composition Root      | `DBF-DD-002` (`F-202`)                          | 計画（v1） |
| IF-005 | `StorageKernel.resume_projection`    | 提供 | インプロセス関数呼び出し（同期）                   | 親プロジェクト運用・管理機能         | `DBF-DD-002` (`F-203`)                          | 計画（v1） |
| IF-006 | `StorageKernel.set_partition_status` | 提供 | インプロセス関数呼び出し（同期）                   | 親プロジェクト運用・管理機能         | `DBF-DD-001` (`F-103`) / `DBF-DD-004` (`F-401`) | 計画（v1） |
| IF-007 | `StorageKernel.enter_maintenance`    | 提供 | インプロセス関数呼び出し（同期）                   | 親プロジェクト運用・バックアップ機能 | `DBF-DD-004` (`F-402`)                          | 計画（v1） |
| IF-008 | `StorageKernel.exit_maintenance`     | 提供 | インプロセス関数呼び出し（同期）                   | 親プロジェクト運用・バックアップ機能 | `DBF-DD-004` (`F-403`)                          | 計画（v1） |
| IF-009 | `StorageKernel.sync`                 | 提供 | インプロセス関数呼び出し（同期）                   | 親プロジェクト同期クライアント       | `DBF-DD-003` (`F-301`)                          | 計画（v1） |
| IF-010 | `StorageKernel.close`                | 提供 | インプロセス関数呼び出し（同期）                   | 親プロジェクト Lifecycle 管理        | `DBF-DD-004` (`F-404`)                          | 計画（v1） |

#### B. Port / SPI（`storage_kernel` が呼び出し、親プロジェクトが実装する）

| IF-ID  | インターフェース名    | 方向              | 方式                                             | 相手先                                    | 対応するDD機能ID                                                                           | 状態       |
| :----- | :-------------------- | :---------------- | :----------------------------------------------- | :---------------------------------------- | :----------------------------------------------------------------------------------------- | :--------- |
| IF-101 | `ShardDAOPort[T]`     | 利用（SPI）       | Protocol メソッド呼び出し                        | 親プロジェクト Shard DAO Adapter          | `DBF-DD-001` (`F-101`, `F-102`) / `DBF-DD-002` (`F-202`) / `DBF-DD-003` (`F-301`)          | 計画（v1） |
| IF-102 | `ShardTxPort[T]`      | 利用（SPI）       | トランザクションコンテキスト内 Protocol 呼び出し | 親プロジェクト Shard DAO トランザクション | `DBF-DD-001` (`F-101`, `F-102`)                                                            | 計画（v1） |
| IF-103 | `MetaDAOPort`         | 利用（SPI）       | Protocol メソッド呼び出し                        | 親プロジェクト Meta DAO Adapter           | `DBF-DD-001` (`F-101`〜`F-103`) / `DBF-DD-002` (`F-202`, `F-203`) / `DBF-DD-003` (`F-301`) | 計画（v1） |
| IF-104 | `ProjectionPort[T]`   | 利用（SPI）       | 非同期ワーカーからの Protocol 呼び出し           | 親プロジェクト Projection Adapter         | `DBF-DD-002` (`F-202`)                                                                     | 計画（v1） |
| IF-105 | `DeadLetterPort[T]`   | 利用（SPI・任意） | 非同期ワーカーからの Protocol 呼び出し           | 親プロジェクト DLQ Adapter                | `DBF-DD-002` (`F-202`)                                                                     | 計画（v1） |
| IF-106 | `PayloadCodec[T]`     | 利用（SPI）       | Protocol メソッド呼び出し                        | 親プロジェクト Codec Adapter              | `DBF-DD-001` (`F-101`, `F-102`) / `DBF-DD-003` (`F-302`)                                   | 計画（v1） |
| IF-107 | `PayloadUpcaster`     | 利用（SPI・任意） | Protocol メソッド呼び出し                        | 親プロジェクト Upcaster Adapter           | `DBF-DD-003` (`F-302`)                                                                     | 計画（v1） |
| IF-108 | `PartitionRouterPort` | 利用（SPI）       | Protocol メソッド呼び出し                        | 親プロジェクト Router Adapter             | `DBF-DD-001` (`F-101`, `F-102`)                                                            | 計画（v1） |
| IF-109 | `MaintenanceHooks`    | 利用（SPI・任意） | メンテナンス遷移時のフック呼び出し               | 親プロジェクト Maintenance Adapter        | `DBF-DD-004` (`F-402`, `F-403`)                                                            | 計画（v1） |

### 2.2 共通ルール

- **プロトコル・トランスポート**: 同一 Python プロセス内の関数・メソッド呼び出し（`typing.Protocol` およびジェネリクス `TypeVar("T")` による静的型契約）。
- **データ形式・文字コード**:
  - 識別子文字列（`partition_key`, `idempotency_key`, `record_type`, `shard_id`, `consumer_id`）はすべて UTF-8 非空文字列とする。
  - ペイロードのシリアライズ形式は `PayloadCodec[T]` に委譲し、バイト列（`bytes`）としてサイズ検査・ハッシュ算出・Upcast を行う。
- **時刻・タイムゾーン**:
  - `created_at` および `failed_at` はすべて UTC エポック秒（`float`、`time.time()` 相当）で表現する。
  - タイムアウトおよびバックオフ待機時間の計測には単調増加クロック（`time.monotonic()`）を使用する。
- **識別子・相関ID**:
  - 書き込み操作の追跡・重複排除には呼び出し元が生成する `idempotency_key`（UUID 等の文字列）を使用し、スコープは `(partition_key, idempotency_key)` とする。
  - 確定後のレコード識別には `(partition_key, seq)`（`seq` は 1 始まりのパーティション内単調増加整数）を使用する。
  - 購読解除・ドロップ数確認には `SubscriptionHandle.subscription_id`、投影進捗管理には `ProjectionPort.consumer_id` を使用する。
- **タイムアウト・再試行**:
  - `append` / `write_snapshot` は同一 `(partition_key, idempotency_key, payload_hash)` での再試行に対して完全に冪等であり、`status="duplicate"` の `WriteAck[T]` を返す。
  - `ProjectionPort.apply_batch` は `RetryPolicy` に基づき自動再試行されるため、`(partition_key, seq)` に対して冪等であることを必須とする。
  - `enter_maintenance(timeout=30.0)` および `close(timeout=5.0)` は秒単位の `float` で最大待機時間を指定する。
- **利用制限**:
  - **ペイロードサイズ上限**: `PayloadCodec.encode(payload)` 後のバイト長が `KernelConfig.max_payload_bytes`（既定 64 KB = 65,536 bytes）を超えた場合、`PayloadTooLarge` を送出して拒否する（スナップショットにも適用）。
  - **システム予約種別**: `"__"`（アンダースコア2文字）で始まる `record_type` はシステム予約（例: `SNAPSHOT_RECORD_TYPE = "__snapshot__"`）とし、通常の `append()` に渡された場合は `ReservedRecordTypeError` で拒否する。
  - **購読パターン文法**: `subscribe` および `register_projection` で指定できるパターンは、①完全一致（例: `"chat:general"`）、②全件一致（`"*"`）、③末尾ワイルドカード前方一致（例: `"chat:*"`、`*` は末尾1か所のみ）の3種類に限定する。

### 2.3 共通契約データ型（`contracts/envelope.py`）

境界を越えて共有される不変データクラス（`frozen=True`）およびハンドル Protocol のフィールド仕様を下表に定義する。

| 型名                      | フィールド / プロパティ         | データ型                                              | 必須性 | NULL可 | 制約・意味                                                                                                                        |
| :------------------------ | :------------------------------ | :---------------------------------------------------- | :----: | :----: | :-------------------------------------------------------------------------------------------------------------------------------- |
| `DurabilityLevel`         | （リテラル型）                  | `Literal["none", "process_crash", "power_loss"]`      |  必須  |  不可  | DAO が保証する永続化水準。既定および SQLite 参照実装は `"power_loss"`、In-Memory は `"none"`                                      |
| `PartitionStatus`         | （リテラル型）                  | `Literal["active", "read_only"]`                      |  必須  |  不可  | パーティションの状態。v1 は2値のみ（v2 で `"migrating"` を追加予定）                                                              |
| `DropPolicy`              | （リテラル型）                  | `Literal["drop_newest", "drop_oldest", "disconnect"]` |  必須  |  不可  | `subscribe` の有界キュー満杯時の破棄ポリシー                                                                                      |
| `ProjectionFailurePolicy` | （リテラル型）                  | `Literal["dlq_and_skip", "pause_partition"]`          |  必須  |  不可  | Projection リトライ上限超過後（Poison Pill 特定後）の処置ポリシー                                                                 |
| `RetryPolicy`             | `max_attempts`                  | `int`                                                 |  任意  |  不可  | 最大試行回数（既定値 `3`、`>= 1`）                                                                                                |
| `RetryPolicy`             | `initial_delay_sec`             | `float`                                               |  任意  |  不可  | 初回リトライ待機秒数（既定値 `0.1`、`>= 0.0`）                                                                                    |
| `RetryPolicy`             | `backoff_multiplier`            | `float`                                               |  任意  |  不可  | 指数バックオフ倍率（既定値 `2.0`、`>= 1.0`）                                                                                      |
| `RetryPolicy`             | `max_delay_sec`                 | `float`                                               |  任意  |  不可  | 最大リトライ待機秒数（既定値 `5.0`、`>= initial_delay_sec`）                                                                      |
| `PartitionInfo`           | `partition_key`                 | `str`                                                 |  必須  |  不可  | パーティション識別キー（非空文字列）                                                                                              |
| `PartitionInfo`           | `shard_id`                      | `str`                                                 |  必須  |  不可  | 割り当てられたシャード識別子（v1 では一度確定したら不変）                                                                         |
| `PartitionInfo`           | `status`                        | `PartitionStatus`                                     |  必須  |  不可  | `"active"` または `"read_only"`                                                                                                   |
| `PartitionInfo`           | `created_at`                    | `float`                                               |  必須  |  不可  | バインド登録日時（UTC エポック秒）                                                                                                |
| `RecordEnvelope[T]`       | `partition_key`                 | `str`                                                 |  必須  |  不可  | パーティション識別キー                                                                                                            |
| `RecordEnvelope[T]`       | `seq`                           | `int`                                                 |  必須  |  不可  | パーティション内の単調増加連番（`>= 1`、欠番なし）                                                                                |
| `RecordEnvelope[T]`       | `idempotency_key`               | `str`                                                 |  必須  |  不可  | 呼び出し元指定の冪等キー                                                                                                          |
| `RecordEnvelope[T]`       | `record_type`                   | `str`                                                 |  必須  |  不可  | レコード種別（スナップショット時は `"__snapshot__"`）                                                                             |
| `RecordEnvelope[T]`       | `schema_version`                | `int`                                                 |  必須  |  不可  | ペイロードのスキーマ版番号（`>= 1`）                                                                                              |
| `RecordEnvelope[T]`       | `payload_hash`                  | `str`                                                 |  必須  |  不可  | 保存時の元データ（`record_type`, `schema_version`, エンコード後バイト列）から算出した SHA-256 16進文字列。Upcast 後も再計算しない |
| `RecordEnvelope[T]`       | `payload`                       | `T`                                                   |  必須  |   可   | ドメインペイロード本体                                                                                                            |
| `RecordEnvelope[T]`       | `created_at`                    | `float`                                               |  必須  |  不可  | レコード作成日時（UTC エポック秒）                                                                                                |
| `RecordEnvelope[T]`       | `covers_through_seq`            | `int \| None`                                         |  任意  |   可   | `record_type == "__snapshot__"` のときのみ必須（`1 <= covers_through_seq <= 最新seq`）。通常レコードでは `None`                   |
| `KernelConfig`            | `schema_registry`               | `dict[str, int]`                                      |  必須  |  不可  | `record_type` ごとの最新 `schema_version` マップ                                                                                  |
| `KernelConfig`            | `max_payload_bytes`             | `int`                                                 |  任意  |  不可  | エンコード後ペイロードの最大バイト数（既定値 `65536` = 64 KB）                                                                    |
| `KernelConfig`            | `default_retry_policy`          | `RetryPolicy`                                         |  任意  |  不可  | Projection の既定リトライポリシー（既定値 `RetryPolicy()`）                                                                       |
| `WriteAck[T]`             | `status`                        | `Literal["committed", "duplicate"]`                   |  必須  |  不可  | 新規書き込み確定時は `"committed"`、冪等再送ヒット時は `"duplicate"`                                                              |
| `WriteAck[T]`             | `shard_id`                      | `str`                                                 |  必須  |  不可  | 保存先シャード識別子                                                                                                              |
| `WriteAck[T]`             | `envelope`                      | `RecordEnvelope[T]`                                   |  必須  |  不可  | 確定または既存ヒットしたレコード封筒                                                                                              |
| `DeadLetterRecord[T]`     | `consumer_id`                   | `str`                                                 |  必須  |  不可  | 失敗した Projection の `consumer_id`                                                                                              |
| `DeadLetterRecord[T]`     | `envelope`                      | `RecordEnvelope[T]`                                   |  必須  |  不可  | 隔離された原因レコード（Poison Pill）                                                                                             |
| `DeadLetterRecord[T]`     | `error_type`                    | `str`                                                 |  必須  |  不可  | 発生した例外のクラス名                                                                                                            |
| `DeadLetterRecord[T]`     | `error_message`                 | `str`                                                 |  必須  |  不可  | 例外メッセージ文字列                                                                                                              |
| `DeadLetterRecord[T]`     | `attempts`                      | `int`                                                 |  必須  |  不可  | 実行した試行回数                                                                                                                  |
| `DeadLetterRecord[T]`     | `failed_at`                     | `float`                                               |  必須  |  不可  | DLQ 隔離確定日時（UTC エポック秒）                                                                                                |
| `ShutdownReport`          | `completed_cleanly`             | `bool`                                                |  必須  |  不可  | タイムアウト内に全指定ドレインが完了した場合 `True`                                                                               |
| `ShutdownReport`          | `dropped_subscriber_events`     | `int`                                                 |  必須  |  不可  | 停止時に破棄された `subscribe` キュー内イベント総数                                                                               |
| `ShutdownReport`          | `pending_projection_partitions` | `tuple[str, ...]`                                     |  必須  |  不可  | 停止時点で未反映差分が残っているパーティションキーのタプル                                                                        |
| `ShutdownReport`          | `elapsed_sec`                   | `float`                                               |  必須  |  不可  | `close()` 処理に要した経過秒数                                                                                                    |
| `SubscriptionHandle`      | `subscription_id`               | `str`                                                 |  必須  |  不可  | 購読識別子（読み取り専用プロパティ）                                                                                              |
| `SubscriptionHandle`      | `is_active`                     | `bool`                                                |  必須  |  不可  | 購読が有効な間 `True`、`unsubscribe()` または `disconnect` 後は `False`                                                           |
| `SubscriptionHandle`      | `dropped_count`                 | `int`                                                 |  必須  |  不可  | キュー満杯により破棄された累積イベント件数                                                                                        |
| `SubscriptionHandle`      | `unsubscribe()`                 | `None`                                                |  必須  |   -    | 購読を解除し、以降の新規イベント投入を停止するメソッド（冪等）                                                                    |

### 2.4 標準例外体系（`contracts/errors.py`）

すべての基盤標準例外は `StorageKernelError(Exception)` を継承する。

| 例外クラス名                  | 保持属性                                                                                 | 発生条件                                                                                                                                                | 再試行可否                                                           |
| :---------------------------- | :--------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------ | :------------------------------------------------------------------- |
| `StorageKernelError`          | `args`                                                                                   | `storage_kernel` が送出する全標準例外の基底クラス                                                                                                       | 子クラスによる                                                       |
| `KernelClosedError`           | なし                                                                                     | `close()` 開始後に書き込み（`append`, `write_snapshot`）や状態変更が呼ばれた場合                                                                        | 不可（再起動が必要）                                                 |
| `KernelMaintenanceError`      | なし                                                                                     | メンテナンスモード（`enter_maintenance`〜`exit_maintenance`）中に書き込み、パーティション作成、状態変更、カーソル更新が呼ばれた場合                     | 条件付き（`exit_maintenance` 後に再試行可）                          |
| `PayloadTooLarge`             | `size: int`, `limit: int`                                                                | `PayloadCodec.encode(payload)` のバイト長 `size` が `limit`（既定 64 KB）を超えた場合                                                                   | 不可（ペイロード縮小が必要）                                         |
| `ReservedRecordTypeError`     | なし                                                                                     | 通常の `append()` に `"__"` で始まる予約 `record_type`（例: `"__snapshot__"`）を渡した場合                                                              | 不可（`write_snapshot` を使うか名称変更）                            |
| `DuplicateKeyError`           | `kind: Literal["sequence", "idempotency_key", "unknown"]`                                | `ShardTxPort.insert_record` で `(partition_key, seq)` または `(partition_key, idempotency_key)` の一意制約違反が発生した際に DAO が変換して送出する例外 | 条件付き（`idempotency_key` 競合時は基盤内部で再照会・ハンドリング） |
| `IdempotencyConflict`         | `partition_key: str`, `idempotency_key: str`, `existing_hash: str`, `incoming_hash: str` | 同じ `(partition_key, idempotency_key)` の既存レコードと `payload_hash` が一致しない場合                                                                | 不可（新しい `idempotency_key` の採番が必要）                        |
| `PartitionReadOnlyError`      | なし                                                                                     | `status == "read_only"` のパーティションに対して `append` または `write_snapshot` を呼んだ場合                                                          | 条件付き（`"active"` へ戻した後にのみ可）                            |
| `PartitionStateConflictError` | なし                                                                                     | `set_partition_status` の `expected_status` が現在の状態と一致しない場合                                                                                | 条件付き（最新状態を確認して再実行）                                 |
| `CursorExpiredError`          | `partition_key: str`, `cursor_seq: int`, `oldest_retained_seq: int`                      | 保持範囲より古いカーソルで `sync` / `fetch_range` を呼んだ場合（※v1 では Retention を行わないため送出されない。v2 互換のため契約のみ先行定義）          | 条件付き（最新の `__snapshot__` から再開する）                       |
| `SchemaVersionError`          | なし                                                                                     | 書き込み時に `schema_registry` の最新版と一致しない、または読み取り時に未知の新しい `schema_version` や Upcast 変換経路の欠落を検出した場合             | 不可（`schema_registry` または `PayloadUpcaster` の修正が必要）      |

### 2.5 コンストラクタ引数（`StorageKernel(...)`）

`StorageKernel` は、すべてキーワード専用引数で組み立てる（`Composition Root` が DAO などを注入する）。

| 引数                | データ型                    | 必須性 | 既定値  | 用途                                                                                                                                                                                                                                |
| :------------------ | :-------------------------- | :----: | :------ | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `config`            | `KernelConfig`              |  必須  | なし    | `schema_registry`、`max_payload_bytes`、`default_retry_policy` を保持する設定                                                                                                                                                       |
| `shard_dao`         | `ShardDAOPort[Any]`         |  必須  | なし    | シャード DB へのレコード永続化                                                                                                                                                                                                      |
| `meta_dao`          | `MetaDAOPort`               |  必須  | なし    | パーティション台帳・カーソルの永続化                                                                                                                                                                                                |
| `codec`             | `PayloadCodec[T]`           |  必須  | なし    | ペイロードと `bytes` の相互変換                                                                                                                                                                                                     |
| `router`            | `PartitionRouterPort`       |  必須  | なし    | `partition_key` からシャードへのルーティング                                                                                                                                                                                        |
| `upcaster`          | `PayloadUpcaster \| None`   |  任意  | `None`  | 旧版レコードの読み取り時 Upcast                                                                                                                                                                                                     |
| `dead_letter_port`  | `DeadLetterPort[T] \| None` |  任意  | `None`  | Poison Pill の DLQ 隔離（`failure_policy="dlq_and_skip"` の前提）                                                                                                                                                                   |
| `maintenance_hooks` | `MaintenanceHooks \| None`  |  任意  | `None`  | メンテナンス移行・解除のフック                                                                                                                                                                                                      |
| `telemetry_sink`    | `TelemetrySink \| None`     |  任意  | `None`  | テレメトリの出力先                                                                                                                                                                                                                  |
| `store_raw_bytes`   | `bool`                      |  任意  | `False` | `True` のとき、`codec.encode` 後の `bytes` を `ShardDAOPort` へ保存し（`RecordEnvelope.payload` に `bytes` が入る）、読み出し時に `codec.decode` で `T` へ戻す。`False` のときは `T` のオブジェクトをそのまま `ShardDAOPort` へ渡す |

`store_raw_bytes=True` は、ペイロードを `bytes` として保存する DAO（例: `codec=None` で組み立てた SQLite シャード DAO のように、自前でシリアライズせずに `bytes` を BLOB 列へ書く実装）と組み合わせる。DAO がオブジェクトを自分で直列化できる場合は `False`（既定）のままでよい。保存形式と DAO の型が食い違うと読み出し時にデコードできないため、`Composition Root` で DAO と同時に決める。

#### 2.5.1 ペイロードの型・`codec`・`store_raw_bytes` の組み合わせ

ペイロードには、次の 3 つが関わる。

- **基盤の `codec`（`StorageKernel` の引数）**: ペイロード `T` を `bytes` に変換する。サイズ検査（`PayloadTooLarge`）と `payload_hash` の計算に、**必ず**使われる。
- **`store_raw_bytes`（`StorageKernel` の引数）**: DAO へ何を渡すかを決める。`True` なら `codec.encode` 後の `bytes`、`False` なら `T` のオブジェクトそのもの。
- **DAO 側の `codec`（DAO のコンストラクタ引数。参照実装 `SqliteShardDAO` の `codec`）**: DAO が `T` を保存するときに、自分で `bytes` へ直列化するために持つ。基盤の `codec` とは別物である。

実際に使われている組み合わせは、次の 3 つである。

|   #   | `store_raw_bytes` | DAO が受け取るもの                        | DAO の実装例                                                     | 向いている場合                                                                                     | 使われている場所                                                    |
| :---: | :---------------- | :---------------------------------------- | :--------------------------------------------------------------- | :------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------ |
|   1   | `True`            | `bytes`（基盤が `codec.encode` した結果） | `SqliteShardDAO[bytes](..., codec=None)`                         | ペイロードを BLOB として、そのまま保存したい。直列化を基盤の `codec` に一本化したい                | `examples/parent_app`、`benchmarks/`、障害注入のテスト              |
|   2   | `False`（既定）   | `T`（ドメインオブジェクト）               | `InMemoryShardDAO`、`SqliteShardDAO(..., codec=<DAO側の codec>)` | DAO が、`T` を自分で直列化できる。あるいは、メモリ上で保持するだけでよい                           | 単体テスト、Contract Test（In-Memory・SQLite）                      |
|   3   | `False`           | `T`（ドメインオブジェクト）               | `ColumnarShardDAO`（`examples/columnar_dao`）                    | ペイロードを**業務カラムへ展開**して保存したい。検索や集計を、ペイロードを全件読み出さずに行いたい | `examples/columnar_dao`、`tests/contract/test_columnar_contract.py` |

選び方の目安は、次のとおりである。

- **まず 1 を検討する**: 保存の形が、基盤の `codec` の出力（`bytes`）と同じになり、直列化の実装が 1 か所で済む。
- **業務カラムへ展開したい、DAO が `T` の構造に依存する場合は 3**: `store_raw_bytes=False` にして、DAO が `T` を直接受け取る。この自由度は、`ColumnarShardDAO` で、Contract Test 13 項目が通ることを確認している。
- **2 は、DAO が `T` を自前で直列化する場合**: ただし、この場合、直列化が**2 回**行われる（基盤の `codec` で、サイズ検査とハッシュのために 1 回、DAO 側で保存のために 1 回）。DAO 側の `codec` が、基盤の `codec` と異なる出力を返しても、基盤は検出しない。保存の形を DAO が決めたいときだけ選ぶ。

注意点:

- **組み合わせの誤りは、基盤では検出できない**。`store_raw_bytes=True` のとき、基盤は、DAO から読み出した `RecordEnvelope.payload` が `bytes` の場合にだけ `codec.decode` を呼ぶ（それ以外は、そのまま返す）。`True` なのに DAO が `bytes` 以外を返す、`False` なのに DAO が `bytes` しか扱えない、といった食い違いは、`Composition Root` で DAO と同時に決める。
- **Upcast は、どの組み合わせでも動く**。旧版のレコードを読むとき、DAO が `bytes` を返せばそのまま、`T` を返す場合（`store_raw_bytes=False`）は、基盤が `codec.encode` で `bytes` へ変換し直してから Upcast する（`DBF-DD-003`）。
- **`durability` とは独立**である。どの組み合わせでも、DAO が `durability`（`DurabilityLevel`）を宣言する（ADR-004）。
- ペイロードを `bytes` に一本化する案（`store_raw_bytes` の廃止）は、DAO が業務カラムへ展開する自由度（上の 3）を狭めるため、v1 では採らない（Issue #54 の判断。案 A の現状維持）。

---

## 3. 認証・認可

| 項目             | 内容                                                                                                                                                                                                                                                                                           |
| :--------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 認証方式         | なし（同一プロセス内の Python メソッド呼び出しのため。外部クライアント認証は親プロジェクトの境界で実施する）                                                                                                                                                                                   |
| 認可（権限）単位 | v1 ではカーネル内の ACL・名前空間制限（`ports/guard.py`）は対象外とする（[`ADR-005`](../adr/ADR-005_v1スコープの限定と将来拡張に向けた前方互換契約の先行定義.md)）。予約種別（`"__"` 始まり）の書き込み禁止とパーティション状態（`read_only`）・メンテナンスモードによる操作制限のみを強制する |
| 資格情報の管理   | カーネル自身は DB 接続文字列やパスワードを保持・関知しない。親プロジェクトの `bootstrap.py` が DAO インスタンスを生成して注入する                                                                                                                                                              |
| 権限不足時の応答 | 予約種別違反時は `ReservedRecordTypeError`、読み取り専用パーティションへの書き込み時は `PartitionReadOnlyError`、メンテナンス中の更新時は `KernelMaintenanceError` を送出する                                                                                                                  |

---

## 4. インターフェース別契約

### 4.1 契約（IF-ID: IF-001 `StorageKernel.append`）

- **概要**: 指定パーティションへ通常レコードを冪等に追記し、単調増加連番 `seq` を採番してシャードへ永続化（Write-Before-ACK）した後に、非同期で Event Bus と Projection へ通知する。
- **呼び出し方式**: `kernel.append(partition_key, record_type, payload, idempotency_key, *, schema_version, preferred_shard=None) -> WriteAck[T]`
- **冪等性**: あり。同一 `(partition_key, idempotency_key)` かつ同一 `payload_hash` の再送時は、`seq` を進めず既存の `RecordEnvelope[T]` を `WriteAck(status="duplicate")` として返す（Event Bus への再配信は行わない）。
- **事前条件 / 事後条件**:
  - **事前条件**: カーネルが `close()` されておらず、メンテナンスモード中でもないこと。`record_type` が `"__"` で始まらないこと。`schema_version` が `KernelConfig.schema_registry[record_type]` と一致すること。
  - **事後条件**: `MetaDAOPort.bind_shard` が先に確定し、対象シャードの `write_transaction` が宣言された `durability` 水準で COMMIT された後に `WriteAck[T]` が返る。

#### 4.1.1 リクエスト / 引数

| パラメータ        | データ型      |         必須性         | NULL可 | バリデーション・制約                                                                             | 例・備考                                 |
| :---------------- | :------------ | :--------------------: | :----: | :----------------------------------------------------------------------------------------------- | :--------------------------------------- |
| `partition_key`   | `str`         |          必須          |  不可  | 非空文字列                                                                                       | `"order:usdjpy"`                         |
| `record_type`     | `str`         |          必須          |  不可  | 非空文字列。`"__"` で始まらないこと。`schema_registry` に登録済みであること                      | `"tick_signal"`                          |
| `payload`         | `T`           |          必須          |   可   | `PayloadCodec.encode(payload)` 後のサイズが `max_payload_bytes`（64 KB）以下であること           | ドメインオブジェクトまたは dict          |
| `idempotency_key` | `str`         |          必須          |  不可  | 非空文字列（UUID 等）                                                                            | `"550e8400-e29b-41d4-a716-446655440000"` |
| `schema_version`  | `int`         | 必須（キーワード専用） |  不可  | `KernelConfig.schema_registry[record_type]` と一致する正の整数                                   | `1`                                      |
| `preferred_shard` | `str \| None` | 任意（キーワード専用） |   可   | 未登録パーティションの初回 `bind_shard` 時にのみ `PartitionRouterPort.select_shard` へ渡すヒント | `"shard_heavy"`（既定値 `None`）         |

#### 4.1.2 レスポンス / 戻り値

| フィールド     | データ型      | 必須性 | NULL可 | 意味・取り得る値                                                                                              |
| :------------- | :------------ | :----: | :----: | :------------------------------------------------------------------------------------------------------------ |
| （戻り値全体） | `WriteAck[T]` |  必須  |  不可  | `status`（`"committed"` または `"duplicate"`）、`shard_id`、`envelope`（`covers_through_seq=None`）を保持する |

#### 4.1.3 エラー応答

| エラーコード / 例外型     | 発生条件                                                                         | 応答形式 | 呼び出し側の対応                                                          | 再試行可否 |
| :------------------------ | :------------------------------------------------------------------------------- | :------- | :------------------------------------------------------------------------ | :--------- |
| `KernelClosedError`       | `close()` 開始後に呼び出された                                                   | 例外送出 | 書き込みを中止する                                                        | 不可       |
| `KernelMaintenanceError`  | メンテナンスモード中に呼び出された                                               | 例外送出 | `exit_maintenance()` 完了を待って同じ `idempotency_key` で再試行する      | 条件付き   |
| `ReservedRecordTypeError` | `record_type` が `"__"` で始まる                                                 | 例外送出 | `write_snapshot` を使うか `record_type` を修正する                        | 不可       |
| `SchemaVersionError`      | `record_type` が未登録、または `schema_version` が最新版と不一致                 | 例外送出 | `schema_registry` または送信側の版番号を揃える                            | 不可       |
| `PayloadTooLarge`         | エンコード後バイト数が `max_payload_bytes`（64 KB）を超過                        | 例外送出 | ペイロードを分割または縮小する                                            | 不可       |
| `PartitionReadOnlyError`  | 対象パーティションの `status` が `"read_only"`                                   | 例外送出 | 書き込み不可として処理するか、`set_partition_status` で `"active"` に戻す | 条件付き   |
| `IdempotencyConflict`     | 同じ `(partition_key, idempotency_key)` で `payload_hash` が既存レコードと異なる | 例外送出 | 冪等キーの使い回しバグを修正し、新規 `idempotency_key` を発行する         | 不可       |

---

### 4.2 契約（IF-ID: IF-002 `StorageKernel.write_snapshot`）

- **概要**: 親プロジェクトが生成したパーティション集約状態を、予約種別 `record_type="__snapshot__"` として冪等に書き込む。
- **呼び出し方式**: `kernel.write_snapshot(partition_key, payload, idempotency_key, *, covers_through_seq, schema_version) -> WriteAck[T]`
- **冪等性**: あり。`append` と同様に `(partition_key, idempotency_key)` と `payload_hash` で重複排除する。
- **事前条件 / 事後条件**:
  - **事前条件**: `covers_through_seq` が `1` 以上かつ書き込み時点の対象パーティションの最終 `seq` 以下であること。`schema_registry["__snapshot__"]`（または対象スナップショット版）と `schema_version` が一致すること。サイズ上限 64 KB 以下であること。
  - **事後条件**: `record_type="__snapshot__"`、`covers_through_seq` 設定済みの `RecordEnvelope[T]` が永続化される。通常の `subscribe` および `include_snapshots=False` の Projection には配信せず、`include_snapshots=True` の Projection にのみ配信する。

#### 4.2.1 リクエスト / 引数

| パラメータ           | データ型 |         必須性         | NULL可 | バリデーション・制約                                         | 例・備考                   |
| :------------------- | :------- | :--------------------: | :----: | :----------------------------------------------------------- | :------------------------- |
| `partition_key`      | `str`    |          必須          |  不可  | 非空文字列（既に1件以上のレコードが存在するパーティション）  | `"order:usdjpy"`           |
| `payload`            | `T`      |          必須          |   可   | エンコード後 64 KB 以下                                      | スナップショットデータ     |
| `idempotency_key`    | `str`    |          必須          |  不可  | 非空文字列                                                   | `"snap-order-usdjpy-1000"` |
| `covers_through_seq` | `int`    | 必須（キーワード専用） |  不可  | `1 <= covers_through_seq <= 現在の最終 seq`                  | `1000`                     |
| `schema_version`     | `int`    | 必須（キーワード専用） |  不可  | `schema_registry["__snapshot__"]` の最新版と一致する正の整数 | `1`                        |

#### 4.2.2 レスポンス / 戻り値

| フィールド     | データ型      | 必須性 | NULL可 | 意味・取り得る値                                                                                                   |
| :------------- | :------------ | :----: | :----: | :----------------------------------------------------------------------------------------------------------------- |
| （戻り値全体） | `WriteAck[T]` |  必須  |  不可  | `envelope.record_type == "__snapshot__"` かつ `envelope.covers_through_seq == covers_through_seq` の `WriteAck[T]` |

#### 4.2.3 エラー応答

| エラーコード / 例外型                                                                                                                        | 発生条件                                                         | 応答形式 | 呼び出し側の対応                    | 再試行可否      |
| :------------------------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------- | :------- | :---------------------------------- | :-------------- |
| `ValueError`                                                                                                                                 | `covers_through_seq < 1` または現在の最終 `seq` を超えている場合 | 例外送出 | 確定済みの `seq` 以下の値を指定する | 不可            |
| `KernelClosedError` / `KernelMaintenanceError` / `SchemaVersionError` / `PayloadTooLarge` / `PartitionReadOnlyError` / `IdempotencyConflict` | `IF-001`（`append`）と同一条件                                   | 例外送出 | `IF-001` と同じ                     | `IF-001` と同じ |

---

### 4.3 契約（IF-ID: IF-003 `StorageKernel.subscribe`）

- **概要**: 指定パターンに一致するパーティションの書き込み確定イベントを、At-Most-Once（取りこぼし許容）で非同期受信するコールバックを登録する。
- **呼び出し方式**: `kernel.subscribe(pattern, callback, *, max_queue_size=256, drop_policy="drop_newest", on_error=None) -> SubscriptionHandle`
- **冪等性**: なし（呼び出しごとに新しい `subscription_id` を持つ独立した有界キューと購読ハンドルを生成する）。
- **事前条件 / 事後条件**:
  - **事前条件**: `pattern` が許可された3パターン（完全一致、`*`、末尾 `:*`）のいずれかであること。`max_queue_size >= 1` であること。
  - **メンテナンス中の扱い**: `subscribe` は永続状態（レコード・カーソル・台帳）を変えないため、メンテナンス中（`enter_maintenance`〜`exit_maintenance`）も登録できる。`KernelMaintenanceError` は送出しない。`close()` 開始後は `KernelClosedError` で拒否する。
  - **事後条件**: 購読者専用の有界キューが割り当てられ、`SubscriptionHandle` が返る。`__snapshot__` レコードは配信されない。コールバック内で発生した例外は他の購読者や Projection を止めず、ログ・telemetry・`on_error` に通知される。

#### 4.3.1 リクエスト / 引数

| パラメータ       | データ型                                                 |         必須性         | NULL可 | バリデーション・制約                                                   | 例・備考        |
| :--------------- | :------------------------------------------------------- | :--------------------: | :----: | :--------------------------------------------------------------------- | :-------------- |
| `pattern`        | `str`                                                    |          必須          |  不可  | 完全一致、`*`、または末尾ワイルドカード（例: `"chat:*"`）              | `"order:*"`     |
| `callback`       | `Callable[[RecordEnvelope[T]], None]`                    |          必須          |  不可  | 1レコードを受け取る関数                                                | 購読ハンドラ    |
| `max_queue_size` | `int`                                                    | 任意（キーワード専用） |  不可  | `>= 1`（既定値 `256`）                                                 | `256`           |
| `drop_policy`    | `DropPolicy`                                             | 任意（キーワード専用） |  不可  | `"drop_newest"`（既定）、`"drop_oldest"`、`"disconnect"` のいずれか    | `"drop_newest"` |
| `on_error`       | `Callable[[RecordEnvelope[T], Exception], None] \| None` | 任意（キーワード専用） |   可   | `callback` が例外を送出した際に呼ばれるエラーハンドラ（既定値 `None`） | `None`          |

#### 4.3.2 レスポンス / 戻り値

| フィールド     | データ型             | 必須性 | NULL可 | 意味・取り得る値                                                                    |
| :------------- | :------------------- | :----: | :----: | :---------------------------------------------------------------------------------- |
| （戻り値全体） | `SubscriptionHandle` |  必須  |  不可  | `subscription_id`, `is_active`, `dropped_count`, `unsubscribe()` を提供するハンドル |

#### 4.3.3 エラー応答

| エラーコード / 例外型 | 発生条件                                                             | 応答形式 | 呼び出し側の対応 | 再試行可否 |
| :-------------------- | :------------------------------------------------------------------- | :------- | :--------------- | :--------- |
| `ValueError`          | `pattern` の文法が不正（途中の `*` 等）、または `max_queue_size < 1` | 例外送出 | 引数を修正する   | 不可       |
| `KernelClosedError`   | `close()` 開始後に呼び出された                                       | 例外送出 | 登録を中止する   | 不可       |

---

### 4.4 契約（IF-ID: IF-004 `StorageKernel.register_projection` / IF-005 `StorageKernel.resume_projection`）

- **概要**:
  - `IF-004` (`register_projection`): 取りこぼしが許されない非同期投影（`ProjectionPort[T]`）を登録し、独立した `ProjectionRunner` ワーカーを起動する（At-Least-Once 保証）。
  - `IF-005` (`resume_projection`): `failure_policy="pause_partition"` により一時停止したパーティションの投影を再開する。
- **呼び出し方式**:
  - `kernel.register_projection(projection, *, partition_patterns=("*",), batch_size=100, retry_policy=RetryPolicy(), failure_policy=None, include_snapshots=False) -> None`
  - `kernel.resume_projection(consumer_id, partition_key, *, skip_current=False) -> None`
- **冪等性**:
  - `register_projection`: 同じ `consumer_id` の重複登録は `ValueError` で拒否する。
  - `resume_projection`: 条件付き冪等（既に稼働中のパーティションに対する `skip_current=False` 呼び出しは何もしない）。
- **事前条件 / 事後条件**:
  - `failure_policy` 省略時（`None`）は、`StorageKernel` に `DeadLetterPort` が注入されていれば `"dlq_and_skip"`、注入されていなければ `"pause_partition"` が自動選択される。
  - `register_projection` は Projection がカーソルを更新するため、メンテナンス中は `KernelMaintenanceError` で拒否する（カーソル更新を遮断する `IF-007` の趣旨に沿う）。
  - `resume_projection`: `skip_current=True` のとき、パーティション状態の確認とカーソル更新を書き込みガードの中で行い、進行中の更新として `enter_maintenance` に待たれる。`skip_current=False` はカーソルを更新しないため、メンテナンス中でも呼べる。どちらも `close()` 開始後は `KernelClosedError` になる。
  - `include_snapshots=False`（既定）のとき `__snapshot__` は `apply_batch` から除外されカーソルのみ通過する。`include_snapshots=True` のときのみ `apply_batch` に渡される。
  - `resume_projection(..., skip_current=True)` の場合、停止原因となった先頭1件（Poison Pill）の `seq` まで `MetaDAOPort.advance_cursor` でカーソルを進めてから投影を再開する。

#### 4.4.1 リクエスト / 引数

| IF-ID  | パラメータ           | データ型                          |         必須性         | NULL可 | バリデーション・制約                                          | 例・備考          |
| :----- | :------------------- | :-------------------------------- | :--------------------: | :----: | :------------------------------------------------------------ | :---------------- |
| IF-004 | `projection`         | `ProjectionPort[T]`               |          必須          |  不可  | 一意な `consumer_id` と冪等な `apply_batch` を持つ実装        | `FtsProjection()` |
| IF-004 | `partition_patterns` | `Sequence[str]`                   | 任意（キーワード専用） |  不可  | 1件以上の有効な購読パターン（既定値 `("*",)`）                | `("chat:*",)`     |
| IF-004 | `batch_size`         | `int`                             | 任意（キーワード専用） |  不可  | `>= 1`（既定値 `100`）                                        | `100`             |
| IF-004 | `retry_policy`       | `RetryPolicy`                     | 任意（キーワード専用） |  不可  | リトライ設定（既定値 `KernelConfig.default_retry_policy`）    | `RetryPolicy()`   |
| IF-004 | `failure_policy`     | `ProjectionFailurePolicy \| None` | 任意（キーワード専用） |   可   | `"dlq_and_skip"`、`"pause_partition"`、または `None`          | `None`            |
| IF-004 | `include_snapshots`  | `bool`                            | 任意（キーワード専用） |  不可  | `__snapshot__` を `apply_batch` に含めるか（既定値 `False`）  | `False`           |
| IF-005 | `consumer_id`        | `str`                             |          必須          |  不可  | 登録済み Projection の `consumer_id`                          | `"fts_index_v1"`  |
| IF-005 | `partition_key`      | `str`                             |          必須          |  不可  | 再開対象のパーティションキー                                  | `"chat:general"`  |
| IF-005 | `skip_current`       | `bool`                            | 任意（キーワード専用） |  不可  | 原因レコード1件を飛ばしてカーソルを進めるか（既定値 `False`） | `True`            |

#### 4.4.2 レスポンス / 戻り値

| IF-ID           | フィールド | データ型 | 必須性 | NULL可 | 意味・取り得る値                             |
| :-------------- | :--------- | :------- | :----: | :----: | :------------------------------------------- |
| IF-004 / IF-005 | （戻り値） | `None`   |   -    |   -    | 正常登録または再開指示完了時は `None` を返す |

#### 4.4.3 エラー応答

| エラーコード / 例外型    | 発生条件                                                                                                                                                                              | 応答形式 | 呼び出し側の対応                    | 再試行可否 |
| :----------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :------- | :---------------------------------- | :--------- |
| `ValueError`             | 重複 `consumer_id`、未登録 `consumer_id`、`DeadLetterPort` 未注入状態で `failure_policy="dlq_and_skip"` を明示指定、不正パターン、`batch_size < 1`                                    | 例外送出 | 設定または引数を修正する            | 不可       |
| `KernelClosedError`      | `close()` 開始後に呼ばれた                                                                                                                                                            | 例外送出 | 処理を中止する                      | 不可       |
| `KernelMaintenanceError` | メンテナンスモード中に `register_projection`、またはカーソル更新を伴う `resume_projection(..., skip_current=True)` を呼んだ場合（`subscribe` と `skip_current=False` の再開は対象外） | 例外送出 | `exit_maintenance()` 後に再実行する | 条件付き   |

---

### 4.5 契約（IF-ID: IF-006 `StorageKernel.set_partition_status`）

- **概要**: パーティションの状態（`"active"` / `"read_only"`）をパーティションロック下で遷移させ、`MetaDAOPort` とカーネル内ルーティングキャッシュを同期更新する。
- **呼び出し方式**: `kernel.set_partition_status(partition_key, status, *, expected_status=None) -> PartitionInfo`
- **冪等性**: 条件付き（`expected_status=None` で同じ `status` を指定した場合は冪等）。
- **事前条件 / 事後条件**:
  - **事前条件**: 対象 `partition_key` が登録済みであること。メンテナンスモード中でも `close()` 後でもないこと。
  - **事後条件**: `ShardLockManager` の対象パーティションロック内で状態遷移とキャッシュ更新が完了し、`"read_only"` への遷移完了後は一切の `append` / `write_snapshot` が成功しない。

#### 4.5.1 リクエスト / 引数

| パラメータ        | データ型                  |         必須性         | NULL可 | バリデーション・制約                               | 例・備考         |
| :---------------- | :------------------------ | :--------------------: | :----: | :------------------------------------------------- | :--------------- |
| `partition_key`   | `str`                     |          必須          |  不可  | 登録済みのパーティションキー                       | `"order:usdjpy"` |
| `status`          | `PartitionStatus`         |          必須          |  不可  | `"active"` または `"read_only"`                    | `"read_only"`    |
| `expected_status` | `PartitionStatus \| None` | 任意（キーワード専用） |   可   | 楽観ロック（CAS）用の期待現在状態（既定値 `None`） | `"active"`       |

#### 4.5.2 レスポンス / 戻り値

| フィールド     | データ型        | 必須性 | NULL可 | 意味・取り得る値                                      |
| :------------- | :-------------- | :----: | :----: | :---------------------------------------------------- |
| （戻り値全体） | `PartitionInfo` |  必須  |  不可  | 更新後の `PartitionInfo`（`status` が更新された状態） |

#### 4.5.3 エラー応答

| エラーコード / 例外型                          | 発生条件                               | 応答形式 | 呼び出し側の対応               | 再試行可否 |
| :--------------------------------------------- | :------------------------------------- | :------- | :----------------------------- | :--------- |
| `PartitionStateConflictError`                  | `expected_status` が現在の状態と不一致 | 例外送出 | 現在状態を再確認して再判断する | 条件付き   |
| `KeyError`                                     | 未登録の `partition_key` を指定した    | 例外送出 | パーティションキーを確認する   | 不可       |
| `KernelClosedError` / `KernelMaintenanceError` | 停止後またはメンテナンスモード中       | 例外送出 | メンテナンス終了後に再試行     | 条件付き   |

---

### 4.6 契約（IF-ID: IF-007 `StorageKernel.enter_maintenance` / IF-008 `StorageKernel.exit_maintenance`）

- **概要**:
  - `IF-007` (`enter_maintenance`): 一貫したバックアップを取得するための整合点を作る。新規書き込み・パーティション作成・状態変更・カーソル更新を `KernelMaintenanceError` で遮断し、進行中のトランザクション（および必要に応じた Projection ドレイン）の完了を待ってから `MaintenanceHooks.on_enter_maintenance(reason)` を呼び出す。
  - `IF-008` (`exit_maintenance`): `MaintenanceHooks.on_exit_maintenance()` を呼び出してからメンテナンスモードを解除し、通常運転へ戻す。
- **呼び出し方式**:
  - `kernel.enter_maintenance(reason, *, drain_projections=False, timeout=30.0) -> None`
  - `kernel.exit_maintenance() -> None`
- **冪等性**: なし（既にメンテナンス中に `enter_maintenance` を再度呼んだ場合は `KernelMaintenanceError`、通常運転中に `exit_maintenance` を呼んだ場合は何もしない）。
- **事前条件 / 事後条件**:
  - **事前条件**: `close()` が開始されていないこと。`reason` が非空文字列、`timeout > 0` であること。
  - **事後条件**: `on_enter_maintenance` が呼ばれている間、すべてのシャードDBとメタDBへの書き込みトランザクションおよびカーソル更新が完全に静止していることが保証される。

#### 4.6.1 リクエスト / 引数

| IF-ID  | パラメータ          | データ型 |         必須性         | NULL可 | バリデーション・制約                                                                                   | 例・備考         |
| :----- | :------------------ | :------- | :--------------------: | :----: | :----------------------------------------------------------------------------------------------------- | :--------------- |
| IF-007 | `reason`            | `str`    |          必須          |  不可  | 非空文字列（ログおよび `on_enter_maintenance` に渡される）                                             | `"daily_backup"` |
| IF-007 | `drain_projections` | `bool`   | 任意（キーワード専用） |  不可  | 登録済み全 Projection が最新 `seq` に追いつくまで待つか（既定値 `False`：実行中の1バッチ完了のみ待機） | `False`          |
| IF-007 | `timeout`           | `float`  | 任意（キーワード専用） |  不可  | 進行中トランザクション・Projection 待機の最大秒数（既定値 `30.0`、`> 0`）                              | `30.0`           |
| IF-008 | （引数なし）        | -        |           -            |   -    | -                                                                                                      | -                |

#### 4.6.2 レスポンス / 戻り値

| IF-ID           | フィールド | データ型 | 必須性 | NULL可 | 意味・取り得る値    |
| :-------------- | :--------- | :------- | :----: | :----: | :------------------ |
| IF-007 / IF-008 | （戻り値） | `None`   |   -    |   -    | 正常完了時は `None` |

#### 4.6.3 エラー応答

| エラーコード / 例外型    | 発生条件                                                                                                                             | 応答形式 | 呼び出し側の対応                                    | 再試行可否 |
| :----------------------- | :----------------------------------------------------------------------------------------------------------------------------------- | :------- | :-------------------------------------------------- | :--------- |
| `TimeoutError`           | `timeout` 秒以内に進行中トランザクションまたは Projection ドレインが完了しなかった場合（メンテナンスモードは自動解除されて元に戻る） | 例外送出 | 負荷が下がってから再試行するか `timeout` を延長する | 可         |
| `KernelMaintenanceError` | 既にメンテナンスモード中に `enter_maintenance` を呼んだ                                                                              | 例外送出 | 重複呼び出しを解消する                              | 不可       |
| `KernelClosedError`      | `close()` 開始後に呼ばれた                                                                                                           | 例外送出 | 処理を中止する                                      | 不可       |

---

### 4.7 契約（IF-ID: IF-009 `StorageKernel.sync`）

- **概要**: `{partition_key: last_seen_seq}` のカーソル辞書を受け取り、複数シャードを横断（Scatter-Gather）して `seq > last_seen_seq` の未処理レコード群を取得する。古い `schema_version` のレコードには読み取り時に `PayloadUpcaster` を適用して最新版へ変換してから返す。
- **呼び出し方式**: `kernel.sync(cursors, *, limit_per_partition=100) -> dict[str, list[RecordEnvelope[T]]]`
- **冪等性**: あり（読み取り専用。保存済みレコードもカーソルも変更しない）。
- **事前条件 / 事後条件**:
  - **事前条件**: `cursors` の各 `last_seen_seq >= 0`、`limit_per_partition >= 1` であること。
  - **事後条件**: 返却される各リストは `seq` の厳密な昇順で欠落がなく、すべての `RecordEnvelope[T]` は最新の `schema_version` に Upcast・decode されている（ただし `payload_hash` は保存時の元データのハッシュ値を維持する）。新規差分が 0 件のパーティション、または未登録のパーティションは返却辞書のキーに含めない（または空リストとして扱わず省略する）。

#### 4.7.1 リクエスト / 引数

| パラメータ            | データ型            |         必須性         | NULL可 | バリデーション・制約                                        | 例・備考                                   |
| :-------------------- | :------------------ | :--------------------: | :----: | :---------------------------------------------------------- | :----------------------------------------- |
| `cursors`             | `Mapping[str, int]` |          必須          |  不可  | `{partition_key: after_seq}`（`after_seq >= 0`）            | `{"order:usdjpy": 120, "chat:general": 0}` |
| `limit_per_partition` | `int`               | 任意（キーワード専用） |  不可  | 1パーティションあたりの最大取得件数（既定値 `100`、`>= 1`） | `100`                                      |

#### 4.7.2 レスポンス / 戻り値

| フィールド     | データ型                             | 必須性 | NULL可 | 意味・取り得る値                                                                       |
| :------------- | :----------------------------------- | :----: | :----: | :------------------------------------------------------------------------------------- |
| （戻り値全体） | `dict[str, list[RecordEnvelope[T]]]` |  必須  |  不可  | 差分が存在した `partition_key` をキー、昇順の `RecordEnvelope[T]` リストを値とする辞書 |

#### 4.7.3 エラー応答

| エラーコード / 例外型 | 発生条件                                                                                                                     | 応答形式 | 呼び出し側の対応                                                         | 再試行可否 |
| :-------------------- | :--------------------------------------------------------------------------------------------------------------------------- | :------- | :----------------------------------------------------------------------- | :--------- |
| `SchemaVersionError`  | 保存済みレコードの `schema_version` が `schema_registry` の最新版より新しい、または `PayloadUpcaster` が未登録・変換経路欠落 | 例外送出 | `schema_registry` と `PayloadUpcaster` の実装を確認する                  | 不可       |
| `CursorExpiredError`  | 指定カーソルが保持範囲より古い（※v1 では送出されない。v2 の Retention 導入時に有効化）                                       | 例外送出 | 最新のスナップショット（`__snapshot__`）を取得してカーソルを再初期化する | 条件付き   |
| `ValueError`          | `after_seq < 0` または `limit_per_partition < 1`                                                                             | 例外送出 | 引数を修正する                                                           | 不可       |

---

### 4.8 契約（IF-ID: IF-010 `StorageKernel.close`）

- **概要**: 新規書き込みを即座に `KernelClosedError` で遮断し、進行中の書き込みトランザクションの完了を待機した上で、`ProjectionRunner` と `subscribe` ワーカーを停止して `ShutdownReport` を返す。
- **呼び出し方式**: `kernel.close(*, timeout=5.0, drain_projections=False, drain_subscribers=False) -> ShutdownReport`
- **冪等性**: あり（2回目以降の呼び出しは即座に直前の `ShutdownReport` を返す）。
- **事前条件 / 事後条件**:
  - **事後条件**:
    1. `close()` 開始と同時に新規 `append` / `write_snapshot` / `set_partition_status` / `enter_maintenance` は `KernelClosedError` で拒否される。
    2. 進行中の `ShardDAOPort.write_transaction` は完了まで待つ。
    3. `drain_projections=False`（既定）では実行中の `apply_batch` + `advance_cursor` の1サイクル完了のみを待ち、残キューは破棄する（次回起動時にカーソルから追いつけるため）。`True` の場合は最新 `seq` 到達まで `timeout` 内で待つ。
    4. `drain_subscribers=False`（既定）では実行中のコールバック完了のみを待ち、残キューは破棄して件数を `dropped_subscriber_events` に計上する。`True` の場合はキューが空になるまで `timeout` 内で待つ。

#### 4.8.1 リクエスト / 引数

| パラメータ          | データ型 |         必須性         | NULL可 | バリデーション・制約                                              | 例・備考 |
| :------------------ | :------- | :--------------------: | :----: | :---------------------------------------------------------------- | :------- |
| `timeout`           | `float`  | 任意（キーワード専用） |  不可  | 停止待機の最大秒数（既定値 `5.0`、`>= 0.0`）                      | `5.0`    |
| `drain_projections` | `bool`   | 任意（キーワード専用） |  不可  | 全 Projection を最新 `seq` まで処理しきるか（既定値 `False`）     | `False`  |
| `drain_subscribers` | `bool`   | 任意（キーワード専用） |  不可  | 全 `subscribe` キューを空になるまで処理しきるか（既定値 `False`） | `False`  |

#### 4.8.2 レスポンス / 戻り値

| フィールド     | データ型         | 必須性 | NULL可 | 意味・取り得る値                                                                                            |
| :------------- | :--------------- | :----: | :----: | :---------------------------------------------------------------------------------------------------------- |
| （戻り値全体） | `ShutdownReport` |  必須  |  不可  | `completed_cleanly`, `dropped_subscriber_events`, `pending_projection_partitions`, `elapsed_sec` を保持する |

#### 4.8.3 エラー応答

- タイムアウト超過時も例外でクラッシュさせず、警告ログと telemetry を出力した上で `completed_cleanly=False` の `ShutdownReport` を返却する。

---

### 4.9 Port（SPI）契約（IF-ID: IF-101〜IF-109）

親プロジェクトが実装して `StorageKernel` に注入する 9 つの Protocol 契約と必須不変条件を下表に定義する。

#### 4.9.1 `ShardDAOPort[T]`（IF-101）および `ShardTxPort[T]`（IF-102）

| Port / メソッド                         | 引数                                                                  | 戻り値                                   | 契約・必須不変条件（異常時の例外）                                                                                                                                                                                        |
| :-------------------------------------- | :-------------------------------------------------------------------- | :--------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ShardDAOPort.durability`（プロパティ） | なし                                                                  | `DurabilityLevel`                        | 自己申告の耐久性レベル（`"power_loss"`, `"process_crash"`, `"none"`）を返す                                                                                                                                               |
| `ShardDAOPort.write_transaction`        | `shard_id: str`                                                       | `AbstractContextManager[ShardTxPort[T]]` | 単一シャード内の排他書き込みトランザクションを開始する。`with` ブロックの正常終了時に COMMIT し、例外発生時はレコードとシーケンス増分の双方を ROLLBACK する。正常終了は宣言した `durability` 水準での永続化確定を意味する |
| `ShardDAOPort.fetch_range`              | `shard_id: str`, `partition_key: str`, `after_seq: int`, `limit: int` | `Sequence[RecordEnvelope[T]]`            | `seq > after_seq` のレコードを厳密な `seq` 昇順で、欠落なく最大 `limit` 件返す。空パーティションの場合は空シーケンス `()` を返す                                                                                          |
| `ShardTxPort.find_by_idempotency`       | `partition_key: str`, `idempotency_key: str`                          | `RecordEnvelope[T] \| None`              | 同一トランザクション内で `(partition_key, idempotency_key)` に一致する既存レコードを検索し、`payload_hash` と `covers_through_seq` を含む完全な `RecordEnvelope[T]` を返す（未存在なら `None`）                           |
| `ShardTxPort.next_sequence`             | `partition_key: str`                                                  | `int`                                    | パーティション単位の単調増加連番 `seq` を採番して返す。初回は `1`、以降は直前の確定 `seq + 1`（欠番・逆転禁止）                                                                                                           |
| `ShardTxPort.insert_record`             | `envelope: RecordEnvelope[T]`                                         | `None`                                   | レコードを永続化する。`(partition_key, seq)` または `(partition_key, idempotency_key)` の一意制約違反が発生した場合は、DB ドライバー固有の例外を漏らさず `DuplicateKeyError(kind=...)` に変換して送出する                 |

#### 4.9.2 `MetaDAOPort`（IF-103）

| メソッド / プロパティ      | 引数                                                                                                  | 戻り値                    | 契約・必須不変条件（異常時の例外）                                                                                                                                                       |
| :------------------------- | :---------------------------------------------------------------------------------------------------- | :------------------------ | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `durability`（プロパティ） | なし                                                                                                  | `DurabilityLevel`         | メタストレージの耐久性レベルを返す                                                                                                                                                       |
| `get_partition`            | `partition_key: str`                                                                                  | `PartitionInfo \| None`   | 登録済みパーティション情報を返す。未登録なら `None`                                                                                                                                      |
| `bind_shard`               | `partition_key: str`, `shard_id: str`                                                                 | `PartitionInfo`           | 未登録なら `status="active"` で新規登録して返す。**登録済みなら決して上書きせず、既存の `PartitionInfo`（最初に確定した `shard_id`）をそのまま返す（並行呼び出しに対して冪等かつ不変）** |
| `set_partition_status`     | `partition_key: str`, `status: PartitionStatus`, `*, expected_status: PartitionStatus \| None = None` | `PartitionInfo`           | パーティション状態を更新して返す。`expected_status` が指定され現在の状態と異なる場合は、状態を更新せず `PartitionStateConflictError` を送出する                                          |
| `list_partitions`          | `*, status: PartitionStatus \| None = None`                                                           | `Sequence[PartitionInfo]` | 登録済みパーティション一覧を返す。`status` 指定時は該当状態のみを絞り込んで返す（`ProjectionRunner` の起動時キャッチアップに使用）                                                       |
| `get_cursor`               | `consumer_id: str`, `partition_key: str`                                                              | `int`                     | 指定 `(consumer_id, partition_key)` の最終処理済み `seq` を返す。未記録時は `0` を返す                                                                                                   |
| `advance_cursor`           | `consumer_id: str`, `partition_key: str`, `seq: int`                                                  | `None`                    | カーソルを **単調非減少（`MAX(current_seq, seq)`）** で更新する。過去の小さな `seq` が渡されても決して後退させてはならない                                                               |

#### 4.9.3 `ProjectionPort[T]`（IF-104）および `DeadLetterPort[T]`（IF-105）

| Port / メソッド                            | 引数                                                     | 戻り値                          | 契約・必須不変条件（異常時の例外）                                                                                                                                                             |
| :----------------------------------------- | :------------------------------------------------------- | :------------------------------ | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ProjectionPort.consumer_id`（プロパティ） | なし                                                     | `str`                           | カーソル管理および DLQ 記録に使われる一意な Consumer 識別子                                                                                                                                    |
| `ProjectionPort.apply_batch`               | `records: Sequence[RecordEnvelope[T]]`                   | `None`                          | レコード群を検索 DB や集計ビューへ投影する。**At-Least-Once 配信のため、`(partition_key, seq)` に対して完全に冪等であること**。失敗時は例外を送出し、基盤側の Retry / Poison Pill 制御に委ねる |
| `DeadLetterPort.put_dead_letter`           | `record: DeadLetterRecord[T]`                            | `None`                          | リトライ上限を超えた単件レコード（Poison Pill）を永続化隔離する                                                                                                                                |
| `DeadLetterPort.list_dead_letters`         | `*, consumer_id: str \| None = None`, `limit: int = 100` | `Sequence[DeadLetterRecord[T]]` | 隔離された Dead Letter の一覧を返す（`consumer_id` 指定時はフィルタリング）                                                                                                                    |

#### 4.9.4 `PayloadCodec[T]`（IF-106）、`PayloadUpcaster`（IF-107）、`PartitionRouterPort`（IF-108）、`MaintenanceHooks`（IF-109）

| Port / メソッド                         | 引数                                                           | 戻り値  | 契約・必須不変条件（異常時の例外）                                                                                                                                                                             |
| :-------------------------------------- | :------------------------------------------------------------- | :------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PayloadCodec.encode`                   | `value: T`                                                     | `bytes` | ペイロードをバイト列へ変換する。**決定論的であること（論理的に等価な値から常に同一バイト列を返すこと。辞書キー順や浮動小数点表記の揺れを排除する）**                                                           |
| `PayloadCodec.decode`                   | `data: bytes`                                                  | `T`     | 最新 `schema_version` のバイト列からドメインペイロード `T` を復元する                                                                                                                                          |
| `PayloadUpcaster.upcast`                | `record_type: str`, `from_version: int`, `raw_payload: bytes`  | `bytes` | 旧版のバイト列を **`from_version` から `from_version + 1` へ 1 版だけ進めたバイト列** を返す。決定論的かつ副作用なし（保存済みレコードは書き換えない）。未知の版や変換不能時は `SchemaVersionError` を送出する |
| `PartitionRouterPort.select_shard`      | `partition_key: str`, `*, preferred_shard: str \| None = None` | `str`   | 新規パーティションの格納先 `shard_id` を決定する純粋なポリシー関数（パーティション移動は扱わない）                                                                                                             |
| `MaintenanceHooks.on_enter_maintenance` | `reason: str`                                                  | `None`  | 新規更新が遮断され進行中トランザクションがすべて完了した整合点で呼ばれる。親プロジェクトはこのフック内でバックアップを実行する                                                                                 |
| `MaintenanceHooks.on_exit_maintenance`  | なし                                                           | `None`  | `exit_maintenance()` 時に通常運転へ戻る直前に呼ばれる                                                                                                                                                          |

---

## 5. 互換性・バージョン管理・変更管理

- **バージョン方式**: セマンティックバージョニング（`MAJOR.MINOR.PATCH`）および Git タグ固定によるサブモジュール管理（[`DBF-ENV-001`](../setup/DBF-ENV-001_環境・開発基盤仕様書.md) 4.3節）。ペイロード個別の版管理は `RecordEnvelope.schema_version`（正の整数）と `KernelConfig.schema_registry` で行う。
- **後方互換性の範囲**:
  - **後方互換（Minor / Patch）**: 新しい `record_type` の追加、新 `schema_version` と対応する `PayloadUpcaster` の追加、`core/`・`runtime/` の内部改善、公開 API へのデフォルト値付きキーワード引数の追加。
  - **破壊的変更（Major）**: `ports/` または `contracts/` の既存メソッド・フィールドの削除や型変更、過去の版の `PayloadUpcaster` の削除、同一 `schema_version` の意味や変換結果の変更、保存済みバイト列と互換性のない `PayloadCodec` の変更。
- **予約済みの拡張口（v1 で名前・型のみ予約）**:
  - `ports/transport.py`: v2 以降のマルチプロセス・別言語 IPC 配信口としてモジュール名を予約（v1 ではファイル未作成）。
  - `PartitionStatus` の `"migrating"` 状態: v2 のパーティション無停止移動用に予約。
  - `CursorExpiredError` および `SNAPSHOT_RECORD_TYPE = "__snapshot__"`: v2 の自動 Retention 導入時に `contracts/` を変更せずに済むよう v1 で先行定義。
- **非推奨・廃止の手順**:
  - 過去の `schema_version` の Upcaster を廃止する場合は、Retention（v2）により該当版のレコードが全シャードから消滅し、かつバックアップの復旧可能期間も満了したことを運用手順で確認してから Major バージョンアップで削除する。
- **変更の通知先**: 親プロジェクトのリポジトリ管理者（サブモジュールのリリースノートおよび ADR で通知）。
- **変更時の影響確認**: 本書（`DBF-IF-001`）の契約を変更する場合は、対応する `DBF-DD-001`〜`004` の機能定義、`DBF-DS-001` のスキーマ定義、および `testing/contract_test.py` のテスト要件（第6章）を同一変更単位で更新する。

---

## 6. 機能テスト要件（外部契約）

親プロジェクトが独自の DAO / Projection / Codec / Upcaster を実装した際、`storage_kernel.testing.contract_test`（`unittest.TestCase` ベース、`make_dao()` フック提供）を継承して検証すべき 13 項目の外部契約テスト要件を下表に定義する（Issue #2・#3・#4）。具体的なテストケース・実行結果・証跡はテスト計画・結果報告書、品質ゲートは [`DBF-ENV-001`](../setup/DBF-ENV-001_環境・開発基盤仕様書.md)（`QG-004`）を正本とし、本書へ再掲しない。

| 要件ID    | 対象IF-ID       | 保証すべき契約上の振る舞い                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | 検証観点               | 対応テストケース                                                          |
| :-------- | :-------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------------------- | :------------------------------------------------------------------------ |
| TR-IF-001 | IF-101 / IF-102 | **ロールバックの原子性（`ShardDAOPort`）**: `write_transaction` 内で例外を送出したとき、挿入レコードと `next_sequence` の増分の両方が巻き戻ること                                                                                                                                                                                                                                                                                                                                                                                                                                                               | 異常 / 原子性          | `ContractTest.test_01_rollback_atomicity`                                 |
| TR-IF-002 | IF-101 / IF-103 | **再接続後の永続性（`ShardDAOPort` / `MetaDAOPort`）**: `make_dao()` で同じバックエンドを指す新しいインスタンスを作り直したとき、直前の書き込み・シャードバインド・カーソルが読み取れること。shard の書き込み検証は `shard_dao.durability != "none"` のときだけ、meta（バインド・カーソル）の検証は `meta_dao.durability != "none"` のときだけ実行し、両方が `"none"` のときに限りテスト全体をスキップする                                                                                                                                                                                                      | 正常 / 永続性          | `ContractTest.test_02_persistence_across_reconnect`                       |
| TR-IF-003 | IF-101 / IF-102 | **単調増加・厳密昇順・欠番なし（`ShardDAOPort`）**: 同一スレッドの連続書き込みと並行書き込みの両方で、`seq` が `1, 2, 3, ...` と欠番・重複なく採番され、`fetch_range` が厳密な昇順で返すこと                                                                                                                                                                                                                                                                                                                                                                                                                    | 正常 / 並行性 / 順序   | `ContractTest.test_03_monotonic_sequence_and_fetch_order`                 |
| TR-IF-004 | IF-101 / IF-102 | **冪等な再送で元のレコードを返す（`ShardDAOPort`）**: 同じ `(partition_key, idempotency_key, payload_hash)` を再送しても `seq` が増えず（再送後の最終 `seq` と次回採番値が変わらない）、`find_by_idempotency` が元と同じ `seq`・`payload_hash`・payload の `RecordEnvelope` を返し、保存件数が 1 件のままであること                                                                                                                                                                                                                                                                                             | 正常 / 冪等性          | `ContractTest.test_04_idempotent_lookup_returns_original`                 |
| TR-IF-005 | IF-102          | **一意制約違反の例外変換（`ShardTxPort`）**: `(partition_key, seq)` または `(partition_key, idempotency_key)` の重複挿入時に、DB 固有の例外ではなく `DuplicateKeyError` が送出されること。`kind` は `seq` の重複なら `"sequence"`、冪等キーの重複なら `"idempotency_key"` を必須とし、`"unknown"` は不合格とする                                                                                                                                                                                                                                                                                                | 異常 / 例外変換        | `ContractTest.test_05_unique_violation_raises_duplicate_key_error`        |
| TR-IF-006 | IF-101 / IF-102 | **パーティション間のスコープ分離（`ShardDAOPort`）**: `partition_key` が異なれば、同じ `idempotency_key` でも衝突せず独立して `seq=1` から書き込めること                                                                                                                                                                                                                                                                                                                                                                                                                                                        | 正常 / 境界            | `ContractTest.test_06_idempotency_scope_isolated_by_partition`            |
| TR-IF-007 | IF-103          | **シャードバインドの冪等性と不変性（`MetaDAOPort`）**: 並行または複数回、異なる `shard_id` で `bind_shard` を呼んでも上書きされず、最初に確定した `shard_id` が維持されること                                                                                                                                                                                                                                                                                                                                                                                                                                   | 正常 / 並行性 / 不変性 | `ContractTest.test_07_bind_shard_idempotent_and_immutable`                |
| TR-IF-008 | IF-101 / IF-103 | **空パーティションの許容（`MetaDAOPort` / `ShardDAOPort`）**: `bind_shard` だけ完了しレコード未挿入のパーティションに対して、`fetch_range` が空列を返し、初回の書き込みが `seq=1` になること                                                                                                                                                                                                                                                                                                                                                                                                                    | 正常 / 境界            | `ContractTest.test_08_empty_partition_allowed`                            |
| TR-IF-009 | IF-103          | **カーソルの単調性（`MetaDAOPort`）**: 現在のカーソルより小さい過去の `seq` で `advance_cursor` を呼んでも、カーソルが後退しないこと                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | 正常 / 不変条件        | `ContractTest.test_09_cursor_monotonic_non_decreasing`                    |
| TR-IF-010 | IF-104          | **同じバッチの再適用の冪等性（`ProjectionPort`）**: 同じ `RecordEnvelope` 群を 2 回続けて `apply_batch` に渡しても、各 `(partition_key, seq)` の保持件数が 1 件のままであること。`ProjectionPort` には読み戻し口がないため、親プロジェクトは Contract Test サブクラスで `projected_seqs(projection, partition_key) -> Sequence[int]` を実装する（Projection が当該パーティションについて保持している行の `seq` を 1 行 1 要素で返す。重複して保持していれば同じ値を重複して返す）。未実装（基底の既定）の場合は黙って通らず、実装方法を示す `NotImplementedError` で失敗する。`ProjectionPort` 自体は変更しない | 正常 / 冪等性          | `ContractTest.test_10_projection_apply_batch_idempotent`                  |
| TR-IF-011 | IF-106          | **encode の決定論性（`PayloadCodec`）**: 論理的に等価な値（キーの挿入順が異なる dict など）を `encode` したとき、常に完全一致するバイト列を返すこと                                                                                                                                                                                                                                                                                                                                                                                                                                                             | 正常 / 決定論性        | `ContractTest.test_11_payload_codec_deterministic_encode`                 |
| TR-IF-012 | IF-103          | **パーティション状態遷移と一覧取得（`MetaDAOPort`）**: `bind_shard` 直後の `status` が `"active"` であること。`set_partition_status(..., expected_status=...)` が不一致のとき `PartitionStateConflictError` を送出し、`list_partitions(status=...)` が条件どおりに返すこと                                                                                                                                                                                                                                                                                                                                      | 正常 / 異常 / 状態遷移 | `ContractTest.test_12_partition_status_transition_and_listing`            |
| TR-IF-013 | IF-107          | **Upcaster の決定論性と未知版の拒否（`PayloadUpcaster`）**: 同じ `(record_type, from_version, raw_payload)` から常に同じバイト列を返し、変換経路のない未知の版に対して `SchemaVersionError` を送出すること                                                                                                                                                                                                                                                                                                                                                                                                      | 正常 / 異常 / 互換性   | `ContractTest.test_13_payload_upcaster_deterministic_and_unknown_version` |

---

## 7. 作成完了チェック

- [x] `document_role`、`ssot_scope`、`status`、`owner`をFrontmatterへ設定した。
- [x] インターフェース一覧のIF-IDと、DDの機能ID（公開範囲が「外部公開」のもの）が対応している。
- [x] 提供者・利用者、方式、データ形式、時刻、相関ID、タイムアウト・再試行を記入した。
- [x] 認証・認可の方式と権限不足時の応答を記入した（秘密情報そのものは記載していない）。
- [x] リクエスト・レスポンスの必須性、NULL可否、制約、エラー応答を記入した。
- [x] バージョン方式、互換性の範囲、非推奨・廃止の手順を記入した。
- [x] 受信形式と保存形式を混同せず、保存後の形式はDSを参照している。
- [x] 機能テスト要件ごとに対応ケースIDを記入し、品質ゲート・テストケースを本書へ再掲していない。
- [x] 未置換の `[ ... ]`、`YYYY-MM-DD`、サンプル固有名が残っていない。

---

## 8. 改訂履歴 (Change Log)

| 版数    | 改訂日     | 変更者 | 変更内容・変更理由 (Why)                                                                                                                                                                  |
| :------ | :--------- | :----- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Rev.1.0 | 2026-11-19 | xzyozi | 新規作成（Issue #13 に基づき公開API、Port/SPI、契約型・例外、Contract Test 13項目を制定）                                                                                                 |
| Rev.1.1 | 2026-11-20 | xzyozi | コンストラクタ引数（`store_raw_bytes` を含む）の節を追加し、メンテナンス中の `subscribe`（許可）・`register_projection`（拒否）、`resume_projection` の close 後の扱いを明記（Issue #40） |
| Rev.1.2 | 2026-11-20 | xzyozi | Contract Test の表を実装と同じ強さに改訂（Issue #38）。TR-IF-010 に `projected_seqs` フックを追加し、TR-IF-002 のスキップ条件を shard/meta 別に分離、TR-IF-003〜005 の基準を強化          |
| Rev.1.3 | 2026-11-20 | xzyozi | v1.0.0 リリース準備に伴い、状態を `draft` から `approved` へ変更（実装・レビューを経て現在の正本とする）                                                                                  |
| Rev.1.4 | 2026-11-20 | xzyozi | §2.5.1 を追加し、ペイロードの型・`codec`・`store_raw_bytes` の組み合わせ（3 種）と選び方、注意点を明記（Issue #54 の判断: 現状維持）。                                                    |
