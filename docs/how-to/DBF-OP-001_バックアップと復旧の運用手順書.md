---
title: "運用手順書（バックアップ・復旧）"
document_type: "operation_procedure"
document_number: "DBF-OP-001"
document_role: "procedure"
ssot_scope: "operation"
status: "approved"
version: "1.0"
created_at: "2026-11-20"
updated_at: "2026-11-20"
author: "xzyozi"
owner: "xzyozi"
purpose: "storage_kernel および SQLite 参照実装（contrib/sqlite/）における整合点バックアップ取得、コールドリストア、Projection 再構築、および DLQ 回復の標準手順を定義するため"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "DBF-ENV-001 環境・開発基盤仕様書（相対パス: ../setup/DBF-ENV-001_環境・開発基盤仕様書.md）"
  - "DBF-BD-001 基本設計書（相対パス: ../design/DBF-BD-001_基本設計書.md）"
  - "DBF-DS-001 データ構造仕様書（相対パス: ../design/DBF-DS-001_データ構造仕様書.md）"
  - "DBF-IF-001 IF仕様書（相対パス: ../design/DBF-IF-001_IF仕様書.md）"
  - "DBF-DD-004 実行制御詳細設計書（相対パス: ../design/DBF-DD-004_実行制御詳細設計書.md）"
---

# 運用手順書
**storage_kernel バックアップ・復旧手順（文書番号: DBF-OP-001）**

---

## 1. 目的・対象範囲

- **目的**: `StorageKernel` が管理する永続データの整合性を損なわずにバックアップを取得し、障害発生時に安全に復旧する手順を定める（Issue #4 D2、[`DBF-DS-001`](../design/DBF-DS-001_データ構造仕様書.md) 4.2節、[`DBF-DD-004`](../design/DBF-DD-004_実行制御詳細設計書.md) `F-402`〜`F-404`）。
- **対象**:
  - メタストレージ: `meta.db`（`partitions`, `cursors`, および同一 DB に配置された場合の `dead_letters`）
  - シャードストレージ: 全シャード DB（`<shard_id>.db` の `records`・`sequences`）
  - バックアップマニフェスト: 対象ファイル一覧、バイトサイズ、SHA-256 チェックサム、バージョン情報、未ドレイン時の `cursors` 位置
  - 親プロジェクト側 Projection DB（[`DBF-DS-001`](../design/DBF-DS-001_データ構造仕様書.md) 4.2節の除外 4 条件を満たさない場合）
- **対象外**: OS・外部クラウドストレージへの転送設定そのもの、複数ノード間レプリケーション、v2 以降の自動 Retention 操作。
- **実施タイミング**: 定期バックアップ時（日次またはアプリ／スキーマ更新前）、DB 破損・誤削除等の障害発生時、Projection の Poison Pill（DLQ）発生時、および月次の復旧訓練時（Projection 除外運用を取る場合）。

## 2. 前提条件・注意事項

- **実行者・必要権限**: 親プロジェクトの運用担当者または親アプリ内の管理ジョブ（DB ディレクトリ・ステージング領域・バックアップ保管先への読み書き権限を持つ実行ユーザー）。
- **事前確認**:
  - **バックアップ時**: `StorageKernel` が `RUNNING` 状態であり、[`SqliteMetaDAO`](../../src/storage_kernel/contrib/sqlite/meta_dao.py) / [`SqliteShardDAO`](../../src/storage_kernel/contrib/sqlite/shard_dao.py) が `durability="power_loss"`（`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;`）または許容された永続水準で稼働していること。
  - **リストア時**: 対象 DB を参照する親アプリプロセスおよび別プロセスが完全に停止していること。
- **停止・中断条件**:
  - `enter_maintenance()` が `TimeoutError`（既定 `30.0` 秒）または `MaintenanceHooks.on_enter_maintenance` 例外で失敗した場合はバックアップを中止し、原因を確認して再実行する（カーネルは自動で `RUNNING` へロールバックされる）。
  - 取得したバックアップまたは復旧用バックアップの SHA-256 検証、あるいは `PRAGMA integrity_check;` が失敗した場合は本番切り替えを中止する。
- **安全上の注意**:
  - **WAL ファイル単体コピーの禁止**: SQLite 参照実装は `WAL` モードを使用するため、通常運転中に `*.db` ファイルを OS コピーしてはならない。必ず `enter_maintenance()` で書き込み・カーソル更新を静止させた上で、**SQLite Backup API（`sqlite3.Connection.backup()`）** または `PRAGMA wal_checkpoint(TRUNCATE);` 実行後のファイルコピーを使用する。
  - **リストア時の WAL/SHM 残骸除去**: 破損 DB 側の `*.db-wal` / `*.db-shm` が復元先ディレクトリに残ると起動時に古い WAL が再生されてデータ不整合を起こすため、リストア時は旧ディレクトリごと退避し、復元先には復元済み `*.db` のみを配置する。
  - **Projection カーソル巻き戻し禁止**: Projection をログ／スナップショットから再構築する際は既存の `consumer_id` のカーソルを巻き戻さず、新しい `consumer_id`（例: `fts_index_v2`）で再生完了後に参照先を切り替える（[`DBF-DS-001`](../design/DBF-DS-001_データ構造仕様書.md) 4.2節）。
  - **秘密情報の非保持**: マニフェストや運用ログに認証情報や利用者固有の秘密情報を含めない。

## 3. 手順

### 3.1 オンライン・整合点バックアップ手順（`enter_maintenance` 使用）

`StorageKernel.enter_maintenance()` により進行中の書き込みトランザクションおよびカーソル更新を静止させ、同一世代のバックアップを取得する。親プロジェクト側では `MaintenanceHooks.on_enter_maintenance(reason)` に処理 2〜4 を実装するか、`enter_maintenance()` 〜 `exit_maintenance()` 間で実行する。

| No. | 操作・確認内容 | 期待結果 | 異常時の対応 |
| :--- | :--- | :--- | :--- |
| 1 | 親管理機能から `kernel.enter_maintenance("scheduled_backup", drain_projections=..., timeout=30.0)` を実行する。<br>※ Projection DB も同一整合点で取得する場合は `drain_projections=True`、Projection 除外運用時は `False`（既定）を指定する | 新規の `append` / `write_snapshot` / `set_partition_status` が `KernelMaintenanceError` で遮断され、`active_mutators == 0` かつ Projection 静止状態（`MAINTENANCE`）へ遷移する | `TimeoutError` または `KernelClosedError` が送出された場合はバックアップを開始せず中断し、タイムアウト超過時は原因（長時間トランザクションや滞留 Projection）を調査する（状態は自動で `RUNNING` に戻る） |
| 2 | 静止期間中に作業用一時ディレクトリを作成し、`meta.db` および全 `<shard_id>.db`（必要に応じて `dead_letters` や Projection DB）を SQLite Backup API で一時ディレクトリへ複製する（例: `src_conn.backup(dst_conn)`） | WAL 上の未反映ページを含め、全 DB が静止時点の一貫した状態として複製される | コピー途中で I/O エラーが発生した場合は一時ディレクトリを破棄し、`finally` 節で `exit_maintenance()` を実行して通常運転へ戻す |
| 3 | 一時ディレクトリ内の各ファイルについてファイル名・バイトサイズ・SHA-256、使用バージョン（`storage_kernel.__version__` / DAO バージョン）、および未ドレイン時は `MetaDAOPort.get_cursor()` の一覧をマニフェストファイルへ記録する | 復元時の整合検証に必要なマニフェストが出力される | 書込失敗時は一時ディレクトリを破棄してステップ 5 へ進む |
| 4 | 一時ディレクトリ内の各 SQLite DB に対して `PRAGMA integrity_check;` を実行し、戻り値が `"ok"` であることを確認する | すべてのバックアップ DB が `"ok"` を返す | 1 ファイルでも `"ok"` 以外を返した場合はバックアップ世代を破棄して異常を記録し、ステップ 5 へ進む |
| 5 | `try ... finally` の `finally` 節で確実に `kernel.exit_maintenance()` を実行し、`MaintenanceHooks.on_exit_maintenance()` 完了後に通常運転（`RUNNING`）へ復帰させる | 書き込み遮断が解除され、一時停止していた `ProjectionRunner` が再開する | `exit_maintenance()` 時に `KernelClosedError` の場合はすでに停止中であるためログ記録のみ行う。フック例外発生時も状態自体は `RUNNING` へ復帰することを確認する |
| 6 | 検証済みの一時ディレクトリを世代保管先ディレクトリへリネーム等で移動し、保管ポリシーに従って古い世代を整理する | 完了した世代のバックアップディレクトリのみが保管先に確定する | 移動失敗時は不完全なディレクトリを残さず削除し、管理者へ通知する |

```python
# 静止点バックアップ処理の実装イメージ（SQLite Backup API 使用例）
import sqlite3
from pathlib import Path

def backup_sqlite_db(src_path: Path, dst_path: Path) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(f"file:{src_path}?mode=ro", uri=True) as src_conn:
        with sqlite3.connect(dst_path) as dst_conn:
            src_conn.backup(dst_conn)
```

### 3.2 コールドリストア（全面復旧）手順

DB ファイルの破損、誤変更、または旧世代への切り戻しを行う手順。必ず親アプリ停止後に実施する。

| No. | 操作・確認内容 | 期待結果 | 異常時の対応 |
| :--- | :--- | :--- | :--- |
| 1 | 親アプリケーションの停止処理を動かし、`kernel.close(timeout=5.0, drain_projections=True, drain_subscribers=True)` が完了して `ShutdownReport` を確認後、対象プロセス（および他の参照プロセス）が完全に終了していることを確認する | `ShutdownReport` が返却され、DB ファイルを開くプロセスが存在しない状態になる | `completed_cleanly=False` の場合はログを確認して未完了ワーカーを把握し、プロセス完全終了を待ってから次へ進む |
| 2 | 復旧対象のバックアップ世代をステージングディレクトリへ展開し、マニフェストに記載された SHA-256 チェックサムの一致および全 SQLite DB の `PRAGMA integrity_check;` が `"ok"` であることを確認する | 全ファイルのハッシュ一致と `"ok"` が確認できる | 不一致または破損が見つかった場合はリストアを中止し、1 世代前のバックアップで再検証する |
| 3 | 現行の本番 DB ディレクトリを別名へリネームして退避する（切り戻し用に保持）。復元先パスに既存の `*.db`, `*.db-wal`, `*.db-shm` が一切残っていないことを確認する | 現行ディレクトリが退避され、復元先が空または未存在になる（古い WAL/SHM の混入防止） | 退避できない（ロックが残っている）場合は対象プロセスが完全に停止しているか再確認する |
| 4 | 検証済みステージングディレクトリ（`meta.db`、全 `<shard_id>.db`、含める場合は Projection DB）を本番 DB パスへリネーム配置し、実行ユーザーに対する適切なファイルパーミッションを設定する | 全 DB ファイルが同一世代のまま本番パスへ配置される | 配置失敗時はステップ 3 で退避した旧ディレクトリを元のパスへ戻す |
| 5 | 親アプリケーションを起動して `StorageKernel` を初期化し、`MetaDAOPort.list_partitions()`、`get_cursor()`、および `kernel.sync({})` により復旧後のパーティション・`seq` 整合性を確認する。<br>※ Projection DB をバックアップ対象外としていた場合は 3.3A の再構築を実施する | 期待されるパーティション数・最終 `seq` が読み取れ、Projection が再開する | 起動時エラーやデータ異常が検知された場合はプロセスを停止し、退避した旧ディレクトリ（または別世代）へ切り戻す |

### 3.3 Projection 再構築および DLQ（Poison Pill）回復手順

#### A. Projection の再構築（Projection DB 除外運用時・インデックス破損時）

| No. | 操作・確認内容 | 期待結果 | 異常時の対応 |
| :--- | :--- | :--- | :--- |
| 1 | 既存の Projection と衝突しない新しい `consumer_id`（例: `fts_index_v2`）を持つ Projection と、初期化済みの新しい Projection 出力先を用意する | 既存 `consumer_id` のカーソルに影響を与えずに再生準備が整う | 誤って旧 `consumer_id` のカーソルを削除・減算した場合は手順を中止し、再発行した `consumer_id` を使用する |
| 2 | `kernel.register_projection(new_projection, ..., include_snapshots=...)` を呼び出して登録する（スナップショット起点の再構築に対応する場合は `include_snapshots=True`） | `ProjectionRunner` がバックグラウンドで `seq=0`（または最新 `__snapshot__`）から適用し、`cursors` に新 `consumer_id` の進捗が記録される | 特定レコードで継続失敗する場合は 3.3B に従って DLQ 内容を確認する |
| 3 | `MetaDAOPort.get_cursor(new_consumer_id, partition_key)` が全対象パーティションの最新 `seq` に追いついたことを確認した上で、親アプリの検索・参照先を新 Projection 出力先へ切り替え、旧 Projection 出力先を破棄する | 無停止または最小停止で新しい Projection が本番参照可能になる | 参照不整合が確認された場合は参照先を旧 Projection 側へ戻して再調査する |

#### B. DLQ（`DEAD_LETTER`）確認および `paused` パーティションの再開

`ProjectionFailurePolicy` によりリトライ上限を超えたレコードは `dead_letters` テーブルへ退避され、該当 `(consumer_id, partition_key)` は `paused` 状態となる。

| No. | 操作・確認内容 | 期待結果 | 異常時の対応 |
| :--- | :--- | :--- | :--- |
| 1 | `SqliteDeadLetterDAO.list_dead_letters(consumer_id=..., limit=...)` で滞留レコードの `partition_key`, `seq`, `record_type`, `schema_version`, `error_type`, `error_message`, `attempts`, `failed_at` を確認する | 失敗原因となったレコード特定情報とエラー詳細が取得できる | DLQ に該当行が無いにも関わらず投影が進まない場合は、エラーログおよびパーティション状態（`read_only` 等）を確認する |
| 2 | エラー種別に応じて原因を解消する（例: Projection 側バグ修正、不足していた `PayloadUpcaster` の追加、外部依存状態の修復、または業務上の手動補償対応） | 再実行すれば成功する状態、またはスキップして問題ない状態になる | スキーマ変更や Upcaster 追加が必要な場合は、アプリ修正・デプロイ後に次ステップへ進む |
| 3 | カーネルが `RUNNING` 状態であることを確認し、`kernel.resume_projection(consumer_id, partition_key, skip_current=...)` を実行する。<br>・修正後に問題レコードから再実行する場合: `skip_current=False`<br>・問題レコードを除外済みとして次 `seq` から再開する場合: `skip_current=True` | 該当パーティションの `paused` 状態が解除され、`ProjectionRunner` が後続処理を再開する（`skip_current=True` の場合はカーソルが `1` 進み記録される） | `KernelMaintenanceError` や `KernelClosedError` が返った場合はメンテナンス解除または起動後に再実行する |

## 4. 補足・例外

| ID | 対象手順 | 標準との差異 | 理由・制約 | 影響 | 代替対応 | 解除条件 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| EX-OP-001 | 3.1・3.2 全般 | テスト用の `durability="none"`（In-Memory 実装）はバックアップ・リストア対象外 | `testing/memory_ports.py` は永続ファイルを持たず、プロセス終了時に全データが消失するため | 永続データの復元は不可 | 本番・ステージングでは必ず `SqliteMetaDAO` / `SqliteShardDAO` 等の `durability="power_loss"` 以上を使用する | 対象外のため解除条件なし（単体テスト専用） |

## 5. 関連文書

- **環境・開発基盤仕様**: [`DBF-ENV-001 環境・開発基盤仕様書`](../setup/DBF-ENV-001_環境・開発基盤仕様書.md)（4.3節 サブモジュール配布、4.4節 品質ゲート）
- **設計書**: [`DBF-BD-001 基本設計書`](../design/DBF-BD-001_基本設計書.md)、[`DBF-DS-001 データ構造仕様書`](../design/DBF-DS-001_データ構造仕様書.md)（3章・4.2節）、[`DBF-IF-001 IF仕様書`](../design/DBF-IF-001_IF仕様書.md)（4.6〜4.8節）、[`DBF-DD-004 実行制御詳細設計書`](../design/DBF-DD-004_実行制御詳細設計書.md)（`F-402`〜`F-404`）
- **テスト証跡・接続例**: [`DBF-TEST-001 性能検証結果報告書`](../test/DBF-TEST-001_性能検証結果報告書.md)、[`examples/parent_app/README.md`](../../examples/parent_app/README.md)

## 6. 作成完了チェック

- [x] `document_role`、`ssot_scope`、`status`、`owner`をFrontmatterへ設定した。
- [x] 目的、対象範囲、対象外、実施タイミングを記入した。
- [x] 実行者、必要権限、事前確認、中断条件を記入した。
- [x] 各手順に操作内容、期待結果、異常時の対応を記入した。
- [x] 不要な監視・バックアップ・エスカレーション項目を無理に追加していない。
- [x] 標準手順から外れる場合は、理由・影響・代替対応・解除条件を記録した。
- [x] 関連する正本文書への相対リンクを設定した。
- [x] 秘密情報そのもの、固定パス、利用者固有の認証情報を記載していない。

## 7. 改訂履歴 (Change Log)

| 版数 | 改訂日 | 変更者 | 変更内容・変更理由 (Why) |
| :--- | :--- | :--- | :--- |
| Rev.1.0 | 2026-11-20 | xzyozi | 新規作成（Issue #17 後回し項目および Issue #4 D2 に基づくバックアップ・復旧・DLQ回復の運用手順を制定） |
