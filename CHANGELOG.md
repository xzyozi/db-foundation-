# 変更履歴

このファイルは、[Keep a Changelog](https://keepachangelog.com/ja/1.1.0/) の形式に準拠し、[セマンティックバージョニング](https://semver.org/lang/ja/) に従います。`ports/` と `contracts/` の変更は Major として扱います。

## [未リリース]

### 追加

- 同一パーティション向け一括書き込み API `StorageKernel.append_batch()` と入力契約型 `BatchAppendItem` を追加（Issue #64）

## [1.0.2] - 2026-11-27

コード（`src/`）、テスト、依存（`pyproject.toml` の依存・`uv.lock` の依存パッケージ）の変更はありません。ドキュメントだけのリリースです（バージョン番号の更新を除く）。

### 追加

- 運用手順書 `DBF-OP-002`（サブモジュールとして組み込む手順）: 追加・タグ固定・`PYTHONPATH` での利用・更新・CI での取得。`examples/parent_app/README.md` の、未検証だった `uv add --editable` の記述を、検証済みの手順への案内に置き換えた

## [1.0.1] - 2026-11-26

### 変更

- 対応する Python を `>=3.12` に変更（`pyproject.toml` の `requires-python`、mypy の `python_version`）。CI が 3.12 のみで、`>=3.10` と宣言していた 3.10・3.11 は検証していなかったため、宣言を検証できている範囲に揃えた（Issue #55）。3.10・3.11 で使っていた場合は、このバージョン以降は非対応になる。

### 追加

- 業務カラムへ展開して保存する DAO の例（`examples/columnar_dao`）と、その Contract Test（Issue #53）
- PostgreSQL による試作 DAO の例（`examples/postgres_dao`）と、その Contract Test（Issue #59）
- `DBF-IF-001` §2.5.1: ペイロードの型・`codec`・`store_raw_bytes` の組み合わせ（3 種）と選び方（Issue #54）
- `ADR-006`: `main` へはリリースブランチ経由のスカッシュで取り込む運用（PR #57）

### 修正

- `DBF-DD-004`（Rev.1.4）: F-402 の呼び出し先に残っていた実在しない名前 `ProjectionRunner.pause_or_drain` を、実装のメソッド名 `ProjectionRunner.enter_maintenance` に修正
- `docs/setup/git_flow_and_branch_policy.md`: リリース手順の改訂（`release/` → `main` をスカッシュ、手順 6 で `main` を `develop` へ通常マージで戻す運用を明記）

## [1.0.0] - 2026-11-20

最初のリリースです。決定の経緯は Issue #1〜#7（壁打ち・性能検証）と ADR-002〜005 を参照してください。

### 追加

- **カーネル（`src/storage_kernel/`）**
  - 追記専用の書き込み `append`: 冪等キー、`payload_hash` による衝突検知、パーティション単位の単調増加 `seq`、64 KB のペイロード上限
  - 差分同期 `sync`: 複数シャード横断、読み取り時の Upcast
  - 配信: `subscribe`（At-Most-Once）、`register_projection`（At-Least-Once、リトライ、Dead Letter、一時停止と再開、定期ポーリングでの追いつき）
  - `write_snapshot`（手動のスナップショット）、`__snapshot__` の予約
  - パーティション状態（`active` / `read_only`）、メンテナンスモード、`close()` の段階的停止
  - 版管理: `schema_version` と `PayloadUpcaster`
- **参照実装・検証キット**
  - `contrib/sqlite/`: SQLite の参照 DAO（WAL + `synchronous=FULL`）
  - `testing/`: In-Memory 実装、Contract Test（13 項目）、障害注入
- **ドキュメント**: 基本設計書、IF/API 仕様書、データ構造仕様書、詳細設計書 4 件、環境・開発基盤仕様書、ADR-002〜005、運用手順書、性能検証結果報告書
- **接続例**: `examples/parent_app/`
- **CI**: ruff・mypy・テスト、文書検証、Mermaid 検証、ミューテーションテスト（手動実行）

### 変更

- 該当なし（初版）

### 既知の制約

- v1 は単一プロセス（マルチスレッド）のみ対応します。別プロセスから DB へ直接書き込まないことは、親プロジェクトが守る制約です。
- `catchup_interval_sec` は `ProjectionRunner` の引数で、`StorageKernel` と `KernelConfig` からは変更できません（既定 5.0 秒）。
- `DBF-DD-004` の F-402 に、実在しない名前 `ProjectionRunner.pause_or_drain` の記述が残っています（1.0.1 で修正済み）。

### v2 以降に回したもの

- パーティション移動（`migrating` 状態、`Migratable*Port`）
- `BlobStorePort` による大容量ペイロードの退避
- スナップショットの自動生成、Retention、VACUUM・チェックポイントの自動実行
- `append_batch()`
- IPC 配信口（`ports/transport.py`）
- `ephemeral`・`guard`
