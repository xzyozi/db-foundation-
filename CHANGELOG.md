# 変更履歴

このファイルは、[Keep a Changelog](https://keepachangelog.com/ja/1.1.0/) の形式に準拠し、[セマンティックバージョニング](https://semver.org/lang/ja/) に従います。`ports/` と `contracts/` の変更は Major として扱います。

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
- `DBF-DD-004` の F-402 に、実在しない名前 `ProjectionRunner.pause_or_drain` の記述が残っています（`[未リリース]` で修正済み）。

### v2 以降に回したもの

- パーティション移動（`migrating` 状態、`Migratable*Port`）
- `BlobStorePort` による大容量ペイロードの退避
- スナップショットの自動生成、Retention、VACUUM・チェックポイントの自動実行
- `append_batch()`
- IPC 配信口（`ports/transport.py`）
- `ephemeral`・`guard`

## [未リリース]

### 修正

- `DBF-DD-004`（Rev.1.4）: F-402 の呼び出し先に残っていた実在しない名前 `ProjectionRunner.pause_or_drain` を、実装のメソッド名 `ProjectionRunner.enter_maintenance` に修正
- `docs/setup/git_flow_and_branch_policy.md`: リリース手順 6 を、`release/` ではなく `main` を `develop` へ取り込む形に修正（早送りで済む場合を明記）
