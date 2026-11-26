# storage_kernel

DAO（テーブル構造・SQL・DB ドライバー）を親プロジェクトへ委譲する、Python 標準ライブラリだけで動く汎用データ基盤のカーネルです。Git サブモジュールとして親プロジェクトへ組み込み、書き込み・同期・配信・版管理を統一された API で扱えます。

- **対象**: 単一マシン上で、複数モジュールが協調するローカルアプリ（単一プロセス・マルチスレッド）
- **Python**: 3.12 以上（CI で検証しているのは 3.12 です）
- **外部依存**: なし（pytest・ruff・mypy などは開発用）
- **ライセンス**: Apache-2.0（[`LICENSE`](./LICENSE)）

## 提供する機能（v1）

| 機能               | 内容                                                                                                                |
| :----------------- | :------------------------------------------------------------------------------------------------------------------ |
| 追記専用の書き込み | `append`。冪等キー（`payload_hash` の一致確認）、パーティション単位の単調増加 `seq`、書き込み確定後に ACK           |
| 差分同期           | `sync`。カーソルからの差分を、複数シャードを横断して取得。読み取り時に Upcast を適用                                |
| 配信               | `subscribe`（取りこぼしを許容する通知）、`register_projection`（取りこぼさない投影。リトライ、DLQ、一時停止と再開） |
| スナップショット   | `write_snapshot`（親が作った内容を `__snapshot__` として保存。自動生成と Retention は v2）                          |
| 運用               | `enter_maintenance` / `exit_maintenance`（バックアップの整合点）、`close`（段階的な停止）                           |
| 版管理             | `schema_version` と `PayloadUpcaster`（バイト列で 1 版ずつ変換）                                                    |

親プロジェクトは、DAO（`ShardDAOPort`・`MetaDAOPort` など）と `PayloadCodec` を実装して `StorageKernel` に注入します。DAO が基盤の不変条件を満たすかは、`storage_kernel.testing.contract_test.StorageKernelContractTest` を継承して検証します。

## 構成

```text
src/storage_kernel/
├── contracts/     契約型・例外（DAO の中身に依存しない）
├── ports/         親プロジェクトが実装する差し替え口（DAO・Codec・Router・Hooks）
├── core/          書き込み・同期・配信・Projection の制御フロー
├── runtime/       排他制御・メンテナンス・停止
├── testing/       In-Memory 実装・Contract Test・障害注入（検証用。本番では使わない）
└── contrib/sqlite/  SQLite の参照 DAO 実装（core から import しない）
examples/
├── parent_app/        親プロジェクトの接続例（BLOB 保存）
├── columnar_dao/      業務カラムへ展開して保存する DAO の例（SQLite）
└── postgres_dao/      PostgreSQL による試作 DAO の例
benchmarks/           性能検証のスクリプト
tests/                unit / contract / boundary
docs/                 設計書・ADR・運用手順・検証結果
```

依存の向きは、親プロジェクト → `ports`・`contracts` ← `core`・`runtime` です。`runtime` は `core` を import せず、`contrib` と `testing` は `core` から参照されません（境界テストで検査しています）。

## 使い方

親プロジェクトへの組み込み（サブモジュール、タグ固定）と DI の組み立て例は、[`examples/parent_app/README.md`](./examples/parent_app/README.md) を参照してください。

## 開発

```powershell
# 依存の同期（uv を採用）
uv sync --dev

# 静的解析・型検査
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync mypy .

# テスト（unittest ベースのため、pytest なしでも `python -m unittest` で実行できます）
uv run --no-sync pytest
```

品質ゲート（実行条件と合格基準）の正本は、[`docs/setup/DBF-ENV-001_環境・開発基盤仕様書.md`](./docs/setup/DBF-ENV-001_環境・開発基盤仕様書.md) です。

### ドキュメント検証

```powershell
python .github/scripts/validate_docs.py docs README.md
```

Mermaid 図を検証する場合は、Node.js が必要です。

```powershell
node .github/scripts/validateMermaidMarkdown.mjs --all
```

## ドキュメント

| 文書                              | 場所                                                                                                                           |
| :-------------------------------- | :----------------------------------------------------------------------------------------------------------------------------- |
| 基本設計書（BD）                  | [`docs/design/DBF-BD-001_基本設計書.md`](./docs/design/DBF-BD-001_基本設計書.md)                                               |
| IF/API 仕様書（公開 API と Port） | [`docs/design/DBF-IF-001_IF仕様書.md`](./docs/design/DBF-IF-001_IF仕様書.md)                                                   |
| データ構造仕様書（DS）            | [`docs/design/DBF-DS-001_データ構造仕様書.md`](./docs/design/DBF-DS-001_データ構造仕様書.md)                                   |
| 詳細設計書（DD）                  | [`docs/design/`](./docs/design/)（`DBF-DD-001`〜`004`）                                                                        |
| 判断理由（ADR）                   | [`docs/adr/`](./docs/adr/)（`ADR-002`〜`005`）                                                                                 |
| 運用手順（バックアップと復旧）    | [`docs/how-to/DBF-OP-001_バックアップと復旧の運用手順書.md`](./docs/how-to/DBF-OP-001_バックアップと復旧の運用手順書.md)       |
| 性能検証結果                      | [`docs/test/DBF-TEST-001_性能検証結果報告書.md`](./docs/test/DBF-TEST-001_性能検証結果報告書.md)                               |
| 文書ガバナンス・Git Flow          | [`docs/README.md`](./docs/README.md)、[`docs/setup/git_flow_and_branch_policy.md`](./docs/setup/git_flow_and_branch_policy.md) |
| 変更履歴                          | [`CHANGELOG.md`](./CHANGELOG.md)                                                                                               |

## v1 に含まないもの（v2 以降）

パーティション移動、大容量ペイロードの退避（`BlobStorePort`）、スナップショットの自動生成と Retention、`append_batch()`、複数プロセス・IPC 対応、`ephemeral`・`guard`。
