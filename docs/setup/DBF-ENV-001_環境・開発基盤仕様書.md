---
title: "環境・開発基盤仕様書"
document_type: "environment_specification"
document_number: "DBF-ENV-001"
document_role: "normative"
ssot_scope: "environment"
status: "draft"
version: "1.0"
created_at: "2026-11-19"
updated_at: "2026-11-19"
author: "xzyozi"
owner: "xzyozi"
purpose: "storage_kernel の開発・検証・実行環境について、変更できない条件、必要なツールチェーン、品質ゲート（QG-001〜QG-006）、およびサブモジュール配布仕様を定義するため"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "Python プロジェクト環境構築手順書（相対パス: ./python_setup.md）"
  - "ドキュメント検証スクリプト運用（相対パス: ./doc_validation.md）"
  - "Mermaid CI 検証運用（相対パス: ./mermaid_ci_validation.md）"
  - "Git Flow 運用規約 ＆ ブランチ自動管理ガイド（相対パス: ./git_flow_and_branch_policy.md）"
  - "ADR-002 DAO委譲とPython標準ライブラリによる独立カーネル構成（相対パス: ../adr/ADR-002_DAO委譲とPython標準ライブラリによる独立カーネル構成.md）"
  - "DBF-BD-001 基本設計書（相対パス: ../design/...）"
---

# 環境・開発基盤仕様書
**storage_kernel 実行・開発環境および品質ゲート定義（文書番号: DBF-ENV-001）**

---

## 1. 目的・対象範囲

- **目的**: `storage_kernel` を親プロジェクトへ Git サブモジュールとして安全に組み込み、外部依存ゼロの純粋 Python パッケージとして開発・検証・配布するための環境制約と品質ゲート（`QG-001`〜`QG-006`）を定義する。
- **対象環境**: ローカル開発環境、GitHub Actions CI 環境、および親プロジェクトへのサブモジュール組み込み実行環境。
- **対象範囲**: Python ランタイム要件、依存管理（`uv` / `pyproject.toml`）、品質ゲート、Contract Test の実行条件、性能試験の扱い、サブモジュール運用とライセンス条件。
- **対象外**:
  - 具体的な環境構築コマンド手順（[`docs/setup/python_setup.md`](./python_setup.md) を正本とする）。
  - 親プロジェクト側が導入する外部 DB エンジン（PostgreSQL、DuckDB 等）やサードパーティ製ドライバーの環境構築。

## 2. ハード要件・制約

- **必須プラットフォーム・ハードウェア**:
  - Python 3.10 以上が動作する OS（Linux / macOS / Windows）。
  - SQLite 参照実装（`contrib/sqlite/`）および単体テストの実行には、Python 標準ライブラリの `sqlite3` モジュールが有効であること。
- **互換性・契約・規制**:
  - **外部依存ゼロ制約**: パッケージ本体（`contracts/`・`ports/`・`core/`・`runtime/`・`testing/`・`contrib/sqlite/`）は Python 標準ライブラリのみで構成し、`[project] dependencies` は空リスト（`[]`）とする（Issue #1 A5、[`ADR-002`](../adr/ADR-002_DAO委譲とPython標準ライブラリによる独立カーネル構成.md)）。
  - **ライセンス**: Apache-2.0（ルート `LICENSE`）。親プロジェクト固有の DB ドライバーのライセンスは親側の Adapter 層へ隔離する（Issue #1 A5）。
  - **FFI 非採用**: Rust / C++ 等によるネイティブ拡張や FFI バインディングは使用しない（Issue #1 A5）。
- **ネットワーク・アクセス制約**:
  - カーネル実行時は外部ネットワーク通信を一切行わない（単一マシン・単一プロセス内のインプロセス呼び出し）。
- **その他の制約**:
  - `testing/contract_test.py` は親プロジェクトのテストランナーを拘束しないよう、サードパーティの `pytest` を import せず標準ライブラリの `unittest.TestCase` ベースで実装する（`pytest` からもそのまま収集・実行可能とする。Issue #1 A5）。

## 3. 環境概要

| 環境 | 用途 | プラットフォーム / 実行方式 | 所有者 | 更新・廃止方針 |
| :--- | :--- | :--- | :--- | :--- |
| ローカル開発環境 | 設計書編集、コード実装、単体テスト、Contract Test、手動ベンチマークの実行 | macOS / Linux / Windows 上のローカル Python 環境（`uv` による `.venv` 管理） | 各開発者 | `.python-version`（`3.12`）と `pyproject.toml` に追従する |
| CI 検証環境 | PR・ブランチ push 時の静的解析、型検査、テスト、文書・Mermaid 検証の自動実行 | GitHub Actions（`ubuntu-latest`、Python 3.12、Node.js 22.14.0） | リポジトリ管理者 | `.github/workflows/` の定義に従って自動プロビジョニングする |
| 親プロジェクト組み込み環境 | 親プロジェクトの `libs/storage_kernel` 等に Git サブモジュールとして配置して本番・テスト稼働 | 親プロジェクトの Python（`>=3.10`）同一プロセス空間 | 親プロジェクト責任者 | セマンティックバージョニングの Git タグ固定で更新する |

## 4. 適用する環境要素

### 4.1 言語・ランタイム・実行基盤

- **言語・最低バージョン**: Python `>=3.10`（`pyproject.toml` の `requires-python = ">=3.10"`）。
- **標準開発バージョン**: Python `3.12`（`.python-version` および GitHub Actions のデフォルトバージョン）。
- **型ヒント・パッケージマーカー**: `src/storage_kernel/py.typed` を配置し、PEP 561 準拠の型付きパッケージとして親プロジェクトの `mypy` から型検査可能にする。

### 4.2 ツールチェーン・依存関係

- **パッケージ・仮想環境管理**: `uv` を使用する。依存解決結果は `uv.lock` で管理する。
- **本番実行時依存（`dependencies`）**: なし（Python 標準ライブラリのみ）。
- **開発・検証専用依存（`[project.optional-dependencies] dev`）**:
  - `pytest >= 8.0.0`: テストランナー（`unittest` ベースのテストおよび Contract Test の実行にも使用）。
  - `ruff >= 0.4.0`: Linter および Formatter（`line-length = 120`、ルール `E`, `F`, `W`, `I`）。
  - `mypy >= 1.0.0`: 静的型チェッカー（`python_version = "3.10"`、strict 相当設定）。
- **ドキュメント検証ツール**:
  - `.github/scripts/validate_docs.py`: Python 標準ライブラリのみで動作する Frontmatter・リンク・コードフェンス検証スクリプト。
  - `@mermaid-js/mermaid-cli@11.4.2`: CI（`.github/workflows/validate-mermaid.yml`）上で Mermaid 図を SVG レンダリング検証するための CLI。

### 4.3 ビルド・パッケージング・配布

- **ビルドバックエンド**: `setuptools >= 61.0`（`setuptools.build_meta`）、`src/` レイアウト（`src/storage_kernel/`）。
- **配布・サブモジュール運用**:
  - 親リポジトリから Git サブモジュールとして参照し、リリース済みの Git タグ（`vMAJOR.MINOR.PATCH`）でコミットを固定する。
  - **セマンティックバージョニング規約**:
    - **Major**: `ports/` または `contracts/` の後方互換性を壊す変更（メソッド削除、引数・戻り値型の非互換変更、不変条件の強化）。
    - **Minor**: 後方互換性を保つ機能追加（`PayloadUpcaster` の追加、新規公開 API メソッドの追加、任意の引数追加など）。
    - **Patch**: `core/`・`runtime/`・`contrib/sqlite/` のバグ修正、内部性能改善、設計書・テストの修正。

### 4.4 CI/CD・品質ゲート

GitHub Actions により、`main` および `develop` ブランチへの `push` / `pull_request` 時に自動検証を実行する。品質ゲートは下表を正本とし、BD・DD・DS・IF・テスト文書からはゲート ID（`QG-001`〜`QG-006`）で参照する。

| ゲートID | 検証種別 | 実行コマンドまたはワークフロー | 適用条件 | 合格基準 |
| :--- | :--- | :--- | :--- | :--- |
| QG-001 | 静的解析（Lint） | `uv run --no-sync ruff check .`（`.github/workflows/ci-python.yml`） | Python コード変更時、および `main` / `develop` への PR・push 時 | 指摘エラー・警告が 0 件であること |
| QG-002 | コードフォーマット検査 | `uv run --no-sync ruff format --check .`（`.github/workflows/ci-python.yml`） | Python コード変更時、および `main` / `develop` への PR・push 時 | フォーマット未適用ファイルが 0 件であること |
| QG-003 | 静的型検査（Type Check） | `uv run --no-sync mypy .`（`.github/workflows/ci-python.yml`） | Python パッケージ実装後、`main` / `develop` への PR・push 時 | `pyproject.toml` の `[tool.mypy]`（strict 設定）のもとで型エラーが 0 件であること |
| QG-004 | 単体テスト・契約テスト・境界テスト | `uv run --no-sync pytest`（`.github/workflows/ci-python.yml`） | テスト存在時、`main` / `develop` への PR・push 時（In-Memory と SQLite 参照実装の双方で Contract Test 13項目を含む全テストを実行） | 全テストケースがパス（失敗 0 件）し、`core/` が `contrib/` や外部パッケージを import していないこと |
| QG-005 | ドキュメント規約検証 | `python .github/scripts/validate_docs.py docs README.md`（`.github/workflows/validate-docs.yml`） | `docs/**/*.md` または `README.md` の変更時 | エラーが 0 件（終了コード `0`）であること |
| QG-006 | Mermaid 図レンダリング検証 | `node .github/scripts/validateMermaidMarkdown.mjs`（`.github/workflows/validate-mermaid.yml`） | `docs/**/*.md` または `README.md` の変更時 | 変更された全 Mermaid コードブロックが構文エラーなく SVG にレンダリングできること |

#### 性能試験（ベンチマーク）の扱い

- Issue #7 で定義する大規模性能試験（1パーティション 10,000 / 100,000 / 300,000 件の連続書き込み、100パーティション分散書き込み、カーソル 0 からの全件再生、プロセス再起動後の再生）は、実行環境のディスク I/O（`fsync`）性能に強く左右されるため、**通常の CI の必須合否判定（品質ゲート）には含めない**（Issue #7）。
- CI（`QG-004`）では件数を絞った正確性テスト（`seq` の欠落・重複・逆転がないこと）のみを実行し、300,000 件規模のベンチマーク（`benchmarks/`）はリリース前または DAO 実装変更時の手動検証として実行し、測定結果をテスト報告および基本設計書（`DBF-BD-001`）へ記録する。

### 4.5 設定・秘密情報

- **設定の注入方針（DI徹底）**: `storage_kernel` は環境変数（`os.environ`）や外部設定ファイルを直接読み込まない。すべての設定値は親プロジェクトの `bootstrap.py` から `KernelConfig`（`schema_registry`, `max_payload_bytes`, `default_retry_policy` 等）としてコンストラクタ注入（DI）で受け取る。
- **秘密情報の扱い**: リポジトリ内（設計書、テストコード、設定ファイル）に認証情報、APIキー、本番DBパス、個人情報を一切保存しない。

### 4.6 再現性・サプライチェーン

- 開発用依存パッケージは `uv.lock` によりバージョンとハッシュを固定する。
- 本番ランタイムは標準ライブラリのみであるため、外部パッケージ由来のサプライチェーン攻撃リスクを持たない。
- Mermaid CLI は `.github/workflows/validate-mermaid.yml` 内で `@mermaid-js/mermaid-cli@11.4.2` にバージョンを完全固定して実行する。

## 5. 検証方法

| 確認項目 | 確認方法 | 合格条件 | 証跡 |
| :--- | :--- | :--- | :--- |
| ドキュメントメタデータ・リンク整合性 | `python3 .github/scripts/validate_docs.py docs README.md` を実行する | `エラー 0 件` が表示され終了コード `0` で完了すること | CI ジョブ `Validate docs` の実行ログ |
| Mermaid 図の構文・描画妥当性 | CI 上で `validateMermaidMarkdown.mjs` を実行する | 対象 Markdown 内の全 Mermaid 図がエラーなく変換されること | CI ジョブ `Validate Mermaid diagrams` の実行ログ |
| Python 静的解析・型検査・単体/契約テスト | `uv sync --extra dev` 後、`QG-001`〜`QG-004` の各コマンドを実行する | 全コマンドが終了コード `0` で完了すること | CI ジョブ `Python Checks` の実行ログ |
| 親プロジェクト DAO の契約適合性 | 親プロジェクト側で `ShardDAOContractTest` を継承したテストクラスを `unittest` または `pytest` で実行する | 13項目の Contract Test がすべてパスすること | 親プロジェクトのテスト実行ログ |

## 6. 例外・適用差異

現時点で解消が予定されているテンプレート初期状態との差異を下表に記録する（Issue #12、Issue #18）。

| ID | 対象 | 標準との差異 | 理由・制約 | 影響 | 代替対応 | 解除条件 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| EX-001 | `pyproject.toml` のメタデータ | `name = "my-awesome-project"`、`description` がテンプレート初期値のままになっている | 設計書策定を先行し、パッケージ雛形の実装コミットで一括更新する計画のため | 設計書のみの変更PRでは影響なし。パッケージ import 時には名称不一致となる | 設計フェーズでは現状維持とする | 実装フェーズの最初（雛形とビルド設定 Issue）で `name = "storage-kernel"` および `src` レイアウトへ更新して解除する |
| EX-002 | `.github/workflows/ci-python.yml` の `mypy .` 実行 | リポジトリ内に `.py` ファイルが存在しない状態（または対象ディレクトリ未指定時）で `mypy .` が失敗する可能性がある | まだ `src/storage_kernel/` 配下の Python ファイルを作成する前の設計フェーズであるため | Python CI 実行時に `There are no .py[i] files in directory '.'` となる場合がある | 設計書専用ブランチでは `validate-docs.yml` と `validate-mermaid.yml` で品質を担保する | 実装フェーズの最初（雛形とビルド設定 Issue）で `src/storage_kernel/__init__.py` 等を追加するかワークフロー条件を調整して解除する |

## 7. 関連文書

- **構築手順**: [`docs/setup/python_setup.md`](./python_setup.md)、[`docs/setup/doc_validation.md`](./doc_validation.md)、[`docs/setup/mermaid_ci_validation.md`](./mermaid_ci_validation.md)
- **設計書**: 基本設計書 `DBF-BD-001`、IF/API仕様書 `DBF-IF-001`、データ構造仕様書 `DBF-DS-001`、詳細設計書 `DBF-DD-001`〜`004`
- **運用・テスト**: [`docs/setup/git_flow_and_branch_policy.md`](./git_flow_and_branch_policy.md)

## 8. 作成完了チェック

- [x] `document_role`、`ssot_scope`、`status`、`owner`をFrontmatterへ設定した。
- [x] 目的、対象範囲、対象外、ハード要件・制約を記載した。
- [x] 必要な環境要素だけを選択し、不要な項目を無理に追加していない。
- [x] 品質ゲートを採用する場合は、ゲートID・実行方法・適用条件・合格基準を4.4に記載した。
- [x] 検証方法と合格条件を記載した。
- [x] 標準から外れる場合は、理由・影響・代替対応・解除条件を記録した。
- [x] セットアップ手順書、設計書、運用・テスト文書への参照を設定した。
- [x] 秘密情報や利用者固有の認証情報を記載していない。

## 9. 改訂履歴 (Change Log)

| 版数    | 改訂日     | 変更者 | 変更内容・変更理由 (Why)                                       |
| :------ | :--------- | :----- | :------------------------------------------------------------- |
| Rev.1.0 | 2026-11-19 | xzyozi | 新規作成（Issue #12 に基づき環境仕様と品質ゲート QG-001〜006 を制定） |
