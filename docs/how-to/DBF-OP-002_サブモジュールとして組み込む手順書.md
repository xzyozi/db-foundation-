---
title: "運用手順書（サブモジュールとして組み込む）"
document_type: "operation_procedure"
document_number: "DBF-OP-002"
document_role: "procedure"
ssot_scope: "operation"
status: "approved"
version: "1.0"
created_at: "2026-11-27"
updated_at: "2026-11-27"
author: "xzyozi"
owner: "xzyozi"
purpose: "storage_kernel を親プロジェクトへ Git サブモジュールとして組み込み、タグで固定し、更新し、組み立てるまでの標準手順を定義するため"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "DBF-ENV-001 環境・開発基盤仕様書（相対パス: ../setup/DBF-ENV-001_環境・開発基盤仕様書.md）"
  - "DBF-IF-001 IF仕様書（相対パス: ../design/DBF-IF-001_IF仕様書.md）"
  - "DBF-OP-001 バックアップと復旧の運用手順書（相対パス: ./DBF-OP-001_バックアップと復旧の運用手順書.md）"
  - "ADR-002 DAO委譲とPython標準ライブラリによる独立カーネル構成（相対パス: ../adr/ADR-002_DAO委譲とPython標準ライブラリによる独立カーネル構成.md）"
---

# 運用手順書
**storage_kernel をサブモジュールとして組み込む手順（文書番号: DBF-OP-002）**

---

## 1. 目的・対象範囲

- **目的**: 親プロジェクトが `storage_kernel` を Git サブモジュールとして組み込み、リリース済みのタグで固定し、更新し、`StorageKernel` を組み立てて動かすまでの手順を定める。
- **対象**: 親プロジェクトの開発者・運用担当者。
- **対象外**:
  - バージョンの決め方・配布の仕様（正本は [`DBF-ENV-001`](../setup/DBF-ENV-001_環境・開発基盤仕様書.md) 4.3 節）
  - 公開 API・Port の契約（正本は [`DBF-IF-001`](../design/DBF-IF-001_IF仕様書.md)）
  - バックアップと復旧（[`DBF-OP-001`](./DBF-OP-001_バックアップと復旧の運用手順書.md)）
  - 親プロジェクト独自の DAO の実装（契約と Contract Test は IF を参照。実装例は 6 章）

## 2. 前提条件・注意事項

- **Python**: 3.12 以上（`pyproject.toml` の `requires-python = ">=3.12"`。CI で検証しているのは 3.12 のみ）。
- **外部依存**: `storage_kernel` の実行時の依存は、**Python 標準ライブラリのみ**（ゼロ）。親プロジェクトの依存と衝突しない。
- **使うバージョン**: リリース済みの Git タグ（`vMAJOR.MINOR.PATCH`）に固定する。`develop` や `main` の先頭を直接追わない。
- **ライセンス**: Apache-2.0（リポジトリの `LICENSE`）。
- **使ってはいけないもの**: `storage_kernel.testing`（In-Memory 実装、テスト用のコーデック・ルーター、障害注入）は、**DAO の検証用**で、`durability="none"` の実装を含む。親プロジェクトの本番の構成では、使わない。
- **検証の状況**: この手順書の、サブモジュールの追加・タグ固定・`PYTHONPATH` での利用は、実際に動かして確認している（4 章の注記を参照）。`uv add --editable` / `pip install -e` による組み込みは、**未検証**である。

## 3. 手順

### 3.1 サブモジュールとして追加し、タグで固定する

| No.  | 操作・確認内容                                                                                                              | 期待結果                                                                                                             | 異常時の対応                                                                                      |
| :--- | :-------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------ |
| 1    | 親リポジトリのルートで、サブモジュールを追加する。<br>`git submodule add <storage_kernelリポジトリURL> libs/storage_kernel` | `libs/storage_kernel/` に、リポジトリの内容が展開され、`.gitmodules` が作られる                                      | URL が間違っている、または認証に失敗する場合は、URL と権限を確認する                              |
| 2    | 使うバージョンのタグを確認する。<br>`git -C libs/storage_kernel tag -l`                                                     | `v1.0.1` などの、リリース済みのタグが表示される                                                                      | タグが表示されない場合は、`git -C libs/storage_kernel fetch --tags` を実行する                    |
| 3    | タグに固定する。<br>`git -C libs/storage_kernel checkout v<バージョン>`                                                     | サブモジュールの `HEAD` が、タグのコミットを指す（detached HEAD になる。想定どおり）                                 | タグ名が存在しない場合は、手順 2 に戻る                                                           |
| 4    | 親リポジトリで、固定したコミットを記録する。<br>`git add .gitmodules libs/storage_kernel` → コミット                        | 親リポジトリが、サブモジュールの**コミットハッシュ**を記録する（`git submodule status` に `(v<バージョン>)` が付く） | `git submodule status` の先頭が `+` なら、記録したコミットと作業ツリーが異なる。手順 3 を確認する |

### 3.2 Python から使えるようにする

親プロジェクトの Python から `import storage_kernel` できるようにする。パッケージは `libs/storage_kernel/src/storage_kernel/` にある（`src` レイアウト）。

**方法 A: `PYTHONPATH`（動作を確認済み）**

`PYTHONPATH` に、`libs/storage_kernel/src` を追加する。

```bash
# Linux / macOS
export PYTHONPATH="$PWD/libs/storage_kernel/src:$PYTHONPATH"
```

```powershell
# Windows (PowerShell)
$env:PYTHONPATH = "$PWD\libs\storage_kernel\src;$env:PYTHONPATH"
```

確認: `python -c "import storage_kernel; print(storage_kernel.__version__)"` で、固定したバージョンが表示される。

**方法 B: editable インストール（未検証）**

`uv add --editable ./libs/storage_kernel`、または `pip install -e ./libs/storage_kernel`。`pyproject.toml` は `setuptools` の `src` レイアウトで、パッケージを検出する設定（`[tool.setuptools.packages.find] where = ["src"]`）を持つが、この手順での組み込みは、検証していない。依存のインストールを伴うため、試す場合は、親プロジェクトの環境で、結果を確認すること。

### 3.3 親プロジェクトで `StorageKernel` を組み立てる

親プロジェクトが用意するもの（最小構成）は、次の 3 つである。

| 部品                                 | 内容                                                                                    | 親が自前で書くか           |
| :----------------------------------- | :-------------------------------------------------------------------------------------- | :------------------------- |
| `PayloadCodec`                       | ペイロードを `bytes` に変換する。**同じ値から、常に同じバイト列を返す（決定論的）こと** | 書く（約 10 行）           |
| `PartitionRouterPort`                | `partition_key` から、シャードを決める                                                  | 書く（約 6 行）            |
| DAO（`ShardDAOPort`・`MetaDAOPort`） | 永続化。SQLite でよければ、参照実装 `storage_kernel.contrib.sqlite` を使える            | 参照実装を使えば、書かない |

**コードは、この手順書に載せず、動く例を参照する**（二重管理の防止）。

- 最小の組み立て: [`examples/parent_app/bootstrap.py`](../../examples/parent_app/bootstrap.py)、部品の例は [`examples/parent_app/adapters.py`](../../examples/parent_app/adapters.py)
- `store_raw_bytes` と DAO の組み合わせの選び方: [`DBF-IF-001`](../design/DBF-IF-001_IF仕様書.md) §2.5.1
- 全体の説明: [`examples/parent_app/README.md`](../../examples/parent_app/README.md)

### 3.4 更新する（新しいタグへ上げる）

| No.  | 操作・確認内容                                                                                                                                                 | 期待結果                                                       | 異常時の対応                                                                                                         |
| :--- | :------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------- |
| 1    | 変更内容を確認する。`CHANGELOG.md` で、上げたいバージョンの節を読む。**Major**（`ports/`・`contracts/` の破壊的変更）の場合は、親の DAO などの修正が必要になる | 影響の有無が分かる                                             | Major の場合は、`DBF-IF-001` を読み、親の実装を直してから、上げる                                                    |
| 2    | サブモジュールに、新しいタグを取り込む。<br>`git -C libs/storage_kernel fetch --tags`                                                                          | 新しいタグが、`git -C libs/storage_kernel tag -l` に表示される | ネットワークや認証を確認する                                                                                         |
| 3    | 新しいタグへ切り替える。<br>`git -C libs/storage_kernel checkout v<新バージョン>`                                                                              | `HEAD` が新しいタグを指す                                      | —                                                                                                                    |
| 4    | **親の Contract Test（自作 DAO がある場合）と、親のテストを実行する**                                                                                          | すべて合格する                                                 | 失敗した場合は、`CHANGELOG.md` と `DBF-IF-001` を見て、原因を調べる。直らなければ、旧タグへ戻す（手順 3 を旧タグで） |
| 5    | 親リポジトリで、新しいコミットを記録する。<br>`git add libs/storage_kernel` → コミット                                                                         | 親が、新しいコミットハッシュを記録する                         | —                                                                                                                    |

### 3.5 他の人・CI が、サブモジュールを取得する

サブモジュールは、通常の `git clone` では、**中身が取得されない**（空のディレクトリになる）。

| 場面                                                          | 操作                                                                             |
| :------------------------------------------------------------ | :------------------------------------------------------------------------------- |
| 新しく clone する                                             | `git clone --recurse-submodules <親リポジトリURL>`                               |
| すでに clone 済み                                             | `git submodule update --init --recursive`                                        |
| 親が、サブモジュールのコミットを更新した後（`git pull` の後） | `git submodule update --init --recursive`                                        |
| GitHub Actions                                                | `actions/checkout` の `with: submodules: true`（このリポジトリの CI も、この形） |

## 4. 動作確認の記録

この手順書の記述は、2 回の実験で確認した（Windows、Python 3.12）。

**調査（Issue #52、対象 `v1.0.0`）**

- 一時ディレクトリに親プロジェクトを作り、`git submodule add` で追加し、タグ `v1.0.0` に固定した。
- `PYTHONPATH` に `libs/storage_kernel/src` を指定して、`append`、冪等な再送（`status="duplicate"`、同じ `seq`）、`sync`、`close()` が期待どおりに動いた。
- `storage_kernel`、`contrib.sqlite`、`testing` をすべて import した後に読み込まれた外部ライブラリは、なかった（標準ライブラリのみ）。
- 親が自前で書いたコードは、約 23 行（コーデック、ルーター、`StorageKernel` の組み立て）だった。

**この手順書の作成時（対象 `v1.0.1`）**

- 3.1: `git submodule add` → タグ一覧（`v1.0.0`・`v1.0.1`）→ `v1.0.1` へ固定 → コミットで記録。`git submodule status` に `(v1.0.1)` が付いた。
- 3.2（方法 A）: `PYTHONPATH` で `import storage_kernel` でき、バージョンは `1.0.1`。最小の親プロジェクトが、`append`・冪等な再送・`sync`・`close()` を通して動いた。
- 3.5: `git clone --recurse-submodules` で、中身が取得された。**通常の `git clone` では**、`libs/storage_kernel` が空のディレクトリになり、`git submodule status` の先頭が `-` になった。`git submodule update --init --recursive` で取得でき、`status` の先頭が空白（記録したコミットと一致）になった。
- 5 章の異常時: `PYTHONPATH` なしの `import storage_kernel` は、`ModuleNotFoundError: No module named 'storage_kernel'` になった。

**未検証**: `uv add --editable` / `pip install -e` による組み込み、3.4 の更新手順（新しいタグへの切り替え）、Python 3.12 以外のバージョン、Linux・macOS、GitHub Actions での `submodules: true`（このリポジトリの CI は、この指定だが、親プロジェクトの CI で試していない）。

## 5. 異常時の対応

| 症状                                                    | 原因の候補                                                                                                           | 対応                                                                                                                                           |
| :------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------- |
| `ModuleNotFoundError: No module named 'storage_kernel'` | `PYTHONPATH` に `libs/storage_kernel/src` が入っていない。またはサブモジュールの中身が空（clone 時に取得していない） | `libs/storage_kernel/src/storage_kernel/` が存在するか確認し、なければ `git submodule update --init --recursive`。次に `PYTHONPATH` を確認する |
| `git submodule status` の先頭が `-`                     | サブモジュールが初期化されていない                                                                                   | `git submodule update --init --recursive`                                                                                                      |
| `git submodule status` の先頭が `+`                     | サブモジュールの `HEAD` が、親が記録したコミットと違う                                                               | 意図した変更なら、親でコミットする。そうでなければ、`git submodule update` で、記録したコミットへ戻す                                          |
| 更新後、親の DAO や Contract Test が失敗する            | Major の変更（`ports/`・`contracts/` の非互換な変更）が入っている                                                    | `CHANGELOG.md` と `DBF-IF-001` を確認して直す。または、旧タグに戻す                                                                            |
| `SyntaxError` や型の構文エラー                          | Python が 3.12 未満                                                                                                  | 対応するのは、3.12 以上（`requires-python = ">=3.12"`）                                                                                        |

## 6. 関連文書

- **DAO を自作する場合**: 契約は [`DBF-IF-001`](../design/DBF-IF-001_IF仕様書.md)、検証は `StorageKernelContractTest`（`storage_kernel.testing.contract_test`、13 項目）。実装例は次のとおり。
  - [`examples/columnar_dao`](../../examples/columnar_dao/README.md): ペイロードを業務カラムへ展開する SQLite の DAO
  - [`examples/postgres_dao`](../../examples/postgres_dao/README.md): PostgreSQL の DAO（サーバー型 DB でも成立するかを検証した試作。本番の品質ではない）
- **バージョンと配布の仕様**: [`DBF-ENV-001`](../setup/DBF-ENV-001_環境・開発基盤仕様書.md) 4.3 節
- **バックアップと復旧**: [`DBF-OP-001`](./DBF-OP-001_バックアップと復旧の運用手順書.md)
- **この構成にした理由**: [`ADR-002`](../adr/ADR-002_DAO委譲とPython標準ライブラリによる独立カーネル構成.md)

## 7. 作成完了チェック

- [x] 目的、対象範囲、対象外を記載した。
- [x] 前提条件（Python のバージョン、依存、使ってはいけないもの）を記載した。
- [x] 手順を、操作・期待結果・異常時の対応の表で記載した。
- [x] 検証済みと未検証を、区別して記載した。
- [x] 正本のある内容（バージョンの決め方、IF、コード例）は、再掲せず参照した。
- [x] 機密情報や認証情報を記載していない。

## 8. 改訂履歴 (Change Log)

| 版数    | 改訂日     | 変更者 | 変更内容・変更理由 (Why)                                                           |
| :------ | :--------- | :----- | :--------------------------------------------------------------------------------- |
| Rev.1.0 | 2026-11-27 | xzyozi | 新規作成（サブモジュールとしての追加・タグ固定・利用・更新・取得の標準手順を制定） |
