# Git Flow 運用規約 ＆ ブランチ自動管理ガイド

本ドキュメントは、プロジェクトにおける **Git Flow 運用ルール**、および **GitHub / CI/CD / ローカル Git** を活用したブランチの自動クリーンアップ・管理仕様を定義します。

---

## 1. ブランチ戦略 (Git Flow 概要)

### 主要ブランチ (Long-lived Branches)
- **`main`**: 本番環境用ブランチ。常時デプロイ可能・安定したコードを保持します。
- **`develop`**: 開発用メインブランチ。最新の開発成果が集約されます。

### サポートブランチ (Short-lived Branches)
- **`feat/<機能名>`**: 新機能開発用ブランチ。`develop` から分岐し、`develop` へ PR を作成してマージします。
- **`fix/<修正内容>`**: バグ修正用ブランチ。`develop` から分岐し、`develop` へマージします。
- **`docs/<ドキュメント名>`**: ドキュメント更新専用ブランチ。
- **`hotfix/<緊急修正>`**: 本番の急激な不具合修正用。`main` から分岐し、`main` および `develop` へマージします。
- **`release/v<MAJOR.MINOR.PATCH>`**: リリース準備用。`develop` から分岐し、`main` へマージします（手順は「5. リリース手順」）。

---

## 2. CI/CD ＆ リモート自動化 (GitHub / GitHub Actions)

不要なリモートブランチが残留しないよう、GitHub 標準機能で自動クリーンアップします。

### GitHub 設定: PRマージ後のリモートブランチ自動削除
- GitHubの管理画面 (`Settings` > `General` > `Pull Requests`) にて **`Automatically delete head branches`** を有効化。
- PRが `develop` または `main` にマージされた時点で、リモートの作業ブランチが自動削除されます。

> 定期クリーンアップ用の GitHub Actions ワークフロー (`branch-cleanup.yml`) は廃止しました。
> コミット前の作成直後ブランチや、オープン中の PR の head ブランチを誤って削除するリスクがあったためです。

---

## 3. ローカル Git 開発環境の自動化設定

リモートで削除されたブランチがローカル環境に古い追跡情報 (`[gone]`) として残るのを防ぐため、以下の設定を推奨します。

### ① `git fetch` への自動 Prune 設定
以下のコマンドを実行すると、`git fetch` や `git pull` を行うたびに、リモートで消えたブランチのローカル追跡情報を自動的に削除します。

```bash
git config --global fetch.prune true
```

### ② ローカル `[gone]` ブランチを一括削除する Git エイリアス
追跡元リモートブランチが消えたローカルブランチを一括で整理・削除するコマンドを追加します。

```bash
# エイリアスの設定 (PowerShell / Git Bash 共通)
git config --global alias.cleanup "!git branch -vv | grep '\[gone\]' | awk '{print $1}' | xargs -r git branch -d"
```

#### 使い方
```bash
git cleanup
```
実行すると、リモートで既にマージ・削除されたローカルブランチが一括で安全に削除されます。

---

## 4. コミットメッセージ規約

コミットメッセージは `[種別] 概要` の形式にします。概要は日本語でかまいません。

| 種別      | 用途                             | 例                                |
| :-------- | :------------------------------- | :-------------------------------- |
| `[feat]`  | 新機能・性能検証・実装の追加     | `[feat] 配信と Projection を実装` |
| `[fix]`   | バグ修正・テストや設定の修正     | `[fix] NULLチェック漏れの修正`    |
| `[docs]`  | ドキュメントの追加・更新         | `[docs] 基本設計書を追加`         |
| `[other]` | 上記以外（設定、依存、整備など） | `[other] .gitignore 更新`         |

- 種別はこの 4 種に揃えます。`[chore]`・`[perf]` などは使いません。
- この規約は運用の約束として扱い、CI では検査しません（#43 の判断）。マージ済みの履歴は書き換えません。
- Issue に関連するコミットは、メッセージの末尾に `(#番号)` を付けます。

---

## 5. リリース手順

リリースは `develop` から `release/v<MAJOR.MINOR.PATCH>` ブランチを切り、`main` へ PR を出して行います。`main` へは**マージコミット**で取り込み、スカッシュしません。

### 方針（マージコミットにする理由）

- 観点ごとに分けたコミットと `(#番号)` による Issue との対応を、`main` にも残すためです。
- スカッシュすると、`main` と `develop` の履歴がつながらず、次のリリースで前回の変更が差分に再び現れ、衝突の原因になります。マージコミットなら、この問題は起きません。

### 手順

1. `develop` を最新にして、`release/v<バージョン>` を切る。
2. リリース準備のコミットを入れる（観点ごとに分ける）。
   - `pyproject.toml`・`uv.lock`・`src/storage_kernel/__init__.py` の `__version__` を同じ値にする。
   - `CHANGELOG.md` に、そのバージョンの節を追加する。
   - 設計書の状態（`status`）を、`approved` に揃える（Frontmatter・改訂履歴を合わせる）。
   - ルートの `README.md` が実態と合っていることを確認する。
3. ローカルで品質ゲート（`DBF-ENV-001` の QG-001〜006）を通す。
4. `release/v<バージョン>` → `main` の PR を作り、CI が通ったら、**マージコミット**でマージする。
5. `main` に、`v<バージョン>` のタグ（注釈付き）を付けて push する。
6. **`main` を `develop` へ取り込む**（リリース準備のコミットと、`main` のマージコミットを `develop` へ戻すため）。`release/` ブランチではなく `main` を元にすると、`main` のマージコミットが確実に含まれ、履歴がつながる。
   - `develop` にリリース準備後の新しいコミットがなければ、**早送り**（`git merge --ff-only origin/main`）で `develop` と `main` が同じ位置になる。余分なマージコミットは作らない。
   - `develop` に新しいコミットがある場合は、`main` → `develop` の PR を、マージコミットで入れる。
7. マージ済みの `release/` ブランチを削除する。

### タグ

```bash
git switch main && git pull --ff-only
git tag -a v1.0.0 -m "storage_kernel v1.0.0"
git push origin v1.0.0
```

親プロジェクトは、このタグでサブモジュールを固定します（`examples/parent_app/README.md` を参照）。

### バージョンの決め方

- `ports/` と `contracts/` の変更（公開契約の破壊的変更）は Major、後方互換な機能追加は Minor、不具合修正は Patch とします。
- `core/`・`runtime/` のバグ修正と性能改善は、親プロジェクトが `git submodule update` だけで取り込める状態を保ちます。
