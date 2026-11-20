---
title: "テスト計画・結果報告書（性能検証・Issue #7）"
document_type: "test_plan_report"
document_number: "DBF-TEST-001"
document_role: "evidence"
ssot_scope: "test_evidence"
status: "approved"
version: "1.0"
created_at: "2026-11-20"
updated_at: "2026-11-20"
author: "xzyozi"
owner: "xzyozi"
purpose: "Issue #7 に基づき、storage_kernel と SQLite 参照実装（durability='power_loss'）を用いた単一・複数パーティション書き込み、カーソル 0 からの全件再生、およびプロセス再起動後再生の性能・正確性検証結果を記録するため"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "DBF-BD-001 基本設計書（相対パス: ../design/DBF-BD-001_基本設計書.md）"
  - "DBF-ENV-001 環境・開発基盤仕様書（相対パス: ../setup/DBF-ENV-001_環境・開発基盤仕様書.md）"
---

# テスト計画・結果報告書
**storage_kernel 性能検証結果報告書（Issue #7 / 文書番号: DBF-TEST-001）**

---

## 1. 目的・対象範囲

- **目的**: Issue #7 の定めに従い、`durability="power_loss"`（SQLite `WAL` + `synchronous=FULL`）の条件下で以下が成立するかを実測検証する。
  - 単件の `append()` で #1 A3 の暫定目標（定常 10〜50 writes/sec、バースト 500〜1,000 writes/sec）以上の書き込み量を処理できること。
  - 1 パーティション 300,000 件を保持でき、`seq` の欠落・重複・逆転が発生しないこと。
  - カーソル 0 からの `sync()` 全件再生が許容時間内に完了すること。
  - v1 で `append_batch()` および自動 Retention を省略する判断が妥当であること。
- **テスト対象**: `StorageKernel`（`core/`・`runtime/`）および SQLite 参照 DAO 実装（`contrib/sqlite/`）。
- **対象外**: 親プロジェクト固有の本番 SQL / PostgreSQL 等のドライバー性能、および 60 KB ペイロードでの 300,000 件 × 3 回実行（§6 参照）。
- **関連する要求・設計**: [`DBF-BD-001 基本設計書`](../design/DBF-BD-001_基本設計書.md)（`NFR-001`, `NFR-002`）、[`DBF-ENV-001 環境・開発基盤仕様書`](../setup/DBF-ENV-001_環境・開発基盤仕様書.md)、Issue #7、#6、#1 A3、#2 B1。

## 2. テスト戦略・合格基準

- [x] 回帰テスト（CI 向け小規模正確性テスト）
- [x] 性能・負荷テスト（手動ベンチマーク）
- [ ] TDD / characterization test / その他

- **選択理由**: 性能値は実行環境のディスク `fsync` 性能に依存するため、通常 CI では件数を絞った正確性テスト（`tests/unit/test_scale_correctness.py`）のみを必須実行し、数十万件規模の実測は独立したベンチマークスクリプト（`benchmarks/bench_storage_kernel.py`）で 3 回ずつ新規 DB 上で計測する（[`DBF-ENV-001`](../setup/DBF-ENV-001_環境・開発基盤仕様書.md) 4.4節）。
- **合格基準**:
  1. 300,000 件の書き込みとカーソル 0 からの全件再生がエラー 0 件で正常終了すること。
  2. 全ケースで `seq` の欠落・重複・逆転が 0 件であること。
  3. p50 / p95 / p99、スループット（writes/sec, reads/sec）、再生時間、最大メモリ使用量、DB サイズを記録すること。
  4. 測定結果をもとに許容値を [`DBF-BD-001`](../design/DBF-BD-001_基本設計書.md) へ反映すること。
- **適用する品質ゲート**: [`DBF-ENV-001`](../setup/DBF-ENV-001_環境・開発基盤仕様書.md) 4.4節（`QG-001`〜`QG-005`、CI 正確性テストは `QG-004`）。
- **使用する証跡**: `benchmarks/bench_storage_kernel.py` 実行出力および `pytest tests/unit/test_scale_correctness.py` 実行ログ。

## 3. テスト環境・前提条件

- **環境**:
  - **OS / Arch**: `macOS-14.3-arm64-arm-64bit`（CPU 8 cores / Apple Silicon SSD）
  - **Python**: `3.12.15`
  - **耐久性設定**: `durability="power_loss"`（`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;`）
- **テストデータ**:
  - **代表値 (`typical`)**: `{"index": i, "kind": "order_tick", "body": "x" * 440}`（決定論的 JSON エンコード後 **481 B**）
  - **上限付近 (`large`)**: `{"index": i, "kind": "large_blob", "body": "x" * 60000}`（決定論的 JSON エンコード後 **60,041 B** / 上限 64 KB 付近）
- **前提条件**: 各試行は新しい一時ディレクトリ上の新規 DB（`meta.db` + 4 シャード構成）で **3 回ずつ** 実行する。プロセス再起動後の再生は、DB 作成後に別サブプロセスを起動して実行する。
- **実行方法**:
  - 代表値 300,000 件: `uv run --no-sync python benchmarks/bench_storage_kernel.py --runs 3 --counts 10000 100000 300000 --multi-partitions 100 --multi-count 300000 --payload-modes typical`
  - 上限付近 100,000 件: `uv run --no-sync python benchmarks/bench_storage_kernel.py --runs 3 --counts 10000 100000 --multi-partitions 100 --multi-count 100000 --payload-modes large`

## 4. テストケース・計測結果

### 4.1 書き込みベンチマーク結果（各条件 3 回試行の中央値）

| ケースID | 対象・条件 | payload | サイズ | 試行回数 | writes/sec | p50 (ms) | p95 (ms) | p99 (ms) | 総時間 (s) | DBサイズ (MiB) | エラー | seq整合 | 判定 |
| :--- | :--- | :--- | ---: | :---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: | :---: |
| TC-PERF-001 | 単一パーティション 10,000 件 (`append`) | typical | 481 B | 3 | 2,099.9 | 0.452 | 0.578 | 0.729 | 4.76 | 7.24 | 0 | OK | Pass |
| TC-PERF-002 | 単一パーティション 100,000 件 (`append`) | typical | 481 B | 3 | 2,059.3 | 0.458 | 0.577 | 0.711 | 48.56 | 72.35 | 0 | OK | Pass |
| TC-PERF-003 | 単一パーティション 300,000 件 (`append`) | typical | 481 B | 3 | 1,758.3 | 0.486 | 0.785 | 2.133 | 170.62 | 217.44 | 0 | OK | Pass |
| TC-PERF-004 | 100 パーティション分散 300,000 件 (`append`) | typical | 481 B | 3 | 1,811.2 | 0.490 | 0.646 | 1.596 | 165.64 | 218.50 | 0 | OK | Pass |
| TC-PERF-005 | 単一パーティション 10,000 件 (`append`) | large | 60,041 B | 3 | 957.4 | 0.941 | 1.261 | 1.838 | 10.44 | 586.73 | 0 | OK | Pass |
| TC-PERF-006 | 単一パーティション 100,000 件 (`append`) | large | 60,041 B | 3 | 935.8 | 0.968 | 1.361 | 2.747 | 106.86 | 5,867.43 | 0 | OK | Pass |
| TC-PERF-007 | 100 パーティション分散 100,000 件 (`append`) | large | 60,041 B | 3 | 963.8 | 0.987 | 1.274 | 2.260 | 103.75 | 5,867.84 | 0 | OK | Pass |

### 4.2 全件再生ベンチマーク結果（カーソル 0 からの `sync(..., limit_per_partition=1000)`、各 3 回中央値）

| ケースID | 対象・条件 | payload | 件数 | 試行回数 | 再生時間 (s) | reads/sec | peak tracemalloc (MiB) | max RSS (MiB) | seq整合 | OSキャッシュ注記 | 判定 |
| :--- | :--- | :--- | ---: | :---: | ---: | ---: | ---: | ---: | :---: | :--- | :---: |
| TC-PERF-008 | 同一プロセス内全件再生 | typical | 300,000 | 3 | 6.194 | 48,430.1 | 3.33 | 51.56 | OK | 書き込み直後の同一プロセス（OS ページキャッシュ有効） | Pass |
| TC-PERF-009 | プロセス再起動後全件再生（サブプロセス） | typical | 300,000 | 3 | 5.730 | 52,354.3 | 3.33 | 38.47 | OK | 新規 Python プロセス（OS ページキャッシュはウォーム状態） | Pass |
| TC-PERF-010 | 同一プロセス内全件再生 | large | 100,000 | 3 | 18.337 | 5,453.5 | 173.79 | 574.83 | OK | 書き込み直後の同一プロセス（1,000件×60KB/バッチ展開） | Pass |
| TC-PERF-011 | プロセス再起動後全件再生（サブプロセス） | large | 100,000 | 3 | 21.746 | 4,598.6 | 173.79 | 597.72 | OK | 新規 Python プロセス（OS ページキャッシュはウォーム状態） | Pass |

## 5. 実施結果と考察

| 実行ID | 実行日 | 実行者・実行環境 | 実行方法 | 結果概要 | 証跡 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| RUN-001 | 2026-11-19 | xzyozi / macOS 14.3 arm64 | `benchmarks/bench_storage_kernel.py` (`typical` 300k × 3) | 全ケース正常完了・エラー 0 件・seq 欠落/重複/逆転なし | 本書 4.1〜4.2 節 |
| RUN-002 | 2026-11-19 | xzyozi / macOS 14.3 arm64 | `benchmarks/bench_storage_kernel.py` (`large` 100k × 3) | 全ケース正常完了・エラー 0 件・seq 欠落/重複/逆転なし | 本書 4.1〜4.2 節 |
| RUN-003 | 2026-11-19 | xzyozi / macOS 14.3 arm64 | `uv run --no-sync pytest tests/unit/test_scale_correctness.py` | 1 passed（CI 向け小規模正確性テスト） | `QG-004` |

- **総合判定**: **合格**
- **判定理由と設計へのフィードバック**:
  1. **書き込み性能**: `durability="power_loss"`（`synchronous=FULL`）の単件 `append()` のみで、代表サイズ（481 B）では **1,758〜2,100 writes/sec**、上限付近（60 KB）でも **935〜964 writes/sec** を記録した。これは #1 A3 の暫定目標（定常 10〜50 writes/sec、バースト 500〜1,000 writes/sec）を満たしており、v1 で `append_batch()` を省略しても問題ないことが確認された。
  2. **全件再生性能とメモリ**: 代表サイズ 300,000 件のカーソル 0 からの全件再生は **5.73〜6.19 秒**（約 48,000〜52,000 reads/sec）、Python トレースメモリピーク **3.33 MiB** に収まった。一方、60 KB レコードが連続する場合に `limit_per_partition=1000` で同期すると 1 バッチあたり約 60 MB を展開するためピークメモリが約 174 MiB（max RSS 約 598 MiB）となるため、大容量ペイロード主体のパーティションでは `limit_per_partition` を小さく（例: 100〜200）する運用が望ましい。
  3. **分散 vs 単一パーティション**: 300,000 件規模では単一パーティション（1,758.3 writes/sec）と 100 パーティション分散（1,811.2 writes/sec）がほぼ同等であり、B-Tree インデックス成長による顕著な劣化は見られなかった。
  4. **v1 スコープ確認**: 上記結果により、[`DBF-BD-001`](../design/DBF-BD-001_基本設計書.md) の `NFR-001`・`NFR-002` の許容値を確定し、v1 での `append_batch()` および自動 Retention 見送りを維持する。

## 6. 未実施・対象外・テスト差異

| ID | 対象項目 | 状態 | 理由・標準との差異 | 影響・リスク | 代替確認・対応 | 承認者・期限 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| EX-PERF-001 | `large`（60,041 B）での 300,000 件 × 3 回実行 | 代替実施（100,000 件 × 3 回） | 60 KB × 300,000 件は 1 DB あたり約 18 GB となり、単一＋分散 × 3 回の合計で約 108 GB の SSD 書き込みと約 33 分を要するため 100,000 件 × 3 回で代替した | 100,000 件時点でスループット（935.8 writes/sec）と DB サイズ（5.87 GB）が線形であることを確認済み。300,000 件時の再生時間は線形外挿で約 55〜65 秒見込み | 親プロジェクトで 60 KB 級を 30 万件保持する要件が出た場合のみ、実機で `benchmarks/bench_storage_kernel.py --payload-modes large` を単独実行して確認する | xzyozi / 必要時実施 |

## 7. 関連文書

- **基本・環境設計**: [`DBF-BD-001 基本設計書`](../design/DBF-BD-001_基本設計書.md)、[`DBF-ENV-001 環境・開発基盤仕様書`](../setup/DBF-ENV-001_環境・開発基盤仕様書.md)
- **実装**: `benchmarks/bench_storage_kernel.py`、`tests/unit/test_scale_correctness.py`

## 8. 作成完了チェック

- [x] `document_role`、`ssot_scope`、`status`、`owner`をFrontmatterへ設定した。
- [x] 目的、対象範囲、対象外、関連する要求・設計を記入した。
- [x] 採用したテスト戦略と、プロジェクト固有の合格基準を記入した。
- [x] テストケースに期待結果、根拠、実績、判定、証跡を記録した。
- [x] 品質ゲートは環境・開発基盤仕様書を参照し、実行コマンドや合格基準を再掲していない。
- [x] 未実施・対象外・標準からの差異がある場合、その理由と影響を記録した。
- [x] テスト文書が要求・設計の正本を上書きしていないことを確認した。
- [x] 秘密情報そのものや利用者固有の認証情報を記載していない。

## 9. 改訂履歴 (Change Log)

| 版数 | 改訂日 | 変更者 | 変更内容・変更理由 (Why) |
| :--- | :--- | :--- | :--- |
| Rev.1.0 | 2026-11-20 | xzyozi | 新規作成（Issue #7 性能検証ベンチマークの実施結果を記録） |
