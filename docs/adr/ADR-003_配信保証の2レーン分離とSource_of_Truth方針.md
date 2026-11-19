---
title: "ADR-003: 配信保証の2レーン分離とSource of Truth方針"
document_type: "architecture_decision_record"
document_number: "ADR-003"
document_role: "rationale"
ssot_scope: "decision_rationale"
status: "accepted"
version: "1.0"
created_at: "2026-11-19"
updated_at: "2026-11-19"
author: "xzyozi"
owner: "xzyozi"
purpose: "インメモリ通知（subscribe）と非同期投影（Projection）の配信保証を分離し、シャードDBとカーソルをSource of Truthとする判断理由を記録するため"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "用語集（相対パス: ../../CONTEXT.md）"
  - "汎用データアクセスAPI層の構想（相対パス: ../analysis/汎用データアクセスAPI層の構想.md）"
  - "storage_kernel サブモジュール構成案（相対パス: ../analysis/storage_kernelサブモジュール構成案.md）"
  - "DBF-BD-001 基本設計書（相対パス: ../design/...）"
  - "DBF-IF-001 IF/API仕様書（相対パス: ../design/...）"
  - "DBF-DD-002 配信とProjection詳細設計書（相対パス: ../design/...）"
---

# ADR-003: 配信保証の2レーン分離とSource of Truth方針

> すべての変更にADRを作成する必要はありません。長期的な影響、複数案の比較、将来の判断に影響する理由がある場合に使用します。軽微な判断はIssueコメントや対象文書の改訂履歴で記録します。

| 項目           | 内容       |
| :------------- | :--------- |
| ステータス     | accepted   |
| 決定日         | 2026-11-19 |
| 決定者・承認者 | xzyozi     |
| 置換元ADR      | なし       |

## 1. 背景・課題

- **背景**: 初期プロトタイプ（`汎用データアクセスAPI層の構想.md` 第3章）では、単一のインメモリキューと1つのディスパッチスレッドで「検索用Projection DBの更新」と「アプリケーション購読者（`subscribe`）への通知」を直列に処理していた。また、キュー満杯時（`queue.Full`）にイベントを黙って破棄し、購読者の例外も握りつぶしていた。
- **要求・制約**:
  - 全文検索インデックスや集計ビューなどの Projection は、イベントの取りこぼしや重複適用によるデータ不整合が許されない（Issue #2 B2・B3）。
  - 一方、UI通知や軽量トリガーなどの購読者が遅い・例外を出した場合でも、他の購読者や Projection、本体の書き込み処理を阻害してはならない（Issue #3 C3）。
- **判断しない場合の影響**: 単一キュー・単一保証モデルのままでは、バースト書き込み時のキュー溢れやプロセス再起動によって Projection が永久に欠落するか、逆に遅い購読者に引きずられてシステム全体が停止する。

## 2. 決定

1. **シャードDBとカーソルを Source of Truth とし、Event Bus は Fast Path とする**:
   - 永続化されたシャードDB（`ShardDAOPort.fetch_range`）とメタDBのカーソル（`MetaDAOPort.get_cursor` / `advance_cursor`）を唯一の正本（Source of Truth）とし、インメモリ Event Bus は低遅延通知のための Fast Path と位置づける（Issue #2 B2）。
2. **通知レーン（`subscribe`）と投影レーン（`register_projection`）の完全分離**:
   - **`subscribe`（At-Most-Once）**:
     - 購読者ごとに独立した有界キューを持ち、取りこぼしを許容する。
     - キュー満杯時は `drop_policy`（`drop_newest` / `drop_oldest` / `disconnect`）に従って処理し、ドロップ件数をログ・telemetry・`SubscriptionHandle.dropped_count` に記録する。
     - リトライおよび DLQ は行わない。コールバックの例外は握りつぶさず、ログ・telemetry・`on_error` に通知して後続処理を継続する（Issue #3 C3）。
   - **`register_projection`（At-Least-Once + 冪等適用）**:
     - Projection ごとに独立した `ProjectionRunner` ワーカーで実行する。
     - Fast Path のキュー溢れ時は対象 `partition_key` をメモリ上の Dirty Set に記録し、起動時・定期実行時・ギャップ検知時（`event.seq > last_seq + 1`）とあわせて `fetch_range` で確実に追いつく（Issue #2 B2）。
     - `ProjectionPort.apply_batch` は `(partition_key, seq)` に対して冪等であることを必須契約とし、`advance_cursor` は `apply_batch` の成功後にのみ呼び出す（Issue #2 B3）。
     - 失敗時は `RetryPolicy`（指数バックオフ）で再試行し、上限超過時はバッチを1件ずつ再実行して原因レコード（Poison Pill）を特定した上で、`failure_policy`（`dlq_and_skip` または `pause_partition`）に従う（Issue #3 C3）。

## 3. 理由・代替案

### 採用理由

1. **書き込みレイテンシーと投影の確実性の両立**: 書き込み時にインメモリキューが溢れても Dirty Set に `partition_key` を記録するだけで済むため、書き込みスレッドをブロックせずに `fetch_range` で後から欠落なく追いつける。
2. **Slow Consumer の隔離**: 購読者ごとの有界キューと Projection ごとの独立ワーカーに分けることで、遅い購読者やエラーを起こす購読者が Projection や他購読者の進行を妨げない。
3. **停止・再起動の高速化**: Projection がカーソルを正本としていつでも追いつけるため、`close()` 時は既定で実行中の1バッチ完了のみを待って残キューを破棄でき、迅速かつ安全にシャットダウンできる（Issue #3 C4）。

### 代替案

| 代替案 | 利点 | 欠点・リスク | 採用しなかった理由 |
| :----- | :--- | :----------- | :----------------- |
| すべての購読者・Projection を単一の永続キュー（DBテーブル）で直列配信する | インメモリキュー管理や Dirty Set が不要になる | 軽量なUI通知まで毎回DBカーソル更新が必要になり、遅い購読者が1つあるだけで全体の配信が詰まる | 用途の異なる通知（低遅延・揮発可）と投影（確実性必須）で要求される特性が異なるため |
| Event Bus キュー満杯時に書き込み側（`append`）をブロッキング待機させる | インメモリキューからの取りこぼしが発生しない | 下流の購読者や Projection の遅延が上流の `append()` を直接ブロックし、書き込みスループットと可用性が劣化する | Write-Before-ACK 後の非同期配信という疎結合設計の利点を損なうため |
| バッチ失敗時にバッチ全体をそのまま DLQ へ送る | Poison Pill 特定の1件ずつ再実行ロジックが不要になる | バッチ内に含まれていた正常なレコードまで未適用のままスキップされ、Projection の整合性が崩れる | 正常レコードを確実に適用し、真の原因レコード1件のみを隔離または停止させるため（Issue #3 C3） |

## 4. 影響・トレードオフ

- **利点**:
  - プロセス突然死やバースト書き込みによるキュー溢れが起きても、Projection は再起動後・次周期に自動的に整合状態を回復できる。
  - Poison Pill 発生時も正常レコードを巻き込まず、DLQ隔離（`dlq_and_skip`）またはパーティション単位の一時停止と手動再開（`pause_partition` / `resume_projection`）を選択できる。
- **欠点・コスト**:
  - 親プロジェクトが実装する `ProjectionPort.apply_batch` に `(partition_key, seq)` 単位の冪等性実装（UPSERT や事前削除＋挿入など）が求められる。
  - `core/event_bus.py`・`core/projection.py`・`core/reliability.py` で Dirty Set、ギャップ検知、Poison Pill 単件特定を実装する必要がある。
- **影響を受ける正本文書**: 基本設計書（`DBF-BD-001`）、IF/API仕様書（`DBF-IF-001`）、データ構造仕様書（`DBF-DS-001`）、配信とProjection詳細設計書（`DBF-DD-002`）。
- **移行・ロールバック**: 新規設計のため移行不要。
- **セキュリティ・運用上の影響**: `pause_partition` 発生時や DLQ 格納時はエラーイベント・ログを出力し、運用者が `list_dead_letters` や `resume_projection` で対処できるようにする。

## 5. 関連文書・後続作業

- **要求・Issue**: Issue #2（B2・B3）、Issue #3（C2・C3・C4）、Issue #9
- **現在の仕様**: 基本設計書 `DBF-BD-001`、IF/API仕様書 `DBF-IF-001`、データ構造仕様書 `DBF-DS-001`、詳細設計書 `DBF-DD-002`
- **実装・検証タスク**: `ProjectionPort` の冪等性 Contract Test（項目10）、Dirty Set 追いつきと Poison Pill 処理のユニットテスト
- **後続ADR**: なし

## 6. 例外・見直し条件（必要な場合のみ）

- **例外・適用差異**: システム予約種別 `__snapshot__` は、既定では `subscribe` にも `ProjectionPort` にも配信しない。スナップショットを必要とする Projection のみ `register_projection(..., include_snapshots=True)` で明示的に受信する（Issue #4 D1）。
- **見直し条件**: なし
- **見直し時期**: なし

## 7. 作成完了チェック

- [x] この判断がADRを必要とする重要度か確認した。
- [x] 背景、決定、理由、代替案、影響を記入した。
- [x] 現在の仕様を定義する正本文書へのリンクを設定した。
- [x] 判断変更時は本書を削除せず、後継ADRを作成する方針を確認した。
- [x] 機密情報や認証情報を記載していない。

## 8. 改訂履歴 (Change Log)

| 版数    | 改訂日     | 変更者 | 変更内容・変更理由 (Why)                             |
| :------ | :--------- | :----- | :--------------------------------------------------- |
| Rev.1.0 | 2026-11-19 | xzyozi | 新規作成（Issue #2 B2・B3、Issue #3 C3 の決定を記録） |
