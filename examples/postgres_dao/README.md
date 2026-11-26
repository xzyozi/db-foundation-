# PostgreSQL 試作 DAO の例（`examples/postgres_dao`）

PostgreSQL 上で `ShardDAOPort` / `MetaDAOPort` を実装し、`storage_kernel` の DAO 委譲抽象化がサーバー型 RDBMS でも成立するかを検証した試作実装です（Issue #59）。`storage_kernel` 本体（`src/storage_kernel`）には含まれません。

## ファイル構成と行数

| ファイル | 役割 | 行数 |
| :--- | :--- | ---: |
| `examples/postgres_dao/__init__.py` | `PostgresShardDAO` / `PostgresMetaDAO` の公開エクスポート | 8 |
| `examples/postgres_dao/_db.py` | 接続・トランザクション（`write_tx`）・スキーマ修飾補助 | 48 |
| `examples/postgres_dao/shard_dao.py` | `PostgresShardDAO` / `_PostgresShardTx`（`ShardDAOPort` 実装） | 237 |
| `examples/postgres_dao/meta_dao.py` | `PostgresMetaDAO`（`MetaDAOPort` 実装） | 172 |
| **合計** | | **465** |

## テーブル設計

1 つの PostgreSQL データベース（または指定 `schema`）内に `shard_id` 列でシャードを区別して保持します。

- `sequences`: `(shard_id, partition_key)` を主キーとし、`last_seq BIGINT` を保持
- `records`: `CONSTRAINT pk_records PRIMARY KEY (shard_id, partition_key, seq)` および `CONSTRAINT uq_records_idempotency UNIQUE (shard_id, partition_key, idempotency_key)`、ペイロードは `BYTEA`、時刻は `DOUBLE PRECISION`
- `partitions`: `partition_key` 主キー、`shard_id`、`status`（`active` / `read_only`）、`created_at DOUBLE PRECISION`
- `cursors`: `(consumer_id, partition_key)` 主キー、`last_seq BIGINT`、`updated_at DOUBLE PRECISION`

## テストの実行方法

開発依存の `postgres` グループ（`psycopg[binary]>=3.2.0`）と接続先 `POSTGRES_TEST_DSN` がある環境で実行できます。いずれかが未設定・未導入の環境では `tests/contract/test_postgres_contract.py` は自動スキップされ、通常の `uv run --no-sync pytest` には影響しません。

```bash
# 例: ローカルで一時 PostgreSQL コンテナを起動して実行する場合
docker run -d --name dbf-pg-test \
  -e POSTGRES_USER=dbf -e POSTGRES_PASSWORD=dbfpass -e POSTGRES_DB=dbf_test \
  -p 55432:5432 postgres:15-alpine

POSTGRES_TEST_DSN="postgresql://dbf:dbfpass@127.0.0.1:55432/dbf_test" \
  uv run --no-sync --with "psycopg[binary]>=3.2.0" \
  pytest tests/contract/test_postgres_contract.py -v

# 後片付け
docker rm -f dbf-pg-test
```

## 検証結果（Issue #59）

PostgreSQL 15（`postgres:15-alpine`）＋ `psycopg` 3.3.9 環境で以下を確認しました。

| 項目 | 結果 |
| :--- | :--- |
| Contract Test 13 項目（`TR-IF-001`〜`013`） | **13 passed**（スキップなし） |
| `StorageKernel` 結合テスト（`store_raw_bytes=False` / `True`、`append` → 冪等再送 → `sync` → `close`） | **2 passed** |
| 未設定・未導入環境での `uv run --no-sync pytest -q` | PostgreSQL テスト 15 件が自動スキップされ、全体エラー 0 件 |
| ネガティブ確認（`CREATE SEQUENCE` の非ロールバック性） | `CREATE TEMP SEQUENCE` 上で `nextval()` が `1` を返した後に `ROLLBACK` すると、次回 `nextval()` が `2` となり巻き戻らないことを確認 |

## 実装して分かったこと・ハマりどころ

1. **`CREATE SEQUENCE` は `TR-IF-001`（ロールバック原子性）を満たせない**
   - PostgreSQL のシーケンスオブジェクト（`nextval()`）はトランザクションを `ROLLBACK` しても巻き戻りません（ネガティブ確認で `1` → `ROLLBACK` → 次回 `2` を確認）。なお `CREATE SEQUENCE IF NOT EXISTS` をロールバック対象トランザクション内で遅延実行した場合は DDL 自体が巻き戻るため初回だけ `1` に見えますが、1 件でも確定した後に異常終了すると欠番が生じます。
   - 本実装では `sequences` テーブルへの `INSERT ... ON CONFLICT (shard_id, partition_key) DO UPDATE SET last_seq = sequences.last_seq + 1 RETURNING last_seq` により、**1 ラウンドトリップ**で採番とロールバック原子性を両立しました。
2. **並行実行と DDL の競合（`TR-IF-003` / `TR-IF-007`）**
   - `psycopg` の接続オブジェクトはスレッド間共有に適さないため、試作ではトランザクション・参照ごとに接続を開閉する方式としました（本番環境では `psycopg_pool.ConnectionPool` 等の利用が望ましい）。
   - PostgreSQL では複数スレッドが同時に `CREATE TABLE IF NOT EXISTS` を実行すると内部カタログの一意制約違反（`pg_type_typname_nsp_index` 等）が起き得るため、`__init__` 内で `threading.Lock` を使ってスキーマ初期化を直列化しました。
   - 採番時は `sequences` テーブルの行ロックにより PostgreSQL 側で直列化されるため、`ThreadPoolExecutor(max_workers=4)` からの同時書き込みでも欠番・重複なく採番できました。
3. **SQLite との方言・型・エラー分類の差**
   - カーソル更新の単調非減少化は、SQLite の `MAX(a, b)` ではなく PostgreSQL のスカラー関数 `GREATEST(cursors.last_seq, EXCLUDED.last_seq)` を使う必要があります。
   - PostgreSQL の `REAL` は 32-bit 浮動小数点であり `time.time()` のサブ秒精度が欠落するため、タイムスタンプ列には `DOUBLE PRECISION`（64-bit）を採用しました。
   - `DuplicateKeyError.kind` の分類では、`psycopg.errors.UniqueViolation.diag.constraint_name`（`pk_records` / `uq_records_idempotency`）を直接参照でき、SQLite のエラーメッセージ文字列判定より簡潔かつ堅牢に実装できました。
   - `BYTEA` 列の読み戻し値は環境やアダプタによって `memoryview` になる場合があるため、`_normalize_bytes()` で `bytes(...)` へ正規化しています。
4. **設計・運用への示唆（#52・#54 関連）**
   - 現行の `ShardDAOPort` / `MetaDAOPort` 契約（`src/storage_kernel/ports/dao.py`）を一切変更せず、PostgreSQL 実装でも 13 項目の契約テストと `StorageKernel` 結合がそのまま成立しました。
   - ただし `ShardDAOPort` / `MetaDAOPort` にはリソース解放メソッド（`close()`）がないため、コネクションプールを DAO 内部に持つ場合は親プロジェクト側の bootstrap / shutdown で寿命管理する設計判断が必要です（#52 関連の留意点）。
   - `store_raw_bytes=False`（`PostgresShardDAO(..., codec=codec)`）と `store_raw_bytes=True`（`PostgresShardDAO(..., codec=None)`）の両方で往復を確認しました。今後 `JSONB` や業務カラム展開を PostgreSQL で活かす場合は `store_raw_bytes=False`（`Generic[T]` 維持、#54 にて案 A 現状維持を採用・`DBF-IF-001` §2.5.1 に整理済み）が有効です。
