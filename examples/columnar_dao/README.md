# 業務カラム展開 DAO の例（`examples/columnar_dao`）

ペイロード（dict）を、業務カラムへ展開して保存する `ShardDAOPort` の実装例です。Issue #53 の検証のために作りました。`storage_kernel` 本体には含まれません。

参照実装（`storage_kernel.contrib.sqlite`）は、ペイロードを BLOB として保存します。親プロジェクトが「業務カラムとして保存したい」場合に、DAO の自由度が実際に成立するかを確かめるためのものです。

## 保存の形

| カラム | 内容 |
| :--- | :--- |
| `symbol`（TEXT）・`qty`（INTEGER）・`price`（REAL） | ペイロードの同名のキーのうち、**型が合うものだけ**を展開する（`bool` は数値として扱わない） |
| `extra`（TEXT、JSON） | 上記以外のキー、および型が合わない値。往復で値を変えないために残す |
| その他 | `partition_key`・`seq`・`idempotency_key`・`record_type`・`schema_version`・`payload_hash`・`created_at`・`covers_through_seq`（基盤の契約どおり） |

読み出すときは、専用カラムと `extra` から、元の dict を復元します。

## 使い方

`StorageKernel` に、`store_raw_bytes=False` で組み合わせます。DAO は、エンコード前のペイロード（dict）を直接受け取ります。

- DAO: `ColumnarShardDAO(base_dir)`
- `MetaDAOPort`: ペイロードに関係しないので、参照実装の `SqliteMetaDAO` をそのまま使えます。
- 組み立ての例とテストは `tests/contract/test_columnar_contract.py` を参照してください。

## 検証結果（Issue #53）

| 項目 | 結果 |
| :--- | :--- |
| Contract Test 13 項目（TR-IF-001〜013） | **すべて合格**（スキップなし）。DAO 側の修正も、契約の変更も、不要だった |
| `StorageKernel`（`store_raw_bytes=False`）との結合 | 動作する。`append` → `sync` で同じ値が戻る |
| 型が合わない値・未知のキーの往復 | 値も型も変わらない（`qty="10"` が `10` にならない、`price=True` が `1.0` にならない） |
| 業務カラムでの検索 | `symbol` で件数を数えられる（ペイロードを全件読み出さずに済む） |
| 壊した DAO での確認 | 型チェックを外す → 往復のテストが失敗。`extra` を保存しない → Contract Test の 4 番を含む 3 件が失敗。どちらも、元に戻した |

## 分かったこと・分かっていないこと

- **分かったこと**: `Generic[T]` のまま DAO にペイロードを渡し、DAO が業務カラムへ展開する自由度は、**契約を変えずに成立する**。ペイロードをバイト列に一本化すると（#54 の案 B）、この自由度は狭まる。
- **分かっていないこと**:
  - 性質の異なるストレージ（PostgreSQL など、サーバー型の DB）では、検証していない。この例も SQLite を使っている。
  - 業務カラムの追加・型の変更に伴うスキーマ移行（`ALTER TABLE` など）は、扱っていない。基盤の `schema_version` と Upcaster が扱うのは、ペイロードの版であり、テーブル構造の移行ではない。
  - 性能は測っていない。
