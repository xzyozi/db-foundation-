# 親プロジェクト接続例（`examples/parent_app`）

`storage_kernel` を親リポジトリへ Git サブモジュールとして組み込み、独自の Adapter と `bootstrap.py`（DI 組み立て）で接続する最小例です。

## サブモジュールとして組み込む手順

追加、タグ固定、`PYTHONPATH` の設定、更新、CI での取得の手順は、運用手順書 [`DBF-OP-002`](../../docs/how-to/DBF-OP-002_サブモジュールとして組み込む手順書.md) を参照してください（この README には、手順を再掲しません）。

最短の流れは、次のとおりです。

```bash
git submodule add <storage_kernelリポジトリURL> libs/storage_kernel
git -C libs/storage_kernel checkout v<バージョン>
export PYTHONPATH="$PWD/libs/storage_kernel/src:$PYTHONPATH"   # Windows は DBF-OP-002 を参照
```

## 構成

- `adapters.py`: 親プロジェクトが持つ部品の例
  - `ParentJsonCodec`（決定論的な `PayloadCodec`）、`ParentHashRouter`（`PartitionRouterPort`）
  - `(partition_key, seq)` の事前削除＋再挿入で冪等性を保つ SQLite FTS5 Projection
  - `MaintenanceHooks`
- `bootstrap.py`: `KernelConfig`・DAO・Projection を DI 注入して `StorageKernel` を起動する例

## ペイロードの保存形式の選び方（`store_raw_bytes` と DAO の組み合わせ）

この例は、`store_raw_bytes=True` と `SqliteShardDAO[bytes](..., codec=None)` の組み合わせを使っています（基盤の `codec` が `bytes` に変換し、DAO はその `bytes` を BLOB として保存する形）。

ペイロードを**業務カラムへ展開して保存したい**場合は、`store_raw_bytes=False` にして、DAO が `T`（dict など）を直接受け取る形にします。実装例は [`examples/columnar_dao`](../columnar_dao/README.md) です。組み合わせの一覧と選び方、注意点は、IF 仕様書（[`DBF-IF-001`](../../docs/design/DBF-IF-001_IF仕様書.md) §2.5.1）を参照してください。

## 注意

- この例は、`storage_kernel.testing`（In-Memory 実装やテスト用のコーデック・ルーター）に依存しません。`testing` は DAO の検証用で、`durability="none"` の実装を含むため、本番の構成では使わないでください。親プロジェクトは、コーデックとルーターを自前で持ちます。
- DAO は、参照実装の `storage_kernel.contrib.sqlite` を使っています。親プロジェクト独自の DAO に置き換える場合は、`StorageKernelContractTest` を継承して、Contract Test を通してください。
