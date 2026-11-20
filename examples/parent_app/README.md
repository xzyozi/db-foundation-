# 親プロジェクト接続例（`examples/parent_app`）

`storage_kernel` を親リポジトリへ Git サブモジュールとして組み込み、独自の Adapter と `bootstrap.py`（DI 組み立て）で接続する最小例です。

## サブモジュール導入・タグ固定の手順例

```bash
# 1. 親プロジェクトにサブモジュールとして追加し、リリース済みタグへ固定する
git submodule add <storage_kernelリポジトリURL> libs/storage_kernel
git -C libs/storage_kernel checkout v1.0.0

# 2. 親プロジェクトの環境へ editable またはパス参照で組み込む
uv add --editable ./libs/storage_kernel
```

## 構成

- `adapters.py`: 親プロジェクトが持つ部品の例
  - `ParentJsonCodec`（決定論的な `PayloadCodec`）、`ParentHashRouter`（`PartitionRouterPort`）
  - `(partition_key, seq)` の事前削除＋再挿入で冪等性を保つ SQLite FTS5 Projection
  - `MaintenanceHooks`
- `bootstrap.py`: `KernelConfig`・DAO・Projection を DI 注入して `StorageKernel` を起動する例

## 注意

- この例は、`storage_kernel.testing`（In-Memory 実装やテスト用のコーデック・ルーター）に依存しません。`testing` は DAO の検証用で、`durability="none"` の実装を含むため、本番の構成では使わないでください。親プロジェクトは、コーデックとルーターを自前で持ちます。
- DAO は、参照実装の `storage_kernel.contrib.sqlite` を使っています。親プロジェクト独自の DAO に置き換える場合は、`StorageKernelContractTest` を継承して、Contract Test を通してください。
