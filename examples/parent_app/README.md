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

- `adapters.py`: `(partition_key, seq)` の事前削除＋再挿入で冪等性を保つ SQLite FTS5 Projection と `MaintenanceHooks` の例
- `bootstrap.py`: `KernelConfig`・DAO・Projection を DI 注入して `StorageKernel` を起動する例
