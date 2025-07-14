# AWS QuickSight Management Tools

AWS QuickSightダッシュボードのコード化、デプロイ、メタデータ管理を行うツール群です。

## ツール構成

1. **Dashboard Export Tool**: QuickSightダッシュボードをJSON形式でエクスポート（特定フォルダからの取得に対応）
2. **Dashboard Deploy Tool**: エクスポートしたダッシュボードを別環境にデプロイ
3. **Metadata Registration Tool**: ダッシュボードメタデータをDynamoDBに登録

## セットアップ

```bash
pip install -r requirements.txt
```

## 使用方法

### ツール1: ダッシュボードエクスポート
特定のフォルダ（例：release/）からダッシュボードを取得する場合は、`.env.dev2`に`QUICKSIGHT_FOLDER_PATH`を設定してください。

```bash
python src/dashboard_export/main.py
```

### ツール2: ダッシュボードデプロイ
```bash
python src/dashboard_deploy/main.py
```

### ツール3: メタデータ登録
```bash
python src/register_metadata/main.py
```

## 環境変数設定

各環境用の `.env` ファイルを作成してください：
- `.env.dev2`: 開発環境（エクスポート用）
  - `QUICKSIGHT_FOLDER_PATH=release/` - 特定フォルダからダッシュボードを取得（オプション）
- `.env.intg`: 統合環境
- `.env.sqa`: SQA環境
- `.env.pre`: プリプロダクション環境
- `.env.prd`: 本番環境

### 新機能: フォルダ指定エクスポート

ツール1では、特定のQuickSightフォルダ内のダッシュボードのみをエクスポートできます：

```bash
# .env.dev2 の例
EXPORT_DASHBOARD_S3_BUCKET=quicksight-export-bucket
EXPORT_DASHBOARD_S3_PREFIX=export/
AWS_ACCOUNT_ID=123456789012
QUICKSIGHT_NAMESPACE=default
AWS_REGION=ap-northeast-1
QUICKSIGHT_FOLDER_PATH=release/
```

`QUICKSIGHT_FOLDER_PATH`を設定すると、指定されたフォルダ内のダッシュボードのみがエクスポートされます。設定しない場合は、すべてのダッシュボードがエクスポートされます。

## テスト実行

```bash
pytest tests/ -v --cov=src
```