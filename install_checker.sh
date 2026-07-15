#!/bin/bash
# site_status_checker.py インストールスクリプト

SCRIPT_DIR="/root/scraper"
SOURCE_FILE="/mnt/user-data/outputs/site_status_checker.py"

echo "=========================================="
echo "site_status_checker.py セットアップ"
echo "=========================================="
echo ""

# 1. スクリプトディレクトリ確認
if [ ! -d "$SCRIPT_DIR" ]; then
    echo "❌ エラー: $SCRIPT_DIR が存在しません"
    exit 1
fi

# 2. スクリプトをコピー
echo "📄 スクリプトをコピー中..."
cp "$SOURCE_FILE" "$SCRIPT_DIR/site_status_checker.py"
chmod +x "$SCRIPT_DIR/site_status_checker.py"
echo "✅ コピー完了: $SCRIPT_DIR/site_status_checker.py"
echo ""

# 3. 設定確認
echo "🔧 設定確認..."
cd "$SCRIPT_DIR"
python3 -c "
import os
os.environ['CHATWORK_TOKEN'] = '987cf44efbf5529a09b1317a85058640'

# 設定表示
print('ChatWork Token: 987cf44efbf5529a09b1317a85058640')
print('Admin Room ID: 413142921')
print('Shop Config: shop_config.json')
print('Report File: site-status-report.json')
"
echo ""

# 4. 依存関係チェック
echo "📦 依存関係チェック..."
python3 -c "
try:
    import asyncio
    import json
    import requests
    from playwright.async_api import async_playwright
    print('✅ 必要なモジュールはすべてインストール済み')
except ImportError as e:
    print(f'❌ エラー: {e}')
    print('以下のコマンドで依存関係をインストールしてください:')
    print('  pip install playwright requests')
    print('  playwright install chromium')
    exit(1)
"
echo ""

# 5. shop_config.json確認
if [ -f "$SCRIPT_DIR/shop_config.json" ]; then
    SITE_COUNT=$(python3 -c "import json; data=json.load(open('shop_config.json')); print(len([s for s in data if s.get('is_active', True)]))")
    echo "✅ shop_config.json: $SITE_COUNT サイト"
else
    echo "⚠️ 警告: shop_config.json が見つかりません"
fi
echo ""

# 6. テスト実行（オプション）
read -p "テスト実行しますか？ (y/N): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🧪 テスト実行中..."
    export CHATWORK_TOKEN='987cf44efbf5529a09b1317a85058640'
    python3 site_status_checker.py
fi

echo ""
echo "=========================================="
echo "✅ セットアップ完了"
echo "=========================================="

