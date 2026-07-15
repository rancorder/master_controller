#!/bin/bash
# site_status_checker.py の cron 設定スクリプト

SCRIPT_DIR="/root/scraper"
SCRIPT_NAME="site_status_checker.py"
LOG_DIR="$SCRIPT_DIR/logs"

# ログディレクトリ作成
mkdir -p "$LOG_DIR"

# 現在のcrontabをバックアップ
crontab -l > /tmp/crontab_backup_$(date +%Y%m%d_%H%M%S).txt 2>/dev/null || true

# 既存のsite_status_checker関連のcronを削除
crontab -l 2>/dev/null | grep -v "site_status_checker\|site-status-checker" > /tmp/new_crontab || true

# 新しいcron設定を追加（6時間ごと: 0時、6時、12時、18時）
cat >> /tmp/new_crontab << 'CRONEOF'

# site_status_checker.py - 6時間ごとに実行（0時、6時、12時、18時）
0 */6 * * * cd /root/scraper && /usr/bin/python3 site_status_checker.py >> /root/scraper/logs/site_status_checker.log 2>&1
CRONEOF

# crontabに設定
crontab /tmp/new_crontab

echo "=========================================="
echo "✅ cron設定完了"
echo "=========================================="
echo ""
echo "【設定内容】"
crontab -l | grep -A 1 "site_status_checker"
echo ""
echo "【実行スケジュール】"
echo "  毎日 0時、6時、12時、18時"
echo ""
echo "【ログファイル】"
echo "  /root/scraper/logs/site_status_checker.log"
echo ""
echo "=========================================="

