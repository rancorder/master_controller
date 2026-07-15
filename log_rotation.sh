#!/bin/bash
# log_rotation.sh - 自動ログローテーション
# 毎日深夜2時に実行

SCRAPER_DIR="/root/scraper"
BACKUP_DIR="${SCRAPER_DIR}/logs_backup"
DATE=$(date +%Y%m%d)
KEEP_DAYS=7

# バックアップディレクトリ作成
mkdir -p "$BACKUP_DIR"

echo "=========================================="
echo "ログローテーション開始: $(date)"
echo "=========================================="

# master_controller.log をローテーション
if [ -f "${SCRAPER_DIR}/master_controller.log" ]; then
    LOG_SIZE=$(du -h "${SCRAPER_DIR}/master_controller.log" | cut -f1)
    echo "現在のログサイズ: ${LOG_SIZE}"
    
    # バックアップ
    cp "${SCRAPER_DIR}/master_controller.log" "${BACKUP_DIR}/master_controller_${DATE}.log"
    
    # 圧縮（容量削減）
    gzip "${BACKUP_DIR}/master_controller_${DATE}.log"
    
    echo "✅ バックアップ完了: master_controller_${DATE}.log.gz"
    
    # 新しいログファイルを作成
    > "${SCRAPER_DIR}/master_controller.log"
    
    echo "✅ ログファイルをリセット"
fi

# treasure.log をローテーション
if [ -f "${SCRAPER_DIR}/treasure.log" ]; then
    cp "${SCRAPER_DIR}/treasure.log" "${BACKUP_DIR}/treasure_${DATE}.log"
    gzip "${BACKUP_DIR}/treasure_${DATE}.log"
    > "${SCRAPER_DIR}/treasure.log"
    echo "✅ treasure.log をローテーション"
fi

# 古いバックアップを削除（7日より古い）
echo "🗑️  古いバックアップを削除中..."
find "$BACKUP_DIR" -name "*.log.gz" -mtime +${KEEP_DAYS} -delete

# 残っているバックアップを表示
echo ""
echo "📁 現在のバックアップ一覧:"
ls -lh "$BACKUP_DIR" | grep ".log.gz"

# 合計サイズ
TOTAL_SIZE=$(du -sh "$BACKUP_DIR" | cut -f1)
echo ""
echo "📊 バックアップ合計サイズ: ${TOTAL_SIZE}"
echo "=========================================="
echo "ログローテーション完了: $(date)"
echo "=========================================="
