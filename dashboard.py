#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dashboard.py - スクレイピングシステム監視ダッシュボード（完全版）
リアルタイム新商品通知対応
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List

app = FastAPI(title="Price Hunter Dashboard")

# CORS設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SCRAPER_DIR = Path("/root/scraper")
LOG_FILE = SCRAPER_DIR / "master_controller.log"
CONFIG_FILE = SCRAPER_DIR / "shop_config.json"

def get_monitoring_stats() -> Dict:
    """監視URL数と成功URL数を取得（最新サイクルベース）"""
    try:
        monitoring_urls = 0
        successful_urls = set()
        
        # shop_config.jsonからis_active=trueの数をカウント
        if CONFIG_FILE.exists():
            with open(CONFIG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                config = json.load(f)
            monitoring_urls = sum(1 for c in config if c.get('is_active', False))
        
        # 最新サイクルで実行されたスクリプトをカウント
        if LOG_FILE.exists():
            with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                
                # 最後のサイクルリセットまたは起動ログを探す
                last_cycle_index = 0
                for i, line in enumerate(reversed(lines)):
                    if 'サイクル' in line and ('開始' in line or 'リセット' in line or '再投入' in line):
                        last_cycle_index = len(lines) - i
                        break
                
                # 最新サイクル以降のログから成功を検出
                recent_lines = lines[last_cycle_index:]
                
                for line in recent_lines:
                    # 実行開始ログまたは成功ログを検出
                    if ('実行開始' in line or '成功' in line) and '.py' in line:
                        match = re.search(r'\[(.*?)\.py\]', line)
                        if match:
                            script_name = match.group(1)
                            # ブロック対象（tresure.py）は除外
                            if script_name != 'tresure':
                                successful_urls.add(script_name)
        
        # 成功数が監視数を超えないように調整
        successful_count = min(len(successful_urls), monitoring_urls)
        
        return {
            'monitoring_urls': monitoring_urls,
            'successful_urls': successful_count
        }
    except Exception as e:
        print(f"監視統計取得エラー: {e}")
        return {'monitoring_urls': 0, 'successful_urls': 0}

def get_cycle_stats() -> Dict:
    """サイクル回数と時間を取得"""
    try:
        cycle_count = 0
        cycle_duration = 0
        
        if LOG_FILE.exists():
            with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                
                # サイクル完了ログをカウント
                for line in lines:
                    if 'サイクル' in line and '完了' in line:
                        cycle_count += 1
                        
                        # サイクル時間を抽出
                        match = re.search(r'\((\d+)秒\)', line)
                        if match:
                            cycle_duration = int(match.group(1))
                
                # 最新のTier2実行ログから推定
                if cycle_duration == 0:
                    for line in reversed(lines[-500:]):
                        if 'Tier2実行' in line or 'Tier2キュー取得' in line:
                            match = re.search(r'経過=(\d+)秒', line)
                            if match:
                                cycle_duration = int(match.group(1))
                                if cycle_duration > 60:  # 60秒以上なら採用
                                    break
        
        return {
            'cycle_count': cycle_count,
            'cycle_duration': cycle_duration
        }
    except Exception as e:
        print(f"サイクル統計取得エラー: {e}")
        return {'cycle_count': 0, 'cycle_duration': 0}

def get_recent_products() -> List[Dict]:
    """最近の新商品通知を取得（新商品検知ログのみ）"""
    products = []
    
    try:
        if LOG_FILE.exists():
            with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                
                i = len(lines) - 1
                while i >= 0 and len(products) < 50:
                    line = lines[i]
                    
                    # 🎉 新しい1位を検知！ のログを探す
                    if '🎉 新しい1位を検知' in line:
                        try:
                            # タイムスタンプ取得
                            timestamp_str = line.split('[')[0].strip()
                            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                            
                            # [ショップ名_カテゴリ_新着] を抽出
                            shop_match = re.search(r'\[(.*?)_新着\]', line)
                            if not shop_match:
                                i -= 1
                                continue
                            
                            shop_info = shop_match.group(1)
                            
                            # ショップ名とカテゴリに分割
                            if '_' in shop_info:
                                parts = shop_info.rsplit('_', 1)
                                site_name = parts[0]
                                category = parts[1] if len(parts) > 1 else '新着'
                            else:
                                site_name = shop_info
                                category = '新着'
                            
                            # 次の行（📦 新商品:）から商品情報を取得
                            price = 0
                            url = ''
                            name = ''
                            
                            # 次の数行を探索
                            for j in range(i + 1, min(len(lines), i + 5)):
                                next_line = lines[j]
                                
                                # 📦 新商品: 行を探す
                                if '📦 新商品:' in next_line:
                                    # 商品名抽出: 📦 新商品: の後、/ 価格: の前
                                    name_match = re.search(r'📦 新商品:\s*(.+?)\s*/\s*価格:', next_line)
                                    if name_match:
                                        name = name_match.group(1).strip()
                                    
                                    # 価格抽出
                                    price_match = re.search(r'価格:\s*(\d{1,3}(?:,\d{3})*|\d+)円', next_line)
                                    if price_match:
                                        price_str = price_match.group(1).replace(',', '')
                                        price = int(price_str)
                                    
                                    # URL抽出
                                    url_match = re.search(r'URL:\s*(https?://[^\s]+)', next_line)
                                    if url_match:
                                        url = url_match.group(1)
                                    
                                    break
                            
                            # 商品名が見つからない場合は前の行から取得
                            if not name:
                                for j in range(i - 1, max(0, i - 5), -1):
                                    prev_line = lines[j]
                                    if '今回1位:' in prev_line and site_name in prev_line:
                                        name_match = re.search(r'今回1位:\s*(.+?)\s*/\s*価格:', prev_line)
                                        if name_match:
                                            name = name_match.group(1).strip()
                                            break
                            
                            # 時間差計算
                            delta = datetime.now() - timestamp
                            if delta.seconds < 60:
                                time_str = f"{delta.seconds}秒前"
                            elif delta.seconds < 3600:
                                time_str = f"{delta.seconds // 60}分前"
                            elif delta.seconds < 86400:
                                time_str = f"{delta.seconds // 3600}時間前"
                            else:
                                time_str = f"{delta.days}日前"
                            
                            # 商品名が有効なら追加
                            if name and len(name) > 3:
                                products.append({
                                    'site': site_name,
                                    'category': category,
                                    'name': name[:150],
                                    'price': price,
                                    'url': url,
                                    'time': time_str
                                })
                        
                        except Exception as e:
                            print(f"商品パースエラー: {e}")
                            pass
                    
                    i -= 1
    
    except Exception as e:
        print(f"新商品取得エラー: {e}")
    
    return products
    
def get_realtime_status() -> Dict:
    """リアルタイムステータス（実行中のスクリプトとログ）"""
    try:
        current_script = None
        recent_logs = []
        cycle_progress = 0
        total_scripts = 0
        
        if LOG_FILE.exists():
            with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                
                # 最新の実行中スクリプトを探す
                for line in reversed(lines[-100:]):
                    if 'Tier2実行' in line or '実行開始' in line:
                        match = re.search(r'Tier2実行:\s*(.*?)\.py|実行開始.*?\[(.*?)\.py\]', line)
                        if match:
                            current_script = match.group(1) or match.group(2)
                            break
                
                # サイクル進捗を取得
                for line in reversed(lines[-50:]):
                    if '残り=' in line:
                        remaining_match = re.search(r'残り=(\d+)件', line)
                        if remaining_match:
                            remaining = int(remaining_match.group(1))
                            # 推定総数（優先度2は35件程度）
                            total_scripts = 35
                            cycle_progress = ((total_scripts - remaining) / total_scripts) * 100
                            break
                
                # 最新10件のログを取得
                for line in reversed(lines[-50:]):
                    if any(keyword in line for keyword in ['実行開始', '成功', '件取得', 'Tier2実行', '新しい1位']):
                        timestamp_str = line.split('[')[0].strip() if '[' in line else ''
                        log_content = line.split('] ')[-1].strip() if '] ' in line else line.strip()
                        
                        # タイムスタンプから経過時間を計算
                        time_str = ''
                        try:
                            if timestamp_str:
                                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                                delta = datetime.now() - timestamp
                                if delta.seconds < 5:
                                    time_str = 'たった今'
                                elif delta.seconds < 60:
                                    time_str = f'{delta.seconds}秒前'
                                else:
                                    time_str = f'{delta.seconds // 60}分前'
                        except:
                            pass
                        
                        recent_logs.append({
                            'time': time_str,
                            'content': log_content[:100]
                        })
                        
                        if len(recent_logs) >= 10:
                            break
        
        return {
            'current_script': current_script,
            'cycle_progress': int(cycle_progress),
            'recent_logs': recent_logs
        }
    except Exception as e:
        print(f"リアルタイムステータス取得エラー: {e}")
        return {
            'current_script': None,
            'cycle_progress': 0,
            'recent_logs': []
        }

@app.get("/api/stats")
async def get_stats():
    """統計API"""
    monitoring = get_monitoring_stats()
    cycle = get_cycle_stats()
    products = get_recent_products()
    realtime = get_realtime_status()
    
    return {
        'monitoring_urls': monitoring['monitoring_urls'],
        'cycle_count': cycle['cycle_count'],
        'cycle_duration': cycle['cycle_duration'],
        'recent_products': products,
        'current_script': realtime['current_script'],
        'cycle_progress': realtime['cycle_progress'],
        'recent_logs': realtime['recent_logs']
    }

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """ダッシュボードHTML"""
    return """
<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Price Hunter Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        @keyframes slide-in {
            from {
                opacity: 0;
                transform: translateY(-10px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }
        
        .animate-slide-in {
            animation: slide-in 0.3s ease-out forwards;
        }
        
        .custom-scrollbar::-webkit-scrollbar {
            width: 8px;
        }
        
        .custom-scrollbar::-webkit-scrollbar-track {
            background: rgba(55, 65, 81, 0.3);
            border-radius: 10px;
        }
        
        .custom-scrollbar::-webkit-scrollbar-thumb {
            background: rgba(96, 165, 250, 0.3);
            border-radius: 10px;
        }
        
        .custom-scrollbar::-webkit-scrollbar-thumb:hover {
            background: rgba(96, 165, 250, 0.5);
        }
    </style>
</head>
<body class="bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 text-white">
    <div class="min-h-screen p-6">
        <!-- ヘッダー -->
        <div class="mb-8">
            <h1 class="text-4xl font-bold mb-2 bg-gradient-to-r from-blue-400 to-purple-600 bg-clip-text text-transparent">
                Price Hunter Dashboard
            </h1>
            <p class="text-gray-400">リアルタイムスクレイピング監視システム</p>
        </div>

        <!-- メトリクスカード -->
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
            <!-- 監視URL数 -->
            <div class="bg-gradient-to-br from-blue-500/10 to-blue-600/10 border border-blue-500/20 rounded-xl p-6 backdrop-blur-sm">
                <div class="flex items-center justify-between mb-4">
                    <svg class="w-8 h-8 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 9a2 2 0 012-2h.93a2 2 0 001.664-.89l.812-1.22A2 2 0 0110.07 4h3.86a2 2 0 011.664.89l.812 1.22A2 2 0 0018.07 7H19a2 2 0 012 2v9a2 2 0 01-2 2H5a2 2 0 01-2-2V9z"></path>
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 13a3 3 0 11-6 0 3 3 0 016 0z"></path>
                    </svg>
                    <div id="monitoring-urls" class="text-3xl font-bold text-blue-400">-</div>
                </div>
                <div class="text-sm text-gray-400">監視URL数</div>
            </div>

            <!-- サイクル回数 -->
            <div class="bg-gradient-to-br from-purple-500/10 to-purple-600/10 border border-purple-500/20 rounded-xl p-6 backdrop-blur-sm">
                <div class="flex items-center justify-between mb-4">
                    <svg class="w-8 h-8 text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"></path>
                    </svg>
                    <div id="cycle-count" class="text-3xl font-bold text-purple-400">-</div>
                </div>
                <div class="text-sm text-gray-400">サイクル回数</div>
            </div>

            <!-- サイクル時間 -->
            <div class="bg-gradient-to-br from-orange-500/10 to-orange-600/10 border border-orange-500/20 rounded-xl p-6 backdrop-blur-sm">
                <div class="flex items-center justify-between mb-4">
                    <svg class="w-8 h-8 text-orange-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                    </svg>
                    <div id="cycle-duration-min" class="text-3xl font-bold text-orange-400">-<span class="text-lg">分</span></div>
                </div>
                <div class="text-sm text-gray-400">1サイクル時間</div>
                <div id="cycle-duration-sec" class="mt-2 text-xs text-orange-400">-</div>
            </div>
        </div>

        <!-- リアルタイムステータス -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
            <!-- 実行中スクリプト＆進捗バー -->
            <div class="bg-gradient-to-br from-gray-800/50 to-gray-900/50 border border-gray-700/50 rounded-xl p-6 backdrop-blur-sm">
                <div class="flex items-center gap-3 mb-4">
                    <div class="w-3 h-3 bg-green-400 rounded-full animate-pulse"></div>
                    <h2 class="text-xl font-bold">リアルタイム実行状況</h2>
                </div>
                
                <div class="mb-4">
                    <div class="text-sm text-gray-400 mb-2">実行中:</div>
                    <div id="current-script" class="text-lg font-mono text-green-400">待機中...</div>
                </div>
                
                <div>
                    <div class="flex justify-between text-sm text-gray-400 mb-2">
                        <span>サイクル進捗</span>
                        <span id="progress-percent">0%</span>
                    </div>
                    <div class="w-full bg-gray-700/30 rounded-full h-4 overflow-hidden">
                        <div id="progress-bar" class="h-full bg-gradient-to-r from-green-500 to-blue-500 transition-all duration-500 ease-out" style="width: 0%"></div>
                    </div>
                </div>
            </div>

            <!-- リアルタイムログ -->
            <div class="bg-gradient-to-br from-gray-800/50 to-gray-900/50 border border-gray-700/50 rounded-xl p-6 backdrop-blur-sm">
                <div class="flex items-center gap-3 mb-4">
                    <svg class="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path>
                    </svg>
                    <h2 class="text-xl font-bold">実行ログ</h2>
                </div>
                
                <div id="realtime-logs" class="space-y-2 max-h-[200px] overflow-y-auto custom-scrollbar font-mono text-xs">
                    <div class="text-gray-500">ログ読み込み中...</div>
                </div>
            </div>
        </div>

        <!-- 新着商品通知エリア -->
        <div class="bg-gradient-to-br from-gray-800/50 to-gray-900/50 border border-gray-700/50 rounded-xl p-6 backdrop-blur-sm">
            <div class="flex items-center gap-3 mb-6">
                <svg class="w-6 h-6 text-yellow-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9"></path>
                </svg>
                <h2 class="text-2xl font-bold">新着商品通知</h2>
                <span class="ml-auto text-sm text-gray-400">リアルタイム更新</span>
            </div>

            <!-- 新着商品リスト -->
            <div id="products-list" class="space-y-3 max-h-[600px] overflow-y-auto pr-2 custom-scrollbar">
                <div class="text-center text-gray-400 py-8">読み込み中...</div>
            </div>
        </div>
    </div>

    <script>
        async function fetchData() {
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                
                // メトリクス更新
                document.getElementById('monitoring-urls').textContent = data.monitoring_urls;
                document.getElementById('cycle-count').textContent = data.cycle_count;
                
                const minutes = Math.floor(data.cycle_duration / 60);
                document.getElementById('cycle-duration-min').innerHTML = `${minutes}<span class="text-lg">分</span>`;
                document.getElementById('cycle-duration-sec').textContent = `${data.cycle_duration}秒`;
                
                // リアルタイムステータス更新
                if (data.current_script) {
                    document.getElementById('current-script').textContent = data.current_script + '.py';
                    document.getElementById('current-script').className = 'text-lg font-mono text-green-400 animate-pulse';
                } else {
                    document.getElementById('current-script').textContent = '待機中...';
                    document.getElementById('current-script').className = 'text-lg font-mono text-gray-500';
                }
                
                // 進捗バー更新
                const progress = data.cycle_progress || 0;
                document.getElementById('progress-bar').style.width = `${progress}%`;
                document.getElementById('progress-percent').textContent = `${progress}%`;
                
                // リアルタイムログ更新
                const logsContainer = document.getElementById('realtime-logs');
                if (data.recent_logs && data.recent_logs.length > 0) {
                    logsContainer.innerHTML = data.recent_logs.map(log => `
                        <div class="flex items-start gap-2 text-gray-300 hover:text-white transition-colors">
                            <span class="text-gray-500 text-[10px] min-w-[50px]">${log.time}</span>
                            <span class="flex-1">${log.content}</span>
                        </div>
                    `).join('');
                } else {
                    logsContainer.innerHTML = '<div class="text-gray-500">ログがありません</div>';
                }
                
                // 新着商品リスト更新
                const productsList = document.getElementById('products-list');
                
                if (data.recent_products && data.recent_products.length > 0) {
                    productsList.innerHTML = data.recent_products.map((product, index) => `
                        <div class="bg-gradient-to-r from-gray-800/80 to-gray-900/80 border border-gray-700/30 rounded-lg p-4 hover:border-blue-500/50 transition-all duration-300 animate-slide-in"
                             style="animation-delay: ${index * 0.05}s">
                            <div class="flex items-start justify-between">
                                <div class="flex-1">
                                    <div class="flex items-center gap-2 mb-2">
                                        <span class="px-2 py-1 bg-blue-500/20 text-blue-400 text-xs rounded font-medium">
                                            ${product.site}
                                        </span>
                                        <span class="text-xs text-gray-500">${product.time}</span>
                                    </div>
                                    <h3 class="text-white font-medium mb-2">
                                        ${product.url ? `<a href="${product.url}" target="_blank" class="hover:text-blue-400 transition-colors">${product.name}</a>` : product.name}
                                    </h3>
                                    <div class="flex items-center gap-4 text-sm">
                                        <span class="text-green-400 font-bold text-lg">
                                            ${product.price > 0 ? `¥${product.price.toLocaleString()}` : '価格未設定'}
                                        </span>
                                        ${product.category ? `
                                            <span class="text-gray-400 text-xs">
                                                ${product.category}
                                            </span>
                                        ` : ''}
                                        ${product.url ? `
                                            <a href="${product.url}" target="_blank" class="text-blue-400 hover:text-blue-300 text-xs flex items-center gap-1">
                                                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"></path>
                                                </svg>
                                                ショップを開く
                                            </a>
                                        ` : ''}
                                    </div>
                                </div>
                                <div class="ml-4">
                                    <span class="px-3 py-1 bg-green-500/10 text-green-400 text-xs rounded-full border border-green-500/20">
                                        NEW
                                    </span>
                                </div>
                            </div>
                        </div>
                    `).join('');
                } else {
                    productsList.innerHTML = '<div class="text-center text-gray-400 py-8">新着商品はまだありません</div>';
                }
                
            } catch (error) {
                console.error('データ取得エラー:', error);
            }
        }
        
        // 初回読み込み
        fetchData();
        
        // 10秒ごとに更新
        setInterval(fetchData, 10000);
    </script>
</body>
</html>
    """

if __name__ == "__main__":
    import uvicorn
    print("=" * 60)
    print("🚀 Price Hunter Dashboard 起動")
    print("=" * 60)
    print("📊 URL: http://0.0.0.0:8004")
    print("🔄 自動更新: 10秒ごと")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=8004)