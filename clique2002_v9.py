#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
clique2002_monitor_v10.py - Image Hash Change Detector

★コンセプト:
「画像の中身」をOCRで読むのをやめ、「画像の指紋（ハッシュ値）」が変わったかを監視する。
Sold-outになると画像が差し替わる、または加工されるサイトの特性を利用する。

依存ライブラリ:
pip install requests beautifulsoup4
"""

import json
import logging
import os
import re
import sys
import time
import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ============================================================================
# 設定
# ============================================================================

@dataclass(frozen=True)
class Config:
    # ターゲット
    BASE_URL: str = "http://www.clique2002.com/"
    TARGET_URL: str = "http://www.clique2002.com/goods-20-used.html"
    
    # 通知設定
    CHATWORK_TOKEN: str = os.getenv('CHATWORK_TOKEN', '')
    CHATWORK_ROOM_ID: str = "385402385"
    
    # 保存ファイル
    STATE_FILE: Path = Path("clique2002_hash_state.json")
    
    # ログ設定
    LOG_LEVEL: int = logging.INFO

# ============================================================================
# ユーティリティ
# ============================================================================

def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(Config.LOG_LEVEL)
    return logger

LOGGER = setup_logger('HashBot')

# ============================================================================
# データモデル
# ============================================================================

@dataclass
class Product:
    product_id: str
    url: str
    image_url: str
    image_hash: str = ""  # 画像のMD5値
    
    def to_dict(self) -> dict:
        return asdict(self)

# ============================================================================
# コアロジック: 状態管理 & ハッシュ比較
# ============================================================================

class StateManager:
    """前回の状態（順位と画像ハッシュ）を管理する"""
    
    def __init__(self, file_path: Path):
        self.file_path = file_path

    def load_previous_state(self) -> Dict[str, dict]:
        """
        前回のデータをロードし、前回実行時のタイムスタンプも保持する。
        Return: {product_id: {'rank': int, 'hash': str}, 'metadata': {'timestamp': str}}
        """
        if not self.file_path.exists():
            return {}
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            products_state = {
                p['product_id']: {'rank': i, 'hash': p.get('image_hash', '')}
                for i, p in enumerate(data.get('products', []))
            }
            # メタデータとしてタイムスタンプを追加
            products_state['metadata'] = {'timestamp': data.get('timestamp')}
            return products_state
        except Exception as e:
            LOGGER.error(f"⚠️ 状態ロード失敗: {e}")
            return {}

    def save_state(self, products: List['Product']): # 'Product'は型ヒントのため
        """現在の状態と実行タイムスタンプを保存"""
        current_time = datetime.now().isoformat()
        data = {
            'timestamp': current_time, # ここで最新のタイムスタンプを保存
            'count': len(products),
            'products': [p.to_dict() for p in products]
        }
        try:
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            LOGGER.info(f"💾 状態を保存しました: {len(products)}件")
        except Exception as e:
            LOGGER.error(f"❌ 保存失敗: {e}")

# ============================================================================
# サービス: スクレイパー & ハッシュ計算
# ============================================================================

class HashScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
        })

    def get_image_hash(self, img_url: str) -> str:
        """画像URLからバイナリを取得しMD5ハッシュを返す"""
        if not img_url:
            return "no_image"
        
        try:
            # 画像ダウンロード（タイムアウト短め）
            resp = self.session.get(img_url, timeout=10)
            if resp.status_code == 200:
                return hashlib.md5(resp.content).hexdigest()
            return "download_error"
        except Exception:
            return "download_error"

    def scrape(self) -> List[Product]:
        """サイトから商品リストと画像情報を取得"""
        LOGGER.info(f"📥 サイト取得開始: {Config.TARGET_URL}")
        try:
            resp = self.session.get(Config.TARGET_URL, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            LOGGER.error(f"❌ 接続エラー: {e}")
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        products = []
        
        # 商品リンクのパターン
        links = soup.find_all('a', href=re.compile(r'ct-([A-Z]{2}-\d{3})\.html', re.IGNORECASE))
        
        LOGGER.info(f"🔍 発見商品数: {len(links)}")

        for link in links:
            href = link.get('href')
            match = re.search(r'ct-([A-Z]{2}-\d{3})\.html', href, re.IGNORECASE)
            if not match:
                continue
                
            pid = match.group(1)
            detail_url = urljoin(Config.BASE_URL, href)
            
            # 画像URLの特定 (リストページのサムネイル、またはボタン画像を想定)
            # ※サイト構造に合わせて調整: ここでは行(tr)内のimgタグを探す
            img_url = ""
            parent_tr = link.find_parent('tr')
            if parent_tr:
                # 商品画像っぽいものを探す（IDが含まれるjpgなど）
                img = parent_tr.find('img', src=re.compile(r'\.jpg|\.gif', re.IGNORECASE))
                if img:
                    img_url = urljoin(Config.BASE_URL, img.get('src', ''))

            products.append(Product(
                product_id=pid,
                url=detail_url,
                image_url=img_url
            ))
            
        return products

# ============================================================================
# 通知サービス
# ============================================================================

def send_chatwork(messages: List[str]):
    """Chatwork通知"""
    if not messages or not Config.CHATWORK_TOKEN:
        if messages:
            print("\n".join(messages)) # トークンがない場合は標準出力
        return

    body = "[info][title]Clique Monitor Report (Hash)[/title]" + "\n".join(messages) + "[/info]"
    url = f"https://api.chatwork.com/v2/rooms/{Config.CHATWORK_ROOM_ID}/messages"
    headers = {'X-ChatWorkToken': Config.CHATWORK_TOKEN}
    
    try:
        requests.post(url, headers=headers, data={'body': body}, timeout=5)
        LOGGER.info("📢 通知送信完了")
    except Exception as e:
        LOGGER.error(f"❌ 通知失敗: {e}")

# ============================================================================
# メイン処理
# ============================================================================

def main():
    start_time = time.time()
    
    scraper = HashScraper()
    state_manager = StateManager(Config.STATE_FILE)
    
    # 1. 前回の状態ロード
    prev_state = state_manager.load_previous_state()
    prev_ids = list(prev_state.keys())
    
    # 2. 現在のリスト取得
    current_products = scraper.scrape()
    if not current_products:
        return

    current_ids = [p.product_id for p in current_products]
    messages = []

    # 3. 順位変動と画像変更のチェック
    # リストの上位が変わったか？
    if prev_ids and current_ids[:30] != prev_ids[:30]:
        messages.append(f"🔄 順位変動あり (Top 30): {prev_ids[:30]} -> {current_ids[:30]}")

    # 新着チェック
    new_items = set(current_ids) - set(prev_ids)
    if new_items:
        messages.append(f"✨ 新着商品: {', '.join(new_items)}")

    # 画像ハッシュチェック（重い処理なので、必要な商品だけ実施するのが吉だが、今回は全件やる）
    # ※全件やっても画像ダウンロードだけなら早いが、頻繁に叩くなら上位20件に絞るなどの調整も可
    LOGGER.info("📸 画像ハッシュチェック開始...")
    
    changes_detected = False
    
    for product in current_products:
        # 新着 または 既存商品の画像変更をチェック
        prev_data = prev_state.get(product.product_id)
        
        # ハッシュ計算実行
        current_hash = scraper.get_image_hash(product.image_url)
        product.image_hash = current_hash
        
        if prev_data:
            prev_hash = prev_data['hash']
            # 前回のハッシュがあり、かつ今回と違う場合
            if prev_hash and prev_hash != "download_error" and prev_hash != current_hash:
                LOGGER.info(f"⚠️ 画像変更検知: {product.product_id}")
                messages.append(f"🎨 画像変化 (状態変更の可能性): {product.product_id}\n{product.url}")
                changes_detected = True
        elif product.product_id in new_items:
            # 新着はハッシュ比較できないのでスルー（新着通知でカバー）
            pass

    # 4. 状態保存
    state_manager.save_state(current_products)
    
    # 5. 通知
    if messages:
        LOGGER.info(f"📢 通知対象: {len(messages)}件")
        send_chatwork(messages)
    else:
        LOGGER.info("💤 変化なし")

    elapsed = time.time() - start_time
    LOGGER.info(f"✅ 完了: {elapsed:.2f}秒")

if __name__ == "__main__":
    main()