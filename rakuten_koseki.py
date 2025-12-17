#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kamera_koseki.py - カメラの小石スクレイパー
master_controller一元管理対応: DB保存処理削除、標準出力のみ

対象URL: https://item.rakuten.co.jp/kameranokoseki/c/0000000214/?s=4&i=1
取得データ: 商品名、価格
"""

import hashlib
import re
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ==========================================
# 設定
# ==========================================

# 開発モード（本番では False に設定）
DEBUG_MODE = False

# 対象URL
TARGET_URL = "https://item.rakuten.co.jp/kameranokoseki/c/0000000214/?s=4&i=1"

# リトライ設定
MAX_RETRIES = 3
RETRY_DELAYS = [0, 5, 10]  # 秒（1回目は即座、2回目は5秒、3回目は10秒）

# タイムアウト設定
REQUEST_TIMEOUT = 30  # 秒

# レート制限対策
REQUEST_DELAY = 2  # リクエスト間隔（秒）

# User-Agent
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0"
}


# ==========================================
# ログ出力関数
# ==========================================

def log_debug(message: str) -> None:
    """デバッグログ出力（開発モードのみ）
    
    Args:
        message: ログメッセージ
    """
    if DEBUG_MODE:
        print(f"[DEBUG] {message}", file=sys.stderr)


def log_info(message: str) -> None:
    """情報ログ出力
    
    Args:
        message: ログメッセージ
    """
    print(f"[INFO] {message}", file=sys.stderr)


def log_error(message: str) -> None:
    """エラーログ出力
    
    Args:
        message: ログメッセージ
    """
    print(f"[ERROR] {message}", file=sys.stderr)


# ==========================================
# スクレイピング処理
# ==========================================

def fetch_html_with_retry(url: str) -> Optional[str]:
    """HTMLを取得（リトライ機能付き）
    
    Args:
        url: 取得対象URL
        
    Returns:
        Optional[str]: HTMLテキスト、失敗時はNone
    """
    for attempt in range(MAX_RETRIES):
        try:
            if attempt > 0:
                delay = RETRY_DELAYS[attempt]
                log_info(f"リトライ {attempt}/{MAX_RETRIES} - {delay}秒待機...")
                time.sleep(delay)
            
            log_debug(f"リクエスト送信: {url}")
            response = requests.get(
                url,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True
            )
            
            log_debug(f"レスポンス: {response.status_code}")
            
            # ステータスコードチェック
            if response.status_code == 200:
                log_debug(f"HTML取得成功 ({len(response.text)} bytes)")
                return response.text
            elif response.status_code == 429:
                log_error(f"レート制限エラー (429) - リトライ {attempt + 1}/{MAX_RETRIES}")
                time.sleep(REQUEST_DELAY * 2)  # 通常の2倍待機
                continue
            elif response.status_code in [503, 502, 504]:
                log_error(f"サーバーエラー ({response.status_code}) - リトライ {attempt + 1}/{MAX_RETRIES}")
                continue
            else:
                log_error(f"HTTPエラー: {response.status_code}")
                return None
                
        except requests.exceptions.Timeout:
            log_error(f"タイムアウト ({REQUEST_TIMEOUT}秒) - リトライ {attempt + 1}/{MAX_RETRIES}")
            continue
        except requests.exceptions.ConnectionError as e:
            log_error(f"接続エラー: {e} - リトライ {attempt + 1}/{MAX_RETRIES}")
            continue
        except requests.exceptions.RequestException as e:
            log_error(f"リクエストエラー: {e}")
            return None
        except Exception as e:
            log_error(f"予期しないエラー: {e}")
            return None
    
    log_error(f"全リトライ失敗 ({MAX_RETRIES}回)")
    return None


def parse_products(html: str) -> List[Dict[str, str]]:
    """HTMLから商品情報を抽出
    
    Args:
        html: HTMLテキスト
        
    Returns:
        List[Dict[str, str]]: 商品情報のリスト
            - name: 商品名
            - price: 価格（数値文字列）
            - hash: 商品ハッシュ（重複検知用）
    """
    try:
        soup = BeautifulSoup(html, 'html.parser')
        products = []
        
        # 商品リンクを取得（class="category_itemnamelink"）
        product_links = soup.find_all('a', class_='category_itemnamelink')
        log_debug(f"商品リンク検出数: {len(product_links)}")
        
        for link in product_links:
            try:
                # 商品名取得
                name = link.get_text(strip=True)
                if not name:
                    log_debug("商品名が空 - スキップ")
                    continue
                
                # 価格取得（次の兄弟要素から）
                price_span = link.find_next('span', class_='category_itemprice')
                if not price_span:
                    log_debug(f"価格要素未検出: {name[:30]}... - スキップ")
                    continue
                
                price_text = price_span.get_text(strip=True)
                price = extract_price(price_text)
                
                if price is None:
                    log_debug(f"価格抽出失敗: {price_text} - スキップ")
                    continue
                
                # 商品ハッシュ生成（重複検知用）
                product_hash = generate_product_hash(name, price)
                
                products.append({
                    'name': name,
                    'price': str(price),
                    'hash': product_hash
                })
                
                log_debug(f"商品取得: {name[:50]}... / {price}円")
                
            except Exception as e:
                log_debug(f"商品パースエラー: {e}")
                continue
        
        log_info(f"商品パース完了: {len(products)}件")
        return products
        
    except Exception as e:
        log_error(f"HTMLパースエラー: {e}")
        return []


def extract_price(price_text: str) -> Optional[int]:
    """価格テキストから数値を抽出
    
    Args:
        price_text: 価格テキスト（例: "11,000円 "）
        
    Returns:
        Optional[int]: 価格（数値）、抽出失敗時はNone
    """
    try:
        # 数字とカンマのみ抽出
        price_str = re.sub(r'[^\d,]', '', price_text)
        # カンマ削除
        price_str = price_str.replace(',', '')
        
        if not price_str:
            return None
        
        price = int(price_str)
        
        # 妥当性チェック（100円～1000万円）
        if 100 <= price <= 10000000:
            return price
        else:
            log_debug(f"価格範囲外: {price}円")
            return None
            
    except ValueError:
        return None
    except Exception:
        return None


def generate_product_hash(name: str, price: int) -> str:
    """商品ハッシュを生成（重複検知用）
    
    Args:
        name: 商品名
        price: 価格
        
    Returns:
        str: MD5ハッシュ（8桁）
    """
    key = f"{name}_{price}"
    return hashlib.md5(key.encode('utf-8')).hexdigest()[:8]


# ==========================================
# メイン処理
# ==========================================

def scrape_kamera_koseki() -> int:
    """カメラの古関商品スクレイピング
    
    Returns:
        int: 取得商品数
    """
    log_info(f"scraper_kamera_koseki 実行開始: {datetime.now()}")
    
    try:
        # HTML取得
        html = fetch_html_with_retry(TARGET_URL)
        
        if html is None:
            log_error("HTML取得失敗")
            print("ERROR: HTML取得失敗")
            return 0
        
        # レート制限対策：少し待機
        time.sleep(REQUEST_DELAY)
        
        # 商品情報抽出
        products = parse_products(html)
        
        if not products:
            log_error("商品データ0件")
            print("ERROR: 商品データ0件")
            return 0
        
        log_info(f"総取得数: {len(products)}件")
        
        # 商品情報を標準出力（master_controller用）
        for product in products:
            print(f"{product['name']} {product['price']}円")
        
        # 成功判定
        if len(products) >= 10:
            print("SUCCESS")
        else:
            print("PARTIAL SUCCESS")
        
        return len(products)
        
    except Exception as e:
        log_error(f"予期しないエラー: {e}")
        print(f"ERROR: {e}")
        return 0


def scrape() -> int:
    """既存システムとの互換性維持
    
    Returns:
        int: 取得商品数
    """
    return scrape_kamera_koseki()


def main() -> None:
    """メイン処理"""
    result = scrape_kamera_koseki()
    log_info(f"実行完了: {result}件取得")


if __name__ == "__main__":
    main()