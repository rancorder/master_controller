#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
clique2002_v8.py - Playwright + OCR完全版

★真の解決策:
1. Playwrightでページを開く
2. 各商品の情報エリア（右側の青い部分）をスクリーンショット
3. OCRで黄色の "Sold-out" テキストを検出
4. 最初の販売中商品が見つかったら残りはスキップ
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from PIL import Image, ImageEnhance

# Playwright
from playwright.sync_api import sync_playwright, Page

# OCR
try:
    import pytesseract
except ImportError:
    print("❌ pytesseract未インストール")
    print("pip install pytesseract")
    sys.exit(1)

# ============================================================================
# 設定
# ============================================================================

class Config:
    BASE_URL = "http://www.clique2002.com/"
    TARGET_URL = "http://www.clique2002.com/goods-20-used.html"
    
    CHATWORK_TOKEN = os.getenv('CHATWORK_TOKEN', '')
    CHATWORK_ROOM_ID = "385402385"
    
    SNAPSHOT_FILE = "clique2002_snapshot_v8.json"
    OCR_CACHE_FILE = "clique2002_ocr_cache_v8.json"
    
    SOLD_OUT_KEYWORDS = [
        'sold', 'soldout', 'sold-out', 'sold out',
        '完売', '売切', 'うりきれ'
    ]
    
    # スクリーンショット保存ディレクトリ
    SCREENSHOT_DIR = Path("clique2002_screenshots")


# ============================================================================
# ロガー
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
LOGGER = logging.getLogger('Clique2002')


# ============================================================================
# データクラス
# ============================================================================

@dataclass
class Product:
    """商品データ"""
    product_id: str
    product_image_url: str
    detail_url: str
    position: int
    is_sold_out: bool = False
    sold_out_method: str = ""  # 'cache', 'ocr', 'skip', 'error'


# ============================================================================
# OCR Sold-out判定 (Playwright版)
# ============================================================================

class PlaywrightOCRDetector:
    """Playwright + OCRでSold-out判定"""
    
    def __init__(self):
        self.cache: Dict[str, bool] = {}
        self.cache_file = Config.OCR_CACHE_FILE
        self._load_cache()
        self._verify_tesseract()
        
        # スクリーンショット保存ディレクトリ
        Config.SCREENSHOT_DIR.mkdir(exist_ok=True)
        LOGGER.info(f"📁 スクリーンショット保存先: {Config.SCREENSHOT_DIR}")
    
    def _verify_tesseract(self) -> None:
        """Tesseract確認"""
        try:
            version = pytesseract.get_tesseract_version()
            LOGGER.info(f"✅ Tesseract OCR: v{version}")
        except Exception as e:
            LOGGER.error(f"❌ Tesseract未インストール: {e}")
            sys.exit(1)
    
    def _load_cache(self) -> None:
        """キャッシュ読み込み"""
        try:
            if Path(self.cache_file).exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.cache = data.get('cache', {})
                LOGGER.info(f"📁 OCRキャッシュ: {len(self.cache)}件")
        except Exception as e:
            LOGGER.warning(f"⚠️ キャッシュ読み込み失敗: {e}")
            self.cache = {}
    
    def _save_cache(self) -> None:
        """キャッシュ保存"""
        try:
            if len(self.cache) > 1000:
                items = list(self.cache.items())[-1000:]
                self.cache = dict(items)
            
            data = {
                'cache': self.cache,
                'last_updated': datetime.now().isoformat()
            }
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            LOGGER.warning(f"⚠️ キャッシュ保存失敗: {e}")
    
    def check_products_with_browser(self, products: List[Product]) -> List[Product]:
        """
        ★Playwrightでブラウザ起動してOCRチェック
        
        処理フロー:
        1. ページ読み込み
        2. 上位商品から順にボタン画像をスクリーンショット
        3. OCRで判定
        4. 最初の販売中商品が見つかったら残りはスキップ
        """
        LOGGER.info(f"🔍 Playwright + OCR Sold-outチェック: 最大{len(products)}件")
        
        checked_count = 0
        cache_hits = 0
        ocr_checks = 0
        sold_out_count = 0
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            try:
                # ページ読み込み
                LOGGER.info(f"📥 ページ読み込み: {Config.TARGET_URL}")
                page.goto(Config.TARGET_URL, timeout=60000)
                page.wait_for_load_state('networkidle')
                LOGGER.info("✅ ページ読み込み完了")
                
                # 上位から順にチェック
                for i, product in enumerate(products, 1):
                    product_id = product.product_id
                    
                    # キャッシュチェック
                    if product_id in self.cache:
                        is_sold_out = self.cache[product_id]
                        product.is_sold_out = is_sold_out
                        product.sold_out_method = 'cache'
                        cache_hits += 1
                        
                        if is_sold_out:
                            sold_out_count += 1
                            LOGGER.info(f"   [{i}/{len(products)}] ❌ {product_id}: Sold-out (cache)")
                        else:
                            LOGGER.info(f"   [{i}/{len(products)}] ✅ {product_id}: 販売中 (cache)")
                            LOGGER.info(f"     💡 最初の販売中商品発見: {product_id}")
                            # 残りは全て販売中
                            for remaining in products[i:]:
                                if remaining.sold_out_method == "":
                                    remaining.is_sold_out = False
                                    remaining.sold_out_method = 'skip'
                            break
                        
                        checked_count += 1
                        continue
                    
                    # ★ボタン画像要素を特定してスクリーンショット
                    is_sold_out, method = self._screenshot_and_ocr(page, product_id)
                    
                    product.is_sold_out = is_sold_out
                    product.sold_out_method = method
                    
                    if method == 'ocr':
                        ocr_checks += 1
                        checked_count += 1
                        
                        # キャッシュ保存
                        self.cache[product_id] = is_sold_out
                        
                        if is_sold_out:
                            sold_out_count += 1
                            LOGGER.info(f"   [{i}/{len(products)}] ❌ {product_id}: Sold-out (ocr)")
                        else:
                            LOGGER.info(f"   [{i}/{len(products)}] ✅ {product_id}: 販売中 (ocr)")
                            LOGGER.info(f"     💡 最初の販売中商品発見: {product_id}")
                            
                            # 残りは全て販売中
                            for remaining in products[i:]:
                                if remaining.sold_out_method == "":
                                    remaining.is_sold_out = False
                                    remaining.sold_out_method = 'skip'
                            break
                    else:
                        checked_count += 1
                
            finally:
                browser.close()
        
        # キャッシュ保存
        if ocr_checks > 0:
            self._save_cache()
        
        available_count = sum(1 for p in products if not p.is_sold_out)
        
        LOGGER.info(
            f"✅ OCRチェック完了: チェック済={checked_count}件, "
            f"販売中={available_count}件, Sold-out={sold_out_count}件 "
            f"(cache={cache_hits}, ocr={ocr_checks})"
        )
        
        return products
    
    def _screenshot_and_ocr(self, page: Page, product_id: str) -> Tuple[bool, str]:
        """
        ★商品情報エリア（右側の青い部分）をスクリーンショット → OCR
        
        Args:
            page: Playwrightページ
            product_id: 商品ID (例: HC-078)
        
        Returns:
            (is_sold_out, method)
        """
        try:
            # ★方法1: アンカー要素 <a name="HC-078"> の親tdをスクリーンショット
            # <td colspan="4"><a name="HC-078"><img src="button...gif"></a></td>
            selector = f'a[name="{product_id}"]'
            
            element = page.query_selector(selector)
            if not element:
                LOGGER.warning(f"⚠️ {product_id}: アンカー要素が見つかりません")
                return False, 'error'
            
            # 親のtd要素（青い商品情報エリア全体）を取得
            parent_td = element.locator('xpath=ancestor::td[@bgcolor="#666666"]').first
            
            # スクリーンショット撮影
            screenshot_path = Config.SCREENSHOT_DIR / f"{product_id}_info.png"
            parent_td.screenshot(path=str(screenshot_path))
            
            # 画像読み込み
            img = Image.open(screenshot_path)
            
            # ★画像前処理: OCR精度向上
            img = img.convert('L')  # グレースケール
            enhancer = ImageEnhance.Contrast(img)
            img = enhancer.enhance(3.0)  # コントラスト強化（黄色文字を強調）
            
            # リサイズ（2倍に拡大）
            new_size = (img.width * 2, img.height * 2)
            img = img.resize(new_size, Image.Resampling.LANCZOS)
            
            # 前処理済み画像を保存（デバッグ用）
            processed_path = Config.SCREENSHOT_DIR / f"{product_id}_processed.png"
            img.save(processed_path)
            
            # OCR実行
            text = pytesseract.image_to_string(img, lang='eng+jpn')
            text_lower = text.lower()
            
            # デバッグ
            LOGGER.debug(f"OCR結果({product_id}): {text[:150]}")
            
            # キーワード検索
            is_sold_out = any(kw in text_lower for kw in Config.SOLD_OUT_KEYWORDS)
            
            if is_sold_out:
                matched_kw = [kw for kw in Config.SOLD_OUT_KEYWORDS if kw in text_lower]
                LOGGER.info(f"     🔍 Sold-out検出: キーワード={matched_kw}")
            
            return is_sold_out, 'ocr'
        
        except Exception as e:
            LOGGER.warning(f"OCRエラー({product_id}): {e}")
            return False, 'error'


# ============================================================================
# スクレイパー
# ============================================================================

class Clique2002Scraper:
    """Clique2002スクレイパー v8.0"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
        })
        
        self.ocr_detector = PlaywrightOCRDetector()
    
    def scrape(self) -> List[Product]:
        """メインスクレイピング"""
        start_time = time.time()
        
        LOGGER.info(f"🚀 スクレイピング開始: {Config.TARGET_URL}")
        
        # HTML取得
        response = self.session.get(Config.TARGET_URL, timeout=30)
        if response.status_code != 200:
            LOGGER.error(f"❌ HTTPエラー: {response.status_code}")
            return []
        
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        
        # 商品リンク抽出
        product_links = soup.find_all('a', href=re.compile(r'ct-([A-Z]{2}-\d{3})\.html', re.IGNORECASE))
        
        LOGGER.info(f"📦 商品リンク抽出: {len(product_links)}件")
        
        products: List[Product] = []
        
        for i, link in enumerate(product_links, 1):
            href = link.get('href', '')
            match = re.search(r'ct-([A-Z]{2}-\d{3})\.html', href, re.IGNORECASE)
            if not match:
                continue
            
            product_id = match.group(1)
            detail_url = urljoin(Config.BASE_URL, href)
            
            # 商品画像URL（左側のサムネイル）
            parent_tr = link.find_parent('tr')
            if parent_tr:
                img_tag = parent_tr.find('img', src=re.compile(rf'{product_id}.*\.jpg', re.IGNORECASE))
                if img_tag:
                    product_image_url = urljoin(Config.BASE_URL, img_tag.get('src', ''))
                else:
                    product_image_url = ""
            else:
                product_image_url = ""
            
            product = Product(
                product_id=product_id,
                product_image_url=product_image_url,
                detail_url=detail_url,
                position=i
            )
            
            products.append(product)
        
        LOGGER.info(f"✅ 商品データ構築: {len(products)}件")
        
        # ★Playwright + OCRでSold-outチェック
        products = self.ocr_detector.check_products_with_browser(products)
        
        duration = time.time() - start_time
        
        total = len(products)
        available = sum(1 for p in products if not p.is_sold_out)
        sold_out = sum(1 for p in products if p.is_sold_out)
        
        LOGGER.info(
            f"✅ 完了: 総計{total}件, 販売中{available}件, Sold-out{sold_out}件, {duration:.1f}秒"
        )
        
        return products


# ============================================================================
# メイン
# ============================================================================

def main():
    """メイン処理"""
    print("=" * 80)
    print("Clique2002 Scraper v8.0 - Playwright + OCR完全版")
    print("=" * 80)
    
    scraper = Clique2002Scraper()
    products = scraper.scrape()
    
    # 販売中商品のみ表示
    available_products = [p for p in products if not p.is_sold_out]
    
    print(f"\n📋 販売中商品: {len(available_products)}件")
    for p in available_products[:10]:
        print(f"  - {p.product_id}: {p.detail_url}")
    
    if len(available_products) > 10:
        print(f"  ... 他{len(available_products) - 10}件")


if __name__ == "__main__":
    main()