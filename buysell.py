#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
buysell.py - BrandChee 修正版
master_controller一元管理対応: DB保存処理削除、標準出力のみ
複数URL対応: url_index切り替え機能追加
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import re

def scrape_brandchee():
    """BrandChee商品スクレイピング（複数URL対応）"""
    
    print(f"scraper_04 実行開始: {datetime.now()}")
    
    try:
        BASE_URLS = [
            "https://brandchee.com/collections/camera",      # url_index: 0 (カメラ)
            "https://brandchee.com/collections/all-watches"  # url_index: 1 (時計)
        ]
        
        products = []
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        
        for url_index, url in enumerate(BASE_URLS):
            # URL切り替えマーカー出力
            print(f"---URL_INDEX:{url_index}---")
            print(f"URL {url_index + 1}/{len(BASE_URLS)} 処理中...")
            
            try:
                response = requests.get(url, headers=headers, timeout=30)
                print(f"  レスポンス: {response.status_code}")
                
                if response.status_code != 200:
                    continue
                
                soup = BeautifulSoup(response.text, "html.parser")
                
                selectors = [
                    "div.sections_pickup-txt",
                    ".product-item",
                    ".product-card"
                ]
                
                items = []
                for selector in selectors:
                    items = soup.select(selector)
                    if items:
                        print(f"    {selector}: {len(items)}個発見")
                        break
                
                if not items:
                    print("  商品要素が見つからない")
                    continue
                
                page_products = 0
                for item in items:
                    try:
                        name_elem = item.select_one("p.sections_pickup-ttl, .product-title, h3")
                        price_elem = item.select_one("p.sections_pickup-price, .product-price, .price")
                        
                        if name_elem and price_elem:
                            name_text = name_elem.get_text(strip=True)
                            price_text = price_elem.get_text(strip=True)
                            price_clean = re.sub(r'[^\d]', '', price_text)
                            
                            if name_text and price_clean:
                                product_hash = hashlib.md5(f"{name_text}_{price_clean}".encode()).hexdigest()
                                
                                products.append({
                                    'hash': product_hash,
                                    'name': name_text,
                                    'price': price_clean,
                                    'url_index': url_index
                                })
                                page_products += 1
                    except:
                        continue
                
                print(f"  {page_products}件取得")
                
            except Exception as e:
                print(f"  エラー: {e}")
                continue
        
        print(f"総取得数: {len(products)}件")
        
        # 商品情報を標準出力（master_controller用）
        # url_index別に出力
        for url_index in range(len(BASE_URLS)):
            print(f"---URL_INDEX:{url_index}---")
            url_products = [p for p in products if p['url_index'] == url_index]
            for product in url_products:
                print(f"{product['name']} {product['price']}円")
        
        if len(products) >= 10:
            print("SUCCESS")
        else:
            print("PARTIAL SUCCESS")
            
        return len(products)
        
    except Exception as e:
        print(f"ERROR: {e}")
        return 0

def scrape():
    """既存システムとの互換性維持"""
    return scrape_brandchee()

def main():
    """メイン処理"""
    scrape_brandchee()

if __name__ == "__main__":
    main()