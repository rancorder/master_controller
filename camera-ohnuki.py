#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
camera-ohnuki.py 修正版
master_controller一元管理対応: DB保存処理削除、標準出力のみ
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import re

BASE_URL = "https://www.camera-ohnuki.com"
START_URL = BASE_URL + "/collections/all"

def scrape_ohnuki():
    """大貫カメラスクレイピング"""
    
    print(f"camera-ohnuki 実行開始: {datetime.now()}")
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    products = []
    
    try:
        response = session.get(START_URL, timeout=15)
        if response.status_code != 200:
            print(f"HTTPエラー: {response.status_code}")
            return 0
            
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 複数のセレクタパターンを試す
        print("セレクタテスト開始...")
        
        # パターン1: 元のセレクタ
        titles = soup.select(".product-item__title")
        prices = soup.select(".product-item__price-list .price")
        print(f"  パターン1: タイトル={len(titles)}個, 価格={len(prices)}個")
        
        # パターン2: より広範なセレクタ
        if len(titles) == 0:
            titles = soup.select("[class*='product'][class*='title']")
            print(f"  パターン2: タイトル={len(titles)}個")
        
        if len(prices) == 0:
            prices = soup.select("[class*='price']")
            print(f"  パターン2: 価格={len(prices)}個")
        
        # パターン3: 商品カード全体から抽出
        if len(titles) == 0 or len(prices) == 0:
            print("  パターン3: 商品カードから抽出...")
            product_cards = soup.select("[class*='product-item'], [class*='product-card']")
            print(f"  商品カード数: {len(product_cards)}個")
            
            for card in product_cards:
                try:
                    # 商品名
                    name_tag = (
                        card.select_one("[class*='title']") or
                        card.select_one("h3") or
                        card.select_one("a[href*='/products/']")
                    )
                    
                    # 価格
                    price_tag = card.select_one("[class*='price']")
                    
                    if name_tag and price_tag:
                        name = name_tag.get_text(strip=True)
                        price_text = price_tag.get_text(strip=True)
                        
                        # 価格から数字のみ抽出
                        price_match = re.search(r'[\d,]+', price_text)
                        if price_match:
                            price = price_match.group().replace(',', '')
                        else:
                            continue
                        
                        # 重複チェック用ハッシュ
                        product_hash = hashlib.md5(f"{name}_{price}".encode()).hexdigest()
                        
                        if not any(p['hash'] == product_hash for p in products):
                            products.append({
                                'hash': product_hash,
                                'name': name,
                                'price': price
                            })
                
                except:
                    continue
        
        else:
            # パターン1または2で取得できた場合
            for title, price in zip(titles, prices):
                try:
                    name = title.get_text(strip=True)
                    price_text = price.get_text(strip=True)
                    
                    # 価格から数字のみ抽出
                    price_match = re.search(r'[\d,]+', price_text)
                    if price_match:
                        price_clean = price_match.group().replace(',', '')
                    else:
                        continue
                    
                    if name and price_clean:
                        product_hash = hashlib.md5(f"{name}_{price_clean}".encode()).hexdigest()
                        
                        if not any(p['hash'] == product_hash for p in products):
                            products.append({
                                'hash': product_hash,
                                'name': name,
                                'price': price_clean
                            })
                
                except:
                    continue
        
        print(f"総取得数: {len(products)}件")
        
        # 商品情報を標準出力（master_controller用）
        if len(products) > 0:
            for product in products:
                print(f"{product['name']} {product['price']}円")
        
        # 結果判定
        if len(products) >= 10:
            print("SUCCESS")
        elif len(products) > 0:
            print("PARTIAL SUCCESS")
        else:
            print("NO DATA")
        
        return len(products)
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 0

def scrape():
    return scrape_ohnuki()

def main():
    scrape_ohnuki()

if __name__ == "__main__":
    main()