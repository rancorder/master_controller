#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
yaotomi.py 修正版
master_controller一元管理対応: DB保存処理削除、標準出力のみ
"""
import requests
from bs4 import BeautifulSoup
import hashlib
import re
from datetime import datetime

BASE_URL = "https://www.yaotomi.co.jp/products/list?search_type=used&disp_number=100&disp_soldout=1&pageno=1"

def extract_products_flexibly(html):
    """商品情報抽出"""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()
    text = re.sub(r"\s+", " ", text)

    # 商品名＋価格の抽出
    pattern = re.compile(r"(【.*?】.*?)(\d{1,3}(?:,\d{3})+円|\¥\d{1,3}(?:,\d{3})+)")
    matches = pattern.findall(text)

    products = []
    for name, price in matches[:30]:
        # 価格から数字のみ抽出（カンマと円を除去）
        price_clean = re.sub(r'[^\d]', '', price)
        
        # 重複チェック用ハッシュ
        product_hash = hashlib.md5(f"{name}_{price_clean}".encode()).hexdigest()
        
        products.append({
            "hash": product_hash,
            "name": name.strip(),
            "price": price_clean
        })
    
    return products

def scrape_yaotomi():
    """Yaotomi スクレイピング"""
    
    print(f"yaotomi 実行開始: {datetime.now()}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(BASE_URL, headers=headers, timeout=15)
        response.encoding = response.apparent_encoding
        response.raise_for_status()
        
        products = extract_products_flexibly(response.text)
        
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
    """既存システムとの互換性維持"""
    return scrape_yaotomi()

if __name__ == "__main__":
    scrape_yaotomi()