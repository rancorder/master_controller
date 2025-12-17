#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sanwa.py 修正版
master_controller一元管理対応: DB保存処理削除、標準出力のみ
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import re

BASE_URL = "http://www.camera-sanwa.co.jp"
START_URL = BASE_URL + "/list.php?312812942"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

def scrape_sanwa():
    """三和カメラスクレイピング"""
    
    print(f"sanwa 実行開始: {datetime.now()}")
    
    products = []
    
    try:
        response = requests.get(START_URL, headers=HEADERS, timeout=15)
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, "html.parser")

        rows = soup.select("#listtb tr")
        print(f"行数: {len(rows)}個")

        for row in rows:
            try:
                onclick_attr = row.get("onclick")
                cols = row.find_all("td")
                
                if onclick_attr and len(cols) >= 7:
                    name = cols[4].get_text(strip=True)
                    price_text = cols[6].get_text(strip=True)
                    
                    if name and price_text:
                        # 価格から数字のみ抽出
                        price_match = re.search(r'[\d,]+', price_text)
                        if price_match:
                            price = price_match.group().replace(',', '')
                        else:
                            price = '0'
                        
                        # バリデーション
                        if len(name) < 3 or price == '0':
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
    return scrape_sanwa()

def main():
    scrape_sanwa()

if __name__ == "__main__":
    main()