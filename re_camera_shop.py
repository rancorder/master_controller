#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
re_camera_shop.py（URL index対応）
master_controller一元管理対応: DB保存処理削除、標準出力のみ
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import re

BASE_URLS = [
    "https://re-camera-shop.com/category/index.jsp?ctglyid=480_2",  # url_index: 0
    "https://re-camera-shop.com/category/index.jsp?ctglyid=407_2",  # url_index: 1
    "https://re-camera-shop.com/category/index.jsp?ctglyid=376_2"   # url_index: 2
]

def scrape_re_camera():
    """re-camera-shopスクレイピング"""
    
    print(f"re_camera_shop 実行開始: {datetime.now()}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        for url_index, url in enumerate(BASE_URLS):
            # URL切り替えを明示
            print(f"---URL_INDEX:{url_index}---")
            print(f"URL {url_index+1}/{len(BASE_URLS)} 処理中...")
            
            try:
                response = requests.get(url, headers=headers, timeout=15)
                response.encoding = response.apparent_encoding
                
                if response.status_code != 200:
                    print(f"  HTTPエラー: {response.status_code}")
                    continue
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 商品リストを取得
                items = soup.select("li.item")
                print(f"  商品数: {len(items)}個")
                
                page_products = 0
                seen_hashes = set()
                
                for item in items:
                    try:
                        # メーカー・商品名
                        manufacturer_tag = item.select_one("p.manufacturer")
                        if not manufacturer_tag:
                            continue
                        
                        name = manufacturer_tag.get_text(strip=True)
                        
                        # 価格
                        price_tag = item.select_one("p.price")
                        if not price_tag:
                            continue
                        
                        price_text = price_tag.get_text(strip=True)
                        
                        # 価格から数字のみ抽出
                        price_match = re.search(r'[\d,]+', price_text)
                        if price_match:
                            price = price_match.group().replace(',', '')
                        else:
                            continue
                        
                        # バリデーション
                        if len(name) < 3 or len(price) < 2:
                            continue
                        
                        # 重複チェック用ハッシュ
                        product_hash = hashlib.md5(f"{name}_{price}".encode()).hexdigest()
                        
                        if product_hash not in seen_hashes:
                            seen_hashes.add(product_hash)
                            print(f"{name} {price}円")
                            page_products += 1
                    
                    except:
                        continue
                
                print(f"  {page_products}件取得")
            
            except Exception as e:
                print(f"  URL処理エラー: {e}")
                continue
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

def scrape():
    return scrape_re_camera()

def main():
    scrape_re_camera()

if __name__ == "__main__":
    main()