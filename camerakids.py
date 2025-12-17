#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
camerakids.py（URL index対応）
master_controller一元管理対応: DB保存処理削除、標準出力のみ
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import re

BASE_URLS = [
    "https://www.camerakids.jp/SHOP/307752/t01/list.html",  # url_index: 0
    "https://www.camerakids.jp/SHOP/307754/t01/list.html"   # url_index: 1
]

def scrape_camerakids():
    """camerakidsスクレイピング"""
    
    print(f"camerakids 実行開始: {datetime.now()}")
    
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
                
                # セレクタテスト
                print("  セレクタテスト:")
                
                tbody_rows = soup.select("tbody tr")
                print(f"    tbody tr: {len(tbody_rows)}個")
                
                all_rows = soup.select("tr")
                print(f"    tr: {len(all_rows)}個")
                
                items = soup.select("div.item")
                print(f"    div.item: {len(items)}個")
                
                # trを使用（2列構造: 画像列 + 情報列）
                print(f"  全てのtrを使用（2列構造）")
                rows = all_rows
                
                page_products = 0
                seen_hashes = set()
                
                for row in rows:
                    try:
                        # 商品名（h2.goods または imgのalt）
                        name_tag = row.select_one("h2.goods")
                        if not name_tag:
                            img_tag = row.select_one("img[alt]")
                            if img_tag:
                                name = img_tag.get('alt', '').strip()
                            else:
                                continue
                        else:
                            name = name_tag.get_text(strip=True)
                        
                        # 価格
                        price_tag = row.select_one("div.price")
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
    return scrape_camerakids()

def main():
    scrape_camerakids()

if __name__ == "__main__":
    main()