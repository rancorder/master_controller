#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
isio28_clean.py 修正版
master_controller一元管理対応: DB保存処理削除、標準出力のみ
"""
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib

BASE_URL = "http://www.isio28.com"
START_URL = "http://www.isio28.com/newpage1.html"

def scrape_isio28():
    """ISIO28スクレイピング"""
    
    print(f"isio28 実行開始: {datetime.now()}")
    
    products = []
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(START_URL, headers=headers, timeout=15)
        
        # エンコーディングを自動検出
        response.encoding = response.apparent_encoding
        
        if response.status_code != 200:
            print(f"HTTPエラー: {response.status_code}")
            return 0
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # テーブルを取得
        tables = soup.find_all('table')
        print(f"テーブル数: {len(tables)}個")
        
        for table_idx, table in enumerate(tables[:5]):
            rows = table.find_all('tr')
            
            # ヘッダー行をスキップして商品行を処理
            for row_idx, row in enumerate(rows[1:]):
                try:
                    cells = row.find_all(['td', 'th'])
                    cell_texts = [cell.get_text(strip=True) for cell in cells]
                    
                    # 4列想定：管理番号、メーカー、商品名、価格
                    if len(cell_texts) >= 4:
                        管理番号 = cell_texts[0]
                        メーカー = cell_texts[1]
                        商品名 = cell_texts[2]
                        価格テキスト = cell_texts[3]
                        
                        # ヘッダー行をスキップ
                        if 管理番号 == '管理番号' or 価格テキスト == '価格':
                            continue
                        
                        # 有効なデータかチェック
                        if not (管理番号 and メーカー and 商品名 and 価格テキスト):
                            continue
                        
                        # 商品名を統合
                        full_name = f"{メーカー} {商品名}".strip()
                        
                        # 価格から数字のみ抽出
                        # 全角数字を半角に変換
                        価格テキスト = 価格テキスト.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
                        
                        # 数字とカンマのみ抽出
                        price_match = re.search(r'[\d,]+', 価格テキスト)
                        if price_match:
                            price = price_match.group().replace(',', '')
                        else:
                            continue
                        
                        # バリデーション
                        if len(full_name) < 3 or len(price) < 2:
                            continue
                        
                        # 重複チェック用ハッシュ
                        product_hash = hashlib.md5(f"{full_name}_{price}".encode()).hexdigest()
                        
                        if not any(p['hash'] == product_hash for p in products):
                            products.append({
                                'hash': product_hash,
                                'name': full_name,
                                'price': price
                            })
                
                except Exception as e:
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
    return scrape_isio28()

def main():
    scrape_isio28()

if __name__ == "__main__":
    main()