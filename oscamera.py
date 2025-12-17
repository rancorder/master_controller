#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
oscamera.py 修正版
master_controller一元管理対応: DB保存処理削除、標準出力のみ
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import re

BASE_URL = "https://www.oscameraservice.com/new-product.html"

def scrape_oscamera():
    """OSカメラスクレイピング"""
    
    print(f"oscamera 実行開始: {datetime.now()}")
    
    products = []
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(BASE_URL, headers=headers, timeout=15)
        response.encoding = response.apparent_encoding
        
        if response.status_code != 200:
            print(f"HTTPエラー: {response.status_code}")
            return 0
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # テーブル行を取得
        rows = soup.find_all('tr')
        print(f"行数: {len(rows)}個")
        
        # 商品情報を抽出（柔軟なパターン）
        print("商品抽出開始...")
        
        for idx, row in enumerate(rows):
            try:
                # strongタグを含む行を商品名候補とする
                strong_tag = row.find('strong')
                if not strong_tag:
                    continue
                
                name_text = strong_tag.get_text(strip=True)
                
                # 短すぎる・空白のみは除外
                if len(name_text) < 5:
                    continue
                
                # ヘッダー・フッター・お知らせ行を除外
                skip_keywords = ['OS CAMERA', 'お問い合わせ', '担当', '更新', 'gmail', '電話']
                if any(kw in name_text for kw in skip_keywords):
                    continue
                
                # 管理番号・状態・価格表示を除外
                invalid_patterns = [
                    r'^[A-Z]{2,3}-\d+',                  # RU-1130, AH-9597など
                    r'^\d+$',                            # 数字のみ
                    r'^特価[\d,]+$',                     # 特価17,800など
                    r'^(新同品|極上品|良品|現状渡し)(\(外観\))?$' # 状態表示
                ]
                
                if any(re.match(pattern, name_text) for pattern in invalid_patterns):
                    continue
                
                # この行の近く（前後）で価格を探す
                price = None
                for offset in range(-2, 8):
                    if idx + offset < 0 or idx + offset >= len(rows):
                        continue
                    
                    nearby_row = rows[idx + offset]
                    nearby_text = nearby_row.get_text()
                    
                    # 価格パターンを探す
                    price_match = re.search(r'¥?\s*(\d{1,3}(?:,\d{3})+)', nearby_text)
                    if price_match:
                        price = price_match.group(1).replace(',', '')
                        break
                
                if not price or len(price) < 3:
                    continue
                
                # 商品名をクリーニング（改行とスペースを整理）
                name = re.sub(r'\s+', ' ', name_text).strip()
                
                # メーカー名の後に空白を追加
                name = re.sub(r'(Nikon|Canon|Sony|Leica|MINOLTA|HASSELBLAD)([A-Z])', r'\1 \2', name)
                
                if len(name) < 3:
                    continue
                
                product_hash = hashlib.md5(f"{name}_{price}".encode()).hexdigest()
                
                if not any(p['hash'] == product_hash for p in products):
                    products.append({
                        'hash': product_hash,
                        'name': name,
                        'price': price
                    })
                    print(f"  商品{len(products)}: {name[:30]}... {price}円")
            
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
    return scrape_oscamera()

def main():
    scrape_oscamera()

if __name__ == "__main__":
    main()