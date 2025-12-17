#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
suzuki_camera.py - 鈴木カメラスクレイパー
table構造解析版
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re

def scrape():
    """鈴木カメラの商品をスクレイピング"""
    
    print(f"suzuki_camera 実行開始: {datetime.now()}")
    
    try:
        url = "http://www.suzuki-camera.com/shop_01.html"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        # 文字コード自動判定
        if response.encoding.lower() not in ['utf-8', 'utf8']:
            response.encoding = response.apparent_encoding
        
        if response.status_code != 200:
            print(f"ERROR: HTTP {response.status_code}")
            return
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        product_count = 0
        
        # パターン1: table構造
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            
            for row in rows:
                try:
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) < 2:
                        continue
                    
                    # セル内容から商品名と価格を探す
                    text_content = []
                    for cell in cells:
                        text = cell.get_text(strip=True)
                        if text:
                            text_content.append(text)
                    
                    # 価格を含む行を探す
                    price_found = None
                    name_parts = []
                    
                    for text in text_content:
                        price_match = re.search(r'[¥￥]?\s*([\d,]+)\s*円?', text)
                        if price_match:
                            price_found = price_match.group(1).replace(',', '')
                        else:
                            # 価格以外は商品名として扱う
                            if len(text) > 3 and not re.match(r'^[\d,]+$', text):
                                name_parts.append(text)
                    
                    if price_found and name_parts:
                        name = ' '.join(name_parts)
                        
                        # 明らかにヘッダーではない
                        if '商品名' not in name and '価格' not in name:
                            print(f"{name} {price_found}円")
                            product_count += 1
                    
                except Exception as e:
                    continue
        
        # パターン2: div/list構造（table以外）
        if product_count == 0:
            items = soup.select('.item, .product, .goods')
            
            for item in items[:30]:
                try:
                    text = item.get_text(strip=True)
                    
                    # 価格抽出
                    price_match = re.search(r'[¥￥]?\s*([\d,]+)\s*円', text)
                    if not price_match:
                        continue
                    
                    price = price_match.group(1).replace(',', '')
                    
                    # 商品名抽出（価格を除いた部分）
                    name = re.sub(r'[¥￥]?\s*[\d,]+\s*円', '', text).strip()
                    
                    if name and len(name) > 3:
                        print(f"{name} {price}円")
                        product_count += 1
                        
                except Exception as e:
                    continue
        
        print(f"取得数: {product_count}件")
        
        if product_count > 0:
            print("SUCCESS")
        else:
            print("PARTIAL SUCCESS")
            
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    scrape()
