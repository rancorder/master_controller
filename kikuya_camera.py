#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kikuya_camera_debug.py - デバッグ版

実行してHTML構造を確認
"""

import requests
from bs4 import BeautifulSoup

def debug_scrape():
    """HTML構造を詳細出力"""
    
    url = "https://cameranokikuya.shop-pro.jp/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    response = requests.get(url, headers=headers, timeout=30)
    
    print(f"ステータスコード: {response.status_code}")
    print(f"Content-Type: {response.headers.get('Content-Type')}")
    print(f"HTML長: {len(response.text)}文字")
    print("=" * 60)
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # パターン1: div.itemarea
    items1 = soup.select("div.itemarea")
    print(f"✅ div.itemarea: {len(items1)}個")
    
    # パターン2: div.itembox
    items2 = soup.select("div.itembox")
    print(f"✅ div.itembox: {len(items2)}個")
    
    # パターン3: li.item
    items3 = soup.select("li.item")
    print(f"✅ li.item: {len(items3)}個")
    
    # パターン4: 汎用的な商品コンテナ
    items4 = soup.select("[class*='item']")
    print(f"✅ class*='item': {len(items4)}個")
    
    print("=" * 60)
    
    # 最初の商品要素を詳細表示
    if items1:
        print("📦 最初の商品 (div.itemarea):")
        print(items1[0].prettify()[:500])
    elif items2:
        print("📦 最初の商品 (div.itembox):")
        print(items2[0].prettify()[:500])
    elif items4:
        print("📦 最初の商品 (class*='item'):")
        print(items4[0].prettify()[:500])
    else:
        print("❌ 商品要素が見つかりません")
        print("\n🔍 HTML全体の最初の1000文字:")
        print(response.text[:1000])

if __name__ == "__main__":
    debug_scrape()