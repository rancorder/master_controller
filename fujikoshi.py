#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fujikoshi.py - 富士越カメラスクレイパー（URL index対応）
"""
import requests
from bs4 import BeautifulSoup
import time
import re

# URL順序固定（url_index対応）
URLS = [
    "https://www.fujikoshi-camera.com/#%E4%B8%AD%E5%8F%A4%E6%96%B0%E7%9D%80",  # url_index: 0
]

def clean_price(price_text):
    """価格テキストから数値部分を抽出"""
    if not price_text:
        return "N/A"
    price_match = re.search(r'[\d,]+円', price_text)
    if price_match:
        return price_match.group()
    return price_text.strip()

def clean_product_name(name_text):
    """商品名をクリーンアップ"""
    if not name_text:
        return "N/A"
    cleaned = re.sub(r'\s+', ' ', name_text.strip())
    return cleaned

def fetch_and_parse(url, retries=3, timeout=20):
    """指定URLの商品情報を取得"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "ja,en-US;q=0.5"
    }

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            response.encoding = 'euc-jp'
            soup = BeautifulSoup(response.text, "html.parser")

            products = []
            product_links = soup.find_all('a', href=lambda href: href and '/shopdetail/' in href)
            
            for link in product_links:
                product_name = clean_product_name(link.get_text())
                
                if product_name and product_name != "N/A" and len(product_name) > 1:
                    price = "価格情報なし"
                    parent = link.parent
                    
                    while parent and parent.name != 'body':
                        price_text = parent.get_text()
                        if '円' in price_text:
                            price_match = re.search(r'[\d,]+円[（\(][^）\)]*[）\)]', price_text)
                            if price_match:
                                price = clean_price(price_match.group())
                                break
                            price_match = re.search(r'[\d,]+円', price_text)
                            if price_match:
                                price = clean_price(price_match.group())
                                break
                        parent = parent.parent
                    
                    product_info = {
                        "name": product_name,
                        "price": price
                    }
                    products.append(product_info)
            
            # 重複除去
            seen_names = set()
            unique_products = []
            for product in products:
                if product["name"] not in seen_names:
                    seen_names.add(product["name"])
                    unique_products.append(product)
            
            return unique_products

        except Exception:
            time.sleep(5)

    return []

def main():
    """メイン関数（url_index対応版）"""
    
    for url_index, url in enumerate(URLS):
        # URL切り替えを明示
        print(f"---URL_INDEX:{url_index}---")
        
        products = fetch_and_parse(url)
        
        for product in products:
            price_only = product['price'].replace('円', '').replace(',', '')
            print(f"{product['name']} {price_only}円", flush=True)

if __name__ == "__main__":
    main()