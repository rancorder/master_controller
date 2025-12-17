#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tokiwa-camera.py - トキワカメラスクレイパー
master_controller対応版
"""
import requests
from bs4 import BeautifulSoup
import re

BASE_URL = "https://tokiwa-camera.co.jp"
START_URL = "https://tokiwa-camera.co.jp/collections/%E4%B8%AD%E5%8F%A4-%E6%96%B0%E7%9D%80%E5%95%86%E5%93%81"

def scrape_tokiwa():
    """tokiwa-camera スクレイピング"""
    results = []
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(START_URL, headers=headers, timeout=10)
        if response.status_code != 200:
            return []
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 複数のセレクタパターンを試行
        products = soup.select(".product-item")
        if not products:
            products = soup.select(".grid__item")
        if not products:
            products = soup.select("[data-product-id]")
        if not products:
            products = soup.select(".card")
        
        for product in products:  # 件数制限なし
            try:
                # 商品名の取得
                name_elem = (
                    product.select_one(".product-item__title") or
                    product.select_one(".card__heading") or
                    product.select_one(".product-title") or
                    product.select_one("h3") or
                    product.select_one("h2") or
                    product.select_one(".title")
                )
                if not name_elem:
                    continue
                
                name = name_elem.get_text(strip=True)
                if not name:
                    continue
                
                # 価格の取得
                price_elem = (
                    product.select_one(".product-item__price") or
                    product.select_one(".price") or
                    product.select_one(".money") or
                    product.select_one(".price__regular") or
                    product.select_one("[data-price]")
                )
                if not price_elem:
                    continue
                
                price_text = price_elem.get_text(strip=True)
                
                # 価格から数字のみ抽出
                price_match = re.search(r'([0-9,]+)', price_text)
                if not price_match:
                    continue
                
                price = price_match.group(1).replace(',', '')
                
                if name and price:
                    results.append({"name": name, "price": price})
                
            except:
                continue
        
    except:
        pass
    
    return results

def main():
    items = scrape_tokiwa()
    
    for item in items:
        print(f"{item['name']} {item['price']}円")

if __name__ == "__main__":
    main()