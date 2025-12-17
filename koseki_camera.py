#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
koseki_camera.py - コセキカメラスクレイパー
master_controller対応版
"""
import requests
from bs4 import BeautifulSoup
import re
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding='utf-8')

URL = "https://www.koseki-camera.jp/"

def fetch_products():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        response = requests.get(URL, headers=headers, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        product_cells = soup.select("td.lims")
        
        for cell in product_cells:
            try:
                # 商品名
                name_link = cell.select_one("tr.woong td a")
                if not name_link:
                    continue
                name = name_link.text.strip()
                
                # 価格
                price_elements = cell.select("tr.woong td")
                if len(price_elements) < 2:
                    continue
                
                price_text = price_elements[1].get_text(strip=True)
                
                # 価格から数字のみ抽出
                price_match = re.search(r'([0-9,]+)', price_text)
                if not price_match:
                    continue
                
                price = price_match.group(1).replace(',', '')
                
                if name and price:
                    print(f"{name} {price}円")
                    
            except:
                continue

    except:
        pass

def main():
    fetch_products()

if __name__ == "__main__":
    main()