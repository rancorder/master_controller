#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
akasaka_camera.py - 赤坂カメラスクレイパー
master_controller対応版
"""
import requests
from bs4 import BeautifulSoup
import re

def scrape_akasaka():
    url = "https://www.akasaka-camera.com/product/index/page:1"
    
    try:
        response = requests.get(url, timeout=15)
        soup = BeautifulSoup(response.text, "html.parser")
        
        items = soup.select('.col-xs-12 .product_figure')
        
        for item in items:
            try:
                # 品名
                name_tag = item.select_one('table.table_columns tr:nth-child(2) td:nth-child(2)')
                if not name_tag:
                    continue
                name = name_tag.get_text(strip=True)
                
                # 程度
                condition_tag = item.select_one('table.table_columns tr:nth-child(3) td:nth-child(2)')
                if condition_tag:
                    condition = condition_tag.get_text(strip=True)
                    name = f"{name} {condition}"
                
                # 価格
                price_tag = item.select_one('.price')
                if not price_tag:
                    continue
                
                price_text = price_tag.get_text(strip=True)
                
                # SOLD OUT除外
                if "SOLD OUT" in price_text or "売り切れ" in price_text:
                    continue
                
                # 価格から数字のみ抽出
                price_match = re.search(r'([0-9,]+)', price_text)
                if not price_match:
                    continue
                
                price = price_match.group(1).replace(',', '')
                
                # master_controller用出力
                print(f"{name} {price}円")
                
            except:
                continue
                
    except Exception as e:
        pass

def main():
    scrape_akasaka()

if __name__ == "__main__":
    main()