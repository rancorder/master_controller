#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tanaridocamera.py - 多成堂スクレイパー（URL index対応）
"""
import requests
from bs4 import BeautifulSoup
import re

URLS = [
    "https://www.tanaridocamera.shop/shopbrand/sample3/",  # url_index: 0
    "https://www.tanaridocamera.shop/shopbrand/ct5/",  # url_index: 1
    "https://www.tanaridocamera.shop/shopbrand/ct32/",  # url_index: 2
    "https://www.tanaridocamera.shop/shopbrand/ct34/",  # url_index: 3
    "https://www.tanaridocamera.shop/shopbrand/ct7/",  # url_index: 4
    "https://www.tanaridocamera.shop/shopbrand/ct8/",  # url_index: 5
    "https://www.tanaridocamera.shop/shopbrand/ct9/",  # url_index: 6
    "https://www.tanaridocamera.shop/shopbrand/ct10/",  # url_index: 7
    "https://www.tanaridocamera.shop/shopbrand/ct31/",  # url_index: 8
    "https://www.tanaridocamera.shop/shopbrand/ct29/",  # url_index: 9
    "https://www.tanaridocamera.shop/shopbrand/ct12/",  # url_index: 10
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}

def scrape_once():
    for url_index, url in enumerate(URLS):
        # URL切り替えを明示
        print(f"---URL_INDEX:{url_index}---")
        
        try:
            res = requests.get(url, headers=HEADERS, timeout=20)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")

            for box in soup.find_all("div", class_="innerBox"):
                name_tag = box.find("p", class_="name")
                price_tag = box.find("p", class_="price")
                if not name_tag or not price_tag:
                    continue

                # 余分な空白や改行を正規化
                name = re.sub(r"\s+", " ", name_tag.get_text(strip=True)).strip()
                price_text = price_tag.get_text(strip=True)
                
                # 価格から数字のみ抽出
                price_match = re.search(r'([0-9,]+)', price_text)
                if price_match:
                    price = price_match.group(1).replace(',', '')
                    if name and price:
                        print(f"{name} {price}円")
        except Exception:
            pass

if __name__ == "__main__":
    scrape_once()