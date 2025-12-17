#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hatosya.py - はと社スクレイパー
master_controller対応版
"""
import requests
from bs4 import BeautifulSoup
import re
import sys

# コンソールのエンコーディングをUTF-8に設定
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding='utf-8')

# ターゲットURL
url = "https://www.hatosya.com/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
}

def extract_price(price_text):
    price_text = price_text.replace("\\", "").replace(",", "").replace("円", "").strip()
    price_match = re.search(r"\d+", price_text)
    return int(price_match.group()) if price_match else None

def scrape_data():
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.encoding = "shift_jis"
        soup = BeautifulSoup(response.text, "html.parser")

        # "新着情報" から "このページのTOPへ" までの範囲を取得
        start_marker = soup.find("img", {"alt": "新着情報"})
        end_marker = soup.find("a", string="▲このページのＴＯＰへ")

        if not start_marker or not end_marker:
            return []

        table = start_marker.find_next("table")
        if not table:
            return []

        items = []
        for row in table.find_all("tr")[1:]:  # ヘッダー行をスキップ
            cols = row.find_all("td")
            if len(cols) < 5:
                continue

            name = cols[1].get_text(strip=True)
            price = extract_price(cols[2].get_text(strip=True))

            if name and price:
                items.append({
                    "商品名": name,
                    "価格": price
                })

        return items
    except:
        return []

def main():
    items = scrape_data()
    
    for item in items:
        print(f"{item['商品名']} {item['価格']}円")

if __name__ == "__main__":
    main()