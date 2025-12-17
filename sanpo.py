#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sanpo.py - 三宝カメラスクレイパー
"""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re

START_URL = "https://www.sanpou.ne.jp/"

def scrape_page(url):
    items = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=60000)
            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, "html.parser")

        for cell in soup.select(".item-list td[valign='top']"):
            name_tag = cell.select_one("tr.woong a")
            price_tds = cell.select("tr.woong td")

            if not name_tag or not price_tds:
                continue

            name = name_tag.get_text(strip=True)

            price = ""
            for td in price_tds:
                txt = td.get_text()
                if "円" in txt:
                    price = td.get_text(strip=True)
                    break

            if name and price:
                items.append({"name": name, "price": price})

    except:
        pass

    return items

def main():
    items = scrape_page(START_URL)
    
    for it in items:
        # 価格から数字のみ抽出
        price_match = re.search(r'([0-9,]+)', it['price'])
        if price_match:
            price = price_match.group(1).replace(',', '')
            print(f"{it['name']} {price}円")

if __name__ == "__main__":
    main()