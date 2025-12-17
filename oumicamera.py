#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
oumicamera.py - 近江カメラスクレイパー
master_controller対応版
"""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re

START_URL = "https://oumicamera.base.shop/categories/4685722"

def scrape_oumicamera():
    items = []
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(START_URL, timeout=60000)
            page.wait_for_load_state("networkidle")

            # 「もっと見る」を全て展開
            for _ in range(10):  # 最大10回
                try:
                    btn = page.locator("#paginatorButton")
                    if btn.count() > 0 and btn.is_visible():
                        btn.click()
                        page.wait_for_load_state("networkidle")
                        page.wait_for_timeout(1000)
                    else:
                        break
                except:
                    break

            page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            page.wait_for_timeout(1000)

            soup = BeautifulSoup(page.content(), "html.parser")
            browser.close()

        titles = soup.select(".items-grid_itemTitleText_5c97110f")
        prices = soup.select(".items-grid_price_5c97110f")

        for t, pz in zip(titles, prices):
            name = t.get_text(strip=True)
            price_text = pz.get_text(strip=True)
            
            # 価格から数字のみ抽出
            price_match = re.search(r'([0-9,]+)', price_text)
            if price_match:
                price = price_match.group(1).replace(',', '')
                if name and price:
                    items.append({"name": name, "price": price})
    except:
        pass

    return items

def main():
    items = scrape_oumicamera()
    
    for item in items:
        print(f"{item['name']} {item['price']}円")

if __name__ == "__main__":
    main()