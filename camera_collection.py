#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re

url = "https://camera-collection.jp/itemlist/"

def main():
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=60000)
            page.wait_for_timeout(3000)

            html = page.content()
            soup = BeautifulSoup(html, "html.parser")

            for item in soup.find_all("div", class_="vk_post"):
                if "type-itemlist" not in item.get("class", []):
                    continue

                title_tag = item.find("h5", class_="vk_post_title")
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)

                details_tag = item.find("p", class_="vk_post_excerpt")
                if not details_tag:
                    continue
                details_text = details_tag.get_text(strip=True)

                # 価格抽出（シンプル版）
                match = re.search(r"(\d{1,3}(?:,\d{3})+)円", details_text)
                if not match:
                    continue
                
                price = match.group(1).replace(',', '')
                
                # タイトルクリーン
                clean_title = title.replace('新着!!', '').replace('値下', '').strip()
                
                print(f"{clean_title} {price}円")

            browser.close()
            
    except Exception as e:
        pass

if __name__ == "__main__":
    main()