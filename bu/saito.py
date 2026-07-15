#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
saito.py - 斉藤カメラスクレイパー
master_controller対応版
"""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re

BASE_URL = "https://www.saito-camera.com/"
START_URL = BASE_URL + "list.php?819168222&mk=&ct=&sh=1&cd=0&pl=&ph=&w=&sr=-12&re=&p=0"

def get_all_pages(start_url):
    page_urls = [start_url]
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(start_url, timeout=60000)
            soup = BeautifulSoup(page.content(), "html.parser")

            pagination = soup.select_one(".plist")
            if pagination:
                for link in pagination.find_all("a", href=True):
                    href = link["href"]
                    if "list.php" in href:
                        full_url = BASE_URL + href
                        if full_url not in page_urls:
                            page_urls.append(full_url)
            browser.close()
    except:
        pass
    return list(dict.fromkeys(page_urls))

def scrape_page(url):
    results = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=60000)
            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, "html.parser")

        for row in soup.select("#listtb tr[onclick]"):
            cols = row.find_all("td")
            if len(cols) >= 5:
                name = cols[2].get_text(strip=True)
                price_text = cols[3].get_text(strip=True)
                condition = cols[4].get_text(strip=True)
                
                if "新品" in condition:
                    continue
                
                # 価格から数字のみ抽出
                price_match = re.search(r'([0-9,]+)', price_text)
                if price_match:
                    price = price_match.group(1).replace(',', '')
                    if name and price:
                        results.append({"name": name, "price": price})
    except:
        pass

    return results

def main():
    all_items = []
    for page_url in get_all_pages(START_URL):
        all_items.extend(scrape_page(page_url))

    for item in all_items:
        print(f"{item['name']} {item['price']}円")

if __name__ == "__main__":
    main()