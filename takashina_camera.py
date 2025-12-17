# -*- coding: utf-8 -*-
import sys
import os
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

BASE_URL = "https://shop.cam-all.com"
START_URL = "https://shop.cam-all.com/shopbrand/all_items/"

def scrape_takashina():
    """スクレイピング"""
    results = []
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            page = browser.new_page()
            page.set_default_timeout(15000)
            
            page.goto(START_URL, wait_until="domcontentloaded")
            page.wait_for_timeout(2000)
            
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            
            # 商品抽出
            items = soup.select('.category-list-inner')
            if not items:
                items = soup.select('.product-item')
            if not items:
                items = soup.select('.item')
            
            for item in items[:30]:
                try:
                    # 商品名
                    name_elem = (
                        item.select_one('.category-list-detail .name a') or
                        item.select_one('.name a') or
                        item.select_one('.product-title') or
                        item.select_one('h3 a')
                    )
                    if not name_elem:
                        continue
                    
                    name = name_elem.get_text(strip=True)
                    if not name or len(name) < 3:
                        continue
                    
                    # 価格
                    price_elem = (
                        item.select_one('.category-list-detail .price .price') or
                        item.select_one('.price')
                    )
                    if not price_elem:
                        continue
                    
                    price_text = price_elem.get_text(strip=True)
                    if not price_text:
                        continue
                    
                    # 在庫状況チェック
                    sold_out = item.select_one('.soldout')
                    if sold_out:
                        continue  # SOLD OUTは除外
                    
                    results.append({
                        "name": name,
                        "price": price_text
                    })
                    
                except Exception:
                    continue
            
            browser.close()
            
    except Exception as e:
        print(f"[ERROR] スクレイピングエラー: {e}")
    
    return results

def main():
    """メイン処理"""
    try:
        items = scrape_takashina()
        
        if items:
            for item in items:
                print(f"{item['name']} {item['price']}円")
        else:
            print("[WARNING] データ取得0件")
        
    except Exception as e:
        print(f"[ERROR] メイン処理エラー: {e}")

if __name__ == "__main__":
    main()