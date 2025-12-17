# -*- coding: utf-8 -*-
import sys
import os

if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import time
import re

BASE_URL = "https://www.first-shokai.com/showroom/"

def get_details_from_page(page, url):
    """詳細ページから価格と商品名を取得"""
    try:
        page.goto(url, timeout=30000)
        page.wait_for_load_state('networkidle', timeout=10000)
        
        content = page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        # 商品名取得（より具体的に）
        name = None
        
        # パターン1: h1タグ
        h1 = soup.find('h1', class_='title')
        if not h1:
            h1 = soup.find('h1')
        if h1:
            name = h1.get_text(strip=True)
        
        # パターン2: タイトルタグ
        if not name:
            title = soup.find('title')
            if title:
                name = title.get_text(strip=True).split('|')[0].strip()
        
        if not name:
            name = "商品名不明"
        
        # 価格取得（より広範囲に検索）
        price = None
        
        # パターン1: 価格用のクラスやID
        price_selectors = [
            '.price', '.product-price', '#price',
            '[class*="price"]', '[id*="price"]'
        ]
        
        for selector in price_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                # 数字を含むかチェック
                numbers = re.findall(r'[0-9,]+', text)
                if numbers:
                    price = numbers[0].replace(',', '')
                    break
        
        # パターン2: ページ全体から価格パターンを検索
        if not price:
            text_content = soup.get_text()
            price_patterns = [
                r'¥\s*([0-9,]+)',
                r'([0-9,]+)\s*円',
                r'価格[:\s]*¥?\s*([0-9,]+)',
                r'販売価格[:\s]*¥?\s*([0-9,]+)'
            ]
            
            for pattern in price_patterns:
                match = re.search(pattern, text_content)
                if match:
                    price = match.group(1).replace(',', '')
                    break
        
        if price and price.isdigit() and int(price) >= 100:
            return {'name': name[:100], 'price': f"{price}円"}
        else:
            return {'name': name[:100], 'price': "お問い合わせ"}
        
    except Exception as e:
        print(f"[ERROR] 詳細取得エラー ({url}): {e}")
        return None

def scrape_latest_page():
    """最新ページのスクレイピング"""
    items = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        
        page = context.new_page()
        
        try:
            page.goto(BASE_URL, timeout=30000)
            page.wait_for_load_state('networkidle', timeout=15000)
            
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # 商品リンク抽出: article.item_animate内のaタグ
            product_links = []
            articles = soup.find_all('article', class_='item_animate')
            
            for article in articles:
                link = article.find('a', class_='link')
                if link and link.get('href'):
                    href = link.get('href')
                    if href not in product_links:
                        product_links.append(href)
            
            # もし見つからなければフォールバック
            if not product_links:
                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    href = link.get('href')
                    if '/showroom/' in href and '/showroom_category/' not in href:
                        if href not in product_links and href != BASE_URL:
                            product_links.append(href)
            
            print(f"[INFO] {len(product_links)}個の商品リンクを発見")
            
            # 各商品ページから情報取得
            for i, link in enumerate(product_links[:20], 1):
                print(f"[INFO] 商品 {i}/{min(20, len(product_links))} 処理中...")
                item = get_details_from_page(page, link)
                if item:
                    items.append(item)
                    print(f"{item['name']} {item['price']}")
                time.sleep(1)
            
        except Exception as e:
            print(f"[ERROR] スクレイピングエラー: {e}")
        
        finally:
            browser.close()
    
    return items

if __name__ == "__main__":
    try:
        items = scrape_latest_page()
        
        if not items:
            print("[WARNING] データ取得0件")
        
    except Exception as e:
        print(f"[ERROR] メイン処理エラー: {e}")