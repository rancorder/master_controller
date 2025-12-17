#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
matsuzakaya.py Playwright版
master_controller一元管理対応: DB保存処理削除、標準出力のみ
"""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import re

ROOT_URL = "http://www.matsuzakayacamera.com/"

def scrape_matsuzakaya():
    """松坂屋カメラスクレイピング（Playwright版）"""
    
    print(f"matsuzakaya 実行開始: {datetime.now()}")
    
    products = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            # トップページにアクセス
            print("トップページにアクセス中...")
            page.goto(ROOT_URL, timeout=30000)
            page.wait_for_timeout(2000)
            
            # 新着一覧ボタンをクリック
            print("新着一覧ボタンをクリック...")
            try:
                # 画像のaltで探す
                newitem_button = page.locator("img[alt*='新着']")
                newitem_button.click(timeout=5000)
                page.wait_for_timeout(3000)
                print("ボタンクリック成功")
            except Exception as e:
                print(f"ボタンクリック失敗: {e}")
                browser.close()
                return 0
            
            # ページのHTMLを取得
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            
            # ページネーションリンクを取得
            page_links = soup.select("td.plist a[href]")
            page_urls = [page.url]  # 現在のページ
            
            for link in page_links:
                href = link.get("href")
                if href and "list.php" in href:
                    full_url = f"{ROOT_URL}{href}" if not href.startswith("http") else href
                    if full_url not in page_urls:
                        page_urls.append(full_url)
            
            print(f"ページ数: {len(page_urls)}ページ")
            
            # 各ページを巡回
            for i, url in enumerate(page_urls, 1):
                try:
                    if i > 1:  # 最初のページは既に開いている
                        print(f"  ページ{i}に移動中...")
                        page.goto(url, timeout=30000)
                        page.wait_for_timeout(2000)
                    else:
                        print(f"  ページ{i}処理中...")
                    
                    html = page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    
                    rows = soup.select("tr")
                    print(f"  tr要素数: {len(rows)}個")
                    
                    page_products = 0
                    
                    for row in rows:
                        try:
                            cols = row.find_all("td")
                            
                            if len(cols) >= 5:
                                name_tag = cols[1].find("a")
                                price_tag = cols[3]
                                
                                if name_tag and price_tag:
                                    name = name_tag.get_text(strip=True)
                                    price_text = price_tag.get_text(strip=True)
                                    
                                    if not name or "円" not in price_text:
                                        continue
                                    
                                    name = name.replace("｜", "").strip()
                                    
                                    price_match = re.search(r'[\d,]+', price_text)
                                    if price_match:
                                        price = price_match.group().replace(',', '')
                                    else:
                                        continue
                                    
                                    if len(name) < 3 or len(price) < 3:
                                        continue
                                    
                                    product_hash = hashlib.md5(f"{name}_{price}".encode()).hexdigest()
                                    
                                    if not any(p['hash'] == product_hash for p in products):
                                        products.append({
                                            'hash': product_hash,
                                            'name': name,
                                            'price': price
                                        })
                                        page_products += 1
                        
                        except:
                            continue
                    
                    if page_products > 0:
                        print(f"  ページ{i}: {page_products}件取得")
                    else:
                        print(f"  ページ{i}: 0件")
                
                except Exception as e:
                    print(f"  ページ{i}エラー: {e}")
        
        finally:
            try:
                page.close()
            except:
                pass
            try:
                browser.close()
            except:
                pass
    
    print(f"総取得数: {len(products)}件")
    
    # 商品情報を標準出力
    if len(products) > 0:
        for product in products:
            print(f"{product['name']} {product['price']}円")
    
    if len(products) >= 10:
        print("SUCCESS")
    elif len(products) > 0:
        print("PARTIAL SUCCESS")
    else:
        print("NO DATA")
    
    return len(products)

def scrape():
    return scrape_matsuzakaya()

def main():
    scrape_matsuzakaya()

if __name__ == "__main__":
    main()