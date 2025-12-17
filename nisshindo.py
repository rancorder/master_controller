#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nisshindo.py - 日進堂カメラ Playwright版
動的サイト対応：JavaScriptレンダリング後にスクレイピング
【v2.0】URL_INDEX出力対応（master_controller_v29完全準拠）
"""

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re
from datetime import datetime
import time

def scrape_nisshindo():
    """日進堂カメラ スクレイピング（Playwright版・4URL対応）"""
    
    print(f"日進堂カメラ 実行開始: {datetime.now()}")
    
    # 4つのURL（shop_config.jsonと完全対応）
    URLS = [
        "https://www.nisshindo.com/?mode=cate&cbid=2449441&csid=0&sort=n",  # url_index: 0 フィルムカメラ
        "https://www.nisshindo.com/?mode=cate&cbid=2449732&csid=0&sort=n",  # url_index: 1 交換レンズ
        "https://www.nisshindo.com/?mode=cate&cbid=2449733&csid=0&sort=n",  # url_index: 2 デジタルカメラ
        "https://www.nisshindo.com/?mode=cate&cbid=2449735&csid=0&sort=n",  # url_index: 3 その他カメラ用品
    ]
    
    all_products = []
    
    try:
        with sync_playwright() as p:
            # ブラウザ起動（headlessモード）
            browser = p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox']
            )
            
            page = browser.new_page()
            
            # User-Agent設定
            page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })
            
            for url_index, url in enumerate(URLS):
                # ===== 【重要】URL切り替えを明示（hardoff.py方式） =====
                print(f"---URL_INDEX:{url_index}---")
                print(f"カテゴリ {url_index + 1}/{len(URLS)} 処理中...")
                
                try:
                    # ページ読み込み
                    page.goto(url, timeout=30000, wait_until='networkidle')
                    
                    # JavaScript実行完了を待つ
                    time.sleep(2)
                    
                    # 商品リストが表示されるまで待機
                    try:
                        page.wait_for_selector('li.prd-lst-unit', timeout=10000)
                    except:
                        print(f"  商品リスト要素が見つかりません")
                        continue
                    
                    # HTML取得
                    html = page.content()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # 商品リスト取得
                    product_items = soup.find_all('li', class_='prd-lst-unit')
                    
                    page_count = 0
                    
                    for item in product_items:
                        try:
                            # 商品名取得
                            name_elem = item.find('span', class_='prd-lst-name')
                            if not name_elem:
                                continue
                            
                            name_link = name_elem.find('a')
                            if not name_link:
                                continue
                            
                            product_name = name_link.get_text(strip=True)
                            
                            # 価格取得（SOLD OUTは除外）
                            price_elem = item.find('span', class_='prd-lst-price')
                            
                            if not price_elem:
                                # SOLD OUT商品はスキップ
                                soldout = item.find('span', class_='prd-lst-soldout')
                                if soldout:
                                    continue
                                continue
                            
                            price_text = price_elem.get_text(strip=True)
                            
                            # 価格抽出："10,000円(税込)" → "10000"
                            price_match = re.search(r'([\d,]+)円', price_text)
                            if not price_match:
                                continue
                            
                            price = price_match.group(1).replace(',', '')
                            
                            # 出力（master_controller_v29が解析）
                            print(f"{product_name} {price}円")
                            
                            all_products.append({
                                'name': product_name,
                                'price': price,
                                'url_index': url_index
                            })
                            
                            page_count += 1
                            
                        except Exception as e:
                            continue
                    
                    print(f"  {page_count}件取得")
                    
                except Exception as e:
                    print(f"  エラー: {e}")
                    continue
            
            browser.close()
    
    except Exception as e:
        print(f"Playwright初期化エラー: {e}")
        return 0
    
    print(f"総取得数: {len(all_products)}件")
    
    if len(all_products) > 0:
        print("SUCCESS")
    else:
        print("WARNING: 0件")
    
    return len(all_products)

def scrape():
    """既存システムとの互換性維持"""
    return scrape_nisshindo()

def main():
    """メイン処理"""
    scrape_nisshindo()

if __name__ == "__main__":
    main()