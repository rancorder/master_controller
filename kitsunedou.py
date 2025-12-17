#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kitsunedou.py 修正版
master_controller一元管理対応: DB保存処理削除、標準出力のみ
"""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import re

def scrape_kitsunedou():
    """きつね堂スクレイピング（修正版）"""
    
    print(f"kitsunedou 実行開始: {datetime.now()}")
    
    try:
        BASE_URL = "https://kitsunedou.com/"
        products = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            try:
                print(f"URL処理中...")
                page.goto(BASE_URL, timeout=30000, wait_until="load")
                page.wait_for_timeout(2000)
                
                # 「すべてを表示する」ボタンをクリック
                print("「すべてを表示する」ボタンをクリック...")
                try:
                    # 複数のボタンがある場合、全てクリック
                    show_all_buttons = page.locator("a.button:has-text('すべてを表示')")
                    count = show_all_buttons.count()
                    print(f"  ボタン数: {count}個")
                    
                    for i in range(count):
                        try:
                            show_all_buttons.nth(i).click(timeout=3000)
                            page.wait_for_timeout(1000)
                        except:
                            continue
                    
                    print("  ボタンクリック完了")
                    page.wait_for_timeout(2000)
                    
                except Exception as e:
                    print(f"  ボタンクリック失敗: {e}")
                
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                
                # 商品カードを取得
                items = soup.select(".grid__item")
                print(f"  商品カード数: {len(items)}個")
                
                page_products = 0
                
                for item in items:
                    try:
                        # 商品名取得
                        name_tag = (
                            item.select_one(".card-information__text") or
                            item.select_one("h3") or
                            item.select_one("a[href*='/products/']")
                        )
                        
                        if not name_tag:
                            continue
                        
                        name = name_tag.get_text(strip=True)
                        
                        # 価格取得（正規表現で柔軟に）
                        price_tag = item.select_one(".price-item")
                        
                        if price_tag:
                            price_text = price_tag.get_text(strip=True)
                            # 数字のみ抽出
                            price_match = re.search(r'¥\s*([\d,]+)', price_text)
                            if price_match:
                                price = price_match.group(1).replace(',', '')
                            else:
                                price = '0'
                        else:
                            # 価格タグがない場合は全体から検索
                            item_text = item.get_text()
                            price_match = re.search(r'¥\s*([\d,]+)', item_text)
                            if price_match:
                                price = price_match.group(1).replace(',', '')
                            else:
                                price = '0'
                        
                        # バリデーション
                        if not name or len(name) < 5:
                            continue
                        
                        # 重複チェック用ハッシュ
                        product_hash = hashlib.md5(f"{name}_{price}".encode()).hexdigest()
                        
                        if not any(p['hash'] == product_hash for p in products):
                            products.append({
                                'hash': product_hash,
                                'name': name,
                                'price': price
                            })
                            page_products += 1
                        
                    except Exception as e:
                        continue
                
                print(f"  {page_products}件取得")
                
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
        
        # 商品情報を標準出力（master_controller用）
        if len(products) > 0:
            for product in products:
                print(f"{product['name']} {product['price']}円")
        
        # 結果判定
        if len(products) >= 10:
            print("SUCCESS")
        elif len(products) > 0:
            print("PARTIAL SUCCESS")
        else:
            print("NO DATA")
        
        return len(products)
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 0

def scrape():
    return scrape_kitsunedou()

def main():
    scrape_kitsunedou()

if __name__ == "__main__":
    main()