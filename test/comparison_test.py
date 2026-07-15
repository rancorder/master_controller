#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
comparison_test.py - v2.0 vs v3.1 比較テスト
実際にどちらの方法が正確か検証する
"""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import time
import re

BASE_URL = "https://ec.treasure-f.com/search?category=1029&category2=1031&size=grid&order=newarrival&number=30&step=1"

def extract_product_info(element, method="playwright"):
    """商品情報を抽出"""
    if method == "playwright":
        # Playwright方式
        img = element.query_selector("img")
        name = img.get_attribute('alt') if img else ""
        
        price_container = element.query_selector(".cm-itemlist_price")
        price = "0"
        if price_container:
            price_text = price_container.inner_text().strip()
            price_match = re.search(r'[\d,]+', price_text)
            if price_match:
                price = re.sub(r'[^\d]', '', price_match.group())
        
        return {"name": name, "price": price, "method": "Playwright"}
    else:
        # BeautifulSoup方式
        img = element.select_one("img")
        name = img.get('alt', '') if img else ""
        
        price_container = element.select_one(".cm-itemlist_price")
        price = "0"
        if price_container:
            price_text = price_container.get_text(strip=True)
            price_match = re.search(r'[\d,]+', price_text)
            if price_match:
                price = re.sub(r'[^\d]', '', price_match.group())
        
        return {"name": name, "price": price, "method": "BeautifulSoup"}

def test_v2_approach():
    """v2.0のアプローチ（BeautifulSoup + time.sleep(5)）"""
    print("\n" + "=" * 80)
    print("🔵 v2.0アプローチテスト（BeautifulSoup + time.sleep(5)）")
    print("=" * 80)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        print("⏳ ページ読み込み中...")
        page.goto(BASE_URL, timeout=90000, wait_until="load")
        
        print("⏳ 商品リスト表示待機中...")
        page.wait_for_selector("li.pj-search_item", timeout=30000)
        
        print("⏳ 5秒待機（v2.0方式）...")
        time.sleep(5)
        
        print("📄 HTMLをBeautifulSoupで解析...")
        html = page.content()
        browser.close()
        
        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("li.pj-search_item")
        
        print(f"✅ 取得商品数: {len(items)}件")
        
        if items:
            # 上位5件を取得
            top_5 = []
            for i, item in enumerate(items[:5]):
                product = extract_product_info(item, method="beautifulsoup")
                product['rank'] = i + 1
                top_5.append(product)
                print(f"   {i+1}位: {product['name'][:60]}... (¥{product['price']})")
            
            return top_5
        else:
            print("❌ 商品が見つかりません")
            return []

def test_v3_approach():
    """v3.1のアプローチ（Playwright直接 + DOM安定化確認）"""
    print("\n" + "=" * 80)
    print("🟢 v3.1アプローチテスト（Playwright直接 + DOM安定化）")
    print("=" * 80)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        print("⏳ ページ読み込み中...")
        page.goto(BASE_URL, timeout=90000, wait_until="load")
        
        print("⏳ 商品リスト表示待機中...")
        page.wait_for_selector("li.pj-search_item", timeout=30000)
        
        print("⏳ 初期待機（3秒）...")
        time.sleep(3)
        
        print("⏳ DOM安定化確認中...")
        stable_count = 0
        last_count = 0
        last_first_name = ""
        
        for check_num in range(15):
            items = page.query_selector_all("li.pj-search_item")
            current_count = len(items)
            
            current_first_name = ""
            if items:
                first_img = items[0].query_selector("img")
                if first_img:
                    current_first_name = first_img.get_attribute('alt') or ""
            
            if (current_count == last_count and 
                current_count > 0 and
                current_first_name == last_first_name and
                current_first_name != ""):
                stable_count += 1
                print(f"   ✓ 安定: {stable_count}/3回")
                
                if stable_count >= 3:
                    print(f"✅ DOM安定化確認完了（チェック{check_num+1}回）")
                    break
            else:
                if stable_count > 0:
                    print(f"   ⚠ 変動検知: リセット")
                stable_count = 0
            
            last_count = current_count
            last_first_name = current_first_name
            time.sleep(0.5)
        
        # Playwrightで直接取得
        items = page.query_selector_all("li.pj-search_item")
        print(f"✅ 取得商品数: {len(items)}件")
        
        top_5 = []
        if items:
            for i, item in enumerate(items[:5]):
                product = extract_product_info(item, method="playwright")
                product['rank'] = i + 1
                top_5.append(product)
                print(f"   {i+1}位: {product['name'][:60]}... (¥{product['price']})")
        
        browser.close()
        return top_5

def test_manual_verification():
    """手動検証用：ブラウザを表示して確認"""
    print("\n" + "=" * 80)
    print("👁️  手動検証モード（ブラウザ表示）")
    print("=" * 80)
    print("実際のブラウザで表示される順序を確認します...")
    print("10秒間ブラウザを表示します。実際の1位を確認してください。")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        page.goto(BASE_URL, timeout=90000, wait_until="load")
        page.wait_for_selector("li.pj-search_item", timeout=30000)
        
        print("\n⏰ 10秒待機中...")
        time.sleep(10)
        
        items = page.query_selector_all("li.pj-search_item")
        
        print("\n📸 現在表示されている上位5件:")
        manual_top_5 = []
        if items:
            for i, item in enumerate(items[:5]):
                product = extract_product_info(item, method="playwright")
                product['rank'] = i + 1
                manual_top_5.append(product)
                print(f"   {i+1}位: {product['name'][:60]}... (¥{product['price']})")
        
        browser.close()
        return manual_top_5

def compare_results(v2_results, v3_results, manual_results):
    """結果を比較"""
    print("\n" + "=" * 80)
    print("📊 比較結果")
    print("=" * 80)
    
    # 1位の比較
    print("\n【1位の比較】")
    print(f"v2.0 (BS):  {v2_results[0]['name'][:60] if v2_results else 'N/A'}")
    print(f"v3.1 (PW):  {v3_results[0]['name'][:60] if v3_results else 'N/A'}")
    print(f"手動確認:   {manual_results[0]['name'][:60] if manual_results else 'N/A'}")
    
    # 一致判定
    v2_match = v2_results[0]['name'] == manual_results[0]['name'] if v2_results and manual_results else False
    v3_match = v3_results[0]['name'] == manual_results[0]['name'] if v3_results and manual_results else False
    
    print("\n【正確性判定】")
    if v2_match:
        print("✅ v2.0: 手動確認と一致")
    else:
        print("❌ v2.0: 手動確認と不一致")
    
    if v3_match:
        print("✅ v3.1: 手動確認と一致")
    else:
        print("❌ v3.1: 手動確認と不一致")
    
    # 上位5件の一致率
    print("\n【上位5件の一致率】")
    v2_matches = sum(1 for i in range(min(5, len(v2_results), len(manual_results))) 
                     if v2_results[i]['name'] == manual_results[i]['name'])
    v3_matches = sum(1 for i in range(min(5, len(v3_results), len(manual_results))) 
                     if v3_results[i]['name'] == manual_results[i]['name'])
    
    print(f"v2.0: {v2_matches}/5件一致 ({v2_matches*20}%)")
    print(f"v3.1: {v3_matches}/5件一致 ({v3_matches*20}%)")
    
    # 結論
    print("\n" + "=" * 80)
    print("🎯 結論")
    print("=" * 80)
    
    if v3_match and not v2_match:
        print("✅ v3.1の方が正確です")
    elif v2_match and not v3_match:
        print("⚠️  v2.0の方が正確です（要調査）")
    elif v2_match and v3_match:
        print("✅ 両方とも正確です")
    else:
        print("❌ 両方とも不正確です（要調査）")

def main():
    """メインテスト実行"""
    print("━" * 80)
    print("🧪 v2.0 vs v3.1 比較テスト開始")
    print("━" * 80)
    
    # 3つのテストを実行
    v2_results = test_v2_approach()
    v3_results = test_v3_approach()
    manual_results = test_manual_verification()
    
    # 結果比較
    if v2_results and v3_results and manual_results:
        compare_results(v2_results, v3_results, manual_results)
    else:
        print("\n❌ テスト失敗: 一部の結果が取得できませんでした")
    
    print("\n━" * 80)
    print("🏁 テスト完了")
    print("━" * 80)

if __name__ == "__main__":
    main()