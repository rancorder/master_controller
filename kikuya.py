#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kikuya_debug.py - 診断用（master_controller経由で実行）
"""

from playwright.sync_api import sync_playwright
import sys
import time

def diagnose():
    """診断実行"""
    
    # stderr に診断開始を出力
    print("=== KIKUYA DIAGNOSTIC START ===", file=sys.stderr, flush=True)
    
    # URL_INDEX（master_controller用）
    print("---URL_INDEX:0---", flush=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        url = "https://kikuya-camera.shop-pro.jp/?mode=srh&cid=&keyword=&sort=n"
        page.goto(url, timeout=60000, wait_until="load")
        page.wait_for_selector(".item_box", timeout=30000)
        
        # 3秒待機
        time.sleep(3)
        
        # 要素取得
        all_items = page.query_selector_all(".item_box:not(.box_last)")
        visible_items = [item for item in all_items if item.is_visible()]
        
        print(f"DIAGNOSTIC: 全要素数={len(all_items)}", file=sys.stderr, flush=True)
        print(f"DIAGNOSTIC: 可視要素数={len(visible_items)}", file=sys.stderr, flush=True)
        
        # 先頭5件の商品名を出力（stderrとstdout両方）
        for i, item in enumerate(visible_items[:5], start=1):
            name_elem = item.query_selector('.item_name a')
            if name_elem:
                name = name_elem.inner_text().strip()
                price_elem = item.query_selector('.item_price')
                price = "0"
                if price_elem:
                    import re
                    price_text = price_elem.inner_text().strip()
                    price_match = re.search(r'([\d,]+)円', price_text)
                    if price_match:
                        price = price_match.group(1).replace(',', '')
                
                # stderr（診断用）
                print(f"DIAGNOSTIC: [{i}位] {name[:50]} - {price}円", file=sys.stderr, flush=True)
                
                # stdout（master_controller用）
                print(f"{name} {price}円", flush=True)
        
        browser.close()
    
    print("=== KIKUYA DIAGNOSTIC END ===", file=sys.stderr, flush=True)

if __name__ == "__main__":
    diagnose()