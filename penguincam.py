#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
penguincam.py - ペンギンカメラスクレイパー
"""

import requests
from bs4 import BeautifulSoup
import re

BASE_URL = "http://penguincam.shop26.makeshop.jp/shopbrand/025/P/"

def scrape_penguincam():
    results = []
    seen = set()
    
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        response = requests.get(BASE_URL, headers=headers, timeout=15)
        response.encoding = response.apparent_encoding
        
        if response.status_code != 200:
            print(f"エラー: ステータスコード {response.status_code}")
            return []
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # パターン1: tbody内のtr要素を探す（グリッド表示の商品）
        all_trs = soup.find_all("tr")
        
        for tr in all_trs:
            try:
                # class="woong"のtr要素を探す（商品名の行）
                if tr.get("class") and "woong" in tr.get("class"):
                    # 商品名を取得
                    name_td = tr.find("td", valign="top", align="center")
                    if name_td:
                        name_link = name_td.find("a")
                        if name_link:
                            name = name_link.get_text(strip=True)
                            
                            # 次のtr要素で価格を探す
                            next_tr = tr.find_next_sibling("tr")
                            if next_tr and next_tr.get("class") and "woong" in next_tr.get("class"):
                                price_td = next_tr.find("td", valign="top", align="center")
                                if price_td:
                                    price_text = price_td.get_text(strip=True)
                                    # "11,000円 (税込)" のようなテキストから数字を抽出
                                    price_match = re.search(r'([0-9,]+)円', price_text)
                                    if price_match:
                                        price = price_match.group(1).replace(",", "")
                                        
                                        # 重複チェック
                                        key = f"{name} {price}"
                                        if key not in seen and name and price:
                                            seen.add(key)
                                            results.append({"name": name, "price": price})
                
                # パターン2: height="20"のtr要素（リスト表示の商品）
                if tr.get("height") == "20" and tr.get("class") and "woong" in tr.get("class"):
                    # 商品名を探す
                    name_td = tr.find("td", valign="top", align="center")
                    if name_td:
                        name_link = name_td.find("a")
                        if name_link and "/shopbrand/" in name_link.get("href", ""):
                            name = name_link.get_text(strip=True)
                            
                            # 価格を探す（次のtr）
                            next_tr = tr.find_next_sibling("tr")
                            if next_tr:
                                price_td = next_tr.find("td", valign="top", align="center")
                                if price_td:
                                    price_text = price_td.get_text(strip=True)
                                    price_match = re.search(r'([0-9,]+)円', price_text)
                                    if price_match:
                                        price = price_match.group(1).replace(",", "")
                                        
                                        key = f"{name}||{price}"
                                        if key not in seen and name and price:
                                            seen.add(key)
                                            results.append({"name": name, "price": price})
                
            except Exception as e:
                continue
        
        # パターン3: すべての商品リンクを基準に探す
        all_links = soup.find_all("a", href=re.compile(r'/shopdetail/\d+'))
        
        for link in all_links:
            try:
                name = link.get_text(strip=True)
                if not name or len(name) < 5:
                    continue
                
                # 親要素から価格を探す
                parent_td = link.find_parent("td")
                if parent_td:
                    # 同じtable内で価格を探す
                    parent_table = parent_td.find_parent("table")
                    if parent_table:
                        # table内のすべてのテキストから価格を探す
                        table_text = parent_table.get_text()
                        price_match = re.search(r'([0-9,]+)円\s*\(税込\)', table_text)
                        if price_match:
                            price = price_match.group(1).replace(",", "")
                            
                            key = f"{name}||{price}"
                            if key not in seen:
                                seen.add(key)
                                results.append({"name": name, "price": price})
                                
            except Exception as e:
                continue
                
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        return []
    
    return results

def main():
    items = scrape_penguincam()
    
    if not items:
        print("商品が見つかりませんでした")
        return 0
    
    for item in items:
        print(f"{item['name']}　{item['price']}円")
    
    return len(items)

if __name__ == "__main__":
    main()