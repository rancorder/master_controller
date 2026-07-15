# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time

BASE_URL = "https://www.antiquary.jp"
TARGET_URL = BASE_URL + "/"

def fetch_items():
    """ウェブサイトからアイテムデータを取得する関数"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    REQUEST_TIMEOUT = 10  # 10秒に短縮
    MAX_RETRIES = 2  # リトライ回数削減
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(TARGET_URL, headers=headers, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(1)
                    continue
                else:
                    return []
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            items = []
            seen = set()
            
            product_tables = soup.select("td.lims table")
            
            for table in product_tables[:100]:  # 最大100件に制限
                try:
                    name_tag = table.select("a[href^='/shopdetail']")
                    price_tag = table.select_one("tr.woong:nth-of-type(3)")
                    
                    if name_tag and price_tag and len(name_tag) >= 2:
                        name = name_tag[1].get_text(strip=True)
                        price = price_tag.get_text(strip=True).replace("円(税込)", "").replace(",", "").strip()
                        identifier = f"{name}｜{price}"
                        
                        if identifier not in seen:
                            seen.add(identifier)
                            items.append({
                                "name": name,
                                "price": price + "円"
                            })
                            
                except:
                    continue
            
            return items
            
        except:
            if attempt < MAX_RETRIES - 1:
                time.sleep(1)
                continue
            else:
                return []
    
    return []

def main():
    try:
        current_items = fetch_items()
        
        # 取得した商品を表示
        for item in current_items:
            # priceに既に「円」が含まれているので追加しない
            print(f"{item['name']} {item['price']}", flush=True)
        
    except Exception as e:
        print(f"ERROR: {str(e)}")

if __name__ == "__main__":
    main()