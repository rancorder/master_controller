# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re

BASE_URL = "http://www.suzuki-camera.com"
PAGES = ["/shop_01.html", "/shop_02.html"]

def clean_name(raw_name):
    if not raw_name:
        return ""
    cleaned = re.sub(r"^\d+\s*", "", raw_name.strip())
    cleaned = cleaned.replace("\n", "").replace("<br>", "").strip()
    return cleaned

def fetch_items():
    items = []
    seen = set()
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    for page_path in PAGES:
        full_url = BASE_URL + page_path
        
        try:
            response = requests.get(full_url, headers=headers, timeout=10)
            
            if response.encoding.lower() in ['iso-8859-1', 'ascii']:
                response.encoding = response.apparent_encoding or 'utf-8'
            
            soup = BeautifulSoup(response.text, "html.parser")
            item_blocks = soup.select('td.m[width="185"]')
            
            for block in item_blocks:
                try:
                    if "SOLD" in block.get_text():
                        continue
                    
                    name_tag = block.select_one("span.item")
                    price_tag = block.select_one("strong")
                    
                    if not name_tag or not price_tag:
                        continue
                    
                    raw_name = name_tag.get_text(separator=" ", strip=True)
                    name = clean_name(raw_name)
                    price = price_tag.get_text(strip=True).replace("¥", "").replace(",", "").strip()
                    
                    if not name or not price:
                        continue
                    
                    identifier = f"{name}｜{price}"
                    
                    if identifier not in seen:
                        seen.add(identifier)
                        items.append({"name": name, "price": price})
                        
                        # 15件で制限
                        if len(items) >= 15:
                            break
                    
                except:
                    continue
            
            if len(items) >= 15:
                break
                
        except:
            continue
    
    return items

def main():
    try:
        current_items = fetch_items()
        
        # 簡潔な結果出力
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{timestamp} - SUCCESS - Total: {len(current_items)}")
        
        # 取得した商品を表示
        for item in current_items:
            print(f"{item['name']} {item['price']}円")
        
    except Exception as e:
        print(f"ERROR: {str(e)}")

if __name__ == "__main__":
    main()