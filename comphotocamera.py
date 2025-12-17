import requests
from bs4 import BeautifulSoup
import re
import sys

# コンソールのエンコーディングをUTF-8に設定
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding='utf-8')

# スクレイピング対象URLとヘッダー
url = "http://www.comphotocamera.com/sale/NEW.html"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36"}

# 商品データを取得する関数
def fetch_product_data():
    response = requests.get(url, headers=headers)
    response.encoding = response.apparent_encoding
    soup = BeautifulSoup(response.text, "html.parser")
    
    products = {}
    rows = soup.find_all("tr", bordercolor="#333333")
    
    for row in rows:
        name_td = row.find("td", width="240", rowspan="2")
        name = name_td.get_text(strip=True) if name_td else None
        
        price_td = row.find("td", width="61", rowspan="2")
        price = price_td.get_text(strip=True) if price_td else None
        
        if price:
            price = re.sub(r"[^\d]", "", price)  # 数字だけ抽出
        
        if name and price:
            products[name] = price
    
    return products

if __name__ == "__main__":
    products = fetch_product_data()
    
    for name, price in products.items():
        # 価格から数字のみ抽出
        price_match = re.search(r'([0-9,]+)', str(price))
        if price_match:
            price_clean = price_match.group(1).replace(',', '')
            print(f"{name} {price_clean}円")