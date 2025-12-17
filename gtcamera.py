import requests
from bs4 import BeautifulSoup
import re

# 設定
BASE_URL = "http://gtcamera.co.jp"
TARGET_URL = BASE_URL + "/"

# 価格文字列を数字に変換
def clean_price(price_text):
    price = re.sub(r'[^\d]', '', price_text)
    try:
        return int(price)
    except ValueError:
        return None

# 商品情報取得
def fetch_items():
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(TARGET_URL, headers=headers, timeout=10)
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, "html.parser")

        section = soup.find("div", id="r_new")
        if not section:
            print("新着商品セクションが見つかりません")
            return []

        product_blocks = section.select("td.lims table")
        items = []

        for block in product_blocks:
            name_tag = block.select_one("tr:nth-child(2) td a")
            price_tag = block.select("tr.woong")
            if name_tag and price_tag:
                name = name_tag.get_text(strip=True)
                price = clean_price(price_tag[-1].get_text(strip=True))
                if name and price is not None:
                    items.append((name, price))

        return items
    except Exception as e:
        print(f"取得エラー: {e}")
        return []

if __name__ == "__main__":
    items = fetch_items()
    if items:
        for name, price in items:
            # 価格から数字のみ抽出
            price_clean = str(price).replace(',', '')
            print(f"{name} {price_clean}円")
    else:
        print("商品情報を取得できませんでした")
