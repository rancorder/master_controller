#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
syuukou.py
master_controller一元管理対応: DB保存処理削除、標準出力のみ
SSLエラー回避版（証明書検証無効・警告抑制）
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import re
import sys
import urllib3

# SSL警告を非表示にする
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://www.syuukou.com/camera/"

def scrape_syuukou():
    """syuukouスクレイピング（SSL検証無効）"""
    print(f"syuukou 実行開始: {datetime.now()}")

    products = []

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        # SSL検証無効で取得
        response = requests.get(BASE_URL, headers=headers, timeout=15, verify=False)
        response.encoding = response.apparent_encoding

        if response.status_code != 200:
            print(f"HTTPエラー: {response.status_code}")
            return 0

        soup = BeautifulSoup(response.text, 'html.parser')

        items = soup.select("div.item_list")
        print(f"商品数: {len(items)}個")

        for item in items:
            try:
                title_tag = item.select_one("p.title")
                if not title_tag:
                    continue
                name = title_tag.get_text(strip=True)

                price_tag = item.select_one("dd.f-blk")
                if not price_tag:
                    continue
                price_text = price_tag.get_text(strip=True)
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

            except Exception as e:
                print(f"個別商品取得エラー: {e}", file=sys.stderr)
                continue

        print(f"総取得数: {len(products)}件")
        for product in products:
            print(f"{product['name']} {product['price']}円")

        if len(products) >= 10:
            print("SUCCESS")
        elif len(products) > 0:
            print("PARTIAL SUCCESS")
        else:
            print("NO DATA")

        return len(products)

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 0


def scrape():
    return scrape_syuukou()


def main():
    scrape_syuukou()


if __name__ == "__main__":
    main()
