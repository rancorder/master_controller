#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ymmtca.py - 山本写真機店スクレイパー（URL index対応）
"""
import requests
from bs4 import BeautifulSoup
import time
import re
import random

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1'
]

def scrape_ymmtca():
    """
    指定されたURLリストから商品情報をスクレイピングし、
    標準出力に「商品名 価格円」形式で出力します。
    """
    target_urls = [
        "http://www.avis.ne.jp/~ymmtca/medama2.htm",  # url_index: 0
        "http://www.avis.ne.jp/~ymmtca/leica_2.htm",  # url_index: 1
        "http://www.avis.ne.jp/~ymmtca/tinnpinn_2.htm",  # url_index: 2
        "http://www.avis.ne.jp/~ymmtca/contax_2.htm",  # url_index: 3
        "http://www.avis.ne.jp/~ymmtca/nikon_2.htm",  # url_index: 4
        "http://www.avis.ne.jp/~ymmtca/sonota%20tyuuko_2.htm",  # url_index: 5
        "http://www.avis.ne.jp/~ymmtca/fokutorennda_2.htm",  # url_index: 6
        "http://www.avis.ne.jp/~ymmtca/akusesari-2.htm"  # url_index: 7
    ]

    for url_index, url in enumerate(target_urls):
        # URL切り替えを明示
        print(f"---URL_INDEX:{url_index}---")
        
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            response = requests.get(url, headers=headers, timeout=15)
            
            response.raise_for_status()
            response.encoding = response.apparent_encoding

            soup = BeautifulSoup(response.text, "html.parser")

            product_table = soup.find("table", attrs={"border": "1"})
            
            if not product_table:
                all_tables = soup.find_all("table")
                if len(all_tables) >= 3:
                    product_table = all_tables[2]
                else:
                    continue

            table_rows = product_table.find_all("tr")[1:]

            if not table_rows:
                continue

            for row in table_rows:
                columns = row.find_all("td")
                
                if len(columns) >= 4:
                    product_name = columns[1].text.strip()
                    price = columns[3].text.strip()
                elif len(columns) == 3:
                    product_name = columns[0].text.strip()
                    price = columns[2].text.strip()
                else:
                    continue

                # SOLD OUTをスキップ
                if 'SOLD OUT' in price.upper():
                    continue

                # 価格の数値抽出
                price_match = re.search(r'[\d,]+', price)
                if price_match:
                    price_num = price_match.group().replace(',', '')
                    
                    # 商品名のクリーニング
                    clean_name = re.sub(r'\s+', ' ', product_name).strip()
                    
                    # master_controller用の標準出力
                    if len(clean_name) > 3 and price_num:
                        print(f"{clean_name} {price_num}円")

        except requests.exceptions.RequestException:
            continue
        except Exception:
            continue
        
        if url_index < len(target_urls) - 1:
            time.sleep(3)

if __name__ == "__main__":
    scrape_ymmtca()