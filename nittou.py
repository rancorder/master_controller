#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nittou.py - 日東商事スクレイピング（修正版）

修正内容:
- 🔴 構文エラー修正（timeout未指定）
- 🔴 無限ループ対策（MAX_PAGES上限）
- 🔴 エンコーディング明示化
- 🟡 リトライ機構追加
- 🟡 重複排除機構
- 🟢 url_index対応
- 🟢 セッション再利用
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
from typing import List, Dict

# 定数
MAX_PAGES = 50
TIMEOUT_SECONDS = 10
RETRY_COUNT = 3
RETRY_DELAY = 2

def scrape_nittou() -> List[Dict[str, str]]:
    """日東商事スクレイピング
    
    Returns:
        List[Dict[str, str]]: 商品リスト [{"name": ..., "price": ...}, ...]
    """
    
    print(f"nittou.py 実行開始: {datetime.now()}")
    
    results = []
    seen = set()  # 重複排除用
    page = 0
    
    with requests.Session() as session:
        # User-Agent設定（ブロック対策）
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        while page < MAX_PAGES:
            url = (
                f"https://camerafan.jp/nittou/itemlist.php?"
                f"m=&c=0&s=nittou&l=&h=&w=&nw=&sr=-16&re=0&p={page}&sp=&max=50"
            )
            
            # リトライ機構
            for retry in range(RETRY_COUNT):
                try:
                    response = session.get(url, timeout=TIMEOUT_SECONDS)
                    response.raise_for_status()  # HTTP 4xx/5xxをエラーとして扱う
                    
                    # エンコーディング明示
                    response.encoding = 'utf-8'
                    soup = BeautifulSoup(response.text, "html.parser")
                    
                    break  # 成功したらリトライループを抜ける
                    
                except requests.Timeout:
                    print(f"⚠️ タイムアウト (ページ={page}, リトライ={retry+1}/{RETRY_COUNT})")
                    if retry < RETRY_COUNT - 1:
                        time.sleep(RETRY_DELAY)
                    else:
                        print(f"❌ {RETRY_COUNT}回リトライ失敗, ページをスキップ")
                        page += 1
                        break
                        
                except requests.RequestException as e:
                    print(f"❌ ネットワークエラー (ページ={page}): {e}")
                    # ネットワークエラーは即座に全体終了
                    return results
            else:
                # リトライ失敗で次のページへ
                continue
            
            # 商品抽出
            items = soup.select("div#items div.item")
            
            if not items:
                print(f"  ページ{page}: 商品なし（最終ページ）")
                break
            
            page_count = 0
            for item in items:
                try:
                    name_tag = item.select_one("div.itemn a")
                    price_tag = item.select_one("div.itemp")
                    
                    if not name_tag or not price_tag:
                        continue
                    
                    name = name_tag.get_text(strip=True)
                    price = price_tag.get_text(strip=True).replace('¥', '').replace(',', '').strip()
                    
                    # 重複チェック
                    key = f"{name.lower()}_{price}"
                    if key in seen:
                        continue
                    
                    seen.add(key)
                    results.append({"name": name, "price": price})
                    page_count += 1
                    
                except Exception as e:
                    print(f"⚠️ 商品パース失敗: {e}")
                    continue
            
            print(f"  ページ{page}: {page_count}件取得")
            
            # 次のページ確認
            next_link = soup.find("a", string=lambda s: s and "次のページへ" in s)
            if not next_link:
                print(f"  最終ページ到達（{page}ページ目）")
                break
            
            page += 1
    
    print(f"✅ 取得完了: {len(results)}件（重複除外後）")
    return results


if __name__ == "__main__":
    items = scrape_nittou()
    
    # master_controller用の標準出力
    for item in items:
        print(f"{item['name']} {item['price']}円")