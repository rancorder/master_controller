# -*- coding: utf-8 -*-
"""
antiquary.py - アンティクアリィスクレイパー
おすすめ商品と新着商品の両方に対応

出力形式:
  [おすすめ] 商品名 価格円
  [新着] 商品名 価格円
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import re

BASE_URL = "https://www.antiquary.jp"
TARGET_URL = BASE_URL + "/"

def extract_section_html(html_text, start_marker, end_marker=None):
    """
    HTMLコメントマーカーを使ってセクションを抽出
    
    Args:
        html_text: 全体のHTML文字列
        start_marker: 開始マーカー（例: "<!--▼おすすめ▼-->"）
        end_marker: 終了マーカー（Noneの場合は末尾まで）
    
    Returns:
        抽出されたHTMLセクション（文字列）
    """
    try:
        start_idx = html_text.find(start_marker)
        if start_idx == -1:
            return None
        
        if end_marker:
            end_idx = html_text.find(end_marker, start_idx)
            if end_idx == -1:
                return html_text[start_idx:]
            return html_text[start_idx:end_idx]
        else:
            return html_text[start_idx:]
    except:
        return None

def extract_products_from_html(section_html, category_label):
    """
    HTMLセクションから商品情報を抽出
    
    Args:
        section_html: 対象のHTMLセクション
        category_label: カテゴリラベル（"[おすすめ]" or "[新着]"）
    
    Returns:
        商品情報の辞書リスト
    """
    if not section_html:
        return []
    
    soup = BeautifulSoup(section_html, "html.parser")
    products = []
    
    product_tables = soup.select("td.lims table")
    
    for table in product_tables[:100]:  # 最大100件に制限
        try:
            name_tag = table.select("a[href^='/shopdetail']")
            price_tag = table.select_one("tr.woong:nth-of-type(3)")
            
            if name_tag and price_tag and len(name_tag) >= 2:
                name = name_tag[1].get_text(strip=True)
                price = price_tag.get_text(strip=True).replace("円(税込)", "").replace(",", "").strip()
                
                products.append({
                    "name": name,
                    "price": price,
                    "category": category_label
                })
                
        except:
            continue
    
    return products

def fetch_items(url_index=0):
    """
    ウェブサイトからアイテムデータを取得する関数
    
    Args:
        url_index: 0=おすすめのみ, 1=新着のみ
    
    Returns:
        商品情報の辞書リスト
        [
            {"name": "商品名", "price": "39800", "category": "[おすすめ]"},
            {"name": "商品名", "price": "12800", "category": "[新着]"},
            ...
        ]
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    REQUEST_TIMEOUT = 10  # 10秒
    MAX_RETRIES = 2
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"DEBUG: リクエスト試行 {attempt+1}/{MAX_RETRIES}", flush=True)
            response = requests.get(TARGET_URL, headers=headers, timeout=REQUEST_TIMEOUT)
            print(f"DEBUG: HTTPステータス={response.status_code}", flush=True)
            
            if response.status_code != 200:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(1)
                    continue
                else:
                    return []
            
            # 文字コードを明示的に指定（EUC-JP）
            response.encoding = 'euc-jp'
            html_text = response.text
            
            # デバッグ: HTMLコメント検出
            has_recommended = "<!--▼おすすめ▼-->" in html_text
            has_new_arrivals = "<!--▼新着商品▼-->" in html_text
            
            print(f"DEBUG: おすすめコメント存在={has_recommended}", flush=True)
            print(f"DEBUG: 新着コメント存在={has_new_arrivals}", flush=True)
            
            # おすすめ商品セクションを抽出
            recommended_section = extract_section_html(
                html_text, 
                "<!--▼おすすめ▼-->",
                "<!--▼新着商品▼-->"
            )
            
            # 新着商品セクションを抽出
            new_arrivals_section = extract_section_html(
                html_text,
                "<!--▼新着商品▼-->",
                None
            )
            
            # デバッグ: セクション抽出結果
            print(f"DEBUG: おすすめセクション={'有効' if recommended_section else '無効'}", flush=True)
            print(f"DEBUG: 新着セクション={'有効' if new_arrivals_section else '無効'}", flush=True)
            
            items = []
            seen = set()
            
            # おすすめ商品を処理（url_index=0の場合のみ）
            if url_index == 0 and recommended_section:
                recommended_products = extract_products_from_html(
                    recommended_section, 
                    "[おすすめ]"
                )
                print(f"DEBUG: おすすめ商品数={len(recommended_products)}件", flush=True)
                for product in recommended_products:
                    identifier = f"{product['name']}|{product['price']}"
                    if identifier not in seen:
                        seen.add(identifier)
                        items.append(product)
            else:
                print("DEBUG: おすすめセクションなし", flush=True)
            
            # 新着商品を処理（url_index=1の場合のみ）
            if url_index == 1 and new_arrivals_section:
                new_products = extract_products_from_html(
                    new_arrivals_section,
                    "[新着]"
                )
                print(f"DEBUG: 新着商品数={len(new_products)}件", flush=True)
                for product in new_products:
                    identifier = f"{product['name']}|{product['price']}"
                    if identifier not in seen:
                        seen.add(identifier)
                        items.append(product)
            else:
                print("DEBUG: 新着セクションなし", flush=True)
            
            return items
            
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(1)
                continue
            else:
                return []
    
    return []

def main():
    """メイン処理"""
    try:
        # おすすめ商品を取得
        print("---URL_INDEX:0---", flush=True)
        recommended_items = fetch_items(url_index=0)
        for item in recommended_items:
            print(f"{item['name']} {item['price']}円", flush=True)
        
        # 新着商品を取得
        print("---URL_INDEX:1---", flush=True)
        new_items = fetch_items(url_index=1)
        for item in new_items:
            print(f"{item['name']} {item['price']}円", flush=True)
        
    except Exception as e:
        print(f"ERROR: {str(e)}")

if __name__ == "__main__":
    main()