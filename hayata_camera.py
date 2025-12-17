#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hayata_camera.py - 早田カメララボスクレイピング（requests版）

master_controller一元管理対応: 標準出力のみ
静的スクレイピング: requests + BeautifulSoup使用
画像URL出力なし（商品名と価格のみ）
"""

import hashlib
import re
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


class HayataCameraConfig:
    """設定管理クラス（Google Style Guide準拠）"""
    
    # スクレイピング対象URL
    BASE_URL: str = "https://www.hayatacamera.co.jp"
    TARGET_URL: str = f"{BASE_URL}/category/consign-new/"
    
    # タイムアウト設定（秒）
    REQUEST_TIMEOUT: int = 30
    
    # 再試行設定
    MAX_RETRIES: int = 3
    RETRY_DELAY: float = 2.0
    
    # 最低取得商品数
    MIN_PRODUCTS: int = 5
    
    # User-Agent
    USER_AGENT: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )


class ProductData:
    """商品データクラス（型安全）
    
    Attributes:
        name: 商品名
        price: 価格（文字列）
        img_url: 画像URL
        category: カテゴリ
        detail_url: 詳細ページURL
        hash: 商品ハッシュ（重複排除用）
    """
    
    def __init__(
        self,
        name: str,
        price: str,
        img_url: str = "",
        category: str = "",
        detail_url: str = "",
    ):
        self.name = name
        self.price = price
        self.img_url = img_url
        self.category = category
        self.detail_url = detail_url
        self.hash = self._generate_hash()
    
    def _generate_hash(self) -> str:
        """商品のハッシュ値を生成（重複排除用）
        
        Returns:
            MD5ハッシュ（8桁）
        """
        unique_key = f"{self.name}_{self.price}_{self.img_url}"
        return hashlib.md5(unique_key.encode('utf-8')).hexdigest()[:8]
    
    def to_dict(self) -> Dict[str, str]:
        """辞書形式に変換
        
        Returns:
            商品データの辞書
        """
        return {
            'name': self.name,
            'price': self.price,
            'img_url': self.img_url,
            'category': self.category,
            'detail_url': self.detail_url,
            'hash': self.hash
        }


class HayataCameraScraper:
    """早田カメララボスクレイピングメインクラス
    
    requests + BeautifulSoupを使用した静的スクレイピング実装
    Google Style Guide準拠、型ヒント完備
    """
    
    def __init__(self, config: Optional[HayataCameraConfig] = None):
        """初期化
        
        Args:
            config: 設定オブジェクト（省略時はデフォルト）
        """
        self.config = config or HayataCameraConfig()
        
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.config.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        })
        
        self.products: List[ProductData] = []
        self.errors: List[str] = []
    
    def scrape(self) -> int:
        """スクレイピングメイン処理
        
        Returns:
            取得した商品数
            
        Raises:
            Exception: スクレイピング失敗時
        """
        print(f"scraper 実行開始: {datetime.now()}")
        
        try:
            # 一覧ページから商品リンク取得
            product_links = self._get_product_links()
            
            if not product_links:
                print("ERROR: 商品リンク取得失敗")
                return 0
            
            print(f"商品リンク取得: {len(product_links)}件")
            
            # 詳細ページから商品情報取得
            for detail_url, img_url, category in product_links:
                product = self._fetch_product_detail(detail_url, img_url, category)
                if product:
                    self.products.append(product)
            
            # 結果出力
            self._output_results()
            
            return len(self.products)
            
        except Exception as e:
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def _get_product_links(self) -> List[tuple]:
        """一覧ページから商品リンクを取得
        
        Returns:
            商品リンク、画像URL、カテゴリのタプルリスト
            
        Raises:
            Exception: ページ取得失敗時
        """
        for attempt in range(self.config.MAX_RETRIES):
            try:
                print(f"一覧ページ取得試行 {attempt + 1}/{self.config.MAX_RETRIES}")
                
                response = self.session.get(
                    self.config.TARGET_URL,
                    timeout=self.config.REQUEST_TIMEOUT
                )
                
                print(f"  HTTPステータス: {response.status_code}")
                
                if response.status_code != 200:
                    if attempt < self.config.MAX_RETRIES - 1:
                        print(f"  エラー、{self.config.RETRY_DELAY}秒後に再試行...")
                        time.sleep(self.config.RETRY_DELAY)
                        continue
                    else:
                        raise Exception(f"HTTPエラー: {response.status_code}")
                
                # HTML解析
                soup = BeautifulSoup(response.content, 'html.parser')
                
                product_links = []
                
                # 複数のセレクタパターンを試行
                selectors = [
                    'article.post',
                    'article',
                    'div.post',
                    'li.post',
                    '.p-postList__item'
                ]
                
                articles = []
                for selector in selectors:
                    articles = soup.select(selector)
                    if articles:
                        print(f"  商品要素: {len(articles)}件（{selector}）")
                        break
                
                if not articles:
                    # 直接リンクを探す
                    print("  セレクタが見つからないため、リンクを直接検索...")
                    all_links = soup.find_all('a', href=True)
                    
                    for link in all_links:
                        href = link.get('href', '')
                        
                        # 商品詳細ページのURLパターン
                        if self._is_product_url(href):
                            detail_url = urljoin(self.config.BASE_URL, href)
                            
                            # 画像取得
                            img_elem = link.find('img')
                            if not img_elem and link.parent:
                                img_elem = link.parent.find('img')
                            
                            img_url = ""
                            if img_elem:
                                # data-src属性を優先（Lazy Loading対応）
                                img_url = img_elem.get('data-src', '') or img_elem.get('src', '')
                                # Base64エンコード画像を除外
                                if img_url and img_url.startswith('data:image/'):
                                    img_url = ""
                                # URLのクリーンアップ
                                if img_url:
                                    img_url = self._clean_image_url(img_url)
                            
                            # カテゴリ取得
                            category = self._extract_category(link)
                            
                            product_links.append((detail_url, img_url, category))
                    
                    print(f"  直接検索: {len(product_links)}件")
                    # 重複削除
                    product_links = list(set(product_links))
                    return product_links
                
                # セレクタから商品情報を抽出
                for article in articles:
                    try:
                        # リンク取得
                        link_elem = article.find('a', href=True)
                        if not link_elem:
                            continue
                        
                        detail_url = urljoin(self.config.BASE_URL, link_elem['href'])
                        
                        # 画像URL取得
                        img_elem = article.find('img')
                        img_url = ""
                        if img_elem:
                            # data-src属性を優先（Lazy Loading対応）
                            img_url = img_elem.get('data-src', '') or img_elem.get('src', '')
                            # Base64エンコード画像を除外
                            if img_url and img_url.startswith('data:image/'):
                                img_url = ""
                            # URLのクリーンアップ
                            if img_url:
                                img_url = self._clean_image_url(img_url)
                        
                        # カテゴリ取得
                        category = self._extract_category(article)
                        
                        product_links.append((detail_url, img_url, category))
                        
                    except Exception as e:
                        self.errors.append(f"商品リンク抽出エラー: {e}")
                        continue
                
                print(f"  取得成功: {len(product_links)}件")
                return product_links
                
            except requests.exceptions.Timeout:
                if attempt < self.config.MAX_RETRIES - 1:
                    print(f"  タイムアウト、{self.config.RETRY_DELAY}秒後に再試行...")
                    time.sleep(self.config.RETRY_DELAY)
                else:
                    raise
            except Exception as e:
                if attempt < self.config.MAX_RETRIES - 1:
                    print(f"  エラー: {e}、{self.config.RETRY_DELAY}秒後に再試行...")
                    time.sleep(self.config.RETRY_DELAY)
                else:
                    raise
        
        return []
    
    def _is_product_url(self, url: str) -> bool:
        """商品詳細URLか判定
        
        Args:
            url: 判定するURL
            
        Returns:
            商品URLの場合True
        """
        # 除外パターン
        exclude_patterns = [
            'category', 'page', 'tag', 'author', 
            'search', 'wp-content', 'wp-includes',
            '#', 'javascript:', 'mailto:'
        ]
        
        if not url or url == '/' or url == '#':
            return False
        
        for pattern in exclude_patterns:
            if pattern in url.lower():
                return False
        
        # 含むべきパターン
        if 'hayatacamera.co.jp' in url or (url.startswith('/') and len(url) > 10):
            return True
        
        return False
    
    def _extract_category(self, element) -> str:
        """要素からカテゴリを抽出
        
        Args:
            element: BeautifulSoup要素
            
        Returns:
            カテゴリ名
        """
        # カテゴリ要素を探す
        cat_elem = element.find(class_=lambda x: x and 'cat' in x.lower())
        if cat_elem:
            return cat_elem.get_text(strip=True)
        
        # span要素を探す
        span_elem = element.find('span')
        if span_elem and span_elem.get_text(strip=True):
            text = span_elem.get_text(strip=True)
            # カテゴリっぽいテキスト（短い単語）のみ
            if len(text) < 20:
                return text
        
        return ""
    
    def _clean_image_url(self, url: str) -> str:
        """画像URLをクリーンアップ
        
        Args:
            url: 元のURL
            
        Returns:
            クリーンアップされたURL、不正な場合は空文字
        """
        if not url:
            return ""
        
        # 前後の空白・改行・タブを削除
        url = url.strip()
        
        # 改行コードを削除
        url = url.replace('\n', '').replace('\r', '')
        
        # http/httpsで始まらない場合は除外
        if not url.startswith('http://') and not url.startswith('https://'):
            return ""
        
        # 画像の拡張子をチェック
        valid_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']
        url_lower = url.lower()
        
        # 拡張子がある場合はチェック
        has_extension = any(ext in url_lower for ext in valid_extensions)
        if not has_extension:
            # クエリパラメータの前で切り取る
            if '?' in url:
                url = url.split('?')[0]
        
        # 最大長チェック（異常に長いURLを除外）
        if len(url) > 500:
            return ""
        
        # スペースが含まれている場合は除外
        if ' ' in url:
            return ""
        
        return url
    
    def _fetch_product_detail(
        self,
        detail_url: str,
        img_url: str,
        category: str
    ) -> Optional[ProductData]:
        """単一商品の詳細情報を取得
        
        Args:
            detail_url: 詳細ページURL
            img_url: 画像URL
            category: カテゴリ
            
        Returns:
            ProductData、失敗時はNone
        """
        try:
            response = self.session.get(
                detail_url,
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code != 200:
                self.errors.append(f"HTTP {response.status_code}: {detail_url}")
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 商品名取得（複数パターン）
            title_selectors = [
                'h1.p-postDetail__title',
                'h1.entry-title',
                'h1.post-title',
                'h1',
                '.entry-title',
                '.post-title'
            ]
            
            product_name = None
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    product_name = title_elem.get_text(strip=True)
                    break
            
            if not product_name:
                self.errors.append(f"商品名取得失敗: {detail_url}")
                return None
            
            # 価格取得
            price = self._extract_price(soup)
            
            if not price:
                self.errors.append(f"価格取得失敗: {detail_url}")
                return None
            
            return ProductData(
                name=product_name,
                price=price,
                img_url=img_url,
                category=category,
                detail_url=detail_url
            )
            
        except Exception as e:
            self.errors.append(f"詳細取得エラー ({detail_url}): {e}")
            return None
    
    def _extract_price(self, soup: BeautifulSoup) -> str:
        """価格を抽出（入力検証付き）
        
        Args:
            soup: BeautifulSoupオブジェクト
            
        Returns:
            価格文字列（数字のみ）、取得失敗時は空文字
        """
        # 複数のセレクタを試行
        price_selectors = [
            'span.woocommerce-Price-amount',
            'span.price',
            'div.price',
            'p.price',
            '.price'
        ]
        
        for selector in price_selectors:
            price_elem = soup.select_one(selector)
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                
                # 価格パターンマッチング
                price_match = re.search(r'([0-9,]+)\s*円?', price_text)
                if price_match:
                    price_clean = price_match.group(1).replace(',', '')
                    
                    try:
                        price_int = int(price_clean)
                        if 100 <= price_int <= 100000000:
                            return str(price_int)
                    except ValueError:
                        continue
        
        # テキスト全体から探索
        body_text = soup.get_text()
        price_patterns = [
            r'([0-9,]+)\s*円',
            r'¥\s*([0-9,]+)',
            r'価格[：:]\s*([0-9,]+)'
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, body_text)
            if match:
                price_clean = match.group(1).replace(',', '')
                try:
                    price_int = int(price_clean)
                    if 100 <= price_int <= 100000000:
                        return str(price_int)
                except ValueError:
                    continue
        
        return ""
    
    def _output_results(self) -> None:
        """結果を標準出力（master_controller互換形式）"""
        print(f"\n総取得数: {len(self.products)}件")
        
        if self.errors:
            print(f"エラー数: {len(self.errors)}件")
            for error in self.errors[:3]:
                print(f"  - {error}")
        
        # 商品情報を標準出力（master_controller用）
        # 画像URLは出力しない（商品名と価格のみ）
        for product in self.products:
            print(f"{product.name} {product.price}円")
        
        # ステータス出力
        if len(self.products) >= self.config.MIN_PRODUCTS:
            print("SUCCESS")
        elif len(self.products) > 0:
            print("PARTIAL SUCCESS")
        else:
            print("FAILED")


def main() -> None:
    """メイン処理（エントリーポイント）"""
    scraper = HayataCameraScraper()
    
    start_time = time.time()
    product_count = scraper.scrape()
    elapsed_time = time.time() - start_time
    
    print(f"\n実行時間: {elapsed_time:.2f}秒")
    print(f"取得商品数: {product_count}件")


def scrape() -> int:
    """既存システムとの互換性維持用ラッパー
    
    Returns:
        取得した商品数
    """
    scraper = HayataCameraScraper()
    return scraper.scrape()


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except KeyboardInterrupt:
        print("\n中断されました")
        sys.exit(1)
    except Exception as e:
        print(f"致命的エラー: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)