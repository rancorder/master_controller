#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""kanto_camera.py - 関東カメラスクレイピングツール (Master Controller対応)"""
from __future__ import annotations
import hashlib, logging, os, re, sys, time, uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Final, Generator, List, Optional, Tuple
from playwright.sync_api import BrowserContext, Page, sync_playwright, TimeoutError as PlaywrightTimeout

class Constants:
    # 複数URLに対応
    BASE_URL: Final[str] = "https://www.kantocamera.com/collections/all?sort_by=created-descending"
    LEICA_URL: Final[str] = "https://www.kantocamera.com/collections/leica?sort_by=created-descending"
    
    TARGET_URLS: Final[Tuple[str, ...]] = (
        BASE_URL,
        # LEICA_URL,  # 必要に応じて追加
    )
    
    PAGE_LOAD_TIMEOUT_MS: Final[int] = 60000
    STABILITY_WAIT_MS: Final[int] = 3000
    MIN_VALID_PRICE: Final[int] = 1000
    MAX_VALID_PRICE: Final[int] = 10_000_000

class ScraperExitCode(Enum):
    SUCCESS = 0
    PARTIAL_SUCCESS = 1
    FAILURE = 2

@dataclass(frozen=True, slots=True)
class ProductData:
    name: str
    price: int
    url_index: int
    product_hash: str = ""
    rank: int = 0
    
    @classmethod
    def create(cls, name: str, price: int, url_index: int, rank: int = 0) -> ProductData:
        product_hash = hashlib.md5(f"{name}_{price}".encode("utf-8")).hexdigest()[:8]
        return cls(name=name, price=price, url_index=url_index, product_hash=product_hash, rank=rank)
    
    def to_output_line(self) -> str:
        # Master Controller形式: カンマなし
        return f"{self.name} {self.price}円"

@dataclass
class ScrapeResult:
    success: bool
    products: List[ProductData]
    error_message: Optional[str] = None
    duration_seconds: float = 0.0
    exit_code: ScraperExitCode = ScraperExitCode.SUCCESS
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

class StructuredLogger:
    def __init__(self, name: str = "kanto", level: int = logging.INFO):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)
        self._cid: Optional[str] = None
        if not self._logger.handlers:
            h = logging.StreamHandler(sys.stdout)
            h.setLevel(level)
            h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
            self._logger.addHandler(h)
    
    def set_correlation_id(self, cid: str) -> None:
        self._cid = cid
    
    def _f(self, m: str) -> str:
        return f"[{self._cid}] {m}" if self._cid else m
    
    def info(self, m: str) -> None:
        self._logger.info(self._f(m))
    
    def debug(self, m: str) -> None:
        self._logger.debug(self._f(m))
    
    def error(self, m: str) -> None:
        self._logger.error(self._f(m))

class ProductValidator:
    @staticmethod
    def validate_price(t: str) -> Optional[int]:
        # "230,000 円" → 230000
        c = re.sub(r'[^\d]', '', t)
        try:
            p = int(c)
            return p if Constants.MIN_VALID_PRICE <= p <= Constants.MAX_VALID_PRICE else None
        except:
            return None
    
    @staticmethod
    def validate_name(n: str) -> Optional[str]:
        n = re.sub(r"\s+", " ", n).strip()
        # 不要な文字列を削除
        n = re.sub(r'（委託販売品）', '', n)
        n = re.sub(r'\(委託販売品\)', '', n)
        n = n.strip()
        return n if len(n) >= 3 else None

class PlaywrightManager:
    def __init__(self, headless: bool = True, logger=None):
        self._headless = headless
        self._logger = logger or StructuredLogger()
    
    @contextmanager
    def browser_context(self) -> Generator[BrowserContext, None, None]:
        pw, br, ctx = None, None, None
        try:
            pw = sync_playwright().start()
            br = pw.chromium.launch(
                headless=self._headless,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
            )
            ctx = br.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            yield ctx
        finally:
            for r in [ctx, br]:
                if r:
                    try:
                        r.close()
                    except:
                        pass
            if pw:
                try:
                    pw.stop()
                except:
                    pass

class KantoCameraParser:
    def __init__(self, validator, logger=None):
        self._validator = validator
        self._logger = logger or StructuredLogger()
    
    def parse_page(self, page: Page, url_index: int) -> List[ProductData]:
        """ページから商品情報を抽出"""
        products = []
        
        try:
            # Shopifyの商品カードを探す
            # セレクタ候補
            selectors = [
                'div.grid__item',  # 一般的なShopifyグリッド
                'div.product-item',
                'div[class*="product"]',
                'li.grid__item',
            ]
            
            product_cards = None
            for selector in selectors:
                try:
                    cards = page.query_selector_all(selector)
                    if cards and len(cards) > 5:  # 5個以上見つかったら採用
                        product_cards = cards
                        if os.getenv("DEBUG"):
                            self._logger.debug(f"セレクタ '{selector}' で{len(cards)}個の商品を検出")
                        break
                except:
                    continue
            
            if not product_cards:
                self._logger.error("商品カードが見つかりません")
                return []
            
            # 各商品カードから情報を抽出
            for rank, card in enumerate(product_cards, start=1):
                try:
                    # 商品名を探す
                    name_elem = None
                    name_selectors = [
                        'h3 a',
                        'a.product-item-meta__title',
                        'a[href*="/products/"]',
                        '.card-information__text h3',
                        'h3.card__heading a',
                    ]
                    
                    for ns in name_selectors:
                        try:
                            name_elem = card.query_selector(ns)
                            if name_elem:
                                break
                        except:
                            continue
                    
                    if not name_elem:
                        continue
                    
                    name_text = name_elem.inner_text().strip()
                    name = self._validator.validate_name(name_text)
                    
                    if not name or len(name) < 3:
                        continue
                    
                    # 価格を探す（セール価格を優先）
                    card_html = card.inner_html()
                    
                    price = None
                    sale_price = None
                    regular_price = None
                    
                    # 1. セール価格を探す（最優先）
                    sale_selectors = [
                        'span.price-item--sale',
                        '.price__sale span.price-item',
                        'span[class*="sale"]',
                    ]
                    
                    for ss in sale_selectors:
                        try:
                            sale_elem = card.query_selector(ss)
                            if sale_elem:
                                sale_text = sale_elem.inner_text().strip()
                                sale_price = self._validator.validate_price(sale_text)
                                if sale_price:
                                    break
                        except:
                            continue
                    
                    # 2. 通常価格を探す
                    regular_selectors = [
                        'span.price-item--regular',
                        '.price__regular span.price-item',
                        'span.price-item',
                    ]
                    
                    for rs in regular_selectors:
                        try:
                            reg_elem = card.query_selector(rs)
                            if reg_elem:
                                reg_text = reg_elem.inner_text().strip()
                                regular_price = self._validator.validate_price(reg_text)
                                if regular_price:
                                    break
                        except:
                            continue
                    
                    # 3. 優先順位: セール価格 > 通常価格
                    price = sale_price if sale_price else regular_price
                    
                    # 4. 見つからなければHTMLから正規表現で抽出
                    if not price:
                        price_patterns = [
                            r'(\d{1,3}(?:,\d{3})*)\s*円',
                            r'¥\s*(\d{1,3}(?:,\d{3})*)',
                            r'JPY\s*(\d{1,3}(?:,\d{3})*)',
                        ]
                        
                        for pattern in price_patterns:
                            match = re.search(pattern, card_html)
                            if match:
                                price_text = match.group(1)
                                price = self._validator.validate_price(price_text)
                                if price:
                                    break
                    
                    if not price:
                        if os.getenv("DEBUG"):
                            self._logger.debug(f"価格が見つかりません: {name}")
                        continue
                    
                    products.append(ProductData.create(
                        name=name,
                        price=price,
                        url_index=url_index,
                        rank=rank
                    ))
                    
                except Exception as e:
                    self._logger.debug(f"商品カード{rank}の解析エラー: {e}")
                    continue
            
        except Exception as e:
            self._logger.error(f"ページ解析エラー: {e}")
        
        return products

class KantoCameraScraper:
    def __init__(self, logger=None):
        self._logger = logger or StructuredLogger()
        self._validator = ProductValidator()
        self._parser = KantoCameraParser(self._validator, self._logger)
        self._pw = PlaywrightManager(logger=self._logger)
    
    def scrape(self) -> ScrapeResult:
        cid = str(uuid.uuid4())[:8]
        self._logger.set_correlation_id(cid)
        start = time.time()
        all_prods = []
        
        try:
            with self._pw.browser_context() as ctx:
                page = ctx.new_page()
                
                # 各URLをスクレイプ
                for url_index, url in enumerate(Constants.TARGET_URLS):
                    try:
                        self._logger.info(f"URL[{url_index}]アクセス: {url}")
                        
                        page.goto(url, wait_until="networkidle", timeout=Constants.PAGE_LOAD_TIMEOUT_MS)
                        page.wait_for_timeout(Constants.STABILITY_WAIT_MS)
                        
                        # 1ページ目を解析
                        prods = self._parser.parse_page(page, url_index)
                        all_prods.extend(prods)
                        
                        self._logger.info(f"URL[{url_index}]: {len(prods)}件取得")
                        
                        # ページネーション対応（オプション）
                        if os.getenv("SCRAPE_ALL_PAGES"):
                            page_num = 2
                            while page_num <= 4:  # 最大4ページ
                                try:
                                    next_url = f"{url}&page={page_num}"
                                    self._logger.info(f"ページ{page_num}アクセス: {next_url}")
                                    
                                    page.goto(next_url, wait_until="networkidle", timeout=Constants.PAGE_LOAD_TIMEOUT_MS)
                                    page.wait_for_timeout(2000)
                                    
                                    page_prods = self._parser.parse_page(page, url_index)
                                    
                                    if not page_prods:
                                        break
                                    
                                    all_prods.extend(page_prods)
                                    self._logger.info(f"ページ{page_num}: {len(page_prods)}件取得")
                                    page_num += 1
                                    
                                except Exception as e:
                                    self._logger.debug(f"ページ{page_num}エラー: {e}")
                                    break
                        
                    except Exception as e:
                        self._logger.error(f"URL[{url_index}]エラー: {e}")
                        continue
                
                page.close()
            
            dur = time.time() - start
            
            # Master Controller形式で出力
            if all_prods:
                for idx in range(len(Constants.TARGET_URLS)):
                    url_prods = [p for p in all_prods if p.url_index == idx]
                    if url_prods:
                        print(f"---URL_INDEX:{idx}---")
                        for p in url_prods:
                            print(p.to_output_line())
                print("SUCCESS")
            else:
                print("ERROR:No products found")
            
            return ScrapeResult(
                success=len(all_prods) > 0,
                products=all_prods,
                duration_seconds=dur,
                exit_code=ScraperExitCode.SUCCESS if all_prods else ScraperExitCode.FAILURE,
                correlation_id=cid
            )
            
        except Exception as e:
            self._logger.error(f"致命的エラー: {e}")
            print(f"ERROR:{str(e)}")
            return ScrapeResult(
                success=False,
                products=all_prods,
                error_message=str(e),
                duration_seconds=time.time() - start,
                exit_code=ScraperExitCode.FAILURE,
                correlation_id=cid
            )

def main() -> int:
    logger = StructuredLogger(level=logging.DEBUG if os.getenv("DEBUG") else logging.INFO)
    scraper = KantoCameraScraper(logger=logger)
    result = scraper.scrape()
    return result.exit_code.value

if __name__ == "__main__":
    sys.exit(main())
