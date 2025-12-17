#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""keiz_camera.py - Master Controller対応版"""
from __future__ import annotations
import hashlib, logging, os, re, sys, time, uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Final, Generator, List, Optional, Tuple
from playwright.sync_api import BrowserContext, Page, sync_playwright, TimeoutError as PlaywrightTimeout, Frame

class Constants:
    MAIN_URL: Final[str] = "http://ks-camera.jp/hanbai/index.html"
    IFRAME_URL: Final[str] = "http://ks-camera.jp/hanbai/kscameraorder/cgi-bin/shopping/main.cgi?display=normal&class=&word=&FF=&NP=&TOTAL=&enumber="
    PAGE_LOAD_TIMEOUT_MS: Final[int] = 60000
    STABILITY_WAIT_MS: Final[int] = 5000
    MIN_VALID_PRICE: Final[int] = 100
    MAX_VALID_PRICE: Final[int] = 50_000_000

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
        # master_controller形式: カンマなし
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
    def __init__(self, name: str = "keiz", level: int = logging.INFO):
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
        c = re.sub(r'[^\d]', '', t)
        try:
            p = int(c)
            return p if Constants.MIN_VALID_PRICE <= p <= Constants.MAX_VALID_PRICE else None
        except:
            return None
    
    @staticmethod
    def validate_name(n: str) -> Optional[str]:
        n = re.sub(r"\s+", " ", n).strip()
        n = re.sub(r'^[\[\(]?\s*\d+\s*[\]\)]?\s*[\.、．]?\s*', '', n)
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

class KeizCameraParser:
    def __init__(self, validator, logger=None):
        self._validator = validator
        self._logger = logger or StructuredLogger()
    
    def parse_frame(self, frame: Frame, url_index: int) -> List[ProductData]:
        """フレームから商品を抽出（テキストベース）"""
        products = []
        
        try:
            body_text = frame.evaluate("() => document.body.innerText")
            html_content = frame.content()
            
            # 商品パターン: [ 番号 ] 商品名
            product_pattern = r'\[\s*(\d+)\s*\]\s*([^\n]{10,}?)(?=\n|$)'
            matches = re.finditer(product_pattern, body_text, re.MULTILINE)
            
            for match in matches:
                try:
                    product_code = match.group(1)
                    product_text = match.group(2).strip()
                    
                    # クリーンアップ
                    product_name = re.sub(r'\s*(すべての商品|新商品|おすすめ商品)\s*$', '', product_text)
                    product_name = re.sub(r'\s+', ' ', product_name).strip()
                    
                    if len(product_name) < 5:
                        continue
                    
                    # 価格を探す
                    start_pos = match.end()
                    text_after = body_text[start_pos:start_pos + 500]
                    
                    price_patterns = [
                        r'([0-9,]+)\s*円',
                        r'[¥￥]\s*([0-9,]+)',
                        r'消費税[：:]\s*([0-9,]+)円',
                    ]
                    
                    price = None
                    for pattern in price_patterns:
                        price_match = re.search(pattern, text_after)
                        if price_match:
                            price_text = price_match.group(1)
                            price = self._validator.validate_price(price_text)
                            if price:
                                break
                    
                    # HTMLからも探す
                    if not price:
                        code_pos = html_content.find(f'[ {product_code} ]')
                        if code_pos == -1:
                            code_pos = html_content.find(f'[{product_code}]')
                        
                        if code_pos != -1:
                            html_around = html_content[code_pos:code_pos + 1000]
                            for pattern in price_patterns:
                                price_match = re.search(pattern, html_around)
                                if price_match:
                                    price_text = price_match.group(1)
                                    price = self._validator.validate_price(price_text)
                                    if price:
                                        break
                    
                    if price:
                        products.append(ProductData.create(
                            name=product_name,
                            price=price,
                            url_index=url_index,
                            rank=len(products) + 1
                        ))
                
                except Exception as e:
                    self._logger.debug(f"商品抽出エラー: {e}")
                    continue
            
        except Exception as e:
            self._logger.error(f"フレーム解析エラー: {e}")
        
        return products

class KeizCameraScraper:
    def __init__(self, logger=None):
        self._logger = logger or StructuredLogger()
        self._validator = ProductValidator()
        self._parser = KeizCameraParser(self._validator, self._logger)
        self._pw = PlaywrightManager(logger=self._logger)
    
    def scrape(self) -> ScrapeResult:
        cid = str(uuid.uuid4())[:8]
        self._logger.set_correlation_id(cid)
        start = time.time()
        all_prods = []
        
        try:
            with self._pw.browser_context() as ctx:
                page = ctx.new_page()
                
                # iframe URLに直接アクセス
                try:
                    page.goto(Constants.IFRAME_URL, wait_until="networkidle", timeout=Constants.PAGE_LOAD_TIMEOUT_MS)
                    page.wait_for_timeout(Constants.STABILITY_WAIT_MS)
                    prods = self._parser.parse_frame(page.main_frame, 0)
                    all_prods.extend(prods)
                except Exception as e:
                    self._logger.error(f"iframe直接アクセス失敗: {e}")
                
                # 失敗したらメインページ経由
                if not all_prods:
                    try:
                        page.goto(Constants.MAIN_URL, wait_until="networkidle", timeout=Constants.PAGE_LOAD_TIMEOUT_MS)
                        page.wait_for_timeout(Constants.STABILITY_WAIT_MS)
                        
                        iframes = page.frames
                        for iframe in iframes:
                            if 'cgi-bin' in iframe.url or 'shopping' in iframe.url:
                                page.wait_for_timeout(3000)
                                prods = self._parser.parse_frame(iframe, 0)
                                all_prods.extend(prods)
                                break
                    except Exception as e:
                        self._logger.error(f"メインページ経由失敗: {e}")
                
                page.close()
            
            dur = time.time() - start
            
            # master_controller形式で出力
            if all_prods:
                print("---URL_INDEX:0---")
                for p in all_prods:
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
    scraper = KeizCameraScraper(logger=logger)
    result = scraper.scrape()
    return result.exit_code.value

if __name__ == "__main__":
    sys.exit(main())
