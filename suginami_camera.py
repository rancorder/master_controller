#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
suginami_camera.py - 杉並カメラ エンタープライズスクレイパー v1.0

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Architecture Decision Record (ADR)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADR-001: Playwright必須（JS100個・動的レンダリング）
=========================================
- Status: ACCEPTED
- Context: QAレポートでJS100個・DOM動的変化あり
- Decision: Playwright採用、DOM安定化待機実装
- Consequences: suwa_shashinkan.pyパターン継承

【SLI/SLO定義】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- SLI: 商品取得成功率
- SLO: 99.5% (30日間ローリング)

【依存関係】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- playwright: ^1.40.0
"""

from __future__ import annotations

import hashlib
import logging
import os
import random
import re
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Final, Generator, List, Optional, Protocol, Tuple, TypeVar

from playwright.sync_api import Browser, BrowserContext, Page, Playwright, sync_playwright, TimeoutError as PlaywrightTimeoutError

T = TypeVar("T")

class LoggerProtocol(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...

class Constants:
    TARGET_URLS: Final[Tuple[str, ...]] = ("https://suginami-camera.jp/sales-page/",)
    CB_FAILURE_THRESHOLD: Final[int] = 5
    CB_RECOVERY_TIMEOUT_SECONDS: Final[float] = 30.0
    RETRY_MAX_ATTEMPTS: Final[int] = 3
    RETRY_BASE_DELAY_SECONDS: Final[float] = 1.0
    RETRY_MAX_DELAY_SECONDS: Final[float] = 30.0
    PAGE_LOAD_TIMEOUT_MS: Final[int] = 30000
    ELEMENT_TIMEOUT_MS: Final[int] = 10000
    STABILITY_WAIT_MS: Final[int] = 3000  # JS100個のため長めに設定
    MIN_VALID_PRICE: Final[int] = 100
    MAX_VALID_PRICE: Final[int] = 50_000_000
    MIN_PRODUCT_NAME_LENGTH: Final[int] = 3
    PRODUCT_CONTAINER_SELECTOR: Final[str] = ".product, .item, li, [class*='product']"

class CircuitState(Enum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()

class ScraperExitCode(Enum):
    SUCCESS = 0
    PARTIAL_SUCCESS = 1
    FAILURE = 2
    CIRCUIT_OPEN = 3

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
        return f"{self.name} {self.price}円"

@dataclass
class CircuitBreakerState:
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    half_open_call_count: int = 0

@dataclass
class ScrapeResult:
    success: bool
    products: List[ProductData]
    error_message: Optional[str] = None
    duration_seconds: float = 0.0
    exit_code: ScraperExitCode = ScraperExitCode.SUCCESS
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

class ScraperException(Exception):
    def __init__(self, message: str, correlation_id: Optional[str] = None):
        self.correlation_id = correlation_id or str(uuid.uuid4())[:8]
        super().__init__(f"[{self.correlation_id}] {message}")

class CircuitOpenException(ScraperException):
    pass

class RetryExhaustedException(ScraperException):
    pass

class StructuredLogger:
    def __init__(self, name: str = "suginami_camera_scraper", level: int = logging.INFO):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)
        self._correlation_id: Optional[str] = None
        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(level)
            handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
            self._logger.addHandler(handler)
    
    def set_correlation_id(self, correlation_id: str) -> None:
        self._correlation_id = correlation_id
    
    def _format(self, msg: str) -> str:
        return f"[{self._correlation_id}] {msg}" if self._correlation_id else msg
    
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.debug(self._format(msg), *args, **kwargs)
    
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.info(self._format(msg), *args, **kwargs)
    
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.warning(self._format(msg), *args, **kwargs)
    
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.error(self._format(msg), *args, **kwargs)

class CircuitBreaker:
    def __init__(self, failure_threshold: int = Constants.CB_FAILURE_THRESHOLD,
                 recovery_timeout: float = Constants.CB_RECOVERY_TIMEOUT_SECONDS,
                 logger: Optional[LoggerProtocol] = None):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = timedelta(seconds=recovery_timeout)
        self._logger = logger or StructuredLogger()
        self._state = CircuitBreakerState()
    
    def can_execute(self) -> bool:
        self._check_transition()
        return self._state.state != CircuitState.OPEN
    
    def _check_transition(self) -> None:
        if self._state.state == CircuitState.OPEN and self._state.last_failure_time:
            if datetime.now() - self._state.last_failure_time >= self._recovery_timeout:
                self._state.state = CircuitState.HALF_OPEN
                self._state.half_open_call_count = 0
                self._logger.info("Circuit Breaker: OPEN -> HALF_OPEN")
    
    def record_success(self) -> None:
        if self._state.state == CircuitState.HALF_OPEN:
            self._state.half_open_call_count += 1
            if self._state.half_open_call_count >= 3:
                self._state.state = CircuitState.CLOSED
                self._state.failure_count = 0
                self._logger.info("Circuit Breaker: HALF_OPEN -> CLOSED")
        elif self._state.state == CircuitState.CLOSED:
            self._state.failure_count = 0
    
    def record_failure(self) -> None:
        self._state.failure_count += 1
        self._state.last_failure_time = datetime.now()
        if self._state.state == CircuitState.HALF_OPEN or self._state.failure_count >= self._failure_threshold:
            self._state.state = CircuitState.OPEN
            self._logger.warning("Circuit Breaker: -> OPEN")
    
    @contextmanager
    def protect(self) -> Generator[None, None, None]:
        if not self.can_execute():
            raise CircuitOpenException("Circuit Breaker is OPEN")
        try:
            yield
            self.record_success()
        except Exception:
            self.record_failure()
            raise

class RetryPolicy:
    def __init__(self, max_attempts: int = Constants.RETRY_MAX_ATTEMPTS,
                 base_delay: float = Constants.RETRY_BASE_DELAY_SECONDS,
                 max_delay: float = Constants.RETRY_MAX_DELAY_SECONDS,
                 logger: Optional[LoggerProtocol] = None):
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._logger = logger or StructuredLogger()
    
    def execute_with_retry(self, operation: Callable[[], T], operation_name: str = "operation") -> T:
        last_exception: Optional[Exception] = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                return operation()
            except Exception as e:
                last_exception = e
                self._logger.warning(f"{operation_name}: 失敗 (attempt {attempt}/{self._max_attempts})")
                if attempt < self._max_attempts:
                    delay = min(self._base_delay * (2 ** (attempt - 1)), self._max_delay)
                    delay += random.uniform(-delay * 0.25, delay * 0.25)
                    time.sleep(max(0.1, delay))
        raise RetryExhaustedException(f"{operation_name}: リトライ失敗") from last_exception

class ProductValidator:
    @staticmethod
    def validate_price(price_text: str) -> Optional[int]:
        match = re.search(r"([0-9,]+)円", price_text)
        if not match:
            return None
        try:
            price = int(match.group(1).replace(",", ""))
            if Constants.MIN_VALID_PRICE <= price <= Constants.MAX_VALID_PRICE:
                return price
        except ValueError:
            pass
        return None
    
    @staticmethod
    def validate_name(name: str) -> Optional[str]:
        name = re.sub(r"\s+", " ", name).strip()
        return name if len(name) >= Constants.MIN_PRODUCT_NAME_LENGTH else None

class PlaywrightManager:
    def __init__(self, headless: bool = True, logger: Optional[LoggerProtocol] = None):
        self._headless = headless
        self._logger = logger or StructuredLogger()
    
    @contextmanager
    def browser_context(self) -> Generator[BrowserContext, None, None]:
        playwright: Optional[Playwright] = None
        browser: Optional[Browser] = None
        context: Optional[BrowserContext] = None
        try:
            playwright = sync_playwright().start()
            browser = playwright.chromium.launch(headless=self._headless,
                                                 args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"])
            context = browser.new_context(viewport={"width": 1920, "height": 1080},
                                         user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")
            yield context
        finally:
            for resource in [context, browser]:
                if resource:
                    try:
                        resource.close()
                    except Exception:
                        pass
            if playwright:
                try:
                    playwright.stop()
                except Exception:
                    pass

class SuginamiCameraParser:
    def __init__(self, validator: ProductValidator, logger: Optional[LoggerProtocol] = None):
        self._validator = validator
        self._logger = logger or StructuredLogger()
    
    def parse(self, page: Page, url_index: int) -> List[ProductData]:
        products: List[ProductData] = []
        seen_hashes: set = set()
        
        # li.product要素を取得
        items = page.query_selector_all('li.product')
        self._logger.info(f"セレクタ 'li.product' で {len(items)}個の要素検出")
        
        if not items:
            self._logger.warning("商品要素が0件")
            return products
        
        for idx, item in enumerate(items):
            try:
                # デバッグ：最初の2件の完全なHTMLを出力
                if idx < 2:
                    item_html = item.inner_html()
                    self._logger.info(f"\n{'='*60}\n商品{idx+1}の完全HTML:\n{item_html}\n{'='*60}")
                
                # SOLD OUT商品をスキップ
                item_class = item.get_attribute('class') or ''
                if 'outofstock' in item_class:
                    self._logger.debug(f"商品{idx+1}: SOLD OUT (クラス)")
                    continue
                
                # 商品名取得を複数パターンで試行
                name_elem = (
                    item.query_selector('h2.woocommerce-loop-product__title') or
                    item.query_selector('h2') or
                    item.query_selector('.product-title') or
                    item.query_selector('a[class*="title"]')
                )
                
                if not name_elem:
                    self._logger.warning(f"商品{idx+1}: 商品名要素が見つかりません")
                    # imgのalt属性から取得を試行
                    img_elem = item.query_selector('img')
                    if img_elem:
                        alt_text = img_elem.get_attribute('alt')
                        if alt_text:
                            name = self._validator.validate_name(alt_text)
                            if name:
                                self._logger.info(f"商品{idx+1}: alt属性から商品名取得: {name[:50]}")
                            else:
                                continue
                        else:
                            continue
                    else:
                        continue
                else:
                    name_raw = name_elem.inner_text()
                    name = self._validator.validate_name(name_raw)
                    if not name:
                        self._logger.debug(f"商品{idx+1}: 商品名バリデーション失敗")
                        continue
                    self._logger.info(f"商品{idx+1}: 商品名={name[:50]}")
                
                # 価格取得を複数パターンで試行
                price_elem = (
                    item.query_selector('span.price bdi') or
                    item.query_selector('span.woocommerce-Price-amount bdi') or
                    item.query_selector('bdi') or
                    item.query_selector('span.price') or
                    item.query_selector('.price')
                )
                
                if not price_elem:
                    self._logger.warning(f"商品{idx+1}: 価格要素が見つかりません")
                    continue
                
                price_text = price_elem.inner_text().strip()
                self._logger.info(f"商品{idx+1}: 価格テキスト={price_text}")
                
                # 価格抽出: カンマを除去してから数字のみ抽出
                price_clean = re.sub(r'[^\d]', '', price_text)  # 数字のみ
                
                if not price_clean:
                    self._logger.warning(f"商品{idx+1}: 価格が数字を含んでいません")
                    continue
                
                try:
                    price = int(price_clean)
                    # バリデーション
                    if not (Constants.MIN_VALID_PRICE <= price <= Constants.MAX_VALID_PRICE):
                        self._logger.warning(f"商品{idx+1}: 価格が範囲外 ({price}円)")
                        continue
                except ValueError:
                    self._logger.warning(f"商品{idx+1}: 価格の数値変換失敗 ({price_clean})")
                    continue
                
                # 重複チェック
                product_hash = hashlib.md5(f"{name}_{price}".encode("utf-8")).hexdigest()[:8]
                if product_hash in seen_hashes:
                    continue
                seen_hashes.add(product_hash)
                
                self._logger.info(f"✓ 商品{idx+1}: 追加成功 - {name[:30]}... {price}円")
                products.append(ProductData.create(name=name, price=price, url_index=url_index, rank=len(products)+1))
                
            except Exception as e:
                self._logger.error(f"商品{idx+1}パース失敗: {e}", exc_info=True)
                continue
        
        return products

class SuginamiCameraScraper:
    def __init__(self, target_urls: Tuple[str, ...] = Constants.TARGET_URLS,
                 circuit_breaker: Optional[CircuitBreaker] = None,
                 retry_policy: Optional[RetryPolicy] = None,
                 logger: Optional[LoggerProtocol] = None):
        self._target_urls = target_urls
        self._logger = logger or StructuredLogger()
        self._circuit_breaker = circuit_breaker or CircuitBreaker(logger=self._logger)
        self._retry_policy = retry_policy or RetryPolicy(logger=self._logger)
        self._validator = ProductValidator()
        self._parser = SuginamiCameraParser(self._validator, self._logger)
        self._playwright_manager = PlaywrightManager(logger=self._logger)
    
    def scrape(self) -> ScrapeResult:
        correlation_id = str(uuid.uuid4())[:8]
        self._logger.set_correlation_id(correlation_id)
        start_time = time.time()
        all_products: List[ProductData] = []
        self._logger.info(f"スクレイピング開始: {len(self._target_urls)} URLs")
        
        try:
            if not self._circuit_breaker.can_execute():
                return ScrapeResult(success=False, products=[], error_message="Circuit Breaker is OPEN",
                                  duration_seconds=time.time() - start_time, exit_code=ScraperExitCode.CIRCUIT_OPEN,
                                  correlation_id=correlation_id)
            
            with self._playwright_manager.browser_context() as context:
                page = context.new_page()
                for url_index, url in enumerate(self._target_urls):
                    print(f"---URL_INDEX:{url_index}---")
                    try:
                        products = self._scrape_single_url(page, url, url_index)
                        all_products.extend(products)
                        for product in products:
                            print(product.to_output_line())
                    except Exception as e:
                        self._logger.error(f"URL index {url_index} エラー: {e}")
                        continue
                page.close()
            
            duration = time.time() - start_time
            self._logger.info(f"スクレイピング完了: {len(all_products)}件取得 ({duration:.2f}秒)")
            return ScrapeResult(success=True, products=all_products, duration_seconds=duration,
                              exit_code=ScraperExitCode.SUCCESS if all_products else ScraperExitCode.PARTIAL_SUCCESS,
                              correlation_id=correlation_id)
        except Exception as e:
            self._logger.error(f"予期せぬエラー: {e}")
            return ScrapeResult(success=False, products=all_products, error_message=str(e),
                              duration_seconds=time.time() - start_time, exit_code=ScraperExitCode.FAILURE,
                              correlation_id=correlation_id)
    
    def _scrape_single_url(self, page: Page, url: str, url_index: int) -> List[ProductData]:
        def _scrape() -> List[ProductData]:
            with self._circuit_breaker.protect():
                page.goto(url, wait_until="domcontentloaded", timeout=Constants.PAGE_LOAD_TIMEOUT_MS)
                try:
                    page.wait_for_selector(Constants.PRODUCT_CONTAINER_SELECTOR, timeout=Constants.ELEMENT_TIMEOUT_MS)
                except PlaywrightTimeoutError:
                    self._logger.warning(f"URL index {url_index}: セレクタ待機タイムアウト")
                page.wait_for_timeout(Constants.STABILITY_WAIT_MS)
                return self._parser.parse(page, url_index)
        return self._retry_policy.execute_with_retry(_scrape, f"scrape_url_{url_index}")

def main() -> int:
    logger = StructuredLogger(level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO))
    scraper = SuginamiCameraScraper(logger=logger)
    result = scraper.scrape()
    if result.success and len(result.products) >= 10:
        print("SUCCESS")
    elif result.success and result.products:
        print("PARTIAL SUCCESS")
    else:
        print(f"ERROR: {result.error_message or 'Unknown error'}")
    return result.exit_code.value

if __name__ == "__main__":
    sys.exit(main())
