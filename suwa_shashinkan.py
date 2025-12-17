#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
suwa_shashinkan.py - 諏訪写真機 エンタープライズスクレイパー v2.0

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Architecture Decision Record (ADR)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADR-001: URL順序をshop_config.jsonと一致させる
=========================================
- Status: ACCEPTED
- Date: 2025-11-21
- Context: 旧バージョンではURL順序がshop_config.jsonと逆だった
  
  【旧版（問題）】
  url_index: 0 → products (新着2P)
  url_index: 1 → page:2 (新着1P)
  
  【shop_config.json】
  url_index: 0 → page:2 (新着1P)
  url_index: 1 → products (新着2P)

- Decision: shop_config.jsonの順序に完全一致させる
- Consequences:
  + product_snapshots.jsonとサイト実態が一致
  + カテゴリ別の正確な変更検知

ADR-002: DOM安定化待機の改善
=========================================
- Status: ACCEPTED
- Context: 動的レンダリングによるDOM不安定性
- Decision: wait_for_selectorとwait_for_load_stateの組み合わせ
- Consequences: 安定したデータ取得

【SLI/SLO定義】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- SLI: 商品取得成功率
- SLO: 99.5% (30日間ローリング)
- Error Budget: 0.5%

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
from typing import (
    Any,
    Callable,
    Dict,
    Final,
    Generator,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    TypeVar,
)

from playwright.sync_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

# ============================================================================
# 型定義
# ============================================================================

T = TypeVar("T")


class LoggerProtocol(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...


# ============================================================================
# 定数定義
# ============================================================================

class Constants:
    """アプリケーション定数"""
    
    # 【重要修正】shop_config.jsonと順序を一致させる
    # shop_config.json:
    #   url_index: 0 → page:2 (新着1P)
    #   url_index: 1 → products (新着2P)
    TARGET_URLS: Final[Tuple[str, ...]] = (
        # url_index: 0 - 新着1P（shop_config.jsonのurl_index:0と一致）
        "https://suwashashinki.com/Products/index/page:2?maker=0&category=0&keyword=&limit=200&search_btn=",
        # url_index: 1 - 新着2P（shop_config.jsonのurl_index:1と一致）
        "https://suwashashinki.com/products?maker=0&category=0&keyword=&limit=200&search_btn=",
    )
    
    # Circuit Breaker
    CB_FAILURE_THRESHOLD: Final[int] = 5
    CB_RECOVERY_TIMEOUT_SECONDS: Final[float] = 30.0
    
    # Retry
    RETRY_MAX_ATTEMPTS: Final[int] = 3
    RETRY_BASE_DELAY_SECONDS: Final[float] = 1.0
    RETRY_MAX_DELAY_SECONDS: Final[float] = 30.0
    
    # Playwright
    PAGE_LOAD_TIMEOUT_MS: Final[int] = 30000
    ELEMENT_TIMEOUT_MS: Final[int] = 10000
    STABILITY_WAIT_MS: Final[int] = 2000
    
    # バリデーション
    MIN_VALID_PRICE: Final[int] = 100
    MAX_VALID_PRICE: Final[int] = 50_000_000
    MIN_PRODUCT_NAME_LENGTH: Final[int] = 3
    
    # セレクタ
    PRODUCT_CONTAINER_SELECTOR: Final[str] = "div.list_product.style_A ul li"
    PRODUCT_NAME_SELECTOR: Final[str] = "div.product_txt strong"
    PRODUCT_PRICE_SELECTOR: Final[str] = "p.price"


# ============================================================================
# Enums
# ============================================================================

class CircuitState(Enum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class ScraperExitCode(Enum):
    SUCCESS = 0
    PARTIAL_SUCCESS = 1
    FAILURE = 2
    CIRCUIT_OPEN = 3


# ============================================================================
# データクラス
# ============================================================================

@dataclass(frozen=True, slots=True)
class ProductData:
    """商品データ"""
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


# ============================================================================
# 例外
# ============================================================================

class ScraperException(Exception):
    def __init__(self, message: str, correlation_id: Optional[str] = None):
        self.correlation_id = correlation_id or str(uuid.uuid4())[:8]
        super().__init__(f"[{self.correlation_id}] {message}")


class CircuitOpenException(ScraperException):
    pass


class RetryExhaustedException(ScraperException):
    pass


# ============================================================================
# ロガー
# ============================================================================

class StructuredLogger:
    def __init__(self, name: str = "suwa_scraper", level: int = logging.INFO):
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


# ============================================================================
# Circuit Breaker
# ============================================================================

class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = Constants.CB_FAILURE_THRESHOLD,
        recovery_timeout: float = Constants.CB_RECOVERY_TIMEOUT_SECONDS,
        logger: Optional[LoggerProtocol] = None,
    ):
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


# ============================================================================
# Retry Policy
# ============================================================================

class RetryPolicy:
    def __init__(
        self,
        max_attempts: int = Constants.RETRY_MAX_ATTEMPTS,
        base_delay: float = Constants.RETRY_BASE_DELAY_SECONDS,
        max_delay: float = Constants.RETRY_MAX_DELAY_SECONDS,
        logger: Optional[LoggerProtocol] = None,
    ):
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


# ============================================================================
# バリデーター
# ============================================================================

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


# ============================================================================
# Playwright管理
# ============================================================================

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
            browser = playwright.chromium.launch(
                headless=self._headless,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            )
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


# ============================================================================
# HTMLパーサー
# ============================================================================

class SuwaHtmlParser:
    def __init__(self, validator: ProductValidator, logger: Optional[LoggerProtocol] = None):
        self._validator = validator
        self._logger = logger or StructuredLogger()
    
    def parse(self, page: Page, url_index: int) -> List[ProductData]:
        products: List[ProductData] = []
        seen: Set[str] = set()
        
        items = page.query_selector_all(Constants.PRODUCT_CONTAINER_SELECTOR)
        self._logger.debug(f"URL index {url_index}: {len(items)}個の商品要素検出")
        
        for rank, item in enumerate(items, start=1):
            try:
                # 商品名
                name_elem = item.query_selector(Constants.PRODUCT_NAME_SELECTOR)
                if not name_elem:
                    continue
                name_raw = name_elem.inner_text().strip()
                name = self._validator.validate_name(name_raw)
                if not name:
                    continue
                
                # 価格
                price_elem = item.query_selector(Constants.PRODUCT_PRICE_SELECTOR)
                if not price_elem:
                    continue
                price_text = price_elem.inner_text().strip()
                price = self._validator.validate_price(price_text)
                if price is None:
                    continue
                
                # 重複チェック
                key = f"{name}||{price}"
                if key in seen:
                    continue
                seen.add(key)
                
                products.append(ProductData.create(name=name, price=price, url_index=url_index, rank=rank))
                
            except Exception:
                continue
        
        return products


# ============================================================================
# メインスクレイパー
# ============================================================================

class SuwaShashinkanScraper:
    """諏訪写真機スクレイパー
    
    【重要修正】
    URL順序をshop_config.jsonと完全一致させた
    """
    
    def __init__(
        self,
        target_urls: Tuple[str, ...] = Constants.TARGET_URLS,
        circuit_breaker: Optional[CircuitBreaker] = None,
        retry_policy: Optional[RetryPolicy] = None,
        logger: Optional[LoggerProtocol] = None,
    ):
        self._target_urls = target_urls
        self._logger = logger or StructuredLogger()
        self._circuit_breaker = circuit_breaker or CircuitBreaker(logger=self._logger)
        self._retry_policy = retry_policy or RetryPolicy(logger=self._logger)
        self._validator = ProductValidator()
        self._parser = SuwaHtmlParser(self._validator, self._logger)
        self._playwright_manager = PlaywrightManager(logger=self._logger)
    
    def scrape(self) -> ScrapeResult:
        correlation_id = str(uuid.uuid4())[:8]
        self._logger.set_correlation_id(correlation_id)
        start_time = time.time()
        all_products: List[ProductData] = []
        
        self._logger.info(f"スクレイピング開始: {len(self._target_urls)} URLs")
        
        try:
            if not self._circuit_breaker.can_execute():
                return ScrapeResult(
                    success=False, products=[], error_message="Circuit Breaker is OPEN",
                    duration_seconds=time.time() - start_time, exit_code=ScraperExitCode.CIRCUIT_OPEN,
                    correlation_id=correlation_id,
                )
            
            with self._playwright_manager.browser_context() as context:
                page = context.new_page()
                
                for url_index, url in enumerate(self._target_urls):
                    # URL Index出力
                    print(f"---URL_INDEX:{url_index}---")
                    
                    try:
                        products = self._scrape_single_url(page, url, url_index)
                        all_products.extend(products)
                        
                        # 商品出力
                        for product in products:
                            print(product.to_output_line())
                            
                    except Exception as e:
                        self._logger.error(f"URL index {url_index} エラー: {e}")
                        continue
                
                page.close()
            
            duration = time.time() - start_time
            self._logger.info(f"スクレイピング完了: {len(all_products)}件取得 ({duration:.2f}秒)")
            
            return ScrapeResult(
                success=True, products=all_products, duration_seconds=duration,
                exit_code=ScraperExitCode.SUCCESS if all_products else ScraperExitCode.PARTIAL_SUCCESS,
                correlation_id=correlation_id,
            )
            
        except Exception as e:
            self._logger.error(f"予期せぬエラー: {e}")
            return ScrapeResult(
                success=False, products=all_products, error_message=str(e),
                duration_seconds=time.time() - start_time, exit_code=ScraperExitCode.FAILURE,
                correlation_id=correlation_id,
            )
    
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


# ============================================================================
# エントリーポイント
# ============================================================================

def main() -> int:
    logger = StructuredLogger(level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO))
    scraper = SuwaShashinkanScraper(logger=logger)
    result = scraper.scrape()
    
    if result.success and len(result.products) >= 20:
        print("SUCCESS")
    elif result.success and result.products:
        print("PARTIAL SUCCESS")
    else:
        print(f"ERROR: {result.error_message or 'Unknown error'}")
    
    return result.exit_code.value


if __name__ == "__main__":
    sys.exit(main())