#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hardoff.py - HardOff エンタープライズスクレイパー v3.0

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Architecture Decision Record (ADR)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADR-001: 商品コード明示的抽出
=========================================
- Status: MAINTAINED (v2からの継続)
- Context: 商品コードをmaster_controllerの正規化ロジックに対応
- Decision: [CODE]形式で明示的に出力
- Consequences: 商品識別の精度向上

ADR-002: URL順序確認
=========================================
- Status: VERIFIED
- Context: shop_config.json確認結果
  url_index: 0 → フィルムカメラ
  url_index: 1 → コンパクトデジカメ
  url_index: 2 → レンズ
  url_index: 3 → アクセサリ
  url_index: 4 → 双眼鏡
  url_index: 5 → 時計新着
- Decision: 現状維持（正しい順序）

【SLI/SLO定義】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- SLI: 商品取得成功率
- SLO: 99.5% (Priority 1スクリプト)

【依存関係】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- playwright: ^1.40.0
- beautifulsoup4: ^4.12.0
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
    Tuple,
    TypeVar,
)

from bs4 import BeautifulSoup, Tag
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
    
    # 【shop_config.json順序確認済み】
    TARGET_URLS: Final[Tuple[str, ...]] = (
        "https://netmall.hardoff.co.jp/cate/0001000300020002/",      # url_index: 0 - フィルムカメラ
        "https://netmall.hardoff.co.jp/cate/00010003000200010001/",  # url_index: 1 - コンパクトデジカメ
        "https://netmall.hardoff.co.jp/cate/000100030001/",          # url_index: 2 - レンズ
        "https://netmall.hardoff.co.jp/cate/000100030003/",          # url_index: 3 - アクセサリ
        "https://netmall.hardoff.co.jp/cate/000100030005/",          # url_index: 4 - 双眼鏡
        "https://netmall.hardoff.co.jp/cate/00010004000100010003/",  # url_index: 5 - 時計新着
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
    STABILITY_WAIT_MS: Final[int] = 3000
    
    # バリデーション
    MIN_VALID_PRICE: Final[int] = 100
    MAX_VALID_PRICE: Final[int] = 50_000_000
    MIN_PRODUCT_NAME_LENGTH: Final[int] = 3
    
    # セレクタ
    PRODUCT_CONTAINER_SELECTOR: Final[str] = ".item-infowrap"
    PRODUCT_NAME_SELECTOR: Final[str] = ".item-name"
    PRODUCT_BRAND_SELECTOR: Final[str] = ".item-brand-name"
    PRODUCT_CODE_SELECTOR: Final[str] = ".item-code"
    PRODUCT_PRICE_SELECTOR: Final[str] = ".item-price-en"


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
    code: str = ""
    product_hash: str = ""
    rank: int = 0
    
    @classmethod
    def create(
        cls, name: str, price: int, url_index: int, code: str = "", rank: int = 0
    ) -> ProductData:
        product_hash = hashlib.md5(f"{name}_{price}".encode("utf-8")).hexdigest()[:8]
        return cls(
            name=name, price=price, url_index=url_index, code=code,
            product_hash=product_hash, rank=rank
        )
    
    def to_output_line(self) -> str:
        """商品コードを[CODE]形式で明示的出力"""
        if self.code:
            return f"{self.name} [{self.code}] {self.price}円"
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
    def __init__(self, name: str = "hardoff_scraper", level: int = logging.INFO):
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
        cleaned = price_text.replace(",", "").replace("円", "").strip()
        try:
            price = int(cleaned)
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

class HardoffHtmlParser:
    """HardOff HTML解析器"""
    
    def __init__(self, validator: ProductValidator, logger: Optional[LoggerProtocol] = None):
        self._validator = validator
        self._logger = logger or StructuredLogger()
    
    def parse(self, html: str, url_index: int) -> List[ProductData]:
        soup = BeautifulSoup(html, "html.parser")
        products: List[ProductData] = []
        
        items = soup.select(Constants.PRODUCT_CONTAINER_SELECTOR)
        self._logger.debug(f"URL index {url_index}: {len(items)}個の商品コンテナ検出")
        
        for rank, item in enumerate(items, start=1):
            product = self._parse_item(item, url_index, rank)
            if product:
                products.append(product)
        
        return products
    
    def _parse_item(self, item: Tag, url_index: int, rank: int) -> Optional[ProductData]:
        try:
            # 商品名
            name_elem = item.select_one(Constants.PRODUCT_NAME_SELECTOR)
            if not name_elem:
                return None
            name_raw = name_elem.get_text(strip=True)
            
            # ブランド名
            brand_elem = item.select_one(Constants.PRODUCT_BRAND_SELECTOR)
            brand = brand_elem.get_text(strip=True) if brand_elem else ""
            
            # 商品コード
            code_elem = item.select_one(Constants.PRODUCT_CODE_SELECTOR)
            code = code_elem.get_text(strip=True) if code_elem else ""
            
            # フルネーム構築
            full_name = f"{brand} {name_raw}".strip()
            name = self._validator.validate_name(full_name)
            if not name:
                return None
            
            # 価格
            price_elem = item.select_one(Constants.PRODUCT_PRICE_SELECTOR)
            if not price_elem:
                return None
            price_text = price_elem.get_text(strip=True)
            price = self._validator.validate_price(price_text)
            if price is None:
                return None
            
            return ProductData.create(
                name=name, price=price, url_index=url_index, code=code, rank=rank
            )
            
        except Exception:
            return None


# ============================================================================
# メインスクレイパー
# ============================================================================

class HardoffScraper:
    """HardOffスクレイパー"""
    
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
        self._parser = HardoffHtmlParser(self._validator, self._logger)
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
                        
                        self._logger.info(f"URL index {url_index}: {len(products)}件取得")
                        
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
                page.goto(url, timeout=Constants.PAGE_LOAD_TIMEOUT_MS)
                page.wait_for_timeout(Constants.STABILITY_WAIT_MS)
                html = page.content()
                return self._parser.parse(html, url_index)
        
        return self._retry_policy.execute_with_retry(_scrape, f"scrape_url_{url_index}")


# ============================================================================
# エントリーポイント
# ============================================================================

def main() -> int:
    logger = StructuredLogger(level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO))
    scraper = HardoffScraper(logger=logger)
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