#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hardoff.py - HardOff エンタープライズスクレイパー v4.1

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Architecture Decision Record (ADR)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADR-004: master_controller出力形式完全互換対応 (v4.1)
=========================================
- Status: IMPLEMENTED (2025-01-XX)
- Context: master_controller.py StableDataExtractor が2要素形式のみサポート
- Problem: v4.0の3要素形式（商品名||商品URL||画像URL）が非互換
- Decision: 2要素形式（商品名||画像URL）に変更
- Consequences: 
  - ✅ 既存システムと完全互換
  - ✅ 画像URL通知対応
  - ❌ 商品URLは内部保持のみ（出力には含めない）

【v4.0 → v4.1変更点】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ CRITICAL: 出力形式を2要素に修正（master_controller互換）
✅ ProductData.to_output_line() 修正
✅ 互換性テスト追加
✅ ドキュメント更新

実行方法:
    python hardoff.py

必要なライブラリ:
    pip install playwright beautifulsoup4
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
import sys
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
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
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
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
    
    BASE_URL: Final[str] = "https://netmall.hardoff.co.jp"
    
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
    MAX_CONCURRENT_PAGES: Final[int] = 3
    
    # バリデーション
    MIN_VALID_PRICE: Final[int] = 100
    MAX_VALID_PRICE: Final[int] = 50_000_000
    MIN_PRODUCT_NAME_LENGTH: Final[int] = 3
    
    # セレクタ
    PRODUCT_LINK_SELECTOR: Final[str] = "a:has(.item-infowrap)"
    PRODUCT_CONTAINER_SELECTOR: Final[str] = ".item-infowrap"
    PRODUCT_NAME_SELECTOR: Final[str] = ".item-name"
    PRODUCT_BRAND_SELECTOR: Final[str] = ".item-brand-name"
    PRODUCT_CODE_SELECTOR: Final[str] = ".item-code"
    PRODUCT_PRICE_SELECTOR: Final[str] = ".item-price-en"
    PRODUCT_IMAGE_SELECTOR: Final[str] = ".item-img-square img"


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
    """
    商品データ（v4.1: master_controller完全互換版）
    
    重要な設計判断:
    - product_url は内部で保持するが、to_output_line() では出力しない
    - master_controller.py が2要素形式のみサポートのため
    - 出力形式: "商品名 [コード] 価格||画像URL"
    """
    name: str
    price: int
    url_index: int
    product_url: str
    code: str = ""
    product_hash: str = ""
    rank: int = 0
    img_url: str = ""
    
    @classmethod
    def create(
        cls,
        name: str,
        price: int,
        url_index: int,
        product_url: str,
        code: str = "",
        rank: int = 0,
        img_url: str = "",
    ) -> ProductData:
        product_hash = hashlib.md5(f"{name}_{price}".encode("utf-8")).hexdigest()[:8]
        return cls(
            name=name,
            price=price,
            url_index=url_index,
            product_url=product_url,
            code=code,
            product_hash=product_hash,
            rank=rank,
            img_url=img_url,
        )
    
    def to_output_line(self) -> str:
        """
        master_controller完全互換形式出力（v4.1修正版）
        
        形式: 商品名 [コード] 価格||画像URL
        
        【重要】
        - 2要素形式（master_controller.py StableDataExtractor互換）
        - 商品URLは出力に含めない（内部保持のみ）
        - 画像URLのみ || の後に配置
        
        例:
        CASIO エディフィス [ECB-950] 22000円||https://p1-d9ebd2ee.imageflux.jp/.../IMG.jpg
        """
        base = f"{self.name}"
        if self.code:
            base += f" [{self.code}]"
        base += f" {self.price}円"
        
        # v4.1修正: 画像URLのみ出力（商品URLは含めない）
        if self.img_url:
            base += f"||{self.img_url}"
        
        return base


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
    url_results: Dict[int, bool] = field(default_factory=dict)
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
# Circuit Breaker（URL単位管理）
# ============================================================================

class CircuitBreaker:
    """URL単位Circuit Breaker"""
    
    def __init__(
        self,
        failure_threshold: int = Constants.CB_FAILURE_THRESHOLD,
        recovery_timeout: float = Constants.CB_RECOVERY_TIMEOUT_SECONDS,
        logger: Optional[LoggerProtocol] = None,
    ):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = timedelta(seconds=recovery_timeout)
        self._logger = logger or StructuredLogger()
        self._states: Dict[str, CircuitBreakerState] = {}
    
    def _get_state(self, url_key: str) -> CircuitBreakerState:
        if url_key not in self._states:
            self._states[url_key] = CircuitBreakerState()
        return self._states[url_key]
    
    def can_execute(self, url_key: str) -> bool:
        state = self._get_state(url_key)
        self._check_transition(state, url_key)
        return state.state != CircuitState.OPEN
    
    def _check_transition(self, state: CircuitBreakerState, url_key: str) -> None:
        if state.state == CircuitState.OPEN and state.last_failure_time:
            if datetime.now() - state.last_failure_time >= self._recovery_timeout:
                state.state = CircuitState.HALF_OPEN
                state.half_open_call_count = 0
                self._logger.info(f"Circuit Breaker [{url_key}]: OPEN -> HALF_OPEN")
    
    def record_success(self, url_key: str) -> None:
        state = self._get_state(url_key)
        if state.state == CircuitState.HALF_OPEN:
            state.half_open_call_count += 1
            if state.half_open_call_count >= 3:
                state.state = CircuitState.CLOSED
                state.failure_count = 0
                self._logger.info(f"Circuit Breaker [{url_key}]: HALF_OPEN -> CLOSED")
        elif state.state == CircuitState.CLOSED:
            state.failure_count = 0
    
    def record_failure(self, url_key: str) -> None:
        state = self._get_state(url_key)
        state.failure_count += 1
        state.last_failure_time = datetime.now()
        
        if state.state == CircuitState.HALF_OPEN or state.failure_count >= self._failure_threshold:
            state.state = CircuitState.OPEN
            self._logger.warning(f"Circuit Breaker [{url_key}]: -> OPEN")
    
    @contextmanager
    def protect(self, url_key: str) -> Generator[None, None, None]:
        if not self.can_execute(url_key):
            raise CircuitOpenException(f"Circuit Breaker is OPEN for {url_key}")
        try:
            yield
            self.record_success(url_key)
        except Exception:
            self.record_failure(url_key)
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
    
    async def execute_with_retry_async(
        self, operation: Callable[[], T], operation_name: str = "operation"
    ) -> T:
        last_exception: Optional[Exception] = None
        
        for attempt in range(1, self._max_attempts + 1):
            try:
                return await operation()
            except Exception as e:
                last_exception = e
                self._logger.warning(f"{operation_name}: 失敗 (attempt {attempt}/{self._max_attempts})")
                
                if attempt < self._max_attempts:
                    delay = min(self._base_delay * (2 ** (attempt - 1)), self._max_delay)
                    delay += random.uniform(-delay * 0.25, delay * 0.25)
                    await asyncio.sleep(max(0.1, delay))
        
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
    
    @staticmethod
    def normalize_url(url: str, base_url: str = Constants.BASE_URL) -> str:
        """相対URLを絶対URLに変換"""
        if not url:
            return ""
        if url.startswith("http"):
            return url
        if url.startswith("/"):
            return f"{base_url}{url}"
        return url


# ============================================================================
# Playwright管理
# ============================================================================

class PlaywrightManager:
    def __init__(self, headless: bool = True, logger: Optional[LoggerProtocol] = None):
        self._headless = headless
        self._logger = logger or StructuredLogger()
    
    @asynccontextmanager
    async def browser_context(self) -> BrowserContext:
        playwright: Optional[Playwright] = None
        browser: Optional[Browser] = None
        context: Optional[BrowserContext] = None
        
        try:
            playwright = await async_playwright().start()
            browser = await playwright.chromium.launch(
                headless=self._headless,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
            )
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            )
            yield context
        finally:
            for resource in [context, browser]:
                if resource:
                    try:
                        await resource.close()
                    except Exception:
                        pass
            if playwright:
                try:
                    await playwright.stop()
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
        
        items = soup.select(Constants.PRODUCT_LINK_SELECTOR)
        self._logger.debug(f"URL index {url_index}: {len(items)}個の商品リンク検出")
        
        for rank, item in enumerate(items, start=1):
            product = self._parse_item(item, url_index, rank)
            if product:
                products.append(product)
        
        return products
    
    def _parse_item(self, link_elem: Tag, url_index: int, rank: int) -> Optional[ProductData]:
        try:
            # 商品URL取得
            product_url = link_elem.get("href", "")
            product_url = self._validator.normalize_url(product_url)
            
            # 商品情報コンテナ取得
            container = link_elem.select_one(Constants.PRODUCT_CONTAINER_SELECTOR)
            if not container:
                return None
            
            # 商品名
            name_elem = container.select_one(Constants.PRODUCT_NAME_SELECTOR)
            if not name_elem:
                return None
            name_raw = name_elem.get_text(strip=True)
            
            # ブランド名
            brand_elem = container.select_one(Constants.PRODUCT_BRAND_SELECTOR)
            brand = brand_elem.get_text(strip=True) if brand_elem else ""
            
            # 商品コード
            code_elem = container.select_one(Constants.PRODUCT_CODE_SELECTOR)
            code = code_elem.get_text(strip=True) if code_elem else ""
            
            # フルネーム構築
            full_name = f"{brand} {name_raw}".strip()
            name = self._validator.validate_name(full_name)
            if not name:
                return None
            
            # 価格
            price_elem = container.select_one(Constants.PRODUCT_PRICE_SELECTOR)
            if not price_elem:
                return None
            price_text = price_elem.get_text(strip=True)
            price = self._validator.validate_price(price_text)
            if price is None:
                return None
            
            # 画像URL取得
            img_elem = link_elem.select_one(Constants.PRODUCT_IMAGE_SELECTOR)
            img_url = ""
            if img_elem:
                img_url = img_elem.get("src", "")
                if not img_url:
                    img_url = img_elem.get("data-src", "")
                img_url = self._validator.normalize_url(img_url, "")
            
            return ProductData.create(
                name=name,
                price=price,
                url_index=url_index,
                product_url=product_url,
                code=code,
                rank=rank,
                img_url=img_url,
            )
            
        except Exception as e:
            self._logger.debug(f"商品パースエラー (rank={rank}): {e}")
            return None


# ============================================================================
# メインスクレイパー
# ============================================================================

class HardoffScraper:
    """HardOffスクレイパー（非同期並列版）"""
    
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
    
    async def scrape_async(self) -> ScrapeResult:
        """非同期スクレイピング実行"""
        correlation_id = str(uuid.uuid4())[:8]
        self._logger.set_correlation_id(correlation_id)
        start_time = time.time()
        
        self._logger.info(f"スクレイピング開始: {len(self._target_urls)} URLs（並列実行）")
        
        try:
            async with self._playwright_manager.browser_context() as context:
                semaphore = asyncio.Semaphore(Constants.MAX_CONCURRENT_PAGES)
                tasks = [
                    self._scrape_with_semaphore(context, url, idx, semaphore)
                    for idx, url in enumerate(self._target_urls)
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 結果集約
            all_products: List[ProductData] = []
            url_results: Dict[int, bool] = {}
            
            for idx, result in enumerate(results):
                if isinstance(result, Exception):
                    self._logger.error(f"URL index {idx} 致命的エラー: {result}")
                    url_results[idx] = False
                else:
                    all_products.extend(result)
                    url_results[idx] = True
            
            duration = time.time() - start_time
            self._logger.info(
                f"スクレイピング完了: {len(all_products)}件取得 "
                f"({duration:.2f}秒, 成功URL: {sum(url_results.values())}/{len(self._target_urls)})"
            )
            
            # 結果出力（master_controller統合形式）
            self._output_results(all_products)
            
            # 成功判定
            success = len(all_products) > 0
            exit_code = self._determine_exit_code(all_products, url_results)
            
            return ScrapeResult(
                success=success,
                products=all_products,
                url_results=url_results,
                duration_seconds=duration,
                exit_code=exit_code,
                correlation_id=correlation_id,
            )
            
        except Exception as e:
            self._logger.error(f"予期せぬエラー: {e}", exc_info=True)
            return ScrapeResult(
                success=False,
                products=[],
                error_message=str(e),
                duration_seconds=time.time() - start_time,
                exit_code=ScraperExitCode.FAILURE,
                correlation_id=correlation_id,
            )
    
    async def _scrape_with_semaphore(
        self, context: BrowserContext, url: str, url_index: int, semaphore: asyncio.Semaphore
    ) -> List[ProductData]:
        """セマフォで並列数制限してスクレイピング"""
        async with semaphore:
            return await self._scrape_single_url_async(context, url, url_index)
    
    async def _scrape_single_url_async(
        self, context: BrowserContext, url: str, url_index: int
    ) -> List[ProductData]:
        """単一URLスクレイピング"""
        url_key = f"url_{url_index}"
        
        async def _scrape() -> List[ProductData]:
            with self._circuit_breaker.protect(url_key):
                page = await context.new_page()
                try:
                    await page.goto(url, timeout=Constants.PAGE_LOAD_TIMEOUT_MS)
                    await page.wait_for_timeout(Constants.STABILITY_WAIT_MS)
                    html = await page.content()
                    return self._parser.parse(html, url_index)
                finally:
                    await page.close()
        
        return await self._retry_policy.execute_with_retry_async(_scrape, f"scrape_url_{url_index}")
    
    def _output_results(self, products: List[ProductData]) -> None:
        """結果出力（master_controller統合形式）"""
        by_index: Dict[int, List[ProductData]] = {}
        for product in products:
            if product.url_index not in by_index:
                by_index[product.url_index] = []
            by_index[product.url_index].append(product)
        
        for url_index in sorted(by_index.keys()):
            print(f"---URL_INDEX:{url_index}---")
            for product in by_index[url_index]:
                print(product.to_output_line())
    
    def _determine_exit_code(
        self, products: List[ProductData], url_results: Dict[int, bool]
    ) -> ScraperExitCode:
        """終了コード判定"""
        if not products:
            return ScraperExitCode.FAILURE
        
        successful_urls = sum(url_results.values())
        if successful_urls == len(self._target_urls) and len(products) >= 20:
            return ScraperExitCode.SUCCESS
        elif products:
            return ScraperExitCode.PARTIAL_SUCCESS
        else:
            return ScraperExitCode.FAILURE
    
    def scrape(self) -> ScrapeResult:
        """同期ラッパー（後方互換性）"""
        return asyncio.run(self.scrape_async())


# ============================================================================
# エントリーポイント
# ============================================================================

def main() -> int:
    log_level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logger = StructuredLogger(level=log_level)
    
    scraper = HardoffScraper(logger=logger)
    result = scraper.scrape()
    
    # 最終ステータス出力
    if result.exit_code == ScraperExitCode.SUCCESS:
        print("SUCCESS")
    elif result.exit_code == ScraperExitCode.PARTIAL_SUCCESS:
        print("PARTIAL SUCCESS")
    else:
        print(f"ERROR: {result.error_message or 'Unknown error'}")
    
    return result.exit_code.value


if __name__ == "__main__":
    sys.exit(main())