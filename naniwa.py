#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
naniwa.py - カメラのナニワ 値下げ検知対応スクレイパー v3.0

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Architecture Decision Record (ADR)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADR-001: 値下げページの監視ロジック
=========================================
- Status: ACCEPTED
- Date: 2025-11-21
- Context: 値下げページは「新商品出現」ではなく「価格変動」を検知すべき

【HTML構造】
- 通常価格のみ: <div class="price_">￥14,800</div>
- 値下げ中:
    <div class="price_before_">￥14,800</div>
    <div class="price_sale_">￥13,000</div>

【検知パターン】
パターン1: price_sale なし → price_sale 出現
  → 通知「値下げ！¥14,800 → ¥13,000」

パターン2: price_sale あり → price_sale 価格変動
  → 通知「さらに値下げ！¥13,000 → ¥12,000」

- Decision: 1位商品の価格状態（通常/セール）と価格を監視
- Consequences:
  + 値下げタイミングを正確に検知
  + 価格変動も追跡可能

ADR-002: URL構成
=========================================
- Status: UPDATED
- Context: 値下げページを2URLに分割
  url_index: 0 → e16042201 (値下げページ1)
  url_index: 1 → e16042201_p2 (値下げページ2)
  url_index: 2 → ezaiko (新着ページ) ※従来通り

【SLI/SLO定義】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- SLI: 値下げ検知成功率
- SLO: 99.5%

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
    Set,
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
    
    # 【値下げページ2URL + 新着ページ1URL】
    TARGET_URLS: Final[Tuple[Tuple[str, str], ...]] = (
        ("値下げ1P", "https://cameranonaniwa.jp/shop/e/e16042201/"),      # url_index: 0
        ("値下げ2P", "https://cameranonaniwa.jp/shop/e/e16042201_p2/"),   # url_index: 1
        ("新着", "https://cameranonaniwa.jp/shop/e/ezaiko/"),              # url_index: 2
    )
    
    # Circuit Breaker
    CB_FAILURE_THRESHOLD: Final[int] = 5
    CB_RECOVERY_TIMEOUT_SECONDS: Final[float] = 30.0
    
    # Retry
    RETRY_MAX_ATTEMPTS: Final[int] = 3
    RETRY_BASE_DELAY_SECONDS: Final[float] = 1.0
    RETRY_MAX_DELAY_SECONDS: Final[float] = 30.0
    
    # Playwright
    PAGE_LOAD_TIMEOUT_MS: Final[int] = 60000
    ELEMENT_TIMEOUT_MS: Final[int] = 10000
    STABILITY_WAIT_MS: Final[int] = 3000
    
    # バリデーション
    MIN_VALID_PRICE: Final[int] = 100
    MAX_VALID_PRICE: Final[int] = 50_000_000
    MIN_PRODUCT_NAME_LENGTH: Final[int] = 5
    
    # セレクタ（画像のHTML構造に基づく）
    # 注意: クラス名末尾にアンダースコアあり
    PRODUCT_BLOCK_SELECTOR: Final[str] = "div[class*='tile_item']"
    PRODUCT_BLOCK_SELECTOR_ALT: Final[str] = "div[class*='Item_tile']"
    
    # 価格セレクタ（画像のHTML構造に基づく）
    PRICE_SALE_SELECTOR: Final[str] = "div[class*='price_sale']"
    PRICE_BEFORE_SELECTOR: Final[str] = "div[class*='price_before']"
    PRICE_NORMAL_SELECTOR: Final[str] = "div.price_"
    PRICE_NORMAL_SELECTOR_ALT: Final[str] = "div[class*='price_']"
    
    # 商品名セレクタ
    PRODUCT_NAME_SELECTOR: Final[str] = "div[class*='name_']"
    PRODUCT_NAME1_SELECTOR: Final[str] = "div[class*='name1_']"


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


class PriceType(Enum):
    """価格タイプ"""
    NORMAL = "normal"      # 通常価格（値下げなし）
    SALE = "sale"          # セール価格あり


# ============================================================================
# データクラス
# ============================================================================

@dataclass(frozen=True, slots=True)
class PriceInfo:
    """価格情報"""
    price_type: PriceType
    current_price: int           # 現在の実売価格（セール価格 or 通常価格）
    original_price: Optional[int] = None  # 元値（セール時のみ）
    
    def to_price_string(self) -> str:
        """価格文字列を生成"""
        if self.price_type == PriceType.SALE and self.original_price:
            return f"¥{self.original_price:,} → ¥{self.current_price:,}"
        return f"¥{self.current_price:,}"
    
    @property
    def monitoring_price(self) -> int:
        """監視対象価格（master_controller用）"""
        return self.current_price


@dataclass(frozen=True, slots=True)
class ProductData:
    """商品データ（値下げ検知対応版）"""
    name: str
    price_info: PriceInfo
    url_index: int
    page_type: str = ""  # "値下げ" or "新着"
    product_hash: str = ""
    rank: int = 0
    
    @classmethod
    def create(
        cls, name: str, price_info: PriceInfo, url_index: int, 
        page_type: str = "", rank: int = 0
    ) -> ProductData:
        # ハッシュは商品名+現在価格で生成
        product_hash = hashlib.md5(
            f"{name}_{price_info.current_price}".encode("utf-8")
        ).hexdigest()[:8]
        return cls(
            name=name, price_info=price_info, url_index=url_index,
            page_type=page_type, product_hash=product_hash, rank=rank
        )
    
    def to_output_line(self) -> str:
        """master_controller用出力形式
        
        【値下げページの場合】
        - セール価格あり: "商品名 [SALE:14800→13000] 13000円"
        - 通常価格のみ: "商品名 13000円"
        
        【新着ページの場合】
        - 通常: "商品名 13000円"
        """
        if self.price_info.price_type == PriceType.SALE and self.price_info.original_price:
            # セール価格情報を明示（master_controllerで検知用）
            return (
                f"{self.name} "
                f"[SALE:{self.price_info.original_price}→{self.price_info.current_price}] "
                f"{self.price_info.current_price}円"
            )
        return f"{self.name} {self.price_info.current_price}円"


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
    def __init__(self, name: str = "naniwa_scraper", level: int = logging.INFO):
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
    # 価格抽出パターン
    PRICE_PATTERNS: Tuple[str, ...] = (
        r"[￥¥]\s*(\d{1,3}(?:,\d{3})+)",
        r"[￥¥]\s*(\d+)",
    )
    
    @classmethod
    def extract_price(cls, price_text: str) -> Optional[int]:
        """価格テキストから数値を抽出"""
        for pattern in cls.PRICE_PATTERNS:
            match = re.search(pattern, price_text)
            if match:
                try:
                    price = int(match.group(1).replace(",", ""))
                    if Constants.MIN_VALID_PRICE <= price <= Constants.MAX_VALID_PRICE:
                        return price
                except ValueError:
                    continue
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
# HTMLパーサー（値下げ検知対応版）
# ============================================================================

class NaniwaHtmlParser:
    """カメラのナニワHTML解析器（値下げ検知対応版）"""
    
    def __init__(self, validator: ProductValidator, logger: Optional[LoggerProtocol] = None):
        self._validator = validator
        self._logger = logger or StructuredLogger()
    
    def parse(self, html: str, url_index: int, page_type: str) -> List[ProductData]:
        soup = BeautifulSoup(html, "html.parser")
        products: List[ProductData] = []
        seen_hashes: Set[str] = set()
        
        # 商品ブロック取得（複数セレクタ試行）
        blocks = []
        selectors_to_try = [
            Constants.PRODUCT_BLOCK_SELECTOR,
            Constants.PRODUCT_BLOCK_SELECTOR_ALT,
            ".StyleT_Item_tile_item_",
            ".StyleIf_item_tile_item",
            "div[class*='StyleT_Item_tile']",
            "div[class*='tile_item']",
            "div[class*='tile_elm']",
        ]
        
        for selector in selectors_to_try:
            blocks = soup.select(selector)
            if blocks:
                self._logger.debug(f"セレクタ '{selector}' で {len(blocks)}件検出")
                break
        
        self._logger.info(f"URL index {url_index} ({page_type}): {len(blocks)}個の商品ブロック検出")
        
        for rank, block in enumerate(blocks, start=1):
            product = self._parse_block(block, url_index, page_type, rank, seen_hashes)
            if product:
                products.append(product)
        
        return products
    
    def _parse_block(
        self, block: Tag, url_index: int, page_type: str, rank: int, seen_hashes: Set[str]
    ) -> Optional[ProductData]:
        try:
            # 商品名抽出
            name = self._extract_name(block)
            if not name:
                return None
            
            # 価格情報抽出（値下げ検知対応）
            price_info = self._extract_price_info(block)
            if price_info is None:
                return None
            
            # 重複チェック
            product_hash = hashlib.md5(
                f"{name}_{price_info.current_price}".encode("utf-8")
            ).hexdigest()
            if product_hash in seen_hashes:
                return None
            seen_hashes.add(product_hash)
            
            return ProductData.create(
                name=name, price_info=price_info, url_index=url_index,
                page_type=page_type, rank=rank
            )
            
        except Exception:
            return None
    
    def _extract_name(self, block: Tag) -> Optional[str]:
        """商品名を抽出
        
        画像のHTML構造:
        <a href="..." title="【中古】キエフ4+ジュピター8 50/2">
        <div class="name_">
            <div class="name1_">その他 【中古】キエフ4+ジュピター8 50/2</div>
        </div>
        """
        # 方法1: aタグのtitle属性（最も信頼性が高い）
        a_tag = block.select_one("a[title]")
        if a_tag and a_tag.get("title"):
            name = a_tag.get("title", "").strip()
            validated = self._validator.validate_name(name)
            if validated:
                return validated
        
        # 方法2: div.name1_（画像の構造に基づく）
        name1_div = block.select_one(Constants.PRODUCT_NAME1_SELECTOR)
        if not name1_div:
            name1_div = block.select_one(".name1_")
        if not name1_div:
            name1_div = block.select_one("div[class*='name1']")
        if name1_div:
            name = name1_div.get_text(strip=True)
            validated = self._validator.validate_name(name)
            if validated:
                return validated
        
        # 方法3: div.name_（親要素）
        name_div = block.select_one(Constants.PRODUCT_NAME_SELECTOR)
        if not name_div:
            name_div = block.select_one(".name_")
        if not name_div:
            name_div = block.select_one("div[class*='name_']")
        if not name_div:
            name_div = block.select_one("div[class*='name']")
        if name_div:
            name = name_div.get_text(strip=True)
            validated = self._validator.validate_name(name)
            if validated:
                return validated
        
        return None
    
    def _extract_price_info(self, block: Tag) -> Optional[PriceInfo]:
        """価格情報を抽出（値下げ検知対応）
        
        【パターン1】セール価格あり
        <div class="price_before_">￥14,800</div>
        <div class="price_sale_">￥13,000</div>
        → PriceInfo(SALE, current=13000, original=14800)
        
        【パターン2】通常価格のみ
        <div class="price_">￥14,800</div>
        → PriceInfo(NORMAL, current=14800, original=None)
        """
        # まずセール価格をチェック（複数セレクタ試行）
        sale_elem = block.select_one(Constants.PRICE_SALE_SELECTOR)
        if not sale_elem:
            sale_elem = block.select_one(".price_sale_")
        
        if sale_elem:
            # セール価格あり
            sale_text = sale_elem.get_text(strip=True)
            sale_price = self._validator.extract_price(sale_text)
            
            if sale_price is None:
                return None
            
            # 元値を取得
            before_elem = block.select_one(Constants.PRICE_BEFORE_SELECTOR)
            if not before_elem:
                before_elem = block.select_one(".price_before_")
            
            original_price = None
            if before_elem:
                before_text = before_elem.get_text(strip=True)
                original_price = self._validator.extract_price(before_text)
            
            return PriceInfo(
                price_type=PriceType.SALE,
                current_price=sale_price,
                original_price=original_price
            )
        
        else:
            # 通常価格のみ - 複数セレクタ試行
            price_elem = None
            for selector in [
                Constants.PRICE_NORMAL_SELECTOR,
                Constants.PRICE_NORMAL_SELECTOR_ALT,
                ".price_",
                "div[class*='price_']",
                "div[class*='price']",
            ]:
                price_elem = block.select_one(selector)
                if price_elem:
                    break
            
            if not price_elem:
                return None
            
            price_text = price_elem.get_text(strip=True)
            current_price = self._validator.extract_price(price_text)
            
            if current_price is None:
                return None
            
            return PriceInfo(
                price_type=PriceType.NORMAL,
                current_price=current_price,
                original_price=None
            )


# ============================================================================
# メインスクレイパー
# ============================================================================

class NaniwaScraper:
    """カメラのナニワスクレイパー（値下げ検知対応版）"""
    
    def __init__(
        self,
        target_urls: Tuple[Tuple[str, str], ...] = Constants.TARGET_URLS,
        circuit_breaker: Optional[CircuitBreaker] = None,
        retry_policy: Optional[RetryPolicy] = None,
        logger: Optional[LoggerProtocol] = None,
    ):
        self._target_urls = target_urls
        self._logger = logger or StructuredLogger()
        self._circuit_breaker = circuit_breaker or CircuitBreaker(logger=self._logger)
        self._retry_policy = retry_policy or RetryPolicy(logger=self._logger)
        self._validator = ProductValidator()
        self._parser = NaniwaHtmlParser(self._validator, self._logger)
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
                
                for url_index, (page_type, url) in enumerate(self._target_urls):
                    # URL Index出力
                    print(f"---URL_INDEX:{url_index}---")
                    
                    try:
                        products = self._scrape_single_url(page, url, url_index, page_type)
                        all_products.extend(products)
                        
                        # 商品出力
                        for product in products:
                            print(product.to_output_line())
                        
                        self._logger.info(f"URL index {url_index} ({page_type}): {len(products)}件取得")
                        
                        # 値下げページの場合、価格タイプ統計をログ出力
                        if "値下げ" in page_type:
                            sale_count = sum(1 for p in products if p.price_info.price_type == PriceType.SALE)
                            normal_count = len(products) - sale_count
                            self._logger.info(f"  └ セール価格: {sale_count}件, 通常価格: {normal_count}件")
                        
                    except Exception as e:
                        self._logger.error(f"URL index {url_index} ({page_type}) エラー: {e}")
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
    
    def _scrape_single_url(self, page: Page, url: str, url_index: int, page_type: str) -> List[ProductData]:
        def _scrape() -> List[ProductData]:
            with self._circuit_breaker.protect():
                page.goto(url, timeout=Constants.PAGE_LOAD_TIMEOUT_MS, wait_until="load")
                page.wait_for_timeout(Constants.STABILITY_WAIT_MS)
                
                try:
                    page.wait_for_selector(
                        f"{Constants.PRODUCT_BLOCK_SELECTOR}, {Constants.PRODUCT_BLOCK_SELECTOR_ALT}",
                        timeout=Constants.ELEMENT_TIMEOUT_MS
                    )
                except PlaywrightTimeoutError:
                    self._logger.warning(f"URL index {url_index}: セレクタ待機タイムアウト（代替処理）")
                
                page.wait_for_timeout(1000)
                html = page.content()
                return self._parser.parse(html, url_index, page_type)
        
        return self._retry_policy.execute_with_retry(_scrape, f"scrape_{page_type}")


# ============================================================================
# エントリーポイント
# ============================================================================

def main() -> int:
    logger = StructuredLogger(level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO))
    scraper = NaniwaScraper(logger=logger)
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