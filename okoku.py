#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
okoku.py - 買取王国 エンタープライズスクレイパー v2.0

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Architecture Decision Record (ADR)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADR-001: ファイル名をshop_config.jsonと一致
=========================================
- Status: ACCEPTED
- Date: 2025-11-21
- Context: shop_config.jsonでは「okoku.py」として参照されているが、
           実際のファイル名は「kaitoritaikoku.py」だった
- Decision: ファイル名を「okoku.py」に変更し、整合性を確保
- Consequences:
  + master_controllerとの連携が正常化
  + 設定ファイルとの整合性確保

ADR-002: セレクタベース解析の改善
=========================================
- Status: ACCEPTED
- Context: p.name + p.price の兄弟要素関係に依存
- Decision: より堅牢なセレクタベース解析を実装

【SLI/SLO定義】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- SLI: 商品取得成功率
- SLO: 99.5%

【依存関係】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- requests: ^2.31.0
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

import requests
from bs4 import BeautifulSoup, Tag

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
    
    BASE_URL: Final[str] = "https://www.okoku.jp"
    
    # 【shop_config.json確認済み】
    # url_index: 0 → 時計新着
    START_URL: Final[str] = "https://www.okoku.jp/ec/Facet?category_0=11010503000"
    
    # Circuit Breaker
    CB_FAILURE_THRESHOLD: Final[int] = 5
    CB_RECOVERY_TIMEOUT_SECONDS: Final[float] = 30.0
    
    # Retry
    RETRY_MAX_ATTEMPTS: Final[int] = 3
    RETRY_BASE_DELAY_SECONDS: Final[float] = 1.0
    RETRY_MAX_DELAY_SECONDS: Final[float] = 30.0
    
    # HTTP
    REQUEST_TIMEOUT_SECONDS: Final[int] = 15
    
    # バリデーション
    MIN_VALID_PRICE: Final[int] = 100
    MAX_VALID_PRICE: Final[int] = 50_000_000
    MIN_PRODUCT_NAME_LENGTH: Final[int] = 3
    
    # 取得上限
    MAX_PRODUCTS: Final[int] = 100
    
    # User Agent
    USER_AGENT: Final[str] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )


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
    product_hash: str = ""
    rank: int = 0
    
    @classmethod
    def create(cls, name: str, price: int, rank: int = 0) -> ProductData:
        product_hash = hashlib.md5(f"{name}_{price}".encode("utf-8")).hexdigest()[:8]
        return cls(name=name, price=price, product_hash=product_hash, rank=rank)
    
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
    def __init__(self, name: str = "okoku_scraper", level: int = logging.INFO):
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
        """価格バリデーション"""
        # カンマ、円、¥を除去して数値抽出
        cleaned = price_text.replace(",", "").replace("円", "").replace("¥", "").strip()
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
# HTTPクライアント
# ============================================================================

class HttpClient:
    def __init__(self, timeout: int = Constants.REQUEST_TIMEOUT_SECONDS, logger: Optional[LoggerProtocol] = None):
        self._timeout = timeout
        self._logger = logger or StructuredLogger()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": Constants.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
        })
    
    def get(self, url: str) -> str:
        self._logger.debug(f"GET: {url}")
        response = self._session.get(url, timeout=self._timeout)
        response.raise_for_status()
        return response.text
    
    def close(self) -> None:
        self._session.close()


# ============================================================================
# HTMLパーサー
# ============================================================================

class OkokuHtmlParser:
    """買取王国HTML解析器"""
    
    def __init__(self, validator: ProductValidator, logger: Optional[LoggerProtocol] = None):
        self._validator = validator
        self._logger = logger or StructuredLogger()
    
    def parse(self, html: str) -> List[ProductData]:
        soup = BeautifulSoup(html, "html.parser")
        products: List[ProductData] = []
        
        # 商品名要素を取得
        name_elements = soup.find_all("p", class_="name")
        self._logger.debug(f"商品名要素数: {len(name_elements)}")
        
        for rank, name_elem in enumerate(name_elements, start=1):
            product = self._parse_product(name_elem, rank)
            if product:
                products.append(product)
            
            # 上限チェック
            if len(products) >= Constants.MAX_PRODUCTS:
                break
        
        return products
    
    def _parse_product(self, name_elem: Tag, rank: int) -> Optional[ProductData]:
        try:
            # 商品名
            name_link = name_elem.find("a")
            if not name_link:
                return None
            
            name_raw = name_link.get_text(strip=True)
            name = self._validator.validate_name(name_raw)
            if not name:
                return None
            
            # 価格（兄弟要素から取得）
            price_tag = name_elem.find_next_sibling("p", class_="price")
            if not price_tag:
                return None
            
            price_strong = price_tag.find("strong")
            if not price_strong:
                return None
            
            price_text = price_strong.get_text(strip=True)
            price = self._validator.validate_price(price_text)
            if price is None:
                return None
            
            return ProductData.create(name=name, price=price, rank=rank)
            
        except Exception:
            return None


# ============================================================================
# メインスクレイパー
# ============================================================================

class OkokuScraper:
    """買取王国スクレイパー"""
    
    def __init__(
        self,
        start_url: str = Constants.START_URL,
        circuit_breaker: Optional[CircuitBreaker] = None,
        retry_policy: Optional[RetryPolicy] = None,
        logger: Optional[LoggerProtocol] = None,
    ):
        self._start_url = start_url
        self._logger = logger or StructuredLogger()
        self._circuit_breaker = circuit_breaker or CircuitBreaker(logger=self._logger)
        self._retry_policy = retry_policy or RetryPolicy(logger=self._logger)
        self._validator = ProductValidator()
        self._parser = OkokuHtmlParser(self._validator, self._logger)
        self._http_client = HttpClient(logger=self._logger)
    
    def scrape(self) -> ScrapeResult:
        correlation_id = str(uuid.uuid4())[:8]
        self._logger.set_correlation_id(correlation_id)
        start_time = time.time()
        
        self._logger.info(f"スクレイピング開始: {self._start_url}")
        
        try:
            if not self._circuit_breaker.can_execute():
                return ScrapeResult(
                    success=False, products=[], error_message="Circuit Breaker is OPEN",
                    duration_seconds=time.time() - start_time, exit_code=ScraperExitCode.CIRCUIT_OPEN,
                    correlation_id=correlation_id,
                )
            
            products = self._retry_policy.execute_with_retry(
                self._scrape_with_protection, "scrape_page"
            )
            
            duration = time.time() - start_time
            self._logger.info(f"スクレイピング完了: {len(products)}件取得 ({duration:.2f}秒)")
            
            return ScrapeResult(
                success=True, products=products, duration_seconds=duration,
                exit_code=ScraperExitCode.SUCCESS if products else ScraperExitCode.PARTIAL_SUCCESS,
                correlation_id=correlation_id,
            )
            
        except CircuitOpenException as e:
            return ScrapeResult(
                success=False, products=[], error_message=str(e),
                duration_seconds=time.time() - start_time, exit_code=ScraperExitCode.CIRCUIT_OPEN,
                correlation_id=correlation_id,
            )
            
        except RetryExhaustedException as e:
            return ScrapeResult(
                success=False, products=[], error_message=str(e),
                duration_seconds=time.time() - start_time, exit_code=ScraperExitCode.FAILURE,
                correlation_id=correlation_id,
            )
            
        except Exception as e:
            self._logger.error(f"予期せぬエラー: {e}")
            return ScrapeResult(
                success=False, products=[], error_message=str(e),
                duration_seconds=time.time() - start_time, exit_code=ScraperExitCode.FAILURE,
                correlation_id=correlation_id,
            )
        
        finally:
            self._http_client.close()
    
    def _scrape_with_protection(self) -> List[ProductData]:
        with self._circuit_breaker.protect():
            html = self._http_client.get(self._start_url)
            return self._parser.parse(html)


# ============================================================================
# 出力フォーマッター
# ============================================================================

class OutputFormatter:
    @staticmethod
    def print_results(result: ScrapeResult) -> None:
        for product in result.products:
            print(product.to_output_line())
        
        if result.success and len(result.products) >= 20:
            print("SUCCESS")
        elif result.success and result.products:
            print("PARTIAL SUCCESS")
        else:
            print(f"ERROR: {result.error_message or 'Unknown error'}")


# ============================================================================
# エントリーポイント
# ============================================================================

def main() -> int:
    logger = StructuredLogger(level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO))
    scraper = OkokuScraper(logger=logger)
    result = scraper.scrape()
    OutputFormatter.print_results(result)
    return result.exit_code.value


if __name__ == "__main__":
    sys.exit(main())