#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mediajoy.py - メディアジョイ エンタープライズスクレイパー v1.0

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Architecture Decision Record (ADR)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADR-001: 静的サイト・requests+BeautifulSoup採用
=========================================
- Status: ACCEPTED
- Context: QAレポートで静的サイトと判定（JS5個・DOM静的）
- Decision: Playwrightは不要、requestsで十分
- Consequences: 高速・安定・リソース効率的
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

import requests
from bs4 import BeautifulSoup

T = TypeVar("T")

class LoggerProtocol(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...

class Constants:
    TARGET_URLS: Final[Tuple[str, ...]] = ("https://mediajoycamera.com/",)
    CB_FAILURE_THRESHOLD: Final[int] = 5
    CB_RECOVERY_TIMEOUT_SECONDS: Final[float] = 30.0
    RETRY_MAX_ATTEMPTS: Final[int] = 3
    RETRY_BASE_DELAY_SECONDS: Final[float] = 1.0
    RETRY_MAX_DELAY_SECONDS: Final[float] = 30.0
    REQUEST_TIMEOUT_SECONDS: Final[int] = 30
    MIN_VALID_PRICE: Final[int] = 100
    MAX_VALID_PRICE: Final[int] = 50_000_000
    MIN_PRODUCT_NAME_LENGTH: Final[int] = 3
    USER_AGENT: Final[str] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

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
    def __init__(self, name: str = "mediajoy_scraper", level: int = logging.INFO):
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
        price_clean = re.sub(r'[^\d]', '', price_text)
        try:
            price = int(price_clean)
            if Constants.MIN_VALID_PRICE <= price <= Constants.MAX_VALID_PRICE:
                return price
        except (ValueError, TypeError):
            pass
        return None
    
    @staticmethod
    def validate_name(name: str) -> Optional[str]:
        name = re.sub(r"\s+", " ", name).strip()
        return name if len(name) >= Constants.MIN_PRODUCT_NAME_LENGTH else None

class HttpClient:
    def __init__(self, timeout: int = Constants.REQUEST_TIMEOUT_SECONDS, logger: Optional[LoggerProtocol] = None):
        self._timeout = timeout
        self._logger = logger or StructuredLogger()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": Constants.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        })
    
    def get_html(self, url: str) -> str:
        self._logger.debug(f"GET: {url}")
        response = self._session.get(url, timeout=self._timeout)
        response.raise_for_status()
        response.encoding = response.apparent_encoding or 'utf-8'
        return response.text
    
    def close(self) -> None:
        self._session.close()

class MediajoyParser:
    def __init__(self, validator: ProductValidator, logger: Optional[LoggerProtocol] = None):
        self._validator = validator
        self._logger = logger or StructuredLogger()
    
    def parse(self, html: str, url_index: int) -> List[ProductData]:
        products: List[ProductData] = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # section-block全体を取得
        sections = soup.select('section.section-block')
        self._logger.debug(f"URL index {url_index}: {len(sections)}個のセクション検出")
        
        for section in sections:
            # セクションタイトルを確認
            title_elem = section.select_one('h2.title')
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                # 「おすすめ」セクションはスキップ
                if 'おすすめ' in title_text or '人気' in title_text or 'ランキング' in title_text:
                    self._logger.debug(f"おすすめセクションをスキップ: {title_text}")
                    continue
            
            # セクション内の商品リスト（ul.list-product）を取得
            product_lists = section.select('ul.list-product li')
            if not product_lists:
                # フォールバック：セクション内の全li要素
                product_lists = section.select('li')
            
            for rank, item in enumerate(product_lists, start=len(products)+1):
                parsed = self._parse_item(item, url_index, rank)
                if parsed:
                    products.append(parsed)
        
        # セクションがない場合のフォールバック
        if not sections:
            items = soup.select('.item, .product, li, tr')
            self._logger.debug(f"フォールバック: {len(items)}個の要素検出")
            for rank, item in enumerate(items, start=1):
                parsed = self._parse_item(item, url_index, rank)
                if parsed:
                    products.append(parsed)
        
        return products
    
    def _parse_item(self, item, url_index: int, rank: int) -> Optional[ProductData]:
        try:
            text = item.get_text()
            
            # 数字で始まる行（おすすめ商品）をスキップ
            # 例: "1 リンホフ スーパーテヒニカ4×5 105000円"
            text_stripped = text.strip()
            if re.match(r'^\d+\s+', text_stripped):
                return None
            
            price_match = re.search(r'([\d,]+)\s*円', text)
            if not price_match:
                return None
            price = self._validator.validate_price(price_match.group(1))
            if price is None:
                return None
            name_part = text[:price_match.start()].strip()
            
            # 商品名も数字で始まる場合を除外（念のため）
            if re.match(r'^\d+\s+', name_part):
                return None
            
            name = self._validator.validate_name(name_part)
            if not name:
                return None
            return ProductData.create(name=name, price=price, url_index=url_index, rank=rank)
        except Exception:
            return None

class MediajoyScraper:
    def __init__(self, target_urls: Tuple[str, ...] = Constants.TARGET_URLS,
                 circuit_breaker: Optional[CircuitBreaker] = None,
                 retry_policy: Optional[RetryPolicy] = None,
                 logger: Optional[LoggerProtocol] = None):
        self._target_urls = target_urls
        self._logger = logger or StructuredLogger()
        self._circuit_breaker = circuit_breaker or CircuitBreaker(logger=self._logger)
        self._retry_policy = retry_policy or RetryPolicy(logger=self._logger)
        self._validator = ProductValidator()
        self._parser = MediajoyParser(self._validator, self._logger)
        self._http_client = HttpClient(logger=self._logger)
    
    def scrape(self) -> ScrapeResult:
        correlation_id = str(uuid.uuid4())[:8]
        self._logger.set_correlation_id(correlation_id)
        start_time = time.time()
        all_products: List[ProductData] = []
        self._logger.info(f"スクレイピング開始: {len(self._target_urls)} URL")
        
        try:
            if not self._circuit_breaker.can_execute():
                return ScrapeResult(success=False, products=[], error_message="Circuit Breaker is OPEN",
                                  duration_seconds=time.time() - start_time, exit_code=ScraperExitCode.CIRCUIT_OPEN,
                                  correlation_id=correlation_id)
            
            for url_index, url in enumerate(self._target_urls):
                print(f"---URL_INDEX:{url_index}---")
                try:
                    products = self._scrape_single_url(url, url_index)
                    all_products.extend(products)
                    for product in products:
                        print(product.to_output_line())
                except Exception as e:
                    self._logger.error(f"URL index {url_index} エラー: {e}")
                    continue
            
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
        finally:
            self._http_client.close()
    
    def _scrape_single_url(self, url: str, url_index: int) -> List[ProductData]:
        def _scrape() -> List[ProductData]:
            with self._circuit_breaker.protect():
                html = self._http_client.get_html(url)
                return self._parser.parse(html, url_index)
        return self._retry_policy.execute_with_retry(_scrape, f"scrape_url_{url_index}")

def main() -> int:
    logger = StructuredLogger(level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO))
    scraper = MediajoyScraper(logger=logger)
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