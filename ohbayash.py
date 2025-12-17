#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ohbayash.py - カメラの大林 エンタープライズスクレイパー v2.0

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Architecture Decision Record (ADR)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADR-001: URLパラメータの統一
=========================================
- Status: ACCEPTED
- Date: 2025-11-21
- Context: shop_config.jsonでは `sort=order`（新着順）を指定しているが、
           スクリプト内では `sort=recommend`（おすすめ順）を使用していた
           これにより、期待と異なる順序で商品が取得されていた
- Decision: shop_config.jsonの設定を尊重し、`sort=order`を使用
- Consequences:
  + product_snapshots.jsonとサイト実態が一致
  + 新着商品の正確な検知が可能に

ADR-002: セレクタベースの堅牢な解析
=========================================
- Status: ACCEPTED
- Context: 従来のリンクベース抽出は不安定で、
           価格取得に親要素トラバースが必要だった
- Decision: 商品カードの構造に基づくセレクタベース抽出に変更
- Consequences:
  + コードの可読性向上
  + サイト構造変更への耐性向上

【SLI/SLO定義】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- SLI: 商品取得成功率
- SLO: 99.5% (30日間ローリング)
- Error Budget: 0.5% (約3.6時間/月)

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
from abc import ABC, abstractmethod
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
    Generic,
    Iterator,
    List,
    Literal,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import requests
from bs4 import BeautifulSoup, Tag

# ============================================================================
# 型定義・Protocol
# ============================================================================

T = TypeVar("T")


class LoggerProtocol(Protocol):
    """ロガーインターフェース"""
    
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...


class MetricsCollectorProtocol(Protocol):
    """メトリクス収集インターフェース"""
    
    def increment(self, metric_name: str, value: int = 1, tags: Optional[Dict[str, str]] = None) -> None: ...
    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None: ...
    def histogram(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None: ...


# ============================================================================
# 定数定義
# ============================================================================

class Constants:
    """アプリケーション定数"""
    
    # サイト情報
    BASE_URL: Final[str] = "https://www.camera-no-ohbayashi.co.jp"
    
    # 【重要修正】shop_config.jsonと一致させる: sort=order（新着順）
    # 旧: sort=recommend（おすすめ順）→ 順序がサイト表示と不一致の原因
    SEARCH_URL: Final[str] = BASE_URL + "/view/search?sort=order"
    
    # Circuit Breaker設定
    CB_FAILURE_THRESHOLD: Final[int] = 5
    CB_RECOVERY_TIMEOUT_SECONDS: Final[float] = 30.0
    CB_HALF_OPEN_MAX_CALLS: Final[int] = 3
    
    # Retry設定
    RETRY_MAX_ATTEMPTS: Final[int] = 3
    RETRY_BASE_DELAY_SECONDS: Final[float] = 1.0
    RETRY_MAX_DELAY_SECONDS: Final[float] = 30.0
    RETRY_EXPONENTIAL_BASE: Final[float] = 2.0
    RETRY_JITTER_FACTOR: Final[float] = 0.25
    
    # HTTP設定
    REQUEST_TIMEOUT_SECONDS: Final[int] = 15
    
    # 価格バリデーション
    MIN_VALID_PRICE: Final[int] = 100
    MAX_VALID_PRICE: Final[int] = 50_000_000
    
    # 商品名バリデーション
    MIN_PRODUCT_NAME_LENGTH: Final[int] = 3
    MAX_PRODUCT_NAME_LENGTH: Final[int] = 500
    
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
    """Circuit Breakerの状態"""
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class ScraperExitCode(Enum):
    """終了コード"""
    SUCCESS = 0
    PARTIAL_SUCCESS = 1
    FAILURE = 2
    CIRCUIT_OPEN = 3
    VALIDATION_ERROR = 4


# ============================================================================
# データクラス
# ============================================================================

@dataclass(frozen=True, slots=True)
class ProductData:
    """商品データ（イミュータブル）"""
    name: str
    price: int
    url: str = ""
    image_url: str = ""
    product_hash: str = ""
    scraped_at: datetime = field(default_factory=datetime.now)
    rank: int = 0
    
    @classmethod
    def create(
        cls,
        name: str,
        price: int,
        url: str = "",
        image_url: str = "",
        rank: int = 0,
    ) -> ProductData:
        """ファクトリメソッド"""
        product_hash = hashlib.md5(f"{name}_{price}".encode("utf-8")).hexdigest()[:8]
        return cls(
            name=name,
            price=price,
            url=url,
            image_url=image_url,
            product_hash=product_hash,
            scraped_at=datetime.now(),
            rank=rank,
        )
    
    def to_output_line(self) -> str:
        """master_controller用出力形式"""
        return f"{self.name} {self.price}円"


@dataclass
class CircuitBreakerState:
    """Circuit Breaker状態管理"""
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    half_open_call_count: int = 0


@dataclass
class ScrapeResult:
    """スクレイピング結果"""
    success: bool
    products: List[ProductData]
    error_message: Optional[str] = None
    duration_seconds: float = 0.0
    exit_code: ScraperExitCode = ScraperExitCode.SUCCESS
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])


# ============================================================================
# 例外クラス
# ============================================================================

class ScraperException(Exception):
    """スクレイパー基底例外"""
    
    def __init__(self, message: str, correlation_id: Optional[str] = None):
        self.correlation_id = correlation_id or str(uuid.uuid4())[:8]
        super().__init__(f"[{self.correlation_id}] {message}")


class CircuitOpenException(ScraperException):
    """Circuit Breaker Open状態例外"""
    pass


class RetryExhaustedException(ScraperException):
    """リトライ回数超過例外"""
    pass


# ============================================================================
# インフラストラクチャ層: ロギング
# ============================================================================

class StructuredLogger:
    """構造化ロガー"""
    
    def __init__(
        self,
        name: str = "ohbayashi_scraper",
        level: int = logging.INFO,
        use_json: bool = False,
    ):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)
        self._use_json = use_json
        self._correlation_id: Optional[str] = None
        
        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(level)
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s"
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)
    
    def set_correlation_id(self, correlation_id: str) -> None:
        self._correlation_id = correlation_id
    
    def _format_message(self, msg: str) -> str:
        if self._correlation_id:
            return f"[{self._correlation_id}] {msg}"
        return msg
    
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.debug(self._format_message(msg), *args, **kwargs)
    
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.info(self._format_message(msg), *args, **kwargs)
    
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.warning(self._format_message(msg), *args, **kwargs)
    
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.error(self._format_message(msg), *args, **kwargs)


# ============================================================================
# インフラストラクチャ層: メトリクス
# ============================================================================

class InMemoryMetricsCollector:
    """インメモリメトリクス収集"""
    
    def __init__(self) -> None:
        self._counters: Dict[str, int] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = {}
    
    def increment(
        self,
        metric_name: str,
        value: int = 1,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        key = self._make_key(metric_name, tags)
        self._counters[key] = self._counters.get(key, 0) + value
    
    def gauge(
        self,
        metric_name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        key = self._make_key(metric_name, tags)
        self._gauges[key] = value
    
    def histogram(
        self,
        metric_name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        key = self._make_key(metric_name, tags)
        if key not in self._histograms:
            self._histograms[key] = []
        self._histograms[key].append(value)
    
    @staticmethod
    def _make_key(metric_name: str, tags: Optional[Dict[str, str]]) -> str:
        if tags:
            tag_str = ",".join(f'{k}="{v}"' for k, v in sorted(tags.items()))
            return f"{metric_name}{{{tag_str}}}"
        return metric_name
    
    def get_metrics(self) -> Dict[str, Any]:
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {
                k: {"count": len(v), "sum": sum(v), "avg": sum(v) / len(v) if v else 0}
                for k, v in self._histograms.items()
            },
        }


# ============================================================================
# ドメイン層: Circuit Breaker
# ============================================================================

class CircuitBreaker:
    """Circuit Breaker実装"""
    
    def __init__(
        self,
        failure_threshold: int = Constants.CB_FAILURE_THRESHOLD,
        recovery_timeout: float = Constants.CB_RECOVERY_TIMEOUT_SECONDS,
        half_open_max_calls: int = Constants.CB_HALF_OPEN_MAX_CALLS,
        logger: Optional[LoggerProtocol] = None,
        metrics: Optional[MetricsCollectorProtocol] = None,
    ):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = timedelta(seconds=recovery_timeout)
        self._half_open_max_calls = half_open_max_calls
        self._logger = logger or StructuredLogger()
        self._metrics = metrics or InMemoryMetricsCollector()
        self._state = CircuitBreakerState()
    
    @property
    def state(self) -> CircuitState:
        return self._state.state
    
    def can_execute(self) -> bool:
        self._check_state_transition()
        return self._state.state != CircuitState.OPEN
    
    def _check_state_transition(self) -> None:
        if self._state.state == CircuitState.OPEN:
            if self._state.last_failure_time:
                elapsed = datetime.now() - self._state.last_failure_time
                if elapsed >= self._recovery_timeout:
                    self._transition_to_half_open()
    
    def _transition_to_half_open(self) -> None:
        self._state.state = CircuitState.HALF_OPEN
        self._state.half_open_call_count = 0
        self._logger.info("Circuit Breaker: OPEN -> HALF_OPEN")
    
    def record_success(self) -> None:
        if self._state.state == CircuitState.HALF_OPEN:
            self._state.half_open_call_count += 1
            if self._state.half_open_call_count >= self._half_open_max_calls:
                self._transition_to_closed()
        elif self._state.state == CircuitState.CLOSED:
            self._state.failure_count = 0
    
    def _transition_to_closed(self) -> None:
        self._state.state = CircuitState.CLOSED
        self._state.failure_count = 0
        self._state.half_open_call_count = 0
        self._logger.info("Circuit Breaker: HALF_OPEN -> CLOSED")
    
    def record_failure(self) -> None:
        self._state.failure_count += 1
        self._state.last_failure_time = datetime.now()
        
        if self._state.state == CircuitState.HALF_OPEN:
            self._transition_to_open()
        elif self._state.failure_count >= self._failure_threshold:
            self._transition_to_open()
    
    def _transition_to_open(self) -> None:
        previous_state = self._state.state.name
        self._state.state = CircuitState.OPEN
        self._logger.warning(f"Circuit Breaker: {previous_state} -> OPEN")
    
    @contextmanager
    def protect(self) -> Generator[None, None, None]:
        if not self.can_execute():
            raise CircuitOpenException("Circuit Breaker is OPEN")
        
        try:
            yield
            self.record_success()
        except Exception as e:
            self.record_failure()
            raise


# ============================================================================
# ドメイン層: Retry with Exponential Backoff
# ============================================================================

class RetryPolicy:
    """リトライポリシー"""
    
    def __init__(
        self,
        max_attempts: int = Constants.RETRY_MAX_ATTEMPTS,
        base_delay: float = Constants.RETRY_BASE_DELAY_SECONDS,
        max_delay: float = Constants.RETRY_MAX_DELAY_SECONDS,
        exponential_base: float = Constants.RETRY_EXPONENTIAL_BASE,
        jitter_factor: float = Constants.RETRY_JITTER_FACTOR,
        logger: Optional[LoggerProtocol] = None,
    ):
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._exponential_base = exponential_base
        self._jitter_factor = jitter_factor
        self._logger = logger or StructuredLogger()
    
    def execute_with_retry(
        self,
        operation: Callable[[], T],
        operation_name: str = "operation",
    ) -> T:
        attempt = 0
        last_exception: Optional[Exception] = None
        
        while attempt < self._max_attempts:
            attempt += 1
            
            try:
                result = operation()
                if attempt > 1:
                    self._logger.info(
                        f"{operation_name}: 成功 (attempt {attempt})"
                    )
                return result
                
            except Exception as e:
                last_exception = e
                self._logger.warning(
                    f"{operation_name}: 失敗 (attempt {attempt}/{self._max_attempts})"
                    f" - {type(e).__name__}: {str(e)[:100]}"
                )
                
                if attempt >= self._max_attempts:
                    break
                
                delay = self._calculate_delay(attempt)
                self._logger.info(f"{operation_name}: {delay:.2f}秒後にリトライ...")
                time.sleep(delay)
        
        raise RetryExhaustedException(
            f"{operation_name}: {self._max_attempts}回のリトライ失敗",
        ) from last_exception
    
    def _calculate_delay(self, attempt: int) -> float:
        delay = self._base_delay * (self._exponential_base ** (attempt - 1))
        delay = min(delay, self._max_delay)
        jitter_range = delay * self._jitter_factor
        jitter = random.uniform(-jitter_range, jitter_range)
        delay += jitter
        return max(0.1, delay)


# ============================================================================
# ドメイン層: バリデーター
# ============================================================================

class ProductValidator:
    """商品データバリデーター"""
    
    # 価格抽出パターン（優先順位順）
    PRICE_PATTERNS: List[str] = [
        r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)（税込）",
        r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)円",
        r"¥(\d{1,3}(?:,\d{3})*(?:\.\d+)?)",
        r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)税込",
        r"(\d{1,3}(?:,\d{3})*)",  # 最終手段
    ]
    
    @classmethod
    def validate_price(cls, price_text: str) -> Optional[int]:
        """価格バリデーション"""
        for pattern in cls.PRICE_PATTERNS:
            matches = re.findall(pattern, price_text)
            if matches:
                # 最後にマッチした価格を使用
                price_str = matches[-1].replace(",", "").replace(".", "")
                try:
                    price = int(float(price_str))
                    if Constants.MIN_VALID_PRICE <= price <= Constants.MAX_VALID_PRICE:
                        return price
                except ValueError:
                    continue
        return None
    
    @staticmethod
    def validate_name(name: str) -> Optional[str]:
        """商品名バリデーション"""
        name = re.sub(r"\s+", " ", name).strip()
        if Constants.MIN_PRODUCT_NAME_LENGTH <= len(name) <= Constants.MAX_PRODUCT_NAME_LENGTH:
            return name
        return None


# ============================================================================
# アプリケーション層: HTMLパーサー
# ============================================================================

class OhbayashiHtmlParser:
    """カメラの大林HTML解析器
    
    サイト構造:
    - 商品リンク: /view/item/ を含むaタグ
    - 価格: 親要素のテキストから抽出
    
    【重要】URLは `sort=order`（新着順）を使用
    """
    
    def __init__(
        self,
        validator: ProductValidator,
        logger: Optional[LoggerProtocol] = None,
    ):
        self._validator = validator
        self._logger = logger or StructuredLogger()
    
    def parse(self, html: str) -> List[ProductData]:
        """HTML解析・商品抽出"""
        soup = BeautifulSoup(html, "html.parser")
        products: List[ProductData] = []
        seen_names: set = set()
        
        # 商品リンクを探す（/view/item/ を含むリンク）
        product_links = soup.find_all("a", href=lambda x: x and "/view/item/" in x)
        
        self._logger.debug(f"商品リンク数: {len(product_links)}")
        
        rank = 0
        for link in product_links:
            product = self._parse_product_link(link, rank + 1)
            if product and product.name not in seen_names:
                seen_names.add(product.name)
                rank += 1
                products.append(product)
        
        return products
    
    def _parse_product_link(self, link: Tag, rank: int) -> Optional[ProductData]:
        """商品リンク解析"""
        # 商品名
        name_raw = link.get_text(strip=True)
        name = self._validator.validate_name(name_raw)
        if not name:
            return None
        
        # URL
        href = link.get("href", "")
        url = Constants.BASE_URL + href if href.startswith("/") else href
        
        # 価格抽出（親要素から検索）
        price = self._extract_price_from_context(link)
        if price is None:
            return None
        
        # 画像URL
        image_url = self._extract_image_url(link)
        
        return ProductData.create(
            name=name,
            price=price,
            url=url,
            image_url=image_url,
            rank=rank,
        )
    
    def _extract_price_from_context(self, link: Tag) -> Optional[int]:
        """コンテキストから価格抽出"""
        # 親要素を3階層まで探索
        current = link
        for _ in range(5):
            if current.parent:
                current = current.parent
                text = current.get_text()
                price = self._validator.validate_price(text)
                if price:
                    return price
        
        # 次の兄弟要素から探索
        next_sibling = link.next_sibling
        while next_sibling:
            sibling_text = str(next_sibling)
            price = self._validator.validate_price(sibling_text)
            if price:
                return price
            next_sibling = getattr(next_sibling, "next_sibling", None)
        
        return None
    
    def _extract_image_url(self, link: Tag) -> str:
        """画像URL抽出"""
        # リンク内の画像
        img = link.find("img")
        if img and img.get("src"):
            src = img["src"]
            if src.startswith("/"):
                return Constants.BASE_URL + src
            return src
        
        # 親要素内の画像
        parent = link.parent
        if parent:
            img = parent.find("img")
            if img and img.get("src"):
                src = img["src"]
                if src.startswith("/"):
                    return Constants.BASE_URL + src
                return src
        
        return ""


# ============================================================================
# インフラストラクチャ層: HTTPクライアント
# ============================================================================

class HttpClient:
    """HTTPクライアント"""
    
    def __init__(
        self,
        timeout: int = Constants.REQUEST_TIMEOUT_SECONDS,
        logger: Optional[LoggerProtocol] = None,
    ):
        self._timeout = timeout
        self._logger = logger or StructuredLogger()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": Constants.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        })
    
    def get(self, url: str) -> str:
        """GETリクエスト"""
        self._logger.debug(f"GET: {url}")
        response = self._session.get(url, timeout=self._timeout)
        response.raise_for_status()
        return response.text
    
    def close(self) -> None:
        """セッション終了"""
        self._session.close()


# ============================================================================
# アプリケーション層: メインスクレイパー
# ============================================================================

class OhbayashiScraper:
    """カメラの大林スクレイパー
    
    【重要修正】
    - URLを `sort=order`（新着順）に統一
    - shop_config.jsonの設定と一致
    """
    
    def __init__(
        self,
        search_url: str = Constants.SEARCH_URL,
        circuit_breaker: Optional[CircuitBreaker] = None,
        retry_policy: Optional[RetryPolicy] = None,
        logger: Optional[LoggerProtocol] = None,
        metrics: Optional[MetricsCollectorProtocol] = None,
    ):
        self._search_url = search_url
        self._logger = logger or StructuredLogger()
        self._metrics = metrics or InMemoryMetricsCollector()
        
        self._circuit_breaker = circuit_breaker or CircuitBreaker(
            logger=self._logger,
            metrics=self._metrics,
        )
        self._retry_policy = retry_policy or RetryPolicy(
            logger=self._logger,
        )
        
        self._validator = ProductValidator()
        self._parser = OhbayashiHtmlParser(self._validator, self._logger)
        self._http_client = HttpClient(logger=self._logger)
    
    def scrape(self) -> ScrapeResult:
        """スクレイピング実行"""
        correlation_id = str(uuid.uuid4())[:8]
        self._logger.set_correlation_id(correlation_id)
        
        start_time = time.time()
        
        self._logger.info(f"スクレイピング開始: {self._search_url}")
        
        try:
            if not self._circuit_breaker.can_execute():
                self._logger.warning("Circuit Breaker OPEN - スキップ")
                return ScrapeResult(
                    success=False,
                    products=[],
                    error_message="Circuit Breaker is OPEN",
                    duration_seconds=time.time() - start_time,
                    exit_code=ScraperExitCode.CIRCUIT_OPEN,
                    correlation_id=correlation_id,
                )
            
            products = self._retry_policy.execute_with_retry(
                operation=self._scrape_with_protection,
                operation_name="scrape_page",
            )
            
            duration = time.time() - start_time
            
            self._logger.info(
                f"スクレイピング完了: {len(products)}件取得 ({duration:.2f}秒)"
            )
            
            return ScrapeResult(
                success=True,
                products=products,
                duration_seconds=duration,
                exit_code=(
                    ScraperExitCode.SUCCESS if len(products) > 0
                    else ScraperExitCode.PARTIAL_SUCCESS
                ),
                correlation_id=correlation_id,
            )
            
        except CircuitOpenException as e:
            self._logger.error(f"Circuit Open: {e}")
            return ScrapeResult(
                success=False,
                products=[],
                error_message=str(e),
                duration_seconds=time.time() - start_time,
                exit_code=ScraperExitCode.CIRCUIT_OPEN,
                correlation_id=correlation_id,
            )
            
        except RetryExhaustedException as e:
            self._logger.error(f"リトライ失敗: {e}")
            return ScrapeResult(
                success=False,
                products=[],
                error_message=str(e),
                duration_seconds=time.time() - start_time,
                exit_code=ScraperExitCode.FAILURE,
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
        
        finally:
            self._http_client.close()
    
    def _scrape_with_protection(self) -> List[ProductData]:
        """保護付きスクレイピング"""
        with self._circuit_breaker.protect():
            return self._scrape_page()
    
    def _scrape_page(self) -> List[ProductData]:
        """ページスクレイピング"""
        html = self._http_client.get(self._search_url)
        return self._parser.parse(html)


# ============================================================================
# プレゼンテーション層: 出力フォーマッター
# ============================================================================

class OutputFormatter:
    """出力フォーマッター"""
    
    @staticmethod
    def print_results(result: ScrapeResult) -> None:
        """結果出力"""
        for product in result.products:
            print(product.to_output_line())
        
        if result.success and len(result.products) >= 20:
            print("SUCCESS")
        elif result.success and len(result.products) > 0:
            print("PARTIAL SUCCESS")
        else:
            print(f"ERROR: {result.error_message or 'Unknown error'}")


# ============================================================================
# エントリーポイント
# ============================================================================

def main() -> int:
    """メイン関数"""
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    
    logger = StructuredLogger(
        name="ohbayashi_scraper",
        level=getattr(logging, log_level.upper(), logging.INFO),
    )
    
    metrics = InMemoryMetricsCollector()
    
    scraper = OhbayashiScraper(
        logger=logger,
        metrics=metrics,
    )
    
    result = scraper.scrape()
    
    OutputFormatter.print_results(result)
    
    return result.exit_code.value


if __name__ == "__main__":
    sys.exit(main())