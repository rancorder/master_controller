#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ymmtca.py - 山本写真機店 エンタープライズスクレイパー v2.0

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Architecture Decision Record (ADR)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADR-001: 複数テーブル構造への適応
=========================================
- Status: ACCEPTED
- Date: 2025-11-21
- Context: 山本写真機店は複数のカテゴリページを持ち、
           各ページのテーブル構造が微妙に異なる（3列/4列）
- Decision: 柔軟なカラム検出アルゴリズムを実装し、
           テーブル構造変更に対して自己適応できるようにする
- Consequences:
  + サイト構造変更への耐性向上
  + カラム数変動に自動対応

ADR-002: URL Index管理の明示化
=========================================
- Status: ACCEPTED
- Context: master_controllerは複数URL設定に対応しており、
           各URLに対してurl_indexで区別している
- Decision: URL Index出力を標準化し、master_controllerとの連携を強化
- Consequences:
  + 各カテゴリの商品が正しく区別される
  + スナップショット管理の精度向上

ADR-003: エンコーディング自動検出
=========================================
- Status: ACCEPTED
- Context: 古いサイトはShift_JIS/EUC-JPを使用している可能性
- Decision: response.apparent_encodingを使用し自動検出
- Consequences:
  + 文字化け防止
  + 日本語サイトへの汎用対応

【SLI/SLO定義】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- SLI: 商品取得成功率
- SLO: 99.0% (30日間ローリング)  # 古いサイトのため少し緩め
- Error Budget: 1.0% (約7.2時間/月)

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
    Sequence,
    Tuple,
    TypeVar,
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
    
    # ターゲットURL（shop_config.jsonと一致）
    TARGET_URLS: Final[Tuple[str, ...]] = (
        "http://www.avis.ne.jp/~ymmtca/medama2.htm",        # url_index: 0 - 目玉商品
        "http://www.avis.ne.jp/~ymmtca/leica_2.htm",        # url_index: 1 - ライカ
        "http://www.avis.ne.jp/~ymmtca/tinnpinn_2.htm",     # url_index: 2 - アンティーク
        "http://www.avis.ne.jp/~ymmtca/contax_2.htm",       # url_index: 3 - ハッセル
        "http://www.avis.ne.jp/~ymmtca/nikon_2.htm",        # url_index: 4 - Nikon
        "http://www.avis.ne.jp/~ymmtca/sonota%20tyuuko_2.htm",  # url_index: 5 - その他
        "http://www.avis.ne.jp/~ymmtca/fokutorennda_2.htm", # url_index: 6 - フォクトレンダー
        "http://www.avis.ne.jp/~ymmtca/akusesari-2.htm",    # url_index: 7 - アクセサリ
    )
    
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
    
    # Rate Limiting（古いサーバー保護）
    MIN_REQUEST_INTERVAL_SECONDS: Final[float] = 2.0
    MAX_REQUEST_INTERVAL_SECONDS: Final[float] = 4.0
    
    # 価格バリデーション
    MIN_VALID_PRICE: Final[int] = 100
    MAX_VALID_PRICE: Final[int] = 50_000_000
    
    # 商品名バリデーション
    MIN_PRODUCT_NAME_LENGTH: Final[int] = 3
    MAX_PRODUCT_NAME_LENGTH: Final[int] = 500
    
    # User Agents（ローテーション用）
    USER_AGENTS: Final[Tuple[str, ...]] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
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


# ============================================================================
# データクラス
# ============================================================================

@dataclass(frozen=True, slots=True)
class ProductData:
    """商品データ（イミュータブル）"""
    name: str
    price: int
    url_index: int
    product_hash: str = ""
    scraped_at: datetime = field(default_factory=datetime.now)
    rank: int = 0
    
    @classmethod
    def create(
        cls,
        name: str,
        price: int,
        url_index: int,
        rank: int = 0,
    ) -> ProductData:
        """ファクトリメソッド"""
        product_hash = hashlib.md5(f"{name}_{price}".encode("utf-8")).hexdigest()[:8]
        return cls(
            name=name,
            price=price,
            url_index=url_index,
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
        name: str = "ymmtca_scraper",
        level: int = logging.INFO,
    ):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)
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
            "histograms": dict(self._histograms),
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
    ):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = timedelta(seconds=recovery_timeout)
        self._half_open_max_calls = half_open_max_calls
        self._logger = logger or StructuredLogger()
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
        self._logger.info("Circuit Breaker: HALF_OPEN -> CLOSED")
    
    def record_failure(self) -> None:
        self._state.failure_count += 1
        self._state.last_failure_time = datetime.now()
        
        if self._state.state == CircuitState.HALF_OPEN:
            self._transition_to_open()
        elif self._state.failure_count >= self._failure_threshold:
            self._transition_to_open()
    
    def _transition_to_open(self) -> None:
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
                return operation()
            except Exception as e:
                last_exception = e
                self._logger.warning(
                    f"{operation_name}: 失敗 (attempt {attempt}/{self._max_attempts})"
                )
                
                if attempt >= self._max_attempts:
                    break
                
                delay = self._calculate_delay(attempt)
                time.sleep(delay)
        
        raise RetryExhaustedException(
            f"{operation_name}: {self._max_attempts}回のリトライ失敗",
        ) from last_exception
    
    def _calculate_delay(self, attempt: int) -> float:
        delay = self._base_delay * (self._exponential_base ** (attempt - 1))
        delay = min(delay, self._max_delay)
        jitter_range = delay * self._jitter_factor
        jitter = random.uniform(-jitter_range, jitter_range)
        return max(0.1, delay + jitter)


# ============================================================================
# ドメイン層: Rate Limiter
# ============================================================================

class RateLimiter:
    """レートリミッター"""
    
    def __init__(
        self,
        min_interval: float = Constants.MIN_REQUEST_INTERVAL_SECONDS,
        max_interval: float = Constants.MAX_REQUEST_INTERVAL_SECONDS,
    ):
        self._min_interval = min_interval
        self._max_interval = max_interval
        self._last_request_time: Optional[datetime] = None
    
    def wait(self) -> None:
        if self._last_request_time:
            elapsed = (datetime.now() - self._last_request_time).total_seconds()
            target_interval = random.uniform(self._min_interval, self._max_interval)
            
            if elapsed < target_interval:
                time.sleep(target_interval - elapsed)
        
        self._last_request_time = datetime.now()


# ============================================================================
# ドメイン層: バリデーター
# ============================================================================

class ProductValidator:
    """商品データバリデーター"""
    
    # SOLD OUT判定パターン
    SOLD_OUT_PATTERNS: Tuple[str, ...] = (
        "SOLD OUT",
        "SOLD",
        "売切",
        "完売",
    )
    
    @classmethod
    def is_sold_out(cls, text: str) -> bool:
        """売り切れ判定"""
        text_upper = text.upper()
        return any(pattern in text_upper for pattern in cls.SOLD_OUT_PATTERNS)
    
    @staticmethod
    def validate_price(price_text: str) -> Optional[int]:
        """価格バリデーション"""
        # 数字とカンマを抽出
        match = re.search(r"[\d,]+", price_text)
        if not match:
            return None
        
        try:
            price = int(match.group().replace(",", ""))
            if Constants.MIN_VALID_PRICE <= price <= Constants.MAX_VALID_PRICE:
                return price
        except ValueError:
            pass
        
        return None
    
    @staticmethod
    def validate_name(name: str) -> Optional[str]:
        """商品名バリデーション"""
        # 空白正規化
        name = re.sub(r"\s+", " ", name).strip()
        
        if Constants.MIN_PRODUCT_NAME_LENGTH <= len(name) <= Constants.MAX_PRODUCT_NAME_LENGTH:
            return name
        return None


# ============================================================================
# アプリケーション層: HTMLパーサー
# ============================================================================

class YmmtcaHtmlParser:
    """山本写真機店HTML解析器
    
    サイト構造:
    - テーブル: border="1" 属性を持つテーブル
    - カラム構造: 3列または4列（ページによって異なる）
      - 4列: [番号, 商品名, 状態, 価格]
      - 3列: [商品名, 状態, 価格]
    
    【自己適応アルゴリズム】
    - カラム数を動的に検出
    - 価格位置を最終カラムと仮定
    - 商品名位置をカラム数に応じて調整
    """
    
    def __init__(
        self,
        validator: ProductValidator,
        logger: Optional[LoggerProtocol] = None,
    ):
        self._validator = validator
        self._logger = logger or StructuredLogger()
    
    def parse(self, html: str, url_index: int) -> List[ProductData]:
        """HTML解析・商品抽出"""
        soup = BeautifulSoup(html, "html.parser")
        products: List[ProductData] = []
        
        # テーブル検出（border="1"）
        product_table = soup.find("table", attrs={"border": "1"})
        
        if not product_table:
            # フォールバック: 3番目のテーブル
            all_tables = soup.find_all("table")
            if len(all_tables) >= 3:
                product_table = all_tables[2]
            else:
                self._logger.warning(f"URL index {url_index}: テーブル未検出")
                return products
        
        # ヘッダー行をスキップ（最初の行）
        rows = product_table.find_all("tr")[1:]
        
        if not rows:
            self._logger.debug(f"URL index {url_index}: 行データなし")
            return products
        
        # カラム構造を検出
        sample_row = rows[0] if rows else None
        if sample_row:
            cols = sample_row.find_all("td")
            self._logger.debug(f"URL index {url_index}: カラム数={len(cols)}")
        
        for rank, row in enumerate(rows, start=1):
            product = self._parse_row(row, url_index, rank)
            if product:
                products.append(product)
        
        return products
    
    def _parse_row(
        self,
        row: Tag,
        url_index: int,
        rank: int,
    ) -> Optional[ProductData]:
        """テーブル行解析（自己適応型）"""
        cols = row.find_all("td")
        
        if len(cols) < 3:
            return None
        
        # カラム構造に応じた抽出
        # 4列以上: cols[1]=商品名, cols[-1]=価格
        # 3列: cols[0]=商品名, cols[-1]=価格
        if len(cols) >= 4:
            name_col = cols[1]
            price_col = cols[-1]  # 最終カラムは常に価格
        else:
            name_col = cols[0]
            price_col = cols[-1]
        
        # 商品名抽出
        name_raw = name_col.get_text(strip=True)
        name = self._validator.validate_name(name_raw)
        if not name:
            return None
        
        # 価格抽出
        price_raw = price_col.get_text(strip=True)
        
        # SOLD OUT判定
        if self._validator.is_sold_out(price_raw):
            return None
        
        price = self._validator.validate_price(price_raw)
        if price is None:
            return None
        
        return ProductData.create(
            name=name,
            price=price,
            url_index=url_index,
            rank=rank,
        )


# ============================================================================
# インフラストラクチャ層: HTTPクライアント
# ============================================================================

class HttpClient:
    """HTTPクライアント（User Agentローテーション付き）"""
    
    def __init__(
        self,
        timeout: int = Constants.REQUEST_TIMEOUT_SECONDS,
        logger: Optional[LoggerProtocol] = None,
    ):
        self._timeout = timeout
        self._logger = logger or StructuredLogger()
        self._session = requests.Session()
    
    def get(self, url: str) -> str:
        """GETリクエスト（エンコーディング自動検出）"""
        headers = {
            "User-Agent": random.choice(Constants.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
        }
        
        self._logger.debug(f"GET: {url}")
        response = self._session.get(url, headers=headers, timeout=self._timeout)
        response.raise_for_status()
        
        # エンコーディング自動検出
        response.encoding = response.apparent_encoding
        
        return response.text
    
    def close(self) -> None:
        self._session.close()


# ============================================================================
# アプリケーション層: メインスクレイパー
# ============================================================================

class YmmtcaScraper:
    """山本写真機店スクレイパー
    
    Features:
    - 複数URL（カテゴリ）対応
    - URL Index出力（master_controller連携）
    - Circuit Breaker Pattern
    - Exponential Backoff with Jitter
    - Rate Limiting（古いサーバー保護）
    """
    
    def __init__(
        self,
        target_urls: Sequence[str] = Constants.TARGET_URLS,
        circuit_breaker: Optional[CircuitBreaker] = None,
        retry_policy: Optional[RetryPolicy] = None,
        rate_limiter: Optional[RateLimiter] = None,
        logger: Optional[LoggerProtocol] = None,
        metrics: Optional[MetricsCollectorProtocol] = None,
    ):
        self._target_urls = target_urls
        self._logger = logger or StructuredLogger()
        self._metrics = metrics or InMemoryMetricsCollector()
        
        self._circuit_breaker = circuit_breaker or CircuitBreaker(
            logger=self._logger,
        )
        self._retry_policy = retry_policy or RetryPolicy(
            logger=self._logger,
        )
        self._rate_limiter = rate_limiter or RateLimiter()
        
        self._validator = ProductValidator()
        self._parser = YmmtcaHtmlParser(self._validator, self._logger)
        self._http_client = HttpClient(logger=self._logger)
    
    def scrape(self) -> ScrapeResult:
        """全URLスクレイピング"""
        correlation_id = str(uuid.uuid4())[:8]
        self._logger.set_correlation_id(correlation_id)
        
        start_time = time.time()
        all_products: List[ProductData] = []
        
        self._logger.info(f"スクレイピング開始: {len(self._target_urls)} URLs")
        
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
            
            for url_index, url in enumerate(self._target_urls):
                # URL Index出力（master_controller用）
                print(f"---URL_INDEX:{url_index}---")
                
                try:
                    products = self._scrape_single_url(url, url_index)
                    all_products.extend(products)
                    
                    # 各URL結果を出力
                    for product in products:
                        print(product.to_output_line())
                    
                except Exception as e:
                    self._logger.error(f"URL index {url_index} エラー: {e}")
                    continue
                
                # Rate Limiting（最後のURL以外）
                if url_index < len(self._target_urls) - 1:
                    self._rate_limiter.wait()
            
            duration = time.time() - start_time
            
            self._logger.info(
                f"スクレイピング完了: {len(all_products)}件取得 ({duration:.2f}秒)"
            )
            
            return ScrapeResult(
                success=True,
                products=all_products,
                duration_seconds=duration,
                exit_code=(
                    ScraperExitCode.SUCCESS if len(all_products) > 0
                    else ScraperExitCode.PARTIAL_SUCCESS
                ),
                correlation_id=correlation_id,
            )
            
        except Exception as e:
            self._logger.error(f"予期せぬエラー: {e}", exc_info=True)
            return ScrapeResult(
                success=False,
                products=all_products,
                error_message=str(e),
                duration_seconds=time.time() - start_time,
                exit_code=ScraperExitCode.FAILURE,
                correlation_id=correlation_id,
            )
        
        finally:
            self._http_client.close()
    
    def _scrape_single_url(
        self,
        url: str,
        url_index: int,
    ) -> List[ProductData]:
        """単一URLスクレイピング"""
        def _scrape() -> List[ProductData]:
            with self._circuit_breaker.protect():
                html = self._http_client.get(url)
                return self._parser.parse(html, url_index)
        
        return self._retry_policy.execute_with_retry(
            operation=_scrape,
            operation_name=f"scrape_url_{url_index}",
        )


# ============================================================================
# プレゼンテーション層: 出力フォーマッター
# ============================================================================

class OutputFormatter:
    """出力フォーマッター"""
    
    @staticmethod
    def print_summary(result: ScrapeResult) -> None:
        """サマリー出力"""
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
        name="ymmtca_scraper",
        level=getattr(logging, log_level.upper(), logging.INFO),
    )
    
    metrics = InMemoryMetricsCollector()
    
    scraper = YmmtcaScraper(
        logger=logger,
        metrics=metrics,
    )
    
    # スクレイピング実行（出力はscrape()内で行われる）
    result = scraper.scrape()
    
    # サマリー出力
    OutputFormatter.print_summary(result)
    
    return result.exit_code.value


if __name__ == "__main__":
    sys.exit(main())