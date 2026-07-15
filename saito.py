#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
saito.py - サイトウカメラ エンタープライズスクレイパー v3.0

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
v3.0 変更履歴 (2025-11-22)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔧 サイト構造変更対応
  - 旧: #listtb tr[onclick] セレクタ
  - 新: <tr><td> 内の <div> 構造に対応
  - 価格: &yen;XXXX 形式から抽出

実行方法:
    python saito.py

必要なライブラリ:
    pip install playwright beautifulsoup4
    playwright install chromium

【SLI/SLO定義】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- SLI: 商品取得成功率
- SLO: 99.5% (30日間ローリング)
- Error Budget: 0.5% (約3.6時間/月)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import re
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from pathlib import Path
from typing import (
    Any,
    Dict,
    Final,
    Generator,
    List,
    Optional,
    Sequence,
)

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import (
    Browser,
    BrowserContext,
    Page,
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

# ============================================================================
# 定数定義
# ============================================================================

class Constants:
    """アプリケーション定数"""
    
    # サイト情報
    BASE_URL: Final[str] = "https://www.saito-camera.com/"
    
    # shop_config.jsonで指定されているURL（新着順 sr=-12）
    DEFAULT_START_URL: Final[str] = (
        "https://www.saito-camera.com/list.php?"
        "478778882&mk=&ct=&sh=1&cd=0&pl=&ph=&w=&sr=-12&re=&p=0&282274156"
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
    
    # タイムアウト設定
    PAGE_LOAD_TIMEOUT_MS: Final[int] = 60000
    NAVIGATION_TIMEOUT_MS: Final[int] = 30000
    
    # Rate Limiting
    MIN_REQUEST_INTERVAL_SECONDS: Final[float] = 2.0
    MAX_REQUEST_INTERVAL_SECONDS: Final[float] = 5.0
    
    # 価格バリデーション
    MIN_VALID_PRICE: Final[int] = 100
    MAX_VALID_PRICE: Final[int] = 50_000_000
    
    # 商品名バリデーション
    MIN_PRODUCT_NAME_LENGTH: Final[int] = 3
    MAX_PRODUCT_NAME_LENGTH: Final[int] = 500
    
    # 状態ファイル
    STATE_FILE: Final[str] = "saito_circuit_state.json"


# ============================================================================
# Enums
# ============================================================================

class CircuitState(Enum):
    """Circuit Breakerの状態"""
    CLOSED = auto()      # 正常動作
    OPEN = auto()        # 遮断中
    HALF_OPEN = auto()   # 試験的に開放


class ProductCondition(Enum):
    """商品状態"""
    NEW = "新品"
    USED = "中古"
    UNKNOWN = "不明"


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
    """商品データ（イミュータブル）
    
    Attributes:
        name: 商品名
        price: 価格（円）
        condition: 商品状態
        url: 商品詳細URL
        image_url: 商品画像URL
        product_hash: 商品識別ハッシュ
        scraped_at: スクレイピング日時
        rank: サイト上の表示順位（1始まり）
    """
    name: str
    price: int
    condition: ProductCondition = ProductCondition.UNKNOWN
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
        condition: ProductCondition = ProductCondition.UNKNOWN,
        url: str = "",
        image_url: str = "",
        rank: int = 0,
    ) -> ProductData:
        """ファクトリメソッド（ハッシュ自動生成）"""
        product_hash = cls._generate_hash(name, price)
        return cls(
            name=name,
            price=price,
            condition=condition,
            url=url,
            image_url=image_url,
            product_hash=product_hash,
            scraped_at=datetime.now(),
            rank=rank,
        )
    
    @staticmethod
    def _generate_hash(name: str, price: int) -> str:
        """商品識別ハッシュ生成"""
        key = f"{name}_{price}"
        return hashlib.md5(key.encode("utf-8")).hexdigest()[:8]
    
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
    
    def to_dict(self) -> Dict[str, Any]:
        """辞書に変換"""
        return {
            'state': self.state.name,
            'failure_count': self.failure_count,
            'last_failure_time': self.last_failure_time.isoformat() if self.last_failure_time else None,
            'half_open_call_count': self.half_open_call_count
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CircuitBreakerState:
        """辞書から生成"""
        last_failure = data.get('last_failure_time')
        return cls(
            state=CircuitState[data.get('state', 'CLOSED')],
            failure_count=data.get('failure_count', 0),
            last_failure_time=datetime.fromisoformat(last_failure) if last_failure else None,
            half_open_call_count=data.get('half_open_call_count', 0)
        )


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
# ユーティリティ関数
# ============================================================================

@contextmanager
def atomic_write(filepath: Path) -> Generator[Path, None, None]:
    """アトミックなファイル書き込み（破損防止）"""
    temp_fd, temp_path = tempfile.mkstemp(
        dir=filepath.parent,
        prefix=f".{filepath.name}.",
        suffix=".tmp"
    )
    
    temp_filepath = Path(temp_path)
    
    try:
        os.close(temp_fd)
        yield temp_filepath
        temp_filepath.replace(filepath)
    except Exception:
        if temp_filepath.exists():
            temp_filepath.unlink()
        raise


# ============================================================================
# ロギング
# ============================================================================

class StructuredLogger:
    """構造化ロガー（JSON形式対応）"""
    
    def __init__(
        self,
        name: str = "saito_scraper",
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
            
            if use_json:
                formatter = logging.Formatter(
                    '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
                    '"logger": "%(name)s", "message": "%(message)s"}'
                )
            else:
                formatter = logging.Formatter(
                    "%(asctime)s [%(levelname)s] %(message)s"
                )
            
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)
    
    def set_correlation_id(self, correlation_id: str) -> None:
        """Correlation ID設定"""
        self._correlation_id = correlation_id
    
    def _format_message(self, msg: str) -> str:
        """メッセージフォーマット"""
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
# Circuit Breaker
# ============================================================================

class CircuitBreaker:
    """Circuit Breaker実装（状態永続化対応）"""
    
    def __init__(
        self,
        failure_threshold: int = Constants.CB_FAILURE_THRESHOLD,
        recovery_timeout: float = Constants.CB_RECOVERY_TIMEOUT_SECONDS,
        half_open_max_calls: int = Constants.CB_HALF_OPEN_MAX_CALLS,
        logger: Optional[StructuredLogger] = None,
    ):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max_calls = half_open_max_calls
        self._logger = logger or StructuredLogger()
        self._state = CircuitBreakerState()
        
        # 状態読み込み
        self._load_state()
    
    def _load_state(self) -> None:
        """状態を永続化ファイルから読み込み"""
        state_file = Path(Constants.STATE_FILE)
        if not state_file.exists():
            return
        
        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self._state = CircuitBreakerState.from_dict(data)
            self._logger.info(f"Circuit Breaker状態読み込み: {self._state.state.name}")
        except Exception as e:
            self._logger.warning(f"Circuit Breaker状態読み込みエラー: {e}")
    
    def _save_state(self) -> None:
        """状態を永続化ファイルに保存"""
        state_file = Path(Constants.STATE_FILE)
        
        try:
            with atomic_write(state_file) as temp_path:
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(self._state.to_dict(), f, ensure_ascii=False, indent=2)
        except Exception as e:
            self._logger.warning(f"Circuit Breaker状態保存エラー: {e}")
    
    def can_execute(self) -> bool:
        """実行可能かチェック"""
        current_state = self._state.state
        
        if current_state == CircuitState.CLOSED:
            return True
        
        if current_state == CircuitState.OPEN:
            # 回復タイムアウト経過をチェック
            if self._state.last_failure_time:
                elapsed = (datetime.now() - self._state.last_failure_time).total_seconds()
                if elapsed >= self._recovery_timeout:
                    self._logger.info("Circuit Breaker: OPEN -> HALF_OPEN")
                    self._state.state = CircuitState.HALF_OPEN
                    self._state.half_open_call_count = 0
                    self._save_state()
                    return True
            return False
        
        if current_state == CircuitState.HALF_OPEN:
            return self._state.half_open_call_count < self._half_open_max_calls
        
        return False
    
    def record_success(self) -> None:
        """成功を記録"""
        if self._state.state == CircuitState.HALF_OPEN:
            self._state.half_open_call_count += 1
            if self._state.half_open_call_count >= self._half_open_max_calls:
                self._logger.info("Circuit Breaker: HALF_OPEN -> CLOSED")
                self._state.state = CircuitState.CLOSED
                self._state.failure_count = 0
        else:
            self._state.failure_count = 0
        
        self._save_state()
    
    def record_failure(self) -> None:
        """失敗を記録"""
        self._state.failure_count += 1
        self._state.last_failure_time = datetime.now()
        
        if self._state.state == CircuitState.HALF_OPEN:
            self._logger.warning("Circuit Breaker: HALF_OPEN -> OPEN (失敗)")
            self._state.state = CircuitState.OPEN
        elif self._state.failure_count >= self._failure_threshold:
            self._logger.warning(
                f"Circuit Breaker: CLOSED -> OPEN "
                f"(失敗回数: {self._state.failure_count})"
            )
            self._state.state = CircuitState.OPEN
        
        self._save_state()
    
    @contextmanager
    def protect(self) -> Generator[None, None, None]:
        """保護コンテキスト"""
        try:
            yield
            self.record_success()
        except Exception:
            self.record_failure()
            raise


# ============================================================================
# リトライポリシー
# ============================================================================

class RetryPolicy:
    """Exponential Backoff with Jitter"""
    
    def __init__(
        self,
        max_attempts: int = Constants.RETRY_MAX_ATTEMPTS,
        base_delay: float = Constants.RETRY_BASE_DELAY_SECONDS,
        max_delay: float = Constants.RETRY_MAX_DELAY_SECONDS,
        jitter_factor: float = Constants.RETRY_JITTER_FACTOR,
        logger: Optional[StructuredLogger] = None,
    ):
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._jitter_factor = jitter_factor
        self._logger = logger or StructuredLogger()
    
    def calculate_delay(self, attempt: int) -> float:
        """遅延時間計算（Exponential Backoff with Jitter）"""
        delay = min(
            self._base_delay * (Constants.RETRY_EXPONENTIAL_BASE ** (attempt - 1)),
            self._max_delay
        )
        
        # Jitter追加
        jitter = delay * self._jitter_factor * (random.random() * 2 - 1)
        return max(0.1, delay + jitter)
    
    def execute_with_retry(
        self,
        operation: callable,
        operation_name: str = "operation",
    ) -> Any:
        """リトライ付き実行"""
        last_exception: Optional[Exception] = None
        
        for attempt in range(1, self._max_attempts + 1):
            try:
                self._logger.debug(f"{operation_name}: 試行 {attempt}/{self._max_attempts}")
                return operation()
            
            except Exception as e:
                last_exception = e
                self._logger.warning(
                    f"{operation_name}: 試行 {attempt}/{self._max_attempts} 失敗 - {e}"
                )
                
                if attempt < self._max_attempts:
                    delay = self.calculate_delay(attempt)
                    self._logger.info(f"{delay:.2f}秒後にリトライ...")
                    time.sleep(delay)
        
        raise RetryExhaustedException(
            f"{operation_name}: {self._max_attempts}回の試行が全て失敗"
        ) from last_exception


# ============================================================================
# Rate Limiter
# ============================================================================

class RateLimiter:
    """リクエストレート制限"""
    
    def __init__(
        self,
        min_interval: float = Constants.MIN_REQUEST_INTERVAL_SECONDS,
        max_interval: float = Constants.MAX_REQUEST_INTERVAL_SECONDS,
    ):
        self._min_interval = min_interval
        self._max_interval = max_interval
        self._last_request_time: Optional[datetime] = None
    
    def wait(self) -> None:
        """必要なら待機"""
        if self._last_request_time is None:
            self._last_request_time = datetime.now()
            return
        
        target_interval = random.uniform(self._min_interval, self._max_interval)
        elapsed = (datetime.now() - self._last_request_time).total_seconds()
        
        if elapsed < target_interval:
            sleep_time = target_interval - elapsed
            time.sleep(sleep_time)
        
        self._last_request_time = datetime.now()


# ============================================================================
# HTMLパーサー（新構造対応 v3.0）
# ============================================================================

class ProductValidator:
    """商品データバリデーター"""
    
    @staticmethod
    def validate_price(price_text: str) -> Optional[int]:
        """価格バリデーション
        
        対応形式:
        - &yen;4,800
        - ¥4,800
        - 4,800円
        - 4800
        """
        # &yen; や ¥ を除去し、数字とカンマのみ抽出
        cleaned = price_text.replace('&yen;', '').replace('¥', '').replace('円', '')
        match = re.search(r"([0-9,]+)", cleaned)
        if not match:
            return None
        
        try:
            price = int(match.group(1).replace(",", ""))
            
            if not (Constants.MIN_VALID_PRICE <= price <= Constants.MAX_VALID_PRICE):
                return None
            
            return price
        except ValueError:
            return None
    
    @staticmethod
    def validate_name(name: str) -> Optional[str]:
        """商品名バリデーション"""
        # 空白正規化
        name = re.sub(r"\s+", " ", name).strip()
        # <br>タグの残骸を除去
        name = re.sub(r"<br\s*/?>", " ", name, flags=re.IGNORECASE)
        name = name.strip()
        
        # 長さチェック
        if not (Constants.MIN_PRODUCT_NAME_LENGTH <= len(name) <= Constants.MAX_PRODUCT_NAME_LENGTH):
            return None
        
        return name
    
    @staticmethod
    def parse_condition(condition_text: str) -> ProductCondition:
        """商品状態パース"""
        if "新品" in condition_text:
            return ProductCondition.NEW
        elif "中古" in condition_text:
            return ProductCondition.USED
        return ProductCondition.UNKNOWN


class SaitoHtmlParser:
    """サイトウカメラHTML解析器 v3.0
    
    【v3.0 変更点】
    サイト構造が変更されたため、パースロジックを全面改修
    
    新構造:
    <tr>
      <td>
        <a href="det.php?...&id=XXXXX">
          <img src="./dat/img/XXXXX_0m.jpg" alt="商品名" />
        </a>
        <div>
          <span class="i_chuko">中古並品</span>
          メーカー<br />
          商品名<br />
          &yen;価格
        </div>
      </td>
      ...
    </tr>
    """
    
    def __init__(
        self,
        validator: ProductValidator,
        logger: Optional[StructuredLogger] = None,
    ):
        self._validator = validator
        self._logger = logger or StructuredLogger()
    
    def parse(self, html: str) -> List[ProductData]:
        """HTML解析・商品抽出
        
        Args:
            html: ページHTML
            
        Returns:
            商品リスト（サイト表示順）
        """
        soup = BeautifulSoup(html, "html.parser")
        products: List[ProductData] = []
        
        # 新構造: <td> 内に <a> と <div> がある構造
        # 商品セル: <a href="det.php?..."> を含む <td>
        product_cells = soup.find_all("td")
        
        rank = 1
        for cell in product_cells:
            product = self._parse_cell(cell, rank)
            if product:
                products.append(product)
                rank += 1
        
        self._logger.info(f"商品抽出: {len(products)}件")
        return products
    
    def _parse_cell(self, cell: Tag, rank: int) -> Optional[ProductData]:
        """セル解析
        
        構造:
        <td>
          <a href="det.php?...&id=XXXXX">
            <img src="./dat/img/XXXXX_0m.jpg" alt="商品名" />
          </a>
          <div>
            <span class="i_chuko">中古並品</span>
            メーカー<br />
            商品名<br />
            &yen;価格
          </div>
        </td>
        """
        # 商品リンクを探す
        link = cell.find("a", href=re.compile(r"det\.php\?.*id=\d+"))
        if not link:
            return None
        
        # 商品情報を含む div を探す
        info_div = cell.find("div")
        if not info_div:
            return None
        
        # 商品URL抽出
        url = self._extract_url(link)
        
        # 画像URL抽出
        img = link.find("img")
        image_url = ""
        img_alt = ""
        if img:
            image_url = self._extract_image_url(img)
            img_alt = img.get("alt", "")
        
        # 商品状態抽出
        condition_span = info_div.find("span", class_=re.compile(r"i_(chuko|shinpin)"))
        condition_text = condition_span.get_text(strip=True) if condition_span else ""
        condition = self._validator.parse_condition(condition_text)
        
        # div内のテキストを解析
        # 構造: [状態] メーカー<br>商品名<br>&yen;価格
        div_html = str(info_div)
        
        # 価格抽出 (&yen;XXX,XXX または ¥XXX,XXX)
        price_match = re.search(r'[&yen;¥]([\d,]+)', div_html)
        if not price_match:
            return None
        
        price = self._validator.validate_price(price_match.group(0))
        if price is None:
            return None
        
        # 商品名抽出
        # 方法1: imgのalt属性から
        name = img_alt
        
        # 方法2: alt属性がない場合、div内のテキストから
        if not name or len(name) < 3:
            name = self._extract_name_from_div(info_div)
        
        name = self._validator.validate_name(name)
        if not name:
            return None
        
        return ProductData.create(
            name=name,
            price=price,
            condition=condition,
            url=url,
            image_url=image_url,
            rank=rank,
        )
    
    def _extract_name_from_div(self, div: Tag) -> str:
        """div内のテキストから商品名を抽出
        
        構造: [span]状態[/span]メーカー<br>商品名<br>&yen;価格
        """
        # span(状態)を除去したテキストを取得
        for span in div.find_all("span"):
            span.decompose()
        
        # テキスト取得
        text = div.get_text(separator="\n", strip=True)
        
        # 価格行を除去
        lines = [line for line in text.split("\n") 
                 if line and not re.match(r'^[¥&]|^\d+,?\d*$', line.strip())]
        
        if len(lines) >= 2:
            # メーカー + 商品名
            maker = lines[0].strip()
            product_name = lines[1].strip()
            return f"{maker} {product_name}".strip()
        elif len(lines) == 1:
            return lines[0].strip()
        
        return ""
    
    def _extract_url(self, link: Tag) -> str:
        """商品URL抽出"""
        href = link.get("href", "")
        if href:
            if href.startswith("./"):
                return Constants.BASE_URL + href[2:]
            elif not href.startswith("http"):
                return Constants.BASE_URL + href
            return href
        return ""
    
    def _extract_image_url(self, img: Tag) -> str:
        """画像URL抽出"""
        src = img.get("src", "")
        if src:
            # クエリパラメータを除去
            src = src.split("?")[0]
            if src.startswith("./"):
                return Constants.BASE_URL + src[2:]
            elif not src.startswith("http"):
                return Constants.BASE_URL + src
            return src
        return ""


# ============================================================================
# Playwright Manager
# ============================================================================

class PlaywrightManager:
    """Playwrightリソース管理"""
    
    def __init__(self, logger: Optional[StructuredLogger] = None, headless: bool = False):
        self._logger = logger or StructuredLogger()
        self._headless = headless
    
    @contextmanager
    def browser_context(self) -> Generator[BrowserContext, None, None]:
        """ブラウザコンテキスト管理"""
        playwright = None
        browser = None
        context = None
        
        try:
            playwright = sync_playwright().start()
            
            # VPS版: headless=False でBot検出回避
            # ローカルテスト時は headless=True に変更可能
            browser = playwright.chromium.launch(
                headless=self._headless,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--disable-gpu',
                    '--window-size=1920,1080',
                    '--start-maximized',
                ]
            )
            
            # Bot検出回避の強化設定
            context = browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                ),
                viewport={'width': 1920, 'height': 1080},
                locale='ja-JP',
                timezone_id='Asia/Tokyo',
                # 追加のBot検出回避設定
                java_script_enabled=True,
                has_touch=False,
                is_mobile=False,
                device_scale_factor=1,
            )
            
            # WebDriver検出を回避するスクリプト
            context.add_init_script("""
                // webdriver プロパティを隠す
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // Chrome検出を回避
                window.chrome = {
                    runtime: {},
                };
                
                // Permissions API を偽装
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
                
                // plugins を偽装
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                
                // languages を偽装
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['ja-JP', 'ja', 'en-US', 'en'],
                });
            """)
            
            self._logger.info(f"ブラウザ起動: headless={self._headless}")
            
            yield context
            
        finally:
            if context:
                try:
                    context.close()
                except Exception as e:
                    self._logger.warning(f"Context close error: {e}")
            
            if browser:
                try:
                    browser.close()
                except Exception as e:
                    self._logger.warning(f"Browser close error: {e}")
            
            if playwright:
                try:
                    playwright.stop()
                except Exception as e:
                    self._logger.warning(f"Playwright stop error: {e}")


# ============================================================================
# メインスクレイパー
# ============================================================================

class SaitoCameraScraper:
    """サイトウカメラスクレイパー v3.0"""
    
    def __init__(
        self,
        start_url: str = Constants.DEFAULT_START_URL,
        circuit_breaker: Optional[CircuitBreaker] = None,
        retry_policy: Optional[RetryPolicy] = None,
        rate_limiter: Optional[RateLimiter] = None,
        logger: Optional[StructuredLogger] = None,
        headless: bool = False,  # VPS版: デフォルトはheadless=False
    ):
        self._start_url = start_url
        self._logger = logger or StructuredLogger()
        
        # 依存性注入
        self._circuit_breaker = circuit_breaker or CircuitBreaker(logger=self._logger)
        self._retry_policy = retry_policy or RetryPolicy(logger=self._logger)
        self._rate_limiter = rate_limiter or RateLimiter()
        
        # コンポーネント
        self._validator = ProductValidator()
        self._parser = SaitoHtmlParser(self._validator, self._logger)
        self._playwright_manager = PlaywrightManager(logger=self._logger, headless=headless)
    
    def scrape(self) -> ScrapeResult:
        """スクレイピング実行"""
        correlation_id = str(uuid.uuid4())[:8]
        self._logger.set_correlation_id(correlation_id)
        
        start_time = time.time()
        
        self._logger.info(f"スクレイピング開始: {self._start_url}")
        
        try:
            # Circuit Breaker チェック
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
            
            # リトライ付き実行
            products = self._retry_policy.execute_with_retry(
                operation=self._scrape_with_protection,
                operation_name="scrape_pages",
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
            self._logger.error(f"予期せぬエラー: {e}")
            return ScrapeResult(
                success=False,
                products=[],
                error_message=str(e),
                duration_seconds=time.time() - start_time,
                exit_code=ScraperExitCode.FAILURE,
                correlation_id=correlation_id,
            )
    
    def _scrape_with_protection(self) -> List[ProductData]:
        """保護付きスクレイピング"""
        with self._circuit_breaker.protect():
            return self._scrape_all_pages()
    
    def _scrape_all_pages(self) -> List[ProductData]:
        """全ページスクレイピング"""
        all_products: List[ProductData] = []
        
        with self._playwright_manager.browser_context() as context:
            page = context.new_page()
            
            # 1ページ目取得
            self._rate_limiter.wait()
            self._logger.info(f"ページアクセス: {self._start_url}")
            
            # レスポンスを取得してステータスコードをログ出力
            response = page.goto(self._start_url, timeout=Constants.PAGE_LOAD_TIMEOUT_MS)
            
            # HTTPステータスコードをログ出力
            if response:
                status_code = response.status
                status_text = response.status_text
                self._logger.info(f"HTTPステータス: {status_code} {status_text}")
                
                # 異常ステータスの場合は警告
                if status_code >= 400:
                    self._logger.warning(f"⚠️ HTTPエラー: {status_code} {status_text}")
                elif status_code >= 300:
                    self._logger.info(f"リダイレクト: {status_code} → {response.url}")
            else:
                self._logger.warning("⚠️ レスポンスなし（response is None）")
            
            page.wait_for_load_state("networkidle", timeout=Constants.NAVIGATION_TIMEOUT_MS)
            
            html = page.content()
            
            # HTMLサイズをログ出力（デバッグ用）
            html_size = len(html)
            self._logger.info(f"HTMLサイズ: {html_size:,} bytes")
            
            # 商品抽出
            products = self._parser.parse(html)
            all_products.extend(products)
            
            self._logger.info(f"1ページ目: {len(products)}件取得")
            
            # 0件の場合は追加情報をログ出力
            if len(products) == 0:
                self._logger.warning("⚠️ 商品が0件です。サイト構造が変更された可能性があります。")
                # HTMLの一部をデバッグ出力（最初の1000文字）
                self._logger.debug(f"HTML先頭1000文字: {html[:1000]}")
            
            page.close()
        
        return all_products


# ============================================================================
# 出力フォーマッター
# ============================================================================

class OutputFormatter:
    """出力フォーマッター（master_controller互換）"""
    
    @staticmethod
    def print_results(result: ScrapeResult) -> None:
        """結果出力"""
        for product in result.products:
            print(product.to_output_line())
        
        # ステータス出力
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
    """メイン関数
    
    環境変数:
        LOG_LEVEL: ログレベル (DEBUG, INFO, WARNING, ERROR)
        USE_JSON_LOGS: JSON形式ログ出力 (true/false)
        HEADLESS: ヘッドレスモード (true/false) - デフォルトはfalse（VPS版）
    """
    # 環境変数から設定読み込み
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    use_json_logs = os.environ.get("USE_JSON_LOGS", "false").lower() == "true"
    
    # ヘッドレスモード設定
    # VPS版: デフォルトはheadless=False（Bot検出回避）
    # ローカルテスト: HEADLESS=true で headless=True に設定可能
    headless = os.environ.get("HEADLESS", "false").lower() == "true"
    
    # ロガー初期化
    logger = StructuredLogger(
        name="saito_scraper",
        level=getattr(logging, log_level.upper(), logging.INFO),
        use_json=use_json_logs,
    )
    
    logger.info(f"設定: headless={headless}, log_level={log_level}")
    
    # スクレイパー初期化・実行
    scraper = SaitoCameraScraper(logger=logger, headless=headless)
    result = scraper.scrape()
    
    # 結果出力
    OutputFormatter.print_results(result)
    
    return result.exit_code.value


if __name__ == "__main__":
    sys.exit(main())