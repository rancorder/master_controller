#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Controller v27 - P1個別スナップショット対応版（リファクタリング済）

【v27改善点】
🔧 P1スクリプト用の個別JSONファイル化（競合解消）
🔧 snapshotsディレクトリへの統合配置
🔧 P2は共有ファイル維持（p2_shared.json）
🔧 全URLタイムスタンプ監視（30分ごと）
🔧 ハードオフ専用フォーマット対応

【ファイル構成】
snapshots/
  p1_{script}_{url_index}.json  ← P1個別
  p2_shared.json                ← P2共有
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import unicodedata
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from functools import lru_cache
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Final

import pandas as pd
import requests

# ==================== 環境変数設定 ====================
os.environ['PYTHONIOENCODING'] = 'utf-8'
os.environ['PYTHONUNBUFFERED'] = '1'
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

# ==================== 設定定数 ====================
SUBPROCESS_TIMEOUT: Final[int] = int(os.getenv('SCRAPER_TIMEOUT', '120'))
HTTP_TIMEOUT: Final[int] = int(os.getenv('HTTP_TIMEOUT', '10'))
DB_TIMEOUT: Final[float] = 30.0

USE_SQLITE_HISTORY: Final[bool] = True

# ==================== スナップショットディレクトリ ====================
SNAPSHOTS_DIR: Final[Path] = Path("snapshots")
P2_SHARED_SNAPSHOT: Final[str] = "p2_shared.json"

# ==================== ログ設定 ====================
log_handler_file = RotatingFileHandler(
    'master_controller.log',
    maxBytes=10*1024*1024,
    backupCount=2,
    encoding='utf-8'
)
log_handler_file.setFormatter(
    logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
)

log_handler_stdout = logging.StreamHandler(sys.stdout)
log_handler_stdout.setFormatter(
    logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
)

logging.basicConfig(
    level=logging.INFO,
    handlers=[log_handler_file, log_handler_stdout]
)

LOGGER = logging.getLogger('MasterController')


# ==================== エラーハンドリング統一 ====================

class ErrorSeverity(Enum):
    """エラー重要度の分類"""
    RECOVERABLE = auto()
    EXPECTED = auto()
    FATAL = auto()


class ErrorHandler:
    """エラー処理戦略パターン"""
    
    @staticmethod
    def handle(error: Exception, context: str, severity: ErrorSeverity) -> None:
        if severity == ErrorSeverity.RECOVERABLE:
            LOGGER.warning(f"[{context}] リトライ可能エラー: {error}")
        elif severity == ErrorSeverity.EXPECTED:
            LOGGER.info(f"[{context}] 想定内エラー: {error}")
        elif severity == ErrorSeverity.FATAL:
            LOGGER.critical(f"[{context}] 致命的エラー: {error}", exc_info=True)
            sys.exit(1)


# ==================== データクラス ====================

class TimeSlot(Enum):
    """時間帯の分類"""
    DAYTIME = "daytime"
    NIGHTTIME = "nighttime"


class Priority(Enum):
    """スクリプト優先度"""
    HIGH = 1
    LOW = 2


@dataclass(frozen=True)
class ScraperConfig:
    """スクレイパー設定データクラス"""
    py_file: str
    display_name: str
    category: str
    scraping_url: str
    url_index: int
    priority: Priority
    is_active: bool
    notification_room_ids: Optional[str] = None


@dataclass(frozen=True)
class ProductData:
    """商品データクラス"""
    name: str
    price: str
    site_name: str
    url: str
    url_index: int
    img_url: str = ""


@dataclass
class ExecutionResult:
    """実行結果データクラス"""
    success: bool
    duration: float = 0.0
    error: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SnapshotStatus:
    """スナップショット状態データクラス"""
    site: str
    priority: str
    elapsed_minutes: float
    is_fresh: bool
    file: str


# ==================== 時間帯管理 ====================

class TimeManager:
    """時間帯管理クラス"""
    
    NIGHT_START_HOUR: Final[int] = 1
    NIGHT_END_HOUR: Final[int] = 8
    NIGHT_INTERVAL_SECONDS: Final[int] = 1800
    
    @classmethod
    def get_current_timeslot(cls) -> TimeSlot:
        current_hour = datetime.now().hour
        if cls.NIGHT_START_HOUR <= current_hour < cls.NIGHT_END_HOUR:
            return TimeSlot.NIGHTTIME
        return TimeSlot.DAYTIME
    
    @classmethod
    def is_nighttime(cls) -> bool:
        return cls.get_current_timeslot() == TimeSlot.NIGHTTIME
    
    @classmethod
    def get_interval_for_priority1(cls, idle_seconds: float, is_night: bool) -> int:
        if is_night:
            return cls.NIGHT_INTERVAL_SECONDS
        if idle_seconds < 1800:
            return 60
        elif idle_seconds < 3600:
            return 300
        else:
            return 3600
    
    @classmethod
    def get_interval_for_priority2(cls, is_night: bool) -> int:
        if is_night:
            return cls.NIGHT_INTERVAL_SECONDS
        return 300


# ==================== データ抽出 ====================

class StableDataExtractor:
    """標準出力からの商品データ抽出器"""
    
    SKIP_KEYWORDS: Final[List[str]] = [
        'info', 'error', 'debug', 'warning', 'log', 'traceback',
        'selenium', 'driver', 'browser', 'playwright'
    ]
    
    PRICE_PATTERNS: Final[List[re.Pattern]] = [
        re.compile(r'([0-9,]+)\s*円'),
        re.compile(r'¥\s*([0-9,]+)'),
        re.compile(r'(\d{4,})\s*円')
    ]
    
    MAX_OUTPUT_SIZE: Final[int] = 1_000_000
    
    def __init__(self, logger: logging.Logger = LOGGER) -> None:
        self.logger = logger
    
    def extract_stable(self, output: str, script_name: str) -> Dict[str, Any]:
        if len(output) > self.MAX_OUTPUT_SIZE:
            raise ValueError(f"出力サイズ超過: {len(output)} > {self.MAX_OUTPUT_SIZE}")
        
        if not output or len(output.strip()) < 10:
            return {'count': 0, 'success': False, 'products': []}
        
        script_display = script_name.replace('.py', '')
        products: List[Dict[str, Any]] = []
        lines = output.split('\n')
        current_url_index = 0
        
        for line in lines:
            line = line.strip()
            if len(line) < 10:
                continue
            
            if line.startswith("---URL_INDEX:"):
                match = re.search(r'---URL_INDEX:(\d+)---', line)
                if match:
                    current_url_index = int(match.group(1))
                continue
            
            if self._should_skip_line(line):
                continue
            
            product = self._extract_product_from_line(
                line, script_display, current_url_index
            )
            if product:
                products.append(product)
        
        product_count = len(products)
        success = product_count > 0
        
        if success:
            self.logger.info(f"[{script_display}] データ取得成功: {product_count}件")
        else:
            self.logger.warning(f"[{script_display}] データ取得0件")
        
        return {
            'count': product_count,
            'success': success,
            'products': products
        }
    
    def _should_skip_line(self, line: str) -> bool:
        line_lower = line.lower()
        return any(word in line_lower for word in self.SKIP_KEYWORDS)
    
    def _extract_product_from_line(
        self, line: str, site_name: str, url_index: int
    ) -> Optional[Dict[str, Any]]:
        img_url = ""
        name_price_part = line
        
        if '||' in line:
            parts = line.split('||')
            name_price_part = parts[0]
            img_url = parts[1] if len(parts) > 1 else ""
        
        price = self._extract_price(name_price_part)
        if not price:
            return None
        
        product_name = self._extract_product_name(name_price_part)
        if len(product_name) <= 3:
            return None
        
        return {
            'name': product_name[:200],
            'price': str(price),
            'site_name': site_name,
            'url': 'N/A',
            'url_index': url_index,
            'img_url': img_url
        }
    
    def _extract_price(self, text: str) -> Optional[int]:
        for pattern in self.PRICE_PATTERNS:
            match = pattern.search(text)
            if match:
                price_text = match.group(1).replace(',', '')
                try:
                    price = int(price_text)
                    if 100 <= price <= 10_000_000:
                        return price
                except ValueError:
                    continue
        return None
    
    def _extract_product_name(self, text: str) -> str:
        product_name = text
        for pattern in self.PRICE_PATTERNS:
            product_name = pattern.sub('', product_name)
        product_name = re.sub(r'\s+', ' ', product_name).strip()
        product_name = re.sub(r'[|│]+', ' ', product_name).strip()
        return product_name


# ==================== 通知履歴管理 ====================

class NotificationHistoryManager:
    """通知履歴管理(抽象基底クラス)"""
    
    def should_notify(self, product_key: str, cooldown_hours: int = 6) -> bool:
        raise NotImplementedError
    
    def add_notification(self, product_key: str, site_name: str) -> None:
        raise NotImplementedError
    
    def cleanup(self, retention_hours: int = 24) -> int:
        raise NotImplementedError


class SQLiteNotificationHistory(NotificationHistoryManager):
    """SQLite形式の通知履歴"""
    
    def __init__(
        self,
        db_path: str = "notification_history.db",
        logger: logging.Logger = LOGGER
    ) -> None:
        self.db_path = db_path
        self.logger = logger
        self._init_db()
    
    def _init_db(self) -> None:
        conn = None
        try:
            conn = sqlite3.connect(
                self.db_path,
                timeout=DB_TIMEOUT,
                isolation_level=None,
                check_same_thread=False
            )
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")
            conn.execute("BEGIN")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    product_key TEXT PRIMARY KEY,
                    site_name TEXT NOT NULL,
                    notified_at TIMESTAMP NOT NULL
                ) WITHOUT ROWID
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_notified_at
                ON notifications(notified_at)
            """)
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
        
        self.logger.info("SQLite通知履歴初期化完了(WALモード有効)")
    
    @contextmanager
    def _get_connection(self):
        conn = None
        max_retries = 10
        base_delay = 0.05
        
        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(
                    self.db_path,
                    timeout=DB_TIMEOUT,
                    isolation_level='DEFERRED',
                    detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                    check_same_thread=False
                )
                sqlite3.register_adapter(datetime, lambda val: val.isoformat())
                sqlite3.register_converter(
                    "TIMESTAMP",
                    lambda val: datetime.fromisoformat(val.decode())
                )
                conn.execute("PRAGMA busy_timeout=30000")
                conn.execute("BEGIN")
                yield conn
                conn.commit()
                return
            except sqlite3.OperationalError as e:
                if conn:
                    try:
                        conn.rollback()
                    except:
                        pass
                if "locked" in str(e).lower() and attempt < max_retries - 1:
                    jitter = random.uniform(0, 0.1)
                    wait_time = min(base_delay * (2 ** attempt) + jitter, 5.0)
                    self.logger.warning(
                        f"SQLite locked (試行{attempt+1}/{max_retries}): "
                        f"{wait_time:.2f}秒後にリトライ"
                    )
                    time.sleep(wait_time)
                    continue
                raise
            except Exception:
                if conn:
                    try:
                        conn.rollback()
                    except:
                        pass
                raise
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
    
    def should_notify(self, product_key: str, cooldown_hours: int = 6) -> bool:
        cutoff = datetime.now() - timedelta(hours=cooldown_hours)
        try:
            with self._get_connection() as conn:
                result = conn.execute(
                    "SELECT notified_at FROM notifications "
                    "WHERE product_key = ? AND notified_at > ?",
                    (product_key, cutoff)
                ).fetchone()
                return result is None
        except Exception as e:
            ErrorHandler.handle(e, "通知判定", ErrorSeverity.EXPECTED)
            return False
    
    def add_notification(self, product_key: str, site_name: str) -> None:
        try:
            with self._get_connection() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO notifications "
                    "(product_key, site_name, notified_at) VALUES (?, ?, ?)",
                    (product_key, site_name, datetime.now())
                )
        except Exception as e:
            ErrorHandler.handle(e, "通知履歴追加", ErrorSeverity.RECOVERABLE)
    
    def cleanup(self, retention_hours: int = 24) -> int:
        cutoff = datetime.now() - timedelta(hours=retention_hours)
        try:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    "DELETE FROM notifications WHERE notified_at < ?",
                    (cutoff,)
                )
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    self.logger.debug(f"🗑️ 古い通知履歴削除: {deleted_count}件")
                if deleted_count > 100:
                    conn.execute("VACUUM")
                return deleted_count
        except Exception as e:
            ErrorHandler.handle(e, "通知履歴クリーンアップ", ErrorSeverity.EXPECTED)
            return 0


# ==================== 差分検知システム(v26改修版) ====================

class SimpleMemoryDiffSystem:
    """メモリベース差分検知システム(v26: P1個別ファイル対応)"""
    
    NOTIFICATION_COOLDOWN_HOURS: Final[int] = 6
    CLEANUP_THRESHOLD_HOURS: Final[int] = 24
    SNAPSHOT_THRESHOLD_MINUTES: Final[int] = 30
    
    NOISE_WORDS: Final[List[str]] = ['新着!!', '新着', '値下', '美品', '極上品', '良品', '並品']
    
    def __init__(
        self,
        snapshot_dir: Path = SNAPSHOTS_DIR,
        logger: logging.Logger = LOGGER
    ) -> None:
        self.snapshot_dir = snapshot_dir
        self.logger = logger
        self.last_snapshots: Dict[str, Dict[str, Any]] = {}
        self.file_locks: Dict[str, threading.RLock] = {}
        self.global_lock = threading.RLock()
        
        self.notification_manager = SQLiteNotificationHistory(logger=logger)
        
        # 正規表現を事前コンパイル
        self.BRACKET_PATTERN = re.compile(r'[\[「\(].*?[\]」\)]')
        self.NONWORD_PATTERN = re.compile(r'[^\w]')
        noise_escaped = '|'.join(map(re.escape, self.NOISE_WORDS))
        self.NOISE_PATTERN = re.compile(noise_escaped, re.IGNORECASE)
        
        # スナップショットディレクトリ作成
        self._ensure_snapshot_dir()
        
        # P1/P2のプライオリティマッピング(後で設定される)
        self.priority_mapping: Dict[str, int] = {}
        
        self.logger.info(f"スナップショットディレクトリ: {self.snapshot_dir}")
        self.logger.info("通知履歴: SQLite形式(推奨版・ACID保証)")
    
    def _ensure_snapshot_dir(self) -> None:
        """スナップショットディレクトリを作成"""
        if not self.snapshot_dir.exists():
            self.snapshot_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"📁 スナップショットディレクトリ作成: {self.snapshot_dir}")
    
    def set_priority_mapping(self, mapping: Dict[str, int]) -> None:
        """スクリプトごとのプライオリティマッピングを設定"""
        self.priority_mapping = mapping
        self.logger.info(f"プライオリティマッピング設定: P1={sum(1 for v in mapping.values() if v == 1)}件, P2={sum(1 for v in mapping.values() if v == 2)}件")
    
    def _get_file_lock(self, filepath: str) -> threading.RLock:
        """ファイルごとのロックを取得"""
        with self.global_lock:
            if filepath not in self.file_locks:
                self.file_locks[filepath] = threading.RLock()
            return self.file_locks[filepath]
    
    def _get_snapshot_path(self, site_name: str, script_name: str, url_index: int) -> Path:
        """
        スナップショットファイルパスを取得
        
        P1: snapshots/p1_{script}_{url_index}.json(個別)
        P2: snapshots/p2_shared.json(共有)
        """
        # スクリプト名からプライオリティを判定
        priority = self.priority_mapping.get(script_name, 2)
        
        if priority == 1:
            # P1は個別ファイル
            safe_script = script_name.replace('.py', '').replace('/', '_').replace(' ', '_')
            filename = f"p1_{safe_script}_{url_index}.json"
            self.logger.debug(f"[{site_name}] P1個別ファイル: {filename}")
        else:
            # P2は共有ファイル
            filename = P2_SHARED_SNAPSHOT
            self.logger.debug(f"[{site_name}] P2共有ファイル: {filename}")
        
        return self.snapshot_dir / filename
    
    def _load_snapshot_file(self, filepath: Path) -> Dict[str, Dict[str, Any]]:
        """スナップショットファイルを読み込み"""
        if not filepath.exists():
            return {}
        
        file_lock = self._get_file_lock(str(filepath))
        try:
            with file_lock:
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except json.JSONDecodeError as e:
            ErrorHandler.handle(e, f"スナップショット読み込み({filepath.name})", ErrorSeverity.EXPECTED)
            return {}
        except Exception as e:
            ErrorHandler.handle(e, f"スナップショット読み込み({filepath.name})", ErrorSeverity.EXPECTED)
            return {}
    
    def _save_snapshot_file(self, filepath: Path, data: Dict[str, Dict[str, Any]]) -> None:
        """スナップショットファイルを保存(アトミック書き込み)"""
        file_lock = self._get_file_lock(str(filepath))
        try:
            with file_lock:
                temp_path = filepath.with_suffix('.tmp')
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                shutil.move(str(temp_path), str(filepath))
        except Exception as e:
            ErrorHandler.handle(e, f"スナップショット保存({filepath.name})", ErrorSeverity.RECOVERABLE)
            with suppress(OSError):
                temp_path = filepath.with_suffix('.tmp')
                if temp_path.exists():
                    temp_path.unlink()
    
    def detect_new_products(
        self,
        site_name: str,
        products: List[Dict[str, Any]],
        scraping_url: str = '',
        script_name: str = '',
        url_index: int = 0
    ) -> List[Dict[str, Any]]:
        """新商品を検出(v26: 個別ファイル対応)"""
        if not products:
            self.logger.warning(f"[{site_name}] 商品データなし")
            return []
        
        # スナップショットファイルパスを取得
        snapshot_path = self._get_snapshot_path(site_name, script_name, url_index)
        
        # スナップショット読み込み
        snapshot_data = self._load_snapshot_file(snapshot_path)
        
        is_first_run = site_name not in snapshot_data
        
        if is_first_run:
            return self._handle_first_run(site_name, products, scraping_url, snapshot_path, snapshot_data)
        
        snapshot = snapshot_data.get(site_name, {})
        remembered_first_key = snapshot.get('first_product_key')
        remembered_name = snapshot.get('first_product_name', '不明')
        
        current_first_key = self._normalize_product_key(products[0])
        if current_first_key == remembered_first_key:
            self.logger.info(f"[{site_name}] ✅ 変更なし(1位は同じ)")
            
            # タイムスタンプのみ更新
            if site_name in snapshot_data:
                snapshot_data[site_name]['timestamp'] = datetime.now().isoformat()
                self._save_snapshot_file(snapshot_path, snapshot_data)
                self.logger.debug(f"[{site_name}] タイムスタンプのみ更新")
            
            return []
        
        previous_first_position = None
        for idx, product in enumerate(products):
            if self._normalize_product_key(product) == remembered_first_key:
                previous_first_position = idx
                break
        
        self.logger.info(f"[{site_name}] 前回1位: {remembered_name[:50]}")
        self.logger.info(f"   前回ハッシュ: {remembered_first_key}")
        
        if previous_first_position is None:
            new_products = products[:20]
            self.logger.info(
                f"[{site_name}] 🎉 前回1位消失: 上位{len(new_products)}件を新商品として検知"
            )
            for i, p in enumerate(new_products, 1):
                self.logger.info(f"   新{i}位: {p['name'][:50]} / {p.get('price', '0')}円")
        elif previous_first_position == 0:
            new_products = []
        else:
            new_products = products[:previous_first_position]
            self.logger.info(
                f"[{site_name}] 🎉 新商品検知: {len(new_products)}件が上位に挿入"
            )
            for i, p in enumerate(new_products, 1):
                self.logger.info(f"   新{i}位: {p['name'][:50]} / {p.get('price', '0')}円")
            self.logger.info(
                f"   前回1位は現在{previous_first_position + 1}位に後退"
            )
        
        self._update_snapshot(site_name, products[0], scraping_url, snapshot_path, snapshot_data)
        
        return self._apply_notification_cooldown(site_name, new_products)
    
    def _handle_first_run(
        self,
        site_name: str,
        products: List[Dict[str, Any]],
        scraping_url: str,
        snapshot_path: Path,
        snapshot_data: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """初回実行時の処理"""
        first_product = products[0]
        first_key = self._normalize_product_key(first_product)
        first_name = first_product['name']
        first_price = first_product.get('price', '0')
        
        snapshot_data[site_name] = {
            'first_product_key': first_key,
            'first_product_name': first_name,
            'first_product_price': first_price,
            'first_product_url': scraping_url,
            'timestamp': datetime.now().isoformat()
        }
        self._save_snapshot_file(snapshot_path, snapshot_data)
        
        price_text = f"{first_price}円" if first_price != '0' else "お問い合わせ"
        
        self.logger.info(f"[{site_name}] 初回実行: 1位を記憶(通知スキップ)")
        self.logger.info(f"   商品名: {first_name[:50]}")
        self.logger.info(f"   価格: {price_text}")
        self.logger.info(f"   ハッシュ: {first_key}")
        self.logger.info(f"   保存先: {snapshot_path.name}")
        
        return []
    
    def _update_snapshot(
        self,
        site_name: str,
        current_first: Dict[str, Any],
        scraping_url: str,
        snapshot_path: Path,
        snapshot_data: Dict[str, Dict[str, Any]]
    ) -> None:
        """スナップショットを更新"""
        first_key = self._normalize_product_key(current_first)
        first_name = current_first['name']
        first_price = current_first.get('price', '0')
        
        snapshot_data[site_name] = {
            'first_product_key': first_key,
            'first_product_name': first_name,
            'first_product_price': first_price,
            'first_product_url': scraping_url,
            'timestamp': datetime.now().isoformat()
        }
        self._save_snapshot_file(snapshot_path, snapshot_data)
    
    def _apply_notification_cooldown(
        self,
        site_name: str,
        new_products: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """通知クールダウンを適用"""
        if not new_products:
            return []
        
        notifiable_products = []
        
        for product in new_products:
            product_key = self._normalize_product_key(product)
            
            should_notify = self.notification_manager.should_notify(
                product_key,
                self.NOTIFICATION_COOLDOWN_HOURS
            )
            
            if should_notify:
                self.notification_manager.add_notification(product_key, site_name)
                notifiable_products.append(product)
            else:
                self.logger.info(
                    f"⏸️ [{site_name}] 重複通知防止: {product['name'][:30]}... をスキップ"
                )
        
        if len(notifiable_products) > 0:
            self.notification_manager.cleanup(self.CLEANUP_THRESHOLD_HOURS)
        
        return notifiable_products
    
    @lru_cache(maxsize=10000)
    def _normalize_product_name_cached(self, product_name: str) -> str:
        """キャッシュ付き正規化"""
        name = unicodedata.normalize('NFKC', product_name)
        name = self.BRACKET_PATTERN.sub('', name)
        name = self.NOISE_PATTERN.sub('', name)
        name = self.NONWORD_PATTERN.sub('', name.lower())
        return name
    
    def _normalize_product_key(self, product: Dict[str, Any]) -> str:
        """商品キーを正規化"""
        name = product.get('name', '')
        
        code_match = re.search(r'[A-Z0-9]{8,}', name.upper())
        if code_match:
            product_key = code_match.group()
            return hashlib.md5(product_key.encode('utf-8')).hexdigest()[:8]
        
        if product.get('img_url'):
            img_url = product['img_url'].split('?')[0]
            return hashlib.md5(img_url.encode('utf-8')).hexdigest()[:8]
        
        normalized = self._normalize_product_name_cached(name)
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()[:8]
    
    def get_snapshot_stats(self) -> Dict[str, Any]:
        """スナップショット統計を取得"""
        total_sites = 0
        p1_files = list(self.snapshot_dir.glob("p1_*.json"))
        p2_file = self.snapshot_dir / P2_SHARED_SNAPSHOT
        
        for p1_file in p1_files:
            data = self._load_snapshot_file(p1_file)
            total_sites += len(data)
        
        if p2_file.exists():
            data = self._load_snapshot_file(p2_file)
            total_sites += len(data)
        
        return {
            'total_sites': total_sites,
            'total_products': total_sites,
            'p1_files': len(p1_files),
            'p2_shared': p2_file.exists()
        }


# ==================== ハードオフ専用フォーマッター (新規) ====================

class HardOffFormatter:
    """ハードオフ専用商品フォーマッター
    
    変換例（カメラ系のみ）:
    CANON コンパクトデジカメ [IXY 650]・34800円
    ↓
    ■【IXY 650・34800円】CANON コンパクトデジカメ
    
    時計系は従来形式のまま:
    OMEGA DEVILLE クォーツ [195.0075.2]・57200円
    ↓
    ▪ OMEGA DEVILLE クォーツ [195.0075.2]・57200円
    """
    
    CODE_PATTERN: Final[re.Pattern] = re.compile(r'\[([^\]]+)\]')
    
    # ハードオフの時計URL index (従来形式を維持)
    HARDOFF_WATCH_URL_INDEX: Final[int] = 5
    
    @classmethod
    def is_hardoff(cls, display_name: str, scraping_url: str) -> bool:
        """ハードオフ判定"""
        return "ハードオフ" in display_name or "hardoff" in scraping_url.lower()
    
    @classmethod
    def should_use_new_format(
        cls,
        display_name: str,
        scraping_url: str,
        url_index: int
    ) -> bool:
        """新形式（商品コード先頭）を使用すべきか判定
        
        Args:
            display_name: サイト表示名
            scraping_url: スクレイピング対象URL
            url_index: URL index (0-5)
        
        Returns:
            True: 新形式（カメラ系）
            False: 従来形式（時計系 or ハードオフ以外）
        """
        # ハードオフでなければ従来形式
        if not cls.is_hardoff(display_name, scraping_url):
            return False
        
        # ハードオフの時計（url_index=5）は従来形式
        if url_index == cls.HARDOFF_WATCH_URL_INDEX:
            return False
        
        # それ以外のハードオフ（カメラ系: url_index=0,1,2,3,4）は新形式
        return True
    
    @classmethod
    def format_product_line(cls, product: Dict[str, Any]) -> str:
        """商品行をハードオフ形式でフォーマット"""
        name = product['name']
        price = product.get('price', '0')
        
        # 商品名から [商品コード] を抽出
        code_match = cls.CODE_PATTERN.search(name)
        if code_match:
            code = code_match.group(1)
            # 商品コードを除いた残りの名前
            name_without_code = cls.CODE_PATTERN.sub('', name).strip()
            
            price_text = f"{price}円" if price != '0' else "お問い合わせ"
            return f"■【{code}・{price_text}】{name_without_code}\n\n"
        else:
            # コードがない場合は従来形式
            price_text = f"{price}円" if price != '0' else "お問い合わせ"
            return f"▪ {name}・{price_text}\n\n"


# ==================== スナップショットレポート生成器 (新規) ====================

class SnapshotReportGenerator:
    """P1/P2全URLのタイムスタンプレポート生成専門クラス"""
    
    SNAPSHOT_THRESHOLD_SECONDS: Final[int] = 1800  # 30分
    
    def __init__(
        self,
        memory_system: SimpleMemoryDiffSystem,
        logger: logging.Logger = LOGGER
    ) -> None:
        self.memory_system = memory_system
        self.logger = logger
    
    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """P1/P2全URLのタイムスタンプレポート生成"""
        current_time = datetime.now()
        all_status: List[SnapshotStatus] = []
        
        # P1個別JSONファイル読み込み
        p1_files = list(self.memory_system.snapshot_dir.glob("p1_*.json"))
        for p1_file in p1_files:
            data = self.memory_system._load_snapshot_file(p1_file)
            for site_name, snapshot in data.items():
                status = self._create_snapshot_status(
                    site_name, snapshot, current_time, 'P1', p1_file.name
                )
                if status:
                    all_status.append(status)
        
        # P2共有JSON読み込み
        p2_file = self.memory_system.snapshot_dir / P2_SHARED_SNAPSHOT
        if p2_file.exists():
            data = self.memory_system._load_snapshot_file(p2_file)
            for site_name, snapshot in data.items():
                status = self._create_snapshot_status(
                    site_name, snapshot, current_time, 'P2', P2_SHARED_SNAPSHOT
                )
                if status:
                    all_status.append(status)
        
        # 新鮮度でソート
        fresh_sites = [s for s in all_status if s.is_fresh]
        stale_sites = [s for s in all_status if not s.is_fresh]
        
        return {
            'all_status': all_status,
            'fresh_sites': fresh_sites,
            'stale_sites': stale_sites,
            'total_sites': len(all_status),
            'fresh_count': len(fresh_sites),
            'stale_count': len(stale_sites)
        }
    
    def _create_snapshot_status(
        self,
        site_name: str,
        snapshot: Dict[str, Any],
        current_time: datetime,
        priority: str,
        filename: str
    ) -> Optional[SnapshotStatus]:
        """スナップショット状態オブジェクトを生成"""
        timestamp = snapshot.get('timestamp')
        if not timestamp:
            return None
        
        try:
            update_time = datetime.fromisoformat(timestamp)
            elapsed = (current_time - update_time).total_seconds()
            
            return SnapshotStatus(
                site=site_name,
                priority=priority,
                elapsed_minutes=elapsed / 60,
                is_fresh=elapsed <= self.SNAPSHOT_THRESHOLD_SECONDS,
                file=filename
            )
        except Exception as e:
            self.logger.warning(f"タイムスタンプ解析失敗 [{site_name}]: {e}")
            return None
    
    def format_report_message(self, report_data: Dict[str, Any]) -> str:
        """レポートをChatWork形式でフォーマット"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        message = "[info]"
        message += "=" * 40 + "\n"
        message += "📊 全URLタイムスタンプレポート\n"
        message += f"🕐 {timestamp}\n"
        message += "=" * 40 + "\n\n"
        
        message += "【統計】\n"
        message += f"✅ 新鮮: {report_data['fresh_count']}件 (30分以内)\n"
        message += f"⚠️ 古い: {report_data['stale_count']}件 (30分超過)\n"
        message += f"📁 総計: {report_data['total_sites']}サイト\n\n"
        
        # 古いサイトのみ詳細表示
        if report_data['stale_sites']:
            message += f"【⚠️ 更新が古いサイト: {len(report_data['stale_sites'])}件】\n"
            sorted_stale = sorted(
                report_data['stale_sites'],
                key=lambda x: x.elapsed_minutes,
                reverse=True
            )
            for status in sorted_stale:
                elapsed_min = status.elapsed_minutes
                
                if elapsed_min < 60:
                    time_str = f"{elapsed_min:.0f}分前"
                else:
                    time_str = f"{elapsed_min/60:.1f}時間前"
                
                message += f"  ⚠️ [{status.priority}] {status.site}: {time_str}\n"
            message += "\n"
        else:
            message += "✅ 全サイト正常更新中\n\n"
        
        message += "=" * 40 + "\n"
        message += "[/info]"
        
        return message


# ==================== ChatWork通知 (改修) ====================

class ChatWorkNotifier:
    """ChatWork通知クラス(ハードオフ対応版)"""
    
    DEFAULT_ROOM_ID: Final[str] = '385402385'
    MAX_RETRIES: Final[int] = 3
    RETRY_DELAY: Final[float] = 1.0
    
    def __init__(
        self,
        token: Optional[str] = None,
        logger: logging.Logger = LOGGER
    ) -> None:
        self.token = token or os.getenv('CHATWORK_TOKEN')
        if not self.token:
            raise ValueError(
                "CHATWORK_TOKEN環境変数が設定されていません。\n"
                "export CHATWORK_TOKEN='your_token_here' を実行してください。"
            )
        self.logger = logger
    
    def send_notification(
        self, message: str, room_id: str, retry: int = 0
    ) -> bool:
        if not room_id or room_id.lower() in ['nan', 'none', '']:
            return False
        
        try:
            response = requests.post(
                f"https://api.chatwork.com/v2/rooms/{room_id}/messages",
                headers={"X-ChatWorkToken": self.token},
                data={"body": message},
                timeout=HTTP_TIMEOUT
            )
            
            if response.status_code == 200:
                self.logger.info(f"ChatWork通知送信成功 (ルーム: {room_id})")
                return True
            elif response.status_code == 429 and retry < self.MAX_RETRIES:
                time.sleep(self.RETRY_DELAY * (retry + 1))
                return self.send_notification(message, room_id, retry + 1)
            else:
                self.logger.error(
                    f"ChatWork通知送信失敗: {response.status_code} - {response.text}"
                )
                return False
                
        except requests.Timeout:
            if retry < self.MAX_RETRIES:
                time.sleep(self.RETRY_DELAY)
                return self.send_notification(message, room_id, retry + 1)
            ErrorHandler.handle(
                Exception("ChatWorkタイムアウト"),
                "通知送信",
                ErrorSeverity.RECOVERABLE
            )
            return False
        except Exception as e:
            ErrorHandler.handle(e, "ChatWork通知", ErrorSeverity.RECOVERABLE)
            return False
    
    def format_new_products_notification(
        self,
        display_name: str,
        category: str,
        scraping_url: str,
        products: List[Dict[str, Any]],
        url_index: int = 0  # ← 追加
    ) -> str:
        """新商品通知フォーマット(ハードオフURL index対応版)
        
        Args:
            display_name: サイト表示名
            category: カテゴリ名
            scraping_url: スクレイピング対象URL
            products: 商品リスト
            url_index: URL index (ハードオフの判定に使用)
        """
        message = "[info]"
        message += "━━━━━━━━━━━━━━━━━━━\n"
        message += f"🔔 {display_name} + {category}\n"
        message += "┣━━━━━━━━━━━━━━━━━━━┫\n"
        message += f"🔗 {scraping_url}\n"
        message += "┣━━━━━━━━━━━━━━━━━━━┫\n\n"
        
        # ハードオフ判定（URL index考慮）
        use_hardoff_format = HardOffFormatter.should_use_new_format(
            display_name, scraping_url, url_index
        )
        
        for product in products[:20]:
            if use_hardoff_format:
                # ハードオフカメラ系: 新形式
                message += HardOffFormatter.format_product_line(product)
            else:
                # 従来形式（ハードオフ時計 or その他サイト）
                price_text = (
                    f"{product['price']}円"
                    if product.get('price', '0') != '0'
                    else "お問い合わせ"
                )
                message += f"▪ {product['name']}・{price_text}\n\n"
        
        if len(products) > 20:
            message += f"...他{len(products) - 20}件\n"
        
        message += "━━━━━━━━━━━━━━━━━━━[/info]"
        return message


# ==================== Playwright並列制御 ====================

class PlaywrightSemaphore:
    """Playwright並列実行制御"""
    
    def __init__(self, max_concurrent: int = 2, logger: logging.Logger = LOGGER) -> None:
        self.semaphore = threading.Semaphore(max_concurrent)
        self.logger = logger
    
    def acquire(self, script_name: str) -> bool:
        try:
            acquired = self.semaphore.acquire(timeout=5)
            if acquired:
                self.logger.info(f"Playwright並列制御: {script_name} 開始")
            return acquired
        except Exception as e:
            ErrorHandler.handle(e, "Playwrightセマフォ取得", ErrorSeverity.EXPECTED)
            return False
    
    def release(self, script_name: str) -> None:
        try:
            self.semaphore.release()
            self.logger.info(f"Playwright並列制御: {script_name} 完了")
        except Exception as e:
            ErrorHandler.handle(e, "Playwrightセマフォ解放", ErrorSeverity.EXPECTED)


# ==================== 設定ファイル管理 ====================

class SafeCSVManager:
    """設定ファイル管理クラス"""
    
    def __init__(self, config_file: str = "shop_config.json", logger: logging.Logger = LOGGER) -> None:
        self.config_file = config_file
        self.priority1_scripts: List[str] = []
        self.priority2_scripts: List[str] = []
        self.notification_config: Dict[str, Dict[str, Any]] = {}
        self.url_config_mapping: Dict[str, List[Dict[str, Any]]] = {}
        self.script_priority_mapping: Dict[str, int] = {}  # v26追加
        self.logger = logger
        self.load_config()
    
    def load_config(self) -> None:
        if not Path(self.config_file).exists():
            self.logger.error(f"設定ファイルが見つかりません: {self.config_file}")
            return
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            df = pd.DataFrame(data)
            if df.empty:
                return
            
            df['priority'] = pd.to_numeric(df['priority'], errors='coerce').fillna(2).astype(int)
            df['is_active'] = df['is_active'].astype(str).str.lower().map(
                {'true': True, 'false': False}
            ).fillna(False)
            df['url_index'] = pd.to_numeric(
                df.get('url_index', 0), errors='coerce'
            ).fillna(0).astype(int)
            
            active_scripts = df[df['is_active'] == True]
            
            self.priority1_scripts = active_scripts[
                active_scripts['priority'] == 1
            ]['py_file'].unique().tolist()
            
            self.priority2_scripts = active_scripts[
                active_scripts['priority'] > 1
            ]['py_file'].unique().tolist()
            
            # v26追加: プライオリティマッピング構築
            for _, row in active_scripts.iterrows():
                py_file = row['py_file']
                priority = int(row['priority'])
                self.script_priority_mapping[py_file] = priority
                self._process_config_row(row)
            
            self.logger.info(
                f"設定読み込み完了: "
                f"P1={len(self.priority1_scripts)}件, "
                f"P2={len(self.priority2_scripts)}件"
            )
            
        except Exception as e:
            ErrorHandler.handle(e, "設定ファイル読み込み", ErrorSeverity.FATAL)
    
    def _process_config_row(self, row: pd.Series) -> None:
        py_file = row['py_file']
        
        notification_ids = str(row['notification_enabled']).strip() if (
            'notification_enabled' in row and pd.notna(row['notification_enabled'])
        ) else ''
        
        if notification_ids.lower() == 'true':
            notification_ids = ChatWorkNotifier.DEFAULT_ROOM_ID
        
        config_entry = {
            'notification_room_ids': notification_ids if notification_ids.lower() not in [
                'nan', 'non', 'none', 'false', ''
            ] else None,
            'display_name': str(row['display_name']),
            'category': str(row.get('category', '新着')),
            'scraping_url': str(row.get('scraping_url', '')),
            'url_index': int(row['url_index']),
            'priority': int(row['priority'])  # v26追加
        }
        
        if py_file not in self.url_config_mapping:
            self.url_config_mapping[py_file] = []
        
        self.url_config_mapping[py_file].append(config_entry)
        
        if config_entry['url_index'] == 0:
            self.notification_config[py_file] = config_entry
    
    def get_priority1_scripts(self) -> List[str]:
        return self.priority1_scripts
    
    def get_priority2_scripts(self) -> List[str]:
        return self.priority2_scripts
    
    def get_all_url_configs(self, py_file: str) -> List[Dict[str, Any]]:
        return self.url_config_mapping.get(py_file, [])
    
    def get_script_priority_mapping(self) -> Dict[str, int]:
        """v26追加: プライオリティマッピングを取得"""
        return self.script_priority_mapping


# ==================== 非同期スクリプト実行器 ====================

class AsyncStableExecutor:
        
    def __init__(
        self,
        blocked_scripts: Set[str],
        memory_system: SimpleMemoryDiffSystem,
        playwright_semaphore: PlaywrightSemaphore,
        csv_manager: SafeCSVManager,
        chatwork_notifier: ChatWorkNotifier,
        logger: logging.Logger = LOGGER
    ) -> None:
        self.logger = logger
        self.extractor = StableDataExtractor(logger)
        self.blocked_scripts = blocked_scripts
        self.memory_system = memory_system
        self.playwright_semaphore = playwright_semaphore
        self.csv_manager = csv_manager
        self.chatwork_notifier = chatwork_notifier
        self.running = False
        self.stats: Dict[str, int] = {}
        self.error_log: List[Dict[str, str]] = []
    
    async def execute_async(self, script: str) -> ExecutionResult:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.execute_stable, script)
    
    def is_playwright_script(self, script_path: str) -> bool:
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                content = f.read()
            playwright_patterns = ['playwright', 'async_playwright', 'browser.new_page']
            return any(pattern in content for pattern in playwright_patterns)
        except Exception:
            return False
    
    def execute_stable(self, script: str) -> ExecutionResult:
        script_path = Path(script)
        
        if not script_path.exists():
            return ExecutionResult(
                success=False,
                error='ファイル不存在',
                data={'count': 0}
            )
        
        script_name = script_path.name
        
        if script_name in self.blocked_scripts:
            self.logger.warning(f"[{script_name}] ブロック対象のためスキップ")
            return ExecutionResult(
                success=False,
                error='ブロック対象',
                data={'count': 0}
            )
        
        is_playwright = self.is_playwright_script(str(script_path))
        acquired_semaphore = False
        
        if is_playwright:
            acquired_semaphore = self.playwright_semaphore.acquire(script_name)
            if not acquired_semaphore:
                self.logger.warning(f"[{script_name}] Playwright並列制限によりスキップ")
                return ExecutionResult(
                    success=True,
                    data={'count': 0}
                )
        
        self.logger.info(f"[{script_name}] 実行開始")
        start_time = time.time()
        
        try:
            result = self._run_subprocess(script_path)
            duration = time.time() - start_time
            
            data_result = self.extractor.extract_stable(result.stdout, script_name)
            success = result.returncode == 0 and data_result['success']
            
            if success and data_result.get('products'):
                self.logger.info(
                    f"[{script_name}] 成功 ({duration:.1f}s) - {data_result['count']}件取得"
                )
                self.process_products_by_url_index(script_name, data_result['products'])
            elif not success:
                self.logger.error(f"[{script_name}] 失敗 ({duration:.1f}s)")
            else:
                self.logger.info(f"[{script_name}] 成功 ({duration:.1f}s) - 0件")
            
            return ExecutionResult(
                success=success,
                duration=duration,
                data=data_result
            )
            
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            self.logger.warning(f"[{script_name}] タイムアウト ({SUBPROCESS_TIMEOUT}s)")
            self.error_log.append({'script': script_name, 'error': f'タイムアウト({SUBPROCESS_TIMEOUT}s)'})
            return ExecutionResult(
                success=False,
                duration=duration,
                error=f'タイムアウト({SUBPROCESS_TIMEOUT}s)',
                data={'count': 0}
            )
        except Exception as e:
            duration = time.time() - start_time
            ErrorHandler.handle(e, f"スクリプト実行[{script_name}]", ErrorSeverity.RECOVERABLE)
            self.error_log.append({'script': script_name, 'error': str(e)})
            return ExecutionResult(
                success=False,
                duration=duration,
                error=str(e),
                data={'count': 0}
            )
        finally:
            if acquired_semaphore:
                self.playwright_semaphore.release(script_name)
    
    def _run_subprocess(self, script_path: Path) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        env['PYTHONUNBUFFERED'] = '1'
        
        return subprocess.run(
            [sys.executable, '-u', str(script_path)],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=SUBPROCESS_TIMEOUT,
            cwd=str(script_path.parent),
            env=env
        )
    
    def process_products_by_url_index(
        self,
        script_name: str,
        all_products: List[Dict[str, Any]]
    ) -> None:
        all_url_configs = self.csv_manager.get_all_url_configs(script_name)
        
        if not all_url_configs:
            self.logger.warning(f"[{script_name}] URL設定が見つかりません")
            return
        
        products_by_index: Dict[int, List[Dict[str, Any]]] = {}
        for product in all_products:
            url_idx = product.get('url_index', 0)
            if url_idx not in products_by_index:
                products_by_index[url_idx] = []
            products_by_index[url_idx].append(product)
        
        self.logger.info(
            f"[{script_name}] URL別商品数: "
            f"{[(idx, len(prods)) for idx, prods in products_by_index.items()]}"
        )
        
        for url_index, url_products in products_by_index.items():
            matching_configs = [c for c in all_url_configs if c['url_index'] == url_index]
            if not matching_configs:
                continue
            
            url_config = matching_configs[0]
            display_name = url_config['display_name']
            category = url_config['category']
            unique_display_name = f"{display_name}_{category}"
            
            self.logger.info(
                f"[{display_name}] URL index {url_index} ({category}): "
                f"{len(url_products)}件"
            )
            
            scraping_url = url_config.get('scraping_url', '')
            
            # v26: script_nameとurl_indexを渡す
            new_products = self.memory_system.detect_new_products(
                unique_display_name,
                url_products,
                scraping_url=scraping_url,
                script_name=script_name,
                url_index=url_index
            )
            
            if new_products:
                self.send_notification_for_url(script_name, url_config, new_products, display_name)
    
    def send_notification_for_url(
        self,
        script_name: str,
        url_config: Dict[str, Any],
        new_products: List[Dict[str, Any]],
        display_name: Optional[str] = None
    ) -> None:
        if display_name is None:
            display_name = url_config['display_name']
        
        category = url_config['category']
        scraping_url = url_config['scraping_url']
        url_index = url_config.get('url_index', 0)  # ← URL index を取得
        
        # URL index を渡す
        message = self.chatwork_notifier.format_new_products_notification(
            display_name, category, scraping_url, new_products, url_index
        )
        
        notification_ids = url_config.get('notification_room_ids')
        if notification_ids:
            room_ids = [r.strip() for r in str(notification_ids).split(',') if r.strip()]
            for room_id in room_ids:
                self.logger.info(f"[{display_name}] ChatWork通知送信: ルーム {room_id}")
                self.chatwork_notifier.send_notification(message, room_id)
        
        if hasattr(self, 'master_controller'):
            self.master_controller.last_new_product_time[script_name] = datetime.now()


# ==================== Priority2サイクル制御 (リファクタリング版) ====================

class Tier2CycleController:
    """Priority2スクリプト実行制御(リファクタリング版)"""
    
    REPORT_INTERVAL_SECONDS: Final[int] = 1800  # 30分
    SNAPSHOT_THRESHOLD_SECONDS: Final[int] = 1800
    ADMIN_ROOM_ID: Final[str] = "413142921"
    
    def __init__(
        self,
        scripts: List[str],
        executor: AsyncStableExecutor,
        memory_system: SimpleMemoryDiffSystem,
        chatwork_notifier: ChatWorkNotifier,
        logger: logging.Logger = LOGGER
    ) -> None:
        self.scripts = scripts
        self.executor = executor
        self.memory_system = memory_system
        self.chatwork_notifier = chatwork_notifier
        self.logger = logger
        
        # レポート生成器を初期化
        self.report_generator = SnapshotReportGenerator(memory_system, logger)
        
        self.script_queue: List[str] = []
        self.queue_lock = threading.Lock()
        self.cycle_start_time = datetime.now()
        self.executed_scripts: Set[str] = set()
        self.cycle_count = 0
        self.last_report_time = datetime.now()
        
        self.script_to_snapshot_keys: Dict[str, List[str]] = {}
        self._build_snapshot_key_mapping()
        
        with self.queue_lock:
            self.script_queue = list(scripts)
        
        self.logger.info(
            f"完全実行モード初期化: {len(scripts)}件 - 全件実行完了まで次サイクル待機"
        )
    
    def _build_snapshot_key_mapping(self) -> None:
        for script in self.scripts:
            all_configs = self.executor.csv_manager.get_all_url_configs(script)
            
            snapshot_keys = []
            for config in all_configs:
                display_name = config['display_name']
                category = config['category']
                key = f"{display_name}_{category}"
                snapshot_keys.append(key)
            
            self.script_to_snapshot_keys[script] = snapshot_keys
            
            if snapshot_keys:
                self.logger.debug(f"[{script}] マッピング: {snapshot_keys}")
    
    def should_send_report(self) -> bool:
        """30分ごとにレポート送信"""
        elapsed = (datetime.now() - self.last_report_time).total_seconds()
        return elapsed >= self.REPORT_INTERVAL_SECONDS
    
    def send_comprehensive_snapshot_report(self) -> None:
        """全URLタイムスタンプレポート送信"""
        try:
            report_data = self.report_generator.generate_comprehensive_report()
            message = self.report_generator.format_report_message(report_data)
            
            self.chatwork_notifier.send_notification(message, self.ADMIN_ROOM_ID)
            self.last_report_time = datetime.now()
            
            self.logger.info(
                f"📊 全URLタイムスタンプレポート送信完了: "
                f"新鮮{report_data['fresh_count']}/古い{report_data['stale_count']}"
            )
            
        except Exception as e:
            ErrorHandler.handle(e, "全URLレポート送信", ErrorSeverity.RECOVERABLE)
    
    async def run_cycle_async(self) -> None:
        """サイクル実行(30分ごとのレポート追加)"""
        running_tasks: List[asyncio.Task] = []
        MAX_CONCURRENT_P2 = 1
        
        while self.executor.running:
            now = datetime.now()
            is_night = TimeManager.is_nighttime()
            
            # 30分ごとのレポート送信チェック
            if self.should_send_report():
                self.send_comprehensive_snapshot_report()
            
            running_tasks = [t for t in running_tasks if not t.done()]
            
            if not self.script_queue and not running_tasks:
                self.cycle_count += 1
                cycle_duration = (now - self.cycle_start_time).total_seconds()
                
                not_executed = set(self.scripts) - self.executed_scripts
                
                if not_executed:
                    self.logger.warning(f"⚠️ 未実行: {len(not_executed)}件")
                
                self.logger.info(
                    f"✅ サイクル{self.cycle_count}完了: "
                    f"{cycle_duration:.0f}秒 ({len(self.executed_scripts)}件実行)"
                )
                
                # 深夜は全体スクレイピング後にレポート送信
                if is_night:
                    self.logger.info("🌙 深夜サイクル完了 - 全URLレポート送信")
                    self.send_comprehensive_snapshot_report()
                
                with self.queue_lock:
                    retry_scripts = list(not_executed)
                    normal_scripts = [s for s in self.scripts if s not in retry_scripts]
                    
                    self.script_queue = retry_scripts + normal_scripts
                    
                    if retry_scripts:
                        self.logger.info(f"🔄 リトライ優先: {len(retry_scripts)}件")
                        for script in retry_scripts:
                            self.logger.info(f"   📌 {script}")
                    
                    self.executed_scripts = set()
                    
                    target_interval = TimeManager.get_interval_for_priority2(is_night)
                    wait_time = max(5, target_interval - cycle_duration)
                    
                    time_label = "深夜" if is_night else "通常"
                    self.logger.info(
                        f"🔄 サイクル{self.cycle_count + 1}開始予定: "
                        f"{wait_time:.0f}秒後 ({time_label}時間帯: {target_interval}秒間隔)"
                    )
                    self.cycle_start_time = now
                
                self.logger.info(
                    f"⏰ 次サイクルまで {wait_time:.0f}秒待機... ({wait_time/60:.1f}分)"
                )
                await asyncio.sleep(wait_time)
                continue
            
            while len(running_tasks) < MAX_CONCURRENT_P2:
                with self.queue_lock:
                    if not self.script_queue:
                        break
                    
                    script_to_execute = self.script_queue.pop(0)
                    remaining = len(self.script_queue)
                    elapsed = (now - self.cycle_start_time).total_seconds()
                    
                    self.logger.info(
                        f"Tier2実行: {script_to_execute} "
                        f"(残り={remaining}件, 経過={elapsed:.0f}秒)"
                    )
                    
                    task = asyncio.create_task(self._execute_and_record(script_to_execute))
                    running_tasks.append(task)
            
            await asyncio.sleep(1)
    
    async def _execute_and_record(self, script: str) -> ExecutionResult:
        result = await self.executor.execute_async(script)
        
        self.executed_scripts.add(script)
        
        if result.success:
            self.executor.stats['total_executions'] += 1
            self.executor.stats['successful_executions'] += 1
            self.executor.stats['total_products'] += result.data['count']
        elif not isinstance(result, Exception):
            self.executor.stats['total_executions'] += 1
        
        return result


# ==================== マスターコントローラー ====================

class FinalStableMasterController:
    """最終安定版マスターコントローラー v27(P1個別スナップショット対応)"""
    
    VERSION: Final[str] = "27"
    
    def __init__(self) -> None:
        self.running = False
        self.start_time: Optional[datetime] = None
        self.blocked_scripts: Set[str] = set()
        self.logger = LOGGER
        
        self.script_intervals: Dict[str, int] = {}
        self.last_new_product_time: Dict[str, datetime] = {}
        
        self.csv_manager = SafeCSVManager("shop_config.json", self.logger)
        
        # v26: SimpleMemoryDiffSystemにプライオリティマッピングを設定
        self.memory_system = SimpleMemoryDiffSystem(logger=self.logger)
        self.memory_system.set_priority_mapping(self.csv_manager.get_script_priority_mapping())
        
        self.chatwork_notifier = ChatWorkNotifier(logger=self.logger)
        self.playwright_semaphore = PlaywrightSemaphore(max_concurrent=3, logger=self.logger)
        
        self.executor = AsyncStableExecutor(
            self.blocked_scripts,
            self.memory_system,
            self.playwright_semaphore,
            self.csv_manager,
            self.chatwork_notifier,
            self.logger
        )
        
        self.executor.master_controller = self
        
        priority2_scripts = self.csv_manager.get_priority2_scripts()
        self.tier2_controller = Tier2CycleController(
            priority2_scripts,
            self.executor,
            self.memory_system,
            self.chatwork_notifier,
            self.logger
        )
        
        self.stats = {
            'cycles': 0,
            'total_executions': 0,
            'successful_executions': 0,
            'total_products': 0,
            'blocked_scripts': len(self.blocked_scripts)
        }
        self.executor.stats = self.stats
        
        signal.signal(signal.SIGINT, self.signal_handler)
    
    def signal_handler(self, signum: int, frame: Any) -> None:
        self.logger.info(f"終了シグナル受信: {signum}")
        self.stop()
    
    def stop(self) -> None:
        self.running = False
        self.executor.running = False
        
        if isinstance(self.memory_system.notification_manager, SQLiteNotificationHistory):
            self.logger.info("SQLite通知履歴は自動保存されています")
        
        self.logger.info("システム停止")
    
    async def execute_priority1_async(self) -> None:
        priority1_scripts = self.csv_manager.get_priority1_scripts()
        
        if not priority1_scripts:
            return
        
        last_run_times = {script: datetime.min for script in priority1_scripts}
        
        is_night_initial = TimeManager.is_nighttime()
        for script in priority1_scripts:
            if is_night_initial:
                self.script_intervals[script] = TimeManager.NIGHT_INTERVAL_SECONDS
                self.logger.info(
                    f"[{script}] 初期間隔: {TimeManager.NIGHT_INTERVAL_SECONDS}秒(深夜起動)"
                )
            else:
                self.script_intervals[script] = 60
            self.last_new_product_time[script] = datetime.now() - timedelta(hours=2)
        
        while self.running:
            now = datetime.now()
            is_night = TimeManager.is_nighttime()
            
            tasks = []
            
            for script in priority1_scripts:
                if not self.running:
                    break
                
                last_new_product = self.last_new_product_time.get(script, now)
                idle_time = (now - last_new_product).total_seconds()
                
                interval = TimeManager.get_interval_for_priority1(idle_time, is_night)
                
                if self.script_intervals.get(script) != interval:
                    reason = "深夜固定" if is_night else f"無更新: {idle_time/60:.1f}分"
                    self.logger.info(
                        f"[{script}] 間隔変更: "
                        f"{self.script_intervals.get(script, 60)}秒 → {interval}秒 "
                        f"(理由: {reason})"
                    )
                    self.script_intervals[script] = interval
                
                elapsed = (now - last_run_times[script]).total_seconds()
                if elapsed >= interval:
                    last_run_times[script] = now
                    tasks.append(self._execute_with_interval_tracking(script))
            
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if not isinstance(result, Exception) and result.success:
                        self.stats['total_executions'] += 1
                        self.stats['successful_executions'] += 1
                        self.stats['total_products'] += result.data['count']
                    elif not isinstance(result, Exception):
                        self.stats['total_executions'] += 1
            
            await asyncio.sleep(5)
    
    async def _execute_with_interval_tracking(self, script: str) -> ExecutionResult:
        result = await self.executor.execute_async(script)
        
        if result.success and result.data.get('count', 0) > 0:
            self.last_new_product_time[script] = datetime.now()
        
        return result
    
    async def execute_cycle_async(self) -> None:
        while self.running:
            self.stats['cycles'] += 1
            cycle_start = time.time()
            
            self.logger.info(f"=== サイクル {self.stats['cycles']} 開始 ===")
            
            mem_stats = self.memory_system.get_snapshot_stats()
            cycle_duration = time.time() - cycle_start
            
            self.logger.info(
                f"=== サイクル {self.stats['cycles']} 完了 ({cycle_duration:.1f}s) ==="
            )
            
            if self.stats['cycles'] % 3 == 0:
                executed = self.stats['total_executions'] - self.stats['blocked_scripts']
                success_rate = (
                    (self.stats['successful_executions'] / max(1, executed)) * 100
                )
                self.logger.info("--- 統計 ---")
                self.logger.info(f"総実行: {executed}, 成功率: {success_rate:.1f}%")
                self.logger.info(f"累計商品: {self.stats['total_products']}件")
                self.logger.info(f"スナップショット: P1={mem_stats['p1_files']}ファイル, P2共有={mem_stats['p2_shared']}")
            
            for i in range(300):
                if not self.running:
                    break
                await asyncio.sleep(1)
    
    async def start_async(self) -> None:
        self.running = True
        self.executor.running = True
        self.start_time = datetime.now()
        
        print("=" * 60)
        print(f"Master Controller v{self.VERSION} - P1個別スナップショット対応版")
        print("=" * 60)
        print("🔧 v27新機能: 全URLタイムスタンプ監視(30分ごと)")
        print("🔧 v27新機能: ハードオフ専用フォーマット対応")
        print("📁 スナップショットディレクトリ: snapshots/")
        print("   P1: snapshots/p1_{script}_{url_index}.json(個別)")
        print("   P2: snapshots/p2_shared.json(共有)")
        print("")
        print(f"深夜間隔: {TimeManager.NIGHT_INTERVAL_SECONDS}秒 = {TimeManager.NIGHT_INTERVAL_SECONDS/60:.0f}分")
        print("📊 通知履歴: SQLite形式(推奨版・ACID保証・WALモード)")
        print("Ctrl+C で停止")
        print("=" * 60)
        
        priority1_scripts = self.csv_manager.get_priority1_scripts()
        priority2_scripts = self.csv_manager.get_priority2_scripts()
        
        self.logger.info("実行対象:")
        self.logger.info(
            f"  優先度1: {len(priority1_scripts)}件"
            f"(動的間隔: 60秒〜1時間、深夜: 30分固定) → 個別JSONファイル"
        )
        self.logger.info(
            f"  優先度2: {len(priority2_scripts)}件"
            f"(5分固定、深夜: 30分固定) → 共有JSONファイル"
        )
        
        if self.blocked_scripts:
            self.logger.info(f"ブロック対象: {', '.join(self.blocked_scripts)}")
        
        try:
            tasks = []
            
            if priority1_scripts:
                tasks.append(asyncio.create_task(self.execute_priority1_async()))
            
            if priority2_scripts:
                tasks.append(asyncio.create_task(self.tier2_controller.run_cycle_async()))
            
            tasks.append(asyncio.create_task(self.execute_cycle_async()))
            
            await asyncio.gather(*tasks)
        
        except KeyboardInterrupt:
            self.logger.info("キーボード割り込み")
        except Exception as e:
            ErrorHandler.handle(e, "システム実行", ErrorSeverity.FATAL)
        
        finally:
            self.stop()
            executed = self.stats['total_executions'] - self.stats['blocked_scripts']
            if executed > 0:
                success_rate = (
                    (self.stats['successful_executions'] / executed) * 100
                )
                self.logger.info(
                    f"最終統計: 成功率{success_rate:.1f}%, "
                    f"累計商品{self.stats['total_products']}件"
                )
            
            mem_stats = self.memory_system.get_snapshot_stats()
            self.logger.info(f"スナップショット: {mem_stats['total_sites']}サイト")
            self.logger.info("システム終了")
    
    def start(self) -> None:
        asyncio.run(self.start_async())


# ==================== エントリーポイント ====================

def main() -> None:
    """エントリーポイント"""
    print(f"Master Controller v{FinalStableMasterController.VERSION}")
    print("=" * 40)
    print("🔧 v27 - リファクタリング完了版")
    print("=" * 40)
    print("")
    print("【主な改善点】")
    print("✅ クラスの単一責任原則徹底")
    print("✅ ハードオフフォーマッター分離")
    print("✅ レポート生成器専用クラス化")
    print("✅ データクラス活用で型安全性向上")
    print("✅ 30分ごとの全URLタイムスタンプ監視")
    print("")
    
    controller = FinalStableMasterController()
    controller.start()


if __name__ == "__main__":
    main()