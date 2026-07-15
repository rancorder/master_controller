#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
site_status_checker.py - サイトステータス単独チェッカー（Python版）

【v2.0 簡易版】
✅ 全サイトHTTPステータス確認（200のみチェック）
✅ レスポンスタイム測定（ms）
✅ ChatWorkレポート（異常サイトのみ通知）
✅ JSON出力（site-status-report.json）

【実行方法】
python3 site_status_checker.py

【cron設定例（30分ごと）】
0,30 * * * * cd /path/to/scraper && python3 site_status_checker.py
"""

import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests
from playwright.async_api import async_playwright, Browser, Page, Response


# ==================== 設定 ====================

@dataclass
class Config:
    """設定クラス"""
    chatwork_token: str = os.getenv('CHATWORK_TOKEN', '987cf44efbf5529a09b1317a85058640')
    admin_room_id: str = '413142921'
    shop_config_file: str = 'shop_config.json'
    report_file: str = 'site-status-report.json'
    timeout: int = 30000  # ミリ秒
    slow_response_threshold: int = 10000  # 10秒


CONFIG = Config()


# ==================== ロガー ====================

class Logger:
    """カラフルなログ出力"""
    
    @staticmethod
    def _log(level: str, message: str, color: str = '') -> None:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        reset = '\033[0m' if color else ''
        print(f"{color}[{level}] {timestamp} - {message}{reset}")
    
    @staticmethod
    def info(message: str) -> None:
        Logger._log('INFO', message)
    
    @staticmethod
    def success(message: str) -> None:
        Logger._log('SUCCESS', message, '\033[92m')  # 緑
    
    @staticmethod
    def warn(message: str) -> None:
        Logger._log('WARN', message, '\033[93m')  # 黄
    
    @staticmethod
    def error(message: str) -> None:
        Logger._log('ERROR', message, '\033[91m')  # 赤
    
    @staticmethod
    def separator() -> None:
        print('=' * 60)


# ==================== データクラス ====================

@dataclass
class SiteCheckResult:
    """サイトチェック結果"""
    site_name: str
    display_name: str
    category: str
    url: str
    script: str
    priority: int
    url_index: int
    status: Optional[int] = None
    status_text: str = ''
    response_time: int = 0
    error: Optional[str] = None
    checked_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class StatusReport:
    """ステータスレポート"""
    checked_at: str
    total_sites: int
    normal_count: int
    error_count: int
    slow_count: int
    normal_sites: List[Dict[str, Any]]
    error_sites: List[Dict[str, Any]]
    slow_sites: List[Dict[str, Any]]
    all_results: List[Dict[str, Any]]


# ==================== サイトチェッカー ====================

class SiteStatusChecker:
    """サイトステータスチェッカー"""
    
    def __init__(self):
        self.browser: Optional[Browser] = None
        self.results: List[SiteCheckResult] = []
    
    async def initialize(self) -> None:
        """ブラウザ初期化"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-blink-features=AutomationControlled'
            ]
        )
        Logger.info('ブラウザ起動完了')
    
    async def close(self) -> None:
        """ブラウザ終了"""
        if self.browser:
            await self.browser.close()
            Logger.info('ブラウザ終了')
    
    async def check_site(self, config: Dict[str, Any]) -> SiteCheckResult:
        """個別サイトチェック"""
        display_name = config['display_name']
        category = config['category']
        scraping_url = config['scraping_url']
        py_file = config['py_file']
        priority = config['priority']
        url_index = config['url_index']
        
        site_name = f"{display_name}_{category}"
        Logger.info(f"チェック開始: {site_name} ({scraping_url[:50]}...)")
        
        start_time = time.time()
        result = SiteCheckResult(
            site_name=site_name,
            display_name=display_name,
            category=category,
            url=scraping_url,
            script=py_file,
            priority=priority,
            url_index=url_index
        )
        
        page: Optional[Page] = None
        
        try:
            page = await self.browser.new_page()
            
            # User-Agent設定
            await page.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                              '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'Accept-Language': 'ja,en-US;q=0.9'
            })
            
            # ページアクセス
            response: Response = await page.goto(
                scraping_url,
                wait_until='domcontentloaded',
                timeout=CONFIG.timeout
            )
            
            result.response_time = int((time.time() - start_time) * 1000)
            result.status = response.status
            result.status_text = response.status_text
            
            # HTTPステータス確認
            if result.status == 200:
                Logger.success(f"  ✅ {site_name}: HTTP {result.status} ({result.response_time}ms)")
            else:
                result.error = f"HTTP {result.status}"
                Logger.error(f"  ❌ {site_name}: HTTP {result.status}")
        
        except Exception as e:
            result.error = str(e)
            result.response_time = int((time.time() - start_time) * 1000)
            Logger.error(f"  ❌ {site_name}: {str(e)}")
        
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass
        
        return result
    
    async def check_all_sites(self, shop_config: List[Dict[str, Any]]) -> None:
        """全サイトチェック"""
        Logger.separator()
        Logger.info(f"🔍 サイトステータスチェック開始: {len(shop_config)}サイト")
        Logger.separator()
        
        for config in shop_config:
            if not config.get('is_active', True):
                continue
            
            result = await self.check_site(config)
            self.results.append(result)
            
            # サイト間隔（1秒）
            await asyncio.sleep(1)
        
        Logger.separator()
        Logger.success(f"✅ チェック完了: {len(self.results)}サイト")
        Logger.separator()
    
    def generate_report(self) -> StatusReport:
        """レポート生成"""
        normal_sites = [r for r in self.results if r.status == 200]
        error_sites = [r for r in self.results if r.status != 200 or r.error]
        slow_sites = [r for r in self.results if r.response_time > CONFIG.slow_response_threshold]
        
        # dataclassをdictに変換
        def to_dict(result: SiteCheckResult) -> Dict[str, Any]:
            return {
                'siteName': result.site_name,
                'displayName': result.display_name,
                'category': result.category,
                'url': result.url,
                'script': result.script,
                'priority': result.priority,
                'urlIndex': result.url_index,
                'status': result.status,
                'statusText': result.status_text,
                'responseTime': result.response_time,
                'error': result.error,
                'checkedAt': result.checked_at
            }
        
        return StatusReport(
            checked_at=datetime.now().isoformat(),
            total_sites=len(self.results),
            normal_count=len(normal_sites),
            error_count=len(error_sites),
            slow_count=len(slow_sites),
            normal_sites=[to_dict(r) for r in normal_sites],
            error_sites=[to_dict(r) for r in error_sites],
            slow_sites=[to_dict(r) for r in slow_sites],
            all_results=[to_dict(r) for r in self.results]
        )
    
    def save_report(self, report: StatusReport) -> None:
        """レポート保存"""
        try:
            with open(CONFIG.report_file, 'w', encoding='utf-8') as f:
                json.dump(report.__dict__, f, ensure_ascii=False, indent=2)
            Logger.success(f"📄 レポート保存: {CONFIG.report_file}")
        except Exception as e:
            Logger.error(f"レポート保存失敗: {str(e)}")


# ==================== ChatWork通知 ====================

class ChatWorkNotifier:
    """ChatWork通知"""
    
    def __init__(self, token: str):
        self.token = token
        self.base_url = 'https://api.chatwork.com/v2'
    
    def send_report(self, report: StatusReport) -> bool:
        """レポート送信"""
        message = self._format_report(report)
        
        try:
            response = requests.post(
                f"{self.base_url}/rooms/{CONFIG.admin_room_id}/messages",
                headers={'X-ChatWorkToken': self.token},
                data={'body': message},
                timeout=10
            )
            response.raise_for_status()
            Logger.success(f"📨 ChatWork通知送信成功 → ルーム{CONFIG.admin_room_id}")
            return True
        except Exception as e:
            Logger.error(f"ChatWork通知失敗: {str(e)}")
            return False
    
    def _format_report(self, report: StatusReport) -> str:
        """レポートフォーマット"""
        timestamp = datetime.fromisoformat(report.checked_at).strftime('%Y-%m-%d %H:%M:%S')
        
        lines = ["[info]"]
        lines.append("=" * 40)
        lines.append("🔍 サイトステータスレポート")
        lines.append(f"🕐 {timestamp}")
        lines.append("=" * 40)
        lines.append("")
        
        lines.append("【統計】")
        lines.append(f"✅ 正常: {report.normal_count}件")
        lines.append(f"❌ 異常: {report.error_count}件")
        lines.append(f"🐢 遅延: {report.slow_count}件 (10秒以上)")
        lines.append(f"📊 総計: {report.total_sites}サイト")
        lines.append("")
        
        # 異常サイト詳細
        if report.error_count > 0:
            lines.append(f"【❌ 異常サイト: {report.error_count}件】")
            for site in report.error_sites:
                status = site.get('status') or 'N/A'
                error = site.get('error') or 'Unknown'
                lines.append(f"  ❌ {site['siteName']}")
                lines.append(f"     HTTP: {status} / {error}")
                lines.append(f"     URL: {site['url'][:60]}...")
            lines.append("")
        
        # 遅延サイト詳細
        if report.slow_count > 0:
            lines.append(f"【🐢 遅延サイト: {report.slow_count}件】")
            for site in report.slow_sites:
                seconds = site['responseTime'] / 1000
                lines.append(f"  🐢 {site['siteName']}: {seconds:.1f}秒")
            lines.append("")
        
        # 正常サイト（簡易表示）
        if report.normal_count > 0:
            lines.append(f"【✅ 正常サイト: {report.normal_count}件】")
            avg_time = sum(s['responseTime'] for s in report.normal_sites) / len(report.normal_sites)
            lines.append(f"  平均レスポンス: {avg_time / 1000:.1f}秒")
            lines.append("")
        
        lines.append("=" * 40)
        lines.append("[/info]")
        
        return "\n".join(lines)


# ==================== メイン処理 ====================

async def main() -> None:
    """メイン処理"""
    try:
        Logger.separator()
        Logger.info('🚀 サイトステータスチェッカー起動')
        Logger.separator()
        
        # shop_config.json読み込み
        if not Path(CONFIG.shop_config_file).exists():
            Logger.error(f"{CONFIG.shop_config_file} が見つかりません")
            sys.exit(1)
        
        with open(CONFIG.shop_config_file, 'r', encoding='utf-8') as f:
            shop_config = json.load(f)
        
        active_configs = [c for c in shop_config if c.get('is_active', True)]
        Logger.info(f"対象サイト: {len(active_configs)}件")
        
        # チェッカー初期化
        checker = SiteStatusChecker()
        await checker.initialize()
        
        try:
            # 全サイトチェック
            await checker.check_all_sites(active_configs)
            
            # レポート生成
            report = checker.generate_report()
            
            # レポート保存
            checker.save_report(report)
            
            # ChatWork通知（異常がある場合のみ）
            if report.error_count > 0 or report.slow_count > 0:
                notifier = ChatWorkNotifier(CONFIG.chatwork_token)
                notifier.send_report(report)
            else:
                Logger.info('💚 全サイト正常 - ChatWork通知スキップ')
            
            # サマリー表示
            Logger.separator()
            Logger.info('【最終結果】')
            Logger.info(f"✅ 正常: {report.normal_count}件")
            Logger.info(f"❌ 異常: {report.error_count}件")
            Logger.info(f"🐢 遅延: {report.slow_count}件")
            Logger.separator()
        
        finally:
            await checker.close()
    
    except Exception as e:
        Logger.error(f"致命的エラー: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# ==================== エントリーポイント ====================

if __name__ == '__main__':
    try:
        asyncio.run(main())
        Logger.success('✅ チェック完了')
        sys.exit(0)
    except KeyboardInterrupt:
        Logger.warn('⚠️ ユーザーによる中断')
        sys.exit(0)
    except Exception as e:
        Logger.error(f"エラー: {str(e)}")
        sys.exit(1)