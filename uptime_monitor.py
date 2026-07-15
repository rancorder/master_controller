#!/usr/bin/env python3
"""
master_controller稼働時間監視 + ChatWork通知
"""

import psutil
import sqlite3
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

class UptimeMonitor:
    def __init__(
        self, 
        db_path: str = "uptime_stats.db",
        chatwork_token: str = "987cf44efbf5529a09b1317a85058640",
        chatwork_room_id: str = "413142921"
    ):
        self.db_path = db_path
        self.process_name = "master_controller"
        self.chatwork_token = chatwork_token
        self.chatwork_room_id = chatwork_room_id
        self._init_db()
    
    def _init_db(self):
        """統計DB初期化"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS uptime_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_seconds INTEGER,
                stop_reason TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS current_session (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                pid INTEGER,
                start_time TIMESTAMP,
                last_check TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS notification_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                notification_date DATE UNIQUE,
                notification_time TIMESTAMP,
                uptime_hours REAL,
                total_sessions INTEGER
            )
        """)
        conn.commit()
        conn.close()
    
    def find_process(self) -> Optional[psutil.Process]:
        """master_controllerプロセスを検索"""
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if 'master_controller' in cmdline and 'python' in cmdline:
                    return proc
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return None
    
    def initialize_from_existing_process(self) -> tuple[Optional[int], Optional[datetime]]:
        """既存プロセスから初期化"""
        proc = self.find_process()
        if proc:
            try:
                start_timestamp = proc.create_time()
                start_time = datetime.fromtimestamp(start_timestamp)
                
                print(f"既存プロセス検出:")
                print(f"  PID: {proc.pid}")
                print(f"  起動時刻: {start_time}")
                print(f"  現在の稼働時間: {datetime.now() - start_time}")
                
                self._record_session_start(proc.pid, start_time)
                return proc.pid, start_time
            except Exception as e:
                print(f"エラー: {e}")
        return None, None
    
    def send_chatwork_notification(self, message: str) -> bool:
        """ChatWorkに通知送信"""
        url = f"https://api.chatwork.com/v2/rooms/{self.chatwork_room_id}/messages"
        headers = {
            "X-ChatWorkToken": self.chatwork_token
        }
        data = {
            "body": message
        }
        
        try:
            response = requests.post(url, headers=headers, data=data)
            if response.status_code == 200:
                print(f"ChatWork通知送信成功: {datetime.now()}")
                return True
            else:
                print(f"ChatWork通知失敗: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"ChatWork通知エラー: {e}")
            return False
    
    def check_and_send_daily_notification(self) -> bool:
        """1日1回の通知チェック・送信"""
        today = datetime.now().date()
        
        # 今日既に送信済みか確認
        conn = sqlite3.connect(self.db_path)
        existing = conn.execute(
            "SELECT id FROM notification_log WHERE notification_date = ?",
            (today,)
        ).fetchone()
        conn.close()
        
        if existing:
            return False  # 既に送信済み
        
        # 統計取得
        stats = self.get_statistics()
        
        # メッセージ生成
        message = self._generate_daily_report(stats)
        
        # 送信
        if self.send_chatwork_notification(message):
            # 送信ログ記録
            conn = sqlite3.connect(self.db_path)
            current_uptime = stats["current_session"]["running_hours"] if stats["current_session"] else 0
            conn.execute(
                """INSERT INTO notification_log 
                   (notification_date, notification_time, uptime_hours, total_sessions) 
                   VALUES (?, ?, ?, ?)""",
                (today, datetime.now(), current_uptime, stats["total_sessions"])
            )
            conn.commit()
            conn.close()
            return True
        
        return False
    
    def _generate_daily_report(self, stats: dict) -> str:
        """日次レポートメッセージ生成"""
        lines = [
            "[info][title]master_controller 稼働レポート[/title]",
            f"報告日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ""
        ]
        
        if stats["current_session"]:
            cs = stats["current_session"]
            lines.extend([
                "■ 現在の状態: 稼働中 ✓",
                f"  起動時刻: {cs['start_time']}",
                f"  連続稼働: {cs['running_hours']:.2f} 時間",
                ""
            ])
        else:
            lines.extend([
                "■ 現在の状態: 停止中 ✗",
                ""
            ])
        
        lines.extend([
            "■ 累計統計",
            f"  総セッション数: {stats['total_sessions']}",
            f"  累計稼働時間: {stats['total_uptime_hours']:.2f} 時間",
            f"  平均稼働時間: {stats['avg_uptime_hours']:.2f} 時間",
            f"  最長稼働記録: {stats['max_uptime_hours']:.2f} 時間",
            "[/info]"
        ])
        
        return "\n".join(lines)
    
    def monitor_loop(self, check_interval: int = 60, notification_hour: int = 9):
        """
        監視ループ
        
        Args:
            check_interval: プロセスチェック間隔（秒）
            notification_hour: 通知送信時刻（時）
        """
        print(f"監視開始: {datetime.now()}")
        print(f"通知時刻: 毎日 {notification_hour}:00")
        
        current_pid = None
        session_start = None
        
        # 既存プロセスから初期化
        current_pid, session_start = self.initialize_from_existing_process()
        
        while True:
            try:
                now = datetime.now()
                
                # プロセス監視
                proc = self.find_process()
                
                if proc:
                    if current_pid != proc.pid:
                        # 新しいセッション開始
                        if current_pid:
                            self._record_session_end(
                                session_start, now, "process_restart"
                            )
                        
                        current_pid = proc.pid
                        start_timestamp = proc.create_time()
                        session_start = datetime.fromtimestamp(start_timestamp)
                        self._record_session_start(current_pid, session_start)
                        print(f"[{now}] 新セッション検出 PID:{current_pid}")
                    
                    self._update_last_check(now)
                
                else:
                    if current_pid:
                        # プロセス停止検出
                        self._record_session_end(
                            session_start, now, "process_stopped"
                        )
                        print(f"[{now}] プロセス停止検出 稼働時間:{now - session_start}")
                        current_pid = None
                        session_start = None
                
                # 1日1回の通知チェック（指定時刻以降）
                if now.hour >= notification_hour:
                    self.check_and_send_daily_notification()
                
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                print("\n監視終了")
                if current_pid and session_start:
                    self._record_session_end(
                        session_start, datetime.now(), "monitor_stopped"
                    )
                break
            except Exception as e:
                print(f"エラー: {e}")
                time.sleep(check_interval)
    
    def _record_session_start(self, pid: int, start_time: datetime):
        """セッション開始記録"""
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT OR REPLACE INTO current_session VALUES (1, ?, ?, ?)",
            (pid, start_time, start_time)
        )
        conn.commit()
        conn.close()
    
    def _update_last_check(self, check_time: datetime):
        """最終チェック時刻更新"""
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "UPDATE current_session SET last_check = ? WHERE id = 1",
            (check_time,)
        )
        conn.commit()
        conn.close()
    
    def _record_session_end(self, start: datetime, end: datetime, reason: str):
        """セッション終了記録"""
        duration = int((end - start).total_seconds())
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            """INSERT INTO uptime_records 
               (start_time, end_time, duration_seconds, stop_reason) 
               VALUES (?, ?, ?, ?)""",
            (start, end, duration, reason)
        )
        conn.execute("DELETE FROM current_session WHERE id = 1")
        conn.commit()
        conn.close()
    
    def get_statistics(self) -> dict:
        """統計情報取得"""
        conn = sqlite3.connect(self.db_path)
        
        current = conn.execute(
            "SELECT pid, start_time FROM current_session WHERE id = 1"
        ).fetchone()
        
        stats = conn.execute("""
            SELECT 
                COUNT(*) as session_count,
                SUM(duration_seconds) as total_uptime,
                AVG(duration_seconds) as avg_uptime,
                MAX(duration_seconds) as max_uptime,
                MIN(duration_seconds) as min_uptime
            FROM uptime_records
        """).fetchone()
        
        conn.close()
        
        result = {
            "current_session": None,
            "total_sessions": stats[0] or 0,
            "total_uptime_hours": round((stats[1] or 0) / 3600, 2),
            "avg_uptime_hours": round((stats[2] or 0) / 3600, 2),
            "max_uptime_hours": round((stats[3] or 0) / 3600, 2),
            "min_uptime_hours": round((stats[4] or 0) / 3600, 2) if stats[4] else 0
        }
        
        if current:
            start = datetime.fromisoformat(current[1])
            current_duration = (datetime.now() - start).total_seconds()
            result["current_session"] = {
                "pid": current[0],
                "start_time": current[1],
                "running_hours": round(current_duration / 3600, 2)
            }
        
        return result
    
    def print_report(self):
        """レポート表示"""
        stats = self.get_statistics()
        print("\n=== master_controller 稼働統計 ===")
        
        if stats["current_session"]:
            cs = stats["current_session"]
            print(f"\n現在のセッション:")
            print(f"  PID: {cs['pid']}")
            print(f"  開始: {cs['start_time']}")
            print(f"  稼働時間: {cs['running_hours']} 時間")
        else:
            print("\n現在のセッション: なし（停止中）")
        
        print(f"\n過去の統計:")
        print(f"  総セッション数: {stats['total_sessions']}")
        print(f"  累計稼働時間: {stats['total_uptime_hours']} 時間")
        print(f"  平均稼働時間: {stats['avg_uptime_hours']} 時間")
        print(f"  最長稼働時間: {stats['max_uptime_hours']} 時間")


if __name__ == "__main__":
    monitor = UptimeMonitor(
        chatwork_token="987cf44efbf5529a09b1317a85058640",
        chatwork_room_id="413142921"
    )
    
    monitor.print_report()
    print("\n監視を開始します（Ctrl+Cで終了）")
    
    # 毎日9時に通知（変更可能）
    monitor.monitor_loop(check_interval=60, notification_hour=9)