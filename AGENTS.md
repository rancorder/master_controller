# AGENTS.md

このファイルは、Codexなどのコーディングエージェントが本リポジトリを変更する際の実行規約である。

詳細設計は必ず [`docs/SYSTEM_ARCHITECTURE.md`](docs/SYSTEM_ARCHITECTURE.md) を先に読むこと。

---

## 1. 最初に読むファイル

変更前に、最低限以下を確認する。

1. `AGENTS.md`
2. `docs/SYSTEM_ARCHITECTURE.md`
3. `master_controller_v29.py`
4. 対象スクレイパー
5. 対象スクレイパーに対応する `shop_config.json` の全行

`README.md` は過去版の起動例・監視件数・バージョンが混在しているため、実装判断の正本にしない。

---

## 2. 現行コードの境界

### 現行として扱う

- `master_controller_v29.py`
- `shop_config.json`
- ルート直下のスクレイパー `.py`
- `site_status_checker.py`
- `dashboard.py`
- `log_viewer.py`
- `uptime_monitor.py`
- 運用shellスクリプト

### 原則変更しない

- `bu/` 配下: 過去版・退避コード
- `*_state.json`, `*_snapshot*.json`, `*_history.json`: 実行状態
- `*.db`, `*.log`: 実行時生成物
- `site-status-report.json`: 監視結果
- OCRスクリーンショットやキャッシュ

履歴ファイルを変更する依頼でない限り、生成物をコミットへ含めない。

---

## 3. 絶対に壊してはいけない契約

### 3.1 スクレイパーは原則one-shot

通常スクレイパーは、起動後に1回取得して終了する。

```bash
python3 scraper.py
```

親プロセスが周期実行するため、通常スクレイパー内へ無限ループや独自スケジューラを追加しない。

`tresure.py` など、明示的な独立常駐監視は例外である。

### 3.2 stdoutはAPI

単一URL:

```text
商品名 12800円
```

複数URL:

```text
---URL_INDEX:0---
商品名A 12800円
---URL_INDEX:1---
商品名B 22000円
```

画像URL付き:

```text
商品名 12800円||https://example.com/image.jpg
```

守ること:

- `url_index` は `shop_config.json` と内部URL配列に一致させる
- 商品を新着順/表示順で出す
- `||` 後段は画像URLの1要素だけ
- 商品詳細URLを第3要素として追加しない
- 利用可能な商品がある場合、親が成功扱いできる終了コード0を返す
- 商品行へ `INFO`, `DEBUG`, `ERROR` 等を付けない

### 3.3 商品の1位は特別

親は各カテゴリの先頭商品を「現在1位」とみなす。商品をset化、辞書順ソート、価格順ソートしてはならない。

DOM順が新着順でない場合は、サイト仕様に基づく正しい新着順へ明示的に整列する。

### 3.4 既存url_indexを動かさない

既存URLのindex変更は、以下を同時に壊す。

- カテゴリ振り分け
- スナップショット
- 通知ルーム
- 差分検知

URL追加は原則として末尾indexへ追加する。

### 3.5 通知は親へ集約

通常スクレイパーからChatWorkへ直接通知しない。

通知文言は以下を変更する。

- `ChatWorkNotifier`
- `HardOffFormatter`

---

## 4. セキュリティ規約

リポジトリには、ChatWorkトークンと見られる値が直書きされた既存ファイルがある。

- 値を回答、文書、Issue、PR本文、ログへ転載しない
- 新しい秘密情報をコミットしない
- 認証情報は `CHATWORK_TOKEN` 等の環境変数から読む
- 既存値は漏洩前提でローテーション対象とする
- サンプルは必ずプレースホルダーを使う

例:

```python
token = os.environ["CHATWORK_TOKEN"]
```

禁止:

```python
token = "実トークン"
```

---

## 5. 対象スクレイパー改修手順

### Step 1: 設定を逆引き

`shop_config.json` で対象 `py_file` の全設定行を確認する。

確認項目:

- 全 `url_index`
- `display_name`
- `category`
- `scraping_url`
- `priority`
- `notification_enabled`
- `is_active`

同じスクリプトが複数URLを持つ場合、1URLだけ見て改修しない。

### Step 2: 内部URL順を確認

スクレイパー内の `TARGET_URLS`, `urls`, `BASE_URL` 等と設定を照合する。

親は設定URLを子へ渡さない。設定とコードの二重管理である。

### Step 3: 最小変更

サイトDOM変更への対応では、まず以下だけを修正する。

- URL
- セレクタ
- 待機条件
- sold-out判定
- ページネーション
- 文字コード

共通基盤の全面書き換えは、契約テストなしで同時に行わない。

### Step 4: 出力確認

```bash
python3 TARGET.py > /tmp/target-output.txt
printf 'exit=%s\n' "$?"
head -100 /tmp/target-output.txt
```

確認:

- 価格が数字として取得できる
- markerが正しい
- 1位が実サイトと一致する
- sold-out商品が意図どおり除外される
- tracebackやデバッグ文が商品として見えない
- 120秒以内を目標に終了する

### Step 5: 親パーサー確認

```bash
python3 - <<'PY'
from pathlib import Path
from master_controller_v29 import StableDataExtractor

text = Path('/tmp/target-output.txt').read_text(encoding='utf-8')
result = StableDataExtractor().extract_stable(text, 'TARGET.py')
print(result['success'])
print(result['count'])
print(sorted({p['url_index'] for p in result['products']}))
print(result['products'][:3])
PY
```

### Step 6: 構文確認

```bash
python3 -m py_compile TARGET.py
python3 -m py_compile master_controller_v29.py
```

---

## 6. 新規スクレイパーの要件

新規ファイルは以下を満たすこと。

- Python 3.11+で動作
- UTF-8
- one-shot
- 明示的なtimeout
- User-Agent設定
- 商品名・価格のvalidation
- sold-out除外方針
- 新着順維持
- 複数URL時のmarker
- 終了コード0/非0の明確化
- 秘密情報なし
- ローカルDB/通知処理なし

推奨構造:

```python
def scrape_url(url: str, url_index: int) -> list[Product]:
    ...


def main() -> int:
    total = 0
    for index, url in enumerate(TARGET_URLS):
        print(f"---URL_INDEX:{index}---")
        for product in scrape_url(url, index):
            print(product.to_output_line())
            total += 1
    return 0 if total > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
```

複数URLの一部失敗でも有効商品を親へ処理させる必要がある場合、現行親との互換性上は終了コード0を返し、失敗URLはstderr/ログで明示する。

---

## 7. `shop_config.json` 変更規約

- JSONとしてvalidに保つ
- activeな `py_file` が実在すること
- 同一スクリプト内でpriorityを混在させない
- `display_name + category` を一意にする
- `url_index` を0から安定して維持する
- `notification_enabled` は実態がルームIDであることを理解する
- 複数ルームはカンマ区切り
- `custom_interval` は現行マスターでは効かないため、変更しただけで動作が変わると考えない

設定変更後は、対象スクリプトのstdout markerと必ず照合する。

---

## 8. `master_controller_v29.py` 変更規約

親コントローラーを変更する場合は、個別サイト修正より高い回帰リスクがある。

最低限、以下のケースを確認する。

1. 単一URLの商品抽出
2. 複数URLの商品抽出
3. 画像URL付き商品
4. 初回実行は通知なし
5. 1位不変
6. 新商品が前回1位の上へ入る
7. 前回1位が消える
8. 重複通知クールダウン
9. P1個別snapshot
10. P2共有snapshot
11. ChatWork token未設定
12. subprocess timeout
13. Playwright semaphore
14. Ctrl+C停止

既知の注意点:

- ファイル名v29と内部VERSION v27が不一致
- P1の最終新商品時刻が、商品取得成功時にも更新される
- `custom_interval` 未使用
- 部分成功の終了コード1を親が失敗扱いする
- Playwright semaphore未取得が成功扱いになる
- SQLite `VACUUM` の実行位置に懸念がある

これらを修正する場合は、1つのPRへ無関係な変更を詰め込まない。

---

## 9. 周辺ツール変更規約

### `site_status_checker.py`

HTTP到達性監視であり、スクレイピング成功監視ではない。セレクタ破損を検出できると誤解しない。

### `dashboard.py`

以下に依存する。

- `/root/scraper`
- `master_controller.log` の日本語文言
- P2総数の固定推定
- inline HTML

ログ文言変更時はダッシュボードの正規表現も確認する。

### `uptime_monitor.py`

プロセス名ではなくコマンドライン文字列で検出する。起動コマンド変更時は監視への影響を確認する。

---

## 10. 変更してはいけない典型例

- `bu/master_controller_*.py` だけを直して現行修正とみなす
- 商品順をアルファベット順へ整理する
- URL indexを見た目の都合で並べ替える
- scraper stdoutをJSONへ変更する
- 商品詳細URLを `||` の第3要素へ追加する
- 正常取得した部分データがあるのにexit 1を返す
- 既存snapshotをテスト目的で削除する
- tokenをハードコードする
- 全スクレイパーへ一括置換を行い、個別サイトを確認しない
- `requirements_current.txt` をそのまま新環境へインストールする前提にする

---

## 11. 完了条件

スクレイパー修正は、以下を満たした時だけ完了とする。

- [ ] 対象の全設定行を確認した
- [ ] 内部URL順とurl_indexが一致した
- [ ] 構文確認に成功した
- [ ] 個別実行が終了した
- [ ] 終了コードを確認した
- [ ] 親パーサーで商品が抽出できた
- [ ] 1位と商品順を実サイトで確認した
- [ ] 秘密情報を追加していない
- [ ] runtime artifactをコミットしていない
- [ ] 変更範囲と残存リスクをPRへ記載した

親コントローラー修正では、上記に加えて差分検知・通知・P1/P2の回帰確認を行う。
