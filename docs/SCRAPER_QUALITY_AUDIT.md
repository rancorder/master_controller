# 個別スクレイパー品質監査・均一化設計

> **監査日:** 2026-07-15  
> **対象:** `shop_config.json` で有効化されている個別スクレイパー48本  
> **監査方法:** コード静的監査、設定とのURL/index照合、Master Controllerのstdout契約との照合  
> **重要:** 本監査は「約6カ月間、システム内部不具合なく稼働」という実績を否定しない。評価対象は、ショップ側URL・DOM変更が起きた際の復旧速度、改修安全性、保守性の均一度である。

---

## 1. 結論

個別 `.py` の品質差は大きい。ただし、全ファイルを同じ大規模クラス構造へ書き換えるべきではない。

現状は大きく次の二極に分かれている。

1. **短い実動スクリプト**
   - 40〜150行程度
   - 現場で動かしやすい
   - 依存関係と障害点が少ない
   - 一方で `except: pass`、0件理由不明、timeout不足、stdout混在が多い

2. **エンタープライズ型単一ファイル**
   - 350〜900行程度
   - 型、Retry、Circuit Breaker、Validator、ADRが充実
   - 一方で共通処理を各ファイルへ複製し、修正箇所とレビュー量が増大

均一化の正解は、**抽出ロジックを同じにすることではなく、運用シェルを共通化すること**である。

- 共通化する80%: Product、Result、timeout、retry、logger、validation、dedupe、stdout、終了コード、URL index実行
- サイト固有として残す20%: URL、セレクタ、DOM待機、売切判定、ページネーション、価格・商品名の抽出

目標は「全部を500行へ揃える」ことではなく、通常サイトを**80〜180行の薄いAdapter**へ揃えることである。

---

## 2. 品質基準

評価記号は、過去の障害数や実運用の安定性ではなく、**静的な改修容易性・診断性・契約の明確さ**を示す。C/Dであっても、現行環境で安定稼働している事実とは矛盾しない。

### A: 改修基準として使える

- URL/index契約が明確
- timeout・HTTPエラー処理あり
- validation・sold-out・順序の意識あり
- リソース解放が明確
- 失敗理由を追跡可能
- ただし共通コード重複は別途改善対象

### B: 実用的だが統一余地あり

- 実運用に必要な主要機能はある
- ログ、例外、出力、重複排除の一部が不統一
- 共通コアへ載せることで安定してAへ上げられる

### C: 動作はするが修復性が弱い

- シンプルで実績はある
- bare except、失敗理由消失、validation不足、stdout混在などがある
- ショップ仕様変更時の調査時間が担当者依存になる

### D: 優先監査・修正対象

- 設定と実装の不整合
- 無制限ループ
- timeoutなし
- stdout誤パースの可能性
- 診断用コードが本番設定で有効

---

## 3. 全48本の評価

| # | ファイル | 評価 | 型 | 主な強み | 主な均一化ポイント |
|---:|---|:---:|---|---|---|
| 1 | `camera_collection.py` | C | Playwright最小型 | 短く明快、timeoutあり | bare except、0件理由、cleanup統一 |
| 2 | `koseki_camera.py` | C+ | requests最小型 | timeout、`raise_for_status` | bare except、Result/終了コード |
| 3 | `comphotocamera.py` | D | requests最小型 | 単純な表解析 | **timeoutなし**、HTTPエラー、例外処理 |
| 4 | `hatosya.py` | C | 旧式HTML型 | Shift_JIS明示、価格関数分離 | bare except、HTTPエラー、売切方針 |
| 5 | `takashina_camera.py` | B | Playwright実用型 | fallback selector、sold-out、timeout | stdout/stderr分離、Result統一 |
| 6 | `akasaka_camera.py` | C+ | requests複数URL型 | URL marker、sold-out、timeout | bare except、validation、終了コード |
| 7 | `suwa_shashinkan.py` | A- | Enterprise Playwright型 | index整合ADR、Retry、Validator | 共通基盤重複、部分成功契約 |
| 8 | `kikuya.py` | D | 診断用Playwright | stderr診断、可視要素確認 | **診断コードの本番利用、2店舗設定不整合、上位5件限定** |
| 9 | `nittou.py` | B+ | requests実用型 | max pages、retry、dedupe、明示ログ | stdoutログ分離、共通Result |
| 10 | `oumicamera.py` | C | Playwright最小型 | もっと見る回数上限 | hashed class依存、bare except |
| 11 | `camera-ohnuki.py` | C+ | requests診断型 | fallback selector、dedupe | stdout診断過多、例外、O(n²)重複確認 |
| 12 | `ohbayash.py` | A- | Enterprise requests型 | 新着順ADR、Retry、型、Validator | 800行級の共通処理複製 |
| 13 | `sanwa.py` | C+ | requests実用型 | timeout、validation、dedupe | stdout診断、bare except、O(n²) |
| 14 | `sanpo.py` | C | Playwright最小型 | 単純で理解しやすい | bare except、HTTP/DOM待機、sold-out |
| 15 | `matsuo.py` | D+ | Playwright最小型 | 商品カード解析が明確 | **load-more無制限ループ**、例外・上限 |
| 16 | `matsuzakaya.py` | B | Playwright実用型 | pagination、finally cleanup、dedupe | stdoutログ、共通Result、bare except |
| 17 | `lucky-camera.py` | C | Playwright最小型 | ページ数上限、timeout | 例外、validation、sold-out、cleanup |
| 18 | `first-shokai.py` | C+ | 詳細ページ巡回型 | fallback豊富、最大20件、cleanup | ページ全体価格探索の誤結合、問い合わせ価格と親契約 |
| 19 | `syuukou.py` | C+ | requests実用型 | timeout、validation、stderr一部利用 | `verify=False`常用、警告全抑制 |
| 20 | `jw.py` | B- | requests分離型 | fetch/parse分離、timeout、HTTP検証 | sold-out、dedupe、Result |
| 21 | `fujikoshi.py` | C+ | requests複数URL型 | retry、timeout、EUC-JP、marker | bare except、非数値価格、DOM探索範囲 |
| 22 | `penguincam.py` | B- | 旧式HTML柔軟型 | 3方式fallback、dedupe、timeout | 抽出ロジック重複、例外粒度 |
| 23 | `tanaridocamera.py` | B- | requests多URL型 | **11 URLのindex対応が明快** | bare except、sold-out、Result |
| 24 | `camerakids.py` | C+ | requests複数URL診断型 | index、timeout、dedupe | selector診断をstderrへ、bare except |
| 25 | `otsukashokai.py` | D+ | Playwright pagination型 | ページ遷移が単純 | **無制限pagination**、例外、sold-out、stdout見出し |
| 26 | `antiquary.py` | B | requests複数セクション型 | 関数分離、retry、encoding、marker | debug stdout、bare except、終了コード |
| 27 | `suzuki.py` | B- | 旧式HTML実用型 | sold-out、timeout、dedupe、件数上限 | bare except、ページ別結果可視化 |
| 28 | `gtcamera.py` | C+ | requests最小型 | fetch/price関数分離、timeout | HTTP検証、sold-out、Result |
| 29 | `kitsunedou.py` | C+ | Playwright実用型 | cleanup、fallback、dedupe | 価格0を内部成功扱い、stdoutログ |
| 30 | `oscamera.py` | D+ | 近傍行推測型 | 柔軟な旧式HTML対応 | **診断商品行がstdoutで商品として再パースされ得る、価格誤結合リスク** |
| 31 | `tokiwa-camera.py` | C | requests fallback型 | timeout、複数selector | bare except、sold-out、dedupe |
| 32 | `yaotomi.py` | C+ | body正規表現型 | timeout、HTTP検証、30件上限 | body横断で商品名/価格の誤結合、dedupe不足 |
| 33 | `isio28_clean.py` | B- | 旧式table実用型 | 全角数字対応、validation、dedupe | stdoutログ、bare except、テーブル決め打ち |
| 34 | `ymmtca.py` | A- | Enterprise requests多URL型 | 8 URL、rate limit、encoding、ADR | 共通基盤重複、部分成功契約 |
| 35 | `re_camera_shop.py` | C+ | requests複数URL型 | 3 URL、timeout、dedupe | 商品名がメーカーのみか要確認、bare except |
| 36 | `hardoff.py` | A- | 専用Enterprise型 | 非同期並列、画像URL、商品コード、ADR | **有効商品ありでもexit 1となる条件**、共通基盤重複 |
| 37 | `buysell.py` | B- | requests複数URL型 | 2カテゴリ、timeout、fallback | marker二重出力、dedupe、stdout診断 |
| 38 | `wonderrex.py` | A- | Shopify JSON API型 | HTML非依存、index明確、Retry、型 | 共通基盤重複、部分成功契約 |
| 39 | `naniwa.py` | A- | 値下げ専用型 | 価格状態モデル、3 URL、ADR | 共通基盤重複、親の新商品モデルとの境界 |
| 40 | `okoku.py` | B+ | Enterprise requests型 | Retry、Validator、整合ADR | 単一サイトに500行超、共通処理複製 |
| 41 | `rakuten_koseki.py` | B+ | requests実用堅牢型 | stderrログ、retry、429/5xx対応、validation | 共通Result、ログ様式統一 |
| 42 | `hayata_camera.py` | B | 詳細ページ巡回クラス型 | retry、エラー蓄積、型、validation | 580行級、設定URLとの不一致確認、stdout要約 |
| 43 | `uctrade.py` | B+ | Enterprise requests型 | ADR、Retry、Validator、型 | 共通処理複製、部分成功契約 |
| 44 | `suginami_camera.py` | B | Enterprise Playwright型 | 動的サイト判断、Retry、validation | selectorが広すぎる、商品ごとのINFO過多、共通処理複製 |
| 45 | `mediajoy.py` | B+ | Enterprise requests型 | 静的サイト判断、parser分離、Retry | 単一サイトに共通基盤複製、部分成功契約 |
| 46 | `keiz_camera.py` | A- | iframe専用型 | iframe直アクセス＋fallback、Resource Manager | 共通Result/Loggerへ寄せる、compact import |
| 47 | `nisshindo.py` | B+ | Playwright複数URL型 | 4 URL、sold-out、selector待機、cleanup | stdout診断、共通Result、bare except |
| 48 | `kanto_camera.py` | B+ | Playwright Shopify型 | sale優先、validation、bounded pagination | selector広め、共通基盤化、dedupe確認 |

---

## 4. P0: 最初に確認・修正すべき項目

### 4.1 `kikuya.py` の設定・実装不整合

`shop_config.json` では同じ `kikuya.py` が異なる2店舗へ設定され、どちらも `url_index: 0` である。一方、コードは1店舗のURLだけをハードコードし、可視商品の先頭5件のみを出力する診断版である。

これは過去障害の断定ではなく、現時点の静的な設定不整合である。次のどちらかへ分離する。

- `kikuya_iwate.py`
- `kikuya_camera_watch.py`

または1ファイル内で `url_index: 0/1` を明示する。

### 4.2 `oscamera.py` のstdout汚染

解析途中で次の形式をstdoutへ出している。

```text
商品1: 商品名... 12300円
```

Master Controllerは価格を含む行を商品候補として読むため、後段の正式商品行と二重登録される可能性がある。進捗・診断はstderrへ移す。

### 4.3 `comphotocamera.py` のtimeout欠如

Master Controller側に120秒のsubprocess timeoutはあるが、個別HTTP timeoutがないため、1サイトが親の上限まで占有する。全requestsに明示timeoutを必須化する。

### 4.4 無制限ループ

- `matsuo.py`: load-moreボタンが消えるまで無制限
- `otsukashokai.py`: nextリンクが消えるまで無制限

最大ページ数、最大クリック回数、既訪問URL、商品増分ゼロの停止条件を追加する。

### 4.5 `hardoff.py` の終了コード

有効商品が存在しても、全URL成功かつ20件以上でなければ `PARTIAL_SUCCESS=1` を返す。現行Master Controllerはreturn code 0の時だけ商品を処理するため、取得済み商品が無視され得る。

個別スクレイパーの内部状態と、親へ返すprocess exit codeを分離する。

### 4.6 URLの二重管理差分

例として `hayata_camera.py` のコードURLと `shop_config.json` の通知・監視URLが異なる。redirect等で意図的な場合もあるため、即時修正ではなく、URL整合テストと例外許可リストを設ける。

---

## 5. 目標アーキテクチャ

```text
scraper_core/
├── models.py          # Product, ScrapeResult, UrlResult
├── output.py          # marker/product/stdout契約
├── logging.py         # stderr logger
├── validation.py      # name/price/url validation
├── http.py            # requests session, timeout, retry
├── playwright.py      # browser/context/page lifecycle
├── dedupe.py          # 順序保持dedupe
├── runner.py          # 複数URL実行、exit policy
└── errors.py          # FetchError, ParseError, EmptyResultError

scrapers/
├── akasaka_camera.py  # URL + parse(html) + sold-outのみ
├── tanaridocamera.py
└── ...
```

現行のルート直下ファイル名は `shop_config.json` 互換のため維持し、内部から `scraper_core` をimportする方式でもよい。

### 5.1 標準Product

```python
@dataclass(frozen=True, slots=True)
class Product:
    name: str
    price: int
    url_index: int
    rank: int
    image_url: str = ""
    product_url: str = ""
```

### 5.2 標準Adapter契約

```python
class ScraperAdapter(Protocol):
    urls: Sequence[str]

    def fetch(self, url: str, url_index: int) -> str: ...
    def parse(self, payload: str, url_index: int) -> list[Product]: ...
```

Playwrightの場合はpayloadをHTMLまたはPageに置き換える。

### 5.3 stdout/stderr契約

stdout:

```text
---URL_INDEX:0---
商品名 12800円
```

stderr:

```text
INFO url_index=0 fetched=20 parsed=18 skipped_soldout=2
ERROR url_index=1 stage=parse selector=.product-card reason=not_found
```

商品行以外をstdoutへ出さない。

### 5.4 終了コード

- `0`: 1件以上の有効商品をstdoutへ出した
- `2`: 全URLで有効商品0件、または実行不能
- URL単位の部分失敗はstderrとResultへ残すが、他URLの商品が有効ならprocessは0

これは現行Master Controllerとの互換方針である。

---

## 6. 共通化すべきもの／してはいけないもの

### 共通化する

- timeout
- retry/backoff
- User-Agent
- HTTP status validation
- Playwright cleanup
- Product/Result
- name/price validation
- 順序保持dedupe
- stdout formatter
- stderr logger
- process exit code
- URL index整合検証
- 最大ページ・最大クリック・最大商品数

### 共通化しない

- CSS selector
- 古いtable構造の列判定
- sold-out判定のサイト固有条件
- JavaScript待機条件
- OCR
- iframe
- Shopify JSON API
- 値下げ状態モデル
- 商品順を決定するサイト固有ロジック

サイト固有ロジックを無理に抽象化すると、変更時に全サイトへ影響する。

---

## 7. 推奨移行順

### Phase 0: 契約テスト

コードを書き換える前に、全48本へ最低限の自動検査を追加する。

- activeなファイルが存在する
- URL indexの重複・欠番
- 内部URL数とconfig行数
- stdout marker
- 商品行のparent parser投入
- 商品順維持
- timeoutの存在
- bare `except` 数
- stdoutへ診断価格行が混ざらない

### Phase 1: P0のみ修正

1. `kikuya.py`
2. `oscamera.py`
3. `comphotocamera.py`
4. `matsuo.py`
5. `otsukashokai.py`
6. `hardoff.py` exit policy
7. URL二重管理差分の確認

既存商品順とスナップショットを変えない。

### Phase 2: 共通コア最小版

最初は次の5機能だけに限定する。

1. Product
2. validation
3. stderr logger
4. stdout writer
5. exit policy

RetryやCircuit Breakerを最初から全面移行しない。

### Phase 3: requests型移行

1. 単一URL・静的サイト
2. 複数URL・静的サイト
3. 旧式encodingサイト

### Phase 4: Playwright型移行

1. 単一ページ
2. load-more/pagination
3. iframe
4. OCR・特殊動作

### Phase 5: Enterprise単一ファイルの縮小

`hardoff.py`、`naniwa.py`、`wonderrex.py` 等から共通部分を抜き、ドメイン固有ADR・Parserは保持する。

---

## 8. 完成基準

個別スクレイパーは、次を全て満たせば品質均一化完了とする。

1. `shop_config.json` とURL/indexが一致
2. 商品順が実サイトの監視対象順と一致
3. timeoutが明示されている
4. fetch失敗とparse 0件を区別できる
5. sold-out方針がコードまたは文書にある
6. 商品名・価格validationが共通
7. 重複排除が順序を壊さない
8. stdoutはmarkerと商品だけ
9. ログはstderr
10. 有効商品があればexit 0
11. pagination/load-moreに上限がある
12. fixtureでparser単体テスト可能
13. browser/sessionが必ずcloseされる
14. 秘密情報を持たない
15. 既存snapshotを不用意に初期化しない

---

## 9. 最重要判断

本システムの強さは、48サイトの異質な構造へ個別適応している点にある。

したがって、品質均一化は「全コードを同じ見た目にする作業」ではない。

> **外側の契約と運用部品は統一し、内側のサイト固有知識は保護する。**

この方針が、6カ月安定稼働の実績を維持しながら、ショップ側URL・DOM変更時の復旧速度を最も高める。