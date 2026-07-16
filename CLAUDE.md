# CLAUDE.md

Claude Codeは、本リポジトリを変更する前に以下を必ず読むこと。

1. [`AGENTS.md`](AGENTS.md)
2. [`docs/SYSTEM_ARCHITECTURE.md`](docs/SYSTEM_ARCHITECTURE.md)
3. `master_controller_v29.py`
4. 対象スクレイパー
5. `shop_config.json` 内の対象 `py_file` 全設定行

`AGENTS.md` をClaude Codeにも適用される共通作業規約とする。このファイルは入口であり、詳細規約を重複定義しない。

---

## 最重要ルール

- ルート直下が現行コード。`bu/` は過去版・退避であり、通常は変更しない
- 通常スクレイパーはone-shotで終了させる
- stdoutを外部APIとして扱う
- 複数URLでは `---URL_INDEX:n---` を維持する
- 商品の出力順を変えない。先頭商品は差分検知上の1位である
- `||` 後段は画像URLの1要素だけ。商品詳細URLを追加しない
- 利用可能な商品がある場合、現行親が成功扱いできる終了コード0を返す
- `shop_config.json` とスクレイパー内部URL配列を必ず照合する
- 既存 `url_index` を並べ替えない
- 通常スクレイパーからChatWorkへ直接通知しない
- 秘密情報をコード・文書・PRへ書かない
- state、snapshot、DB、log、reportを意図せずコミットしない

---

## 修正前の調査

対象ファイルだけを見て修正を始めない。

```bash
# 対象スクリプトの設定行を確認
python3 - <<'PY'
import json
from pathlib import Path

TARGET = 'TARGET.py'
data = json.loads(Path('shop_config.json').read_text(encoding='utf-8'))
for row in data:
    if row.get('py_file') == TARGET:
        print(row)
PY
```

確認すること:

- URL数と順序
- 全url_index
- priority
- category
- 通知ルーム
- active状態
- 対象サイトの実DOM
- one-shotか独立常駐か

---

## 修正後の最低検証

```bash
python3 -m py_compile TARGET.py
python3 TARGET.py > /tmp/target-output.txt
printf 'exit=%s\n' "$?"
head -100 /tmp/target-output.txt
```

親パーサー確認:

```bash
python3 - <<'PY'
from pathlib import Path
from master_controller_v29 import StableDataExtractor

text = Path('/tmp/target-output.txt').read_text(encoding='utf-8')
result = StableDataExtractor().extract_stable(text, 'TARGET.py')
print(result)
PY
```

ネットワークアクセスが使えない環境では、構文確認、fixture、既存HTML、純粋関数のテストを先に行い、実サイト未検証を明記する。推測で「修正完了」としない。

---

## 大きな改修を行う場合

以下を別コミットまたは別PRへ分離する。

- secrets環境変数化
- `.gitignore` と生成物整理
- stdout契約テスト
- 終了コード統一
- version/README整合
- `custom_interval` 対応
- P1スケジュール修正
- 共通scraper core抽出
- dashboard構造化イベント化

サイト1件のDOM修正と基盤リファクタを同時に行わない。
