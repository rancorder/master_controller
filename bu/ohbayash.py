import requests
from bs4 import BeautifulSoup
import re

BASE_URL = "https://www.camera-no-ohbayashi.co.jp"
SEARCH_URL = BASE_URL + "/view/search?sort=recommend"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def scrape_ohbayashi():
    try:
        print(f"🔍 {SEARCH_URL} にアクセス中...")
        response = requests.get(SEARCH_URL, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 商品リンクを探す（/view/item/ を含むリンク）
        product_links = soup.find_all("a", href=lambda x: x and "/view/item/" in x)
        print(f"🔗 商品リンク発見: {len(product_links)} 個")
        
        for link in product_links:
            try:
                # 商品名を取得
                name = link.get_text(strip=True)
                if not name or len(name) < 3:
                    continue
                
                # 価格を探す - より広範囲で検索
                price_text = "価格不明"
                
                # 方法1: 親要素全体のテキストから価格を探す
                current_element = link
                for _ in range(3):  # 3階層上まで探す
                    if current_element.parent:
                        current_element = current_element.parent
                        element_text = current_element.get_text()
                        
                        # より広いパターンで価格を探す
                        price_patterns = [
                            r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)（税込）',
                            r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)円',
                            r'¥(\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                            r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)税込',
                        ]
                        
                        for pattern in price_patterns:
                            matches = re.findall(pattern, element_text)
                            if matches:
                                price_text = matches[-1] + "円"  # 最後にマッチした価格
                                break
                        
                        if price_text != "価格不明":
                            break
                
                # 方法2: 次の兄弟要素から価格を探す
                if price_text == "価格不明":
                    next_sibling = link.next_sibling
                    if next_sibling:
                        sibling_text = str(next_sibling)
                        price_match = re.search(r'(\d{1,3}(?:,\d{3})*)', sibling_text)
                        if price_match:
                            price_text = price_match.group(1) + "円"
                
                # 方法3: リンクの後にある全てのテキストノードから価格を探す
                if price_text == "価格不明":
                    # リンクの後にある全てのテキストを取得
                    all_following_text = ""
                    current = link
                    while current.next_sibling:
                        current = current.next_sibling
                        if hasattr(current, 'get_text'):
                            all_following_text += current.get_text()
                        else:
                            all_following_text += str(current)
                    
                    # 価格パターンを探す
                    price_match = re.search(r'(\d{1,3}(?:,\d{3})*)', all_following_text)
                    if price_match:
                        price_text = price_match.group(1) + "円"
                
                # 価格から数字のみ抽出
                price = price_text.replace(',', '').replace('円', '').replace('¥', '').strip()
                print(f"{name} {price}円")
                
            except Exception as e:
                print(f"⚠️ 商品処理エラー: {e}")
                continue
        
    except Exception as e:
        print(f"❌ エラー: {e}")

if __name__ == "__main__":
    scrape_ohbayashi()