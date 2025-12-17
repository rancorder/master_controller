import requests
from bs4 import BeautifulSoup

BASE_URL = "https://j2camera.jp/index.php?main_page=products_new"
MAX_PAGES = 1  # 必要に応じて増やす

def fetch_html(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=20)
        res.raise_for_status()
        return res.text
    except Exception as e:
        print(f"HTML取得失敗: {e}")
        return None

def parse_items(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("td[colspan='2'].main")
    results = []

    for row in rows:
        title_tag = row.select_one("strong")
        if not title_tag:
            continue
        title = title_tag.get_text(strip=True)

        # 価格
        price_text = row.get_text()
        if "価格:" not in price_text:
            continue
        price_line = [line for line in price_text.split("\n") if "価格:" in line][0]
        # 価格の数字のみを抽出
        import re
        price_match = re.search(r'価格:\s*([0-9,]+)', price_line)
        price = price_match.group(1).replace(",", "") if price_match else "0"

        # メーカー
        manufacturer_tag = row.select_one("a[href*='manufacturer']")
        manufacturer = manufacturer_tag.get_text(strip=True) if manufacturer_tag else ""

        # 品名にメーカー名を付加
        display_name = f"{manufacturer}{title}" if manufacturer else title

        results.append({
            "品名": display_name,
            "価格": price
        })

    return results

def parse_all_pages():
    all_items = []
    for page_num in range(1, MAX_PAGES + 1):
        url = f"{BASE_URL}&page={page_num}" if page_num > 1 else BASE_URL
        html = fetch_html(url)
        if not html:
            break
        items = parse_items(html)
        if not items:
            break
        all_items.extend(items)
    return all_items

if __name__ == "__main__":
    items = parse_all_pages()
    
    for item in items:
        # 価格から数字のみ抽出（既に数字のみの場合もある）
        price = str(item['価格']).replace(',', '')
        print(f"{item['品名']} {price}円")