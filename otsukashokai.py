from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

START_URL = "https://otsukashokai.co.jp/used-item-2/"

def scrape_all_pages():
    items = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(START_URL, timeout=60000)

        while True:
            soup = BeautifulSoup(page.content(), "html.parser")
            for detail in soup.select("div.item_details"):
                name_tag = detail.select_one("h2.item_name a")
                price_tag = detail.select_one("span.price")

                name_text = name_tag.get_text(strip=True) if name_tag else ""
                price_text = price_tag.get_text(strip=True) if price_tag else ""

                if name_text and price_text:
                    items.append({
                        "name": name_text,
                        "price": price_text
                    })

            # 次ページがあればクリック
            next_button = page.locator('a.next.page-numbers')
            if next_button.count() > 0:
                next_button.first.click()
                page.wait_for_timeout(1500)
            else:
                break

        browser.close()
    return items

if __name__ == "__main__":
    print("🚀 Otsuka Camera スクレイピング開始")
    items = scrape_all_pages()
    if items:
        print("===== スクレイピング結果 =====")
        for item in items:
            print(f"{item['name']} {item['price']}")
    else:
        print("✅ 商品データが見つかりませんでした")
