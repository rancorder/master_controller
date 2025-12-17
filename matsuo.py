from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

START_URL = "https://www.matsuocamera.net/"

def scrape_all_pages():
    results = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(START_URL, timeout=60000)

            # ページ下部までスクロールして「もっと見る」クリックループ
            while True:
                if page.locator('button[data-hook="load-more-button"]').is_visible():
                    page.locator('button[data-hook="load-more-button"]').click()
                    page.wait_for_timeout(1500)
                else:
                    break

            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, "html.parser")
        product_blocks = soup.select('[data-hook="product-item-root"]')

        for block in product_blocks:
            name_tag = block.select_one('[data-hook="product-item-name"]')
            price_tag = block.select_one('[data-hook="product-item-price-to-pay"]')

            # 在庫なし除外（価格がない場合）
            if not name_tag or not price_tag:
                continue

            name = name_tag.get_text(strip=True)
            price_text = price_tag.get_text(strip=True)
            if name and price_text:
                results.append({"name": name, "price": price_text})

    except Exception as e:
        print(f"❌ エラー: {e}")

    return results


if __name__ == "__main__":
    items = scrape_all_pages()
    
    for item in items:
        # 価格から数字のみ抽出
        price = str(item['price']).replace(',', '').replace('円', '').replace('¥', '').strip()
        print(f"{item['name']} {price}円")
