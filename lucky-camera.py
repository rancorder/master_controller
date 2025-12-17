from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

BASE_URL_TEMPLATE = "https://lucky-camera.com/item/page/{}/"
PAGES_TO_SCRAPE = 1

def scrape_lucky_camera():
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for page_num in range(1, PAGES_TO_SCRAPE + 1):
            url = BASE_URL_TEMPLATE.format(page_num)
            page.goto(url, timeout=60000)
            page.wait_for_timeout(1500)
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            articles = soup.select("article.article")

            for art in articles:
                name_tag = art.select_one("h4 a")
                price_tag = art.select_one("div.price a")
                if name_tag and price_tag:
                    name = name_tag.text.strip()
                    price = price_tag.text.strip()
                    results.append({"name": name, "price": price})

        browser.close()
    return results


if __name__ == "__main__":
    items = scrape_lucky_camera()
    
    for item in items:
        # 価格から数字のみ抽出
        price = str(item['price']).replace(',', '').replace('円', '').replace('¥', '').strip()
        print(f"{item['name']} {price}円")
