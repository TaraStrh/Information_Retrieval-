from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import csv, time, random, os
from datetime import datetime, timezone
from pathlib import Path

SUBREDDITS = ["news", "worldnews", "technology", "politics", "sports"]
BASE = "https://news.ycombinator.com"  
CSV_PATH = Path("reddit_posts_selenium.csv")
DEBUG_DIR = Path("debug"); DEBUG_DIR.mkdir(exist_ok=True)

def setup_driver(headless=False):
    opts = Options()
    if headless is True:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(45)
    return driver

def maybe_accept_consent(driver):
    if "consent" in driver.current_url:
        try:
            btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[.//*[contains(text(),'Accept') or contains(text(),'AGREE') or contains(text(),'accept')]] | //button[contains(text(),'Accept') or contains(text(),'AGREE')]")
                )
            )
            btn.click()
            time.sleep(2)
        except Exception:
            pass

def wait_for_posts(driver):
    try:
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.thing, div[data-testid='post-container']")
            )
        )
        return True
    except Exception:
        return False

def parse_posts_from_html(html, subreddit, limit=30):
    soup = BeautifulSoup(html, "html.parser")
    posts = []

    things = soup.select("div.thing")
    if not things:
        things = soup.select("div[data-testid='post-container']")

    for it in things[:limit]:
        title_a = it.select_one("a.title")
        comments_a = it.select_one("a.comments")
        pid = it.get("data-fullname") or it.get("id") or ""

        if not title_a:
            title_h3 = it.find("h3")
            title_a = title_h3
        if not comments_a:
            comments_a = it.select_one("a[data-click-id='comments']")

        url = ""
        if title_a and title_a.has_attr("href"):
            url = title_a["href"]
        if not url:
            body_a = it.select_one("a[data-click-id='body']")
            if body_a and body_a.has_attr("href"):
                url = body_a["href"]

        post = {
            "id": pid,
            "subreddit": subreddit,
            "title": (title_a.text.strip() if title_a else ""),
            "url": url,
            "num_comments": (comments_a.text.strip() if comments_a else ""),
            "permalink": "",
            "score": it.get("data-score") or "",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        posts.append(post)
    return posts

def save_csv(rows):
    if not rows:
        return
    new = not CSV_PATH.exists()
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        if new:
            w.writeheader()
        w.writerows(rows)

def main():
    driver = setup_driver(headless=False) 
    all_rows = []
    try:
        for sub in SUBREDDITS:
            url = f"{BASE}/r/{sub}/"
            print(f"Fetching {url} ...")
            driver.get(url)
            time.sleep(2)
            maybe_accept_consent(driver)

            ok = wait_for_posts(driver)
            if not ok:
                png = DEBUG_DIR / f"{sub}.png"
                html = DEBUG_DIR / f"{sub}.html"
                driver.save_screenshot(str(png))
                with open(html, "w", encoding="utf-8") as fh:
                    fh.write(driver.page_source)
                print(f"No posts detected on r/{sub}. Saved {png.name} and {html.name} for debug.")
                continue

            rows = parse_posts_from_html(driver.page_source, sub, limit=30)
            print(f" → parsed {len(rows)} posts from r/{sub}")
            save_csv(rows)
            all_rows.extend(rows)
            time.sleep(random.uniform(2.5, 5.0))
        print(f"Saved total {len(all_rows)} posts → {CSV_PATH}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
