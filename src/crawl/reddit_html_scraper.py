# hn_scraper.py
import requests, csv, time, random
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from pathlib import Path

BASE = "https://news.ycombinator.com"
CSV_PATH = Path("hn_posts.csv")
TARGET_POSTS = 300

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

def fetch_page(path="/news"):
    url = f"{BASE}{path}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.text

def parse_listing(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    items = soup.select("tr.athing")
    for it in items:
        rank = it.select_one("span.rank")
        title_a = it.select_one("a.storylink, a.titlelink")
        post_id = it.get("id", "")
        subtext = it.find_next_sibling("tr").select_one("td.subtext")
        score_span = subtext.select_one("span.score") if subtext else None
        comments_a = None
        if subtext:
            links = subtext.select("a")
            comments_a = links[-1] if links else None

        rows.append({
            "id": post_id,
            "rank": (rank.text.strip().rstrip(".") if rank else ""),
            "title": (title_a.text.strip() if title_a else ""),
            "url": (title_a.get("href","") if title_a else ""),
            "score": (score_span.text.split()[0] if score_span else "0"),
            "comments": (comments_a.text if comments_a else "0 comments"),
            "permalink": f"{BASE}/item?id={post_id}" if post_id else "",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        })
    return rows

def find_next_path(html):
    soup = BeautifulSoup(html, "html.parser")
    more = soup.select_one("a.morelink")
    if more:
        return more.get("href")
    return None

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
    collected = 0
    path = "/news"
    all_rows = []

    while collected < TARGET_POSTS and path:
        html = fetch_page(path)
        rows = parse_listing(html)

        need = TARGET_POSTS - collected
        rows = rows[:need]

        save_csv(rows)
        all_rows.extend(rows)
        collected += len(rows)
        print(f"Collected {collected}/{TARGET_POSTS} …")

        path = find_next_path(html)
        time.sleep(random.uniform(1.2, 2.5))  

    print(f"Saved {len(all_rows)} posts → {CSV_PATH}")

if __name__ == "__main__":
    main()
