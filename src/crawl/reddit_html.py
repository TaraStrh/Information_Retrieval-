# HTML-only Reddit scraper (old.reddit.com)
# - No API usage
# - Respects robots.txt (via PoliteSession.can_fetch)
# - Randomized backoff, crawl-delay, min spacing handled by PoliteSession
# - Canonicalization + dedup via post URL
# - Stable ID = md5(permalink)
# - Fields written: id, source, url, title, text_or_summary, created_utc, score, num_comments, author, fetched_at

import csv
import hashlib
import logging
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup as BS

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from utils.polite_fetch import PoliteSession

BASE = "https://old.reddit.com"
SUBS = ["news", "worldnews", "technology", "politics", "sports"]
PAGES_PER_SUB = 2  # Reduced to avoid too many requests

# More realistic browser headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'DNT': '1',
}

TIMEOUT = 30  # Increased timeout for proxy connections
CSV_PATH = Path("data/outputs/reddit_raw.csv")
CSV_HEADERS = [
    "id",
    "source_type",
    "source_name",
    "url",
    "title",
    "text",
    "author",
    "published_at",
    "score",
    "num_comments",
    "fetched_at",
]

def ensure_header(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_HEADERS)

def stable_id(permalink: str) -> str:
    return hashlib.md5(permalink.encode("utf-8")).hexdigest()

def parse_listing(html: str) -> Tuple[List[Dict], Optional[str]]:
    soup = BS(html, "lxml")
    posts = []
    for thing in soup.select("div.thing"):
        title_el = thing.select_one("a.title")
        permalink = thing.get("data-permalink") or (
            thing.select_one("a.comments").get("href") if thing.select_one("a.comments") else ""
        )
        if not permalink:
            continue
        url = urljoin(BASE, permalink)
        title = title_el.get_text(strip=True) if title_el else ""
        posts.append({"url": url, "title": title})
    nextbtn = soup.select_one("span.next-button > a")
    next_url = nextbtn["href"] if nextbtn else None
    return posts, next_url

def parse_post_page(html: str) -> Dict:
    soup = BS(html, "lxml")

    # Title
    title_el = soup.select_one("a.title") or soup.select_one("h1.title")
    title = title_el.get_text(strip=True) if title_el else ""

    # Selftext (for self posts)
    body_el = soup.select_one("div.usertext-body") or soup.select_one("div.expando .usertext-body")
    text = body_el.get_text(" ", strip=True) if body_el else ""

    # Timestamp
    created_utc = None
    time_el = soup.select_one("time")
    if time_el and time_el.has_attr("datetime"):
        try:
            dt = datetime.fromisoformat(time_el["datetime"].replace("Z", "+00:00"))
            created_utc = int(dt.timestamp())
        except Exception:
            pass

    # Score
    score = None
    score_el = soup.select_one("div.score.likes")
    if score_el:
        tit = score_el.get("title") or score_el.get_text(strip=True)
        m = re.search(r"(\d+)", tit or "")
        if m:
            score = int(m.group(1))

    # Comments
    num_comments = None
    c_el = soup.select_one("a.comments")
    if c_el:
        m = re.search(r"(\d+)", c_el.get_text(strip=True))
        if m:
            num_comments = int(m.group(1))

    # Author
    author = None
    a_el = soup.select_one("a.author")
    if a_el:
        author = a_el.get_text(strip=True)

    return {
        "title": title,
        "text": text,
        "created_utc": created_utc,
        "score": score,
        "num_comments": num_comments,
        "author": author,
    }

def fetch_page(polite_session: PoliteSession, url: str) -> Optional[BS]:
    """Fetch a page using PoliteSession for robots.txt compliance"""
    try:
        html = polite_session.get_html(url)
        if html:
            return BS(html, 'html.parser')
        return None
    except Exception as e:
        logging.error(f"Failed to fetch {url}: {e}")
        return None

def crawl() -> None:
    ensure_header(CSV_PATH)
    
    # Use PoliteSession for robots.txt compliance
    polite_session = PoliteSession(
        user_agent="Mozilla/5.0 (compatible; IRProject/1.0; Educational)",
        max_retries=3,
        min_interval=2.0
    )

    for sub in SUBS:
        url = f"{BASE}/r/{sub}/"
        pages = 0
        seen = set()

        # Check robots.txt first
        if not polite_session.can_fetch(url):
            print(f"Robots.txt disallows crawling {url}, skipping /r/{sub}")
            continue

        # listing pages
        while url and pages < PAGES_PER_SUB:
            print(f"Fetching page {pages + 1} of {PAGES_PER_SUB} for /r/{sub}: {url}")
            soup = fetch_page(polite_session, url)
            if not soup:
                print(f"Failed to fetch {url}, moving to next subreddit")
                break

            try:
                posts, next_url = parse_listing(str(soup))
            except Exception as e:
                print(f"Error parsing listing: {e}")
                break

            rows = []
            for item in posts:
                post_url = item.get("url")
                if not post_url or post_url in seen:
                    continue
                seen.add(post_url)

                # Fetch individual post page for full content
                print(f"  Fetching post: {post_url}")
                if polite_session.can_fetch(post_url):
                    post_soup = fetch_page(polite_session, post_url)
                    if post_soup:
                        post_data = parse_post_page(str(post_soup), item)
                        if post_data:
                            rows.append(post_data)
                            print(f"    Saved: {post_data['title'][:50]}...")
                else:
                    print(f"    Skipped (robots.txt): {post_url}")

            # Write batch to CSV
            if rows:
                with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                    writer.writerows(rows)
                print(f"Saved {len(rows)} posts from /r/{sub} page {pages + 1}")
            
            url = next_url
            pages += 1
            # PoliteSession handles delays automatically

    print("Reddit crawling completed!")
    
    # Show summary
    if CSV_PATH.exists():
        with open(CSV_PATH, 'r') as f:
            line_count = sum(1 for line in f) - 1  # Subtract header
        print(f"Total posts collected: {line_count}")
    else:
        print("No data collected")

if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("reddit_crawler.log"),
            logging.StreamHandler()
        ]
    )
    
    try:
        print("Starting Reddit crawler...")
        crawl()
        print("Reddit crawling completed!")
        
        # Show summary
        if CSV_PATH.exists():
            with open(CSV_PATH, 'r') as f:
                line_count = sum(1 for line in f) - 1  # Subtract header
            print(f"Total posts collected: {line_count}")
        else:
            print("No data collected")
    except Exception as e:
        logging.error(f"Reddit crawler failed: {e}")
        print(f"Crawler failed: {e}")
