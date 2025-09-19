# Web news crawler per spec (depth=2)
# - Seeds: multiple news domains
# - BFS queue with depth cap = 2
# - Canonicalization and deduplication
# - Respects robots.txt + crawl-delay via PoliteSession
# - Extracts main article text via trafilatura
# - Fields: id, source, url, title, text, created_at, fetched_at

import csv
import hashlib
import random
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup as BS
import trafilatura

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from utils.polite_fetch import PoliteSession

UA = "ir-news-crawler/1.0 (+https://example.edu)"
TIMEOUT = 12
MIN_INTERVAL = 0.8
CSV_PATH = Path("data/outputs/news_raw.csv")
HEADERS = ["id", "source_type", "source_name", "url", "title", "text", "author", "published_at", "score", "num_comments", "fetched_at"]

SEEDS = [
    "https://www.bbc.com/news",
    "https://www.reuters.com/world/",
    "https://apnews.com/",
]

ARTICLE_HINTS = ["/news", "/article", "/story", "/world", "/politics", "/tech", "/business", "/science", "/health"]

def ensure_header(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADERS)

def canonicalize(url: str) -> str:
    p = urlparse(url)
    # strip fragment; keep query (you can drop UTM if you want to)
    return p._replace(fragment="").geturl()

def same_domain(u: str, root: str) -> bool:
    return urlparse(u).netloc == urlparse(root).netloc

def extract_links(base_url: str, html: str) -> List[str]:
    soup = BS(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a["href"].strip()
        if href.startswith("#"):
            continue
        full = urljoin(base_url, href)
        links.append(canonicalize(full))
    return links

def extract_article(html: str, url: str) -> tuple[str, str]:
    soup = BS(html, "lxml")
    title = soup.title.get_text(strip=True) if soup.title else ""
    try:
        text = trafilatura.extract(html, url=url, include_comments=False, favor_precision=True) or ""
    except Exception:
        text = ""
    return title, text

def stable_id(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()

def crawl_news():
    ensure_header(CSV_PATH)
    client = PoliteSession(user_agent=UA, timeout=TIMEOUT, max_retries=4, min_interval=MIN_INTERVAL)
    seen: Set[str] = set()

    for seed in SEEDS:
        q = deque([(seed, 0)])
        while q:
            url, depth = q.popleft()
            if depth > 2:
                continue
            url = canonicalize(url)
            if url in seen:
                continue
            seen.add(url)

            if not client.can_fetch(url):
                continue
            response = client.get(url)
            if not response or response.status_code != 200:
                continue
            html = response.text

            # If URL looks like an article page, try to extract
            if any(seg in url for seg in ARTICLE_HINTS):
                title, text = extract_article(html, url)
                if text and len(text) > 120:
                    _id = stable_id(url)
                    domain = urlparse(url).netloc
                    created_at = ""  # (optional) parse from meta if you need it
                    fetched_at = datetime.now(timezone.utc).isoformat()
                    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
                        csv.writer(f).writerow([_id, "news", domain, url, title, text, "", created_at, "", "", fetched_at])
                    print(f"Saved article: {title[:50]}...")

            # enqueue links within the same domain
            if depth < 2:
                for link in extract_links(url, html):
                    if same_domain(link, seed) and link not in seen:
                        q.append((link, depth + 1))

            # tiny pause between dequeued pages (PoliteSession already spaces per domain)
            time.sleep(0.2 + random.random() * 0.3)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    crawl_news()
