#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hacker News HTML scraper (legal, no API)
- Crawls news.ycombinator.com with polite delays and pagination
- Saves clean CSV suitable for IR project pipelines
"""
import csv, time, random, logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser

BASE = "https://news.ycombinator.com"
START_PATH = "/news"        
CSV_PATH = Path("data/outputs/hn_posts.csv")
N_PAGES = 10        
REQUEST_TIMEOUT = 20
MIN_DELAY, MAX_DELAY = 1.5, 3.5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def robots_allowed(path: str) -> bool:
    rp = RobotFileParser()
    rp.set_url(f"{BASE}/robots.txt")
    rp.read()
    return rp.can_fetch(HEADERS["User-Agent"], f"{BASE}{path}")

def polite_get(session: requests.Session, url: str, max_retry: int = 4) -> requests.Response:
    backoff = 2.0
    for i in range(max_retry):
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            return r
        if r.status_code in (429, 403, 503):
            sleep_for = backoff + random.uniform(0.4, 1.2)
            logging.warning(f"{r.status_code} on {url} → retry in {sleep_for:.1f}s")
            time.sleep(sleep_for)
            backoff = min(backoff * 1.8, 20)
            continue
        r.raise_for_status()
    raise RuntimeError(f"Failed after {max_retry} retries → {url}")

def ensure_csv_header() -> None:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0:
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "id", "source_type", "source_name", "url", "title",
                "score", "num_comments", "rank", "published_at",
                "author", "permalink", "fetched_at", "language"
            ])

def parse_listing(html: str, limit: Optional[int] = None) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict] = []

    items = soup.select("tr.athing")
    for it in items[:limit] if limit else items:
        post_id = it.get("id", "")
        rank = (it.select_one("span.rank").text.strip().rstrip(".")
                if it.select_one("span.rank") else "")
        title_a = it.select_one("a.storylink, a.titlelink")
        title = title_a.text.strip() if title_a else ""
        href = title_a.get("href", "") if title_a else ""
        subtext = it.find_next_sibling("tr").select_one("td.subtext")
        score_span = subtext.select_one("span.score") if subtext else None
        score = score_span.text.split()[0] if score_span else "0"
        author_a = subtext.select_one("a.hnuser") if subtext else None
        author = author_a.text.strip() if author_a else ""
        comments = "0"
        if subtext:
            links = subtext.select("a")
            if links:
                last = links[-1].text.strip()
                if "comment" in last:
                    comments = last.split()[0] if last.split()[0].isdigit() else "0"

        rows.append({
            "id": post_id,
            "source_type": "social",           
            "source_name": "HackerNews",
            "url": href if href.startswith("http") else (BASE + "/" + href.lstrip("/")),
            "title": title,
            "score": score,
            "num_comments": comments,
            "rank": rank,
            "published_at": "",                 
            "author": author,
            "permalink": f"{BASE}/item?id={post_id}" if post_id else "",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "language": "en",
        })
    return rows

def find_next_path(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    more = soup.select_one("a.morelink")
    if not more:
        return None
    href = more.get("href", "")
    if href.startswith("http"):
        return href.replace(BASE, "")
    return href if href.startswith("/") else f"/{href}"

def save_csv(rows: List[Dict]) -> None:
    if not rows:
        return
    new = not CSV_PATH.exists()
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        if new:
            w.writeheader()
        w.writerows(rows)

def main():
    if not robots_allowed(START_PATH):
        raise SystemExit("Blocked by robots.txt for the selected path.")
    ensure_csv_header()

    all_rows: List[Dict] = []
    current_path = START_PATH

    with requests.Session() as s:
        s.headers.update(HEADERS)
        for page in range(N_PAGES):
            url = f"{BASE}{current_path}"
            logging.info(f"Fetching page {page+1}/{N_PAGES}: {url}")
            resp = polite_get(s, url)
            page_rows = parse_listing(resp.text)
            save_csv(page_rows)
            all_rows.extend(page_rows)

            next_path = find_next_path(resp.text)
            if not next_path:
                logging.info("No more pages.")
                break
            current_path = next_path
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    logging.info(f"Saved {len(all_rows)} posts → {CSV_PATH}")

if __name__ == "__main__":
    main()
