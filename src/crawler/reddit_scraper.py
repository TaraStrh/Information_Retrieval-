import re, hashlib
from datetime import datetime, timezone
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from .logger import get_logger
from .robots import allowed
from .rate_limit import DomainRateLimiter
from .downloader import fetch
from .canonicalize import canonicalize_url
from ..config import SUBREDDITS, USER_AGENT, REDDIT_CSV
from ..storage.writer import CSVDatasetWriter

logger = get_logger("reddit")

POST_SEL = "div.thing.link"
TITLE_SEL = "a.title"
AUTHOR_SEL = "a.author"
SCORE_SEL = "div.score.unvoted"
COMMENTS_SEL = "a.comments"

def sha_uid(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()

def parse_listing(html: str, base_url: str):
    soup = BeautifulSoup(html, "lxml")
    for thing in soup.select(POST_SEL):
        a = thing.select_one(TITLE_SEL)
        if not a:
            continue
        title = a.get_text(strip=True)
        href = a.get("href")
        permalink = thing.get("data-permalink") or ""
        author = (thing.select_one(AUTHOR_SEL).get_text(strip=True) if thing.select_one(AUTHOR_SEL) else "")
        score_txt = (thing.select_one(SCORE_SEL).get("title") if thing.select_one(SCORE_SEL) else "")
        try:
            score = int(score_txt) if score_txt else ""
        except Exception:
            score = ""
        comments_a = thing.select_one(COMMENTS_SEL)
        num_comments = ""
        if comments_a and comments_a.get_text():
            m = re.search(r"(\d+)", comments_a.get_text())
            if m:
                num_comments = int(m.group(1))
        url = urljoin(base_url, permalink or href or "")
        yield {"title": title, "url": url, "author": author, "score": score, "num_comments": num_comments}

def parse_post_page(html: str):
    soup = BeautifulSoup(html, "lxml")
    text_parts = []
    for sel in ["div.expando div.usertext-body", "div.md", "div.entry .usertext-body"]:
        for node in soup.select(sel):
            text_parts.append(node.get_text(" ", strip=True))
    text = "\n\n".join([t for t in text_parts if t])
    published_at = ""
    tnode = soup.select_one("time")
    if tnode and tnode.get("datetime"):
        published_at = tnode.get("datetime")
    return text, published_at

def run():
    rl = DomainRateLimiter()
    writer = CSVDatasetWriter(REDDIT_CSV)
    rows = []
    base = "https://old.reddit.com"
    for sub in SUBREDDITS:
        page = f"{base}/r/{sub}/?sort=new"
        logger.info(f"Scraping subreddit listing: r/{sub}")
        if not allowed(USER_AGENT, page):
            logger.warning(f"Blocked by robots.txt: {page}")
            continue
        rl.wait(page)
        code, final_url, html = fetch(page)
        if not html:
            continue
        for item in parse_listing(html, base):
            post_url = canonicalize_url(item["url"])
            if not allowed(USER_AGENT, post_url):
                logger.info(f"Disallowed post URL by robots: {post_url}")
                continue
            rl.wait(post_url)
            code, final, phtml = fetch(post_url)
            text = ""
            published_at = ""
            if phtml:
                text, published_at = parse_post_page(phtml)
            uid = sha_uid(post_url)
            rows.append({
                "uid": uid, "record_type": "reddit", "source": f"r/{sub}",
                "url": post_url, "title": item["title"], "text": text,
                "author": item.get("author",""), "published_at": published_at, "lang": "",
                "score": item.get("score",""), "num_comments": item.get("num_comments",""),
                "created_ts": datetime.now(timezone.utc).isoformat()
            })
    if rows:
        writer.append_rows(rows)
        logger.info(f"Wrote {len(rows)} rows to {REDDIT_CSV}")
    else:
        logger.info("No rows scraped.")

if __name__ == "__main__":
    run()
