import re, hashlib
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from .logger import get_logger
from .robots import allowed
from .rate_limit import DomainRateLimiter
from .downloader import fetch
from .canonicalize import canonicalize_url
from .queue import CrawlQueue
from ..config import NEWS_SEEDS, USER_AGENT, CRAWL_DEPTH, NEWS_CSV
from ..storage.writer import CSVDatasetWriter
try:
    from readability import Document  # readability-lxml normal path
except Exception:
    try:
        from readability.readability import Document  # some envs expose it here
    except Exception:
        Document = None

logger = get_logger("news")

MAIN_SEL = "article, main, div[itemprop='articleBody'], div#content"
TITLE_SEL = "h1, h1.headline, h1.story-title, h1[itemprop='headline']"

SAFE_LINK_RE = re.compile(r"^(https?://)")

def sha_uid(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()

def extract_links(html: str, base_url: str):
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a"):
        href = a.get("href")
        if not href:
            continue
        if not SAFE_LINK_RE.search(href):
            href = urljoin(base_url, href)
        yield href

def looks_like_trap(u: str) -> bool:
    u = u.lower()
    if re.search(r"/page/\d{2,}|[?&]page=\d{2,}", u):
        return True
    if re.search(r"(calendar|events)/\d{4}", u):
        return True
    return False

def extract_article(html: str):
    # Try readability first if available
    title = ""
    text = ""
    if Document is not None:
        try:
            doc = Document(html)
            title = (doc.short_title() or "").strip()
            article_html = doc.summary(html_partial=True)
            if article_html:
                soup2 = BeautifulSoup(article_html, "lxml")
                text = soup2.get_text(" ", strip=True)
        except Exception:
            pass  # fall back to pure-BeautifulSoup below

    # --- Fallbacks (BeautifulSoup-only) ---
    soup_full = BeautifulSoup(html, "lxml")

    if not title:
        tnode = soup_full.select_one("h1, h1.headline, h1.story-title, h1[itemprop='headline']")
        if tnode:
            title = tnode.get_text(" ", strip=True)

    if not text:
        parts = [p.get_text(" ", strip=True) for p in soup_full.find_all("p")]
        text = "\n\n".join([p for p in parts if p])

    # Published time if present
    published_at = ""
    t = soup_full.find("time")
    if t and (t.get("datetime") or t.text):
        published_at = t.get("datetime") or t.get_text(" ", strip=True)

    return title, text, published_at


def run():
    rl = DomainRateLimiter()
    q = CrawlQueue()
    writer = CSVDatasetWriter(NEWS_CSV)
    for seed in NEWS_SEEDS:
        q.seed(seed, source=urlparse(seed).netloc, record_type="news")

    rows = []
    while True:
        item = q.pop()
        if not item:
            break
        url, depth, source, record_type = item
        can_url = canonicalize_url(url)
        if looks_like_trap(can_url):
            logger.info(f"Skipping trap-like URL: {can_url}")
            continue
        if not allowed(USER_AGENT, can_url):
            logger.info(f"Disallowed by robots: {can_url}")
            continue
        rl.wait(can_url)
        code, final_url, html = fetch(can_url)
        if not html:
            continue

        title, text, published_at = extract_article(html)
        uid = sha_uid(can_url)
        rows.append({
            "uid": uid, "record_type": "news", "source": source, "url": can_url,
            "title": title, "text": text, "author": "", "published_at": published_at,
            "lang": "", "score": "", "num_comments": "", "created_ts": datetime.now(timezone.utc).isoformat()
        })

        if depth < CRAWL_DEPTH:
            for href in extract_links(html, can_url):
                if not href.startswith("http"):
                    continue
                p = urlparse(href)
                if p.netloc != urlparse(url).netloc:
                    continue
                if looks_like_trap(href):
                    continue
                q.push(href, depth+1, source=source, record_type="news")

    if rows:
        writer.append_rows(rows)
        logger.info(f"Wrote {len(rows)} news rows to {NEWS_CSV}")
    else:
        logger.info("No news rows scraped.")

if __name__ == "__main__":
    run()
