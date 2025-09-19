from pathlib import Path
import csv
from urllib.parse import urlparse


# RAW stage (written by crawlers)
RAW_HEADERS = [
"id", # stable unique id (e.g., md5 of canonical URL)
"source_type", # 'reddit' | 'news'
"source_name", # subreddit name OR news domain (netloc)
"url", # canonical URL of the post/article
"title",
"text",
"author", # optional, empty if unknown
"published_at", # unix epoch or ISO8601 if available, else empty
"score", # quality signals (reddit)
"num_comments", # quality signals (reddit)
"fetched_at", # ISO8601 of fetch time
]


# PROCESSED stage (after enrichment)
PROCESSED_HEADERS = RAW_HEADERS + [
"lang", # detected language code
"token_count", # token count of the raw text (simple whitespace tokens)
]


# FINAL stage (after labeling)
FINAL_HEADERS = PROCESSED_HEADERS + [
"label", # predicted topic label (single) or pipe-separated multi-labels
]




def ensure_header(csv_path: Path, headers: list[str]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(headers)




def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""