"""
Reuters-21578 dataset utilities:
- download_and_extract: fetches the .tar.gz and extracts into data/external/reuters21578
- parse_sgm_dir: parses SGML files to a pandas DataFrame with columns:
  id, title, text, topics (list[str])

Notes:
- SGML is parsed with BeautifulSoup's html.parser/lxml for robustness.
- Some documents have multiple TOPICS; we keep all as a list.
"""
from __future__ import annotations

import os
import tarfile
import re
from pathlib import Path
from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup as BS

DATA_DIR = Path("data/external/reuters21578")
ARCHIVE_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/reuters21578-mld/reuters21578.tar.gz"
ARCHIVE_PATH = DATA_DIR.with_suffix(".tar.gz")


def download_and_extract(url: str = ARCHIVE_URL, out_dir: Path = DATA_DIR) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    if not ARCHIVE_PATH.exists():
        resp = requests.get(url, stream=True, timeout=60)
        resp.raise_for_status()
        with open(ARCHIVE_PATH, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)
    with tarfile.open(ARCHIVE_PATH, "r:gz") as tar:
        tar.extractall(out_dir)
    return out_dir


def _extract_text(reuters_tag) -> str:
    # Prefer BODY; fallback to TEXT
    body = reuters_tag.find("body")
    if body and body.text:
        return body.text.strip()
    text = reuters_tag.find("text")
    return text.text.strip() if text and text.text else ""


def _extract_title(reuters_tag) -> str:
    title = reuters_tag.find("title")
    return title.text.strip() if title and title.text else ""


def _extract_topics(reuters_tag) -> List[str]:
    topics_tag = reuters_tag.find("topics")
    if not topics_tag:
        return []
    return [d.text.strip() for d in topics_tag.find_all("d") if d.text]


SGM_PATTERN = re.compile(r"reut2-\d{3}\.sgm$", re.I)


def parse_sgm_dir(sgm_dir: Path = DATA_DIR) -> pd.DataFrame:
    rows = []
    for root, _, files in os.walk(sgm_dir):
        for name in files:
            if not SGM_PATTERN.search(name):
                continue
            p = Path(root) / name
            with open(p, "rb") as f:
                data = f.read()
            # Parse as html/sgml
            soup = BS(data, "lxml") if BS else None
            if soup is None:
                continue
            for r in soup.find_all("reuters"):
                rid = r.get("newid") or r.get("id") or ""
                title = _extract_title(r)
                text = _extract_text(r)
                topics = _extract_topics(r)
                rows.append({
                    "id": rid,
                    "title": title,
                    "text": text,
                    "topics": topics,
                })
    df = pd.DataFrame(rows)
    # Deduplicate by id
    if not df.empty:
        df = df.drop_duplicates(subset=["id"])  # keep first occurrence
    return df


if __name__ == "__main__":
    d = download_and_extract()
    df = parse_sgm_dir(d)
    out = Path("data/processed/reuters21578_raw.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"Wrote {len(df)} rows -> {out}")
