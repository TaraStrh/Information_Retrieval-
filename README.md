# Information Retrieval Final Project — News & Reddit Scraper + Classifier

This implements the full pipeline required by your **Information Retrieval Final Project** PDF:

- **HTML-only** Reddit scraping (no API) via `old.reddit.com`.
- **Multi-domain news crawler** with a **central queue**, BFS to a depth cap, and **canonicalization**.
- **robots.txt** respect, **crawl-delay**, **per-domain rate limiting**, **random jitter**, **exponential backoff** on 3xx/4xx/5xx/timeout.
- **CSV schema** with required fields + optional quality signals (score, comments) and preprocessing outputs.
- **Text preprocessing** (normalization, URL/emoji removal, lowercasing EN, Persian char fixes, tokenization, token counts, language detection).
- **Reuters‑21578**-based **topic labeling** using **TF‑IDF (uni+bi‑gram)** weighted lexicons; score by shared n‑gram weights; pick max.
- **Vertical scalability** knobs; **checkpointing** with SQLite (`data/state.db`) to avoid duplication and safely resume.

> ✏️ Edit `src/config.py` to customize subreddits, seed domains, crawl depth, rate limits, and output paths.
> ⚠️ Always comply with site policies. The code enforces robots.txt but you should also include a **contact** in `USER_AGENT`.

---

## How to run

```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 1) Reddit (HTML only)
python -m scripts.run_reddit

# 2) News crawler
python -m scripts.run_news

# 3) Preprocess (clean + tokens + counts + lang)
python -m scripts.run_preprocess

# 4) Build Reuters lexicon (first time only)
python -m scripts.build_lexicon --reuters_dir /path/to/reuters21578  --top_k 200

# 5) Label your data
python -m scripts.run_classify
```

Outputs land in `data/outputs/`:

- `reddit_raw.csv`, `news_raw.csv`
- `reddit_clean.csv`, `news_clean.csv`
- `reddit_labeled.csv`, `news_labeled.csv`

---

## CSV Schema

**Raw** (`*_raw.csv`)
- `uid`: SHA256 of canonical URL (stable per record)
- `record_type`: `"reddit"` or `"news"`
- `source`: subreddit (`r/worldnews`) or domain (`bbc.com`)
- `url`: canonical URL
- `title`: page/post title
- `text`: post/selftext or article main text (if available)
- `author`: author (if shown)
- `published_at`: ISO8601 UTC or as found in page
- `lang`: detected language (may be empty at this stage)
- `score`: (reddit) numeric score if visible
- `num_comments`: (reddit) number of comments if visible
- `created_ts`: UTC timestamp when scraped

**Clean** (`*_clean.csv`) adds:
- `text_clean`: normalized text (URLs/emojis removed, EN lowercased, Persian char fixes, collapsed whitespace)
- `tokens`: space‑joined tokens
- `tokens_count`: integer

**Labeled** (`*_labeled.csv`) adds:
- `label`: best Reuters category by lexicon score
- `top3`: `cat:score` pipe‑separated top‑3
- `score_max`: max score value

---

## Components

- `src/crawler/reddit_scraper.py` — HTML scraping of subreddit listing + post pages on **old.reddit.com**; extracts title, permalink, author, score, comments, time if present.
- `src/crawler/news_crawler.py` — Central SQLite‑backed queue, BFS within each domain, link extraction, canonicalization, basic trap avoidance (pagination loops), article text/metadata extraction.
- `src/crawler/robots.py` — robots.txt `can_fetch` + `crawl_delay` (cached).
- `src/crawler/rate_limit.py` — per‑domain min delay (robots or defaults) + jitter.
- `src/crawler/downloader.py` — retries with exponential backoff and `Retry-After` support.
- `src/crawler/canonicalize.py` — normalize scheme/host, remove fragments, sort/drop tracking params (`utm_*`, `gclid`, etc.).
- `src/storage/state.py` — SQLite persistence for queue and seen URL hashes (checkpointing).
- `src/storage/writer.py` — CSV writer with header management.
- `src/preprocess/clean_text.py` — normalization + URL/emoji removal + Persian char fixes + lowercasing EN.
- `src/preprocess/tokenize.py` — Unicode‑aware regex tokenizer (EN + Persian).
- `src/preprocess/run_preprocess.py` — applies cleaning and builds `tokens`, `tokens_count`, fills missing `lang` via `langdetect`.
- `src/classify/reuters_build_lexicon.py` — parses `.sgm` files, builds TF‑IDF (uni+bi) centroids per category, exports top‑K weighted lexicons as JSON.
- `src/classify/score_and_label.py` — scores each row by summing weights of overlapping n‑grams; assigns best label + top‑3.

---

## Compliance & Safety

- **Robots**: every fetch is checked with `can_fetch`. Crawl‑delay is honored; otherwise a conservative default is used.
- **Rate limits**: per‑domain minimum gap + jitter; slows down as needed.
- **Errors**: timeouts and 5xx/429 are retried with exponential backoff and `Retry-After` headers.
- **Traps**: simple heuristics skip paginated archives like `/page/NN` or `?page=NN` and calendar loops.
- **No API**: Reddit collection uses only HTML via `old.reddit.com`.

---

## Notes

- Reuters categories are **English**; non‑EN rows are likely unlabeled or low‑confidence.
- If you collect a lot, raise `DEFAULT_MIN_DELAY_S` and/or reduce `MAX_PAGES_PER_DOMAIN`.
- Put your contact in `USER_AGENT` for responsible crawling.


### Reddit robots.txt (Why you might see “Blocked by robots.txt”)

Reddit currently disallows crawling of most subreddit pages for generic user-agents via `robots.txt`.  
This project **respects robots.txt**, so you may see logs like:

```
WARNING reddit: Blocked by robots.txt: https://old.reddit.com/r/worldnews/?sort=new
```

Options:
- **Recommended for compliance:** run only the news crawler; Reddit code remains implemented but idle.
- **For pipeline testing (no live crawl):** generate sample Reddit rows, then continue preprocessing/classification:
  ```bash
  python -m scripts.make_sample_reddit
  python -m scripts.run_preprocess
  python -m scripts.run_classify
  ```
