from pathlib import Path
from datetime import timedelta

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "outputs"
STATE_DB = DATA_DIR / "state.db"

USER_AGENT = "IR-FinalProjectBot/1.0 (klahouti81@gmail.com)"

SUBREDDITS = ["worldnews", "technology", "news"]
NEWS_SEEDS = [
    "https://apnews.com/hub/world-news",     # AP hubs are allowed for generic UA (not GPTBot etc.)
    "https://www.aljazeera.com/news/",       # News index; generic UA allowed
    "https://www.dw.com/en/world",           # DW world section
]
CRAWL_DEPTH = 1
MAX_PAGES_PER_DOMAIN = 200
REQUEST_TIMEOUT_S = 20
RETRY_MAX = 3
BACKOFF_BASE = 1.8
JITTER_RANGE_S = (0.3, 1.2)

DEFAULT_MIN_DELAY_S = 3.0
PER_DOMAIN_MIN_DELAY = {"old.reddit.com": 8.0, "reddit.com": 8.0}

NEWS_CSV = OUTPUT_DIR / "news_raw.csv"
REDDIT_CSV = OUTPUT_DIR / "reddit_raw.csv"

NEWS_CLEAN_CSV = OUTPUT_DIR / "news_clean.csv"
REDDIT_CLEAN_CSV = OUTPUT_DIR / "reddit_clean.csv"

NEWS_LABELED_CSV = OUTPUT_DIR / "news_labeled.csv"
REDDIT_LABELED_CSV = OUTPUT_DIR / "reddit_labeled.csv"

LEXICON_JSON = DATA_DIR / "reuters_lexicon.json"
