
from pathlib import Path
from datetime import datetime, timezone

from src.config import REDDIT_CSV
from src.storage.writer import CSVDatasetWriter

SAMPLE_ROWS = [
    {
        "uid": "sample1",
        "record_type": "reddit",
        "source": "r/worldnews",
        "url": "https://reddit.example/post/1",
        "title": "UN Security Council holds emergency session on regional tensions",
        "text": "Leaders convened in New York amid rising concerns...",
        "author": "u/example_user1",
        "published_at": datetime.now(timezone.utc).isoformat(),
        "lang": "en",
        "score": 1523,
        "num_comments": 418,
        "created_ts": datetime.now(timezone.utc).isoformat(),
    },
    {
        "uid": "sample2",
        "record_type": "reddit",
        "source": "r/technology",
        "url": "https://reddit.example/post/2",
        "title": "New breakthrough in battery technology promises faster charging",
        "text": "Researchers report a novel anode material that...",
        "author": "u/example_user2",
        "published_at": datetime.now(timezone.utc).isoformat(),
        "lang": "en",
        "score": 987,
        "num_comments": 233,
        "created_ts": datetime.now(timezone.utc).isoformat(),
    },
]

def main():
    writer = CSVDatasetWriter(REDDIT_CSV)
    writer.append_rows(SAMPLE_ROWS)
    print(f"Wrote {len(SAMPLE_ROWS)} sample rows to {REDDIT_CSV}")

if __name__ == "__main__":
    main()
