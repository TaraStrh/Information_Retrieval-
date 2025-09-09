RAW_COLUMNS = [
    "uid", "record_type", "source", "url",
    "title", "text", "author", "published_at", "lang",
    "score", "num_comments", "created_ts"
]

CLEAN_COLUMNS = RAW_COLUMNS + ["text_clean", "tokens", "tokens_count"]

LABELED_COLUMNS = CLEAN_COLUMNS + ["label", "top3", "score_max"]
