import pandas as pd
import re
from pathlib import Path
from langdetect import detect, DetectorFactory

DetectorFactory.seed = 42


def detect_lang(text: str) -> str:
    try:
        return detect(text) if text and isinstance(text, str) else ""
    except Exception:
        return ""


def simple_token_count(text: str) -> int:
    if not isinstance(text, str) or not text:
        return 0
    # very simple whitespace tokenization after light cleanup
    t = re.sub(r"\s+", " ", text).strip()
    return len(t.split(" ")) if t else 0


def run(input_csv: str, output_csv: str) -> None:
    df = pd.read_csv(input_csv)
    if "text" not in df.columns:
        raise ValueError("input CSV must contain a 'text' column")
    df["lang"] = df["text"].map(detect_lang)
    df["token_count"] = df["text"].map(simple_token_count)
    df.to_csv(output_csv, index=False)


if __name__ == "__main__":
    run("data/outputs/news_raw.csv", "data/outputs/news_enriched.csv")
    run("data/outputs/reddit_raw.csv", "data/outputs/reddit_enriched.csv")
