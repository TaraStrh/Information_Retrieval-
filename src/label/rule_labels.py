import re
import pandas as pd
from pathlib import Path

RULES = {
    "politics":  r"\b(election|parliament|congress|minister|policy|sanction|diplomat|government|senate|president)\b",
    "economy":   r"\b(gdp|inflation|market|stock|bond|econom|trade|tariff|budget|currency)\b",
    "tech":      r"\b(tech|ai|software|hardware|chip|cyber|privacy|google|apple|microsoft|startups?)\b",
    "sports":    r"\b(match|league|cup|tournament|goal|coach|fifa|olympic|nba|premier)\b",
    "health":    r"\b(health|vaccine|disease|covid|cancer|hospital|mental|medicine)\b",
    "science":   r"\b(science|research|study|astronomy|space|physics|biology|chemistry)\b",
    "culture":   r"\b(culture|movie|film|music|art|festival|literature|museum|theater)\b",
    "world":     r"\b(world|international|global|abroad|foreign)\b",
}
COMPILED = {k: re.compile(v, re.I) for k, v in RULES.items()}


def apply_rules(title: str, text: str) -> str:
    blob = f"{title or ''} {text or ''}"
    for label, rx in COMPILED.items():
        if rx.search(blob):
            return label
    return "world"


def run(input_csvs: list[str], output_csv: str) -> None:
    df = pd.concat([pd.read_csv(p) for p in input_csvs], ignore_index=True)
    if "title" not in df.columns or "text" not in df.columns:
        raise ValueError("input must have 'title' and 'text'")
    df["label"] = [apply_rules(ti, te) for ti, te in zip(df["title"], df["text"])]
    df.to_csv(output_csv, index=False)


if __name__ == "__main__":
    run(["data/outputs/news_enriched.csv", "data/outputs/reddit_enriched.csv"],
        "data/outputs/final_labeled.csv")
