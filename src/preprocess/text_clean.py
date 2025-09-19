# Language-aware text preprocessing:
# - Normalize (deduplicate lines/paragraphs, unify whitespace, remove URLs & emojis)
# - Language detection (optional but enabled when mixed languages)
# - Lowercasing for English
# - Tokenize at word level
# - Lemmatization/Stemming per language (EN: WordNet/Porter; FA: Hazm Stemmer)
# - Store `clean` text and `token_count` columns

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd

# Language detection (optional but recommended for mixed-language corpora)
try:
    from langdetect import detect, DetectorFactory
    DetectorFactory.seed = 42
except Exception:  # langdetect not installed; fall back to empty
    detect = None

# English NLP (NLTK) - with fallback for missing resources
try:
    import nltk
    from nltk.stem import WordNetLemmatizer, PorterStemmer
    from nltk.tokenize import word_tokenize
    # Try to initialize, but fall back gracefully if resources missing
    try:
        _wnl = WordNetLemmatizer()
    except:
        _wnl = None
    _porter = PorterStemmer()
except Exception:
    nltk = None
    _wnl = None
    _porter = None
    word_tokenize = None


URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
WHITESPACE_RE = re.compile(r"\s+")

# Broad emoji & pictograph ranges (BMP + supplementary planes)
EMOJI_RE = re.compile(
    "["  # start class
    "\U0001F300-\U0001F64F"  # Misc Symbols & Pictographs, Emoticons
    "\U0001F680-\U0001F6FF"  # Transport & Map
    "\U0001F700-\U0001F77F"  # Alchemical
    "\U0001F780-\U0001F7FF"  # Geometric Extended
    "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
    "\U0001F900-\U0001F9FF"  # Supplemental Symbols & Pictographs
    "\U0001FA00-\U0001FAFF"  # Chess Symbols etc.
    "\u2600-\u26FF"          # Misc symbols
    "\u2700-\u27BF"          # Dingbats
    "]+",
    flags=re.UNICODE,
)

PUNCT_RE = re.compile(r"[^\w\s]")


def dedup_paragraphs(text: str) -> str:
    if not isinstance(text, str):
        return ""
    parts = [p.strip() for p in re.split(r"[\r\n]+", text) if p.strip()]
    seen = set()
    uniq = []
    for p in parts:
        key = p.lower()
        if len(p) < 30:  # skip very short fragments in dedup keying
            uniq.append(p)
            continue
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)
    return "\n".join(uniq)


def normalize_common(text: str, rm_emoji: bool = True) -> str:
    text = text or ""
    text = dedup_paragraphs(text)
    text = URL_RE.sub(" ", text)
    if rm_emoji:
        text = EMOJI_RE.sub(" ", text)
    text = WHITESPACE_RE.sub(" ", text)
    return text.strip()


def detect_lang_safe(text: str) -> str:
    if detect is None or not text:
        return ""
    try:
        return detect(text)
    except Exception:
        return ""


def process_en(text: str) -> tuple[str, int]:
    if not text:
        return "", 0
    s = text.lower()
    s = PUNCT_RE.sub(" ", s)
    
    # Simple tokenization fallback if NLTK not available
    if word_tokenize is None:
        toks = [t for t in s.split() if t.isalpha()]
    else:
        try:
            toks = [t for t in word_tokenize(s) if t.isalpha()]
        except:
            # Fallback to simple split if NLTK resources missing
            toks = [t for t in s.split() if t.isalpha()]
    
    # Lemmatize; if unavailable, fall back to Porter stemming
    if _wnl is not None:
        try:
            toks = [_wnl.lemmatize(t) for t in toks]
        except:
            # Fallback to Porter if lemmatizer fails
            if _porter is not None:
                toks = [_porter.stem(t) for t in toks]
    elif _porter is not None:
        toks = [_porter.stem(t) for t in toks]
    
    clean = " ".join(toks)
    return clean, len(toks)


def process_fa(text: str) -> tuple[str, int]:
    """Process Persian/Arabic text using generic text processing."""
    return process_generic(text)


def process_generic(text: str) -> tuple[str, int]:
    # Fallback for other languages: whitespace tokenize without heavy changes
    if not text:
        return "", 0
    toks = [t for t in text.split() if t]
    return " ".join(toks), len(toks)


def preprocess_file(input_csv: str, output_csv: str, text_col: str = "text") -> None:
    df = pd.read_csv(input_csv)
    if text_col not in df.columns:
        raise ValueError(f"'{text_col}' column not found in {input_csv}")

    # Normalize common patterns first
    df["_norm"] = df[text_col].astype(str).map(lambda x: normalize_common(x, rm_emoji=True))

    # Language detection (optional)
    df["lang"] = df["_norm"].map(detect_lang_safe)

    clean_texts = []
    token_counts = []

    for txt, lang in zip(df["_norm"].tolist(), df["lang"].tolist()):
        if lang == "en":
            clean, n = process_en(txt)
        elif lang in ("fa", "ar"):  # treat Persian/Arabic with hazm if available
            clean, n = process_fa(txt)
        else:
            clean, n = process_generic(txt)
        clean_texts.append(clean)
        token_counts.append(n)

    df["clean"] = clean_texts
    df["token_count"] = token_counts

    # Drop temp column
    df = df.drop(columns=["_norm"])

    # Persist
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)


if __name__ == "__main__":
    in1 = sys.argv[1] if len(sys.argv) > 1 else "../crawl/hn_posts.csv"
    out1 = sys.argv[2] if len(sys.argv) > 2 else "../../data/outputs/hn_posts_clean.csv"
    text_col = sys.argv[3] if len(sys.argv) > 3 else "title" 

    preprocess_file(in1, out1, text_col)

