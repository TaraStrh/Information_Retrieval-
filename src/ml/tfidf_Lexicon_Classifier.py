"""
TF-IDF Lexicon Classifier for Reuters-21578 and project news:

Pipeline:
1) Load Reuters-21578 CSV (from src/datasets/reuters21578.py) or a custom CSV with 'text' and optional 'label_true'.
2) Preprocess (simple normalize + stemming). (For EN; extendable.)
3) Vectorize with TF-IDF over unigrams+bigrams.
4) Build per-class lexicon weights by averaging TF-IDF of docs in each class.
   - Keep top-K features per class (by weight).
5) Score each document by dot-product between its TF-IDF vector and each class lexicon.
6) Predict the max-score class; write CSV with scores and predicted labels.

Outputs:
- data/outputs/reuters_lexicons/*.csv  (per-class top-K n-grams and weights)
- data/outputs/reuters_preds.csv       (id, label_true, scores..., label_pred)

You can also pass an input CSV (e.g., your crawled news) to classify with the same lexicons.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

# Optional NLTK stemming - with fallback
try:
    import nltk
    from nltk.stem import PorterStemmer
    from nltk.tokenize import word_tokenize
    # Try to initialize, but fall back gracefully if resources missing
    try:
        _porter = PorterStemmer()
    except:
        _porter = None
except Exception:
    nltk = None
    _porter = None
    word_tokenize = None


def normalize_en(s: str) -> str:
    s = s or ""
    s = re.sub(r"https?://\S+|www\.\S+", " ", s)
    s = re.sub(r"[^A-Za-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def stem_en(s: str) -> str:
    if not s:
        return s
    
    # Simple tokenization fallback if NLTK not available
    if word_tokenize is None:
        toks = [t for t in s.split() if t.isalpha()]
    else:
        try:
            toks = [t for t in word_tokenize(s) if t.isalpha()]
        except:
            # Fallback to simple split if NLTK resources missing
            toks = [t for t in s.split() if t.isalpha()]
    
    # Apply stemming if available
    if _porter is not None:
        try:
            toks = [_porter.stem(t) for t in toks]
        except:
            # If stemming fails, just return tokens as-is
            pass
    
    return " ".join(toks)


def preprocess_series(texts: pd.Series) -> pd.Series:
    return texts.fillna("").map(normalize_en).map(stem_en)


def select_top_classes(topics_list: List[List[str]], top_n: int) -> List[str]:
    cnt = Counter()
    for ts in topics_list:
        cnt.update(ts or [])
    return [c for c, _ in cnt.most_common(top_n)]


def filter_single_label(df: pd.DataFrame, allowed: List[str]) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        ts = [t for t in (r.get("topics") or []) if t in allowed]
        if len(ts) == 1:
            rows.append({"id": r["id"], "title": r.get("title", ""), "text": r.get("text", ""), "label_true": ts[0]})
    return pd.DataFrame(rows)


def build_vectorizer() -> TfidfVectorizer:
    return TfidfVectorizer(ngram_range=(1, 2), min_df=2, max_features=100_000)


def build_lexicons(X, y: List[str], vocab: List[str], topk: int = 500) -> Dict[str, Dict[str, float]]:
    # Compute per-class centroid (mean TF-IDF), then keep top-K terms
    labels = sorted(set(y))
    lexicons: Dict[str, Dict[str, float]] = {}
    for lab in labels:
        idx = np.where(np.array(y) == lab)[0]
        if len(idx) == 0:
            continue
        centroid = X[idx].mean(axis=0)  # 1 x V
        if not hasattr(centroid, "A1"):
            centroid = centroid.A  # to dense
        arr = np.asarray(centroid).ravel()
        # top-K
        if len(arr) > topk:
            top_idx = np.argpartition(-arr, topk)[:topk]
        else:
            top_idx = np.where(arr > 0)[0]
        pairs = {vocab[i]: float(arr[i]) for i in top_idx if arr[i] > 0}
        # sort by weight desc
        pairs = dict(sorted(pairs.items(), key=lambda kv: kv[1], reverse=True))
        lexicons[lab] = pairs
    return lexicons


def score_documents(X, lexicons: Dict[str, Dict[str, float]], vocab_index: Dict[str, int]) -> Tuple[np.ndarray, List[str]]:
    # Build dense weight matrix W (C x V) from lexicons for fast scoring
    classes = list(lexicons.keys())
    V = len(vocab_index)
    W = np.zeros((len(classes), V), dtype=np.float32)
    for ci, lab in enumerate(classes):
        for term, w in lexicons[lab].items():
            j = vocab_index.get(term)
            if j is not None:
                W[ci, j] = w
    # Scores = X dot W^T
    S = X @ W.T  # shape (N, C)
    return S, classes


def save_lexicons(lexicons: Dict[str, Dict[str, float]], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for lab, weights in lexicons.items():
        df = pd.DataFrame([(t, w) for t, w in weights.items()], columns=["term", "weight"])
        df.to_csv(out_dir / f"lexicon_{lab}.csv", index=False)


def run_reuters_pipeline(reuters_csv: str, n_classes: int = 8, topk: int = 500) -> Path:
    df_raw = pd.read_csv(reuters_csv)
    # topics column is a string representation of list if saved from pandas; try to eval safely
    def parse_topics(x):
        if isinstance(x, list):
            return x
        if isinstance(x, str):
            x = x.strip()
            if x.startswith("["):
                try:
                    return json.loads(x.replace("'", '"'))
                except Exception:
                    return []
        return []
    df_raw["topics"] = df_raw["topics"].map(parse_topics)

    top_classes = select_top_classes(df_raw["topics"].tolist(), n_classes)
    df = filter_single_label(df_raw, top_classes)

    df["clean"] = preprocess_series(df["text"])

    vec = build_vectorizer()
    X = vec.fit_transform(df["clean"].tolist())
    y = df["label_true"].tolist()

    vocab = vec.get_feature_names_out().tolist()
    vocab_index = {t: i for i, t in enumerate(vocab)}

    lexicons = build_lexicons(X, y, vocab, topk=topk)
    save_lexicons(lexicons, Path("data/outputs/reuters_lexicons"))

    # Score and predict on the same set or split as needed
    S, classes = score_documents(X, lexicons, vocab_index)
    pred_idx = S.argmax(axis=1)
    y_pred = [classes[i] for i in pred_idx]

    out = Path("data/outputs/reuters_preds.csv")
    score_cols = [f"score_{c}" for c in classes]
    df_out = df[["id", "label_true"]].copy()
    df_scores = pd.DataFrame(S, columns=score_cols)
    df_out = pd.concat([df_out, df_scores], axis=1)
    df_out["label_pred"] = y_pred
    out.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out, index=False)
    return out


def classify_custom(input_csv: str, lexicon_dir: str) -> Path:
    # Classify arbitrary CSV with a 'text' column using previously built lexicons.
    # This reuses the same vectorizer vocabulary by inferring from lexicons' terms.
    lexicons = {}
    classes = []
    terms = set()
    p = Path(lexicon_dir)
    for f in p.glob("lexicon_*.csv"):
        lab = f.stem.replace("lexicon_", "")
        classes.append(lab)
        df = pd.read_csv(f)
        lex = {row["term"]: float(row["weight"]) for _, row in df.iterrows()}
        lexicons[lab] = lex
        terms.update(lex.keys())

    # Build a vectorizer restricted to lexicon terms to ensure consistent scoring
    vec = TfidfVectorizer(vocabulary=sorted(terms), ngram_range=(1, 2))

    df = pd.read_csv(input_csv)
    # Use existing clean column if available, otherwise preprocess the text column
    if "clean" in df.columns:
        # Clean column already exists from preprocessing
        pass
    elif "text" in df.columns:
        df["clean"] = preprocess_series(df["text"])
    elif "content" in df.columns:
        df["clean"] = preprocess_series(df["content"])
    else:
        raise ValueError("No suitable text column found (expected 'clean', 'text', or 'content')")

    X = vec.fit_transform(df["clean"].tolist())
    vocab = vec.get_feature_names_out().tolist()
    vocab_index = {t: i for i, t in enumerate(vocab)}

    S, classes = score_documents(X, lexicons, vocab_index)
    pred_idx = S.argmax(axis=1)
    y_pred = [classes[i] for i in pred_idx]

    out = Path("data/outputs/custom_preds.csv")
    score_cols = [f"score_{c}" for c in classes]
    df_out = df.copy()
    df_scores = pd.DataFrame(S, columns=score_cols)
    df_out = pd.concat([df_out, df_scores], axis=1)
    df_out["label_pred"] = y_pred
    out.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out, index=False)
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["reuters", "classify"], help="Pipeline mode")
    ap.add_argument("--reuters_csv", default="data/processed/reuters21578_raw.csv")
    ap.add_argument("--n_classes", type=int, default=8)
    ap.add_argument("--topk", type=int, default=500)
    ap.add_argument("--input_csv", default="data/outputs/news_clean.csv")
    ap.add_argument("--lexicon_dir", default="data/outputs/reuters_lexicons")
    args = ap.parse_args()

    if args.mode == "reuters":
        out = run_reuters_pipeline(args.reuters_csv, n_classes=args.n_classes, topk=args.topk)
        print(f"Wrote predictions -> {out}")
    elif args.mode == "classify":
        out = classify_custom(args.input_csv, args.lexicon_dir)
        print(f"Wrote predictions -> {out}")
