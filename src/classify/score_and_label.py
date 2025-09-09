import json, pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple
from ..config import LEXICON_JSON, NEWS_CLEAN_CSV, REDDIT_CLEAN_CSV, NEWS_LABELED_CSV, REDDIT_LABELED_CSV

def load_lexicon(path: Path):
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    vocab = set(obj.get("vocabulary", []))
    lex = obj["lexicon"]
    d = {c: {term: float(w) for term, w in terms} for c, terms in lex.items()}
    return vocab, d

def tokens_to_ngrams(tokens: List[str]):
    for t in tokens:
        yield t
    for i in range(len(tokens)-1):
        yield tokens[i] + " " + tokens[i+1]

def score_row(tokens: str, lexicon: Dict[str, Dict[str,float]]):
    toks = tokens.split()
    grams = list(tokens_to_ngrams(toks))
    scores = []
    for c, weights in lexicon.items():
        s = 0.0
        for g in grams:
            if g in weights:
                s += weights[g]
        if s > 0:
            scores.append((c, s))
    if not scores:
        return "", []
    scores.sort(key=lambda x: x[1], reverse=True)
    label = scores[0][0]
    top3 = scores[:3]
    return label, top3

def process(in_csv: Path, out_csv: Path, lexicon: Dict[str, Dict[str,float]]):
    try:
        df = pd.read_csv(in_csv)
    except FileNotFoundError:
        return
    if df.empty:
        df.to_csv(out_csv, index=False)
        return
    labels, top3s, maxs = [], [], []
    for tok in df["tokens"].fillna("").astype(str):
        label, top3 = score_row(tok, lexicon)
        labels.append(label)
        top3s.append("|".join([f"{c}:{s:.4f}" for c,s in top3]))
        maxs.append(top3[0][1] if top3 else 0.0)
    df["label"] = labels
    df["top3"] = top3s
    df["score_max"] = maxs
    df.to_csv(out_csv, index=False)

def run():
    vocab, lexicon = load_lexicon(LEXICON_JSON)
    process(NEWS_CLEAN_CSV, NEWS_LABELED_CSV, lexicon)
    process(REDDIT_CLEAN_CSV, REDDIT_LABELED_CSV, lexicon)

if __name__ == "__main__":
    run()
