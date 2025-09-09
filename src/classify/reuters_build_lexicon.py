# src/classify/reuters_build_lexicon.py
from pathlib import Path
from typing import List, Tuple, Dict
from collections import defaultdict
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import TfidfVectorizer
from tqdm import tqdm
import numpy as np
import json
import sys


def load_reuters_texts(reuters_dir: Path) -> Tuple[List[str], List[List[str]]]:
    """Parse Reuters-21578 .sgm files recursively and return (texts, labels)."""
    reuters_dir = Path(reuters_dir)
    if not reuters_dir.exists():
        raise FileNotFoundError(f"Reuters directory does not exist: {reuters_dir}")

    # Find .sgm files recursively (some zips extract into nested folders)
    sgm_files = sorted(reuters_dir.rglob("*.sgm"))
    if not sgm_files:
        raise FileNotFoundError(
            f"No .sgm files found under: {reuters_dir}\n"
            "Make sure you passed the folder containing the Reuters-21578 .sgm files."
        )

    texts: List[str] = []
    labels: List[List[str]] = []

    for sgm in sgm_files:
        with open(sgm, "rb") as f:
            soup = BeautifulSoup(f.read(), "lxml")

        # Reuters docs are under <REUTERS> ... </REUTERS>
        for reu in soup.find_all("reuters"):
            # Topics (labels)
            tps = []
            topics_tag = reu.find("topics")
            if topics_tag:
                tps = [d.get_text(strip=True) for d in topics_tag.find_all("d") if d.get_text(strip=True)]

            # Prefer <TEXT><BODY>...</BODY></TEXT>, but fall back to the whole <TEXT>
            txt = ""
            text_tag = reu.find("text")
            if text_tag:
                body_tag = text_tag.find("body")
                if body_tag and body_tag.get_text():
                    txt = body_tag.get_text(" ", strip=True)
                else:
                    txt = text_tag.get_text(" ", strip=True)

            # Keep only reasonably non-empty samples with at least one topic
            if txt and tps and len(txt.split()) >= 5:
                texts.append(txt)
                labels.append(tps)

    if not texts:
        raise ValueError(
            "Parsed 0 Reuters articles with topics/body.\n"
            "Possible causes:\n"
            "  • Wrong directory (no valid .sgm files)\n"
            "  • Corrupted files\n"
            "  • Parser failed to find <TEXT>/<BODY> or <TOPICS>\n"
            "Check your path and contents, e.g. `ls -R /path/to/reuters21578 | grep -i .sgm`."
        )

    return texts, labels


def build_lexicon(
    texts: List[str],
    labels: List[List[str]],
    top_k: int = 200,
    min_df: int = 1,
    stop_words: str = "none",  # "none" or "english"
) -> Tuple[Dict[str, List[Tuple[str, float]]], List[str]]:
    """
    Build a weighted n-gram lexicon per category using TF-IDF centroids.
    Safer defaults: min_df=1 and no stop words to avoid empty vocabulary issues.
    """
    if len(texts) != len(labels):
        raise ValueError("texts and labels length mismatch")

    if stop_words not in {"none", "english"}:
        raise ValueError("stop_words must be 'none' or 'english'")

    sw = None if stop_words == "none" else "english"

    vect = TfidfVectorizer(
        ngram_range=(1, 2),
        max_features=120_000,
        min_df=min_df,
        stop_words=sw,
    )

    # Fit TF-IDF, provide helpful errors if vocabulary ends up empty
    try:
        X = vect.fit_transform(texts)
    except ValueError as e:
        msg = str(e)
        if "empty vocabulary" in msg:
            raise ValueError(
                "TF-IDF produced an empty vocabulary.\n"
                "Try one or more of these:\n"
                f"  • Use --min_df 1 (current: {min_df})\n"
                "  • Use --stop_words none\n"
                "  • Verify your Reuters path actually contains .sgm files with text.\n"
            )
        raise

    vocab = vect.get_feature_names_out()

    # Collect doc indices per category
    from collections import defaultdict
    cat_docs = defaultdict(list)
    for i, labs in enumerate(labels):
        for c in labs:
            cat_docs[c].append(i)

    # Build category centroids and take top_k features
    lexicon: Dict[str, List[Tuple[str, float]]] = {}
    Xcsr = X.tocsr()
    for c, idxs in tqdm(cat_docs.items(), desc="Building lexicon per category"):
        if len(idxs) < 5:
            # Skip extremely small categories (too noisy)
            continue
        M = Xcsr[idxs]
        centroid = np.asarray(M.mean(axis=0)).ravel()
        if centroid.sum() == 0:
            continue
        top_idx = np.argsort(centroid)[::-1][:top_k]
        terms = [(vocab[i], float(centroid[i])) for i in top_idx if centroid[i] > 0]
        if terms:
            lexicon[c] = terms

    if not lexicon:
        raise ValueError(
            "Built an empty lexicon (no categories passed thresholds).\n"
            "Consider lowering --top_k, decreasing --min_df, or disabling stop words."
        )

    return lexicon, vocab.tolist()


def save_lexicon(out_json: Path, lexicon, vocab):
    out_json = Path(out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(
        json.dumps({"vocabulary": vocab, "lexicon": lexicon}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main(reuters_dir: str, out_json: str, top_k: int = 200, min_df: int = 1, stop_words: str = "none"):
    texts, labels = load_reuters_texts(Path(reuters_dir))
    lexicon, vocab = build_lexicon(texts, labels, top_k=top_k, min_df=min_df, stop_words=stop_words)
    save_lexicon(Path(out_json), lexicon, vocab)
    print(f"Saved lexicon for {len(lexicon)} categories to {out_json}")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--reuters_dir", required=True, help="Path containing Reuters-21578 .sgm files")
    ap.add_argument("--out_json", default=str(Path(__file__).resolve().parents[2] / "data" / "reuters_lexicon.json"))
    ap.add_argument("--top_k", type=int, default=200, help="Top terms per category")
    ap.add_argument("--min_df", type=int, default=1, help="Min doc frequency for TF-IDF (use 1 if dataset is small)")
    ap.add_argument("--stop_words", choices=["none", "english"], default="none", help="Stop words handling")
    args = ap.parse_args()

    try:
        main(args.reuters_dir, args.out_json, args.top_k, args.min_df, args.stop_words)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
