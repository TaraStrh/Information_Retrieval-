import pandas as pd
from langdetect import detect, DetectorFactory
from tqdm import tqdm
from ..config import NEWS_CSV, REDDIT_CSV, NEWS_CLEAN_CSV, REDDIT_CLEAN_CSV
from .clean_text import clean_pipeline
from .tokenize import tokenize, join_tokens

DetectorFactory.seed = 42

def process_one(path_in, path_out):
    try:
        df = pd.read_csv(path_in)
    except FileNotFoundError:
        return
    if df.empty:
        df.to_csv(path_out, index=False)
        return
    langs, texts_clean, tokenss, tokens_count = [], [], [], []
    for t in tqdm(df["text"].fillna(""), desc=f"preprocess {path_in.name}"):
        c = clean_pipeline(t)
        texts_clean.append(c)
        toks = tokenize(c)
        tokenss.append(join_tokens(toks))
        tokens_count.append(len(toks))
        s = (c[:400])
        try:
            langs.append(detect(s) if s else "")
        except Exception:
            langs.append("")
    df["text_clean"] = texts_clean
    df["tokens"] = tokenss
    df["tokens_count"] = tokens_count
    df["lang"] = df["lang"].fillna("")
    df.loc[df["lang"].eq(""), "lang"] = langs
    df.to_csv(path_out, index=False)

def run():
    process_one(REDDIT_CSV, REDDIT_CLEAN_CSV)
    process_one(NEWS_CSV, NEWS_CLEAN_CSV)

if __name__ == "__main__":
    run()
