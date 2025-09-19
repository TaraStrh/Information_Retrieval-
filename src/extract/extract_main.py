import pandas as pd
import trafilatura
from tqdm import tqdm

def extract_text(url: str) -> str:
    try:
        # trafilatura خودش HTML را دانلود می‌کند؛ اگر خواستی از downloaded=True با html خام خودت هم می‌شود
        txt = trafilatura.extract(url=url, include_comments=False, favor_precision=True)
        return txt or ""
    except Exception:
        return ""

def run(input_csv: str, output_csv: str):
    df = pd.read_csv(input_csv)
    if "text" not in df.columns:
        df["text"] = ""
    out = []
    seen = set()
    for row in tqdm(df.to_dict("records")):
        uid = row["id"]
        if uid in seen:
            continue
        seen.add(uid)
        text = row.get("text") or ""
        if len(text) < 80:
            text = extract_text(row["url"])
        if not text or len(text) < 120:
            continue
        row["text"] = text
        out.append(row)
    pd.DataFrame(out).to_csv(output_csv, index=False)

if __name__ == "__main__":
    run("data/outputs/reddit_raw.csv", "data/outputs/reddit_extracted.csv")
    run("data/outputs/news_raw.csv",   "data/outputs/news_extracted.csv")
