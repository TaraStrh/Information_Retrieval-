#!/usr/bin/env python3
"""
Script to classify collected news using Reuters-21578 lexicons.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ml.tfidf_Lexicon_Classifier import classify_custom

def main():
    print("Starting news classification...")
    
    lexicon_dir = "data/outputs/reuters_lexicons"
    if not Path(lexicon_dir).exists():
        print(f"Error: Lexicon directory {lexicon_dir} not found!")
        print("Please run 'python scripts/build_lexicon.py' first.")
        return
    
    # Classify Reddit data if it exists
    reddit_clean = "data/outputs/reddit_clean.csv"
    if Path(reddit_clean).exists():
        print(f"Classifying {reddit_clean}...")
        out_path = classify_custom(reddit_clean, lexicon_dir)
        print(f"Reddit classification results saved to {out_path}")
    
    # Classify news data if it exists and has content
    news_clean = "data/outputs/news_clean.csv"
    if Path(news_clean).exists():
        # Check if file has actual data (more than just header)
        with open(news_clean, 'r') as f:
            lines = f.readlines()
        if len(lines) > 1 and any(line.strip() for line in lines[1:]):
            print(f"Classifying {news_clean}...")
            out_path = classify_custom(news_clean, lexicon_dir)
            print(f"News classification results saved to {out_path}")
        else:
            print(f"Skipping {news_clean} - no data content found")
    
    print("Classification completed!")

if __name__ == "__main__":
    main()
