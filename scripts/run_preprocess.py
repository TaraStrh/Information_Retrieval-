#!/usr/bin/env python3
"""
Script to run text preprocessing on collected data.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from preprocess.text_clean import preprocess_file

def main():
    print("Starting text preprocessing...")
    
    # Process Reddit data if it exists
    reddit_raw = "data/outputs/hn_posts.csv"
    reddit_clean = "data/outputs/hn_posts.csv"
    if Path(reddit_raw).exists():
        print(f"Processing {reddit_raw}...")
        preprocess_file(reddit_raw, reddit_clean, text_col="text")
        print(f"Saved clean Reddit data to {reddit_clean}")
    
    # Process news data if it exists
    news_raw = "data/outputs/hn_posts.csv"
    news_clean = "data/outputs/hn_posts.csv"
    if Path(news_raw).exists():
        print(f"Processing {news_raw}...")
        preprocess_file(news_raw, news_clean, text_col="text")
        print(f"Saved clean news data to {news_clean}")
    
    print("Text preprocessing completed!")

if __name__ == "__main__":
    main()
