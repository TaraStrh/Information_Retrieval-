#!/usr/bin/env python3
"""
Script to build Reuters-21578 lexicons for classification.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dataset.reuters21578 import download_and_extract, parse_sgm_dir
from ml.tfidf_Lexicon_Classifier import run_reuters_pipeline

def main():
    print("Building Reuters-21578 lexicons...")
    
    # Download and extract Reuters dataset
    print("Downloading Reuters-21578 dataset...")
    reuters_dir = download_and_extract()
    
    # Parse SGML files to CSV
    print("Parsing SGML files...")
    df = parse_sgm_dir(reuters_dir)
    
    # Save raw Reuters data
    reuters_csv = Path("data/processed/reuters21578_raw.csv")
    reuters_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(reuters_csv, index=False)
    print(f"Saved {len(df)} Reuters documents to {reuters_csv}")
    
    # Build lexicons
    print("Building TF-IDF lexicons...")
    out_path = run_reuters_pipeline(str(reuters_csv), n_classes=8, topk=500)
    print(f"Lexicons built and saved. Predictions: {out_path}")
    
    print("Lexicon building completed!")

if __name__ == "__main__":
    main()
