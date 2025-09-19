#!/usr/bin/env python3
"""
Complete pipeline runner for IR Final Project.
Runs all components in sequence: crawling, preprocessing, and classification.
"""
import sys
import subprocess
from pathlib import Path

def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    print(f"RUNNING: {description}")
    print(f"COMMAND: {cmd}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        print(f"✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} failed with exit code {e.returncode}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return False

def main():
    print("IR Final Project - Complete Pipeline Runner")
    print("==========================================")
    
    # Change to project directory
    project_dir = Path(__file__).parent
    print(f"Working directory: {project_dir}")
    
    # 1. Smart data collection with automatic fallbacks
    print("1. Running smart data collection...")
    print()
    success = run_command("python scripts/smart_data_collector.py", "Smart data collection")
    if not success:
        print("Smart data collection failed, pipeline cannot continue.")
        return
    
    # 2. Text preprocessing
    print("\n2. Running text preprocessing...")
    if not run_command("python scripts/run_preprocess.py", "Text preprocessing"):
        print("Text preprocessing failed, skipping classification...")
        return
    
    # 3. Build Reuters lexicons
    print("\n3. Building Reuters-21578 lexicons...")
    if not run_command("python scripts/build_lexicon.py", "Reuters lexicon building"):
        print("Lexicon building failed, skipping classification...")
        return
    
    # 4. Classification
    print("\n4. Running classification...")
    if not run_command("python scripts/run_classify.py", "News classification"):
        print("Classification failed...")
        return
    
    print("\n" + "="*60)
    print("PIPELINE COMPLETED!")
    print("="*60)
    print("\nOutput files:")
    output_dir = Path("data/outputs")
    if output_dir.exists():
        for file in output_dir.glob("*.csv"):
            size = file.stat().st_size
            print(f"  {file.name}: {size:,} bytes")
    
    print("\nTo view results:")
    print("  - Raw data: data/outputs/*_raw.csv")
    print("  - Clean data: data/outputs/*_clean.csv") 
    print("  - Classified data: data/outputs/custom_preds.csv")
    print("  - Reuters lexicons: data/outputs/reuters_lexicons/")

if __name__ == "__main__":
    main()
