from src.classify.reuters_build_lexicon import main
if __name__ == '__main__':
 import argparse
 ap=argparse.ArgumentParser(); ap.add_argument('--reuters_dir', required=True); ap.add_argument('--out_json', default=None); ap.add_argument('--top_k', type=int, default=200); a=ap.parse_args();
 out=a.out_json or str((__import__('pathlib').Path(__file__).resolve().parents[1] / 'data' / 'reuters_lexicon.json'));
 main(a.reuters_dir, out, a.top_k)
