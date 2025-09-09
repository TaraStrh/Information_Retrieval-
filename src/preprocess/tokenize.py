import re
from typing import List

TOKEN_RE = re.compile(r"[A-Za-z0-9\u0600-\u06FF]+", re.UNICODE)

def tokenize(s: str) -> List[str]:
    if not s:
        return []
    return TOKEN_RE.findall(s)

def join_tokens(tokens) -> str:
    return " ".join(tokens)
