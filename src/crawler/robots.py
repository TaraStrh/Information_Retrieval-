import urllib.robotparser as rp
from urllib.parse import urlparse
from functools import lru_cache
from typing import Optional

@lru_cache(maxsize=256)
def _get_parser(root: str) -> rp.RobotFileParser:
    r = rp.RobotFileParser()
    r.set_url(root + "/robots.txt")
    try:
        r.read()
    except Exception:
        pass
    return r

def rootsite(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"

def allowed(user_agent: str, url: str) -> bool:
    r = _get_parser(rootsite(url))
    try:
        return r.can_fetch(user_agent, url)
    except Exception:
        return True

def crawl_delay(user_agent: str, url: str) -> Optional[float]:
    r = _get_parser(rootsite(url))
    try:
        return r.crawl_delay(user_agent)
    except Exception:
        return None
