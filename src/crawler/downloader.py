import requests, time
from ..config import USER_AGENT, REQUEST_TIMEOUT_S, RETRY_MAX, BACKOFF_BASE
from .logger import get_logger

logger = get_logger("downloader")

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en;q=0.8, *;q=0.5",
    "Connection": "close",
}

def fetch(url: str):
    session = requests.Session()
    session.headers.update(HEADERS)
    attempt = 0
    backoff = 0.0
    while attempt <= RETRY_MAX:
        try:
            if backoff > 0:
                time.sleep(backoff)
            resp = session.get(url, timeout=REQUEST_TIMEOUT_S, allow_redirects=True)
            code = resp.status_code
            if 200 <= code < 300:
                return code, resp.url, resp.text
            elif code in (429, 503, 502, 500, 408):
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        backoff = max(float(ra), backoff)
                    except Exception:
                        pass
                attempt += 1
                backoff = max(1.0, (BACKOFF_BASE ** attempt))
                logger.warning(f"Retrying {url} after HTTP {code}, attempt {attempt}, backoff={backoff:.1f}s")
                continue
            elif 300 <= code < 400:
                return code, resp.url, resp.text
            else:
                logger.error(f"Non-retriable HTTP status {code} for {url}")
                return code, resp.url, resp.text
        except requests.Timeout:
            attempt += 1
            backoff = max(1.0, (BACKOFF_BASE ** attempt))
            logger.warning(f"Timeout fetching {url}, attempt {attempt}, backoff={backoff:.1f}s")
        except requests.RequestException as e:
            attempt += 1
            backoff = max(1.0, (BACKOFF_BASE ** attempt))
            logger.warning(f"Request error {e} on {url}, attempt {attempt}, backoff={backoff:.1f}s")
    logger.error(f"Failed to fetch {url} after {RETRY_MAX} retries")
    return None, None, None
