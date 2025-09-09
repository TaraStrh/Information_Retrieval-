from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import re

TRACKING_KEYS = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","gclid","fbclid","igshid"}

def canonicalize_url(url: str) -> str:
    p = urlparse(url)
    scheme = p.scheme.lower()
    netloc = p.netloc.lower()
    path = re.sub(r"/{2,}", "/", p.path or "/")
    fragment = ""
    query_pairs = [(k,v) for (k,v) in parse_qsl(p.query, keep_blank_values=True) if k not in TRACKING_KEYS]
    query = urlencode(sorted(query_pairs))
    return urlunparse((scheme, netloc, path, "", query, fragment))
