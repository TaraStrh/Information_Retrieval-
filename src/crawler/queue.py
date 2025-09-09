from urllib.parse import urlparse
from ..config import STATE_DB, MAX_PAGES_PER_DOMAIN
from .logger import get_logger
from ..storage.state import StateDB

logger = get_logger("queue")

class CrawlQueue:
    def __init__(self):
        self.db = StateDB(STATE_DB)
        self.domain_counts = {}

    def _domain(self, url: str) -> str:
        return urlparse(url).netloc.lower()

    def seed(self, url: str, source: str, record_type: str):
        self.db.push(url, depth=0, source=source, record_type=record_type)

    def push(self, url: str, depth: int, source: str, record_type: str):
        dom = self._domain(url)
        if self.domain_counts.get(dom, 0) >= MAX_PAGES_PER_DOMAIN:
            return
        self.db.push(url, depth, source, record_type)

    def pop(self):
        row = self.db.pop()
        if not row:
            return None
        url, depth, source, record_type = row
        dom = self._domain(url)
        self.domain_counts[dom] = self.domain_counts.get(dom, 0) + 1
        return row
