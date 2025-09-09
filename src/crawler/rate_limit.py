import time, random
from urllib.parse import urlparse
from typing import Dict
from ..config import PER_DOMAIN_MIN_DELAY, DEFAULT_MIN_DELAY_S, USER_AGENT
from .robots import crawl_delay

class DomainRateLimiter:
    def __init__(self):
        self.last_time: Dict[str, float] = {}

    def _min_delay(self, url: str) -> float:
        host = urlparse(url).netloc
        if host in PER_DOMAIN_MIN_DELAY:
            return PER_DOMAIN_MIN_DELAY[host]
        rd = crawl_delay(USER_AGENT, url)
        if rd is not None:
            return float(rd)
        return DEFAULT_MIN_DELAY_S

    def wait(self, url: str, jitter: tuple[float,float] = (0.3,1.2)):
        host = urlparse(url).netloc
        now = time.time()
        need = self._min_delay(url)
        last = self.last_time.get(host, 0.0)
        to_sleep = max(0.0, (last + need) - now)
        if to_sleep > 0:
            time.sleep(to_sleep)
        time.sleep(random.uniform(*jitter))
        self.last_time[host] = time.time()
