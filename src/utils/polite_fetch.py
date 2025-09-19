"""
PoliteSession: resilient HTTP GET with robots.txt checks, crawl-delay, min interval,
adaptive backoff on 429/5xx/timeouts, and structured logging.
"""
from __future__ import annotations

import logging
import random
import re
import time
from dataclasses import dataclass, field
from typing import Dict, Optional
from urllib.parse import urlparse
from urllib import robotparser

import requests

_CRAWL_DELAY_RX = re.compile(r"^\s*crawl-delay\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*$", re.I)
_UA_RX = re.compile(r"^\s*user-agent\s*:\s*(.+?)\s*$", re.I)


@dataclass
class PoliteSession:
    user_agent: str = "ir-news-crawler/1.0 (+https://example.edu)"
    timeout: float = 12.0
    max_retries: int = 4
    backoff_base: float = 1.6
    jitter: float = 0.3
    min_interval: float = 1.0  # minimum spacing between requests per domain
    max_extra_delay: float = 10.0  # cap adaptive slowdown

    session: requests.Session = field(default_factory=requests.Session, init=False)
    last_request_ts: Dict[str, float] = field(default_factory=dict, init=False)
    domain_delay: Dict[str, float] = field(default_factory=dict, init=False)
    robots_cache: Dict[str, robotparser.RobotFileParser] = field(default_factory=dict, init=False)
    robots_text: Dict[str, str] = field(default_factory=dict, init=False)
    adaptive_extra: Dict[str, float] = field(default_factory=dict, init=False)

    def __post_init__(self):
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'DNT': '1',
        })
        logging.getLogger(__name__).setLevel(logging.INFO)
        # Configure retry strategy with urllib3.Retry
        from urllib3.util.retry import Retry
        retry_strategy = Retry(
            total=5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            backoff_factor=1
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    # -------------------- robots.txt handling --------------------
    def _fetch_robots_text(self, netloc: str) -> str:
        if netloc in self.robots_text:
            return self.robots_text[netloc]
        url = f"https://{netloc}/robots.txt"
        try:
            r = self.session.get(url, timeout=self.timeout)
            if r.status_code == 200 and r.text:
                self.robots_text[netloc] = r.text
                return r.text
        except Exception:
            pass
        self.robots_text[netloc] = ""
        return ""

    def _crawl_delay_from_text(self, netloc: str) -> Optional[float]:
        text = self._fetch_robots_text(netloc)
        if not text:
            return None
        # find the block for our UA or *; small parser
        blocks = []
        current = None
        lines = []
        for line in text.splitlines():
            if _UA_RX.match(line):
                if current is not None:
                    blocks.append((current, lines))
                current = _UA_RX.match(line).group(1).strip()
                lines = []
            else:
                lines.append(line)
        if current is not None:
            blocks.append((current, lines))

        def extract(lines):
            for l in lines:
                m = _CRAWL_DELAY_RX.match(l)
                if m:
                    try:
                        return float(m.group(1))
                    except Exception:
                        return None
            return None

        # exact UA
        for ua, lines in blocks:
            if ua.lower() == self.user_agent.lower():
                d = extract(lines)
                if d is not None:
                    return d
        # wildcard
        for ua, lines in blocks:
            if ua.strip() == "*":
                d = extract(lines)
                if d is not None:
                    return d
        return None

    def _robot_parser(self, netloc: str) -> robotparser.RobotFileParser:
        if netloc in self.robots_cache:
            return self.robots_cache[netloc]
        rp = robotparser.RobotFileParser()
        rp.set_url(f"https://{netloc}/robots.txt")
        try:
            rp.read()
        except Exception:
            pass
        self.robots_cache[netloc] = rp
        return rp

    def can_fetch(self, url: str) -> bool:
        netloc = urlparse(url).netloc
        rp = self._robot_parser(netloc)
        try:
            return rp.can_fetch(self.user_agent, url)
        except Exception:
            return True  # be permissive if parser failed

    # -------------------- pacing + backoff --------------------
    def _effective_delay(self, netloc: str) -> float:
        base = self.min_interval
        cd = self.domain_delay.get(netloc)
        if cd is None:
            cd = self._crawl_delay_from_text(netloc) or base
            self.domain_delay[netloc] = max(cd, base)
        extra = min(self.adaptive_extra.get(netloc, 0.0), self.max_extra_delay)
        return max(base, cd) + extra

    def get(self, url: str, **kwargs) -> requests.Response | None:
        """GET with rate limiting and retries."""
        parsed = urlparse(url)
        netloc = parsed.netloc
        now = time.monotonic()

        # Respect crawl delay
        if netloc in self.last_request_ts:
            elapsed = now - self.last_request_ts[netloc]
            delay = self._effective_delay(netloc)
            if elapsed < delay:
                time.sleep(delay - elapsed)

        # Check robots.txt
        if not self.can_fetch(url):
            logging.warning(f"robots.txt disallows: {url}")
            return None

        # Make the request with retries
        for attempt in range(self.max_retries + 1):
            try:
                # Add jitter
                jitter_delay = random.uniform(0, self.jitter)
                time.sleep(jitter_delay)

                response = self.session.get(url, timeout=self.timeout, **kwargs)
                self.last_request_ts[netloc] = time.monotonic()
                
                # Handle different status codes
                if response.status_code == 200:
                    # Reset adaptive delay on success
                    self.adaptive_extra[netloc] = max(0, self.adaptive_extra.get(netloc, 0) - 0.5)
                    return response
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 30))
                    logging.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    # Increase adaptive delay
                    self.adaptive_extra[netloc] = min(self.max_extra_delay, 
                                                    self.adaptive_extra.get(netloc, 0) + 2.0)
                    continue
                elif response.status_code >= 500:
                    # Server error - retry with backoff
                    backoff = (self.backoff_base ** attempt) + random.uniform(0, self.jitter)
                    logging.warning(f"Server error {response.status_code} for {url}, retrying in {backoff:.2f}s")
                    time.sleep(backoff)
                    continue
                else:
                    # Client error or other - don't retry
                    logging.warning(f"Request failed with status {response.status_code} for {url}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                backoff = (self.backoff_base ** attempt) + random.uniform(0, self.jitter)
                logging.warning("request_exception url=%s err=%s backoff=%.2f", url, type(e).__name__, backoff)
                time.sleep(backoff)
                
        logging.error("give_up url=%s after=%s attempts", url, self.max_retries)
        return None

    def get_html(self, url: str) -> str | None:
        """Get HTML content from URL."""
        response = self.get(url)
        if response and response.status_code == 200:
            return response.text
        return None

    def _record_after(self, netloc: str) -> None:
        """Record timestamp after request."""
        self.last_request_ts[netloc] = time.monotonic()
