# src/preprocess/clean_text.py
import re

# Try to import the emoji library and support both old/new APIs.
try:
    import emoji
    _HAS_EMOJI = True
except Exception:
    emoji = None
    _HAS_EMOJI = False

# --- Regexes & Maps ---------------------------------------------------------

# Basic URL removal (http/https/www)
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)

# Minimal Persian character normalization map
PERSIAN_MAP = {
    "\u064a": "\u06cc",  # ARABIC YEH -> FARSI YEH
    "\u0643": "\u06a9",  # ARABIC KAF -> KEHEH
}

# --- Helpers ---------------------------------------------------------------

def normalize_text(s: str) -> str:
    """Light normalization incl. Persian char fixes and whitespace collapse."""
    if not s:
        return ""
    s = "".join(PERSIAN_MAP.get(ch, ch) for ch in s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def strip_urls(s: str) -> str:
    """Remove URLs."""
    if not s:
        return ""
    return URL_RE.sub(" ", s)

def strip_emojis(s: str) -> str:
    """Remove emojis, supporting both emoji<2 and emoji>=2."""
    if not s or not _HAS_EMOJI:
        return s or ""
    # emoji v2+: use replace_emoji
    if hasattr(emoji, "replace_emoji"):
        return emoji.replace_emoji(s, replace=" ")
    # older emoji versions: fall back to get_emoji_regexp if available
    get_re = getattr(emoji, "get_emoji_regexp", None)
    if callable(get_re):
        EMOJI_RE = get_re()
        return EMOJI_RE.sub(" ", s)
    # last resort: return unchanged
    return s

def lowercase_english(s: str) -> str:
    """Lowercase only ASCII letters (keeps Persian unaffected)."""
    if not s:
        return ""
    return re.sub(r"[A-Z]", lambda m: m.group(0).lower(), s)

def clean_pipeline(s: str) -> str:
    """End-to-end cleaning used by the preprocessing step."""
    s = s or ""
    s = strip_urls(s)
    s = strip_emojis(s)
    s = normalize_text(s)
    s = lowercase_english(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s
