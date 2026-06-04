"""
Cache HTML local - resume-safe scraping.
Cada URL eh chaveado por SHA-1. TTL configuravel (default 7 dias).
"""
import hashlib
import time
from pathlib import Path

CACHE_DIR = Path(__file__).parent / ".cache"
DEFAULT_TTL_SECONDS = 7 * 24 * 3600


def _key(url: str) -> str:
    return hashlib.sha1(url.encode()).hexdigest()


def _path(url: str) -> Path:
    return CACHE_DIR / f"{_key(url)}.html"


def get(url: str, ttl: int = DEFAULT_TTL_SECONDS) -> str | None:
    p = _path(url)
    if not p.exists():
        return None
    if (time.time() - p.stat().st_mtime) > ttl:
        return None
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        return None


def put(url: str, html: str) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        _path(url).write_text(html, encoding="utf-8")
    except Exception:
        pass


def clear() -> int:
    if not CACHE_DIR.exists():
        return 0
    n = 0
    for f in CACHE_DIR.iterdir():
        try:
            f.unlink()
            n += 1
        except Exception:
            pass
    return n
