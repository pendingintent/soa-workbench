"""Small TTL cache for remote CDISC Library lookups.

``CachedResource`` is a ``dict`` so existing callers and tests that read
or ``.update()`` the cache dictionaries keep working; the methods just
name the freshness, store and failure steps that were copy-pasted.
"""

import time
from typing import Any, Optional


class CachedResource(dict):
    """A dict of cached values plus ``fetched_at`` and ``last_error``."""

    def __init__(self, ttl: int, **fields: Any) -> None:
        super().__init__(fetched_at=0, last_error=None, **fields)
        self.ttl = ttl

    def is_fresh(
        self,
        key: str,
        now: Optional[float] = None,
        allow_empty: bool = False,
        ttl: Optional[int] = None,
    ) -> bool:
        """True when ``self[key]`` was stored less than ``ttl`` ago.

        With ``allow_empty=False`` an empty value (e.g. ``[]`` after a
        failed fetch) is treated as stale so the next call retries.
        """
        value = self.get(key)
        if value is None or (not allow_empty and not value):
            return False
        now = time.time() if now is None else now
        ttl = self.ttl if ttl is None else ttl
        return now - (self.get("fetched_at") or 0) < ttl

    def store(self, now: Optional[float] = None, **values: Any) -> None:
        """Record a successful fetch."""
        self.update(
            fetched_at=time.time() if now is None else now,
            last_error=None,
            **values,
        )

    def fail(self, error: str, now: Optional[float] = None, **values: Any) -> None:
        """Record a failed fetch (``values`` usually resets the payload)."""
        self.update(
            fetched_at=time.time() if now is None else now,
            last_error=error,
            **values,
        )

    def invalidate(self) -> None:
        """Force the next ``is_fresh`` check to fail."""
        self["fetched_at"] = 0
