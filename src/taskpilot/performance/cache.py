"""Task result caching with TTL and LRU eviction.

Thread-safe in-memory cache for task execution results, supporting
per-entry TTL, LRU eviction, and key-prefix invalidation.
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

_DEFAULT_TTL = 300
_DEFAULT_MAX_SIZE = 5_000


@dataclass
class CacheEntry:
    """Single cached value with metadata."""
    key: str
    value: Any
    created_at: float = field(default_factory=time.monotonic)
    ttl: float = _DEFAULT_TTL
    hit_count: int = 0

    @property
    def expires_at(self) -> float:
        return self.created_at + self.ttl

    @property
    def is_expired(self) -> bool:
        return time.monotonic() > self.expires_at


@dataclass
class CacheStats:
    """Aggregated cache statistics."""
    size: int = 0
    max_size: int = _DEFAULT_MAX_SIZE
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expired_purges: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "size": self.size, "max_size": self.max_size,
            "hits": self.hits, "misses": self.misses,
            "hit_rate": round(self.hit_rate, 2),
            "evictions": self.evictions,
            "expired_purges": self.expired_purges,
        }


class TaskCache:
    """Thread-safe task result cache with TTL and LRU eviction."""

    def __init__(self, default_ttl: float = _DEFAULT_TTL,
                 max_size: int = _DEFAULT_MAX_SIZE):
        self._default_ttl = default_ttl
        self._max_size = max_size
        self._entries: Dict[str, CacheEntry] = {}
        self._access_order: List[str] = []
        self._lock = threading.Lock()
        self._stats = CacheStats(max_size=max_size)

    def get(self, key: str) -> Optional[Any]:
        """Retrieve a cached value. Returns None on miss or expiry."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self._stats.misses += 1
                return None
            if entry.is_expired:
                self._remove_entry(key)
                self._stats.misses += 1
                self._stats.expired_purges += 1
                return None
            entry.hit_count += 1
            self._stats.hits += 1
            self._touch(key)
            return entry.value

    def put(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Insert or overwrite a cache entry."""
        with self._lock:
            self._purge_expired()
            while len(self._entries) >= self._max_size:
                self._evict_lru()
            if key in self._entries:
                self._remove_entry(key)
            entry = CacheEntry(
                key=key, value=value,
                ttl=ttl if ttl is not None else self._default_ttl,
            )
            self._entries[key] = entry
            self._access_order.append(key)
            self._stats.size = len(self._entries)

    def invalidate(self, key: str) -> bool:
        """Remove a single entry by key."""
        with self._lock:
            if key in self._entries:
                self._remove_entry(key)
                return True
            return False

    def invalidate_by_prefix(self, prefix: str) -> int:
        """Invalidate all entries whose key starts with prefix."""
        with self._lock:
            to_remove = [k for k in self._entries if k.startswith(prefix)]
            for k in to_remove:
                self._remove_entry(k)
            return len(to_remove)

    def get_or_compute(self, key: str, compute_fn: Callable[[], Any],
                       ttl: Optional[float] = None) -> Any:
        """Return cached value or compute, cache, and return it."""
        val = self.get(key)
        if val is not None:
            return val
        result = compute_fn()
        self.put(key, result, ttl=ttl)
        return result

    def clear(self) -> int:
        """Flush the entire cache."""
        with self._lock:
            count = len(self._entries)
            self._entries.clear()
            self._access_order.clear()
            self._stats.size = 0
            return count

    def get_stats(self) -> CacheStats:
        with self._lock:
            self._stats.size = len(self._entries)
            s = self._stats
            return CacheStats(
                size=s.size, max_size=s.max_size, hits=s.hits,
                misses=s.misses, evictions=s.evictions,
                expired_purges=s.expired_purges,
            )

    def _remove_entry(self, key: str) -> None:
        self._entries.pop(key, None)
        if key in self._access_order:
            self._access_order.remove(key)
        self._stats.size = len(self._entries)

    def _touch(self, key: str) -> None:
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    def _evict_lru(self) -> None:
        if not self._access_order:
            return
        self._remove_entry(self._access_order[0])
        self._stats.evictions += 1

    def _purge_expired(self) -> None:
        expired = [k for k, e in self._entries.items() if e.is_expired]
        for k in expired:
            self._remove_entry(k)
            self._stats.expired_purges += 1
