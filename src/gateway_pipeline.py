from __future__ import annotations

import time
from typing import Any, Optional, Dict, List, Callable
from collections import OrderedDict, defaultdict


class GatewayCache:
    """Production-grade LRU cache with TTL for LLM Router Pro gateway responses."""

    def __init__(self, max_entries: int = 500, default_ttl: float = 120.0) -> None:
        self.max_entries = max_entries
        self.default_ttl = default_ttl
        self._store: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._access_times: Dict[str, float] = {}
        self._stats = {"hits": 0, "misses": 0, "evictions": 0, "sets": 0}

    def get(self, key: str) -> Any | None:
        if key not in self._store:
            self._stats["misses"] += 1
            return None
        value, expiry = self._store[key]
        if time.time() > expiry:
            self._store.pop(key, None)
            self._access_times.pop(key, None)
            self._stats["misses"] += 1
            return None
        self._store.move_to_end(key)
        self._access_times[key] = time.time()
        self._stats["hits"] += 1
        return value

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        expiry = time.time() + (ttl or self.default_ttl)
        if key in self._store:
            self._store.move_to_end(key)
        self._store[key] = (value, expiry)
        self._access_times[key] = time.time()
        self._stats["sets"] += 1
        while len(self._store) > self.max_entries:
            evicted_key, _ = self._store.popitem(last=False)
            self._access_times.pop(evicted_key, None)
            self._stats["evictions"] += 1

    def delete(self, key: str) -> bool:
        if key in self._store:
            self._store.pop(key, None)
            self._access_times.pop(key, None)
            return True
        return False

    def clear_expired(self) -> int:
        now = time.time()
        expired = [k for k, (_, exp) in self._store.items() if now > exp]
        for k in expired:
            self._store.pop(k, None)
            self._access_times.pop(k, None)
        return len(expired)

    def stats(self) -> Dict[str, Any]:
        total = self._stats["hits"] + self._stats["misses"]
        return {
            "entries": len(self._store),
            "max_entries": self.max_entries,
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "evictions": self._stats["evictions"],
            "sets": self._stats["sets"],
            "hit_rate": round(self._stats["hits"] / total, 4) if total > 0 else 0.0,
        }


class GatewayPipeline:
    """Production pipeline for request processing with middleware chain."""

    def __init__(self) -> None:
        self.middleware: List[Callable] = []
        self.cache = GatewayCache()

    def register(self, middleware: Callable) -> None:
        self.middleware.append(middleware)

    def process(self, request: Dict[str, Any]) -> Dict[str, Any]:
        result = request.copy()
        for mw in self.middleware:
            result = mw(result)
        return result

    def process_cached(self, request: Dict[str, Any], cache_key: str) -> Dict[str, Any]:
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached
        result = self.process(request)
        self.cache.set(cache_key, result)
        return result
