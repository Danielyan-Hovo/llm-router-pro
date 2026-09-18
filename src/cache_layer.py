from __future__ import annotations

import time
from typing import Any, Optional, Callable
from collections import OrderedDict


class CacheLayer:
    """Production in-memory cache with TTL and LRU eviction for LLM Router Pro."""

    def __init__(self, max_size: int = 1000, default_ttl: float = 300.0) -> None:
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._stats: dict[str, int] = {"hits": 0, "misses": 0, "evictions": 0}

    def get(self, key: str) -> Any | None:
        if key not in self._cache:
            self._stats["misses"] += 1
            return None
        value, expiry = self._cache[key]
        if time.time() > expiry:
            self._cache.pop(key, None)
            self._stats["misses"] += 1
            return None
        self._cache.move_to_end(key)
        self._stats["hits"] += 1
        return value

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        expiry = time.time() + (ttl or self.default_ttl)
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = (value, expiry)
        if len(self._cache) > self.max_size:
            evicted_key, _ = self._cache.popitem(last=False)
            self._stats["evictions"] += 1

    def delete(self, key: str) -> bool:
        if key in self._cache:
            self._cache.pop(key)
            return True
        return False

    def clear(self) -> None:
        self._cache.clear()
        self._stats = {"hits": 0, "misses": 0, "evictions": 0}

    def stats(self) -> dict[str, Any]:
        total = self._stats["hits"] + self._stats["misses"]
        hit_rate = self._stats["hits"] / total if total > 0 else 0.0
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "evictions": self._stats["evictions"],
            "hit_rate": round(hit_rate, 4),
        }


class MiddlewareStack:
    """Production middleware stack for request/response processing."""

    def __init__(self) -> None:
        self._middlewares: list[Callable] = []

    def add(self, middleware: Callable) -> None:
        self._middlewares.append(middleware)

    def process(self, request: dict[str, Any]) -> dict[str, Any]:
        result = request.copy()
        for mw in self._middlewares:
            result = mw(result)
        return result

    def count(self) -> int:
        return len(self._middlewares)
