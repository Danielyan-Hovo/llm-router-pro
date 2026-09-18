from __future__ import annotations

import time
from typing import Any, Optional


class RateLimiter:
    """Production rate limiter for LLM Router Pro gateway."""

    def __init__(self, max_requests: int = 100, window_seconds: float = 60.0) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: dict[str, list[float]] = {}

    def is_allowed(self, key: str) -> bool:
        now = time.time()
        window_start = now - self.window_seconds
        requests = self._requests.get(key, [])
        # Clean old requests outside window
        requests = [t for t in requests if t > window_start]
        self._requests[key] = requests
        if len(requests) >= self.max_requests:
            return False
        requests.append(now)
        return True

    def remaining(self, key: str) -> int:
        now = time.time()
        window_start = now - self.window_seconds
        requests = self._requests.get(key, [])
        requests = [t for t in requests if t > window_start]
        self._requests[key] = requests
        return max(0, self.max_requests - len(requests))

    def reset(self, key: str) -> None:
        self._requests.pop(key, None)


class RequestLogger:
    """Production request logger with structured output."""

    def __init__(self) -> None:
        self.logs: list[dict[str, Any]] = []

    def log_request(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        tenant_id: Optional[str] = None,
        model: Optional[str] = None,
    ) -> None:
        entry = {
            "timestamp": time.time(),
            "method": method,
            "path": path,
            "status_code": status_code,
            "duration_ms": duration_ms,
            "tenant_id": tenant_id,
            "model": model,
        }
        self.logs.append(entry)

    def get_logs(self, limit: int = 100) -> list[dict[str, Any]]:
        return self.logs[-limit:]

    def clear(self) -> None:
        self.logs.clear()
