from __future__ import annotations

import time
from typing import Any, Optional
from collections import defaultdict


class LoadBalancer:
    """Production load balancer for LLM Router Pro with weighted round-robin."""

    def __init__(self, weights: dict[str, float] | None = None) -> None:
        self.weights = weights or {}
        self._current_index: dict[str, int] = defaultdict(int)
        self._request_counts: dict[str, int] = defaultdict(int)

    def select_provider(self, available: list[str]) -> str | None:
        if not available:
            return None
        if not self.weights:
            # Simple round-robin
            for provider in available:
                self._current_index[provider] = (self._current_index.get(provider, 0) + 1) % len(available)
            return available[self._current_index.get(available[0], 0) % len(available)]
        # Weighted selection
        weighted = [(p, self.weights.get(p, 1.0)) for p in available]
        total = sum(w for _, w in weighted)
        if total == 0:
            return available[0]
        # Select based on lowest request count relative to weight
        best = min(weighted, key=lambda item: self._request_counts[item[0]] / item[1])
        self._request_counts[best[0]] += 1
        return best[0]

    def get_stats(self) -> dict[str, Any]:
        return {
            "weights": dict(self.weights),
            "request_counts": dict(self._request_counts),
            "current_index": dict(self._current_index),
        }

    def reset(self) -> None:
        self._current_index.clear()
        self._request_counts.clear()


class GatewayMetrics:
    """Production metrics aggregator for gateway operations."""

    def __init__(self) -> None:
        self.metrics: dict[str, Any] = {
            "requests_total": 0,
            "requests_success": 0,
            "requests_failed": 0,
            "latency_sum_ms": 0.0,
            "latency_count": 0,
            "provider_selections": defaultdict(int),
        }

    def record_request(self, provider: str, success: bool, latency_ms: float) -> None:
        self.metrics["requests_total"] += 1
        if success:
            self.metrics["requests_success"] += 1
        else:
            self.metrics["requests_failed"] += 1
        self.metrics["latency_sum_ms"] += latency_ms
        self.metrics["latency_count"] += 1
        self.metrics["provider_selections"][provider] += 1

    def get_summary(self) -> dict[str, Any]:
        total = self.metrics["requests_total"]
        success = self.metrics["requests_success"]
        latency_count = self.metrics["latency_count"]
        avg_latency = self.metrics["latency_sum_ms"] / latency_count if latency_count > 0 else 0.0
        return {
            "total_requests": total,
            "success_rate": success / total if total > 0 else 0.0,
            "avg_latency_ms": round(avg_latency, 3),
            "provider_distribution": dict(self.metrics["provider_selections"]),
        }
