from __future__ import annotations

import time
from typing import Any, Optional, Dict, List
from collections import defaultdict


class GatewayMonitor:
    """Production gateway monitoring with real-time health tracking."""

    def __init__(self, alert_threshold: float = 0.95) -> None:
        self.alert_threshold = alert_threshold
        self._health_history: Dict[str, List[bool]] = defaultdict(list)
        self._latency_history: Dict[str, List[float]] = defaultdict(list)
        self._error_counts: Dict[str, int] = defaultdict(int)
        self._request_counts: Dict[str, int] = defaultdict(int)

    def record_request(
        self,
        provider: str,
        success: bool,
        latency_ms: float,
        model: Optional[str] = None,
    ) -> None:
        self._request_counts[provider] += 1
        self._health_history[provider].append(success)
        self._latency_history[provider].append(latency_ms)
        if not success:
            self._error_counts[provider] += 1
        # Keep history bounded
        if len(self._health_history[provider]) > 1000:
            self._health_history[provider] = self._health_history[provider][-500:]
        if len(self._latency_history[provider]) > 1000:
            self._latency_history[provider] = self._latency_history[provider][-500:]

    def health_rate(self, provider: str, window: int = 100) -> float:
        history = self._health_history.get(provider, [])
        if not history:
            return 1.0
        recent = history[-window:] if len(history) > window else history
        return sum(recent) / len(recent)

    def avg_latency(self, provider: str, window: int = 100) -> float:
        history = self._latency_history.get(provider, [])
        if not history:
            return 0.0
        recent = history[-window:] if len(history) > window else history
        return sum(recent) / len(recent)

    def error_rate(self, provider: str) -> float:
        total = self._request_counts.get(provider, 0)
        errors = self._error_counts.get(provider, 0)
        return errors / total if total > 0 else 0.0

    def is_healthy(self, provider: str) -> bool:
        return self.health_rate(provider) >= self.alert_threshold

    def get_alerts(self) -> List[str]:
        alerts = []
        for provider in self._request_counts:
            if not self.is_healthy(provider):
                alerts.append(
                    f"ALERT: Provider {provider} health rate {self.health_rate(provider):.2%} "
                    f"below threshold {self.alert_threshold:.2%}"
                )
        return alerts

    def summary(self) -> Dict[str, Any]:
        summary = {}
        for provider in self._request_counts:
            summary[provider] = {
                "requests": self._request_counts[provider],
                "errors": self._error_counts.get(provider, 0),
                "error_rate": round(self.error_rate(provider), 4),
                "health_rate": round(self.health_rate(provider), 4),
                "avg_latency_ms": round(self.avg_latency(provider), 3),
                "healthy": self.is_healthy(provider),
            }
        return summary
