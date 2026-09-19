from __future__ import annotations

import time
from typing import Any, Optional, Dict, List, Callable, Union, Tuple
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field, asdict
from enum import StrEnum


class GatewayStatus(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    INITIALIZING = "initializing"


@dataclass
class GatewayHealthReport:
    status: GatewayStatus
    timestamp: float = field(default_factory=lambda: time.time())
    uptime_seconds: float = 0.0
    total_requests: int = 0
    success_rate: float = 0.0
    avg_latency_ms: float = 0.0
    error_rate: float = 0.0
    provider_health: Dict[str, float] = field(default_factory=dict)
    active_connections: int = 0
    cache_hit_rate: float = 0.0
    rate_limit_remaining: Dict[str, int] = field(default_factory=dict)
    alerts: List[str] = field(default_factory=list)


class ProductionGatewayDashboard:
    """Production dashboard aggregator for LLM Router Pro gateway."""

    def __init__(self) -> None:
        self._reports: List[GatewayHealthReport] = []
        self._max_reports = 1000
        self._metrics: Dict[str, Any] = {
            "gateway_version": "2.0.0",
            "deployment_region": "us-east-1",
            "replica_count": 3,
        }

    def generate_report(
        self,
        status: GatewayStatus,
        uptime: float,
        requests: int,
        success_rate: float,
        latency: float,
        error_rate: float,
        provider_health: Dict[str, float],
        connections: int,
        cache_rate: float,
        rate_limits: Dict[str, int],
        alerts: Optional[List[str]] = None,
    ) -> GatewayHealthReport:
        report = GatewayHealthReport(
            status=status,
            timestamp=time.time(),
            uptime_seconds=uptime,
            total_requests=requests,
            success_rate=success_rate,
            avg_latency_ms=latency,
            error_rate=error_rate,
            provider_health=provider_health,
            active_connections=connections,
            cache_hit_rate=cache_rate,
            rate_limit_remaining=rate_limits,
            alerts=alerts or [],
        )
        self._reports.append(report)
        if len(self._reports) > self._max_reports:
            self._reports = self._reports[-500:]
        return report

    def get_latest(self) -> Optional[GatewayHealthReport]:
        return self._reports[-1] if self._reports else None

    def get_trend(self, window: int = 10) -> List[Dict[str, Any]]:
        recent = self._reports[-window:] if len(self._reports) >= window else self._reports
        return [
            {
                "timestamp": r.timestamp,
                "status": r.status.value,
                "success_rate": r.success_rate,
                "avg_latency_ms": r.avg_latency_ms,
                "error_rate": r.error_rate,
                "alerts_count": len(r.alerts),
            }
            for r in recent
        ]

    def export_summary(self) -> Dict[str, Any]:
        latest = self.get_latest()
        if not latest:
            return {"status": "no_data", "metrics": self._metrics}
        return {
            "status": latest.status.value,
            "timestamp": latest.timestamp,
            "uptime_seconds": latest.uptime_seconds,
            "total_requests": latest.total_requests,
            "success_rate": latest.success_rate,
            "avg_latency_ms": latest.avg_latency_ms,
            "error_rate": latest.error_rate,
            "provider_health": latest.provider_health,
            "alerts": latest.alerts,
            "metrics_config": self._metrics,
        }
