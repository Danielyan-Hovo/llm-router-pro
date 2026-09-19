from __future__ import annotations

import time
from typing import Any, Optional, Dict, List, Callable, Union
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field, asdict


@dataclass
class GatewayMetricsSnapshot:
    timestamp: float = field(default_factory=lambda: time.time())
    total_requests: int = 0
    success_rate: float = 0.0
    avg_latency_ms: float = 0.0
    error_rate: float = 0.0
    provider_distribution: Dict[str, int] = field(default_factory=dict)
    health_rates: Dict[str, float] = field(default_factory=dict)
    active_connections: int = 0
    cache_hit_rate: float = 0.0
    rate_limit_remaining: Dict[str, int] = field(default_factory=dict)


class ProductionMetricsExporter:
    """Exports gateway metrics in Prometheus text format for production monitoring."""

    def __init__(self) -> None:
        self._snapshots: List[GatewayMetricsSnapshot] = []
        self._labels: Dict[str, str] = {}

    def set_labels(self, labels: Dict[str, str]) -> None:
        self._labels = labels.copy()

    def record_snapshot(self, snapshot: GatewayMetricsSnapshot) -> None:
        self._snapshots.append(snapshot)
        if len(self._snapshots) > 1000:
            self._snapshots = self._snapshots[-500:]

    def export_prometheus(self) -> str:
        lines = []
        lines.append("# HELP gateway_requests_total Total gateway requests")
        lines.append("# TYPE gateway_requests_total counter")
        for snap in self._snapshots[-10:]:
            label_str = ",".join(f'{k}="{v}"' for k, v in self._labels.items())
            lines.append(f'gateway_requests_total{{{label_str}}} {snap.total_requests}')
        lines.append("# HELP gateway_success_rate Gateway success rate")
        lines.append("# TYPE gateway_success_rate gauge")
        for snap in self._snapshots[-10:]:
            label_str = ",".join(f'{k}="{v}"' for k, v in self._labels.items())
            lines.append(f'gateway_success_rate{{{label_str}}} {snap.success_rate}')
        lines.append("# HELP gateway_avg_latency_ms Average latency in ms")
        lines.append("# TYPE gateway_avg_latency_ms gauge")
        for snap in self._snapshots[-10:]:
            label_str = ",".join(f'{k}="{v}"' for k, v in self._labels.items())
            lines.append(f'gateway_avg_latency_ms{{{label_str}}} {snap.avg_latency_ms}')
        return "\n".join(lines) + "\n"

    def export_json(self) -> str:
        import json
        return json.dumps({
            "snapshots": [asdict(s) for s in self._snapshots[-10:]],
            "labels": self._labels,
        })

    def clear(self) -> None:
        self._snapshots.clear()
        self._labels.clear()
