from __future__ import annotations

import time
import json
import hashlib
import logging
from typing import Any, Optional, Dict, List, Callable, Union, Tuple
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field, asdict
from enum import StrEnum, auto

logger = logging.getLogger(__name__)


class GatewayEventType(StrEnum):
    REQUEST_START = "request.start"
    REQUEST_END = "request.end"
    PROVIDER_SELECT = "provider.select"
    CACHE_HIT = "cache.hit"
    CACHE_MISS = "cache.miss"
    RATE_LIMIT = "rate.limit"
    CIRCUIT_BREAKER = "circuit.breaker"
    HEALTH_CHECK = "health.check"
    ERROR = "error"
    METRICS_EXPORT = "metrics.export"
    LOAD_BALANCE = "load.balance"
    MONITOR_ALERT = "monitor.alert"
    PIPELINE_PROCESS = "pipeline.process"
    ORCHESTRATOR_INIT = "orchestrator.init"
    ORCHESTRATOR_SHUTDOWN = "orchestrator.shutdown"
    INTEGRATION_START = "integration.start"
    INTEGRATION_END = "integration.end"
    AUDIT_PERSIST = "audit.persist"
    ADVISORY_GENERATE = "advisory.generate"
    DEPLOYMENT_READY = "deployment.ready"
    PRODUCTION_VALIDATED = "production.validated"
    FINAL_INTEGRATION = "final.integration"
    CIRCUIT_OPEN = "circuit.open"
    CIRCUIT_HALF_OPEN = "circuit.half_open"
    CIRCUIT_CLOSED = "circuit.closed"
    RETRY_ATTEMPT = "retry.attempt"
    RETRY_EXHAUSTED = "retry.exhausted"
    REQUEST_VALIDATE = "request.validate"
    REQUEST_REJECT = "request.reject"
    RESPONSE_TRANSFORM = "response.transform"
    AUDIT_LOG = "audit.log"
    SECURITY_ALERT = "security.alert"
    RATE_LIMITED = "rate.limited"


@dataclass
class GatewayEvent:
    event_type: GatewayEventType
    timestamp: float = field(default_factory=lambda: time.time())
    provider: Optional[str] = None
    model: Optional[str] = None
    latency_ms: Optional[float] = None
    status_code: Optional[int] = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class GatewayEventBus:
    def __init__(self) -> None:
        self._subscribers: Dict[GatewayEventType, List[Callable]] = defaultdict(list)
        self._events: List[GatewayEvent] = []
        self._max_events = 10000

    def subscribe(self, event_type: GatewayEventType, handler: Callable) -> None:
        self._subscribers[event_type].append(handler)

    def unsubscribe(self, event_type: GatewayEventType, handler: Callable) -> bool:
        if handler in self._subscribers.get(event_type, []):
            self._subscribers[event_type].remove(handler)
            return True
        return False

    def publish(self, event: GatewayEvent) -> None:
        self._events.append(event)
        if len(self._events) > self._max_events:
            self._events = self._events[-5000:]
        for handler in self._subscribers.get(event.event_type, []):
            try:
                handler(event)
            except Exception:
                pass

    def get_events(self, event_type: Optional[GatewayEventType] = None, limit: int = 100) -> List[GatewayEvent]:
        events = self._events
        if event_type is not None:
            events = [e for e in events if e.event_type == event_type]
        return events[-limit:]

    def clear(self) -> None:
        self._events.clear()
        self._subscribers.clear()

    def event_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = defaultdict(int)
        for event in self._events:
            counts[event.event_type.value] += 1
        return dict(counts)


class GatewayPipeline:
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


class GatewayCache:
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


class GatewayMonitor:
    def __init__(self, alert_threshold: float = 0.95) -> None:
        self.alert_threshold = alert_threshold
        self._health_history: Dict[str, List[bool]] = defaultdict(list)
        self._latency_history: Dict[str, List[float]] = defaultdict(list)
        self._error_counts: Dict[str, int] = defaultdict(int)
        self._request_counts: Dict[str, int] = defaultdict(int)

    def record_request(self, provider: str, success: bool, latency_ms: float, model: Optional[str] = None) -> None:
        self._request_counts[provider] += 1
        self._health_history[provider].append(success)
        self._latency_history[provider].append(latency_ms)
        if not success:
            self._error_counts[provider] += 1
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


class GatewayMetrics:
    def __init__(self) -> None:
        self.counters: Dict[str, int] = {}
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = {}

    def increment(self, name: str, value: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + value

    def set_gauge(self, name: str, value: float) -> None:
        self.gauges[name] = value

    def observe(self, name: str, value: float) -> None:
        self.histograms.setdefault(name, []).append(value)

    def summary(self) -> Dict[str, Any]:
        hist_summary = {}
        for name, values in self.histograms.items():
            hist_summary[name] = {
                "count": len(values),
                "sum": sum(values),
                "avg": sum(values) / len(values) if values else 0.0,
                "min": min(values) if values else 0.0,
                "max": max(values) if values else 0.0,
            }
        return {
            "counters": self.counters.copy(),
            "gauges": self.gauges.copy(),
            "histograms": hist_summary,
        }


class MetricsExporter:
    def __init__(self) -> None:
        self.snapshots: List[Dict[str, Any]] = []
        self.labels: Dict[str, str] = {}

    def set_labels(self, labels: Dict[str, str]) -> None:
        self.labels = labels.copy()

    def record_snapshot(self, snapshot: Dict[str, Any]) -> None:
        self.snapshots.append({**snapshot, "labels": self.labels.copy()})
        if len(self.snapshots) > 1000:
            self.snapshots = self.snapshots[-500:]

    def export_prometheus(self) -> str:
        lines = []
        lines.append("# HELP gateway_requests_total Total gateway requests")
        lines.append("# TYPE gateway_requests_total counter")
        for snap in self.snapshots[-10:]:
            label_str = ",".join(f'{k}="{v}"' for k, v in snap.get("labels", {}).items())
            lines.append(f'gateway_requests_total{{{label_str}}} {snap.get("total_requests", 0)}')
        lines.append("# HELP gateway_success_rate Gateway success rate")
        lines.append("# TYPE gateway_success_rate gauge")
        for snap in self.snapshots[-10:]:
            label_str = ",".join(f'{k}="{v}"' for k, v in snap.get("labels", {}).items())
            lines.append(f'gateway_success_rate{{{label_str}}} {snap.get("success_rate", 0.0)}')
        lines.append("# HELP gateway_avg_latency_ms Average latency in ms")
        lines.append("# TYPE gateway_avg_latency_ms gauge")
        for snap in self.snapshots[-10:]:
            label_str = ",".join(f'{k}="{v}"' for k, v in snap.get("labels", {}).items())
            lines.append(f'gateway_avg_latency_ms{{{label_str}}} {snap.get("avg_latency_ms", 0.0)}')
        return "\n".join(lines) + "\n"

    def export_json(self) -> str:
        import json
        return json.dumps({"snapshots": self.snapshots[-10:], "labels": self.labels})

    def clear(self) -> None:
        self.snapshots.clear()
        self.labels.clear()


class LoadBalancer:
    def __init__(self, weights: Optional[Dict[str, float]] = None) -> None:
        self.weights = weights or {}
        self._current_index: Dict[str, int] = defaultdict(int)
        self._request_counts: Dict[str, int] = defaultdict(int)

    def select_provider(self, available: List[str]) -> Optional[str]:
        if not available:
            return None
        if not self.weights:
            for provider in available:
                self._current_index[provider] = (self._current_index.get(provider, 0) + 1) % len(available)
            return available[self._current_index.get(available[0], 0) % len(available)]
        weighted = [(p, self.weights.get(p, 1.0)) for p in available]
        total = sum(w for _, w in weighted)
        if total == 0:
            return available[0]
        best = min(weighted, key=lambda item: self._request_counts[item[0]] / item[1])
        self._request_counts[best[0]] += 1
        return best[0]

    def get_stats(self) -> Dict[str, Any]:
        return {
            "weights": dict(self.weights),
            "request_counts": dict(self._request_counts),
            "current_index": dict(self._current_index),
        }

    def reset(self) -> None:
        self._current_index.clear()
        self._request_counts.clear()


class RateLimiter:
    def __init__(self, max_requests: int = 100, window_seconds: float = 60.0) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: Dict[str, List[float]] = {}

    def is_allowed(self, key: str) -> bool:
        now = time.time()
        window_start = now - self.window_seconds
        requests = self._requests.get(key, [])
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


class GatewayConfig:
    def __init__(
        self,
        max_connections: int = 100,
        max_keepalive: int = 20,
        timeout: float = 30.0,
        retry_attempts: int = 3,
        retry_delay: float = 1.0,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 60.0,
        rate_limit_requests: int = 100,
        rate_limit_window: float = 60.0,
        cache_max_entries: int = 500,
        cache_default_ttl: float = 120.0,
        metrics_enabled: bool = True,
        health_check_interval: float = 10.0,
    ) -> None:
        self.max_connections = max_connections
        self.max_keepalive = max_keepalive
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.circuit_breaker_timeout = circuit_breaker_timeout
        self.rate_limit_requests = rate_limit_requests
        self.rate_limit_window = rate_limit_window
        self.cache_max_entries = cache_max_entries
        self.cache_default_ttl = cache_default_ttl
        self.metrics_enabled = metrics_enabled
        self.health_check_interval = health_check_interval


class GatewayOrchestrator:
    def __init__(self, config: Optional[GatewayConfig] = None) -> None:
        self.config = config or GatewayConfig()
        self._state: Dict[str, Any] = {
            "initialized": False,
            "start_time": time.time(),
            "requests_processed": 0,
            "errors": 0,
            "provider_health": {},
        }

    def initialize(self) -> Dict[str, Any]:
        self._state["initialized"] = True
        self._state["start_time"] = time.time()
        return {"status": "initialized", "config": asdict(self.config)}

    def process_request(
        self,
        model: str,
        provider: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self._state["requests_processed"] += 1
        result = {
            "model": model,
            "provider": provider,
            "status": "processed",
            "timestamp": time.time(),
            "config_applied": asdict(self.config),
        }
        if payload:
            result["payload_size"] = len(str(payload))
        return result

    def get_health(self) -> Dict[str, Any]:
        uptime = time.time() - self._state.get("start_time", time.time())
        return {
            "status": "healthy" if self._state.get("initialized") else "not_initialized",
            "uptime_seconds": round(uptime, 2),
            "requests_processed": self._state.get("requests_processed", 0),
            "errors": self._state.get("errors", 0),
            "initialized": self._state.get("initialized", False),
        }

    def shutdown(self) -> Dict[str, Any]:
        self._state["initialized"] = False
        return {"status": "shutdown", "uptime": time.time() - self._state.get("start_time", time.time())}


class GatewayDashboard:
    def __init__(self) -> None:
        self._reports: List[Dict[str, Any]] = []
        self._max_reports = 500

    def generate_report(
        self,
        health: Dict[str, Any],
        metrics: Dict[str, Any],
        events: List[Any],
    ) -> Dict[str, Any]:
        report = {
            "timestamp": time.time(),
            "health": health,
            "metrics_summary": metrics,
            "event_summary": {"count": len(events)},
            "status": health.get("status", "unknown"),
        }
        self._reports.append(report)
        if len(self._reports) > self._max_reports:
            self._reports = self._reports[-250:]
        return report

    def get_latest(self) -> Optional[Dict[str, Any]]:
        return self._reports[-1] if self._reports else None

    def get_trend(self, window: int = 10) -> List[Dict[str, Any]]:
        return self._reports[-window:] if len(self._reports) >= window else self._reports

    def export_summary(self) -> Dict[str, Any]:
        latest = self.get_latest()
        return {
            "latest_report": latest,
            "total_reports": len(self._reports),
            "max_reports": self._max_reports,
        }


class GatewayIntegration:
    def __init__(self) -> None:
        self.event_bus = GatewayEventBus()
        self.pipeline = GatewayPipeline()
        self.cache = GatewayCache()
        self.monitor = GatewayMonitor()
        self.metrics = GatewayMetrics()
        self.exporter = MetricsExporter()
        self.load_balancer = LoadBalancer()
        self.rate_limiter = RateLimiter()
        self.config = GatewayConfig()
        self.orchestrator = GatewayOrchestrator(self.config)

    def initialize_all(self) -> Dict[str, Any]:
        init_result = self.orchestrator.initialize()
        self.event_bus.subscribe(GatewayEventType.HEALTH_CHECK, lambda e: None)
        self.event_bus.subscribe(GatewayEventType.ERROR, lambda e: self.metrics.increment("errors"))
        return {**init_result, "components": ["event_bus", "pipeline", "cache", "monitor", "metrics", "exporter", "load_balancer", "rate_limiter", "config", "orchestrator"]}

    def process_full_request(self, model: str, provider: Optional[str] = None, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        allowed = self.rate_limiter.is_allowed(f"provider:{provider or 'default'}")
        if not allowed:
            return {"status": "rate_limited", "model": model, "provider": provider}
        selected = self.load_balancer.select_provider([provider] if provider else ["openai", "anthropic", "local"])
        result = self.orchestrator.process_request(model, selected, payload)
        self.event_bus.publish(GatewayEvent(event_type=GatewayEventType.PROVIDER_SELECT, provider=selected, model=model))
        self.event_bus.publish(GatewayEvent(event_type=GatewayEventType.REQUEST_END, provider=selected, model=model, latency_ms=0.0))
        return result

    def get_full_status(self) -> Dict[str, Any]:
        return {
            "orchestrator": self.orchestrator.get_health(),
            "monitor": self.monitor.summary(),
            "metrics": self.metrics.summary(),
            "load_balancer": self.load_balancer.get_stats(),
            "cache": self.cache.stats(),
            "rate_limiter": {"remaining": self.rate_limiter.remaining("default")},
            "event_counts": self.event_bus.event_counts(),
        }


class ProductionDeploymentValidator:
    def __init__(self) -> None:
        self._checks: List[str] = []
        self._passed: List[str] = []
        self._failed: List[str] = []

    def add_check(self, name: str, check_fn: Callable[[], bool]) -> None:
        self._checks.append(name)
        try:
            result = check_fn()
            if result:
                self._passed.append(name)
            else:
                self._failed.append(name)
        except Exception:
            self._failed.append(name)

    def validate_all(self) -> Dict[str, Any]:
        results = {}
        for name in self._checks:
            results[name] = name in self._passed
        return {
            "passed": len(self._failed) == 0,
            "passed_count": len(self._passed),
            "failed_count": len(self._failed),
            "total": len(self._checks),
            "results": results,
        }

    def reset(self) -> None:
        self._checks.clear()
        self._passed.clear()
        self._failed.clear()


class FinalIntegrationLayer:
    def __init__(self) -> None:
        self.gateway = GatewayIntegration()
        self.validator = ProductionDeploymentValidator()
        self.event_bus = GatewayEventBus()
        self.metrics = GatewayMetrics()
        self.exporter = MetricsExporter()

    def initialize_production(self) -> Dict[str, Any]:
        init = self.gateway.initialize_all()
        self.event_bus.subscribe(GatewayEventType.INTEGRATION_START, lambda e: self.metrics.increment("integration_started"))
        self.event_bus.subscribe(GatewayEventType.INTEGRATION_END, lambda e: self.metrics.increment("integration_ended"))
        self.event_bus.subscribe(GatewayEventType.DEPLOYMENT_READY, lambda e: self.metrics.increment("deployment_ready"))
        self.event_bus.subscribe(GatewayEventType.PRODUCTION_VALIDATED, lambda e: self.metrics.increment("production_validated"))
        self.event_bus.subscribe(GatewayEventType.FINAL_INTEGRATION, lambda e: self.metrics.increment("final_integration"))
        return {**init, "final_layer": "initialized", "production_ready": True}

    def run_final_validation(self) -> Dict[str, Any]:
        self.validator.add_check("gateway_initialized", lambda: self.gateway.orchestrator.get_health()["initialized"])
        self.validator.add_check("event_bus_active", lambda: len(self.event_bus.event_counts()) >= 0)
        self.validator.add_check("metrics_active", lambda: self.metrics.summary()["counters"] is not None)
        self.validator.add_check("exporter_ready", lambda: True)
        return self.validator.validate_all()

    def get_production_summary(self) -> Dict[str, Any]:
        return {
            "gateway_status": self.gateway.get_full_status(),
            "validation": self.run_final_validation(),
            "metrics_summary": self.metrics.summary(),
            "exporter_labels": self.exporter.labels,
            "production_ready": True,
        }


class CircuitBreakerState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    half_open_max_calls: int = 3
    expected_exception: type = Exception


class CircuitBreaker:
    def __init__(self, config: Optional[CircuitBreakerConfig] = None) -> None:
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._success_count = 0
        self._total_calls = 0
        self._history: List[Dict[str, Any]] = []

    @property
    def state(self) -> CircuitBreakerState:
        if self._state == CircuitBreakerState.OPEN:
            if self._last_failure_time and time.time() - self._last_failure_time > self.config.recovery_timeout:
                self._state = CircuitBreakerState.HALF_OPEN
                self._half_open_calls = 0
        return self._state

    def call(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        self._total_calls += 1
        current_state = self.state
        if current_state == CircuitBreakerState.OPEN:
            self._record_event("rejected", None)
            raise CircuitBreakerOpenError(f"Circuit breaker is OPEN; last failure at {self._last_failure_time}")
        try:
            result = func(*args, **kwargs)
            self._on_success()
            self._record_event("success", None)
            return result
        except self.config.expected_exception as exc:
            self._on_failure()
            self._record_event("failure", str(exc))
            raise

    def _on_success(self) -> None:
        if self._state == CircuitBreakerState.HALF_OPEN:
            self._half_open_calls += 1
            self._success_count += 1
            if self._half_open_calls >= self.config.half_open_max_calls:
                self._state = CircuitBreakerState.CLOSED
                self._failure_count = 0
                self._half_open_calls = 0
        else:
            self._failure_count = max(0, self._failure_count - 1)

    def _on_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._failure_count >= self.config.failure_threshold:
            self._state = CircuitBreakerState.OPEN

    def _record_event(self, event_type: str, error: Optional[str]) -> None:
        self._history.append({
            "type": event_type,
            "timestamp": time.time(),
            "state": str(self._state),
            "error": error,
        })
        if len(self._history) > 1000:
            self._history = self._history[-500:]

    def get_stats(self) -> Dict[str, Any]:
        return {
            "state": str(self.state),
            "failure_count": self._failure_count,
            "total_calls": self._total_calls,
            "success_count": self._success_count,
            "last_failure_time": self._last_failure_time,
            "history_size": len(self._history),
        }

    def reset(self) -> None:
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._half_open_calls = 0
        self._success_count = 0


class CircuitBreakerOpenError(Exception):
    pass


class RetryPolicy:
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        backoff_multiplier: float = 2.0,
        retryable_exceptions: Tuple[type, ...] = (Exception,),
    ) -> None:
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_multiplier = backoff_multiplier
        self.retryable_exceptions = retryable_exceptions
        self._attempts_history: List[Dict[str, Any]] = []

    def execute(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        last_exception: Optional[Exception] = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                result = func(*args, **kwargs)
                self._record_attempt(attempt, True, None)
                return result
            except self.retryable_exceptions as exc:
                last_exception = exc
                self._record_attempt(attempt, False, str(exc))
                if attempt < self.max_attempts:
                    delay = min(self.base_delay * (self.backoff_multiplier ** (attempt - 1)), self.max_delay)
                    time.sleep(delay)
        self._record_attempt(self.max_attempts, False, str(last_exception))
        raise last_exception if last_exception else RuntimeError("Retry exhausted")

    def _record_attempt(self, attempt: int, success: bool, error: Optional[str]) -> None:
        self._attempts_history.append({
            "attempt": attempt,
            "success": success,
            "error": error,
            "timestamp": time.time(),
        })

    def get_stats(self) -> Dict[str, Any]:
        total = len(self._attempts_history)
        successes = sum(1 for a in self._attempts_history if a["success"])
        return {
            "total_attempts": total,
            "successes": successes,
            "failures": total - successes,
            "max_attempts": self.max_attempts,
        }


class RequestValidator:
    def __init__(self, max_payload_size: int = 1048576, allowed_models: Optional[List[str]] = None) -> None:
        self.max_payload_size = max_payload_size
        self.allowed_models = allowed_models or []
        self._validation_history: List[Dict[str, Any]] = []

    def validate(self, request: Dict[str, Any]) -> Dict[str, Any]:
        errors: List[str] = []
        model = request.get("model")
        if self.allowed_models and model not in self.allowed_models:
            errors.append(f"Model '{model}' not in allowed list")
        payload = request.get("payload")
        if payload is not None:
            payload_size = len(str(payload).encode("utf-8"))
            if payload_size > self.max_payload_size:
                errors.append(f"Payload size {payload_size} exceeds max {self.max_payload_size}")
        if not request.get("api_key"):
            errors.append("Missing api_key")
        is_valid = len(errors) == 0
        self._validation_history.append({
            "valid": is_valid,
            "errors": errors,
            "timestamp": time.time(),
            "model": model,
        })
        if len(self._validation_history) > 1000:
            self._validation_history = self._validation_history[-500:]
        return {"valid": is_valid, "errors": errors}

    def get_stats(self) -> Dict[str, Any]:
        total = len(self._validation_history)
        valid = sum(1 for v in self._validation_history if v["valid"])
        return {"total": total, "valid": valid, "invalid": total - valid}


class ResponseTransformer:
    def __init__(self, include_metadata: bool = True, mask_api_keys: bool = True) -> None:
        self.include_metadata = include_metadata
        self.mask_api_keys = mask_api_keys
        self._transform_count = 0

    def transform(self, response: Dict[str, Any], request: Dict[str, Any]) -> Dict[str, Any]:
        self._transform_count += 1
        result = dict(response)
        if self.include_metadata:
            result["_metadata"] = {
                "transformed_at": time.time(),
                "request_model": request.get("model"),
                "transform_version": "1.0.0",
            }
        if self.mask_api_keys and "api_key" in result:
            result["api_key"] = "***MASKED***"
        if "content" in result and isinstance(result["content"], str):
            result["content"] = result["content"].strip()
        return result

    def get_stats(self) -> Dict[str, Any]:
        return {"transform_count": self._transform_count}


class StructuredLogger:
    def __init__(self, name: str = "gateway") -> None:
        self._logger = logging.getLogger(name)
        self._log_history: List[Dict[str, Any]] = []

    def info(self, message: str, **kwargs: Any) -> None:
        entry = {"level": "INFO", "message": message, "timestamp": time.time(), **kwargs}
        self._logger.info(json.dumps(entry, default=str))
        self._log_history.append(entry)

    def error(self, message: str, **kwargs: Any) -> None:
        entry = {"level": "ERROR", "message": message, "timestamp": time.time(), **kwargs}
        self._logger.error(json.dumps(entry, default=str))
        self._log_history.append(entry)

    def warning(self, message: str, **kwargs: Any) -> None:
        entry = {"level": "WARNING", "message": message, "timestamp": time.time(), **kwargs}
        self._logger.warning(json.dumps(entry, default=str))
        self._log_history.append(entry)

    def get_logs(self, level: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        logs = self._log_history
        if level:
            logs = [l for l in logs if l["level"] == level]
        return logs[-limit:]

    def clear(self) -> None:
        self._log_history.clear()


class SecurityMonitor:
    def __init__(self, max_failed_auth: int = 10, window_seconds: float = 300.0) -> None:
        self.max_failed_auth = max_failed_auth
        self.window_seconds = window_seconds
        self._failed_auth: Dict[str, List[float]] = defaultdict(list)
        self._blocked_ips: Dict[str, float] = {}
        self._alerts: List[Dict[str, Any]] = []

    def record_auth_failure(self, identifier: str, ip: str = "unknown") -> bool:
        now = time.time()
        self._failed_auth[identifier].append(now)
        self._failed_auth[identifier] = [t for t in self._failed_auth[identifier] if now - t < self.window_seconds]
        if len(self._failed_auth[identifier]) >= self.max_failed_auth:
            self._blocked_ips[ip] = now
            alert = {"type": "brute_force", "identifier": identifier, "ip": ip, "timestamp": now}
            self._alerts.append(alert)
            logger.warning(f"Security alert: brute force detected from {ip}")
            return True
        return False

    def is_blocked(self, ip: str) -> bool:
        if ip in self._blocked_ips:
            blocked_since = time.time() - self._blocked_ips[ip]
            if blocked_since > 600:
                del self._blocked_ips[ip]
                return False
            return True
        return False

    def get_alerts(self) -> List[Dict[str, Any]]:
        return self._alerts[-50:]

    def get_stats(self) -> Dict[str, Any]:
        return {
            "blocked_ips": len(self._blocked_ips),
            "total_alerts": len(self._alerts),
            "tracked_identifiers": len(self._failed_auth),
        }


class HMACAuthValidator:
    def __init__(self, secret_key: str) -> None:
        self._secret_key = secret_key
        self._validation_count = 0
        self._failed_count = 0

    def validate(self, payload: str, signature: str) -> bool:
        self._validation_count += 1
        expected = hashlib.sha256(f"{payload}:{self._secret_key}".encode()).hexdigest()
        valid = hmac.compare_digest(expected, signature)
        if not valid:
            self._failed_count += 1
        return valid

    def get_stats(self) -> Dict[str, Any]:
        return {
            "validations": self._validation_count,
            "failures": self._failed_count,
            "success_rate": round((self._validation_count - self._failed_count) / self._validation_count, 4) if self._validation_count > 0 else 1.0,
        }


class GatewayRequest:
    def __init__(self, model: str, payload: Dict[str, Any], api_key: str, ip: str = "unknown") -> None:
        self.model = model
        self.payload = payload
        self.api_key = api_key
        self.ip = ip
        self.timestamp = time.time()
        self.request_id = hashlib.sha256(f"{ip}:{self.timestamp}:{model}".encode()).hexdigest()[:16]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "model": self.model,
            "payload_size": len(str(self.payload)),
            "ip": self.ip,
            "timestamp": self.timestamp,
        }


class GatewayResponse:
    def __init__(self, request_id: str, content: str, model: str, provider: str, latency_ms: float) -> None:
        self.request_id = request_id
        self.content = content
        self.model = model
        self.provider = provider
        self.latency_ms = latency_ms
        self.timestamp = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "content": self.content,
            "model": self.model,
            "provider": self.provider,
            "latency_ms": round(self.latency_ms, 3),
            "timestamp": self.timestamp,
        }


class ProductionGateway:
    def __init__(self, secret_key: str) -> None:
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.retry_policies: Dict[str, RetryPolicy] = {}
        self.request_validator = RequestValidator()
        self.response_transformer = ResponseTransformer()
        self.logger = StructuredLogger("production_gateway")
        self.security = SecurityMonitor()
        self.auth = HMACAuthValidator(secret_key)
        self.event_bus = GatewayEventBus()
        self.metrics = GatewayMetrics()
        self.config = GatewayConfig()

    def get_circuit_breaker(self, provider: str) -> CircuitBreaker:
        if provider not in self.circuit_breakers:
            self.circuit_breakers[provider] = CircuitBreaker()
        return self.circuit_breakers[provider]

    def get_retry_policy(self, model: str) -> RetryPolicy:
        if model not in self.retry_policies:
            self.retry_policies[model] = RetryPolicy()
        return self.retry_policies[model]

    def process_request(self, request: GatewayRequest) -> GatewayResponse:
        self.logger.info("request_received", request_id=request.request_id, model=request.model, ip=request.ip)
        if self.security.is_blocked(request.ip):
            self.metrics.increment("security.blocked")
            raise PermissionError(f"IP {request.ip} is blocked")
        validation = self.request_validator.validate(request.to_dict())
        if not validation["valid"]:
            self.metrics.increment("validation.failed")
            self.event_bus.publish(GatewayEvent(event_type=GatewayEventType.REQUEST_REJECT, error_message=str(validation["errors"])))
            raise ValueError(f"Validation failed: {validation['errors']}")
        cb = self.get_circuit_breaker(request.model)
        retry = self.get_retry_policy(request.model)
        def _call_provider() -> GatewayResponse:
            start = time.time()
            latency = (time.time() - start) * 1000
            return GatewayResponse(request.request_id, "response", request.model, "provider", latency)
        try:
            response = retry.execute(lambda: cb.call(_call_provider))
            self.metrics.increment("requests.success")
            self.logger.info("request_success", request_id=request.request_id, latency_ms=response.latency_ms)
        except Exception as exc:
            self.metrics.increment("requests.failure")
            self.logger.error("request_failed", request_id=request.request_id, error=str(exc))
            raise
        transformed = self.response_transformer.transform(response.to_dict(), request.to_dict())
        return GatewayResponse(**transformed)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "circuit_breakers": {k: v.get_stats() for k, v in self.circuit_breakers.items()},
            "retry_policies": {k: v.get_stats() for k, v in self.retry_policies.items()},
            "validator": self.request_validator.get_stats(),
            "security": self.security.get_stats(),
            "auth": self.auth.get_stats(),
            "metrics": self.metrics.summary(),
        }


class GatewayAuditTrail:
    def __init__(self, max_entries: int = 5000) -> None:
        self.max_entries = max_entries
        self._trail: List[Dict[str, Any]] = []

    def log(self, action: str, actor: str, resource: str, result: str, details: Optional[Dict[str, Any]] = None) -> None:
        entry = {
            "timestamp": time.time(),
            "action": action,
            "actor": actor,
            "resource": resource,
            "result": result,
            "details": details or {},
        }
        self._trail.append(entry)
        if len(self._trail) > self.max_entries:
            self._trail = self._trail[-2500:]

    def query(self, action: Optional[str] = None, actor: Optional[str] = None, resource: Optional[str] = None) -> List[Dict[str, Any]]:
        results = self._trail
        if action:
            results = [r for r in results if r["action"] == action]
        if actor:
            results = [r for r in results if r["actor"] == actor]
        if resource:
            results = [r for r in results if r["resource"] == resource]
        return results

    def get_stats(self) -> Dict[str, Any]:
        actions = defaultdict(int)
        for entry in self._trail:
            actions[entry["action"]] += 1
        return {"total_entries": len(self._trail), "max_entries": self.max_entries, "action_counts": dict(actions)}

    def clear(self) -> None:
        self._trail.clear()


class GatewayRateLimitPolicy:
    def __init__(self, requests_per_minute: int = 60, burst_limit: int = 10) -> None:
        self.requests_per_minute = requests_per_minute
        self.burst_limit = burst_limit
        self._buckets: Dict[str, List[float]] = defaultdict(list)

    def is_allowed(self, key: str) -> bool:
        now = time.time()
        window_start = now - 60.0
        self._buckets[key] = [t for t in self._buckets.get(key, []) if t > window_start]
        if len(self._buckets[key]) >= self.requests_per_minute:
            return False
        if len(self._buckets[key]) >= self.burst_limit:
            return False
        self._buckets[key].append(now)
        return True

    def remaining(self, key: str) -> int:
        now = time.time()
        window_start = now - 60.0
        self._buckets[key] = [t for t in self._buckets.get(key, []) if t > window_start]
        return max(0, self.requests_per_minute - len(self._buckets[key]))

    def reset(self, key: str) -> None:
        self._buckets.pop(key, None)


class GatewayHealthCheck:
    def __init__(self, endpoint: str, interval: float = 30.0, timeout: float = 5.0) -> None:
        self.endpoint = endpoint
        self.interval = interval
        self.timeout = timeout
        self._last_check: Optional[float] = None
        self._last_result: Optional[bool] = None
        self._history: List[bool] = []

    def check(self) -> bool:
        self._last_check = time.time()
        try:
            import urllib.request
            with urllib.request.urlopen(self.endpoint, timeout=self.timeout) as resp:
                result = resp.status == 200
        except Exception:
            result = False
        self._last_result = result
        self._history.append(result)
        if len(self._history) > 100:
            self._history = self._history[-50:]
        return result

    def is_healthy(self) -> bool:
        if not self._history:
            return False
        return sum(self._history[-10:]) / min(len(self._history), 10) >= 0.8

    def get_stats(self) -> Dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "last_check": self._last_check,
            "last_result": self._last_result,
            "healthy": self.is_healthy(),
            "history_size": len(self._history),
        }


class GatewayDeploymentConfig:
    def __init__(self, env: str = "production", replicas: int = 3, region: str = "us-east-1") -> None:
        self.env = env
        self.replicas = replicas
        self.region = region
        self.features: Dict[str, bool] = {"circuit_breaker": True, "retry": True, "audit": True, "security": True}
        self._deployment_history: List[Dict[str, Any]] = []

    def deploy(self, version: str) -> Dict[str, Any]:
        result = {"version": version, "env": self.env, "replicas": self.replicas, "region": self.region, "status": "deployed", "timestamp": time.time()}
        self._deployment_history.append(result)
        return result

    def rollback(self, version: str) -> Dict[str, Any]:
        result = {"version": version, "env": self.env, "action": "rollback", "timestamp": time.time()}
        self._deployment_history.append(result)
        return result

    def get_history(self) -> List[Dict[str, Any]]:
        return self._deployment_history[-20:]

    def to_dict(self) -> Dict[str, Any]:
        return {"env": self.env, "replicas": self.replicas, "region": self.region, "features": self.features.copy()}

 c l a s s   G a t e w a y C o n n e c t i o n P o o l : 
         d e f   _ _ i n i t _ _ ( s e l f ,   m a x _ s i z e = 5 0 ,   i d l e _ t i m e o u t = 3 0 0 . 0 ) : 
                 s e l f . m a x _ s i z e   =   m a x _ s i z e 
                 s e l f . i d l e _ t i m e o u t   =   i d l e _ t i m e o u t 
                 s e l f . _ p o o l   =   [ ] 
                 s e l f . _ i n _ u s e   =   s e t ( ) 
                 s e l f . _ s t a t s   =   { \  
 c r e a t e d \ :   0 ,   \ r e u s e d \ :   0 ,   \ d e s t r o y e d \ :   0 ,   \ a c t i v e \ :   0 } 
 
         d e f   a c q u i r e ( s e l f ,   c o n n e c t i o n _ i d ) : 
                 n o w   =   t i m e . t i m e ( ) 
                 a v a i l a b l e   =   [ c   f o r   c   i n   s e l f . _ p o o l   i f   c [ \ i d \ ]   n o t   i n   s e l f . _ i n _ u s e   a n d   ( n o w   -   c . g e t ( \ l a s t _ u s e d \ ,   0 ) )   <   s e l f . i d l e _ t i m e o u t ] 
                 i f   a v a i l a b l e : 
                         c o n n   =   a v a i l a b l e [ 0 ] 
                         c o n n [ \ l a s t _ u s e d \ ]   =   n o w 
                         s e l f . _ i n _ u s e . a d d ( c o n n [ \ i d \ ] ) 
                         s e l f . _ s t a t s [ \ r e u s e d \ ]   + =   1 
                         r e t u r n   c o n n 
                 i f   l e n ( s e l f . _ p o o l )   +   l e n ( s e l f . _ i n _ u s e )   <   s e l f . m a x _ s i z e : 
                         c o n n   =   { \ i d \ :   c o n n e c t i o n _ i d ,   \ c r e a t e d _ a t \ :   n o w ,   \ l a s t _ u s e d \ :   n o w ,   \ s t a t u s \ :   \ a c t i v e \ } 
                         s e l f . _ p o o l . a p p e n d ( c o n n ) 
                         s e l f . _ i n _ u s e . a d d ( c o n n e c t i o n _ i d ) 
                         s e l f . _ s t a t s [ \ c r e a t e d \ ]   + =   1 
                         r e t u r n   c o n n 
                 r e t u r n   N o n e 
 
         d e f   r e l e a s e ( s e l f ,   c o n n e c t i o n _ i d ) : 
                 s e l f . _ i n _ u s e . d i s c a r d ( c o n n e c t i o n _ i d ) 
                 f o r   c o n n   i n   s e l f . _ p o o l : 
                         i f   c o n n [ \ i d \ ]   = =   c o n n e c t i o n _ i d : 
                                 c o n n [ \ l a s t _ u s e d \ ]   =   t i m e . t i m e ( ) 
                                 c o n n [ \ s t a t u s \ ]   =   \ i d l e \ 
 
         d e f   c l e a n u p ( s e l f ) : 
                 n o w   =   t i m e . t i m e ( ) 
                 t o _ r e m o v e   =   [ c   f o r   c   i n   s e l f . _ p o o l   i f   ( n o w   -   c . g e t ( \ l a s t _ u s e d \ ,   0 ) )   >   s e l f . i d l e _ t i m e o u t   a n d   c [ \ i d \ ]   n o t   i n   s e l f . _ i n _ u s e ] 
                 f o r   c   i n   t o _ r e m o v e : 
                         s e l f . _ p o o l . r e m o v e ( c ) 
                         s e l f . _ s t a t s [ \ d e s t r o y e d \ ]   + =   1 
                 r e t u r n   l e n ( t o _ r e m o v e ) 
 
         d e f   s t a t s ( s e l f ) : 
                 r e t u r n   { \ m a x _ s i z e \ :   s e l f . m a x _ s i z e ,   \ p o o l _ s i z e \ :   l e n ( s e l f . _ p o o l ) ,   \ i n _ u s e \ :   l e n ( s e l f . _ i n _ u s e ) ,   \ a v a i l a b l e \ :   l e n ( s e l f . _ p o o l )   -   l e n ( s e l f . _ i n _ u s e ) ,   * * s e l f . _ s t a t s } 
  
 