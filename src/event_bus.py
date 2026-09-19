from __future__ import annotations

import time
from typing import Any, Optional, Dict, List, Callable, Union, Tuple
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field, asdict
from enum import StrEnum, auto


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
    """Production event bus for gateway operations with subscribers."""

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
