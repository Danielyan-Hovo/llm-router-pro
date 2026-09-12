from prometheus_client import Counter, Histogram, generate_latest
from fastapi import Response

LATENCY = Histogram('llm_request_latency_seconds', 'Latency per provider', ['provider', 'model'])
REQUESTS = Counter('llm_requests_total', 'Total requests', ['provider', 'status'])
COST = Counter('llm_cost_cents_total', 'Total cost in cents', ['provider'])


def record_success(provider: str, model: str, latency: float, cost_cents: float) -> None:
    LATENCY.labels(provider=provider, model=model).observe(latency)
    REQUESTS.labels(provider=provider, status="success").inc()
    COST.labels(provider=provider).inc(cost_cents)


def record_failure(provider: str, status: str) -> None:
    REQUESTS.labels(provider=provider, status=status).inc()

async def metrics_endpoint():
    return Response(generate_latest(), media_type="text/plain")
