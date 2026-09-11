from prometheus_client import Histogram, Counter, generate_latest
from fastapi import Response

LATENCY = Histogram('llm_request_latency_seconds', 'Latency per provider', ['provider', 'model'])
REQUESTS = Counter('llm_requests_total', 'Total requests', ['provider', 'status'])
COST = Counter('llm_cost_cents_total', 'Total cost in cents', ['provider'])

async def metrics_endpoint():
    return Response(generate_latest(), media_type="text/plain")
