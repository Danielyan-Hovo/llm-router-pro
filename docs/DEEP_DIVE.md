## Detailed Implementation Deep Dive

### FastAPI Gateway Architecture
The gateway uses async/await patterns. Each provider has its own httpx.AsyncClient with connection pooling (Limits: max_connections=100, max_keepalive=20). Provider resolution examines model prefixes: "gpt-" routes to OpenAI, "claude-" to Anthropic, "local/" to vLLM/Ollama. Retry logic uses tenacity with exponential backoff (base=1, max=10, stop_after_attempt=3) and retries on HTTP 429, 500, 502, 503, 504.

### PostgreSQL Schema Design
The tenants table uses UUID primary keys (gen_random_uuid()). The api_key_hash stores SHA-256 hashes (not raw keys). Rate limits use a sliding window SQL query counting requests within the current minute. The request_logs table has composite indexes: (tenant_id, created_at) for billing queries, (provider, model) for analytics. Monthly budget tracking uses a SQL trigger summing cost_cents per tenant per calendar month.

### HMAC Authentication
The middleware reads the Authorization header, extracts the API key, and verifies HMAC-SHA256 signature of the request body. Signature = HMAC(key=api_key, message=request_body, digest=sha256). compare_digest() performs constant-time comparison preventing timing attacks. Invalid signatures return HTTP 401 with generic messages (no information leakage).

### Prometheus Metrics
Three metric types: Histogram (latency per provider/model with buckets [0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10] seconds), Counter (requests per provider/status, cost in cents per provider), Gauge (active connections per provider). The /metrics endpoint returns Prometheus text format. Metrics are scraped every 15 seconds and visualized in Grafana showing p50, p95, p99 latency, error rates, and cost trends.

### Circuit Breaker Pattern
pybreaker provides circuit breaker functionality. Each provider has its own breaker: fail_max=5 (opens after 5 failures), reset_timeout=60 (half-open after 60 seconds). When open, requests return HTTP 503 and route to fallback (local model). Breaker state exposed via /health endpoint returning JSON with provider status (healthy/degraded/down) and breaker state (closed/open/half-open).

### Docker Multi-Stage Build
Stage 1 (builder): installs dependencies, compiles C extensions. Stage 2 (runtime): python:3.12-slim image with only compiled artifacts and source. Image size reduced by ~60% compared to full Python image.

### Kubernetes Deployment
Deployment: 3 replicas, resource limits (512Mi memory, 500m CPU), secrets for DB/API keys. Service: LoadBalancer on port 80. HPA: autoscaling/v2, min 3, max 10, target CPU 70%, stabilization window 60 seconds.

### Architecture Decision Records
ADR-001: FastAPI over Flask (native async, OpenAPI docs, Pydantic). ADR-002: PostgreSQL over NoSQL (ACID billing, JSONB flexibility). ADR-003: HMAC-SHA256 over JWT (simpler rotation, no expiration). ADR-004: Prometheus (industry standard, K8s native). ADR-005: Circuit breaker (prevents cascading failures). ADR-006: Multi-stage Docker (security, size reduction).
