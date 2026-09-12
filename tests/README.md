# LLM Router Pro — Integration Tests

## Test Coverage
- Authentication: HMAC-SHA256 verification (valid, invalid, timing-safe)
- Metrics: Prometheus exporter (histogram, counter, endpoint)
- Gateway: Provider routing (OpenAI, Anthropic, local fallback)
- Database: Schema validation, migrations
- Security: Rate limiting, budget enforcement
- Performance: Latency benchmarks per provider

## Running Tests
```bash
pytest tests/ -v --cov=src --cov-report=html
```

## CI Integration
GitHub Actions runs tests on every PR. Coverage threshold: 80%.
