# LLM Router Pro — Load Testing Guide

## Load Test Scenarios
1. Single provider (OpenAI) — 100 concurrent requests
2. Multi-provider routing — 50 requests per provider
3. Circuit breaker activation — provider failure simulation
4. Rate limit enforcement — exceed RPM limits
5. Cost tracking accuracy — verify billing calculations

## Tools
- `locust` for Python load testing
- `k6` for Kubernetes-level load testing
- `prometheus` for metrics collection during load

## Metrics to Monitor
- p99 latency per provider
- Error rate (circuit breaker triggers)
- Cost per 1000 requests
- Database connection pool saturation
