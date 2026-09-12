# LLM Router Pro — Production Deployment Guide

## Prerequisites
- Kubernetes 1.28+ (K3s or full cluster)
- PostgreSQL 15+ with pgvector extension
- Docker registry access
- Keycloak instance for RBAC

## Deployment Steps
1. Apply secrets: `kubectl apply -f k8s/secrets.yaml`
2. Apply ConfigMap: `kubectl apply -f k8s/configmap.yaml`
3. Apply database migrations: `kubectl run migrate --image=llm-router-pro --rm -i --restart=Never -- python -m migrations`
4. Apply deployment: `kubectl apply -f k8s/deployment.yaml`
5. Verify health: `kubectl get pods -n llm-router-pro`
6. Monitor metrics: `kubectl port-forward svc/prometheus 9090:9090`

## Security Checklist
- [ ] HMAC keys rotated every 90 days
- [ ] Rate limits configured per tenant
- [ ] Circuit breaker thresholds set
- [ ] Audit logging enabled
- [ ] TLS certificates valid
