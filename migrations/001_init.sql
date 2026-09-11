CREATE TABLE tenants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    api_key_hash TEXT UNIQUE NOT NULL,
    rate_limit_rpm INTEGER DEFAULT 60,
    monthly_budget_cents INTEGER DEFAULT 10000,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE request_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID REFERENCES tenants(id),
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    tokens_in INTEGER,
    tokens_out INTEGER,
    cost_cents INTEGER,
    latency_ms INTEGER,
    created_at TIMESTAMPTZ DEFAULT now()
);
