import pybreaker

breaker = pybreaker.CircuitBreaker(fail_max=5, reset_timeout=60)

@breaker
async def call_provider(client, endpoint, payload):
    resp = await client.post(endpoint, json=payload)
    return resp.json()
