from __future__ import annotations

import httpx
import pybreaker


class ProviderCircuitOpen(RuntimeError):
    """Raised when a provider circuit is open."""


class AsyncCircuit:
    def __init__(self, fail_max: int = 5, reset_timeout: int = 60) -> None:
        self.breaker = pybreaker.CircuitBreaker(fail_max=fail_max, reset_timeout=reset_timeout)

    async def call(self, operation):
        if self.breaker.current_state == "open":
            raise ProviderCircuitOpen("provider circuit is open")
        try:
            result = await operation()
        except (httpx.HTTPError, TimeoutError) as exc:
            self.breaker._state_storage.increment_counter()
            if self.breaker.current_state == "open":
                raise ProviderCircuitOpen("provider circuit opened after failures") from exc
            raise
        else:
            self.breaker.close()
            return result


async def call_provider(provider_router, config, payload):
    circuit = getattr(provider_router, "circuits", {}).get(config.name)
    if circuit is None:
        circuit = AsyncCircuit()
        if not hasattr(provider_router, "circuits"):
            provider_router.circuits = {}
        provider_router.circuits[config.name] = circuit
    return await circuit.call(lambda: provider_router.request(config, payload))
