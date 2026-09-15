from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential


class ProviderError(RuntimeError):
    """Raised when a provider cannot produce a valid response."""


class ProviderUnavailable(ProviderError):
    """Raised when a provider is unavailable or timed out."""


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    base_url: str
    endpoint: str
    api_key_env: str | None
    timeout: float
    cost_per_million_input: float
    cost_per_million_output: float
    model_prefixes: tuple[str, ...]


PROVIDERS = (
    ProviderConfig(
        name="openai",
        base_url="https://api.openai.com",
        endpoint="/v1/chat/completions",
        api_key_env="OPENAI_API_KEY",
        timeout=30.0,
        cost_per_million_input=2.50,
        cost_per_million_output=10.00,
        model_prefixes=("gpt-", "o1", "o3"),
    ),
    ProviderConfig(
        name="anthropic",
        base_url="https://api.anthropic.com",
        endpoint="/v1/messages",
        api_key_env="ANTHROPIC_API_KEY",
        timeout=30.0,
        cost_per_million_input=3.00,
        cost_per_million_output=15.00,
        model_prefixes=("claude-",),
    ),
    ProviderConfig(
        name="local",
        base_url="http://localhost:8001",
        endpoint="/v1/chat/completions",
        api_key_env=None,
        timeout=60.0,
        cost_per_million_input=0.0,
        cost_per_million_output=0.0,
        model_prefixes=("local/", "llama", "mistral"),
    ),
)


class ProviderRouter:
    def __init__(self, configs: tuple[ProviderConfig, ...] = PROVIDERS) -> None:
        self.configs = {config.name: config for config in configs}
        self.clients = {
            config.name: httpx.AsyncClient(
                base_url=config.base_url,
                timeout=httpx.Timeout(config.timeout),
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            )
            for config in configs
        }

    def resolve(self, model: str, preferred_provider: str | None = None) -> ProviderConfig:
        if preferred_provider:
            if preferred_provider not in self.configs:
                raise ProviderError(f"Unknown provider: {preferred_provider}")
            return self.configs[preferred_provider]

        normalized = model.lower()
        # Policy-based routing: match model prefix, then select by lowest cost
        matches = [
            c for c in self.configs.values() if any(normalized.startswith(p) for p in c.model_prefixes)
        ]
        if not matches:
            raise ProviderError(f"No provider supports model: {model}")
        return min(matches, key=lambda c: c.cost_per_million_input + c.cost_per_million_output)

    def client(self, provider: str) -> httpx.AsyncClient:
        try:
            return self.clients[provider]
        except KeyError as exc:
            raise ProviderError(f"Unknown provider: {provider}") from exc

    async def close(self) -> None:
        await _gather_close(self.clients.values())

    async def request(
        self,
        config: ProviderConfig,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        client = self.client(config.name)
        request_headers = headers or {}
        retrying = AsyncRetrying(
            retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.2, min=0.2, max=2.0),
            reraise=True,
        )
        try:
            async for attempt in retrying:
                with attempt:
                    response = await client.post(config.endpoint, json=payload, headers=request_headers)
                    response.raise_for_status()
                    return response
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise ProviderUnavailable(config.name) from exc
        except httpx.HTTPStatusError as exc:
            raise ProviderError(f"{config.name} returned HTTP {exc.response.status_code}") from exc
        raise ProviderError(f"{config.name} request failed")


def estimate_cost(config: ProviderConfig, input_tokens: int, output_tokens: int) -> float:
    input_cost = input_tokens * config.cost_per_million_input / 1_000_000
    output_cost = output_tokens * config.cost_per_million_output / 1_000_000
    return round(input_cost + output_cost, 8)


async def _gather_close(clients: Any) -> None:
    for client in clients:
        await client.aclose()
