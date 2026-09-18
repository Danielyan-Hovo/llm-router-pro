from __future__ import annotations

import os
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from src.auth import extract_signature, verify_signature
from src.circuit_breaker import ProviderCircuitOpen, call_provider
from src.metrics import metrics_endpoint, record_failure, record_success
from src.models import LLMRequest, LLMResponse, Usage
from src.providers import ProviderError, ProviderRouter, ProviderUnavailable, estimate_cost


router = APIRouter(prefix="/v1")
provider_router = ProviderRouter()


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    await provider_router.close()


app = FastAPI(title="LLM Router Pro", version="0.2.0", lifespan=lifespan)


@app.get("/healthz")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def ready() -> dict[str, str]:
    return {"status": "ready", "providers": str(len(provider_router.configs))}


@app.get("/metrics")
async def metrics() -> Response:
    return await metrics_endpoint()


@app.exception_handler(ProviderUnavailable)
async def provider_unavailable_handler(_: Request, exc: ProviderUnavailable) -> JSONResponse:
    return JSONResponse(status_code=503, content={"error": "provider_unavailable", "provider": str(exc)})


@app.exception_handler(ProviderError)
async def provider_error_handler(_: Request, exc: ProviderError) -> JSONResponse:
    return JSONResponse(status_code=502, content={"error": "provider_error", "detail": str(exc)})


@router.post("/chat/completions", response_model=LLMResponse)
async def route_llm(request: Request, payload: LLMRequest) -> LLMResponse:
    started = time.perf_counter()
    config = provider_router.resolve(payload.model, payload.provider)
    signature = extract_signature(dict(request.headers))
    signing_key = os.getenv("LLM_ROUTER_SIGNING_KEY")
    raw_body = await request.body()
    if signing_key and not verify_signature(raw_body, signing_key, signature or ""):
        raise HTTPException(status_code=401, detail="Invalid request signature")

    provider_payload = payload.model_dump(exclude={"provider", "metadata"})
    provider_payload["messages"] = [message.model_dump() for message in payload.messages]
    try:
        response = await call_provider(provider_router, config, provider_payload)
    except (ProviderUnavailable, ProviderError):
        record_failure(config.name, "error")
        raise

    elapsed = time.perf_counter() - started
    body = response.json()
    usage_data = body.get("usage", {})
    usage = Usage(
        prompt_tokens=usage_data.get("prompt_tokens", usage_data.get("input_tokens", 0)),
        completion_tokens=usage_data.get("completion_tokens", usage_data.get("output_tokens", 0)),
        total_tokens=usage_data.get("total_tokens", 0),
    )
    cost = estimate_cost(config, usage.prompt_tokens, usage.completion_tokens)
    record_success(config.name, payload.model, elapsed, cost * 100)
    return LLMResponse(
        id=body.get("id", f"router-{uuid.uuid4().hex}"),
        provider=config.name,
        model=body.get("model", payload.model),
        choices=body.get("choices", []),
        usage=usage,
        cost=cost,
    )


@router.get("/providers/health")
async def provider_health() -> dict[str, Any]:
    stats = provider_router.get_provider_stats()
    health = provider_router.health_check()
    return {"providers": stats, "health": health, "status": "ok" if all(health.values()) else "degraded"}


@router.get("/providers/stats")
async def provider_stats() -> dict[str, Any]:
    return {"stats": provider_router.get_provider_stats(), "status": "ok"}
