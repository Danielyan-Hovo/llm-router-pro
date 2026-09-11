from fastapi import FastAPI, APIRouter
import httpx

app = FastAPI(title="LLM Router Pro")
router = APIRouter(prefix="/v1")

clients = {
    "openai": httpx.AsyncClient(base_url="https://api.openai.com", timeout=30.0),
    "anthropic": httpx.AsyncClient(base_url="https://api.anthropic.com", timeout=30.0),
    "local": httpx.AsyncClient(base_url="http://localhost:8001", timeout=60.0),
}

@router.post("/chat/completions")
async def route_llm(request: dict):
    provider = "openai"  # resolve by model prefix
    resp = await clients[provider].post("/v1/chat/completions", json=request)
    return resp.json()

app.include_router(router)
