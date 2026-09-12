from typing import Any

from pydantic import BaseModel, Field, field_validator


class Message(BaseModel):
    role: str = Field(pattern="^(system|user|assistant|tool)$")
    content: str = Field(min_length=1)

class LLMRequest(BaseModel):
    model: str = Field(min_length=1, max_length=128)
    messages: list[Message] = Field(min_length=1)
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    max_tokens: int = Field(default=1000, ge=1, le=32_000)
    provider: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("model")
    @classmethod
    def normalize_model(cls, value: str) -> str:
        return value.strip()


class Usage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class LLMResponse(BaseModel):
    id: str
    provider: str
    model: str
    choices: list[dict[str, Any]]
    usage: Usage = Field(default_factory=Usage)
    cost: float = 0.0
