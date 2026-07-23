from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from octopal.runtime.memory.influence import require_complete_memory_influence_ids

_EMBEDDING_MODEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,199}$")


class MemoryRetrievalTrace(BaseModel):
    """Content-free explanation of why one memory entry was selected."""

    model_config = ConfigDict(frozen=True, allow_inf_nan=False)

    memory_id: str
    rank: int = Field(ge=1, le=20)
    score: float = Field(ge=0.0, le=2.0)
    mode: Literal["hybrid", "semantic", "lexical"]
    semantic_similarity: float | None = Field(default=None, ge=0.0, le=1.0)
    semantic_rank: int | None = Field(default=None, ge=1)
    lexical_rank: int | None = Field(default=None, ge=1)
    quality_weight: float = Field(ge=0.0, le=1.0)
    embedding_model: str | None = Field(default=None, max_length=200)

    @field_validator("memory_id")
    @classmethod
    def validate_memory_id(cls, value: str) -> str:
        [normalized] = require_complete_memory_influence_ids([value])
        if not normalized.startswith("memory_entry:"):
            raise ValueError("retrieval trace must reference a memory entry")
        return normalized

    @field_validator("embedding_model")
    @classmethod
    def normalize_embedding_model(cls, value: str | None) -> str | None:
        normalized = str(value or "").strip()
        if not normalized:
            return None
        if not _EMBEDDING_MODEL_RE.fullmatch(normalized):
            raise ValueError("embedding model identifier is invalid")
        return normalized
