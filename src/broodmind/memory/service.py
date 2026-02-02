from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
import uuid

from broodmind.providers.embeddings import EmbeddingsProvider
from broodmind.store.base import Store
from broodmind.store.models import MemoryEntry
from broodmind.utils import utc_now


@dataclass
class MemoryService:
    store: Store
    embeddings: EmbeddingsProvider | None
    top_k: int = 5
    min_score: float = 0.25
    max_chars: int = 2000

    async def add_message(
        self,
        role: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        trimmed = content.strip()
        if not trimmed:
            return
        if len(trimmed) > self.max_chars:
            trimmed = trimmed[: self.max_chars]
        embedding = None
        if self.embeddings is not None:
            vectors = await self.embeddings.embed([trimmed])
            embedding = vectors[0] if vectors else None
        entry = MemoryEntry(
            id=str(uuid.uuid4()),
            role=role,
            content=trimmed,
            embedding=embedding,
            created_at=utc_now(),
            metadata=metadata or {},
        )
        self.store.add_memory_entry(entry)

    async def get_context(self, query: str) -> list[str]:
        if self.embeddings is None:
            return []
        trimmed = query.strip()
        if not trimmed:
            return []
        vectors = await self.embeddings.embed([trimmed])
        if not vectors:
            return []
        query_embedding = vectors[0]
        candidates = self.store.list_memory_entries(limit=200)
        scored: list[tuple[float, MemoryEntry]] = []
        for entry in candidates:
            if not entry.embedding:
                continue
            score = _cosine_similarity(query_embedding, entry.embedding)
            if score >= self.min_score:
                scored.append((score, entry))
        scored.sort(key=lambda item: item[0], reverse=True)
        top = scored[: self.top_k]
        return [f"{entry.role}: {entry.content}" for _, entry in top]

    def get_recent_history(self, chat_id: int, limit: int = 6) -> list[tuple[str, str]]:
        entries = self.store.list_memory_entries_by_chat(chat_id, limit=limit)
        entries.reverse()
        return [(entry.role, entry.content) for entry in entries]


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)