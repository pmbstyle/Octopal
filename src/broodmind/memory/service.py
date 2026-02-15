from __future__ import annotations

import asyncio
import math
import uuid
from dataclasses import dataclass
from typing import Any

from broodmind.providers.embeddings import EmbeddingsProvider
from broodmind.store.base import Store
from broodmind.store.models import MemoryEntry
from broodmind.utils import utc_now


@dataclass
class MemoryService:
    store: Store
    embeddings: EmbeddingsProvider | None
    owner_id: str = "default"
    top_k: int = 5
    prefilter_k: int = 80
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
            try:
                vectors = await self.embeddings.embed([trimmed])
                embedding = vectors[0] if vectors else None
            except Exception:
                embedding = None

        merged_metadata = dict(metadata or {})
        merged_metadata.setdefault("owner_id", self.owner_id)
        entry = MemoryEntry(
            id=str(uuid.uuid4()),
            role=role,
            content=trimmed,
            embedding=embedding,
            created_at=utc_now(),
            metadata=merged_metadata,
        )
        await asyncio.to_thread(self.store.add_memory_entry, entry)

    async def get_context(self, query: str, exclude_chat_id: int | None = None) -> list[str]:
        if self.embeddings is None:
            return []
        trimmed = query.strip()
        if not trimmed:
            return []
        try:
            vectors = await self.embeddings.embed([trimmed])
        except Exception:
            return []
        if not vectors:
            return []
        query_embedding = vectors[0]
        candidates = await asyncio.to_thread(
            self.store.search_memory_entries_lexical,
            self.owner_id,
            trimmed,
            self.prefilter_k,
            exclude_chat_id,
        )
        if not candidates:
            candidates = await asyncio.to_thread(
                self.store.list_memory_entries_for_owner,
                self.owner_id,
                max(self.prefilter_k, 200),
            )
        scored: list[tuple[float, MemoryEntry]] = []
        for entry in candidates:
            if not entry.embedding:
                continue
            
            # Skip entries from the current chat to avoid duplication with recent history
            if exclude_chat_id is not None:
                entry_chat_id = entry.metadata.get("chat_id")
                if entry_chat_id == exclude_chat_id:
                    continue

            score = _cosine_similarity(query_embedding, entry.embedding)
            if score >= self.min_score:
                scored.append((score, entry))
        scored.sort(key=lambda item: item[0], reverse=True)
        top = scored[: self.top_k]
        return [f"{entry.role}: {entry.content}" for _, entry in top]

    async def get_recent_history(self, chat_id: int, limit: int = 6) -> list[tuple[str, str]]:
        entries = await asyncio.to_thread(
            self.store.list_memory_entries_by_chat, chat_id, limit=limit
        )
        entries.reverse()
        return [(entry.role, entry.content) for entry in entries]


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)
