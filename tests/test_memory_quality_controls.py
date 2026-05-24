from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

from octopal.infrastructure.store.models import MemoryEntry
from octopal.runtime.memory.service import MemoryService
from octopal.utils import utc_now


class _StoreStub:
    def __init__(self) -> None:
        self.entries: list[MemoryEntry] = []

    def add_memory_entry(self, entry: MemoryEntry) -> None:
        self.entries.append(entry)

    def list_memory_entries_by_chat(self, chat_id: int, limit: int = 50) -> list[MemoryEntry]:
        rows = [e for e in self.entries if e.metadata.get("chat_id") == chat_id]
        return list(reversed(rows))[:limit]

    def search_memory_entries_lexical(
        self,
        owner_id: str,
        query: str,
        limit: int = 80,
        exclude_chat_id: int | None = None,
    ) -> list[MemoryEntry]:
        return self.entries[:limit]

    def list_memory_entries_for_owner(self, owner_id: str, limit: int = 200) -> list[MemoryEntry]:
        rows = [e for e in self.entries if e.metadata.get("owner_id") == owner_id]
        return rows[:limit]


class _EmbedStub:
    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [[1.0, 0.0] for _ in texts]


def test_memory_dedup_skips_exact_duplicate_in_chat() -> None:
    store = _StoreStub()
    service = MemoryService(store=store, embeddings=None, owner_id="default")

    async def scenario() -> None:
        await service.add_message("user", "Deploy now", {"chat_id": 7})
        await service.add_message("user", "  deploy   now  ", {"chat_id": 7})

    asyncio.run(scenario())
    assert len(store.entries) == 1


def test_memory_contradiction_is_flagged() -> None:
    store = _StoreStub()
    service = MemoryService(store=store, embeddings=None, owner_id="default")

    async def scenario() -> None:
        await service.add_message("assistant", "Service is healthy.", {"chat_id": 9})
        await service.add_message("assistant", "Service is not healthy.", {"chat_id": 9})

    asyncio.run(scenario())
    assert len(store.entries) == 2
    latest = store.entries[-1]
    assert latest.metadata.get("contradiction_detected") is True
    assert isinstance(latest.metadata.get("contradiction_with"), list)
    assert latest.metadata.get("confidence", 1.0) < 0.7


def test_memory_context_uses_confidence_weighting() -> None:
    store = _StoreStub()
    high_conf_old = MemoryEntry(
        id=str(uuid.uuid4()),
        role="assistant",
        content="High confidence old fact",
        embedding=[1.0, 0.0],
        created_at=utc_now() - timedelta(days=1),
        metadata={"owner_id": "default", "chat_id": 1, "confidence": 0.95},
    )
    low_conf_new = MemoryEntry(
        id=str(uuid.uuid4()),
        role="assistant",
        content="Low confidence new fact",
        embedding=[1.0, 0.0],
        created_at=utc_now(),
        metadata={"owner_id": "default", "chat_id": 2, "confidence": 0.2},
    )
    store.entries.extend([low_conf_new, high_conf_old])
    service = MemoryService(store=store, embeddings=_EmbedStub(), owner_id="default", top_k=1, min_score=0.05)

    async def scenario() -> list[str]:
        return await service.get_context("fact", exclude_chat_id=None)

    context = asyncio.run(scenario())
    assert len(context) == 1
    assert "High confidence old fact" in context[0]


def test_recent_history_excludes_internal_system_entries() -> None:
    store = _StoreStub()
    service = MemoryService(store=store, embeddings=None, owner_id="default")

    async def scenario() -> list[tuple[str, str, str]]:
        await service.add_message("user", "ага, давай", {"chat_id": 7})
        await service.add_message(
            "system",
            "Worker completed: unrelated scheduled report",
            {"chat_id": 7, "worker_result": True},
        )
        await service.add_message(
            "system",
            "Planner mode=execute; steps=4",
            {"chat_id": 7, "planner": True},
        )
        await service.add_message("assistant", "Шаг 6/40, ищет профиль.", {"chat_id": 7})
        return await service.get_recent_history(7, limit=10)

    history = asyncio.run(scenario())

    assert [(role, content) for role, content, _created_at in history] == [
        ("user", "ага, давай"),
        ("assistant", "Шаг 6/40, ищет профиль."),
    ]


def test_memory_adds_typed_enrichment_metadata() -> None:
    store = _StoreStub()
    service = MemoryService(store=store, embeddings=None, owner_id="default")

    async def scenario() -> None:
        await service.add_message(
            "assistant",
            "We decided to switch to uv because pip was too slow.",
            {"chat_id": 11},
        )

    asyncio.run(scenario())
    assert len(store.entries) == 1
    metadata = store.entries[0].metadata
    facets = metadata.get("memory_facets") or []
    assert "decision" in facets
    assert "fact_candidate" not in facets


def test_memory_adds_problem_emotional_and_fact_hints() -> None:
    store = _StoreStub()
    service = MemoryService(store=store, embeddings=None, owner_id="default")

    async def scenario() -> None:
        await service.add_message(
            "assistant",
            "Service is not healthy and I'm worried about the deploy.",
            {"chat_id": 12},
        )

    asyncio.run(scenario())
    metadata = store.entries[-1].metadata
    facets = metadata.get("memory_facets") or []
    assert "problem" in facets
    assert "emotional" in facets
    assert "fact_candidate" in facets
    assert metadata.get("fact_candidate") is True
    assert metadata.get("fact_subject_hint") == "service"
    assert metadata.get("fact_value_hint") == "healthy and i'm worried about the deploy"
    assert metadata.get("fact_negated") is True


def test_memory_preserves_explicit_enrichment_overrides() -> None:
    store = _StoreStub()
    service = MemoryService(store=store, embeddings=None, owner_id="default")

    async def scenario() -> None:
        await service.add_message(
            "assistant",
            "Service is healthy.",
            {
                "chat_id": 13,
                "memory_facets": ["custom_marker"],
                "fact_candidate": False,
            },
        )

    asyncio.run(scenario())
    metadata = store.entries[-1].metadata
    facets = metadata.get("memory_facets") or []
    assert "custom_marker" in facets
    assert "fact_candidate" not in facets
    assert metadata.get("fact_candidate") is False


def test_memory_context_prefers_matching_facets_when_available() -> None:
    store = _StoreStub()
    store.entries.extend(
        [
            MemoryEntry(
                id=str(uuid.uuid4()),
                role="assistant",
                content="We decided to use uv for installs.",
                embedding=[1.0, 0.0],
                created_at=utc_now(),
                metadata={"owner_id": "default", "chat_id": 21, "memory_facets": ["decision"]},
            ),
            MemoryEntry(
                id=str(uuid.uuid4()),
                role="assistant",
                content="The deploy is broken right now.",
                embedding=[1.0, 0.0],
                created_at=utc_now(),
                metadata={"owner_id": "default", "chat_id": 22, "memory_facets": ["problem"]},
            ),
        ]
    )
    service = MemoryService(store=store, embeddings=_EmbedStub(), owner_id="default", top_k=1, min_score=0.05)

    async def scenario() -> list[str]:
        return await service.get_context_by_facets(
            "why did we decide to use uv?",
            exclude_chat_id=None,
            memory_facets=["decision"],
        )

    context = asyncio.run(scenario())
    assert len(context) == 1
    assert "decided to use uv" in context[0]
