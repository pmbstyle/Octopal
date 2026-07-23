from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

from octopal.infrastructure.store.models import MemoryEmbeddingCandidate, MemoryEntry
from octopal.runtime.memory.service import MemoryService
from octopal.utils import utc_now


class _StoreStub:
    def __init__(self) -> None:
        self.entries: list[MemoryEntry] = []
        self.hydrated_ids: list[str] = []

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

    def list_memory_embedding_candidates(
        self,
        owner_id: str,
        model: str,
        exclude_chat_id: int | None = None,
    ) -> list[MemoryEmbeddingCandidate]:
        return [
            MemoryEmbeddingCandidate(
                id=entry.id,
                embedding=entry.embedding,
                created_at=entry.created_at,
                metadata=entry.metadata,
            )
            for entry in self.entries
            if entry.metadata.get("owner_id") == owner_id
            and entry.metadata.get("embedding_model") == model
            and entry.embedding is not None
            and (exclude_chat_id is None or entry.metadata.get("chat_id") != exclude_chat_id)
        ]

    def list_memory_entries_by_ids(
        self,
        owner_id: str,
        entry_ids: list[str],
    ) -> list[MemoryEntry]:
        self.hydrated_ids.extend(entry_ids)
        entries_by_id = {
            entry.id: entry for entry in self.entries if entry.metadata.get("owner_id") == owner_id
        }
        return [entries_by_id[entry_id] for entry_id in entry_ids if entry_id in entries_by_id]


class _EmbedStub:
    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [[1.0, 0.0] for _ in texts]


class _PrefixAwareEmbedStub:
    model_id = "test-e5"

    def __init__(self) -> None:
        self.document_calls: list[list[str]] = []
        self.query_calls: list[list[str]] = []

    async def embed_documents(self, texts: list[str]) -> list[list[float]]:
        self.document_calls.append(texts)
        return [[1.0, 0.0] for _ in texts]

    async def embed_queries(self, texts: list[str]) -> list[list[float]]:
        self.query_calls.append(texts)
        return [[1.0, 0.0] for _ in texts]


class _UnavailableQueryEmbedStub(_PrefixAwareEmbedStub):
    async def embed_queries(self, texts: list[str]) -> list[list[float]]:
        raise RuntimeError("embedding runtime unavailable")


class _MigrationStore(_StoreStub):
    def list_memory_entries_requiring_embedding_migration(
        self, owner_id: str, model: str, limit: int = 100
    ) -> list[MemoryEntry]:
        return [
            entry
            for entry in self.entries
            if entry.metadata.get("owner_id") == owner_id
            and entry.metadata.get("embedding_model") != model
        ][:limit]

    def replace_memory_embeddings(
        self, model: str, embeddings: list[tuple[str, list[float]]]
    ) -> None:
        vectors = dict(embeddings)
        self.entries = [
            entry.model_copy(
                update={
                    "embedding": vectors.get(entry.id, entry.embedding),
                    "metadata": {
                        **entry.metadata,
                        **({"embedding_model": model} if entry.id in vectors else {}),
                    },
                }
            )
            for entry in self.entries
        ]


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
    service = MemoryService(
        store=store, embeddings=_EmbedStub(), owner_id="default", top_k=1, min_score=0.05
    )

    async def scenario() -> list[str]:
        return await service.get_context("fact", exclude_chat_id=None)

    context = asyncio.run(scenario())
    assert len(context) == 1
    assert "High confidence old fact" in context[0]


def test_memory_uses_document_and_query_embedding_contracts() -> None:
    store = _StoreStub()
    embeddings = _PrefixAwareEmbedStub()
    service = MemoryService(store=store, embeddings=embeddings, owner_id="default", min_score=0.05)

    async def scenario() -> list[str]:
        await service.add_message("assistant", "Use uv for installs.", {"chat_id": 7})
        return await service.get_context("What should install dependencies?", exclude_chat_id=None)

    context = asyncio.run(scenario())

    assert embeddings.document_calls == [["Use uv for installs."]]
    assert embeddings.query_calls == [["What should install dependencies?"]]
    assert store.entries[0].metadata["embedding_model"] == "test-e5"
    assert context == ["assistant: Use uv for installs."]


def test_hybrid_memory_recalls_semantic_match_outside_lexical_candidates() -> None:
    store = _StoreStub()
    lexical_decoy = MemoryEntry(
        id="lexical-decoy",
        role="assistant",
        content="Dependency status dashboard",
        embedding=[0.0, 1.0],
        created_at=utc_now(),
        metadata={
            "owner_id": "default",
            "chat_id": 1,
            "confidence": 0.9,
            "embedding_model": "test-e5",
        },
    )
    semantic_match = MemoryEntry(
        id="semantic-match",
        role="assistant",
        content="Use uv for Python package installation.",
        embedding=[1.0, 0.0],
        created_at=utc_now() - timedelta(days=1),
        metadata={
            "owner_id": "default",
            "chat_id": 2,
            "confidence": 0.9,
            "embedding_model": "test-e5",
        },
    )
    store.entries.extend([lexical_decoy, semantic_match])

    def lexical_only(
        owner_id: str,
        query: str,
        limit: int = 80,
        exclude_chat_id: int | None = None,
    ) -> list[MemoryEntry]:
        return [lexical_decoy]

    store.search_memory_entries_lexical = lexical_only  # type: ignore[method-assign]
    service = MemoryService(
        store=store,
        embeddings=_PrefixAwareEmbedStub(),
        owner_id="default",
        top_k=1,
        min_score=0.05,
    )

    for query in (
        "How should dependencies be installed?",
        "Как устанавливать зависимости проекта?",
    ):
        retrievals = asyncio.run(service.get_context_retrievals_by_facets(query))
        assert [retrieval.entry.id for retrieval in retrievals] == ["semantic-match"]
        trace = retrievals[0].to_trace(rank=1)
        assert trace.memory_id == "memory_entry:semantic-match"
        assert trace.mode == "semantic"
        assert trace.semantic_similarity == 1.0
        assert trace.semantic_rank == 1
        assert trace.lexical_rank is None


def test_memory_uses_lexical_fallback_when_query_embeddings_are_unavailable() -> None:
    store = _StoreStub()
    unrelated_match = MemoryEntry(
        id="unrelated-match",
        role="assistant",
        content="The deployment is currently broken.",
        embedding=None,
        created_at=utc_now(),
        metadata={
            "owner_id": "default",
            "chat_id": 2,
            "confidence": 1.0,
            "memory_facets": ["problem"],
        },
    )
    lexical_match = MemoryEntry(
        id="lexical-match",
        role="assistant",
        content="Use uv for installs.",
        embedding=None,
        created_at=utc_now(),
        metadata={
            "owner_id": "default",
            "chat_id": 1,
            "confidence": 0.9,
            "memory_facets": ["decision"],
        },
    )
    store.entries.extend([unrelated_match, lexical_match])
    service = MemoryService(
        store=store,
        embeddings=_UnavailableQueryEmbedStub(),
        owner_id="default",
        top_k=1,
    )

    retrievals = asyncio.run(
        service.get_context_retrievals_by_facets(
            "Use uv",
            memory_facets=["decision"],
        )
    )

    assert [retrieval.entry.id for retrieval in retrievals] == ["lexical-match"]
    trace = retrievals[0].to_trace(rank=1)
    assert trace.mode == "lexical"
    assert trace.semantic_similarity is None
    assert trace.lexical_rank == 1
    assert trace.embedding_model is None


def test_semantic_min_score_applies_after_memory_quality_weighting() -> None:
    store = _StoreStub()
    low_quality_match = MemoryEntry(
        id="low-quality-semantic",
        role="assistant",
        content="Untrusted old guidance.",
        embedding=[1.0, 0.0],
        created_at=utc_now() - timedelta(days=1),
        metadata={
            "owner_id": "default",
            "chat_id": 1,
            "confidence": 0.1,
            "embedding_model": "test-e5",
        },
    )
    store.entries.append(low_quality_match)
    store.search_memory_entries_lexical = lambda *args, **kwargs: []  # type: ignore[method-assign]
    service = MemoryService(
        store=store,
        embeddings=_PrefixAwareEmbedStub(),
        owner_id="default",
        min_score=0.25,
    )

    retrievals = asyncio.run(service.get_context_retrievals_by_facets("guidance"))

    assert retrievals == []


def test_memory_applies_facet_preference_across_lexical_and_semantic_sources() -> None:
    store = _StoreStub()
    decision = MemoryEntry(
        id="decision",
        role="assistant",
        content="We decided to keep the local runtime.",
        embedding=None,
        created_at=utc_now(),
        metadata={
            "owner_id": "default",
            "chat_id": 1,
            "confidence": 0.9,
            "memory_facets": ["decision"],
        },
    )
    unrelated_semantic = MemoryEntry(
        id="problem",
        role="assistant",
        content="The remote runtime is unavailable.",
        embedding=[1.0, 0.0],
        created_at=utc_now(),
        metadata={
            "owner_id": "default",
            "chat_id": 2,
            "confidence": 1.0,
            "memory_facets": ["problem"],
            "embedding_model": "test-e5",
        },
    )
    store.entries.extend([decision, unrelated_semantic])
    store.search_memory_entries_lexical = (  # type: ignore[method-assign]
        lambda *args, **kwargs: [decision]
    )
    service = MemoryService(
        store=store,
        embeddings=_PrefixAwareEmbedStub(),
        owner_id="default",
        top_k=1,
        min_score=0.25,
    )

    retrievals = asyncio.run(
        service.get_context_retrievals_by_facets(
            "Why did we decide on the runtime?",
            memory_facets=["decision"],
        )
    )

    assert [retrieval.entry.id for retrieval in retrievals] == ["decision"]
    assert retrievals[0].mode == "lexical"


def test_semantic_retrieval_hydrates_only_prefiltered_top_ids() -> None:
    store = _StoreStub()
    for entry_id, embedding in (
        ("best", [1.0, 0.0]),
        ("second", [0.8, 0.2]),
        ("third", [0.0, 1.0]),
    ):
        store.entries.append(
            MemoryEntry(
                id=entry_id,
                role="assistant",
                content=f"content-{entry_id}",
                embedding=embedding,
                created_at=utc_now(),
                metadata={
                    "owner_id": "default",
                    "chat_id": 1,
                    "confidence": 1.0,
                    "embedding_model": "test-e5",
                },
            )
        )
    store.search_memory_entries_lexical = lambda *args, **kwargs: []  # type: ignore[method-assign]
    service = MemoryService(
        store=store,
        embeddings=_PrefixAwareEmbedStub(),
        owner_id="default",
        top_k=1,
        prefilter_k=1,
        min_score=0.25,
    )

    retrievals = asyncio.run(service.get_context_retrievals_by_facets("best"))

    assert [retrieval.entry.id for retrieval in retrievals] == ["best"]
    assert store.hydrated_ids == ["best"]


def test_memory_migrates_vectors_from_previous_embedding_model() -> None:
    store = _MigrationStore()
    store.entries.append(
        MemoryEntry(
            id=str(uuid.uuid4()),
            role="assistant",
            content="Use uv for installs.",
            embedding=[0.0] * 1536,
            created_at=utc_now(),
            metadata={"owner_id": "default", "embedding_model": "openai-text-embedding-3-small"},
        )
    )
    embeddings = _PrefixAwareEmbedStub()
    service = MemoryService(store=store, embeddings=embeddings, owner_id="default")

    migrated = asyncio.run(service.migrate_embeddings())

    assert migrated == 1
    assert embeddings.document_calls == [["Use uv for installs."]]
    assert store.entries[0].embedding == [1.0, 0.0]
    assert store.entries[0].metadata["embedding_model"] == "test-e5"


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
    service = MemoryService(
        store=store, embeddings=_EmbedStub(), owner_id="default", top_k=1, min_score=0.05
    )

    async def scenario() -> list[str]:
        return await service.get_context_by_facets(
            "why did we decide to use uv?",
            exclude_chat_id=None,
            memory_facets=["decision"],
        )

    context = asyncio.run(scenario())
    assert len(context) == 1
    assert "decided to use uv" in context[0]
