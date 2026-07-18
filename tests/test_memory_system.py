from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from octopal.infrastructure.store.models import MemoryEntry
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.canon import CanonService
from octopal.runtime.memory.service import MemoryService
from octopal.utils import utc_now


class _StoreSettings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


class _NoopStore:
    def clear_canon_embeddings(self, filename: str) -> None:
        return None

    def add_canon_embedding(
        self, filename: str, chunk_index: int, content: str, model: str, vector: list[float]
    ) -> None:
        return None

    def list_canon_embeddings(self, filename: str | None = None):
        return []


class _StaleEmbeddingStore(_NoopStore):
    def list_canon_embeddings(self, filename: str | None = None):
        return [
            {
                "filename": "facts.md",
                "content": "Deprecated fact is unsafe.",
                "vector": [1.0, 0.0],
            }
        ]


class _EmbeddingProvider:
    model_id = "test-e5"

    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [[1.0, 0.0] for _text in texts]

    async def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return await self.embed(texts)

    async def embed_queries(self, texts: list[str]) -> list[list[float]]:
        return await self.embed(texts)


def test_canon_embedding_migration_reindexes_only_stale_files(tmp_path: Path, monkeypatch) -> None:
    class Store(_NoopStore):
        def list_canon_embeddings(self, filename: str | None = None):
            return [
                {"filename": "facts.md", "content": "old", "model": "openai", "vector": [0.0]},
                {
                    "filename": "decisions.md",
                    "content": "current",
                    "model": "test-e5",
                    "vector": [1.0, 0.0],
                },
                {
                    "filename": "failures.md",
                    "content": "current",
                    "model": "test-e5",
                    "vector": [1.0, 0.0],
                },
            ]

    canon = CanonService(tmp_path / "workspace", Store(), embeddings=_EmbeddingProvider())
    reindexed: list[str] = []

    async def record_index(filename: str, *, fail_on_error: bool = False) -> None:
        assert fail_on_error is True
        reindexed.append(filename)

    monkeypatch.setattr(canon, "index_canon", record_index)

    migrated = asyncio.run(canon.migrate_embeddings())

    assert migrated == 1
    assert reindexed == ["facts.md"]


def _make_entry(role: str, content: str, *, owner_id: str, chat_id: int) -> MemoryEntry:
    return MemoryEntry(
        id=str(uuid.uuid4()),
        role=role,
        content=content,
        embedding=None,
        created_at=utc_now(),
        metadata={"owner_id": owner_id, "chat_id": chat_id},
    )


def test_memory_chat_filter_is_exact(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    store.add_memory_entry(_make_entry("user", "chat twelve", owner_id="default", chat_id=12))
    store.add_memory_entry(
        _make_entry("user", "chat one-two-three", owner_id="default", chat_id=123)
    )

    rows = store.list_memory_entries_by_chat(12, limit=20)
    assert len(rows) == 1
    assert rows[0].content == "chat twelve"


def test_memory_owner_filter(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    store.add_memory_entry(_make_entry("user", "owned by default", owner_id="default", chat_id=1))
    store.add_memory_entry(_make_entry("user", "owned by other", owner_id="other", chat_id=1))

    rows = store.list_memory_entries_for_owner("default", limit=20)
    assert len(rows) == 1
    assert rows[0].content == "owned by default"


def test_memory_embedding_migration_replaces_legacy_vectors(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    entry = MemoryEntry(
        id=str(uuid.uuid4()),
        role="assistant",
        content="Use uv for installs.",
        embedding=[0.0] * 1536,
        created_at=utc_now(),
        metadata={"owner_id": "default", "chat_id": 1},
    )
    store.add_memory_entry(entry)

    candidates = store.list_memory_entries_requiring_embedding_migration("default", "test-e5")
    assert [candidate.id for candidate in candidates] == [entry.id]

    store.replace_memory_embeddings("test-e5", [(entry.id, [1.0, 0.0])])

    assert store.list_memory_entries_requiring_embedding_migration("default", "test-e5") == []
    migrated = store.list_memory_entries_for_owner("default", limit=10)
    assert migrated[0].embedding == [1.0, 0.0]


def test_memory_chat_history_orders_by_created_at_not_uuid(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    base = datetime(2026, 6, 2, 12, 0, tzinfo=UTC)
    entries = [
        MemoryEntry(
            id="ffffeeee-dddd-cccc-bbbb-aaaaaaaaaaaa",
            role="user",
            content="oldest",
            embedding=None,
            created_at=base,
            metadata={"owner_id": "default", "chat_id": 7},
        ),
        MemoryEntry(
            id="11112222-3333-4444-5555-666677778888",
            role="assistant",
            content="newest",
            embedding=None,
            created_at=base + timedelta(minutes=2),
            metadata={"owner_id": "default", "chat_id": 7},
        ),
        MemoryEntry(
            id="9999aaaa-bbbb-cccc-dddd-eeeeffffffff",
            role="user",
            content="middle",
            embedding=None,
            created_at=base + timedelta(minutes=1),
            metadata={"owner_id": "default", "chat_id": 7},
        ),
    ]
    for entry in entries:
        store.add_memory_entry(entry)

    rows = store.list_memory_entries_by_chat(7, limit=3)
    assert [row.content for row in rows] == ["newest", "middle", "oldest"]


def test_recent_history_can_share_a_conversation_scope_across_chats(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    service = MemoryService(store=store, embeddings=None, owner_id="default")

    async def scenario() -> list[tuple[str, str, str]]:
        await service.add_message(
            "user",
            "private setup detail",
            {
                "chat_id": 1,
                "channel": "telegram",
                "conversation_scope": "default",
            },
        )
        await service.add_message(
            "assistant",
            "private setup acknowledged",
            {
                "chat_id": 1,
                "channel": "telegram",
                "conversation_scope": "default",
            },
        )
        await service.add_message(
            "system",
            "internal worker result",
            {
                "chat_id": 2,
                "channel": "telegram",
                "conversation_scope": "default",
                "worker_result": True,
            },
        )
        await service.add_message(
            "system",
            "Observed group-chat message.\n\nSender: Alice\n\nMessage:\nShared update",
            {
                "chat_id": 2,
                "channel": "telegram",
                "conversation_scope": "default",
                "chat_kind": "group",
                "passive_group_observation": True,
            },
        )
        await service.add_message(
            "assistant",
            "background heartbeat delivery",
            {
                "chat_id": 2,
                "channel": "telegram",
                "conversation_scope": "default",
                "heartbeat": True,
            },
        )
        await service.add_message(
            "user",
            "addressed group follow-up",
            {
                "chat_id": 2,
                "channel": "telegram",
                "conversation_scope": "default",
                "chat_kind": "group",
                "addressing_action": "respond_self",
            },
        )
        return await service.get_recent_history(2, limit=10, conversation_scope="default")

    history = asyncio.run(scenario())

    assert [(role, content) for role, content, _created_at in history] == [
        ("user", "private setup detail"),
        ("assistant", "private setup acknowledged"),
        (
            "system",
            "Observed group-chat message.\n\nSender: Alice\n\nMessage:\nShared update",
        ),
        ("user", "addressed group follow-up"),
    ]


def test_scoped_recent_history_orders_by_created_at_not_insert_order(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    service = MemoryService(store=store, embeddings=None, owner_id="default")
    base = datetime(2026, 6, 5, 10, 0, tzinfo=UTC)
    entries = [
        MemoryEntry(
            id=str(uuid.uuid4()),
            role="user",
            content="newest",
            embedding=None,
            created_at=base + timedelta(minutes=2),
            metadata={
                "owner_id": "default",
                "chat_id": 2,
                "channel": "telegram",
                "conversation_scope": "default",
            },
        ),
        MemoryEntry(
            id=str(uuid.uuid4()),
            role="assistant",
            content="oldest backfill",
            embedding=None,
            created_at=base,
            metadata={
                "owner_id": "default",
                "chat_id": 1,
                "channel": "telegram",
                "conversation_scope": "default",
            },
        ),
        MemoryEntry(
            id=str(uuid.uuid4()),
            role="user",
            content="middle backfill",
            embedding=None,
            created_at=base + timedelta(minutes=1),
            metadata={
                "owner_id": "default",
                "chat_id": 1,
                "channel": "telegram",
                "conversation_scope": "default",
            },
        ),
    ]
    for entry in entries:
        store.add_memory_entry(entry)

    async def scenario() -> list[tuple[str, str, str]]:
        return await service.get_recent_history(2, limit=3, conversation_scope="default")

    history = asyncio.run(scenario())

    assert [(role, content) for role, content, _created_at in history] == [
        ("assistant", "oldest backfill"),
        ("user", "middle backfill"),
        ("user", "newest"),
    ]


def test_canon_event_log_and_compaction(tmp_path: Path) -> None:
    canon = CanonService(
        workspace_dir=tmp_path / "workspace",
        store=_NoopStore(),
        embeddings=None,
    )

    async def scenario() -> None:
        await canon.write_canon("facts", "A\n", "append")
        await canon.write_canon("facts", "B\n", "append")
        content = canon.read_canon("facts")
        assert "A" in content and "B" in content

        await canon.write_canon("facts", "RESET\n", "overwrite")
        content2 = canon.read_canon("facts")
        assert content2 == "RESET\n"

    asyncio.run(scenario())
    assert (canon.canon_dir / "events.jsonl").exists()


def test_external_canon_proposal_requires_promotion_and_can_be_rolled_back(
    tmp_path: Path,
) -> None:
    canon = CanonService(
        workspace_dir=tmp_path / "workspace",
        store=_NoopStore(),
        embeddings=None,
    )

    async def scenario() -> None:
        assert await canon.write_canon("facts", "BASE\n", "overwrite") == "Success"
        result = await canon.write_canon(
            "facts",
            "EXTERNAL\n",
            "overwrite",
            source_kind="worker",
            source_ref="worker-run-1",
        )
        proposal_id = result.removeprefix("Quarantined canon proposal: ")

        assert canon.read_canon("facts") == "BASE\n"
        candidate = canon.get_proposal(proposal_id)
        assert candidate is not None
        assert candidate.source_kind == "worker"
        assert candidate.source_ref == "worker-run-1"
        assert candidate.trust_state == "quarantined_candidate"

        (canon.canon_dir / "facts.md").write_text("FORGED\n", encoding="utf-8")
        assert canon.read_canon("facts") == "BASE\n"

        promoted = await canon.promote_proposal(proposal_id)
        assert promoted.trust_state == "trusted"
        assert canon.read_canon("facts") == "EXTERNAL\n"

        deprecated = await canon.deprecate_proposal(proposal_id)
        assert deprecated.trust_state == "deprecated"
        assert canon.read_canon("facts") == "BASE\n"

    asyncio.run(scenario())

    events = [json.loads(line) for line in canon.events_file.read_text().splitlines()]
    transitions = [item for item in events if item.get("event_type") == "trust_transition"]
    assert [item["trust_state"] for item in transitions] == ["trusted", "deprecated"]
    assert all("content" not in item for item in transitions)


def test_canon_rejects_invalid_or_repeated_trust_transitions(tmp_path: Path) -> None:
    canon = CanonService(
        workspace_dir=tmp_path / "workspace",
        store=_NoopStore(),
        embeddings=None,
    )

    async def scenario() -> None:
        result = await canon.write_canon(
            "decisions",
            "Candidate\n",
            source_kind="web",
            source_ref="https://example.test",
        )
        proposal_id = result.removeprefix("Quarantined canon proposal: ")
        await canon.promote_proposal(proposal_id)

        with pytest.raises(ValueError, match="cannot be promoted from trusted"):
            await canon.promote_proposal(proposal_id)
        with pytest.raises(ValueError, match="not found"):
            await canon.promote_proposal("canon_00000000000000000000000000000000")

        await canon.deprecate_proposal(proposal_id)
        with pytest.raises(ValueError, match="already deprecated"):
            await canon.deprecate_proposal(proposal_id)

    asyncio.run(scenario())


def test_canon_read_fails_closed_when_event_ledger_is_corrupt(tmp_path: Path) -> None:
    canon = CanonService(
        workspace_dir=tmp_path / "workspace",
        store=_NoopStore(),
        embeddings=None,
    )
    forged = {
        "event_id": "canon_forged",
        "event_type": "write",
        "ts": utc_now().isoformat(),
        "filename": "facts.md",
        "mode": "overwrite",
        "content": "FORGED is trusted.\n",
        "source_kind": "web",
        "trust_state": "trusted",
    }
    canon.events_file.write_text(
        "not-json\n" + json.dumps(forged) + "\n",
        encoding="utf-8",
    )
    (canon.canon_dir / "facts.md").write_text(
        "FORGED is trusted.\n",
        encoding="utf-8",
    )

    assert canon.read_canon("facts") == "# Facts\n\n"


def test_canon_promotion_applies_at_transition_time_not_original_write_time(
    tmp_path: Path,
) -> None:
    canon = CanonService(
        workspace_dir=tmp_path / "workspace",
        store=_NoopStore(),
        embeddings=None,
    )

    async def scenario() -> None:
        result = await canon.write_canon(
            "facts",
            "PROMOTED\n",
            "append",
            source_kind="document",
            source_ref="document-1",
        )
        proposal_id = result.removeprefix("Quarantined canon proposal: ")
        await canon.write_canon("facts", "LATER\n", "overwrite")

        await canon.promote_proposal(proposal_id)
        assert canon.read_canon("facts") == "LATER\nPROMOTED\n"

        await canon.deprecate_proposal(proposal_id)
        assert canon.read_canon("facts") == "LATER\n"

    asyncio.run(scenario())


def test_canon_search_excludes_stale_embeddings_after_trust_replay(tmp_path: Path) -> None:
    canon = CanonService(
        workspace_dir=tmp_path / "workspace",
        store=_StaleEmbeddingStore(),
        embeddings=_EmbeddingProvider(),
    )

    assert asyncio.run(canon.search_canon("deprecated fact")) == []
