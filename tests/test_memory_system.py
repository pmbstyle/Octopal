from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

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

    def add_canon_embedding(self, filename: str, chunk_index: int, content: str, model: str, vector: list[float]) -> None:
        return None

    def list_canon_embeddings(self, filename: str | None = None):
        return []


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
    store.add_memory_entry(_make_entry("user", "chat one-two-three", owner_id="default", chat_id=123))

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
