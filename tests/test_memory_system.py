from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from octopal.infrastructure.store.models import MemoryEntry
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.canon import CanonService
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
