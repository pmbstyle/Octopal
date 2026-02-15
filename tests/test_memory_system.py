from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

from broodmind.memory.canon import CanonService
from broodmind.store.models import MemoryEntry
from broodmind.store.sqlite import SQLiteStore
from broodmind.utils import utc_now


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
