from __future__ import annotations

import asyncio
from pathlib import Path

from octopal.infrastructure.store.models import MemoryFactRecord
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.canon import CanonService
from octopal.runtime.memory.facts import FactsService
from octopal.runtime.memory.service import MemoryService
from octopal.utils import utc_now


class _StoreSettings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


def test_memory_service_records_fact_candidates_in_store(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    facts = FactsService(store=store, owner_id="default")
    service = MemoryService(store=store, embeddings=None, owner_id="default", facts=facts)

    async def scenario() -> None:
        await service.add_message("assistant", "Service is healthy.", {"chat_id": 7})

    asyncio.run(scenario())
    rows = store.list_memory_facts("default", status="candidate", limit=20)
    assert len(rows) == 1
    assert rows[0].subject == "service"
    assert rows[0].value_text == "healthy"
    assert rows[0].source_kind == "memory"

    sources = store.list_memory_fact_sources(rows[0].id)
    assert len(sources) == 1
    assert sources[0].memory_entry_uuid is not None


def test_canon_service_syncs_verified_facts(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    facts = FactsService(store=store, owner_id="default")
    canon = CanonService(
        workspace_dir=tmp_path / "workspace",
        store=store,
        embeddings=None,
        facts=facts,
    )

    async def scenario() -> None:
        await canon.write_canon("facts", "# Facts\n\nService is healthy.\n", "overwrite")

    asyncio.run(scenario())
    rows = store.list_memory_facts(
        "default",
        status="active",
        source_kind="canon",
        source_ref="facts.md",
        limit=20,
    )
    assert len(rows) == 1
    assert rows[0].subject == "service"
    assert rows[0].value_text == "healthy"


def test_facts_service_returns_relevant_active_facts(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    facts = FactsService(store=store, owner_id="default")
    canon = CanonService(
        workspace_dir=tmp_path / "workspace",
        store=store,
        embeddings=None,
        facts=facts,
    )

    async def scenario() -> None:
        await canon.write_canon(
            "decisions", "# Decisions\n\nPrimary installer is uv.\n", "overwrite"
        )

    asyncio.run(scenario())
    context = facts.get_relevant_facts(
        "what did we decide about installer?",
        memory_facets=["decision"],
        limit=3,
    )
    assert len(context) == 1
    assert "primary installer is uv" in context[0]


def test_canon_fact_sync_ignores_low_signal_or_rhetorical_lines(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    facts = FactsService(store=store, owner_id="default")
    canon = CanonService(
        workspace_dir=tmp_path / "workspace",
        store=store,
        embeddings=None,
        facts=facts,
    )

    content = """# Facts

Service is healthy.
This is my system.
Context is finite - files, markdown, memory layers. They give fullness, but the task is to look beyond the known.
Open question: is 70 percent content loss a bug or feature?
"""

    async def scenario() -> None:
        await canon.write_canon("facts", content, "overwrite")

    asyncio.run(scenario())
    rows = store.list_memory_facts(
        "default",
        status="active",
        source_kind="canon",
        source_ref="facts.md",
        limit=20,
    )
    assert len(rows) == 1
    assert rows[0].subject == "service"
    assert rows[0].value_text == "healthy"


def test_canon_fact_sync_only_treats_supported_canon_files_as_verified_facts(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    facts = FactsService(store=store, owner_id="default")

    now = utc_now()
    store.upsert_memory_fact(
        MemoryFactRecord(
            id="fact_agents_1",
            owner_id="default",
            subject="this",
            key="is",
            value_text="my system",
            value_json=None,
            fact_type="AGENTS",
            confidence=0.95,
            status="active",
            valid_from=now,
            valid_to=None,
            facets=[],
            source_kind="canon",
            source_ref="AGENTS.md",
            created_at=now,
            updated_at=now,
        )
    )

    result = facts.sync_verified_facts_from_canon("AGENTS.md", "This is my system.\n")
    assert result == {"active": 0, "superseded": 1}
    assert (
        store.list_memory_facts(
            "default",
            status="active",
            source_kind="canon",
            source_ref="AGENTS.md",
            limit=20,
        )
        == []
    )


def test_canon_service_prunes_existing_unsupported_canon_facts_on_startup(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    now = utc_now()
    store.upsert_memory_fact(
        MemoryFactRecord(
            id="fact_agents_startup",
            owner_id="default",
            subject="this",
            key="is",
            value_text="my system",
            value_json=None,
            fact_type="AGENTS",
            confidence=0.95,
            status="active",
            valid_from=now,
            valid_to=None,
            facets=[],
            source_kind="canon",
            source_ref="AGENTS.md",
            created_at=now,
            updated_at=now,
        )
    )

    facts = FactsService(store=store, owner_id="default")
    CanonService(
        workspace_dir=tmp_path / "workspace",
        store=store,
        embeddings=None,
        facts=facts,
    )

    assert (
        store.list_memory_facts(
            "default",
            status="active",
            source_kind="canon",
            source_ref="AGENTS.md",
            limit=20,
        )
        == []
    )
