from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from pydantic import ValidationError

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
    assert rows[0].source_kind == "assistant_inference"
    assert rows[0].trust_state == "quarantined_candidate"

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
        source_kind="imported_canon",
        source_ref="facts.md",
        limit=20,
    )
    assert len(rows) == 1
    assert rows[0].subject == "service"
    assert rows[0].value_text == "healthy"
    assert rows[0].trust_state == "trusted"


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
        source_kind="imported_canon",
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
            trust_state="trusted",
            valid_from=now,
            valid_to=None,
            facets=[],
            source_kind="imported_canon",
            source_ref="AGENTS.md",
            created_at=now,
            updated_at=now,
        )
    )

    result = facts.sync_verified_facts_from_canon("AGENTS.md", "This is my system.\n")
    assert result == {"active": 0, "superseded": 1}
    superseded = store.list_memory_facts(
        "default",
        source_kind="imported_canon",
        source_ref="AGENTS.md",
        limit=20,
    )
    assert len(superseded) == 1
    assert superseded[0].status == "superseded"
    assert superseded[0].trust_state == "superseded"
    assert (
        store.list_memory_facts(
            "default",
            status="active",
            source_kind="imported_canon",
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
            trust_state="trusted",
            valid_from=now,
            valid_to=None,
            facets=[],
            source_kind="imported_canon",
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
            source_kind="imported_canon",
            source_ref="AGENTS.md",
            limit=20,
        )
        == []
    )


def test_direct_user_fact_candidate_is_observed_with_provenance(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    facts = FactsService(store=store, owner_id="default")
    service = MemoryService(store=store, embeddings=None, owner_id="default", facts=facts)

    async def scenario() -> None:
        await service.add_message(
            "user",
            "Primary installer is uv.",
            {"chat_id": 8, "fact_candidate": True},
        )

    asyncio.run(scenario())
    rows = store.list_memory_facts("default", status="candidate", limit=20)
    assert len(rows) == 1
    assert rows[0].source_kind == "direct_user"
    assert rows[0].trust_state == "observed"
    assert store.list_memory_fact_sources(rows[0].id)[0].source_note == (
        "memory_candidate:direct_user"
    )


def test_external_memory_origin_is_quarantined_and_not_retrieved(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    facts = FactsService(store=store, owner_id="default")
    service = MemoryService(store=store, embeddings=None, owner_id="default", facts=facts)

    async def scenario() -> None:
        await service.add_message(
            "system",
            "Deployment target is production.",
            {
                "chat_id": 9,
                "fact_candidate": True,
                "memory_origin": "web",
                "trust_state": "trusted",
            },
        )

    asyncio.run(scenario())
    rows = store.list_memory_facts("default", status="candidate", limit=20)
    assert len(rows) == 1
    assert rows[0].source_kind == "web"
    assert rows[0].trust_state == "quarantined_candidate"
    assert facts.get_relevant_facts("deployment target") == []


def test_memory_fact_model_rejects_external_direct_trust() -> None:
    now = utc_now()
    with pytest.raises(ValidationError, match="cannot directly create a trusted memory fact"):
        MemoryFactRecord(
            id="fact_web",
            owner_id="default",
            subject="deployment target",
            key="is",
            value_text="production",
            value_json=None,
            fact_type="assertion",
            confidence=0.9,
            status="active",
            trust_state="trusted",
            valid_from=now,
            valid_to=None,
            facets=[],
            source_kind="web",
            source_ref="https://example.test",
            created_at=now,
            updated_at=now,
        )


def test_sqlite_migrates_legacy_fact_origins_and_trust_states(tmp_path: Path) -> None:
    state_dir = tmp_path / "data"
    workspace_dir = tmp_path / "workspace"
    state_dir.mkdir()
    workspace_dir.mkdir()
    db_path = state_dir / "octopal.db"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE memory_facts (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            subject TEXT NOT NULL,
            key TEXT NOT NULL,
            value_text TEXT NOT NULL,
            value_json TEXT,
            fact_type TEXT NOT NULL,
            confidence REAL NOT NULL,
            status TEXT NOT NULL,
            valid_from TEXT,
            valid_to TEXT,
            facets_json TEXT NOT NULL,
            source_kind TEXT,
            source_ref TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """)
    now = utc_now().isoformat()
    conn.executemany(
        """
        INSERT INTO memory_facts (
            id, owner_id, subject, key, value_text, fact_type, confidence, status,
            facets_json, source_kind, source_ref, created_at, updated_at
        ) VALUES (?, 'default', 'service', 'is', 'healthy', 'assertion', 0.9, ?, '[]', ?, ?, ?, ?)
        """,
        [
            ("legacy_canon", "active", "canon", "facts.md", now, now),
            ("legacy_memory", "candidate", "memory", "entry-missing", now, now),
        ],
    )
    conn.commit()
    conn.close()

    store = SQLiteStore(_StoreSettings(state_dir, workspace_dir))
    canon = store.list_memory_facts("default", source_ref="facts.md", limit=10)[0]
    candidate = store.list_memory_facts("default", source_ref="entry-missing", limit=10)[0]

    assert canon.source_kind == "imported_canon"
    assert canon.trust_state == "trusted"
    assert candidate.source_kind == "assistant_inference"
    assert candidate.trust_state == "quarantined_candidate"

    reopened = SQLiteStore(_StoreSettings(state_dir, workspace_dir))
    assert (
        reopened.list_memory_facts("default", source_ref="facts.md", limit=10)[0].trust_state
        == "trusted"
    )
    assert (
        reopened.list_memory_facts("default", source_ref="entry-missing", limit=10)[0].trust_state
        == "quarantined_candidate"
    )


def test_promoted_canon_fact_retains_source_event_provenance(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    facts = FactsService(store=store, owner_id="default")
    canon = CanonService(
        workspace_dir=tmp_path / "workspace",
        store=store,
        embeddings=None,
        facts=facts,
    )

    async def scenario() -> str:
        result = await canon.write_canon(
            "facts",
            "Deployment target is production.\n",
            source_kind="worker",
            source_ref="worker-run-2",
        )
        proposal_id = result.removeprefix("Quarantined canon proposal: ")
        assert facts.get_relevant_facts("deployment target") == []
        await canon.promote_proposal(proposal_id)
        return proposal_id

    proposal_id = asyncio.run(scenario())
    rows = store.list_memory_facts(
        "default",
        status="active",
        source_kind="imported_canon",
        source_ref="facts.md",
        limit=20,
    )
    assert len(rows) == 1
    assert rows[0].trust_state == "trusted"
    sources = store.list_memory_fact_sources(rows[0].id)
    assert [source.source_note for source in sources] == [f"canon_event:{proposal_id}:worker"]

    asyncio.run(canon.deprecate_proposal(proposal_id))
    superseded = store.list_memory_facts(
        "default",
        source_kind="imported_canon",
        source_ref="facts.md",
        limit=20,
    )
    assert len(superseded) == 1
    assert superseded[0].trust_state == "superseded"
