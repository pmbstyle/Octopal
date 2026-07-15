from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from octopal.infrastructure.store.models import IntentRecord
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.canon import CanonService
from octopal.runtime.memory.influence import normalize_memory_influence_ids
from octopal.runtime.octo.prompt_builder import build_octo_prompt
from octopal.runtime.octo.router import _handle_octo_tool_call
from octopal.tools.registry import ToolSpec


class _StoreSettings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


def _store(tmp_path: Path) -> SQLiteStore:
    return SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))


def test_memory_influence_ids_are_typed_bounded_and_content_free() -> None:
    values: list[object] = [
        "memory_entry:entry-1",
        "memory_entry:entry-1",
        "memory_fact:fact_2",
        "canon_event:canon_3",
        "octo_diary:diary-4",
        "operational_memory:omem-5",
        "raw prompt content",
        "memory_entry:contains spaces",
        None,
    ]

    assert normalize_memory_influence_ids(values) == [
        "memory_entry:entry-1",
        "memory_fact:fact_2",
        "canon_event:canon_3",
        "octo_diary:diary-4",
        "operational_memory:omem-5",
    ]
    assert (
        len(normalize_memory_influence_ids([f"memory_entry:item-{index}" for index in range(200)]))
        == 128
    )

    with pytest.raises(ValidationError):
        IntentRecord(
            id="intent-1",
            worker_id="octo",
            type="exec.run",
            payload={},
            payload_hash="hash",
            risk="high",
            requires_approval=True,
            memory_influence_ids=["raw prompt content"],
            status="pending",
            created_at=datetime.now(UTC),
        )


def test_canon_context_reports_only_events_represented_after_truncation(
    tmp_path: Path,
) -> None:
    canon = CanonService(
        workspace_dir=tmp_path / "workspace",
        store=_store(tmp_path),
        embeddings=None,
    )

    async def scenario() -> None:
        await canon.write_canon("decisions", "# Decisions\n\nold-only\n", "overwrite")
        await canon.write_canon("decisions", ("middle\n" * 500), "append")
        await canon.write_canon("decisions", "current-decision\n", "append")

    asyncio.run(scenario())
    events = [json.loads(line) for line in canon.events_file.read_text().splitlines()]
    writes = [event for event in events if event.get("event_type") == "write"]
    old_id, middle_id, current_id = [str(event["event_id"]) for event in writes[-3:]]

    context, selected_ids = canon.get_tier1_context_with_ids()

    assert "current-decision" in context
    assert f"canon_event:{old_id}" not in selected_ids
    assert f"canon_event:{middle_id}" in selected_ids
    assert f"canon_event:{current_id}" in selected_ids


def test_legacy_canon_context_gets_a_stable_synthetic_event_id(tmp_path: Path) -> None:
    canon = CanonService(
        workspace_dir=tmp_path / "workspace",
        store=_store(tmp_path),
        embeddings=None,
    )
    legacy_event = {
        "ts": datetime.now(UTC).isoformat(),
        "filename": "decisions.md",
        "mode": "overwrite",
        "content": "# Decisions\n\nLegacy choice is retained.\n",
    }
    canon.events_file.write_text(json.dumps(legacy_event) + "\n", encoding="utf-8")

    first_context, first_ids = canon.get_tier1_context_with_ids()
    second_context, second_ids = canon.get_tier1_context_with_ids()

    assert "Legacy choice is retained." in first_context
    assert first_context == second_context
    assert first_ids == second_ids
    assert len(first_ids) == 1
    assert first_ids[0].startswith("canon_event:legacy_")


def test_canon_context_bounds_content_to_its_recorded_event_ids(tmp_path: Path) -> None:
    canon = CanonService(
        workspace_dir=tmp_path / "workspace",
        store=_store(tmp_path),
        embeddings=None,
    )

    async def scenario() -> None:
        await canon.write_canon("decisions", "# Decisions\n", "overwrite")
        for index in range(40):
            await canon.write_canon("decisions", f"decision-{index:02d}\n", "append")

    asyncio.run(scenario())
    context, selected_ids = canon.get_tier1_context_with_ids()

    assert len(selected_ids) == 32
    assert "decision-00" not in context
    assert "decision-07" not in context
    assert "decision-08" in context
    assert "decision-39" in context


def test_prompt_collects_only_recent_history_left_after_pruning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_entry = SimpleNamespace(
        id="old-entry",
        role="assistant",
        content="a" * 1600,
        created_at=datetime.now(UTC),
    )
    recent_entry = SimpleNamespace(
        id="recent-entry",
        role="assistant",
        content="b" * 1600,
        created_at=datetime.now(UTC),
    )

    class _Memory:
        async def get_context_entries_by_facets(self, *args, **kwargs):
            return []

        async def get_recent_history_entries(self, *args, **kwargs):
            return [old_entry, recent_entry]

    class _Canon:
        def get_tier1_context_with_ids(self):
            return "", []

    monkeypatch.setenv("OCTOPAL_CONTEXT_PRUNE_MAX_HISTORY_CHARS", "2000")
    monkeypatch.setenv("OCTOPAL_CONTEXT_PRUNE_KEEP_RECENT", "1")
    selected_ids: list[str] = []

    async def scenario() -> None:
        await build_octo_prompt(
            store=object(),
            memory=_Memory(),
            canon=_Canon(),
            user_text="continue",
            chat_id=7,
            bootstrap_context="",
            memory_influence_ids=selected_ids,
        )

    asyncio.run(scenario())
    assert selected_ids == ["memory_entry:recent-entry"]


def test_prompt_collects_canon_fact_and_semantic_memory_ids() -> None:
    semantic_entry = SimpleNamespace(
        id="semantic-entry",
        role="assistant",
        content="Use the narrow deployment path.",
    )
    fact_record = SimpleNamespace(
        id="fact-1",
        subject="deployment",
        key="is",
        value_text="bounded",
        source_ref="decisions.md",
    )

    class _Memory:
        async def get_context_entries_by_facets(self, *args, **kwargs):
            return [semantic_entry]

        async def get_recent_history_entries(self, *args, **kwargs):
            return []

    class _Canon:
        def get_tier1_context_with_ids(self):
            return "<canon_decisions>bounded deploy</canon_decisions>", ["canon_event:canon-1"]

    class _Facts:
        def get_relevant_fact_records(self, *args, **kwargs):
            return [fact_record]

    selected_ids: list[str] = []

    async def scenario() -> None:
        await build_octo_prompt(
            store=object(),
            memory=_Memory(),
            canon=_Canon(),
            facts=_Facts(),
            user_text="deploy",
            chat_id=7,
            bootstrap_context="",
            memory_influence_ids=selected_ids,
        )

    asyncio.run(scenario())
    assert selected_ids == [
        "canon_event:canon-1",
        "memory_fact:fact-1",
        "memory_entry:semantic-entry",
    ]


def test_intent_memory_influence_column_migrates_existing_database(tmp_path: Path) -> None:
    state_dir = tmp_path / "data"
    state_dir.mkdir(parents=True)
    database = sqlite3.connect(state_dir / "octopal.db")
    database.execute("""
        CREATE TABLE intents (
            id TEXT PRIMARY KEY,
            worker_id TEXT NOT NULL,
            type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            risk TEXT NOT NULL,
            requires_approval INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """)
    database.execute(
        "INSERT INTO intents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "legacy-intent",
            "worker-1",
            "http.get",
            "{}",
            "hash",
            "low",
            0,
            "approved",
            datetime.now(UTC).isoformat(),
        ),
    )
    database.commit()
    database.close()

    store = SQLiteStore(_StoreSettings(state_dir, tmp_path / "workspace"))
    row = store._conn.execute(
        "SELECT memory_influence_ids_json FROM intents WHERE id = 'legacy-intent'"
    ).fetchone()

    assert row is not None
    assert json.loads(row["memory_influence_ids_json"]) == []


@pytest.mark.asyncio
async def test_sensitive_octo_intent_persists_memory_ids_outside_payload(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)

    class _Octo:
        def __init__(self) -> None:
            self.store = store

    approval_calls: list[object] = []
    handler_calls: list[dict[str, object]] = []

    async def requester(intent: object) -> bool:
        approval_calls.append(intent)
        return True

    tool = ToolSpec(
        name="exec_run",
        description="exec",
        parameters={"type": "object"},
        permission="exec",
        handler=lambda args, _ctx: handler_calls.append(args) or {"ok": True},
    )
    result, meta = await _handle_octo_tool_call(
        {"function": {"name": "exec_run", "arguments": '{"command":"sudo true"}'}},
        [tool],
        {
            "octo": _Octo(),
            "approval_requester": requester,
            "memory_influence_ids": [
                "memory_fact:fact-1",
                "memory_entry:entry-2",
            ],
        },
    )

    assert result == {"ok": True}
    assert meta["had_error"] is False
    assert len(approval_calls) == 1
    assert handler_calls == [{"command": "sudo true"}]
    row = store._conn.execute(
        "SELECT payload_json, payload_hash, memory_influence_ids_json, status FROM intents"
    ).fetchone()
    assert row is not None
    assert json.loads(row["memory_influence_ids_json"]) == [
        "memory_fact:fact-1",
        "memory_entry:entry-2",
    ]
    assert "memory_fact" not in row["payload_json"]
    assert row["status"] == "approved"


@pytest.mark.asyncio
async def test_sensitive_octo_intent_fails_closed_when_provenance_cannot_persist() -> None:
    class _BrokenStore:
        def save_intent(self, _record: object) -> None:
            raise RuntimeError("database unavailable")

    class _Octo:
        store = _BrokenStore()

    approval_calls: list[object] = []
    handler_calls: list[object] = []

    async def requester(intent: object) -> bool:
        approval_calls.append(intent)
        return True

    tool = ToolSpec(
        name="exec_run",
        description="exec",
        parameters={"type": "object"},
        permission="exec",
        handler=lambda args, _ctx: handler_calls.append(args) or {"ok": True},
    )
    result, meta = await _handle_octo_tool_call(
        {"function": {"name": "exec_run", "arguments": '{"command":"sudo true"}'}},
        [tool],
        {"octo": _Octo(), "approval_requester": requester},
    )

    assert result["type"] == "policy_denied"
    assert result["reason"] == "sensitive intent provenance could not be persisted"
    assert meta["had_error"] is True
    assert meta["error_type"] == "policy_denied"
    assert approval_calls == []
    assert handler_calls == []
