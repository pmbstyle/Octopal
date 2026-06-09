from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from octopal.infrastructure.providers.base import Message
from octopal.infrastructure.store.models import OperationalMemoryItemRecord
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.operational import OperationalMemoryService
from octopal.runtime.octo.router import _build_operational_memory_context
from octopal.tools.plans import get_plan_tools
from octopal.utils import utc_now


class _StoreSettings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


class _Provider:
    def __init__(self, response: dict) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []

    async def complete(self, messages: list[Message | dict], **kwargs: object) -> str:
        self.calls.append({"messages": messages, **kwargs})
        return json.dumps(self.response, ensure_ascii=False)


def _store(tmp_path: Path) -> SQLiteStore:
    return SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))


def _tool(name: str):
    return {tool.name: tool for tool in get_plan_tools()}[name]


def test_operational_memory_extractor_stores_model_semantics(tmp_path: Path) -> None:
    store = _store(tmp_path)
    provider = _Provider(
        {
            "items": [
                {
                    "kind": "assistant_commitment",
                    "statement": "Nika will inspect the failing deployment.",
                    "next_action": "Check runtime logs and report the root cause.",
                    "priority": 3,
                    "confidence": 0.91,
                    "requires_plan": True,
                    "evidence": ["見てから原因を伝えるね"],
                }
            ]
        }
    )
    service = OperationalMemoryService(store=store, provider=provider, owner_id="default")

    records = asyncio.run(
        service.extract_and_store_turn(
            chat_id=7,
            user_message="デプロイが変だ、見てくれる？",
            assistant_message="うん、見てから原因を伝えるね。",
            channel="telegram",
            conversation_scope="default",
            source_ref="turn-jp-1",
        )
    )

    assert len(records) == 1
    rows = store.list_operational_memory_items(
        "default",
        chat_id=7,
        statuses=["active"],
        limit=10,
    )
    assert len(rows) == 1
    assert rows[0].kind == "assistant_commitment"
    assert rows[0].next_action == "Check runtime logs and report the root cause."
    assert rows[0].metadata["requires_plan"] is True
    assert provider.calls[0]["response_format"]["type"] == "json_schema"


def test_operational_memory_context_lists_active_items(tmp_path: Path) -> None:
    store = _store(tmp_path)
    now = utc_now()
    store.upsert_operational_memory_item(
        OperationalMemoryItemRecord(
            id="omem-1",
            owner_id="default",
            chat_id=42,
            kind="blocker",
            statement="CI is blocked by a missing secret.",
            next_action="Ask the user to restore the secret before rerunning CI.",
            status="active",
            priority=2,
            confidence=0.8,
            source_kind="turn",
            source_ref="turn-1",
            created_at=now,
            updated_at=now,
        )
    )
    service = OperationalMemoryService(store=store, provider=_Provider({"items": []}))
    octo = SimpleNamespace(operational_memory=service)

    context = _build_operational_memory_context(octo, 42)

    assert "<operational_memory>" in context
    assert "CI is blocked" in context
    assert "commitment_ids" in context


def test_plan_tools_link_and_resolve_operational_memory(tmp_path: Path) -> None:
    store = _store(tmp_path)
    now = utc_now()
    store.upsert_operational_memory_item(
        OperationalMemoryItemRecord(
            id="omem-link",
            owner_id="default",
            chat_id=42,
            kind="assistant_commitment",
            statement="Nika will inspect CI.",
            next_action="Inspect CI and summarize blockers.",
            status="active",
            priority=3,
            confidence=0.9,
            source_kind="turn",
            source_ref="turn-2",
            created_at=now,
            updated_at=now,
        )
    )
    ctx = {"octo": SimpleNamespace(store=store), "chat_id": 42, "correlation_id": "turn-2"}
    created = json.loads(
        _tool("plan_create").handler(
            {
                "goal": "Inspect CI",
                "metadata": {"commitment_ids": ["omem-link"]},
                "steps": [{"id": "inspect", "kind": "tool", "title": "Inspect CI"}],
            },
            ctx,
        )
    )

    linked = store.list_operational_memory_items(
        "default",
        chat_id=42,
        statuses=["in_progress"],
        limit=10,
    )
    assert linked[0].plan_run_id == created["run_id"]

    updated = json.loads(
        _tool("plan_update_step").handler(
            {
                "run_id": created["run_id"],
                "step_id": "inspect",
                "status": "completed",
                "output": {"summary": "CI inspected"},
            },
            ctx,
        )
    )

    assert updated["snapshot"]["run"]["status"] == "completed"
    resolved = store.list_operational_memory_items(
        "default",
        chat_id=42,
        statuses=["satisfied"],
        limit=10,
    )
    assert len(resolved) == 1
    assert resolved[0].resolved_at is not None


def test_plan_tools_link_and_resolve_group_commitment_from_string_chat_id(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    now = utc_now()
    group_chat_id = -1001234567890
    store.upsert_operational_memory_item(
        OperationalMemoryItemRecord(
            id="omem-group",
            owner_id="default",
            chat_id=group_chat_id,
            kind="assistant_commitment",
            statement="Nika will inspect the group report.",
            next_action="Inspect the group report and summarize findings.",
            status="active",
            priority=3,
            confidence=0.9,
            source_kind="turn",
            source_ref="turn-group",
            created_at=now,
            updated_at=now,
        )
    )
    ctx = {
        "octo": SimpleNamespace(store=store),
        "chat_id": str(group_chat_id),
        "correlation_id": "turn-group",
    }

    created = json.loads(
        _tool("plan_create").handler(
            {
                "goal": "Inspect group report",
                "metadata": {"commitment_ids": ["omem-group"]},
                "steps": [{"id": "inspect", "kind": "tool", "title": "Inspect report"}],
            },
            ctx,
        )
    )
    active = json.loads(_tool("plan_status").handler({"active_only": True}, ctx))

    linked = store.list_operational_memory_items(
        "default",
        chat_id=group_chat_id,
        statuses=["in_progress"],
        limit=10,
    )
    assert created["snapshot"]["run"]["chat_id"] == group_chat_id
    assert active["count"] == 1
    assert active["plans"][0]["id"] == created["run_id"]
    assert linked[0].plan_run_id == created["run_id"]

    _tool("plan_update_step").handler(
        {
            "run_id": created["run_id"],
            "step_id": "inspect",
            "status": "completed",
            "output": {"summary": "Group report inspected"},
        },
        ctx,
    )

    resolved = store.list_operational_memory_items(
        "default",
        chat_id=group_chat_id,
        statuses=["satisfied"],
        limit=10,
    )
    assert len(resolved) == 1
    assert resolved[0].resolved_at is not None


def test_plan_tools_do_not_link_commitment_from_another_chat(tmp_path: Path) -> None:
    store = _store(tmp_path)
    now = utc_now()
    store.upsert_operational_memory_item(
        OperationalMemoryItemRecord(
            id="omem-other-chat",
            owner_id="default",
            chat_id=99,
            kind="assistant_commitment",
            statement="Nika will inspect another chat's CI.",
            next_action="Inspect CI for another chat.",
            status="active",
            priority=3,
            confidence=0.9,
            source_kind="turn",
            source_ref="turn-other",
            created_at=now,
            updated_at=now,
        )
    )
    ctx = {"octo": SimpleNamespace(store=store), "chat_id": 42, "correlation_id": "turn-42"}

    _tool("plan_create").handler(
        {
            "goal": "Inspect CI",
            "metadata": {"commitment_ids": ["omem-other-chat"]},
            "steps": [{"id": "inspect", "kind": "tool", "title": "Inspect CI"}],
        },
        ctx,
    )

    rows = store.list_operational_memory_items(
        "default",
        chat_id=99,
        statuses=["active"],
        limit=10,
    )
    assert len(rows) == 1
    assert rows[0].plan_run_id is None


def test_plan_tools_do_not_link_global_operational_memory_item(tmp_path: Path) -> None:
    store = _store(tmp_path)
    now = utc_now()
    store.upsert_operational_memory_item(
        OperationalMemoryItemRecord(
            id="omem-global",
            owner_id="default",
            chat_id=None,
            kind="assistant_commitment",
            statement="Nika will maintain a global rollout checklist.",
            next_action="Keep the rollout checklist current.",
            status="active",
            priority=3,
            confidence=0.9,
            source_kind="turn",
            source_ref="turn-global",
            created_at=now,
            updated_at=now,
        )
    )
    ctx = {"octo": SimpleNamespace(store=store), "chat_id": 42, "correlation_id": "turn-42"}

    _tool("plan_create").handler(
        {
            "goal": "Inspect CI",
            "metadata": {"commitment_ids": ["omem-global"]},
            "steps": [{"id": "inspect", "kind": "tool", "title": "Inspect CI"}],
        },
        ctx,
    )

    rows = store.list_operational_memory_items(
        "default",
        chat_id=42,
        statuses=["active"],
        limit=10,
    )
    assert len(rows) == 1
    assert rows[0].id == "omem-global"
    assert rows[0].chat_id is None
    assert rows[0].plan_run_id is None
