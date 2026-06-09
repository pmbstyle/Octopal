from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.octo.router import _build_runtime_plan_context
from octopal.tools.plans import get_plan_tools


class _StoreSettings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


def _store(tmp_path: Path) -> SQLiteStore:
    return SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))


def _tool(name: str):
    tools = {tool.name: tool for tool in get_plan_tools()}
    return tools[name]


def test_plan_tools_create_status_and_update(tmp_path: Path) -> None:
    store = _store(tmp_path)
    ctx = {"octo": SimpleNamespace(store=store), "chat_id": 42, "correlation_id": "turn-1"}

    created = json.loads(
        _tool("plan_create").handler(
            {
                "goal": "Check release readiness",
                "steps": [
                    {"id": "inspect", "kind": "tool", "title": "Inspect repo"},
                    {"id": "summarize", "kind": "final", "title": "Reply to user"},
                ],
            },
            ctx,
        )
    )

    assert created["status"] == "ok"
    run_id = created["run_id"]

    updated = json.loads(
        _tool("plan_update_step").handler(
            {
                "run_id": run_id,
                "step_id": "inspect",
                "status": "completed",
                "output": {"summary": "repo inspected"},
            },
            ctx,
        )
    )

    assert updated["status"] == "ok"
    assert updated["snapshot"]["run"]["status"] == "needs_next_step"
    assert updated["snapshot"]["run"]["current_step_id"] == "summarize"

    status = json.loads(_tool("plan_status").handler({"run_id": run_id}, ctx))
    assert status["snapshot"]["steps"][0]["output"] == {"summary": "repo inspected"}


def test_runtime_plan_context_is_compact_and_preserves_focus(tmp_path: Path) -> None:
    store = _store(tmp_path)
    ctx = {"octo": SimpleNamespace(store=store), "chat_id": 42}
    created = json.loads(
        _tool("plan_create").handler(
            {
                "goal": "Research a topic",
                "steps": [
                    {"id": "research", "kind": "worker", "title": "Research", "executor": "web"},
                    {"id": "reply", "kind": "final", "title": "Reply"},
                ],
            },
            ctx,
        )
    )

    context = _build_runtime_plan_context(SimpleNamespace(store=store), 42)

    assert "Runtime plan state is active" in context
    assert created["run_id"] in context
    assert "Research a topic" in context
    assert "without cancelling or overwriting" in context


def test_runtime_plan_context_includes_negative_group_chat_ids(tmp_path: Path) -> None:
    store = _store(tmp_path)
    group_chat_id = -1001234567890
    ctx = {"octo": SimpleNamespace(store=store), "chat_id": group_chat_id}
    created = json.loads(
        _tool("plan_create").handler(
            {
                "goal": "Follow up in the group",
                "steps": [
                    {"id": "collect", "kind": "worker", "title": "Collect details"},
                    {"id": "reply", "kind": "final", "title": "Reply to group"},
                ],
            },
            ctx,
        )
    )

    context = _build_runtime_plan_context(SimpleNamespace(store=store), group_chat_id)

    assert "Runtime plan state is active" in context
    assert created["run_id"] in context
    assert "Follow up in the group" in context


def test_plan_update_step_reports_unknown_step(tmp_path: Path) -> None:
    store = _store(tmp_path)
    ctx = {"octo": SimpleNamespace(store=store), "chat_id": 42}
    created = json.loads(
        _tool("plan_create").handler(
            {
                "goal": "Ship a fix",
                "steps": [{"id": "fix", "kind": "tool", "title": "Patch code"}],
            },
            ctx,
        )
    )

    updated = json.loads(
        _tool("plan_update_step").handler(
            {
                "run_id": created["run_id"],
                "step_id": "missing",
                "status": "completed",
            },
            ctx,
        )
    )

    assert updated == {
        "status": "not_found",
        "run_id": created["run_id"],
        "step_id": "missing",
    }


def test_plan_update_step_preserves_blocked_step_status(tmp_path: Path) -> None:
    store = _store(tmp_path)
    ctx = {"octo": SimpleNamespace(store=store), "chat_id": 42}
    created = json.loads(
        _tool("plan_create").handler(
            {
                "goal": "Ship a fix",
                "steps": [{"id": "review", "kind": "approval", "title": "Need approval"}],
            },
            ctx,
        )
    )

    updated = json.loads(
        _tool("plan_update_step").handler(
            {
                "run_id": created["run_id"],
                "step_id": "review",
                "status": "blocked",
                "error": "waiting on sign-off",
            },
            ctx,
        )
    )

    assert updated["status"] == "ok"
    assert updated["snapshot"]["run"]["status"] == "blocked"
    assert updated["snapshot"]["steps"][0]["status"] == "blocked"
    assert updated["snapshot"]["steps"][0]["error"] == "waiting on sign-off"
