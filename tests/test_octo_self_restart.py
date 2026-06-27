from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.octo.core import Octo
from octopal.runtime.self_control import (
    SELF_RESTART_ACTION,
    SELF_RESTART_REQUESTED_BY,
    SELF_UPDATE_ACTION,
    SELF_UPDATE_REQUESTED_BY,
    append_control_ack,
    append_control_request,
    due_self_restart_requests,
    due_self_update_requests,
    find_recent_control_action,
    read_pending_restart_resume,
    run_update_helper,
)
from octopal.runtime.workers import runtime as worker_runtime
from octopal.tools.catalog import _tool_octo_restart_self, _tool_octo_update_self
from octopal.tools.workers import management as worker_management


class _StoreSettings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


class _Runtime:
    def __init__(self, settings: _StoreSettings) -> None:
        self.settings = settings


class _Memory:
    async def add_message(self, role: str, content: str, metadata: dict) -> None:
        return None


def test_self_restart_requires_confirmation(tmp_path: Path) -> None:
    settings = _StoreSettings(tmp_path / "data", tmp_path / "workspace")
    store = SQLiteStore(settings)
    octo = Octo(
        provider=object(),
        store=store,
        policy=object(),
        runtime=_Runtime(settings),
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )

    async def scenario() -> None:
        result = await octo.request_self_restart(42, {"reason": "reload settings"})
        assert result["status"] == "needs_confirmation"

    asyncio.run(scenario())
    assert not (settings.state_dir / "control_requests.jsonl").exists()


def test_self_restart_persists_handoff_resume_and_request(tmp_path: Path) -> None:
    settings = _StoreSettings(tmp_path / "data", tmp_path / "workspace")
    store = SQLiteStore(settings)
    octo = Octo(
        provider=object(),
        store=store,
        policy=object(),
        runtime=_Runtime(settings),
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )

    async def scenario() -> dict:
        return await octo.request_self_restart(
            42,
            {
                "reason": "reload authorized connectors",
                "goal_now": "Continue connector setup.",
                "next_step": "Verify connector status.",
                "confirm": True,
                "delay_seconds": 3,
            },
        )

    result = asyncio.run(scenario())
    assert result["status"] == "restart_requested"
    assert result["request"]["action"] == SELF_RESTART_ACTION
    assert result["request"]["requested_by"] == SELF_RESTART_REQUESTED_BY
    assert (settings.workspace_dir / "memory" / "handoff.json").exists()

    resume = read_pending_restart_resume(settings.state_dir)
    assert resume is not None
    assert resume["request_id"] == result["request"]["request_id"]
    assert resume["handoff"]["goal_now"] == "Continue connector setup."


def test_self_restart_rejects_recent_duplicate_request(tmp_path: Path) -> None:
    settings = _StoreSettings(tmp_path / "data", tmp_path / "workspace")
    store = SQLiteStore(settings)
    octo = Octo(
        provider=object(),
        store=store,
        policy=object(),
        runtime=_Runtime(settings),
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )

    async def scenario() -> tuple[dict, dict]:
        first = await octo.request_self_restart(
            42,
            {
                "reason": "apply git pull",
                "confirm": True,
                "delay_seconds": 3,
            },
        )
        second = await octo.request_self_restart(
            42,
            {
                "reason": "apply the same git pull again",
                "confirm": True,
                "delay_seconds": 3,
            },
        )
        return first, second

    first, second = asyncio.run(scenario())
    assert first["status"] == "restart_requested"
    assert second["status"] == "duplicate_recent_control_action"
    assert second["duplicate"]["request"]["request_id"] == first["request"]["request_id"]
    requests = (settings.state_dir / "control_requests.jsonl").read_text(encoding="utf-8")
    assert len(requests.splitlines()) == 1


def test_self_restart_force_allows_recent_duplicate_request(tmp_path: Path) -> None:
    settings = _StoreSettings(tmp_path / "data", tmp_path / "workspace")
    store = SQLiteStore(settings)
    octo = Octo(
        provider=object(),
        store=store,
        policy=object(),
        runtime=_Runtime(settings),
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )

    async def scenario() -> tuple[dict, dict]:
        first = await octo.request_self_restart(
            42,
            {
                "reason": "apply git pull",
                "confirm": True,
                "delay_seconds": 3,
            },
        )
        second = await octo.request_self_restart(
            42,
            {
                "reason": "explicit second restart",
                "confirm": True,
                "force": True,
                "delay_seconds": 3,
            },
        )
        return first, second

    first, second = asyncio.run(scenario())
    assert first["status"] == "restart_requested"
    assert second["status"] == "restart_requested"
    requests = (settings.state_dir / "control_requests.jsonl").read_text(encoding="utf-8")
    assert len(requests.splitlines()) == 2


def test_self_restart_tool_rejects_force_from_background_turn(tmp_path: Path) -> None:
    settings = _StoreSettings(tmp_path / "data", tmp_path / "workspace")
    store = SQLiteStore(settings)
    octo = Octo(
        provider=object(),
        store=store,
        policy=object(),
        runtime=_Runtime(settings),
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )

    async def scenario() -> dict:
        first = await octo.request_self_restart(
            42,
            {
                "reason": "apply git pull",
                "confirm": True,
                "delay_seconds": 3,
            },
        )
        assert first["status"] == "restart_requested"
        raw = await _tool_octo_restart_self(
            {
                "reason": "force stale restart from continuation",
                "confirm": True,
                "force": True,
                "delay_seconds": 3,
            },
            {
                "octo": octo,
                "chat_id": 42,
                "route_mode": "conversation",
                "background_delivery": True,
                "chat_turn_epoch": octo.current_chat_turn_epoch(42),
            },
        )
        return json.loads(raw)

    result = asyncio.run(scenario())
    assert result["status"] == "force_requires_fresh_user_turn"
    requests = (settings.state_dir / "control_requests.jsonl").read_text(encoding="utf-8")
    assert len(requests.splitlines()) == 1


def test_self_restart_tool_allows_force_from_fresh_foreground_turn(tmp_path: Path) -> None:
    settings = _StoreSettings(tmp_path / "data", tmp_path / "workspace")
    store = SQLiteStore(settings)
    octo = Octo(
        provider=object(),
        store=store,
        policy=object(),
        runtime=_Runtime(settings),
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )

    async def scenario() -> dict:
        first = await octo.request_self_restart(
            42,
            {
                "reason": "apply git pull",
                "confirm": True,
                "delay_seconds": 3,
            },
        )
        assert first["status"] == "restart_requested"
        raw = await _tool_octo_restart_self(
            {
                "reason": "user explicitly confirmed a second restart",
                "confirm": True,
                "force": True,
                "delay_seconds": 3,
            },
            {
                "octo": octo,
                "chat_id": 42,
                "route_mode": "conversation",
                "chat_turn_epoch": octo.advance_chat_turn_epoch(42),
            },
        )
        return json.loads(raw)

    result = asyncio.run(scenario())
    assert result["status"] == "restart_requested"
    requests = (settings.state_dir / "control_requests.jsonl").read_text(encoding="utf-8")
    assert len(requests.splitlines()) == 2


def test_self_update_persists_handoff_resume_and_request(tmp_path: Path, monkeypatch) -> None:
    settings = _StoreSettings(tmp_path / "data", tmp_path / "workspace")
    store = SQLiteStore(settings)
    octo = Octo(
        provider=object(),
        store=store,
        policy=object(),
        runtime=_Runtime(settings),
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )
    monkeypatch.setattr(
        "octopal.runtime.octo.core.check_update_status",
        lambda _root: {
            "status": "ok",
            "local_version": "2026.04.26",
            "latest_version": "2026.04.27",
            "update_available": True,
            "can_update": True,
        },
    )

    async def scenario() -> dict:
        return await octo.request_self_update(
            42,
            {
                "reason": "apply latest release",
                "goal_now": "Continue release verification.",
                "next_step": "Report whether update and restart worked.",
                "confirm": True,
                "delay_seconds": 3,
            },
        )

    result = asyncio.run(scenario())
    assert result["status"] == "update_requested"
    assert result["request"]["action"] == SELF_UPDATE_ACTION
    assert result["request"]["requested_by"] == SELF_UPDATE_REQUESTED_BY

    resume = read_pending_restart_resume(settings.state_dir)
    assert resume is not None
    assert resume["request_id"] == result["request"]["request_id"]
    assert resume["handoff"]["source"] == "octo_update_self"
    assert resume["update"]["latest_version"] == "2026.04.27"


def test_self_update_rejects_recent_duplicate_request(tmp_path: Path, monkeypatch) -> None:
    settings = _StoreSettings(tmp_path / "data", tmp_path / "workspace")
    store = SQLiteStore(settings)
    octo = Octo(
        provider=object(),
        store=store,
        policy=object(),
        runtime=_Runtime(settings),
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )
    monkeypatch.setattr(
        "octopal.runtime.octo.core.check_update_status",
        lambda _root: {
            "status": "ok",
            "local_version": "2026.04.26",
            "latest_version": "2026.04.27",
            "update_available": True,
            "can_update": True,
        },
    )

    async def scenario() -> tuple[dict, dict]:
        first = await octo.request_self_update(
            42,
            {
                "reason": "apply latest release",
                "confirm": True,
                "delay_seconds": 3,
            },
        )
        second = await octo.request_self_update(
            42,
            {
                "reason": "apply latest release again",
                "confirm": True,
                "delay_seconds": 3,
            },
        )
        return first, second

    first, second = asyncio.run(scenario())
    assert first["status"] == "update_requested"
    assert second["status"] == "duplicate_recent_control_action"
    requests = (settings.state_dir / "control_requests.jsonl").read_text(encoding="utf-8")
    assert len(requests.splitlines()) == 1


def test_self_update_tool_rejects_force_from_background_turn(tmp_path: Path, monkeypatch) -> None:
    settings = _StoreSettings(tmp_path / "data", tmp_path / "workspace")
    store = SQLiteStore(settings)
    octo = Octo(
        provider=object(),
        store=store,
        policy=object(),
        runtime=_Runtime(settings),
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )
    monkeypatch.setattr(
        "octopal.runtime.octo.core.check_update_status",
        lambda _root: {
            "status": "ok",
            "local_version": "2026.04.26",
            "latest_version": "2026.04.27",
            "update_available": True,
            "can_update": True,
        },
    )

    async def scenario() -> dict:
        first = await octo.request_self_update(
            42,
            {
                "reason": "apply latest release",
                "confirm": True,
                "delay_seconds": 3,
            },
        )
        assert first["status"] == "update_requested"
        raw = await _tool_octo_update_self(
            {
                "reason": "force stale update from continuation",
                "confirm": True,
                "force": True,
                "delay_seconds": 3,
            },
            {
                "octo": octo,
                "chat_id": 42,
                "route_mode": "conversation",
                "background_delivery": True,
                "chat_turn_epoch": octo.current_chat_turn_epoch(42),
            },
        )
        return json.loads(raw)

    result = asyncio.run(scenario())
    assert result["status"] == "force_requires_fresh_user_turn"
    requests = (settings.state_dir / "control_requests.jsonl").read_text(encoding="utf-8")
    assert len(requests.splitlines()) == 1


def test_due_self_restart_requests_are_octo_only(tmp_path: Path) -> None:
    state_dir = tmp_path / "data"
    append_control_request(
        state_dir,
        action=SELF_RESTART_ACTION,
        reason="ok",
        requested_by=SELF_RESTART_REQUESTED_BY,
        delay_seconds=0,
    )
    append_control_request(
        state_dir,
        action=SELF_RESTART_ACTION,
        reason="wrong source",
        requested_by="worker",
        delay_seconds=0,
    )
    append_control_request(
        state_dir,
        action=SELF_RESTART_ACTION,
        reason="future",
        requested_by=SELF_RESTART_REQUESTED_BY,
        delay_seconds=60,
    )

    due = due_self_restart_requests(
        state_dir,
        now=datetime.now(UTC) + timedelta(seconds=1),
    )
    assert len(due) == 1
    assert due[0]["reason"] == "ok"


def test_recent_control_action_ignores_terminal_nonblocking_requests(tmp_path: Path) -> None:
    state_dir = tmp_path / "data"
    failed = append_control_request(
        state_dir,
        action=SELF_RESTART_ACTION,
        reason="failed",
        requested_by=SELF_RESTART_REQUESTED_BY,
        delay_seconds=0,
        metadata={"chat_id": 42},
    )
    append_control_ack(
        state_dir,
        failed["request_id"],
        status="error",
        source="self_restart_helper",
    )
    cleared = append_control_request(
        state_dir,
        action=SELF_RESTART_ACTION,
        reason="cleared",
        requested_by=SELF_RESTART_REQUESTED_BY,
        delay_seconds=0,
        metadata={"chat_id": 42},
    )
    append_control_ack(
        state_dir,
        cleared["request_id"],
        status="cleared",
        source="dashboard_action",
    )
    append_control_request(
        state_dir,
        action=SELF_RESTART_ACTION,
        reason="other chat",
        requested_by=SELF_RESTART_REQUESTED_BY,
        delay_seconds=0,
        metadata={"chat_id": 7},
    )

    duplicate = find_recent_control_action(
        state_dir,
        action=SELF_RESTART_ACTION,
        requested_by=SELF_RESTART_REQUESTED_BY,
        chat_id=42,
    )
    assert duplicate is None


def test_due_self_update_requests_are_octo_only(tmp_path: Path) -> None:
    state_dir = tmp_path / "data"
    append_control_request(
        state_dir,
        action=SELF_UPDATE_ACTION,
        reason="ok",
        requested_by=SELF_UPDATE_REQUESTED_BY,
        delay_seconds=0,
    )
    append_control_request(
        state_dir,
        action=SELF_UPDATE_ACTION,
        reason="wrong source",
        requested_by="worker",
        delay_seconds=0,
    )

    due = due_self_update_requests(
        state_dir,
        now=datetime.now(UTC) + timedelta(seconds=1),
    )
    assert len(due) == 1
    assert due[0]["reason"] == "ok"


def test_update_helper_runs_update_then_restart(tmp_path: Path, monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_run_cli_command(args, *, project_root, timeout_seconds):
        calls.append(list(args))
        return {
            "returncode": 0,
            "stdout_tail": "ok",
            "stderr_tail": "",
            "command": " ".join(args),
        }

    monkeypatch.setattr("octopal.runtime.self_control._run_cli_command", fake_run_cli_command)

    rc = run_update_helper(
        request_id="u1",
        project_root=tmp_path,
        state_dir=tmp_path / "data",
        delay_seconds=0,
    )

    assert rc == 0
    assert calls == [["update"], ["restart"]]


def test_workers_cannot_receive_self_restart_tools() -> None:
    assert "octo_restart_self" in worker_runtime._WORKER_BLOCKED_TOOL_NAMES
    assert "octo_check_update" in worker_runtime._WORKER_BLOCKED_TOOL_NAMES
    assert "octo_update_self" in worker_runtime._WORKER_BLOCKED_TOOL_NAMES
    assert "self_control" in worker_runtime._WORKER_BLOCKED_TOOL_NAMES
    assert "octo_restart_self" in worker_management._WORKER_BLOCKED_TOOL_NAMES
    assert "octo_check_update" in worker_management._WORKER_BLOCKED_TOOL_NAMES
    assert "octo_update_self" in worker_management._WORKER_BLOCKED_TOOL_NAMES
    assert "self_control" in worker_management._WORKER_BLOCKED_TOOL_NAMES
