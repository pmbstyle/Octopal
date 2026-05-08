from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from octopal.runtime.octo import core as octo_core
from octopal.runtime.octo import router as octo_router
from octopal.runtime.octo.core import Octo
from octopal.runtime.scheduler.service import SchedulerService
from octopal.runtime.workers.contracts import WorkerResult
from octopal.tools.tools import (
    _tool_check_schedule,
    _tool_repair_scheduled_tasks,
    _tool_schedule_task,
    _tool_scheduler_status,
)
from octopal.utils import utc_now


class _StoreStub:
    def __init__(self, tasks: list[dict] | None = None, worker_status: str | None = None) -> None:
        self.tasks = tasks or []
        self.last_upsert: dict | None = None
        self.marked_task_ids: list[str] = []
        self.metadata_updates: list[dict[str, object | None]] = []
        self.worker_status = worker_status

    def upsert_scheduled_task(
        self,
        task_id: str,
        name: str,
        frequency: str,
        task_text: str,
        description: str | None = None,
        worker_id: str | None = None,
        inputs: dict | None = None,
        enabled: bool = True,
        metadata: dict | None = None,
    ) -> None:
        self.last_upsert = {
            "task_id": task_id,
            "name": name,
            "frequency": frequency,
            "task_text": task_text,
            "description": description,
            "worker_id": worker_id,
            "inputs": inputs,
            "enabled": enabled,
            "metadata": metadata,
        }
        metadata_json = json.dumps(metadata) if metadata else None
        for index, task in enumerate(self.tasks):
            if str(task.get("id")) != task_id:
                continue
            updated = dict(task)
            updated.update(
                {
                    "name": name,
                    "description": description,
                    "frequency": frequency,
                    "worker_id": worker_id,
                    "task_text": task_text,
                    "inputs_json": json.dumps(inputs) if inputs else None,
                    "enabled": 1 if enabled else 0,
                    "metadata_json": metadata_json,
                }
            )
            self.tasks[index] = updated
            break

    def get_scheduled_tasks(self, enabled_only: bool = False) -> list[dict]:
        if not enabled_only:
            return list(self.tasks)
        return [t for t in self.tasks if int(t.get("enabled", 1)) == 1]

    def update_task_last_run(self, task_id: str, _ts) -> None:
        self.marked_task_ids.append(task_id)

    def update_scheduled_task_metadata(self, task_id: str, metadata: dict | None) -> None:
        self.metadata_updates.append({"task_id": task_id, "metadata": metadata})
        metadata_json = json.dumps(metadata) if metadata else None
        for task in self.tasks:
            if str(task.get("id")) == task_id:
                task["metadata_json"] = metadata_json
                break

    def delete_scheduled_task(self, _task_id: str) -> None:
        return None

    def get_worker(self, _worker_id: str):
        if self.worker_status is None:
            return None
        return SimpleNamespace(status=self.worker_status)


class _MemoryStub:
    async def add_message(self, role: str, text: str, metadata: dict) -> None:
        return None


class _ApprovalsStub:
    bot = None


class _RuntimeStub:
    async def run_task(self, task_request, approval_requester=None):
        return WorkerResult(summary="ok", output={"ok": True})


class _BrowserStub:
    async def shutdown(self) -> None:
        return None


def test_schedule_task_rejects_invalid_frequency(tmp_path: Path) -> None:
    scheduler = SchedulerService(store=_StoreStub(), workspace_dir=tmp_path)
    result = _tool_schedule_task(
        {
            "name": "Daily digest",
            "frequency": "Every often",
            "task": "Generate digest",
        },
        {"octo": SimpleNamespace(scheduler=scheduler)},
    )
    assert result.startswith("schedule_task error:")


def test_schedule_task_normalizes_valid_frequency(tmp_path: Path) -> None:
    store = _StoreStub()
    scheduler = SchedulerService(store=store, workspace_dir=tmp_path)
    task_id = scheduler.schedule_task(
        name="Digest",
        frequency="daily at 7:05",
        task_text="Generate digest",
        worker_id="writer",
        notify_user="always",
        execution_mode="worker",
    )
    assert task_id == "digest"
    assert store.last_upsert is not None
    assert store.last_upsert["frequency"] == "Daily at 07:05"
    assert store.last_upsert["metadata"] == {
        "notify_user": "always",
        "execution_mode": "worker",
    }


def test_schedule_task_rejects_worker_mode_without_worker_id(tmp_path: Path) -> None:
    scheduler = SchedulerService(store=_StoreStub(), workspace_dir=tmp_path)
    result = _tool_schedule_task(
        {
            "name": "Digest",
            "frequency": "Every 30 minutes",
            "task": "Generate digest",
            "execution_mode": "worker",
        },
        {"octo": SimpleNamespace(scheduler=scheduler)},
    )
    assert result == "schedule_task error: worker_id is required when execution_mode=worker."


def test_schedule_task_rejects_worker_id_for_octo_control_mode(tmp_path: Path) -> None:
    scheduler = SchedulerService(store=_StoreStub(), workspace_dir=tmp_path)
    result = _tool_schedule_task(
        {
            "name": "Digest",
            "frequency": "Every 30 minutes",
            "task": "Generate digest",
            "execution_mode": "octo_control",
            "worker_id": "writer",
        },
        {"octo": SimpleNamespace(scheduler=scheduler)},
    )
    assert result == "schedule_task error: worker_id must be omitted when execution_mode=octo_control."


def test_schedule_task_derives_legacy_octo_control_mode_without_worker_id(tmp_path: Path) -> None:
    store = _StoreStub()
    scheduler = SchedulerService(store=store, workspace_dir=tmp_path)
    payload = json.loads(
        _tool_schedule_task(
            {
                "name": "Compact memory",
                "frequency": "Every 30 minutes",
                "task": "Compact memory",
            },
            {"octo": SimpleNamespace(scheduler=scheduler)},
        )
    )

    assert payload["status"] == "scheduled"
    assert payload["execution_mode"] == "octo_control"
    assert payload["notify_user"] == "never"
    assert store.last_upsert is not None
    assert store.last_upsert["metadata"] == {
        "notify_user": "never",
        "execution_mode": "octo_control",
    }


def test_check_schedule_returns_json_with_inputs(tmp_path: Path) -> None:
    store = _StoreStub(
        tasks=[
            {
                "id": "daily_digest",
                "name": "Daily Digest",
                "description": "Build digest",
                "frequency": "Every 30 minutes",
                "worker_id": "writer",
                "task_text": "Generate a concise digest",
                "inputs_json": json.dumps({"section": "news", "max_items": 5}),
                "metadata_json": json.dumps({"notify_user": "always"}),
                "last_run_at": None,
                "enabled": 1,
            }
        ]
    )
    scheduler = SchedulerService(store=store, workspace_dir=tmp_path)
    payload = json.loads(
        asyncio.run(_tool_check_schedule({}, {"octo": SimpleNamespace(scheduler=scheduler)}))
    )
    assert payload["due_count"] == 1
    assert payload["due_tasks"][0]["task_id"] == "daily_digest"
    assert payload["due_tasks"][0]["inputs"] == {"section": "news", "max_items": 5}
    assert payload["due_tasks"][0]["notify_user"] == "always"
    assert payload["due_tasks"][0]["execution_mode"] == "worker"
    assert payload["due_tasks"][0]["dispatch_ready"] is True
    assert payload["due_tasks"][0]["dispatch_policy_reason"] is None
    assert payload["due_tasks"][0]["suggested_execution_mode"] is None


def test_scheduler_status_reports_due_and_next_run_preview(tmp_path: Path) -> None:
    store = _StoreStub(
        tasks=[
            {
                "id": "daily_digest",
                "name": "Daily Digest",
                "description": "Build digest",
                "frequency": "Every 30 minutes",
                "worker_id": "writer",
                "task_text": "Generate a concise digest",
                "inputs_json": json.dumps({"section": "news"}),
                "metadata_json": json.dumps({"notify_user": "if_significant"}),
                "last_run_at": None,
                "enabled": 1,
            },
            {
                "id": "nightly_cleanup",
                "name": "Nightly Cleanup",
                "description": "Compact memory",
                "frequency": "Daily at 23:30",
                "worker_id": None,
                "task_text": "Compact memory",
                "inputs_json": "{}",
                "metadata_json": json.dumps({"notify_user": "never"}),
                "last_run_at": None,
                "enabled": 0,
            },
        ]
    )
    scheduler = SchedulerService(store=store, workspace_dir=tmp_path)
    payload = json.loads(
        asyncio.run(_tool_scheduler_status({}, {"octo": SimpleNamespace(scheduler=scheduler)}))
    )

    assert payload["status"] == "ok"
    assert payload["due_count"] == 1
    assert payload["disabled_count"] == 1
    assert payload["next_due_task"]["task_id"] == "daily_digest"
    assert payload["tasks"][0]["due_now"] is True
    assert payload["tasks"][0]["next_run_at"] is not None
    assert payload["tasks"][0]["notify_user"] == "if_significant"
    assert payload["tasks"][0]["execution_mode"] == "worker"
    assert payload["tasks"][0]["dispatch_ready"] is True
    assert payload["tasks"][0]["suggested_execution_mode"] is None
    assert payload["tasks"][1]["execution_mode"] == "octo_control"
    assert payload["tasks"][1]["dispatch_ready"] is True
    assert payload["tasks"][1]["dispatch_policy_reason"] is None
    assert payload["tasks"][1]["suggested_execution_mode"] is None
    assert any("due now" in hint for hint in payload["hints"])
    assert not any("not dispatch-ready" in hint for hint in payload["hints"])
    assert payload["next_due_task"]["execution_mode"] == "worker"


def test_scheduler_sync_to_markdown_includes_dispatch_readiness(tmp_path: Path) -> None:
    store = _StoreStub(
        tasks=[
            {
                "id": "nightly_cleanup",
                "name": "Nightly Cleanup",
                "description": "Compact memory",
                "frequency": "Daily at 23:30",
                "worker_id": None,
                "task_text": "Compact memory",
                "inputs_json": "{}",
                "metadata_json": json.dumps({"notify_user": "never"}),
                "last_run_at": None,
                "enabled": 1,
            }
        ]
    )
    scheduler = SchedulerService(store=store, workspace_dir=tmp_path)

    scheduler.sync_to_markdown()

    heartbeat = (tmp_path / "HEARTBEAT.md").read_text(encoding="utf-8")
    assert "**Execution mode**: octo_control" in heartbeat
    assert "**Dispatch**: ready" in heartbeat


def test_schedule_task_rejects_invalid_notify_user(tmp_path: Path) -> None:
    scheduler = SchedulerService(store=_StoreStub(), workspace_dir=tmp_path)
    result = _tool_schedule_task(
        {
            "name": "Digest",
            "frequency": "Every 30 minutes",
            "task": "Generate digest",
            "notify_user": "sometimes",
        },
        {"octo": SimpleNamespace(scheduler=scheduler)},
    )
    assert result.startswith("schedule_task error:")


def test_describe_tasks_marks_blocked_octo_control_backoff(tmp_path: Path) -> None:
    blocked_until = utc_now() + timedelta(minutes=30)
    scheduler = SchedulerService(
        store=_StoreStub(
            tasks=[
                {
                    "id": "weather_check",
                    "name": "Weather Check",
                    "description": "Check weather",
                    "frequency": "Every 30 minutes",
                    "worker_id": None,
                    "task_text": "Check the weather",
                    "inputs_json": "{}",
                    "metadata_json": json.dumps(
                        {
                            "notify_user": "never",
                            "execution_mode": "octo_control",
                            "blocked_until": blocked_until.isoformat(),
                            "blocked_reason": "blocked_by_route",
                        }
                    ),
                    "last_run_at": None,
                    "enabled": 1,
                }
            ]
        ),
        workspace_dir=tmp_path,
    )

    described = scheduler.describe_tasks(enabled_only=True)

    assert described[0]["dispatch_ready"] is False
    assert described[0]["dispatch_policy_reason"] == "blocked_by_route"
    assert described[0]["blocked_until"] == blocked_until.isoformat()
    assert described[0]["blocked_reason"] == "blocked_by_route"
    assert described[0]["suggested_execution_mode"] == "worker"
    assert described[0]["due_now"] is False


def test_get_actionable_tasks_excludes_blocked_octo_control_backoff(tmp_path: Path) -> None:
    blocked_until = utc_now() + timedelta(minutes=30)
    scheduler = SchedulerService(
        store=_StoreStub(
            tasks=[
                {
                    "id": "weather_check",
                    "name": "Weather Check",
                    "description": "Check weather",
                    "frequency": "Every 30 minutes",
                    "worker_id": None,
                    "task_text": "Check the weather",
                    "inputs_json": "{}",
                    "metadata_json": json.dumps(
                        {
                            "notify_user": "never",
                            "execution_mode": "octo_control",
                            "blocked_until": blocked_until.isoformat(),
                            "blocked_reason": "blocked_by_route",
                        }
                    ),
                    "last_run_at": None,
                    "enabled": 1,
                }
            ]
        ),
        workspace_dir=tmp_path,
    )

    assert scheduler.get_actionable_tasks() == []


def test_route_blocked_octo_control_stays_not_ready_after_backoff_expires(
    tmp_path: Path,
) -> None:
    blocked_until = utc_now() - timedelta(minutes=1)
    scheduler = SchedulerService(
        store=_StoreStub(
            tasks=[
                {
                    "id": "weather_check",
                    "name": "Weather Check",
                    "description": "Check weather",
                    "frequency": "Every 30 minutes",
                    "worker_id": None,
                    "task_text": "Check the weather",
                    "inputs_json": "{}",
                    "metadata_json": json.dumps(
                        {
                            "notify_user": "never",
                            "execution_mode": "octo_control",
                            "blocked_until": blocked_until.isoformat(),
                            "blocked_reason": "blocked_by_route",
                            "suggested_execution_mode": "worker",
                        }
                    ),
                    "last_run_at": None,
                    "enabled": 1,
                }
            ]
        ),
        workspace_dir=tmp_path,
    )

    described = scheduler.describe_tasks(enabled_only=True)

    assert scheduler.get_actionable_tasks() == []
    assert described[0]["dispatch_ready"] is False
    assert described[0]["dispatch_policy_reason"] == "blocked_by_route"
    assert described[0]["suggested_execution_mode"] == "worker"
    assert described[0]["due_now"] is False


def test_scheduler_sync_to_markdown_shows_suggested_execution_mode_for_blocked_tasks(tmp_path: Path) -> None:
    blocked_until = utc_now() + timedelta(minutes=30)
    store = _StoreStub(
        tasks=[
            {
                "id": "weather_check",
                "name": "Weather Check",
                "description": "Check weather",
                "frequency": "Every 30 minutes",
                "worker_id": None,
                "task_text": "Check the weather",
                "inputs_json": "{}",
                "metadata_json": json.dumps(
                    {
                        "notify_user": "never",
                        "execution_mode": "octo_control",
                        "blocked_until": blocked_until.isoformat(),
                        "blocked_reason": "blocked_by_route",
                    }
                ),
                "last_run_at": None,
                "enabled": 1,
            }
        ]
    )
    scheduler = SchedulerService(store=store, workspace_dir=tmp_path)

    scheduler.sync_to_markdown()

    heartbeat = (tmp_path / "HEARTBEAT.md").read_text(encoding="utf-8")
    assert "**Suggested execution mode**: worker" in heartbeat


def test_scheduler_status_reports_suggested_execution_mode_for_blocked_tasks(tmp_path: Path) -> None:
    blocked_until = utc_now() + timedelta(minutes=30)
    store = _StoreStub(
        tasks=[
            {
                "id": "weather_check",
                "name": "Weather Check",
                "description": "Check weather",
                "frequency": "Every 30 minutes",
                "worker_id": None,
                "task_text": "Check the weather",
                "inputs_json": "{}",
                "metadata_json": json.dumps(
                    {
                        "notify_user": "never",
                        "execution_mode": "octo_control",
                        "blocked_until": blocked_until.isoformat(),
                        "blocked_reason": "blocked_by_route",
                    }
                ),
                "last_run_at": None,
                "enabled": 1,
            }
        ]
    )
    scheduler = SchedulerService(store=store, workspace_dir=tmp_path)
    payload = json.loads(
        asyncio.run(_tool_scheduler_status({}, {"octo": SimpleNamespace(scheduler=scheduler)}))
    )

    assert payload["tasks"][0]["execution_mode"] == "octo_control"
    assert payload["tasks"][0]["dispatch_ready"] is False
    assert payload["tasks"][0]["dispatch_policy_reason"] == "blocked_by_route"
    assert payload["tasks"][0]["suggested_execution_mode"] == "worker"
    assert payload["due_count"] == 0
    assert any("suggested execution mode" in hint for hint in payload["hints"])


def test_repair_scheduled_tasks_previews_candidates_without_applying(tmp_path: Path) -> None:
    blocked_until = utc_now() + timedelta(minutes=30)
    store = _StoreStub(
        tasks=[
            {
                "id": "weather_check",
                "name": "Weather Check",
                "description": "Check weather",
                "frequency": "Every 30 minutes",
                "worker_id": None,
                "task_text": "Check the weather",
                "inputs_json": "{}",
                "metadata_json": json.dumps(
                    {
                        "notify_user": "never",
                        "execution_mode": "octo_control",
                        "blocked_until": blocked_until.isoformat(),
                        "blocked_reason": "blocked_by_route",
                    }
                ),
                "last_run_at": None,
                "enabled": 1,
            }
        ]
    )
    scheduler = SchedulerService(store=store, workspace_dir=tmp_path)

    payload = json.loads(
        _tool_repair_scheduled_tasks({}, {"octo": SimpleNamespace(scheduler=scheduler)})
    )

    assert payload["status"] == "preview"
    assert payload["candidate_count"] == 1
    assert payload["applied_count"] == 0
    assert payload["candidates"][0]["task_id"] == "weather_check"
    assert payload["candidates"][0]["suggested_execution_mode"] == "worker"
    assert payload["candidates"][0]["can_apply"] is False
    assert payload["candidates"][0]["skip_reason"] == "missing_worker_id"
    assert store.last_upsert is None


def test_repair_scheduled_tasks_applies_worker_migration_with_valid_worker_id(tmp_path: Path) -> None:
    blocked_until = utc_now() + timedelta(minutes=30)
    worker_dir = tmp_path / "workers" / "weather_worker"
    worker_dir.mkdir(parents=True)
    (worker_dir / "worker.json").write_text(
        json.dumps(
            {
                "id": "weather_worker",
                "name": "Weather Worker",
                "description": "Fetches weather",
                "system_prompt": "Do weather work.",
                "available_tools": ["web_search"],
                "required_permissions": ["network"],
                "max_thinking_steps": 8,
                "default_timeout_seconds": 300,
            }
        ),
        encoding="utf-8",
    )
    store = _StoreStub(
        tasks=[
            {
                "id": "weather_check",
                "name": "Weather Check",
                "description": "Check weather",
                "frequency": "Every 30 minutes",
                "worker_id": None,
                "task_text": "Check the weather",
                "inputs_json": "{}",
                "metadata_json": json.dumps(
                    {
                        "notify_user": "never",
                        "execution_mode": "octo_control",
                        "blocked_until": blocked_until.isoformat(),
                        "blocked_reason": "blocked_by_route",
                        "suggested_execution_mode": "worker",
                    }
                ),
                "last_run_at": None,
                "enabled": 1,
            }
        ]
    )
    scheduler = SchedulerService(store=store, workspace_dir=tmp_path)

    payload = json.loads(
        _tool_repair_scheduled_tasks(
            {"apply": True, "task_ids": ["weather_check"], "worker_id": "weather_worker"},
            {"octo": SimpleNamespace(scheduler=scheduler)},
        )
    )

    assert payload["status"] == "applied"
    assert payload["candidate_count"] == 1
    assert payload["applied_count"] == 1
    assert payload["skipped_count"] == 0
    assert payload["applied"][0] == {
        "task_id": "weather_check",
        "name": "Weather Check",
        "execution_mode": "worker",
        "worker_id": "weather_worker",
    }
    assert store.last_upsert is not None
    assert store.last_upsert["task_id"] == "weather_check"
    assert store.last_upsert["worker_id"] == "weather_worker"
    assert store.last_upsert["metadata"] == {
        "notify_user": "never",
        "execution_mode": "worker",
    }


def test_octo_marks_scheduled_task_after_successful_worker_run_even_if_store_lags() -> None:
    async def _run(worker_status: str | None) -> list[str]:
        store = _StoreStub(worker_status=worker_status)
        scheduler = SchedulerService(store=store, workspace_dir=Path("."))
        octo = Octo(
            provider=object(),
            store=store,
            policy=object(),
            runtime=_RuntimeStub(),
            approvals=_ApprovalsStub(),
            memory=_MemoryStub(),
            canon=object(),
            scheduler=scheduler,
        )
        await octo._start_worker_async(
            worker_id="writer",
            task="Generate digest",
            chat_id=0,
            inputs={},
            tools=None,
            model=None,
            timeout_seconds=5,
            scheduled_task_id="daily_digest",
        )
        await asyncio.sleep(0.05)
        return store.marked_task_ids

    marked_completed = asyncio.run(_run("completed"))
    marked_missing = asyncio.run(_run(None))
    marked_failed = asyncio.run(_run("failed"))

    assert marked_completed == ["daily_digest"]
    assert marked_missing == ["daily_digest"]
    assert marked_failed == []


@pytest.mark.asyncio
async def test_route_scheduler_tick_uses_control_plane_prompt_and_skips_planner(monkeypatch):
    calls = {"control_prompt": 0, "complete_route": 0}

    class SchedulerStub:
        def get_actionable_tasks(self) -> list[dict]:
            return [
                {
                    "id": "daily_digest",
                    "name": "Daily Digest",
                    "worker_id": "writer",
                    "frequency": "Every 30 minutes",
                    "notify_user": "always",
                    "task_text": "Generate digest",
                }
            ]

        def describe_tasks(self, *, enabled_only: bool = False) -> list[dict]:
            return [
                {
                    "id": "daily_digest",
                    "name": "Daily Digest",
                    "due_now": True,
                    "next_run_at": "2026-04-22T12:00:00+00:00",
                    "notify_user": "always",
                }
            ]

    class DummyOcto:
        provider = object()
        reflection = None
        mcp_manager = None
        scheduler = SchedulerStub()

        async def set_thinking(self, value):
            return None

    async def _build_control_plane_prompt(**kwargs):
        calls["control_prompt"] += 1
        assert kwargs["mode_label"] == "scheduler"
        assert "daily_digest" in kwargs["user_text"]
        return [octo_router.Message(role="system", content="scheduler control plane")]

    async def _complete_route_with_tools(**kwargs):
        calls["complete_route"] += 1
        return "SCHEDULER_IDLE"

    def _build_octo_prompt_should_not_run(*args, **kwargs):
        raise AssertionError("build_octo_prompt should not run for scheduler route")

    def _build_plan_should_not_run(*args, **kwargs):
        raise AssertionError("_build_plan should not run for scheduler route")

    monkeypatch.setattr(octo_router, "build_control_plane_prompt", _build_control_plane_prompt)
    monkeypatch.setattr(octo_router, "_complete_route_with_tools", _complete_route_with_tools)
    monkeypatch.setattr(octo_router, "build_octo_prompt", _build_octo_prompt_should_not_run)
    monkeypatch.setattr(octo_router, "_build_plan", _build_plan_should_not_run)

    result = await octo_router.route_scheduler_tick(DummyOcto())

    assert result == "SCHEDULER_IDLE"
    assert calls == {"control_prompt": 1, "complete_route": 1}


@pytest.mark.asyncio
async def test_route_scheduled_octo_control_uses_control_plane_prompt_and_skips_planner(monkeypatch):
    calls = {"control_prompt": 0, "complete_route": 0}

    class DummyOcto:
        provider = object()
        reflection = None
        mcp_manager = None

        async def set_thinking(self, value):
            return None

    task = {
        "id": "memory_compact",
        "name": "Memory Compact",
        "frequency": "Every 30 minutes",
        "execution_mode": "octo_control",
        "notify_user": "never",
        "task_text": "Compact memory",
        "inputs": {"mode": "soft"},
    }

    async def _build_control_plane_prompt(**kwargs):
        calls["control_prompt"] += 1
        assert kwargs["mode_label"] == "scheduled_octo_control"
        assert "memory_compact" in kwargs["user_text"]
        assert "octo_control" in kwargs["user_text"]
        return [octo_router.Message(role="system", content="scheduled octo control")]

    async def _complete_route_with_tools(**kwargs):
        calls["complete_route"] += 1
        return "SCHEDULED_TASK_DONE"

    def _build_octo_prompt_should_not_run(*args, **kwargs):
        raise AssertionError("build_octo_prompt should not run for scheduled octo control route")

    def _build_plan_should_not_run(*args, **kwargs):
        raise AssertionError("_build_plan should not run for scheduled octo control route")

    monkeypatch.setattr(octo_router, "build_control_plane_prompt", _build_control_plane_prompt)
    monkeypatch.setattr(octo_router, "_complete_route_with_tools", _complete_route_with_tools)
    monkeypatch.setattr(octo_router, "build_octo_prompt", _build_octo_prompt_should_not_run)
    monkeypatch.setattr(octo_router, "_build_plan", _build_plan_should_not_run)

    result = await octo_router.route_scheduled_octo_control(DummyOcto(), task)

    assert result == "SCHEDULED_TASK_DONE"
    assert calls == {"control_prompt": 1, "complete_route": 1}


@pytest.mark.asyncio
async def test_octo_run_scheduler_tick_once_uses_bounded_scheduler_route(monkeypatch):
    calls = {"scheduler_tick": 0, "dispatch": 0}

    async def _route_scheduler_tick(octo, chat_id=0, *, max_tasks=10):
        calls["scheduler_tick"] += 1
        assert chat_id == 0
        assert max_tasks == 7
        return "SCHEDULER_IDLE"

    async def _dispatch_due_scheduled_tasks_once(self, *, chat_id=0, max_tasks=10):
        calls["dispatch"] += 1
        assert chat_id == 0
        assert max_tasks == 7
        return {
            "due_count": 0,
            "attempted": 0,
            "started": 0,
            "completed": 0,
            "duplicates": 0,
            "rejected_by_policy": 0,
            "policy_reasons": {},
            "errors": 0,
        }

    monkeypatch.setattr(octo_router, "route_scheduler_tick", _route_scheduler_tick)
    monkeypatch.setattr(octo_core, "route_scheduler_tick", _route_scheduler_tick)
    monkeypatch.setattr(
        octo_core.Octo,
        "_dispatch_due_scheduled_tasks_once",
        _dispatch_due_scheduled_tasks_once,
    )

    octo = Octo(
        provider=object(),
        store=_StoreStub(),
        policy=object(),
        runtime=_RuntimeStub(),
        approvals=_ApprovalsStub(),
        memory=_MemoryStub(),
        canon=SimpleNamespace(workspace_dir=Path(".")),
        scheduler=SchedulerService(store=_StoreStub(), workspace_dir=Path(".")),
    )

    await octo._run_scheduler_tick_once(max_tasks=7)

    assert calls == {"scheduler_tick": 1, "dispatch": 1}


@pytest.mark.asyncio
async def test_octo_run_scheduler_tick_once_delivers_user_visible_scheduler_output(monkeypatch):
    sent_messages = []

    async def _route_scheduler_tick(octo, chat_id=0, *, max_tasks=10):
        return "Internal note.\n<user_visible>Планировщик нашел важное обновление.</user_visible>"

    async def _dispatch_due_scheduled_tasks_once(self, *, chat_id=0, max_tasks=10):
        return {
            "due_count": 0,
            "attempted": 0,
            "started": 0,
            "completed": 0,
            "duplicates": 0,
            "rejected_by_policy": 0,
            "policy_reasons": {},
            "errors": 0,
        }

    async def _send_scheduler_control_update(octo, chat_id, task_id, text):
        sent_messages.append((chat_id, task_id, text))

    monkeypatch.setattr(octo_core, "route_scheduler_tick", _route_scheduler_tick)
    monkeypatch.setattr(
        octo_core.Octo,
        "_dispatch_due_scheduled_tasks_once",
        _dispatch_due_scheduled_tasks_once,
    )
    monkeypatch.setattr(octo_core, "_send_scheduler_control_update", _send_scheduler_control_update)

    octo = Octo(
        provider=object(),
        store=_StoreStub(),
        policy=object(),
        runtime=_RuntimeStub(),
        approvals=_ApprovalsStub(),
        memory=_MemoryStub(),
        canon=SimpleNamespace(workspace_dir=Path(".")),
        scheduler=SchedulerService(store=_StoreStub(), workspace_dir=Path(".")),
    )
    octo._scheduled_delivery_chat_ids = [777]

    await octo._run_scheduler_tick_once(max_tasks=7)

    assert sent_messages == [(777, None, "Планировщик нашел важное обновление.")]


@pytest.mark.asyncio
async def test_octo_dispatch_due_scheduled_tasks_starts_dispatchable_workers(monkeypatch):
    started_calls = []
    scheduler = SchedulerService(
        store=_StoreStub(
            tasks=[
                {
                    "id": "daily_digest",
                    "name": "Daily Digest",
                    "description": "Build digest",
                    "frequency": "Every 30 minutes",
                    "worker_id": "writer",
                    "task_text": "Generate digest",
                    "inputs_json": json.dumps({"section": "news"}),
                    "metadata_json": json.dumps({"notify_user": "never"}),
                    "last_run_at": None,
                    "enabled": 1,
                }
            ]
        ),
        workspace_dir=Path("."),
    )

    async def _start_worker_async(self, **kwargs):
        started_calls.append(kwargs)
        return {"status": "started", "run_id": "run-1", "worker_id": "run-1"}

    monkeypatch.setattr(octo_core.Octo, "_start_worker_async", _start_worker_async)

    octo = Octo(
        provider=object(),
        store=_StoreStub(),
        policy=object(),
        runtime=_RuntimeStub(),
        approvals=_ApprovalsStub(),
        memory=_MemoryStub(),
        canon=SimpleNamespace(workspace_dir=Path(".")),
        scheduler=scheduler,
    )

    summary = await octo._dispatch_due_scheduled_tasks_once(chat_id=0, max_tasks=5)

    assert summary == {
        "due_count": 1,
        "attempted": 1,
        "started": 1,
        "completed": 0,
        "duplicates": 0,
        "rejected_by_policy": 0,
        "policy_reasons": {},
        "errors": 0,
    }
    assert started_calls == [
        {
            "worker_id": "writer",
            "task": "Generate digest",
            "chat_id": 0,
            "inputs": {"section": "news"},
            "tools": None,
            "model": None,
            "timeout_seconds": None,
            "scheduled_task_id": "daily_digest",
        }
    ]


@pytest.mark.asyncio
async def test_octo_dispatch_due_scheduled_tasks_targets_single_configured_chat(monkeypatch):
    started_calls = []
    scheduler = SchedulerService(
        store=_StoreStub(
            tasks=[
                {
                    "id": "daily_digest",
                    "name": "Daily Digest",
                    "description": "Build digest",
                    "frequency": "Every 30 minutes",
                    "worker_id": "writer",
                    "task_text": "Generate digest",
                    "inputs_json": "{}",
                    "metadata_json": json.dumps(
                        {"notify_user": "if_significant", "execution_mode": "worker"}
                    ),
                    "last_run_at": None,
                    "enabled": 1,
                }
            ]
        ),
        workspace_dir=Path("."),
    )

    async def _start_worker_async(self, **kwargs):
        started_calls.append(kwargs)
        return {"status": "started", "run_id": "run-1", "worker_id": "run-1"}

    monkeypatch.setattr(octo_core.Octo, "_start_worker_async", _start_worker_async)

    octo = Octo(
        provider=object(),
        store=_StoreStub(),
        policy=object(),
        runtime=_RuntimeStub(),
        approvals=_ApprovalsStub(),
        memory=_MemoryStub(),
        canon=SimpleNamespace(workspace_dir=Path(".")),
        scheduler=scheduler,
    )
    octo._scheduled_delivery_chat_ids = [123]

    summary = await octo._dispatch_due_scheduled_tasks_once(chat_id=0, max_tasks=5)

    assert summary["started"] == 1
    assert summary["rejected_by_policy"] == 0
    assert started_calls[0]["chat_id"] == 123
    assert started_calls[0]["scheduled_task_id"] == "daily_digest"


@pytest.mark.asyncio
async def test_octo_dispatch_due_scheduled_tasks_uses_explicit_delivery_chat(monkeypatch):
    started_calls = []
    scheduler = SchedulerService(
        store=_StoreStub(
            tasks=[
                {
                    "id": "daily_digest",
                    "name": "Daily Digest",
                    "description": "Build digest",
                    "frequency": "Every 30 minutes",
                    "worker_id": "writer",
                    "task_text": "Generate digest",
                    "inputs_json": "{}",
                    "metadata_json": json.dumps(
                        {
                            "notify_user": "always",
                            "execution_mode": "worker",
                            "delivery_chat_id": "456",
                        }
                    ),
                    "last_run_at": None,
                    "enabled": 1,
                }
            ]
        ),
        workspace_dir=Path("."),
    )

    async def _start_worker_async(self, **kwargs):
        started_calls.append(kwargs)
        return {"status": "started", "run_id": "run-1", "worker_id": "run-1"}

    monkeypatch.setattr(octo_core.Octo, "_start_worker_async", _start_worker_async)

    octo = Octo(
        provider=object(),
        store=_StoreStub(),
        policy=object(),
        runtime=_RuntimeStub(),
        approvals=_ApprovalsStub(),
        memory=_MemoryStub(),
        canon=SimpleNamespace(workspace_dir=Path(".")),
        scheduler=scheduler,
    )
    octo._scheduled_delivery_chat_ids = [123]

    summary = await octo._dispatch_due_scheduled_tasks_once(chat_id=0, max_tasks=5)

    assert summary["started"] == 1
    assert started_calls[0]["chat_id"] == 456


@pytest.mark.asyncio
async def test_octo_dispatch_due_scheduled_tasks_rejects_user_visible_without_target(monkeypatch):
    started_calls = []
    scheduler = SchedulerService(
        store=_StoreStub(
            tasks=[
                {
                    "id": "daily_digest",
                    "name": "Daily Digest",
                    "description": "Build digest",
                    "frequency": "Every 30 minutes",
                    "worker_id": "writer",
                    "task_text": "Generate digest",
                    "inputs_json": "{}",
                    "metadata_json": json.dumps(
                        {"notify_user": "always", "execution_mode": "worker"}
                    ),
                    "last_run_at": None,
                    "enabled": 1,
                }
            ]
        ),
        workspace_dir=Path("."),
    )

    async def _start_worker_async(self, **kwargs):
        started_calls.append(kwargs)
        return {"status": "started", "run_id": "run-1", "worker_id": "run-1"}

    monkeypatch.setattr(octo_core.Octo, "_start_worker_async", _start_worker_async)

    octo = Octo(
        provider=object(),
        store=_StoreStub(),
        policy=object(),
        runtime=_RuntimeStub(),
        approvals=_ApprovalsStub(),
        memory=_MemoryStub(),
        canon=SimpleNamespace(workspace_dir=Path(".")),
        scheduler=scheduler,
    )
    octo._scheduled_delivery_chat_ids = [123, 456]

    summary = await octo._dispatch_due_scheduled_tasks_once(chat_id=0, max_tasks=5)

    assert summary == {
        "due_count": 1,
        "attempted": 0,
        "started": 0,
        "completed": 0,
        "duplicates": 0,
        "rejected_by_policy": 1,
        "policy_reasons": {"missing_delivery_target": 1},
        "errors": 0,
    }
    assert started_calls == []


@pytest.mark.asyncio
async def test_octo_dispatch_due_scheduled_tasks_runs_octo_control_tasks(monkeypatch):
    scheduler = SchedulerService(
        store=_StoreStub(
            tasks=[
                {
                    "id": "memory_compact",
                    "name": "Memory Compact",
                    "description": "Compact memory",
                    "frequency": "Every 30 minutes",
                    "worker_id": None,
                    "task_text": "Compact memory",
                    "inputs_json": "{}",
                    "metadata_json": json.dumps({"notify_user": "never"}),
                    "last_run_at": None,
                    "enabled": 1,
                }
            ]
        ),
        workspace_dir=Path("."),
    )

    async def _start_worker_async(self, **kwargs):
        raise AssertionError("_start_worker_async should not be called for octo_control tasks")

    monkeypatch.setattr(octo_core.Octo, "_start_worker_async", _start_worker_async)
    monkeypatch.setattr(
        octo_router,
        "route_scheduled_octo_control",
        lambda octo, task, *, chat_id=0: asyncio.sleep(0, result="SCHEDULED_TASK_DONE"),
    )
    monkeypatch.setattr(
        octo_core,
        "route_scheduled_octo_control",
        lambda octo, task, *, chat_id=0: asyncio.sleep(0, result="SCHEDULED_TASK_DONE"),
    )

    octo = Octo(
        provider=object(),
        store=_StoreStub(),
        policy=object(),
        runtime=_RuntimeStub(),
        approvals=_ApprovalsStub(),
        memory=_MemoryStub(),
        canon=SimpleNamespace(workspace_dir=Path(".")),
        scheduler=scheduler,
    )

    summary = await octo._dispatch_due_scheduled_tasks_once(chat_id=0, max_tasks=5)

    assert summary == {
        "due_count": 1,
        "attempted": 1,
        "started": 0,
        "completed": 1,
        "duplicates": 0,
        "rejected_by_policy": 0,
        "policy_reasons": {},
        "errors": 0,
    }
    assert scheduler.store.marked_task_ids == ["memory_compact"]


@pytest.mark.asyncio
async def test_octo_dispatch_due_scheduled_tasks_requires_explicit_octo_control_completion_signal(monkeypatch):
    scheduler = SchedulerService(
        store=_StoreStub(
            tasks=[
                {
                    "id": "memory_compact",
                    "name": "Memory Compact",
                    "description": "Compact memory",
                    "frequency": "Every 30 minutes",
                    "worker_id": None,
                    "task_text": "Compact memory",
                    "inputs_json": "{}",
                    "metadata_json": json.dumps({"notify_user": "never"}),
                    "last_run_at": None,
                    "enabled": 1,
                }
            ]
        ),
        workspace_dir=Path("."),
    )

    async def _start_worker_async(self, **kwargs):
        raise AssertionError("_start_worker_async should not be called for octo_control tasks")

    monkeypatch.setattr(octo_core.Octo, "_start_worker_async", _start_worker_async)
    monkeypatch.setattr(
        octo_router,
        "route_scheduled_octo_control",
        lambda octo, task, *, chat_id=0: asyncio.sleep(0, result="NO_USER_RESPONSE"),
    )
    monkeypatch.setattr(
        octo_core,
        "route_scheduled_octo_control",
        lambda octo, task, *, chat_id=0: asyncio.sleep(0, result="NO_USER_RESPONSE"),
    )

    octo = Octo(
        provider=object(),
        store=_StoreStub(),
        policy=object(),
        runtime=_RuntimeStub(),
        approvals=_ApprovalsStub(),
        memory=_MemoryStub(),
        canon=SimpleNamespace(workspace_dir=Path(".")),
        scheduler=scheduler,
    )

    summary = await octo._dispatch_due_scheduled_tasks_once(chat_id=0, max_tasks=5)

    assert summary == {
        "due_count": 1,
        "attempted": 1,
        "started": 0,
        "completed": 0,
        "duplicates": 0,
        "rejected_by_policy": 0,
        "policy_reasons": {},
        "errors": 1,
    }
    assert scheduler.store.marked_task_ids == []


@pytest.mark.asyncio
async def test_octo_dispatch_due_scheduled_tasks_backs_off_blocked_octo_control_task(monkeypatch):
    store = _StoreStub(
        tasks=[
            {
                "id": "weather_check",
                "name": "Weather Check",
                "description": "Check weather",
                "frequency": "Every 30 minutes",
                "worker_id": None,
                "task_text": "Check the weather",
                "inputs_json": "{}",
                "metadata_json": json.dumps({"notify_user": "never"}),
                "last_run_at": None,
                "enabled": 1,
            }
        ]
    )
    scheduler = SchedulerService(store=store, workspace_dir=Path("."))
    route_calls = {"count": 0}

    async def _start_worker_async(self, **kwargs):
        raise AssertionError("_start_worker_async should not be called for octo_control tasks")

    async def _route_scheduled_octo_control(octo, task, *, chat_id=0):
        route_calls["count"] += 1
        return (
            "This task requires external network access which cannot be performed "
            "from the bounded `octo_control` route."
        )

    monkeypatch.setattr(octo_core.Octo, "_start_worker_async", _start_worker_async)
    monkeypatch.setattr(octo_router, "route_scheduled_octo_control", _route_scheduled_octo_control)
    monkeypatch.setattr(octo_core, "route_scheduled_octo_control", _route_scheduled_octo_control)

    octo = Octo(
        provider=object(),
        store=_StoreStub(),
        policy=object(),
        runtime=_RuntimeStub(),
        approvals=_ApprovalsStub(),
        memory=_MemoryStub(),
        canon=SimpleNamespace(workspace_dir=Path(".")),
        scheduler=scheduler,
    )

    first_summary = await octo._dispatch_due_scheduled_tasks_once(chat_id=0, max_tasks=5)
    second_summary = await octo._dispatch_due_scheduled_tasks_once(chat_id=0, max_tasks=5)

    assert first_summary == {
        "due_count": 1,
        "attempted": 1,
        "started": 0,
        "completed": 0,
        "duplicates": 0,
        "rejected_by_policy": 0,
        "policy_reasons": {},
        "errors": 1,
    }
    assert second_summary == {
        "due_count": 0,
        "attempted": 0,
        "started": 0,
        "completed": 0,
        "duplicates": 0,
        "rejected_by_policy": 0,
        "policy_reasons": {},
        "errors": 0,
    }
    assert route_calls["count"] == 1
    assert scheduler.store.marked_task_ids == []
    metadata = store.metadata_updates[-1]["metadata"]
    assert isinstance(metadata, dict)
    assert metadata["blocked_reason"] == "blocked_by_route"
    assert "blocked_until" in metadata
    assert metadata["suggested_execution_mode"] == "worker"


@pytest.mark.asyncio
async def test_octo_dispatch_due_scheduled_tasks_skips_persisted_octo_control_backoff(monkeypatch):
    blocked_until = utc_now() + timedelta(minutes=30)
    scheduler = SchedulerService(
        store=_StoreStub(
            tasks=[
                {
                    "id": "weather_check",
                    "name": "Weather Check",
                    "description": "Check weather",
                    "frequency": "Every 30 minutes",
                    "worker_id": None,
                    "task_text": "Check the weather",
                    "inputs_json": "{}",
                    "metadata_json": json.dumps(
                        {
                            "notify_user": "never",
                            "execution_mode": "octo_control",
                            "blocked_until": blocked_until.isoformat(),
                            "blocked_reason": "blocked_by_route",
                        }
                    ),
                    "last_run_at": None,
                    "enabled": 1,
                }
            ]
        ),
        workspace_dir=Path("."),
    )
    route_calls = {"count": 0}

    async def _start_worker_async(self, **kwargs):
        raise AssertionError("_start_worker_async should not be called for octo_control tasks")

    async def _route_scheduled_octo_control(octo, task, *, chat_id=0):
        route_calls["count"] += 1
        return "SCHEDULED_TASK_DONE"

    monkeypatch.setattr(octo_core.Octo, "_start_worker_async", _start_worker_async)
    monkeypatch.setattr(octo_router, "route_scheduled_octo_control", _route_scheduled_octo_control)
    monkeypatch.setattr(octo_core, "route_scheduled_octo_control", _route_scheduled_octo_control)

    octo = Octo(
        provider=object(),
        store=_StoreStub(),
        policy=object(),
        runtime=_RuntimeStub(),
        approvals=_ApprovalsStub(),
        memory=_MemoryStub(),
        canon=SimpleNamespace(workspace_dir=Path(".")),
        scheduler=scheduler,
    )

    summary = await octo._dispatch_due_scheduled_tasks_once(chat_id=0, max_tasks=5)

    assert summary == {
        "due_count": 0,
        "attempted": 0,
        "started": 0,
        "completed": 0,
        "duplicates": 0,
        "rejected_by_policy": 0,
        "policy_reasons": {},
        "errors": 0,
    }
    assert route_calls["count"] == 0


@pytest.mark.asyncio
async def test_octo_dispatch_due_scheduled_tasks_sends_user_visible_octo_control_update(monkeypatch):
    sent_messages = []
    scheduler = SchedulerService(
        store=_StoreStub(
            tasks=[
                {
                    "id": "daily_digest",
                    "name": "Daily Digest",
                    "description": "Build digest",
                    "frequency": "Every 30 minutes",
                    "worker_id": None,
                    "task_text": "Send daily digest",
                    "inputs_json": "{}",
                    "metadata_json": json.dumps(
                        {"notify_user": "always", "execution_mode": "octo_control"}
                    ),
                    "last_run_at": None,
                    "enabled": 1,
                }
            ]
        ),
        workspace_dir=Path("."),
    )

    async def _start_worker_async(self, **kwargs):
        raise AssertionError("_start_worker_async should not be called for octo_control tasks")

    async def _internal_send(chat_id, text):
        sent_messages.append((chat_id, text))

    async def _route_scheduled_octo_control(octo, task, *, chat_id=0):
        assert chat_id == 123
        assert task["id"] == "daily_digest"
        return "<user_visible>Daily digest is ready.</user_visible>"

    monkeypatch.setattr(octo_core.Octo, "_start_worker_async", _start_worker_async)
    monkeypatch.setattr(octo_router, "route_scheduled_octo_control", _route_scheduled_octo_control)
    monkeypatch.setattr(octo_core, "route_scheduled_octo_control", _route_scheduled_octo_control)

    octo = Octo(
        provider=object(),
        store=_StoreStub(),
        policy=object(),
        runtime=_RuntimeStub(),
        approvals=_ApprovalsStub(),
        memory=_MemoryStub(),
        canon=SimpleNamespace(workspace_dir=Path(".")),
        scheduler=scheduler,
        internal_send=_internal_send,
    )

    summary = await octo._dispatch_due_scheduled_tasks_once(chat_id=123, max_tasks=5)

    assert summary == {
        "due_count": 1,
        "attempted": 1,
        "started": 0,
        "completed": 1,
        "duplicates": 0,
        "rejected_by_policy": 0,
        "policy_reasons": {},
        "errors": 0,
    }
    assert sent_messages == [(123, "Daily digest is ready.")]
    assert scheduler.store.marked_task_ids == ["daily_digest"]


@pytest.mark.asyncio
async def test_octo_control_if_significant_is_treated_as_never_for_delivery(monkeypatch):
    sent_messages = []
    scheduler = SchedulerService(
        store=_StoreStub(
            tasks=[
                {
                    "id": "daily_digest",
                    "name": "Daily Digest",
                    "description": "Build digest",
                    "frequency": "Every 30 minutes",
                    "worker_id": None,
                    "task_text": "Send daily digest",
                    "inputs_json": "{}",
                    "metadata_json": json.dumps({"notify_user": "if_significant"}),
                    "last_run_at": None,
                    "enabled": 1,
                }
            ]
        ),
        workspace_dir=Path("."),
    )

    async def _start_worker_async(self, **kwargs):
        raise AssertionError("_start_worker_async should not be called for octo_control tasks")

    async def _internal_send(chat_id, text):
        sent_messages.append((chat_id, text))

    async def _route_scheduled_octo_control(octo, task, *, chat_id=0):
        return "<user_visible>Daily digest is ready.</user_visible>"

    monkeypatch.setattr(octo_core.Octo, "_start_worker_async", _start_worker_async)
    monkeypatch.setattr(octo_router, "route_scheduled_octo_control", _route_scheduled_octo_control)
    monkeypatch.setattr(octo_core, "route_scheduled_octo_control", _route_scheduled_octo_control)

    octo = Octo(
        provider=object(),
        store=_StoreStub(),
        policy=object(),
        runtime=_RuntimeStub(),
        approvals=_ApprovalsStub(),
        memory=_MemoryStub(),
        canon=SimpleNamespace(workspace_dir=Path(".")),
        scheduler=scheduler,
        internal_send=_internal_send,
    )

    summary = await octo._dispatch_due_scheduled_tasks_once(chat_id=123, max_tasks=5)

    assert summary == {
        "due_count": 1,
        "attempted": 1,
        "started": 0,
        "completed": 1,
        "duplicates": 0,
        "rejected_by_policy": 0,
        "policy_reasons": {},
        "errors": 0,
    }
    assert sent_messages == []
    assert scheduler.store.marked_task_ids == ["daily_digest"]


@pytest.mark.asyncio
async def test_octo_background_tasks_start_and_stop_scheduler_loop(monkeypatch):
    started = {
        "cleanup": 0,
        "metrics": 0,
        "scheduler": 0,
    }

    async def _periodic_cleanup(self, interval_seconds):
        started["cleanup"] += 1
        await asyncio.Event().wait()

    async def _periodic_metrics_publish(self, interval_seconds):
        started["metrics"] += 1
        await asyncio.Event().wait()

    async def _periodic_scheduler_tick(self, interval_seconds, *, max_tasks=10):
        started["scheduler"] += 1
        await asyncio.Event().wait()

    monkeypatch.setattr(octo_core.Octo, "_periodic_cleanup", _periodic_cleanup)
    monkeypatch.setattr(octo_core.Octo, "_periodic_metrics_publish", _periodic_metrics_publish)
    monkeypatch.setattr(octo_core.Octo, "_periodic_scheduler_tick", _periodic_scheduler_tick)
    monkeypatch.setattr(octo_core, "get_browser_manager", lambda: _BrowserStub())

    octo = Octo(
        provider=object(),
        store=_StoreStub(),
        policy=object(),
        runtime=_RuntimeStub(),
        approvals=_ApprovalsStub(),
        memory=_MemoryStub(),
        canon=SimpleNamespace(workspace_dir=Path(".")),
        scheduler=SchedulerService(store=_StoreStub(), workspace_dir=Path(".")),
    )

    octo.start_background_tasks(cleanup_interval_seconds=30, scheduler_interval_seconds=15)
    await asyncio.sleep(0)

    assert octo._cleanup_task is not None
    assert octo._metrics_task is not None
    assert octo._scheduler_task is not None
    assert started == {"cleanup": 1, "metrics": 1, "scheduler": 1}

    await octo.stop_background_tasks()

    assert octo._cleanup_task.cancelled() is True
    assert octo._metrics_task.cancelled() is True
    assert octo._scheduler_task.cancelled() is True
