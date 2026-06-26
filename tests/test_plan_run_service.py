from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.octo import followup_pipeline
from octopal.runtime.octo.followup_pipeline import (
    _build_runtime_plan_continuation,
    _schedule_runtime_plan_continuation,
    _sync_runtime_plan_with_worker_result,
)
from octopal.runtime.plans import PlanRunService
from octopal.runtime.workers.contracts import WorkerResult


class _StoreSettings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


def _store(tmp_path: Path) -> SQLiteStore:
    return SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))


def test_plan_run_persists_steps_and_events(tmp_path: Path) -> None:
    store = _store(tmp_path)
    service = PlanRunService(store)

    run = service.create_run(
        goal="Check release readiness",
        chat_id=123,
        correlation_id="turn-1",
        steps=[
            {"id": "inspect", "kind": "tool", "title": "Inspect repo"},
            {
                "id": "tests",
                "kind": "worker",
                "title": "Run tests",
                "worker_id": "test_runner",
            },
        ],
    )

    saved = store.get_plan_run(run.id)
    assert saved is not None
    assert saved.goal == "Check release readiness"
    assert saved.current_step_id == "inspect"
    steps = store.get_plan_steps(run.id)
    assert [step.step_id for step in steps] == ["inspect", "tests"]
    assert steps[1].executor == "test_runner"
    events = store.list_plan_events(run.id)
    assert events[0].event_type == "plan.created"
    assert events[0].data["step_count"] == 2


def test_plan_service_advances_to_next_step_and_completes(tmp_path: Path) -> None:
    store = _store(tmp_path)
    service = PlanRunService(store)
    run = service.create_run(
        goal="Do the thing",
        steps=[
            {"id": "one", "kind": "octo", "title": "First"},
            {"id": "two", "kind": "octo", "title": "Second"},
        ],
    )

    service.mark_step_running(run.id, "one")
    service.complete_step(run.id, "one", output={"summary": "done one"})

    mid = store.get_plan_run(run.id)
    assert mid is not None
    assert mid.status == "needs_next_step"
    assert mid.current_step_id == "two"
    first = store.get_plan_steps(run.id)[0]
    assert first.status == "completed"
    assert first.output == {"summary": "done one"}

    service.mark_step_running(run.id, "two")
    service.complete_step(run.id, "two")

    final = store.get_plan_run(run.id)
    assert final is not None
    assert final.status == "completed"
    assert final.completed_at is not None


def test_plan_service_binds_worker_step_for_resume(tmp_path: Path) -> None:
    store = _store(tmp_path)
    service = PlanRunService(store)
    run = service.create_run(
        goal="Research topic",
        steps=[{"id": "research", "kind": "worker", "title": "Research", "worker_id": "web"}],
    )

    service.bind_worker_step(run.id, "research", "worker-123")

    saved = store.get_plan_run(run.id)
    assert saved is not None
    assert saved.status == "awaiting_worker"
    assert saved.current_step_id == "research"
    step = store.get_plan_steps(run.id)[0]
    assert step.status == "awaiting_worker"
    assert step.worker_run_id == "worker-123"
    assert store.list_plan_events(run.id)[-1].event_type == "step.awaiting_worker"


def test_plan_service_completes_worker_step_from_result(tmp_path: Path) -> None:
    store = _store(tmp_path)
    service = PlanRunService(store)
    run = service.create_run(
        goal="Research topic",
        chat_id=123,
        steps=[
            {"id": "research", "kind": "worker", "title": "Research"},
            {"id": "reply", "kind": "final", "title": "Reply"},
        ],
    )
    service.bind_worker_step(run.id, "research", "worker-123")

    matched = service.update_worker_step_result(
        worker_run_id="worker-123",
        chat_id=123,
        result_status="completed",
        summary="Found the answer.",
        output={"artifact_summary": {"durable_paths": ["reports/research.md"]}},
        tools_used=["web_fetch"],
    )

    assert matched is not None
    saved = store.get_plan_run(run.id)
    assert saved is not None
    assert saved.status == "needs_next_step"
    assert saved.current_step_id == "reply"
    step = store.get_plan_steps(run.id)[0]
    assert step.status == "completed"
    assert step.output["worker_status"] == "completed"
    assert step.output["summary"] == "Found the answer."
    assert step.output["tools_used"] == ["web_fetch"]
    assert step.output["output"] == {"artifact_summary": {"durable_paths": ["reports/research.md"]}}


def test_plan_service_fails_worker_step_from_result(tmp_path: Path) -> None:
    store = _store(tmp_path)
    service = PlanRunService(store)
    run = service.create_run(
        goal="Run checks",
        chat_id=123,
        steps=[{"id": "checks", "kind": "worker", "title": "Run checks"}],
    )
    service.bind_worker_step(run.id, "checks", "worker-123")

    service.update_worker_step_result(
        worker_run_id="worker-123",
        chat_id=123,
        result_status="failed",
        summary="Checks failed.",
        output={"error": "pytest failed"},
    )

    saved = store.get_plan_run(run.id)
    assert saved is not None
    assert saved.status == "failed"
    step = store.get_plan_steps(run.id)[0]
    assert step.status == "failed"
    assert step.error == "pytest failed"
    assert step.output["worker_status"] == "failed"
    assert step.output["output"] == {"error": "pytest failed"}


def test_plan_service_clears_stale_error_when_worker_step_is_retried(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    service = PlanRunService(store)
    run = service.create_run(
        goal="Install a skill",
        chat_id=123,
        steps=[{"id": "test", "kind": "worker", "title": "Test worker"}],
    )
    service.bind_worker_step(run.id, "test", "worker-1")
    service.update_worker_step_result(
        worker_run_id="worker-1",
        chat_id=123,
        result_status="failed",
        summary="Missing permissions.",
        output={"error": "invalid_worker_tool_permissions"},
    )

    failed = store.get_plan_run(run.id)
    assert failed is not None
    assert failed.status == "failed"
    assert failed.completed_at is not None
    failed_step = store.get_plan_steps(run.id)[0]
    assert failed_step.status == "failed"
    assert failed_step.completed_at is not None
    assert failed_step.error == "invalid_worker_tool_permissions"

    service.bind_worker_step(run.id, "test", "worker-2")
    retrying = store.get_plan_run(run.id)
    assert retrying is not None
    assert retrying.status == "awaiting_worker"
    assert retrying.completed_at is None
    retrying_step = store.get_plan_steps(run.id)[0]
    assert retrying_step.completed_at is None

    service.update_worker_step_result(
        worker_run_id="worker-2",
        chat_id=123,
        result_status="completed",
        summary="Worker succeeded.",
        output={"findings": 8},
    )

    step = store.get_plan_steps(run.id)[0]
    assert step.status == "completed"
    assert step.error is None
    assert step.output["worker_status"] == "completed"
    assert step.output["summary"] == "Worker succeeded."


def test_plan_service_keeps_instruction_request_step_active(tmp_path: Path) -> None:
    store = _store(tmp_path)
    service = PlanRunService(store)
    run = service.create_run(
        goal="Investigate",
        chat_id=123,
        steps=[{"id": "ask", "kind": "worker", "title": "Ask worker"}],
    )
    service.bind_worker_step(run.id, "ask", "worker-123")

    service.update_worker_step_result(
        worker_run_id="worker-123",
        chat_id=123,
        result_status="awaiting_instruction",
        summary="Need a decision.",
        output={"instruction_request": {"request_id": "req-1"}},
        questions=["Which path?"],
    )

    saved = store.get_plan_run(run.id)
    assert saved is not None
    assert saved.status == "awaiting_worker"
    step = store.get_plan_steps(run.id)[0]
    assert step.status == "awaiting_worker"
    assert step.output["worker_status"] == "awaiting_instruction"
    assert step.output["questions"] == ["Which path?"]
    assert store.list_plan_events(run.id)[-1].event_type == "step.worker_instruction_requested"


def test_plan_service_keeps_legacy_awaiting_input_runs_active(tmp_path: Path) -> None:
    store = _store(tmp_path)
    service = PlanRunService(store)
    run = service.create_run(
        goal="Ask user for missing detail",
        chat_id=123,
        steps=[{"id": "ask", "kind": "octo", "title": "Ask user"}],
    )
    store.update_plan_run(run.id, status="awaiting_input", current_step_id="ask")
    store.update_plan_step(run.id, "ask", status="awaiting_input")

    active = service.active_runs_for_chat(123)
    snapshot = service.get_snapshot(run.id)

    assert [item.id for item in active] == [run.id]
    assert active[0].status == "awaiting_user"
    assert snapshot is not None
    assert snapshot["run"]["status"] == "awaiting_user"
    assert snapshot["steps"][0]["status"] == "awaiting_user"


def test_followup_pipeline_syncs_runtime_plan_before_routing(tmp_path: Path) -> None:
    store = _store(tmp_path)
    service = PlanRunService(store)
    run = service.create_run(
        goal="Collect data",
        chat_id=42,
        steps=[
            {"id": "collect", "kind": "worker", "title": "Collect"},
            {"id": "summarize", "kind": "final", "title": "Summarize"},
        ],
    )
    service.bind_worker_step(run.id, "collect", "worker-123")

    _sync_runtime_plan_with_worker_result(
        SimpleNamespace(store=store),
        42,
        worker_id="worker-123",
        result=WorkerResult(status="completed", summary="Data collected."),
    )

    saved = store.get_plan_run(run.id)
    assert saved is not None
    assert saved.status == "needs_next_step"
    assert saved.current_step_id == "summarize"


def test_followup_pipeline_builds_continuation_for_next_plan_step(tmp_path: Path) -> None:
    store = _store(tmp_path)
    service = PlanRunService(store)
    run = service.create_run(
        goal="Publish and verify",
        chat_id=42,
        steps=[
            {"id": "publish", "kind": "worker", "title": "Publish"},
            {
                "id": "verify",
                "kind": "octo",
                "title": "Verify published post",
                "task": "Check profile visibility and close the plan.",
            },
        ],
    )
    service.bind_worker_step(run.id, "publish", "worker-123")

    synced = _sync_runtime_plan_with_worker_result(
        SimpleNamespace(store=store),
        42,
        worker_id="worker-123",
        result=WorkerResult(status="completed", summary="Worker hit its step limit."),
    )

    continuation = _build_runtime_plan_continuation(
        synced,
        worker_id="worker-123",
        result=WorkerResult(status="completed", summary="Worker hit its step limit."),
        notify_user=None,
    )

    assert continuation is not None
    assert continuation["run_id"] == run.id
    assert continuation["step_id"] == "verify"
    assert continuation["notify_policy"] == "always"
    assert continuation["notify_user"] is True
    assert f"runtime plan `{run.id}`" in continuation["args"]["task"]
    assert "Check profile visibility" in continuation["args"]["task"]
    assert "Worker hit its step limit" in continuation["args"]["context_summary"]


def test_followup_pipeline_preserves_if_significant_continuation_notify_policy(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    service = PlanRunService(store)
    run = service.create_run(
        goal="Publish and verify",
        chat_id=42,
        steps=[
            {"id": "publish", "kind": "worker", "title": "Publish"},
            {"id": "verify", "kind": "octo", "title": "Verify"},
        ],
    )
    service.bind_worker_step(run.id, "publish", "worker-123")
    synced = _sync_runtime_plan_with_worker_result(
        SimpleNamespace(store=store),
        42,
        worker_id="worker-123",
        result=WorkerResult(status="completed", summary="Published."),
    )

    continuation = _build_runtime_plan_continuation(
        synced,
        worker_id="worker-123",
        result=WorkerResult(status="completed", summary="Published."),
        notify_user="if_significant",
    )

    assert continuation is not None
    assert continuation["notify_policy"] == "if_significant"
    assert continuation["notify_user"] is True
    assert continuation["args"]["notify_user"] is True


def test_followup_pipeline_schedules_continuation_for_needs_next_step(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    service = PlanRunService(store)
    run = service.create_run(
        goal="Collect and summarize",
        chat_id=42,
        steps=[
            {"id": "collect", "kind": "worker", "title": "Collect"},
            {"id": "summarize", "kind": "final", "title": "Summarize"},
        ],
    )
    service.bind_worker_step(run.id, "collect", "worker-123")
    synced = _sync_runtime_plan_with_worker_result(
        SimpleNamespace(store=store),
        42,
        worker_id="worker-123",
        result=WorkerResult(status="completed", summary="Data collected."),
    )
    calls: list[dict[str, object]] = []

    async def fake_run(octo, chat_id, args, *, notify_policy):
        calls.append(
            {
                "octo": octo,
                "chat_id": chat_id,
                "args": args,
                "notify_policy": notify_policy,
            }
        )
        return {"status": "continued", "delivered": notify_policy != "never"}

    monkeypatch.setattr(followup_pipeline, "_run_runtime_plan_continuation", fake_run)

    async def scenario() -> None:
        scheduled = _schedule_runtime_plan_continuation(
            SimpleNamespace(store=store),
            42,
            synced,
            worker_id="worker-123",
            task_text="Collect",
            result=WorkerResult(status="completed", summary="Data collected."),
            notify_user=None,
            correlation_id="corr-1",
        )
        assert scheduled is True
        await asyncio.sleep(0)

    asyncio.run(scenario())

    assert len(calls) == 1
    assert calls[0]["chat_id"] == 42
    assert calls[0]["notify_policy"] == "always"
    assert "summarize" in str(calls[0]["args"])


def test_followup_pipeline_runtime_continuation_downgrades_if_significant_to_silent_and_refreshes_metrics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import octopal.tools.catalog as catalog

    store = _store(tmp_path)
    seen: dict[str, object] = {}
    refreshed: list[object] = []

    async def fake_tool(args, ctx):
        seen["args"] = args
        seen["ctx"] = ctx
        return json.dumps({"status": "continued", "delivered": True})

    def fake_publish_runtime_metrics(thinking_count: int = 0, *, octo=None) -> None:
        refreshed.append(octo)

    monkeypatch.setattr(catalog, "_tool_octo_continue_from_control_route", fake_tool)
    monkeypatch.setattr(followup_pipeline, "_publish_runtime_metrics", fake_publish_runtime_metrics)

    payload = asyncio.run(
        followup_pipeline._run_runtime_plan_continuation(
            SimpleNamespace(store=store),
            42,
            {"task": "Continue plan", "context_summary": "Worker finished.", "notify_user": True},
            notify_policy="if_significant",
        )
    )

    assert payload == {"status": "continued", "delivered": True}
    assert isinstance(seen["ctx"], dict)
    assert seen["ctx"]["control_route_notify_user"] == "never"
    assert len(refreshed) == 1


def test_followup_pipeline_falls_back_to_worker_followup_when_continuation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    service = PlanRunService(store)
    run = service.create_run(
        goal="Collect and summarize",
        chat_id=42,
        steps=[
            {"id": "collect", "kind": "worker", "title": "Collect"},
            {"id": "summarize", "kind": "final", "title": "Summarize"},
        ],
    )
    service.bind_worker_step(run.id, "collect", "worker-123")
    result = WorkerResult(status="completed", summary="Data collected.")
    synced = _sync_runtime_plan_with_worker_result(
        SimpleNamespace(store=store),
        42,
        worker_id="worker-123",
        result=result,
    )
    fallbacks: list[dict[str, object]] = []

    async def fake_run(octo, chat_id, args, *, notify_policy):
        return {"status": "error", "delivered": False}

    monkeypatch.setattr(followup_pipeline, "_run_runtime_plan_continuation", fake_run)

    async def scenario() -> None:
        fallback_ready = asyncio.Event()

        async def fake_enqueue(
            octo, chat_id, correlation_id, *, worker_id, task_text, result, notify_user
        ):
            fallbacks.append(
                {
                    "octo": octo,
                    "chat_id": chat_id,
                    "correlation_id": correlation_id,
                    "worker_id": worker_id,
                    "task_text": task_text,
                    "result": result,
                    "notify_user": notify_user,
                }
            )
            fallback_ready.set()

        monkeypatch.setattr(followup_pipeline, "_enqueue_batched_worker_followup", fake_enqueue)
        octo = SimpleNamespace(
            store=store,
            should_suppress_channel_followups=lambda correlation_id: False,
            channel_followup_suppression_reason=lambda correlation_id: None,
        )

        scheduled = _schedule_runtime_plan_continuation(
            octo,
            42,
            synced,
            worker_id="worker-123",
            task_text="Collect",
            result=result,
            notify_user=None,
            correlation_id="corr-1",
        )
        assert scheduled is True
        await asyncio.wait_for(fallback_ready.wait(), timeout=1)

    asyncio.run(scenario())

    assert len(fallbacks) == 1
    assert fallbacks[0]["chat_id"] == 42
    assert fallbacks[0]["correlation_id"] == "corr-1"
    assert fallbacks[0]["worker_id"] == "worker-123"
    assert fallbacks[0]["task_text"] == "Collect"
    assert fallbacks[0]["notify_user"] is None


def test_followup_pipeline_skips_stale_runtime_plan_continuation_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []
    cleared: list[str | None] = []

    async def fake_enqueue(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(followup_pipeline, "_enqueue_batched_worker_followup", fake_enqueue)
    octo = SimpleNamespace(
        should_suppress_channel_followups=lambda correlation_id: False,
        channel_followup_suppression_reason=lambda correlation_id: None,
        is_correlation_current_for_chat=lambda chat_id, correlation_id: False,
        clear_pending_conversational_closure=lambda correlation_id: cleared.append(correlation_id),
    )

    asyncio.run(
        followup_pipeline._enqueue_runtime_plan_continuation_fallback(
            octo,
            42,
            "old-turn",
            worker_id="worker-123",
            task_text="Collect",
            result=WorkerResult(status="completed", summary="Data collected."),
            notify_user=None,
            reason="status=stale",
        )
    )

    assert calls == []
    assert cleared == ["old-turn"]
