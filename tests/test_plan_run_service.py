from __future__ import annotations

from pathlib import Path

from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.plans import PlanRunService


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
