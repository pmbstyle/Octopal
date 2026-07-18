from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.scheduler.service import SchedulerService
from octopal.utils import utc_now


class _StoreSettings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


def _service(tmp_path: Path) -> tuple[SQLiteStore, SchedulerService]:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    store.upsert_scheduled_task(
        task_id="daily_digest",
        name="Daily digest",
        frequency="Every 30 minutes",
        task_text="Generate the daily digest.",
        worker_id="writer",
        metadata={"execution_mode": "worker", "notify_user": "never"},
    )
    return store, SchedulerService(store=store, workspace_dir=tmp_path / "workspace")


def test_scheduler_claim_is_atomic_and_blocks_another_runtime(tmp_path: Path) -> None:
    store, service = _service(tmp_path)
    task = service.get_actionable_tasks()[0]

    claimed = service.claim_due_task(task, lease_owner="runtime-a")

    assert claimed is not None
    assert claimed["attempt_id"]
    assert claimed["idempotency_key"]
    assert service.get_actionable_tasks() == []
    assert not store.claim_scheduled_task(
        "daily_digest",
        lease_owner="runtime-b",
        lease_expires_at=utc_now() + timedelta(hours=1),
        attempt_id="other-attempt",
        idempotency_key="daily_digest:other-attempt",
        started_at=utc_now(),
        expected_last_run_at=task.get("last_run_at"),
        expected_next_run_at=task.get("next_run_at"),
    )

    row = store.get_scheduled_tasks()[0]
    assert row["attempt_no"] == 1
    assert row["last_outcome"] == "running"
    assert row["lease_owner"] == "runtime-a"

    service.mark_executed("daily_digest", attempt_id=claimed["attempt_id"])
    assert service.claim_due_task(task, lease_owner="runtime-b") is None


def test_scheduler_attempt_records_outcome_and_retry_deadline(tmp_path: Path) -> None:
    store, service = _service(tmp_path)
    claimed = service.claim_due_task(service.get_actionable_tasks()[0], lease_owner="runtime-a")
    assert claimed is not None
    first_idempotency_key = claimed["idempotency_key"]

    service.fail_attempt(
        "daily_digest",
        attempt_id=claimed["attempt_id"],
        error_class="worker_duplicate",
    )

    failed = store.get_scheduled_tasks()[0]
    assert failed["lease_owner"] is None
    assert failed["last_outcome"] == "failed"
    assert failed["last_error_class"] == "worker_duplicate"
    assert failed["next_run_at"] is not None
    assert service.get_actionable_tasks() == []
    assert service.describe_tasks()[0]["next_run_at"] == failed["next_run_at"]

    store._conn.execute(
        "UPDATE scheduled_tasks SET next_run_at = ? WHERE id = ?",
        (utc_now().isoformat(), "daily_digest"),
    )
    store._conn.commit()
    retried = service.claim_due_task(service.get_actionable_tasks()[0], lease_owner="runtime-a")
    assert retried is not None
    assert retried["idempotency_key"] == first_idempotency_key

    service.mark_executed("daily_digest", attempt_id=retried["attempt_id"])

    completed = store.get_scheduled_tasks()[0]
    assert completed["lease_owner"] is None
    assert completed["last_outcome"] == "completed"
    assert completed["last_error_class"] is None
    assert completed["last_run_at"] is not None
    assert completed["next_run_at"] is not None
    assert completed["idempotency_key"] == first_idempotency_key
    assert service.get_actionable_tasks() == []

    store._conn.execute(
        "UPDATE scheduled_tasks SET next_run_at = ? WHERE id = ?",
        (utc_now().isoformat(), "daily_digest"),
    )
    store._conn.commit()
    next_occurrence = service.claim_due_task(
        service.get_actionable_tasks()[0],
        lease_owner="runtime-a",
    )
    assert next_occurrence is not None
    assert next_occurrence["idempotency_key"] != first_idempotency_key

    store.upsert_scheduled_task(
        task_id="daily_digest",
        name="Daily digest",
        frequency="Every 5 minutes",
        task_text="Generate the daily digest.",
        worker_id="writer",
        metadata={"execution_mode": "worker", "notify_user": "never"},
    )
    assert store.get_scheduled_tasks()[0]["next_run_at"] is None
    assert store.get_scheduled_tasks()[0]["idempotency_key"] is None
