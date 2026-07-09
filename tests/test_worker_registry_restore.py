from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from octopal.runtime.octo.core import Octo


class _DummyRuntime:
    def __init__(self, running_ids: set[str] | None = None) -> None:
        self._running_ids = running_ids or set()

    def is_worker_running(self, worker_id: str) -> bool:
        return worker_id in self._running_ids


class _DummyStore:
    def __init__(self, workers: list[SimpleNamespace]) -> None:
        self._workers = workers
        self.status_updates: list[tuple[str, str]] = []
        self.result_updates: list[tuple[str, str]] = []

    def list_workers(self):
        return list(self._workers)

    def update_worker_status(self, worker_id: str, status: str) -> None:
        self.status_updates.append((worker_id, status))

    def update_worker_result(self, worker_id: str, **kwargs) -> None:
        self.result_updates.append((worker_id, str(kwargs.get("error", ""))))


def _worker(
    worker_id: str,
    *,
    status: str,
    lineage_id: str | None = None,
    parent_worker_id: str | None = None,
    spawn_depth: int = 0,
) -> SimpleNamespace:
    now = datetime.now(UTC)
    return SimpleNamespace(
        id=worker_id,
        status=status,
        task=f"task-{worker_id}",
        granted_caps=[],
        created_at=now,
        updated_at=now,
        summary=None,
        output=None,
        error=None,
        tools_used=[],
        lineage_id=lineage_id,
        parent_worker_id=parent_worker_id,
        root_task_id=worker_id,
        spawn_depth=spawn_depth,
        template_id=None,
        template_name=None,
    )


def test_octo_restores_worker_registry_and_reconciles_stale_children() -> None:
    parent = _worker("parent-1", status="completed", lineage_id="lin-1")
    child = _worker(
        "child-1",
        status="running",
        lineage_id="lin-1",
        parent_worker_id="parent-1",
        spawn_depth=1,
    )
    orphan = _worker(
        "orphan-1",
        status="running",
        lineage_id="lin-x",
        parent_worker_id="missing-parent",
        spawn_depth=1,
    )
    root_active = _worker("root-1", status="running", lineage_id="lin-root")

    store = _DummyStore([parent, child, orphan, root_active])
    octo = Octo(
        provider=object(),
        store=store,  # type: ignore[arg-type]
        policy=object(),
        runtime=_DummyRuntime(running_ids=set()),  # type: ignore[arg-type]
        approvals=object(),  # type: ignore[arg-type]
        memory=object(),  # type: ignore[arg-type]
        canon=object(),  # type: ignore[arg-type]
    )

    assert octo._worker_children.get("parent-1") == {"child-1"}
    assert octo._lineage_children_total.get("lin-1") == 1
    active_set = octo._lineage_children_active.get("lin-1")
    assert active_set is None or active_set == set()

    reconciled_ids = {
        worker_id for worker_id, status in store.status_updates if status == "stopped"
    }
    assert {"child-1", "orphan-1", "root-1"} <= reconciled_ids
