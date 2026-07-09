from __future__ import annotations

import json
from datetime import timedelta

from octopal.tools.workers.management import _tool_worker_yield
from octopal.utils import utc_now


class _WorkerStub:
    def __init__(
        self,
        *,
        worker_id: str,
        status: str,
        task: str,
        lineage_id: str | None = None,
        parent_worker_id: str | None = None,
        spawn_depth: int = 0,
        summary: str | None = None,
        error: str | None = None,
    ) -> None:
        now = utc_now()
        self.id = worker_id
        self.status = status
        self.task = task
        self.lineage_id = lineage_id
        self.parent_worker_id = parent_worker_id
        self.root_task_id = lineage_id
        self.spawn_depth = spawn_depth
        self.created_at = now - timedelta(minutes=5)
        self.updated_at = now
        self.summary = summary
        self.error = error


class _RuntimeStub:
    def is_worker_running(self, _worker_id: str) -> bool:
        return True


class _StoreStub:
    def __init__(self) -> None:
        self._workers = {
            "w1": _WorkerStub(
                worker_id="w1", status="running", task="Fetch docs", lineage_id="lin-1"
            ),
            "w2": _WorkerStub(
                worker_id="w2",
                status="running",
                task="Parse page",
                lineage_id="lin-1",
                parent_worker_id="w1",
                spawn_depth=1,
            ),
            "w3": _WorkerStub(
                worker_id="w3",
                status="completed",
                task="Summarize findings",
                lineage_id="lin-2",
                summary="Saved report to workspace/report.md",
            ),
            "w4": _WorkerStub(
                worker_id="w4",
                status="completed",
                task="Rank options",
                lineage_id="lin-2",
                summary="Ranked the candidate list",
            ),
            "w5": _WorkerStub(
                worker_id="w5",
                status="failed",
                task="Draft email",
                lineage_id="lin-2",
                error="provider timeout",
            ),
        }

    def get_worker(self, worker_id: str):
        return self._workers.get(worker_id)

    def get_active_workers(self, older_than_minutes: int = 10):
        del older_than_minutes
        return [self._workers["w1"], self._workers["w2"]]

    def list_recent_workers(self, limit: int = 100):
        return list(self._workers.values())[:limit]


class _OctoStub:
    def __init__(self) -> None:
        self.store = _StoreStub()
        self.runtime = _RuntimeStub()


def test_worker_yield_recommends_followup_when_runs_are_still_active() -> None:
    payload = json.loads(_tool_worker_yield({"worker_ids": ["w1", "w2"]}, {"octo": _OctoStub()}))

    assert payload["status"] == "ok"
    assert payload["mode"] == "yield"
    assert payload["followup_required"] is True
    assert payload["next_best_action"] == "wait_for_worker_progress"
    assert payload["pending_count"] == 2
    assert payload["completed_count"] == 0
    assert any("still in flight" in hint for hint in payload["hints"])


def test_worker_yield_recommends_synthesis_when_parallel_results_are_ready() -> None:
    payload = json.loads(
        _tool_worker_yield(
            {"worker_ids": ["w3", "w4", "w5"], "lineage_id": "lin-2"}, {"octo": _OctoStub()}
        )
    )

    assert payload["status"] == "ok"
    assert payload["mode"] == "resume"
    assert payload["followup_required"] is False
    assert payload["synthesize_recommended"] is True
    assert payload["next_best_action"] == "synthesize_worker_results"
    assert payload["completed_count"] == 2
    assert payload["failed_count"] == 1
    assert any("synthesis is the cleanest next step" in hint for hint in payload["hints"])
