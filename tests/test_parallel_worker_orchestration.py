from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

from broodmind.store.models import WorkerRecord, WorkerTemplateRecord
from broodmind.tools.worker_tools import _tool_start_workers_parallel, _tool_synthesize_worker_results


def _template(worker_id: str, description: str, tools: list[str], perms: list[str]) -> WorkerTemplateRecord:
    now = datetime.now(UTC)
    return WorkerTemplateRecord(
        id=worker_id,
        name=worker_id.title(),
        description=description,
        system_prompt=description,
        available_tools=tools,
        required_permissions=perms,
        model=None,
        max_thinking_steps=8,
        default_timeout_seconds=120,
        created_at=now,
        updated_at=now,
    )


def test_start_workers_parallel_launches_multiple() -> None:
    templates = [
        _template("web_researcher", "research web topics", ["web_search"], ["network"]),
        _template("coder", "fix code and bugs", ["fs_read"], ["filesystem_read"]),
    ]

    class _Store:
        def list_worker_templates(self):
            return templates

        def get_worker_template(self, worker_id: str):
            for item in templates:
                if item.id == worker_id:
                    return item
            return None

    class _Queen:
        def __init__(self) -> None:
            self.store = _Store()
            self._i = 0

        async def _start_worker_async(self, **kwargs):
            self._i += 1
            run_id = f"run-{self._i}"
            return {"status": "started", "worker_id": run_id, "run_id": run_id, **kwargs}

    async def _scenario() -> dict:
        payload = await _tool_start_workers_parallel(
            {
                "tasks": [
                    {"task": "search docs about apis", "worker_id": "auto"},
                    {"task": "fix python bug in parser", "worker_id": "auto"},
                ],
                "max_parallel": 2,
            },
            {"queen": _Queen(), "chat_id": 123},
        )
        return json.loads(payload)

    result = asyncio.run(_scenario())
    assert result["status"] in {"ok", "partial"}
    assert result["started_count"] == 2
    assert len(result["launches"]) == 2
    assert all(item["worker_id"] for item in result["launches"])


def test_synthesize_worker_results_reports_completed_failed_and_pending() -> None:
    now = datetime.now(UTC)
    records = {
        "w1": WorkerRecord(
            id="w1",
            status="completed",
            task="one",
            granted_caps=[],
            created_at=now,
            updated_at=now,
            summary="Fetched web docs",
            output={"items": 3},
            error=None,
            tools_used=[],
        ),
        "w2": WorkerRecord(
            id="w2",
            status="failed",
            task="two",
            granted_caps=[],
            created_at=now,
            updated_at=now,
            summary=None,
            output=None,
            error="Timeout",
            tools_used=[],
        ),
        "w3": WorkerRecord(
            id="w3",
            status="running",
            task="three",
            granted_caps=[],
            created_at=now,
            updated_at=now,
            summary=None,
            output=None,
            error=None,
            tools_used=[],
        ),
    }

    class _Store:
        def get_worker(self, worker_id: str):
            return records.get(worker_id)

    class _Queen:
        store = _Store()

    payload = _tool_synthesize_worker_results(
        {"worker_ids": ["w1", "w2", "w3", "missing"]},
        {"queen": _Queen()},
    )
    result = json.loads(payload)
    assert result["completed_count"] == 1
    assert result["failed_count"] == 1
    assert result["pending_count"] == 1
    assert result["missing_count"] == 1
    assert "Completed worker findings:" in result["synthesis"]
