from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from octopal.infrastructure.store.models import WorkerRecord, WorkerTemplateRecord
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.plans import PlanRunService
from octopal.runtime.workers.contracts import WorkerSpec
from octopal.runtime.workers.runtime import (
    _allows_injected_worker_tool,
    _merge_existing_orchestration_plan_output,
)
from octopal.tools.workers.management import (
    _tool_list_workers,
    _tool_orchestration_plan_create,
    _tool_orchestration_plan_update_item,
    _tool_start_workers_parallel,
    _tool_synthesize_worker_results,
    sync_orchestration_plan_with_child_batch,
)


def _template(
    worker_id: str,
    description: str,
    tools: list[str],
    perms: list[str],
    *,
    can_spawn_children: bool = False,
    allowed_child_templates: list[str] | None = None,
) -> WorkerTemplateRecord:
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
        can_spawn_children=can_spawn_children,
        allowed_child_templates=allowed_child_templates or [],
        created_at=now,
        updated_at=now,
    )


class _StoreSettings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


def _sqlite_store(tmp_path: Path) -> SQLiteStore:
    return SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))


def test_list_workers_returns_compact_capability_payload() -> None:
    templates = [
        _template("web_researcher", "research web topics", ["web_search"], ["network"]),
        _template(
            "research_coordinator",
            "split research across child workers",
            ["list_workers", "start_workers_parallel"],
            ["worker_manage"],
            can_spawn_children=True,
            allowed_child_templates=["web_researcher", "repo_researcher"],
        ),
    ]

    class _Store:
        def list_worker_templates(self):
            return templates

    class _Octo:
        store = _Store()

    payload = _tool_list_workers({}, {"octo": _Octo()})
    result = json.loads(payload)

    assert result["count"] == 2
    assert "available_tools" not in payload
    assert "allowed_child_templates" not in payload
    leaf, parent = result["workers"]
    assert leaf == {
        "worker_id": "web_researcher",
        "name": "Web_Researcher",
        "description": "research web topics",
        "tools": ["web_search"],
        "permissions": ["network"],
        "timeout_seconds": 120,
    }
    assert parent["tools"] == ["list_workers", "start_workers_parallel"]
    assert parent["permissions"] == ["worker_manage"]
    assert parent["can_spawn_children"] is True
    assert parent["children"] == ["web_researcher", "repo_researcher"]


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

    class _Octo:
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
                    {"task": "search docs about apis", "worker_id": "web_researcher"},
                    {"task": "fix python bug in parser", "worker_id": "coder"},
                ],
                "max_parallel": 2,
            },
            {"octo": _Octo(), "chat_id": 123},
        )
        return json.loads(payload)

    result = asyncio.run(_scenario())
    assert result["status"] in {"ok", "partial"}
    assert result["started_count"] == 2
    assert result["followup_required"] is True
    assert result["next_best_action"] == "wait_for_worker_progress"
    assert len(result["launches"]) == 2
    assert all(item["worker_id"] for item in result["launches"])


def test_start_workers_parallel_rejects_tools_outside_template_allowlist() -> None:
    templates = [
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

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()

        async def _start_worker_async(self, **kwargs):
            raise AssertionError("worker launch should have been rejected")

    async def _scenario() -> dict:
        payload = await _tool_start_workers_parallel(
            {
                "tasks": [
                    {
                        "task": "Inspect parser",
                        "worker_id": "coder",
                        "tools": ["fs_read", "exec_run"],
                    },
                ],
                "max_parallel": 1,
            },
            {"octo": _Octo(), "chat_id": 123},
        )
        return json.loads(payload)

    result = asyncio.run(_scenario())

    assert result["status"] == "partial"
    assert result["started_count"] == 0
    assert result["failed_count"] == 1
    assert result["followup_required"] is False
    assert result["next_best_action"] == "continue_current_plan"
    assert result["launches"][0]["status"] == "error"
    assert "requested tools exceed template contract" in result["launches"][0]["error"]
    assert "exec_run" in result["launches"][0]["error"]


def test_start_workers_parallel_forwards_allowed_paths_per_task() -> None:
    templates = [
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

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()
            self.launches: list[dict[str, object]] = []

        async def _start_worker_async(self, **kwargs):
            self.launches.append(kwargs)
            run_id = f"run-{len(self.launches)}"
            return {**kwargs, "status": "started", "worker_id": run_id, "run_id": run_id}

    octo = _Octo()

    async def _scenario() -> dict:
        payload = await _tool_start_workers_parallel(
            {
                "tasks": [
                    {"task": "fix parser bug", "worker_id": "coder"},
                    {
                        "task": "edit shared module",
                        "worker_id": "coder",
                        "allowed_paths": ["src/parser.py"],
                    },
                ],
            },
            {"octo": octo, "chat_id": 123},
        )
        return json.loads(payload)

    result = asyncio.run(_scenario())
    assert result["started_count"] == 2
    assert result["followup_required"] is True
    assert result["next_best_action"] == "wait_for_worker_progress"
    assert octo.launches[0]["allowed_paths"] is None
    assert octo.launches[1]["allowed_paths"] == ["src/parser.py"]


def test_start_workers_parallel_passes_null_model_to_runtime() -> None:
    templates = [
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

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()
            self.launches: list[dict[str, object]] = []

        async def _start_worker_async(self, **kwargs):
            self.launches.append(kwargs)
            run_id = f"run-{len(self.launches)}"
            return {**kwargs, "status": "started", "worker_id": run_id, "run_id": run_id}

    octo = _Octo()

    async def _scenario() -> dict:
        payload = await _tool_start_workers_parallel(
            {
                "tasks": [
                    {"task": "fix parser bug", "worker_id": "coder", "model": "gpt-4o"},
                ],
            },
            {"octo": octo, "chat_id": 123},
        )
        return json.loads(payload)

    result = asyncio.run(_scenario())
    assert result["started_count"] == 1
    assert result["followup_required"] is True
    assert result["next_best_action"] == "wait_for_worker_progress"
    assert octo.launches[0]["model"] is None


def test_start_workers_parallel_binds_valid_plan_steps_and_rejects_invalid_ones(
    tmp_path: Path,
) -> None:
    plan_store = _sqlite_store(tmp_path)
    template = _template("coder", "fix code and bugs", ["fs_read"], ["filesystem_read"])
    plan = PlanRunService(plan_store).create_run(
        goal="Patch two areas",
        chat_id=123,
        steps=[
            {"id": "parser", "kind": "worker", "title": "Patch parser"},
            {"id": "tests", "kind": "worker", "title": "Patch tests"},
        ],
    )

    class _Store:
        def list_worker_templates(self):
            return [template]

        def get_worker_template(self, worker_id: str):
            return template if worker_id == template.id else None

        def __getattr__(self, name: str):
            return getattr(plan_store, name)

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()
            self.launches: list[dict[str, object]] = []

        async def _start_worker_async(self, **kwargs):
            self.launches.append(kwargs)
            run_id = f"run-{len(self.launches)}"
            return {**kwargs, "status": "started", "worker_id": run_id, "run_id": run_id}

    octo = _Octo()

    async def _scenario() -> dict:
        payload = await _tool_start_workers_parallel(
            {
                "tasks": [
                    {
                        "task": "Patch parser",
                        "worker_id": "coder",
                        "plan_run_id": plan.id,
                        "plan_step_id": "parser",
                    },
                    {
                        "task": "Patch missing step",
                        "worker_id": "coder",
                        "plan_run_id": plan.id,
                        "plan_step_id": "missing",
                    },
                ],
                "max_parallel": 2,
            },
            {"octo": octo, "chat_id": 123},
        )
        return json.loads(payload)

    result = asyncio.run(_scenario())
    steps = {step.step_id: step for step in plan_store.get_plan_steps(plan.id)}

    assert result["status"] == "partial"
    assert result["started_count"] == 1
    assert result["failed_count"] == 1
    assert result["followup_required"] is True
    assert len(octo.launches) == 1
    assert result["launches"][0]["plan_binding"] == {
        "status": "ok",
        "run_id": plan.id,
        "step_id": "parser",
        "worker_run_id": "run-1",
    }
    assert result["launches"][1]["status"] == "error"
    assert result["launches"][1]["plan_binding"] == {
        "status": "not_found",
        "run_id": plan.id,
        "step_id": "missing",
        "message": "plan step was not found",
    }
    assert steps["parser"].status == "awaiting_worker"
    assert steps["parser"].worker_run_id == "run-1"
    assert steps["tests"].status == "pending"
    assert steps["tests"].worker_run_id is None


def test_orchestrator_plan_binds_parallel_child_runs_and_syncs_results(tmp_path: Path) -> None:
    store = _sqlite_store(tmp_path)
    now = datetime.now(UTC)
    parent_template = _template(
        "implementation_coordinator",
        "coordinate engineering work",
        ["start_workers_parallel"],
        ["worker_manage"],
        can_spawn_children=True,
        allowed_child_templates=["repo_researcher", "coder"],
    )
    assert "orchestration_plan_create" not in parent_template.available_tools
    child_templates = [
        _template("repo_researcher", "read code", ["fs_read"], ["filesystem_read"]),
        _template("coder", "patch code", ["fs_read"], ["filesystem_read"]),
    ]
    templates = {item.id: item for item in [parent_template, *child_templates]}
    parent_id = "parent-run-1"
    store.create_worker(
        WorkerRecord(
            id=parent_id,
            status="running",
            task="Coordinate fix",
            granted_caps=[],
            created_at=now,
            updated_at=now,
            template_id=parent_template.id,
            template_name=parent_template.name,
        )
    )

    class _Store:
        def get_worker_template(self, worker_id: str):
            return templates.get(worker_id)

        def list_worker_templates(self):
            return list(templates.values())

        def __getattr__(self, name: str):
            return getattr(store, name)

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()
            self.launches: list[dict[str, object]] = []

        async def _start_worker_async(self, **kwargs):
            self.launches.append(kwargs)
            run_id = f"child-run-{len(self.launches)}"
            return {**kwargs, "status": "started", "worker_id": run_id, "run_id": run_id}

    octo = _Octo()
    worker_ctx = SimpleNamespace(
        spec=SimpleNamespace(
            id=parent_id,
            run_id=parent_id,
            template_id=parent_template.id,
            available_tools=parent_template.available_tools,
            lineage_id=parent_id,
            root_task_id=parent_id,
            spawn_depth=0,
        )
    )

    created = json.loads(
        _tool_orchestration_plan_create(
            {
                "goal": "Coordinate fix",
                "items": [
                    {
                        "id": "research",
                        "title": "Inspect code",
                        "worker_template_id": "repo_researcher",
                    },
                    {"id": "patch", "title": "Patch code", "worker_template_id": "coder"},
                ],
            },
            {"octo": octo, "worker": worker_ctx},
        )
    )
    assert created["status"] == "ok"

    async def _scenario() -> dict:
        payload = await _tool_start_workers_parallel(
            {
                "tasks": [
                    {
                        "task": "Inspect relevant files",
                        "worker_id": "repo_researcher",
                        "orchestration_item_id": "research",
                    },
                    {
                        "task": "Patch the bug",
                        "worker_id": "coder",
                        "orchestration_item_id": "patch",
                    },
                ]
            },
            {"octo": octo, "chat_id": 123, "worker": worker_ctx},
        )
        return json.loads(payload)

    launch = asyncio.run(_scenario())
    assert launch["status"] == "ok"
    assert launch["launches"][0]["orchestration_binding"]["item"]["status"] == "awaiting_worker"
    assert launch["launches"][1]["orchestration_binding"]["item"]["status"] == "awaiting_worker"

    parent = store.get_worker(parent_id)
    assert parent is not None
    plan = parent.output["_orchestration_plan"]
    items = {item["id"]: item for item in plan["items"]}
    assert items["research"]["worker_run_id"] == "child-run-1"
    assert items["patch"]["worker_run_id"] == "child-run-2"

    sync = sync_orchestration_plan_with_child_batch(
        octo=octo,
        parent_worker_id=parent_id,
        child_batch={
            "completed": [
                {
                    "worker_id": "child-run-1",
                    "status": "completed",
                    "summary": "Found parser path.",
                }
            ],
            "failed": [
                {
                    "worker_id": "child-run-2",
                    "status": "failed",
                    "summary": "Patch failed.",
                    "error": "test fixture missing",
                }
            ],
        },
    )
    assert sync is not None
    assert sync["updated_count"] == 2

    parent = store.get_worker(parent_id)
    assert parent is not None
    plan = parent.output["_orchestration_plan"]
    items = {item["id"]: item for item in plan["items"]}
    assert plan["status"] == "needs_attention"
    assert items["research"]["status"] == "completed"
    assert items["research"]["summary"] == "Found parser path."
    assert items["patch"]["status"] == "failed"
    assert items["patch"]["error"] == "test fixture missing"


def test_partial_orchestrator_plan_stays_running_after_first_child_completes(
    tmp_path: Path,
) -> None:
    store = _sqlite_store(tmp_path)
    now = datetime.now(UTC)
    parent_id = "parent-run-1"
    store.create_worker(
        WorkerRecord(
            id=parent_id,
            status="running",
            task="Coordinate fix",
            granted_caps=[],
            created_at=now,
            updated_at=now,
            output={
                "_orchestration_plan": {
                    "goal": "Coordinate fix",
                    "status": "running",
                    "items": [
                        {
                            "id": "research",
                            "status": "awaiting_worker",
                            "worker_run_id": "child-run-1",
                        },
                        {
                            "id": "patch",
                            "status": "todo",
                        },
                    ],
                }
            },
        )
    )

    octo = SimpleNamespace(store=store)

    sync = sync_orchestration_plan_with_child_batch(
        octo=octo,
        parent_worker_id=parent_id,
        child_batch={
            "completed": [
                {
                    "worker_id": "child-run-1",
                    "status": "completed",
                    "summary": "Research done.",
                }
            ]
        },
    )

    assert sync is not None
    parent = store.get_worker(parent_id)
    assert parent is not None
    plan = parent.output["_orchestration_plan"]
    assert plan["status"] == "running"
    items = {item["id"]: item for item in plan["items"]}
    assert items["research"]["status"] == "completed"
    assert items["patch"]["status"] == "todo"


def test_final_worker_output_preserves_existing_orchestration_plan(tmp_path: Path) -> None:
    store = _sqlite_store(tmp_path)
    now = datetime.now(UTC)
    store.create_worker(
        WorkerRecord(
            id="parent-run-1",
            status="running",
            task="Coordinate fix",
            granted_caps=[],
            created_at=now,
            updated_at=now,
            output={
                "_orchestration_plan": {
                    "goal": "Coordinate fix",
                    "status": "running",
                    "items": [{"id": "research", "status": "awaiting_worker"}],
                }
            },
        )
    )

    merged = _merge_existing_orchestration_plan_output(
        store,
        "parent-run-1",
        {"result": "done"},
    )

    assert merged is not None
    assert merged["result"] == "done"
    assert merged["_orchestration_plan"]["items"][0]["id"] == "research"


def test_orchestration_item_retry_clears_stale_result_fields(tmp_path: Path) -> None:
    store = _sqlite_store(tmp_path)
    now = datetime.now(UTC)
    parent_id = "parent-run-1"
    store.create_worker(
        WorkerRecord(
            id=parent_id,
            status="running",
            task="Coordinate fix",
            granted_caps=[],
            created_at=now,
            updated_at=now,
            output={
                "_orchestration_plan": {
                    "goal": "Coordinate fix",
                    "status": "needs_attention",
                    "items": [
                        {
                            "id": "patch",
                            "status": "failed",
                            "worker_run_id": "child-run-1",
                            "summary": "Patch failed.",
                            "output": {"attempt": 1},
                            "error": "test fixture missing",
                            "completed_at": now.isoformat(),
                        }
                    ],
                }
            },
        )
    )

    octo = SimpleNamespace(store=store)

    worker_ctx = SimpleNamespace(
        spec=SimpleNamespace(
            id=parent_id,
            available_tools=["start_workers_parallel"],
        )
    )
    retry = json.loads(
        _tool_orchestration_plan_update_item(
            {
                "item_id": "patch",
                "status": "running",
                "worker_run_id": "child-run-2",
            },
            {"octo": octo, "worker": worker_ctx},
        )
    )

    assert retry["status"] == "ok"
    retried_item = retry["item"]
    assert retried_item["status"] == "running"
    assert retried_item["worker_run_id"] == "child-run-2"
    assert "completed_at" not in retried_item
    assert "summary" not in retried_item
    assert "output" not in retried_item
    assert "error" not in retried_item

    sync_orchestration_plan_with_child_batch(
        octo=octo,
        parent_worker_id=parent_id,
        child_batch={
            "completed": [
                {
                    "worker_id": "child-run-2",
                    "status": "completed",
                    "summary": "Patch succeeded.",
                }
            ]
        },
    )

    parent = store.get_worker(parent_id)
    assert parent is not None
    item = parent.output["_orchestration_plan"]["items"][0]
    assert item["status"] == "completed"
    assert item["summary"] == "Patch succeeded."
    assert "error" not in item


def test_orchestration_plan_tools_are_injected_for_child_spawning_workers() -> None:
    spec = WorkerSpec(
        id="parent-run-1",
        task="Coordinate fix",
        inputs={},
        system_prompt="Coordinate",
        available_tools=["start_workers_parallel"],
        granted_capabilities=[],
        timeout_seconds=120,
        max_thinking_steps=8,
        effective_permissions=["worker_manage"],
    )

    assert _allows_injected_worker_tool(spec, "orchestration_plan_create")
    assert _allows_injected_worker_tool(spec, "orchestration_plan_status")
    assert _allows_injected_worker_tool(spec, "orchestration_plan_update_item")
    assert not _allows_injected_worker_tool(
        spec.model_copy(update={"available_tools": ["fs_read"]}),
        "orchestration_plan_create",
    )


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

    class _Octo:
        store = _Store()

    payload = _tool_synthesize_worker_results(
        {"worker_ids": ["w1", "w2", "w3", "missing"]},
        {"octo": _Octo()},
    )
    result = json.loads(payload)
    assert result["status"] == "partial"
    assert result["can_synthesize"] is True
    assert result["next_best_action"] == "synthesize_ready_results"
    assert result["followup_required"] is True
    assert result["completed_count"] == 1
    assert result["failed_count"] == 1
    assert result["pending_count"] == 1
    assert result["missing_count"] == 1
    assert len(result["ready_results"]) == 1
    assert len(result["failed_results"]) == 1
    assert len(result["pending_results"]) == 1
    assert len(result["missing_results"]) == 1
    assert result["progress_signature"]
    assert "Completed worker findings:" in result["synthesis"]


def test_synthesize_worker_results_blocks_synthesis_when_nothing_completed() -> None:
    now = datetime.now(UTC)
    records = {
        "w1": WorkerRecord(
            id="w1",
            status="running",
            task="one",
            granted_caps=[],
            created_at=now,
            updated_at=now,
            summary=None,
            output=None,
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
    }

    class _Store:
        def get_worker(self, worker_id: str):
            return records.get(worker_id)

    class _Octo:
        store = _Store()

    payload = _tool_synthesize_worker_results(
        {"worker_ids": ["w1", "w2"]},
        {"octo": _Octo()},
    )
    result = json.loads(payload)
    assert result["status"] == "pending"
    assert result["can_synthesize"] is False
    assert result["next_best_action"] == "wait_for_worker_progress"
    assert result["followup_required"] is True
    assert result["completed_count"] == 0
    assert "Do not synthesize yet" in result["synthesis"]


def test_synthesize_worker_results_compacts_large_child_outputs() -> None:
    now = datetime.now(UTC)
    large_output = {
        "report_path": "reports/out.md",
        "results": [{"idx": idx, "body": "x" * 1200} for idx in range(10)],
        "_telemetry": {"tool_result_truncations": 3},
    }
    records = {
        "w1": WorkerRecord(
            id="w1",
            status="completed",
            task="one",
            granted_caps=[],
            created_at=now,
            updated_at=now,
            summary="Fetched web docs",
            output=large_output,
            error=None,
            tools_used=[],
        ),
        "w2": WorkerRecord(
            id="w2",
            status="completed",
            task="two",
            granted_caps=[],
            created_at=now,
            updated_at=now,
            summary="Summarized findings",
            output=large_output,
            error=None,
            tools_used=[],
        ),
    }

    class _Store:
        def get_worker(self, worker_id: str):
            return records.get(worker_id)

    class _Octo:
        store = _Store()

    payload = _tool_synthesize_worker_results(
        {"worker_ids": ["w1", "w2"]},
        {"octo": _Octo()},
    )
    result = json.loads(payload)

    assert result["status"] == "ready"
    assert result["completed_count"] == 2
    for item in result["ready_results"]:
        assert item["output_truncated"] is True
        assert item["output"] == {"available_keys": ["report_path", "results", "_telemetry"]}
        assert item["output_preview_text"]
        assert "report_path" in item["output_preview_text"]
        assert "_telemetry" not in item["output_preview_text"]
        assert item["output_chars"] > 6000
