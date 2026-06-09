from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from octopal.infrastructure.store.models import WorkerTemplateRecord
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.octo.followup_pipeline import _sync_runtime_plan_with_worker_result
from octopal.runtime.octo.router import _build_runtime_plan_context
from octopal.runtime.plans import PlanRunService
from octopal.runtime.workers.contracts import WorkerResult
from octopal.tools.workers.management import (
    _infer_allowed_paths_from_task,
    _infer_allowed_paths_from_values,
    _tool_start_worker,
)


class _StoreSettings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


def _sqlite_store(tmp_path: Path) -> SQLiteStore:
    return SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))


def _template(
    worker_id: str,
    name: str,
    description: str,
    tools: list[str],
    perms: list[str],
) -> WorkerTemplateRecord:
    now = datetime.now(UTC)
    return WorkerTemplateRecord(
        id=worker_id,
        name=name,
        description=description,
        system_prompt=description,
        available_tools=tools,
        required_permissions=perms,
        model=None,
        max_thinking_steps=10,
        default_timeout_seconds=120,
        created_at=now,
        updated_at=now,
    )


def test_start_worker_rejects_missing_worker_id() -> None:
    templates = [
        _template(
            "coder",
            "Coder",
            "Handles code refactors and bugfixes",
            ["fs_read"],
            ["filesystem_read"],
        ),
        _template(
            "web_researcher",
            "Web Researcher",
            "Searches the web and summarizes findings",
            ["web_search"],
            ["network"],
        ),
    ]

    class _Store:
        def list_worker_templates(self):
            return templates

        def get_worker_template(self, worker_id: str):
            for t in templates:
                if t.id == worker_id:
                    return t
            return None

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()

        async def _start_worker_async(self, **kwargs):
            raise AssertionError("worker launch should have been rejected")

    async def _scenario() -> str:
        return await _tool_start_worker(
            {
                "task": "Find latest web docs and summarize",
            },
            {"octo": _Octo(), "chat_id": 123},
        )

    result = asyncio.run(_scenario())
    assert "worker_id is required" in result
    assert "list_workers" in result


def test_start_worker_rejects_auto_worker_id() -> None:
    templates = [
        _template(
            "coder",
            "Coder",
            "Handles code refactors and bugfixes",
            ["fs_read"],
            ["filesystem_read"],
        ),
        _template(
            "web_researcher",
            "Web Researcher",
            "Searches the web and summarizes findings",
            ["web_search"],
            ["network"],
        ),
    ]

    class _Store:
        def list_worker_templates(self):
            return templates

        def get_worker_template(self, worker_id: str):
            for t in templates:
                if t.id == worker_id:
                    return t
            return None

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()

        async def _start_worker_async(self, **kwargs):
            raise AssertionError("worker launch should have been rejected")

    async def _scenario() -> str:
        return await _tool_start_worker(
            {
                "task": "Find latest web docs and summarize",
                "worker_id": "auto",
            },
            {"octo": _Octo(), "chat_id": 123},
        )

    result = asyncio.run(_scenario())
    assert "automatic worker routing is disabled" in result
    assert "list_workers" in result


def test_start_worker_passes_null_model_to_runtime() -> None:
    templates = [
        _template(
            "coder",
            "Coder",
            "Handles code refactors and bugfixes",
            ["fs_read"],
            ["filesystem_read"],
        ),
    ]

    class _Store:
        def list_worker_templates(self):
            return templates

        def get_worker_template(self, worker_id: str):
            for t in templates:
                if t.id == worker_id:
                    return t
            return None

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()
            self.captured = None

        async def _start_worker_async(self, **kwargs):
            self.captured = kwargs
            return {"status": "started", "worker_id": "run-1", "run_id": "run-1", **kwargs}

    octo = _Octo()

    async def _scenario() -> dict:
        payload = await _tool_start_worker(
            {
                "task": "Fix parser bug",
                "worker_id": "coder",
                "model": "gpt-4o",
            },
            {"octo": octo, "chat_id": 123},
        )
        return json.loads(payload)

    result = asyncio.run(_scenario())
    assert result["worker_template_id"] == "coder"
    assert result["followup_required"] is True
    assert result["next_best_action"] == "wait_for_worker_progress"
    assert octo.captured is not None
    assert octo.captured["model"] is None


def test_start_worker_binds_runtime_plan_step(tmp_path: Path) -> None:
    plan_store = _sqlite_store(tmp_path)
    template = _template(
        "coder",
        "Coder",
        "Handles code refactors and bugfixes",
        ["fs_read"],
        ["filesystem_read"],
    )
    plan = PlanRunService(plan_store).create_run(
        goal="Fix bug",
        chat_id=123,
        steps=[
            {"id": "patch", "kind": "worker", "title": "Patch code"},
            {"id": "reply", "kind": "final", "title": "Reply"},
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

        async def _start_worker_async(self, **kwargs):
            return {**kwargs, "status": "started", "worker_id": "run-1", "run_id": "run-1"}

    async def _scenario() -> dict:
        payload = await _tool_start_worker(
            {
                "task": "Fix parser bug",
                "worker_id": "coder",
                "plan_run_id": plan.id,
                "plan_step_id": "patch",
            },
            {"octo": _Octo(), "chat_id": 123},
        )
        return json.loads(payload)

    result = asyncio.run(_scenario())
    assert result["plan_binding"] == {
        "status": "ok",
        "run_id": plan.id,
        "step_id": "patch",
        "worker_run_id": "run-1",
    }
    saved = plan_store.get_plan_run(plan.id)
    assert saved is not None
    assert saved.status == "awaiting_worker"
    step = plan_store.get_plan_steps(plan.id)[0]
    assert step.status == "awaiting_worker"
    assert step.worker_run_id == "run-1"


def test_runtime_plan_worker_lifecycle_stays_bound_through_followup(tmp_path: Path) -> None:
    plan_store = _sqlite_store(tmp_path)
    template = _template(
        "researcher",
        "Researcher",
        "Collects durable evidence",
        ["fs_read"],
        ["filesystem_read"],
    )
    plan = PlanRunService(plan_store).create_run(
        goal="Collect release evidence",
        chat_id=123,
        steps=[
            {"id": "collect", "kind": "worker", "title": "Collect evidence"},
            {"id": "reply", "kind": "final", "title": "Reply with summary"},
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

        async def _start_worker_async(self, **kwargs):
            return {
                **kwargs,
                "status": "started",
                "worker_id": "worker-run-1",
                "run_id": "worker-run-1",
            }

    octo = _Octo()
    before_context = _build_runtime_plan_context(SimpleNamespace(store=plan_store), 123)

    async def _scenario() -> dict:
        payload = await _tool_start_worker(
            {
                "task": "Collect release evidence",
                "worker_id": "researcher",
                "plan_run_id": plan.id,
                "plan_step_id": "collect",
            },
            {"octo": octo, "chat_id": 123},
        )
        return json.loads(payload)

    launch = asyncio.run(_scenario())
    _sync_runtime_plan_with_worker_result(
        SimpleNamespace(store=plan_store),
        123,
        worker_id="worker-run-1",
        result=WorkerResult(
            status="completed",
            summary="Release evidence collected.",
            output={"artifact_summary": {"durable_paths": ["reports/release.md"]}},
            tools_used=["fs_read"],
        ),
    )

    saved = plan_store.get_plan_run(plan.id)
    steps = plan_store.get_plan_steps(plan.id)
    after_context = _build_runtime_plan_context(SimpleNamespace(store=plan_store), 123)

    assert "Collect release evidence" in before_context
    assert launch["plan_binding"] == {
        "status": "ok",
        "run_id": plan.id,
        "step_id": "collect",
        "worker_run_id": "worker-run-1",
    }
    assert saved is not None
    assert saved.status == "needs_next_step"
    assert saved.current_step_id == "reply"
    assert steps[0].status == "completed"
    assert steps[0].output["worker_status"] == "completed"
    assert steps[0].output["summary"] == "Release evidence collected."
    assert steps[1].status == "pending"
    assert "worker-run-1" in after_context
    assert "Release evidence collected." in after_context


def test_start_worker_rejects_invalid_plan_binding_before_launch(tmp_path: Path) -> None:
    plan_store = _sqlite_store(tmp_path)
    template = _template(
        "coder",
        "Coder",
        "Handles code refactors and bugfixes",
        ["fs_read"],
        ["filesystem_read"],
    )
    plan = PlanRunService(plan_store).create_run(
        goal="Fix bug",
        chat_id=123,
        steps=[{"id": "patch", "kind": "worker", "title": "Patch code"}],
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
            self.launch_called = False

        async def _start_worker_async(self, **kwargs):
            self.launch_called = True
            raise AssertionError("worker launch should be rejected before dispatch")

    octo = _Octo()

    async def _scenario() -> dict:
        payload = await _tool_start_worker(
            {
                "task": "Fix parser bug",
                "worker_id": "coder",
                "plan_run_id": plan.id,
                "plan_step_id": "missing",
            },
            {"octo": octo, "chat_id": 123},
        )
        return json.loads(payload)

    result = asyncio.run(_scenario())
    assert result["status"] == "error"
    assert result["followup_required"] is False
    assert result["plan_binding"] == {
        "status": "not_found",
        "run_id": plan.id,
        "step_id": "missing",
        "message": "plan step was not found",
    }
    assert octo.launch_called is False
    saved_step = plan_store.get_plan_steps(plan.id)[0]
    assert saved_step.status == "pending"
    assert saved_step.worker_run_id is None


def test_start_worker_does_not_bind_duplicate_skip_to_plan_step(tmp_path: Path) -> None:
    plan_store = _sqlite_store(tmp_path)
    template = _template(
        "coder",
        "Coder",
        "Handles code refactors and bugfixes",
        ["fs_read"],
        ["filesystem_read"],
    )
    plan = PlanRunService(plan_store).create_run(
        goal="Fix bug",
        chat_id=123,
        steps=[{"id": "patch", "kind": "worker", "title": "Patch code"}],
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

        async def _start_worker_async(self, **kwargs):
            return {"status": "skipped_duplicate", "run_id": "skipped-duplicate-123"}

    async def _scenario() -> dict:
        payload = await _tool_start_worker(
            {
                "task": "Fix parser bug",
                "worker_id": "coder",
                "plan_run_id": plan.id,
                "plan_step_id": "patch",
            },
            {"octo": _Octo(), "chat_id": 123},
        )
        return json.loads(payload)

    result = asyncio.run(_scenario())
    assert result["status"] == "skipped_duplicate"
    assert result["plan_binding"] == {
        "status": "skipped",
        "run_id": plan.id,
        "step_id": "patch",
        "message": "worker was not started; plan step was not bound",
    }
    saved = plan_store.get_plan_run(plan.id)
    assert saved is not None
    assert saved.status == "planned"
    step = plan_store.get_plan_steps(plan.id)[0]
    assert step.status == "pending"
    assert step.worker_run_id is None


def test_start_worker_rejects_tools_outside_template_allowlist() -> None:
    templates = [
        _template(
            "coder",
            "Coder",
            "Handles code refactors and bugfixes",
            ["fs_read"],
            ["filesystem_read"],
        ),
    ]

    class _Store:
        def list_worker_templates(self):
            return templates

        def get_worker_template(self, worker_id: str):
            for t in templates:
                if t.id == worker_id:
                    return t
            return None

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()

        async def _start_worker_async(self, **kwargs):
            raise AssertionError("worker launch should have been rejected")

    async def _scenario() -> str:
        return await _tool_start_worker(
            {
                "task": "Fix parser bug",
                "worker_id": "coder",
                "tools": ["fs_read", "exec_run"],
            },
            {"octo": _Octo(), "chat_id": 123},
        )

    result = asyncio.run(_scenario())
    assert "requested tools exceed template contract" in result
    assert "exec_run" in result


def test_start_worker_allows_subset_tool_override() -> None:
    templates = [
        _template(
            "coder",
            "Coder",
            "Handles code refactors and bugfixes",
            ["fs_read", "fs_write"],
            ["filesystem_read", "filesystem_write"],
        ),
    ]

    class _Store:
        def list_worker_templates(self):
            return templates

        def get_worker_template(self, worker_id: str):
            for t in templates:
                if t.id == worker_id:
                    return t
            return None

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()
            self.captured = None

        async def _start_worker_async(self, **kwargs):
            self.captured = kwargs
            return {"status": "started", "worker_id": "run-2", "run_id": "run-2", **kwargs}

    octo = _Octo()

    async def _scenario() -> dict:
        payload = await _tool_start_worker(
            {
                "task": "Read the parser file",
                "worker_id": "coder",
                "tools": ["fs_read"],
            },
            {"octo": octo, "chat_id": 123},
        )
        return json.loads(payload)

    result = asyncio.run(_scenario())
    assert result["status"] == "started"
    assert result["followup_required"] is True
    assert result["next_best_action"] == "wait_for_worker_progress"
    assert octo.captured is not None
    assert octo.captured["tools"] == ["fs_read"]


def test_start_worker_rejects_explicit_required_image_tool_without_capability() -> None:
    templates = [
        _template(
            "coder",
            "Coder",
            "Handles code refactors and bugfixes",
            ["fs_read"],
            ["filesystem_read"],
        ),
    ]

    class _Store:
        def list_worker_templates(self):
            return templates

        def get_worker_template(self, worker_id: str):
            for t in templates:
                if t.id == worker_id:
                    return t
            return None

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()

        async def _start_worker_async(self, **kwargs):
            raise AssertionError("worker launch should have been rejected")

    async def _scenario() -> str:
        return await _tool_start_worker(
            {
                "task": "Analyze the image at tmp/telegram_images/img_test.jpg",
                "worker_id": "coder",
                "required_tools": ["analyze_image"],
            },
            {"octo": _Octo(), "chat_id": 123},
        )

    result = asyncio.run(_scenario())
    assert "does not provide required tool(s): analyze_image" in result


def test_start_worker_allows_research_about_image_recognition_without_image_capability() -> None:
    templates = [
        _template(
            "web_researcher",
            "Web Researcher",
            "Searches web",
            ["web_search"],
            ["network"],
        ),
    ]

    class _Store:
        def list_worker_templates(self):
            return templates

        def get_worker_template(self, worker_id: str):
            for t in templates:
                if t.id == worker_id:
                    return t
            return None

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()
            self.captured = None

        async def _start_worker_async(self, **kwargs):
            self.captured = kwargs
            return {
                "status": "started",
                "worker_id": kwargs["worker_id"],
                "run_id": "run-1",
            }

    async def _scenario() -> tuple[str, _Octo]:
        octo = _Octo()
        result = await _tool_start_worker(
            {
                "task": "Research MiniMax MCP servers that support web search and image recognition. Return setup instructions.",
                "worker_id": "web_researcher",
            },
            {"octo": octo, "chat_id": 123},
        )
        return result, octo

    result, octo = asyncio.run(_scenario())
    assert "started" in result
    assert octo.captured is not None
    assert octo.captured["worker_id"] == "web_researcher"


def test_start_worker_rejects_explicit_required_file_write_tool_without_capability() -> None:
    templates = [
        _template("web_researcher", "Web Researcher", "Searches web", ["web_search"], ["network"]),
    ]

    class _Store:
        def list_worker_templates(self):
            return templates

        def get_worker_template(self, worker_id: str):
            for t in templates:
                if t.id == worker_id:
                    return t
            return None

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()

        async def _start_worker_async(self, **kwargs):
            raise AssertionError("worker launch should have been rejected")

    async def _scenario() -> str:
        return await _tool_start_worker(
            {
                "task": "Create a short markdown report at experiments/qa/marker-worker-report.md.",
                "worker_id": "web_researcher",
                "required_tool_calls": ["fs_write"],
            },
            {"octo": _Octo(), "chat_id": 123},
        )

    result = asyncio.run(_scenario())
    assert "does not provide required tool(s): fs_write" in result


def test_start_worker_does_not_infer_workspace_write_requirement_from_task_text() -> None:
    templates = [
        _template("web_researcher", "Web Researcher", "Searches web", ["web_search"], ["network"]),
    ]

    class _Store:
        def list_worker_templates(self):
            return templates

        def get_worker_template(self, worker_id: str):
            for t in templates:
                if t.id == worker_id:
                    return t
            return None

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store()
            self.captured = None

        async def _start_worker_async(self, **kwargs):
            self.captured = kwargs
            return {
                "status": "started",
                "worker_id": kwargs["worker_id"],
                "run_id": "run-1",
            }

    async def _scenario() -> tuple[str, _Octo]:
        octo = _Octo()
        result = await _tool_start_worker(
            {
                "task": "Create a short markdown report at experiments/qa/marker-worker-report.md.",
                "worker_id": "web_researcher",
            },
            {"octo": octo, "chat_id": 123},
        )
        return result, octo

    result, octo = asyncio.run(_scenario())
    assert "started" in result
    assert octo.captured is not None
    assert octo.captured["required_tool_calls"] == []


def test_start_worker_infers_existing_workspace_paths(monkeypatch, tmp_path) -> None:
    image_path = tmp_path / "tmp" / "telegram_images" / "img_test.jpg"
    image_path.parent.mkdir(parents=True)
    image_path.write_bytes(b"jpg")
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(tmp_path))

    inferred = _infer_allowed_paths_from_task("Inspect tmp/telegram_images/img_test.jpg")

    assert inferred == ["tmp/telegram_images/img_test.jpg"]


def test_start_worker_infers_existing_parent_for_new_workspace_file(monkeypatch, tmp_path) -> None:
    report_dir = tmp_path / "experiments" / "qa"
    report_dir.mkdir(parents=True)
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(tmp_path))

    inferred = _infer_allowed_paths_from_task(
        "Create experiments/qa/new-agent-report.md with the requested summary"
    )

    assert inferred == ["experiments/qa"]


def test_start_worker_infers_workspace_paths_from_inputs(monkeypatch, tmp_path) -> None:
    draft_dir = tmp_path / "memory" / "moltbook"
    draft_dir.mkdir(parents=True)
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(tmp_path))

    inferred = _infer_allowed_paths_from_values(
        "Publish the current draft.",
        {"draft_path": "memory/moltbook/draft.md"},
    )

    assert inferred == ["memory/moltbook"]


def test_start_worker_does_not_infer_workspace_paths_from_url_inputs(monkeypatch, tmp_path) -> None:
    (tmp_path / "example.com" / "reports").mkdir(parents=True)
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(tmp_path))

    inferred = _infer_allowed_paths_from_values(
        "Fetch the remote report.",
        {"url": "https://example.com/reports/out.md"},
    )

    assert inferred is None
