from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

from octopal.infrastructure.store.models import WorkerTemplateRecord
from octopal.tools.workers.management import (
    _infer_allowed_paths_from_task,
    _select_worker_template,
    _tool_start_worker,
)


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


def test_select_worker_template_prefers_keyword_overlap() -> None:
    templates = [
        _template("coder", "Coder", "Handles code refactors and bugfixes", ["fs_read"], ["filesystem_read"]),
        _template("web_researcher", "Web Researcher", "Searches the web and summarizes findings", ["web_search"], ["network"]),
    ]
    selected = _select_worker_template(templates=templates, task="Research latest web sources about APIs")
    assert selected is not None
    assert selected["template"].id == "web_researcher"


def test_select_worker_template_respects_required_tools() -> None:
    templates = [
        _template("writer", "Writer", "Writes docs", ["fs_write"], ["filesystem_write"]),
        _template("web_researcher", "Web Researcher", "Searches web", ["web_search"], ["network"]),
    ]
    selected = _select_worker_template(
        templates=templates,
        task="Summarize recent updates",
        required_tools=["web_search"],
    )
    assert selected is not None
    assert selected["template"].id == "web_researcher"


def test_select_worker_template_rejects_missing_required_tools() -> None:
    templates = [
        _template("writer", "Writer", "Writes docs", ["fs_write"], ["filesystem_write"]),
    ]
    selected = _select_worker_template(
        templates=templates,
        task="Analyze an image using mcp_zai_analyze_image",
        required_tools=["mcp_zai_analyze_image"],
    )
    assert selected is None


def test_select_worker_template_prefers_filesystem_worker_for_file_write_task() -> None:
    templates = [
        _template(
            "file_editor",
            "File Editor",
            "Safely edits text and config files in the workspace",
            ["fs_read", "fs_write"],
            ["filesystem_read", "filesystem_write"],
        ),
        _template(
            "web_search_ranked",
            "Web Search Ranked",
            "Search the web and return a ranked list of relevant sources",
            ["web_search"],
            ["network"],
        ),
    ]
    selected = _select_worker_template(
        templates=templates,
        task="Create a short markdown report at experiments/qa/marker-worker-report.md with risks and mitigations.",
    )
    assert selected is not None
    assert selected["template"].id == "file_editor"
    assert "filesystem_write_bonus" in selected["reason"]


def test_select_worker_template_requires_image_capability_for_image_tasks() -> None:
    templates = [
        _template("moltbook_orchestrator", "Presence Manager", "Sequential task manager", ["fs_read"], ["filesystem_read"]),
        _template("vision_worker", "Vision Worker", "Analyzes images", ["mcp_call"], ["network", "filesystem_read"]),
    ]
    selected = _select_worker_template(
        templates=templates,
        task="Analyze the image at tmp/telegram_images/img.jpg",
    )
    assert selected is not None
    assert selected["template"].id == "vision_worker"


def test_start_worker_auto_routes_and_returns_router_metadata() -> None:
    templates = [
        _template("coder", "Coder", "Handles code refactors and bugfixes", ["fs_read"], ["filesystem_read"]),
        _template("web_researcher", "Web Researcher", "Searches the web and summarizes findings", ["web_search"], ["network"]),
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
            return {"status": "started", "worker_id": "run-1", "run_id": "run-1", **kwargs}

    async def _scenario() -> dict:
        payload = await _tool_start_worker(
            {
                "task": "Find latest web docs and summarize",
                "worker_id": "auto",
            },
            {"octo": _Octo(), "chat_id": 123},
        )
        return json.loads(payload)

    result = asyncio.run(_scenario())
    assert result["router_used"] is True
    assert result["worker_template_id"] == "web_researcher"
    assert result["followup_required"] is True
    assert result["next_best_action"] == "wait_for_worker_progress"
    assert isinstance(result["router_reason"], str) and result["router_reason"]


def test_start_worker_passes_null_model_to_runtime() -> None:
    templates = [
        _template("coder", "Coder", "Handles code refactors and bugfixes", ["fs_read"], ["filesystem_read"]),
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


def test_start_worker_rejects_tools_outside_template_allowlist() -> None:
    templates = [
        _template("coder", "Coder", "Handles code refactors and bugfixes", ["fs_read"], ["filesystem_read"]),
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


def test_start_worker_rejects_explicit_worker_without_image_capability() -> None:
    templates = [
        _template("coder", "Coder", "Handles code refactors and bugfixes", ["fs_read"], ["filesystem_read"]),
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
            },
            {"octo": _Octo(), "chat_id": 123},
        )

    result = asyncio.run(_scenario())
    assert "does not advertise image/vision analysis capability" in result


def test_start_worker_rejects_explicit_worker_without_workspace_write_capability() -> None:
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
            },
            {"octo": _Octo(), "chat_id": 123},
        )

    result = asyncio.run(_scenario())
    assert "does not advertise workspace write capability" in result
    assert "fs_write/filesystem_write" in result


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
