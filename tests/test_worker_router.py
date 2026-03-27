from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

from octopal.infrastructure.store.models import WorkerTemplateRecord
from octopal.tools.workers.management import _select_worker_template, _tool_start_worker


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
    assert octo.captured is not None
    assert octo.captured["model"] is None
