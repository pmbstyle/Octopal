from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace

from octopal.infrastructure.store.models import WorkerTemplateRecord
from octopal.tools.workers.management import _tool_start_child_worker


def _template(
    worker_id: str,
    *,
    perms: list[str],
    can_spawn: bool = False,
    allowed_children: list[str] | None = None,
) -> WorkerTemplateRecord:
    now = datetime.now(UTC)
    return WorkerTemplateRecord(
        id=worker_id,
        name=worker_id.title(),
        description=worker_id,
        system_prompt=worker_id,
        available_tools=[],
        required_permissions=perms,
        model=None,
        max_thinking_steps=8,
        default_timeout_seconds=120,
        can_spawn_children=can_spawn,
        allowed_child_templates=allowed_children or [],
        created_at=now,
        updated_at=now,
    )


class _Store:
    def __init__(self, templates: dict[str, WorkerTemplateRecord]) -> None:
        self._templates = templates

    def get_worker_template(self, worker_id: str):
        return self._templates.get(worker_id)

    def list_worker_templates(self):
        return list(self._templates.values())


def _caller_worker(
    *,
    template_id: str = "parent",
    run_id: str = "parent-run",
    lineage_id: str = "lineage-1",
    root_task_id: str = "root-1",
    spawn_depth: int = 0,
    effective_permissions: list[str] | None = None,
):
    spec = SimpleNamespace(
        template_id=template_id,
        id=run_id,
        run_id=run_id,
        lineage_id=lineage_id,
        root_task_id=root_task_id,
        spawn_depth=spawn_depth,
        effective_permissions=effective_permissions or [],
    )
    return SimpleNamespace(spec=spec)


def test_start_child_worker_enforces_opt_in() -> None:
    templates = {
        "parent": _template("parent", perms=["network"], can_spawn=False, allowed_children=["child"]),
        "child": _template("child", perms=["network"]),
    }

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store(templates)

        async def _start_worker_async(self, **kwargs):
            raise AssertionError("should not launch child worker")

    async def _scenario() -> str:
        return await _tool_start_child_worker(
            {"worker_id": "child", "task": "fetch rss"},
            {"octo": _Octo(), "chat_id": 1, "worker": _caller_worker(effective_permissions=["network"])},
        )

    result = asyncio.run(_scenario())
    assert "cannot spawn children" in result


def test_start_child_worker_enforces_whitelist_and_permission_subset() -> None:
    templates = {
        "parent": _template("parent", perms=["network"], can_spawn=True, allowed_children=["child"]),
        "child": _template("child", perms=["exec"]),
    }

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store(templates)

        async def _start_worker_async(self, **kwargs):
            raise AssertionError("should not launch child worker")

    async def _scenario() -> str:
        return await _tool_start_child_worker(
            {"worker_id": "child", "task": "fetch rss"},
            {"octo": _Octo(), "chat_id": 1, "worker": _caller_worker(effective_permissions=["network"])},
        )

    result = asyncio.run(_scenario())
    assert "requests permissions not held by parent" in result


def test_start_child_worker_propagates_lineage_fields() -> None:
    templates = {
        "parent": _template("parent", perms=["network", "filesystem_read"], can_spawn=True, allowed_children=["child"]),
        "child": _template("child", perms=["network"]),
    }

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store(templates)
            self.last_launch = None

        async def _start_worker_async(self, **kwargs):
            self.last_launch = kwargs
            return {
                "status": "started",
                "worker_id": "child-run-1",
                "run_id": "child-run-1",
                "lineage_id": kwargs.get("lineage_id"),
                "parent_worker_id": kwargs.get("parent_worker_id"),
                "root_task_id": kwargs.get("root_task_id"),
                "spawn_depth": kwargs.get("spawn_depth"),
            }

    octo = _Octo()

    async def _scenario() -> dict[str, object]:
        payload = await _tool_start_child_worker(
            {"worker_id": "child", "task": "fetch rss"},
            {
                "octo": octo,
                "chat_id": 1,
                "worker": _caller_worker(
                    template_id="parent",
                    run_id="parent-run-9",
                    lineage_id="lineage-9",
                    root_task_id="root-9",
                    spawn_depth=1,
                    effective_permissions=["network", "filesystem_read"],
                ),
            },
        )
        return json.loads(payload)

    result = asyncio.run(_scenario())
    assert result["status"] == "started"
    assert result["lineage_id"] == "lineage-9"
    assert result["parent_worker_id"] == "parent-run-9"
    assert result["root_task_id"] == "root-9"
    assert result["spawn_depth"] == 2
    assert octo.last_launch["parent_worker_id"] == "parent-run-9"


def test_start_child_worker_preserves_missing_allowed_paths_and_forwards_explicit_ones() -> None:
    templates = {
        "parent": _template("parent", perms=["network", "filesystem_read"], can_spawn=True, allowed_children=["child"]),
        "child": _template("child", perms=["network"]),
    }

    class _Octo:
        def __init__(self) -> None:
            self.store = _Store(templates)
            self.launches: list[dict[str, object]] = []

        async def _start_worker_async(self, **kwargs):
            self.launches.append(kwargs)
            return {
                "status": "started",
                "worker_id": f"child-run-{len(self.launches)}",
                "run_id": f"child-run-{len(self.launches)}",
            }

    octo = _Octo()

    async def _scenario() -> None:
        await _tool_start_child_worker(
            {"worker_id": "child", "task": "fetch rss"},
            {
                "octo": octo,
                "chat_id": 1,
                "worker": _caller_worker(effective_permissions=["network", "filesystem_read"]),
            },
        )
        await _tool_start_child_worker(
            {"worker_id": "child", "task": "inspect file", "allowed_paths": ["src/app.py"]},
            {
                "octo": octo,
                "chat_id": 1,
                "worker": _caller_worker(effective_permissions=["network", "filesystem_read"]),
            },
        )

    asyncio.run(_scenario())
    assert octo.launches[0]["allowed_paths"] is None
    assert octo.launches[1]["allowed_paths"] == ["src/app.py"]
