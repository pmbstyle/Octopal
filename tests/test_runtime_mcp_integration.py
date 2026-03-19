from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

from broodmind.infrastructure.config.settings import Settings
from broodmind.infrastructure.store.models import WorkerTemplateRecord
from broodmind.runtime.workers.contracts import Capability, TaskRequest, WorkerResult
from broodmind.runtime.workers.runtime import WorkerRuntime
from broodmind.tools.registry import ToolSpec


def test_runtime_does_not_auto_inject_global_mcp_tools(tmp_path: Path) -> None:
    template = WorkerTemplateRecord(
        id="worker",
        name="Worker",
        description="Test worker",
        system_prompt="Do work",
        available_tools=["fs_read"],
        required_permissions=["network"],
        model=None,
        max_thinking_steps=3,
        default_timeout_seconds=30,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )

    class _Store:
        def get_worker_template(self, worker_id: str):
            return template

    class _Policy:
        def grant_capabilities(self, capabilities):
            return [Capability(type="network", scope="worker")]

    class _MCP:
        sessions = {"demo": object()}

        def get_all_tools(self):
            return [
                ToolSpec(
                    name="mcp_demo_read_data",
                    description="demo",
                    parameters={"type": "object"},
                    permission="mcp_exec",
                    handler=lambda _args, _ctx: "ok",
                    is_async=True,
                )
            ]

    runtime = WorkerRuntime(
        store=_Store(),
        policy=_Policy(),
        workspace_dir=tmp_path,
        launcher=object(),
        mcp_manager=_MCP(),
        settings=Settings(),
    )

    captured: dict[str, object] = {}

    async def _fake_run(spec, approval_requester=None):
        captured["spec"] = spec
        return WorkerResult(summary="ok")

    runtime.run = _fake_run  # type: ignore[method-assign]

    request = TaskRequest(worker_id="worker", task="hello")
    asyncio.run(runtime.run_task(request))

    spec = captured["spec"]
    assert "mcp_demo_read_data" not in spec.available_tools
    assert spec.mcp_tools == []


def test_runtime_ensures_configured_mcp_before_launch(tmp_path: Path) -> None:
    template = WorkerTemplateRecord(
        id="worker",
        name="Worker",
        description="Test worker",
        system_prompt="Do work",
        available_tools=["mcp_demo_read_data"],
        required_permissions=["network"],
        model=None,
        max_thinking_steps=3,
        default_timeout_seconds=30,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )

    class _Store:
        def get_worker_template(self, worker_id: str):
            return template

    class _Policy:
        def grant_capabilities(self, capabilities):
            return [Capability(type="network", scope="worker")]

    class _MCP:
        def __init__(self) -> None:
            self.sessions = {"demo": object()}
            self.ensure_calls: list[object] = []

        async def ensure_configured_servers_connected(self, server_ids=None):
            self.ensure_calls.append(server_ids)
            self.sessions = {"demo": object()}
            return {"demo": "connected"}

        def get_all_tools(self):
            return [
                ToolSpec(
                    name="mcp_demo_read_data",
                    description="demo",
                    parameters={"type": "object"},
                    permission="mcp_exec",
                    handler=lambda _args, _ctx: "ok",
                    is_async=True,
                    server_id="demo",
                    remote_tool_name="read_data",
                )
            ]

    mcp_manager = _MCP()
    runtime = WorkerRuntime(
        store=_Store(),
        policy=_Policy(),
        workspace_dir=tmp_path,
        launcher=object(),
        mcp_manager=mcp_manager,
        settings=Settings(),
    )

    captured: dict[str, object] = {}

    async def _fake_run(spec, approval_requester=None):
        captured["spec"] = spec
        return WorkerResult(summary="ok")

    runtime.run = _fake_run  # type: ignore[method-assign]

    request = TaskRequest(worker_id="worker", task="hello")
    asyncio.run(runtime.run_task(request))

    spec = captured["spec"]
    assert mcp_manager.ensure_calls == [None]
    assert spec.mcp_tools[0]["server_id"] == "demo"
