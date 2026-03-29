from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

from octopal.infrastructure.config.models import LLMConfig, OctopalConfig
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.store.models import WorkerTemplateRecord
from octopal.runtime.workers.contracts import Capability, TaskRequest, WorkerResult
from octopal.runtime.workers.runtime import WorkerRuntime
from octopal.tools.registry import ToolSpec


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


def test_runtime_launch_env_includes_workspace_dir(tmp_path: Path) -> None:
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

        def create_worker(self, _record):
            return None

        def update_worker_status(self, _worker_id: str, _status: str):
            return None

        def append_audit(self, _event):
            return None

        def update_worker_result(self, _worker_id: str, **_kwargs):
            return None

    class _Policy:
        def grant_capabilities(self, capabilities):
            return [Capability(type="network", scope="worker")]

    captured: dict[str, object] = {}

    class _Launcher:
        async def launch(self, spec_path: str, cwd: str, env: dict[str, str]):
            captured["spec_path"] = spec_path
            captured["cwd"] = cwd
            captured["env"] = env
            raise RuntimeError("stop after env capture")

    runtime = WorkerRuntime(
        store=_Store(),
        policy=_Policy(),
        workspace_dir=tmp_path / "workspace",
        launcher=_Launcher(),
        settings=Settings(),
    )
    runtime.workspace_dir.mkdir(parents=True, exist_ok=True)

    request = TaskRequest(worker_id="worker", task="hello")

    try:
        asyncio.run(runtime.run_task(request))
    except RuntimeError as exc:
        assert "stop after env capture" in str(exc)
    else:
        raise AssertionError("Expected launch to stop after env capture")

    env = captured["env"]
    assert isinstance(env, dict)
    assert env["OCTOPAL_WORKSPACE_DIR"] == str(runtime.workspace_dir.resolve())


def test_runtime_launch_env_includes_search_keys_from_config(tmp_path: Path, monkeypatch) -> None:
    template = WorkerTemplateRecord(
        id="worker",
        name="Worker",
        description="Test worker",
        system_prompt="Do work",
        available_tools=["web_search"],
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

        def create_worker(self, _record):
            return None

        def update_worker_status(self, _worker_id: str, _status: str):
            return None

        def append_audit(self, _event):
            return None

        def update_worker_result(self, _worker_id: str, **_kwargs):
            return None

    class _Policy:
        def grant_capabilities(self, capabilities):
            return [Capability(type="network", scope="worker")]

    captured: dict[str, object] = {}

    class _Launcher:
        async def launch(self, spec_path: str, cwd: str, env: dict[str, str]):
            captured["env"] = env
            raise RuntimeError("stop after env capture")

    monkeypatch.delenv("BRAVE_API_KEY", raising=False)
    settings = Settings(
        config_obj=OctopalConfig(search={"brave_api_key": "brave-from-config"})
    )
    runtime = WorkerRuntime(
        store=_Store(),
        policy=_Policy(),
        workspace_dir=tmp_path / "workspace",
        launcher=_Launcher(),
        settings=settings,
    )
    runtime.workspace_dir.mkdir(parents=True, exist_ok=True)

    request = TaskRequest(worker_id="worker", task="hello")

    try:
        asyncio.run(runtime.run_task(request))
    except RuntimeError as exc:
        assert "stop after env capture" in str(exc)
    else:
        raise AssertionError("Expected launch to stop after env capture")

    env = captured["env"]
    assert isinstance(env, dict)
    assert env["BRAVE_API_KEY"] == "brave-from-config"


def test_runtime_launch_env_keeps_provider_secrets_out_of_worker_env(tmp_path: Path, monkeypatch) -> None:
    template = WorkerTemplateRecord(
        id="worker",
        name="Worker",
        description="Test worker",
        system_prompt="Do work",
        available_tools=["web_search"],
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

        def create_worker(self, _record):
            return None

        def update_worker_status(self, _worker_id: str, _status: str):
            return None

        def append_audit(self, _event):
            return None

        def update_worker_result(self, _worker_id: str, **_kwargs):
            return None

    class _Policy:
        def grant_capabilities(self, capabilities):
            return [Capability(type="network", scope="worker")]

    captured: dict[str, object] = {}

    class _Launcher:
        async def launch(self, spec_path: str, cwd: str, env: dict[str, str]):
            captured["env"] = env
            raise RuntimeError("stop after env capture")

    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    settings = Settings(
        config_obj=OctopalConfig(search={"brave_api_key": "brave-from-config"}),
        OPENROUTER_API_KEY="openrouter-secret",
    )
    runtime = WorkerRuntime(
        store=_Store(),
        policy=_Policy(),
        workspace_dir=tmp_path / "workspace",
        launcher=_Launcher(),
        settings=settings,
    )
    runtime.workspace_dir.mkdir(parents=True, exist_ok=True)

    request = TaskRequest(worker_id="worker", task="hello")

    try:
        asyncio.run(runtime.run_task(request))
    except RuntimeError as exc:
        assert "stop after env capture" in str(exc)
    else:
        raise AssertionError("Expected launch to stop after env capture")

    env = captured["env"]
    assert isinstance(env, dict)
    assert env["BRAVE_API_KEY"] == "brave-from-config"
    assert "OPENROUTER_API_KEY" not in env


def test_runtime_ignores_task_level_model_override(tmp_path: Path) -> None:
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

    settings = Settings(
        config_obj=OctopalConfig(
            llm=LLMConfig(provider_id="zai", model="glm-5"),
            worker_llm_default=LLMConfig(provider_id="openrouter", model="anthropic/claude-sonnet-4"),
        )
    )
    runtime = WorkerRuntime(
        store=_Store(),
        policy=_Policy(),
        workspace_dir=tmp_path,
        launcher=object(),
        settings=settings,
    )

    captured: dict[str, object] = {}

    async def _fake_run(spec, approval_requester=None):
        captured["spec"] = spec
        return WorkerResult(summary="ok")

    runtime.run = _fake_run  # type: ignore[method-assign]

    request = TaskRequest(worker_id="worker", task="hello")
    asyncio.run(runtime.run_task(request))

    spec = captured["spec"]
    assert spec.model is None
    assert spec.llm_config.model == "anthropic/claude-sonnet-4"
