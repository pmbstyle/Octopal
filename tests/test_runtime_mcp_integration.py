from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from octopal.infrastructure.config.models import LLMConfig, OctopalConfig
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.store.models import WorkerRecord, WorkerTemplateRecord
from octopal.runtime.workers.contracts import Capability, TaskRequest, WorkerResult
from octopal.runtime.workers.runtime import (
    WorkerRuntime,
    _validate_worker_local_tool_call,
    _validate_worker_mcp_tool_call,
    _validate_worker_tool_permissions,
)
from octopal.tools.registry import ToolSpec


def test_runtime_blocks_user_communication_tools_for_workers(tmp_path: Path) -> None:
    template = WorkerTemplateRecord(
        id="worker",
        name="Worker",
        description="Test worker",
        system_prompt="Do work",
        available_tools=[
            "fs_read",
            "send_file_to_user",
            "self_control",
            "octo_restart_self",
            "octo_check_update",
            "octo_update_self",
        ],
        required_permissions=["filesystem_read", "self_control"],
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
            return capabilities

    runtime = WorkerRuntime(
        store=_Store(),
        policy=_Policy(),
        workspace_dir=tmp_path,
        launcher=object(),
        mcp_manager=None,
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
    assert "fs_read" in spec.available_tools
    assert "send_file_to_user" not in spec.available_tools
    assert "self_control" not in spec.available_tools
    assert "octo_restart_self" not in spec.available_tools
    assert "octo_check_update" not in spec.available_tools
    assert "octo_update_self" not in spec.available_tools


def test_runtime_uses_task_max_thinking_steps_override(tmp_path: Path) -> None:
    template = WorkerTemplateRecord(
        id="worker",
        name="Worker",
        description="Test worker",
        system_prompt="Do work",
        available_tools=["fs_read"],
        required_permissions=["filesystem_read"],
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
            return capabilities

    runtime = WorkerRuntime(
        store=_Store(),
        policy=_Policy(),
        workspace_dir=tmp_path,
        launcher=object(),
        mcp_manager=None,
        settings=Settings(),
    )

    captured: dict[str, object] = {}

    async def _fake_run(spec, approval_requester=None):
        captured["spec"] = spec
        return WorkerResult(summary="ok")

    runtime.run = _fake_run  # type: ignore[method-assign]

    request = TaskRequest(worker_id="worker", task="hello", max_thinking_steps=19)
    asyncio.run(runtime.run_task(request))

    spec = captured["spec"]
    assert spec.max_thinking_steps == 19


def test_runtime_allows_worker_manage_templates(tmp_path: Path) -> None:
    template = WorkerTemplateRecord(
        id="research_coordinator",
        name="Research Coordinator",
        description="Test worker",
        system_prompt="Coordinate child workers",
        available_tools=["start_child_worker", "get_worker_result"],
        required_permissions=["worker_manage"],
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
            from octopal.runtime.policy.engine import PolicyEngine

            return PolicyEngine().grant_capabilities(capabilities)

    runtime = WorkerRuntime(
        store=_Store(),
        policy=_Policy(),
        workspace_dir=tmp_path,
        launcher=object(),
        mcp_manager=None,
        settings=Settings(),
    )

    captured: dict[str, object] = {}

    async def _fake_run(spec, approval_requester=None):
        captured["spec"] = spec
        return WorkerResult(summary="ok")

    runtime.run = _fake_run  # type: ignore[method-assign]

    result = asyncio.run(runtime.run_task(TaskRequest(worker_id="research_coordinator", task="hello")))

    assert result.status == "completed"
    spec = captured["spec"]
    assert spec.effective_permissions == ["worker_manage"]


def test_runtime_allows_spawn_children_permission_alias(tmp_path: Path) -> None:
    template = WorkerTemplateRecord(
        id="research_coordinator",
        name="Research Coordinator",
        description="Test worker",
        system_prompt="Coordinate child workers",
        available_tools=["start_child_worker", "get_worker_result"],
        required_permissions=["spawn_children", "network"],
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
            from octopal.runtime.policy.engine import PolicyEngine

            return PolicyEngine().grant_capabilities(capabilities)

    runtime = WorkerRuntime(
        store=_Store(),
        policy=_Policy(),
        workspace_dir=tmp_path,
        launcher=object(),
        mcp_manager=None,
        settings=Settings(),
    )

    captured: dict[str, object] = {}

    async def _fake_run(spec, approval_requester=None):
        captured["spec"] = spec
        return WorkerResult(summary="ok")

    runtime.run = _fake_run  # type: ignore[method-assign]

    result = asyncio.run(runtime.run_task(TaskRequest(worker_id="research_coordinator", task="hello")))

    assert result.status == "completed"
    spec = captured["spec"]
    assert spec.effective_permissions == ["worker_manage", "network"]


def test_runtime_persists_preflight_failure_for_worker_status(tmp_path: Path) -> None:
    template = WorkerTemplateRecord(
        id="research_coordinator",
        name="Research Coordinator",
        description="Test worker",
        system_prompt="Coordinate child workers",
        available_tools=["start_child_worker"],
        required_permissions=["worker_manage"],
        model=None,
        max_thinking_steps=3,
        default_timeout_seconds=30,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )

    class _Store:
        def __init__(self) -> None:
            self.records: dict[str, WorkerRecord] = {}

        def get_worker_template(self, worker_id: str):
            return template

        def get_worker(self, worker_id: str):
            return self.records.get(worker_id)

        def create_worker(self, record: WorkerRecord) -> None:
            self.records[record.id] = record

        def update_worker_status(self, worker_id: str, status: str) -> None:
            record = self.records[worker_id]
            self.records[worker_id] = record.model_copy(update={"status": status})

        def update_worker_result(self, worker_id: str, summary=None, output=None, error=None, tools_used=None) -> None:
            record = self.records[worker_id]
            update = {}
            if summary is not None:
                update["summary"] = summary
            if output is not None:
                update["output"] = output
            if error is not None:
                update["error"] = error
            if tools_used is not None:
                update["tools_used"] = tools_used
            self.records[worker_id] = record.model_copy(update=update)

    class _Policy:
        def grant_capabilities(self, capabilities):
            return []

    runtime = WorkerRuntime(
        store=_Store(),
        policy=_Policy(),
        workspace_dir=tmp_path,
        launcher=object(),
        mcp_manager=None,
        settings=Settings(),
    )

    request = TaskRequest(worker_id="research_coordinator", task="hello", run_id="run-1")
    result = asyncio.run(runtime.run_task(request))

    assert result.status == "failed"
    record = runtime.store.get_worker("run-1")
    assert record is not None
    assert record.status == "failed"
    assert record.error == "missing_required_permissions"


def test_runtime_does_not_auto_inject_global_mcp_tools(tmp_path: Path) -> None:
    template = WorkerTemplateRecord(
        id="worker",
        name="Worker",
        description="Test worker",
        system_prompt="Do work",
        available_tools=["fs_read"],
        required_permissions=["filesystem_read"],
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
            return [Capability(type="filesystem_read", scope="worker")]

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
        required_permissions=["mcp_exec"],
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
            return [Capability(type="mcp_exec", scope="worker")]

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


def test_runtime_targets_requested_mcp_server_before_launch(tmp_path: Path) -> None:
    template = WorkerTemplateRecord(
        id="worker",
        name="Worker",
        description="Test worker",
        system_prompt="Do work",
        available_tools=["mcp_agentmail_list_inboxes"],
        required_permissions=["mcp_exec"],
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
            return [Capability(type="mcp_exec", scope="worker")]

    class _MCP:
        def __init__(self) -> None:
            self.sessions = {"AgentMail": object()}
            self.ensure_calls: list[object] = []
            self.resolve_calls: list[list[str]] = []

        def resolve_configured_server_ids_for_tools(self, tool_names):
            self.resolve_calls.append(list(tool_names))
            return ["AgentMail"]

        async def ensure_configured_servers_connected(self, server_ids=None):
            self.ensure_calls.append(server_ids)
            return {"AgentMail": "connected"}

        def get_all_tools(self):
            return [
                ToolSpec(
                    name="mcp_agentmail_list_inboxes",
                    description="demo",
                    parameters={"type": "object"},
                    permission="mcp_exec",
                    handler=lambda _args, _ctx: "ok",
                    is_async=True,
                    server_id="AgentMail",
                    remote_tool_name="list_inboxes",
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
    assert mcp_manager.resolve_calls == [["mcp_agentmail_list_inboxes"]]
    assert mcp_manager.ensure_calls == [["AgentMail"]]
    assert spec.mcp_tools[0]["server_id"] == "AgentMail"


def test_runtime_includes_connector_alias_tools_for_workers(tmp_path: Path) -> None:
    template = WorkerTemplateRecord(
        id="worker",
        name="Worker",
        description="Test worker",
        system_prompt="Do work",
        available_tools=["github_list_repositories"],
        required_permissions=["mcp_exec"],
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
            return [Capability(type="mcp_exec", scope="worker")]

    class _MCP:
        def __init__(self) -> None:
            self.sessions = {"github-core": object()}
            self.ensure_calls: list[object] = []

        async def ensure_configured_servers_connected(self, server_ids=None):
            self.ensure_calls.append(server_ids)
            return {"github-core": "connected"}

        def get_all_tools(self):
            return []

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

    request = TaskRequest(worker_id="worker", task="inspect repos")
    asyncio.run(runtime.run_task(request))

    spec = captured["spec"]
    assert "github_list_repositories" in spec.available_tools
    assert spec.mcp_tools == [
        {
            "name": "github_list_repositories",
            "description": "List repositories visible to the connected GitHub account.",
            "parameters": {
                "type": "object",
                "properties": {
                    "visibility": {"type": "string"},
                    "affiliation": {"type": "string"},
                    "sort": {"type": "string"},
                    "direction": {"type": "string"},
                        "per_page": {"type": "integer", "minimum": 1, "maximum": 50},
                    "page": {"type": "integer", "minimum": 1},
                },
                "additionalProperties": False,
            },
            "permission": "mcp_exec",
            "is_async": True,
            "server_id": "github-core",
            "remote_tool_name": "list_repositories",
        }
    ]


def test_runtime_launch_env_includes_workspace_dir(tmp_path: Path) -> None:
    template = WorkerTemplateRecord(
        id="worker",
        name="Worker",
        description="Test worker",
        system_prompt="Do work",
        available_tools=["fs_read"],
        required_permissions=["filesystem_read"],
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
            return [Capability(type="filesystem_read", scope="worker")]

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
        required_permissions=["filesystem_read"],
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
            return [Capability(type="filesystem_read", scope="worker")]

    settings = Settings(
        config_obj=OctopalConfig(
            llm=LLMConfig(provider_id="zai", model="glm-5.1"),
            worker_llm_default=LLMConfig(provider_id="openrouter", model="x-ai/grok-4.3"),
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
    assert spec.llm_config.model == "x-ai/grok-4.3"


def test_runtime_rejects_task_tool_override_that_widens_template(tmp_path: Path) -> None:
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

    runtime = WorkerRuntime(
        store=_Store(),
        policy=_Policy(),
        workspace_dir=tmp_path,
        launcher=object(),
        settings=Settings(),
    )

    result = asyncio.run(
        runtime.run_task(
            TaskRequest(
                worker_id="worker",
                task="hello",
                tools=["fs_read", "exec_run"],
            )
        )
    )

    assert result.status == "failed"
    assert "requested tools exceed template contract" in result.summary


def test_runtime_rejects_template_tools_with_missing_permissions(tmp_path: Path) -> None:
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

    runtime = WorkerRuntime(
        store=_Store(),
        policy=_Policy(),
        workspace_dir=tmp_path,
        launcher=object(),
        settings=Settings(),
    )

    result = asyncio.run(runtime.run_task(TaskRequest(worker_id="worker", task="hello")))

    assert result.status == "failed"
    assert "requires permission 'filesystem_read'" in result.summary


def test_runtime_worker_tool_permission_validation_reports_all_missing_permissions() -> None:
    error = _validate_worker_tool_permissions(
        tool_names=["exec_run", "use_skill", "fs_write"],
        allowed_permissions=["network"],
        all_tools_by_name={
            "exec_run": SimpleNamespace(permission="exec"),
            "use_skill": SimpleNamespace(permission="skill_use"),
            "fs_write": SimpleNamespace(permission="filesystem_write"),
        },
    )

    assert error is not None
    assert "tool 'exec_run' requires permission 'exec'" in error
    assert "tool 'use_skill' requires permission 'skill_use'" in error
    assert "tool 'fs_write' requires permission 'filesystem_write'" in error
    assert "missing permission(s): exec, filesystem_write, skill_use" in error


def test_runtime_local_bridge_guard_rejects_tool_outside_spec_permissions() -> None:
    spec = type(
        "_Spec",
        (),
        {
            "available_tools": ["start_worker"],
            "effective_permissions": ["network"],
        },
    )()

    error = _validate_worker_local_tool_call(
        spec=spec,
        tool_name="start_worker",
        permission="worker_manage",
    )

    assert error is not None
    assert "requires permission 'worker_manage'" in error


def test_runtime_mcp_bridge_guard_rejects_tool_not_in_spec() -> None:
    spec = type(
        "_Spec",
        (),
        {
            "mcp_tools": [
                {
                    "name": "github_list_repositories",
                    "server_id": "github-core",
                    "permission": "mcp_exec",
                }
            ],
            "effective_permissions": ["mcp_exec"],
        },
    )()

    error = _validate_worker_mcp_tool_call(
        spec=spec,
        server_id="github-core",
        tool_name="github_get_pull_request",
    )

    assert error is not None
    assert "is not allowed by this worker spec" in error


def test_runtime_mcp_bridge_guard_allows_remote_tool_name_match() -> None:
    spec = type(
        "_Spec",
        (),
        {
            "mcp_tools": [
                {
                    "name": "mcp_AgentMail_list_threads",
                    "server_id": "AgentMail",
                    "remote_tool_name": "list_threads",
                    "permission": "mcp_exec",
                }
            ],
            "effective_permissions": ["mcp_exec"],
        },
    )()

    error = _validate_worker_mcp_tool_call(
        spec=spec,
        server_id="AgentMail",
        tool_name="list_threads",
    )

    assert error is None
