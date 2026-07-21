from __future__ import annotations

import asyncio
import contextlib

from octopal.infrastructure.mcp.manager import (
    MCPManager,
    MCPServerConfig,
    _classify_mcp_call_error,
    _resolve_configured_server_id_for_tool_name,
)
from octopal.runtime.tool_errors import MCPToolCallError
from octopal.tools.registry import ToolSpec


def test_mcp_manager_schedules_self_healing_reconnect(tmp_path, monkeypatch) -> None:
    manager = MCPManager(tmp_path)
    manager._server_configs["demo"] = MCPServerConfig(
        id="demo",
        name="Demo",
        command="demo-cmd",
        transport="stdio",
    )

    calls: list[str] = []

    async def _fake_connect(config: MCPServerConfig):
        calls.append(config.id)
        return []

    monkeypatch.setattr("octopal.infrastructure.mcp.manager._MCP_RECONNECT_BASE_SECONDS", 0.01)
    monkeypatch.setattr("octopal.infrastructure.mcp.manager._MCP_RECONNECT_MAX_SECONDS", 0.01)
    manager.connect_server = _fake_connect  # type: ignore[method-assign]

    async def scenario():
        manager._schedule_reconnect("demo")
        await asyncio.sleep(0.05)

    asyncio.run(scenario())
    assert calls == ["demo"]


def test_mcp_manager_reconnect_retries_after_a_failed_attempt(tmp_path, monkeypatch) -> None:
    manager = MCPManager(tmp_path)
    manager._server_configs["demo"] = MCPServerConfig(
        id="demo",
        name="Demo",
        command="demo-cmd",
        transport="stdio",
    )

    calls: list[str] = []

    async def _fake_connect(config: MCPServerConfig):
        calls.append(config.id)
        if len(calls) == 1:
            raise RuntimeError("connection closed")
        return []

    monkeypatch.setattr("octopal.infrastructure.mcp.manager._MCP_RECONNECT_BASE_SECONDS", 0.01)
    monkeypatch.setattr("octopal.infrastructure.mcp.manager._MCP_RECONNECT_MAX_SECONDS", 0.01)
    manager.connect_server = _fake_connect  # type: ignore[method-assign]

    async def scenario() -> None:
        manager._schedule_reconnect("demo")
        for _ in range(25):
            if len(calls) == 2:
                return
            await asyncio.sleep(0.01)

    asyncio.run(scenario())

    assert calls == ["demo", "demo"]
    assert "demo" not in manager._reconnect_tasks


def test_mcp_manager_reconnect_attempt_does_not_cancel_itself(tmp_path, monkeypatch) -> None:
    manager = MCPManager(tmp_path)
    manager._server_configs["demo"] = MCPServerConfig(
        id="demo",
        name="Demo",
        command="demo-cmd",
        transport="stdio",
    )
    attempts: list[str] = []

    async def _exit_before_ready(config: MCPServerConfig, *_args) -> None:
        attempts.append(config.id)

    monkeypatch.setattr(manager, "_run_server_lifecycle", _exit_before_ready)
    monkeypatch.setattr("octopal.infrastructure.mcp.manager._MCP_RECONNECT_BASE_SECONDS", 0.01)
    monkeypatch.setattr("octopal.infrastructure.mcp.manager._MCP_RECONNECT_MAX_SECONDS", 0.01)

    async def scenario() -> None:
        manager._schedule_reconnect("demo")
        for _ in range(25):
            if len(attempts) >= 2:
                break
            await asyncio.sleep(0.01)
        await manager.shutdown()

    asyncio.run(scenario())

    assert attempts == ["demo", "demo"]


def test_mcp_manager_does_not_reconnect_after_intentional_disconnect(tmp_path, monkeypatch) -> None:
    manager = MCPManager(tmp_path)
    manager._server_configs["demo"] = MCPServerConfig(
        id="demo",
        name="Demo",
        command="demo-cmd",
        transport="stdio",
    )

    calls: list[str] = []

    async def _fake_connect(config: MCPServerConfig):
        calls.append(config.id)
        return []

    monkeypatch.setattr("octopal.infrastructure.mcp.manager._MCP_RECONNECT_BASE_SECONDS", 0.01)
    monkeypatch.setattr("octopal.infrastructure.mcp.manager._MCP_RECONNECT_MAX_SECONDS", 0.01)
    manager.connect_server = _fake_connect  # type: ignore[method-assign]

    async def scenario():
        await manager.disconnect_server("demo", intentional=True)
        manager._schedule_reconnect("demo")
        await asyncio.sleep(0.05)

    asyncio.run(scenario())
    assert calls == []


def test_mcp_manager_statuses_report_reconnecting_and_reason(tmp_path) -> None:
    manager = MCPManager(tmp_path)
    manager._server_configs["demo"] = MCPServerConfig(
        id="demo",
        name="Demo",
        command="demo-cmd",
        transport="stdio",
        last_error="socket closed",
    )

    async def _idle() -> None:
        await asyncio.sleep(10)

    async def scenario():
        manager._reconnect_attempts["demo"] = 2
        manager._reconnect_tasks["demo"] = asyncio.create_task(_idle())
        statuses = manager.get_server_statuses()
        task = manager._reconnect_tasks.pop("demo")
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return statuses

    statuses = asyncio.run(scenario())
    payload = statuses["demo"]
    assert payload["status"] == "reconnecting"
    assert payload["configured"] is True
    assert payload["connected"] is False
    assert payload["reconnecting"] is True
    assert payload["reason"] == "Background reconnect scheduled"
    assert payload["reconnect_attempts"] == 2


def test_mcp_manager_cleans_ready_waiter_after_connection_failure(tmp_path, monkeypatch) -> None:
    manager = MCPManager(tmp_path)
    config = MCPServerConfig(id="demo", name="Demo", command="demo-cmd", transport="stdio")

    async def _fail_lifecycle(*_args) -> None:
        raise RuntimeError("connection closed")

    monkeypatch.setattr(manager, "_run_server_lifecycle", _fail_lifecycle)

    async def scenario() -> None:
        with contextlib.suppress(RuntimeError):
            await manager.connect_server(config)
        await asyncio.sleep(0)
        current = asyncio.current_task()
        assert [
            task for task in asyncio.all_tasks() if task is not current and not task.done()
        ] == []

    asyncio.run(scenario())


def test_mcp_manager_shutdown_waits_for_reconnect_tasks(tmp_path) -> None:
    manager = MCPManager(tmp_path)
    manager._server_configs["demo"] = MCPServerConfig(
        id="demo",
        name="Demo",
        command="demo-cmd",
        transport="stdio",
    )

    async def scenario() -> None:
        manager._schedule_reconnect("demo")
        reconnect_task = manager._reconnect_tasks["demo"]
        await manager.shutdown()
        assert reconnect_task.done()
        assert manager._reconnect_tasks == {}

    asyncio.run(scenario())


def test_classify_mcp_invalid_arguments_preserves_missing_fields() -> None:
    error = RuntimeError("""MCP error -32602: Invalid arguments for tool analyze_image: [
  {
    "code": "invalid_type",
    "expected": "string",
    "received": "undefined",
    "path": [
      "image_source"
    ],
    "message": "Required"
  },
  {
    "code": "invalid_type",
    "expected": "string",
    "received": "undefined",
    "path": [
      "prompt"
    ],
    "message": "Required"
  }
]""")

    info = _classify_mcp_call_error(error)

    assert info["classification"] == "invalid_arguments"
    assert info["retryable"] is False
    assert "image_source" in info["hint"]
    assert "prompt" in info["hint"]


def test_classify_mcp_schema_mismatch_stays_distinct() -> None:
    error = RuntimeError("invalid tools/call result: structuredContent did not match schema")

    info = _classify_mcp_call_error(error)

    assert info["classification"] == "schema_mismatch"
    assert "structuredContent" in info["hint"]


def test_call_tool_preserves_schema_mismatch_metadata(tmp_path) -> None:
    manager = MCPManager(tmp_path)

    class _Session:
        async def call_tool(self, _tool_name: str, arguments: dict):
            raise RuntimeError("invalid tools/call result: structuredContent did not match schema")

    async def scenario() -> None:
        manager.sessions["demo"] = _Session()
        await manager.call_tool("demo", "get_thread", {"thread_id": "abc"})

    try:
        asyncio.run(scenario())
    except MCPToolCallError as exc:
        assert exc.bridge == "mcp"
        assert exc.classification == "schema_mismatch"
        assert exc.retryable is False
        assert exc.server_id == "demo"
        assert exc.tool_name == "get_thread"
    else:
        raise AssertionError("Expected structured MCP tool error")


def test_mcp_manager_hydrates_full_schema_from_compact_registry_stub(tmp_path) -> None:
    manager = MCPManager(tmp_path)
    compact_schema = {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Search query"},
        },
        "required": ["query"],
        "additionalProperties": False,
    }
    full_schema = {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Search query with extra guidance for remote MCP validation.",
                "minLength": 3,
            },
            "limit": {"type": "integer", "minimum": 1, "maximum": 100},
        },
        "required": ["query"],
        "additionalProperties": False,
    }
    spec = ToolSpec(
        name="mcp_demo_search",
        description="demo",
        parameters=compact_schema,
        permission="mcp_exec",
        handler=lambda _args, _ctx: {"ok": True},
        is_async=True,
        server_id="demo",
        remote_tool_name="search",
    )

    manager._tools["demo"] = [spec]
    manager._tool_schemas[("demo", "mcp_demo_search")] = full_schema

    registry_spec = manager.get_all_tools()[0]
    hydrated_spec = manager.hydrate_tool_spec(registry_spec)

    assert registry_spec.parameters == compact_schema
    assert hydrated_spec.parameters == full_schema


def test_mcp_manager_reads_legacy_workspace_mcp_config(tmp_path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "mcp.json").write_text(
        """
{
  "servers": {
    "AgentMail": {
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "agentmail-mcp"],
      "tools": ["mcp_AgentMail_list_inboxes"]
    }
  }
}
""".strip(),
        encoding="utf-8",
    )

    manager = MCPManager(tmp_path)

    resolved = manager.resolve_configured_server_ids_for_tools(["mcp_agentmail_list_inboxes"])

    assert resolved == ["AgentMail"]


def test_mcp_manager_reads_claude_style_mcp_servers_config(tmp_path) -> None:
    (tmp_path / ".mcp.json").write_text(
        """
{
  "mcpServers": {
    "minimax": {
      "command": "uvx",
      "args": ["minimax-coding-plan-mcp"],
      "tools": ["mcp_minimax_web_search"]
    }
  }
}
""".strip(),
        encoding="utf-8",
    )

    manager = MCPManager(tmp_path)

    resolved = manager.resolve_configured_server_ids_for_tools(["mcp_minimax_web_search"])

    assert resolved == ["minimax"]


def test_mcp_manager_skips_empty_compat_config_and_reads_canonical(tmp_path) -> None:
    (tmp_path / ".mcp.json").write_text("", encoding="utf-8")
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "mcp.json").write_text(
        """
{
  "servers": {
    "docs": {
      "command": "uvx",
      "args": ["docs-mcp"],
      "tools": ["mcp_docs_search"]
    }
  }
}
""".strip(),
        encoding="utf-8",
    )

    manager = MCPManager(tmp_path)

    resolved = manager.resolve_configured_server_ids_for_tools(["mcp_docs_search"])

    assert resolved == ["docs"]


def test_resolve_configured_server_id_for_tool_name_is_case_insensitive(tmp_path) -> None:
    configs = {
        "AgentMail": MCPServerConfig(
            id="AgentMail",
            name="AgentMail",
            command="npx",
            tools=["mcp_AgentMail_list_inboxes"],
        )
    }

    resolved = _resolve_configured_server_id_for_tool_name(
        "mcp_agentmail_list_inboxes",
        configs,
    )

    assert resolved == "AgentMail"


def test_mcp_manager_ensure_empty_list_skips_connections(tmp_path) -> None:
    (tmp_path / "mcp_servers.json").write_text(
        """
{
  "demo": {
    "command": "demo-cmd",
    "type": "stdio"
  }
}
""".strip(),
        encoding="utf-8",
    )

    manager = MCPManager(tmp_path)
    calls: list[str] = []

    async def _fake_connect(config: MCPServerConfig):
        calls.append(config.id)
        return []

    manager.connect_server = _fake_connect  # type: ignore[method-assign]

    result = asyncio.run(manager.ensure_configured_servers_connected([]))

    assert result == {}
    assert calls == []
