from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from mcp import types as mcp_types

from octopal.infrastructure.mcp.manager import MCPManager, MCPServerConfig
from octopal.infrastructure.mcp.tasks import (
    MCPTaskContext,
    build_task_record,
    client_capability_meta,
    extension_declared,
    legacy_tasks_declared,
    parse_task_state,
    task_ref,
    task_status_result,
)
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.tool_errors import MCPToolCallError


class _StoreSettings:
    def __init__(self, state_dir, workspace_dir) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


def _store(tmp_path) -> SQLiteStore:
    return SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))


def _timestamp(seconds: int = 0) -> str:
    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat()


class _ExtensionSession:
    def __init__(self, *, created: dict[str, Any], states: list[dict[str, Any]]) -> None:
        self.created = created
        self.states = list(states)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def send_request(self, request, result_type, **_kwargs):
        self.calls.append((request.method, request.params))
        if request.method == "tools/call":
            payload = self.created
        elif request.method == "tasks/get":
            payload = self.states.pop(0)
        elif request.method in {"tasks/update", "tasks/cancel"}:
            payload = {"resultType": "complete"}
        else:
            raise AssertionError(f"Unexpected request: {request.method}")
        return result_type.model_validate(payload)


class _LegacyFeatures:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.created_at = datetime.now(UTC)

    async def call_tool_as_task(self, _name, _arguments, *, ttl):
        self.calls.append(f"create:{ttl}")
        return mcp_types.CreateTaskResult(
            task=mcp_types.Task(
                taskId="legacy-secret-id",
                status="working",
                createdAt=self.created_at,
                lastUpdatedAt=self.created_at,
                ttl=ttl,
                pollInterval=0,
            )
        )

    async def get_task(self, _task_id):
        self.calls.append("get")
        return mcp_types.GetTaskResult(
            taskId="legacy-secret-id",
            status="completed",
            createdAt=self.created_at,
            lastUpdatedAt=datetime.now(UTC),
            ttl=60_000,
            pollInterval=0,
        )

    async def get_task_result(self, _task_id, result_type):
        self.calls.append("result")
        return result_type(content=[mcp_types.TextContent(type="text", text="legacy complete")])


class _LegacySession:
    def __init__(self) -> None:
        self.experimental = _LegacyFeatures()


def _configured_manager(tmp_path, *, protocol: str, session: Any) -> MCPManager:
    manager = MCPManager(tmp_path / "workspace", store=_store(tmp_path))
    manager._server_configs["demo"] = MCPServerConfig(
        id="demo",
        name="Demo",
        command="demo-server",
        env={"DEMO_TOKEN": "secret-value"},
        transport="stdio",
    )
    manager.sessions["demo"] = session
    manager._task_protocols["demo"] = protocol  # type: ignore[assignment]
    return manager


def test_task_capability_helpers_cover_extension_and_legacy() -> None:
    assert extension_declared({"extensions": {"io.modelcontextprotocol/tasks": {}}})
    assert legacy_tasks_declared({"tasks": {"requests": {"tools": {"call": {}}}}})
    assert client_capability_meta() == {
        "io.modelcontextprotocol/clientCapabilities": {
            "extensions": {"io.modelcontextprotocol/tasks": {}}
        }
    }


def test_task_state_mapping_and_public_result_hide_remote_handle() -> None:
    state = parse_task_state(
        {
            "resultType": "task",
            "taskId": "remote-bearer-token",
            "status": "input_required",
            "createdAt": _timestamp(),
            "lastUpdatedAt": _timestamp(),
            "ttlMs": 60_000,
            "pollIntervalMs": 2500,
            "inputRequests": {"approval": {"method": "elicitation/create"}},
        },
        protocol="extension",
    )
    record = build_task_record(
        state=state,
        server_id="demo",
        tool_name="deploy",
        protocol="extension",
        auth_context_id="auth-fingerprint",
        context=MCPTaskContext(worker_run_id="worker-1", chat_turn_id="42:3"),
    )

    result = task_status_result(record)
    text = result.content[0].text

    assert record.runtime_status == "awaiting_instruction"
    assert record.input_requests == {"approval": {"method": "elicitation/create"}}
    assert record.id in text
    assert "remote-bearer-token" not in text
    assert task_ref("remote-bearer-token") not in text
    assert "remote-bearer-token" not in repr(record)


def test_extension_task_polls_to_completed_and_persists_bindings(tmp_path) -> None:
    created_at = _timestamp()
    session = _ExtensionSession(
        created={
            "resultType": "task",
            "taskId": "extension-secret-id",
            "status": "working",
            "createdAt": created_at,
            "lastUpdatedAt": created_at,
            "ttlMs": 60_000,
            "pollIntervalMs": 0,
        },
        states=[
            {
                "resultType": "complete",
                "taskId": "extension-secret-id",
                "status": "completed",
                "createdAt": created_at,
                "lastUpdatedAt": _timestamp(1),
                "ttlMs": 60_000,
                "pollIntervalMs": 0,
                "result": {
                    "content": [{"type": "text", "text": "extension complete"}],
                    "isError": False,
                },
            }
        ],
    )
    manager = _configured_manager(tmp_path, protocol="extension", session=session)
    context = MCPTaskContext(
        correlation_id="corr-1",
        trace_id="trace-1",
        span_id="span-1",
        worker_run_id="worker-1",
        chat_id=42,
        chat_turn_id="42:3",
        plan_run_id="plan-1",
        plan_step_id="step-2",
    )

    result = asyncio.run(
        manager.call_tool(
            "demo",
            "long_tool",
            {"value": 1},
            task_context=context,
        )
    )

    assert result.content[0].text == "extension complete"
    records = manager.store.list_recoverable_mcp_tasks()  # type: ignore[union-attr]
    assert records == []
    audits = manager.store.list_audit_for_correlation("corr-1")  # type: ignore[union-attr]
    assert [event.event_type for event in audits] == [
        "mcp_task_created",
        "mcp_task_status_changed",
    ]
    task_id = audits[0].data["task_id"]
    record = manager.store.get_mcp_task(task_id)  # type: ignore[union-attr]
    assert record is not None
    assert record.remote_status == "completed"
    assert record.trace_id == "trace-1"
    assert record.span_id == "span-1"
    assert record.chat_turn_id == "42:3"
    assert record.plan_run_id == "plan-1"
    assert record.plan_step_id == "step-2"
    assert session.calls[0][1]["_meta"] == client_capability_meta()


def test_extension_sync_result_preserves_normal_tool_contract(tmp_path) -> None:
    session = _ExtensionSession(
        created={
            "resultType": "complete",
            "content": [{"type": "text", "text": "immediate"}],
            "isError": False,
        },
        states=[],
    )
    manager = _configured_manager(tmp_path, protocol="extension", session=session)

    result = asyncio.run(manager.call_tool("demo", "sync_tool", {"value": 1}))

    assert result.content[0].text == "immediate"
    assert manager.store.list_recoverable_mcp_tasks() == []  # type: ignore[union-attr]
    assert manager.store.list_audit() == []  # type: ignore[union-attr]


def test_extension_input_required_returns_durable_wait_state(tmp_path) -> None:
    created_at = _timestamp()
    session = _ExtensionSession(
        created={
            "resultType": "task",
            "taskId": "input-secret-id",
            "status": "input_required",
            "createdAt": created_at,
            "lastUpdatedAt": created_at,
            "ttlMs": 60_000,
            "pollIntervalMs": 1000,
            "inputRequests": {
                "approval": {
                    "method": "elicitation/create",
                    "params": {"message": "Approve deployment?"},
                }
            },
        },
        states=[],
    )
    manager = _configured_manager(tmp_path, protocol="extension", session=session)

    result = asyncio.run(
        manager.call_tool(
            "demo",
            "deploy",
            {},
            task_context=MCPTaskContext(worker_run_id="worker-1"),
        )
    )

    assert result.structuredContent["mcp_task"]["runtime_status"] == "awaiting_instruction"
    task_id = result.structuredContent["mcp_task"]["id"]
    stored = manager.store.get_mcp_task(task_id)  # type: ignore[union-attr]
    assert stored is not None
    assert stored.remote_status == "input_required"
    assert stored.worker_run_id == "worker-1"


def test_legacy_task_uses_sdk_lifecycle_and_sync_result_contract(tmp_path) -> None:
    session = _LegacySession()
    manager = _configured_manager(tmp_path, protocol="legacy", session=session)
    manager._tool_task_support[("demo", "legacy_tool")] = "optional"

    result = asyncio.run(manager.call_tool("demo", "legacy_tool", {"value": 1}))

    assert result.content[0].text == "legacy complete"
    assert session.experimental.calls[0].startswith("create:")
    assert session.experimental.calls[1:] == ["get", "result"]


def test_task_access_is_bound_to_originating_worker(tmp_path) -> None:
    created_at = _timestamp()
    session = _ExtensionSession(
        created={
            "resultType": "task",
            "taskId": "bound-secret-id",
            "status": "input_required",
            "createdAt": created_at,
            "lastUpdatedAt": created_at,
            "ttlMs": 60_000,
            "inputRequests": {"x": {"method": "elicitation/create"}},
        },
        states=[],
    )
    manager = _configured_manager(tmp_path, protocol="extension", session=session)
    result = asyncio.run(
        manager.call_tool(
            "demo",
            "bound_tool",
            {},
            task_context=MCPTaskContext(worker_run_id="worker-a"),
        )
    )
    task_id = result.structuredContent["mcp_task"]["id"]

    with pytest.raises(PermissionError, match="another worker"):
        asyncio.run(
            manager.get_task(
                task_id,
                task_context=MCPTaskContext(worker_run_id="worker-b"),
            )
        )


def test_extension_update_and_cancel_use_bound_durable_handle(tmp_path) -> None:
    created_at = _timestamp()
    session = _ExtensionSession(
        created={
            "resultType": "task",
            "taskId": "managed-secret-id",
            "status": "input_required",
            "createdAt": created_at,
            "lastUpdatedAt": created_at,
            "ttlMs": 60_000,
            "inputRequests": {"answer": {"method": "elicitation/create"}},
        },
        states=[],
    )
    manager = _configured_manager(tmp_path, protocol="extension", session=session)
    context = MCPTaskContext(worker_run_id="worker-a")
    initial = asyncio.run(manager.call_tool("demo", "managed_tool", {}, task_context=context))
    task_id = initial.structuredContent["mcp_task"]["id"]
    session.states.append(
        {
            "resultType": "complete",
            "taskId": "managed-secret-id",
            "status": "input_required",
            "createdAt": created_at,
            "lastUpdatedAt": _timestamp(1),
            "ttlMs": 60_000,
            "pollIntervalMs": 1000,
            "inputRequests": {"answer": {"method": "elicitation/create"}},
        }
    )

    updated = asyncio.run(
        manager.update_task(
            task_id,
            {"answer": {"action": "accept", "content": {"value": "yes"}}},
            task_context=context,
        )
    )
    with pytest.raises(MCPToolCallError) as duplicate_error:
        asyncio.run(
            manager.update_task(
                task_id,
                {"answer": {"action": "accept", "content": {"value": "yes"}}},
                task_context=context,
            )
        )
    cancelled = asyncio.run(manager.cancel_task(task_id, task_context=context))

    assert updated.remote_status == "input_required"
    assert updated.responded_input_keys == ["answer"]
    assert updated.input_requests == {}
    assert duplicate_error.value.classification == "invalid_task_input"
    assert cancelled.id == task_id
    assert [method for method, _params in session.calls][-3:] == [
        "tasks/update",
        "tasks/get",
        "tasks/cancel",
    ]
    audits = manager.store.list_audit()  # type: ignore[union-attr]
    assert "mcp_task_input_submitted" in {event.event_type for event in audits}
    assert "mcp_task_cancel_requested" in {event.event_type for event in audits}


def test_recovery_resumes_working_task_from_store(tmp_path) -> None:
    created_at = _timestamp()
    store = _store(tmp_path)
    first = MCPManager(tmp_path / "workspace", store=store)
    config = MCPServerConfig(
        id="demo",
        name="Demo",
        command="demo-server",
        env={"DEMO_TOKEN": "secret-value"},
        transport="stdio",
    )
    first._server_configs["demo"] = config
    state = parse_task_state(
        {
            "resultType": "task",
            "taskId": "recover-secret-id",
            "status": "working",
            "createdAt": created_at,
            "lastUpdatedAt": created_at,
            "ttlMs": 60_000,
            "pollIntervalMs": 0,
        },
        protocol="extension",
    )
    record = asyncio.run(
        first._persist_task_state(
            state,
            server_id="demo",
            tool_name="recover_tool",
            protocol="extension",
            task_context=MCPTaskContext(correlation_id="corr-recovery"),
        )
    )
    session = _ExtensionSession(
        created={},
        states=[
            {
                "resultType": "complete",
                "taskId": "recover-secret-id",
                "status": "completed",
                "createdAt": created_at,
                "lastUpdatedAt": _timestamp(1),
                "ttlMs": 60_000,
                "pollIntervalMs": 0,
                "result": {
                    "content": [{"type": "text", "text": "recovered"}],
                    "isError": False,
                },
            }
        ],
    )
    restarted = MCPManager(tmp_path / "workspace", store=store)
    restarted._server_configs["demo"] = config
    restarted.sessions["demo"] = session
    restarted._task_protocols["demo"] = "extension"

    async def recover() -> None:
        await restarted._schedule_task_recovery("demo")
        tasks = list(restarted._task_recovery_tasks.values())
        assert len(tasks) == 1
        await asyncio.gather(*tasks)

    asyncio.run(recover())

    recovered = store.get_mcp_task(record.id)
    assert recovered is not None
    assert recovered.remote_status == "completed"
    assert restarted._task_metrics["recovered"] == 1
    assert any(
        event.event_type == "mcp_task_recovery_started"
        for event in store.list_audit_for_correlation("corr-recovery")
    )


def test_protocol_negotiation_prefers_extension_and_supports_legacy(tmp_path) -> None:
    manager = MCPManager(tmp_path, store=_store(tmp_path))

    extension = asyncio.run(
        manager._negotiate_task_protocol(
            SimpleNamespace(),
            SimpleNamespace(
                protocolVersion="2026-06-30",
                capabilities={"extensions": {"io.modelcontextprotocol/tasks": {}}},
            ),
        )
    )
    legacy = asyncio.run(
        manager._negotiate_task_protocol(
            SimpleNamespace(),
            SimpleNamespace(
                protocolVersion="2025-11-25",
                capabilities={"tasks": {"requests": {"tools": {"call": {}}}}},
            ),
        )
    )

    assert extension == "extension"
    assert legacy == "legacy"


def test_protocol_negotiation_requires_durable_store_and_matching_version(tmp_path) -> None:
    extension_capabilities = {"extensions": {"io.modelcontextprotocol/tasks": {}}}
    without_store = MCPManager(tmp_path)
    with_store = MCPManager(tmp_path, store=_store(tmp_path))

    no_store = asyncio.run(
        without_store._negotiate_task_protocol(
            SimpleNamespace(),
            SimpleNamespace(
                protocolVersion="2026-06-30",
                capabilities=extension_capabilities,
            ),
        )
    )
    wrong_version = asyncio.run(
        with_store._negotiate_task_protocol(
            SimpleNamespace(),
            SimpleNamespace(
                protocolVersion="2025-11-25",
                capabilities=extension_capabilities,
            ),
        )
    )

    assert no_store is None
    assert wrong_version is None


def test_task_state_requires_server_timestamps() -> None:
    with pytest.raises(ValueError, match="required timestamp"):
        parse_task_state(
            {
                "taskId": "missing-timestamp",
                "status": "working",
                "createdAt": _timestamp(),
            },
            protocol="extension",
        )


def test_terminal_task_state_cannot_regress_from_stale_poll(tmp_path) -> None:
    manager = _configured_manager(
        tmp_path,
        protocol="extension",
        session=_ExtensionSession(created={}, states=[]),
    )
    created_at = _timestamp(-10)
    completed = parse_task_state(
        {
            "taskId": "terminal-secret-id",
            "status": "completed",
            "createdAt": created_at,
            "lastUpdatedAt": _timestamp(),
            "ttlMs": 60_000,
            "result": {
                "content": [{"type": "text", "text": "done"}],
                "isError": False,
            },
        },
        protocol="extension",
    )
    record = asyncio.run(
        manager._persist_task_state(
            completed,
            server_id="demo",
            tool_name="terminal_tool",
            protocol="extension",
            task_context=MCPTaskContext(),
        )
    )
    stale = parse_task_state(
        {
            "taskId": "terminal-secret-id",
            "status": "working",
            "createdAt": created_at,
            "lastUpdatedAt": _timestamp(-5),
            "ttlMs": 60_000,
        },
        protocol="extension",
    )

    preserved = asyncio.run(
        manager._persist_task_state(
            stale,
            server_id="demo",
            tool_name="terminal_tool",
            protocol="extension",
            task_context=MCPTaskContext(),
            previous=record,
        )
    )

    assert preserved.remote_status == "completed"
    assert manager.store.get_mcp_task(record.id).remote_status == "completed"  # type: ignore[union-attr]


def test_expired_task_fails_locally_without_another_remote_poll(tmp_path) -> None:
    created_at = _timestamp(-120)
    session = _ExtensionSession(
        created={
            "resultType": "task",
            "taskId": "expired-secret-id",
            "status": "working",
            "createdAt": created_at,
            "lastUpdatedAt": created_at,
            "ttlMs": 1,
            "pollIntervalMs": 0,
        },
        states=[],
    )
    manager = _configured_manager(tmp_path, protocol="extension", session=session)

    with pytest.raises(MCPToolCallError) as error:
        asyncio.run(manager.call_tool("demo", "expired_tool", {}))

    assert error.value.classification == "task_ttl_expired"
    assert [method for method, _params in session.calls] == ["tools/call"]
    task_id = error.value.details["task_id"]
    stored = manager.store.get_mcp_task(task_id)  # type: ignore[union-attr]
    assert stored is not None
    assert stored.remote_status == "working"
    assert stored.runtime_status == "failed"
    assert manager.store.list_recoverable_mcp_tasks() == []  # type: ignore[union-attr]
