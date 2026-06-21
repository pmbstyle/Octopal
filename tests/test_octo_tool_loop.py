from __future__ import annotations

import asyncio
import json

import pytest

from octopal.runtime.capability_outcomes import CAPABILITY_OUTCOME_KEY
from octopal.runtime.octo.router import (
    _budget_tool_specs,
    _build_octo_tool_policy_summary,
    _dangerous_exec_command_reason,
    _get_octo_tools,
    _get_scheduled_octo_control_tools,
    _handle_octo_tool_call,
    _record_octo_tool_call,
    _tool_result_payload_error_type,
)
from octopal.tools.catalog import get_tools
from octopal.tools.diagnostics import resolve_tool_diagnostics
from octopal.tools.metadata import ToolMetadata
from octopal.tools.registry import ToolSpec


def _tool(name: str, *, handler, is_async: bool = False) -> ToolSpec:
    return ToolSpec(
        name=name,
        description=f"{name} tool",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
        permission="network",
        handler=handler,
        is_async=is_async,
    )


@pytest.mark.asyncio
async def test_handle_octo_tool_call_reports_unknown_tool() -> None:
    result, meta = await _handle_octo_tool_call(
        {"function": {"name": "missing_tool", "arguments": "{}"}},
        [],
        {},
    )

    assert result == {"error": "Unknown tool: missing_tool"}
    assert meta["had_error"] is True


@pytest.mark.asyncio
async def test_handle_octo_tool_call_skips_stale_chat_turn_before_handler() -> None:
    calls: list[dict] = []

    class DummyOcto:
        def is_chat_turn_epoch_current(self, chat_id: int, epoch: int | None) -> bool:
            return False

    tool = _tool("delete_comment", handler=lambda args, _ctx: calls.append(args))

    result, meta = await _handle_octo_tool_call(
        {"function": {"name": "delete_comment", "arguments": '{"id":"c1"}'}},
        [tool],
        {"octo": DummyOcto(), "chat_id": 123, "chat_turn_epoch": 1},
    )

    assert calls == []
    assert result["status"] == "stale"
    assert result["tool"] == "delete_comment"
    assert meta["had_error"] is True
    assert meta["error_type"] == "stale_chat_turn_epoch"


def test_octo_tool_policy_requires_approval_for_dangerous_exec_run() -> None:
    class DummyOcto:
        mcp_manager = None

    tool_specs, ctx = _get_octo_tools(DummyOcto(), 0)

    assert "exec_run" in {tool.name for tool in tool_specs}
    assert "test_run" not in {tool.name for tool in tool_specs}

    async def scenario() -> None:
        result, meta = await _handle_octo_tool_call(
            {"function": {"name": "exec_run", "arguments": '{"command":"rm -rf workspace"}'}},
            tool_specs,
            ctx,
        )

        assert result["type"] == "approval_required"
        assert result["tool"] == "exec_run"
        assert "dangerous" in result["message"].lower()
        assert result[CAPABILITY_OUTCOME_KEY]["kind"] == "needs_approval"
        assert "approval" in result[CAPABILITY_OUTCOME_KEY]["next_action"].lower()
        assert meta["error_type"] == "approval_required"

    asyncio.run(scenario())


def test_exec_run_approval_detector_ignores_dangerous_words_in_arguments() -> None:
    assert _dangerous_exec_command_reason("echo rm -rf workspace") is None
    assert _dangerous_exec_command_reason("printf 'sudo true'") is None
    assert (
        _dangerous_exec_command_reason(
            "find workspace/config-type 2>/dev/null; ls workspace/config/ 2>/dev/null"
        )
        is None
    )
    assert (
        _dangerous_exec_command_reason("echo ok && rm -rf workspace")
        == "uses dangerous command `rm`"
    )
    assert _dangerous_exec_command_reason("echo ok > /dev/sda") == "writes to a device path"


def test_octo_exec_run_uses_direct_approval_for_dangerous_commands() -> None:
    calls: list[object] = []
    tool = ToolSpec(
        name="exec_run",
        description="exec",
        parameters={"type": "object"},
        permission="exec",
        handler=lambda _args, _ctx: {"ok": True},
    )

    async def requester(intent):
        calls.append(intent)
        return True

    async def scenario() -> None:
        result, meta = await _handle_octo_tool_call(
            {"function": {"name": "exec_run", "arguments": '{"command":"sudo true"}'}},
            [tool],
            {"approval_requester": requester},
        )

        assert result == {"ok": True}
        assert meta["had_error"] is False

    asyncio.run(scenario())

    assert len(calls) == 1
    assert calls[0].type == "exec.run"
    assert calls[0].requires_approval is True
    assert calls[0].risk == "high"


def test_octo_computer_use_requires_approval_for_mutating_actions() -> None:
    tool = ToolSpec(
        name="computer_use",
        description="desktop",
        parameters={"type": "object"},
        permission="desktop_control",
        handler=lambda _args, _ctx: {"ok": True},
    )

    async def scenario() -> None:
        result, meta = await _handle_octo_tool_call(
            {
                "function": {
                    "name": "computer_use",
                    "arguments": '{"action":"click","pid":123,"element_index":4}',
                }
            },
            [tool],
            {},
        )

        assert result["type"] == "approval_required"
        assert result["tool"] == "computer_use"
        assert result[CAPABILITY_OUTCOME_KEY]["kind"] == "needs_approval"
        assert meta["error_type"] == "approval_required"

    asyncio.run(scenario())


def test_octo_computer_use_uses_direct_approval_for_mutating_actions() -> None:
    calls: list[object] = []
    tool = ToolSpec(
        name="computer_use",
        description="desktop",
        parameters={"type": "object"},
        permission="desktop_control",
        handler=lambda _args, _ctx: {"ok": True},
    )

    async def requester(intent):
        calls.append(intent)
        return True

    async def scenario() -> None:
        result, meta = await _handle_octo_tool_call(
            {
                "function": {
                    "name": "computer_use",
                    "arguments": '{"action":"type","pid":123,"text":"hello"}',
                }
            },
            [tool],
            {"approval_requester": requester},
        )

        assert result == {"ok": True}
        assert meta["had_error"] is False

    asyncio.run(scenario())

    assert len(calls) == 1
    assert calls[0].type == "desktop.control"
    assert calls[0].payload["action"] == "type"
    assert calls[0].requires_approval is True
    assert calls[0].risk == "high"


def test_octo_computer_use_readonly_actions_do_not_require_approval() -> None:
    calls: list[object] = []
    tool = ToolSpec(
        name="computer_use",
        description="desktop",
        parameters={"type": "object"},
        permission="desktop_control",
        handler=lambda _args, _ctx: {"ok": True},
    )

    async def requester(intent):
        calls.append(intent)
        return False

    async def scenario() -> None:
        result, meta = await _handle_octo_tool_call(
            {"function": {"name": "computer_use", "arguments": '{"action":"status"}'}},
            [tool],
            {"approval_requester": requester},
        )

        assert result == {"ok": True}
        assert meta["had_error"] is False

    asyncio.run(scenario())
    assert calls == []


def test_default_octo_tool_policy_blocks_test_run() -> None:
    class DummyOcto:
        mcp_manager = None

    tool_specs, ctx = _get_octo_tools(DummyOcto(), 0)

    assert "test_run" not in {tool.name for tool in tool_specs}

    async def scenario() -> None:
        result, meta = await _handle_octo_tool_call(
            {"function": {"name": "test_run", "arguments": '{"command":"pytest -q"}'}},
            tool_specs,
            ctx,
        )

        assert result["type"] == "policy_block"
        assert result["tool"] == "test_run"
        assert result["reason"] == "blocked_by_deny:octo.direct_exec_denylist"
        assert result[CAPABILITY_OUTCOME_KEY]["kind"] == "policy_denied"
        assert result[CAPABILITY_OUTCOME_KEY]["policy_reason"] == (
            "blocked_by_deny:octo.direct_exec_denylist"
        )
        assert meta["error_type"] == "policy_block"

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_handle_octo_tool_call_captures_tool_exceptions() -> None:
    def _boom(_args, _ctx):
        raise RuntimeError("kaboom")

    result, meta = await _handle_octo_tool_call(
        {"function": {"name": "web_search", "arguments": "{}"}},
        [_tool("web_search", handler=_boom)],
        {},
    )

    assert "kaboom" in result["error"]
    assert meta["had_error"] is True


@pytest.mark.asyncio
async def test_handle_octo_tool_call_emits_ws_tool_start_event() -> None:
    class DummyOcto:
        is_ws_active = True

        def __init__(self) -> None:
            self.progress: list[dict[str, object]] = []

        async def emit_ws_progress(
            self,
            chat_id: int,
            state: str,
            text: str,
            meta: dict,
        ) -> None:
            self.progress.append({"chat_id": chat_id, "state": state, "text": text, "meta": meta})

    octo = DummyOcto()
    result, meta = await _handle_octo_tool_call(
        {"function": {"name": "fs_read", "arguments": '{"path":"README.md"}'}},
        [_tool("fs_read", handler=lambda _args, _ctx: {"ok": True})],
        {"octo": octo, "chat_id": 42},
    )

    assert result == {"ok": True}
    assert meta["had_error"] is False
    assert octo.progress == [
        {
            "chat_id": 42,
            "state": "tool_start",
            "text": "Octo using fs_read",
            "meta": {
                "tool_name": "fs_read",
                "args_preview": '{"path": "README.md"}',
            },
        }
    ]

    octo.progress.clear()
    result, meta = await _handle_octo_tool_call(
        {"function": {"name": "start_worker", "arguments": '{"worker_id":"coder"}'}},
        [_tool("start_worker", handler=lambda _args, _ctx: {"status": "started"})],
        {"octo": octo, "chat_id": 42},
    )

    assert result == {"status": "started"}
    assert meta["had_error"] is False
    assert octo.progress[0]["text"] == "Octo starting coder worker"


def test_record_octo_tool_call_returns_warning_for_repeated_no_progress() -> None:
    history: list[dict[str, str]] = []
    call = {"function": {"name": "web_search", "arguments": '{"query":"same"}'}}
    thresholds = {"warning": 3, "critical": 5, "global_breaker": 10}

    state = None
    for _ in range(3):
        state = _record_octo_tool_call(
            history,
            call=call,
            tool_result={"items": []},
            tool_meta={"had_error": False, "timed_out": False},
            thresholds=thresholds,
        )

    assert state is not None
    assert state["level"] == "warning"


def test_tool_result_payload_error_detection_ignores_successful_error_words() -> None:
    result = json.dumps(
        {
            "status": "ok",
            "reply_text": "The peer mentioned a failed publish but A2A worked.",
            "response": {"errors": []},
        }
    )

    assert _tool_result_payload_error_type(result) is None


def test_tool_result_payload_error_detection_uses_structured_status() -> None:
    result = json.dumps(
        {
            "status": "error",
            "ok": False,
            "error_type": "validation",
            "message": "peer_id is required.",
        }
    )

    assert _tool_result_payload_error_type(result) == "validation"


def test_record_octo_tool_call_returns_critical_for_global_breaker() -> None:
    history: list[dict[str, str]] = []
    thresholds = {"warning": 3, "critical": 5, "global_breaker": 4}

    state = None
    for idx in range(4):
        state = _record_octo_tool_call(
            history,
            call={"function": {"name": f"tool_{idx}", "arguments": "{}"}},
            tool_result={"ok": idx},
            tool_meta={"had_error": False, "timed_out": False},
            thresholds=thresholds,
        )

    assert state is not None
    assert state["level"] == "critical"
    assert state["detector"] == "global_circuit_breaker"


@pytest.mark.asyncio
async def test_handle_octo_tool_call_returns_policy_block_for_known_blocked_tool() -> None:
    blocked_tool = _tool("exec_run", handler=lambda _args, _ctx: "ok")
    blocked_tool = ToolSpec(
        name=blocked_tool.name,
        description=blocked_tool.description,
        parameters=blocked_tool.parameters,
        permission="exec",
        handler=blocked_tool.handler,
        is_async=blocked_tool.is_async,
        metadata=blocked_tool.metadata,
    )
    report = resolve_tool_diagnostics(
        [blocked_tool],
        permissions={"exec": False},
    )

    result, meta = await _handle_octo_tool_call(
        {"function": {"name": "exec_run", "arguments": "{}"}},
        [],
        {"tool_resolution_report": report},
    )

    assert result["type"] == "policy_block"
    assert result["tool"] == "exec_run"
    assert result["reason"] == "blocked_by_permission:exec"
    assert result[CAPABILITY_OUTCOME_KEY]["kind"] == "policy_denied"
    assert meta["error_type"] == "policy_block"


@pytest.mark.asyncio
async def test_handle_octo_tool_call_returns_capability_outcome_for_inactive_known_tool() -> None:
    class DummyOcto:
        mcp_manager = None

    tools, ctx = _get_scheduled_octo_control_tools(DummyOcto(), 123)

    result, meta = await _handle_octo_tool_call(
        {"function": {"name": "start_worker", "arguments": "{}"}},
        tools,
        ctx,
    )

    assert result["type"] == "tool_unavailable"
    assert result["tool"] == "start_worker"
    assert result[CAPABILITY_OUTCOME_KEY]["kind"] == "needs_continuation"
    assert result[CAPABILITY_OUTCOME_KEY]["missing_tool"] == "start_worker"
    assert "octo_continue_from_control_route" in result[CAPABILITY_OUTCOME_KEY]["next_action"]
    assert meta["error_type"] == "tool_unavailable"


def test_build_octo_tool_policy_summary_counts_risk_classes() -> None:
    safe_tool = _tool("web_search", handler=lambda _args, _ctx: "ok")
    dangerous_tool = ToolSpec(
        name="exec_run",
        description="exec tool",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
        permission="exec",
        handler=lambda _args, _ctx: "ok",
        metadata=ToolMetadata(category="ops", risk="dangerous"),
    )
    report = resolve_tool_diagnostics(
        [safe_tool, dangerous_tool],
        permissions={"network": True, "exec": False},
    )

    summary = _build_octo_tool_policy_summary([safe_tool], report)

    assert "Tool policy contract:" in summary
    assert "active_safe=1" in summary
    assert "blocked_dangerous=1" in summary


@pytest.mark.asyncio
async def test_tool_catalog_search_can_find_available_tool_outside_active_budget() -> None:
    class _Manager:
        def get_all_tools(self):
            return [
                ToolSpec(
                    name=f"mcp_demo_tool_{index}",
                    description="demo",
                    parameters={"type": "object"},
                    permission="mcp_exec",
                    handler=lambda _args, _ctx: "ok",
                    is_async=True,
                )
                for index in range(40)
            ]

    all_tools = get_tools(mcp_manager=_Manager())
    report = resolve_tool_diagnostics(
        all_tools,
        permissions={
            "filesystem_read": True,
            "filesystem_write": True,
            "worker_manage": True,
            "llm_subtask": True,
            "canon_manage": True,
            "network": True,
            "exec": True,
            "service_read": True,
            "service_control": True,
            "deploy_control": True,
            "db_admin": True,
            "security_audit": True,
            "self_control": True,
            "mcp_exec": True,
            "skill_use": True,
            "skill_exec": True,
            "skill_manage": True,
        },
    )
    active_tools = _budget_tool_specs(list(report.available_tools), max_count=64)
    active_names = {spec.name for spec in active_tools}
    assert "mcp_demo_tool_39" not in active_names

    catalog_tool = next(spec for spec in active_tools if spec.name == "tool_catalog_search")
    result = catalog_tool.handler(
        {"query": "mcp_demo_tool_39", "limit": 5},
        {
            "tool_resolution_report": report,
            "all_tool_specs": all_tools,
            "active_tool_specs": active_tools,
        },
    )

    payload = json.loads(result)
    names = {item["name"] for item in payload["results"]}

    assert payload["status"] == "ok"
    assert "mcp_demo_tool_39" in names
    match = next(item for item in payload["results"] if item["name"] == "mcp_demo_tool_39")
    assert match["active_now"] is False
