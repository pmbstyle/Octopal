from __future__ import annotations

import json

import pytest

from octopal.runtime.octo.router import (
    _build_octo_tool_policy_summary,
    _handle_octo_tool_call,
    _budget_tool_specs,
    _record_octo_tool_call,
)
from octopal.tools.diagnostics import resolve_tool_diagnostics
from octopal.tools.catalog import get_tools
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
    assert meta["error_type"] == "policy_block"


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
