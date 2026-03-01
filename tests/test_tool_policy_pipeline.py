from __future__ import annotations

from broodmind.tools.registry import (
    ToolPolicy,
    ToolPolicyPipelineStep,
    ToolSpec,
    apply_tool_policy,
    apply_tool_policy_pipeline,
    filter_tools,
    parse_tool_list,
)


def _tool(name: str, permission: str = "network") -> ToolSpec:
    return ToolSpec(
        name=name,
        description=f"{name} tool",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
        permission=permission,
        handler=lambda _args, _ctx: "ok",
    )


def _names(tools: list[ToolSpec]) -> list[str]:
    return [tool.name for tool in tools]


def test_parse_tool_list_from_csv_normalizes_and_deduplicates() -> None:
    parsed = parse_tool_list(" read,Write, read , ,EXEC ")
    assert parsed == ["read", "write", "exec"]


def test_apply_tool_policy_allow_only() -> None:
    tools = [_tool("read"), _tool("write"), _tool("exec")]
    out = apply_tool_policy(tools, ToolPolicy(allow=["read", "exec"]))
    assert _names(out) == ["read", "exec"]


def test_apply_tool_policy_deny_only() -> None:
    tools = [_tool("read"), _tool("write"), _tool("exec")]
    out = apply_tool_policy(tools, ToolPolicy(deny=["write"]))
    assert _names(out) == ["read", "exec"]


def test_apply_tool_policy_wildcards() -> None:
    tools = [_tool("read"), _tool("write"), _tool("exec")]
    out_allow_all = apply_tool_policy(tools, ToolPolicy(allow=["*"]))
    assert _names(out_allow_all) == ["read", "write", "exec"]

    out_deny_all = apply_tool_policy(tools, ToolPolicy(deny=["*"]))
    assert out_deny_all == []


def test_apply_tool_policy_pipeline_applies_in_order() -> None:
    tools = [_tool("read"), _tool("write"), _tool("exec")]
    steps = [
        ToolPolicyPipelineStep(label="allow base", policy=ToolPolicy(allow=["read", "write"])),
        ToolPolicyPipelineStep(label="deny write", policy=ToolPolicy(deny=["write"])),
    ]
    out = apply_tool_policy_pipeline(tools, steps)
    assert _names(out) == ["read"]


def test_filter_tools_permissions_then_pipeline() -> None:
    tools = [
        _tool("read", permission="filesystem_read"),
        _tool("write", permission="filesystem_write"),
        _tool("web_search", permission="network"),
    ]
    perms = {"filesystem_read": True, "filesystem_write": False, "network": True}
    steps = [ToolPolicyPipelineStep(label="deny web", policy=ToolPolicy(deny=["web_search"]))]

    out = filter_tools(tools, permissions=perms, policy_pipeline_steps=steps)
    assert _names(out) == ["read"]
