from __future__ import annotations

from broodmind.tools.diagnostics import resolve_tool_diagnostics
from broodmind.tools.registry import ToolPolicy, ToolPolicyPipelineStep, ToolSpec


def _tool(name: str, permission: str = "network") -> ToolSpec:
    return ToolSpec(
        name=name,
        description=f"{name} tool",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
        permission=permission,
        handler=lambda _args, _ctx: "ok",
    )


def test_resolve_tool_diagnostics_reports_permission_blocks() -> None:
    report = resolve_tool_diagnostics(
        [_tool("web_search", permission="network")],
        permissions={"network": False},
    )

    assert report.available_tools == ()
    assert report.blocked_tools[0].name == "web_search"
    assert report.blocked_tools[0].reasons == ("blocked_by_permission:network",)


def test_resolve_tool_diagnostics_reports_profile_allowlist_blocks() -> None:
    report = resolve_tool_diagnostics(
        [_tool("service_health", permission="service_read")],
        permissions={"service_read": True},
        profile_name="research",
    )

    assert report.available_tools == ()
    assert report.blocked_tools[0].reasons == ("blocked_by_allowlist:profile.research",)


def test_resolve_tool_diagnostics_reports_pipeline_deny_blocks() -> None:
    report = resolve_tool_diagnostics(
        [_tool("web_search")],
        permissions={"network": True},
        policy_pipeline_steps=[
            ToolPolicyPipelineStep(label="queen.raw_fetch_denylist", policy=ToolPolicy(deny=["web_search"]))
        ],
    )

    assert report.available_tools == ()
    assert report.blocked_tools[0].reasons == ("blocked_by_deny:queen.raw_fetch_denylist",)


def test_resolve_tool_diagnostics_returns_available_tools_in_order() -> None:
    report = resolve_tool_diagnostics(
        [
            _tool("web_search"),
            _tool("fs_read", permission="filesystem_read"),
            _tool("service_health", permission="service_read"),
        ],
        permissions={"network": True, "filesystem_read": True, "service_read": True},
        profile_name="coding",
    )

    assert [tool.name for tool in report.available_tools] == ["fs_read"]
