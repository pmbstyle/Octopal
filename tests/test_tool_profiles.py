from __future__ import annotations

from broodmind.tools.metadata import ToolMetadata, normalize_tool_tags
from broodmind.tools.profiles import DEFAULT_TOOL_PROFILES, apply_tool_profile, get_tool_profile
from broodmind.tools.registry import ToolSpec


def _tool(name: str, permission: str = "network") -> ToolSpec:
    return ToolSpec(
        name=name,
        description=f"{name} tool",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
        permission=permission,
        handler=lambda _args, _ctx: "ok",
    )


def test_normalize_tool_tags_deduplicates_and_normalizes() -> None:
    assert normalize_tool_tags([" Research ", "ops", "research", "", "OPS"]) == (
        "research",
        "ops",
    )


def test_tool_metadata_normalizes_values() -> None:
    metadata = ToolMetadata(
        category=" Web ",
        profile_tags=("Research", "research", "ops"),
        capabilities=("Fetch", "fetch", "summarize"),
    )
    assert metadata.category == "web"
    assert metadata.profile_tags == ("research", "ops")
    assert metadata.capabilities == ("fetch", "summarize")


def test_get_tool_profile_is_case_insensitive() -> None:
    profile = get_tool_profile("Coding")
    assert profile is not None
    assert profile.name == "coding"


def test_apply_tool_profile_filters_tool_list() -> None:
    tools = [_tool("fs_read"), _tool("web_search"), _tool("service_health")]
    out = apply_tool_profile(tools, "coding")
    assert [tool.name for tool in out] == ["fs_read"]


def test_default_profiles_include_expected_foundation_profiles() -> None:
    assert {"minimal", "research", "coding", "ops", "communication"} <= set(
        DEFAULT_TOOL_PROFILES
    )


def test_research_profile_exposes_fetch_plan_tool() -> None:
    tools = [_tool("fetch_plan_tool"), _tool("web_search"), _tool("service_health", permission="service_read")]
    out = apply_tool_profile(tools, "research")

    assert [tool.name for tool in out] == ["fetch_plan_tool", "web_search"]


def test_ops_profile_exposes_new_read_only_diagnostics() -> None:
    tools = [
        _tool("gateway_status", permission="service_read"),
        _tool("scheduler_status", permission="self_control"),
        _tool("worker_session_status", permission="worker_manage"),
        _tool("worker_yield", permission="worker_manage"),
        _tool("mcp_discover", permission="self_control"),
        _tool("web_search"),
    ]
    out = apply_tool_profile(tools, "ops")

    assert [tool.name for tool in out] == [
        "gateway_status",
        "scheduler_status",
        "worker_session_status",
        "worker_yield",
        "mcp_discover",
    ]
