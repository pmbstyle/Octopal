from __future__ import annotations

from octopal.runtime.workers.agent_worker import _tool_schema_chars
from octopal.tools.catalog import get_tools
from octopal.tools.inventory import annotate_tool_specs, resolve_tool_metadata
from octopal.tools.registry import ToolSpec, filter_tools


def _tool(name: str, permission: str = "network") -> ToolSpec:
    return ToolSpec(
        name=name,
        description=f"{name} tool",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
        permission=permission,
        handler=lambda _args, _ctx: "ok",
    )


def test_resolve_tool_metadata_knows_core_tools() -> None:
    metadata = resolve_tool_metadata("fs_delete")
    assert metadata.category == "filesystem"
    assert metadata.risk == "dangerous"
    assert "delete_workspace" in metadata.capabilities


def test_resolve_tool_metadata_heuristics_cover_skill_tools() -> None:
    metadata = resolve_tool_metadata("skill_agentmail")
    assert metadata.category == "skills"
    assert metadata.owner == "workspace"
    assert metadata.risk == "guarded"


def test_annotate_tool_specs_applies_core_metadata() -> None:
    annotated = annotate_tool_specs([_tool("web_search"), _tool("mcp_demo_lookup")])
    assert annotated[0].metadata.category == "web"
    assert annotated[1].metadata.category == "mcp"
    assert annotated[1].metadata.owner == "mcp"


def test_filter_tools_can_apply_profile_after_permission_filtering() -> None:
    tools = [
        _tool("fs_read", permission="filesystem_read"),
        _tool("web_search", permission="network"),
        _tool("service_health", permission="service_read"),
    ]
    out = filter_tools(
        tools,
        permissions={"filesystem_read": True, "network": True, "service_read": True},
        profile_name="research",
    )
    assert [tool.name for tool in out] == ["web_search"]


def test_catalog_returns_annotated_tools() -> None:
    tools = {tool.name: tool for tool in get_tools(mcp_manager=None)}
    assert tools["web_search"].metadata.category == "web"
    assert tools["start_worker"].metadata.category == "workers"
    assert tools["worker_yield"].metadata.category == "workers"


def test_web_search_schema_stays_compact_without_contract_loss() -> None:
    tool = next(tool for tool in get_tools(mcp_manager=None) if tool.name == "web_search")

    assert _tool_schema_chars([tool]) < 860
    assert tool.parameters["required"] == ["query"]
    assert tool.parameters["additionalProperties"] is False
    assert tool.parameters["properties"]["provider"]["enum"] == ["auto", "brave", "firecrawl"]


def test_catalog_classifies_browser_scheduler_database_release_and_template_tools() -> None:
    tools = {tool.name: tool for tool in get_tools(mcp_manager=None)}

    assert tools["browser_open"].metadata.category == "browser"
    assert tools["browser_workflow"].metadata.category == "browser"
    assert tools["fetch_plan_tool"].metadata.category == "web"
    assert tools["check_schedule"].metadata.category == "scheduler"
    assert tools["scheduler_status"].metadata.category == "scheduler"
    assert tools["gateway_status"].metadata.category == "ops"
    assert tools["mcp_discover"].metadata.category == "mcp"
    assert tools["db_restore"].metadata.category == "database"
    assert tools["rollback_release"].metadata.category == "release"
    assert tools["update_worker_template"].metadata.category == "templates"


def test_catalog_marks_high_impact_mutating_tools_with_higher_risk() -> None:
    tools = {tool.name: tool for tool in get_tools(mcp_manager=None)}

    assert tools["db_restore"].metadata.risk == "dangerous"
    assert tools["rollback_release"].metadata.risk == "dangerous"
    assert tools["delete_worker_template"].metadata.risk == "dangerous"
    assert tools["docker_compose_control"].metadata.risk == "guarded"


def test_misc_bucket_is_reduced_after_taxonomy_cleanup() -> None:
    tools = get_tools(mcp_manager=None)
    misc_names = [tool.name for tool in tools if tool.metadata.category == "misc"]

    assert len(misc_names) < 10
