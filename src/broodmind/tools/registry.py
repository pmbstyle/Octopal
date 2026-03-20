from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from broodmind.tools.metadata import ToolMetadata


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    parameters: dict[str, Any]
    permission: str
    handler: Any
    is_async: bool = False
    scope: str | None = field(default=None, compare=False)  # Deprecated, kept for compatibility
    server_id: str | None = field(default=None, compare=False)
    remote_tool_name: str | None = field(default=None, compare=False)
    metadata: ToolMetadata = field(default_factory=ToolMetadata, compare=False)

    def to_openai_tool(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


@dataclass(frozen=True)
class ToolPolicy:
    """Name-based tool policy."""

    allow: list[str] | None = None
    deny: list[str] | None = None


@dataclass(frozen=True)
class ToolPolicyPipelineStep:
    """Single tool-policy pipeline step."""

    label: str
    policy: ToolPolicy | None


def parse_tool_list(value: str | Iterable[str] | None) -> list[str]:
    """
    Parse a tool list from CSV text or an iterable.

    Returns a normalized list (trimmed, lower-cased, de-duplicated), preserving order.
    """
    if value is None:
        return []
    if isinstance(value, str):
        raw_items = value.split(",")
    else:
        raw_items = [str(item) for item in value]
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_items:
        name = _normalize_tool_name(raw)
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def apply_tool_policy(tools: Iterable[ToolSpec], policy: ToolPolicy | None) -> list[ToolSpec]:
    """Apply a single allow/deny policy to tools."""
    tool_list = list(tools)
    if policy is None:
        return tool_list

    allow = parse_tool_list(policy.allow)
    deny = parse_tool_list(policy.deny)
    deny_set = set(deny)

    if allow and "*" not in allow:
        allow_set = set(allow)
        tool_list = [tool for tool in tool_list if _normalize_tool_name(tool.name) in allow_set]

    if deny:
        if "*" in deny_set:
            return []
        tool_list = [tool for tool in tool_list if _normalize_tool_name(tool.name) not in deny_set]

    return tool_list


def apply_tool_policy_pipeline(
    tools: Iterable[ToolSpec],
    steps: Iterable[ToolPolicyPipelineStep] | None,
) -> list[ToolSpec]:
    """Apply policy steps sequentially."""
    filtered = list(tools)
    if not steps:
        return filtered
    for step in steps:
        filtered = apply_tool_policy(filtered, step.policy)
    return filtered


def filter_tools(
    tools: Iterable[ToolSpec],
    *,
    permissions: dict[str, bool],
    profile_name: str | None = None,
    policy_pipeline_steps: Iterable[ToolPolicyPipelineStep] | None = None,
) -> list[ToolSpec]:
    """Filter tools by permissions, profiles, and policy steps."""
    from broodmind.tools.diagnostics import resolve_tool_diagnostics

    report = resolve_tool_diagnostics(
        tools,
        permissions=permissions,
        profile_name=profile_name,
        policy_pipeline_steps=policy_pipeline_steps,
    )
    return list(report.available_tools)


def _normalize_tool_name(name: str) -> str:
    return str(name).strip().lower()
