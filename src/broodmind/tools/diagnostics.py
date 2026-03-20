from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from broodmind.tools.profiles import get_tool_profile
from broodmind.tools.registry import ToolPolicy, ToolPolicyPipelineStep, ToolSpec, parse_tool_list


@dataclass(frozen=True)
class ToolResolutionEntry:
    tool: ToolSpec
    available: bool
    reasons: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        return self.tool.name


@dataclass(frozen=True)
class ToolResolutionReport:
    available_tools: tuple[ToolSpec, ...]
    blocked_tools: tuple[ToolResolutionEntry, ...]
    entries: tuple[ToolResolutionEntry, ...]


def resolve_tool_diagnostics(
    tools: Iterable[ToolSpec],
    *,
    permissions: dict[str, bool],
    profile_name: str | None = None,
    policy_pipeline_steps: Iterable[ToolPolicyPipelineStep] | None = None,
) -> ToolResolutionReport:
    tool_list = list(tools)
    profile = get_tool_profile(profile_name) if profile_name else None
    entries: list[ToolResolutionEntry] = []

    for tool in tool_list:
        reasons: list[str] = []
        if not permissions.get(tool.permission, False):
            reasons.append(f"blocked_by_permission:{tool.permission}")

        if not reasons and profile is not None:
            reason = _policy_block_reason(tool, profile.policy, label=f"profile.{profile.name}")
            if reason:
                reasons.append(reason)

        if not reasons and policy_pipeline_steps:
            for step in policy_pipeline_steps:
                reason = _policy_block_reason(tool, step.policy, label=step.label)
                if reason:
                    reasons.append(reason)
                    break

        entries.append(ToolResolutionEntry(tool=tool, available=not reasons, reasons=tuple(reasons)))

    available_tools = tuple(entry.tool for entry in entries if entry.available)
    blocked_tools = tuple(entry for entry in entries if not entry.available)
    return ToolResolutionReport(
        available_tools=available_tools,
        blocked_tools=blocked_tools,
        entries=tuple(entries),
    )


def _policy_block_reason(tool: ToolSpec, policy: ToolPolicy | None, *, label: str) -> str | None:
    if policy is None:
        return None

    normalized_name = _normalize_tool_name(tool.name)
    allow = parse_tool_list(policy.allow)
    deny = parse_tool_list(policy.deny)

    if allow and "*" not in allow and normalized_name not in set(allow):
        return f"blocked_by_allowlist:{label}"

    deny_set = set(deny)
    if "*" in deny_set or normalized_name in deny_set:
        return f"blocked_by_deny:{label}"

    return None


def _normalize_tool_name(name: str) -> str:
    return str(name).strip().lower()
