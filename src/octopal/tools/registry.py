from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from octopal.tools.metadata import ToolMetadata

_MAX_TOOL_USAGE_EXAMPLES = 2
_MAX_TOOL_USAGE_EXAMPLE_CHARS = 600
_MAX_TOOL_USAGE_EXAMPLE_EVIDENCE_CHARS = 200


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
    usage_examples: tuple[dict[str, Any], ...] = field(default_factory=tuple, compare=False)
    usage_example_evidence: str | None = field(default=None, compare=False)
    _rendered_usage_examples: str = field(default="", init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        examples = tuple(self.usage_examples or ())
        evidence = str(self.usage_example_evidence or "").strip()
        if len(examples) > _MAX_TOOL_USAGE_EXAMPLES:
            raise ValueError(f"usage_examples supports at most {_MAX_TOOL_USAGE_EXAMPLES} examples")
        if examples and not evidence:
            raise ValueError("usage examples require a measured evidence reference")
        if not examples and evidence:
            raise ValueError("usage example evidence requires at least one example")
        if len(evidence) > _MAX_TOOL_USAGE_EXAMPLE_EVIDENCE_CHARS:
            raise ValueError(
                "usage example evidence exceeds "
                f"{_MAX_TOOL_USAGE_EXAMPLE_EVIDENCE_CHARS} characters"
            )
        if any(ord(character) < 32 or ord(character) == 127 for character in evidence):
            raise ValueError("usage example evidence must be a single printable line")
        for example in examples:
            if not isinstance(example, dict) or not example:
                raise ValueError("each usage example must be a non-empty argument object")
            if any(not isinstance(key, str) or not key.strip() for key in example):
                raise ValueError("usage example keys must be non-empty strings")
        rendered = _render_usage_examples(examples)
        if len(rendered) > _MAX_TOOL_USAGE_EXAMPLE_CHARS:
            raise ValueError(
                "rendered usage examples exceed " f"{_MAX_TOOL_USAGE_EXAMPLE_CHARS} characters"
            )
        object.__setattr__(self, "usage_examples", examples)
        object.__setattr__(self, "usage_example_evidence", evidence or None)
        object.__setattr__(self, "_rendered_usage_examples", rendered)

    def to_openai_tool(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description + self._rendered_usage_examples,
                "parameters": self.parameters,
            },
        }

    def usage_example_prompt_chars(self) -> int:
        return len(self._rendered_usage_examples)


def _render_usage_examples(examples: tuple[dict[str, Any], ...]) -> str:
    if not examples:
        return ""
    try:
        rendered = [
            json.dumps(
                example,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            for example in examples
        ]
    except (TypeError, ValueError) as exc:
        raise ValueError("usage examples must be finite JSON objects") from exc
    if len(rendered) == 1:
        return f"\nExample arguments: {rendered[0]}"
    return "\nExample arguments:\n" + "\n".join(f"- {item}" for item in rendered)


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
    raw_items = value.split(",") if isinstance(value, str) else [str(item) for item in value]
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
    from octopal.tools.diagnostics import resolve_tool_diagnostics

    report = resolve_tool_diagnostics(
        tools,
        permissions=permissions,
        profile_name=profile_name,
        policy_pipeline_steps=policy_pipeline_steps,
    )
    return list(report.available_tools)


def _normalize_tool_name(name: str) -> str:
    return str(name).strip().lower()
