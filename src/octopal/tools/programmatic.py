from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from octopal.tools.metadata import ProgrammaticReadContract
from octopal.tools.registry import ToolSpec

_FORBIDDEN_CATEGORIES = frozenset({"browser", "communication", "desktop", "mcp"})
_ALLOWED_PERMISSIONS = frozenset({"filesystem_read", "network", "security_audit", "service_read"})


@dataclass(frozen=True)
class ProgrammaticReadDecision:
    """Explain whether a tool may enter the programmatic read-only path."""

    tool_name: str
    allowed: bool
    reasons: tuple[str, ...]
    contract: ProgrammaticReadContract | None


def resolve_programmatic_read_tool(tool: ToolSpec) -> ProgrammaticReadDecision:
    """Resolve programmatic eligibility from explicit, fail-closed metadata."""
    metadata = tool.metadata
    contract = metadata.programmatic_read
    reasons: list[str] = []

    if contract is None:
        reasons.append("programmatic_read_contract_missing")
    if not metadata.read_only:
        reasons.append("tool_not_declared_read_only")
    if metadata.risk != "safe":
        reasons.append("tool_risk_not_safe")
    if metadata.owner != "core":
        reasons.append("tool_owner_not_core")
    if metadata.category in _FORBIDDEN_CATEGORIES:
        reasons.append("tool_category_forbidden")
    if tool.permission not in _ALLOWED_PERMISSIONS:
        reasons.append("tool_permission_not_programmatic_read")
    if contract is not None and not contract.idempotent:
        reasons.append("tool_not_declared_idempotent")

    return ProgrammaticReadDecision(
        tool_name=tool.name,
        allowed=not reasons,
        reasons=tuple(reasons),
        contract=contract,
    )


def filter_programmatic_read_tools(tools: Iterable[ToolSpec]) -> list[ToolSpec]:
    """Return only tools whose explicit programmatic read contract is accepted."""
    return [tool for tool in tools if resolve_programmatic_read_tool(tool).allowed]
