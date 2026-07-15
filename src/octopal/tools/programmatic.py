from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from octopal.tools.metadata import ProgrammaticReadContract, ProgrammaticResultShape
from octopal.tools.registry import ToolSpec

_FORBIDDEN_CATEGORIES = frozenset({"browser", "communication", "desktop", "mcp"})
_ALLOWED_PERMISSIONS = frozenset({"filesystem_read", "network", "security_audit", "service_read"})


def _reject_non_finite_json(_value: str) -> None:
    raise ValueError("non-finite JSON number")


@dataclass(frozen=True)
class ProgrammaticReadDecision:
    """Explain whether a tool may enter the programmatic read-only path."""

    tool_name: str
    allowed: bool
    reasons: tuple[str, ...]
    contract: ProgrammaticReadContract | None


ProgrammaticReadValue = dict[str, Any] | list[Any] | str


@dataclass(frozen=True)
class ValidatedProgrammaticReadResult:
    """A programmatic result that passed its declared shape and byte limits."""

    tool_name: str
    result_shape: ProgrammaticResultShape
    byte_count: int
    value: ProgrammaticReadValue


class ProgrammaticReadResultError(ValueError):
    """Reject an unsafe result without copying raw tool output into the error."""

    def __init__(self, tool_name: str, code: str, *, details: tuple[str, ...] = ()) -> None:
        self.tool_name = tool_name
        self.code = code
        self.details = details
        super().__init__(f"{tool_name}: {code}")


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


def validate_programmatic_read_result(
    tool: ToolSpec, result: Any
) -> ValidatedProgrammaticReadResult:
    """Validate a raw tool result before exposing it to programmatic callers."""
    decision = resolve_programmatic_read_tool(tool)
    if not decision.allowed or decision.contract is None:
        raise ProgrammaticReadResultError(
            tool.name,
            "tool_not_eligible",
            details=decision.reasons,
        )
    contract = decision.contract

    if not isinstance(result, str):
        raise ProgrammaticReadResultError(tool.name, "result_not_text")
    try:
        byte_count = len(result.encode("utf-8"))
    except UnicodeEncodeError:
        byte_count = None
    if byte_count is None:
        raise ProgrammaticReadResultError(tool.name, "result_not_utf8")
    if byte_count > contract.max_result_bytes:
        raise ProgrammaticReadResultError(
            tool.name,
            "result_too_large",
            details=(
                f"actual_bytes={byte_count}",
                f"max_result_bytes={contract.max_result_bytes}",
            ),
        )

    if contract.result_shape == "text":
        value: ProgrammaticReadValue = result
    else:
        parse_failed = False
        try:
            value = json.loads(result, parse_constant=_reject_non_finite_json)
        except (ValueError, RecursionError):
            parse_failed = True
            value = ""
        if parse_failed:
            raise ProgrammaticReadResultError(tool.name, "result_invalid_json")
        expected_type = dict if contract.result_shape == "json_object" else list
        if not isinstance(value, expected_type):
            raise ProgrammaticReadResultError(tool.name, "result_shape_mismatch")

    return ValidatedProgrammaticReadResult(
        tool_name=tool.name,
        result_shape=contract.result_shape,
        byte_count=byte_count,
        value=value,
    )
