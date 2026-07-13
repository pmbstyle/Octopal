from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Literal

ToolRiskLevel = Literal["safe", "guarded", "dangerous"]
ToolOwner = Literal["core", "workspace", "plugin", "mcp"]
ProgrammaticResultShape = Literal["json_object", "json_array", "text"]

PROGRAMMATIC_READ_MAX_PARALLEL_CALLS = 8
PROGRAMMATIC_READ_MAX_RESULT_BYTES = 1_000_000
_PROGRAMMATIC_RESULT_SHAPES = frozenset({"json_object", "json_array", "text"})


def normalize_tool_tags(values: Iterable[str] | None) -> tuple[str, ...]:
    """Normalize string tags while preserving order."""
    if values is None:
        return ()
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = str(raw).strip().lower()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return tuple(out)


@dataclass(frozen=True)
class ProgrammaticReadContract:
    """Bounded result contract for side-effect-free programmatic tool calls.

    Idempotence means repeated calls add no side effects; it does not promise
    deterministic results from changing external data sources.
    """

    idempotent: bool
    max_parallel_calls: int
    result_shape: ProgrammaticResultShape
    max_result_bytes: int

    def __post_init__(self) -> None:
        if self.result_shape not in _PROGRAMMATIC_RESULT_SHAPES:
            raise ValueError(f"unsupported result_shape: {self.result_shape!r}")
        if not 1 <= self.max_parallel_calls <= PROGRAMMATIC_READ_MAX_PARALLEL_CALLS:
            raise ValueError(
                "max_parallel_calls must be between 1 and "
                f"{PROGRAMMATIC_READ_MAX_PARALLEL_CALLS}"
            )
        if not 1 <= self.max_result_bytes <= PROGRAMMATIC_READ_MAX_RESULT_BYTES:
            raise ValueError(
                "max_result_bytes must be between 1 and " f"{PROGRAMMATIC_READ_MAX_RESULT_BYTES}"
            )


@dataclass(frozen=True)
class ToolMetadata:
    """
    Declarative metadata for tools.

    This is intentionally separate from permission gating so we can evolve
    profiles, diagnostics, and policy explanations without changing handlers.
    """

    category: str = "misc"
    risk: ToolRiskLevel = "safe"
    owner: ToolOwner = "core"
    read_only: bool = False
    programmatic_read: ProgrammaticReadContract | None = None
    profile_tags: tuple[str, ...] = field(default_factory=tuple)
    capabilities: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "category", str(self.category).strip().lower() or "misc")
        object.__setattr__(self, "profile_tags", normalize_tool_tags(self.profile_tags))
        object.__setattr__(self, "capabilities", normalize_tool_tags(self.capabilities))
