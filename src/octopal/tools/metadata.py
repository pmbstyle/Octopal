from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Literal

ToolRiskLevel = Literal["safe", "guarded", "dangerous"]
ToolOwner = Literal["core", "workspace", "plugin", "mcp"]


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
    profile_tags: tuple[str, ...] = field(default_factory=tuple)
    capabilities: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "category", str(self.category).strip().lower() or "misc")
        object.__setattr__(self, "profile_tags", normalize_tool_tags(self.profile_tags))
        object.__setattr__(self, "capabilities", normalize_tool_tags(self.capabilities))
