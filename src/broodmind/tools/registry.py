from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    parameters: dict[str, Any]
    permission: str
    handler: Any
    is_async: bool = False
    scope: str | None = field(default=None, compare=False)  # Deprecated, kept for compatibility

    def to_openai_tool(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


def filter_tools(
    tools: Iterable[ToolSpec],
    *,
    permissions: dict[str, bool],
) -> list[ToolSpec]:
    """Filter tools by permissions only. Scope filtering has been removed."""
    available: list[ToolSpec] = []
    for tool in tools:
        if not permissions.get(tool.permission, False):
            continue
        available.append(tool)
    return available
