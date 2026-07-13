from octopal.tools.diagnostics import (
    ToolResolutionEntry,
    ToolResolutionReport,
    resolve_tool_diagnostics,
)
from octopal.tools.metadata import ProgrammaticReadContract, ToolMetadata
from octopal.tools.profiles import (
    DEFAULT_TOOL_PROFILES,
    ToolProfile,
    apply_tool_profile,
    get_tool_profile,
)
from octopal.tools.programmatic import (
    ProgrammaticReadDecision,
    filter_programmatic_read_tools,
    resolve_programmatic_read_tool,
)
from octopal.tools.registry import ToolSpec, filter_tools

__all__ = [
    "ToolMetadata",
    "ProgrammaticReadContract",
    "ProgrammaticReadDecision",
    "ToolResolutionEntry",
    "ToolResolutionReport",
    "ToolProfile",
    "DEFAULT_TOOL_PROFILES",
    "apply_tool_profile",
    "get_tool_profile",
    "resolve_tool_diagnostics",
    "filter_programmatic_read_tools",
    "resolve_programmatic_read_tool",
    "ToolSpec",
    "filter_tools",
    "get_tools",
]


def __getattr__(name: str):
    if name == "get_tools":
        from octopal.tools.catalog import get_tools

        return get_tools
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
