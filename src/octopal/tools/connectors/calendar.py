from __future__ import annotations

import json
from typing import Any

from octopal.tools.metadata import ToolMetadata
from octopal.tools.registry import ToolSpec

_CALENDAR_SERVER_ID = "google-calendar"


def _extract_mcp_payload(result: Any) -> Any:
    content_items = getattr(result, "content", None)
    if not content_items:
        return result

    if len(content_items) == 1:
        item = content_items[0]
        text = getattr(item, "text", None)
        if isinstance(text, str):
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text
        if hasattr(item, "model_dump"):
            return item.model_dump()
        return str(item)

    normalized: list[Any] = []
    for item in content_items:
        text = getattr(item, "text", None)
        if isinstance(text, str):
            try:
                normalized.append(json.loads(text))
            except json.JSONDecodeError:
                normalized.append(text)
            continue
        if hasattr(item, "model_dump"):
            normalized.append(item.model_dump())
            continue
        normalized.append(str(item))
    return normalized


def _resolve_mcp_manager(ctx: dict[str, Any], fallback: Any) -> Any:
    octo = (ctx or {}).get("octo")
    if octo is not None and getattr(octo, "mcp_manager", None) is not None:
        return octo.mcp_manager
    return fallback


async def _calendar_mcp_proxy(
    remote_tool_name: str,
    args: dict[str, Any],
    ctx: dict[str, Any],
    *,
    fallback_manager: Any,
) -> Any:
    manager = _resolve_mcp_manager(ctx, fallback_manager)
    if manager is None:
        return {
            "ok": False,
            "error": "Calendar tools are unavailable because no MCP manager is active.",
            "hint": "Restart Octopal after authorizing the Google Calendar connector.",
        }

    try:
        result = await manager.call_tool(
            _CALENDAR_SERVER_ID,
            remote_tool_name,
            args or {},
            allow_name_fallback=True,
        )
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "server_id": _CALENDAR_SERVER_ID,
            "tool": remote_tool_name,
            "hint": "Check connector status and confirm the Google Calendar MCP server is connected.",
        }

    return _extract_mcp_payload(result)


def _calendar_tool(
    *,
    name: str,
    remote_tool_name: str,
    description: str,
    parameters: dict[str, Any],
    fallback_manager: Any,
    capabilities: tuple[str, ...],
) -> ToolSpec:
    return ToolSpec(
        name=name,
        description=description,
        parameters=parameters,
        permission="mcp_exec",
        handler=lambda args, ctx, _remote=remote_tool_name, _manager=fallback_manager: _calendar_mcp_proxy(
            _remote,
            args,
            ctx,
            fallback_manager=_manager,
        ),
        is_async=True,
        server_id=_CALENDAR_SERVER_ID,
        remote_tool_name=remote_tool_name,
        metadata=ToolMetadata(
            category="connectors",
            risk="safe",
            profile_tags=("planning", "communication"),
            capabilities=capabilities,
        ),
    )


def get_calendar_connector_tools(mcp_manager: Any = None) -> list[ToolSpec]:
    if mcp_manager is None:
        return []

    return [
        _calendar_tool(
            name="calendar_list_calendars",
            remote_tool_name="list_calendars",
            description="List calendars available in the connected Google account.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            fallback_manager=mcp_manager,
            capabilities=("calendar_read", "connector_use"),
        ),
        _calendar_tool(
            name="calendar_list_events",
            remote_tool_name="list_events",
            description="List events for a Google Calendar, optionally filtered by time window or query.",
            parameters={
                "type": "object",
                "properties": {
                    "calendar_id": {"type": "string"},
                    "max_results": {"type": "integer", "minimum": 1, "maximum": 25},
                    "query": {"type": "string"},
                    "time_min": {"type": "string"},
                    "time_max": {"type": "string"},
                    "single_events": {"type": "boolean"},
                    "order_by": {"type": "string"},
                },
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("calendar_read", "connector_use"),
        ),
        _calendar_tool(
            name="calendar_search_events",
            remote_tool_name="search_events",
            description="Search Google Calendar events by free-text query.",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "calendar_id": {"type": "string"},
                    "max_results": {"type": "integer", "minimum": 1, "maximum": 25},
                    "time_min": {"type": "string"},
                    "time_max": {"type": "string"},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("calendar_read", "connector_use"),
        ),
        _calendar_tool(
            name="calendar_get_event",
            remote_tool_name="get_event",
            description="Read a Google Calendar event by event ID.",
            parameters={
                "type": "object",
                "properties": {
                    "calendar_id": {"type": "string"},
                    "event_id": {"type": "string"},
                },
                "required": ["event_id"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("calendar_read", "connector_use"),
        ),
        _calendar_tool(
            name="calendar_create_event",
            remote_tool_name="create_event",
            description="Create a Google Calendar event with summary, start, and end time.",
            parameters={
                "type": "object",
                "properties": {
                    "calendar_id": {"type": "string"},
                    "summary": {"type": "string"},
                    "start": {"type": "object"},
                    "end": {"type": "object"},
                    "description": {"type": "string"},
                    "location": {"type": "string"},
                    "attendees": {"type": "array", "items": {"type": "object"}},
                    "time_zone": {"type": "string"},
                },
                "required": ["summary", "start", "end"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("calendar_write", "connector_use"),
        ),
        _calendar_tool(
            name="calendar_update_event",
            remote_tool_name="update_event",
            description="Update one or more mutable fields on a Google Calendar event.",
            parameters={
                "type": "object",
                "properties": {
                    "calendar_id": {"type": "string"},
                    "event_id": {"type": "string"},
                    "summary": {"type": "string"},
                    "description": {"type": "string"},
                    "location": {"type": "string"},
                    "start": {"type": "object"},
                    "end": {"type": "object"},
                    "attendees": {"type": "array", "items": {"type": "object"}},
                    "time_zone": {"type": "string"},
                },
                "required": ["event_id"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("calendar_write", "connector_use"),
        ),
        _calendar_tool(
            name="calendar_delete_event",
            remote_tool_name="delete_event",
            description="Delete a Google Calendar event by event ID.",
            parameters={
                "type": "object",
                "properties": {
                    "calendar_id": {"type": "string"},
                    "event_id": {"type": "string"},
                },
                "required": ["event_id"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("calendar_write", "connector_use"),
        ),
        _calendar_tool(
            name="calendar_freebusy",
            remote_tool_name="freebusy",
            description="Return busy windows for one or more Google Calendars in a time range.",
            parameters={
                "type": "object",
                "properties": {
                    "calendar_ids": {"type": "array", "items": {"type": "string"}},
                    "time_min": {"type": "string"},
                    "time_max": {"type": "string"},
                    "time_zone": {"type": "string"},
                },
                "required": ["calendar_ids", "time_min", "time_max"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("calendar_read", "connector_use"),
        ),
    ]
