from __future__ import annotations

import json
from typing import Any

from octopal.tools.metadata import ToolMetadata
from octopal.tools.registry import ToolSpec

_GMAIL_SERVER_ID = "google-gmail"


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


async def _gmail_mcp_proxy(
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
            "error": "Gmail tools are unavailable because no MCP manager is active.",
            "hint": "Restart Octopal after authorizing the Google Gmail connector.",
        }

    try:
        result = await manager.call_tool(
            _GMAIL_SERVER_ID,
            remote_tool_name,
            args or {},
            allow_name_fallback=True,
        )
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "server_id": _GMAIL_SERVER_ID,
            "tool": remote_tool_name,
            "hint": "Check connector status and confirm the Gmail MCP server is connected.",
        }

    return _extract_mcp_payload(result)


def _gmail_tool(
    *,
    name: str,
    remote_tool_name: str,
    description: str,
    parameters: dict[str, Any],
    fallback_manager: Any,
) -> ToolSpec:
    return ToolSpec(
        name=name,
        description=description,
        parameters=parameters,
        permission="mcp_exec",
        handler=lambda args, ctx, _remote=remote_tool_name, _manager=fallback_manager: _gmail_mcp_proxy(
            _remote,
            args,
            ctx,
            fallback_manager=_manager,
        ),
        is_async=True,
        server_id=_GMAIL_SERVER_ID,
        remote_tool_name=remote_tool_name,
        metadata=ToolMetadata(
            category="connectors",
            risk="safe",
            profile_tags=("research", "communication"),
            capabilities=("gmail_read", "connector_use"),
        ),
    )


def get_gmail_connector_tools(mcp_manager: Any = None) -> list[ToolSpec]:
    if mcp_manager is None:
        return []

    return [
        _gmail_tool(
            name="gmail_get_profile",
            remote_tool_name="get_profile",
            description="Get the connected Gmail account profile. Use this to confirm which mailbox is active.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            fallback_manager=mcp_manager,
        ),
        _gmail_tool(
            name="gmail_list_labels",
            remote_tool_name="list_labels",
            description="List Gmail labels available in the connected mailbox.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            fallback_manager=mcp_manager,
        ),
        _gmail_tool(
            name="gmail_list_messages",
            remote_tool_name="list_messages",
            description=(
                "List recent Gmail messages and return message IDs. Use this first when you need the latest email "
                "or when you do not already have a message ID."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "label_ids": {"type": "array", "items": {"type": "string"}},
                    "max_results": {"type": "integer", "minimum": 1, "maximum": 25},
                    "page_token": {"type": "string"},
                    "include_spam_trash": {"type": "boolean"},
                },
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
        ),
        _gmail_tool(
            name="gmail_search_messages",
            remote_tool_name="search_messages",
            description=(
                "Search Gmail with standard Gmail query syntax and return matching message IDs. "
                "Use this when the user asks for emails by sender, subject, label, or date."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "max_results": {"type": "integer", "minimum": 1, "maximum": 25},
                    "page_token": {"type": "string"},
                    "include_spam_trash": {"type": "boolean"},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
        ),
        _gmail_tool(
            name="gmail_get_message",
            remote_tool_name="get_message",
            description="Read a Gmail message by ID after discovering that ID via gmail_list_messages or gmail_search_messages.",
            parameters={
                "type": "object",
                "properties": {
                    "message_id": {"type": "string"},
                    "format": {"type": "string"},
                },
                "required": ["message_id"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
        ),
        _gmail_tool(
            name="gmail_batch_get_messages",
            remote_tool_name="batch_get_messages",
            description="Read multiple Gmail messages by ID in one call.",
            parameters={
                "type": "object",
                "properties": {
                    "message_ids": {"type": "array", "items": {"type": "string"}},
                    "format": {"type": "string"},
                },
                "required": ["message_ids"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
        ),
        _gmail_tool(
            name="gmail_get_thread",
            remote_tool_name="get_thread",
            description="Read an entire Gmail thread by thread ID.",
            parameters={
                "type": "object",
                "properties": {
                    "thread_id": {"type": "string"},
                    "format": {"type": "string"},
                },
                "required": ["thread_id"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
        ),
        _gmail_tool(
            name="gmail_get_unread_count",
            remote_tool_name="get_unread_count",
            description="Return the unread email count, optionally scoped to a label such as INBOX.",
            parameters={
                "type": "object",
                "properties": {
                    "label_id": {"type": "string"},
                },
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
        ),
    ]
