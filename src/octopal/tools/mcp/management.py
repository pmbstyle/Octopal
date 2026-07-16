from __future__ import annotations

import json
from typing import Any

import structlog

from octopal.infrastructure.mcp.manager import MCPServerConfig
from octopal.infrastructure.mcp.tasks import MCPTaskContext, task_status_result
from octopal.infrastructure.store.models import MCPTaskRecord
from octopal.tools.registry import ToolSpec

logger = structlog.get_logger(__name__)


def _task_context(ctx: dict[str, Any]) -> MCPTaskContext:
    worker = ctx.get("worker")
    spec = getattr(worker, "spec", None)
    worker_run_id = str(ctx.get("worker_id") or getattr(spec, "id", None) or "").strip()
    chat_id: int | None = None
    try:
        raw_chat_id = ctx.get("chat_id")
        if raw_chat_id is not None:
            chat_id = int(raw_chat_id)
    except (TypeError, ValueError):
        pass
    chat_turn_id: str | None = None
    if chat_id is not None and ctx.get("chat_turn_epoch") is not None:
        try:
            chat_turn_id = f"{chat_id}:{int(ctx['chat_turn_epoch'])}"
        except (TypeError, ValueError):
            chat_turn_id = None
    return MCPTaskContext(
        correlation_id=(
            str(ctx.get("correlation_id") or getattr(spec, "correlation_id", None) or "").strip()
            or None
        ),
        worker_run_id=worker_run_id or None,
        chat_id=chat_id,
        chat_turn_id=chat_turn_id,
    )


def _task_record_payload(record: MCPTaskRecord) -> dict[str, Any]:
    structured = task_status_result(record).structuredContent
    return dict(structured or {})


def _tool_result_payload(result: Any) -> dict[str, Any]:
    return {
        "ok": not bool(getattr(result, "isError", False)),
        "content": [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else str(item)
            for item in result.content
        ],
        "structured_content": getattr(result, "structuredContent", None),
    }


async def mcp_connect(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Connect to a new MCP server."""
    safe_args = dict(args)
    if isinstance(safe_args.get("headers"), dict):
        safe_args["headers"] = {
            k: ("***" if "authorization" in str(k).lower() else v)
            for k, v in safe_args["headers"].items()
        }
    if isinstance(safe_args.get("env"), dict):
        safe_args["env"] = {
            k: (
                "***"
                if "key" in str(k).lower()
                or "token" in str(k).lower()
                or "secret" in str(k).lower()
                else v
            )
            for k, v in safe_args["env"].items()
        }
    logger.info("mcp_connect tool called", arguments=safe_args)
    octo = ctx.get("octo")
    if not octo or not octo.mcp_manager:
        return "Error: MCP Manager not initialized."

    server_id = args.get("id")
    name = args.get("name", server_id)
    command = args.get("command")
    server_args = args.get("args", [])
    env = args.get("env", {})
    url = args.get("url")
    headers = args.get("headers", {})
    transport = args.get("transport") or args.get("type")

    if not server_id or (not command and not url):
        return "Error: 'id' and either 'command' or 'url' are required."

    # Prevent common mistake where Octo uses ID as command
    if command == server_id:
        return f"Error: You provided the server ID '{server_id}' as the command. If this is an SSE server, use the 'url' parameter instead. If it's a local server, you likely need a real command like 'npx' or 'python' with appropriate arguments."

    # Helpful hint for local development confusion
    if url and "localhost" in url.lower() and "http://localhost:3000" in url.lower():
        logger.warning(
            "Octo is attempting to connect to what looks like a default localhost URL. This might be a mistake if the server is external."
        )

    config = MCPServerConfig(
        id=server_id,
        name=name,
        command=command,
        args=server_args,
        env=env,
        url=url,
        headers=headers,
        transport=transport,
    )

    try:
        tools = await octo.mcp_manager.connect_server(config)
        # Use the actual names from the ToolSpec objects
        tool_names = [t.name for t in tools]
        return json.dumps(
            {
                "status": "connected",
                "server_id": server_id,
                "message": f"Successfully connected to MCP server '{server_id}'. {len(tools)} tools have been added to your toolset and are ready to be used. You can call them directly just like any other tool (e.g. by using their name in a tool call block).",
                "transport": config.transport or "auto",
                "tools_added": tool_names,
            },
            indent=2,
        )
    except Exception as e:
        logger.error(
            "Dynamic MCP connection failed", server_id=server_id, error=str(e), exc_info=True
        )
        return f"Failed to connect to MCP server '{server_id}': {e}. Please check the URL/command and ensure the server is reachable."


async def mcp_disconnect(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Disconnect from an MCP server."""
    octo = ctx.get("octo")
    if not octo or not octo.mcp_manager:
        return "Error: MCP Manager not initialized."

    server_id = args.get("id")
    if not server_id:
        return "Error: 'id' is required."

    try:
        await octo.mcp_manager.disconnect_server(server_id)
        return f"Disconnected from MCP server {server_id}."
    except Exception as e:
        return f"Error disconnecting from MCP server {server_id}: {e}"


def mcp_list(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """List connected MCP servers and their tools."""
    octo = ctx.get("octo")
    if not octo or not octo.mcp_manager:
        return "Error: MCP Manager not initialized."

    servers = []
    for server_id, _session in octo.mcp_manager.sessions.items():
        tools = octo.mcp_manager._tools.get(server_id, [])
        servers.append(
            {
                "id": server_id,
                "tool_count": len(tools),
                "tools": [t.name for t in tools],  # Full names like mcp_server_tool
            }
        )

    return json.dumps({"connected_servers": servers}, indent=2)


def mcp_status(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """List status for all known MCP servers, including disconnected/error states."""
    octo = ctx.get("octo")
    if not octo or not octo.mcp_manager:
        return "Error: MCP Manager not initialized."

    statuses = octo.mcp_manager.get_server_statuses()
    return json.dumps(
        {
            "servers": statuses,
            "connected_count": len(octo.mcp_manager.sessions),
            "known_count": len(statuses),
            "configured_count": len(statuses),
            "reconnecting_count": sum(
                1
                for payload in statuses.values()
                if str(payload.get("status", "")).lower() == "reconnecting"
            ),
            "error_count": sum(
                1
                for payload in statuses.values()
                if str(payload.get("status", "")).lower() == "error"
            ),
        },
        indent=2,
    )


def mcp_discover(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Summarize MCP server usability, exposed tools, and suggested next actions."""
    octo = ctx.get("octo")
    if not octo or not octo.mcp_manager:
        return "Error: MCP Manager not initialized."

    server_filter = str(args.get("server_id", "") or "").strip()
    limit = max(1, min(int(args.get("limit") or 20), 50))
    statuses = octo.mcp_manager.get_server_statuses()
    tool_map = getattr(octo.mcp_manager, "_tools", {}) or {}

    server_ids = [server_filter] if server_filter else list(statuses.keys())
    server_summaries: list[dict[str, Any]] = []
    for server_id in server_ids:
        status_payload = statuses.get(server_id)
        if status_payload is None:
            continue
        specs = list(tool_map.get(server_id, []))
        remote_tools: list[dict[str, Any]] = []
        for spec in specs[:limit]:
            remote_name = str(getattr(spec, "remote_tool_name", "") or spec.name)
            remote_tools.append(
                {
                    "generated_name": spec.name,
                    "remote_name": remote_name,
                    "description": str(spec.description or "")[:200],
                    "direct_call_hint": f"Call `{spec.name}` directly, or use mcp_call with server_id='{server_id}' and tool_name='{remote_name}'.",
                }
            )

        status = str(status_payload.get("status", "unknown") or "unknown")
        reason = str(status_payload.get("reason", "") or "")
        suggested_action = "connect_or_reconnect"
        if status == "connected" and remote_tools:
            suggested_action = "call_tool_directly"
        elif status == "configured":
            suggested_action = "mcp_connect"
        elif status == "reconnecting":
            suggested_action = "wait_for_reconnect"
        elif status == "error":
            suggested_action = "inspect_connection_error"

        server_summaries.append(
            {
                "server_id": server_id,
                "name": status_payload.get("name") or server_id,
                "status": status,
                "connected": bool(status_payload.get("connected")),
                "reason": reason,
                "tool_count": len(specs),
                "transport": status_payload.get("transport"),
                "tools": remote_tools,
                "suggested_action": suggested_action,
            }
        )

    connected_servers = [server for server in server_summaries if server["connected"]]
    unavailable_servers = [server for server in server_summaries if not server["connected"]]
    hints: list[str] = []
    if connected_servers:
        hints.append(
            f"{len(connected_servers)} MCP server(s) are ready; prefer direct generated tool calls before raw mcp_call."
        )
    if unavailable_servers:
        hints.append(
            f"{len(unavailable_servers)} MCP server(s) are not ready; inspect status/reason before relying on them."
        )
    if not hints:
        hints.append("No MCP servers are known yet; connect one before planning MCP-backed work.")

    return json.dumps(
        {
            "status": "ok",
            "server_filter": server_filter or None,
            "server_count": len(server_summaries),
            "connected_count": len(connected_servers),
            "unavailable_count": len(unavailable_servers),
            "servers": server_summaries,
            "hints": hints,
        },
        indent=2,
    )


async def mcp_call(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Call an MCP tool on a specific server."""
    octo = ctx.get("octo")
    if not octo or not octo.mcp_manager:
        return "Error: MCP Manager not initialized."

    server_id = args.get("server_id")
    tool_name = args.get("tool_name")
    tool_args = args.get("arguments", {})

    if not server_id or not tool_name:
        return "Error: 'server_id' and 'tool_name' are required."

    session = octo.mcp_manager.sessions.get(server_id)
    if not session:
        return f"Error: MCP session '{server_id}' not active. Available servers: {list(octo.mcp_manager.sessions.keys())}"

    logger.info("Octo calling MCP tool via mcp_call", server_id=server_id, tool=tool_name)
    try:
        result = await octo.mcp_manager.call_tool(
            server_id,
            tool_name,
            tool_args,
            task_context=_task_context(ctx),
        )
        return json.dumps(
            [
                item.model_dump() if hasattr(item, "model_dump") else str(item)
                for item in result.content
            ],
            indent=2,
        )
    except Exception as e:
        logger.exception("MCP tool call failed", server_id=server_id, tool=tool_name)
        return json.dumps(
            {
                "ok": False,
                "server_id": server_id,
                "tool": tool_name,
                "error": str(e),
            },
            indent=2,
        )


async def mcp_task_get(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Refresh or resume a durable MCP task by its client-side handle."""
    octo = ctx.get("octo")
    if not octo or not octo.mcp_manager:
        return "Error: MCP Manager not initialized."
    task_id = str(args.get("task_id") or "").strip()
    if not task_id:
        return "Error: 'task_id' is required."
    try:
        result = await octo.mcp_manager.resume_task(
            task_id,
            task_context=_task_context(ctx),
        )
        return json.dumps(_tool_result_payload(result), indent=2)
    except Exception as exc:
        logger.exception("MCP task refresh failed", task_id=task_id)
        return json.dumps({"ok": False, "task_id": task_id, "error": str(exc)}, indent=2)


async def mcp_task_update(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Submit trusted responses for an MCP task waiting for input."""
    octo = ctx.get("octo")
    if not octo or not octo.mcp_manager:
        return "Error: MCP Manager not initialized."
    task_id = str(args.get("task_id") or "").strip()
    input_responses = args.get("input_responses")
    if not task_id or not isinstance(input_responses, dict):
        return "Error: 'task_id' and object 'input_responses' are required."
    try:
        record = await octo.mcp_manager.update_task(
            task_id,
            input_responses,
            task_context=_task_context(ctx),
        )
        return json.dumps({"ok": True, **_task_record_payload(record)}, indent=2)
    except Exception as exc:
        logger.exception("MCP task update failed", task_id=task_id)
        return json.dumps({"ok": False, "task_id": task_id, "error": str(exc)}, indent=2)


async def mcp_task_cancel(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Request cancellation for a durable MCP task."""
    octo = ctx.get("octo")
    if not octo or not octo.mcp_manager:
        return "Error: MCP Manager not initialized."
    task_id = str(args.get("task_id") or "").strip()
    if not task_id:
        return "Error: 'task_id' is required."
    try:
        record = await octo.mcp_manager.cancel_task(
            task_id,
            task_context=_task_context(ctx),
        )
        return json.dumps({"ok": True, **_task_record_payload(record)}, indent=2)
    except Exception as exc:
        logger.exception("MCP task cancellation failed", task_id=task_id)
        return json.dumps({"ok": False, "task_id": task_id, "error": str(exc)}, indent=2)


def get_mcp_mgmt_tools() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="mcp_connect",
            description="Connect to an external MCP server. Use 'command' for local stdio servers. For URL-based servers, set 'transport' explicitly when known: 'sse' or 'streamable-http'.",
            parameters={
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Unique ID for this server (e.g. 'sqlite').",
                    },
                    "name": {"type": "string", "description": "Human-readable name."},
                    "command": {
                        "type": "string",
                        "description": "Command to run for stdio servers (e.g. 'npx', 'python', 'node').",
                    },
                    "args": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Arguments for the command (e.g. ['-y', '@modelcontextprotocol/server-everything']).",
                    },
                    "env": {
                        "type": "object",
                        "description": "Environment variables for stdio (e.g. API keys).",
                    },
                    "url": {"type": "string", "description": "URL for HTTP MCP servers."},
                    "headers": {
                        "type": "object",
                        "description": "HTTP headers (e.g. {'Authorization': 'Bearer ...'}).",
                    },
                    "transport": {
                        "type": "string",
                        "enum": ["auto", "sse", "streamable-http", "stdio"],
                        "description": "Connection transport. Default is 'auto'.",
                    },
                    "type": {
                        "type": "string",
                        "description": "Alias for transport (legacy compatibility).",
                    },
                },
                "required": ["id"],
            },
            permission="self_control",
            handler=mcp_connect,
            is_async=True,
        ),
        ToolSpec(
            name="mcp_disconnect",
            description="Disconnect from an MCP server.",
            parameters={
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "Server ID to disconnect."},
                },
                "required": ["id"],
            },
            permission="self_control",
            handler=mcp_disconnect,
            is_async=True,
        ),
        ToolSpec(
            name="mcp_list",
            description="List active MCP servers and their tools.",
            parameters={"type": "object", "properties": {}},
            permission="self_control",
            handler=mcp_list,
        ),
        ToolSpec(
            name="mcp_status",
            description="Show status for all known MCP servers, including connected/error/disconnected states.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=mcp_status,
        ),
        ToolSpec(
            name="mcp_call",
            description="Call an MCP tool on a specific server. (Note: You can also call MCP tools directly by their generated names like 'mcp_serverid_toolname').",
            parameters={
                "type": "object",
                "properties": {
                    "server_id": {"type": "string", "description": "ID of the MCP server."},
                    "tool_name": {
                        "type": "string",
                        "description": "Name of the tool on that server.",
                    },
                    "arguments": {"type": "object", "description": "Arguments for the tool."},
                },
                "required": ["server_id", "tool_name"],
            },
            permission="mcp_exec",
            handler=mcp_call,
            is_async=True,
        ),
        ToolSpec(
            name="mcp_discover",
            description="Summarize MCP server readiness, exposed tools, and the best next step for using them.",
            parameters={
                "type": "object",
                "properties": {
                    "server_id": {
                        "type": "string",
                        "description": "Optional server ID to focus on.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max tools to preview per server (default 20, max 50).",
                    },
                },
                "additionalProperties": False,
            },
            permission="self_control",
            handler=mcp_discover,
        ),
        ToolSpec(
            name="mcp_task_get",
            description="Refresh or resume a durable MCP task returned by an earlier MCP tool call.",
            parameters={
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Durable client-side MCP task ID.",
                    }
                },
                "required": ["task_id"],
                "additionalProperties": False,
            },
            permission="mcp_exec",
            handler=mcp_task_get,
            is_async=True,
        ),
        ToolSpec(
            name="mcp_task_update",
            description="Submit trusted input for a durable MCP task in input_required state. Only use responses obtained through the normal instruction or approval path.",
            parameters={
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Durable client-side MCP task ID.",
                    },
                    "input_responses": {
                        "type": "object",
                        "description": "Responses keyed by the outstanding input request names.",
                    },
                },
                "required": ["task_id", "input_responses"],
                "additionalProperties": False,
            },
            permission="mcp_exec",
            handler=mcp_task_update,
            is_async=True,
        ),
        ToolSpec(
            name="mcp_task_cancel",
            description="Request cancellation of a durable MCP task returned by an earlier MCP tool call.",
            parameters={
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Durable client-side MCP task ID.",
                    }
                },
                "required": ["task_id"],
                "additionalProperties": False,
            },
            permission="mcp_exec",
            handler=mcp_task_cancel,
            is_async=True,
        ),
    ]
