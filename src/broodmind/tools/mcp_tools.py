from __future__ import annotations

import json
import structlog
from typing import Any, Dict

from broodmind.mcp.manager import MCPServerConfig
from broodmind.tools.registry import ToolSpec

logger = structlog.get_logger(__name__)

async def mcp_connect(args: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    """Connect to a new MCP server."""
    queen = ctx.get("queen")
    if not queen or not queen.mcp_manager:
        return "Error: MCP Manager not initialized."
    
    server_id = args.get("id")
    name = args.get("name", server_id)
    command = args.get("command")
    server_args = args.get("args", [])
    env = args.get("env", {})
    url = args.get("url")
    headers = args.get("headers", {})

    if not server_id or (not command and not url):
        return "Error: 'id' and either 'command' or 'url' are required."

    config = MCPServerConfig(
        id=server_id,
        name=name,
        command=command,
        args=server_args,
        env=env,
        url=url,
        headers=headers
    )

    try:
        tools = await queen.mcp_manager.connect_server(config)
        tool_names = [t.name for t in tools]
        return json.dumps({
            "status": "connected",
            "server_id": server_id,
            "message": f"Successfully connected to MCP server '{server_id}'. {len(tools)} tools have been added to your toolset and are ready to be used. You can call them directly just like any other tool (e.g. by using their name in a tool call block).",
            "tools_added": tool_names
        }, indent=2)
    except Exception as e:
        logger.error("Dynamic MCP connection failed", server_id=server_id, error=str(e))
        return f"Failed to connect to MCP server '{server_id}': {e}"

async def mcp_disconnect(args: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    """Disconnect from an MCP server."""
    queen = ctx.get("queen")
    if not queen or not queen.mcp_manager:
        return "Error: MCP Manager not initialized."

    server_id = args.get("id")
    if not server_id:
        return "Error: 'id' is required."

    try:
        await queen.mcp_manager.disconnect_server(server_id)
        return f"Disconnected from MCP server {server_id}."
    except Exception as e:
        return f"Error disconnecting from MCP server {server_id}: {e}"

def mcp_list(args: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    """List connected MCP servers and their tools."""
    queen = ctx.get("queen")
    if not queen or not queen.mcp_manager:
        return "Error: MCP Manager not initialized."

    servers = []
    for server_id, session in queen.mcp_manager.sessions.items():
        tools = queen.mcp_manager._tools.get(server_id, [])
        servers.append({
            "id": server_id,
            "tool_count": len(tools),
            "tools": [t.name for t in tools]
        })

    return json.dumps({"connected_servers": servers}, indent=2)

async def mcp_call(args: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    """Call an MCP tool on a specific server."""
    queen = ctx.get("queen")
    if not queen or not queen.mcp_manager:
        return "Error: MCP Manager not initialized."

    server_id = args.get("server_id")
    tool_name = args.get("tool_name")
    tool_args = args.get("arguments", {})

    if not server_id or not tool_name:
        return "Error: 'server_id' and 'tool_name' are required."

    session = queen.mcp_manager.sessions.get(server_id)
    if not session:
        return f"Error: MCP session '{server_id}' not active. Available servers: {list(queen.mcp_manager.sessions.keys())}"

    logger.info("Queen calling MCP tool via mcp_call", server_id=server_id, tool=tool_name)
    try:
        # We reuse the same logic as the generated handlers
        result = await session.call_tool(tool_name, arguments=tool_args)
        return json.dumps([c.model_dump() if hasattr(c, "model_dump") else str(c) for c in result.content], indent=2)
    except Exception as e:
        logger.exception("MCP tool call failed", server_id=server_id, tool=tool_name)
        return f"Error calling MCP tool {tool_name}: {e}"

def get_mcp_mgmt_tools() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="mcp_connect",
            description="Connect to an external MCP server (stdio or SSE/HTTP).",
            parameters={
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "Unique ID for this server."},
                    "name": {"type": "string", "description": "Human-readable name."},
                    "command": {"type": "string", "description": "Command to run for stdio servers (e.g., 'python')."},
                    "args": {"type": "array", "items": {"type": "string"}, "description": "Arguments for the command."},
                    "env": {"type": "object", "description": "Environment variables for stdio."},
                    "url": {"type": "string", "description": "URL for SSE (HTTP) servers."},
                    "headers": {"type": "object", "description": "HTTP headers for SSE servers (e.g. for Auth)."},
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
            name="mcp_call",
            description="Call an MCP tool on a specific server. (Note: You can also call MCP tools directly by their generated names like 'mcp_serverid_toolname').",
            parameters={
                "type": "object",
                "properties": {
                    "server_id": {"type": "string", "description": "ID of the MCP server."},
                    "tool_name": {"type": "string", "description": "Name of the tool on that server."},
                    "arguments": {"type": "object", "description": "Arguments for the tool."},
                },
                "required": ["server_id", "tool_name"],
            },
            permission="mcp_exec",
            handler=mcp_call,
            is_async=True,
        ),
    ]
