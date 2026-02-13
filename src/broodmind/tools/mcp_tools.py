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

    if not server_id or not command:
        return "Error: 'id' and 'command' are required."

    config = MCPServerConfig(
        id=server_id,
        name=name,
        command=command,
        args=server_args,
        env=env
    )

    try:
        tools = await queen.mcp_manager.connect_server(config)
        return json.dumps({
            "status": "connected",
            "server_id": server_id,
            "tool_count": len(tools),
            "tools": [t.name for t in tools]
        }, indent=2)
    except Exception as e:
        return f"Error connecting to MCP server {server_id}: {e}"

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

def get_mcp_mgmt_tools() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="mcp_connect",
            description="Connect to an external MCP server (stdio).",
            parameters={
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "Unique ID for this server."},
                    "name": {"type": "string", "description": "Human-readable name."},
                    "command": {"type": "string", "description": "Command to run (e.g., 'python', 'npx')."},
                    "args": {"type": "array", "items": {"type": "string"}, "description": "Arguments for the command."},
                    "env": {"type": "object", "description": "Environment variables."},
                },
                "required": ["id", "command"],
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
    ]
