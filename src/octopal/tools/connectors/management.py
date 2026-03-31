from __future__ import annotations

import json
from typing import Any

import structlog

from octopal.tools.registry import ToolSpec

logger = structlog.get_logger(__name__)

async def connector_status(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Get the status of all available connectors."""
    octo = ctx.get("octo")
    if not octo or not octo.connector_manager:
        return "Error: Connector Manager not initialized."
    
    statuses = await octo.connector_manager.get_all_statuses()
    return json.dumps({"connectors": statuses}, indent=2)

async def connector_configure(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Configure a connector."""
    octo = ctx.get("octo")
    if not octo or not octo.connector_manager:
        return "Error: Connector Manager not initialized."
    
    name = args.get("name")
    settings = args.get("settings", {})
    
    if name not in octo.connector_manager.connectors:
        return f"Error: Connector '{name}' not found. Available: {list(octo.connector_manager.connectors.keys())}"
    
    await octo.connector_manager.connectors[name].configure(settings)
    return f"Connector '{name}' configured successfully."

async def connector_setup(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Start the setup process for a connector."""
    octo = ctx.get("octo")
    if not octo or not octo.connector_manager:
        return "Error: Connector Manager not initialized."
    
    name = args.get("name")
    if name not in octo.connector_manager.connectors:
        return f"Error: Connector '{name}' not found."
    
    result = await octo.connector_manager.connectors[name].setup()
    return json.dumps(result, indent=2)

async def connector_complete_setup(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Complete the setup process for a connector."""
    octo = ctx.get("octo")
    if not octo or not octo.connector_manager:
        return "Error: Connector Manager not initialized."
    
    name = args.get("name")
    data = args.get("data", {})
    
    if name not in octo.connector_manager.connectors:
        return f"Error: Connector '{name}' not found."
    
    result = await octo.connector_manager.connectors[name].complete_setup(data)
    return json.dumps(result, indent=2)

def get_connector_management_tools() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="connector_status",
            description="List all available connectors and their current status.",
            parameters={"type": "object", "properties": {}},
            permission="self_control",
            handler=connector_status,
            is_async=True,
        ),
        ToolSpec(
            name="connector_configure",
            description="Configure a connector with required settings (e.g. client_id, client_secret).",
            parameters={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Connector name (e.g. 'google')."},
                    "settings": {"type": "object", "description": "Settings for the connector."},
                },
                "required": ["name", "settings"],
            },
            permission="self_control",
            handler=connector_configure,
            is_async=True,
        ),
        ToolSpec(
            name="connector_setup",
            description="Start the interactive setup/auth flow for a connector.",
            parameters={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Connector name."},
                },
                "required": ["name"],
            },
            permission="self_control",
            handler=connector_setup,
            is_async=True,
        ),
        ToolSpec(
            name="connector_complete_setup",
            description="Complete the setup flow for a connector using the provided data (e.g. auth_code).",
            parameters={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Connector name."},
                    "data": {"type": "object", "description": "Setup data (e.g. {'auth_code': '...'})"},
                },
                "required": ["name", "data"],
            },
            permission="self_control",
            handler=connector_complete_setup,
            is_async=True,
        ),
    ]
