from __future__ import annotations

import asyncio
import json
import os
import structlog
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from broodmind.tools.registry import ToolSpec

logger = structlog.get_logger(__name__)

@dataclass
class MCPServerConfig:
    id: str
    name: str
    command: str
    args: List[str] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=dict)

class MCPManager:
    def __init__(self, workspace_dir: Path):
        self.workspace_dir = workspace_dir
        self.sessions: Dict[str, ClientSession] = {}
        self.server_params: Dict[str, StdioServerParameters] = {}
        self._exit_stacks: Dict[str, Any] = {}
        self._tools: Dict[str, List[ToolSpec]] = {}
        self.config_path = workspace_dir / "mcp_servers.json"

    async def load_and_connect_all(self):
        if not self.config_path.exists():
            return
        try:
            config_data = json.loads(self.config_path.read_text(encoding="utf-8"))
            for server_id, cfg in config_data.items():
                mcp_cfg = MCPServerConfig(
                    id=server_id,
                    name=cfg.get("name", server_id),
                    command=cfg["command"],
                    args=cfg.get("args", []),
                    env=cfg.get("env", {})
                )
                try:
                    await self.connect_server(mcp_cfg)
                except Exception:
                    logger.exception("Failed to connect to MCP server on startup", server_id=server_id)
        except Exception:
            logger.exception("Failed to load MCP config")

    def _save_config(self):
        # Implementation could be added here to persist connections
        pass

    async def connect_server(self, config: MCPServerConfig) -> List[ToolSpec]:
        if config.id in self.sessions:
            return self._tools.get(config.id, [])

        logger.info("Connecting to MCP server", server_id=config.id, command=config.command)
        
        params = StdioServerParameters(
            command=config.command,
            args=config.args,
            env={**config.env, "PATH": os.environ.get("PATH", "")} if config.env else None
        )
        
        from contextlib import AsyncExitStack
        exit_stack = AsyncExitStack()
        self._exit_stacks[config.id] = exit_stack
        
        try:
            read_stream, write_stream = await exit_stack.enter_async_context(stdio_client(params))
            session = await exit_stack.enter_async_context(ClientSession(read_stream, write_stream))
            
            await session.initialize()
            self.sessions[config.id] = session
            
            # Fetch tools
            mcp_tools = await session.list_tools()
            
            specs = []
            for tool in mcp_tools.tools:
                spec = ToolSpec(
                    name=f"mcp_{config.id}_{tool.name}",
                    description=f"[MCP:{config.name}] {tool.description}",
                    parameters=tool.inputSchema,
                    permission="mcp_exec",
                    handler=self._generate_handler(config.id, tool.name),
                    is_async=True
                )
                specs.append(spec)
            
            self._tools[config.id] = specs
            logger.info("Connected to MCP server", server_id=config.id, tool_count=len(specs))
            return specs
            
        except Exception as e:
            logger.exception("Failed to connect to MCP server", server_id=config.id)
            await exit_stack.aclose()
            self._exit_stacks.pop(config.id, None)
            raise

    def _generate_handler(self, server_id: str, tool_name: str):
        async def handler(args: Dict[str, Any], ctx: Dict[str, Any]) -> Any:
            # Check if we are in a worker context
            worker = ctx.get("worker")
            if worker:
                # In worker context, we must use call_mcp_tool to call the MCP tool
                # because the session is in the main process.
                logger.info("Worker requesting MCP tool call", server_id=server_id, tool=tool_name)
                try:
                    result = await worker.call_mcp_tool(server_id, tool_name, args)
                    return result
                except Exception as e:
                    return f"Error calling MCP tool via proxy: {e}"

            session = self.sessions.get(server_id)
            if not session:
                return f"Error: MCP session {server_id} not active."
            
            logger.info("Calling MCP tool", server_id=server_id, tool=tool_name)
            try:
                result = await session.call_tool(tool_name, arguments=args)
                return [c.model_dump() if hasattr(c, "model_dump") else str(c) for c in result.content]
            except Exception as e:
                logger.exception("MCP tool call failed", server_id=server_id, tool=tool_name)
                return f"Error calling MCP tool {tool_name}: {e}"
        
        return handler

    async def disconnect_server(self, server_id: str):
        if server_id in self._exit_stacks:
            await self._exit_stacks[server_id].aclose()
            self._exit_stacks.pop(server_id, None)
            self.sessions.pop(server_id, None)
            self._tools.pop(server_id, None)
            logger.info("Disconnected MCP server", server_id=server_id)

    def get_all_tools(self) -> List[ToolSpec]:
        all_specs = []
        for specs in self._tools.values():
            all_specs.extend(specs)
        return all_specs

    async def shutdown(self):
        for server_id in list(self._exit_stacks.keys()):
            await self.disconnect_server(server_id)
