from __future__ import annotations

import asyncio
import json
import os
import structlog
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

@dataclass
class MCPServerConfig:
    id: str
    name: str
    command: Optional[str] = None
    args: List[str] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=dict)
    url: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.sse import sse_client

from broodmind.tools.registry import ToolSpec

logger = structlog.get_logger(__name__)

class MCPManager:
    def __init__(self, workspace_dir: Path):
        self.workspace_dir = workspace_dir
        self.sessions: Dict[str, ClientSession] = {}
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
                    command=cfg.get("command"),
                    args=cfg.get("args", []),
                    env=cfg.get("env", {}),
                    url=cfg.get("url"),
                    headers=cfg.get("headers", {})
                )
                # Support different type names for SSE
                if cfg.get("type") in ("streamable-http", "sse", "http") and not mcp_cfg.url:
                    # If type is SSE but url is missing, maybe it's in another field or we should warn
                    pass

                try:
                    await self.connect_server(mcp_cfg)
                except Exception:
                    logger.exception("Failed to connect to MCP server on startup", server_id=server_id)
        except Exception:
            logger.exception("Failed to load MCP config")

    async def connect_server(self, config: MCPServerConfig) -> List[ToolSpec]:
        if config.id in self.sessions:
            logger.info("MCP server already connected", server_id=config.id)
            return self._tools.get(config.id, [])

        from contextlib import AsyncExitStack
        exit_stack = AsyncExitStack()
        self._exit_stacks[config.id] = exit_stack
        
        try:
            if config.url:
                logger.info("Connecting to MCP SSE server", server_id=config.id, url=config.url)
                # Ensure headers are a dict
                headers = config.headers if isinstance(config.headers, dict) else {}
                
                try:
                    read_stream, write_stream = await exit_stack.enter_async_context(
                        sse_client(url=config.url, headers=headers)
                    )
                except Exception as e:
                    logger.error("Failed to establish SSE transport", server_id=config.id, error=str(e))
                    raise
            elif config.command:
                logger.info("Connecting to MCP stdio server", server_id=config.id, command=config.command)
                params = StdioServerParameters(
                    command=config.command,
                    args=config.args,
                    env={**config.env, "PATH": os.environ.get("PATH", "")} if config.env else None
                )
                try:
                    read_stream, write_stream = await exit_stack.enter_async_context(stdio_client(params))
                except Exception as e:
                    logger.error("Failed to establish stdio transport", server_id=config.id, error=str(e))
                    raise
            else:
                raise ValueError(f"MCP server {config.id} must have 'url' or 'command'.")

            logger.info("Initializing MCP session", server_id=config.id)
            session = await exit_stack.enter_async_context(ClientSession(read_stream, write_stream))
            
            # Use a timeout for initialization to avoid hanging
            try:
                await asyncio.wait_for(session.initialize(), timeout=30.0)
            except asyncio.TimeoutError:
                logger.error("MCP session initialization timed out", server_id=config.id)
                raise
            
            self.sessions[config.id] = session
            
            # Fetch tools
            logger.info("Fetching MCP tools", server_id=config.id)
            mcp_tools = await session.list_tools()
            
            specs = []
            for tool in mcp_tools.tools:
                spec = ToolSpec(
                    name=f"mcp_{config.id}_{tool.name}",
                    description=f"[MCP Tool: {tool.name} from {config.name}] {tool.description}. Call this tool directly by using the name '{f'mcp_{config.id}_{tool.name}'}' in your tool call block.",
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
            # If any step fails, we must close the exit stack to release resources/streams
            error_msg = str(e)
            
            # Handle ExceptionGroup (Python 3.11+) which is common with TaskGroups/anyio
            if hasattr(e, "exceptions") and isinstance(e, BaseExceptionGroup):
                error_msg = f"ExceptionGroup: {', '.join(str(ex) for ex in e.exceptions)}"
                logger.error("MCP connection failed with multiple errors", server_id=config.id, errors=[str(ex) for ex in e.exceptions])

            logger.exception("Failed to connect to MCP server", server_id=config.id, error_type=type(e).__name__, message=error_msg)
            
            await exit_stack.aclose()
            self._exit_stacks.pop(config.id, None)
            raise RuntimeError(f"MCP Connection Error ({config.id}): {error_msg}") from e

    def _generate_handler(self, server_id: str, tool_name: str):
        async def handler(args: Dict[str, Any], ctx: Dict[str, Any]) -> Any:
            worker = ctx.get("worker")
            if worker:
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
