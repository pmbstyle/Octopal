from __future__ import annotations

import asyncio
import json
import os
import structlog
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

@dataclass
class MCPServerConfig:
    id: str
    name: str
    command: Optional[str] = None
    args: List[str] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=dict)
    url: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    transport: Optional[Literal["auto", "sse", "streamable-http", "stdio"]] = None
    last_error: Optional[str] = None

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.sse import sse_client
from mcp.client.streamable_http import streamablehttp_client

from broodmind.tools.registry import ToolSpec

logger = structlog.get_logger(__name__)

class MCPManager:
    def __init__(self, workspace_dir: Path):
        self.workspace_dir = workspace_dir
        self.sessions: Dict[str, ClientSession] = {}
        # Stores the background task that keeps the session alive
        self._tasks: Dict[str, asyncio.Task] = {}
        # Communication queues for disconnect signals
        self._stop_events: Dict[str, asyncio.Event] = {}
        self._tools: Dict[str, List[ToolSpec]] = {}
        self._server_configs: Dict[str, MCPServerConfig] = {}
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
                    headers=cfg.get("headers", {}),
                    transport=_normalize_transport(cfg.get("transport") or cfg.get("type")),
                )

                try:
                    await self.connect_server(mcp_cfg)
                except Exception:
                    logger.exception("Failed to connect to MCP server on startup", server_id=server_id)
        except Exception:
            logger.exception("Failed to load MCP config")

    async def connect_server(self, config: MCPServerConfig) -> List[ToolSpec]:
        self._server_configs[config.id] = config
        if config.id in self.sessions:
            logger.info("MCP server already connected", server_id=config.id)
            return self._tools.get(config.id, [])

        # Create an event to signal connection readiness and an event for stopping
        ready_event = asyncio.Event()
        stop_event = asyncio.Event()
        self._stop_events[config.id] = stop_event
        
        # Start background task to manage the lifecycle
        task = asyncio.create_task(self._run_server_lifecycle(config, ready_event, stop_event))
        self._tasks[config.id] = task
        
        # Wait for the session to be initialized or task to fail
        try:
            # Monitor both the ready event and the task itself
            done, pending = await asyncio.wait(
                [asyncio.create_task(ready_event.wait()), task],
                return_when=asyncio.FIRST_COMPLETED,
                timeout=45.0
            )
            
            # Check if we timed out
            if not done:
                for p in pending: p.cancel()
                config.last_error = "Connection timed out after 45s"
                raise RuntimeError(f"Connection to MCP server '{config.id}' timed out after 45s.")

            if ready_event.is_set():
                # Success!
                config.last_error = None
                return self._tools.get(config.id, [])
            
            # If the task finished but ready_event is not set, it failed
            if task in done:
                exc = task.exception()
                if exc:
                    config.last_error = str(exc)
                    raise exc
                config.last_error = "Exited unexpectedly"
                raise RuntimeError(f"MCP server task '{config.id}' exited unexpectedly.")
            
            config.last_error = "Failed (unknown state)"
            raise RuntimeError(f"Connection to MCP server '{config.id}' failed (unknown state).")

        except Exception as e:
            logger.error("Failed to connect to MCP server", server_id=config.id, error=str(e))
            if not config.last_error:
                config.last_error = str(e)
            await self.disconnect_server(config.id)
            if isinstance(e, RuntimeError) and "timed out" in str(e):
                raise
            raise RuntimeError(f"MCP Connection Error ({config.id}): {e}") from e

    async def _run_server_lifecycle(self, config: MCPServerConfig, ready_event: asyncio.Event, stop_event: asyncio.Event):
        """Manages the lifetime of a single MCP server connection."""
        from contextlib import AsyncExitStack
        exit_stack = AsyncExitStack()
        
        try:
            selected_transport = _resolve_transport(config)
            if selected_transport == "sse":
                logger.info("Establishing MCP SSE transport", server_id=config.id, url=config.url)
                read_stream, write_stream = await exit_stack.enter_async_context(
                    sse_client(url=config.url or "", headers=config.headers)
                )
            elif selected_transport == "streamable-http":
                logger.info("Establishing MCP streamable-http transport", server_id=config.id, url=config.url)
                read_stream, write_stream, _get_session_id = await exit_stack.enter_async_context(
                    streamablehttp_client(
                        url=config.url or "",
                        headers=config.headers or None,
                        timeout=timedelta(seconds=30),
                        sse_read_timeout=timedelta(seconds=300),
                    )
                )
            elif selected_transport == "stdio":
                logger.info("Establishing MCP stdio transport", server_id=config.id, command=config.command)
                params = StdioServerParameters(
                    command=config.command,
                    args=config.args,
                    env={**config.env, "PATH": os.environ.get("PATH", "")} if config.env else None
                )
                read_stream, write_stream = await exit_stack.enter_async_context(stdio_client(params))
            else:
                raise ValueError(f"Unsupported MCP transport '{selected_transport}' for server {config.id}.")

            logger.info("Initializing MCP session", server_id=config.id)
            session = await exit_stack.enter_async_context(ClientSession(read_stream, write_stream))
            
            await session.initialize()
            self.sessions[config.id] = session
            
            # Fetch tools
            logger.info("Fetching tools from MCP server", server_id=config.id)
            mcp_tools_list = await session.list_tools()
            
            specs = []
            for tool in mcp_tools_list.tools:
                # Normalize tool name: replace dashes with underscores for better LLM compatibility
                safe_id = config.id.replace("-", "_")
                safe_tool_name = tool.name.replace("-", "_")
                mcp_tool_name = f"mcp_{safe_id}_{safe_tool_name}"
                
                spec = ToolSpec(
                    name=mcp_tool_name,
                    description=f"[MCP Tool from {config.name}] {tool.description}. Call this tool directly by using the name '{mcp_tool_name}' in your tool call block.",
                    parameters=tool.inputSchema,
                    permission="mcp_exec",
                    handler=self._generate_handler(config.id, tool.name),
                    is_async=True
                )
                specs.append(spec)
            
            self._tools[config.id] = specs
            logger.info("MCP server connected and tools ready", server_id=config.id, tool_count=len(specs))
            
            # Signal that we are ready
            ready_event.set()
            
            # Keep alive until signaled to stop
            await stop_event.wait()
            logger.info("Shutting down MCP server session (signaled)", server_id=config.id)
            
        except Exception as e:
            hint = _connection_hint(e)
            logger.exception("MCP server lifecycle error", server_id=config.id, transport=config.transport or "auto", hint=hint)
            if not ready_event.is_set():
                # Task failed before becoming ready - signal the waiter with an error if possible
                # But here we just let the waiter catch the fact that ready_event was never set.
                pass
        finally:
            # Clean up
            self.sessions.pop(config.id, None)
            self._tools.pop(config.id, None)
            self._tasks.pop(config.id, None)
            self._stop_events.pop(config.id, None)
            
            # Closing the stack will close the context managers (stdio/sse clients)
            # This happens in the same task that created them, which anyio requires.
            await exit_stack.aclose()
            logger.info("MCP server resources released", server_id=config.id)

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
        event = self._stop_events.get(server_id)
        if event:
            event.set()
        
        task = self._tasks.get(server_id)
        if task:
            try:
                # Wait for cleanup to finish
                await asyncio.wait_for(task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                if not task.done():
                    task.cancel()
            logger.info("Disconnected MCP server", server_id=server_id)

    def get_all_tools(self) -> List[ToolSpec]:
        all_specs = []
        for specs in self._tools.values():
            all_specs.extend(specs)
        return all_specs

    async def shutdown(self):
        # Trigger all stop events
        for server_id in list(self._stop_events.keys()):
            await self.disconnect_server(server_id)

    def get_server_statuses(self) -> Dict[str, Dict[str, Any]]:
        statuses = {}
        for server_id, config in self._server_configs.items():
            is_connected = server_id in self.sessions
            tools = self._tools.get(server_id, [])
            statuses[server_id] = {
                "name": config.name,
                "status": "connected" if is_connected else ("error" if config.last_error else "disconnected"),
                "tool_count": len(tools),
                "error": config.last_error,
                "transport": config.transport or "auto",
            }
        return statuses


def _normalize_transport(raw: Any) -> Literal["auto", "sse", "streamable-http", "stdio"] | None:
    if raw is None:
        return None
    value = str(raw).strip().lower()
    if value in {"auto", ""}:
        return "auto"
    if value in {"sse", "http-sse"}:
        return "sse"
    if value in {"streamable-http", "streamable_http", "streamablehttp", "http"}:
        return "streamable-http"
    if value in {"stdio", "local"}:
        return "stdio"
    return None


def _resolve_transport(config: MCPServerConfig) -> Literal["sse", "streamable-http", "stdio"]:
    normalized = _normalize_transport(config.transport) or "auto"
    if normalized != "auto":
        return normalized
    if config.command:
        return "stdio"
    if config.url:
        url = config.url.lower()
        if "streamable" in url:
            return "streamable-http"
        return "sse"
    raise ValueError(f"MCP server {config.id} must have either 'command' or 'url'.")


def _connection_hint(error: Exception) -> str:
    text = str(error).lower()
    if "text/event-stream" in text and "application/json" in text:
        return "Transport mismatch: server returned JSON, but client expected SSE. Try transport='streamable-http'."
    if "404" in text or "not found" in text:
        return "Endpoint not found: verify MCP URL/path and provider docs."
    if "timed out" in text:
        return "Connection timed out: check network egress, DNS, firewall, or provider availability."
    if "connection closed" in text:
        return "Remote side closed connection early: verify auth and protocol compatibility."
    return "Unknown MCP connection issue. Verify URL/transport/auth."
