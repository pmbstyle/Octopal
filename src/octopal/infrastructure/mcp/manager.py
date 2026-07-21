from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import structlog
from mcp import ClientSession, StdioServerParameters
from mcp import types as mcp_types
from mcp.client.sse import sse_client
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client

from octopal.infrastructure.mcp.tasks import (
    MCP_TASK_TERMINAL_STATUSES,
    MCPTaskContext,
    MCPTaskState,
    RawMCPRequest,
    RawMCPResult,
    build_task_record,
    client_capability_meta,
    extension_declared,
    legacy_tasks_declared,
    parse_task_state,
    task_expired,
    task_poll_seconds,
    task_ref,
    task_status_result,
)
from octopal.infrastructure.store.models import AuditEvent, MCPTaskProtocol, MCPTaskRecord
from octopal.runtime.tool_errors import MCPToolCallError
from octopal.utils import utc_now

if TYPE_CHECKING:
    from octopal.infrastructure.store.base import Store
    from octopal.tools.registry import ToolSpec


@dataclass
class MCPServerConfig:
    id: str
    name: str
    command: str | None = None
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    url: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    tools: list[str] = field(default_factory=list)
    transport: Literal["auto", "sse", "streamable-http", "stdio"] | None = None
    last_error: str | None = None


logger = structlog.get_logger(__name__)
_MCP_PERMANENT_ERROR_THRESHOLD = 2
_MCP_PERMANENT_ERROR_OPEN_SECONDS = 300.0
_MCP_TRANSIENT_ERROR_THRESHOLD = 5
_MCP_TRANSIENT_ERROR_OPEN_SECONDS = 60.0
_MCP_DEFAULT_TIMEOUT_SECONDS = 120.0
_MCP_SLOW_TIMEOUT_SECONDS = 300.0
_MCP_RECONNECT_BASE_SECONDS = 2.0
_MCP_RECONNECT_MAX_SECONDS = 60.0
_MCP_TASK_MAX_TTL_MS = 600_000
_MCP_TASK_RECOVERY_BUDGET_SECONDS = 300.0
_MCP_RETRYABLE_CLASSIFICATIONS = {
    "timeout",
    "rate_limited",
    "upstream_5xx",
    "unknown_error",
}
_MCP_SLOW_TOOL_HINTS = (
    "search",
    "fetch",
    "crawl",
    "thread",
    "inbox",
    "mail",
    "list_",
    "query",
)


def _extract_mcp_server_configs(config_data: Any) -> Any:
    if not isinstance(config_data, dict):
        return None
    if isinstance(config_data.get("servers"), dict):
        return config_data["servers"]
    if isinstance(config_data.get("mcpServers"), dict):
        return config_data["mcpServers"]
    return config_data


class MCPManager:
    def __init__(self, workspace_dir: Path, *, store: Store | None = None):
        self.workspace_dir = workspace_dir
        self.store = store
        self.sessions: dict[str, ClientSession] = {}
        # Stores the background task that keeps the session alive
        self._tasks: dict[str, asyncio.Task] = {}
        # Communication queues for disconnect signals
        self._stop_events: dict[str, asyncio.Event] = {}
        self._tools: dict[str, list[ToolSpec]] = {}
        self._tool_schemas: dict[tuple[str, str], dict[str, Any]] = {}
        self._tool_task_support: dict[tuple[str, str], str] = {}
        self._task_protocols: dict[str, MCPTaskProtocol | None] = {}
        self._task_recovery_tasks: dict[str, asyncio.Task[None]] = {}
        self._task_metrics: dict[str, int] = {
            "created": 0,
            "recovered": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0,
            "input_required": 0,
        }
        self._server_configs: dict[str, MCPServerConfig] = {}
        self._tool_failure_state: dict[tuple[str, str], dict[str, Any]] = {}
        self._reconnect_tasks: dict[str, asyncio.Task] = {}
        self._reconnect_attempts: dict[str, int] = {}
        self._connection_locks: dict[str, asyncio.Lock] = {}
        self._server_states: dict[
            str,
            Literal[
                "disconnected",
                "connecting",
                "ready",
                "degraded",
                "reconnect_wait",
                "stopping",
            ],
        ] = {}
        self._manual_disconnects: set[str] = set()
        self._shutdown_requested = False
        self.config_path = workspace_dir / "mcp_servers.json"
        self.legacy_config_path = workspace_dir / "config" / "mcp.json"
        self.root_config_path = workspace_dir / "mcp.json"
        self.claude_config_path = workspace_dir / ".mcp.json"
        self._configs_loaded = False

    def _config_paths(self) -> list[Path]:
        # Read broad compatibility paths first, then let canonical Octopal
        # config files override duplicates from imported MCP client configs.
        return [
            self.claude_config_path,
            self.root_config_path,
            self.legacy_config_path,
            self.config_path,
        ]

    def _load_configs_from_disk(self) -> dict[str, MCPServerConfig]:
        if self._configs_loaded:
            return dict(self._server_configs)
        if not any(path.exists() for path in self._config_paths()):
            self._configs_loaded = True
            return dict(self._server_configs)

        loaded: dict[str, MCPServerConfig] = {}
        for config_path in self._config_paths():
            if not config_path.exists():
                continue
            config_text = config_path.read_text(encoding="utf-8").strip()
            if not config_text:
                logger.warning("Skipping empty MCP config file", path=str(config_path))
                continue
            try:
                config_data = json.loads(config_text)
            except json.JSONDecodeError:
                logger.warning(
                    "Skipping invalid MCP config file", path=str(config_path), exc_info=True
                )
                continue
            server_configs = _extract_mcp_server_configs(config_data)
            if not isinstance(server_configs, dict):
                continue
            for server_id, cfg in server_configs.items():
                if not isinstance(cfg, dict):
                    continue
                server_id = str(server_id)
                existing = loaded.get(server_id) or self._server_configs.get(server_id)
                loaded[server_id] = MCPServerConfig(
                    id=server_id,
                    name=str(cfg.get("name") or server_id),
                    command=cfg.get("command"),
                    args=cfg.get("args", []),
                    env=cfg.get("env", {}),
                    url=cfg.get("url"),
                    headers=cfg.get("headers", {}),
                    tools=_normalize_configured_tool_names(cfg.get("tools", [])),
                    transport=_normalize_transport(cfg.get("transport") or cfg.get("type")),
                    last_error=existing.last_error if existing else None,
                )
        self._server_configs.update(loaded)
        self._configs_loaded = True
        return dict(self._server_configs)

    async def load_and_connect_all(self) -> None:
        if not any(path.exists() for path in self._config_paths()):
            return
        try:
            for server_id, mcp_cfg in self._load_configs_from_disk().items():
                try:
                    await self.connect_server(mcp_cfg)
                except Exception:
                    logger.exception(
                        "Failed to connect to MCP server on startup", server_id=server_id
                    )
        except Exception:
            logger.exception("Failed to load MCP config")

    async def ensure_configured_servers_connected(
        self,
        server_ids: list[str] | None = None,
    ) -> dict[str, str]:
        """Reconnect configured MCP servers that should already be available."""
        try:
            configs = self._load_configs_from_disk()
        except Exception:
            logger.exception("Failed to load MCP config for ensure_configured_servers_connected")
            return {}

        requested_ids = None
        if server_ids is not None:
            requested_ids = [
                str(server_id).strip() for server_id in server_ids if str(server_id).strip()
            ]
        target_ids = list(configs.keys()) if requested_ids is None else requested_ids
        results: dict[str, str] = {}
        for server_id in target_ids:
            cfg = configs.get(server_id)
            if cfg is None:
                results[server_id] = "unknown"
                continue
            if server_id in self.sessions:
                results[server_id] = "connected"
                continue
            try:
                await self.connect_server(cfg)
                results[server_id] = "connected"
            except Exception as exc:
                cfg.last_error = str(exc)
                results[server_id] = "error"
                logger.warning(
                    "Failed to ensure configured MCP server is connected",
                    server_id=server_id,
                    error=str(exc),
                )
        return results

    def resolve_configured_server_ids_for_tools(self, tool_names: list[str]) -> list[str]:
        try:
            configs = self._load_configs_from_disk()
        except Exception:
            logger.exception("Failed to load MCP config while resolving requested tool servers")
            return []

        resolved: list[str] = []
        seen: set[str] = set()
        for tool_name in _normalize_configured_tool_names(tool_names):
            if not tool_name.startswith("mcp_"):
                continue
            server_id = _resolve_configured_server_id_for_tool_name(tool_name, configs)
            if not server_id or server_id in seen:
                continue
            seen.add(server_id)
            resolved.append(server_id)
        return resolved

    async def connect_server(self, config: MCPServerConfig) -> list[ToolSpec]:
        lock = self._connection_locks.setdefault(config.id, asyncio.Lock())
        async with lock:
            return await self._connect_server(config)

    async def _connect_server(self, config: MCPServerConfig) -> list[ToolSpec]:
        self._shutdown_requested = False
        self._server_configs[config.id] = config
        self._manual_disconnects.discard(config.id)
        current_task = asyncio.current_task()
        reconnect_task = self._reconnect_tasks.get(config.id)
        if reconnect_task and reconnect_task is not current_task:
            self._reconnect_tasks.pop(config.id, None)
            if not reconnect_task.done():
                reconnect_task.cancel()
                with suppress(asyncio.CancelledError):
                    await reconnect_task
        if config.id in self.sessions:
            logger.info("MCP server already connected", server_id=config.id)
            return self._tools.get(config.id, [])

        self._server_states[config.id] = "connecting"

        # Create an event to signal connection readiness and an event for stopping
        ready_event = asyncio.Event()
        stop_event = asyncio.Event()
        self._stop_events[config.id] = stop_event

        # Start background task to manage the lifecycle
        task = asyncio.create_task(self._run_server_lifecycle(config, ready_event, stop_event))
        self._tasks[config.id] = task

        # Wait for the session to be initialized or task to fail
        ready_waiter = asyncio.create_task(ready_event.wait())
        try:
            # Monitor both the ready event and the lifecycle task itself.
            done, _pending = await asyncio.wait(
                [ready_waiter, task],
                return_when=asyncio.FIRST_COMPLETED,
                timeout=45.0,
            )

            # Check if we timed out
            if not done:
                config.last_error = "Connection timed out after 45s"
                raise RuntimeError(f"Connection to MCP server '{config.id}' timed out after 45s.")

            if ready_event.is_set():
                # Success!
                config.last_error = None
                self._reconnect_attempts.pop(config.id, None)
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
            await self.disconnect_server(config.id, intentional=False)
            if isinstance(e, RuntimeError) and "timed out" in str(e):
                raise
            raise RuntimeError(f"MCP Connection Error ({config.id}): {e}") from e
        finally:
            if not ready_waiter.done():
                ready_waiter.cancel()
            with suppress(asyncio.CancelledError):
                await ready_waiter

    async def _run_server_lifecycle(
        self, config: MCPServerConfig, ready_event: asyncio.Event, stop_event: asyncio.Event
    ) -> None:
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
                logger.info(
                    "Establishing MCP streamable-http transport",
                    server_id=config.id,
                    url=config.url,
                )
                read_stream, write_stream, _get_session_id = await exit_stack.enter_async_context(
                    streamablehttp_client(
                        url=config.url or "",
                        headers=config.headers or None,
                        timeout=timedelta(seconds=30),
                        sse_read_timeout=timedelta(seconds=300),
                    )
                )
            elif selected_transport == "stdio":
                logger.info(
                    "Establishing MCP stdio transport", server_id=config.id, command=config.command
                )
                params = StdioServerParameters(
                    command=config.command,
                    args=config.args,
                    env={**config.env, "PATH": os.environ.get("PATH", "")} if config.env else None,
                )
                read_stream, write_stream = await exit_stack.enter_async_context(
                    stdio_client(params)
                )
            else:
                raise ValueError(
                    f"Unsupported MCP transport '{selected_transport}' for server {config.id}."
                )

            logger.info("Initializing MCP session", server_id=config.id)
            session = await exit_stack.enter_async_context(ClientSession(read_stream, write_stream))

            initialize_result = await session.initialize()
            self.sessions[config.id] = session

            # Fetch tools
            logger.info("Fetching tools from MCP server", server_id=config.id)
            mcp_tools_list = await session.list_tools()
            task_protocol = await self._negotiate_task_protocol(
                session,
                initialize_result,
            )
            if task_protocol == "extension" and selected_transport == "streamable-http":
                logger.warning(
                    "MCP Tasks extension disabled because transport routing headers are unavailable",
                    server_id=config.id,
                )
                task_protocol = None
            self._task_protocols[config.id] = task_protocol

            specs = []
            from octopal.tools.registry import ToolSpec

            for tool in mcp_tools_list.tools:
                # Normalize tool name into the lowercase worker/runtime form so
                # legacy mixed-case config ids still match worker templates.
                safe_id = config.id.replace("-", "_").lower()
                safe_tool_name = tool.name.replace("-", "_").lower()
                mcp_tool_name = f"mcp_{safe_id}_{safe_tool_name}"
                full_schema = tool.inputSchema if isinstance(tool.inputSchema, dict) else {}
                self._tool_schemas[(config.id, mcp_tool_name)] = full_schema
                task_support = str(
                    getattr(getattr(tool, "execution", None), "taskSupport", None) or "forbidden"
                )
                self._tool_task_support[(config.id, tool.name)] = task_support

                spec = ToolSpec(
                    name=mcp_tool_name,
                    description=f"[MCP Tool from {config.name}] {tool.description}. Call this tool directly by using the name '{mcp_tool_name}' in your tool call block.",
                    parameters=_compact_mcp_input_schema(full_schema),
                    permission="mcp_exec",
                    handler=self._generate_handler(config.id, tool.name),
                    is_async=True,
                    server_id=config.id,
                    remote_tool_name=tool.name,
                )
                specs.append(spec)

            self._tools[config.id] = specs
            logger.info(
                "MCP server connected and tools ready",
                server_id=config.id,
                tool_count=len(specs),
                task_protocol=self._task_protocols.get(config.id) or "none",
            )

            await self._schedule_task_recovery(config.id)

            # Signal that we are ready
            ready_event.set()
            self._server_states[config.id] = "ready"

            # Keep alive until signaled to stop
            await stop_event.wait()
            logger.info("Shutting down MCP server session (signaled)", server_id=config.id)

        except Exception as e:
            config.last_error = str(e)
            self._server_states[config.id] = "degraded"
            hint = _connection_hint(e)
            logger.exception(
                "MCP server lifecycle error",
                server_id=config.id,
                transport=config.transport or "auto",
                hint=hint,
            )
            if not ready_event.is_set():
                # Task failed before becoming ready - signal the waiter with an error if possible
                # But here we just let the waiter catch the fact that ready_event was never set.
                pass
        finally:
            # Clean up
            self.sessions.pop(config.id, None)
            self._tools.pop(config.id, None)
            for schema_key in [key for key in self._tool_schemas if key[0] == config.id]:
                self._tool_schemas.pop(schema_key, None)
            for support_key in [key for key in self._tool_task_support if key[0] == config.id]:
                self._tool_task_support.pop(support_key, None)
            self._task_protocols.pop(config.id, None)
            self._tasks.pop(config.id, None)
            self._stop_events.pop(config.id, None)

            # Closing the stack will close the context managers (stdio/sse clients)
            # This happens in the same task that created them, which anyio requires.
            await exit_stack.aclose()
            logger.info("MCP server resources released", server_id=config.id)
            if (
                not self._shutdown_requested
                and config.id not in self._manual_disconnects
                and config.id in self._server_configs
                and config.id not in self.sessions
            ):
                self._schedule_reconnect(config.id)
            elif self._server_states.get(config.id) != "stopping":
                self._server_states[config.id] = "disconnected"

    async def call_tool(
        self,
        server_id: str,
        tool_name: str,
        args: dict[str, Any],
        *,
        allow_name_fallback: bool = False,
        task_context: MCPTaskContext | None = None,
    ) -> Any:
        session = self.sessions.get(server_id)
        if not session:
            raise RuntimeError(f"MCP session '{server_id}' is not active.")

        tool_candidates = [tool_name]
        alt_name = _alternate_tool_name(tool_name) if allow_name_fallback else None
        if alt_name:
            tool_candidates.append(alt_name)

        timeout_seconds = _mcp_timeout_seconds(tool_name, args)
        last_exc: Exception | None = None
        for index, candidate_name in enumerate(tool_candidates):
            state_key = (server_id, candidate_name)
            now = time.monotonic()
            state = self._tool_failure_state.get(state_key)
            if state and float(state.get("open_until", 0.0)) > now:
                remaining = max(1, int(float(state["open_until"]) - now))
                last_class = str(state.get("classification", "unknown"))
                raise MCPToolCallError(
                    classification=last_class,
                    hint="Previous failures keep this MCP tool circuit open.",
                    retryable=_is_retryable_mcp_classification(last_class),
                    server_id=server_id,
                    tool_name=candidate_name,
                    message=(
                        f"MCP tool '{candidate_name}' on '{server_id}' is temporarily paused for {remaining}s "
                        f"after repeated '{last_class}' failures. Try a fallback path or a different tool."
                    ),
                    details={"cooldown_seconds": remaining, "circuit_open": True},
                )

            try:
                result = await asyncio.wait_for(
                    self._call_tool_once(
                        session,
                        server_id=server_id,
                        tool_name=candidate_name,
                        args=args,
                        timeout_seconds=timeout_seconds,
                        task_context=task_context or MCPTaskContext(),
                    ),
                    timeout=timeout_seconds + 0.25,
                )
                self._tool_failure_state.pop(state_key, None)
                if index > 0:
                    logger.warning(
                        "MCP tool name fallback succeeded",
                        server_id=server_id,
                        requested_tool=tool_name,
                        resolved_tool=candidate_name,
                    )
                return result
            except TimeoutError:
                last_exc = RuntimeError(
                    f"MCP call timed out after {int(timeout_seconds)}s for '{candidate_name}' on '{server_id}'."
                )
                exc_to_classify: Exception = last_exc
            except MCPToolCallError:
                raise
            except Exception as exc:
                last_exc = exc
                exc_to_classify = exc
                if index == 0 and len(tool_candidates) > 1 and _is_tool_not_found_error(exc):
                    logger.warning(
                        "Retrying MCP call with alternate tool name",
                        server_id=server_id,
                        requested_tool=tool_name,
                        alternate_tool=tool_candidates[1],
                    )
                    continue

            error_info = _classify_mcp_call_error(exc_to_classify)
            entry = self._tool_failure_state.get(
                state_key,
                {"count": 0, "open_until": 0.0, "classification": error_info["classification"]},
            )
            entry["count"] = int(entry.get("count", 0)) + 1
            entry["classification"] = error_info["classification"]
            entry["last_error"] = str(exc_to_classify)

            if error_info["retryable"]:
                if entry["count"] >= _MCP_TRANSIENT_ERROR_THRESHOLD:
                    entry["open_until"] = now + _MCP_TRANSIENT_ERROR_OPEN_SECONDS
            else:
                if entry["count"] >= _MCP_PERMANENT_ERROR_THRESHOLD:
                    entry["open_until"] = now + _MCP_PERMANENT_ERROR_OPEN_SECONDS

            self._tool_failure_state[state_key] = entry

            if float(entry.get("open_until", 0.0)) > now:
                logger.warning(
                    "Opened MCP tool circuit after repeated failures",
                    server_id=server_id,
                    tool=tool_name,
                    classification=error_info["classification"],
                    cooldown_seconds=int(float(entry["open_until"]) - now),
                    failure_count=entry["count"],
                )

            raise MCPToolCallError(
                classification=str(error_info["classification"]),
                hint=str(error_info["hint"]),
                retryable=bool(error_info["retryable"]),
                server_id=server_id,
                tool_name=candidate_name,
            ) from exc_to_classify

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"MCP call failed for '{tool_name}' on '{server_id}'")

    async def _negotiate_task_protocol(
        self,
        session: ClientSession,
        initialize_result: Any,
    ) -> MCPTaskProtocol | None:
        if self.store is None:
            return None
        capabilities = getattr(initialize_result, "capabilities", None)
        protocol_version = str(getattr(initialize_result, "protocolVersion", "") or "")
        if protocol_version >= "2026-06-30":
            if extension_declared(capabilities):
                return "extension"
            try:
                discover = await session.send_request(
                    RawMCPRequest(
                        method="server/discover",
                        params={"_meta": client_capability_meta()},
                    ),
                    RawMCPResult,
                    request_read_timeout_seconds=timedelta(seconds=5),
                )
                discover_payload = discover.model_dump(
                    mode="json", by_alias=True, exclude_none=True
                )
                if extension_declared(discover_payload.get("capabilities")):
                    return "extension"
            except Exception:
                logger.debug(
                    "MCP server did not negotiate the Tasks extension",
                    protocol_version=protocol_version,
                    exc_info=True,
                )

        if legacy_tasks_declared(capabilities):
            return "legacy"
        return None

    async def _call_tool_once(
        self,
        session: ClientSession,
        *,
        server_id: str,
        tool_name: str,
        args: dict[str, Any],
        timeout_seconds: float,
        task_context: MCPTaskContext,
    ) -> Any:
        protocol = self._task_protocols.get(server_id)
        if protocol == "extension":
            result = await session.send_request(
                RawMCPRequest(
                    method="tools/call",
                    params={
                        "name": tool_name,
                        "arguments": args,
                        "_meta": client_capability_meta(),
                    },
                ),
                RawMCPResult,
            )
            payload = result.model_dump(mode="json", by_alias=True, exclude_none=True)
            if payload.get("resultType") != "task":
                return mcp_types.CallToolResult.model_validate(payload)
            state = parse_task_state(payload, protocol="extension")
            record = await self._persist_task_state(
                state,
                server_id=server_id,
                tool_name=tool_name,
                protocol="extension",
                task_context=task_context,
            )
            return await self._wait_for_task(
                session,
                record,
                timeout_seconds=timeout_seconds,
            )

        task_support = self._tool_task_support.get((server_id, tool_name), "forbidden")
        if protocol == "legacy" and task_support in {"optional", "required"}:
            result = await session.experimental.call_tool_as_task(
                tool_name,
                args,
                ttl=max(1000, min(int(timeout_seconds * 1000), _MCP_TASK_MAX_TTL_MS)),
            )
            state = parse_task_state(result, protocol="legacy")
            record = await self._persist_task_state(
                state,
                server_id=server_id,
                tool_name=tool_name,
                protocol="legacy",
                task_context=task_context,
            )
            return await self._wait_for_task(
                session,
                record,
                timeout_seconds=timeout_seconds,
            )

        return await session.call_tool(tool_name, arguments=args)

    async def _wait_for_task(
        self,
        session: ClientSession,
        record: MCPTaskRecord,
        *,
        timeout_seconds: float,
    ) -> mcp_types.CallToolResult:
        deadline = time.monotonic() + max(0.05, timeout_seconds)
        current = record
        while True:
            if current.remote_status in MCP_TASK_TERMINAL_STATUSES:
                return await self._terminal_task_result(session, current)
            if current.remote_status == "input_required":
                return task_status_result(current)
            if task_expired(current):
                expired = current.model_copy(
                    update={
                        "runtime_status": "failed",
                        "status_message": "MCP task TTL elapsed before completion.",
                        "error": {"classification": "task_ttl_expired"},
                        "updated_at": utc_now(),
                    }
                )
                effective = await self._persist_task_record(expired, previous=current)
                if effective.remote_status in MCP_TASK_TERMINAL_STATUSES:
                    return await self._terminal_task_result(session, effective)
                if effective.runtime_status != "failed":
                    current = effective
                    continue
                raise MCPToolCallError(
                    classification="task_ttl_expired",
                    hint="The remote MCP task exceeded its advertised TTL.",
                    retryable=False,
                    server_id=current.server_id,
                    tool_name=current.tool_name,
                    details={"task_id": current.id},
                )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._ensure_task_recovery(current)
                return task_status_result(current)
            poll_seconds = task_poll_seconds(current)
            if poll_seconds >= remaining:
                await asyncio.sleep(remaining)
                self._ensure_task_recovery(current)
                return task_status_result(current)
            await asyncio.sleep(poll_seconds)
            current = await self._refresh_task(session, current)

    async def _refresh_task(
        self,
        session: ClientSession,
        record: MCPTaskRecord,
    ) -> MCPTaskRecord:
        if record.protocol == "extension":
            result = await session.send_request(
                RawMCPRequest(
                    method="tasks/get",
                    params={
                        "taskId": record.task_id,
                        "_meta": client_capability_meta(),
                    },
                ),
                RawMCPResult,
            )
            state = parse_task_state(result, protocol="extension")
        else:
            state = parse_task_state(
                await session.experimental.get_task(record.task_id),
                protocol="legacy",
            )
        context = MCPTaskContext(
            correlation_id=record.correlation_id,
            trace_id=record.trace_id,
            span_id=record.span_id,
            worker_run_id=record.worker_run_id,
            chat_id=record.chat_id,
            chat_turn_id=record.chat_turn_id,
            plan_run_id=record.plan_run_id,
            plan_step_id=record.plan_step_id,
        )
        return await self._persist_task_state(
            state,
            server_id=record.server_id,
            tool_name=record.tool_name,
            protocol=record.protocol,
            task_context=context,
            previous=record,
        )

    async def _terminal_task_result(
        self,
        session: ClientSession | None,
        record: MCPTaskRecord,
    ) -> mcp_types.CallToolResult:
        if record.remote_status == "completed":
            if isinstance(record.result, dict):
                return mcp_types.CallToolResult.model_validate(record.result)
            if record.protocol == "extension":
                if not isinstance(record.result, dict):
                    raise MCPToolCallError(
                        classification="schema_mismatch",
                        hint="Completed MCP task omitted its tools/call result.",
                        retryable=False,
                        server_id=record.server_id,
                        tool_name=record.tool_name,
                        details={"task_id": record.id},
                    )
                return mcp_types.CallToolResult.model_validate(record.result)
            if session is None:
                raise RuntimeError(f"MCP session '{record.server_id}' is not active.")
            result = await session.experimental.get_task_result(
                record.task_id,
                mcp_types.CallToolResult,
            )
            payload = result.model_dump(mode="json", by_alias=True, exclude_none=True)
            await self._persist_task_record(
                record.model_copy(update={"result": payload, "updated_at": utc_now()}),
                previous=record,
            )
            return result

        classification = "task_cancelled" if record.remote_status == "cancelled" else "task_failed"
        raise MCPToolCallError(
            classification=classification,
            hint=record.status_message or f"Remote MCP task ended as {record.remote_status}.",
            retryable=False,
            server_id=record.server_id,
            tool_name=record.tool_name,
            details={"task_id": record.id, "remote_error": record.error or {}},
        )

    async def _persist_task_state(
        self,
        state: MCPTaskState,
        *,
        server_id: str,
        tool_name: str,
        protocol: MCPTaskProtocol,
        task_context: MCPTaskContext,
        previous: MCPTaskRecord | None = None,
    ) -> MCPTaskRecord:
        config = self._server_configs.get(server_id)
        if config is None:
            raise RuntimeError(f"MCP server '{server_id}' is not configured.")
        auth_context_id = _mcp_auth_context_id(config)
        provisional = build_task_record(
            state=state,
            server_id=server_id,
            tool_name=tool_name,
            protocol=protocol,
            auth_context_id=auth_context_id,
            context=task_context,
            previous=previous,
        )
        if previous is None and self.store is not None:
            previous = await asyncio.to_thread(self.store.get_mcp_task, provisional.id)
        if previous is not None and (
            state.created_at != previous.remote_created_at
            or state.updated_at < previous.remote_updated_at
            or (
                previous.remote_status in MCP_TASK_TERMINAL_STATUSES
                and state.status != previous.remote_status
            )
        ):
            logger.warning(
                "Ignoring stale or invalid MCP task transition",
                task_id=previous.id,
                server_id=server_id,
                previous_status=previous.remote_status,
                received_status=state.status,
            )
            return previous
        if previous is not None:
            provisional = build_task_record(
                state=state,
                server_id=server_id,
                tool_name=tool_name,
                protocol=protocol,
                auth_context_id=auth_context_id,
                context=task_context,
                previous=previous,
            )
        return await self._persist_task_record(provisional, previous=previous)

    async def _persist_task_record(
        self,
        record: MCPTaskRecord,
        *,
        previous: MCPTaskRecord | None,
    ) -> MCPTaskRecord:
        effective = record
        persisted_previous = previous
        applied = True
        if self.store is not None:
            effective, persisted_previous, applied = await asyncio.to_thread(
                self.store.upsert_mcp_task, record
            )
        if not applied:
            return effective

        created = persisted_previous is None
        status_changed = (
            persisted_previous is None
            or persisted_previous.remote_status != effective.remote_status
            or persisted_previous.runtime_status != effective.runtime_status
        )
        if not status_changed:
            return effective
        if created:
            self._task_metrics["created"] += 1
        metric_status = (
            "failed" if effective.runtime_status == "failed" else effective.remote_status
        )
        if metric_status in self._task_metrics:
            self._task_metrics[metric_status] += 1
        event_type = "mcp_task_created" if created else "mcp_task_status_changed"
        await self._append_task_audit(
            event_type,
            effective,
            data={
                "previous_status": (
                    persisted_previous.remote_status if persisted_previous else None
                ),
                "remote_status": effective.remote_status,
                "runtime_status": effective.runtime_status,
                "protocol": effective.protocol,
            },
        )
        return effective

    async def _append_task_audit(
        self,
        event_type: str,
        record: MCPTaskRecord,
        *,
        data: dict[str, Any] | None = None,
        level: Literal["debug", "info", "warning", "error", "critical"] = "info",
    ) -> None:
        if self.store is None:
            return
        payload = {
            "task_id": record.id,
            "task_ref": task_ref(record.task_id),
            "server_id": record.server_id,
            "tool_name": record.tool_name,
            "worker_run_id": record.worker_run_id,
            "chat_id": record.chat_id,
            "chat_turn_id": record.chat_turn_id,
            "plan_run_id": record.plan_run_id,
            "plan_step_id": record.plan_step_id,
            **(data or {}),
        }
        await asyncio.to_thread(
            self.store.append_audit,
            AuditEvent(
                id=str(uuid.uuid4()),
                ts=utc_now(),
                correlation_id=record.correlation_id or record.worker_run_id,
                level=level,
                event_type=event_type,
                data=payload,
            ),
        )

    async def _schedule_task_recovery(self, server_id: str) -> None:
        if self.store is None:
            return
        config = self._server_configs.get(server_id)
        if config is None:
            return
        auth_context_id = _mcp_auth_context_id(config)
        records = await asyncio.to_thread(
            self.store.list_recoverable_mcp_tasks,
            server_id=server_id,
            auth_context_id=auth_context_id,
        )
        for record in records:
            self._ensure_task_recovery(record)

    def _ensure_task_recovery(self, record: MCPTaskRecord) -> None:
        if (
            self.store is None
            or record.remote_status != "working"
            or record.runtime_status == "failed"
            or task_expired(record)
            or self._shutdown_requested
            or record.server_id in self._manual_disconnects
            or record.server_id not in self.sessions
        ):
            return
        existing = self._task_recovery_tasks.get(record.id)
        if existing is not None and not existing.done():
            return
        self._task_recovery_tasks[record.id] = asyncio.create_task(self._recover_task(record))

    async def _recover_task(self, record: MCPTaskRecord) -> None:
        cancelled = False
        wait_completed = False
        latest: MCPTaskRecord | None = None
        try:
            session = self.sessions.get(record.server_id)
            if session is None:
                return
            await self._append_task_audit("mcp_task_recovery_started", record)
            await self._wait_for_task(
                session,
                record,
                timeout_seconds=_MCP_TASK_RECOVERY_BUDGET_SECONDS,
            )
            wait_completed = True
        except asyncio.CancelledError:
            cancelled = True
            raise
        except Exception as exc:
            logger.warning(
                "MCP task recovery stopped",
                task_id=record.id,
                server_id=record.server_id,
                error_type=type(exc).__name__,
            )
        finally:
            if self.store is not None:
                latest = await asyncio.to_thread(self.store.get_mcp_task, record.id)
                if latest is not None and (
                    latest.remote_status in MCP_TASK_TERMINAL_STATUSES
                    or latest.runtime_status == "failed"
                ):
                    self._task_metrics["recovered"] += 1
            if self._task_recovery_tasks.get(record.id) is asyncio.current_task():
                self._task_recovery_tasks.pop(record.id, None)
            if not cancelled and wait_completed and latest is not None:
                self._ensure_task_recovery(latest)

    async def get_task(
        self,
        task_record_id: str,
        *,
        task_context: MCPTaskContext | None = None,
    ) -> MCPTaskRecord:
        record = await self._load_bound_task(task_record_id, task_context=task_context)
        if record.remote_status in MCP_TASK_TERMINAL_STATUSES:
            return record
        session = self.sessions.get(record.server_id)
        if session is None:
            raise RuntimeError(f"MCP session '{record.server_id}' is not active.")
        refreshed = await self._refresh_task(session, record)
        self._ensure_task_recovery(refreshed)
        return refreshed

    async def resume_task(
        self,
        task_record_id: str,
        *,
        task_context: MCPTaskContext | None = None,
    ) -> mcp_types.CallToolResult:
        record = await self._load_bound_task(task_record_id, task_context=task_context)
        if record.remote_status in MCP_TASK_TERMINAL_STATUSES and isinstance(record.result, dict):
            return await self._terminal_task_result(None, record)
        if record.remote_status in MCP_TASK_TERMINAL_STATUSES and record.protocol == "extension":
            return await self._terminal_task_result(None, record)
        session = self.sessions.get(record.server_id)
        if session is None:
            raise RuntimeError(f"MCP session '{record.server_id}' is not active.")
        if record.remote_status not in MCP_TASK_TERMINAL_STATUSES:
            record = await self._refresh_task(session, record)
        if record.remote_status in MCP_TASK_TERMINAL_STATUSES:
            return await self._terminal_task_result(session, record)
        self._ensure_task_recovery(record)
        return task_status_result(record)

    async def update_task(
        self,
        task_record_id: str,
        input_responses: dict[str, Any],
        *,
        task_context: MCPTaskContext | None = None,
    ) -> MCPTaskRecord:
        record = await self._load_bound_task(task_record_id, task_context=task_context)
        if record.protocol != "extension":
            raise MCPToolCallError(
                classification="unsupported_operation",
                hint="Legacy MCP tasks do not support tasks/update.",
                retryable=False,
                server_id=record.server_id,
                tool_name=record.tool_name,
                details={"task_id": record.id},
            )
        response_keys = {str(key) for key in input_responses}
        outstanding_keys = set(record.input_requests)
        if not response_keys or not response_keys.issubset(outstanding_keys):
            raise MCPToolCallError(
                classification="invalid_task_input",
                hint="Task input responses must match currently outstanding request keys.",
                retryable=False,
                server_id=record.server_id,
                tool_name=record.tool_name,
                details={"task_id": record.id},
            )
        session = self.sessions.get(record.server_id)
        if session is None:
            raise RuntimeError(f"MCP session '{record.server_id}' is not active.")
        await session.send_request(
            RawMCPRequest(
                method="tasks/update",
                params={
                    "taskId": record.task_id,
                    "inputResponses": input_responses,
                    "_meta": client_capability_meta(),
                },
            ),
            RawMCPResult,
        )
        acknowledged = record.model_copy(
            update={
                "input_requests": {
                    key: value
                    for key, value in record.input_requests.items()
                    if key not in response_keys
                },
                "responded_input_keys": sorted({*record.responded_input_keys, *response_keys}),
                "updated_at": utc_now(),
            }
        )
        acknowledged = await self._persist_task_record(acknowledged, previous=record)
        await self._append_task_audit(
            "mcp_task_input_submitted",
            acknowledged,
            data={"response_keys": sorted(response_keys)},
        )
        if acknowledged.remote_status in MCP_TASK_TERMINAL_STATUSES:
            return acknowledged
        refreshed = await self._refresh_task(session, acknowledged)
        self._ensure_task_recovery(refreshed)
        return refreshed

    async def cancel_task(
        self,
        task_record_id: str,
        *,
        task_context: MCPTaskContext | None = None,
    ) -> MCPTaskRecord:
        record = await self._load_bound_task(task_record_id, task_context=task_context)
        session = self.sessions.get(record.server_id)
        if session is None:
            raise RuntimeError(f"MCP session '{record.server_id}' is not active.")
        if record.protocol == "extension":
            await session.send_request(
                RawMCPRequest(
                    method="tasks/cancel",
                    params={
                        "taskId": record.task_id,
                        "_meta": client_capability_meta(),
                    },
                ),
                RawMCPResult,
            )
            refreshed = await self._refresh_task(session, record)
        else:
            state = parse_task_state(
                await session.experimental.cancel_task(record.task_id),
                protocol="legacy",
            )
            refreshed = await self._persist_task_state(
                state,
                server_id=record.server_id,
                tool_name=record.tool_name,
                protocol=record.protocol,
                task_context=task_context or MCPTaskContext(),
                previous=record,
            )
        await self._append_task_audit("mcp_task_cancel_requested", refreshed)
        self._ensure_task_recovery(refreshed)
        return refreshed

    async def _load_bound_task(
        self,
        task_record_id: str,
        *,
        task_context: MCPTaskContext | None,
    ) -> MCPTaskRecord:
        if self.store is None:
            raise RuntimeError("MCP task persistence is unavailable.")
        record = await asyncio.to_thread(self.store.get_mcp_task, task_record_id)
        if record is None:
            raise KeyError(f"Unknown MCP task: {task_record_id}")
        config = self._server_configs.get(record.server_id)
        if config is None or _mcp_auth_context_id(config) != record.auth_context_id:
            raise PermissionError("MCP task authorization context no longer matches.")
        if (
            task_context is not None
            and task_context.worker_run_id
            and record.worker_run_id
            and task_context.worker_run_id != record.worker_run_id
        ):
            raise PermissionError("MCP task is bound to another worker run.")
        if (
            task_context is not None
            and task_context.chat_id is not None
            and record.chat_id is not None
            and task_context.chat_id != record.chat_id
        ):
            raise PermissionError("MCP task is bound to another chat.")
        return record

    def _generate_handler(
        self, server_id: str, tool_name: str
    ) -> Callable[[dict[str, Any], dict[str, Any]], Awaitable[Any]]:
        async def handler(args: dict[str, Any], ctx: dict[str, Any]) -> Any:
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
                result = await self.call_tool(
                    server_id,
                    tool_name,
                    args,
                    task_context=_mcp_task_context_from_tool_ctx(ctx),
                )
                return [
                    c.model_dump() if hasattr(c, "model_dump") else str(c) for c in result.content
                ]
            except Exception as e:
                logger.exception("MCP tool call failed", server_id=server_id, tool=tool_name)
                return {
                    "ok": False,
                    "error": str(e),
                    "server_id": server_id,
                    "tool": tool_name,
                }

        return handler

    def _schedule_reconnect(self, server_id: str) -> None:
        if self._shutdown_requested or server_id in self._manual_disconnects:
            return
        existing = self._reconnect_tasks.get(server_id)
        if existing and not existing.done():
            return
        config = self._server_configs.get(server_id)
        if config is None:
            return
        attempt = int(self._reconnect_attempts.get(server_id, 0)) + 1
        self._reconnect_attempts[server_id] = attempt
        self._server_states[server_id] = "reconnect_wait"
        delay = min(_MCP_RECONNECT_MAX_SECONDS, _MCP_RECONNECT_BASE_SECONDS * (2 ** (attempt - 1)))

        async def _reconnect() -> None:
            retry = False
            try:
                logger.warning(
                    "Scheduling MCP reconnect",
                    server_id=server_id,
                    attempt=attempt,
                    delay_seconds=delay,
                )
                await asyncio.sleep(delay)
                if (
                    self._shutdown_requested
                    or server_id in self._manual_disconnects
                    or server_id in self.sessions
                ):
                    return
                self._server_states[server_id] = "connecting"
                await self.connect_server(config)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "MCP reconnect attempt failed",
                    server_id=server_id,
                    attempt=attempt,
                    exc_info=True,
                )
                retry = True
            finally:
                task = self._reconnect_tasks.get(server_id)
                if task is asyncio.current_task():
                    self._reconnect_tasks.pop(server_id, None)
                if retry:
                    self._schedule_reconnect(server_id)

        self._reconnect_tasks[server_id] = asyncio.create_task(_reconnect())

    async def disconnect_server(self, server_id: str, *, intentional: bool = True) -> None:
        if intentional:
            self._manual_disconnects.add(server_id)
            self._server_states[server_id] = "stopping"
            reconnect_task = self._reconnect_tasks.pop(server_id, None)
            if (
                reconnect_task
                and reconnect_task is not asyncio.current_task()
                and not reconnect_task.done()
            ):
                reconnect_task.cancel()
                with suppress(asyncio.CancelledError):
                    await reconnect_task
        event = self._stop_events.get(server_id)
        if event:
            event.set()

        task = self._tasks.get(server_id)
        if task:
            try:
                # Wait for cleanup to finish
                await asyncio.wait_for(task, timeout=5.0)
            except (TimeoutError, asyncio.CancelledError):
                if not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            logger.info("Disconnected MCP server", server_id=server_id)
        if intentional:
            self._server_states[server_id] = "disconnected"

    def get_all_tools(self) -> list[ToolSpec]:
        all_specs = []
        for specs in self._tools.values():
            all_specs.extend(specs)
        return all_specs

    def hydrate_tool_spec(self, spec: ToolSpec) -> ToolSpec:
        server_id = str(getattr(spec, "server_id", "") or "").strip()
        generated_name = str(getattr(spec, "name", "") or "").strip()
        if not server_id or not generated_name:
            return spec
        full_schema = self._tool_schemas.get((server_id, generated_name))
        if not isinstance(full_schema, dict) or not full_schema:
            return spec
        if spec.parameters == full_schema:
            return spec
        return replace(spec, parameters=full_schema)

    async def shutdown(self) -> None:
        self._shutdown_requested = True
        recovery_tasks = list(self._task_recovery_tasks.values())
        for task in recovery_tasks:
            if not task.done():
                task.cancel()
        self._task_recovery_tasks.clear()
        reconnect_tasks = list(self._reconnect_tasks.values())
        for task in reconnect_tasks:
            if not task.done():
                task.cancel()
        self._reconnect_tasks.clear()
        await asyncio.gather(*recovery_tasks, *reconnect_tasks, return_exceptions=True)
        # Trigger all stop events
        for server_id in list(self._stop_events.keys()):
            await self.disconnect_server(server_id)

    def get_server_statuses(self) -> dict[str, dict[str, Any]]:
        statuses = {}
        for server_id, config in self._server_configs.items():
            is_connected = server_id in self.sessions
            tools = self._tools.get(server_id, [])
            reconnect_task = self._reconnect_tasks.get(server_id)
            reconnecting = bool(reconnect_task and not reconnect_task.done())
            reconnect_attempts = int(self._reconnect_attempts.get(server_id, 0))
            if is_connected:
                status = "connected"
                reason = f"{len(tools)} tool(s) available"
            elif reconnecting:
                status = "reconnecting"
                reason = "Background reconnect scheduled"
            elif config.last_error:
                status = "error"
                reason = str(config.last_error)
            else:
                status = "configured"
                reason = "Configured but not connected"
            statuses[server_id] = {
                "name": config.name,
                "status": status,
                "configured": True,
                "connected": is_connected,
                "reconnecting": reconnecting,
                "reason": reason,
                "tool_count": len(tools),
                "error": config.last_error,
                "transport": config.transport or "auto",
                "reconnect_attempts": reconnect_attempts,
                "manual_disconnect": server_id in self._manual_disconnects,
                "lifecycle_state": self._server_states.get(
                    server_id,
                    "ready" if is_connected else "disconnected",
                ),
                "task_protocol": self._task_protocols.get(server_id) or "none",
                "task_recoveries_active_total": sum(
                    1 for task in self._task_recovery_tasks.values() if not task.done()
                ),
                "task_metrics": dict(self._task_metrics),
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


def _mcp_auth_context_id(config: MCPServerConfig) -> str:
    """Fingerprint the configured remote principal without persisting raw credentials."""
    inherited_credentials = {
        key: value
        for key, value in os.environ.items()
        if not config.env
        and any(marker in key.upper() for marker in ("TOKEN", "KEY", "SECRET", "AUTH", "PASSWORD"))
    }
    payload = {
        "server_id": config.id,
        "command": config.command,
        "args": config.args,
        "url": config.url,
        "transport": config.transport or "auto",
        "headers": config.headers,
        "env": config.env or inherited_credentials,
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _mcp_task_context_from_tool_ctx(ctx: dict[str, Any]) -> MCPTaskContext:
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
        correlation_id=str(ctx.get("correlation_id") or "").strip() or None,
        worker_run_id=str(ctx.get("worker_id") or "").strip() or None,
        chat_id=chat_id,
        chat_turn_id=chat_turn_id,
    )


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
        return (
            "Connection timed out: check network egress, DNS, firewall, or provider availability."
        )
    if "connection closed" in text:
        return "Remote side closed connection early: verify auth and protocol compatibility."
    return "Unknown MCP connection issue. Verify URL/transport/auth."


def _classify_mcp_call_error(error: Exception) -> dict[str, Any]:
    text = str(error).lower()
    if "invalid arguments for tool" in text:
        missing_fields = _extract_mcp_missing_argument_names(str(error))
        field_suffix = ""
        if missing_fields:
            field_suffix = f" Missing required fields: {', '.join(missing_fields)}."
        return {
            "classification": "invalid_arguments",
            "retryable": False,
            "hint": (
                "Remote MCP server rejected the tool arguments before execution." f"{field_suffix}"
            ),
        }
    if "invalid tools/call result" in text or "structuredcontent" in text:
        return {
            "classification": "schema_mismatch",
            "retryable": False,
            "hint": "Remote MCP response schema is incompatible (structuredContent is invalid).",
        }
    if "unknown tool" in text or "not found" in text:
        return {
            "classification": "tool_not_found",
            "retryable": False,
            "hint": "Tool name mismatch between Octopal and remote MCP server.",
        }
    if "timeout" in text or "timed out" in text:
        return {
            "classification": "timeout",
            "retryable": True,
            "hint": "Remote MCP call timed out; retry may succeed.",
        }
    if "429" in text or "rate limit" in text:
        return {
            "classification": "rate_limited",
            "retryable": True,
            "hint": "Remote MCP server is rate-limiting requests.",
        }
    if "500" in text or "502" in text or "503" in text:
        return {
            "classification": "upstream_5xx",
            "retryable": True,
            "hint": "Remote MCP server/upstream returned a temporary server error.",
        }
    return {
        "classification": "unknown_error",
        "retryable": True,
        "hint": "MCP call failed with an unclassified error.",
    }


def _extract_mcp_missing_argument_names(error_text: str) -> list[str]:
    matches = re.findall(r'"path"\s*:\s*\[\s*"([^"]+)"\s*\]', error_text, flags=re.IGNORECASE)
    unique: list[str] = []
    for match in matches:
        name = str(match).strip()
        if name and name not in unique:
            unique.append(name)
    return unique


def _is_retryable_mcp_classification(classification: str) -> bool:
    return classification in _MCP_RETRYABLE_CLASSIFICATIONS


def _alternate_tool_name(tool_name: str) -> str | None:
    if "_" in tool_name:
        alt = tool_name.replace("_", "-")
        return alt if alt != tool_name else None
    if "-" in tool_name:
        alt = tool_name.replace("-", "_")
        return alt if alt != tool_name else None
    return None


def _is_tool_not_found_error(error: Exception) -> bool:
    text = str(error).lower()
    return "unknown tool" in text or "not found" in text


def _mcp_timeout_seconds(tool_name: str, args: dict[str, Any]) -> float:
    if isinstance(args, dict):
        explicit = args.get("timeout_seconds")
        if explicit is not None:
            try:
                value = float(explicit)
                if value > 0:
                    return max(5.0, min(value, 600.0))
            except Exception:
                pass
    lowered = tool_name.lower()
    if any(hint in lowered for hint in _MCP_SLOW_TOOL_HINTS):
        return _MCP_SLOW_TIMEOUT_SECONDS
    return _MCP_DEFAULT_TIMEOUT_SECONDS


def _normalize_configured_tool_names(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item or "").strip().replace("-", "_").lower()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _resolve_configured_server_id_for_tool_name(
    tool_name: str,
    configs: dict[str, MCPServerConfig],
) -> str | None:
    normalized_tool_name = str(tool_name or "").strip().replace("-", "_").lower()
    if not normalized_tool_name.startswith("mcp_"):
        return None

    for server_id, cfg in configs.items():
        if normalized_tool_name in _normalize_configured_tool_names(cfg.tools):
            return server_id

    normalized_prefixes = sorted(
        ((server_id.replace("-", "_").lower(), server_id) for server_id in configs),
        key=lambda item: len(item[0]),
        reverse=True,
    )
    for safe_id, server_id in normalized_prefixes:
        if normalized_tool_name.startswith(f"mcp_{safe_id}_"):
            return server_id
    return None


def _compact_mcp_input_schema(schema: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(schema, dict):
        return {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        }

    compact: dict[str, Any] = {
        "type": str(schema.get("type", "object") or "object"),
        "properties": {},
        "additionalProperties": bool(schema.get("additionalProperties", False)),
    }

    description = _compact_schema_description(schema.get("description"))
    if description:
        compact["description"] = description

    properties = schema.get("properties")
    if isinstance(properties, dict):
        compact_properties: dict[str, Any] = {}
        for key, raw_value in properties.items():
            value = raw_value if isinstance(raw_value, dict) else {}
            compact_prop: dict[str, Any] = {}
            prop_type = value.get("type")
            if isinstance(prop_type, str) and prop_type.strip():
                compact_prop["type"] = prop_type
            elif isinstance(value.get("enum"), list):
                compact_prop["type"] = "string"
            else:
                compact_prop["type"] = "object"

            prop_description = _compact_schema_description(value.get("description"))
            if prop_description:
                compact_prop["description"] = prop_description

            enum_values = value.get("enum")
            if isinstance(enum_values, list):
                compact_prop["enum"] = enum_values[:12]
                omitted = len(enum_values) - len(compact_prop["enum"])
                if omitted > 0:
                    compact_prop["description"] = (
                        (compact_prop.get("description", "") + " ").strip()
                        + f"(plus {omitted} more enum values in full schema)"
                    ).strip()

            compact_properties[str(key)] = compact_prop
        compact["properties"] = compact_properties

    required = schema.get("required")
    if isinstance(required, list):
        compact["required"] = [str(item) for item in required if str(item).strip()]

    return compact


def _compact_schema_description(value: Any, *, max_chars: int = 180) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    trimmed = text[: max_chars - 20].rstrip()
    return f"{trimmed}... [see full schema]"
