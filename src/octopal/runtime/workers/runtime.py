"""
Simplified Worker Runtime

Octo creates tasks -> Runtime looks up worker template -> Launches agent worker
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import os
import re
import signal
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import quote

import httpx
import structlog

from octopal.infrastructure.config.models import LLMConfig
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.mcp.manager import MCPManager
from octopal.infrastructure.observability.base import (
    TraceSink,
    bind_trace_context,
    get_current_trace_context,
    now_ms,
    reset_trace_context,
)
from octopal.infrastructure.observability.helpers import safe_preview, summarize_exception
from octopal.infrastructure.store.base import Store
from octopal.infrastructure.store.models import AuditEvent, WorkerRecord, WorkerTemplateRecord
from octopal.runtime.housekeeping import remove_tree_with_retries
from octopal.runtime.intents.types import ActionIntent
from octopal.runtime.memory.episodes import build_worker_execution_episode
from octopal.runtime.policy.engine import PolicyEngine
from octopal.runtime.tool_errors import ToolBridgeError
from octopal.runtime.workers.contracts import (
    ChildBatchResume,
    ChildWorkerOutcome,
    TaskRequest,
    WorkerInstructionRequest,
    WorkerResult,
    WorkerSpec,
)
from octopal.runtime.workers.launcher import DockerLauncher, WorkerLauncher
from octopal.utils import utc_now

logger = structlog.get_logger(__name__)
_WORKER_BLOCKED_TOOL_NAMES = {
    "send_file_to_user",
    "self_control",
    "octo_restart_self",
    "octo_check_update",
    "octo_update_self",
}
_CHILD_SPAWN_TOOL_NAMES = {
    "start_child_worker",
    "start_workers_parallel",
}
_INJECTED_ORCHESTRATION_TOOL_NAMES = {
    "answer_worker_instruction",
    "orchestration_plan_create",
    "orchestration_plan_status",
    "orchestration_plan_update_item",
}
_PERMISSION_ALIASES = {
    "spawn_children": "worker_manage",
}

# Constants
_MAX_RECOVERY_ATTEMPTS = 1
_RECOVERY_BACKOFF_SECONDS = 0.2
_STDERR_BATCH_IDLE_SECONDS = 0.05
_STDERR_BATCH_MAX_LINES = 40
_STDERR_BATCH_MAX_CHARS = 12000
_CHILD_WAIT_POLL_SECONDS = 0.25
_CHILD_WAIT_MISSING_GRACE_SECONDS = 5.0
_WORKER_ACTIVE_TIMEOUT_POLL_SECONDS = 0.25

WORKER_MODULE = "octopal.runtime.workers.agent_worker"
_TASK_LOG_PREVIEW_CHARS = 100
_AUDIT_TASK_PREVIEW_CHARS = 200
_SECRET_PATTERNS = (
    re.compile(r"(?i)\b(authorization\s*:\s*bearer\s+)([^\s,;]+)"),
    re.compile(r"(?i)\b(bearer\s+)([^\s,;]+)"),
    re.compile(r'(?i)\b(api[_ -]?key|token|secret|password)\b(\s*[:=]\s*)(["\']?)([^"\'\s,;]+)\3'),
    re.compile(r"\b(?:moltbook|openai|sk|rk)_[A-Za-z0-9_-]{12,}\b"),
)
_WORKER_ENV_SETTING_FIELDS = (
    "litellm_num_retries",
    "litellm_timeout",
    "litellm_fallbacks",
    "litellm_drop_params",
    "litellm_caching",
    "litellm_max_concurrency",
    "litellm_rate_limit_max_retries",
    "litellm_rate_limit_base_delay_seconds",
    "litellm_rate_limit_max_delay_seconds",
    "webclaw_enabled",
    "webclaw_binary",
    "webclaw_timeout_seconds",
    "webclaw_prefer_local",
    "browser_backend",
    "pinchtab_base_url",
    "pinchtab_browser",
    "pinchtab_timeout_seconds",
)
_WORKER_HOST_ENV_ALLOWLIST = {
    "ALL_PROXY",
    "COMSPEC",
    "CURL_CA_BUNDLE",
    "HOME",
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "NO_PROXY",
    "PATH",
    "PATHEXT",
    "Path",
    "REQUESTS_CA_BUNDLE",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "TMPDIR",
    "USERPROFILE",
    "WINDIR",
}


class _WorkerStopRequested(RuntimeError):
    """Raised when a worker has been explicitly stopped by the runtime."""


@dataclass
class _WorkerPauseTracker:
    loop: asyncio.AbstractEventLoop
    paused_at: float | None = None
    paused_total: float = 0.0
    reason: str | None = None

    def pause(self, reason: str) -> None:
        if self.paused_at is not None:
            return
        self.paused_at = self.loop.time()
        self.reason = reason

    def resume(self) -> None:
        if self.paused_at is None:
            return
        self.paused_total += max(0.0, self.loop.time() - self.paused_at)
        self.paused_at = None
        self.reason = None

    def active_elapsed_since(self, started_at: float) -> float:
        now = self.loop.time()
        paused_now = max(0.0, now - self.paused_at) if self.paused_at is not None else 0.0
        return max(0.0, now - started_at - self.paused_total - paused_now)


@dataclass
class WorkerRuntime:
    store: Store
    policy: PolicyEngine
    workspace_dir: Path
    launcher: WorkerLauncher
    settings: Settings
    mcp_manager: MCPManager | None = None
    octo: Any | None = None
    trace_sink: TraceSink | None = None
    _running: dict[str, asyncio.subprocess.Process] = field(default_factory=dict)
    _stop_requests: set[str] = field(default_factory=set)
    _instruction_waiters: dict[tuple[str, str], asyncio.Future[str]] = field(default_factory=dict)

    async def run_task(
        self,
        task_request: TaskRequest,
        approval_requester: Callable[[ActionIntent], Awaitable[bool]] | None = None,
    ) -> WorkerResult:
        """Run a task with the specified worker template."""
        # Get worker template
        template: WorkerTemplateRecord | None = await asyncio.to_thread(
            self.store.get_worker_template, task_request.worker_id
        )
        if not template:
            return await self._preflight_failure(
                task_request,
                template=None,
                granted_capabilities=[],
                summary=f"Worker template not found: {task_request.worker_id}",
            )

        required_permissions = _normalize_permission_names(template.required_permissions)

        # Build capabilities from template permissions
        capabilities = self._build_capabilities(required_permissions)

        # Get granted capabilities from policy
        granted = self.policy.grant_capabilities(capabilities)
        granted_permission_names = _capability_types(granted)

        missing_permissions = sorted(set(required_permissions) - set(granted_permission_names))
        if missing_permissions:
            return await self._preflight_failure(
                task_request,
                template=template,
                granted_capabilities=granted,
                summary=(
                    "Permission denied for worker task: missing required permissions "
                    f"({', '.join(missing_permissions)})"
                ),
                output={
                    "error": "missing_required_permissions",
                    "permissions": missing_permissions,
                },
            )

        try:
            requested_tool_names = _resolve_effective_worker_tools(
                template_tools=template.available_tools,
                requested_tools=task_request.tools,
            )
        except ValueError as exc:
            return await self._preflight_failure(
                task_request,
                template=template,
                granted_capabilities=granted,
                status="failed",
                summary=f"Worker tool validation failed: {exc}",
                output={"error": "invalid_worker_tool_override", "detail": str(exc)},
            )
        required_tool_calls = _normalize_name_list(task_request.required_tool_calls)
        missing_required_tool_calls = sorted(
            tool_name
            for tool_name in required_tool_calls
            if tool_name not in set(requested_tool_names)
        )
        if missing_required_tool_calls:
            return await self._preflight_failure(
                task_request,
                template=template,
                granted_capabilities=granted,
                status="failed",
                summary=(
                    "Worker tool validation failed: required tool call(s) are not available "
                    f"({', '.join(missing_required_tool_calls)})"
                ),
                output={
                    "error": "missing_required_tool_calls",
                    "tools": missing_required_tool_calls,
                },
            )
        has_requested_mcp_tools = any(
            str(tool_name).startswith("mcp_") for tool_name in requested_tool_names
        )
        if self.mcp_manager:
            try:
                ensure_server_ids: list[str] | None = []
                if has_requested_mcp_tools:
                    resolver = getattr(
                        self.mcp_manager,
                        "resolve_configured_server_ids_for_tools",
                        None,
                    )
                    resolved_server_ids: list[str] = []
                    if callable(resolver):
                        resolved_server_ids = list(resolver(requested_tool_names) or [])
                    # Fall back to the older "connect everything configured"
                    # behavior only when we cannot map requested MCP tools to a
                    # concrete server set.
                    ensure_server_ids = resolved_server_ids or None
                await self.mcp_manager.ensure_configured_servers_connected(ensure_server_ids)
            except Exception:
                logger.warning(
                    "Failed to ensure configured MCP servers before worker launch",
                    worker_id=task_request.worker_id,
                    requested_mcp_tools=has_requested_mcp_tools,
                    requested_tool_names=requested_tool_names,
                    exc_info=True,
                )

        # Get all tools to find MCP tool definitions
        from octopal.tools.tools import get_tools

        all_tools = get_tools(mcp_manager=self.mcp_manager)
        all_tools_by_name = {str(tool.name).strip().lower(): tool for tool in all_tools}

        tool_validation_error = _validate_worker_tool_permissions(
            tool_names=requested_tool_names,
            allowed_permissions=granted_permission_names,
            all_tools_by_name=all_tools_by_name,
        )
        if tool_validation_error:
            return await self._preflight_failure(
                task_request,
                template=template,
                granted_capabilities=granted,
                summary=f"Worker tool validation failed: {tool_validation_error}",
                output={
                    "error": "invalid_worker_tool_permissions",
                    "detail": tool_validation_error,
                },
            )

        mcp_tools_data = []
        known_server_ids = list(self.mcp_manager.sessions.keys()) if self.mcp_manager else []

        # 1. Add explicitly requested MCP-backed tools, including connector aliases.
        for tool_name in requested_tool_names:
            # Find the tool spec
            spec_found = all_tools_by_name.get(str(tool_name).strip().lower())
            if spec_found is None:
                continue

            server_id = getattr(spec_found, "server_id", None)
            remote_tool_name = getattr(spec_found, "remote_tool_name", None)
            if (not server_id or not remote_tool_name) and str(tool_name).startswith("mcp_"):
                server_id, remote_tool_name = _extract_mcp_tool_identity(
                    spec_found.name, known_server_ids
                )
            if not server_id or not remote_tool_name:
                continue

            mcp_tools_data.append(
                {
                    "name": spec_found.name,
                    "description": spec_found.description,
                    "parameters": spec_found.parameters,
                    "permission": spec_found.permission,
                    "is_async": spec_found.is_async,
                    "server_id": server_id,
                    "remote_tool_name": remote_tool_name,
                }
            )

        # Global MCP tools are intentionally NOT auto-injected.
        # Workers only receive MCP-backed tools explicitly listed in task_request/tools or template available_tools.

        # Resolve worker LLM configuration
        llm_config = self._resolve_worker_llm_config(template, task_request)

        # Create worker spec
        worker_id = task_request.run_id or str(uuid.uuid4())
        spec = WorkerSpec(
            id=worker_id,
            template_id=template.id,
            template_name=template.name,
            task=task_request.task,
            inputs=task_request.inputs,
            system_prompt=template.system_prompt,
            available_tools=requested_tool_names,
            required_tool_calls=required_tool_calls,
            mcp_tools=mcp_tools_data,
            model=template.model,
            llm_config=llm_config,
            granted_capabilities=[c.model_dump() for c in granted],
            timeout_seconds=task_request.timeout_seconds or template.default_timeout_seconds,
            max_thinking_steps=task_request.max_thinking_steps or template.max_thinking_steps,
            run_id=task_request.run_id or worker_id,
            lifecycle="ephemeral",
            correlation_id=task_request.correlation_id,
            parent_worker_id=task_request.parent_worker_id,
            lineage_id=task_request.lineage_id,
            root_task_id=task_request.root_task_id,
            spawn_depth=task_request.spawn_depth,
            effective_permissions=granted_permission_names,
            allowed_paths=task_request.allowed_paths,
        )

        # Run worker
        return await self.run(spec, approval_requester=approval_requester)

    async def _preflight_failure(
        self,
        task_request: TaskRequest,
        *,
        template: WorkerTemplateRecord | None,
        granted_capabilities: list[Any],
        summary: str,
        output: dict[str, Any] | None = None,
        status: str = "failed",
    ) -> WorkerResult:
        worker_id = task_request.run_id or str(uuid.uuid4())
        now = utc_now()
        error_value = None
        if isinstance(output, dict):
            raw_error = output.get("error")
            if raw_error is not None:
                error_value = str(raw_error)

        get_worker = getattr(self.store, "get_worker", None)
        create_worker = getattr(self.store, "create_worker", None)
        update_worker_status = getattr(self.store, "update_worker_status", None)
        update_worker_result = getattr(self.store, "update_worker_result", None)

        if callable(get_worker) and callable(create_worker):
            existing = await asyncio.to_thread(get_worker, worker_id)
            if existing is None:
                await asyncio.to_thread(
                    create_worker,
                    WorkerRecord(
                        id=worker_id,
                        status=status,
                        task=task_request.task,
                        granted_caps=[
                            cap.model_dump() if hasattr(cap, "model_dump") else dict(cap)
                            for cap in granted_capabilities
                        ],
                        created_at=now,
                        updated_at=now,
                        summary=summary,
                        output=output,
                        error=error_value,
                        lineage_id=task_request.lineage_id,
                        parent_worker_id=task_request.parent_worker_id,
                        root_task_id=task_request.root_task_id,
                        spawn_depth=task_request.spawn_depth,
                        template_id=template.id if template else task_request.worker_id,
                        template_name=template.name if template else task_request.worker_id,
                    ),
                )
            else:
                if callable(update_worker_status):
                    await asyncio.to_thread(update_worker_status, worker_id, status)
                if callable(update_worker_result):
                    await asyncio.to_thread(
                        update_worker_result,
                        worker_id,
                        summary=summary,
                        output=output,
                        error=error_value,
                    )

        return WorkerResult(status="failed", summary=summary, output=output)

    def _resolve_worker_llm_config(
        self, template: WorkerTemplateRecord, task_request: TaskRequest
    ) -> LLMConfig | None:
        """Resolve LLM configuration for a worker task."""
        config_obj = self.settings.config_obj
        if not config_obj:
            return None

        # 1. Start with worker-specific override from config.json
        # Check by template name or ID
        worker_config = config_obj.worker_llm_overrides.get(
            template.id
        ) or config_obj.worker_llm_overrides.get(template.name)

        # 2. If no specific override, use default worker LLM config
        if not worker_config and (
            config_obj.worker_llm_default.provider_id or config_obj.worker_llm_default.model
        ):
            # Only use worker_llm_default if it has at least provider_id or model set;
            # otherwise it might be just an empty default object.
            worker_config = config_obj.worker_llm_default

        # 3. If still none, fallback to Octo's LLM config
        if not worker_config:
            worker_config = config_obj.llm

        # Create a copy to avoid modifying the original config
        resolved = worker_config.model_copy()

        # 4. Apply worker template model override if provided.
        # Per-task worker model overrides from Octo are intentionally ignored.
        if template.model:
            resolved.model = template.model

        return resolved

    async def run(
        self,
        spec: WorkerSpec,
        approval_requester: Callable[[ActionIntent], Awaitable[bool]] | None = None,
    ) -> WorkerResult:
        """Run a worker with the given spec."""
        task_preview = _sanitize_task_text(spec.task, limit=_TASK_LOG_PREVIEW_CHARS)
        trace_sink = self.trace_sink
        parent_trace_ctx = get_current_trace_context()
        worker_trace_ctx = None
        worker_trace_token = None
        worker_started_at_ms = now_ms()
        worker_trace_status = "ok"
        worker_trace_output: dict[str, Any] | None = None
        worker_trace_metadata: dict[str, Any] = {
            "worker_run_id": spec.id,
            "template_id": spec.template_id,
            "template_name": spec.template_name,
            "task_preview": task_preview,
            "timeout_seconds": spec.timeout_seconds,
            "tools_allowed": len(spec.available_tools),
            "lineage_id": spec.lineage_id,
            "parent_worker_id": spec.parent_worker_id,
            "spawn_depth": spec.spawn_depth,
        }
        if trace_sink is not None and parent_trace_ctx is not None:
            worker_trace_ctx = await trace_sink.start_span(
                parent_trace_ctx,
                name="worker.run",
                metadata=worker_trace_metadata,
            )
            worker_trace_token = bind_trace_context(worker_trace_ctx)
        logger.info(
            "WorkerRuntime run: id=%s task=%s timeout=%ss tools=%s",
            spec.id,
            task_preview,
            spec.timeout_seconds,
            len(spec.available_tools),
        )

        # Create worker directory
        worker_dir = self._worker_dir(spec.id)
        await asyncio.to_thread(worker_dir.mkdir, parents=True, exist_ok=True)

        # Write spec file
        spec_path = worker_dir / "spec.json"
        await asyncio.to_thread(
            spec_path.write_text, json.dumps(spec.model_dump(), indent=2), encoding="utf-8"
        )

        # Create worker record
        now = utc_now()
        await asyncio.to_thread(
            self.store.create_worker,
            WorkerRecord(
                id=spec.id,
                status="started",
                task=spec.task,
                granted_caps=spec.granted_capabilities,
                created_at=now,
                updated_at=now,
                lineage_id=spec.lineage_id,
                parent_worker_id=spec.parent_worker_id,
                root_task_id=spec.root_task_id,
                spawn_depth=spec.spawn_depth,
                template_id=spec.template_id or None,
                template_name=spec.template_name,
            ),
        )
        await self._append_audit(
            "worker_spawned",
            correlation_id=spec.id,
            data={
                "task": _sanitize_task_text(spec.task, limit=_AUDIT_TASK_PREVIEW_CHARS),
                "template_id": spec.template_id,
                "lineage_id": spec.lineage_id,
                "parent_worker_id": spec.parent_worker_id,
                "spawn_depth": spec.spawn_depth,
            },
        )

        # Build environment and mint a least-privilege browser credential when needed.
        env = self._build_worker_env(spec)
        pinchtab_session_id: str | None = None
        pinchtab_session_token: str | None = None
        pinchtab_ownership_file = worker_dir / "pinchtab-tabs.json"

        attempts = 0
        max_attempts = 1 + _MAX_RECOVERY_ATTEMPTS
        last_error: Exception | None = None
        result: WorkerResult | None = None

        try:
            pinchtab_session_id, pinchtab_session_token = await self._prepare_pinchtab_worker_env(
                spec, env, pinchtab_ownership_file
            )
            while attempts < max_attempts:
                if self._is_stop_requested(spec.id):
                    raise _WorkerStopRequested(f"Worker {spec.id} stop requested before launch")
                attempts += 1
                process = await self.launcher.launch(
                    spec_path=str(spec_path.resolve()),
                    cwd=str(worker_dir),
                    env=env,
                )
                attempt_timeout = _attempt_timeout_seconds(
                    base_timeout=spec.timeout_seconds,
                    attempt=attempts,
                    tools=spec.available_tools,
                )
                logger.info(
                    "WorkerRuntime process started: id=%s pid=%s attempt=%s/%s timeout_budget=%ss",
                    spec.id,
                    process.pid,
                    attempts,
                    max_attempts,
                    int(attempt_timeout),
                )
                self._running[spec.id] = process
                await asyncio.to_thread(self.store.update_worker_status, spec.id, "running")
                await self._append_audit(
                    "worker_started",
                    correlation_id=spec.id,
                    data={
                        "attempt": attempts,
                        "max_attempts": max_attempts,
                        "timeout_budget_seconds": int(attempt_timeout),
                    },
                )
                stderr_task: asyncio.Task[None] | None = None
                process_stderr = getattr(process, "stderr", None)
                if process_stderr is not None:
                    stderr_task = asyncio.create_task(
                        self._read_stderr_loop(spec.id, process_stderr)
                    )

                try:
                    if self._is_stop_requested(spec.id):
                        raise _WorkerStopRequested(
                            f"Worker {spec.id} stop requested before read loop"
                        )
                    result = await self._read_loop_with_active_timeout(
                        spec,
                        process,
                        approval_requester=approval_requester,
                        timeout_seconds=attempt_timeout,
                    )
                    if self._is_stop_requested(spec.id):
                        raise _WorkerStopRequested(
                            f"Worker {spec.id} stop requested after read loop"
                        )
                    await self._wait_for_worker_exit(spec.id, process)
                    break
                except _WorkerStopRequested:
                    last_error = _WorkerStopRequested(f"Worker {spec.id} stop requested")
                    await self._safe_terminate_process(process)
                    raise last_error from None
                except Exception as exc:
                    last_error = exc
                    recoverable, reason = _classify_recoverable_error(exc)
                    await self._safe_terminate_process(process)
                    if self._is_stop_requested(spec.id):
                        raise _WorkerStopRequested(f"Worker {spec.id} stop requested") from None
                    if recoverable and attempts < max_attempts:
                        await self._append_audit(
                            "worker_recovery_attempt",
                            level="warning",
                            correlation_id=spec.id,
                            data={
                                "attempt": attempts,
                                "next_attempt": attempts + 1,
                                "reason": reason,
                                "error": str(exc),
                            },
                        )
                        await asyncio.sleep(_RECOVERY_BACKOFF_SECONDS * attempts)
                        continue
                    raise
                finally:
                    if stderr_task is not None:
                        stderr_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await stderr_task
                    self._running.pop(spec.id, None)

            if result is None:
                raise RuntimeError("Worker failed without result after recovery attempts")

            if isinstance(result.output, dict):
                result.output["_recovery"] = {
                    "attempts": attempts,
                    "recovered": attempts > 1,
                }
            if str(result.status).strip().lower() == "failed":
                worker_trace_status = "error"
            worker_trace_output = {
                "status": result.status,
                "summary_preview": safe_preview(result.summary, limit=240),
                "summary_len": len(result.summary),
                "questions_count": len(result.questions),
                "tools_used": list(result.tools_used),
                "recovery_attempts": attempts,
                "recovered": attempts > 1,
            }
            logger.info("WorkerRuntime result: id=%s summary_len=%s", spec.id, len(result.summary))
            await self._append_audit(
                "worker_result",
                correlation_id=spec.id,
                data={"summary": result.summary, "attempts": attempts, "recovered": attempts > 1},
            )
            return result
        except _WorkerStopRequested:
            await asyncio.to_thread(self.store.update_worker_status, spec.id, "stopped")
            await asyncio.to_thread(
                self.store.update_worker_result,
                spec.id,
                summary="Worker stopped.",
                error="Worker stop requested.",
            )
            await self._append_audit(
                "worker_stopped",
                level="warning",
                correlation_id=spec.id,
                data={
                    "reason": "stop_requested",
                    "attempts": attempts,
                    "max_attempts": max_attempts,
                },
            )
            worker_trace_status = "error"
            worker_trace_metadata["stop_reason"] = "stop_requested"
            worker_trace_output = {
                "status": "failed",
                "summary_preview": "Worker stopped.",
                "summary_len": len("Worker stopped."),
                "questions_count": 0,
                "tools_used": [],
                "recovery_attempts": attempts,
                "recovered": attempts > 1,
            }
            return WorkerResult(
                status="failed", summary="Worker stopped.", output={"stopped": True}
            )
        except TimeoutError:
            await asyncio.to_thread(self.store.update_worker_status, spec.id, "failed")
            await asyncio.to_thread(
                self.store.update_worker_result,
                spec.id,
                error=f"Worker timed out after recovery attempts ({attempts}/{max_attempts})",
            )
            await self._append_audit(
                "worker_failed",
                level="error",
                correlation_id=spec.id,
                data={"reason": "timeout", "attempts": attempts, "max_attempts": max_attempts},
            )
            worker_trace_status = "error"
            worker_trace_metadata["error_type"] = "TimeoutError"
            worker_trace_metadata["error_message_short"] = (
                f"Worker timed out after recovery attempts ({attempts}/{max_attempts})"
            )
            raise RuntimeError("Worker timed out after recovery attempts") from None
        except Exception as exc:
            await asyncio.to_thread(self.store.update_worker_status, spec.id, "failed")
            await asyncio.to_thread(
                self.store.update_worker_result,
                spec.id,
                error=f"Worker failed after recovery attempts: {exc}",
            )
            await self._append_audit(
                "worker_failed",
                level="error",
                correlation_id=spec.id,
                data={
                    "reason": "exception",
                    "error": str(exc),
                    "attempts": attempts,
                    "max_attempts": max_attempts,
                },
            )
            worker_trace_status = "error"
            worker_trace_metadata.update(summarize_exception(exc))
            if attempts >= max_attempts and last_error is not None:
                raise RuntimeError(f"Worker failed after recovery attempts: {last_error}") from None
            raise
        finally:
            if pinchtab_session_token is not None:
                await self._close_pinchtab_worker_tabs(
                    pinchtab_session_token, pinchtab_ownership_file
                )
            if pinchtab_session_id is not None:
                await self._revoke_pinchtab_session(pinchtab_session_id)
            if worker_trace_ctx is not None and trace_sink is not None:
                finish_meta = dict(worker_trace_metadata)
                finish_meta["duration_ms"] = round(now_ms() - worker_started_at_ms, 2)
                await trace_sink.finish_span(
                    worker_trace_ctx,
                    status=worker_trace_status,
                    output=worker_trace_output,
                    metadata=finish_meta,
                )
            if worker_trace_token is not None:
                reset_trace_context(worker_trace_token)
            self._stop_requests.discard(spec.id)
            if spec.lifecycle == "ephemeral":
                await self._cleanup_worker_dir(worker_dir)

    async def stop_worker(self, worker_id: str) -> bool:
        """Stop a running worker."""
        self._stop_requests.add(worker_id)
        process = self._running.get(worker_id)
        if not process:
            worker = await asyncio.to_thread(self.store.get_worker, worker_id)
            if worker and worker.status in {
                "started",
                "running",
                "waiting_for_children",
                "awaiting_instruction",
            }:
                await asyncio.to_thread(self.store.update_worker_status, worker_id, "stopped")
                await asyncio.to_thread(
                    self.store.update_worker_result,
                    worker_id,
                    error="Worker process not found in runtime; stale running state reconciled.",
                )
                await self._append_audit(
                    "worker_stopped",
                    level="warning",
                    correlation_id=worker_id,
                    data={"reason": "stale_record_reconciled"},
                )
                return True
            return False
        try:
            await self._safe_terminate_process(process)
        except Exception:
            logger.exception("Failed to stop worker: %s", worker_id)
            return False
        await asyncio.to_thread(self.store.update_worker_status, worker_id, "stopped")
        await asyncio.to_thread(
            self.store.update_worker_result,
            worker_id,
            summary="Worker stopped.",
            error="Worker stop requested.",
        )
        await self._append_audit(
            "worker_stopped",
            level="warning",
            correlation_id=worker_id,
            data={"reason": "explicit_stop"},
        )
        return True

    def is_worker_running(self, worker_id: str) -> bool:
        """Return True if worker process is currently tracked as live in this runtime."""
        process = self._running.get(worker_id)
        if not process:
            return False
        return process.returncode is None

    def _is_stop_requested(self, worker_id: str) -> bool:
        return worker_id in self._stop_requests

    def _build_worker_env(self, spec: WorkerSpec) -> dict[str, str]:
        env = {
            **_safe_worker_host_env(os.environ),
            "PYTHONPATH": _pythonpath(),
            "OCTOPAL_WORKSPACE_DIR": str(self.workspace_dir.resolve()),
        }

        for field_name in _WORKER_ENV_SETTING_FIELDS:
            value = getattr(self.settings, field_name, None)
            if value in (None, ""):
                continue
            env[_settings_env_name(field_name)] = str(value)

        if self.settings.pinchtab_worker_base_url:
            env["OCTOPAL_PINCHTAB_BASE_URL"] = self.settings.pinchtab_worker_base_url
        if isinstance(self.launcher, DockerLauncher) and self.settings.webclaw_enabled:
            env["OCTOPAL_WEBCLAW_BINARY"] = "webclaw"

        tool_env = _tool_env_from_settings(self.settings, spec.available_tools)
        env.update(tool_env)

        return env

    async def _prepare_pinchtab_worker_env(
        self,
        spec: WorkerSpec,
        env: dict[str, str],
        ownership_file: Path,
    ) -> tuple[str | None, str | None]:
        if not self._worker_uses_pinchtab(spec):
            return None, None

        env["OCTOPAL_PINCHTAB_OWNERSHIP_FILE"] = ownership_file.name
        session_id: str | None = None
        session_token: str | None = None
        if self.settings.pinchtab_token:
            try:
                session_id, session_token = await self._create_pinchtab_session(spec)
            except Exception:
                if not self.settings.pinchtab_fallback_to_playwright:
                    raise
                logger.warning(
                    "PinchTab session unavailable; worker will use Playwright",
                    worker_id=spec.id,
                    exc_info=True,
                )
                env["OCTOPAL_BROWSER_BACKEND"] = "playwright"
                env.pop("OCTOPAL_PINCHTAB_OWNERSHIP_FILE", None)
                return None, None
        elif self.settings.pinchtab_session:
            session_token = self.settings.pinchtab_session

        if session_token:
            env["OCTOPAL_PINCHTAB_SESSION"] = session_token
        return session_id, session_token

    def _worker_uses_pinchtab(self, spec: WorkerSpec) -> bool:
        if self.settings.browser_backend.strip().lower() != "pinchtab":
            return False
        return any(
            str(name).startswith("browser_") or str(name) == "fetch_plan_tool"
            for name in spec.available_tools
        )

    async def _close_pinchtab_worker_tabs(self, session_token: str, ownership_file: Path) -> None:
        try:
            raw = await asyncio.to_thread(ownership_file.read_text, encoding="utf-8")
            payload = json.loads(raw)
            tab_ids = (
                [str(tab_id).strip() for tab_id in payload if str(tab_id).strip()]
                if isinstance(payload, list)
                else []
            )
        except FileNotFoundError:
            return
        except (OSError, ValueError, TypeError) as exc:
            logger.warning(
                "Failed to read PinchTab worker tab ownership",
                path=str(ownership_file),
                error=str(exc),
            )
            return

        headers = {"Authorization": f"Session {session_token}"}
        failed_tab_ids: list[str] = []
        for tab_id in tab_ids:
            try:
                async with self._pinchtab_client() as client:
                    response = await client.post(
                        f"/tabs/{quote(tab_id, safe='')}/close", headers=headers, json={}
                    )
                    response.raise_for_status()
            except httpx.HTTPError as exc:
                failed_tab_ids.append(tab_id)
                logger.warning(
                    "Failed to close PinchTab worker tab",
                    tab_id=tab_id,
                    error=str(exc),
                )
        if failed_tab_ids:
            with contextlib.suppress(OSError):
                await asyncio.to_thread(
                    ownership_file.write_text,
                    json.dumps(failed_tab_ids),
                    encoding="utf-8",
                )
            return
        with contextlib.suppress(OSError):
            await asyncio.to_thread(ownership_file.unlink)

    async def _create_pinchtab_session(self, spec: WorkerSpec) -> tuple[str, str]:
        headers = {"Authorization": f"Bearer {self.settings.pinchtab_token}"}
        payload = {
            "agentId": f"octopal-worker-{spec.id}",
            "label": f"Octopal worker {spec.id}",
            "browser": self.settings.pinchtab_browser,
        }
        try:
            async with self._pinchtab_client() as client:
                response = await client.post("/sessions", headers=headers, json=payload)
                response.raise_for_status()
                result = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise RuntimeError(f"Failed to create PinchTab worker session: {exc}") from exc

        session_id = str(result.get("id") or "").strip() if isinstance(result, dict) else ""
        session_token = (
            str(result.get("sessionToken") or "").strip() if isinstance(result, dict) else ""
        )
        if not session_id or not session_token:
            raise RuntimeError("PinchTab session response is missing id or sessionToken")
        return session_id, session_token

    async def _revoke_pinchtab_session(self, session_id: str) -> None:
        headers = {"Authorization": f"Bearer {self.settings.pinchtab_token}"}
        try:
            async with self._pinchtab_client() as client:
                response = await client.post(
                    f"/sessions/{quote(session_id, safe='')}/revoke", headers=headers
                )
                response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(
                "Failed to revoke PinchTab worker session",
                session_id=session_id,
                error=str(exc),
            )

    def _pinchtab_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self.settings.pinchtab_base_url.rstrip("/"),
            timeout=self.settings.pinchtab_timeout_seconds,
        )

    async def _write_to_worker(
        self, process: asyncio.subprocess.Process, payload: dict[str, Any]
    ) -> None:
        """Write a JSON message to the worker's stdin."""
        if process.stdin is None:
            logger.error("Worker process has no stdin")
            return
        line = json.dumps(payload) + "\n"
        process.stdin.write(line.encode("utf-8"))
        await process.stdin.drain()

    def _resolve_octo_chat_id(self, worker_id: str) -> int:
        if self.octo is None:
            return 0
        resolver = getattr(self.octo, "get_worker_chat_id", None)
        if not callable(resolver):
            return 0
        try:
            return int(resolver(worker_id) or 0)
        except Exception:
            logger.debug("Failed to resolve worker chat id", worker_id=worker_id, exc_info=True)
            return 0

    async def _read_loop_with_active_timeout(
        self,
        spec: WorkerSpec,
        process: asyncio.subprocess.Process,
        *,
        approval_requester: Callable[[ActionIntent], Awaitable[bool]] | None,
        timeout_seconds: float,
    ) -> WorkerResult:
        loop = asyncio.get_running_loop()
        pause_tracker = _WorkerPauseTracker(loop=loop)
        started_at = loop.time()
        read_loop_kwargs: dict[str, Any] = {"approval_requester": approval_requester}
        try:
            read_loop_params = inspect.signature(self._read_loop).parameters
            accepts_pause_tracker = "pause_tracker" in read_loop_params or any(
                param.kind is inspect.Parameter.VAR_KEYWORD for param in read_loop_params.values()
            )
        except (TypeError, ValueError):
            accepts_pause_tracker = True
        if accepts_pause_tracker:
            read_loop_kwargs["pause_tracker"] = pause_tracker
        read_task = asyncio.create_task(
            self._read_loop(
                spec,
                process,
                **read_loop_kwargs,
            )
        )
        try:
            while True:
                if read_task.done():
                    return await read_task
                remaining = timeout_seconds - pause_tracker.active_elapsed_since(started_at)
                if remaining <= 0:
                    read_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await read_task
                    raise TimeoutError
                await asyncio.wait(
                    {read_task},
                    timeout=min(_WORKER_ACTIVE_TIMEOUT_POLL_SECONDS, remaining),
                    return_when=asyncio.FIRST_COMPLETED,
                )
        finally:
            if not read_task.done():
                read_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await read_task

    async def answer_instruction(
        self,
        *,
        worker_id: str,
        request_id: str,
        instruction: str,
        answerer_worker_id: str | None = None,
    ) -> bool:
        key = (str(worker_id).strip(), str(request_id).strip())
        answerer_id = str(answerer_worker_id or "").strip()
        if answerer_id:
            worker = await asyncio.to_thread(self.store.get_worker, key[0])
            parent_worker_id = str(getattr(worker, "parent_worker_id", "") or "").strip()
            if worker is None or parent_worker_id != answerer_id:
                await self._append_audit(
                    "worker_instruction_answer_denied",
                    correlation_id=key[0],
                    data={
                        "worker_id": key[0],
                        "request_id": key[1],
                        "answerer_worker_id": answerer_id,
                        "reason": "not_parent_worker",
                    },
                )
                return False
        future = self._instruction_waiters.get(key)
        if future is None or future.done():
            return False
        await asyncio.to_thread(self.store.update_worker_status, key[0], "running")
        audit_data = {"worker_id": key[0], "request_id": key[1]}
        if answerer_id:
            audit_data["answerer_worker_id"] = answerer_id
        await self._append_audit(
            "worker_instruction_answered",
            correlation_id=key[0],
            data=audit_data,
        )
        future.set_result(str(instruction or "").strip())
        return True

    async def _await_child_batch(
        self,
        *,
        spec: WorkerSpec,
        process: asyncio.subprocess.Process,
        worker_ids: list[str],
        pause_tracker: _WorkerPauseTracker | None = None,
    ) -> ChildBatchResume:
        child_ids = list(
            dict.fromkeys(
                str(worker_id).strip() for worker_id in worker_ids if str(worker_id).strip()
            )
        )
        if not child_ids:
            return ChildBatchResume()

        await asyncio.to_thread(self.store.update_worker_status, spec.id, "waiting_for_children")
        await asyncio.to_thread(
            self.store.update_worker_result,
            spec.id,
            summary=(
                f"Waiting for {len(child_ids)} child worker(s) to finish: " + ", ".join(child_ids)
            ),
        )
        await self._append_audit(
            "worker_waiting_for_children",
            correlation_id=spec.id,
            data={"child_worker_ids": child_ids, "count": len(child_ids)},
        )
        if pause_tracker is not None:
            pause_tracker.pause("waiting_for_children")

        missing_since: dict[str, float] = {}

        try:
            while True:
                if self._is_stop_requested(spec.id):
                    raise _WorkerStopRequested(
                        f"Worker {spec.id} stop requested while waiting for children"
                    )
                if process.returncode is not None:
                    raise RuntimeError("Parent worker exited while waiting for child workers")

                pending_ids: list[str] = []
                completed: list[ChildWorkerOutcome] = []
                failed: list[ChildWorkerOutcome] = []
                stopped: list[ChildWorkerOutcome] = []
                missing: list[ChildWorkerOutcome] = []
                awaiting_instruction: list[ChildWorkerOutcome] = []

                for child_id in child_ids:
                    record = await asyncio.to_thread(self.store.get_worker, child_id)
                    if record is None:
                        now_monotonic = asyncio.get_running_loop().time()
                        first_seen = missing_since.setdefault(child_id, now_monotonic)
                        elapsed_missing = now_monotonic - first_seen
                        if elapsed_missing >= _CHILD_WAIT_MISSING_GRACE_SECONDS:
                            missing.append(
                                ChildWorkerOutcome(
                                    worker_id=child_id,
                                    status="missing",
                                    error=(
                                        "Child worker record did not appear within the runtime "
                                        "spawn grace window."
                                    ),
                                )
                            )
                        else:
                            pending_ids.append(child_id)
                        continue

                    missing_since.pop(child_id, None)
                    outcome = _child_worker_outcome_from_record(record)
                    normalized_status = str(record.status or "").strip().lower()
                    if normalized_status == "completed":
                        completed.append(outcome)
                    elif normalized_status == "failed":
                        failed.append(outcome)
                    elif normalized_status == "stopped":
                        stopped.append(outcome)
                    elif normalized_status == "awaiting_instruction":
                        awaiting_instruction.append(outcome)
                    else:
                        pending_ids.append(child_id)

                if awaiting_instruction:
                    resume = ChildBatchResume(
                        worker_ids=child_ids,
                        completed_count=len(completed),
                        failed_count=len(failed),
                        stopped_count=len(stopped),
                        missing_count=len(missing),
                        awaiting_instruction_count=len(awaiting_instruction),
                        status="awaiting_instruction",
                        completed=completed,
                        failed=failed,
                        stopped=stopped,
                        missing=missing,
                        awaiting_instruction=awaiting_instruction,
                    )
                    await asyncio.to_thread(self.store.update_worker_status, spec.id, "running")
                    await self._append_audit(
                        "worker_resumed_for_child_instruction",
                        correlation_id=spec.id,
                        data={
                            "child_worker_ids": child_ids,
                            "awaiting_instruction_count": len(awaiting_instruction),
                        },
                    )
                    await self._sync_orchestration_plan_with_child_batch(spec, resume)
                    await self._write_to_worker(
                        process,
                        {"type": "resume_children", "child_batch": resume.model_dump(mode="json")},
                    )
                    return resume

                if not pending_ids:
                    status = "completed"
                    if failed or stopped or missing:
                        status = "partial"
                    resume = ChildBatchResume(
                        worker_ids=child_ids,
                        completed_count=len(completed),
                        failed_count=len(failed),
                        stopped_count=len(stopped),
                        missing_count=len(missing),
                        status=status,
                        completed=completed,
                        failed=failed,
                        stopped=stopped,
                        missing=missing,
                    )
                    await asyncio.to_thread(self.store.update_worker_status, spec.id, "running")
                    await self._append_audit(
                        "worker_resumed_after_children",
                        correlation_id=spec.id,
                        data={
                            "child_worker_ids": child_ids,
                            "completed_count": len(completed),
                            "failed_count": len(failed),
                            "stopped_count": len(stopped),
                            "missing_count": len(missing),
                        },
                    )
                    await self._sync_orchestration_plan_with_child_batch(spec, resume)
                    await self._write_to_worker(
                        process,
                        {"type": "resume_children", "child_batch": resume.model_dump(mode="json")},
                    )
                    return resume

                await asyncio.sleep(_CHILD_WAIT_POLL_SECONDS)
        finally:
            if pause_tracker is not None:
                pause_tracker.resume()

    async def _sync_orchestration_plan_with_child_batch(
        self,
        spec: WorkerSpec,
        resume: ChildBatchResume,
    ) -> None:
        if self.octo is None:
            return
        try:
            from octopal.tools.workers.management import (
                sync_orchestration_plan_with_child_batch,
            )

            await asyncio.to_thread(
                sync_orchestration_plan_with_child_batch,
                octo=self.octo,
                parent_worker_id=spec.id,
                child_batch=resume.model_dump(mode="json"),
            )
        except Exception:
            logger.debug(
                "Failed to sync orchestration plan with child batch",
                worker_id=spec.id,
                exc_info=True,
            )

    async def _await_instruction(
        self,
        *,
        spec: WorkerSpec,
        process: asyncio.subprocess.Process,
        payload: dict[str, Any],
        pause_tracker: _WorkerPauseTracker | None = None,
    ) -> None:
        request_id = str(payload.get("request_id") or uuid.uuid4()).strip()
        target = str(payload.get("target") or "octo").strip().lower()
        if target not in {"octo", "parent"}:
            target = "octo"
        question = str(payload.get("question") or "").strip() or "Worker requested instruction."
        raw_context = payload.get("context")
        context = raw_context if isinstance(raw_context, dict) else {}
        timeout_seconds = max(1, int(payload.get("timeout_seconds") or 120))
        request = WorkerInstructionRequest(
            request_id=request_id,
            worker_id=spec.id,
            target=target,  # type: ignore[arg-type]
            question=question,
            context=context,
            timeout_seconds=timeout_seconds,
            created_at=utc_now(),
        )
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        key = (spec.id, request.request_id)
        self._instruction_waiters[key] = future

        await asyncio.to_thread(self.store.update_worker_status, spec.id, "awaiting_instruction")
        await asyncio.to_thread(
            self.store.update_worker_result,
            spec.id,
            summary=f"Awaiting instruction: {request.question}",
            output={"instruction_request": request.model_dump(mode="json")},
        )
        await self._append_audit(
            "worker_awaiting_instruction",
            correlation_id=spec.id,
            data=request.model_dump(mode="json"),
        )
        if target == "octo":
            handler = getattr(self.octo, "handle_worker_instruction_request", None)
            if callable(handler):
                try:
                    maybe_result = handler(spec=spec, request=request)
                    if inspect.isawaitable(maybe_result):
                        await maybe_result
                except Exception:
                    logger.exception(
                        "Failed to enqueue worker instruction request for Octo",
                        worker_id=spec.id,
                        request_id=request.request_id,
                    )
        if pause_tracker is not None:
            pause_tracker.pause("awaiting_instruction")

        try:
            instruction = await asyncio.wait_for(future, timeout=timeout_seconds)
            response = {
                "type": "resume_instruction",
                "request_id": request.request_id,
                "status": "answered",
                "instruction": instruction,
            }
            await self._append_audit(
                "worker_instruction_answered",
                correlation_id=spec.id,
                data={"request_id": request.request_id},
            )
        except TimeoutError:
            response = {
                "type": "resume_instruction",
                "request_id": request.request_id,
                "status": "timed_out",
                "instruction": "",
                "message": f"No instruction received within {timeout_seconds}s.",
            }
            await self._append_audit(
                "worker_instruction_timeout",
                level="warning",
                correlation_id=spec.id,
                data={"request_id": request.request_id, "timeout_seconds": timeout_seconds},
            )
        finally:
            self._instruction_waiters.pop(key, None)
            if pause_tracker is not None:
                pause_tracker.resume()

        await asyncio.to_thread(self.store.update_worker_status, spec.id, "running")
        await self._write_to_worker(process, response)

    async def _read_loop(
        self,
        spec: WorkerSpec,
        process: asyncio.subprocess.Process,
        approval_requester: Callable[[ActionIntent], Awaitable[bool]] | None = None,
        pause_tracker: _WorkerPauseTracker | None = None,
    ) -> WorkerResult:
        """Read worker output."""
        invalid_lines = 0
        consecutive_invalid_lines = 0
        max_invalid_lines = 200
        invalid_limit_reached = False
        max_buffer_bytes = 256 * 1024
        assert process.stdout is not None
        buffer = b""

        async def _handle_line(line: bytes) -> WorkerResult | None:
            nonlocal invalid_lines, consecutive_invalid_lines, invalid_limit_reached
            payload = _safe_parse_json(line)
            if payload is None:
                text_line = line.decode("utf-8", errors="replace").strip()
                if text_line:
                    self._log_non_json_output(text_line)
                invalid_lines += 1
                consecutive_invalid_lines += 1
                if consecutive_invalid_lines >= max_invalid_lines and not invalid_limit_reached:
                    logger.warning(
                        "Worker emitted too many non-JSON lines; continuing to wait for structured result",
                        worker_id=spec.id,
                        invalid_lines=invalid_lines,
                    )
                    invalid_limit_reached = True
                return None
            consecutive_invalid_lines = 0

            msg_type = payload.get("type")
            if msg_type == "log":
                level = str(payload.get("level", "debug") or "debug").strip().lower()
                message = str(payload.get("message", "") or "").strip()
                if not message:
                    return None
                log_method = getattr(logger, level, logger.debug)
                log_method("Worker %s: %s", spec.id, message)
                return None
            if msg_type == "octo_tool_call":
                if not self.octo:
                    await self._write_to_worker(
                        process,
                        {
                            "type": "octo_tool_result",
                            "ok": False,
                            "error": "Octo runtime bridge unavailable.",
                        },
                    )
                    return None

                tool_name = str(payload.get("tool_name", "")).strip()
                arguments = payload.get("arguments", {})
                if not isinstance(arguments, dict):
                    arguments = {}

                try:
                    from octopal.tools.workers.management import get_worker_tools

                    specs = {t.name: t for t in get_worker_tools()}
                    spec_tool = specs.get(tool_name)
                    if spec_tool is None:
                        await self._write_to_worker(
                            process,
                            {
                                "type": "octo_tool_result",
                                "ok": False,
                                "error": f"Unknown octo tool: {tool_name}",
                            },
                        )
                        return None
                    local_tool_error = _validate_worker_local_tool_call(
                        spec=spec,
                        tool_name=tool_name,
                        permission=str(getattr(spec_tool, "permission", "") or ""),
                    )
                    if local_tool_error is not None:
                        await self._write_to_worker(
                            process,
                            {"type": "octo_tool_result", "ok": False, "error": local_tool_error},
                        )
                        return None

                    tool_ctx: dict[str, Any] = {
                        "octo": self.octo,
                        "chat_id": self._resolve_octo_chat_id(spec.id),
                        "base_dir": self.workspace_dir,
                        "worker": SimpleNamespace(spec=spec),
                    }
                    if spec_tool.is_async:
                        result = spec_tool.handler(arguments, tool_ctx)
                        if inspect.isawaitable(result):
                            result = await result
                    else:
                        result = await asyncio.to_thread(spec_tool.handler, arguments, tool_ctx)
                    await self._write_to_worker(
                        process,
                        {"type": "octo_tool_result", "ok": True, "result": result},
                    )
                except Exception as exc:
                    await self._write_to_worker(
                        process,
                        {"type": "octo_tool_result", "ok": False, "error": str(exc)},
                    )
                return None
            if msg_type == "mcp_call":
                server_id = payload.get("server_id")
                tool_name = payload.get("tool_name")
                args = payload.get("arguments", {})
                mcp_tool_error = _validate_worker_mcp_tool_call(
                    spec=spec,
                    server_id=server_id,
                    tool_name=tool_name,
                )
                if mcp_tool_error is not None:
                    await self._write_to_worker(
                        process, {"type": "error", "message": mcp_tool_error}
                    )
                    return None

                if not self.mcp_manager:
                    await self._write_to_worker(
                        process,
                        {"type": "error", "message": "MCP Manager not available in runtime."},
                    )
                    return None

                session = self.mcp_manager.sessions.get(server_id)
                if not session:
                    try:
                        await self.mcp_manager.ensure_configured_servers_connected([str(server_id)])
                    except Exception:
                        logger.warning(
                            "Failed to restore MCP session for worker call",
                            worker_id=spec.id,
                            server_id=server_id,
                            tool=tool_name,
                            exc_info=True,
                        )
                    session = self.mcp_manager.sessions.get(server_id)
                if not session:
                    await self._write_to_worker(
                        process,
                        {"type": "error", "message": f"MCP session {server_id} not active."},
                    )
                    return None

                try:
                    logger.info(
                        "Executing MCP call for worker",
                        worker_id=spec.id,
                        server_id=server_id,
                        tool=tool_name,
                    )
                    result = await self.mcp_manager.call_tool(
                        str(server_id),
                        str(tool_name),
                        args,
                        allow_name_fallback=True,
                    )
                    # Convert MCP content objects to something serializable
                    content = [
                        c.model_dump() if hasattr(c, "model_dump") else str(c)
                        for c in result.content
                    ]
                    await self._write_to_worker(process, {"type": "mcp_result", "result": content})
                except Exception as e:
                    logger.exception("Worker MCP call failed")
                    payload = (
                        e.to_payload()
                        if isinstance(e, ToolBridgeError)
                        else {"type": "error", "message": str(e)}
                    )
                    await self._write_to_worker(process, payload)
                return None
            if msg_type == "intent_request":
                from octopal.infrastructure.store.models import IntentRecord, PermitRecord
                from octopal.runtime.intents.registry import (
                    IntentValidationError,
                    validate_intent,
                )
                from octopal.runtime.intents.types import IntentRequest

                try:
                    req_data = payload.get("intent")
                    request = IntentRequest.model_validate(req_data)
                    action_intent = validate_intent(
                        request=request,
                        worker_id=spec.id,
                        intent_id=str(uuid.uuid4()),
                    )
                    await asyncio.to_thread(
                        self.store.save_intent,
                        IntentRecord(
                            id=action_intent.id,
                            worker_id=action_intent.worker_id,
                            type=action_intent.type,
                            payload=action_intent.payload,
                            payload_hash=action_intent.payload_hash,
                            risk=action_intent.risk,
                            requires_approval=action_intent.requires_approval,
                            status="pending",
                            created_at=utc_now(),
                        ),
                    )

                    # Check if approval is needed
                    # Note: For now, we only support auto-approved intents in this runtime loop
                    approval_req = self.policy.check_intent(action_intent)

                    if approval_req.requires_approval:
                        await asyncio.to_thread(
                            self.store.update_intent_status,
                            action_intent.id,
                            "requires_approval",
                        )
                        approved = False
                        if approval_requester:
                            await self._append_audit(
                                "intent_approval_requested",
                                correlation_id=spec.id,
                                data={
                                    "intent_id": action_intent.id,
                                    "intent_type": action_intent.type,
                                    "risk": action_intent.risk,
                                },
                            )
                            try:
                                approved = await approval_requester(action_intent)
                            except Exception as exc:
                                logger.exception("Approval requester failed")
                                await self._append_audit(
                                    "intent_approval_failed",
                                    level="error",
                                    correlation_id=spec.id,
                                    data={
                                        "intent_id": action_intent.id,
                                        "error": str(exc),
                                    },
                                )
                        if approved:
                            permit = self.policy.issue_permit(action_intent, spec.id)
                            await asyncio.to_thread(
                                self.store.update_intent_status,
                                action_intent.id,
                                "approved",
                            )
                            await asyncio.to_thread(
                                self.store.create_permit,
                                PermitRecord(
                                    id=permit.id,
                                    intent_id=action_intent.id,
                                    intent_type=action_intent.type,
                                    worker_id=spec.id,
                                    payload_hash=permit.payload_hash,
                                    expires_at=permit.expires_at,
                                    created_at=utc_now(),
                                ),
                            )
                            await self._append_audit(
                                "intent_approval_granted",
                                correlation_id=spec.id,
                                data={"intent_id": action_intent.id},
                            )
                            response = {"type": "permit", "permit": permit.model_dump()}
                        else:
                            await asyncio.to_thread(
                                self.store.update_intent_status,
                                action_intent.id,
                                "denied",
                            )
                            await self._append_audit(
                                "intent_approval_denied",
                                level="warning",
                                correlation_id=spec.id,
                                data={"intent_id": action_intent.id},
                            )
                            response = {
                                "type": "permit_denied",
                                "reason": f"Intent requires approval: {approval_req.reason or 'denied'}",
                            }
                    else:
                        # Auto-approve
                        permit = self.policy.issue_permit(action_intent, spec.id)
                        await asyncio.to_thread(
                            self.store.update_intent_status,
                            action_intent.id,
                            "approved",
                        )
                        # Save permit to store (for audit/verification)
                        await asyncio.to_thread(
                            self.store.create_permit,
                            PermitRecord(
                                id=permit.id,
                                intent_id=action_intent.id,
                                intent_type=action_intent.type,
                                worker_id=spec.id,
                                payload_hash=permit.payload_hash,
                                expires_at=permit.expires_at,
                                created_at=utc_now(),
                            ),
                        )
                        response = {"type": "permit", "permit": permit.model_dump()}

                    # Send response back to worker
                    await self._write_to_worker(process, response)
                except IntentValidationError as exc:
                    error_resp = {
                        "type": "permit_denied",
                        "reason": f"Intent validation failed: {exc}",
                    }
                    await self._write_to_worker(process, error_resp)

                except Exception as exc:
                    logger.exception("Failed to process intent request")
                    error_resp = {"type": "permit_denied", "reason": f"Internal error: {exc}"}
                    await self._write_to_worker(process, error_resp)
                return None

            if msg_type == "intent_executed":
                logger.info("Worker %s intent executed report received", spec.id)
                await self._append_audit(
                    "intent_executed_reported",
                    correlation_id=spec.id,
                    data={
                        "intent_id": payload.get("intent_id"),
                        "permit_id": payload.get("permit_id"),
                        "intent_type": payload.get("intent_type"),
                        "success": bool(payload.get("success")),
                    },
                )
                return None

            if msg_type == "instruction_request":
                await self._await_instruction(
                    spec=spec,
                    process=process,
                    payload=payload,
                    pause_tracker=pause_tracker,
                )
                return None

            if msg_type == "await_children":
                raw_worker_ids = payload.get("worker_ids")
                worker_ids = (
                    raw_worker_ids
                    if isinstance(raw_worker_ids, list)
                    else [raw_worker_ids] if raw_worker_ids is not None else []
                )
                await self._await_child_batch(
                    spec=spec,
                    process=process,
                    worker_ids=[str(worker_id).strip() for worker_id in worker_ids],
                    pause_tracker=pause_tracker,
                )
                return None

            if msg_type == "result":
                raw_result = payload.get("result", {})
                repaired_result = _repair_worker_result_payload(raw_result)
                result = WorkerResult.model_validate(repaired_result)
                if raw_result != repaired_result:
                    await self._append_audit(
                        "worker_result_repaired",
                        level="warning",
                        correlation_id=spec.id,
                        data={"reason": "malformed_worker_result_payload"},
                    )
                worker_status = "failed" if result.status == "failed" else "completed"
                stored_output = _merge_existing_orchestration_plan_output(
                    self.store,
                    spec.id,
                    result.output,
                )
                await asyncio.to_thread(self.store.update_worker_status, spec.id, worker_status)
                await asyncio.to_thread(
                    self.store.update_worker_result,
                    spec.id,
                    summary=result.summary,
                    output=stored_output,
                    error=_worker_result_error_text(result) if worker_status == "failed" else None,
                    tools_used=result.tools_used,
                )
                await self._record_execution_episode(
                    spec=spec,
                    result=result,
                    stored_output=stored_output,
                    worker_status=worker_status,
                )
                return result
            return None

        while True:
            chunk = await process.stdout.read(4096)
            if not chunk:
                break
            buffer += chunk
            if len(buffer) > max_buffer_bytes and b"\n" not in buffer:
                logger.warning(
                    "Worker output buffer exceeded %s bytes without newline", max_buffer_bytes
                )
                await _handle_line(buffer)
                buffer = b""
                if consecutive_invalid_lines >= max_invalid_lines:
                    break
                continue
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                result = await _handle_line(line)
                if result is not None:
                    return result
                if consecutive_invalid_lines >= max_invalid_lines:
                    buffer = b""
        if buffer.strip():
            result = await _handle_line(buffer)
            if result is not None:
                return result

        await asyncio.to_thread(self.store.update_worker_status, spec.id, "failed")
        await asyncio.to_thread(
            self.store.update_worker_result, spec.id, error="Worker exited without result"
        )
        raise RuntimeError("Worker exited without result")

    async def _record_execution_episode(
        self,
        *,
        spec: WorkerSpec,
        result: WorkerResult,
        stored_output: dict[str, Any] | None,
        worker_status: str,
    ) -> None:
        add_execution_episode = getattr(self.store, "add_execution_episode", None)
        if not callable(add_execution_episode):
            return
        try:
            episode = build_worker_execution_episode(
                spec=spec,
                result=result,
                stored_output=stored_output,
                status=worker_status,
                launcher_kind=type(self.launcher).__name__,
            )
            await asyncio.to_thread(add_execution_episode, episode)
        except Exception as exc:
            logger.warning(
                "Failed to record execution episode",
                worker_id=spec.id,
                error_type=type(exc).__name__,
            )
            try:
                await self._append_audit(
                    "execution_episode_record_failed",
                    level="warning",
                    correlation_id=spec.id,
                    data={"error_type": type(exc).__name__},
                )
            except Exception:
                logger.warning(
                    "Failed to audit execution episode recording failure",
                    worker_id=spec.id,
                    exc_info=True,
                )
            return
        try:
            await self._append_audit(
                "execution_episode_recorded",
                correlation_id=spec.id,
                data={
                    "episode_id": episode.id,
                    "source_kind": episode.source_kind,
                    "trust_state": episode.trust_state,
                },
            )
        except Exception:
            logger.warning(
                "Failed to audit execution episode recording",
                worker_id=spec.id,
                exc_info=True,
            )

    def _build_capabilities(self, permissions: list[str]) -> list[Any]:
        """Build capability objects from permission strings."""
        from octopal.runtime.workers.contracts import Capability

        caps = []
        for perm in permissions:
            caps.append(Capability(type=perm, scope="worker"))
        return caps

    async def _append_audit(
        self,
        event_type: str,
        *,
        level: str = "info",
        correlation_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        event = AuditEvent(
            id=str(uuid.uuid4()),
            ts=utc_now(),
            correlation_id=correlation_id,
            level=level,
            event_type=event_type,
            data=data or {},
        )
        await asyncio.to_thread(self.store.append_audit, event)

    def _worker_dir(self, worker_id: str) -> Path:
        return self.workspace_dir / "workers" / worker_id

    async def _cleanup_worker_dir(self, worker_dir: Path) -> None:
        if not await asyncio.to_thread(worker_dir.exists):
            return

        removed = await asyncio.to_thread(
            remove_tree_with_retries,
            worker_dir,
            retries=8,
            base_delay_seconds=0.25,
            docker_cleanup_image=getattr(self.launcher, "image", None),
        )
        if removed:
            logger.info("WorkerRuntime cleaned up worker dir: %s", worker_dir)
            return
        logger.warning("WorkerRuntime cleanup failed after retries: %s", worker_dir)

    async def _safe_terminate_process(self, process: asyncio.subprocess.Process) -> None:
        try:
            if process.returncode is None:
                await self._terminate_process_tree(process)
                await asyncio.wait_for(process.wait(), timeout=5)
        except Exception:
            logger.debug("Failed to terminate worker process cleanly", exc_info=True)

    async def _wait_for_worker_exit(
        self, worker_id: str, process: asyncio.subprocess.Process
    ) -> None:
        if process.returncode is not None:
            return

        try:
            await asyncio.wait_for(process.wait(), timeout=2)
            return
        except TimeoutError:
            logger.warning(
                "Worker process did not exit after returning a result; terminating process tree",
                worker_id=worker_id,
                pid=process.pid,
            )
        except Exception:
            logger.debug(
                "Failed while waiting for worker process exit", worker_id=worker_id, exc_info=True
            )

        await self._safe_terminate_process(process)

    async def _terminate_process_tree(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return

        if os.name == "nt":
            proc = await asyncio.create_subprocess_exec(
                "taskkill",
                "/PID",
                str(process.pid),
                "/T",
                "/F",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()
            return

        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), timeout=2)
            return
        except Exception:
            pass
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)

    def _log_non_json_output(self, text: str) -> None:
        """Log non-JSON output from worker intelligently."""
        self._emit_worker_text_log("stdout", None, text)

    def _emit_worker_text_log(self, source: str, worker_id: str | None, text: str) -> None:
        clean_text = _sanitize_worker_text(text)
        if not clean_text:
            return
        level = _classify_worker_text_log_level(clean_text, source=source)
        if source == "stderr":
            message = (
                f"Worker stderr: id={worker_id} {clean_text}"
                if worker_id
                else f"Worker stderr: {clean_text}"
            )
        elif level == "error":
            message = f"Worker output (error?): {clean_text}"
        elif level == "debug":
            message = f"Worker output (non-JSON): {clean_text}"
        else:
            message = f"Worker output: {clean_text}"
        getattr(logger, level)(message)

    async def _read_stderr_loop(self, worker_id: str, stderr: asyncio.StreamReader) -> None:
        """Read worker stderr logs without affecting stdout JSON protocol parsing."""
        buffer: list[str] = []
        buffered_chars = 0

        def _flush() -> None:
            nonlocal buffer, buffered_chars
            if not buffer:
                return
            self._emit_worker_text_log("stderr", worker_id, "\n".join(buffer))
            buffer = []
            buffered_chars = 0

        while True:
            if buffer:
                try:
                    line = await asyncio.wait_for(
                        stderr.readline(),
                        timeout=_STDERR_BATCH_IDLE_SECONDS,
                    )
                except TimeoutError:
                    _flush()
                    continue
            else:
                line = await stderr.readline()
            if not line:
                _flush()
                break
            text = line.decode("utf-8", errors="replace").strip()
            if not text:
                _flush()
                continue
            buffer.append(text)
            buffered_chars += len(text)
            if len(buffer) >= _STDERR_BATCH_MAX_LINES or buffered_chars >= _STDERR_BATCH_MAX_CHARS:
                _flush()


def _child_worker_outcome_from_record(record: WorkerRecord) -> ChildWorkerOutcome:
    return ChildWorkerOutcome(
        worker_id=str(record.id),
        status=str(record.status),
        summary=record.summary,
        output=record.output,
        error=record.error,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def _safe_parse_json(line: bytes) -> dict[str, Any] | None:
    try:
        return json.loads(line.decode("utf-8"))
    except Exception:
        return None


def _repair_worker_result_payload(raw_result: Any) -> dict[str, Any]:
    if not isinstance(raw_result, dict):
        return {
            "status": "failed",
            "summary": "Worker returned malformed result payload",
            "output": {"raw_result": _truncate_text(str(raw_result), 32000)},
        }

    summary = str(raw_result.get("summary", "") or "").strip() or "Worker completed"
    output = raw_result.get("output")
    if output is not None and not _is_json_serializable(output):
        output = {"repr": _truncate_text(repr(output), 32000)}

    repaired: dict[str, Any] = {"summary": summary}
    status = _normalize_worker_result_status(raw_result, output)
    if status is not None:
        repaired["status"] = status
    if status == "failed" and _is_invalid_final_pause_status(raw_result):
        output = _merge_worker_result_error(
            output,
            {
                "error": "invalid_final_worker_status",
                "invalid_status": str(raw_result.get("status") or "").strip().lower(),
                "detail": (
                    "Workers must use instruction_request to pause; final result payloads "
                    "may only be completed or failed."
                ),
            },
        )
    if output is not None:
        repaired["output"] = output

    questions = raw_result.get("questions")
    if isinstance(questions, list):
        repaired["questions"] = [str(item).strip() for item in questions if str(item).strip()][:20]

    tools_used = raw_result.get("tools_used")
    if isinstance(tools_used, list):
        repaired["tools_used"] = [str(item).strip() for item in tools_used if str(item).strip()][
            :200
        ]

    thinking_steps = raw_result.get("thinking_steps")
    if isinstance(thinking_steps, int):
        repaired["thinking_steps"] = max(0, thinking_steps)

    knowledge_proposals = raw_result.get("knowledge_proposals")
    if isinstance(knowledge_proposals, list):
        repaired["knowledge_proposals"] = knowledge_proposals

    return repaired


def _normalize_worker_result_status(raw_result: dict[str, Any], output: Any) -> str | None:
    status = str(raw_result.get("status") or "").strip().lower()
    if status in {"completed", "failed"}:
        return status
    if _is_invalid_final_pause_status(raw_result):
        return "failed"
    if status in {"error", "failure"}:
        return "failed"
    if str(raw_result.get("error") or "").strip():
        return "failed"
    if isinstance(output, dict):
        output_status = str(output.get("status") or "").strip().lower()
        if output_status in {"error", "failed", "failure"}:
            return "failed"
        if str(output.get("error") or "").strip():
            return "failed"
    return None


def _is_invalid_final_pause_status(raw_result: dict[str, Any]) -> bool:
    return str(raw_result.get("status") or "").strip().lower() == "awaiting_instruction"


def _merge_worker_result_error(output: Any, error_payload: dict[str, Any]) -> dict[str, Any]:
    if isinstance(output, dict):
        merged = dict(output)
    elif output is None:
        merged = {}
    else:
        merged = {"result": output}
    for key, value in error_payload.items():
        merged.setdefault(key, value)
    return merged


def _is_json_serializable(value: Any) -> bool:
    try:
        json.dumps(value)
        return True
    except Exception:
        return False


def _worker_result_error_text(result: WorkerResult) -> str:
    if isinstance(result.output, dict):
        error = result.output.get("error")
        if isinstance(error, str) and error.strip():
            return error.strip()
    summary = str(result.summary or "").strip()
    return summary or "Worker reported failure"


def _merge_existing_orchestration_plan_output(
    store: Store,
    worker_id: str,
    output: dict[str, Any] | None,
) -> dict[str, Any] | None:
    try:
        existing = store.get_worker(worker_id)
    except Exception:
        return output
    existing_output = getattr(existing, "output", None) if existing is not None else None
    if not isinstance(existing_output, dict):
        return output
    plan = existing_output.get("_orchestration_plan")
    if not isinstance(plan, dict):
        return output
    merged = dict(output or {})
    merged.setdefault("_orchestration_plan", plan)
    return merged


def _sanitize_worker_text(text: str) -> str:
    import re

    ansi_escape = re.compile(r"\x1b\[[0-9;]*[mK]")
    clean_text = ansi_escape.sub("", str(text or ""))
    return clean_text.strip()


def _classify_worker_text_log_level(text: str, *, source: str) -> str:
    lowered = (text or "").lower()
    if source == "stderr" and any(
        token in lowered for token in ("rate limited", "retrying in", "backing off")
    ):
        return "info"
    if any(token in lowered for token in ("traceback", "exception", "critical")):
        return "error"
    if "error" in lowered and "rate limit" not in lowered:
        return "error"
    if "failed" in lowered and "retrying" not in lowered:
        return "error"
    if "warning" in lowered:
        return "warning"
    if "info" in lowered:
        return "info"
    if "debug" in lowered:
        return "debug"
    return "debug" if source == "stdout" else "info"


def _truncate_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + f"...[truncated {len(text) - max_chars} chars]"


def _pythonpath() -> str:
    import sys

    return os.pathsep.join([p for p in sys.path if p])


def _settings_env_name(field_name: str) -> str:
    field = Settings.model_fields.get(field_name)
    if field and field.alias:
        return str(field.alias)
    return field_name.upper()


def _safe_worker_host_env(source: Mapping[str, str]) -> dict[str, str]:
    """Keep process essentials without inheriting unrelated host secrets."""
    return {
        key: str(value)
        for key, value in source.items()
        if key in _WORKER_HOST_ENV_ALLOWLIST and value not in (None, "")
    }


def _tool_env_from_settings(settings: Settings, tool_names: list[str]) -> dict[str, str]:
    lowered_tools = {str(name).strip().lower() for name in tool_names if str(name).strip()}
    env: dict[str, str] = {}
    brave_api_key = (
        settings.config_obj.search.brave_api_key if settings.config_obj else None
    ) or settings.brave_api_key
    firecrawl_api_key = (
        settings.config_obj.search.firecrawl_api_key if settings.config_obj else None
    ) or settings.firecrawl_api_key

    if "web_search" in lowered_tools and brave_api_key:
        env["BRAVE_API_KEY"] = brave_api_key

    if (
        any(name in lowered_tools for name in {"web_fetch", "markdown_new_fetch"})
        and firecrawl_api_key
    ):
        env["FIRECRAWL_API_KEY"] = firecrawl_api_key

    return env


def _extract_mcp_tool_identity(
    tool_name: str, server_ids: list[str]
) -> tuple[str | None, str | None]:
    """Best-effort extraction of MCP server and remote tool names from generated tool names."""
    if not tool_name.startswith("mcp_"):
        return None, None
    # Preferred path: longest matching normalized server id prefix.
    normalized = sorted(
        ((sid.replace("-", "_"), sid) for sid in server_ids), key=lambda x: len(x[0]), reverse=True
    )
    for safe_id, original_id in normalized:
        prefix = f"mcp_{safe_id}_"
        if tool_name.startswith(prefix):
            remote_safe_name = tool_name[len(prefix) :]
            return original_id, remote_safe_name

    # Legacy fallback if server list is unavailable.
    parts = tool_name.split("_")
    if len(parts) < 3:
        return None, None
    return parts[1], "_".join(parts[2:])


async def _call_mcp_with_name_fallback(session: Any, tool_name: str, args: dict[str, Any]) -> Any:
    """Call MCP tool and retry once with underscore/hyphen variant if tool is not found."""
    try:
        return await session.call_tool(tool_name, arguments=args)
    except Exception as exc:
        error_text = str(exc).lower()
        if "not found" not in error_text and "unknown tool" not in error_text:
            raise
        alt_name: str | None = None
        if "_" in tool_name:
            alt_name = tool_name.replace("_", "-")
        elif "-" in tool_name:
            alt_name = tool_name.replace("-", "_")
        if not alt_name or alt_name == tool_name:
            raise
        logger.warning(
            "Retrying MCP call with alternate tool name",
            original_tool=tool_name,
            alternate_tool=alt_name,
        )
        return await session.call_tool(alt_name, arguments=args)


def _classify_recoverable_error(exc: Exception) -> tuple[bool, str]:
    if isinstance(exc, TimeoutError):
        return True, "timeout"
    lowered = str(exc or "").lower()
    if "exited without result" in lowered:
        return True, "exited_without_result"
    if "stalled" in lowered:
        return True, "stalled"
    if "connection reset" in lowered or "temporarily unavailable" in lowered:
        return True, "transient_io"
    return False, "non_recoverable"


def _sanitize_task_text(text: str, *, limit: int) -> str:
    value = (text or "").strip()
    if not value:
        return ""

    sanitized = value
    for pattern in _SECRET_PATTERNS:
        if pattern.groups < 2:
            sanitized = pattern.sub("[REDACTED_SECRET]", sanitized)
            continue

        def _replace(match: re.Match[str]) -> str:
            groups = match.groups()
            if len(groups) >= 4:
                return f"{groups[0]}{groups[1]}{groups[2]}[REDACTED_SECRET]{groups[2]}"
            return f"{groups[0]}[REDACTED_SECRET]"

        sanitized = pattern.sub(_replace, sanitized)

    if len(sanitized) <= limit:
        return sanitized
    return f"{sanitized[:limit]}..."


def _attempt_timeout_seconds(base_timeout: int, attempt: int, tools: list[str]) -> float:
    """Compute per-attempt timeout budget with a gentle boost for slow/network-heavy workloads."""
    timeout = float(max(10, int(base_timeout)))
    lowered_tools = {str(t).lower() for t in tools}
    if any(
        marker in name
        for name in lowered_tools
        for marker in ("mcp_", "web_", "browser", "fetch", "search", "crawl")
    ):
        timeout *= 1.25
    if attempt > 1:
        timeout *= 1.2
    return min(timeout, 1800.0)


def _normalize_name_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item).strip().lower()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _normalize_permission_names(value: object) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in _normalize_name_list(value):
        canonical = _PERMISSION_ALIASES.get(item, item)
        if canonical in seen:
            continue
        seen.add(canonical)
        normalized.append(canonical)
    return normalized


def _capability_types(capabilities: list[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for capability in capabilities:
        cap_type = str(getattr(capability, "type", "")).strip().lower()
        if not cap_type or cap_type in seen:
            continue
        seen.add(cap_type)
        out.append(cap_type)
    return out


def _effective_template_tool_names(value: object) -> list[str]:
    return [
        tool_name
        for tool_name in _normalize_name_list(value)
        if tool_name not in _WORKER_BLOCKED_TOOL_NAMES
    ]


def _resolve_effective_worker_tools(
    *,
    template_tools: object,
    requested_tools: object,
) -> list[str]:
    effective_template_tools = _effective_template_tool_names(template_tools)
    if requested_tools is None:
        return effective_template_tools

    normalized_requested = [
        tool_name
        for tool_name in _normalize_name_list(requested_tools)
        if tool_name not in _WORKER_BLOCKED_TOOL_NAMES
    ]
    allowed = set(effective_template_tools)
    unexpected = sorted(tool_name for tool_name in normalized_requested if tool_name not in allowed)
    if unexpected:
        raise ValueError(
            "requested tools exceed template contract "
            f"({', '.join(unexpected)}); update the worker template instead"
        )
    return normalized_requested


def _validate_worker_tool_permissions(
    *,
    tool_names: list[str],
    allowed_permissions: list[str],
    all_tools_by_name: dict[str, Any],
) -> str | None:
    allowed = set(_normalize_permission_names(allowed_permissions))
    unknown_tools: list[str] = []
    missing_permissions: list[tuple[str, str]] = []
    for tool_name in tool_names:
        spec_tool = all_tools_by_name.get(str(tool_name).strip().lower())
        if spec_tool is None:
            unknown_tools.append(str(tool_name))
            continue
        permission = str(getattr(spec_tool, "permission", "") or "").strip().lower()
        if permission and permission not in allowed:
            missing_permissions.append((str(tool_name), permission))
    if unknown_tools:
        return f"unknown worker tool(s): {', '.join(unknown_tools)}"
    if missing_permissions:
        requirements = "; ".join(
            f"tool '{tool_name}' requires permission '{permission}'"
            for tool_name, permission in missing_permissions
        )
        unique_permissions = sorted({permission for _, permission in missing_permissions})
        return (
            f"{requirements}; missing permission(s): {', '.join(unique_permissions)}; "
            f"worker only has {sorted(allowed)}"
        )
    return None


def _validate_worker_local_tool_call(
    *,
    spec: WorkerSpec,
    tool_name: object,
    permission: str,
) -> str | None:
    normalized_tool_name = str(tool_name or "").strip().lower()
    allowed_tools = set(_normalize_name_list(spec.available_tools))
    if normalized_tool_name not in allowed_tools and not _allows_injected_worker_tool(
        spec,
        normalized_tool_name,
        allowed_tools=allowed_tools,
    ):
        return f"Worker tool '{normalized_tool_name}' is not allowed by this worker spec."
    normalized_permission = str(permission or "").strip().lower()
    allowed_permissions = set(_normalize_permission_names(spec.effective_permissions))
    if normalized_permission and normalized_permission not in allowed_permissions:
        return (
            f"Worker tool '{normalized_tool_name}' requires permission '{normalized_permission}' "
            "which is not granted to this worker."
        )
    return None


def _allows_injected_worker_tool(
    spec: WorkerSpec,
    normalized_tool_name: str,
    *,
    allowed_tools: set[str] | None = None,
) -> bool:
    """Mirror agent-worker injected tools at the runtime bridge boundary."""
    if normalized_tool_name not in _INJECTED_ORCHESTRATION_TOOL_NAMES:
        return False
    tools = (
        allowed_tools
        if allowed_tools is not None
        else set(_normalize_name_list(spec.available_tools))
    )
    return bool(tools & _CHILD_SPAWN_TOOL_NAMES)


def _validate_worker_mcp_tool_call(
    *,
    spec: WorkerSpec,
    server_id: object,
    tool_name: object,
) -> str | None:
    requested_server = str(server_id or "").strip()
    requested_tool_name = str(tool_name or "").strip().lower()
    allowed_permissions = set(_normalize_permission_names(spec.effective_permissions))

    for mcp_tool in spec.mcp_tools:
        candidate_name = str(mcp_tool.get("name", "") or "").strip().lower()
        candidate_remote_name = str(mcp_tool.get("remote_tool_name", "") or "").strip().lower()
        candidate_server = str(mcp_tool.get("server_id", "") or "").strip()
        if candidate_server != requested_server:
            continue
        if requested_tool_name not in {candidate_name, candidate_remote_name}:
            continue
        permission = str(mcp_tool.get("permission", "") or "").strip().lower()
        if permission and permission not in allowed_permissions:
            return (
                f"Worker MCP tool '{requested_tool_name}' requires permission '{permission}' "
                "which is not granted to this worker."
            )
        return None

    return (
        f"Worker MCP tool '{requested_tool_name}' on server '{requested_server}' "
        "is not allowed by this worker spec."
    )
