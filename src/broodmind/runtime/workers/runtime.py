"""
Simplified Worker Runtime

Queen creates tasks -> Runtime looks up worker template -> Launches agent worker
"""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import os
import shutil
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import structlog

from broodmind.infrastructure.mcp.manager import MCPManager
from broodmind.infrastructure.config.settings import Settings
from broodmind.infrastructure.config.models import LLMConfig
from broodmind.infrastructure.store.base import Store
from broodmind.infrastructure.store.models import AuditEvent, WorkerRecord, WorkerTemplateRecord


@dataclass
class WorkerRuntime:
    store: Store
    policy: PolicyEngine
    workspace_dir: Path
    launcher: WorkerLauncher
    settings: Settings
    mcp_manager: MCPManager | None = None
    queen: Any | None = None
    _running: dict[str, asyncio.subprocess.Process] = field(default_factory=dict)

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
            return WorkerResult(summary=f"Worker template not found: {task_request.worker_id}")

        # Build capabilities from template permissions
        capabilities = self._build_capabilities(template.required_permissions)

        # Get granted capabilities from policy
        granted = self.policy.grant_capabilities(capabilities)

        if not granted:
            return WorkerResult(summary="Permission denied for worker task")

        requested_tool_names = list(task_request.tools or template.available_tools)
        has_requested_mcp_tools = any(str(tool_name).startswith("mcp_") for tool_name in requested_tool_names)
        if self.mcp_manager:
            try:
                await self.mcp_manager.ensure_configured_servers_connected(None if has_requested_mcp_tools else [])
            except Exception:
                logger.warning(
                    "Failed to ensure configured MCP servers before worker launch",
                    worker_id=task_request.worker_id,
                    requested_mcp_tools=has_requested_mcp_tools,
                    exc_info=True,
                )

        # Get all tools to find MCP tool definitions
        from broodmind.tools.tools import get_tools
        all_tools = get_tools(mcp_manager=self.mcp_manager)

        mcp_tools_data = []
        known_server_ids = list(self.mcp_manager.sessions.keys()) if self.mcp_manager else []

        # 1. Add explicitly requested MCP tools
        for tool_name in requested_tool_names:
            if tool_name.startswith("mcp_"):
                # Find the tool spec
                spec_found = next((t for t in all_tools if t.name == tool_name), None)
                if spec_found:
                    server_id = getattr(spec_found, "server_id", None)
                    remote_tool_name = getattr(spec_found, "remote_tool_name", None)
                    if not server_id or not remote_tool_name:
                        server_id, remote_tool_name = _extract_mcp_tool_identity(
                            spec_found.name, known_server_ids
                        )
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
        # Workers only receive MCP tools explicitly listed in task_request/tools or template available_tools.

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
            mcp_tools=mcp_tools_data,
            model=task_request.model or template.model,
            llm_config=llm_config,
            granted_capabilities=[c.model_dump() for c in granted],
            timeout_seconds=task_request.timeout_seconds or template.default_timeout_seconds,
            max_thinking_steps=template.max_thinking_steps,
            run_id=task_request.run_id or worker_id,
            lifecycle="ephemeral",
            correlation_id=task_request.correlation_id,
            parent_worker_id=task_request.parent_worker_id,
            lineage_id=task_request.lineage_id,
            root_task_id=task_request.root_task_id,
            spawn_depth=task_request.spawn_depth,
            effective_permissions=list(template.required_permissions),
        )

        # Run worker
        return await self.run(spec, approval_requester=approval_requester)

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
        if not worker_config:
            # Only use worker_llm_default if it has at least provider_id or model set
            # otherwise it might be just an empty default object.
            if config_obj.worker_llm_default.provider_id or config_obj.worker_llm_default.model:
                worker_config = config_obj.worker_llm_default

        # 3. If still none, fallback to Queen's LLM config
        if not worker_config:
            worker_config = config_obj.llm

        # Create a copy to avoid modifying the original config
        resolved = worker_config.model_copy()

        # 4. Apply TaskRequest/Template model override if provided
        # This is strictly a model name override, keeping other provider settings.
        model_override = task_request.model or template.model
        if model_override:
            resolved.model = model_override

        return resolved

    async def run(
        self,
        spec: WorkerSpec,
        approval_requester: Callable[[ActionIntent], Awaitable[bool]] | None = None,
    ) -> WorkerResult:
        """Run a worker with the given spec."""
        logger.info(
            "WorkerRuntime run: id=%s task=%s timeout=%ss tools=%s",
            spec.id,
            spec.task[:100],
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
                "task": spec.task[:200],
                "template_id": spec.template_id,
                "lineage_id": spec.lineage_id,
                "parent_worker_id": spec.parent_worker_id,
                "spawn_depth": spec.spawn_depth,
            },
        )

        # Build environment
        env = {
            **os.environ,
            "PYTHONPATH": _pythonpath(),
        }

        attempts = 0
        max_attempts = 1 + _MAX_RECOVERY_ATTEMPTS
        last_error: Exception | None = None
        result: WorkerResult | None = None

        try:
            while attempts < max_attempts:
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
                    stderr_task = asyncio.create_task(self._read_stderr_loop(spec.id, process_stderr))

                try:
                    result = await asyncio.wait_for(
                        self._read_loop(spec, process, approval_requester=approval_requester),
                        timeout=attempt_timeout,
                    )
                    break
                except Exception as exc:
                    last_error = exc
                    recoverable, reason = _classify_recoverable_error(exc)
                    await self._safe_terminate_process(process)
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
            logger.info("WorkerRuntime result: id=%s summary_len=%s", spec.id, len(result.summary))
            await self._append_audit(
                "worker_result",
                correlation_id=spec.id,
                data={"summary": result.summary, "attempts": attempts, "recovered": attempts > 1},
            )
            return result
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
            if attempts >= max_attempts and last_error is not None:
                raise RuntimeError(f"Worker failed after recovery attempts: {last_error}") from None
            raise
        finally:
            if spec.lifecycle == "ephemeral":
                await self._cleanup_worker_dir(worker_dir)

    async def stop_worker(self, worker_id: str) -> bool:
        """Stop a running worker."""
        process = self._running.get(worker_id)
        if not process:
            worker = await asyncio.to_thread(self.store.get_worker, worker_id)
            if worker and worker.status in {"started", "running"}:
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
            process.kill()
        except Exception:
            logger.exception("Failed to stop worker: %s", worker_id)
            return False
        await asyncio.to_thread(self.store.update_worker_status, worker_id, "stopped")
        await self._append_audit(
            "worker_stopped",
            level="warning",
            correlation_id=worker_id,
        )
        return True

    def is_worker_running(self, worker_id: str) -> bool:
        """Return True if worker process is currently tracked as live in this runtime."""
        process = self._running.get(worker_id)
        if not process:
            return False
        return process.returncode is None

    async def _write_to_worker(self, process: asyncio.subprocess.Process, payload: dict[str, Any]) -> None:
        """Write a JSON message to the worker's stdin."""
        if process.stdin is None:
            logger.error("Worker process has no stdin")
            return
        line = json.dumps(payload) + "\n"
        process.stdin.write(line.encode("utf-8"))
        await process.stdin.drain()

    async def _read_loop(
        self,
        spec: WorkerSpec,
        process: asyncio.subprocess.Process,
        approval_requester: Callable[[ActionIntent], Awaitable[bool]] | None = None,
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
                logger.debug("Worker %s: %s", spec.id, payload.get("message"))
                return None
            if msg_type == "queen_tool_call":
                if not self.queen:
                    await self._write_to_worker(
                        process,
                        {"type": "queen_tool_result", "ok": False, "error": "Queen runtime bridge unavailable."},
                    )
                    return None

                tool_name = str(payload.get("tool_name", "")).strip()
                arguments = payload.get("arguments", {})
                if not isinstance(arguments, dict):
                    arguments = {}

                try:
                    from broodmind.tools.workers.management import get_worker_tools

                    specs = {t.name: t for t in get_worker_tools()}
                    spec_tool = specs.get(tool_name)
                    if spec_tool is None:
                        await self._write_to_worker(
                            process,
                            {"type": "queen_tool_result", "ok": False, "error": f"Unknown queen tool: {tool_name}"},
                        )
                        return None

                    tool_ctx: dict[str, Any] = {
                        "queen": self.queen,
                        "chat_id": 0,
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
                        {"type": "queen_tool_result", "ok": True, "result": result},
                    )
                except Exception as exc:
                    await self._write_to_worker(
                        process,
                        {"type": "queen_tool_result", "ok": False, "error": str(exc)},
                    )
                return None
            if msg_type == "mcp_call":
                server_id = payload.get("server_id")
                tool_name = payload.get("tool_name")
                args = payload.get("arguments", {})

                if not self.mcp_manager:
                    await self._write_to_worker(process, {"type": "error", "message": "MCP Manager not available in runtime."})
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
                    await self._write_to_worker(process, {"type": "error", "message": f"MCP session {server_id} not active."})
                    return None

                try:
                    logger.info("Executing MCP call for worker", worker_id=spec.id, server_id=server_id, tool=tool_name)
                    result = await self.mcp_manager.call_tool(
                        str(server_id),
                        str(tool_name),
                        args,
                        allow_name_fallback=True,
                    )
                    # Convert MCP content objects to something serializable
                    content = [c.model_dump() if hasattr(c, "model_dump") else str(c) for c in result.content]
                    await self._write_to_worker(process, {"type": "mcp_result", "result": content})
                except Exception as e:
                    logger.exception("Worker MCP call failed")
                    payload = e.to_payload() if isinstance(e, ToolBridgeError) else {"type": "error", "message": str(e)}
                    await self._write_to_worker(process, payload)
                return None
            if msg_type == "intent_request":
                from broodmind.infrastructure.store.models import IntentRecord, PermitRecord
                from broodmind.runtime.intents.registry import (
                    IntentValidationError,
                    validate_intent,
                )
                from broodmind.runtime.intents.types import IntentRequest

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
                            )
                        )
                        response = {"type": "permit", "permit": permit.model_dump()}

                    # Send response back to worker
                    await self._write_to_worker(process, response)
                except IntentValidationError as exc:
                    error_resp = {"type": "permit_denied", "reason": f"Intent validation failed: {exc}"}
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
                await asyncio.to_thread(self.store.update_worker_status, spec.id, worker_status)
                await asyncio.to_thread(
                    self.store.update_worker_result,
                    spec.id,
                    summary=result.summary,
                    output=result.output,
                    error=_worker_result_error_text(result) if worker_status == "failed" else None,
                    tools_used=result.tools_used,
                )
                return result
            return None

        while True:
            chunk = await process.stdout.read(4096)
            if not chunk:
                break
            buffer += chunk
            if len(buffer) > max_buffer_bytes and b"\n" not in buffer:
                logger.warning("Worker output buffer exceeded %s bytes without newline", max_buffer_bytes)
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

    def _build_capabilities(self, permissions: list[str]) -> list[Any]:
        """Build capability objects from permission strings."""
        from broodmind.runtime.workers.contracts import Capability

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
        try:
            if await asyncio.to_thread(worker_dir.exists):
                await asyncio.to_thread(shutil.rmtree, worker_dir)
                logger.info("WorkerRuntime cleaned up worker dir: %s", worker_dir)
        except Exception as exc:
            logger.warning("WorkerRuntime cleanup failed: %s", exc)

    async def _safe_terminate_process(self, process: asyncio.subprocess.Process) -> None:
        try:
            if process.returncode is None:
                process.kill()
                await process.wait()
        except Exception:
            logger.debug("Failed to terminate worker process cleanly", exc_info=True)

    def _log_non_json_output(self, text: str) -> None:
        """Log non-JSON output from worker intelligently."""
        self._emit_worker_text_log("stdout", None, text)

    def _emit_worker_text_log(self, source: str, worker_id: str | None, text: str) -> None:
        clean_text = _sanitize_worker_text(text)
        if not clean_text:
            return
        level = _classify_worker_text_log_level(clean_text, source=source)
        if source == "stderr":
            message = f"Worker stderr: id={worker_id} {clean_text}" if worker_id else f"Worker stderr: {clean_text}"
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
    status = raw_result.get("status")
    if status in {"completed", "failed"}:
        repaired["status"] = status
    if output is not None:
        repaired["output"] = output

    questions = raw_result.get("questions")
    if isinstance(questions, list):
        repaired["questions"] = [str(item).strip() for item in questions if str(item).strip()][:20]

    tools_used = raw_result.get("tools_used")
    if isinstance(tools_used, list):
        repaired["tools_used"] = [str(item).strip() for item in tools_used if str(item).strip()][:200]

    thinking_steps = raw_result.get("thinking_steps")
    if isinstance(thinking_steps, int):
        repaired["thinking_steps"] = max(0, thinking_steps)

    knowledge_proposals = raw_result.get("knowledge_proposals")
    if isinstance(knowledge_proposals, list):
        repaired["knowledge_proposals"] = knowledge_proposals

    return repaired


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


def _sanitize_worker_text(text: str) -> str:
    import re

    ansi_escape = re.compile(r"\x1b\[[0-9;]*[mK]")
    clean_text = ansi_escape.sub("", str(text or ""))
    return clean_text.strip()


def _classify_worker_text_log_level(text: str, *, source: str) -> str:
    lowered = (text or "").lower()
    if source == "stderr" and any(token in lowered for token in ("rate limited", "retrying in", "backing off")):
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


def _extract_mcp_tool_identity(tool_name: str, server_ids: list[str]) -> tuple[str | None, str | None]:
    """Best-effort extraction of MCP server and remote tool names from generated tool names."""
    if not tool_name.startswith("mcp_"):
        return None, None
    # Preferred path: longest matching normalized server id prefix.
    normalized = sorted(((sid.replace("-", "_"), sid) for sid in server_ids), key=lambda x: len(x[0]), reverse=True)
    for safe_id, original_id in normalized:
        prefix = f"mcp_{safe_id}_"
        if tool_name.startswith(prefix):
            remote_safe_name = tool_name[len(prefix):]
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
