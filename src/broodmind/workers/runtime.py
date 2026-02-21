"""
Simplified Worker Runtime

Queen creates tasks -> Runtime looks up worker template -> Launches agent worker
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import structlog
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from broodmind.intents.types import ActionIntent
from broodmind.mcp.manager import MCPManager
from broodmind.policy.engine import PolicyEngine
from broodmind.store.base import Store
from broodmind.store.models import AuditEvent, WorkerRecord
from broodmind.utils import utc_now
from broodmind.workers.contracts import TaskRequest, WorkerResult, WorkerSpec
from broodmind.workers.launcher import WorkerLauncher

logger = structlog.get_logger(__name__)

WORKER_MODULE = "broodmind.workers.agent_worker"


@dataclass
class WorkerRuntime:
    store: Store
    policy: PolicyEngine
    workspace_dir: Path
    launcher: WorkerLauncher
    mcp_manager: MCPManager | None = None
    _running: dict[str, asyncio.subprocess.Process] = field(default_factory=dict)

    async def run_task(
        self,
        task_request: TaskRequest,
        approval_requester: Callable[[ActionIntent], Awaitable[bool]] | None = None,
    ) -> WorkerResult:
        """Run a task with the specified worker template."""
        # Get worker template
        template = await asyncio.to_thread(
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

        # Get all tools to find MCP tool definitions
        from broodmind.tools.tools import get_tools
        all_tools = get_tools(mcp_manager=self.mcp_manager)
        requested_tool_names = list(task_request.tools or template.available_tools)
        
        mcp_tools_data = []
        known_server_ids = list(self.mcp_manager.sessions.keys()) if self.mcp_manager else []
        
        # 1. Add explicitly requested MCP tools
        for tool_name in requested_tool_names:
            if tool_name.startswith("mcp_"):
                # Find the tool spec
                spec_found = next((t for t in all_tools if t.name == tool_name), None)
                if spec_found:
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

        # Create worker spec
        worker_id = task_request.run_id or str(uuid.uuid4())
        spec = WorkerSpec(
            id=worker_id,
            task=task_request.task,
            inputs=task_request.inputs,
            system_prompt=template.system_prompt,
            available_tools=requested_tool_names,
            mcp_tools=mcp_tools_data,
            model=task_request.model or template.model,
            granted_capabilities=[c.model_dump() for c in granted],
            timeout_seconds=task_request.timeout_seconds or template.default_timeout_seconds,
            max_thinking_steps=template.max_thinking_steps,
            run_id=task_request.run_id or worker_id,
            lifecycle="ephemeral",
            correlation_id=task_request.correlation_id,
        )

        # Run worker
        return await self.run(spec, approval_requester=approval_requester)

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
            ),
        )
        await self._append_audit(
            "worker_spawned",
            correlation_id=spec.id,
            data={"task": spec.task[:200]},
        )

        # Build environment
        env = {
            **os.environ,
            "PYTHONPATH": _pythonpath(),
        }

        # Launch worker process
        process = await self.launcher.launch(
            spec_path=str(spec_path.resolve()),
            cwd=str(worker_dir),
            env=env,
        )
        logger.info("WorkerRuntime process started: id=%s pid=%s", spec.id, process.pid)
        self._running[spec.id] = process
        await asyncio.to_thread(self.store.update_worker_status, spec.id, "running")
        await self._append_audit("worker_started", correlation_id=spec.id)

        try:
            result = await asyncio.wait_for(
                self._read_loop(spec, process, approval_requester=approval_requester),
                timeout=spec.timeout_seconds,
            )
            logger.info("WorkerRuntime result: id=%s summary_len=%s", spec.id, len(result.summary))
            await self._append_audit(
                "worker_result",
                correlation_id=spec.id,
                data={"summary": result.summary},
            )
            return result
        except TimeoutError:
            logger.error("Worker %s timed out", spec.id)
            process.kill()
            await process.wait()
            await asyncio.to_thread(self.store.update_worker_status, spec.id, "failed")
            await asyncio.to_thread(self.store.update_worker_result, spec.id, error="Worker timed out")
            await self._append_audit(
                "worker_failed",
                level="error",
                correlation_id=spec.id,
                data={"reason": "timeout"},
            )
            raise RuntimeError("Worker timed out") from None
        except Exception as exc:
            await asyncio.to_thread(self.store.update_worker_status, spec.id, "failed")
            await asyncio.to_thread(
                self.store.update_worker_result, spec.id, error=f"Worker failed: {exc}"
            )
            await self._append_audit(
                "worker_failed",
                level="error",
                correlation_id=spec.id,
                data={"reason": "exception", "error": str(exc)},
            )
            raise
        finally:
            self._running.pop(spec.id, None)
            if spec.lifecycle == "ephemeral":
                await self._cleanup_worker_dir(worker_dir)

    async def stop_worker(self, worker_id: str) -> bool:
        """Stop a running worker."""
        process = self._running.get(worker_id)
        if not process:
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
        max_invalid_lines = 50
        max_buffer_bytes = 256 * 1024
        assert process.stdout is not None
        buffer = b""

        async def _handle_line(line: bytes) -> WorkerResult | None:
            nonlocal invalid_lines, consecutive_invalid_lines
            payload = _safe_parse_json(line)
            if payload is None:
                text_line = line.decode("utf-8", errors="replace").strip()
                if text_line:
                    self._log_non_json_output(text_line)
                invalid_lines += 1
                consecutive_invalid_lines += 1
                if consecutive_invalid_lines >= max_invalid_lines:
                    logger.error("Worker emitted too many invalid lines")
                    process.kill()
                return None
            consecutive_invalid_lines = 0

            msg_type = payload.get("type")
            if msg_type == "log":
                logger.debug("Worker %s: %s", spec.id, payload.get("message"))
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
                    await self._write_to_worker(process, {"type": "error", "message": f"MCP session {server_id} not active."})
                    return None
                
                try:
                    logger.info("Executing MCP call for worker", worker_id=spec.id, server_id=server_id, tool=tool_name)
                    result = await session.call_tool(tool_name, arguments=args)
                    # Convert MCP content objects to something serializable
                    content = [c.model_dump() if hasattr(c, "model_dump") else str(c) for c in result.content]
                    await self._write_to_worker(process, {"type": "mcp_result", "result": content})
                except Exception as e:
                    logger.exception("Worker MCP call failed")
                    await self._write_to_worker(process, {"type": "error", "message": str(e)})
                return None
            if msg_type == "intent_request":
                from broodmind.intents.registry import IntentValidationError, validate_intent
                from broodmind.intents.types import IntentRequest
                from broodmind.store.models import IntentRecord, PermitRecord

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
                result = WorkerResult.model_validate(payload.get("result", {}))
                await asyncio.to_thread(self.store.update_worker_status, spec.id, "completed")
                await asyncio.to_thread(
                    self.store.update_worker_result,
                    spec.id,
                    summary=result.summary,
                    output=result.output,
                    error=None,
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
                    break
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
        from broodmind.workers.contracts import Capability

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

    def _log_non_json_output(self, text: str) -> None:
        """Log non-JSON output from worker intelligently."""
        import re
        # Strip ANSI escape sequences (color codes)
        ansi_escape = re.compile(r'\x1b\[[0-9;]*[mK]')
        clean_text = ansi_escape.sub('', text)
        
        # Keywords that suggest an actual error
        error_keywords = {"error", "exception", "failed", "traceback", "critical"}
        lower_text = clean_text.lower()
        
        if any(kw in lower_text for kw in error_keywords):
            logger.error("Worker output (error?): %s", clean_text)
        elif "info" in lower_text:
            logger.info("Worker output: %s", clean_text)
        elif "debug" in lower_text:
            logger.debug("Worker output: %s", clean_text)
        else:
            # Default to debug for unknown non-JSON output to avoid log noise
            logger.debug("Worker output (non-JSON): %s", clean_text)





def _safe_parse_json(line: bytes) -> dict[str, Any] | None:
    try:
        return json.loads(line.decode("utf-8"))
    except Exception:
        return None


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
            remote_tool_name = remote_safe_name.replace("_", "-")
            return original_id, remote_tool_name

    # Legacy fallback if server list is unavailable.
    parts = tool_name.split("_")
    if len(parts) < 3:
        return None, None
    return parts[1], "_".join(parts[2:])
