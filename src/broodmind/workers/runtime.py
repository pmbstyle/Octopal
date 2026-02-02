"""
Simplified Worker Runtime

Queen creates tasks -> Runtime looks up worker template -> Launches agent worker
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from broodmind.policy.engine import PolicyEngine
from broodmind.store.base import Store
from broodmind.store.models import AuditEvent, WorkerRecord, WorkerTemplateRecord
from broodmind.utils import utc_now
from broodmind.workers.contracts import TaskRequest, WorkerResult, WorkerSpec
from broodmind.workers.launcher import WorkerLauncher

logger = logging.getLogger(__name__)

WORKER_MODULE = "broodmind.workers.agent_worker"


@dataclass
class WorkerRuntime:
    store: Store
    policy: PolicyEngine
    workspace_dir: Path
    launcher: WorkerLauncher
    _running: dict[str, asyncio.subprocess.Process] = field(default_factory=dict)

    async def run_task(self, task_request: TaskRequest) -> WorkerResult:
        """Run a task with the specified worker template."""
        # Get worker template
        template = self.store.get_worker_template(task_request.worker_id)
        if not template:
            return WorkerResult(summary=f"Worker template not found: {task_request.worker_id}")

        # Build capabilities from template permissions
        capabilities = self._build_capabilities(template.required_permissions)

        # Get granted capabilities from policy
        granted = self.policy.grant_capabilities(capabilities)

        if not granted:
            return WorkerResult(summary="Permission denied for worker task")

        # Create worker spec
        worker_id = str(uuid.uuid4())
        spec = WorkerSpec(
            id=worker_id,
            task=task_request.task,
            inputs=task_request.inputs,
            system_prompt=template.system_prompt,
            available_tools=task_request.tools or template.available_tools,
            granted_capabilities=[c.model_dump() for c in granted],
            timeout_seconds=task_request.timeout_seconds or template.default_timeout_seconds,
            max_thinking_steps=template.max_thinking_steps,
            lifecycle="ephemeral",
        )

        # Run worker
        return await self.run(spec)

    async def run(
        self,
        spec: WorkerSpec,
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
        worker_dir.mkdir(parents=True, exist_ok=True)

        # Write spec file
        spec_path = worker_dir / "spec.json"
        spec_path.write_text(json.dumps(spec.model_dump(), indent=2), encoding="utf-8")

        # Create worker record
        now = utc_now()
        self.store.create_worker(
            WorkerRecord(
                id=spec.id,
                status="started",
                task=spec.task,
                granted_caps=spec.granted_capabilities,
                created_at=now,
                updated_at=now,
            )
        )
        self._append_audit(
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
        self.store.update_worker_status(spec.id, "running")
        self._append_audit("worker_started", correlation_id=spec.id)

        try:
            result = await asyncio.wait_for(
                self._read_loop(spec, process),
                timeout=spec.timeout_seconds,
            )
            logger.info("WorkerRuntime result: id=%s summary_len=%s", spec.id, len(result.summary))
            self._append_audit(
                "worker_result",
                correlation_id=spec.id,
                data={"summary": result.summary},
            )
            return result
        except asyncio.TimeoutError:
            logger.error("Worker %s timed out", spec.id)
            process.kill()
            self.store.update_worker_status(spec.id, "failed")
            self.store.update_worker_result(spec.id, error="Worker timed out")
            self._append_audit(
                "worker_failed",
                level="error",
                correlation_id=spec.id,
                data={"reason": "timeout"},
            )
            raise RuntimeError("Worker timed out")
        except Exception as exc:
            self.store.update_worker_status(spec.id, "failed")
            self.store.update_worker_result(spec.id, error=f"Worker failed: {exc}")
            self._append_audit(
                "worker_failed",
                level="error",
                correlation_id=spec.id,
                data={"reason": "exception", "error": str(exc)},
            )
            raise
        finally:
            self._running.pop(spec.id, None)
            if spec.lifecycle == "ephemeral":
                self._cleanup_worker_dir(worker_dir)

    def stop_worker(self, worker_id: str) -> bool:
        """Stop a running worker."""
        process = self._running.get(worker_id)
        if not process:
            return False
        try:
            process.kill()
        except Exception:
            logger.exception("Failed to stop worker: %s", worker_id)
            return False
        self.store.update_worker_status(worker_id, "stopped")
        self._append_audit(
            "worker_stopped",
            level="warning",
            correlation_id=worker_id,
        )
        return True

    async def _read_loop(
        self,
        spec: WorkerSpec,
        process: asyncio.subprocess.Process,
    ) -> WorkerResult:
        """Read worker output."""
        invalid_lines = 0
        max_invalid_lines = 50
        assert process.stdout is not None
        buffer = b""

        def _handle_line(line: bytes) -> WorkerResult | None:
            nonlocal invalid_lines
            payload = _safe_parse_json(line)
            if payload is None:
                text_line = line.decode("utf-8", errors="replace").strip()
                if text_line:
                    logger.error("Worker output (non-JSON): %s", text_line)
                invalid_lines += 1
                if invalid_lines >= max_invalid_lines:
                    logger.error("Worker emitted too many invalid lines")
                    process.kill()
                return None

            msg_type = payload.get("type")
            if msg_type == "log":
                logger.info("Worker %s: %s", spec.id, payload.get("message"))
                return None
            if msg_type == "result":
                result = WorkerResult.model_validate(payload.get("result", {}))
                self.store.update_worker_status(spec.id, "completed")
                self.store.update_worker_result(
                    spec.id,
                    summary=result.summary,
                    output=result.output,
                    error=None,
                )
                return result
            return None

        while True:
            chunk = await process.stdout.read(4096)
            if not chunk:
                break
            buffer += chunk
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                result = _handle_line(line)
                if result is not None:
                    return result
                if invalid_lines >= max_invalid_lines:
                    buffer = b""
                    break
        if buffer.strip():
            result = _handle_line(buffer)
            if result is not None:
                return result

        self.store.update_worker_status(spec.id, "failed")
        self.store.update_worker_result(spec.id, error="Worker exited without result")
        raise RuntimeError("Worker exited without result")

    def _build_capabilities(self, permissions: list[str]) -> list[Any]:
        """Build capability objects from permission strings."""
        from broodmind.workers.contracts import Capability

        caps = []
        for perm in permissions:
            caps.append(Capability(type=perm, scope="worker"))
        return caps

    def _append_audit(
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
        self.store.append_audit(event)

    def _worker_dir(self, worker_id: str) -> Path:
        return self.workspace_dir / "workers" / worker_id

    def _cleanup_worker_dir(self, worker_dir: Path) -> None:
        try:
            if worker_dir.exists():
                shutil.rmtree(worker_dir)
                logger.info("WorkerRuntime cleaned up worker dir: %s", worker_dir)
        except Exception as exc:
            logger.warning("WorkerRuntime cleanup failed: %s", exc)





def _safe_parse_json(line: bytes) -> dict[str, Any] | None:
    try:
        return json.loads(line.decode("utf-8"))
    except Exception:
        return None


def _pythonpath() -> str:
    import sys

    return os.pathsep.join([p for p in sys.path if p])
