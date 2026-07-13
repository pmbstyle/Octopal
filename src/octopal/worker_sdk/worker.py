from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from octopal.runtime.intents.registry import canonical_json, normalize_payload
from octopal.runtime.intents.types import IntentRequest
from octopal.runtime.policy.permits import Permit
from octopal.runtime.tool_errors import ToolBridgeError
from octopal.runtime.workers.contracts import KnowledgeProposal, WorkerResult, WorkerSpec


@dataclass
class Worker:
    spec: WorkerSpec
    knowledge_proposals: list[KnowledgeProposal] = field(default_factory=list)

    @classmethod
    def from_spec_file(cls, path: str) -> Worker:
        data = json.loads(_read_text(path))
        spec = WorkerSpec.model_validate(data)
        return cls(spec=spec)

    async def log(self, level: str, message: str) -> None:
        await self._write_message({"type": "log", "level": level, "message": message})

    def add_proposal(self, category: str, content: str) -> None:
        self.knowledge_proposals.append(KnowledgeProposal(category=category, content=content))

    async def request_intent(self, request: IntentRequest) -> Permit:
        await self._write_message({"type": "intent_request", "intent": request.model_dump()})
        response = await self._read_message()
        if response.get("type") == "permit_denied":
            reason = response.get("reason", "denied")
            raise RuntimeError(f"Intent denied: {reason}")
        if response.get("type") != "permit":
            raise RuntimeError("Unexpected response from Octo")

        permit = Permit.model_validate(response.get("permit", {}))
        normalized = normalize_payload(request.type, request.payload)
        payload_hash = _hash_payload(normalized)
        if permit.payload_hash != payload_hash:
            raise RuntimeError("Permit payload hash mismatch")
        return permit

    async def notify_intent_executed(
        self,
        intent_id: str,
        permit_id: str,
        intent_type: str,
        executed_payload: dict[str, Any],
        success: bool,
        result: str | None = None,
    ) -> None:
        normalized = normalize_payload(intent_type, executed_payload)
        payload_hash = _hash_payload(normalized)
        await self._write_message(
            {
                "type": "intent_executed",
                "intent_id": intent_id,
                "permit_id": permit_id,
                "intent_type": intent_type,
                "payload_hash": payload_hash,
                "payload_hash_verified": True,
                "worker_id": self.spec.id,
                "success": success,
                "result": result,
            }
        )

    async def complete(self, result: WorkerResult) -> None:
        await self._write_message({"type": "result", "result": result.model_dump()})

    async def await_children(self, worker_ids: list[str]) -> dict[str, Any]:
        child_ids = [str(worker_id).strip() for worker_id in worker_ids if str(worker_id).strip()]
        if not child_ids:
            raise ValueError("await_children requires at least one worker id")

        await self._write_message({"type": "await_children", "worker_ids": child_ids})
        while True:
            response = await self._read_message()
            response_type = str(response.get("type") or "").strip()
            if response_type == "resume_children":
                payload = response.get("child_batch")
                return payload if isinstance(payload, dict) else {}
            if response_type == "shutdown":
                raise RuntimeError("Worker shutdown requested while waiting for child workers")
            raise RuntimeError(
                f"Unexpected response from Octo while waiting for children: {response_type}"
            )

    async def request_instruction(
        self,
        *,
        question: str,
        context: dict[str, Any] | None = None,
        target: str = "octo",
        timeout_seconds: int = 120,
    ) -> dict[str, Any]:
        clean_question = str(question or "").strip()
        if not clean_question:
            raise ValueError("request_instruction requires a question")
        clean_target = str(target or "octo").strip().lower()
        if clean_target not in {"octo", "parent"}:
            clean_target = "octo"
        request_id = f"{self.spec.id}-instruction-{uuid.uuid4().hex[:12]}"
        await self._write_message(
            {
                "type": "instruction_request",
                "request_id": request_id,
                "target": clean_target,
                "question": clean_question,
                "context": context if isinstance(context, dict) else {},
                "timeout_seconds": max(1, int(timeout_seconds or 120)),
            }
        )
        while True:
            response = await self._read_message()
            response_type = str(response.get("type") or "").strip()
            if response_type == "resume_instruction":
                if str(response.get("request_id") or "") != request_id:
                    raise RuntimeError("Unexpected instruction response id from Octo")
                return response
            if response_type == "shutdown":
                raise RuntimeError("Worker shutdown requested while waiting for instruction")
            raise RuntimeError(
                f"Unexpected response from Octo while waiting for instruction: {response_type}"
            )

    async def call_mcp_tool(self, server_id: str, tool_name: str, arguments: dict[str, Any]) -> Any:
        await self._write_message(
            {
                "type": "mcp_call",
                "server_id": server_id,
                "tool_name": tool_name,
                "arguments": arguments,
            }
        )
        response = await self._read_message()
        if response.get("type") == "error":
            raise ToolBridgeError.from_payload(response, default_bridge="mcp")
        return response.get("result")

    async def call_octo_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        await self._write_message(
            {
                "type": "octo_tool_call",
                "tool_name": tool_name,
                "arguments": arguments,
            }
        )
        response = await self._read_message()
        if response.get("type") != "octo_tool_result":
            raise RuntimeError("Unexpected response from Octo tool bridge")
        if not bool(response.get("ok", False)):
            raise ToolBridgeError(
                str(response.get("error") or "Octo tool call failed"),
                bridge="octo",
                tool_name=tool_name,
            )
        return response.get("result")

    async def programmatic_read_batch(self, calls: list[dict[str, Any]]) -> dict[str, Any]:
        """Call the host's inventory-bound read-only batch bridge."""
        request_id = f"prb-{uuid.uuid4().hex}"
        await self._write_message(
            {
                "type": "programmatic_read_batch",
                "request_id": request_id,
                "calls": calls,
            }
        )
        response = await self._read_message()
        if response.get("type") != "programmatic_read_batch_result":
            raise RuntimeError("Unexpected response from programmatic read bridge")
        if response.get("request_id") != request_id:
            raise RuntimeError("Programmatic read bridge response id mismatch")
        if not bool(response.get("ok", False)):
            error = response.get("error")
            error_obj = error if isinstance(error, dict) else {}
            code = str(error_obj.get("code") or "programmatic_read_failed")
            details = error_obj.get("details")
            raise ToolBridgeError(
                code,
                bridge="programmatic_read",
                classification=code,
                retryable=False,
                details={"reasons": details} if isinstance(details, list) else None,
            )
        result = response.get("result")
        return result if isinstance(result, dict) else {}

    async def _write_message(self, payload: dict[str, Any]) -> None:
        line = json.dumps(payload)
        sys.stdout.write(line + "\n")
        sys.stdout.flush()

    async def _read_message(self) -> dict[str, Any]:
        line = await asyncio.to_thread(sys.stdin.readline)
        if not line:
            raise RuntimeError("No response from Octo")
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise RuntimeError("Invalid response from Octo")
        return payload


def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")
