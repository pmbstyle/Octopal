from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import dataclass, field
from typing import Any

from broodmind.intents.registry import canonical_json, normalize_payload
from broodmind.intents.types import IntentRequest
from broodmind.policy.permits import Permit
from broodmind.workers.contracts import KnowledgeProposal, WorkerResult, WorkerSpec


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
        await self._write_message(
            {"type": "intent_request", "intent": request.model_dump()}
        )
        response = await self._read_message()
        if response.get("type") == "permit_denied":
            reason = response.get("reason", "denied")
            raise RuntimeError(f"Intent denied: {reason}")
        if response.get("type") != "permit":
            raise RuntimeError("Unexpected response from Queen")

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

    async def call_mcp_tool(self, server_id: str, tool_name: str, arguments: dict[str, Any]) -> Any:
        await self._write_message({
            "type": "mcp_call",
            "server_id": server_id,
            "tool_name": tool_name,
            "arguments": arguments
        })
        response = await self._read_message()
        if response.get("type") == "error":
            raise RuntimeError(response.get("message", "Unknown MCP error"))
        return response.get("result")

    async def call_queen_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        await self._write_message(
            {
                "type": "queen_tool_call",
                "tool_name": tool_name,
                "arguments": arguments,
            }
        )
        response = await self._read_message()
        if response.get("type") != "queen_tool_result":
            raise RuntimeError("Unexpected response from Queen tool bridge")
        if not bool(response.get("ok", False)):
            raise RuntimeError(str(response.get("error") or "Queen tool call failed"))
        return response.get("result")

    async def _write_message(self, payload: dict[str, Any]) -> None:
        line = json.dumps(payload)
        sys.stdout.write(line + "\n")
        sys.stdout.flush()

    async def _read_message(self) -> dict[str, Any]:
        line = await asyncio.to_thread(sys.stdin.readline)
        if not line:
            raise RuntimeError("No response from Queen")
        return json.loads(line)


def _hash_payload(payload: dict[str, Any]) -> str:
    return __import__("hashlib").sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _read_text(path: str) -> str:
    return __import__("pathlib").Path(path).read_text(encoding="utf-8")
