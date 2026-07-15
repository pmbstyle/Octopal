from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from mcp import types as mcp_types
from pydantic import BaseModel, ConfigDict, Field

from octopal.infrastructure.store.models import (
    MCPTaskProtocol,
    MCPTaskRecord,
    MCPTaskRemoteStatus,
    MCPTaskRuntimeStatus,
)
from octopal.utils import utc_now

MCP_TASK_EXTENSION_ID = "io.modelcontextprotocol/tasks"
MCP_TASK_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})
_REMOTE_TO_RUNTIME: dict[MCPTaskRemoteStatus, MCPTaskRuntimeStatus] = {
    "working": "running",
    "input_required": "awaiting_instruction",
    "completed": "completed",
    "failed": "failed",
    "cancelled": "stopped",
}


class RawMCPRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    method: str
    params: dict[str, Any] = Field(default_factory=dict)


class RawMCPResult(BaseModel):
    model_config = ConfigDict(extra="allow")


@dataclass(frozen=True)
class MCPTaskContext:
    correlation_id: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    worker_run_id: str | None = None
    chat_id: int | None = None
    chat_turn_id: str | None = None
    plan_run_id: str | None = None
    plan_step_id: str | None = None


@dataclass(frozen=True)
class MCPTaskState:
    task_id: str
    status: MCPTaskRemoteStatus
    created_at: datetime
    updated_at: datetime
    ttl_ms: int | None = None
    poll_interval_ms: int | None = None
    status_message: str | None = None
    input_requests: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None


def client_capability_meta() -> dict[str, Any]:
    return {
        "io.modelcontextprotocol/clientCapabilities": {
            "extensions": {MCP_TASK_EXTENSION_ID: {}},
        }
    }


def extension_declared(capabilities: Any) -> bool:
    payload = _as_dict(capabilities)
    extensions = payload.get("extensions")
    return isinstance(extensions, dict) and MCP_TASK_EXTENSION_ID in extensions


def legacy_tasks_declared(capabilities: Any) -> bool:
    payload = _as_dict(capabilities)
    tasks = payload.get("tasks")
    if not isinstance(tasks, dict):
        return False
    requests = tasks.get("requests")
    if not isinstance(requests, dict):
        return False
    tools = requests.get("tools")
    return isinstance(tools, dict) and isinstance(tools.get("call"), dict)


def parse_task_state(payload: Any, *, protocol: MCPTaskProtocol) -> MCPTaskState:
    data = _as_dict(payload)
    if protocol == "legacy" and isinstance(data.get("task"), dict):
        data = data["task"]

    task_id = str(data.get("taskId") or "").strip()
    status = str(data.get("status") or "").strip().lower()
    if not task_id:
        raise ValueError("MCP task response omitted taskId")
    if status not in _REMOTE_TO_RUNTIME:
        raise ValueError(f"MCP task response has unsupported status: {status or '<missing>'}")

    ttl_key = "ttlMs" if protocol == "extension" else "ttl"
    poll_key = "pollIntervalMs" if protocol == "extension" else "pollInterval"
    return MCPTaskState(
        task_id=task_id,
        status=status,  # type: ignore[arg-type]
        created_at=_parse_datetime(data.get("createdAt")),
        updated_at=_parse_datetime(data.get("lastUpdatedAt")),
        ttl_ms=_optional_non_negative_int(data.get(ttl_key)),
        poll_interval_ms=_optional_non_negative_int(data.get(poll_key)),
        status_message=_optional_text(data.get("statusMessage")),
        input_requests=(
            dict(data["inputRequests"]) if isinstance(data.get("inputRequests"), dict) else None
        ),
        result=dict(data["result"]) if isinstance(data.get("result"), dict) else None,
        error=dict(data["error"]) if isinstance(data.get("error"), dict) else None,
    )


def build_task_record(
    *,
    state: MCPTaskState,
    server_id: str,
    tool_name: str,
    protocol: MCPTaskProtocol,
    auth_context_id: str,
    context: MCPTaskContext,
    previous: MCPTaskRecord | None = None,
) -> MCPTaskRecord:
    now = utc_now()
    record_id = task_record_id(server_id, state.task_id, auth_context_id)
    responded_input_keys = previous.responded_input_keys if previous else []
    input_requests = {
        key: value
        for key, value in (state.input_requests or {}).items()
        if key not in responded_input_keys
    }
    return MCPTaskRecord(
        id=record_id,
        server_id=server_id,
        task_id=state.task_id,
        protocol=protocol,
        remote_status=state.status,
        runtime_status=_REMOTE_TO_RUNTIME[state.status],
        tool_name=tool_name,
        auth_context_id=auth_context_id,
        correlation_id=previous.correlation_id if previous else context.correlation_id,
        trace_id=previous.trace_id if previous else context.trace_id,
        span_id=previous.span_id if previous else context.span_id,
        worker_run_id=previous.worker_run_id if previous else context.worker_run_id,
        chat_id=previous.chat_id if previous else context.chat_id,
        chat_turn_id=previous.chat_turn_id if previous else context.chat_turn_id,
        plan_run_id=previous.plan_run_id if previous else context.plan_run_id,
        plan_step_id=previous.plan_step_id if previous else context.plan_step_id,
        status_message=state.status_message,
        ttl_ms=state.ttl_ms,
        poll_interval_ms=state.poll_interval_ms,
        input_requests=input_requests,
        responded_input_keys=responded_input_keys,
        result=state.result,
        error=state.error,
        remote_created_at=state.created_at,
        remote_updated_at=state.updated_at,
        created_at=previous.created_at if previous else now,
        updated_at=now,
    )


def task_record_id(server_id: str, task_id: str, auth_context_id: str) -> str:
    encoded = f"{server_id}\0{task_id}\0{auth_context_id}".encode()
    return f"mcp_task_{hashlib.sha256(encoded).hexdigest()}"


def task_ref(task_id: str) -> str:
    return hashlib.sha256(task_id.encode("utf-8")).hexdigest()[:16]


def task_expired(record: MCPTaskRecord, *, now: datetime | None = None) -> bool:
    if record.ttl_ms is None:
        return False
    current = now or utc_now()
    return current >= record.remote_created_at + timedelta(milliseconds=record.ttl_ms)


def task_poll_seconds(record: MCPTaskRecord, *, default_seconds: float = 1.0) -> float:
    if record.poll_interval_ms is None:
        return default_seconds
    return max(0.05, record.poll_interval_ms / 1000.0)


def task_status_result(record: MCPTaskRecord) -> mcp_types.CallToolResult:
    payload: dict[str, Any] = {
        "mcp_task": {
            "id": record.id,
            "server_id": record.server_id,
            "protocol": record.protocol,
            "remote_status": record.remote_status,
            "runtime_status": record.runtime_status,
            "status_message": record.status_message,
            "poll_interval_ms": record.poll_interval_ms,
            "expires_at": (
                (record.remote_created_at + timedelta(milliseconds=record.ttl_ms)).isoformat()
                if record.ttl_ms is not None
                else None
            ),
        }
    }
    if record.input_requests:
        payload["mcp_task"]["input_requests"] = record.input_requests
        payload["next_action"] = (
            "The remote task requires trusted input. Surface the request through the normal "
            "instruction or approval path; do not invent a response."
        )
    elif record.remote_status == "working":
        payload["next_action"] = "The durable task is still running and can be resumed later."

    return mcp_types.CallToolResult(
        content=[
            mcp_types.TextContent(
                type="text",
                text=json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            )
        ],
        structuredContent=payload,
        isError=False,
    )


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(mode="json", by_alias=True, exclude_none=True)
        return dict(dumped) if isinstance(dumped, dict) else {}
    return {}


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            raise ValueError("MCP task response omitted a required timestamp")
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _optional_non_negative_int(value: Any) -> int | None:
    if value is None:
        return None
    parsed = int(value)
    if parsed < 0:
        raise ValueError("MCP task duration values must be non-negative")
    return parsed


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
