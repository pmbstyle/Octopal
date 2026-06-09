from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import dataclass
from typing import Any

import structlog

from octopal.infrastructure.observability.helpers import safe_preview
from octopal.runtime.octo.delivery import resolve_user_delivery
from octopal.runtime.workers.contracts import WorkerResult

logger = structlog.get_logger(__name__)

_MCP_LONG_TASK_INITIAL_DELAY_SECONDS = 4.0
_MCP_LONG_TASK_POLL_INTERVAL_SECONDS = 6.0
_MCP_LONG_TASK_MAX_POLLS = 80

_PHONE_SERVER_IDS = {"glm_cellphone", "glm-cellphone"}
_PHONE_START_TOOL = "start_phone_task"
_PHONE_STATUS_TOOL = "get_phone_task_status"
_PHONE_RESULT_TOOL = "get_phone_task_result"
_PHONE_PENDING_STATUSES = {
    "created",
    "pending",
    "queued",
    "running",
    "in_progress",
    "working",
    "processing",
    "started",
}
_PHONE_DONE_STATUSES = {"completed", "complete", "done", "finished", "success", "succeeded"}
_PHONE_FAILED_STATUSES = {"failed", "failure", "error", "cancelled", "canceled", "timeout"}


@dataclass(frozen=True)
class _PendingMCPTask:
    server_id: str
    task_id: str
    task_id_key: str
    status_tool: str
    result_tool: str
    correlation_id: str | None


def maybe_track_mcp_long_task(
    *,
    octo: Any,
    chat_id: int,
    correlation_id: str | None,
    tool_name: str | None,
    args: dict[str, Any],
    result: Any,
    server_id: str | None = None,
    remote_tool_name: str | None = None,
) -> bool:
    """Track MCP tools that start long-running remote work and need a later result fetch."""
    if octo is None:
        return False
    identity = _resolve_mcp_identity(
        tool_name=tool_name,
        args=args,
        server_id=server_id,
        remote_tool_name=remote_tool_name,
    )
    if identity is None:
        return False
    resolved_server_id, resolved_tool_name = identity
    if _normalize_name(resolved_server_id) not in _PHONE_SERVER_IDS:
        return False

    payload = _coerce_payload(result)
    if resolved_tool_name == _PHONE_RESULT_TOOL:
        task_ref = _extract_task_ref(args, payload)
        if task_ref is not None:
            _cancel_phone_task_poll(octo, chat_id, resolved_server_id, task_ref[0], correlation_id)
        return False

    if resolved_tool_name not in {_PHONE_START_TOOL, _PHONE_STATUS_TOOL}:
        return False

    task_ref = _extract_task_ref(args, payload)
    if task_ref is None:
        logger.info(
            "Skipping MCP long-task tracking without task/job id",
            server_id=resolved_server_id,
            tool=resolved_tool_name,
            result_preview=safe_preview(payload, limit=240),
        )
        return False
    task_id, task_id_key = task_ref

    status = _extract_status(payload)
    if resolved_tool_name == _PHONE_STATUS_TOOL and status in _PHONE_FAILED_STATUSES:
        return False

    if status and status not in _PHONE_PENDING_STATUSES and status not in _PHONE_DONE_STATUSES:
        return False

    _mark_followup_required(octo, correlation_id)
    _schedule_phone_task_poll(
        octo,
        chat_id=chat_id,
        task=_PendingMCPTask(
            server_id=resolved_server_id,
            task_id=task_id,
            task_id_key=task_id_key,
            status_tool=_PHONE_STATUS_TOOL,
            result_tool=_PHONE_RESULT_TOOL,
            correlation_id=correlation_id,
        ),
        immediate=status in _PHONE_DONE_STATUSES,
    )
    return True


def _resolve_mcp_identity(
    *,
    tool_name: str | None,
    args: dict[str, Any],
    server_id: str | None,
    remote_tool_name: str | None,
) -> tuple[str, str] | None:
    if server_id and remote_tool_name:
        return str(server_id), _normalize_name(remote_tool_name)

    if _normalize_name(tool_name) == "mcp_call":
        raw_server = str(args.get("server_id") or "").strip()
        raw_tool = str(args.get("tool_name") or "").strip()
        if raw_server and raw_tool:
            return raw_server, _normalize_name(raw_tool)
        return None

    normalized_tool_name = _normalize_name(tool_name)
    prefix = "mcp_glm_cellphone_"
    if normalized_tool_name.startswith(prefix):
        return "glm_cellphone", normalized_tool_name[len(prefix) :]
    return None


def _mark_followup_required(octo: Any, correlation_id: str | None) -> None:
    marker = getattr(octo, "mark_structured_followup_required", None)
    if not callable(marker):
        return
    marker(correlation_id)


def _schedule_phone_task_poll(
    octo: Any,
    *,
    chat_id: int,
    task: _PendingMCPTask,
    immediate: bool = False,
) -> None:
    if chat_id == 0 or not task.task_id:
        return
    tasks = getattr(octo, "_pending_mcp_long_tasks", None)
    if tasks is None:
        tasks = {}
        octo._pending_mcp_long_tasks = tasks
    key = (chat_id, task.server_id, task.task_id)
    existing = tasks.get(key)
    if existing is not None and not existing.done():
        return
    tasks[key] = asyncio.create_task(
        _poll_phone_task(octo, chat_id=chat_id, task=task, key=key, immediate=immediate)
    )


def _cancel_phone_task_poll(
    octo: Any,
    chat_id: int,
    server_id: str,
    task_id: str,
    correlation_id: str | None,
) -> None:
    tasks = getattr(octo, "_pending_mcp_long_tasks", None)
    if isinstance(tasks, dict):
        existing = tasks.pop((chat_id, server_id, task_id), None)
        if existing is not None and not existing.done():
            existing.cancel()
    clearer = getattr(octo, "clear_pending_conversational_closure", None)
    if callable(clearer):
        clearer(correlation_id)


async def _poll_phone_task(
    octo: Any,
    *,
    chat_id: int,
    task: _PendingMCPTask,
    key: tuple[int, str, str],
    immediate: bool,
) -> None:
    try:
        if not immediate:
            await asyncio.sleep(_MCP_LONG_TASK_INITIAL_DELAY_SECONDS)

        for attempt in range(1, _MCP_LONG_TASK_MAX_POLLS + 1):
            status_payload = await _call_mcp_json(
                octo,
                task.server_id,
                task.status_tool,
                {task.task_id_key: task.task_id},
            )
            status = _extract_status(status_payload)
            logger.info(
                "Polled MCP long-running task",
                chat_id=chat_id,
                server_id=task.server_id,
                task_id=task.task_id,
                status=status or "unknown",
                attempt=attempt,
            )
            if status in _PHONE_FAILED_STATUSES:
                await _route_mcp_task_result_to_octo(
                    octo,
                    chat_id=chat_id,
                    task=task,
                    status="failed",
                    payload=status_payload,
                )
                return
            if status in _PHONE_DONE_STATUSES or _payload_has_result(status_payload):
                result_payload = await _call_mcp_json(
                    octo,
                    task.server_id,
                    task.result_tool,
                    {task.task_id_key: task.task_id},
                )
                await _route_mcp_task_result_to_octo(
                    octo,
                    chat_id=chat_id,
                    task=task,
                    status="completed",
                    payload=result_payload,
                )
                return
            await asyncio.sleep(_MCP_LONG_TASK_POLL_INTERVAL_SECONDS)

        await _route_mcp_task_result_to_octo(
            octo,
            chat_id=chat_id,
            task=task,
            status="failed",
            payload={
                "status": "timeout",
                "message": "MCP long-running task did not complete before the polling budget ended.",
            },
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(
            "Failed to poll MCP long-running task",
            chat_id=chat_id,
            server_id=task.server_id,
            task_id=task.task_id,
        )
    finally:
        tasks = getattr(octo, "_pending_mcp_long_tasks", None)
        if isinstance(tasks, dict):
            tasks.pop(key, None)
        clearer = getattr(octo, "clear_pending_conversational_closure", None)
        if callable(clearer):
            clearer(task.correlation_id)


async def _call_mcp_json(
    octo: Any,
    server_id: str,
    tool_name: str,
    args: dict[str, Any],
) -> Any:
    manager = getattr(octo, "mcp_manager", None)
    if manager is None:
        raise RuntimeError("MCP manager is not available.")
    result = await manager.call_tool(server_id, tool_name, args)
    return _coerce_payload(
        [c.model_dump() if hasattr(c, "model_dump") else str(c) for c in result.content]
    )


async def _route_mcp_task_result_to_octo(
    octo: Any,
    *,
    chat_id: int,
    task: _PendingMCPTask,
    status: str,
    payload: Any,
) -> None:
    result_status = "completed" if status == "completed" else "failed"
    result = WorkerResult(
        status=result_status,
        summary=f"MCP long-running task {result_status}.",
        output={
            "source": "mcp_long_task",
            "server_id": task.server_id,
            "task_id": task.task_id,
            "task_id_key": task.task_id_key,
            "status_tool": task.status_tool,
            "result_tool": task.result_tool,
            "payload": payload,
        },
        tools_used=[task.status_tool, task.result_tool],
    )
    add_message = getattr(getattr(octo, "memory", None), "add_message", None)
    if callable(add_message):
        await add_message(
            "system",
            f"MCP long-running task {result_status}: {safe_preview(payload, limit=500)}",
            {
                "chat_id": chat_id,
                "mcp_long_task": True,
                "worker_result": True,
                "correlation_id": task.correlation_id,
                "server_id": task.server_id,
                "task_id": task.task_id,
            },
        )

    route = _route_worker_results_back_to_octo_callable()
    if callable(route):
        try:
            routed_text = await route(
                octo,
                chat_id,
                [(f"mcp:{task.server_id}", _mcp_task_text(task), result)],
            )
        except Exception:
            logger.exception(
                "Failed to route MCP long-running task result through Octo",
                chat_id=chat_id,
                server_id=task.server_id,
                task_id=task.task_id,
            )
            routed_text = _format_result_text(payload)
    else:
        routed_text = _format_result_text(payload)

    await _deliver_mcp_octo_reply(
        octo,
        chat_id=chat_id,
        correlation_id=task.correlation_id,
        text=routed_text,
    )


def _route_worker_results_back_to_octo_callable():
    core_module = sys.modules.get("octopal.runtime.octo.core")
    if core_module is None:
        return None
    return getattr(core_module, "route_worker_results_back_to_octo", None)


def _mcp_task_text(task: _PendingMCPTask) -> str:
    return (
        f"Handle completed long-running MCP task from server '{task.server_id}'. "
        "Decide whether to send the user a clean channel response."
    )


async def _deliver_mcp_octo_reply(
    octo: Any,
    *,
    chat_id: int,
    correlation_id: str | None,
    text: str,
) -> None:
    decision = resolve_user_delivery(text)
    if not decision.user_visible:
        logger.info(
            "MCP long-running task produced no user-visible follow-up",
            chat_id=chat_id,
            reason=decision.reason,
        )
        return
    sender = getattr(octo, "internal_send", None)
    if not callable(sender):
        logger.info(
            "MCP long-running task produced a reply but no sender is attached",
            chat_id=chat_id,
            text_len=len(decision.text),
        )
        return
    await sender(chat_id, decision.text)
    note_delivery = getattr(octo, "note_user_visible_delivery", None)
    if callable(note_delivery):
        note_delivery(chat_id, decision.text)
    clearer = getattr(octo, "clear_pending_conversational_closure", None)
    if callable(clearer):
        clearer(correlation_id)
    memory = getattr(octo, "memory", None)
    add_message = getattr(memory, "add_message", None)
    if callable(add_message):
        await add_message(
            "assistant",
            decision.text,
            {
                "chat_id": chat_id,
                "background_delivery": True,
                "mcp_long_task": True,
                "correlation_id": correlation_id,
            },
        )


def _format_failure_text(payload: Any) -> str:
    message = _first_present_text(payload, ("error", "message", "reason", "details"))
    if message:
        return f"Phone task failed: {message}"
    return "Phone task failed before a result was available."


def _format_result_text(payload: Any) -> str:
    if isinstance(payload, str):
        return payload.strip()
    text = _first_present_text(
        payload,
        (
            "message",
            "answer",
            "final_result",
            "result",
            "output",
            "content",
            "data",
            "text",
        ),
    )
    if text:
        return text
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _coerce_payload(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return {}
        try:
            return _coerce_payload(json.loads(stripped))
        except Exception:
            return stripped
    if isinstance(value, list):
        if len(value) == 1:
            return _coerce_payload(value[0])
        return [_coerce_payload(item) for item in value]
    if isinstance(value, dict):
        if "text" in value and len(value) <= 3:
            parsed_text = _coerce_payload(value.get("text"))
            if parsed_text != value.get("text"):
                return parsed_text
        return {str(key): _coerce_payload(val) for key, val in value.items()}
    return value


def _extract_task_ref(args: dict[str, Any], payload: Any) -> tuple[str, str] | None:
    for source in (args, payload):
        found = _find_key_with_name(source, ("task_id", "taskId", "job_id", "jobId", "id"))
        if found is None:
            continue
        key, value = found
        task_id = str(value).strip()
        if not task_id:
            continue
        if _normalize_name(key) == "job_id":
            return task_id, "job_id"
        return task_id, "task_id"
    return None


def _extract_status(payload: Any) -> str:
    value = _find_key(payload, ("status", "state"))
    return _normalize_name(value)


def _payload_has_result(payload: Any) -> bool:
    return bool(
        _find_key(
            payload,
            ("result", "final_result", "answer", "output"),
        )
    )


def _first_present_text(payload: Any, keys: tuple[str, ...]) -> str:
    value = _find_key(payload, keys)
    if value is None:
        return ""
    value = _coerce_payload(value)
    if isinstance(value, dict):
        nested = _first_present_text(
            value,
            (
                "message",
                "answer",
                "final_result",
                "result",
                "output",
                "content",
                "data",
                "text",
            ),
        )
        if nested:
            return nested
    if isinstance(value, str):
        return value.strip()
    return json.dumps(value, ensure_ascii=False, indent=2)


def _find_key(value: Any, keys: tuple[str, ...]) -> Any:
    found = _find_key_with_name(value, keys)
    if found is None:
        return None
    return found[1]


def _find_key_with_name(value: Any, keys: tuple[str, ...]) -> tuple[str, Any] | None:
    if isinstance(value, dict):
        lowered = {str(key).lower(): (str(key), val) for key, val in value.items()}
        for key in keys:
            if key.lower() in lowered:
                return lowered[key.lower()]
        for item in value.values():
            found = _find_key_with_name(item, keys)
            if found is not None:
                return found
    if isinstance(value, list):
        for item in value:
            found = _find_key_with_name(item, keys)
            if found is not None:
                return found
    return None


def _normalize_name(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")
