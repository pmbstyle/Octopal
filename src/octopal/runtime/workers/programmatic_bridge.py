from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, cast

from octopal.runtime.workers.contracts import WorkerSpec
from octopal.tools.programmatic_execution import (
    ProgrammaticReadBatchError,
    ProgrammaticReadBatchLimits,
    ProgrammaticReadBatchResult,
    ProgrammaticReadCall,
    execute_programmatic_read_batch,
)
from octopal.tools.registry import ToolSpec

_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$")


@dataclass(frozen=True)
class ProgrammaticReadBridgeOutcome:
    response: dict[str, Any]
    consumed_calls: int


async def handle_programmatic_read_bridge_request(
    *,
    spec: WorkerSpec,
    payload: Mapping[str, Any],
    remaining_calls: int,
    ctx: Mapping[str, Any] | None = None,
    tools: Iterable[ToolSpec] | None = None,
) -> ProgrammaticReadBridgeOutcome:
    """Bind a worker batch to its declared inventory, permissions, and run budget."""
    declared_budget = int(spec.programmatic_read_call_budget)
    remaining_calls = max(0, min(int(remaining_calls), declared_budget))
    request_id = safe_programmatic_read_request_id(payload)
    if not request_id:
        return _error_outcome("", "request_id_invalid", remaining_calls=remaining_calls)
    if remaining_calls <= 0:
        return _error_outcome(
            request_id,
            "call_budget_exhausted",
            remaining_calls=0,
        )

    raw_calls = payload.get("calls")
    if not isinstance(raw_calls, list):
        return _error_outcome(
            request_id,
            "calls_not_array",
            remaining_calls=remaining_calls,
        )
    if len(raw_calls) > remaining_calls:
        return _error_outcome(
            request_id,
            "call_budget_exhausted",
            remaining_calls=0,
            consumed_calls=remaining_calls,
            details=(
                f"requested_calls={len(raw_calls)}",
                f"remaining_calls={remaining_calls}",
            ),
        )

    reserved_calls = len(raw_calls)
    remaining_after = remaining_calls - reserved_calls
    parsed_calls: list[ProgrammaticReadCall] = []
    for item in raw_calls:
        if not isinstance(item, dict):
            return _error_outcome(
                request_id,
                "call_not_object",
                remaining_calls=remaining_after,
                consumed_calls=reserved_calls,
            )
        call_id = item.get("call_id")
        tool_name = item.get("tool_name")
        arguments = item.get("arguments")
        if not isinstance(call_id, str) or not isinstance(tool_name, str):
            return _error_outcome(
                request_id,
                "call_identity_invalid",
                remaining_calls=remaining_after,
                consumed_calls=reserved_calls,
            )
        if not isinstance(arguments, dict):
            return _error_outcome(
                request_id,
                "call_arguments_not_object",
                remaining_calls=remaining_after,
                consumed_calls=reserved_calls,
            )
        parsed_calls.append(
            ProgrammaticReadCall(
                call_id=call_id,
                tool_name=tool_name.strip().lower(),
                arguments=arguments,
            )
        )

    catalog = list(tools) if tools is not None else _core_tool_catalog()
    catalog_by_name = {str(tool.name).strip().lower(): tool for tool in catalog}
    allowed_names = _normalized_names(spec.available_tools)
    allowed_permissions = _normalized_names(spec.effective_permissions)
    selected_tools: dict[str, ToolSpec] = {}
    for call in parsed_calls:
        if call.tool_name not in allowed_names:
            return _error_outcome(
                request_id,
                "tool_not_in_worker_inventory",
                remaining_calls=remaining_after,
                consumed_calls=reserved_calls,
                details=(f"tool_name={call.tool_name}",),
            )
        tool = catalog_by_name.get(call.tool_name)
        if tool is None:
            return _error_outcome(
                request_id,
                "tool_not_available",
                remaining_calls=remaining_after,
                consumed_calls=reserved_calls,
                details=(f"tool_name={call.tool_name}",),
            )
        permission = str(tool.permission or "").strip().lower()
        if permission and permission not in allowed_permissions:
            return _error_outcome(
                request_id,
                "tool_permission_not_granted",
                remaining_calls=remaining_after,
                consumed_calls=reserved_calls,
                details=(f"tool_name={call.tool_name}", f"permission={permission}"),
            )
        selected_tools[call.tool_name] = tool

    try:
        result = await execute_programmatic_read_batch(
            selected_tools.values(),
            parsed_calls,
            ctx=dict(ctx or {}),
            limits=ProgrammaticReadBatchLimits(max_calls=max(1, remaining_calls)),
        )
    except ProgrammaticReadBatchError as exc:
        return _error_outcome(
            request_id,
            exc.code,
            remaining_calls=remaining_after,
            consumed_calls=reserved_calls,
            details=exc.details,
        )

    return ProgrammaticReadBridgeOutcome(
        response={
            "type": "programmatic_read_batch_result",
            "request_id": request_id,
            "ok": True,
            "remaining_calls": remaining_after,
            "result": _serialize_batch_result(result),
        },
        consumed_calls=reserved_calls,
    )


def safe_programmatic_read_request_id(payload: Mapping[str, Any]) -> str:
    """Return a correlation id only when it is safe to echo to the worker."""
    request_id = payload.get("request_id")
    if not isinstance(request_id, str) or not _REQUEST_ID_PATTERN.fullmatch(request_id):
        return ""
    return request_id


def _core_tool_catalog() -> list[ToolSpec]:
    from octopal.tools.catalog import get_tools

    return cast(list[ToolSpec], get_tools(mcp_manager=None))


def _normalized_names(values: Iterable[object]) -> set[str]:
    return {str(value).strip().lower() for value in values if str(value).strip()}


def _serialize_batch_result(result: ProgrammaticReadBatchResult) -> dict[str, Any]:
    serialized_results: list[dict[str, Any]] = []
    for call_result in result.results:
        item: dict[str, Any] = {
            "call_id": call_result.call_id,
            "tool_name": call_result.tool_name,
            "status": call_result.status,
            "elapsed_ms": call_result.elapsed_ms,
        }
        if call_result.status == "completed":
            item["value"] = call_result.value
            item["byte_count"] = call_result.byte_count
        else:
            item["error_code"] = call_result.error_code
            if call_result.error_details:
                item["error_details"] = list(call_result.error_details)
        serialized_results.append(item)
    return {
        "results": serialized_results,
        "elapsed_ms": result.elapsed_ms,
        "completed_count": result.completed_count,
        "failed_count": result.failed_count,
        "timed_out_count": result.timed_out_count,
    }


def _error_outcome(
    request_id: str,
    code: str,
    *,
    remaining_calls: int,
    consumed_calls: int = 0,
    details: tuple[str, ...] = (),
) -> ProgrammaticReadBridgeOutcome:
    error: dict[str, Any] = {"code": code}
    if details:
        error["details"] = list(details)
    return ProgrammaticReadBridgeOutcome(
        response={
            "type": "programmatic_read_batch_result",
            "request_id": request_id,
            "ok": False,
            "remaining_calls": max(0, remaining_calls),
            "error": error,
        },
        consumed_calls=max(0, consumed_calls),
    )
