from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from octopal.infrastructure.logging import correlation_id_var
from octopal.infrastructure.observability.base import (
    bind_trace_context,
    get_current_trace_context,
    now_ms,
    reset_trace_context,
)
from octopal.infrastructure.observability.helpers import (
    hash_payload,
    safe_preview,
    summarize_exception,
)
from octopal.runtime.octo.mcp_long_tasks import maybe_track_mcp_long_task
from octopal.runtime.octo.tool_policy import (
    _maybe_request_octo_tool_approval,
    _resolve_octo_policy_block,
    _resolve_octo_unavailable_tool,
)
from octopal.runtime.tool_loop import (
    _detect_tool_loop,
    _hash_tool_call,
    _hash_tool_outcome,
)
from octopal.tools.registry import ToolSpec

logger = structlog.get_logger(__name__)
EmitToolUseEvent = Callable[..., Awaitable[None]]


def _stale_chat_turn_tool_payload(
    *, tool_name: str | None, ctx: dict[str, object]
) -> dict[str, Any] | None:
    octo = ctx.get("octo")
    if octo is None:
        return None
    try:
        chat_id = int(ctx.get("chat_id", 0) or 0)
    except Exception:
        chat_id = 0
    if chat_id == 0:
        return None
    epoch = ctx.get("chat_turn_epoch")
    is_current = getattr(octo, "is_chat_turn_epoch_current", None)
    if epoch is not None and callable(is_current) and is_current(chat_id, epoch):
        return None
    if epoch is None:
        correlation_id = str(ctx.get("correlation_id") or correlation_id_var.get() or "").strip()
        is_correlation_current = getattr(octo, "is_correlation_current_for_chat", None)
        if not correlation_id or not callable(is_correlation_current):
            return None
        if is_correlation_current(chat_id, correlation_id):
            return None
    elif not callable(is_current):
        return None

    return {
        "type": "stale_chat_turn",
        "status": "stale",
        "tool": str(tool_name or ""),
        "message": "tool call skipped because a newer chat turn already advanced",
    }


async def _handle_octo_tool_call(
    call: dict,
    tools: list[ToolSpec],
    ctx: dict[str, object],
    *,
    emit_tool_use_event: EmitToolUseEvent | None = None,
) -> tuple[Any, dict[str, Any]]:
    function = call.get("function") or {}
    name = function.get("name")
    args_raw = function.get("arguments", "{}")
    try:
        args = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
    except Exception:
        args = {}

    trace_sink = getattr(ctx.get("octo"), "trace_sink", None)
    parent_trace_ctx = get_current_trace_context()
    tool_trace_ctx = None
    tool_trace_token = None
    tool_started_at_ms = now_ms()
    tool_trace_status = "ok"
    tool_trace_output: dict[str, Any] | None = None
    tool_trace_metadata: dict[str, Any] = {
        "tool_name": str(name or ""),
        "args_hash": hash_payload(args),
        "args_preview": safe_preview(args, limit=240),
    }
    if trace_sink is not None and parent_trace_ctx is not None:
        tool_trace_ctx = await trace_sink.start_span(
            parent_trace_ctx,
            name="octo.tool",
            metadata=tool_trace_metadata,
        )
        tool_trace_token = bind_trace_context(tool_trace_ctx)

    try:
        logger.debug("Octo tool call", tool_name=name, args=args)
        stale_payload = _stale_chat_turn_tool_payload(tool_name=str(name or ""), ctx=ctx)
        if stale_payload is not None:
            tool_trace_status = "error"
            tool_trace_metadata["error_type"] = "stale_chat_turn_epoch"
            tool_trace_output = {
                "result_preview": safe_preview(stale_payload, limit=240),
                "result_size": len(str(stale_payload)),
            }
            logger.info(
                "Octo tool call skipped for stale chat turn",
                tool_name=name,
                chat_id=int(ctx.get("chat_id", 0) or 0),
            )
            return stale_payload, {
                "timed_out": False,
                "had_error": True,
                "error_type": "stale_chat_turn_epoch",
            }
        if emit_tool_use_event is not None:
            await emit_tool_use_event(
                octo=ctx.get("octo"),
                chat_id=int(ctx.get("chat_id", 0) or 0),
                tool_name=str(name or ""),
                args=args if isinstance(args, dict) else {},
            )
        for spec in tools:
            if spec.name == name:
                approval_payload = await _maybe_request_octo_tool_approval(
                    spec=spec,
                    args=args if isinstance(args, dict) else {},
                    ctx=ctx,
                )
                if approval_payload is not None:
                    tool_trace_status = "error"
                    tool_trace_metadata["approval_required"] = True
                    tool_trace_output = {
                        "result_preview": safe_preview(approval_payload, limit=240),
                        "result_size": len(str(approval_payload)),
                    }
                    return approval_payload, {
                        "timed_out": False,
                        "had_error": True,
                        "error_type": "approval_required",
                    }
                try:
                    if spec.is_async:
                        result = spec.handler(args, ctx)
                        if inspect.isawaitable(result):
                            result = await result
                    else:
                        result = await asyncio.to_thread(spec.handler, args, ctx)
                except Exception as exc:
                    tool_trace_status = "error"
                    tool_trace_metadata.update(summarize_exception(exc))
                    logger.exception("Octo tool execution failed", tool_name=name)
                    return {"error": f"Tool execution failed: {name}: {exc}"}, {
                        "timed_out": False,
                        "had_error": True,
                    }
                tool_trace_output = {
                    "result_preview": safe_preview(result, limit=240),
                    "result_size": len(str(result)),
                }
                logger.debug(
                    "Octo tool result", tool_name=name, result_preview=f"{str(result)[:200]}..."
                )
                maybe_track_mcp_long_task(
                    octo=ctx.get("octo"),
                    chat_id=int(ctx.get("chat_id", 0) or 0),
                    correlation_id=str(correlation_id_var.get() or "").strip() or None,
                    tool_name=str(name or ""),
                    args=args if isinstance(args, dict) else {},
                    result=result,
                    server_id=getattr(spec, "server_id", None),
                    remote_tool_name=getattr(spec, "remote_tool_name", None),
                )
                return result, {"timed_out": False, "had_error": False}
        blocked_payload = _resolve_octo_policy_block(tool_name=str(name or ""), ctx=ctx)
        if blocked_payload is not None:
            tool_trace_status = "error"
            tool_trace_metadata["policy_blocked"] = True
            if tool_trace_ctx is not None and trace_sink is not None:
                await trace_sink.annotate(
                    tool_trace_ctx,
                    name="octo.policy_blocked",
                    metadata={
                        "tool_name": str(name or ""),
                        "reason": str(blocked_payload.get("reason") or "blocked_by_policy"),
                        "risk": str(blocked_payload.get("risk") or ""),
                    },
                )
            tool_trace_output = {
                "result_preview": safe_preview(blocked_payload, limit=240),
                "result_size": len(str(blocked_payload)),
            }
            return blocked_payload, {
                "timed_out": False,
                "had_error": True,
                "error_type": "policy_block",
            }
        tool_trace_status = "error"
        tool_trace_metadata["error_type"] = "unknown_tool"
        unavailable_payload = _resolve_octo_unavailable_tool(
            tool_name=str(name or ""),
            active_tools=tools,
            ctx=ctx,
        )
        if unavailable_payload is not None:
            tool_trace_metadata["error_type"] = "tool_unavailable"
            tool_trace_output = {
                "result_preview": safe_preview(unavailable_payload, limit=240),
                "result_size": len(str(unavailable_payload)),
            }
            return unavailable_payload, {
                "timed_out": False,
                "had_error": True,
                "error_type": "tool_unavailable",
            }
        unknown_payload = {"error": f"Unknown tool: {name}"}
        tool_trace_output = {
            "result_preview": safe_preview(unknown_payload, limit=240),
            "result_size": len(str(unknown_payload)),
        }
        return unknown_payload, {"timed_out": False, "had_error": True}
    finally:
        if tool_trace_ctx is not None and trace_sink is not None:
            finish_meta = dict(tool_trace_metadata)
            finish_meta["duration_ms"] = round(now_ms() - tool_started_at_ms, 2)
            await trace_sink.finish_span(
                tool_trace_ctx,
                status=tool_trace_status,
                output=tool_trace_output,
                metadata=finish_meta,
            )
        if tool_trace_token is not None:
            reset_trace_context(tool_trace_token)


def _record_octo_tool_call(
    history: list[dict[str, str]],
    *,
    call: dict[str, Any],
    tool_result: Any,
    tool_meta: dict[str, Any],
    thresholds: dict[str, int],
) -> dict[str, Any] | None:
    function = call.get("function") or {}
    tool_name = str(function.get("name") or "")
    args_raw = function.get("arguments", "{}")
    try:
        tool_args = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
    except Exception:
        tool_args = {}

    args_hash = _hash_tool_call(tool_name, tool_args)
    result_hash = _hash_tool_outcome(tool_result, tool_meta)
    history.append(
        {
            "tool_name": tool_name,
            "args_hash": args_hash,
            "result_hash": result_hash,
        }
    )
    return _detect_tool_loop(
        history,
        tool_name=tool_name,
        args_hash=args_hash,
        warning_threshold=thresholds["warning"],
        critical_threshold=thresholds["critical"],
        global_breaker_threshold=thresholds["global_breaker"],
    )
