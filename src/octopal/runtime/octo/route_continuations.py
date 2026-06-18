from __future__ import annotations

import json
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from octopal.infrastructure.observability.helpers import safe_preview
from octopal.infrastructure.providers.base import Message
from octopal.runtime.capability_outcomes import extract_capability_outcome
from octopal.runtime.octo.route_loop_helpers import _parse_tool_result_payload
from octopal.runtime.octo.route_verification import _messages_to_text
from octopal.runtime.tool_payloads import render_tool_result_for_llm
from octopal.tools.registry import ToolSpec

logger = structlog.get_logger(__name__)

_HandleToolCallFn = Callable[
    [dict[str, Any], list[ToolSpec], dict[str, object]],
    Awaitable[tuple[Any, dict[str, Any]]],
]


async def _continue_after_tool_budget_exhaustion(
    *,
    octo: Any,
    chat_id: int,
    notify_user: object,
    user_text: str,
    messages: list[Message | dict[str, Any]],
) -> str | None:
    if chat_id == 0 or octo is None or not hasattr(octo, "handle_message"):
        return None
    if str(user_text or "").count("Runtime continuation after tool budget exhaustion") >= 2:
        logger.warning("Tool-budget continuation limit reached", chat_id=chat_id)
        return None

    from octopal.tools.catalog import _tool_octo_continue_from_control_route

    context_summary = (
        "The previous route exhausted its tool-call budget after executing tools. "
        "Do not ask the user to say continue merely because the route budget ended. "
        "Use the current evidence, inspect persisted runtime state if needed, repair "
        "recoverable issues, and complete or mark the task with a real blocked/user-input state.\n\n"
        "<previous_route_context>\n"
        f"{safe_preview(_messages_to_text(messages), limit=12000)}\n"
        "</previous_route_context>"
    )
    try:
        payload_raw = await _tool_octo_continue_from_control_route(
            {
                "task": (
                    "Runtime continuation after tool budget exhaustion. Continue the original "
                    "user request autonomously and finish the remaining work end-to-end. "
                    "If the work cannot be completed, leave durable runtime state that explains "
                    "the real blocker instead of asking for a generic continuation."
                ),
                "context_summary": context_summary,
                "notify_user": notify_user,
            },
            {
                "octo": octo,
                "chat_id": chat_id,
                "control_route_notify_user": notify_user,
            },
        )
    except Exception:
        logger.exception("Tool-budget autonomous continuation failed", chat_id=chat_id)
        return None

    payload = _parse_tool_result_payload(payload_raw)
    if not isinstance(payload, dict):
        logger.warning(
            "Tool-budget autonomous continuation returned non-object payload",
            chat_id=chat_id,
        )
        return None
    status = str(payload.get("status") or "").strip().lower()
    if status != "continued":
        logger.warning(
            "Tool-budget autonomous continuation did not complete",
            chat_id=chat_id,
            status=status or None,
        )
        return None

    delivered = bool(payload.get("delivered"))
    continuation_notifies_user = bool(payload.get("notify_user"))
    logger.info(
        "Tool-budget route continued autonomously",
        chat_id=chat_id,
        delivered=delivered,
        notify_user=continuation_notifies_user,
    )
    if delivered or not continuation_notifies_user:
        return "NO_USER_RESPONSE"
    return None


async def _maybe_auto_continue_capability_outcome(
    *,
    call: dict[str, Any],
    tool_result: Any,
    active_tool_specs: list[ToolSpec],
    ctx: dict[str, object],
    messages: list[Message | dict[str, Any]],
    user_text: str,
    internal_followup: bool,
    handle_tool_call_fn: _HandleToolCallFn,
) -> tuple[str | None, bool]:
    outcome = extract_capability_outcome(tool_result)
    if not outcome or outcome.get("kind") != "needs_continuation":
        return None, False

    tool_name = str(call.get("function", {}).get("name") or "").strip()
    if tool_name == "octo_continue_from_control_route":
        return None, False

    continuation_spec = _find_active_tool_spec(
        "octo_continue_from_control_route",
        active_tool_specs,
    )
    if continuation_spec is None:
        return None, False

    continuation_args = _build_auto_continuation_args(
        call=call,
        outcome=outcome,
        user_text=user_text,
    )
    synthetic_call = {
        "id": f"auto-continuation-{uuid.uuid4().hex[:12]}",
        "type": "function",
        "function": {
            "name": "octo_continue_from_control_route",
            "arguments": json.dumps(continuation_args, ensure_ascii=False),
        },
    }
    logger.info(
        "Auto-continuing capability outcome",
        original_tool=tool_name,
        missing_tool=str(outcome.get("missing_tool") or ""),
        route_policy_label=str(ctx.get("route_policy_label") or ""),
    )
    continuation_result, continuation_meta = await handle_tool_call_fn(
        synthetic_call,
        active_tool_specs,
        ctx,
    )
    continuation_payload = _parse_tool_result_payload(continuation_result)
    continuation_status = ""
    if isinstance(continuation_payload, dict):
        continuation_status = str(continuation_payload.get("status") or "").strip().lower()

    if not continuation_meta.get("had_error") and continuation_status == "continued":
        return _auto_continuation_completion_signal(ctx, internal_followup=internal_followup), True

    rendered = render_tool_result_for_llm(
        continuation_result,
        tool_name="octo_continue_from_control_route",
    ).text
    messages.append(
        Message(
            role="system",
            content=(
                "Automatic continuation was attempted but did not complete successfully. "
                "Do not repeat the unavailable tool call. Use the available tools, ask for "
                "safe clarification, or return the route's blocked signal with the concrete blocker.\n"
                f"Continuation result:\n{rendered}"
            ),
        )
    )
    return None, True


def _find_active_tool_spec(name: str, tool_specs: list[ToolSpec]) -> ToolSpec | None:
    normalized_name = str(name or "").strip().lower()
    for spec in tool_specs:
        if str(spec.name).strip().lower() == normalized_name:
            return spec
    return None


def _build_auto_continuation_args(
    *,
    call: dict[str, Any],
    outcome: dict[str, Any],
    user_text: str,
) -> dict[str, Any]:
    function = call.get("function") or {}
    tool_name = str(function.get("name") or "").strip()
    missing_tool = str(outcome.get("missing_tool") or tool_name).strip()
    next_action = str(outcome.get("next_action") or "").strip()
    reason = str(outcome.get("reason") or "").strip()
    args = _parse_tool_call_arguments(function.get("arguments", "{}"))

    task = (
        "Complete the original turn through the normal Octo route. "
        f"The current execution contract could not use `{missing_tool}` directly, "
        "but the task has enough context to continue safely. Use the normal-route tools "
        "needed to finish the work end-to-end. Do not mention route, mode, tool-surface, "
        "or handoff internals unless the user explicitly asks."
    )
    context_summary = (
        "Capability outcome requested normal-route continuation.\n"
        f"- attempted_tool: {tool_name or '<unknown>'}\n"
        f"- missing_tool: {missing_tool or '<unknown>'}\n"
        f"- reason: {reason or '<none>'}\n"
        f"- next_action: {next_action or '<none>'}\n"
        f"- attempted_args: {safe_preview(args, limit=2000)}\n\n"
        "Original/control input:\n"
        f"{safe_preview(user_text, limit=12000)}"
    )
    return {"task": task, "context_summary": context_summary}


def _parse_tool_call_arguments(raw_args: Any) -> Any:
    if isinstance(raw_args, str):
        try:
            return json.loads(raw_args)
        except Exception:
            return raw_args
    return raw_args


def _auto_continuation_completion_signal(
    ctx: dict[str, object],
    *,
    internal_followup: bool,
) -> str:
    if internal_followup:
        return "NO_USER_RESPONSE"
    if str(ctx.get("route_policy_label") or "") == "octo.scheduler_octo_control_allowlist":
        return "SCHEDULED_TASK_DONE"
    return "NO_USER_RESPONSE"
