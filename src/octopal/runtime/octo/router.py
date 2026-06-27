from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from octopal.infrastructure.observability.base import (
    bind_trace_context,
    get_current_trace_context,
    now_ms,
    reset_trace_context,
)
from octopal.infrastructure.observability.helpers import summarize_exception
from octopal.infrastructure.providers.base import InferenceProvider, Message
from octopal.runtime.memory.service import MemoryService
from octopal.runtime.octo import route_completion as _route_completion
from octopal.runtime.octo import route_context as _route_context
from octopal.runtime.octo import route_continuations as _route_continuations
from octopal.runtime.octo import route_contracts as _route_contracts
from octopal.runtime.octo import route_inputs as _route_inputs
from octopal.runtime.octo import route_loop_helpers as _route_loop_helpers
from octopal.runtime.octo import route_planning as _route_planning
from octopal.runtime.octo import route_progress as _route_progress
from octopal.runtime.octo import route_replies as _route_replies
from octopal.runtime.octo import route_verification as _route_verification
from octopal.runtime.octo import tool_execution as _tool_execution
from octopal.runtime.octo import tool_policy as _tool_policy
from octopal.runtime.octo import tool_selection as _tool_selection
from octopal.runtime.octo import worker_followups as _worker_followups
from octopal.runtime.octo import worker_results as _worker_results
from octopal.runtime.octo.control_plane import RouteMode
from octopal.runtime.octo.prompt_builder import (
    build_bootstrap_context_prompt,
    build_control_plane_prompt,
    build_octo_prompt,
)
from octopal.runtime.tool_loop import (
    _resolve_tool_loop_thresholds,
)
from octopal.runtime.tool_payloads import render_tool_result_for_llm
from octopal.runtime.workers.contracts import WorkerResult
from octopal.tools.registry import ToolSpec

logger = structlog.get_logger(__name__)
get_tools = _tool_selection.get_tools
_DURABLE_WORKSPACE_ROOTS = _tool_selection._DURABLE_WORKSPACE_ROOTS
_HEARTBEAT_ALLOWED_TOOL_NAMES = _tool_selection._HEARTBEAT_ALLOWED_TOOL_NAMES
_INTERNAL_MAINTENANCE_ALLOWED_TOOL_NAMES = _tool_selection._INTERNAL_MAINTENANCE_ALLOWED_TOOL_NAMES
_MIN_TOOL_COUNT_ON_OVERFLOW = _tool_selection._MIN_TOOL_COUNT_ON_OVERFLOW
_PROACTIVE_ALLOWED_TOOL_NAMES = _tool_selection._PROACTIVE_ALLOWED_TOOL_NAMES
_SCHEDULED_OCTO_CONTROL_ALLOWED_TOOL_NAMES = (
    _tool_selection._SCHEDULED_OCTO_CONTROL_ALLOWED_TOOL_NAMES
)
_SCHEDULER_ALLOWED_TOOL_NAMES = _tool_selection._SCHEDULER_ALLOWED_TOOL_NAMES
_WORKER_FOLLOWUP_ALLOWED_TOOL_NAMES = _tool_selection._WORKER_FOLLOWUP_ALLOWED_TOOL_NAMES
_budget_tool_specs = _tool_selection._budget_tool_specs
_expand_active_tool_specs_from_catalog_result = (
    _tool_selection._expand_active_tool_specs_from_catalog_result
)
_shrink_tool_specs_for_retry = _tool_selection._shrink_tool_specs_for_retry
_build_a2a_route_context = _route_context._build_a2a_route_context
_build_operational_memory_context = _route_context._build_operational_memory_context
_build_proactive_tick_input = _route_context._build_proactive_tick_input
_build_runtime_plan_context = _route_context._build_runtime_plan_context
_build_runtime_plan_guidance = _route_context._build_runtime_plan_guidance
_build_scheduled_octo_control_input = _route_context._build_scheduled_octo_control_input
_build_scheduled_octo_task_input = _route_context._build_scheduled_octo_task_input
_build_scheduler_tick_input = _route_context._build_scheduler_tick_input
_compact_plan_step_summary = _route_context._compact_plan_step_summary
_WorkerArtifactSummary = _worker_results._WorkerArtifactSummary
_build_generic_worker_completion_message = _worker_results._build_generic_worker_completion_message
_build_worker_result_payload = _worker_results._build_worker_result_payload
_extract_worker_artifact_paths = _worker_results._extract_worker_artifact_paths
_is_durable_workspace_artifact_path = _worker_results._is_durable_workspace_artifact_path
_normalize_worker_artifact_path = _worker_results._normalize_worker_artifact_path
_normalize_worker_result_entry = _worker_results._normalize_worker_result_entry
_summarize_worker_artifacts = _worker_results._summarize_worker_artifacts
build_forced_worker_followup = _worker_results.build_forced_worker_followup
should_force_worker_followup = _worker_results.should_force_worker_followup
should_send_worker_followup = _worker_results.should_send_worker_followup
_record_octo_tool_call = _tool_execution._record_octo_tool_call
_build_octo_tool_policy_summary = _tool_policy._build_octo_tool_policy_summary
_dangerous_exec_command_reason = _tool_policy._dangerous_exec_command_reason
_exec_run_approval_reason = _tool_policy._exec_run_approval_reason
_find_known_tool_spec = _tool_policy._find_known_tool_spec
_maybe_request_octo_tool_approval = _tool_policy._maybe_request_octo_tool_approval
_policy_block_hint = _tool_policy._policy_block_hint
_resolve_octo_approval_requester = _tool_policy._resolve_octo_approval_requester
_resolve_octo_policy_block = _tool_policy._resolve_octo_policy_block
_resolve_octo_unavailable_tool = _tool_policy._resolve_octo_unavailable_tool
_shell_command_words = _tool_policy._shell_command_words
_shell_tokens = _tool_policy._shell_tokens
_exception_chain_text = _route_loop_helpers._exception_chain_text
_has_meaningful_error_value = _route_loop_helpers._has_meaningful_error_value
_is_context_overflow_error = _route_loop_helpers._is_context_overflow_error
_is_transient_provider_error = _route_loop_helpers._is_transient_provider_error
_parse_textual_tool_preview_args = _route_loop_helpers._parse_textual_tool_preview_args
_parse_tool_result_payload = _route_loop_helpers._parse_tool_result_payload
_recover_textual_tool_call = _route_loop_helpers._recover_textual_tool_call
_required_tool_fields = _route_loop_helpers._required_tool_fields
_TEXTUAL_TOOL_NAME_RE = _route_loop_helpers._TEXTUAL_TOOL_NAME_RE
_TEXTUAL_TOOL_PREVIEW_RE = _route_loop_helpers._TEXTUAL_TOOL_PREVIEW_RE
_tool_result_payload_error_type = _route_loop_helpers._tool_result_payload_error_type
normalize_plain_text = _route_loop_helpers.normalize_plain_text
_build_saved_image_fallback_text = _route_inputs._build_saved_image_fallback_text
_coerce_content_to_text = _route_completion._coerce_content_to_text
_coerce_tool_message_to_text = _route_completion._coerce_tool_message_to_text
_complete_text = _route_completion._complete_text
_auto_continuation_completion_signal = (
    _route_continuations._auto_continuation_completion_signal
)
_build_auto_continuation_args = _route_continuations._build_auto_continuation_args
_build_partial_callback = _route_progress._build_partial_callback
_continue_after_tool_budget_exhaustion = (
    _route_continuations._continue_after_tool_budget_exhaustion
)
_decode_and_save_images = _route_inputs._decode_and_save_images
_is_invalid_tool_payload_error = _route_inputs._is_invalid_tool_payload_error
_is_vision_tool_compatibility_error = _route_inputs._is_vision_tool_compatibility_error
_normalize_saved_file_paths = _route_inputs._normalize_saved_file_paths
RuntimeActionContract = _route_contracts.RuntimeActionContract
_ACTIONABLE_PLAN_STEP_KINDS = _route_contracts._ACTIONABLE_PLAN_STEP_KINDS
_coerce_tool_result_dict = _route_contracts._coerce_tool_result_dict
_contracts_created_by_tool_result = _route_contracts._contracts_created_by_tool_result
_RESOLVED_PLAN_STEP_STATUSES = _route_contracts._RESOLVED_PLAN_STEP_STATUSES
_resolve_contracts_from_parallel_worker_launch = (
    _route_contracts._resolve_contracts_from_parallel_worker_launch
)
_resolve_contracts_from_plan_snapshot = _route_contracts._resolve_contracts_from_plan_snapshot
_resolve_contracts_from_worker_launch = _route_contracts._resolve_contracts_from_worker_launch
_runtime_action_contract_blocked_response = (
    _route_contracts._runtime_action_contract_blocked_response
)
_runtime_action_contract_retry_prompt = _route_contracts._runtime_action_contract_retry_prompt
_tool_result_requests_followup = _route_contracts._tool_result_requests_followup
_update_runtime_action_contracts = _route_contracts._update_runtime_action_contracts
_extract_json_object = _route_planning._extract_json_object
_emit_octo_tool_use_event = _route_progress._emit_octo_tool_use_event
_find_active_tool_spec = _route_continuations._find_active_tool_spec
_iter_balanced_json_object_candidates = _route_planning._iter_balanced_json_object_candidates
_MAX_PLAN_STEPS = _route_planning._MAX_PLAN_STEPS
_normalize_plan_payload = _route_planning._normalize_plan_payload
_persist_plan = _route_planning._persist_plan
_build_insufficient_evidence_response = _route_verification._build_insufficient_evidence_response
_MAX_VERIFY_CONTEXT_CHARS = _route_verification._MAX_VERIFY_CONTEXT_CHARS
_messages_include_execution_plan = _route_verification._messages_include_execution_plan
_messages_include_runtime_state_context = _route_verification._messages_include_runtime_state_context
_messages_to_text = _route_verification._messages_to_text
_message_shape = _route_completion._message_shape
_messages_include_tool_call = _route_replies._messages_include_tool_call
_normalize_proactive_reply = _route_replies._normalize_proactive_reply
_normalize_worker_followup_reply = _route_replies._normalize_worker_followup_reply
_normalize_verification_payload = _route_verification._normalize_verification_payload
_looks_like_tool_error = _route_replies._looks_like_tool_error
_log_system_prompt = _route_progress._log_system_prompt
_parse_tool_call_arguments = _route_continuations._parse_tool_call_arguments
_sanitize_messages_for_complete = _route_completion._sanitize_messages_for_complete
_sanitize_control_plane_contract_text = _route_verification._sanitize_control_plane_contract_text


def _get_octo_tools(octo: Any, chat_id: int) -> tuple[list[ToolSpec], dict[str, object]]:
    return _tool_selection._get_octo_tools(octo, chat_id, get_tools_fn=get_tools)


def _get_worker_followup_tools(octo: Any, chat_id: int) -> tuple[list[ToolSpec], dict[str, object]]:
    return _tool_selection._get_worker_followup_tools(octo, chat_id, get_tools_fn=get_tools)


def _get_heartbeat_tools(octo: Any, chat_id: int) -> tuple[list[ToolSpec], dict[str, object]]:
    return _tool_selection._get_heartbeat_tools(octo, chat_id, get_tools_fn=get_tools)


def _get_scheduler_tools(octo: Any, chat_id: int) -> tuple[list[ToolSpec], dict[str, object]]:
    return _tool_selection._get_scheduler_tools(octo, chat_id, get_tools_fn=get_tools)


def _get_proactive_tools(octo: Any, chat_id: int) -> tuple[list[ToolSpec], dict[str, object]]:
    return _tool_selection._get_proactive_tools(octo, chat_id, get_tools_fn=get_tools)


def _get_scheduled_octo_control_tools(
    octo: Any, chat_id: int
) -> tuple[list[ToolSpec], dict[str, object]]:
    return _tool_selection._get_scheduled_octo_control_tools(
        octo, chat_id, get_tools_fn=get_tools
    )


def _get_internal_maintenance_tools(
    octo: Any, chat_id: int
) -> tuple[list[ToolSpec], dict[str, object]]:
    return _tool_selection._get_internal_maintenance_tools(octo, chat_id, get_tools_fn=get_tools)


async def _ensure_mcp_connected_for_routing(octo: Any) -> None:
    mcp_manager = getattr(octo, "mcp_manager", None)
    if mcp_manager is None:
        return
    try:
        await mcp_manager.ensure_configured_servers_connected()
    except Exception:
        logger.warning("Failed to refresh configured MCP servers before routing", exc_info=True)


async def route_or_reply(
    octo: Any,
    provider: InferenceProvider,
    memory: MemoryService,
    user_text: str,
    chat_id: int,
    bootstrap_context: str,
    *,
    internal_followup: bool = False,
    show_typing: bool = True,
    images: list[str] | None = None,
    saved_file_paths: list[str] | None = None,
    include_wakeup: bool = True,
    route_mode: str | RouteMode = RouteMode.CONVERSATION,
    conversation_scope: str | None = None,
    channel_context: dict[str, object] | None = None,
    background_delivery: bool = False,
) -> str:
    """Core routing logic: decide whether to use tools or reply to user."""
    # Internal chat_id (<= 0) should not trigger typing indicators.
    trace_sink = getattr(octo, "trace_sink", None)
    parent_trace_ctx = get_current_trace_context()
    routing_trace_ctx = None
    routing_trace_token = None
    routing_trace_started_ms = now_ms()
    routing_trace_status = "ok"
    routing_trace_output: dict[str, Any] | None = None
    route_mode_value = (
        route_mode.value
        if isinstance(route_mode, RouteMode)
        else str(route_mode or "unknown").strip() or "unknown"
    )
    routing_trace_metadata: dict[str, Any] = {
        "internal_followup": internal_followup,
        "show_typing": show_typing,
        "include_wakeup": include_wakeup,
        "background_delivery": background_delivery,
        "has_images": bool(images),
        "saved_file_paths_count": len(saved_file_paths or []),
        "route_mode": route_mode_value,
        "bootstrap_chars": len(bootstrap_context or ""),
        "planner_used": False,
        "mcp_refresh_attempted": False,
    }
    if trace_sink is not None and parent_trace_ctx is not None:
        routing_trace_ctx = await trace_sink.start_span(
            parent_trace_ctx,
            name="octo.routing",
            metadata=routing_trace_metadata,
        )
        routing_trace_token = bind_trace_context(routing_trace_ctx)
    if chat_id > 0 and show_typing:
        await octo.set_typing(chat_id, True)

    await octo.set_thinking(True)
    try:
        partial_callback = _build_partial_callback(octo=octo, chat_id=chat_id)
        is_ws = getattr(octo, "is_ws_active", False)
        wake_notice = ""
        if include_wakeup and hasattr(octo, "peek_context_wakeup"):
            wake_notice = str(octo.peek_context_wakeup(chat_id) or "")
        routing_trace_metadata["mcp_refresh_attempted"] = True
        await _ensure_mcp_connected_for_routing(octo)

        octo_tools, ctx = _get_octo_tools(octo, chat_id)
        ctx.update(
            {
                "route_mode": route_mode_value,
                "internal_followup": internal_followup,
                "background_delivery": background_delivery,
                "conversation_scope": conversation_scope,
            }
        )
        resolution_report = ctx.get("tool_resolution_report")
        available_count = len(getattr(resolution_report, "available_tools", ()) or ())
        deferred_count = max(0, available_count - len(octo_tools))
        logger.info(
            "Octo tools fetched",
            route_mode=route_mode_value,
            active_tool_count=len(octo_tools),
            available_tool_count=available_count,
            deferred_tool_count=deferred_count,
        )
        if routing_trace_ctx is not None and trace_sink is not None:
            await trace_sink.annotate(
                routing_trace_ctx,
                name="octo.routing.tools",
                metadata={
                    "route_mode": route_mode_value,
                    "active_tool_count": len(octo_tools),
                    "available_tool_count": available_count,
                    "deferred_tool_count": deferred_count,
                },
            )
        tool_policy_summary = _build_octo_tool_policy_summary(
            octo_tools,
            ctx.get("tool_resolution_report"),
        )
        messages = await build_octo_prompt(
            store=octo.store,
            memory=memory,
            canon=octo.canon,
            user_text=user_text,
            chat_id=chat_id,
            bootstrap_context=bootstrap_context,
            is_ws=is_ws,
            images=images,
            saved_file_paths=saved_file_paths,
            wake_notice=wake_notice,
            tool_policy_summary=tool_policy_summary,
            facts=getattr(octo, "facts", None),
            reflection=getattr(octo, "reflection", None),
            conversation_scope=conversation_scope,
            channel_context=channel_context,
        )
        runtime_plan_context = _build_runtime_plan_context(octo, chat_id)
        operational_memory_context = _build_operational_memory_context(octo, chat_id)
        messages.append(Message(role="system", content=_build_runtime_plan_guidance()))
        a2a_context = _build_a2a_route_context(octo)
        if a2a_context:
            messages.append(Message(role="system", content=a2a_context))
        if runtime_plan_context:
            messages.append(Message(role="system", content=runtime_plan_context))
        if operational_memory_context:
            messages.append(Message(role="system", content=operational_memory_context))
        _log_system_prompt(messages, "route")

        plan = await _build_plan(provider, messages, bool(octo_tools))
        if plan:
            routing_trace_metadata["planner_used"] = True
            await _persist_plan(memory, chat_id, plan)
            logger.info(
                "Octo plan ready",
                route_mode=route_mode_value,
                mode=plan["mode"],
                steps=len(plan.get("steps", [])),
            )
            if plan["mode"] == "reply":
                routing_trace_output = {
                    "route_mode": route_mode_value,
                    "planner_mode": plan["mode"],
                    "steps_count": len(plan.get("steps", [])),
                }
                return await _finalize_response(
                    provider=provider,
                    messages=messages,
                    response_text=str(plan.get("response", "")),
                    internal_followup=internal_followup,
                )
            plan_steps = plan.get("steps", [])
            if plan_steps:
                plan_block = "\n".join(
                    [f"{idx + 1}. {step}" for idx, step in enumerate(plan_steps)]
                )
                messages.append(
                    Message(
                        role="system",
                        content=(
                            "Execution plan generated by planner. Execute steps in order and recover gracefully from failures.\n"
                            "<execution_plan>\n"
                            f"{plan_block}\n"
                            "</execution_plan>"
                        ),
                    )
                )
        routing_trace_output = {
            "route_mode": route_mode_value,
            "planner_mode": "execute" if routing_trace_metadata["planner_used"] else "none",
            "steps_count": len(plan.get("steps", [])) if plan else 0,
        }
        return await _complete_route_with_tools(
            octo=octo,
            provider=provider,
            messages=messages,
            tool_specs=octo_tools,
            ctx=ctx,
            internal_followup=internal_followup,
            user_text=user_text,
            images=images,
            saved_file_paths=saved_file_paths,
            on_plain_partial=partial_callback,
            allow_tool_catalog_expansion=True,
        )
    except Exception as exc:
        routing_trace_status = "error"
        routing_trace_metadata.update(summarize_exception(exc))
        logger.exception("Error in route_or_reply")
        raise
    finally:
        await octo.set_thinking(False)
        if chat_id > 0 and show_typing:
            logger.debug("Toggling typing indicator off", chat_id=chat_id)
            await octo.set_typing(chat_id, False)
        if routing_trace_ctx is not None and trace_sink is not None:
            finish_meta = dict(routing_trace_metadata)
            finish_meta["duration_ms"] = round(now_ms() - routing_trace_started_ms, 2)
            await trace_sink.finish_span(
                routing_trace_ctx,
                status=routing_trace_status,
                output=routing_trace_output,
                metadata=finish_meta,
            )
        if routing_trace_token is not None:
            reset_trace_context(routing_trace_token)


async def route_heartbeat(
    octo: Any,
    chat_id: int,
    user_text: str,
    *,
    show_typing: bool = True,
    include_wakeup: bool = True,
) -> str:
    """Run a bounded heartbeat/control-plane turn without the full conversation planner path."""
    if chat_id > 0 and show_typing:
        await octo.set_typing(chat_id, True)

    await octo.set_thinking(True)
    try:
        wake_notice = ""
        if include_wakeup and hasattr(octo, "peek_context_wakeup"):
            wake_notice = str(octo.peek_context_wakeup(chat_id) or "")

        octo_tools, ctx = _get_heartbeat_tools(octo, chat_id)
        resolution_report = ctx.get("tool_resolution_report")
        available_count = len(getattr(resolution_report, "available_tools", ()) or ())
        deferred_count = max(0, available_count - len(octo_tools))
        logger.info(
            "Heartbeat tools fetched",
            route_mode=RouteMode.HEARTBEAT.value,
            active_tool_count=len(octo_tools),
            available_tool_count=available_count,
            deferred_tool_count=deferred_count,
            mcp_refresh_attempted=bool(ctx.get("mcp_refresh_attempted")),
        )
        tool_policy_summary = _build_octo_tool_policy_summary(
            octo_tools,
            ctx.get("tool_resolution_report"),
        )
        messages = await build_control_plane_prompt(
            user_text=user_text,
            chat_id=chat_id,
            tool_policy_summary=tool_policy_summary,
            wake_notice=wake_notice,
            reflection=getattr(octo, "reflection", None),
            mode_label="heartbeat",
            mode_rules=(
                "Heartbeat route rules:\n"
                "- Return exactly one of: HEARTBEAT_OK, NO_USER_RESPONSE, or <user_visible>...</user_visible>.\n"
                "- Use tools only if they are clearly necessary for current heartbeat/scheduler state.\n"
                "- Do not start broad orchestration from this lightweight operational turn.\n"
                "- Do not rely on full workspace bootstrap, recent chat history, or rich memory recall."
            ),
        )
        _log_system_prompt(messages, "heartbeat")
        reply_text = await _complete_route_with_tools(
            octo=octo,
            provider=octo.provider,
            messages=messages,
            tool_specs=octo_tools,
            ctx=ctx,
            internal_followup=False,
            user_text=user_text,
            images=None,
            saved_file_paths=None,
            allow_tool_catalog_expansion=False,
        )
        return normalize_plain_text(reply_text)
    finally:
        await octo.set_thinking(False)
        if chat_id > 0 and show_typing:
            await octo.set_typing(chat_id, False)


async def route_internal_maintenance(
    octo: Any,
    chat_id: int,
    user_text: str,
) -> str:
    """Run a bounded internal maintenance turn without the full conversation planner path."""
    await octo.set_thinking(True)
    try:
        octo_tools, ctx = _get_internal_maintenance_tools(octo, chat_id)
        resolution_report = ctx.get("tool_resolution_report")
        available_count = len(getattr(resolution_report, "available_tools", ()) or ())
        deferred_count = max(0, available_count - len(octo_tools))
        logger.info(
            "Internal maintenance tools fetched",
            route_mode=RouteMode.INTERNAL_MAINTENANCE.value,
            active_tool_count=len(octo_tools),
            available_tool_count=available_count,
            deferred_tool_count=deferred_count,
            mcp_refresh_attempted=bool(ctx.get("mcp_refresh_attempted")),
        )
        tool_policy_summary = _build_octo_tool_policy_summary(
            octo_tools,
            ctx.get("tool_resolution_report"),
        )
        messages = await build_control_plane_prompt(
            user_text=user_text,
            chat_id=chat_id,
            tool_policy_summary=tool_policy_summary,
            reflection=getattr(octo, "reflection", None),
            mode_label="internal-maintenance",
            mode_rules=(
                "Internal maintenance route rules:\n"
                "- Keep this turn operational and bounded.\n"
                "- You may inspect runtime health and worker availability.\n"
                "- Do not start broad orchestration, memory recall, or user-task planning.\n"
                "- Return a short user-visible readiness/update message in plain language."
            ),
        )
        _log_system_prompt(messages, "internal_maintenance")
        reply_text = await _complete_route_with_tools(
            octo=octo,
            provider=octo.provider,
            messages=messages,
            tool_specs=octo_tools,
            ctx=ctx,
            internal_followup=False,
            user_text=user_text,
            images=None,
            saved_file_paths=None,
            allow_tool_catalog_expansion=False,
        )
        return normalize_plain_text(reply_text)
    finally:
        await octo.set_thinking(False)


async def route_scheduler_tick(
    octo: Any,
    chat_id: int = 0,
    *,
    max_tasks: int = 10,
) -> str:
    """Run a bounded scheduler control-plane turn without the full conversation planner path."""
    await octo.set_thinking(True)
    try:
        octo_tools, ctx = _get_scheduler_tools(octo, chat_id)
        resolution_report = ctx.get("tool_resolution_report")
        available_count = len(getattr(resolution_report, "available_tools", ()) or ())
        deferred_count = max(0, available_count - len(octo_tools))
        logger.info(
            "Scheduler tick tools fetched",
            route_mode=RouteMode.SCHEDULER.value,
            active_tool_count=len(octo_tools),
            available_tool_count=available_count,
            deferred_tool_count=deferred_count,
            mcp_refresh_attempted=bool(ctx.get("mcp_refresh_attempted")),
        )
        tool_policy_summary = _build_octo_tool_policy_summary(
            octo_tools,
            ctx.get("tool_resolution_report"),
        )
        scheduler_tick_text = _build_scheduler_tick_input(octo, max_tasks=max_tasks)
        messages = await build_control_plane_prompt(
            user_text=scheduler_tick_text,
            chat_id=chat_id,
            tool_policy_summary=tool_policy_summary,
            reflection=getattr(octo, "reflection", None),
            mode_label="scheduler",
            mode_rules=(
                "Scheduler route rules:\n"
                "- Keep this turn operational and bounded.\n"
                "- You may inspect schedule state and worker availability.\n"
                "- You may apply safe scheduled-task route repairs with repair_scheduled_tasks(apply=true) "
                "when the candidate is unambiguous.\n"
                "- Do not dispatch workers directly during this scheduler tick.\n"
                "- Return one of: SCHEDULER_IDLE, NO_USER_RESPONSE, or <user_visible>...</user_visible>."
            ),
        )
        _log_system_prompt(messages, "scheduler")
        reply_text = await _complete_route_with_tools(
            octo=octo,
            provider=octo.provider,
            messages=messages,
            tool_specs=octo_tools,
            ctx=ctx,
            internal_followup=False,
            user_text=scheduler_tick_text,
            images=None,
            saved_file_paths=None,
            allow_tool_catalog_expansion=False,
            preserve_user_visible_wrapper=True,
        )
        return reply_text.strip()
    finally:
        await octo.set_thinking(False)


async def route_proactive_tick(
    octo: Any,
    chat_id: int = 0,
    *,
    reason: str = "scheduler_idle",
) -> str:
    """Run a bounded proactive control-plane turn that may only manage initiative queue state."""
    await octo.set_thinking(True)
    try:
        octo_tools, ctx = _get_proactive_tools(octo, chat_id)
        resolution_report = ctx.get("tool_resolution_report")
        available_count = len(getattr(resolution_report, "available_tools", ()) or ())
        deferred_count = max(0, available_count - len(octo_tools))
        logger.info(
            "Proactive tick tools fetched",
            route_mode=RouteMode.PROACTIVE.value,
            active_tool_count=len(octo_tools),
            available_tool_count=available_count,
            deferred_tool_count=deferred_count,
            mcp_refresh_attempted=bool(ctx.get("mcp_refresh_attempted")),
            reason=reason,
        )
        tool_policy_summary = _build_octo_tool_policy_summary(
            octo_tools,
            ctx.get("tool_resolution_report"),
        )
        proactive_tick_text = await _build_proactive_tick_input(
            octo,
            chat_id=chat_id,
            reason=reason,
        )
        messages = await build_control_plane_prompt(
            user_text=proactive_tick_text,
            chat_id=chat_id,
            tool_policy_summary=tool_policy_summary,
            reflection=getattr(octo, "reflection", None),
            mode_label="proactive",
            mode_rules=(
                "Proactive route rules:\n"
                "- Keep this turn bounded to initiative discovery and self-queue maintenance.\n"
                "- You may add, claim, execute, cancel, or mark self-queue items only when the payload supports it.\n"
                "- You may preview scheduled-task repair candidates with repair_scheduled_tasks(apply=false).\n"
                "- You may apply scheduled-task repairs only for unambiguous blocked_by_route candidates. "
                "For worker repairs the task must already have a valid worker_id; never invent a worker_id during this tick.\n"
                "- Do not start workers directly, schedule recurring tasks, use filesystem tools, use network/MCP tools, "
                "or perform external side effects during this tick.\n"
                "- Use execute_self_queue_item only for an existing low/medium-risk queue item with an explicit worker_id; "
                "the runtime will start the worker or mark the item blocked.\n"
                "- Prefer queueing one concrete low-risk initiative when there is no safe executable queue item.\n"
                "- Return JSON only using the proactive decision contract."
            ),
        )
        messages.append(
            Message(
                role="system",
                content=(
                    "Return JSON only with this shape:\n"
                    "{\n"
                    '  "decision": "noop|queue|claim|execute|repair|blocked",\n'
                    '  "confidence": 0.0,\n'
                    '  "risk": "low|medium|high",\n'
                    '  "requires_user_input": false,\n'
                    '  "selected_item_id": string|null,\n'
                    '  "queued_item_id": string|null,\n'
                    '  "reason": string\n'
                    "}\n"
                    "Use decision=queue only after a successful octo_self_queue_add call. "
                    "Use decision=claim only after a successful octo_self_queue_take call. "
                    "Use decision=execute only after a successful execute_self_queue_item call. "
                    "Use decision=repair only after a successful guarded repair_scheduled_tasks(apply=true) call. "
                    "Use decision=noop when confidence is below threshold or pending work is not safely executable here."
                ),
            )
        )
        _log_system_prompt(messages, "proactive")
        reply_text = await _complete_route_with_tools(
            octo=octo,
            provider=octo.provider,
            messages=messages,
            tool_specs=octo_tools,
            ctx=ctx,
            internal_followup=True,
            user_text=proactive_tick_text,
            images=None,
            saved_file_paths=None,
            allow_tool_catalog_expansion=False,
        )
        return _normalize_proactive_reply(reply_text)
    finally:
        await octo.set_thinking(False)


async def route_scheduled_octo_control(
    octo: Any,
    task: dict[str, Any],
    *,
    chat_id: int = 0,
) -> str:
    """Run a bounded control-plane turn for one scheduled Octo task."""
    await octo.set_thinking(True)
    try:
        octo_tools, ctx = _get_scheduled_octo_control_tools(octo, chat_id)
        ctx["control_route_notify_user"] = task.get("notify_user")
        resolution_report = ctx.get("tool_resolution_report")
        available_count = len(getattr(resolution_report, "available_tools", ()) or ())
        deferred_count = max(0, available_count - len(octo_tools))
        logger.info(
            "Scheduled Octo control tools fetched",
            route_mode=RouteMode.SCHEDULER.value,
            active_tool_count=len(octo_tools),
            available_tool_count=available_count,
            deferred_tool_count=deferred_count,
            mcp_refresh_attempted=bool(ctx.get("mcp_refresh_attempted")),
            execution_mode="octo_control",
            task_id=str(task.get("id") or "").strip() or None,
        )
        tool_policy_summary = _build_octo_tool_policy_summary(
            octo_tools,
            ctx.get("tool_resolution_report"),
        )
        scheduled_task_text = _build_scheduled_octo_control_input(task)
        messages = await build_control_plane_prompt(
            user_text=scheduled_task_text,
            chat_id=chat_id,
            tool_policy_summary=tool_policy_summary,
            reflection=getattr(octo, "reflection", None),
            mode_label="scheduled_octo_control",
            mode_rules=(
                "Scheduled Octo control route rules:\n"
                "- Keep this turn bounded to the single scheduled task in the payload.\n"
                "- You may use allowed control-plane and maintenance tools when necessary.\n"
                "- Do not start workers or broad orchestration directly inside this control turn.\n"
                "- If the task needs normal Octo tools, workspace writes, workers, external access, "
                "A2A, or broader orchestration and the payload gives enough context to proceed, call "
                "`octo_continue_from_control_route` with one concrete continuation task, then return "
                "SCHEDULED_TASK_DONE after the tool succeeds.\n"
                "- Return exactly one of: SCHEDULED_TASK_DONE, SCHEDULED_TASK_BLOCKED, NO_USER_RESPONSE, or <user_visible>...</user_visible>.\n"
                "- Use SCHEDULED_TASK_DONE only if the task completed successfully with no user-visible update.\n"
                "- Use SCHEDULED_TASK_BLOCKED only when the task cannot complete and cannot be safely continued through the normal route.\n"
                "- Use <user_visible> only for a concise user-facing update after the task is complete."
            ),
        )
        _log_system_prompt(messages, "scheduled_octo_control")
        reply_text = await _complete_route_with_tools(
            octo=octo,
            provider=octo.provider,
            messages=messages,
            tool_specs=octo_tools,
            ctx=ctx,
            internal_followup=False,
            user_text=scheduled_task_text,
            images=None,
            saved_file_paths=None,
            allow_tool_catalog_expansion=False,
        )
        return normalize_plain_text(reply_text)
    finally:
        await octo.set_thinking(False)


async def route_scheduled_octo_task(
    octo: Any,
    task: dict[str, Any],
    *,
    chat_id: int = 0,
) -> str:
    """Run a scheduled task as a full Octo workspace task with normal tools and context."""
    task_text = _build_scheduled_octo_task_input(task)
    bootstrap_context = await build_bootstrap_context_prompt(octo.store, chat_id)
    return await route_or_reply(
        octo,
        octo.provider,
        octo.memory,
        task_text,
        chat_id,
        bootstrap_context.content,
        internal_followup=True,
        show_typing=False,
        images=None,
        saved_file_paths=None,
        include_wakeup=False,
        route_mode=RouteMode.CONVERSATION,
    )


async def _complete_route_with_tools(
    *,
    octo: Any,
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    tool_specs: list[ToolSpec],
    ctx: dict[str, object],
    internal_followup: bool,
    user_text: str,
    images: list[str] | None,
    allow_tool_catalog_expansion: bool,
    saved_file_paths: list[str] | None = None,
    on_plain_partial: Callable[[str], Awaitable[None]] | None = None,
    preserve_user_visible_wrapper: bool = False,
) -> str:
    tool_capable = getattr(provider, "complete_with_tools", None)
    trace_ctx = get_current_trace_context()
    trace_sink = getattr(octo, "trace_sink", None)

    if callable(tool_capable) and tool_specs:
        if trace_ctx is not None and trace_sink is not None:
            await trace_sink.annotate(
                trace_ctx,
                name="octo.routing.mode",
                metadata={"route_mode": "tools", "active_tool_count": len(tool_specs)},
            )
        active_tool_specs = list(tool_specs)
        tools = [spec.to_openai_tool() for spec in active_tool_specs]
        last_error: str | None = None
        had_tool_calls = False
        transient_tool_failures = 0
        tool_call_history: list[dict[str, str]] = []
        tool_loop_thresholds = _resolve_tool_loop_thresholds()
        max_attempts = 10
        vision_tool_fallback_used = False
        structured_followup_required = False
        unbacked_action_retry_used = False
        delegated_recovery_retry_used = False
        auto_continuation_attempted = False
        runtime_action_contracts: list[RuntimeActionContract] = []
        runtime_action_retry_count = 0
        execution_plan_active = _messages_include_execution_plan(messages)

        for _ in range(max_attempts):
            try:
                result = await provider.complete_with_tools(
                    messages, tools=tools, tool_choice="auto"
                )
            except Exception as e:
                if (
                    images
                    and not vision_tool_fallback_used
                    and _is_vision_tool_compatibility_error(e)
                ):
                    logger.warning(
                        "Vision+Tools failed; attempting save-to-disk fallback", error=str(e)
                    )
                    try:
                        saved_paths = _normalize_saved_file_paths(saved_file_paths)
                        if not saved_paths:
                            saved_paths = _decode_and_save_images(images)

                        fallback_text = _build_saved_image_fallback_text(user_text, saved_paths)

                        logger.info(
                            "Retrying with text-only fallback and saved images",
                            count=len(saved_paths),
                        )
                        messages[-1] = {"role": "user", "content": fallback_text}
                        images = None
                        vision_tool_fallback_used = True
                        continue

                    except Exception as fallback_exc:
                        logger.error("Fallback save-and-retry failed", error=str(fallback_exc))
                        return (
                            "I see you sent an image, but I am unable to process it. "
                            "My current model configuration might not support vision, and I could not save it for tool analysis."
                        )
                if vision_tool_fallback_used:
                    logger.warning(
                        "Tool-enabled retry after image save failed; falling back to plain text completion",
                        error=_exception_chain_text(e)[:500],
                    )
                    messages.append(
                        Message(
                            role="system",
                            content=(
                                "Tool calling failed even after converting the image request into a text-only "
                                "instruction with local file paths. Reply without tools. Be transparent that the "
                                "image files were saved locally but could not be inspected automatically."
                            ),
                        )
                    )
                    fallback_text = await _complete_text(
                        provider,
                        messages,
                        context="saved_image_tool_retry_failed",
                    )
                    return await _finalize_response(
                        provider=provider,
                        messages=messages,
                        response_text=fallback_text,
                        internal_followup=internal_followup,
                    )
                if (
                    _is_context_overflow_error(e)
                    and len(active_tool_specs) > _MIN_TOOL_COUNT_ON_OVERFLOW
                ):
                    prior_count = len(active_tool_specs)
                    active_tool_specs = _shrink_tool_specs_for_retry(active_tool_specs)
                    tools = [spec.to_openai_tool() for spec in active_tool_specs]
                    logger.warning(
                        "Retrying completion with fewer tools after context overflow",
                        previous_tool_count=prior_count,
                        reduced_tool_count=len(active_tool_specs),
                    )
                    continue
                if (
                    _is_invalid_tool_payload_error(e)
                    and len(active_tool_specs) > _MIN_TOOL_COUNT_ON_OVERFLOW
                ):
                    prior_count = len(active_tool_specs)
                    active_tool_specs = _shrink_tool_specs_for_retry(active_tool_specs)
                    tools = [spec.to_openai_tool() for spec in active_tool_specs]
                    logger.warning(
                        "Retrying completion with fewer tools after provider rejected tool payload",
                        previous_tool_count=prior_count,
                        reduced_tool_count=len(active_tool_specs),
                        provider_id=getattr(provider, "provider_id", "unknown"),
                        error=_exception_chain_text(e)[:500],
                    )
                    continue
                if _is_transient_provider_error(e):
                    transient_tool_failures += 1
                    if transient_tool_failures >= 3:
                        logger.warning(
                            "Tool completion repeatedly failed with transient provider errors; falling back to plain completion",
                            failures=transient_tool_failures,
                            error=_exception_chain_text(e)[:500],
                        )
                        messages.append(
                            Message(
                                role="system",
                                content=(
                                    "Tool calling is temporarily unavailable due to provider instability. "
                                    "Reply without tools, summarize status, and ask for retry only if needed."
                                ),
                            )
                        )
                        fallback_text = await _complete_text(
                            provider,
                            messages,
                            context="transient_tool_error_fallback",
                        )
                        return await _finalize_response(
                            provider=provider,
                            messages=messages,
                            response_text=fallback_text,
                            internal_followup=internal_followup,
                        )
                    delay_s = min(4.0, 0.8 * (2 ** (transient_tool_failures - 1)))
                    logger.warning(
                        "Transient provider error during tool completion; retrying",
                        failure_count=transient_tool_failures,
                        retry_delay_s=round(delay_s, 2),
                        error=_exception_chain_text(e)[:500],
                    )
                    await asyncio.sleep(delay_s)
                    continue
                raise e

            content_raw = result.get("content", "")
            tool_calls = result.get("tool_calls") or []
            if not tool_calls and content_raw:
                recovered_call = _recover_textual_tool_call(content_raw, active_tool_specs)
                if recovered_call is not None:
                    logger.warning(
                        "Recovered textual tool invocation from model content",
                        tool_name=recovered_call.get("function", {}).get("name"),
                        raw_content=str(content_raw)[:200],
                    )
                    tool_calls = [recovered_call]

            if tool_calls:
                had_tool_calls = True
                assistant_msg: dict[str, Any] = {"role": "assistant", "tool_calls": tool_calls}
                if content_raw:
                    assistant_msg["content"] = content_raw
                messages.append(assistant_msg)

                for call in tool_calls:
                    tool_result, tool_meta = await _handle_octo_tool_call(
                        call, active_tool_specs, ctx
                    )
                    tool_name = str(call.get("function", {}).get("name") or "")
                    runtime_action_contracts = _update_runtime_action_contracts(
                        runtime_action_contracts,
                        tool_name=tool_name,
                        tool_result=tool_result,
                    )
                    if (
                        not internal_followup
                        and not structured_followup_required
                        and _tool_result_requests_followup(tool_name, tool_result)
                    ):
                        structured_followup_required = True
                        marker = getattr(octo, "mark_structured_followup_required", None)
                        if callable(marker):
                            marker()
                    expanded_names: list[str] = []
                    if (
                        allow_tool_catalog_expansion
                        and str(call.get("function", {}).get("name") or "") == "tool_catalog_search"
                    ):
                        active_tool_specs, expanded_names = (
                            _expand_active_tool_specs_from_catalog_result(
                                tool_result,
                                active_tool_specs=active_tool_specs,
                                ctx=ctx,
                            )
                        )
                        tools = [spec.to_openai_tool() for spec in active_tool_specs]
                        if expanded_names:
                            messages.append(
                                Message(
                                    role="system",
                                    content=(
                                        "Tool catalog expansion complete. The following tools are now active for this turn:\n"
                                        + "\n".join(f"- {name}" for name in expanded_names)
                                        + "\nUse them directly if they fit the task."
                                    ),
                                )
                            )
                    rendered_tool_result = render_tool_result_for_llm(
                        tool_result,
                        tool_name=tool_name,
                    )
                    tool_result_text = rendered_tool_result.text
                    if rendered_tool_result.was_compacted:
                        logger.debug(
                            "Octo tool result compacted",
                            tool_name=tool_name,
                            rendered_chars=len(tool_result_text),
                        )
                    payload_error_type = _tool_result_payload_error_type(tool_result)
                    if payload_error_type and not tool_meta.get("had_error"):
                        tool_meta = {
                            **tool_meta,
                            "had_error": True,
                            "error_type": payload_error_type,
                        }

                    loop_state = _record_octo_tool_call(
                        tool_call_history,
                        call=call,
                        tool_result=tool_result,
                        tool_meta=tool_meta,
                        thresholds=tool_loop_thresholds,
                    )
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": call.get("id"),
                            "name": call.get("function", {}).get("name"),
                            "content": tool_result_text,
                        }
                    )
                    if loop_state is not None:
                        current_trace_ctx = get_current_trace_context()
                        trace_sink = getattr(octo, "trace_sink", None)
                        if current_trace_ctx is not None and trace_sink is not None:
                            await trace_sink.annotate(
                                current_trace_ctx,
                                name="octo.tool_loop_detected",
                                metadata={
                                    "detector": loop_state["detector"],
                                    "level": loop_state["level"],
                                    "count": loop_state["count"],
                                    "message": loop_state["message"],
                                    "tool_name": tool_name,
                                },
                            )
                        logger.warning(
                            "Octo tool loop detected",
                            detector=loop_state["detector"],
                            level=loop_state["level"],
                            count=loop_state["count"],
                            message=loop_state["message"],
                        )
                        if loop_state["level"] == "critical":
                            messages.append(
                                Message(
                                    role="system",
                                    content=(
                                        "Tool loop breaker triggered. Do not call more tools in this turn. "
                                        "Summarize what happened, what is blocked, and what the next best step is."
                                    ),
                                )
                            )
                            fallback_text = await _complete_text(
                                provider,
                                messages,
                                context="octo_tool_loop_breaker",
                            )
                            return await _finalize_response(
                                provider=provider,
                                messages=messages,
                                response_text=fallback_text,
                                internal_followup=internal_followup,
                            )
                    if not auto_continuation_attempted:
                        auto_reply, attempted = await _maybe_auto_continue_capability_outcome(
                            octo=octo,
                            call=call,
                            tool_result=tool_result,
                            active_tool_specs=active_tool_specs,
                            ctx=ctx,
                            messages=messages,
                            user_text=user_text,
                            internal_followup=internal_followup,
                        )
                        auto_continuation_attempted = attempted
                        if auto_reply is not None:
                            return auto_reply
                    if tool_meta.get("had_error"):
                        last_error = tool_result_text
                continue

            if content_raw:
                if runtime_action_contracts:
                    if runtime_action_retry_count >= 2:
                        logger.warning(
                            "Runtime action contract still pending after retries; returning blocked status",
                            pending_contracts=[
                                contract.__dict__ for contract in runtime_action_contracts
                            ],
                        )
                        return await _finalize_response(
                            provider=provider,
                            messages=messages,
                            response_text=_runtime_action_contract_blocked_response(
                                runtime_action_contracts
                            ),
                            internal_followup=internal_followup,
                        )
                    runtime_action_retry_count += 1
                    logger.warning(
                        "Runtime action contract pending; forcing execution-or-state retry",
                        pending_contracts=[
                            contract.__dict__ for contract in runtime_action_contracts
                        ],
                    )
                    messages.append(Message(role="assistant", content=str(content_raw)))
                    messages.append(
                        Message(
                            role="system",
                            content=_runtime_action_contract_retry_prompt(runtime_action_contracts),
                        )
                    )
                    continue
                if (
                    not had_tool_calls
                    and not unbacked_action_retry_used
                    and await _needs_action_or_blocked_retry(
                        provider=provider,
                        messages=messages,
                        candidate=str(content_raw),
                    )
                ):
                    unbacked_action_retry_used = True
                    logger.warning(
                        "Assistant response requires concrete action state; forcing action-or-blocked retry",
                        preview=str(content_raw)[:200],
                    )
                    messages.append(Message(role="assistant", content=str(content_raw)))
                    messages.append(
                        Message(
                            role="system",
                            content=(
                                "Your previous answer was classified as requiring concrete runtime action state, "
                                "but this turn has not used any tool, started a worker, queued a task, or changed "
                                "runtime state. Continue this same turn now: call the appropriate tool, add/execute "
                                "a self-queue item, or rewrite the response as a clear blocked/clarifying answer. "
                                "Do not describe future work unless this turn creates a concrete runtime action."
                            ),
                        )
                    )
                    continue
                if (
                    (had_tool_calls or execution_plan_active)
                    and not delegated_recovery_retry_used
                    and await _needs_autonomous_recovery_retry(
                        provider=provider,
                        messages=messages,
                        candidate=str(content_raw),
                    )
                ):
                    delegated_recovery_retry_used = True
                    logger.warning(
                        "Assistant response delegated recoverable action choices; forcing autonomous recovery retry",
                        preview=str(content_raw)[:200],
                    )
                    retry_reason = (
                        "instead of following the active execution plan"
                        if execution_plan_active
                        else "instead of continuing autonomously after using tools"
                    )
                    messages.append(Message(role="assistant", content=str(content_raw)))
                    messages.append(
                        Message(
                            role="system",
                            content=(
                                "Your previous answer delegated recoverable execution choices to the user "
                                f"{retry_reason}. "
                                "Choose the best safe next action yourself and act now with available tools. "
                                "Prefer bounded, lower-risk recovery such as a smaller retry, decomposition, "
                                "state inspection, or durable task update before asking the user. "
                                "Ask the user only for a real human-only blocker: missing credentials, "
                                "approval for risky or irreversible work, a product choice with no safe default, "
                                "or an external state change you cannot perform. If truly blocked, record the "
                                "durable runtime state and explain the concrete blocker without generic "
                                "continue-later language."
                            ),
                        )
                    )
                    continue
                logger.debug("Octo output", output=content_raw)
                return await _finalize_response(
                    provider=provider,
                    messages=messages,
                    response_text=content_raw,
                    internal_followup=internal_followup,
                )

            if had_tool_calls:
                logger.warning(
                    "Tool execution completed without a final assistant response; falling back to text completion",
                )
                messages.append(
                    Message(
                        role="system",
                        content=(
                            "You have already used tools for this turn, but your last response was empty. "
                            "Reply to the user now with a concise plain-language status update or answer."
                        ),
                    )
                )
                fallback_text = await _complete_text(
                    provider,
                    messages,
                    context="empty_tool_response_fallback",
                )
                return await _finalize_response(
                    provider=provider,
                    messages=messages,
                    response_text=fallback_text,
                    internal_followup=internal_followup,
                )

            return await _finalize_response(
                provider=provider,
                messages=messages,
                response_text=content_raw,
                internal_followup=internal_followup,
            )

        if had_tool_calls:
            if internal_followup:
                return "NO_USER_RESPONSE"
            continuation_reply = await _continue_after_tool_budget_exhaustion(
                octo=octo,
                chat_id=int(ctx.get("chat_id", 0) or 0),
                notify_user=ctx.get("control_route_notify_user", "always"),
                user_text=user_text,
                messages=messages,
            )
            if continuation_reply is not None:
                return continuation_reply
            messages.append(
                Message(
                    role="system",
                    content=(
                        "Tool execution reached the route budget and autonomous continuation "
                        "was unavailable. Do not promise later work. Give a concise grounded "
                        "status: what is complete, what is blocked, and what exact user input "
                        "or external change is required, if any."
                    ),
                )
            )
            final_resp = await _complete_text(
                provider,
                messages,
                context="tool_limit_fallback",
            )
            return await _finalize_response(
                provider=provider,
                messages=messages,
                response_text=final_resp,
                internal_followup=internal_followup,
                preserve_user_visible_wrapper=preserve_user_visible_wrapper,
            )

        if last_error and _looks_like_tool_error(last_error):
            if internal_followup:
                return "NO_USER_RESPONSE"
            messages.append(
                Message(
                    role="system",
                    content=f"A tool call failed: {last_error}. Explain the problem to the user naturally and ask for guidance if needed.",
                )
            )
            final_resp = await _complete_text(
                provider,
                messages,
                context="tool_error_fallback",
            )
            return await _finalize_response(
                provider=provider,
                messages=messages,
                response_text=final_resp,
                internal_followup=internal_followup,
            )

        return ""

    response_raw = await _complete_text(
        provider,
        messages,
        context="plain_completion",
        on_partial=on_plain_partial,
    )
    logger.debug("Octo output", output=response_raw)
    return await _finalize_response(
        provider=provider,
        messages=messages,
        response_text=response_raw,
        internal_followup=internal_followup,
        preserve_user_visible_wrapper=preserve_user_visible_wrapper,
    )


async def route_worker_result_back_to_octo(
    octo: Any,
    chat_id: int,
    task_text: str,
    result: WorkerResult,
) -> str:
    """Decide next steps after a worker completes its task."""
    return await route_worker_results_back_to_octo(
        octo,
        chat_id,
        [("", task_text, result)],
    )


async def route_worker_results_back_to_octo(
    octo: Any,
    chat_id: int,
    worker_results: list[tuple[str, WorkerResult] | tuple[str, str, WorkerResult]],
    *,
    notify_user: str | None = None,
) -> str:
    """Decide on one combined follow-up after one or more worker updates."""
    normalized_results = [_normalize_worker_result_entry(item) for item in worker_results]
    payload_json = json.dumps(
        [
            _build_worker_result_payload(worker_id, task_text, result)
            for worker_id, task_text, result in normalized_results
        ],
        ensure_ascii=False,
    )

    worker_result_prompt = (
        "One or more worker updates arrived for the same user request. Use this lightweight "
        "worker-result follow-up contract to decide whether the payload can be handled with the "
        "visible tools, needs one combined user follow-up, or should be continued through the "
        "normal Octo route.\n"
        "<worker_results>\n"
        f"{payload_json}\n"
        "</worker_results>\n\n"
        "Interpretation rules:\n"
        "- Each `summary` is internal worker/runtime text and is not user-facing by default.\n"
        "- If a payload has `status=awaiting_instruction` or `output.instruction_request`, the "
        "worker is paused waiting for guidance.\n"
        "- For an awaiting-instruction payload, use `answer_worker_instruction` when you can answer "
        "from current context. Then return exactly NO_USER_RESPONSE unless the user must see an update.\n"
        "- If the instruction requires the user's decision, ask exactly one concise user-facing "
        "question instead of answering the worker yourself.\n"
        "- Never forward transport/debug/auth/orchestration text to the user.\n"
        "- Only `artifact_summary.durable_paths` and `artifact_summary.primary_report_path` are safe "
        "to mention as file outputs for the user.\n"
        "- Treat raw `output.report_path`, `output.output_path`, `output.path`, `output.file`, and "
        "`output.files` as internal unless the artifact summary marks them durable.\n"
        "- If you answer the user, write exactly one clean Octo response in plain language.\n"
        "- Synthesize across the payloads once; do not emit multiple overlapping summaries.\n"
        "- Do not start, stop, schedule, or orchestrate workers directly from this path.\n"
        "- If the worker result requires normal Octo tools, workspace writes outside the durable "
        "artifact roots, scheduling, worker orchestration, A2A, or any other full-turn action, "
        "call `octo_continue_from_control_route` with one concrete continuation task, then return "
        "exactly NO_USER_RESPONSE unless the user must answer a question first.\n"
        "- Do not invent follow-up tool needs beyond the tools already exposed here.\n\n"
        "- Never mention bounded mode, worker-result follow-up mode, full orchestration mode, "
        "or promise that you will take an action on a later turn. If more orchestration is needed, "
        "state only the concrete needed action or ask the user one concise question.\n\n"
        "If a worker was asked to write or save its result to a workspace path and it returned "
        "the content instead, use `fs_write` only when that requested path is under "
        "`reports/` or `artifacts/`. Do not write worker-returned content to any other "
        "workspace path. "
        "Use `manage_canon` only for durable canonical knowledge in the supported canon files.\n\n"
        "If any payload output is truncated and a payload includes `worker_id`, you may use "
        "`get_worker_output_path` for a specific dotted path lookup.\n"
        "If a payload includes `instruction_request`, its `request_id` and `worker_id` are the values "
        "to pass to `answer_worker_instruction`.\n"
        "If there are knowledge_proposals, review them and use `manage_canon` to save them if valid.\n"
        "Return JSON only, with this shape:\n"
        "{\n"
        '  "user_response": string|null,\n'
        '  "no_user_response": boolean,\n'
        '  "actions_taken": [{"type": string, "summary": string}],\n'
        '  "reason": string\n'
        "}\n"
        "`user_response` may contain any useful user-facing answer, including markdown, questions, "
        "or durable file references. It must not contain hidden reasoning, tool notes, transport/debug/auth "
        "text, or <user_visible> wrappers. If no user-facing response is needed, set "
        "`no_user_response` to true and `user_response` to null."
    )

    await octo.set_thinking(True)
    try:
        octo_tools, ctx = _get_worker_followup_tools(octo, chat_id)
        if notify_user is not None:
            ctx["control_route_notify_user"] = notify_user
        tool_policy_summary = _build_octo_tool_policy_summary(
            octo_tools,
            ctx.get("tool_resolution_report"),
        )
        bootstrap_context = await build_bootstrap_context_prompt(octo.store, chat_id)
        messages = await build_octo_prompt(
            store=octo.store,
            memory=octo.memory,
            canon=octo.canon,
            user_text=worker_result_prompt,
            chat_id=chat_id,
            bootstrap_context=bootstrap_context.content,
            is_ws=getattr(octo, "is_ws_active", False),
            images=None,
            saved_file_paths=None,
            wake_notice="",
            tool_policy_summary=tool_policy_summary,
            facts=getattr(octo, "facts", None),
            reflection=getattr(octo, "reflection", None),
        )
        messages.append(
            Message(
                role="system",
                content=(
                    "Worker-result follow-up path rules:\n"
                    "- Keep this turn cheap, bounded, and deterministic.\n"
                    "- Use tools only if they are clearly necessary to inspect a specific worker result detail.\n"
                    "- If visible tools are insufficient and enough context exists, use the continuation tool; "
                    "do not explain internal path limits.\n"
                    "- Return JSON only using the worker follow-up contract from the user message.\n"
                ),
            )
        )
        _log_system_prompt(messages, "worker_followup")
        reply_text = await _complete_route_with_tools(
            octo=octo,
            provider=octo.provider,
            messages=messages,
            tool_specs=octo_tools,
            ctx=ctx,
            internal_followup=True,
            user_text=worker_result_prompt,
            images=None,
            allow_tool_catalog_expansion=False,
        )
        normalized_reply = _normalize_worker_followup_reply(reply_text)
        if (
            normalized_reply != "NO_USER_RESPONSE"
            and not _messages_include_tool_call(messages, "octo_continue_from_control_route")
            and await _worker_followup_requires_autonomous_continuation(
                provider=octo.provider,
                messages=messages,
                worker_results_payload=payload_json,
                reply_text=reply_text,
            )
        ):
            continued = await _continue_worker_followup_autonomously(
                octo=octo,
                chat_id=chat_id,
                tool_specs=octo_tools,
                ctx=ctx,
                worker_result_prompt=worker_result_prompt,
                reply_text=reply_text,
            )
            if continued:
                return "NO_USER_RESPONSE"
            return "I could not finish the remaining work automatically. The task is still active and needs a retry."
        return normalized_reply
    finally:
        await octo.set_thinking(False)


async def _worker_followup_requires_autonomous_continuation(
    *,
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    worker_results_payload: str,
    reply_text: str,
) -> bool:
    return await _worker_followups._worker_followup_requires_autonomous_continuation(
        provider=provider,
        messages=messages,
        worker_results_payload=worker_results_payload,
        reply_text=reply_text,
        complete_text_fn=_complete_text,
    )


async def _continue_worker_followup_autonomously(
    *,
    octo: Any,
    chat_id: int,
    tool_specs: list[ToolSpec],
    ctx: dict[str, Any],
    worker_result_prompt: str,
    reply_text: str,
) -> bool:
    del octo
    return await _worker_followups._continue_worker_followup_autonomously(
        chat_id=chat_id,
        tool_specs=tool_specs,
        ctx=ctx,
        worker_result_prompt=worker_result_prompt,
        reply_text=reply_text,
        find_active_tool_spec_fn=_find_active_tool_spec,
        handle_tool_call_fn=_handle_octo_tool_call,
    )


async def _handle_octo_tool_call(
    call: dict,
    tools: list[ToolSpec],
    ctx: dict[str, object],
) -> tuple[Any, dict[str, Any]]:
    return await _tool_execution._handle_octo_tool_call(
        call,
        tools,
        ctx,
        emit_tool_use_event=_emit_octo_tool_use_event,
    )


async def _maybe_auto_continue_capability_outcome(
    *,
    octo: Any,
    call: dict[str, Any],
    tool_result: Any,
    active_tool_specs: list[ToolSpec],
    ctx: dict[str, object],
    messages: list[Message | dict[str, Any]],
    user_text: str,
    internal_followup: bool,
) -> tuple[str | None, bool]:
    del octo
    return await _route_continuations._maybe_auto_continue_capability_outcome(
        call=call,
        tool_result=tool_result,
        active_tool_specs=active_tool_specs,
        ctx=ctx,
        messages=messages,
        user_text=user_text,
        internal_followup=internal_followup,
        handle_tool_call_fn=_handle_octo_tool_call,
    )


async def _build_plan(
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    has_tools: bool,
) -> dict[str, Any] | None:
    async def _planner_complete_text(
        planner_provider: InferenceProvider,
        planner_messages: list[Message | dict[str, Any]],
    ) -> str:
        return await _complete_text(planner_provider, planner_messages, context="planner")

    return await _route_planning._build_plan(
        provider,
        messages,
        has_tools,
        complete_text_fn=_planner_complete_text,
    )


async def _finalize_response(
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    response_text: str,
    *,
    internal_followup: bool,
    preserve_user_visible_wrapper: bool = False,
) -> str:
    return await _route_verification._finalize_response(
        provider,
        messages,
        response_text,
        internal_followup=internal_followup,
        preserve_user_visible_wrapper=preserve_user_visible_wrapper,
        complete_text_fn=_complete_text,
    )


async def _needs_action_or_blocked_retry(
    *,
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    candidate: str,
) -> bool:
    return await _route_verification._needs_action_or_blocked_retry(
        provider=provider,
        messages=messages,
        candidate=candidate,
        complete_text_fn=_complete_text,
    )


async def _needs_autonomous_recovery_retry(
    *,
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    candidate: str,
) -> bool:
    return await _route_verification._needs_autonomous_recovery_retry(
        provider=provider,
        messages=messages,
        candidate=candidate,
        complete_text_fn=_complete_text,
    )


async def _review_runtime_state_user_response(
    *,
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    candidate: str,
) -> str:
    return await _route_verification._review_runtime_state_user_response(
        provider=provider,
        messages=messages,
        candidate=candidate,
        complete_text_fn=_complete_text,
    )


async def _verify_final_response(
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    candidate: str,
) -> str:
    return await _route_verification._verify_final_response(
        provider,
        messages,
        candidate,
        complete_text_fn=_complete_text,
    )
