from __future__ import annotations

import asyncio
import json
import os
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import structlog

from octopal.browser.manager import get_browser_manager
from octopal.channels.telegram.approvals import ApprovalManager
from octopal.infrastructure.connectors.manager import ConnectorManager
from octopal.infrastructure.logging import correlation_id_var
from octopal.infrastructure.mcp.manager import MCPManager
from octopal.infrastructure.observability.base import (
    TraceSink,
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
from octopal.infrastructure.observability.noop import NoopTraceSink
from octopal.infrastructure.providers.base import InferenceProvider, Message
from octopal.infrastructure.store.base import Store
from octopal.infrastructure.store.models import AuditEvent
from octopal.runtime.housekeeping import (
    cleanup_ephemeral_worker_dirs,
    cleanup_workspace_tmp,
    rotate_canon_events,
)
from octopal.runtime.intents.types import ActionIntent
from octopal.runtime.memory.canon import CanonService
from octopal.runtime.memory.facts import FactsService
from octopal.runtime.memory.memchain import memchain_record
from octopal.runtime.memory.reflection import ReflectionService
from octopal.runtime.memory.service import MemoryService
from octopal.runtime.metrics import update_component_gauges
from octopal.runtime.octo.control_plane import (
    RouteMode,
    RouteRequest,
    resolve_turn_route_mode,
)
from octopal.runtime.octo.delivery import (
    DeliveryMode,
    _result_has_blocking_failure,
    resolve_user_delivery,
    resolve_worker_followup_delivery,
)
from octopal.runtime.octo.prompt_builder import (
    build_bootstrap_context_prompt,
)
from octopal.runtime.octo.router import (
    _complete_text,
    build_forced_worker_followup,
    normalize_plain_text,
    route_heartbeat,
    route_internal_maintenance,
    route_or_reply,
    route_scheduled_octo_control,
    route_scheduler_tick,
    route_worker_results_back_to_octo,
    should_force_worker_followup,
)
from octopal.runtime.policy.engine import PolicyEngine
from octopal.runtime.scheduler.service import (
    SCHEDULED_TASK_BLOCKED_REASON_KEY,
    SCHEDULED_TASK_BLOCKED_UNTIL_KEY,
    SCHEDULED_TASK_DELIVERY_CHAT_ID_KEY,
    SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY,
    SCHEDULED_TASK_TARGET_CHAT_ID_KEY,
    SchedulerService,
    normalize_notify_user_policy,
    parse_scheduled_task_blocked_until,
)
from octopal.runtime.self_control import (
    SELF_RESTART_ACTION,
    SELF_RESTART_REQUESTED_BY,
    SELF_UPDATE_ACTION,
    SELF_UPDATE_REQUESTED_BY,
    append_control_ack,
    append_control_request,
    check_update_status,
    due_self_restart_requests,
    due_self_update_requests,
    launch_restart_helper,
    launch_update_helper,
    mark_restart_resume_consumed,
    read_pending_restart_resume,
    write_pending_restart_resume,
)
from octopal.runtime.state import update_last_internal_heartbeat, update_last_scheduler_tick
from octopal.runtime.workers.contracts import (
    TaskRequest,
    WorkerInstructionRequest,
    WorkerResult,
    WorkerSpec,
)
from octopal.runtime.workers.runtime import WorkerRuntime
from octopal.utils import (
    extract_heartbeat_user_visible_message,
    extract_reaction_and_strip,
    has_no_user_response_suffix,
    is_control_response,
    sanitize_user_facing_text,
    sanitize_user_facing_text_preserving_reaction,
    should_suppress_user_delivery,
    utc_now,
)

logger = structlog.get_logger(__name__)
_FOLLOWUP_QUEUES: dict[int, asyncio.Queue] = {}
_FOLLOWUP_TASKS: dict[int, asyncio.Task] = {}
_INTERNAL_QUEUES: dict[int, asyncio.Queue] = {}
_INTERNAL_TASKS: dict[int, asyncio.Task] = {}
_WORKER_FOLLOWUP_BATCHES: dict[tuple[int, str], _PendingWorkerFollowupBatch] = {}
_QUEUE_IDLE_TIMEOUT_SECONDS = 300.0
# Worker-result follow-up can include multiple provider retries and a fallback
# pass through Octo, so it needs a wider budget than a single LLM request.
_WORKER_RESULT_ROUTING_TIMEOUT_SECONDS = 900.0
_RESET_CONFIRM_THRESHOLD = 2
_RESET_CONFIDENCE_MIN = 0.7


@dataclass
class _PendingWorkerFollowupBatch:
    texts: list[str]
    items: list[_PendingWorkerFollowupItem]
    task: asyncio.Task | None = None
    loop: asyncio.AbstractEventLoop | None = None
    created_during_active_turn: bool = False


@dataclass(frozen=True)
class _PendingWorkerFollowupItem:
    worker_id: str
    task_text: str
    result: WorkerResult
    notify_user: str | None = None


def _build_worker_result_timeout_followup(result: WorkerResult) -> str:
    """Return a minimal user-facing fallback when Octo routing times out."""
    lead = "Worker finished, but the follow-up routing step timed out."

    lines = [lead]
    if result.questions:
        lines.append("")
        lines.append("Open questions:")
        lines.extend(f"- {question}" for question in result.questions[:3] if str(question).strip())

    return "\n".join(lines).strip()


def _build_worker_result_batch_timeout_followup(items: list[_PendingWorkerFollowupItem]) -> str:
    if len(items) == 1:
        return _build_worker_result_timeout_followup(items[0].result)

    lines = [f"{len(items)} worker tasks finished, but the follow-up routing step timed out."]
    questions: list[str] = []
    for item in items:
        for question in item.result.questions[:3]:
            value = str(question).strip()
            if value and value not in questions:
                questions.append(value)
    if questions:
        lines.append("")
        lines.append("Open questions:")
        lines.extend(f"- {question}" for question in questions[:5])
    return "\n".join(lines).strip()


def _is_instruction_request_result(result: WorkerResult) -> bool:
    if str(result.status or "").strip().lower() == "awaiting_instruction":
        return True
    output = result.output if isinstance(result.output, dict) else {}
    return isinstance(output.get("instruction_request"), dict)


def _instruction_request_question(result: WorkerResult) -> str:
    output = result.output if isinstance(result.output, dict) else {}
    request = output.get("instruction_request")
    if isinstance(request, dict):
        question = str(request.get("question") or "").strip()
        if question:
            return question
    if result.questions:
        question = str(result.questions[0] or "").strip()
        if question:
            return question
    return str(result.summary or "").strip()


def _build_worker_followup_batch_result(items: list[_PendingWorkerFollowupItem]) -> WorkerResult:
    summaries = [
        str(item.result.summary or "").strip()
        for item in items
        if str(item.result.summary or "").strip()
    ]
    questions: list[str] = []
    knowledge_proposals = []
    tools_used: list[str] = []
    has_failure = False
    for item in items:
        if _result_has_blocking_failure(item.result):
            has_failure = True
        for question in item.result.questions:
            value = str(question).strip()
            if value and value not in questions:
                questions.append(value)
        for proposal in item.result.knowledge_proposals:
            if proposal not in knowledge_proposals:
                knowledge_proposals.append(proposal)
        for tool_name in item.result.tools_used:
            value = str(tool_name).strip()
            if value and value not in tools_used:
                tools_used.append(value)
    summary = "\n\n".join(summaries)
    if has_failure and "failed" not in summary.lower():
        summary = f"{summary}\n\nAt least one worker failed.".strip()
    return WorkerResult(
        status="failed" if has_failure else "completed",
        summary=summary,
        output={"status": "failed" if has_failure else "completed", "batched_count": len(items)},
        questions=questions,
        knowledge_proposals=knowledge_proposals,
        tools_used=tools_used,
    )


def _build_forced_worker_followup_batch_item(result: WorkerResult) -> str:
    forced_text = build_forced_worker_followup(result).strip()
    if forced_text:
        return forced_text

    summary = sanitize_user_facing_text(result.summary or "").strip()
    if not summary:
        return ""

    summary = re.sub(r"^(?:worker completed|completed)\s*:\s*", "", summary, flags=re.IGNORECASE)
    summary = re.sub(r"\s+", " ", summary).strip(" -")
    if not summary or should_suppress_user_delivery(summary):
        return ""
    if len(summary) > 240:
        summary = summary[:237].rstrip() + "..."
    return summary


def _build_forced_worker_followup_batch(items: list[_PendingWorkerFollowupItem]) -> str:
    if len(items) == 1:
        return build_forced_worker_followup(items[0].result)

    synthetic = _build_worker_followup_batch_result(items)
    if synthetic.questions:
        return "Tasks finished. I need your input on the next step."
    item_summaries: list[str] = []
    for item in items:
        summary = _build_forced_worker_followup_batch_item(item.result)
        if summary and summary not in item_summaries:
            item_summaries.append(summary)
    if item_summaries:
        lead = (
            f"Completed {len(items)} worker tasks, but at least one needs attention:"
            if synthetic.status == "failed"
            else f"Completed {len(items)} worker tasks:"
        )
        bullets = "\n".join(f"- {summary}" for summary in item_summaries[:3])
        return f"{lead}\n{bullets}".strip()
    if synthetic.status == "failed":
        return f"Completed {len(items)} worker tasks, but at least one needs attention."
    return f"Completed {len(items)} worker tasks. The results are ready."


def _combine_worker_followup_notify_policy(items: list[_PendingWorkerFollowupItem]) -> str | None:
    policies = [normalize_notify_user_policy(item.notify_user) for item in items]
    if any(policy == "always" for policy in policies):
        return "always"
    if policies and all(policy == "never" for policy in policies):
        return "never"
    return "if_significant"


def _coerce_control_plane_reply(text: str) -> str:
    """Normalize internal control-plane replies to a strict channel-safe token."""
    value = normalize_plain_text(text or "")
    if is_control_response(value):
        return value
    if has_no_user_response_suffix(value):
        return "NO_USER_RESPONSE"
    return "HEARTBEAT_OK"


async def _normalize_heartbeat_delivery_reply(provider: InferenceProvider | None, text: str) -> str:
    """Normalize heartbeat output to the explicit delivery contract."""
    raw_value = str(text or "")
    explicit = extract_heartbeat_user_visible_message(raw_value)
    if explicit:
        return explicit
    value = normalize_plain_text(raw_value)
    if is_control_response(value) or has_no_user_response_suffix(value):
        return _coerce_control_plane_reply(value)
    if provider is None:
        return _coerce_control_plane_reply(value)

    rewrite_prompt = (
        "Rewrite the draft heartbeat reply into the strict heartbeat delivery contract.\n"
        "Return exactly one of:\n"
        "- HEARTBEAT_OK\n"
        "- NO_USER_RESPONSE\n"
        "- <user_visible>...</user_visible>\n"
        "Use <user_visible> only for a completed result that is explicitly user-facing.\n"
        "Do not include planning, self-talk, tool notes, or any extra text outside the wrapper."
    )
    try:
        rewritten = await _complete_text(
            provider,
            [
                Message(role="system", content=rewrite_prompt),
                Message(role="user", content=f"<draft>\n{value}\n</draft>"),
            ],
            context="heartbeat_delivery_rewrite",
        )
    except Exception:
        logger.debug("Heartbeat delivery rewrite failed", exc_info=True)
        return _coerce_control_plane_reply(value)

    explicit = extract_heartbeat_user_visible_message(rewritten)
    if explicit:
        return explicit
    return _coerce_control_plane_reply(rewritten)


_SCHEDULED_OCTO_CONTROL_DONE = "SCHEDULED_TASK_DONE"
_SCHEDULED_OCTO_CONTROL_BLOCKED = "SCHEDULED_TASK_BLOCKED"
_SCHEDULED_OCTO_CONTROL_BLOCKED_MARKERS = (
    "bounded `octo_control` route",
    "bounded octo_control route",
    "requires external network access",
    "cannot be performed from the bounded",
    "no workers may be launched",
    "no direct weather tools available",
    "requires a worker",
)


def _looks_like_scheduled_octo_control_route_block(text: str) -> bool:
    value = normalize_plain_text(text or "").casefold()
    if not value:
        return False
    return any(marker in value for marker in _SCHEDULED_OCTO_CONTROL_BLOCKED_MARKERS)


def _coerce_scheduled_octo_control_reply(text: str) -> str:
    raw_value = str(text or "")
    explicit = extract_heartbeat_user_visible_message(raw_value)
    if explicit:
        return explicit
    value = normalize_plain_text(raw_value)
    normalized_upper = value.strip().upper()
    if normalized_upper == _SCHEDULED_OCTO_CONTROL_DONE:
        return _SCHEDULED_OCTO_CONTROL_DONE
    if normalized_upper == _SCHEDULED_OCTO_CONTROL_BLOCKED:
        return _SCHEDULED_OCTO_CONTROL_BLOCKED
    if normalized_upper == "NO_USER_RESPONSE" or has_no_user_response_suffix(value):
        return "NO_USER_RESPONSE"
    return "NO_USER_RESPONSE"


async def _normalize_scheduled_octo_control_reply(
    provider: InferenceProvider | None,
    text: str,
) -> str:
    raw_value = str(text or "")
    explicit = extract_heartbeat_user_visible_message(raw_value)
    if explicit:
        return explicit
    value = normalize_plain_text(raw_value)
    normalized_upper = value.strip().upper()
    if normalized_upper == _SCHEDULED_OCTO_CONTROL_DONE:
        return _SCHEDULED_OCTO_CONTROL_DONE
    if normalized_upper == _SCHEDULED_OCTO_CONTROL_BLOCKED:
        return _SCHEDULED_OCTO_CONTROL_BLOCKED
    if normalized_upper == "NO_USER_RESPONSE" or has_no_user_response_suffix(value):
        return "NO_USER_RESPONSE"
    if provider is None:
        return _coerce_scheduled_octo_control_reply(value)

    rewrite_prompt = (
        "Rewrite the draft scheduled Octo control reply into the strict completion contract.\n"
        "Return exactly one of:\n"
        "- SCHEDULED_TASK_DONE\n"
        "- SCHEDULED_TASK_BLOCKED\n"
        "- NO_USER_RESPONSE\n"
        "- <user_visible>...</user_visible>\n"
        "Use SCHEDULED_TASK_DONE only if the task completed successfully with no user-visible update.\n"
        "Use SCHEDULED_TASK_BLOCKED when the task cannot complete from the bounded route because it needs workers, external access, or unavailable tools.\n"
        "Use <user_visible> only for a concise completed user-facing update.\n"
        "Use NO_USER_RESPONSE if the task did not complete or there is no completion signal.\n"
        "Do not include any extra text outside the token or wrapper."
    )
    try:
        rewritten = await _complete_text(
            provider,
            [
                Message(role="system", content=rewrite_prompt),
                Message(role="user", content=f"<draft>\n{value}\n</draft>"),
            ],
            context="scheduled_octo_control_delivery_rewrite",
        )
    except Exception:
        logger.debug("Scheduled Octo control delivery rewrite failed", exc_info=True)
        return _coerce_scheduled_octo_control_reply(value)

    return _coerce_scheduled_octo_control_reply(rewritten)


def _normalize_scheduled_octo_control_notify_policy(notify_user: str | None) -> str:
    policy = normalize_notify_user_policy(notify_user)
    if policy == "if_significant":
        return "never"
    return policy


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return max(minimum, value)


def _env_float(name: str, default: float, *, minimum: float = 0.0, maximum: float = 1.0) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return min(maximum, max(minimum, value))


_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS = float(
    _env_int(
        "OCTOPAL_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS",
        8,
        minimum=1,
    )
)


_PENDING_CONVERSATIONAL_CLOSURE_TTL_SECONDS = _env_int(
    "OCTOPAL_PENDING_CONVERSATIONAL_CLOSURE_TTL_SECONDS",
    3600,
    minimum=60,
)
_HEARTBEAT_USER_VISIBLE_COOLDOWN_SECONDS = _env_int(
    "OCTOPAL_HEARTBEAT_USER_VISIBLE_COOLDOWN_SECONDS",
    300,
    minimum=0,
)
_RECENT_WORKER_TASK_TTL_SECONDS = float(
    _env_int(
        "OCTOPAL_RECENT_WORKER_TASK_TTL_SECONDS",
        1800,
        minimum=60,
    )
)
_SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS = float(
    _env_int(
        "OCTOPAL_SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS",
        1800,
        minimum=0,
    )
)


_WATCH_THRESHOLDS = {
    "context_size_estimate": _env_int("OCTOPAL_CONTEXT_WATCH_SIZE", 150000, minimum=5000),
    "repetition_score": _env_float(
        "OCTOPAL_CONTEXT_WATCH_REPETITION", 0.65, minimum=0.0, maximum=1.0
    ),
    "error_streak": _env_int("OCTOPAL_CONTEXT_WATCH_ERROR_STREAK", 3, minimum=1),
    "no_progress_turns": _env_int("OCTOPAL_CONTEXT_WATCH_NO_PROGRESS", 4, minimum=1),
}
_RESET_SOON_THRESHOLDS = {
    "context_size_estimate": _env_int("OCTOPAL_CONTEXT_RESET_SOON_SIZE", 250000, minimum=5000),
    "repetition_score": _env_float(
        "OCTOPAL_CONTEXT_RESET_SOON_REPETITION", 0.75, minimum=0.0, maximum=1.0
    ),
    "error_streak": _env_int("OCTOPAL_CONTEXT_RESET_SOON_ERROR_STREAK", 5, minimum=1),
    "no_progress_turns": _env_int("OCTOPAL_CONTEXT_RESET_SOON_NO_PROGRESS", 7, minimum=1),
}

# Keep RESET_SOON at or above WATCH thresholds, even with custom env values.
_RESET_SOON_THRESHOLDS["context_size_estimate"] = max(
    int(_RESET_SOON_THRESHOLDS["context_size_estimate"]),
    int(_WATCH_THRESHOLDS["context_size_estimate"]),
)
_RESET_SOON_THRESHOLDS["repetition_score"] = max(
    float(_RESET_SOON_THRESHOLDS["repetition_score"]),
    float(_WATCH_THRESHOLDS["repetition_score"]),
)
_RESET_SOON_THRESHOLDS["error_streak"] = max(
    int(_RESET_SOON_THRESHOLDS["error_streak"]),
    int(_WATCH_THRESHOLDS["error_streak"]),
)
_RESET_SOON_THRESHOLDS["no_progress_turns"] = max(
    int(_RESET_SOON_THRESHOLDS["no_progress_turns"]),
    int(_WATCH_THRESHOLDS["no_progress_turns"]),
)

_WORKER_TIMEOUT_MIN_SECONDS = 30
_WORKER_TIMEOUT_MAX_SECONDS = 1800
_TIMEOUT_STEP_PATTERN = re.compile(r"(?im)^\s*(?:step\s+\d+[:.)-]?|\d+[.)]|[-*])\s+")
_NETWORK_TOOL_MARKERS = (
    "mcp_",
    "web_",
    "browser",
    "fetch",
    "search",
    "crawl",
    "http",
    "api",
)
_CONTEXT_HEAVY_TASK_MARKERS = (
    "full",
    "entire",
    "whole",
    "conversation",
    "thread",
    "history",
    "transcript",
    "timeline",
    "dm",
    "inbox",
    "mailbox",
    "email",
    "messages",
    "message history",
    "read through",
    "catch up",
    "review",
    "digest",
    "summarize",
)
_SYNTHESIS_HEAVY_TASK_MARKERS = (
    "reply",
    "respond",
    "draft",
    "write back",
    "thoughtful",
    "careful",
    "analyze",
    "compare",
    "recommend",
    "decide",
)


def _clamp_worker_timeout(timeout_seconds: float) -> int:
    return max(
        _WORKER_TIMEOUT_MIN_SECONDS,
        min(_WORKER_TIMEOUT_MAX_SECONDS, int(round(timeout_seconds))),
    )


def _resolve_worker_timeout_seconds(
    *,
    explicit_timeout_seconds: int | None,
    template: Any | None,
    task: str,
    tools: list[str] | None,
    scheduled_task_id: str | None,
) -> tuple[int, dict[str, Any]]:
    """Resolve worker timeout from explicit override or task/template heuristics."""
    if explicit_timeout_seconds is not None:
        explicit = max(1, int(explicit_timeout_seconds))
        return explicit, {"source": "explicit", "reasons": ["explicit_override"]}

    template_default = int(getattr(template, "default_timeout_seconds", 300) or 300)
    timeout = float(max(_WORKER_TIMEOUT_MIN_SECONDS, template_default))
    reasons: list[str] = [f"template_default={template_default}"]

    effective_tools = [
        str(tool_name).strip().lower()
        for tool_name in (tools or getattr(template, "available_tools", []) or [])
        if str(tool_name).strip()
    ]
    permissions = {
        str(permission).strip().lower()
        for permission in (getattr(template, "required_permissions", []) or [])
        if str(permission).strip()
    }
    lowered_task = (task or "").lower()
    word_count = len(re.findall(r"\w+", lowered_task))
    step_count = len(_TIMEOUT_STEP_PATTERN.findall(task or ""))
    network_bound = "network" in permissions or any(
        marker in tool_name for tool_name in effective_tools for marker in _NETWORK_TOOL_MARKERS
    )
    context_hits = sum(1 for marker in _CONTEXT_HEAVY_TASK_MARKERS if marker in lowered_task)
    synthesis_hits = sum(1 for marker in _SYNTHESIS_HEAVY_TASK_MARKERS if marker in lowered_task)

    if scheduled_task_id:
        timeout = max(timeout, 180.0)
        reasons.append("scheduled_task")
    if network_bound:
        timeout *= 1.25
        reasons.append("network_bound")
    if step_count >= 2:
        timeout *= 1.0 + min(0.30, (step_count - 1) * 0.10)
        reasons.append(f"step_count={step_count}")
    if word_count >= 80:
        timeout *= 1.15
        reasons.append(f"task_words={word_count}")
    if context_hits >= 2:
        timeout *= 1.30
        reasons.append(f"context_heavy={context_hits}")
    elif context_hits == 1:
        timeout *= 1.15
        reasons.append("context_heavy=1")
    if synthesis_hits >= 2:
        timeout *= 1.20
        reasons.append(f"synthesis_heavy={synthesis_hits}")
    elif synthesis_hits == 1:
        timeout *= 1.10
        reasons.append("synthesis_heavy=1")
    if context_hits and synthesis_hits:
        timeout = max(timeout, template_default * 2.0)
        reasons.append("retrieve_then_synthesize")

    return _clamp_worker_timeout(timeout), {"source": "policy", "reasons": reasons}


def _publish_runtime_metrics(thinking_count: int = 0) -> None:
    update_component_gauges(
        "octo",
        {
            "followup_queues": len(_FOLLOWUP_QUEUES),
            "followup_tasks": len(_FOLLOWUP_TASKS),
            "internal_queues": len(_INTERNAL_QUEUES),
            "internal_tasks": len(_INTERNAL_TASKS),
            "thinking_count": thinking_count,
        },
    )


def _empty_scheduler_metric_counters() -> dict[str, int]:
    return {
        "ticks_total": 0,
        "failures_total": 0,
        "started_total": 0,
        "completed_total": 0,
        "duplicates_total": 0,
        "rejected_by_policy_total": 0,
        "errors_total": 0,
    }


def _coerce_positive_chat_id(value: Any) -> int | None:
    try:
        chat_id = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return chat_id if chat_id > 0 else None


async def _followup_worker(chat_id: int, queue: asyncio.Queue) -> None:
    while True:
        try:
            future, coro = await asyncio.wait_for(queue.get(), timeout=_QUEUE_IDLE_TIMEOUT_SECONDS)
        except TimeoutError:
            break
        try:
            result = await coro
            if not future.cancelled():
                future.set_result(result)
        except Exception as exc:
            if not future.cancelled():
                future.set_exception(exc)
        finally:
            queue.task_done()
    _FOLLOWUP_TASKS.pop(chat_id, None)
    if queue.empty():
        _FOLLOWUP_QUEUES.pop(chat_id, None)
    _publish_runtime_metrics()


def _enqueue_followup(chat_id: int, coro) -> asyncio.Future[str]:
    loop = asyncio.get_running_loop()
    queue = _FOLLOWUP_QUEUES.get(chat_id)
    if queue is not None and getattr(queue, "_loop", None) not in (None, loop):
        _FOLLOWUP_QUEUES.pop(chat_id, None)
        prior_task = _FOLLOWUP_TASKS.pop(chat_id, None)
        if prior_task and not prior_task.done():
            prior_task.cancel()
        queue = None
    if not queue:
        queue = asyncio.Queue()
        _FOLLOWUP_QUEUES[chat_id] = queue
    if chat_id not in _FOLLOWUP_TASKS or _FOLLOWUP_TASKS[chat_id].done():
        _FOLLOWUP_TASKS[chat_id] = asyncio.create_task(_followup_worker(chat_id, queue))
    _publish_runtime_metrics()
    future: asyncio.Future[str] = loop.create_future()
    queue.put_nowait((future, coro))
    return future


def _merge_worker_followup_texts(texts: list[str]) -> str:
    merged: list[str] = []
    seen: set[str] = set()
    for raw_text in texts:
        text = normalize_plain_text(raw_text)
        if not resolve_user_delivery(text).user_visible:
            continue
        fingerprint = text.casefold()
        if fingerprint in seen:
            continue
        replacement_index: int | None = None
        should_skip = False
        for idx, existing in enumerate(merged):
            overlap = _worker_followup_overlap(existing, text)
            if overlap == "existing_contains_new":
                logger.info(
                    "Dropped overlapping worker follow-up",
                    kept_len=len(existing),
                    dropped_len=len(text),
                    reason="existing_contains_new",
                )
                should_skip = True
                break
            if overlap == "new_contains_existing":
                logger.info(
                    "Replacing overlapping worker follow-up",
                    prior_len=len(existing),
                    replacement_len=len(text),
                    reason="new_contains_existing",
                )
                replacement_index = idx
                break
        if should_skip:
            continue
        seen.add(fingerprint)
        if replacement_index is not None:
            prior = merged[replacement_index]
            seen.discard(prior.casefold())
            merged[replacement_index] = text
        else:
            merged.append(text)
    if not merged:
        return ""
    if len(merged) == 1:
        return merged[0]
    return "\n\n".join(merged)


def _worker_followup_overlap(existing: str, candidate: str) -> str | None:
    existing_norm = _normalize_compact(existing)
    candidate_norm = _normalize_compact(candidate)
    if not existing_norm or not candidate_norm:
        return None
    if existing_norm == candidate_norm:
        return "existing_contains_new"
    if existing_norm in candidate_norm:
        return "new_contains_existing"
    if candidate_norm in existing_norm:
        return "existing_contains_new"

    existing_words = set(_worker_followup_keywords(existing_norm))
    candidate_words = set(_worker_followup_keywords(candidate_norm))
    if len(existing_words) < 12 or len(candidate_words) < 12:
        return None

    shared = existing_words.intersection(candidate_words)
    if not shared:
        return None

    containment = len(shared) / float(min(len(existing_words), len(candidate_words)))
    if containment < 0.72:
        return None

    if len(candidate_norm) >= int(len(existing_norm) * 1.2):
        return "new_contains_existing"
    if len(existing_norm) >= int(len(candidate_norm) * 1.2):
        return "existing_contains_new"
    return None


def _worker_followup_keywords(value: str) -> list[str]:
    return re.findall(r"\w+", value, flags=re.UNICODE)


async def _start_background_trace_context(
    trace_sink: TraceSink | None,
    *,
    name: str,
    chat_id: int,
    correlation_id: str | None,
    metadata: dict[str, Any] | None = None,
) -> tuple[Any | None, Any | None, bool]:
    if trace_sink is None:
        return None, None, False
    parent_trace_ctx = get_current_trace_context()
    if parent_trace_ctx is not None:
        trace_ctx = await trace_sink.start_span(
            parent_trace_ctx,
            name=name,
            metadata=metadata,
        )
        return trace_ctx, bind_trace_context(trace_ctx), False
    trace_id = f"{name.replace('.', '-')}-{uuid4().hex}"
    root_trace_id = str(correlation_id or trace_id)
    trace_ctx = await trace_sink.start_trace(
        name=name,
        trace_id=trace_id,
        root_trace_id=root_trace_id,
        session_id=f"chat:{chat_id}",
        chat_id=chat_id,
        metadata=metadata,
    )
    return trace_ctx, bind_trace_context(trace_ctx), True


async def _finish_background_trace_context(
    trace_sink: TraceSink | None,
    trace_ctx: Any | None,
    trace_token: Any | None,
    *,
    is_root_trace: bool,
    status: str,
    output: dict[str, Any] | None,
    metadata: dict[str, Any] | None,
) -> None:
    try:
        if trace_ctx is None or trace_sink is None:
            return
        if is_root_trace:
            await trace_sink.finish_trace(
                trace_ctx,
                status=status,
                output=output,
                metadata=metadata,
            )
            return
        await trace_sink.finish_span(
            trace_ctx,
            status=status,
            output=output,
            metadata=metadata,
        )
    finally:
        if trace_token is not None:
            reset_trace_context(trace_token)


async def _send_worker_followup(
    octo: Octo,
    chat_id: int,
    correlation_id: str | None,
    text: str,
    *,
    batched_count: int = 1,
) -> None:
    trace_started_at_ms = now_ms()
    trace_metadata: dict[str, Any] = {
        "delivery_channel": "internal",
        "delivery_source": "worker_followup",
        "correlation_id": correlation_id,
        "batched_count": batched_count,
        "text_preview": safe_preview(text, limit=240),
        "text_len": len(text or ""),
    }
    trace_ctx, trace_token, is_root_trace = await _start_background_trace_context(
        octo.trace_sink,
        name="channel.delivery",
        chat_id=chat_id,
        correlation_id=correlation_id,
        metadata=trace_metadata,
    )
    trace_status = "ok"
    trace_output: dict[str, Any] | None = None
    try:
        decision = resolve_user_delivery(text)
        trace_metadata.update(
            {
                "delivery_mode": decision.mode,
                "user_visible": decision.user_visible,
                "suppressed_reason": decision.reason,
            }
        )
        if not decision.user_visible:
            trace_output = {"status": "suppressed", "reason": decision.reason}
            logger.info(
                "Internal worker follow-up skipped", chat_id=chat_id, reason=decision.reason
            )
            return
        if octo.internal_send:
            await octo.internal_send(chat_id, decision.text)
            octo.note_user_visible_delivery(chat_id, decision.text)
            octo.clear_pending_conversational_closure(correlation_id)
            trace_output = {
                "status": "sent",
                "message_len": len(decision.text),
                "batched_count": batched_count,
            }
            logger.info(
                "Internal worker follow-up sent",
                chat_id=chat_id,
                text_len=len(decision.text),
                batched_count=batched_count,
            )
            await octo.memory.add_message(
                "assistant",
                decision.text,
                {
                    "chat_id": chat_id,
                    "worker_followup": True,
                    "batched_count": batched_count,
                },
            )
            return
        trace_status = "error"
        trace_metadata["error_type"] = "no_sender_attached"
        trace_output = {"status": "dropped", "reason": "no_sender_attached"}
        logger.info(
            "Worker follow-up produced but no sender attached",
            chat_id=chat_id,
            text_len=len(text),
            batched_count=batched_count,
        )
    except Exception as exc:
        trace_status = "error"
        trace_metadata.update(summarize_exception(exc))
        trace_output = {"status": "failed"}
        raise
    finally:
        trace_metadata["duration_ms"] = round(now_ms() - trace_started_at_ms, 2)
        await _finish_background_trace_context(
            octo.trace_sink,
            trace_ctx,
            trace_token,
            is_root_trace=is_root_trace,
            status=trace_status,
            output=trace_output,
            metadata=trace_metadata,
        )


async def _send_scheduler_control_update(
    octo: Octo,
    chat_id: int,
    task_id: str | None,
    text: str,
) -> None:
    trace_started_at_ms = now_ms()
    trace_metadata: dict[str, Any] = {
        "delivery_channel": "internal",
        "delivery_source": "scheduler_octo_control",
        "scheduled_task_id": task_id,
        "text_preview": safe_preview(text, limit=240),
        "text_len": len(text or ""),
    }
    trace_ctx, trace_token, is_root_trace = await _start_background_trace_context(
        octo.trace_sink,
        name="channel.delivery",
        chat_id=chat_id,
        correlation_id=None,
        metadata=trace_metadata,
    )
    trace_status = "ok"
    trace_output: dict[str, Any] | None = None
    try:
        decision = resolve_user_delivery(text)
        trace_metadata.update(
            {
                "delivery_mode": decision.mode,
                "user_visible": decision.user_visible,
                "suppressed_reason": decision.reason,
            }
        )
        if not decision.user_visible:
            trace_output = {"status": "suppressed", "reason": decision.reason}
            logger.info(
                "Scheduled Octo control update skipped",
                chat_id=chat_id,
                task_id=task_id,
                reason=decision.reason,
            )
            return
        if octo.internal_send:
            await octo.internal_send(chat_id, decision.text)
            octo.note_user_visible_delivery(chat_id, decision.text)
            trace_output = {
                "status": "sent",
                "message_len": len(decision.text),
            }
            logger.info(
                "Scheduled Octo control update sent",
                chat_id=chat_id,
                task_id=task_id,
                text_len=len(decision.text),
            )
            await octo.memory.add_message(
                "assistant",
                decision.text,
                {
                    "chat_id": chat_id,
                    "background_delivery": True,
                    "scheduler_octo_control": True,
                    "scheduled_task_id": task_id,
                },
            )
            return
        trace_status = "error"
        trace_metadata["error_type"] = "no_sender_attached"
        trace_output = {"status": "dropped", "reason": "no_sender_attached"}
        logger.info(
            "Scheduled Octo control update produced but no sender attached",
            chat_id=chat_id,
            task_id=task_id,
            text_len=len(text),
        )
    except Exception as exc:
        trace_status = "error"
        trace_metadata.update(summarize_exception(exc))
        trace_output = {"status": "failed"}
        raise
    finally:
        trace_metadata["duration_ms"] = round(now_ms() - trace_started_at_ms, 2)
        await _finish_background_trace_context(
            octo.trace_sink,
            trace_ctx,
            trace_token,
            is_root_trace=is_root_trace,
            status=trace_status,
            output=trace_output,
            metadata=trace_metadata,
        )


async def _flush_worker_followup_batch(octo: Octo, chat_id: int, correlation_id: str) -> None:
    trace_started_at_ms = now_ms()
    trace_metadata: dict[str, Any] = {
        "correlation_id": correlation_id,
        "batch_window_seconds": _WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS,
        "routing_timeout_seconds": _WORKER_RESULT_ROUTING_TIMEOUT_SECONDS,
    }
    trace_ctx, trace_token, is_root_trace = await _start_background_trace_context(
        octo.trace_sink,
        name="worker.followup",
        chat_id=chat_id,
        correlation_id=correlation_id,
        metadata=trace_metadata,
    )
    trace_status = "ok"
    trace_output: dict[str, Any] | None = None
    try:
        await asyncio.sleep(_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS)
        batch_key = (chat_id, correlation_id)
        batch = _WORKER_FOLLOWUP_BATCHES.pop(batch_key, None)
        if batch is None:
            trace_output = {"status": "empty_batch"}
            return
        batched_count = len(batch.texts) + len(batch.items)
        trace_metadata.update(
            {
                "batched_count": batched_count,
                "text_count": len(batch.texts),
                "worker_result_count": len(batch.items),
                "created_during_active_turn": batch.created_during_active_turn,
            }
        )
        merged_texts = list(batch.texts)

        if batch.items:
            try:
                batched_text = await asyncio.wait_for(
                    route_worker_results_back_to_octo(
                        octo,
                        chat_id,
                        [(item.worker_id, item.task_text, item.result) for item in batch.items],
                    ),
                    timeout=_WORKER_RESULT_ROUTING_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                trace_metadata["routing_timed_out"] = True
                logger.warning(
                    "Worker-result batch routing timed out",
                    chat_id=chat_id,
                    batched_count=len(batch.items),
                )
                batched_text = _build_worker_result_batch_timeout_followup(batch.items)

            pending_closure = octo.has_pending_conversational_closure(correlation_id)
            synthetic_result = _build_worker_followup_batch_result(batch.items)
            notify_user = _combine_worker_followup_notify_policy(batch.items)
            trace_metadata.update(
                {
                    "pending_closure": pending_closure,
                    "notify_user_policy_resolved": notify_user,
                    "synthetic_result_status": synthetic_result.status,
                    "questions_count": len(synthetic_result.questions),
                }
            )
            delivery = resolve_worker_followup_delivery(
                batched_text,
                result=synthetic_result,
                pending_closure=pending_closure,
                suppress_followup=octo.should_suppress_turn_followups(correlation_id),
                should_force=any(should_force_worker_followup(item.result) for item in batch.items),
                notify_user=notify_user,
                forced_text_factory=lambda _result: _build_forced_worker_followup_batch(
                    batch.items
                ),
            )
            if delivery.reason == "forced_substantive_followup":
                logger.info(
                    "Forcing substantive batched worker follow-up",
                    chat_id=chat_id,
                    batched_count=len(batch.items),
                )
            if delivery.user_visible:
                trace_metadata["delivery_reason"] = delivery.reason
                merged_texts.append(delivery.text)
            else:
                octo.clear_pending_conversational_closure(correlation_id)
                trace_output = {"status": "suppressed", "reason": delivery.reason}
                if not merged_texts:
                    logger.info(
                        "Internal worker follow-up skipped",
                        chat_id=chat_id,
                        reason=delivery.reason,
                        batched_count=batched_count,
                    )
                    return

        final_text = _merge_worker_followup_texts(merged_texts)
        if not final_text:
            octo.clear_pending_conversational_closure(correlation_id)
            trace_output = {"status": "suppressed", "reason": "empty_batched_followup"}
            logger.info(
                "Internal worker follow-up skipped",
                chat_id=chat_id,
                reason="empty_batched_followup",
                batched_count=batched_count,
            )
            return
        trace_output = {
            "status": "ready_to_send",
            "final_text_preview": safe_preview(final_text, limit=240),
            "final_text_len": len(final_text),
            "batched_count": batched_count,
        }
        await _send_worker_followup(
            octo,
            chat_id,
            correlation_id,
            final_text,
            batched_count=batched_count,
        )
    except asyncio.CancelledError:
        trace_status = "error"
        trace_metadata["error_type"] = "CancelledError"
        trace_output = {"status": "cancelled"}
        raise
    except Exception as exc:
        trace_status = "error"
        trace_metadata.update(summarize_exception(exc))
        trace_output = {"status": "failed"}
        logger.exception("Failed to flush batched worker follow-up", chat_id=chat_id)
    finally:
        trace_metadata["duration_ms"] = round(now_ms() - trace_started_at_ms, 2)
        await _finish_background_trace_context(
            octo.trace_sink,
            trace_ctx,
            trace_token,
            is_root_trace=is_root_trace,
            status=trace_status,
            output=trace_output,
            metadata=trace_metadata,
        )


def _schedule_worker_followup_flush(octo: Octo, chat_id: int, correlation_id: str | None) -> None:
    if not correlation_id:
        return
    batch_key = (chat_id, correlation_id)
    batch = _WORKER_FOLLOWUP_BATCHES.get(batch_key)
    if batch is None:
        return
    if octo.should_suppress_turn_followups(correlation_id):
        existing_task = batch.task
        if existing_task and not existing_task.done():
            existing_task.cancel()
        batch.task = None
        return
    if not octo.should_flush_worker_followups(correlation_id):
        existing_task = batch.task
        if existing_task and not existing_task.done():
            existing_task.cancel()
        batch.task = None
        return
    existing_task = batch.task
    if existing_task and not existing_task.done():
        existing_task.cancel()
    batch.task = asyncio.create_task(_flush_worker_followup_batch(octo, chat_id, correlation_id))


def _discard_worker_followup_batch(
    chat_id: int,
    correlation_id: str | None,
    *,
    only_if_created_during_active_turn: bool = False,
) -> bool:
    if not correlation_id:
        return False
    batch_key = (chat_id, correlation_id)
    batch = _WORKER_FOLLOWUP_BATCHES.get(batch_key)
    if batch is None:
        return False
    if only_if_created_during_active_turn and not batch.created_during_active_turn:
        return False
    task = batch.task
    if task and not task.done():
        task.cancel()
    _WORKER_FOLLOWUP_BATCHES.pop(batch_key, None)
    return True


async def _enqueue_batched_worker_followup(
    octo: Octo,
    chat_id: int,
    correlation_id: str | None,
    text: str | None = None,
    *,
    worker_id: str = "",
    task_text: str | None = None,
    result: WorkerResult | None = None,
    notify_user: str | None = None,
) -> None:
    if not correlation_id:
        if text is not None:
            await _send_worker_followup(octo, chat_id, correlation_id, text)
            return
        if task_text is not None and result is not None:
            routed_text = await route_worker_results_back_to_octo(
                octo,
                chat_id,
                [(str(worker_id or "").strip(), task_text, result)],
            )
            decision = resolve_user_delivery(routed_text)
            if decision.user_visible:
                await _send_worker_followup(octo, chat_id, correlation_id, decision.text)
        return

    loop = asyncio.get_running_loop()
    batch_key = (chat_id, correlation_id)
    batch = _WORKER_FOLLOWUP_BATCHES.get(batch_key)
    if batch is not None and batch.loop not in (None, loop):
        prior_task = batch.task
        if prior_task and not prior_task.done():
            prior_task.cancel()
        _WORKER_FOLLOWUP_BATCHES.pop(batch_key, None)
        batch = None
    if batch is None:
        batch = _PendingWorkerFollowupBatch(texts=[], items=[], loop=loop)
        _WORKER_FOLLOWUP_BATCHES[batch_key] = batch
    if text is not None:
        batch.texts.append(text)
    if task_text is not None and result is not None:
        batch.items.append(
            _PendingWorkerFollowupItem(
                worker_id=str(worker_id or "").strip(),
                task_text=task_text,
                result=result,
                notify_user=notify_user,
            )
        )
    if octo.has_active_user_turn(correlation_id):
        batch.created_during_active_turn = True
    _schedule_worker_followup_flush(octo, chat_id, correlation_id)


async def _internal_worker(octo: Octo, chat_id: int, queue: asyncio.Queue) -> None:
    """Process completed worker results.

    Worker results are logged and stored in memory but NOT automatically sent to the user.
    The octo decides what to communicate based on worker results.
    """
    while True:
        correlation_id: str | None = None
        try:
            item = await asyncio.wait_for(queue.get(), timeout=_QUEUE_IDLE_TIMEOUT_SECONDS)
            notify_user = None
            worker_id = ""
            if len(item) == 5:
                worker_id, task_text, result, correlation_id, notify_user = item
            elif len(item) == 4:
                task_text, result, correlation_id, notify_user = item
            else:
                task_text, result, correlation_id = item
        except TimeoutError:
            break
        try:
            logger.info(
                "Processing internal worker result",
                chat_id=chat_id,
                summary_len=len(result.summary or ""),
            )
            # Add worker result to memory for context, but don't auto-send
            if result.summary:
                memory_prefix = (
                    "Worker requested instruction"
                    if _is_instruction_request_result(result)
                    else "Worker completed"
                )
                await octo.memory.add_message(
                    "system",
                    f"{memory_prefix}: {result.summary}",
                    {"worker_result": True, "task": task_text, "chat_id": chat_id},
                )
            output_error = ""
            if isinstance(result.output, dict):
                raw_error = result.output.get("error")
                if raw_error is not None:
                    output_error = str(raw_error).strip()
            if output_error:
                await octo.memory.add_message(
                    "system",
                    f"Worker error: {output_error}",
                    {"worker_result": True, "task": task_text, "chat_id": chat_id},
                )
            if _is_instruction_request_result(result):
                await _route_instruction_request_to_octo(
                    octo,
                    chat_id,
                    worker_id=worker_id,
                    task_text=task_text,
                    result=result,
                    correlation_id=correlation_id,
                )
            # System/internal chat (chat_id <= 0) should never emit user-facing follow-ups.
            elif chat_id <= 0:
                logger.info("Skipping user follow-up for internal chat", chat_id=chat_id)
            else:
                notify_policy = normalize_notify_user_policy(notify_user)
                if notify_policy == "never" and not _result_has_blocking_failure(result):
                    octo.clear_pending_conversational_closure(correlation_id)
                    logger.info(
                        "Internal worker follow-up skipped",
                        chat_id=chat_id,
                        reason="scheduled_notify_never",
                    )
                else:
                    await _enqueue_batched_worker_followup(
                        octo,
                        chat_id,
                        correlation_id,
                        worker_id=worker_id,
                        task_text=task_text,
                        result=result,
                        notify_user=notify_user,
                    )
                    logger.info(
                        "Internal worker follow-up deferred",
                        chat_id=chat_id,
                        reason="batched_worker_results",
                    )
            logger.debug("Worker result processed", summary_len=len(result.summary or ""))
        except Exception:
            logger.exception("Failed to process internal worker result")
        finally:
            octo.mark_internal_result_processed(correlation_id)
            if octo.should_flush_worker_followups(correlation_id):
                octo.clear_suppressed_turn_followups(correlation_id)
            _schedule_worker_followup_flush(octo, chat_id, correlation_id)
            queue.task_done()
    _INTERNAL_TASKS.pop(chat_id, None)
    if queue.empty():
        _INTERNAL_QUEUES.pop(chat_id, None)
    _publish_runtime_metrics()


async def _route_instruction_request_to_octo(
    octo: Octo,
    chat_id: int,
    *,
    worker_id: str,
    task_text: str,
    result: WorkerResult,
    correlation_id: str | None,
) -> None:
    logger.info(
        "Routing worker instruction request to Octo",
        chat_id=chat_id,
        worker_id=worker_id,
        correlation_id=correlation_id,
    )
    try:
        reply_text = await asyncio.wait_for(
            route_worker_results_back_to_octo(
                octo,
                chat_id,
                [(worker_id, task_text, result)],
            ),
            timeout=_WORKER_RESULT_ROUTING_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning(
            "Worker instruction request routing timed out",
            chat_id=chat_id,
            worker_id=worker_id,
        )
        reply_text = _instruction_request_question(result)

    decision = resolve_user_delivery(reply_text)
    if not decision.user_visible:
        logger.info(
            "Worker instruction request handled without user follow-up",
            chat_id=chat_id,
            worker_id=worker_id,
            reason=decision.reason,
        )
        return
    if chat_id <= 0:
        logger.info(
            "Skipping user-visible worker instruction follow-up for internal chat",
            chat_id=chat_id,
            worker_id=worker_id,
        )
        return
    await _send_worker_followup(octo, chat_id, correlation_id, decision.text, batched_count=1)


def _enqueue_internal_result(
    octo: Octo,
    chat_id: int,
    worker_id: str,
    task_text: str,
    result: WorkerResult,
    *,
    correlation_id: str | None,
    notify_user: str | None = None,
) -> None:
    loop = asyncio.get_running_loop()
    queue = _INTERNAL_QUEUES.get(chat_id)
    if queue is not None and getattr(queue, "_loop", None) not in (None, loop):
        _INTERNAL_QUEUES.pop(chat_id, None)
        prior_task = _INTERNAL_TASKS.pop(chat_id, None)
        if prior_task and not prior_task.done():
            prior_task.cancel()
        queue = None
    if not queue:
        queue = asyncio.Queue()
        _INTERNAL_QUEUES[chat_id] = queue
    if chat_id not in _INTERNAL_TASKS or _INTERNAL_TASKS[chat_id].done():
        _INTERNAL_TASKS[chat_id] = asyncio.create_task(_internal_worker(octo, chat_id, queue))
    octo.mark_internal_result_pending(correlation_id)
    queue.put_nowait((str(worker_id or "").strip(), task_text, result, correlation_id, notify_user))
    logger.info("Queued internal worker result", chat_id=chat_id, queue_size=queue.qsize())
    _publish_runtime_metrics()


@dataclass
class Octo:
    provider: InferenceProvider
    store: Store
    policy: PolicyEngine
    runtime: WorkerRuntime
    approvals: ApprovalManager
    memory: MemoryService
    canon: CanonService
    facts: FactsService | None = None
    reflection: ReflectionService | None = None
    scheduler: SchedulerService | None = None
    mcp_manager: MCPManager | None = None
    connector_manager: ConnectorManager | None = None
    trace_sink: TraceSink | None = None
    internal_send: callable | None = None
    internal_send_file: callable | None = None
    internal_progress_send: callable | None = None
    internal_worker_event_send: callable | None = None
    internal_typing_control: callable | None = None
    _cleanup_task: asyncio.Task | None = None
    _metrics_task: asyncio.Task | None = None
    _scheduler_task: asyncio.Task | None = None
    _self_control_task: asyncio.Task | None = None
    _scheduler_metric_counters: dict[str, int] | None = None
    _scheduler_interval_seconds: int | None = None
    _scheduler_max_tasks: int | None = None
    _scheduled_octo_control_backoff_by_task: dict[str, tuple[float, str]] | None = None
    _recent_tasks: dict[tuple[int, str, str], float] = (
        None  # Track in-flight worker launches per chat/correlation scope
    )
    _approval_requesters: dict[int, Callable[[Any], Awaitable[bool]]] | None = None
    _thinking_count: int = 0
    _ws_active: bool = False
    _ws_owner: str | None = None
    _tg_send: callable | None = None
    _tg_send_file: callable | None = None
    _tg_progress: callable | None = None
    _tg_worker_event: callable | None = None
    _tg_typing: callable | None = None
    _scheduled_delivery_chat_ids: list[int] | None = None
    _spawn_limits: dict[str, int] | None = None
    _worker_children: dict[str, set[str]] | None = None
    _worker_lineage: dict[str, str] | None = None
    _worker_depth: dict[str, int] | None = None
    _lineage_children_total: dict[str, int] | None = None
    _lineage_children_active: dict[str, set[str]] | None = None
    _worker_correlation_by_run_id: dict[str, str] | None = None
    _worker_chat_by_run_id: dict[str, int] | None = None
    _scheduled_notify_user_by_run_id: dict[str, str] | None = None
    _active_workers_by_correlation: dict[str, set[str]] | None = None
    _pending_internal_results_by_correlation: dict[str, int] | None = None
    _housekeeping_cfg: dict[str, int] | None = None
    _pending_wakeup_by_chat: dict[int, str] | None = None
    _context_health_by_chat: dict[int, dict[str, Any]] | None = None
    _last_reply_norm_by_chat: dict[int, str] | None = None
    _last_user_visible_delivery_at_by_chat: dict[int, Any] | None = None
    _pending_conversational_closure_by_correlation: dict[str, Any] | None = None
    _structured_followup_required_by_correlation: dict[str, Any] | None = None
    _suppressed_followups_by_correlation: dict[str, Any] | None = None
    _no_progress_turns_by_chat: dict[int, int] | None = None
    _progress_revision_by_chat: dict[int, int] | None = None
    _reset_streak_without_progress_by_chat: dict[int, int] | None = None
    _last_reset_progress_revision_by_chat: dict[int, int] | None = None
    _watch_escalation_streak_by_chat: dict[int, int] | None = None
    _self_queue_by_chat: dict[int, list[dict[str, Any]]] | None = None
    _last_opportunities_by_chat: dict[int, list[dict[str, Any]]] | None = None
    _active_user_turns_by_correlation: dict[str, Any] | None = None

    def __post_init__(self):
        if self.trace_sink is None:
            self.trace_sink = NoopTraceSink()
        if self._scheduled_octo_control_backoff_by_task is None:
            self._scheduled_octo_control_backoff_by_task = {}
        if self._recent_tasks is None:
            self._recent_tasks = {}
        if self._approval_requesters is None:
            self._approval_requesters = {}
        if self._scheduled_delivery_chat_ids is None:
            self._scheduled_delivery_chat_ids = []
        if self._worker_children is None:
            self._worker_children = {}
        if self._worker_lineage is None:
            self._worker_lineage = {}
        if self._worker_depth is None:
            self._worker_depth = {}
        if self._lineage_children_total is None:
            self._lineage_children_total = {}
        if self._lineage_children_active is None:
            self._lineage_children_active = {}
        if self._worker_correlation_by_run_id is None:
            self._worker_correlation_by_run_id = {}
        if self._worker_chat_by_run_id is None:
            self._worker_chat_by_run_id = {}
        if self._scheduled_notify_user_by_run_id is None:
            self._scheduled_notify_user_by_run_id = {}
        if self._active_workers_by_correlation is None:
            self._active_workers_by_correlation = {}
        if self._pending_internal_results_by_correlation is None:
            self._pending_internal_results_by_correlation = {}
        if self._pending_wakeup_by_chat is None:
            self._pending_wakeup_by_chat = {}
        if self._context_health_by_chat is None:
            self._context_health_by_chat = {}
        if self._last_reply_norm_by_chat is None:
            self._last_reply_norm_by_chat = {}
        if self._last_user_visible_delivery_at_by_chat is None:
            self._last_user_visible_delivery_at_by_chat = {}
        if self._pending_conversational_closure_by_correlation is None:
            self._pending_conversational_closure_by_correlation = {}
        if self._structured_followup_required_by_correlation is None:
            self._structured_followup_required_by_correlation = {}
        if self._suppressed_followups_by_correlation is None:
            self._suppressed_followups_by_correlation = {}
        if self._active_user_turns_by_correlation is None:
            self._active_user_turns_by_correlation = {}
        if self._no_progress_turns_by_chat is None:
            self._no_progress_turns_by_chat = {}
        if self._progress_revision_by_chat is None:
            self._progress_revision_by_chat = {}
        if self._reset_streak_without_progress_by_chat is None:
            self._reset_streak_without_progress_by_chat = {}
        if self._last_reset_progress_revision_by_chat is None:
            self._last_reset_progress_revision_by_chat = {}
        if self._watch_escalation_streak_by_chat is None:
            self._watch_escalation_streak_by_chat = {}
        if self._self_queue_by_chat is None:
            self._self_queue_by_chat = {}
        if self._last_opportunities_by_chat is None:
            self._last_opportunities_by_chat = {}
        if self._spawn_limits is None:
            max_depth = _env_int("OCTOPAL_WORKER_MAX_SPAWN_DEPTH", 2, minimum=0)
            max_total = _env_int("OCTOPAL_WORKER_MAX_CHILDREN_TOTAL", 20, minimum=1)
            max_concurrent = _env_int("OCTOPAL_WORKER_MAX_CHILDREN_CONCURRENT", 10, minimum=1)
            self._spawn_limits = {
                "max_depth": max_depth,
                "max_children_total": max_total,
                "max_children_concurrent": max_concurrent,
            }
        if self._housekeeping_cfg is None:
            self._housekeeping_cfg = {
                "tmp_retention_hours": _env_int(
                    "OCTOPAL_WORKSPACE_TMP_RETENTION_HOURS", 48, minimum=1
                ),
                "worker_dir_retention_minutes": _env_int(
                    "OCTOPAL_WORKER_DIR_RETENTION_MINUTES", 15, minimum=1
                ),
                "canon_events_max_bytes": _env_int(
                    "OCTOPAL_CANON_EVENTS_MAX_BYTES", 2_000_000, minimum=1024
                ),
                "canon_events_keep_archives": _env_int(
                    "OCTOPAL_CANON_EVENTS_KEEP_ARCHIVES", 7, minimum=1
                ),
            }
        if self._scheduler_metric_counters is None:
            self._scheduler_metric_counters = _empty_scheduler_metric_counters()
        if self._scheduler_interval_seconds is None:
            self._scheduler_interval_seconds = None
        if self._scheduler_max_tasks is None:
            self._scheduler_max_tasks = None
        self._restore_worker_registry_state()
        self._thinking_count = 0
        self._tg_send = self.internal_send
        self._tg_send_file = self.internal_send_file
        self._tg_progress = self.internal_progress_send
        self._tg_worker_event = self.internal_worker_event_send
        self._tg_typing = self.internal_typing_control

    def _reserve_recent_task(
        self,
        *,
        chat_id: int,
        correlation_id: str | None,
        task_signature: str,
    ) -> bool:
        self._prune_recent_tasks()
        scope_id = str(correlation_id or f"chat:{chat_id}")
        key = (chat_id, scope_id, task_signature)
        if key in self._recent_tasks:
            return False
        self._recent_tasks[key] = time.monotonic()
        return True

    def _release_recent_task(
        self,
        *,
        chat_id: int,
        correlation_id: str | None,
        task_signature: str,
    ) -> None:
        scope_id = str(correlation_id or f"chat:{chat_id}")
        self._recent_tasks.pop((chat_id, scope_id, task_signature), None)

    def _prune_recent_tasks(self) -> None:
        now = time.monotonic()
        cutoff = now - _RECENT_WORKER_TASK_TTL_SECONDS
        stale_keys = [key for key, seen_at in self._recent_tasks.items() if seen_at < cutoff]
        for key in stale_keys:
            self._recent_tasks.pop(key, None)

    @property
    def is_ws_active(self) -> bool:
        return self._ws_active

    def set_output_channel(
        self,
        is_ws: bool,
        send: callable | None = None,
        send_file: callable | None = None,
        progress: callable | None = None,
        worker_event: callable | None = None,
        typing: callable | None = None,
        owner_id: str | None = None,
        force: bool = False,
    ) -> bool:
        """Switch between Telegram and WebSocket output channels."""
        if is_ws:
            if self._ws_active and self._ws_owner and owner_id and self._ws_owner != owner_id:
                if force:
                    logger.warning(
                        "Forcing WebSocket channel takeover",
                        current_owner=self._ws_owner,
                        new_owner=owner_id,
                    )
                else:
                    logger.warning(
                        "Rejected WebSocket channel switch due to existing owner",
                        current_owner=self._ws_owner,
                        attempted_owner=owner_id,
                    )
                    return False
        else:
            if self._ws_owner and owner_id and self._ws_owner != owner_id:
                if force:
                    logger.warning(
                        "Forcing output channel reset from non-owner",
                        current_owner=self._ws_owner,
                        attempted_owner=owner_id,
                    )
                else:
                    logger.warning(
                        "Rejected output channel reset from non-owner",
                        current_owner=self._ws_owner,
                        attempted_owner=owner_id,
                    )
                    return False

        self._ws_active = is_ws
        if is_ws:
            self.internal_send = send
            self.internal_send_file = send_file
            self.internal_progress_send = progress
            self.internal_worker_event_send = worker_event
            self.internal_typing_control = typing
            self._ws_owner = owner_id or "ws-default"
            logger.info("Octo switched to WebSocket output channel")
        else:
            self.internal_send = self._tg_send
            self.internal_send_file = self._tg_send_file
            self.internal_progress_send = self._tg_progress
            self.internal_worker_event_send = self._tg_worker_event
            self.internal_typing_control = self._tg_typing
            self._ws_owner = None
            logger.info("Octo switched to Telegram output channel")

        # Update system status file if possible
        try:
            import json

            from octopal.infrastructure.config.settings import load_settings
            from octopal.runtime.state import _status_path, read_status

            settings = load_settings()
            status_data = read_status(settings) or {}
            status_data["active_channel"] = "WebSocket" if is_ws else "Telegram"
            _status_path(settings).write_text(json.dumps(status_data, indent=2), encoding="utf-8")
        except Exception:
            logger.debug("Failed to update status file with active channel", exc_info=True)
        return True

    async def set_thinking(self, active: bool) -> None:
        """Toggle global thinking indicator."""
        if active:
            self._thinking_count += 1
        else:
            self._thinking_count = max(0, self._thinking_count - 1)
        _publish_runtime_metrics(self._thinking_count)

    async def set_typing(self, chat_id: int, active: bool):
        """Toggle typing indicator for a specific chat."""
        if self.internal_typing_control:
            try:
                await self.internal_typing_control(chat_id, active)
            except Exception:
                logger.debug(
                    "Failed to set typing status", chat_id=chat_id, active=active, exc_info=True
                )

    async def _periodic_cleanup(self, interval_seconds: int):
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                deleted = await asyncio.to_thread(self.store.cleanup_old_workers)
                if deleted > 0:
                    logger.info("Periodic cleanup complete", deleted_workers=deleted)

                cfg = self._housekeeping_cfg or {}
                worker_result = await asyncio.to_thread(
                    cleanup_ephemeral_worker_dirs,
                    self.canon.workspace_dir,
                    retention_minutes=int(cfg.get("worker_dir_retention_minutes", 15)),
                    docker_cleanup_image=getattr(self.runtime.launcher, "image", None),
                )
                if worker_result.deleted_dirs or worker_result.errors:
                    logger.info(
                        "Ephemeral worker dir cleanup complete",
                        deleted_dirs=worker_result.deleted_dirs,
                        errors=worker_result.errors,
                    )

                tmp_result = await asyncio.to_thread(
                    cleanup_workspace_tmp,
                    self.canon.workspace_dir,
                    retention_hours=int(cfg.get("tmp_retention_hours", 48)),
                )
                if tmp_result.deleted_files or tmp_result.deleted_dirs or tmp_result.errors:
                    logger.info(
                        "Workspace tmp cleanup complete",
                        deleted_files=tmp_result.deleted_files,
                        deleted_dirs=tmp_result.deleted_dirs,
                        errors=tmp_result.errors,
                    )

                rotate_result = await asyncio.to_thread(
                    rotate_canon_events,
                    self.canon.workspace_dir,
                    max_bytes=int(cfg.get("canon_events_max_bytes", 2_000_000)),
                    keep_archives=int(cfg.get("canon_events_keep_archives", 7)),
                )
                if rotate_result.rotated or rotate_result.deleted_archives:
                    logger.info(
                        "Canon events rotation complete",
                        rotated=rotate_result.rotated,
                        archived_file=rotate_result.archived_file,
                        deleted_archives=rotate_result.deleted_archives,
                        bootstrap_entries=rotate_result.bootstrap_entries,
                    )
            except Exception:
                logger.exception("Periodic worker cleanup failed")

    async def _periodic_metrics_publish(self, interval_seconds: int):
        from octopal.runtime.metrics import update_component_gauges

        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await asyncio.to_thread(self._reconcile_stale_worker_records)
                mcp_status = {}
                if self.mcp_manager:
                    mcp_status = self.mcp_manager.get_server_statuses()

                update_component_gauges("connectivity", {"mcp_servers": mcp_status})
            except Exception:
                logger.debug("Failed to publish periodic metrics", exc_info=True)

    def _publish_scheduler_metrics(
        self,
        *,
        running: bool,
        interval_seconds: int | None = None,
        max_tasks: int | None = None,
        last_tick_status: str | None = None,
        due_count: int | None = None,
        result_preview: str | None = None,
        dispatch_summary: dict[str, int] | None = None,
    ) -> None:
        counters = self._scheduler_metric_counters or _empty_scheduler_metric_counters()
        payload: dict[str, Any] = {
            "running": bool(running),
            "configured": self.scheduler is not None,
            **counters,
        }
        resolved_interval = interval_seconds
        if resolved_interval is None:
            resolved_interval = self._scheduler_interval_seconds
        resolved_max_tasks = max_tasks
        if resolved_max_tasks is None:
            resolved_max_tasks = self._scheduler_max_tasks
        if resolved_interval is not None:
            payload["interval_seconds"] = int(resolved_interval)
        if resolved_max_tasks is not None:
            payload["max_tasks"] = int(resolved_max_tasks)
        if last_tick_status is not None:
            payload["last_tick_status"] = str(last_tick_status)
        if due_count is not None:
            payload["last_due_count"] = int(due_count)
        if result_preview is not None:
            payload["last_result_preview"] = str(result_preview)
        if dispatch_summary is not None:
            payload["last_dispatch_attempted"] = int(dispatch_summary.get("attempted") or 0)
            payload["last_dispatch_started"] = int(dispatch_summary.get("started") or 0)
            payload["last_dispatch_completed"] = int(dispatch_summary.get("completed") or 0)
            payload["last_dispatch_duplicates"] = int(dispatch_summary.get("duplicates") or 0)
            payload["last_dispatch_rejected_by_policy"] = int(
                dispatch_summary.get("rejected_by_policy") or 0
            )
            payload["last_dispatch_errors"] = int(dispatch_summary.get("errors") or 0)
            payload["last_policy_reasons"] = dict(dispatch_summary.get("policy_reasons") or {})
        update_component_gauges("scheduler", payload)

    async def _run_scheduler_tick_once(self, *, chat_id: int = 0, max_tasks: int = 10) -> None:
        if self.scheduler is None:
            return
        trace_started_at_ms = now_ms()
        trace_metadata: dict[str, Any] = {
            "route_mode": RouteMode.SCHEDULER.value,
            "chat_id": chat_id,
            "dry_run": False,
            "max_tasks": max_tasks,
        }
        trace_ctx, trace_token, is_root_trace = await _start_background_trace_context(
            self.trace_sink,
            name="octo.scheduler_tick",
            chat_id=chat_id,
            correlation_id=None,
            metadata=trace_metadata,
        )
        trace_status = "ok"
        trace_output: dict[str, Any] | None = None
        try:
            result = await route_scheduler_tick(self, chat_id=chat_id, max_tasks=max_tasks)
            normalized = normalize_plain_text(result)
            normalized_upper = normalized.strip().upper()
            dispatch_summary = await self._dispatch_due_scheduled_tasks_once(
                chat_id=chat_id,
                max_tasks=max_tasks,
            )
            due_count = int(dispatch_summary.get("due_count") or 0)
            trace_metadata.update(
                {
                    "due_count": due_count,
                    "result_preview": safe_preview(normalized, limit=240),
                    "result_len": len(normalized or ""),
                    "dispatch_started": int(dispatch_summary.get("started") or 0),
                    "dispatch_completed": int(dispatch_summary.get("completed") or 0),
                    "dispatch_duplicates": int(dispatch_summary.get("duplicates") or 0),
                    "dispatch_rejected_by_policy": int(
                        dispatch_summary.get("rejected_by_policy") or 0
                    ),
                    "dispatch_policy_reasons": dict(dispatch_summary.get("policy_reasons") or {}),
                    "dispatch_errors": int(dispatch_summary.get("errors") or 0),
                }
            )
            counters = self._scheduler_metric_counters or _empty_scheduler_metric_counters()
            counters["ticks_total"] = int(counters.get("ticks_total", 0) or 0) + 1
            counters["started_total"] = int(counters.get("started_total", 0) or 0) + int(
                dispatch_summary.get("started") or 0
            )
            counters["completed_total"] = int(counters.get("completed_total", 0) or 0) + int(
                dispatch_summary.get("completed") or 0
            )
            counters["duplicates_total"] = int(counters.get("duplicates_total", 0) or 0) + int(
                dispatch_summary.get("duplicates") or 0
            )
            counters["rejected_by_policy_total"] = int(
                counters.get("rejected_by_policy_total", 0) or 0
            ) + int(dispatch_summary.get("rejected_by_policy") or 0)
            counters["errors_total"] = int(counters.get("errors_total", 0) or 0) + int(
                dispatch_summary.get("errors") or 0
            )
            self._scheduler_metric_counters = counters
            if normalized_upper in {"", "SCHEDULER_IDLE", "NO_USER_RESPONSE"}:
                self._publish_scheduler_metrics(
                    running=True,
                    last_tick_status="idle",
                    due_count=due_count,
                    result_preview=safe_preview(normalized, limit=160),
                    dispatch_summary=dispatch_summary,
                )
                trace_output = {
                    "status": "idle",
                    "due_count": due_count,
                    "dispatch": dispatch_summary,
                }
                logger.debug(
                    "Scheduler tick complete",
                    due_count=due_count,
                    dispatch=dispatch_summary,
                    result=normalized_upper or "EMPTY",
                )
                return

            delivery = resolve_user_delivery(normalized)
            trace_metadata.update(
                {
                    "delivery_mode": delivery.mode,
                    "user_visible": delivery.user_visible,
                    "suppressed_reason": delivery.reason,
                }
            )
            user_visible_sent = False
            if delivery.user_visible:
                delivery_chat_id, delivery_target_source = self._resolve_scheduler_delivery_chat_id(
                    requested_chat_id=chat_id,
                )
                trace_metadata["delivery_target_source"] = delivery_target_source
                if delivery_chat_id is not None:
                    await _send_scheduler_control_update(
                        self,
                        delivery_chat_id,
                        None,
                        delivery.text,
                    )
                    user_visible_sent = True
                    trace_metadata["delivery_chat_id"] = delivery_chat_id
                else:
                    logger.warning(
                        "Scheduler tick produced user-visible text without delivery target",
                        result_preview=safe_preview(delivery.text, limit=160),
                    )

            self._publish_scheduler_metrics(
                running=True,
                last_tick_status="decision_ready",
                due_count=due_count,
                result_preview=safe_preview(normalized, limit=160),
                dispatch_summary=dispatch_summary,
            )
            trace_output = {
                "status": "decision_ready",
                "due_count": due_count,
                "result_preview": safe_preview(normalized, limit=160),
                "user_visible_sent": user_visible_sent,
                "dispatch": dispatch_summary,
            }
            logger.info(
                "Scheduler tick produced decision",
                due_count=due_count,
                dispatch=dispatch_summary,
                user_visible_sent=user_visible_sent,
                result_preview=safe_preview(normalized, limit=160),
            )
        except Exception as exc:
            counters = self._scheduler_metric_counters or _empty_scheduler_metric_counters()
            counters["ticks_total"] = int(counters.get("ticks_total", 0) or 0) + 1
            counters["failures_total"] = int(counters.get("failures_total", 0) or 0) + 1
            self._scheduler_metric_counters = counters
            self._publish_scheduler_metrics(
                running=True,
                last_tick_status="failed",
                result_preview=safe_preview(str(exc), limit=160),
            )
            trace_status = "error"
            trace_metadata.update(summarize_exception(exc))
            trace_output = {"status": "failed"}
            logger.exception("Scheduler tick failed")
        finally:
            runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
            if runtime_settings is not None:
                await asyncio.to_thread(
                    update_last_scheduler_tick,
                    runtime_settings,
                    status=trace_status,
                )
            trace_metadata["duration_ms"] = round(now_ms() - trace_started_at_ms, 2)
            await _finish_background_trace_context(
                self.trace_sink,
                trace_ctx,
                trace_token,
                is_root_trace=is_root_trace,
                status=trace_status,
                output=trace_output,
                metadata=trace_metadata,
            )

    async def _periodic_scheduler_tick(self, interval_seconds: int, *, max_tasks: int = 10) -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            await self._run_scheduler_tick_once(chat_id=0, max_tasks=max_tasks)

    async def _periodic_self_control_requests(self, interval_seconds: int = 1) -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await self._run_self_control_requests_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Self-control request executor failed")

    async def _run_self_control_requests_once(self) -> None:
        runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
        if runtime_settings is None:
            return
        state_dir = Path(runtime_settings.state_dir)
        for request in await asyncio.to_thread(due_self_restart_requests, state_dir):
            request_id = str(request.get("request_id", "") or "").strip()
            if not request_id:
                continue
            append_control_ack(
                state_dir,
                request_id,
                status="accepted",
                source="octo_self_control",
                message="Self-restart request accepted; launching restart helper.",
            )
            try:
                launch_restart_helper(
                    state_dir,
                    request_id=request_id,
                    project_root=Path(__file__).resolve().parents[4],
                    delay_seconds=1,
                )
            except Exception as exc:
                append_control_ack(
                    state_dir,
                    request_id,
                    status="error",
                    source="octo_self_control",
                    message=f"Failed to launch restart helper: {exc}",
                )
                logger.exception(
                    "Failed to launch self-restart helper",
                    request_id=request_id,
                )
        for request in await asyncio.to_thread(due_self_update_requests, state_dir):
            request_id = str(request.get("request_id", "") or "").strip()
            if not request_id:
                continue
            append_control_ack(
                state_dir,
                request_id,
                status="accepted",
                source="octo_self_control",
                message="Self-update request accepted; launching update helper.",
            )
            try:
                launch_update_helper(
                    state_dir,
                    request_id=request_id,
                    project_root=Path(__file__).resolve().parents[4],
                    delay_seconds=1,
                )
            except Exception as exc:
                append_control_ack(
                    state_dir,
                    request_id,
                    status="error",
                    source="octo_self_control",
                    message=f"Failed to launch update helper: {exc}",
                )
                logger.exception(
                    "Failed to launch self-update helper",
                    request_id=request_id,
                )

    def _get_scheduled_octo_control_backoff(self, task_id: str) -> tuple[float, str] | None:
        task_id_value = str(task_id or "").strip()
        if not task_id_value:
            return None
        backoff_map = self._scheduled_octo_control_backoff_by_task
        if not isinstance(backoff_map, dict):
            return None
        entry = backoff_map.get(task_id_value)
        if not entry:
            return None
        deadline, reason = entry
        remaining = float(deadline) - time.monotonic()
        if remaining <= 0:
            backoff_map.pop(task_id_value, None)
            return None
        return remaining, str(reason or "").strip() or "runtime_backoff"

    def _set_scheduled_octo_control_backoff(self, task_id: str, *, reason: str) -> None:
        task_id_value = str(task_id or "").strip()
        if not task_id_value or _SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS <= 0:
            return
        backoff_map = self._scheduled_octo_control_backoff_by_task
        if backoff_map is None:
            backoff_map = {}
            self._scheduled_octo_control_backoff_by_task = backoff_map
        backoff_map[task_id_value] = (
            time.monotonic() + _SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS,
            str(reason or "").strip() or "runtime_backoff",
        )

    def _get_persisted_scheduled_octo_control_backoff(
        self,
        task: dict[str, Any],
    ) -> tuple[float, str] | None:
        metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
        if not isinstance(metadata, dict):
            return None
        blocked_until = parse_scheduled_task_blocked_until(metadata)
        if blocked_until is None:
            return None
        remaining = (blocked_until - utc_now()).total_seconds()
        if remaining <= 0:
            self._update_scheduled_octo_control_backoff_metadata(
                task,
                blocked_until=None,
                reason=None,
            )
            return None
        reason = (
            str(metadata.get(SCHEDULED_TASK_BLOCKED_REASON_KEY) or "").strip() or "blocked_by_route"
        )
        return remaining, reason

    def _update_scheduled_octo_control_backoff_metadata(
        self,
        task: dict[str, Any],
        *,
        blocked_until: datetime | None,
        reason: str | None,
    ) -> None:
        scheduler = self.scheduler
        store = getattr(scheduler, "store", None)
        update_metadata = getattr(store, "update_scheduled_task_metadata", None)
        if scheduler is None or not callable(update_metadata):
            return
        task_id = str(task.get("id") or "").strip()
        if not task_id:
            return
        metadata = (
            dict(task.get("metadata") or {}) if isinstance(task.get("metadata"), dict) else {}
        )
        if blocked_until is None:
            metadata.pop(SCHEDULED_TASK_BLOCKED_UNTIL_KEY, None)
        else:
            metadata[SCHEDULED_TASK_BLOCKED_UNTIL_KEY] = blocked_until.isoformat()
        reason_value = str(reason or "").strip()
        if reason_value:
            metadata[SCHEDULED_TASK_BLOCKED_REASON_KEY] = reason_value
        else:
            metadata.pop(SCHEDULED_TASK_BLOCKED_REASON_KEY, None)
        if reason_value == "blocked_by_route":
            metadata[SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY] = "worker"
        elif blocked_until is None:
            metadata.pop(SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY, None)
        try:
            update_metadata(task_id, metadata or None)
        except Exception:
            logger.exception(
                "Failed to persist scheduled Octo control backoff metadata",
                task_id=task_id or None,
            )
            return
        task["metadata"] = metadata
        task["blocked_until"] = metadata.get(SCHEDULED_TASK_BLOCKED_UNTIL_KEY)
        task["blocked_reason"] = metadata.get(SCHEDULED_TASK_BLOCKED_REASON_KEY)
        task["suggested_execution_mode"] = metadata.get(SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY)

    def _resolve_scheduled_task_delivery_chat_id(
        self,
        task: dict[str, Any],
        *,
        requested_chat_id: int = 0,
    ) -> tuple[int | None, str]:
        notify_user = normalize_notify_user_policy(task.get("notify_user"))
        if notify_user == "never":
            return 0, "notify_never"

        metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
        explicit = _coerce_positive_chat_id(
            task.get("delivery_chat_id")
            or metadata.get(SCHEDULED_TASK_DELIVERY_CHAT_ID_KEY)
            or metadata.get(SCHEDULED_TASK_TARGET_CHAT_ID_KEY)
        )
        if explicit is not None:
            return explicit, "task_metadata"

        requested = _coerce_positive_chat_id(requested_chat_id)
        if requested is not None:
            return requested, "request_context"

        configured = [
            chat_id
            for item in (self._scheduled_delivery_chat_ids or [])
            if (chat_id := _coerce_positive_chat_id(item)) is not None
        ]
        unique_configured = list(dict.fromkeys(configured))
        if len(unique_configured) == 1:
            return unique_configured[0], "single_configured_recipient"

        return None, "missing_delivery_target"

    def _resolve_scheduler_delivery_chat_id(
        self,
        *,
        requested_chat_id: int = 0,
    ) -> tuple[int | None, str]:
        requested = _coerce_positive_chat_id(requested_chat_id)
        if requested is not None:
            return requested, "request_context"

        configured = [
            chat_id
            for item in (self._scheduled_delivery_chat_ids or [])
            if (chat_id := _coerce_positive_chat_id(item)) is not None
        ]
        unique_configured = list(dict.fromkeys(configured))
        if len(unique_configured) == 1:
            return unique_configured[0], "single_configured_recipient"

        return None, "missing_delivery_target"

    async def _dispatch_due_scheduled_tasks_once(
        self,
        *,
        chat_id: int = 0,
        max_tasks: int = 10,
    ) -> dict[str, Any]:
        scheduler = self.scheduler
        summary: dict[str, Any] = {
            "due_count": 0,
            "attempted": 0,
            "started": 0,
            "completed": 0,
            "duplicates": 0,
            "rejected_by_policy": 0,
            "policy_reasons": {},
            "errors": 0,
        }
        if scheduler is None:
            return summary

        due_tasks = list(scheduler.get_actionable_tasks() or [])
        summary["due_count"] = len(due_tasks)
        for task in due_tasks[: max(1, int(max_tasks))]:
            task_id = str(task.get("id") or "").strip()
            execution_mode = str(task.get("execution_mode") or "").strip().lower()
            worker_id = str(task.get("worker_id") or "").strip()
            task_text = str(task.get("task_text") or "").strip()
            inputs = task.get("inputs") if isinstance(task.get("inputs"), dict) else {}
            dispatch_chat_id, delivery_target_source = self._resolve_scheduled_task_delivery_chat_id(
                task,
                requested_chat_id=chat_id,
            )
            if dispatch_chat_id is None:
                summary["rejected_by_policy"] += 1
                reason = "missing_delivery_target"
                policy_reasons = summary.setdefault("policy_reasons", {})
                if isinstance(policy_reasons, dict):
                    policy_reasons[reason] = int(policy_reasons.get(reason, 0) or 0) + 1
                logger.warning(
                    "Rejected scheduled task by delivery policy",
                    task_id=task_id or None,
                    notify_user=task.get("notify_user"),
                    delivery_target_source=delivery_target_source,
                )
                continue
            if execution_mode == "octo_control":
                persisted_backoff = self._get_persisted_scheduled_octo_control_backoff(task)
                if persisted_backoff is not None:
                    remaining_seconds, backoff_reason = persisted_backoff
                    logger.info(
                        "Skipping scheduled Octo control task during persisted backoff",
                        task_id=task_id or None,
                        chat_id=dispatch_chat_id,
                        backoff_reason=backoff_reason,
                        cooldown_seconds=round(remaining_seconds, 1),
                    )
                    continue
                backoff = self._get_scheduled_octo_control_backoff(task_id)
                if backoff is not None:
                    remaining_seconds, backoff_reason = backoff
                    logger.info(
                        "Skipping scheduled Octo control task during runtime backoff",
                        task_id=task_id or None,
                        chat_id=dispatch_chat_id,
                        backoff_reason=backoff_reason,
                        cooldown_seconds=round(remaining_seconds, 1),
                    )
                    continue
                summary["attempted"] += 1
                try:
                    result = await self._run_scheduled_octo_control_task_once(
                        task=task,
                        chat_id=dispatch_chat_id,
                    )
                except Exception:
                    summary["errors"] += 1
                    logger.exception(
                        "Scheduled Octo control task failed",
                        task_id=task_id or None,
                    )
                    continue
                if bool(result.get("completed")):
                    summary["completed"] += 1
                elif str(result.get("status") or "").strip().lower() == "failed":
                    summary["errors"] += 1
                continue
            if not worker_id or not task_text:
                summary["rejected_by_policy"] += 1
                reason = "missing_worker_id" if not worker_id else "missing_task_text"
                policy_reasons = summary.setdefault("policy_reasons", {})
                if isinstance(policy_reasons, dict):
                    policy_reasons[reason] = int(policy_reasons.get(reason, 0) or 0) + 1
                logger.warning(
                    "Rejected scheduled task by dispatch policy",
                    task_id=task_id or None,
                    policy_reason=reason,
                    worker_id=worker_id or None,
                    has_task_text=bool(task_text),
                )
                continue

            summary["attempted"] += 1
            try:
                result = await self._start_worker_async(
                    worker_id=worker_id,
                    task=task_text,
                    chat_id=dispatch_chat_id,
                    inputs=inputs,
                    tools=None,
                    model=None,
                    timeout_seconds=None,
                    scheduled_task_id=task_id or None,
                )
            except Exception:
                summary["errors"] += 1
                logger.exception(
                    "Scheduled task dispatch failed",
                    task_id=task_id or None,
                    worker_id=worker_id,
                )
                continue

            status = str(result.get("status") or "").strip().lower()
            if status == "started":
                summary["started"] += 1
            elif status == "skipped_duplicate":
                summary["duplicates"] += 1
            elif status in {"rejected", "failed"}:
                summary["errors"] += 1

        return summary

    async def _run_scheduled_octo_control_task_once(
        self,
        *,
        task: dict[str, Any],
        chat_id: int = 0,
    ) -> dict[str, Any]:
        scheduler = self.scheduler
        task_id = str(task.get("id") or "").strip()
        notify_user = _normalize_scheduled_octo_control_notify_policy(task.get("notify_user"))
        reply_text = await route_scheduled_octo_control(
            self,
            task,
            chat_id=chat_id,
        )
        normalized_reply = await _normalize_scheduled_octo_control_reply(self.provider, reply_text)
        route_blocked = normalized_reply == _SCHEDULED_OCTO_CONTROL_BLOCKED or (
            normalized_reply == "NO_USER_RESPONSE"
            and _looks_like_scheduled_octo_control_route_block(reply_text)
        )
        if route_blocked:
            self._set_scheduled_octo_control_backoff(task_id, reason="blocked_by_route")
            self._update_scheduled_octo_control_backoff_metadata(
                task,
                blocked_until=utc_now()
                + timedelta(seconds=_SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS),
                reason="blocked_by_route",
            )
            logger.warning(
                "Scheduled Octo control task blocked by bounded route",
                task_id=task_id or None,
                chat_id=chat_id,
                raw_reply_preview=safe_preview(reply_text, limit=200),
                cooldown_seconds=_SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS,
            )
            return {
                "status": "failed",
                "completed": False,
                "reason": "blocked_by_route",
                "cooldown_seconds": _SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS,
            }
        if normalized_reply == "NO_USER_RESPONSE":
            logger.warning(
                "Scheduled Octo control task missing explicit completion signal",
                task_id=task_id or None,
                chat_id=chat_id,
                raw_reply_preview=safe_preview(reply_text, limit=200),
            )
            return {
                "status": "failed",
                "completed": False,
                "reason": "missing_completion_signal",
            }
        if normalized_reply == _SCHEDULED_OCTO_CONTROL_DONE:
            self._update_scheduled_octo_control_backoff_metadata(
                task,
                blocked_until=None,
                reason=None,
            )
            if scheduler is not None and task_id:
                scheduler.mark_executed(task_id)
            logger.info(
                "Scheduled Octo control task completed silently",
                task_id=task_id or None,
                notify_user=notify_user,
                chat_id=chat_id,
            )
            return {
                "status": "completed",
                "completed": True,
                "user_visible_sent": False,
                "delivery_mode": DeliveryMode.SILENT,
            }
        delivery = resolve_user_delivery(normalized_reply)
        user_visible_sent = False
        if delivery.user_visible and notify_user != "never":
            await _send_scheduler_control_update(
                self,
                chat_id,
                task_id or None,
                delivery.text,
            )
            user_visible_sent = True
        elif delivery.user_visible:
            logger.info(
                "Scheduled Octo control update suppressed by notify policy",
                task_id=task_id or None,
                notify_user=notify_user,
                chat_id=chat_id,
            )
        if scheduler is not None and task_id:
            scheduler.mark_executed(task_id)
        logger.info(
            "Scheduled Octo control task completed",
            task_id=task_id or None,
            notify_user=notify_user,
            chat_id=chat_id,
            user_visible_sent=user_visible_sent,
            delivery_mode=delivery.mode,
        )
        return {
            "status": "completed",
            "completed": True,
            "user_visible_sent": user_visible_sent,
            "delivery_mode": delivery.mode,
        }

    def _reconcile_stale_worker_records(self) -> None:
        """Normalize stale DB worker states that no longer exist in runtime."""
        runtime = self.runtime
        if not runtime or not hasattr(runtime, "is_worker_running"):
            return
        workers = self.store.get_active_workers(older_than_minutes=120)
        if not workers:
            return
        grace_cutoff = utc_now() - timedelta(minutes=2)
        reconciled = 0
        for worker in workers:
            if worker.status not in {"started", "running"}:
                continue
            if worker.updated_at >= grace_cutoff:
                continue
            if runtime.is_worker_running(worker.id):
                continue
            self.store.update_worker_status(worker.id, "stopped")
            self.store.update_worker_result(
                worker.id,
                error="Worker process not found in runtime; stale running state reconciled.",
            )
            self._mark_worker_inactive(worker.id)
            reconciled += 1
        if reconciled > 0:
            logger.info("Reconciled stale worker records", reconciled_workers=reconciled)

    def _restore_worker_registry_state(self) -> None:
        """Restore lineage/child bookkeeping from persisted workers."""
        if not hasattr(self.store, "list_workers"):
            return
        try:
            workers = list(self.store.list_workers() or [])
        except Exception:
            logger.debug("Skipping worker registry restore: list_workers failed", exc_info=True)
            return
        if not workers:
            return

        self._worker_children.clear()
        self._worker_lineage.clear()
        self._worker_depth.clear()
        self._lineage_children_total.clear()
        self._lineage_children_active.clear()
        self._worker_correlation_by_run_id.clear()
        self._worker_chat_by_run_id.clear()
        self._scheduled_notify_user_by_run_id.clear()
        self._active_workers_by_correlation.clear()
        self._pending_internal_results_by_correlation.clear()

        worker_by_id: dict[str, Any] = {}
        for worker in workers:
            run_id = str(getattr(worker, "id", "") or "").strip()
            if not run_id:
                continue
            worker_by_id[run_id] = worker
            lineage_id = str(getattr(worker, "lineage_id", "") or run_id).strip() or run_id
            depth = max(0, int(getattr(worker, "spawn_depth", 0) or 0))
            correlation_id = str(getattr(worker, "correlation_id", "") or "").strip() or None
            self._worker_lineage[run_id] = lineage_id
            self._worker_depth[run_id] = depth
            self._worker_chat_by_run_id[run_id] = int(getattr(worker, "chat_id", 0) or 0)
            if correlation_id:
                self._worker_correlation_by_run_id[run_id] = correlation_id

        orphan_reconciled = 0
        for run_id, worker in worker_by_id.items():
            parent_worker_id = str(getattr(worker, "parent_worker_id", "") or "").strip()
            if not parent_worker_id:
                continue
            if parent_worker_id not in worker_by_id:
                if _is_active_worker_status(getattr(worker, "status", "")):
                    self.store.update_worker_status(run_id, "stopped")
                    self.store.update_worker_result(
                        run_id,
                        error=(
                            "Orphaned child worker reconciled during startup: "
                            "parent worker record is missing."
                        ),
                    )
                    orphan_reconciled += 1
                continue
            lineage_id = self._worker_lineage.get(run_id, run_id)
            self._worker_children.setdefault(parent_worker_id, set()).add(run_id)
            self._lineage_children_total[lineage_id] = (
                int(self._lineage_children_total.get(lineage_id, 0)) + 1
            )
            if _is_active_worker_status(getattr(worker, "status", "")):
                correlation_id = self._worker_correlation_by_run_id.get(run_id)
                if correlation_id:
                    self._active_workers_by_correlation.setdefault(correlation_id, set()).add(
                        run_id
                    )
                self._lineage_children_active.setdefault(lineage_id, set()).add(run_id)

        stale_reconciled = self._reconcile_startup_stale_workers(worker_by_id)
        if orphan_reconciled or stale_reconciled:
            logger.info(
                "Restored worker registry state",
                workers_seen=len(worker_by_id),
                orphan_reconciled=orphan_reconciled,
                stale_reconciled=stale_reconciled,
            )

    def _reconcile_startup_stale_workers(self, worker_by_id: dict[str, Any]) -> int:
        runtime = self.runtime
        if not runtime or not hasattr(runtime, "is_worker_running"):
            return 0
        reconciled = 0
        for run_id, worker in worker_by_id.items():
            if not _is_active_worker_status(getattr(worker, "status", "")):
                continue
            if runtime.is_worker_running(run_id):
                continue
            self.store.update_worker_status(run_id, "stopped")
            self.store.update_worker_result(
                run_id,
                error="Worker process not found in runtime during startup reconciliation.",
            )
            self._mark_worker_inactive(run_id)
            reconciled += 1
        return reconciled

    def start_background_tasks(
        self,
        cleanup_interval_seconds: int = 3600,
        *,
        scheduler_interval_seconds: int | None = None,
    ):
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(
                self._periodic_cleanup(cleanup_interval_seconds)
            )
            logger.info("Started periodic worker cleanup task")
        if self._metrics_task is None or self._metrics_task.done():
            self._metrics_task = asyncio.create_task(self._periodic_metrics_publish(10))
            logger.info("Started periodic metrics publishing task")
        if self.scheduler and (self._scheduler_task is None or self._scheduler_task.done()):
            resolved_interval = scheduler_interval_seconds or _env_int(
                "OCTOPAL_SCHEDULER_TICK_INTERVAL_SECONDS", 60, minimum=5
            )
            max_tasks = _env_int("OCTOPAL_SCHEDULER_TICK_MAX_TASKS", 10, minimum=1)
            self._scheduler_interval_seconds = int(resolved_interval)
            self._scheduler_max_tasks = int(max_tasks)
            self._publish_scheduler_metrics(
                running=True,
                interval_seconds=resolved_interval,
                max_tasks=max_tasks,
                last_tick_status="starting",
            )
            self._scheduler_task = asyncio.create_task(
                self._periodic_scheduler_tick(resolved_interval, max_tasks=max_tasks)
            )
            logger.info(
                "Started periodic scheduler tick task",
                interval_seconds=resolved_interval,
                max_tasks=max_tasks,
            )
        if self._self_control_task is None or self._self_control_task.done():
            self._self_control_task = asyncio.create_task(
                self._periodic_self_control_requests()
            )
            logger.info("Started self-control request executor")

    async def stop_background_tasks(self):
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                logger.info("Stopped periodic worker cleanup task")

        if self._metrics_task and not self._metrics_task.done():
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                logger.info("Stopped periodic metrics publishing task")
        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                logger.info("Stopped periodic scheduler tick task")
        if self.scheduler is not None:
            self._publish_scheduler_metrics(running=False, last_tick_status="stopped")
        if self._self_control_task and not self._self_control_task.done():
            self._self_control_task.cancel()
            try:
                await self._self_control_task
            except asyncio.CancelledError:
                logger.info("Stopped self-control request executor")

        # Shutdown MCP sessions
        if self.mcp_manager:
            await self.mcp_manager.shutdown()

        # Shutdown browser sessions
        await get_browser_manager().shutdown()

    async def initialize_system(self, bot=None, allowed_chat_ids: list[int] | None = None) -> None:
        system_chat_id = 0
        logger.info("Octo waking up")
        self.start_background_tasks()

        # Load and connect MCP servers
        if self.mcp_manager:
            await self.mcp_manager.load_and_connect_all()

        # Load and start connectors
        if self.connector_manager:
            await self.connector_manager.load_and_start_all()

        restart_resume = None
        runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
        if runtime_settings is not None:
            restart_resume = await asyncio.to_thread(
                read_pending_restart_resume,
                Path(runtime_settings.state_dir),
            )
            if restart_resume and restart_resume.get("consumed_at"):
                restart_resume = None

        wake_up_prompt = (
            "You are waking up. Inspect runtime health and available workers internally. "
            "Use only bounded control-plane tools if needed, but never output a tool name or tool syntax as your final answer. "
            "Then produce a short friendly startup status message for the user in plain language."
        )
        if restart_resume:
            wake_up_prompt += "\n\n" + _build_restart_resume_message(restart_resume)
        original_send = self.internal_send
        chat_ids = [
            chat_id
            for item in (allowed_chat_ids or [])
            if (chat_id := _coerce_positive_chat_id(item)) is not None
        ]
        self._scheduled_delivery_chat_ids = list(chat_ids)
        if chat_ids and (bot or callable(original_send)):
            logger.info("Octo will send initialization message", count=len(chat_ids))
            logger.debug("Allowed chat_ids", chat_ids=chat_ids)

            async def send_to_allowed_chats(chat_id, text):
                for target_chat_id in chat_ids:
                    try:
                        if callable(original_send):
                            # Reuse the active channel send pipeline when one is attached.
                            await original_send(target_chat_id, text)
                        else:
                            await bot.send_message(chat_id=target_chat_id, text=text)
                        logger.debug("Sent initialization message", chat_id=target_chat_id)
                    except Exception as e:
                        logger.warning("Failed to send to chat_id", chat_id=target_chat_id, error=e)

            self.internal_send = send_to_allowed_chats
        else:
            logger.warning(
                "No allowed user channel recipients configured; octo will not send ready message."
            )
            self.internal_send = None
        try:
            result = await route_internal_maintenance(
                self,
                system_chat_id,
                wake_up_prompt,
            )
            if should_suppress_user_delivery(result):
                result = "Octo is online. Initialization is complete and I am ready for your tasks."
            logger.info(
                "Octo wake up complete", result_preview=f"{result[:60]}..." if result else "empty"
            )

            # Send the Octo's own response to allowed chats if configured.
            if result and self.internal_send and chat_ids:
                try:
                    await self.internal_send(system_chat_id, result)
                    logger.info("Octo initialization response sent")
                except Exception as e:
                    logger.warning("Failed to send octo initialization response", error=e)
            if restart_resume and runtime_settings is not None:
                await asyncio.to_thread(
                    mark_restart_resume_consumed,
                    Path(runtime_settings.state_dir),
                )
        except Exception:
            logger.exception("Octo failed to complete wake-up task")
        finally:
            self.internal_send = original_send

    def peek_context_wakeup(self, chat_id: int) -> str:
        pending = self._pending_wakeup_by_chat or {}
        return str(pending.get(chat_id, "") or "")

    def has_pending_conversational_closure(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        self._prune_pending_conversational_closures()
        pending = self._pending_conversational_closure_by_correlation
        if pending is None:
            return False
        return correlation_id in pending

    def mark_pending_conversational_closure(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        self._prune_pending_conversational_closures()
        pending = self._pending_conversational_closure_by_correlation
        if pending is None:
            pending = {}
            self._pending_conversational_closure_by_correlation = pending
        pending[correlation_id] = utc_now()

    def clear_pending_conversational_closure(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        pending = self._pending_conversational_closure_by_correlation
        if pending is None:
            return
        pending.pop(correlation_id, None)

    def mark_structured_followup_required(self, correlation_id: str | None = None) -> None:
        if not correlation_id:
            correlation_id = str(correlation_id_var.get() or "").strip() or None
        if not correlation_id:
            return
        self._prune_structured_followup_required()
        hints = self._structured_followup_required_by_correlation
        if hints is None:
            hints = {}
            self._structured_followup_required_by_correlation = hints
        hints[correlation_id] = utc_now()

    def consume_structured_followup_required(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        self._prune_structured_followup_required()
        hints = self._structured_followup_required_by_correlation
        if hints is None:
            return False
        return correlation_id in hints and bool(hints.pop(correlation_id, None))

    def clear_structured_followup_required(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        hints = self._structured_followup_required_by_correlation
        if hints is None:
            return
        hints.pop(correlation_id, None)

    def _prune_structured_followup_required(self) -> None:
        hints = self._structured_followup_required_by_correlation
        if hints is None:
            return
        if not hints:
            return
        cutoff = utc_now() - timedelta(seconds=_PENDING_CONVERSATIONAL_CLOSURE_TTL_SECONDS)
        expired = [
            correlation_id
            for correlation_id, created_at in hints.items()
            if not created_at or created_at < cutoff
        ]
        for correlation_id in expired:
            hints.pop(correlation_id, None)

    def _prune_pending_conversational_closures(self) -> None:
        pending = self._pending_conversational_closure_by_correlation
        if pending is None:
            return
        if not pending:
            return
        cutoff = utc_now() - timedelta(seconds=_PENDING_CONVERSATIONAL_CLOSURE_TTL_SECONDS)
        expired = [
            correlation_id
            for correlation_id, created_at in pending.items()
            if not created_at or created_at < cutoff
        ]
        for correlation_id in expired:
            pending.pop(correlation_id, None)

    def suppress_turn_followups(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        self._prune_suppressed_followups()
        suppressed = self._suppressed_followups_by_correlation
        if suppressed is None:
            suppressed = {}
            self._suppressed_followups_by_correlation = suppressed
        suppressed[correlation_id] = utc_now()

    def mark_user_turn_active(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        active = self._active_user_turns_by_correlation
        if active is None:
            active = {}
            self._active_user_turns_by_correlation = active
        active[correlation_id] = utc_now()

    def mark_user_turn_inactive(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        active = self._active_user_turns_by_correlation
        if active is None:
            return
        active.pop(correlation_id, None)

    def has_active_user_turn(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        active = self._active_user_turns_by_correlation
        if active is None:
            return False
        return correlation_id in active

    def should_suppress_turn_followups(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        self._prune_suppressed_followups()
        suppressed = self._suppressed_followups_by_correlation
        if suppressed is None:
            return False
        return correlation_id in suppressed

    def clear_suppressed_turn_followups(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        suppressed = self._suppressed_followups_by_correlation
        if suppressed is None:
            return
        suppressed.pop(correlation_id, None)

    def register_worker_correlation(self, run_id: str, correlation_id: str | None) -> None:
        if not run_id or not correlation_id:
            return
        self._worker_correlation_by_run_id[run_id] = correlation_id
        self._active_workers_by_correlation.setdefault(correlation_id, set()).add(run_id)

    def register_worker_chat(self, run_id: str, chat_id: int) -> None:
        if not run_id:
            return
        self._worker_chat_by_run_id[run_id] = int(chat_id or 0)

    def get_worker_chat_id(self, run_id: str) -> int:
        if not run_id:
            return 0
        value = self._worker_chat_by_run_id.get(run_id)
        if value is not None:
            return int(value or 0)
        worker = None
        try:
            worker = self.store.get_worker(run_id)
        except Exception:
            logger.debug("Failed to resolve worker chat id from store", worker_id=run_id, exc_info=True)
        return int(getattr(worker, "chat_id", 0) or 0) if worker is not None else 0

    async def handle_worker_instruction_request(
        self,
        *,
        spec: WorkerSpec,
        request: WorkerInstructionRequest,
    ) -> None:
        if request.target != "octo":
            return
        chat_id = self.get_worker_chat_id(spec.id)
        result = WorkerResult(
            status="awaiting_instruction",
            summary=f"Worker {spec.id} requested instruction: {request.question}",
            output={
                "status": "awaiting_instruction",
                "instruction_request": request.model_dump(mode="json"),
            },
            questions=[request.question],
        )
        _enqueue_internal_result(
            self,
            chat_id,
            spec.id,
            spec.task,
            result,
            correlation_id=spec.correlation_id,
            notify_user=None,
        )
        await self._emit_worker_event(
            chat_id,
            "worker_awaiting_instruction",
            {
                "run_id": spec.id,
                "worker_template_id": spec.template_id,
                "instruction_request": request.model_dump(mode="json"),
            },
        )

    def has_active_workers_for_correlation(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        return bool(self._active_workers_by_correlation.get(correlation_id))

    def mark_internal_result_pending(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        pending = self._pending_internal_results_by_correlation
        pending[correlation_id] = int(pending.get(correlation_id, 0)) + 1

    def mark_internal_result_processed(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        pending = self._pending_internal_results_by_correlation
        remaining = int(pending.get(correlation_id, 0)) - 1
        if remaining <= 0:
            pending.pop(correlation_id, None)
            return
        pending[correlation_id] = remaining

    def has_pending_internal_results_for_correlation(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        return int(self._pending_internal_results_by_correlation.get(correlation_id, 0)) > 0

    def should_flush_worker_followups(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return True
        return (
            not self.has_active_user_turn(correlation_id)
            and not self.has_active_workers_for_correlation(correlation_id)
            and not self.has_pending_internal_results_for_correlation(correlation_id)
        )

    def _prune_suppressed_followups(self) -> None:
        suppressed = self._suppressed_followups_by_correlation
        if not suppressed:
            return
        cutoff = utc_now() - timedelta(seconds=_PENDING_CONVERSATIONAL_CLOSURE_TTL_SECONDS)
        expired = [
            correlation_id
            for correlation_id, created_at in suppressed.items()
            if not created_at or created_at < cutoff
        ]
        for correlation_id in expired:
            suppressed.pop(correlation_id, None)

    def clear_context_wakeup(self, chat_id: int) -> None:
        pending = self._pending_wakeup_by_chat or {}
        pending.pop(chat_id, None)

    def note_user_visible_delivery(self, chat_id: int, text: str) -> None:
        normalized = _normalize_compact(text)
        if normalized:
            self._last_reply_norm_by_chat[chat_id] = normalized
        self._last_user_visible_delivery_at_by_chat[chat_id] = utc_now()

    def should_suppress_heartbeat_delivery(self, chat_id: int, text: str) -> bool:
        if _HEARTBEAT_USER_VISIBLE_COOLDOWN_SECONDS <= 0:
            return False
        delivered_at = (self._last_user_visible_delivery_at_by_chat or {}).get(chat_id)
        if delivered_at is None:
            return False
        try:
            elapsed = (utc_now() - delivered_at).total_seconds()
        except Exception:
            return False
        if elapsed < 0:
            return False
        suppress = elapsed < _HEARTBEAT_USER_VISIBLE_COOLDOWN_SECONDS
        if suppress:
            logger.info(
                "Suppressing heartbeat delivery after recent visible message",
                chat_id=chat_id,
                cooldown_seconds=_HEARTBEAT_USER_VISIBLE_COOLDOWN_SECONDS,
                elapsed_seconds=round(elapsed, 2),
                text_len=len(text or ""),
            )
        return suppress

    def get_context_thresholds(self) -> dict[str, dict[str, float | int]]:
        return {
            "watch": dict(_WATCH_THRESHOLDS),
            "reset_soon": dict(_RESET_SOON_THRESHOLDS),
        }

    async def get_self_queue(self, chat_id: int) -> list[dict[str, Any]]:
        await self._ensure_self_queue_loaded(chat_id)
        queue = list((self._self_queue_by_chat or {}).get(chat_id, []))
        return [dict(item) for item in queue]

    async def _ensure_self_queue_loaded(self, chat_id: int) -> None:
        if chat_id in self._self_queue_by_chat:
            return
        loaded = await asyncio.to_thread(_load_self_queue, _workspace_dir(), chat_id)
        self._self_queue_by_chat[chat_id] = loaded

    async def add_self_queue_item(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        await self._ensure_self_queue_loaded(chat_id)
        title = str((args or {}).get("title", "") or "").strip()
        task = str((args or {}).get("task", "") or "").strip()
        if not title or not task:
            return {"status": "error", "message": "title and task are required"}
        priority = max(1, min(5, int((args or {}).get("priority", 3) or 3)))
        source = str((args or {}).get("source", "octo") or "octo").strip()[:64]
        queue = self._self_queue_by_chat.setdefault(chat_id, [])
        item = {
            "task_id": str(uuid4()),
            "title": title,
            "task": task,
            "priority": priority,
            "source": source,
            "status": "pending",
            "created_at": utc_now().isoformat(),
            "updated_at": utc_now().isoformat(),
            "notes": str((args or {}).get("notes", "") or "").strip(),
        }
        queue.append(item)
        queue.sort(key=lambda i: (-int(i.get("priority", 3)), str(i.get("created_at", ""))))
        await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
        return {"status": "ok", "item": item, "queue_size": len(queue)}

    async def take_next_self_queue_item(self, chat_id: int) -> dict[str, Any]:
        await self._ensure_self_queue_loaded(chat_id)
        queue = self._self_queue_by_chat.setdefault(chat_id, [])
        for item in queue:
            if str(item.get("status", "pending")) == "pending":
                item["status"] = "claimed"
                item["updated_at"] = utc_now().isoformat()
                await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
                return {"status": "ok", "item": dict(item)}
        return {"status": "empty", "message": "no pending self-queue items"}

    async def update_self_queue_item(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        await self._ensure_self_queue_loaded(chat_id)
        task_id = str((args or {}).get("task_id", "") or "").strip()
        new_status = str((args or {}).get("status", "") or "").strip().lower()
        if not task_id or new_status not in {"pending", "claimed", "done", "cancelled"}:
            return {"status": "error", "message": "task_id and valid status are required"}
        queue = self._self_queue_by_chat.setdefault(chat_id, [])
        for item in queue:
            if str(item.get("task_id", "")) == task_id:
                item["status"] = new_status
                if "notes" in (args or {}):
                    item["notes"] = str((args or {}).get("notes", "") or "").strip()
                item["updated_at"] = utc_now().isoformat()
                await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
                return {"status": "ok", "item": dict(item)}
        return {"status": "not_found", "task_id": task_id}

    async def scan_opportunities(self, chat_id: int, limit: int = 3) -> dict[str, Any]:
        health = await self.get_context_health_snapshot(chat_id)
        await self._ensure_self_queue_loaded(chat_id)
        queue = self._self_queue_by_chat.setdefault(chat_id, [])
        opportunities: list[dict[str, Any]] = []

        context_size = int(health.get("context_size_estimate", 0) or 0)
        repetition = float(health.get("repetition_score", 0.0) or 0.0)
        no_progress = int(health.get("no_progress_turns", 0) or 0)
        resets_since_progress = int(health.get("resets_since_progress", 0) or 0)
        context_health = str(health.get("context_health", "OK") or "OK")

        if context_health == "RESET_SOON":
            opportunities.append(
                _build_opportunity_card(
                    kind="stability",
                    title="Context compaction before quality drops",
                    why_now=f"context_health={context_health}, size={context_size}",
                    impact="high",
                    effort="low",
                    confidence=0.93,
                    next_action="Call octo_context_reset(mode='soft') with concise handoff.",
                )
            )
        if no_progress >= 3 or repetition >= 0.70:
            opportunities.append(
                _build_opportunity_card(
                    kind="momentum",
                    title="Break stagnation with a replan cycle",
                    why_now=f"no_progress_turns={no_progress}, repetition_score={round(repetition, 3)}",
                    impact="high",
                    effort="medium",
                    confidence=0.82,
                    next_action="Create 1 focused task in self-queue and execute immediately.",
                )
            )
        if resets_since_progress >= 1:
            opportunities.append(
                _build_opportunity_card(
                    kind="recovery",
                    title="Post-reset recovery check",
                    why_now=f"resets_since_progress={resets_since_progress}",
                    impact="medium",
                    effort="low",
                    confidence=0.78,
                    next_action="Audit open threads and lock one measurable next step.",
                )
            )
        if not opportunities:
            opportunities.append(
                _build_opportunity_card(
                    kind="improvement",
                    title="Proactive improvement pass",
                    why_now="system is stable; use spare cycle for compounding gains",
                    impact="medium",
                    effort="medium",
                    confidence=0.74,
                    next_action="Pick one automation or cleanup task and add it to self-queue.",
                )
            )

        opportunities = opportunities[: max(1, min(limit, 5))]
        self._last_opportunities_by_chat[chat_id] = [dict(item) for item in opportunities]
        await asyncio.to_thread(
            _persist_last_opportunities, _workspace_dir(), chat_id, opportunities
        )
        pending_count = sum(1 for item in queue if str(item.get("status", "pending")) == "pending")
        return {
            "status": "ok",
            "chat_id": chat_id,
            "opportunities": opportunities,
            "queue_pending": pending_count,
            "generated_at": utc_now().isoformat(),
        }

    async def get_context_health_snapshot(self, chat_id: int) -> dict[str, Any]:
        trace_started_at_ms = now_ms()
        previous_snapshot = dict((self._context_health_by_chat or {}).get(chat_id, {}))
        trace_metadata: dict[str, Any] = {
            "chat_id": chat_id,
            "previous_context_health": str(previous_snapshot.get("context_health") or ""),
        }
        trace_ctx, trace_token, is_root_trace = await _start_background_trace_context(
            self.trace_sink,
            name="context.health",
            chat_id=chat_id,
            correlation_id=str(correlation_id_var.get() or "").strip() or None,
            metadata=trace_metadata,
        )
        trace_status = "ok"
        trace_output: dict[str, Any] | None = None
        try:
            recent_entries_all = await asyncio.to_thread(
                self.store.list_memory_entries_by_chat, chat_id, 120
            )
            recent_entries = [
                entry
                for entry in recent_entries_all
                if not bool((entry.metadata or {}).get("heartbeat"))
            ]
            entry_count = len(recent_entries)
            context_size_estimate = sum(len(e.content or "") for e in recent_entries)
            repetition_score = _estimate_repetition_score(recent_entries)
            error_streak = _estimate_error_streak(recent_entries)
            no_progress_turns = int((self._no_progress_turns_by_chat or {}).get(chat_id, 0))
            resets_since_progress = int(
                (self._reset_streak_without_progress_by_chat or {}).get(chat_id, 0)
            )
            overload_score = min(
                1.0,
                (context_size_estimate / float(_WATCH_THRESHOLDS["context_size_estimate"]))
                + (repetition_score * 0.9)
                + (min(8, error_streak) / 10.0)
                + (min(12, no_progress_turns) / 12.0),
            )
            watch_conditions = _watch_conditions(
                context_size_estimate=context_size_estimate,
                repetition_score=repetition_score,
                error_streak=error_streak,
                no_progress_turns=no_progress_turns,
            )
            watch_signal_count = sum(1 for cond in watch_conditions if cond)
            watch_escalation_streak = int(
                (self._watch_escalation_streak_by_chat or {}).get(chat_id, 0)
            )
            if watch_signal_count >= 2:
                watch_escalation_streak += 1
            else:
                watch_escalation_streak = 0
            self._watch_escalation_streak_by_chat[chat_id] = watch_escalation_streak
            severe = _is_reset_soon_severe(
                context_size_estimate=context_size_estimate,
                repetition_score=repetition_score,
                error_streak=error_streak,
                no_progress_turns=no_progress_turns,
            )
            context_health = (
                "RESET_SOON"
                if (severe or watch_escalation_streak >= 2)
                else ("WATCH" if watch_signal_count > 0 else "OK")
            )
            snapshot = {
                "chat_id": chat_id,
                "entry_count": entry_count,
                "context_size_estimate": context_size_estimate,
                "repetition_score": round(repetition_score, 3),
                "error_streak": error_streak,
                "no_progress_turns": no_progress_turns,
                "resets_since_progress": resets_since_progress,
                "overload_score": round(overload_score, 3),
                "watch_signal_count": watch_signal_count,
                "watch_escalation_streak": watch_escalation_streak,
                "context_health": context_health,
                "updated_at": utc_now().isoformat(),
            }
            self._context_health_by_chat[chat_id] = snapshot
            trace_output = {
                "context_health": context_health,
                "entry_count": entry_count,
                "context_size_estimate": context_size_estimate,
                "repetition_score": round(repetition_score, 3),
                "error_streak": error_streak,
                "no_progress_turns": no_progress_turns,
                "resets_since_progress": resets_since_progress,
                "overload_score": round(overload_score, 3),
                "watch_signal_count": watch_signal_count,
                "watch_escalation_streak": watch_escalation_streak,
            }
            previous_health = str(previous_snapshot.get("context_health") or "")
            if previous_health and previous_health != context_health and trace_ctx is not None:
                await self.trace_sink.annotate(
                    trace_ctx,
                    name="context.health.changed",
                    metadata={
                        "from_state": previous_health,
                        "to_state": context_health,
                        "chat_id": chat_id,
                    },
                )
            return snapshot
        except Exception as exc:
            trace_status = "error"
            trace_metadata.update(summarize_exception(exc))
            raise
        finally:
            trace_metadata["duration_ms"] = round(now_ms() - trace_started_at_ms, 2)
            await _finish_background_trace_context(
                self.trace_sink,
                trace_ctx,
                trace_token,
                is_root_trace=is_root_trace,
                status=trace_status,
                output=trace_output,
                metadata=trace_metadata,
            )

    async def build_heartbeat_context_hint(self, chat_id: int) -> str:
        snap = await self.get_context_health_snapshot(chat_id)
        return (
            "Context health metrics:\n"
            f"- context_size_estimate={snap['context_size_estimate']}\n"
            f"- repetition_score={snap['repetition_score']}\n"
            f"- error_streak={snap['error_streak']}\n"
            f"- no_progress_turns={snap['no_progress_turns']}\n"
            f"- resets_since_progress={snap['resets_since_progress']}\n"
            f"- overload_score={snap['overload_score']}\n"
            f"- watch_signal_count={snap['watch_signal_count']}\n"
            f"- watch_escalation_streak={snap['watch_escalation_streak']}\n"
            f"- context_health={snap['context_health']}\n"
            "Decision thresholds:\n"
            f"- WATCH if any: size>={_WATCH_THRESHOLDS['context_size_estimate']}, repetition>={_WATCH_THRESHOLDS['repetition_score']:.2f}, "
            f"error_streak>={_WATCH_THRESHOLDS['error_streak']}, no_progress>={_WATCH_THRESHOLDS['no_progress_turns']}.\n"
            f"- RESET_SOON if any: size>={_RESET_SOON_THRESHOLDS['context_size_estimate']}, repetition>={_RESET_SOON_THRESHOLDS['repetition_score']:.2f}, "
            f"error_streak>={_RESET_SOON_THRESHOLDS['error_streak']}, no_progress>={_RESET_SOON_THRESHOLDS['no_progress_turns']}.\n"
            "- Also RESET_SOON if 2+ WATCH signals persist for 2+ heartbeats.\n"
            "If context_health is RESET_SOON, call `octo_context_reset` with mode='soft' and a concise handoff."
        )

    def _register_progress(self, chat_id: int, reason: str) -> None:
        self._no_progress_turns_by_chat[chat_id] = 0
        self._reset_streak_without_progress_by_chat[chat_id] = 0
        self._progress_revision_by_chat[chat_id] = (
            int(self._progress_revision_by_chat.get(chat_id, 0)) + 1
        )
        logger.debug("Registered progress", chat_id=chat_id, reason=reason)

    async def request_context_reset(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        trace_started_at_ms = now_ms()
        mode = str(args.get("mode", "soft") or "soft").strip().lower()
        if mode not in {"soft", "hard"}:
            mode = "soft"

        reason = str(args.get("reason", "") or "").strip() or "context overloaded"
        confidence = _coerce_float(args.get("confidence"), default=0.8)
        confirm = bool(args.get("confirm", False))
        trace_metadata: dict[str, Any] = {
            "chat_id": chat_id,
            "mode": mode,
            "reason": reason,
            "confidence": confidence,
            "confirm": confirm,
        }
        trace_ctx, trace_token, is_root_trace = await _start_background_trace_context(
            self.trace_sink,
            name="context.reset",
            chat_id=chat_id,
            correlation_id=str(correlation_id_var.get() or "").strip() or None,
            metadata=trace_metadata,
        )
        trace_status = "ok"
        trace_output: dict[str, Any] | None = None
        try:
            health = await self.get_context_health_snapshot(chat_id)

            progress_rev = int(self._progress_revision_by_chat.get(chat_id, 0))
            last_reset_rev = int(self._last_reset_progress_revision_by_chat.get(chat_id, -1))
            no_progress_since_last_reset = progress_rev <= last_reset_rev
            current_streak = int(self._reset_streak_without_progress_by_chat.get(chat_id, 0))
            proposed_streak = (current_streak + 1) if no_progress_since_last_reset else 1

            requires_confirm_reasons: list[str] = []
            if mode == "hard":
                requires_confirm_reasons.append("hard_reset")
            if confidence < _RESET_CONFIDENCE_MIN:
                requires_confirm_reasons.append("low_confidence_handoff")
            if proposed_streak >= _RESET_CONFIRM_THRESHOLD:
                requires_confirm_reasons.append("repeated_reset_without_progress")
            trace_metadata["requires_confirmation_for"] = list(requires_confirm_reasons)
            if requires_confirm_reasons and not confirm:
                if trace_ctx is not None:
                    await self.trace_sink.annotate(
                        trace_ctx,
                        name="context.reset_requested",
                        metadata={
                            "status": "needs_confirmation",
                            "requires_confirmation_for": list(requires_confirm_reasons),
                            "chat_id": chat_id,
                        },
                    )
                trace_output = {
                    "status": "needs_confirmation",
                    "requires_confirmation_for": list(requires_confirm_reasons),
                    "health_before": health,
                }
                return {
                    "status": "needs_confirmation",
                    "mode": mode,
                    "reason": reason,
                    "confidence": confidence,
                    "requires_confirmation_for": requires_confirm_reasons,
                    "message": (
                        "Reset blocked until confirmation. Re-run octo_context_reset with confirm=true "
                        "to proceed."
                    ),
                    "health": health,
                }

            handoff = {
                "chat_id": chat_id,
                "created_at": utc_now().isoformat(),
                "mode": mode,
                "reason": reason,
                "confidence": confidence,
                "goal_now": str(args.get("goal_now", "") or "").strip(),
                "done": _normalize_string_list(args.get("done")),
                "open_threads": _normalize_string_list(args.get("open_threads")),
                "critical_constraints": _normalize_string_list(args.get("critical_constraints")),
                "next_step": str(args.get("next_step", "") or "").strip(),
                "current_interest": str(args.get("current_interest", "") or "").strip(),
                "pending_human_input": str(args.get("pending_human_input", "") or "").strip(),
                "cognitive_state": str(args.get("cognitive_state", "") or "focused")
                .strip()
                .lower(),
                "health_snapshot": health,
            }
            if not handoff["goal_now"]:
                handoff["goal_now"] = "Continue current task with focused context."
            if not handoff["next_step"]:
                handoff["next_step"] = "Review handoff and choose: continue, clarify, or replan."

            try:
                if trace_ctx is not None:
                    await self.trace_sink.annotate(
                        trace_ctx,
                        name="context.reset_requested",
                        metadata={
                            "status": "executing",
                            "mode": mode,
                            "chat_id": chat_id,
                        },
                    )
                workspace_dir = Path(os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")).resolve()
                file_info = await asyncio.to_thread(
                    _persist_context_reset_files, workspace_dir, handoff
                )
                reflection_entry: dict[str, Any] | None = None
                if self.reflection is not None:
                    try:
                        record = await asyncio.to_thread(
                            self.reflection.record_context_reset,
                            chat_id,
                            handoff,
                        )
                        reflection_entry = {
                            "id": record.id,
                            "kind": record.kind,
                            "summary": record.summary,
                        }
                    except Exception:
                        logger.warning(
                            "Reflection record failed during context reset",
                            chat_id=chat_id,
                            exc_info=True,
                        )
                memchain_info: dict[str, Any] | None = None
                try:
                    memchain_info = await asyncio.to_thread(
                        memchain_record,
                        workspace_dir,
                        reason="context_reset",
                        meta={"mode": mode, "chat_id": chat_id, "source": "octo_context_reset"},
                    )
                except Exception as exc:
                    logger.warning(
                        "Memchain record failed during context reset",
                        chat_id=chat_id,
                        error=str(exc),
                    )

                deleted_entries = await asyncio.to_thread(
                    self.store.delete_memory_entries_by_chat,
                    chat_id,
                    0,
                )
                if mode == "hard":
                    await asyncio.to_thread(
                        self.store.set_chat_bootstrap_hash, chat_id, "", utc_now()
                    )

                self._last_reply_norm_by_chat.pop(chat_id, None)
                self._last_reset_progress_revision_by_chat[chat_id] = progress_rev
                self._reset_streak_without_progress_by_chat[chat_id] = proposed_streak
                self._pending_wakeup_by_chat[chat_id] = _build_wakeup_message(
                    handoff, file_info["handoff_md"]
                )
                self._no_progress_turns_by_chat[chat_id] = 0

                await asyncio.to_thread(
                    self.store.append_audit,
                    AuditEvent(
                        id=str(uuid4()),
                        ts=utc_now(),
                        level="info",
                        event_type="octo.context_reset",
                        data={
                            "chat_id": chat_id,
                            "mode": mode,
                            "reason": reason,
                            "confidence": confidence,
                            "deleted_entries": deleted_entries,
                            "requires_confirmation_for": requires_confirm_reasons,
                            "health_snapshot": health,
                            "files": file_info,
                            "reflection": reflection_entry or {},
                            "memchain": memchain_info or {},
                        },
                    ),
                )
                if trace_ctx is not None:
                    await self.trace_sink.annotate(
                        trace_ctx,
                        name="context.reset_completed",
                        metadata={
                            "mode": mode,
                            "deleted_entries": deleted_entries,
                            "handoff_written": bool(file_info.get("handoff_md")),
                            "reflection_written": bool(reflection_entry),
                            "memchain_written": bool(memchain_info),
                            "chat_id": chat_id,
                        },
                    )
                trace_output = {
                    "status": "reset_complete",
                    "mode": mode,
                    "deleted_entries": deleted_entries,
                    "health_before": health,
                    "handoff_written": bool(file_info.get("handoff_md")),
                    "reflection_written": bool(reflection_entry),
                    "memchain_written": bool(memchain_info),
                }
                return {
                    "status": "reset_complete",
                    "mode": mode,
                    "deleted_entries": deleted_entries,
                    "handoff": handoff,
                    "files": file_info,
                    "reflection": reflection_entry or {},
                    "memchain": memchain_info or {},
                    "health_before": health,
                    "requires_confirmation_for": requires_confirm_reasons,
                    "message": "Context reset completed. Wake-up handoff is queued for the next turn.",
                }
            except Exception as exc:
                trace_status = "error"
                trace_metadata.update(summarize_exception(exc))
                raise
        except Exception as exc:
            trace_status = "error"
            trace_metadata.update(summarize_exception(exc))
            raise
        finally:
            trace_metadata["duration_ms"] = round(now_ms() - trace_started_at_ms, 2)
            await _finish_background_trace_context(
                self.trace_sink,
                trace_ctx,
                trace_token,
                is_root_trace=is_root_trace,
                status=trace_status,
                output=trace_output,
                metadata=trace_metadata,
            )

    async def request_update_check(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
        project_root = Path(__file__).resolve().parents[4]
        if runtime_settings is None:
            return {"status": "error", "message": "runtime settings are unavailable"}
        update_status = await asyncio.to_thread(check_update_status, project_root)
        await asyncio.to_thread(
            self.store.append_audit,
            AuditEvent(
                id=str(uuid4()),
                ts=utc_now(),
                level="info",
                event_type="octo.update_check",
                data={"chat_id": chat_id, "update": update_status},
            ),
        )
        return update_status

    async def request_self_restart(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
        if runtime_settings is None:
            return {"status": "error", "message": "runtime settings are unavailable"}
        reason = str(args.get("reason", "") or "").strip()
        if not reason:
            return {"status": "error", "message": "reason is required"}
        if not bool(args.get("confirm", False)):
            return {
                "status": "needs_confirmation",
                "action": "octo_restart_self",
                "message": "Self restart requires confirm=true.",
            }

        confidence = _coerce_float(args.get("confidence"), default=0.8)
        delay_seconds = _coerce_int(args.get("delay_seconds"), default=5, minimum=3, maximum=60)
        health = await self.get_context_health_snapshot(chat_id)
        handoff = {
            "chat_id": chat_id,
            "created_at": utc_now().isoformat(),
            "mode": "self_restart",
            "source": "octo_restart_self",
            "reason": reason,
            "confidence": confidence,
            "goal_now": str(args.get("goal_now", "") or "").strip(),
            "done": _normalize_string_list(args.get("done")),
            "open_threads": _normalize_string_list(args.get("open_threads")),
            "critical_constraints": _normalize_string_list(args.get("critical_constraints")),
            "next_step": str(args.get("next_step", "") or "").strip(),
            "current_interest": str(args.get("current_interest", "") or "").strip(),
            "pending_human_input": str(args.get("pending_human_input", "") or "").strip(),
            "cognitive_state": str(args.get("cognitive_state", "") or "focused")
            .strip()
            .lower(),
            "health_snapshot": health,
        }
        if not handoff["goal_now"]:
            handoff["goal_now"] = "Resume the current user task after Octo restarts."
        if not handoff["next_step"]:
            handoff["next_step"] = "Read the restart handoff and continue or clarify."

        workspace_dir = Path(
            getattr(
                runtime_settings,
                "workspace_dir",
                os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace"),
            )
        ).resolve()
        state_dir = Path(runtime_settings.state_dir)
        file_info = await asyncio.to_thread(_persist_context_reset_files, workspace_dir, handoff)
        request = await asyncio.to_thread(
            append_control_request,
            state_dir,
            action=SELF_RESTART_ACTION,
            reason=reason,
            requested_by=SELF_RESTART_REQUESTED_BY,
            delay_seconds=delay_seconds,
            metadata={"chat_id": chat_id, "handoff_file": file_info.get("handoff_md", "")},
        )
        resume_payload = {
            "status": "pending",
            "request_id": request["request_id"],
            "created_at": utc_now().isoformat(),
            "handoff": handoff,
            "files": file_info,
        }
        await asyncio.to_thread(write_pending_restart_resume, state_dir, resume_payload)

        try:
            memchain_info = await asyncio.to_thread(
                memchain_record,
                workspace_dir,
                reason="self_restart",
                meta={
                    "chat_id": chat_id,
                    "source": "octo_restart_self",
                    "request_id": request["request_id"],
                },
            )
        except Exception as exc:
            memchain_info = {"status": "error", "message": str(exc)}
            logger.warning("Memchain record failed during self restart request", error=str(exc))

        await asyncio.to_thread(
            self.store.append_audit,
            AuditEvent(
                id=str(uuid4()),
                ts=utc_now(),
                level="info",
                event_type="octo.self_restart_requested",
                data={
                    "chat_id": chat_id,
                    "reason": reason,
                    "request": request,
                    "files": file_info,
                    "memchain": memchain_info,
                },
            ),
        )
        return {
            "status": "restart_requested",
            "request": request,
            "handoff": handoff,
            "files": file_info,
            "memchain": memchain_info,
            "message": "Self restart requested. Handoff is durable and the restart helper will run shortly.",
        }

    async def request_self_update(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
        if runtime_settings is None:
            return {"status": "error", "message": "runtime settings are unavailable"}
        reason = str(args.get("reason", "") or "").strip()
        if not reason:
            return {"status": "error", "message": "reason is required"}
        if not bool(args.get("confirm", False)):
            return {
                "status": "needs_confirmation",
                "action": "octo_update_self",
                "message": "Self update requires confirm=true.",
            }

        project_root = Path(__file__).resolve().parents[4]
        update_status = await asyncio.to_thread(check_update_status, project_root)
        if not bool(update_status.get("can_update")):
            return {
                "status": "blocked",
                "message": "Update is blocked by the current checkout state.",
                "update": update_status,
            }

        confidence = _coerce_float(args.get("confidence"), default=0.8)
        delay_seconds = _coerce_int(args.get("delay_seconds"), default=5, minimum=3, maximum=60)
        health = await self.get_context_health_snapshot(chat_id)
        handoff = {
            "chat_id": chat_id,
            "created_at": utc_now().isoformat(),
            "mode": "self_update",
            "source": "octo_update_self",
            "reason": reason,
            "confidence": confidence,
            "goal_now": str(args.get("goal_now", "") or "").strip(),
            "done": _normalize_string_list(args.get("done")),
            "open_threads": _normalize_string_list(args.get("open_threads")),
            "critical_constraints": _normalize_string_list(args.get("critical_constraints")),
            "next_step": str(args.get("next_step", "") or "").strip(),
            "current_interest": str(args.get("current_interest", "") or "").strip(),
            "pending_human_input": str(args.get("pending_human_input", "") or "").strip(),
            "cognitive_state": str(args.get("cognitive_state", "") or "focused")
            .strip()
            .lower(),
            "health_snapshot": health,
            "update_status": update_status,
        }
        if not handoff["goal_now"]:
            handoff["goal_now"] = "Resume the current user task after Octo updates and restarts."
        if not handoff["next_step"]:
            handoff["next_step"] = "Read the update handoff and report whether update and restart completed."

        workspace_dir = Path(
            getattr(
                runtime_settings,
                "workspace_dir",
                os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace"),
            )
        ).resolve()
        state_dir = Path(runtime_settings.state_dir)
        file_info = await asyncio.to_thread(_persist_context_reset_files, workspace_dir, handoff)
        request = await asyncio.to_thread(
            append_control_request,
            state_dir,
            action=SELF_UPDATE_ACTION,
            reason=reason,
            requested_by=SELF_UPDATE_REQUESTED_BY,
            delay_seconds=delay_seconds,
            metadata={
                "chat_id": chat_id,
                "handoff_file": file_info.get("handoff_md", ""),
                "update": update_status,
            },
        )
        resume_payload = {
            "status": "pending",
            "request_id": request["request_id"],
            "created_at": utc_now().isoformat(),
            "handoff": handoff,
            "files": file_info,
            "update": update_status,
        }
        await asyncio.to_thread(write_pending_restart_resume, state_dir, resume_payload)

        try:
            memchain_info = await asyncio.to_thread(
                memchain_record,
                workspace_dir,
                reason="self_update",
                meta={
                    "chat_id": chat_id,
                    "source": "octo_update_self",
                    "request_id": request["request_id"],
                    "local_version": update_status.get("local_version"),
                    "latest_version": update_status.get("latest_version"),
                },
            )
        except Exception as exc:
            memchain_info = {"status": "error", "message": str(exc)}
            logger.warning("Memchain record failed during self update request", error=str(exc))

        await asyncio.to_thread(
            self.store.append_audit,
            AuditEvent(
                id=str(uuid4()),
                ts=utc_now(),
                level="info",
                event_type="octo.self_update_requested",
                data={
                    "chat_id": chat_id,
                    "reason": reason,
                    "request": request,
                    "files": file_info,
                    "update": update_status,
                    "memchain": memchain_info,
                },
            ),
        )
        return {
            "status": "update_requested",
            "request": request,
            "handoff": handoff,
            "files": file_info,
            "update": update_status,
            "memchain": memchain_info,
            "message": "Self update requested. Handoff is durable and the update helper will run shortly.",
        }

    async def handle_message(
        self,
        text: str,
        chat_id: int,
        approval_requester=None,
        show_typing: bool = True,
        is_ws: bool = False,
        images: list[str] | None = None,
        saved_file_paths: list[str] | None = None,
        persist_to_memory: bool = True,
        track_progress: bool = True,
        include_wakeup: bool = True,
        background_delivery: bool = False,
    ) -> OctoReply:
        if not is_ws and self._ws_active:
            logger.info("Ignoring Telegram message while WebSocket is active", chat_id=chat_id)
            return OctoReply(
                immediate="I'm currently active on WebSocket. Please use the WebSocket client or wait until it's closed.",
                followup=None,
            )
        correlation_token = None
        correlation_id = correlation_id_var.get()
        trace_bind_token = None
        trace_ctx = None
        trace_started_at_ms = now_ms()
        trace_status = "ok"
        trace_output: dict[str, Any] | None = None
        trace_metadata: dict[str, Any] = {
            "channel": "ws" if is_ws else "telegram",
            "message_kind": "heartbeat" if not track_progress else "user",
            "text_len": len(text),
            "has_images": bool(images),
            "has_files": bool(saved_file_paths),
            "persist_to_memory": persist_to_memory,
            "track_progress": track_progress,
            "background_delivery": background_delivery,
        }
        wants_followup = False
        finalized_visible_reply = False
        route_request = RouteRequest(
            mode=resolve_turn_route_mode(
                track_progress=track_progress,
                background_delivery=background_delivery,
            ),
            user_text=text,
            chat_id=chat_id,
            show_typing=show_typing,
            include_wakeup=include_wakeup,
            track_progress=track_progress,
            background_delivery=background_delivery,
        )
        trace_metadata["route_mode"] = route_request.mode.value
        if not correlation_id:
            correlation_id = f"turn-{uuid4()}"
            correlation_token = correlation_id_var.set(correlation_id)

        try:
            session_id = f"{'ws' if is_ws else 'chat'}:{chat_id}"
            if self.trace_sink is not None:
                trace_ctx = await self.trace_sink.start_trace(
                    name="octo.turn",
                    trace_id=correlation_id,
                    root_trace_id=correlation_id,
                    session_id=session_id,
                    chat_id=chat_id,
                    input={
                        "text_preview": safe_preview(text, limit=160),
                        "text_hash": hash_payload(text),
                    },
                    metadata=trace_metadata,
                )
                trace_bind_token = bind_trace_context(trace_ctx)
            self.mark_user_turn_active(correlation_id)
            if callable(approval_requester):
                self._approval_requesters[chat_id] = approval_requester
            logger.info(
                "Handling message",
                chat_id=chat_id,
                is_ws=is_ws,
                has_images=bool(images),
                route_mode=route_request.mode.value,
            )
            logger.debug("Received message text", text_len=len(text), text=text[:500])
            if not track_progress:
                self.suppress_turn_followups(correlation_id)
            if persist_to_memory:
                await self.memory.add_message(
                    "user",
                    text,
                    {
                        "chat_id": chat_id,
                        "has_images": bool(images),
                        "heartbeat": not track_progress,
                    },
                )
            bootstrap_context = None
            if route_request.mode is RouteMode.HEARTBEAT:
                reply_text = await route_heartbeat(
                    self,
                    chat_id,
                    text,
                    show_typing=show_typing,
                    include_wakeup=include_wakeup,
                )
            else:
                bootstrap_context = await build_bootstrap_context_prompt(self.store, chat_id)
                trace_metadata["bootstrap_chars"] = len(bootstrap_context.content)
                if bootstrap_context.files:
                    files_summary = ", ".join(
                        [f"{name} ({size} chars)" for name, size in bootstrap_context.files]
                    )
                    logger.debug(
                        "Octo bootstrap files",
                        route_mode=route_request.mode.value,
                        files=files_summary,
                        file_count=len(bootstrap_context.files),
                        total_chars=len(bootstrap_context.content),
                        hash=bootstrap_context.hash,
                    )
                route_kwargs: dict[str, Any] = {
                    "show_typing": show_typing,
                    "images": images,
                    "saved_file_paths": saved_file_paths,
                    "include_wakeup": include_wakeup,
                    "route_mode": route_request.mode,
                }
                while True:
                    try:
                        reply_text = await route_or_reply(
                            self,
                            self.provider,
                            self.memory,
                            text,
                            chat_id,
                            bootstrap_context.content,
                            **route_kwargs,
                        )
                        break
                    except TypeError as exc:
                        # Backward-compatible fallback for monkeypatched tests/extensions using older signatures.
                        msg = str(exc)
                        if "unexpected keyword argument" not in msg:
                            raise
                        removed = False
                        for key in list(route_kwargs.keys()):
                            if f"'{key}'" in msg:
                                route_kwargs.pop(key, None)
                                removed = True
                                break
                        if not removed:
                            raise
            initial_reaction_emoji, _ = extract_reaction_and_strip(reply_text or "")
            wants_followup = self.consume_structured_followup_required(correlation_id)
            if not track_progress:
                wants_followup = False
                if background_delivery:
                    reply_text = await _normalize_heartbeat_delivery_reply(
                        self.provider, reply_text
                    )
                else:
                    reply_text = _coerce_control_plane_reply(reply_text)
                if route_request.mode is RouteMode.HEARTBEAT:
                    runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
                    if runtime_settings is not None:
                        await asyncio.to_thread(
                            update_last_internal_heartbeat,
                            runtime_settings,
                        )
            logger.info("Octo response ready")
            if track_progress:
                reply_norm = _normalize_compact(reply_text)
                prior_reply = self._last_reply_norm_by_chat.get(chat_id, "")
                if _is_progress_reply(reply_norm, prior_reply):
                    self._register_progress(chat_id, "assistant_response")
                else:
                    self._no_progress_turns_by_chat[chat_id] = (
                        int(self._no_progress_turns_by_chat.get(chat_id, 0)) + 1
                    )
                self._last_reply_norm_by_chat[chat_id] = reply_norm
            if wants_followup:
                self.mark_pending_conversational_closure(correlation_id)
            try:
                await self.get_context_health_snapshot(chat_id)
            except Exception:
                logger.debug(
                    "Failed to refresh context health snapshot", chat_id=chat_id, exc_info=True
                )
            if include_wakeup:
                self.clear_context_wakeup(chat_id)
            if bootstrap_context is not None and bootstrap_context.hash:
                await asyncio.to_thread(
                    self.store.set_chat_bootstrap_hash, chat_id, bootstrap_context.hash, utc_now()
                )
            immediate_text = sanitize_user_facing_text_preserving_reaction(reply_text)
            reaction_emoji, _ = extract_reaction_and_strip(reply_text or "")
            reaction_emoji = reaction_emoji or initial_reaction_emoji
            delivery = resolve_user_delivery(immediate_text, followup_required=wants_followup)
            logger.debug(
                "OctoReply prepared for channel delivery",
                chat_id=chat_id,
                route_mode=route_request.mode.value,
                has_react_tag="<react>" in immediate_text.lower(),
                reaction=reaction_emoji,
                delivery_mode=delivery.mode,
            )
            if not persist_to_memory and delivery.user_visible:
                await self.memory.add_message(
                    "assistant",
                    delivery.text,
                    {
                        "chat_id": chat_id,
                        "background_delivery": True,
                        "heartbeat": not track_progress,
                    },
                )
            elif persist_to_memory and delivery.user_visible:
                await self.memory.add_message(
                    "assistant",
                    delivery.text,
                    {
                        "chat_id": chat_id,
                        "heartbeat": not track_progress,
                    },
                )
            if delivery.user_visible and track_progress:
                self.note_user_visible_delivery(chat_id, delivery.text)
                if not delivery.followup_required:
                    finalized_visible_reply = True
                    self.suppress_turn_followups(correlation_id)
                    logger.info(
                        "Suppressing worker follow-ups after final in-turn reply",
                        chat_id=chat_id,
                    )
            trace_output = {
                "delivery_mode": delivery.mode,
                "followup_required": delivery.followup_required,
                "user_visible": delivery.user_visible,
                "reaction": reaction_emoji,
            }
            return OctoReply(
                immediate=delivery.text,
                followup=None,
                followup_required=delivery.followup_required,
                reaction=reaction_emoji,
                delivery_mode=delivery.mode,
            )
        except Exception as exc:
            trace_status = "error"
            trace_metadata.update(summarize_exception(exc))
            raise
        finally:
            self.mark_user_turn_inactive(correlation_id)
            if track_progress and not finalized_visible_reply:
                self.clear_suppressed_turn_followups(correlation_id)
            if finalized_visible_reply:
                dropped = _discard_worker_followup_batch(
                    chat_id,
                    correlation_id,
                    only_if_created_during_active_turn=True,
                )
                if dropped:
                    logger.info(
                        "Dropped worker follow-up after final in-turn reply",
                        chat_id=chat_id,
                    )
            pending_followup_work = self.has_active_workers_for_correlation(
                correlation_id
            ) or self.has_pending_internal_results_for_correlation(correlation_id)
            if wants_followup and pending_followup_work and not finalized_visible_reply:
                _schedule_worker_followup_flush(self, chat_id, correlation_id)
            else:
                _discard_worker_followup_batch(
                    chat_id,
                    correlation_id,
                    only_if_created_during_active_turn=True,
                )
            self.clear_structured_followup_required(correlation_id)
            if trace_ctx is not None and self.trace_sink is not None:
                finish_meta = dict(trace_metadata)
                finish_meta.update(
                    {
                        "duration_ms": round(now_ms() - trace_started_at_ms, 2),
                        "wants_followup": wants_followup,
                        "finalized_visible_reply": finalized_visible_reply,
                        "active_workers_for_correlation": self.has_active_workers_for_correlation(
                            correlation_id
                        ),
                        "pending_internal_results": self.has_pending_internal_results_for_correlation(
                            correlation_id
                        ),
                    }
                )
                await self.trace_sink.finish_trace(
                    trace_ctx,
                    status=trace_status,
                    output=trace_output,
                    metadata=finish_meta,
                )
            if trace_bind_token is not None:
                reset_trace_context(trace_bind_token)
            if correlation_token is not None:
                correlation_id_var.reset(correlation_token)

    async def _start_worker_async(
        self,
        worker_id: str,
        task: str,
        chat_id: int,
        inputs: dict[str, Any] | None,
        tools: list[str] | None,
        model: str | None,
        timeout_seconds: int | None,
        scheduled_task_id: str | None = None,
        parent_worker_id: str | None = None,
        lineage_id: str | None = None,
        root_task_id: str | None = None,
        spawn_depth: int = 0,
        allowed_paths: list[str] | None = None,
    ) -> dict[str, Any]:
        trace_sink = self.trace_sink
        parent_trace_ctx = get_current_trace_context()
        dispatch_trace_ctx = None
        dispatch_started_at_ms = now_ms()
        dispatch_trace_status = "ok"
        dispatch_trace_output: dict[str, Any] | None = None
        dispatch_trace_metadata: dict[str, Any] = {
            "worker_template_id": worker_id,
            "task_preview": safe_preview(task, limit=240),
            "chat_id": chat_id,
            "scheduled_task_id": scheduled_task_id,
            "parent_worker_id": parent_worker_id,
            "lineage_id": lineage_id,
            "root_task_id": root_task_id,
            "spawn_depth": spawn_depth,
            "allowed_paths_count": len(allowed_paths or []),
        }
        if trace_sink is not None and parent_trace_ctx is not None:
            dispatch_trace_ctx = await trace_sink.start_span(
                parent_trace_ctx,
                name="worker.dispatch",
                metadata=dispatch_trace_metadata,
            )
        if parent_worker_id:
            violation = self._check_child_spawn_limits(
                lineage_id=lineage_id,
                spawn_depth=spawn_depth,
            )
            if violation:
                dispatch_trace_status = "error"
                dispatch_trace_metadata["rejected_reason"] = violation
                if dispatch_trace_ctx is not None and trace_sink is not None:
                    finish_meta = dict(dispatch_trace_metadata)
                    finish_meta["duration_ms"] = round(now_ms() - dispatch_started_at_ms, 2)
                    await trace_sink.finish_span(
                        dispatch_trace_ctx,
                        status=dispatch_trace_status,
                        output={"status": "rejected"},
                        metadata=finish_meta,
                    )
                return {
                    "status": "rejected",
                    "reason": violation,
                    "worker_id": None,
                    "run_id": None,
                }

        # Create a task signature for duplicate detection
        schedule_sig = scheduled_task_id or "-"
        parent_sig = parent_worker_id or "-"
        task_signature = f"{worker_id}:{schedule_sig}:{parent_sig}:{task[:100]}"  # Keep duplicate detection strict per schedule/task pair.
        correlation_id = correlation_id_var.get()
        if not self._reserve_recent_task(
            chat_id=chat_id,
            correlation_id=correlation_id,
            task_signature=task_signature,
        ):
            logger.warning(
                "Duplicate worker task detected, skipping",
                worker_id=worker_id,
                task_prefix=task[:50],
            )
            skipped_id = f"skipped-duplicate-{uuid4().hex[:8]}"
            await self._emit_progress(
                chat_id,
                "duplicate",
                "Duplicate worker request detected; skipping duplicate launch.",
                {"worker_template_id": worker_id},
            )
            dispatch_trace_output = {
                "status": "skipped_duplicate",
                "run_id": skipped_id,
            }
            if dispatch_trace_ctx is not None and trace_sink is not None:
                finish_meta = dict(dispatch_trace_metadata)
                finish_meta["duration_ms"] = round(now_ms() - dispatch_started_at_ms, 2)
                await trace_sink.finish_span(
                    dispatch_trace_ctx,
                    status=dispatch_trace_status,
                    output=dispatch_trace_output,
                    metadata=finish_meta,
                )
            return {
                "status": "skipped_duplicate",
                "run_id": skipped_id,
                "worker_id": None,
            }
        try:
            run_id = str(uuid4())
            effective_lineage_id = lineage_id or run_id
            effective_root_task_id = root_task_id or run_id
            effective_spawn_depth = max(0, int(spawn_depth))
            template = None
            if hasattr(self.store, "get_worker_template"):
                try:
                    template = await asyncio.to_thread(self.store.get_worker_template, worker_id)
                except Exception:
                    logger.debug(
                        "Failed to load worker template for timeout resolution",
                        worker_template_id=worker_id,
                        exc_info=True,
                    )
            resolved_timeout_seconds, timeout_meta = _resolve_worker_timeout_seconds(
                explicit_timeout_seconds=timeout_seconds,
                template=template,
                task=task,
                tools=tools,
                scheduled_task_id=scheduled_task_id,
            )
            dispatch_trace_metadata.update(
                {
                    "run_id": run_id,
                    "lineage_id": effective_lineage_id,
                    "root_task_id": effective_root_task_id,
                    "spawn_depth": effective_spawn_depth,
                    "timeout_seconds": resolved_timeout_seconds,
                    "timeout_source": timeout_meta.get("source"),
                }
            )
            if scheduled_task_id and self.scheduler:
                scheduled_task = self.scheduler.get_task(scheduled_task_id)
                if scheduled_task is not None:
                    self._scheduled_notify_user_by_run_id[run_id] = str(
                        scheduled_task.get("notify_user") or "if_significant"
                    )
            self._register_worker_lineage(
                run_id=run_id,
                lineage_id=effective_lineage_id,
                spawn_depth=effective_spawn_depth,
                parent_worker_id=parent_worker_id,
            )
            logger.info(
                "Resolved worker timeout",
                worker_template_id=worker_id,
                run_id=run_id,
                timeout_seconds=resolved_timeout_seconds,
                timeout_source=timeout_meta.get("source"),
                timeout_reasons=timeout_meta.get("reasons"),
            )
            await self._emit_progress(
                chat_id,
                "queued",
                f"Queued worker '{worker_id}' as {run_id}.",
                {
                    "worker_id": run_id,
                    "worker_template_id": worker_id,
                    "lineage_id": effective_lineage_id,
                    "parent_worker_id": parent_worker_id,
                    "spawn_depth": effective_spawn_depth,
                },
            )
            await self._emit_worker_event(
                chat_id,
                "worker_queued",
                {
                    "run_id": run_id,
                    "worker_template_id": worker_id,
                    "task": task,
                    "lineage_id": effective_lineage_id,
                    "parent_worker_id": parent_worker_id,
                    "spawn_depth": effective_spawn_depth,
                    "timeout_seconds": resolved_timeout_seconds,
                },
            )
            task_request = TaskRequest(
                worker_id=worker_id,
                task=task,
                inputs=inputs or {},
                tools=tools,
                timeout_seconds=resolved_timeout_seconds,
                run_id=run_id,
                correlation_id=correlation_id,
                parent_worker_id=parent_worker_id,
                lineage_id=effective_lineage_id,
                root_task_id=effective_root_task_id,
                spawn_depth=effective_spawn_depth,
                allowed_paths=allowed_paths,
            )
            self.register_worker_correlation(run_id, correlation_id)
            self.register_worker_chat(run_id, chat_id)

            requester = self._approval_requesters.get(chat_id)
            if requester is None and getattr(self.approvals, "bot", None):

                async def _telegram_requester(intent: ActionIntent) -> bool:
                    return await self.approvals.request_approval(chat_id, intent)

                requester = _telegram_requester

            async def _runner() -> None:
                nonlocal dispatch_trace_output, dispatch_trace_status
                failed = False
                runner_trace_token = (
                    bind_trace_context(dispatch_trace_ctx)
                    if dispatch_trace_ctx is not None
                    else None
                )
                try:
                    await self._emit_progress(
                        chat_id,
                        "running",
                        f"Worker {run_id} is running.",
                        {"worker_id": run_id, "worker_template_id": worker_id},
                    )
                    await self._emit_worker_event(
                        chat_id,
                        "worker_running",
                        {
                            "run_id": run_id,
                            "worker_template_id": worker_id,
                            "task": task,
                        },
                    )
                    result = await self.runtime.run_task(task_request, approval_requester=requester)
                    worker_record = await asyncio.to_thread(self.store.get_worker, run_id)
                    worker_status = getattr(worker_record, "status", None)
                    normalized_result_status = (
                        str(getattr(result, "status", "completed") or "completed").strip().lower()
                    )
                    if worker_status is None and normalized_result_status == "failed":
                        worker_status = "failed"
                    failed = (
                        worker_status in {"failed", "stopped"}
                        or normalized_result_status == "failed"
                    )
                    if scheduled_task_id and self.scheduler:
                        if not failed:
                            self.scheduler.mark_executed(scheduled_task_id)
                            logger.info(
                                "Marked scheduled task as executed after worker completion",
                                task_id=scheduled_task_id,
                                run_id=run_id,
                                worker_status=worker_status,
                            )
                        else:
                            logger.warning(
                                "Skipped scheduled task execution mark due to non-completed worker state",
                                task_id=scheduled_task_id,
                                run_id=run_id,
                                worker_status=worker_status,
                            )
                    progress_state = "completed"
                    progress_text = f"Worker {run_id} completed."
                    if failed:
                        normalized_status = str(worker_status or "failed").strip().lower()
                        progress_state = "stopped" if normalized_status == "stopped" else "failed"
                        progress_text = f"Worker {run_id} {normalized_status}."
                    else:
                        self._register_progress(chat_id, "worker_completed")
                    dispatch_trace_output = {
                        "status": progress_state,
                        "worker_status": worker_status,
                        "result_status": normalized_result_status,
                        "summary_preview": safe_preview(
                            getattr(result, "summary", None), limit=240
                        ),
                        "summary_len": len(str(getattr(result, "summary", "") or "")),
                        "questions_count": len(getattr(result, "questions", []) or []),
                        "tools_used": list(getattr(result, "tools_used", []) or []),
                    }
                    if failed:
                        dispatch_trace_status = "error"
                    await self._emit_progress(
                        chat_id,
                        progress_state,
                        progress_text,
                        {
                            "worker_id": run_id,
                            "worker_template_id": worker_id,
                            "worker_status": worker_status,
                        },
                    )
                    await self._emit_worker_event(
                        chat_id,
                        "worker_finished" if not failed else "worker_failed",
                        {
                            "run_id": run_id,
                            "worker_template_id": worker_id,
                            "worker_status": worker_status,
                            "result_summary": getattr(result, "summary", None),
                            "worker": _serialize_worker_record(worker_record),
                        },
                    )
                except Exception as exc:
                    failed = True
                    result = WorkerResult(
                        summary=f"Worker error: {exc}", output={"error": str(exc)}
                    )
                    dispatch_trace_status = "error"
                    dispatch_trace_metadata.update(summarize_exception(exc))
                    dispatch_trace_output = {
                        "status": "failed",
                        "summary_preview": safe_preview(result.summary, limit=240),
                        "summary_len": len(result.summary),
                        "questions_count": 0,
                        "tools_used": [],
                    }
                    await self._emit_progress(
                        chat_id,
                        "failed",
                        f"Worker {run_id} failed: {exc}",
                        {"worker_id": run_id, "worker_template_id": worker_id},
                    )
                    await self._emit_worker_event(
                        chat_id,
                        "worker_failed",
                        {
                            "run_id": run_id,
                            "worker_template_id": worker_id,
                            "error": str(exc),
                        },
                    )
                finally:
                    self._release_recent_task(
                        chat_id=chat_id,
                        correlation_id=correlation_id,
                        task_signature=task_signature,
                    )
                    if dispatch_trace_ctx is not None and trace_sink is not None:
                        finish_meta = dict(dispatch_trace_metadata)
                        finish_meta["duration_ms"] = round(now_ms() - dispatch_started_at_ms, 2)
                        await trace_sink.finish_span(
                            dispatch_trace_ctx,
                            status=dispatch_trace_status,
                            output=dispatch_trace_output,
                            metadata=finish_meta,
                        )
                    if runner_trace_token is not None:
                        reset_trace_context(runner_trace_token)
                if failed:
                    await self._cleanup_orphan_children(
                        parent_run_id=run_id,
                        chat_id=chat_id,
                        reason="parent_failed",
                    )
                scheduled_notify_user = self._scheduled_notify_user_by_run_id.pop(run_id, None)
                self._mark_worker_inactive(run_id)
                _enqueue_internal_result(
                    self,
                    chat_id,
                    run_id,
                    task,
                    result,
                    correlation_id=task_request.correlation_id,
                    notify_user=scheduled_notify_user,
                )

            asyncio.create_task(_runner())
            await self._emit_progress(
                chat_id,
                "worker_started",
                f"Worker started: {run_id}",
                {
                    "worker_id": run_id,
                    "worker_template_id": worker_id,
                    "lineage_id": effective_lineage_id,
                    "parent_worker_id": parent_worker_id,
                    "spawn_depth": effective_spawn_depth,
                },
            )
            await self._emit_worker_event(
                chat_id,
                "worker_started",
                {
                    "run_id": run_id,
                    "worker_template_id": worker_id,
                    "task": task,
                    "lineage_id": effective_lineage_id,
                    "parent_worker_id": parent_worker_id,
                    "spawn_depth": effective_spawn_depth,
                    "timeout_seconds": resolved_timeout_seconds,
                },
            )
            dispatch_trace_output = {
                "status": "started",
                "run_id": run_id,
                "lineage_id": effective_lineage_id,
                "timeout_seconds": resolved_timeout_seconds,
            }
            return {
                "status": "started",
                "run_id": run_id,
                "worker_id": run_id,
                "lineage_id": effective_lineage_id,
                "parent_worker_id": parent_worker_id,
                "root_task_id": effective_root_task_id,
                "spawn_depth": effective_spawn_depth,
            }
        except Exception:
            self._release_recent_task(
                chat_id=chat_id,
                correlation_id=correlation_id,
                task_signature=task_signature,
            )
            dispatch_trace_status = "error"
            dispatch_trace_output = dispatch_trace_output or {"status": "failed"}
            if dispatch_trace_ctx is not None and trace_sink is not None:
                finish_meta = dict(dispatch_trace_metadata)
                finish_meta["duration_ms"] = round(now_ms() - dispatch_started_at_ms, 2)
                await trace_sink.finish_span(
                    dispatch_trace_ctx,
                    status=dispatch_trace_status,
                    output=dispatch_trace_output,
                    metadata=finish_meta,
                )
            raise

    def _check_child_spawn_limits(self, *, lineage_id: str | None, spawn_depth: int) -> str | None:
        limits = self._spawn_limits or {}
        max_depth = int(limits.get("max_depth", 0))
        max_children_total = int(limits.get("max_children_total", 1))
        max_children_concurrent = int(limits.get("max_children_concurrent", 1))

        if spawn_depth > max_depth:
            return f"spawn depth {spawn_depth} exceeds max depth {max_depth}"

        if not lineage_id:
            return None

        total_started = int((self._lineage_children_total or {}).get(lineage_id, 0))
        if total_started >= max_children_total:
            return (
                f"lineage child limit reached ({total_started}/{max_children_total}); "
                "cannot spawn more child workers"
            )

        active = len((self._lineage_children_active or {}).get(lineage_id, set()))
        if active >= max_children_concurrent:
            return (
                f"lineage concurrent child limit reached ({active}/{max_children_concurrent}); "
                "wait for running children to complete"
            )
        return None

    def _register_worker_lineage(
        self,
        *,
        run_id: str,
        lineage_id: str,
        spawn_depth: int,
        parent_worker_id: str | None,
    ) -> None:
        self._worker_lineage[run_id] = lineage_id
        self._worker_depth[run_id] = spawn_depth
        if parent_worker_id:
            self._worker_children.setdefault(parent_worker_id, set()).add(run_id)
            self._lineage_children_total[lineage_id] = (
                int(self._lineage_children_total.get(lineage_id, 0)) + 1
            )
            active = self._lineage_children_active.setdefault(lineage_id, set())
            active.add(run_id)

    def _mark_worker_inactive(self, run_id: str) -> None:
        correlation_id = self._worker_correlation_by_run_id.pop(run_id, None)
        self._worker_chat_by_run_id.pop(run_id, None)
        self._scheduled_notify_user_by_run_id.pop(run_id, None)
        if correlation_id:
            active_by_correlation = self._active_workers_by_correlation.get(correlation_id)
            if active_by_correlation and run_id in active_by_correlation:
                active_by_correlation.discard(run_id)
                if not active_by_correlation:
                    self._active_workers_by_correlation.pop(correlation_id, None)
        lineage_id = self._worker_lineage.get(run_id)
        if not lineage_id:
            return
        active = self._lineage_children_active.get(lineage_id)
        if active and run_id in active:
            active.discard(run_id)
            if not active:
                self._lineage_children_active.pop(lineage_id, None)

    async def _cleanup_orphan_children(
        self, *, parent_run_id: str, chat_id: int, reason: str
    ) -> None:
        child_ids = sorted(self._worker_children.get(parent_run_id, set()))
        if not child_ids:
            return
        for child_id in child_ids:
            stopped = await self.runtime.stop_worker(child_id)
            self._mark_worker_inactive(child_id)
            if stopped:
                await self._emit_progress(
                    chat_id,
                    "child_stopped",
                    f"Stopped orphan child worker {child_id} ({reason}).",
                    {"worker_id": child_id, "reason": reason, "parent_worker_id": parent_run_id},
                )

    async def _emit_progress(
        self,
        chat_id: int,
        state: str,
        text: str,
        meta: dict[str, Any] | None = None,
    ) -> None:
        sender = self.internal_progress_send
        if not sender:
            return
        try:
            await sender(chat_id, state, text, meta or {})
        except Exception:
            logger.debug("Progress emit failed", exc_info=True)

    async def _emit_worker_event(
        self,
        chat_id: int,
        event: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        sender = self.internal_worker_event_send
        if not sender:
            return
        try:
            await sender(chat_id, event, payload or {})
        except Exception:
            logger.debug("Worker event emit failed", exc_info=True)


def _normalize_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        items = [chunk.strip() for chunk in value.split("\n")]
    else:
        return []
    normalized: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            normalized.append(text)
    return normalized[:20]


def _serialize_worker_record(worker_record: Any) -> dict[str, Any] | None:
    if worker_record is None:
        return None
    if hasattr(worker_record, "model_dump"):
        try:
            return worker_record.model_dump(mode="json")
        except TypeError:
            return worker_record.model_dump()
    if isinstance(worker_record, dict):
        return dict(worker_record)
    return None


def _is_active_worker_status(status: Any) -> bool:
    return str(status or "").strip().lower() in {
        "started",
        "running",
        "waiting_for_children",
        "awaiting_instruction",
    }


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _coerce_int(value: Any, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    try:
        result = int(value)
    except Exception:
        result = default
    if minimum is not None:
        result = max(minimum, result)
    if maximum is not None:
        result = min(maximum, result)
    return result


def _watch_conditions(
    *,
    context_size_estimate: int,
    repetition_score: float,
    error_streak: int,
    no_progress_turns: int,
) -> list[bool]:
    return [
        context_size_estimate >= int(_WATCH_THRESHOLDS["context_size_estimate"]),
        repetition_score >= float(_WATCH_THRESHOLDS["repetition_score"]),
        error_streak >= int(_WATCH_THRESHOLDS["error_streak"]),
        no_progress_turns >= int(_WATCH_THRESHOLDS["no_progress_turns"]),
    ]


def _is_reset_soon_severe(
    *,
    context_size_estimate: int,
    repetition_score: float,
    error_streak: int,
    no_progress_turns: int,
) -> bool:
    return (
        context_size_estimate >= int(_RESET_SOON_THRESHOLDS["context_size_estimate"])
        or repetition_score >= float(_RESET_SOON_THRESHOLDS["repetition_score"])
        or error_streak >= int(_RESET_SOON_THRESHOLDS["error_streak"])
        or no_progress_turns >= int(_RESET_SOON_THRESHOLDS["no_progress_turns"])
    )


def _normalize_compact(value: str) -> str:
    lowered = (value or "").strip().lower()
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered


def _is_progress_reply(current_norm: str, prior_norm: str) -> bool:
    if not current_norm:
        return False
    if current_norm == prior_norm:
        return False
    if len(current_norm) < 24:
        return False
    stalled_markers = (
        "please try again",
        "i cannot",
        "i can't",
        "unable to",
        "still working on it",
        "no update",
    )
    return not any(marker in current_norm for marker in stalled_markers)


def _workspace_dir() -> Path:
    return Path(os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")).resolve()


def _build_opportunity_card(
    *,
    kind: str,
    title: str,
    why_now: str,
    impact: str,
    effort: str,
    confidence: float,
    next_action: str,
) -> dict[str, Any]:
    return {
        "opportunity_id": str(uuid4()),
        "kind": kind,
        "title": title,
        "why_now": why_now,
        "impact": impact,
        "effort": effort,
        "confidence": round(max(0.0, min(confidence, 1.0)), 2),
        "next_action": next_action,
        "created_at": utc_now().isoformat(),
    }


def _persist_self_queue(workspace_dir: Path, chat_id: int, queue: list[dict[str, Any]]) -> str:
    memory_dir = workspace_dir / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    path = memory_dir / f"self-queue-{chat_id}.json"
    path.write_text(json.dumps(queue, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _load_self_queue(workspace_dir: Path, chat_id: int) -> list[dict[str, Any]]:
    path = (workspace_dir / "memory" / f"self-queue-{chat_id}.json").resolve()
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    items: list[dict[str, Any]] = []
    for item in payload:
        if isinstance(item, dict) and str(item.get("task_id", "")).strip():
            items.append(dict(item))
    items.sort(key=lambda i: (-int(i.get("priority", 3) or 3), str(i.get("created_at", ""))))
    return items


def _persist_last_opportunities(
    workspace_dir: Path, chat_id: int, opportunities: list[dict[str, Any]]
) -> str:
    memory_dir = workspace_dir / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    path = memory_dir / f"opportunities-{chat_id}.json"
    path.write_text(json.dumps(opportunities, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _estimate_repetition_score(entries: list[Any]) -> float:
    if not entries:
        return 0.0
    sample = entries[:40]
    values = [_normalize_compact(getattr(entry, "content", "")) for entry in sample]
    values = [v for v in values if v]
    if not values:
        return 0.0
    unique = len(set(values))
    return max(0.0, min(1.0, 1.0 - (unique / max(1, len(values)))))


def _estimate_error_streak(entries: list[Any]) -> int:
    streak = 0
    for entry in entries[:20]:
        text = _normalize_compact(getattr(entry, "content", ""))
        if not text:
            continue
        if any(token in text for token in ("error", "failed", "exception", "unable", "timeout")):
            streak += 1
            continue
        break
    return streak


def _persist_context_reset_files(workspace_dir: Path, handoff: dict[str, Any]) -> dict[str, str]:
    memory_dir = workspace_dir / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    handoff_json_path = memory_dir / "handoff.json"
    handoff_md_path = memory_dir / "handoff.md"
    audit_md_path = memory_dir / "context-audit.md"
    audit_jsonl_path = memory_dir / "context-audit.jsonl"

    handoff_json_path.write_text(
        json.dumps(handoff, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    handoff_md_path.write_text(_render_handoff_markdown(handoff), encoding="utf-8")
    _append_context_audit_markdown(audit_md_path, handoff)
    with audit_jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(handoff, ensure_ascii=False) + "\n")

    return {
        "handoff_json": str(handoff_json_path),
        "handoff_md": str(handoff_md_path),
        "audit_md": str(audit_md_path),
        "audit_jsonl": str(audit_jsonl_path),
    }


def _render_handoff_markdown(handoff: dict[str, Any]) -> str:
    lines = [
        "# Octo Handoff",
        "",
        f"- created_at: {handoff.get('created_at', '')}",
        f"- mode: {handoff.get('mode', 'soft')}",
        f"- reason: {handoff.get('reason', '')}",
        f"- confidence: {handoff.get('confidence', 0.0)}",
        f"- cognitive_state: {handoff.get('cognitive_state', 'focused')}",
        "",
        "## Goal Now",
        handoff.get("goal_now", "") or "-",
        "",
        "## Next Step",
        handoff.get("next_step", "") or "-",
        "",
        "## Current Interest",
        handoff.get("current_interest", "") or "-",
        "",
        "## Pending Human Input",
        handoff.get("pending_human_input", "") or "-",
        "",
        "## Done",
    ]
    done = handoff.get("done") or []
    lines.extend([f"- {item}" for item in done] or ["-"])
    lines.extend(["", "## Open Threads"])
    open_threads = handoff.get("open_threads") or []
    lines.extend([f"- {item}" for item in open_threads] or ["-"])
    lines.extend(["", "## Critical Constraints"])
    constraints = handoff.get("critical_constraints") or []
    lines.extend([f"- {item}" for item in constraints] or ["-"])
    lines.extend(["", "## Health Snapshot"])
    health = handoff.get("health_snapshot") or {}
    for key in (
        "context_size_estimate",
        "repetition_score",
        "error_streak",
        "no_progress_turns",
        "resets_since_progress",
        "overload_score",
    ):
        lines.append(f"- {key}: {health.get(key, 0)}")
    return "\n".join(lines).strip() + "\n"


def _append_context_audit_markdown(path: Path, handoff: dict[str, Any]) -> None:
    timestamp = str(handoff.get("created_at", ""))
    mode = str(handoff.get("mode", "soft"))
    reason = str(handoff.get("reason", ""))
    confidence = str(handoff.get("confidence", ""))
    health = handoff.get("health_snapshot") or {}
    section = (
        f"\n## {timestamp} | mode={mode}\n"
        f"- reason: {reason}\n"
        f"- confidence: {confidence}\n"
        f"- context_size_estimate: {health.get('context_size_estimate', 0)}\n"
        f"- repetition_score: {health.get('repetition_score', 0)}\n"
        f"- no_progress_turns: {health.get('no_progress_turns', 0)}\n"
        f"- overload_score: {health.get('overload_score', 0)}\n"
    )
    existing = path.read_text(encoding="utf-8") if path.exists() else "# Context Reset Audit\n"
    path.write_text(existing.rstrip() + section + "\n", encoding="utf-8")


def _build_wakeup_message(handoff: dict[str, Any], handoff_path: str) -> str:
    goal_now = str(handoff.get("goal_now", "") or "").strip()
    next_step = str(handoff.get("next_step", "") or "").strip()
    return (
        "You woke up after a context reset.\n"
        f"Handoff goal: {goal_now}\n"
        f"Suggested next step: {next_step}\n"
        f"Handoff file: {handoff_path}\n"
        "Choose one mode now: continue / clarify / replan."
    )


def _build_restart_resume_message(resume: dict[str, Any]) -> str:
    handoff = resume.get("handoff") if isinstance(resume.get("handoff"), dict) else {}
    files = resume.get("files") if isinstance(resume.get("files"), dict) else {}
    update_status = resume.get("update") if isinstance(resume.get("update"), dict) else {}
    goal_now = str(handoff.get("goal_now", "") or "").strip()
    next_step = str(handoff.get("next_step", "") or "").strip()
    reason = str(handoff.get("reason", "") or "").strip()
    source = str(handoff.get("source", "") or "").strip()
    handoff_path = str(files.get("handoff_md", "") or "").strip()
    if source == "octo_update_self":
        return (
            "You woke up after a supervised self update and restart.\n"
            f"Update reason: {reason}\n"
            f"Version before update: {update_status.get('local_version') or 'unknown'}\n"
            f"Latest version seen before update: {update_status.get('latest_version') or 'unknown'}\n"
            f"Handoff goal: {goal_now}\n"
            f"Suggested next step: {next_step}\n"
            f"Handoff file: {handoff_path}\n"
            "Check runtime health and control acknowledgements, then tell the user whether update and restart completed."
        )
    return (
        "You woke up after a supervised self restart.\n"
        f"Restart reason: {reason}\n"
        f"Handoff goal: {goal_now}\n"
        f"Suggested next step: {next_step}\n"
        f"Handoff file: {handoff_path}\n"
        "Tell the user briefly that the restart completed, then continue, clarify, or replan."
    )


@dataclass
class OctoReply:
    immediate: str
    followup: asyncio.Task[str] | None
    followup_required: bool = False
    reaction: str | None = None
    delivery_mode: DeliveryMode = DeliveryMode.IMMEDIATE
