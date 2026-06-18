"""
Simplified Worker - Agent with tools and system prompt

Workers are pre-defined agents that:
- Have a system prompt defining their purpose
- Have access to specific tools
- Can reason and perform multi-step operations
- Can ask Octo questions when needed
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import random
import time
import traceback
from pathlib import Path
from typing import Any

import structlog

from octopal.infrastructure.config.settings import load_settings
from octopal.infrastructure.providers.base import InferenceProvider
from octopal.infrastructure.providers.factory import build_inference_provider
from octopal.runtime.temporal_context import format_temporal_context_prompt
from octopal.runtime.tool_errors import ToolBridgeError
from octopal.runtime.tool_loop import (
    _detect_tool_loop,
    _hash_tool_call,
    _hash_tool_outcome,
    _resolve_tool_loop_thresholds,
)
from octopal.runtime.tool_payloads import render_tool_result_for_llm
from octopal.runtime.workers.contracts import WorkerResult
from octopal.tools.registry import ToolPolicy, ToolPolicyPipelineStep, apply_tool_policy_pipeline
from octopal.tools.tools import get_tools
from octopal.worker_sdk.worker import Worker

_LOG_MAX_CHARS = 2000
_MAX_TOOL_ITERS = 10
_DEFAULT_TOOL_TIMEOUT_SECONDS = 60
_DEFAULT_MAX_STEP_CAP = 30
_MAX_EMPTY_TURNS = 3
_MAX_MALFORMED_RESULT_TURNS = 2
_ORCHESTRATION_STALL_WARNING_THRESHOLD = 2
_ORCHESTRATION_STALL_CRITICAL_THRESHOLD = 3
_ORCHESTRATION_STALL_WARNING_MIN_ELAPSED_SECONDS = 15
_ORCHESTRATION_STALL_CRITICAL_MIN_ELAPSED_SECONDS = 30
_ORCHESTRATION_POLL_THROTTLE_SECONDS = 3
_TRANSIENT_ERROR_HINTS = (
    "timeout",
    "timed out",
    "rate limit",
    "429",
    "500",
    "502",
    "503",
    "504",
    "connection",
    "temporarily",
    "unavailable",
    "service unavailable",
    "backend down",
    "bad gateway",
    "overloaded",
    "try again",
    "econnreset",
    "network",
)
_PERMANENT_ERROR_HINTS = (
    "permission",
    "denied",
    "unknown tool",
    "invalid",
    "validation",
    "required",
    "not found",
)
_UPSTREAM_UNAVAILABLE_HINTS = (
    "529",
    "500",
    "502",
    "503",
    "504",
    "overloaded",
    "overloaded_error",
    "high load",
    "service unavailable",
    "backend down",
    "bad gateway",
    "gateway timeout",
)
_SYSTEMIC_TOOL_ERROR_CLASSIFICATIONS = {"schema_mismatch"}
_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "result"},
        "status": {"type": "string", "enum": ["completed", "failed", "awaiting_instruction"]},
        "summary": {"type": "string"},
        "output": {"type": ["object", "array", "string", "number", "boolean", "null"]},
        "questions": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["type", "summary"],
    "allOf": [
        {
            "if": {
                "properties": {"status": {"const": "awaiting_instruction"}},
                "required": ["status"],
            },
            "then": {"required": ["questions"]},
            "else": {"required": ["output"]},
        }
    ],
    "additionalProperties": True,
}
logger = structlog.get_logger(__name__)
_WORKER_BASE_PROMPT_CONTENT = ""
_OCTO_PROXY_TOOLS = {
    "list_workers",
    "start_worker",
    "start_child_worker",
    "start_workers_parallel",
    "orchestration_plan_create",
    "orchestration_plan_status",
    "orchestration_plan_update_item",
    "synthesize_worker_results",
    "stop_worker",
    "get_worker_status",
    "list_active_workers",
    "answer_worker_instruction",
    "get_worker_result",
    "get_worker_output_path",
    "create_worker_template",
    "update_worker_template",
    "delete_worker_template",
}
_ORCHESTRATION_PROGRESS_TOOLS = {
    "get_worker_result",
    "synthesize_worker_results",
    "worker_yield",
    "request_instruction",
}
_CHILD_SPAWN_TOOLS = {
    "start_child_worker",
    "start_workers_parallel",
}
_ORCHESTRATION_PLAN_TOOLS = {
    "orchestration_plan_create",
    "orchestration_plan_status",
    "orchestration_plan_update_item",
}
_SKILL_TOOL_NAMES = {
    "list_skills",
    "run_skill_script",
    "use_skill",
}


def _parse_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _parse_nonnegative_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value >= 0 else default


def _parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = str(raw).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _required_tool_call_missing(
    required_tool_calls: list[str], tools_used: list[str], tool_name: str
) -> bool:
    normalized_required = {str(tool).strip().lower() for tool in required_tool_calls}
    normalized_used = {str(tool).strip().lower() for tool in tools_used}
    normalized_tool = str(tool_name).strip().lower()
    return normalized_tool in normalized_required and normalized_tool not in normalized_used


def _force_tool_choice(tool_name: str) -> dict[str, dict[str, str] | str]:
    return {"type": "function", "function": {"name": tool_name}}


def _build_worker_tool_inventory_prompt(tools: list[Any]) -> str:
    if not tools:
        return "- (none)"
    if _parse_bool_env("OCTOPAL_WORKER_PROMPT_TOOL_DESCRIPTIONS", False):
        return "\n".join(f"- {tool.name}: {tool.description}" for tool in tools)
    return "\n".join(f"- {tool.name}" for tool in tools)


def _load_worker_base_prompt() -> str:
    """Load shared worker guardrails used by every worker template."""

    global _WORKER_BASE_PROMPT_CONTENT

    if _WORKER_BASE_PROMPT_CONTENT:
        return _WORKER_BASE_PROMPT_CONTENT

    prompt_path = Path(__file__).parent.parent / "octo" / "prompts" / "worker_system.md"
    try:
        _WORKER_BASE_PROMPT_CONTENT = prompt_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        _WORKER_BASE_PROMPT_CONTENT = (
            "You are an Octopal Worker running one bounded task for Octo. "
            "Use only visible tools, stay within scope, and return verifiable results."
        )
    return _WORKER_BASE_PROMPT_CONTENT


def _tool_names(tools: list[Any]) -> set[str]:
    return {str(getattr(tool, "name", "")).strip() for tool in tools if getattr(tool, "name", "")}


def _build_worker_file_write_prompt(
    tools: list[Any], required_tool_calls: list[str] | None = None
) -> str:
    if "fs_write" not in _tool_names(tools):
        return ""
    required = {str(tool).strip().lower() for tool in required_tool_calls or []}
    if "fs_write" in required:
        return (
            "This task has an explicit required_tool_calls contract for fs_write. "
            "Call fs_write before returning a result, and do not claim completion until "
            "the fs_write tool returns successfully."
        )
    return (
        "If the task asks you to create, write, save, update, or edit a workspace file, "
        "you must call fs_write before returning a result. Do not claim a file was written "
        "until the fs_write tool returns successfully."
    )


def _build_worker_skill_usage_prompt(tools: list[Any]) -> str:
    names = _tool_names(tools)
    dynamic_skill_tools = sorted(name for name in names if name.startswith("skill_"))
    if not (names & _SKILL_TOOL_NAMES or dynamic_skill_tools):
        return ""

    lines = ["Skill usage:", "- Octopal skills are internal tools, not MCP servers."]
    if "list_skills" in names:
        lines.append(
            "- Use list_skills to discover available skills and their readiness/runtime status."
        )
    if "use_skill" in names:
        lines.append("- Use use_skill to read a skill's guidance from SKILL.md.")
    if dynamic_skill_tools:
        if "use_skill" in names:
            lines.append(
                "- Dynamic skill_<id> tools may exist for compatibility, but workers should prefer use_skill."
            )
        else:
            lines.append(
                "- Use available dynamic skill_<id> tools for their matching skill workflows."
            )
    if "run_skill_script" in names:
        lines.append("- If a skill includes bundled scripts, use run_skill_script to execute them.")
        if "exec_run" in names:
            lines.append(
                "- Do not use exec_run for scripts that belong to a skill bundle unless run_skill_script is unavailable."
            )
    return "\n".join(lines)


def _build_worker_task_prompt(task: str, inputs: Any) -> str:
    task_prompt = f"Task: {task}"
    if not inputs:
        return task_prompt
    try:
        inputs_json = json.dumps(inputs, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        inputs_json = str(inputs)
    return f"{task_prompt}\n\nInputs JSON: {inputs_json}"


def _build_worker_completion_protocol_prompt() -> str:
    return (
        "Completion protocol:\n"
        '- When done, return JSON with type="result", summary, and output/questions.\n'
        "- Put findings, records, paths, and domain results in output.\n"
        "- Use questions only when blocked after request_instruction times out or the task must stop.\n"
        "- summary is internal; do not treat it as user-facing copy.\n"
        "- Never present transport/debug/auth details, retries, truncation counts, or orchestration as user-facing."
    )


def _build_structured_result_retry_prompt() -> str:
    return (
        "Your previous response was not a valid worker completion result. "
        "Do not call any tools again. Return exactly one JSON object and nothing else. "
        "Required shape: "
        '{"type":"result","status":"completed","summary":"short internal summary",'
        '"output":{...},"questions":[]}. '
        "Put all task findings and produced data in output. If the previous response was "
        'itself the task result, wrap it in output. Use status="failed" only if the task '
        "could not be completed."
    )


def _completed_result_missing_output(result_block: dict[str, Any], tools_used: list[str]) -> bool:
    status = str(result_block.get("status") or "completed").strip().lower()
    if status == "failed" or not tools_used:
        return False
    if "output" not in result_block:
        return True
    output = result_block.get("output")
    if output is None:
        return True
    if isinstance(output, dict):
        return not any(not str(key).startswith("_") for key in output)
    if isinstance(output, str):
        return not output.strip()
    return False


def _tool_schema_chars(tools: list[Any]) -> int:
    try:
        payload = [
            {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.parameters,
                },
            }
            for tool in tools
        ]
        return len(json.dumps(payload, ensure_ascii=False, default=str))
    except Exception:
        return 0


def _message_chars(messages: list[dict[str, Any]]) -> int:
    try:
        return sum(
            len(json.dumps(message, ensure_ascii=False, default=str)) for message in messages
        )
    except Exception:
        return sum(len(str(message)) for message in messages)


def _value_chars(value: Any) -> int:
    try:
        return len(json.dumps(value, ensure_ascii=False, default=str))
    except Exception:
        return len(str(value))


def _record_worker_llm_context_snapshot(
    telemetry: dict[str, Any],
    *,
    messages: list[dict[str, Any]],
    tools: list[Any],
    step: int,
) -> None:
    context = telemetry.get("context")
    if not isinstance(context, dict):
        return
    message_chars = _message_chars(messages)
    tool_schema_chars = int(context.get("tool_schema_chars") or _tool_schema_chars(tools))
    input_chars = message_chars + tool_schema_chars
    context["llm_input_chars_total"] = int(context.get("llm_input_chars_total") or 0) + input_chars
    context["llm_input_chars_peak"] = max(
        int(context.get("llm_input_chars_peak") or 0), input_chars
    )
    context["message_count_peak"] = max(int(context.get("message_count_peak") or 0), len(messages))

    calls = context.get("llm_calls")
    if isinstance(calls, list) and len(calls) < 64:
        calls.append(
            {
                "step": step,
                "message_count": len(messages),
                "message_chars": message_chars,
                "tool_schema_chars": tool_schema_chars,
                "input_chars": input_chars,
            }
        )


def _record_worker_tool_result_context(
    telemetry: dict[str, Any],
    *,
    tool_name: str,
    raw_result: Any,
    rendered_text: str,
    was_compacted: bool,
) -> None:
    context = telemetry.get("context")
    if not isinstance(context, dict):
        return
    raw_chars = _value_chars(raw_result)
    rendered_chars = len(rendered_text or "")
    context["tool_result_raw_chars_total"] = (
        int(context.get("tool_result_raw_chars_total") or 0) + raw_chars
    )
    context["tool_result_rendered_chars_total"] = (
        int(context.get("tool_result_rendered_chars_total") or 0) + rendered_chars
    )
    if was_compacted and raw_chars > rendered_chars:
        context["tool_result_truncated_chars_total"] = int(
            context.get("tool_result_truncated_chars_total") or 0
        ) + (raw_chars - rendered_chars)
    by_tool = context.get("tool_result_rendered_chars_by_tool")
    if isinstance(by_tool, dict):
        key = tool_name or "<unknown>"
        by_tool[key] = int(by_tool.get(key) or 0) + rendered_chars


def _extract_tool_progress_key(tool_name: str | None, tool_result: Any) -> str | None:
    normalized_tool = str(tool_name or "").strip()
    structured = _decode_structured_tool_result(tool_result)
    if not isinstance(structured, dict):
        return None
    if normalized_tool == "synthesize_worker_results":
        progress_signature = str(structured.get("progress_signature") or "").strip()
        return progress_signature or None
    if normalized_tool == "get_worker_result":
        worker_id = str(structured.get("worker_id") or "").strip()
        status = str(structured.get("status") or "").strip().lower()
        updated_at = str(structured.get("updated_at") or "").strip()
        if not worker_id or not status:
            return None
        if updated_at:
            return f"{worker_id}:{status}:{updated_at}"
        summary = str(
            structured.get("summary") or structured.get("message") or structured.get("error") or ""
        ).strip()
        return f"{worker_id}:{status}:{summary}" if summary else f"{worker_id}:{status}"
    if normalized_tool == "worker_yield":
        pending_count = int(structured.get("pending_count") or 0)
        completed_count = int(structured.get("completed_count") or 0)
        failed_count = int(structured.get("failed_count") or 0)
        mode = str(structured.get("mode") or "").strip().lower()
        lineage_id = str(structured.get("lineage_id") or "").strip()
        pending_ids = ",".join(
            sorted(
                str(item.get("worker_id") or "").strip()
                for item in structured.get("pending_workers", [])
                if isinstance(item, dict) and str(item.get("worker_id") or "").strip()
            )
        )
        return (
            f"yield:{lineage_id}:{mode}:"
            f"p{pending_count}:c{completed_count}:f{failed_count}:{pending_ids}"
        )
    return None


def _tool_progress_streak(
    history: list[dict[str, Any]],
    *,
    tool_name: str,
    progress_key: str,
) -> dict[str, float | int]:
    streak = 0
    first_seen_at: float | None = None
    last_seen_at: float | None = None
    for record in reversed(history):
        if record.get("tool_name") != tool_name:
            continue
        if record.get("progress_key") != progress_key:
            break
        streak += 1
        observed_at = record.get("observed_at")
        if isinstance(observed_at, int | float):
            seen_at = float(observed_at)
            if last_seen_at is None:
                last_seen_at = seen_at
            first_seen_at = seen_at
    elapsed_seconds = 0.0
    if streak > 1 and first_seen_at is not None and last_seen_at is not None:
        elapsed_seconds = max(0.0, last_seen_at - first_seen_at)
    return {"count": streak, "elapsed_seconds": elapsed_seconds}


def _meaningful_tool_history_size(history: list[dict[str, Any]]) -> int:
    count = 0
    last_progress_by_call: dict[tuple[str, str], str] = {}
    for record in history:
        tool_name = str(record.get("tool_name") or "").strip()
        args_hash = str(record.get("args_hash") or "").strip()
        progress_key = str(record.get("progress_key") or "").strip()
        if tool_name in _ORCHESTRATION_PROGRESS_TOOLS and args_hash and progress_key:
            call_key = (tool_name, args_hash)
            if last_progress_by_call.get(call_key) == progress_key:
                continue
            last_progress_by_call[call_key] = progress_key
        count += 1
    return count


def _resolve_orchestration_poll_throttle_seconds() -> int:
    return _parse_nonnegative_int_env(
        "OCTOPAL_ORCHESTRATION_POLL_THROTTLE_SECONDS",
        _ORCHESTRATION_POLL_THROTTLE_SECONDS,
    )


async def _maybe_wait_for_orchestration_poll_window(
    worker: Worker,
    history: list[dict[str, Any]],
    *,
    tool_name: str | None,
    tool_input: dict[str, Any] | None,
) -> dict[str, Any]:
    normalized_tool = str(tool_name or "").strip()
    args = tool_input if isinstance(tool_input, dict) else {}
    args_hash = _hash_tool_call(normalized_tool, args)
    if normalized_tool != "get_worker_result":
        return {"step_exempt": False, "waited_seconds": 0.0, "args_hash": args_hash}
    throttle_seconds = _resolve_orchestration_poll_throttle_seconds()
    if throttle_seconds <= 0:
        return {"step_exempt": False, "waited_seconds": 0.0, "args_hash": args_hash}

    last_seen_at: float | None = None
    for record in reversed(history):
        if record.get("tool_name") != normalized_tool or record.get("args_hash") != args_hash:
            continue
        observed_at = record.get("observed_at")
        if isinstance(observed_at, int | float):
            last_seen_at = float(observed_at)
            break
    if last_seen_at is None:
        return {"step_exempt": False, "waited_seconds": 0.0, "args_hash": args_hash}

    elapsed_seconds = max(0.0, time.monotonic() - last_seen_at)
    remaining_seconds = float(throttle_seconds) - elapsed_seconds
    if remaining_seconds <= 0:
        return {"step_exempt": False, "waited_seconds": 0.0, "args_hash": args_hash}

    worker_id = str(args.get("worker_id") or "").strip()
    wait_seconds = max(0.0, remaining_seconds)
    await worker.log(
        "debug",
        (
            "Throttling get_worker_result poll "
            f"for {worker_id or args_hash[:8]} by {wait_seconds:.2f}s"
        ),
    )
    await asyncio.sleep(wait_seconds)
    return {
        "step_exempt": True,
        "waited_seconds": wait_seconds,
        "args_hash": args_hash,
    }


def _resolve_orchestration_stall_thresholds() -> dict[str, int]:
    warning = _parse_positive_int_env(
        "OCTOPAL_ORCHESTRATION_STALL_WARNING_SECONDS",
        _ORCHESTRATION_STALL_WARNING_MIN_ELAPSED_SECONDS,
    )
    critical = _parse_positive_int_env(
        "OCTOPAL_ORCHESTRATION_STALL_CRITICAL_SECONDS",
        _ORCHESTRATION_STALL_CRITICAL_MIN_ELAPSED_SECONDS,
    )
    if critical <= warning:
        critical = warning + 1
    return {"warning_seconds": warning, "critical_seconds": critical}


def _extract_spawned_worker_ids(tool_name: str | None, tool_result: Any) -> list[str]:
    structured = _decode_structured_tool_result(tool_result)
    if not isinstance(structured, dict):
        return []

    normalized_tool = str(tool_name or "").strip()
    worker_ids: list[str] = []
    if normalized_tool == "start_child_worker":
        status = str(structured.get("status", "") or "").strip().lower()
        worker_id = str(structured.get("run_id") or structured.get("worker_id") or "").strip()
        if status == "started" and worker_id:
            worker_ids.append(worker_id)
    elif normalized_tool == "start_workers_parallel":
        launches = structured.get("launches")
        if isinstance(launches, list):
            for item in launches:
                if not isinstance(item, dict):
                    continue
                status = str(item.get("status", "") or "").strip().lower()
                worker_id = str(item.get("run_id") or item.get("worker_id") or "").strip()
                if status == "started" and worker_id:
                    worker_ids.append(worker_id)
    return worker_ids


def _render_resumed_child_batch_message(child_batch: dict[str, Any]) -> str:
    worker_ids = [
        str(worker_id).strip()
        for worker_id in child_batch.get("worker_ids", [])
        if str(worker_id).strip()
    ]
    batch_status = str(child_batch.get("status") or "").strip()
    if batch_status == "awaiting_instruction":
        lead = (
            "Runtime child-batch resume: at least one child worker paused and is awaiting "
            "instruction. Answer it with answer_worker_instruction, then collect the result."
        )
    else:
        lead = (
            "Runtime child-batch resume: the workers started in the previous tool-call batch "
            "have now reached terminal states."
        )
    lines = [lead, f"Joined worker ids: {', '.join(worker_ids) if worker_ids else '<none>'}"]

    completed = child_batch.get("completed", [])
    failed = child_batch.get("failed", [])
    stopped = child_batch.get("stopped", [])
    missing = child_batch.get("missing", [])
    awaiting_instruction = child_batch.get("awaiting_instruction", [])

    if completed:
        lines.append("Completed workers:")
        for item in completed:
            summary = str(item.get("summary") or "").strip() or "No summary"
            lines.append(f"- {item.get('worker_id')}: {summary}")
    if failed:
        lines.append("Failed workers:")
        for item in failed:
            error = str(item.get("error") or item.get("summary") or "Unknown error").strip()
            lines.append(f"- {item.get('worker_id')}: {error}")
    if stopped:
        lines.append("Stopped workers:")
        for item in stopped:
            error = str(item.get("error") or item.get("summary") or "Stopped").strip()
            lines.append(f"- {item.get('worker_id')}: {error}")
    if missing:
        lines.append("Missing workers:")
        for item in missing:
            error = str(item.get("error") or "Missing worker record after spawn").strip()
            lines.append(f"- {item.get('worker_id')}: {error}")
    if awaiting_instruction:
        lines.append("Workers awaiting instruction:")
        for item in awaiting_instruction:
            output = item.get("output") if isinstance(item, dict) else {}
            request = output.get("instruction_request") if isinstance(output, dict) else {}
            question = (
                str(
                    request.get("question") or item.get("summary") or "Instruction requested"
                ).strip()
                if isinstance(request, dict)
                else str(item.get("summary") or "Instruction requested").strip()
            )
            request_id = (
                str(request.get("request_id") or "").strip() if isinstance(request, dict) else ""
            )
            suffix = f" request_id={request_id}" if request_id else ""
            lines.append(f"- {item.get('worker_id')}{suffix}: {question}")
    if not any((completed, failed, stopped, missing, awaiting_instruction)):
        lines.append("No child worker outcomes were collected.")

    lines.append(
        "Use these results directly in your next reasoning step. Do not re-poll the same child ids unless you are intentionally starting a new retry."
    )
    return "\n".join(lines)


def _record_joined_child_batch(
    joined_results_by_id: dict[str, dict[str, Any]],
    child_batch: dict[str, Any],
) -> None:
    for key in ("completed", "failed", "stopped", "missing"):
        items = child_batch.get(key, [])
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            worker_id = str(item.get("worker_id") or "").strip()
            if not worker_id:
                continue
            joined_results_by_id[worker_id] = dict(item)


def _build_worker_coordination_prompt(*, has_child_spawn_tools: bool) -> str:
    lines = [
        "Worker coordination:",
        (
            "- Use request_instruction when you are blocked on a concrete decision, missing "
            "input, or a scoped clarification. Prefer target=parent when your blocker belongs "
            "to a parent worker's delegated plan; use target=octo for top-level user or runtime decisions."
        ),
        (
            "- request_instruction pauses you in awaiting_instruction. While paused, your active "
            "timeout and thinking-step budget are not consumed; continue only after the runtime resumes you."
        ),
        (
            "- If request_instruction resumes with status=timed_out, make a conservative local "
            "decision or return a clear partial result instead of waiting forever."
        ),
    ]
    if has_child_spawn_tools:
        lines.extend(
            [
                "",
                "Parent-worker coordination:",
                (
                    "- For multi-child work, create a scoped orchestration plan before fan-out. "
                    "Bind each child launch with orchestration_item_id so runtime progress stays visible."
                ),
                (
                    "- You can start child workers for independent subtasks. After starting children, "
                    "the runtime pauses you until the child batch completes or a child asks for instruction."
                ),
                (
                    "- If a child pauses in awaiting_instruction, read its instruction_request and "
                    "answer it with answer_worker_instruction. The answer should be specific enough "
                    "for the child to continue without re-asking the same question."
                ),
                (
                    "- After you answer a child instruction, the runtime pauses you again until the "
                    "remaining children complete or another child asks for instruction."
                ),
                (
                    "- Do not poll children after a runtime child-batch resume unless you intentionally "
                    "start a retry; use the resume payload already placed in context."
                ),
            ]
        )
    return "\n".join(lines)


async def _await_child_batch_for_agent_loop(
    *,
    worker: Worker,
    worker_ids: list[str],
    messages: list[dict[str, Any]],
    telemetry: dict[str, Any],
    joined_results_by_id: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], float]:
    await worker.log(
        "info",
        "Suspending parent worker until child batch completes: " + ", ".join(worker_ids),
    )
    child_wait_started = time.perf_counter()
    child_batch = await worker.await_children(worker_ids)
    waited_seconds = time.perf_counter() - child_wait_started
    telemetry["paused_seconds"] = int(float(telemetry.get("paused_seconds", 0)) + waited_seconds)
    await worker.log(
        "info",
        "Resuming parent worker after child batch update: " + ", ".join(worker_ids),
    )
    messages.append(
        {
            "role": "user",
            "content": _render_resumed_child_batch_message(child_batch),
        }
    )
    _record_joined_child_batch(joined_results_by_id, child_batch)
    return child_batch, waited_seconds


def _build_joined_child_guardrail_result(
    *,
    worker_id: str,
    joined_result: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(joined_result)
    payload.setdefault("worker_id", worker_id)
    payload["joined_via_runtime"] = True
    payload["guardrail"] = "child_result_already_in_context"
    payload["message"] = (
        "Runtime already joined this child worker and injected the authoritative result into context. "
        "Reuse that result instead of polling again unless you are starting a new retry."
    )
    return payload


def _detect_orchestration_stall(
    history: list[dict[str, Any]],
    *,
    tool_name: str | None,
    tool_result: Any,
    progress_key: str | None,
) -> dict[str, Any] | None:
    if str(tool_name or "") != "synthesize_worker_results":
        return None
    structured = _decode_structured_tool_result(tool_result)
    if not progress_key or not isinstance(structured, dict):
        return None
    pending_count = int(structured.get("pending_count") or 0)
    if pending_count <= 0:
        return None
    streak = _tool_progress_streak(
        history,
        tool_name="synthesize_worker_results",
        progress_key=progress_key,
    )
    thresholds = _resolve_orchestration_stall_thresholds()
    count = int(streak["count"])
    elapsed_seconds = float(streak["elapsed_seconds"])
    if (
        count >= _ORCHESTRATION_STALL_CRITICAL_THRESHOLD
        and elapsed_seconds >= thresholds["critical_seconds"]
    ):
        return {
            "detector": "orchestration_no_progress",
            "level": "critical",
            "count": count,
            "elapsed_seconds": elapsed_seconds,
            "message": "Repeated synthesize_worker_results calls found no worker progress.",
        }
    if (
        count >= _ORCHESTRATION_STALL_WARNING_THRESHOLD
        and elapsed_seconds >= thresholds["warning_seconds"]
    ):
        return {
            "detector": "orchestration_no_progress",
            "level": "warning",
            "count": count,
            "elapsed_seconds": elapsed_seconds,
            "message": "synthesize_worker_results is being retried without worker progress.",
        }
    return None


async def run_agent_worker(spec_path: str) -> None:
    """Main entry point for simplified agent worker."""
    from octopal.infrastructure.logging import correlation_id_var
    from octopal.tools.ops.exec_run import cleanup_background_sessions

    worker = Worker.from_spec_file(spec_path)
    worker_dir = Path(spec_path).parent
    workspace_env = os.getenv("OCTOPAL_WORKSPACE_DIR", "").strip()
    workspace_root = Path(workspace_env) if workspace_env else worker_dir

    # Set the correlation ID for this worker's context
    if worker.spec.correlation_id:
        correlation_id_var.set(worker.spec.correlation_id)

    await worker.log(
        "info",
        f"AgentWorker start: id={worker.spec.id} run_id={worker.spec.run_id}",
    )
    await worker.log(
        "info",
        (
            "AgentWorker context: "
            f"cwd={Path.cwd()} "
            f"workspace={workspace_env or '<unset>'} "
            f"worker_dir={worker_dir} "
            f"tools={list(worker.spec.available_tools or [])}"
        ),
    )

    try:
        result = await execute_agent_task(worker, workspace_root, worker_dir)
        await worker.complete(result)
    except Exception as exc:
        error_text = str(exc)
        await worker.log("error", f"AgentWorker failed: id={worker.spec.id} error={error_text}")
        await worker.complete(
            WorkerResult(
                status="failed",
                summary=f"Worker failed: {error_text}",
                output={
                    "error": error_text,
                    "traceback": _truncate_text(traceback.format_exc(), 4000),
                },
            )
        )
    finally:
        cleanup_background_sessions()


async def execute_agent_task(
    worker: Worker, workspace_root: Path, worker_dir: Path
) -> WorkerResult:
    """Execute the agent's task with tools."""
    spec = worker.spec

    # Initialize LLM provider from settings
    settings = load_settings()
    provider = build_inference_provider(settings, model=spec.model, config=spec.llm_config)

    # Build system prompt with tool descriptions
    available_tools = get_tools()
    # Filter tools by name from worker spec
    filtered_tools = apply_tool_policy_pipeline(
        available_tools,
        [
            ToolPolicyPipelineStep(
                label="worker.octo_only_tool_denylist",
                policy=ToolPolicy(
                    deny=[
                        "send_file_to_user",
                        "self_control",
                        "octo_restart_self",
                        "octo_check_update",
                        "octo_update_self",
                    ]
                ),
            ),
            ToolPolicyPipelineStep(
                label="worker.available_tools",
                policy=ToolPolicy(allow=list(spec.available_tools or [])),
            ),
        ],
    )
    filtered_tools = _with_octo_tool_proxies(filtered_tools, worker)
    has_child_spawn_tools = any(
        getattr(tool, "name", "") in _CHILD_SPAWN_TOOLS for tool in filtered_tools
    )
    if not has_child_spawn_tools:
        filtered_tools = [
            tool
            for tool in filtered_tools
            if getattr(tool, "name", "") != "answer_worker_instruction"
        ]
    if has_child_spawn_tools and not any(
        getattr(tool, "name", "") == "answer_worker_instruction" for tool in filtered_tools
    ):
        answer_tool = next(
            (
                tool
                for tool in available_tools
                if getattr(tool, "name", "") == "answer_worker_instruction"
            ),
            None,
        )
        if answer_tool is not None:
            filtered_tools.append(_make_octo_proxy_tool(answer_tool, worker))
    if has_child_spawn_tools:
        existing_tool_names = {getattr(tool, "name", "") for tool in filtered_tools}
        for plan_tool_name in sorted(_ORCHESTRATION_PLAN_TOOLS):
            if plan_tool_name in existing_tool_names:
                continue
            plan_tool = next(
                (tool for tool in available_tools if getattr(tool, "name", "") == plan_tool_name),
                None,
            )
            if plan_tool is not None:
                filtered_tools.append(_make_octo_proxy_tool(plan_tool, worker))
    filtered_tools.append(_make_request_instruction_tool(worker))

    # Add MCP tools from spec
    from octopal.tools.registry import ToolSpec

    for mcp_tool_data in spec.mcp_tools:
        # Generate a proxy handler for this MCP tool.
        identity = _extract_mcp_identity(mcp_tool_data)
        if identity is None:
            await worker.log(
                "warning",
                f"Skipping MCP tool with invalid identity: {mcp_tool_data.get('name', '<unknown>')}",
            )
            continue
        s_id, t_name = identity

        async def mcp_proxy_handler(args: dict, ctx: dict, s_id=s_id, t_name=t_name):
            w = ctx.get("worker")
            return await w.call_mcp_tool(s_id, t_name, args)

        mcp_spec = ToolSpec(
            name=mcp_tool_data["name"],
            description=mcp_tool_data["description"],
            parameters=mcp_tool_data["parameters"],
            permission=mcp_tool_data["permission"],
            handler=mcp_proxy_handler,
            is_async=True,
        )
        filtered_tools.append(mcp_spec)

    tool_inventory = _build_worker_tool_inventory_prompt(filtered_tools)
    coordination_prompt = _build_worker_coordination_prompt(
        has_child_spawn_tools=has_child_spawn_tools
    )
    guidance_prompt = "\n\n".join(
        part
        for part in (
            "Use available tools through normal tool calls. Do not emit ad-hoc JSON tool_use blocks.",
            _build_worker_file_write_prompt(filtered_tools, spec.required_tool_calls),
            coordination_prompt,
            _build_worker_skill_usage_prompt(filtered_tools),
        )
        if part
    )

    temporal_context_prompt = format_temporal_context_prompt()

    worker_base_prompt = _load_worker_base_prompt()

    system_prompt = f"""{worker_base_prompt}

Template role:
{spec.system_prompt}

{temporal_context_prompt}

Available tools:
{tool_inventory}

{guidance_prompt}

{_build_worker_completion_protocol_prompt()}
"""

    task_prompt = _build_worker_task_prompt(spec.task, spec.inputs)
    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": task_prompt,
        },
    ]

    tools_used = []
    thinking_steps = 0
    empty_turns = 0
    tool_map = {t.name: t for t in filtered_tools}
    loop_start = asyncio.get_running_loop().time()
    effective_max_steps = _auto_tune_max_steps(
        spec.max_thinking_steps, spec.available_tools, spec.system_prompt
    )
    telemetry: dict[str, Any] = {
        "max_thinking_steps_configured": spec.max_thinking_steps,
        "max_thinking_steps_effective": effective_max_steps,
        "llm_calls": 0,
        "llm_latency_ms_total": 0,
        "tool_calls": 0,
        "tool_latency_ms_total": 0,
        "tool_retries": 0,
        "tool_timeouts": 0,
        "tool_errors": 0,
        "tool_result_truncations": 0,
        "empty_turns": 0,
        "paused_seconds": 0,
        "tokens": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        "context": {
            "system_prompt_chars": len(system_prompt),
            "task_prompt_chars": len(task_prompt),
            "tool_count": len(filtered_tools),
            "tool_schema_chars": _tool_schema_chars(filtered_tools),
            "llm_input_chars_total": 0,
            "llm_input_chars_peak": 0,
            "message_count_peak": len(messages),
            "tool_result_raw_chars_total": 0,
            "tool_result_rendered_chars_total": 0,
            "tool_result_rendered_chars_by_tool": {},
            "tool_result_truncated_chars_total": 0,
            "llm_calls": [],
        },
    }
    upstream_failures: dict[str, int] = {}
    successful_tool_calls = 0
    tool_call_history: list[dict[str, Any]] = []
    tool_loop_thresholds = _resolve_tool_loop_thresholds()
    joined_child_results_by_id: dict[str, dict[str, Any]] = {}
    pending_child_wait_ids: list[str] = []
    paused_seconds = 0.0
    malformed_result_turns = 0

    while thinking_steps < effective_max_steps:
        llm_start = time.perf_counter()
        force_structured_result = malformed_result_turns > 0
        llm_tools = [] if force_structured_result else filtered_tools
        _record_worker_llm_context_snapshot(
            telemetry,
            messages=messages,
            tools=llm_tools,
            step=thinking_steps,
        )
        force_fs_write = not force_structured_result and _required_tool_call_missing(
            spec.required_tool_calls,
            tools_used,
            "fs_write",
        )
        try:
            response = await _call_llm(
                provider,
                messages,
                llm_tools,
                tool_choice=_force_tool_choice("fs_write") if force_fs_write else "auto",
                response_format_enabled=not force_fs_write,
            )
        except Exception as exc:
            telemetry["llm_latency_ms_total"] += int((time.perf_counter() - llm_start) * 1000)
            error_text = str(exc)
            if _is_upstream_unavailable_error(error_text):
                return _build_inference_unavailable_result(
                    worker=worker,
                    telemetry=telemetry,
                    error_text=error_text,
                    thinking_steps=thinking_steps,
                    tools_used=tools_used,
                    partial=successful_tool_calls > 0 or bool(tools_used),
                )
            raise
        telemetry["llm_calls"] += 1
        telemetry["llm_latency_ms_total"] += int((time.perf_counter() - llm_start) * 1000)
        usage = response.get("usage") or {}
        if isinstance(usage, dict):
            for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
                value = usage.get(key)
                if isinstance(value, int | float):
                    telemetry["tokens"][key] += int(value)
        await worker.log("debug", f"LLM response: {response}")

        # Handle OpenAI-style tool_calls
        tool_calls = response.get("tool_calls", [])
        if tool_calls:
            content = response.get("content", "")
            assistant_msg: dict[str, Any] = {"role": "assistant", "tool_calls": tool_calls}
            if content:
                assistant_msg["content"] = content
            messages.append(assistant_msg)
            round_consumes_step = False
            spawned_child_ids: list[str] = []
            answered_instruction_this_round = False

            # Process tool calls
            for tool_call in tool_calls:
                function = tool_call.get("function", {})
                tool_name = function.get("name")
                tool_input = _parse_tool_arguments(function.get("arguments", "{}"))
                tool_call_id = tool_call.get("id", "") or ""

                await worker.log("info", f"Using tool: {tool_name}")

                poll_window = await _maybe_wait_for_orchestration_poll_window(
                    worker,
                    tool_call_history,
                    tool_name=tool_name,
                    tool_input=tool_input,
                )
                step_exempt = bool(poll_window.get("step_exempt"))

                normalized_tool_name = str(tool_name or "").strip()
                joined_worker_id = ""
                joined_result: dict[str, Any] | None = None
                if normalized_tool_name == "get_worker_result":
                    joined_worker_id = str(tool_input.get("worker_id") or "").strip()
                    if joined_worker_id:
                        joined_result = joined_child_results_by_id.get(joined_worker_id)

                if joined_result is not None and joined_worker_id:
                    tool_result = _build_joined_child_guardrail_result(
                        worker_id=joined_worker_id,
                        joined_result=joined_result,
                    )
                    tool_meta = {
                        "retries": 0,
                        "timed_out": False,
                        "had_error": False,
                        "error_type": "none",
                        "guardrail": True,
                    }
                    step_exempt = True
                    await worker.log(
                        "info",
                        (
                            "Skipping redundant get_worker_result for already-joined child worker: "
                            f"{joined_worker_id}"
                        ),
                    )
                else:
                    # Execute tool
                    elapsed = asyncio.get_running_loop().time() - loop_start - paused_seconds
                    remaining_budget = max(1, spec.timeout_seconds - int(elapsed))
                    tool_timeout = min(_DEFAULT_TOOL_TIMEOUT_SECONDS, remaining_budget)
                    if normalized_tool_name in {
                        "request_instruction",
                        "answer_worker_instruction",
                    }:
                        step_exempt = True
                    if normalized_tool_name == "request_instruction":
                        requested_wait = max(1, int(tool_input.get("timeout_seconds") or 120))
                        tool_timeout = requested_wait + 5
                    tool_start = time.perf_counter()
                    tool_result, tool_meta = await _execute_tool(
                        tool_name,
                        tool_input,
                        workspace_root,
                        worker_dir,
                        worker,
                        tool_map,
                        timeout_seconds=tool_timeout,
                    )
                    telemetry["tool_calls"] += 1
                    tool_elapsed = time.perf_counter() - tool_start
                    telemetry["tool_latency_ms_total"] += int(tool_elapsed * 1000)
                    if normalized_tool_name == "request_instruction":
                        paused_seconds += tool_elapsed
                        telemetry["paused_seconds"] = int(paused_seconds)
                    telemetry["tool_retries"] += int(tool_meta.get("retries", 0))
                    if tool_meta.get("timed_out"):
                        telemetry["tool_timeouts"] += 1
                    if tool_meta.get("had_error"):
                        telemetry["tool_errors"] += 1
                    else:
                        successful_tool_calls += 1
                tools_used.append(tool_name)
                args_hash = str(
                    poll_window.get("args_hash")
                    or _hash_tool_call(str(tool_name or ""), tool_input)
                )
                result_hash = _hash_tool_outcome(tool_result, tool_meta)
                progress_key = _extract_tool_progress_key(tool_name, tool_result)
                tool_call_history.append(
                    {
                        "tool_name": str(tool_name or ""),
                        "args_hash": args_hash,
                        "result_hash": result_hash,
                        "progress_key": progress_key,
                        "observed_at": time.monotonic(),
                        "step_exempt": step_exempt,
                    }
                )
                if not step_exempt:
                    round_consumes_step = True
                if normalized_tool_name == "answer_worker_instruction" and not tool_meta.get(
                    "had_error"
                ):
                    answered_instruction_this_round = True
                loop_state = _detect_tool_loop(
                    tool_call_history,
                    tool_name=str(tool_name or ""),
                    args_hash=args_hash,
                    warning_threshold=tool_loop_thresholds["warning"],
                    critical_threshold=tool_loop_thresholds["critical"],
                    global_breaker_threshold=tool_loop_thresholds["global_breaker"],
                    global_breaker_count=_meaningful_tool_history_size(tool_call_history),
                )
                if loop_state is None:
                    loop_state = _detect_orchestration_stall(
                        tool_call_history,
                        tool_name=tool_name,
                        tool_result=tool_result,
                        progress_key=progress_key,
                    )
                if loop_state is not None:
                    if loop_state["level"] == "warning":
                        await worker.log(
                            "warning",
                            (
                                f"Tool loop warning ({loop_state['detector']}): "
                                f"{loop_state['message']} count={loop_state['count']}"
                            ),
                        )
                    else:
                        await worker.log(
                            "warning",
                            (
                                f"Tool loop breaker ({loop_state['detector']}): "
                                f"{loop_state['message']} count={loop_state['count']}"
                            ),
                        )
                        return WorkerResult(
                            summary=(
                                "Task stopped to prevent an infinite tool loop. "
                                "Please refine the task or provide additional constraints."
                            ),
                            output=_attach_telemetry(
                                {
                                    "degraded": True,
                                    "reason": "tool_loop_detected",
                                    "loop": loop_state,
                                },
                                telemetry,
                            ),
                            knowledge_proposals=worker.knowledge_proposals,
                            thinking_steps=thinking_steps + (1 if round_consumes_step else 0),
                            tools_used=tools_used,
                        )

                if tool_meta.get("had_error"):
                    error_text = _extract_error_text(tool_result)
                    if _is_systemic_tool_bridge_failure(tool_meta):
                        return WorkerResult(
                            status="failed",
                            summary="Task failed: remote MCP tool response schema is incompatible.",
                            output=_attach_telemetry(
                                {
                                    "degraded": True,
                                    "reason": "mcp_schema_mismatch",
                                    "failed_tool": tool_name,
                                    "bridge": tool_meta.get("error_bridge"),
                                    "error_classification": tool_meta.get("error_classification"),
                                    "error": _truncate_text(error_text, 500),
                                },
                                telemetry,
                            ),
                            knowledge_proposals=worker.knowledge_proposals,
                            thinking_steps=thinking_steps + (1 if round_consumes_step else 0),
                            tools_used=tools_used,
                        )
                    if _is_upstream_unavailable_error(error_text):
                        signature = f"{tool_name}:{_upstream_error_bucket(error_text)}"
                        upstream_failures[signature] = upstream_failures.get(signature, 0) + 1
                        if upstream_failures[signature] >= 2 and successful_tool_calls == 0:
                            return WorkerResult(
                                summary=(
                                    "Task partially completed with degraded state: "
                                    "upstream service is currently unavailable."
                                ),
                                output=_attach_telemetry(
                                    {
                                        "degraded": True,
                                        "reason": "upstream_unavailable",
                                        "failed_tool": tool_name,
                                        "error": _truncate_text(error_text, 500),
                                    },
                                    telemetry,
                                ),
                                knowledge_proposals=worker.knowledge_proposals,
                                thinking_steps=thinking_steps + (1 if round_consumes_step else 0),
                                tools_used=tools_used,
                            )

                # Add tool result message
                rendered_tool_result = render_tool_result_for_llm(
                    tool_result,
                    tool_name=str(tool_name or ""),
                )
                if rendered_tool_result.was_compacted:
                    telemetry["tool_result_truncations"] += 1
                _record_worker_tool_result_context(
                    telemetry,
                    tool_name=str(tool_name or ""),
                    raw_result=tool_result,
                    rendered_text=rendered_tool_result.text,
                    was_compacted=rendered_tool_result.was_compacted,
                )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call_id,
                        "content": rendered_tool_result.text,
                    }
                )
                if str(tool_name or "") in _CHILD_SPAWN_TOOLS:
                    spawned_child_ids.extend(_extract_spawned_worker_ids(tool_name, tool_result))
            if spawned_child_ids:
                joined_worker_ids = list(
                    dict.fromkeys(
                        str(worker_id).strip()
                        for worker_id in spawned_child_ids
                        if str(worker_id).strip()
                    )
                )
                if joined_worker_ids:
                    child_batch, waited_seconds = await _await_child_batch_for_agent_loop(
                        worker=worker,
                        worker_ids=joined_worker_ids,
                        messages=messages,
                        telemetry=telemetry,
                        joined_results_by_id=joined_child_results_by_id,
                    )
                    paused_seconds += waited_seconds
                    telemetry["paused_seconds"] = int(paused_seconds)
                    pending_child_wait_ids = (
                        joined_worker_ids
                        if str(child_batch.get("status") or "") == "awaiting_instruction"
                        else []
                    )
            if answered_instruction_this_round and pending_child_wait_ids:
                child_batch, waited_seconds = await _await_child_batch_for_agent_loop(
                    worker=worker,
                    worker_ids=pending_child_wait_ids,
                    messages=messages,
                    telemetry=telemetry,
                    joined_results_by_id=joined_child_results_by_id,
                )
                paused_seconds += waited_seconds
                telemetry["paused_seconds"] = int(paused_seconds)
                if str(child_batch.get("status") or "") != "awaiting_instruction":
                    pending_child_wait_ids = []
            if round_consumes_step:
                thinking_steps += 1
            empty_turns = 0
            telemetry["empty_turns"] = empty_turns
        else:
            # No tool calls, check if this is a completion
            content = str(response.get("content", "") or "").strip()

            # Try to parse structured JSON result, including fenced JSON blocks.
            result_block = _extract_result_block(content)
            if result_block is not None:
                if _required_tool_call_missing(spec.required_tool_calls, tools_used, "fs_write"):
                    messages.append({"role": "assistant", "content": content})
                    messages.append(
                        {
                            "role": "user",
                            "content": (
                                "The task requires an actual fs_write tool call before completion. "
                                "Call fs_write with the requested path and content now, then return "
                                "the structured result only after fs_write succeeds."
                            ),
                        }
                    )
                    thinking_steps += 1
                    continue
                if _completed_result_missing_output(result_block, tools_used):
                    malformed_result_turns += 1
                    telemetry["malformed_result_turns"] = malformed_result_turns
                    messages.append({"role": "assistant", "content": content})
                    if (
                        malformed_result_turns <= _MAX_MALFORMED_RESULT_TURNS
                        and thinking_steps + 1 < effective_max_steps
                    ):
                        await worker.log(
                            "warning",
                            (
                                "Worker returned structured completion without output; "
                                "requesting JSON result"
                            ),
                        )
                        messages.append(
                            {
                                "role": "user",
                                "content": _build_structured_result_retry_prompt(),
                            }
                        )
                        thinking_steps += 1
                        continue
                    return WorkerResult(
                        status="failed",
                        summary="Task failed: worker did not return structured output.",
                        output=_attach_telemetry(
                            {
                                "degraded": True,
                                "reason": "missing_structured_output",
                                "final_text": _truncate_text(content, 1000),
                                "malformed_result_turns": malformed_result_turns,
                            },
                            telemetry,
                        ),
                        knowledge_proposals=worker.knowledge_proposals,
                        thinking_steps=thinking_steps + 1,
                        tools_used=tools_used,
                    )
                cycle_steps = thinking_steps + 1
                return WorkerResult(
                    status=(
                        str(result_block.get("status", "completed"))
                        if result_block.get("status") in {"completed", "failed"}
                        else "completed"
                    ),
                    summary=str(result_block.get("summary", "Task completed")).strip()
                    or "Task completed",
                    output=_attach_telemetry(result_block.get("output"), telemetry),
                    questions=result_block.get("questions", []),
                    knowledge_proposals=worker.knowledge_proposals,
                    thinking_steps=cycle_steps,
                    tools_used=tools_used,
                )

            # Plain text is not a valid worker completion. Retry once in result-only
            # mode so side-effecting tools are not called again, then fail loudly
            # instead of recording a telemetry-only "success".
            if content:
                if _required_tool_call_missing(spec.required_tool_calls, tools_used, "fs_write"):
                    messages.append({"role": "assistant", "content": content})
                    messages.append(
                        {
                            "role": "user",
                            "content": (
                                "The task requires an actual fs_write tool call before completion. "
                                "Call fs_write with the requested path and content now, then return "
                                "the final answer only after fs_write succeeds."
                            ),
                        }
                    )
                    thinking_steps += 1
                    continue
                malformed_result_turns += 1
                telemetry["malformed_result_turns"] = malformed_result_turns
                messages.append({"role": "assistant", "content": content})
                if (
                    malformed_result_turns <= _MAX_MALFORMED_RESULT_TURNS
                    and thinking_steps + 1 < effective_max_steps
                ):
                    await worker.log(
                        "warning",
                        "Worker returned non-structured completion; requesting JSON result",
                    )
                    messages.append(
                        {
                            "role": "user",
                            "content": _build_structured_result_retry_prompt(),
                        }
                    )
                    thinking_steps += 1
                    continue

                return WorkerResult(
                    status="failed",
                    summary="Task failed: worker did not return a structured result.",
                    output=_attach_telemetry(
                        {
                            "degraded": True,
                            "reason": "missing_structured_result",
                            "final_text": _truncate_text(content, 1000),
                            "malformed_result_turns": malformed_result_turns,
                        },
                        telemetry,
                    ),
                    knowledge_proposals=worker.knowledge_proposals,
                    thinking_steps=thinking_steps + 1,
                    tools_used=tools_used,
                )

            # Empty LLM turns do not consume the worker's thinking budget, but they
            # still count toward a separate no-progress guard to prevent loops.
            empty_turns += 1
            telemetry["empty_turns"] = empty_turns
            if empty_turns >= _MAX_EMPTY_TURNS:
                return WorkerResult(
                    summary=f"Task stopped after {empty_turns} empty turns without progress",
                    output=_attach_telemetry(
                        {"degraded": True, "reason": "empty_turn_limit"},
                        telemetry,
                    ),
                    knowledge_proposals=worker.knowledge_proposals,
                    thinking_steps=thinking_steps,
                    tools_used=tools_used,
                )
            continue

    # Max iterations reached without completion
    return WorkerResult(
        summary=f"Task incomplete after {thinking_steps} thinking steps",
        output=_attach_telemetry(None, telemetry),
        knowledge_proposals=worker.knowledge_proposals,
        thinking_steps=thinking_steps,
        tools_used=tools_used,
    )


def _extract_result_block(content: str) -> dict[str, Any] | None:
    if not content:
        return None

    candidates = [content]
    legacy_output_candidates: list[str] = []
    stripped = content.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3 and lines[-1].strip() == "```":
            body = "\n".join(lines[1:-1]).strip()
            candidates.append(body)
            legacy_output_candidates.append(body)
    legacy_output_candidates.extend(_extract_fenced_json_candidates(stripped))

    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except (json.JSONDecodeError, TypeError):
            continue
        normalized = _normalize_result_payload(payload)
        if normalized is not None:
            return normalized

    for payload in _iter_embedded_json_objects(stripped):
        normalized = _normalize_result_payload(payload)
        if normalized is not None:
            return normalized

    if stripped.startswith(("{", "[")):
        legacy_output_candidates.append(stripped)
    for candidate in legacy_output_candidates:
        try:
            payload = json.loads(candidate)
        except (json.JSONDecodeError, TypeError):
            continue
        normalized = _normalize_legacy_output_payload(payload)
        if normalized is not None:
            return normalized
    return None


def _extract_fenced_json_candidates(content: str) -> list[str]:
    candidates: list[str] = []
    lines = content.splitlines()
    index = 0
    while index < len(lines):
        opener = lines[index].strip().lower()
        if not opener.startswith("```"):
            index += 1
            continue
        language = opener[3:].strip()
        body_start = index + 1
        index = body_start
        while index < len(lines) and lines[index].strip() != "```":
            index += 1
        if index >= len(lines):
            break
        body = "\n".join(lines[body_start:index]).strip()
        if body and (not language or language == "json"):
            candidates.append(body)
        index += 1
    return candidates


def _iter_embedded_json_objects(content: str):
    decoder = json.JSONDecoder()
    for index, char in enumerate(content):
        if char != "{":
            continue
        try:
            payload, _end = decoder.raw_decode(content[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            yield payload


def _normalize_result_payload(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("type") != "result" and "summary" not in payload:
        return None

    normalized = dict(payload)
    normalized.setdefault("type", "result")
    status = str(normalized.get("status") or "").strip().lower()
    if status in {"error", "failure"} or (
        "status" not in normalized and _result_payload_indicates_failure(normalized)
    ):
        normalized["status"] = "failed"

    if _is_valid_result_payload(normalized):
        return normalized
    return None


def _normalize_legacy_output_payload(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        if payload.get("type") == "result":
            return _normalize_result_payload(payload)
        if not payload:
            return None
        return {
            "type": "result",
            "summary": "Task completed",
            "output": payload,
        }
    if isinstance(payload, list):
        return {
            "type": "result",
            "summary": "Task completed",
            "output": {"result": payload},
        }
    return None


def _result_payload_indicates_failure(payload: dict[str, Any]) -> bool:
    output = payload.get("output")
    if isinstance(output, dict):
        if str(output.get("error") or "").strip():
            return True
        status = str(output.get("status") or "").strip().lower()
        if status in {"error", "failed", "failure"}:
            return True
    return str(payload.get("error") or "").strip() != ""


async def _call_llm(
    provider: InferenceProvider,
    messages: list[dict],
    tools: list,
    *,
    tool_choice: object = "auto",
    response_format_enabled: bool = True,
) -> dict:
    """Call LLM with tools using the centralized provider."""
    # Build OpenAI-style tools format
    openai_tools = [
        {
            "type": "function",
            "function": {
                "name": t.name,
                "description": t.description,
                "parameters": t.parameters,
            },
        }
        for t in tools
    ]

    response_format = None
    if response_format_enabled:
        response_format = {
            "type": "json_schema",
            "json_schema": {"name": "worker_result", "schema": _RESULT_SCHEMA},
        }
    # Provider handles adaptive response_format downgrade when a route does not
    # support schema-constrained outputs.
    request_kwargs: dict[str, Any] = {"tool_choice": tool_choice}
    if response_format is not None:
        request_kwargs["response_format"] = response_format
    response = await provider.complete_with_tools(
        messages=messages,
        tools=openai_tools if openai_tools else [],
        **request_kwargs,
    )

    # Return in expected format: {"content": "...", "tool_calls": [...]}
    return response


async def _execute_tool(
    tool_name: str | None,
    tool_input: dict,
    workspace_root: Path,
    worker_dir: Path | Worker,
    worker: Worker | dict[str, Any],
    tool_map: dict[str, Any] | None = None,
    *,
    timeout_seconds: int | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Execute a tool by name."""
    if tool_map is None:
        # Backward compatibility for older call sites/tests that passed:
        # (tool_name, tool_input, workspace_root, worker, tool_map, ...)
        legacy_worker = worker_dir
        legacy_tool_map = worker
        if not isinstance(legacy_worker, Worker) or not isinstance(legacy_tool_map, dict):
            raise TypeError(
                "_execute_tool expected either (workspace_root, worker_dir, worker, tool_map) or legacy (workspace_root, worker, tool_map)"
            )
        worker_dir = workspace_root
        worker = legacy_worker
        tool_map = legacy_tool_map

    if not tool_name or tool_name not in tool_map:
        return {"error": f"Unknown tool: {tool_name}"}, {
            "retries": 0,
            "timed_out": False,
            "had_error": True,
            "error_type": "permanent",
        }

    tool = tool_map[tool_name]
    await worker.log(
        "info",
        _summarize_tool_start(tool_name, tool_input, timeout_seconds=timeout_seconds),
    )

    try:
        # Tool handlers expect (args, ctx) where ctx is a dict
        # Filesystem tools use worker_dir as the scratch workspace and
        # workspace_root for explicitly shared paths.
        # worker instance is needed for intent requests
        ctx = {
            "base_dir": worker_dir,
            "worker_dir": worker_dir,
            "workspace_root": workspace_root,
            "worker": worker,
        }

        # Use tool.is_async to determine if it needs to be awaited
        async def _run_tool() -> Any:
            if tool.is_async:
                if inspect.iscoroutinefunction(tool.handler):
                    return await tool.handler(tool_input, ctx)
                maybe_result = tool.handler(tool_input, ctx)
                if inspect.isawaitable(maybe_result):
                    return await maybe_result
                return maybe_result
            # Run sync handlers in a thread to keep loop responsive.
            return await asyncio.to_thread(tool.handler, tool_input, ctx)

        max_attempts = 3 if _is_tool_retryable(tool_name, tool) else 1
        retries = 0
        for attempt in range(max_attempts):
            try:
                if timeout_seconds and timeout_seconds > 0:
                    result = await asyncio.wait_for(_run_tool(), timeout=timeout_seconds)
                else:
                    result = await _run_tool()
            except TimeoutError:
                error_text = f"Tool timed out after {timeout_seconds}s: {tool_name}"
                if attempt < max_attempts - 1:
                    retries += 1
                    await asyncio.sleep(_retry_backoff(attempt))
                    continue
                error_result = {"error": error_text}
                error_meta = {
                    "retries": retries,
                    "timed_out": True,
                    "had_error": True,
                    "error_type": "transient",
                }
                await worker.log(
                    "warning", _summarize_tool_finish(tool_name, error_result, error_meta)
                )
                return error_result, error_meta
            except Exception as exc:
                await worker.log("error", f"Tool execution failed: {tool_name}: {exc}")
                error_text = str(exc)
                error_info = _tool_error_info(
                    error_text,
                    classification=exc.classification if isinstance(exc, ToolBridgeError) else None,
                    bridge=exc.bridge if isinstance(exc, ToolBridgeError) else None,
                    retryable=exc.retryable if isinstance(exc, ToolBridgeError) else None,
                )
                error_type = str(error_info["error_type"])
                if attempt < max_attempts - 1 and error_type == "transient":
                    retries += 1
                    await asyncio.sleep(_retry_backoff(attempt))
                    continue
                error_result = {"error": error_text}
                error_meta = {
                    "retries": retries,
                    "timed_out": False,
                    "had_error": True,
                    **error_info,
                }
                await worker.log(
                    "warning", _summarize_tool_finish(tool_name, error_result, error_meta)
                )
                return error_result, error_meta

            if _result_has_error(result):
                error_text = _extract_error_text(result)
                classification = None
                bridge = None
                retryable = None
                if isinstance(result, dict):
                    if isinstance(result.get("classification"), str):
                        classification = result["classification"]
                    if isinstance(result.get("bridge"), str):
                        bridge = result["bridge"]
                    if isinstance(result.get("retryable"), bool):
                        retryable = result["retryable"]
                error_info = _tool_error_info(
                    error_text,
                    classification=classification,
                    bridge=bridge,
                    retryable=retryable,
                )
                error_type = str(error_info["error_type"])
                if attempt < max_attempts - 1 and error_type == "transient":
                    retries += 1
                    await asyncio.sleep(_retry_backoff(attempt))
                    continue
                error_meta = {
                    "retries": retries,
                    "timed_out": False,
                    "had_error": True,
                    **error_info,
                }
                await worker.log("warning", _summarize_tool_finish(tool_name, result, error_meta))
                return result, error_meta

            success_meta = {
                "retries": retries,
                "timed_out": False,
                "had_error": False,
                "error_type": "none",
            }
            await worker.log("info", _summarize_tool_finish(tool_name, result, success_meta))
            return result, success_meta
    except Exception as exc:
        await worker.log("error", f"Tool execution failed: {tool_name}: {exc}")
        error_info = _tool_error_info(
            str(exc),
            classification=exc.classification if isinstance(exc, ToolBridgeError) else None,
            bridge=exc.bridge if isinstance(exc, ToolBridgeError) else None,
            retryable=exc.retryable if isinstance(exc, ToolBridgeError) else None,
        )
        error_result = {"error": str(exc)}
        error_meta = {
            "retries": 0,
            "timed_out": False,
            "had_error": True,
            **error_info,
        }
        await worker.log("warning", _summarize_tool_finish(tool_name, error_result, error_meta))
        return error_result, error_meta


def _with_octo_tool_proxies(tools: list[Any], worker: Worker) -> list[Any]:
    proxied: list[Any] = []
    for tool in tools:
        if getattr(tool, "name", "") not in _OCTO_PROXY_TOOLS:
            proxied.append(tool)
            continue
        proxied.append(_make_octo_proxy_tool(tool, worker))
    return proxied


def _make_octo_proxy_tool(tool: Any, worker: Worker) -> Any:
    from octopal.tools.registry import ToolSpec

    async def _proxy_handler(args: dict[str, Any], ctx: dict[str, Any]) -> Any:
        return await worker.call_octo_tool(tool.name, args)

    return ToolSpec(
        name=tool.name,
        description=tool.description,
        parameters=tool.parameters,
        permission=tool.permission,
        handler=_proxy_handler,
        is_async=True,
    )


def _make_request_instruction_tool(worker: Worker) -> Any:
    from octopal.tools.registry import ToolSpec

    async def _handler(args: dict[str, Any], ctx: dict[str, Any]) -> Any:
        question = str(args.get("question") or "").strip()
        context = args.get("context")
        target = str(args.get("target") or "octo").strip().lower()
        timeout_seconds = max(1, int(args.get("timeout_seconds") or 120))
        return await worker.request_instruction(
            question=question,
            context=context if isinstance(context, dict) else {},
            target=target,
            timeout_seconds=timeout_seconds,
        )

    return ToolSpec(
        name="request_instruction",
        description=(
            "Pause and ask Octo or the parent for blocking guidance when the task cannot safely continue."
        ),
        parameters={
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "Concrete question needed before continuing.",
                },
                "context": {
                    "type": "object",
                    "description": "Compact structured context.",
                    "additionalProperties": True,
                },
                "target": {
                    "type": "string",
                    "enum": ["octo", "parent"],
                    "description": "Use parent for child-worker coordination.",
                },
                "timeout_seconds": {
                    "type": "number",
                    "description": "Seconds to wait; default 120.",
                },
            },
            "required": ["question"],
            "additionalProperties": False,
        },
        permission="worker_coordination",
        handler=_handler,
        is_async=True,
    )


def _extract_mcp_identity(mcp_tool_data: dict[str, Any]) -> tuple[str, str] | None:
    """Extract MCP server/tool identity from explicit metadata or legacy names."""
    server_id = mcp_tool_data.get("server_id")
    remote_tool_name = mcp_tool_data.get("remote_tool_name")
    if (
        isinstance(server_id, str)
        and server_id
        and isinstance(remote_tool_name, str)
        and remote_tool_name
    ):
        return server_id, remote_tool_name

    name = str(mcp_tool_data.get("name", ""))
    if not name.startswith("mcp_"):
        return None
    # Legacy fallback: mcp_<safe_server_id>_<safe_tool_name>. This may be ambiguous
    # when both include underscores, but keeps compatibility for older specs.
    parts = name.split("_")
    if len(parts) < 3:
        return None
    return parts[1], "_".join(parts[2:])


def _parse_tool_arguments(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
            return {"_arg": parsed}
        except json.JSONDecodeError:
            return {"_raw": value}
    return {}


def _truncate_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + f"...[truncated {len(text) - max_chars} chars]"


def _is_valid_result_payload(payload: dict[str, Any]) -> bool:
    try:
        from jsonschema import ValidationError, validate

        validate(instance=payload, schema=_RESULT_SCHEMA)
        return True
    except ValidationError:
        return False
    except Exception:
        return "summary" in payload and "output" in payload


def _classify_tool_error(text: str) -> str:
    lowered = (text or "").lower()
    if any(token in lowered for token in _TRANSIENT_ERROR_HINTS):
        return "transient"
    if any(token in lowered for token in _PERMANENT_ERROR_HINTS):
        return "permanent"
    return "unknown"


def _tool_error_info(
    error_text: str,
    *,
    classification: str | None = None,
    bridge: str | None = None,
    retryable: bool | None = None,
) -> dict[str, Any]:
    if retryable is True:
        error_type = "transient"
    elif retryable is False:
        error_type = "permanent"
    else:
        error_type = _classify_tool_error(error_text)
    return {
        "error_type": error_type,
        "error_classification": classification or "unknown",
        "error_bridge": bridge or "tool",
        "retryable": retryable,
    }


def _is_systemic_tool_bridge_failure(tool_meta: dict[str, Any]) -> bool:
    return (
        tool_meta.get("error_bridge") == "mcp"
        and tool_meta.get("error_classification") in _SYSTEMIC_TOOL_ERROR_CLASSIFICATIONS
    )


def _result_has_error(result: Any) -> bool:
    structured = _decode_structured_tool_result(result)
    if isinstance(structured, dict):
        if isinstance(structured.get("error"), str) and bool(structured.get("error").strip()):
            return True
        status = str(structured.get("status", "")).strip().lower()
        if status in {"failed", "error"}:
            return True
        if structured.get("ok") is False:
            return True
        returncode = structured.get("returncode")
        if isinstance(returncode, int) and returncode != 0:
            return True
        return isinstance(returncode, float) and int(returncode) != 0
    if isinstance(result, str):
        lowered = result.strip().lower()
        return (
            lowered.startswith("error")
            or lowered.startswith("failed")
            or " error:" in lowered
            or "tool execution failed" in lowered
        )
    return False


def _extract_error_text(result: Any) -> str:
    structured = _decode_structured_tool_result(result)
    if isinstance(structured, dict):
        if isinstance(structured.get("error"), str) and structured.get("error").strip():
            return structured["error"]
        returncode = structured.get("returncode")
        if isinstance(returncode, (int, float)) and int(returncode) != 0:
            stderr = structured.get("stderr")
            if isinstance(stderr, str) and stderr.strip():
                return stderr
            stdout = structured.get("stdout")
            if isinstance(stdout, str) and stdout.strip():
                return stdout
            return f"command exited with return code {int(returncode)}"
        message = structured.get("message")
        if isinstance(message, str) and message.strip():
            return message
        summary = structured.get("summary")
        if isinstance(summary, str) and summary.strip():
            return summary
    if isinstance(result, str):
        return result
    return str(result)


def _decode_structured_tool_result(result: Any) -> Any:
    if isinstance(result, dict):
        return result
    if not isinstance(result, str):
        return None
    stripped = result.strip()
    if not stripped or stripped[0] not in "{[":
        return None
    try:
        return json.loads(stripped)
    except Exception:
        return None


def _is_upstream_unavailable_error(text: str) -> bool:
    lowered = (text or "").lower()
    return any(token in lowered for token in _UPSTREAM_UNAVAILABLE_HINTS)


def _upstream_error_bucket(text: str) -> str:
    lowered = (text or "").lower()
    for token in _UPSTREAM_UNAVAILABLE_HINTS:
        if token in lowered:
            return token
    return "upstream_unavailable"


def _retry_backoff(attempt: int) -> float:
    base = 0.25 * (2**attempt)
    jitter = random.uniform(0.0, 0.2)
    return min(2.0, base + jitter)


def _is_tool_retryable(tool_name: str, tool: Any) -> bool:
    permission = getattr(tool, "permission", "")
    if permission in {"filesystem_write", "service_control", "deploy_control"}:
        return False
    read_like_prefixes = ("get_", "list_", "read_", "web_", "search_", "mcp_")
    return tool_name.startswith(read_like_prefixes) or permission in {
        "network",
        "filesystem_read",
        "service_read",
    }


def _auto_tune_max_steps(base_steps: int, available_tools: list[str], system_prompt: str) -> int:
    tuned = max(3, int(base_steps))
    tool_set = set(available_tools)
    if any(name.startswith("mcp_") or "web" in name for name in tool_set):
        tuned += 3
    if any(
        name in {"exec_run", "test_run", "docker_compose_control", "deploy_manager"}
        for name in tool_set
    ):
        tuned += 2
    if "writer" in system_prompt.lower() and len(tool_set) <= 2:
        tuned -= 2
    return max(3, min(_DEFAULT_MAX_STEP_CAP, tuned))


def _attach_telemetry(output: Any, telemetry: dict[str, Any]) -> dict[str, Any]:
    if isinstance(output, dict):
        payload = dict(output)
    elif output is None:
        payload = {}
    else:
        payload = {"result": output}
    payload["_telemetry"] = telemetry
    return payload


def _build_inference_unavailable_result(
    *,
    worker: Worker,
    telemetry: dict[str, Any],
    error_text: str,
    thinking_steps: int,
    tools_used: list[str],
    partial: bool,
) -> WorkerResult:
    summary = (
        "Task partially completed with degraded state: inference provider is currently overloaded."
        if partial
        else "Task failed temporarily: inference provider is currently overloaded."
    )
    return WorkerResult(
        status="failed",
        summary=summary,
        output=_attach_telemetry(
            {
                "degraded": True,
                "retryable": True,
                "reason": "inference_upstream_unavailable",
                "error": _truncate_text(error_text, 500),
            },
            telemetry,
        ),
        knowledge_proposals=worker.knowledge_proposals,
        thinking_steps=thinking_steps,
        tools_used=tools_used,
    )


def _summarize_tool_start(
    tool_name: str | None, tool_input: dict[str, Any], *, timeout_seconds: int | None
) -> str:
    keys = sorted(str(key) for key in tool_input)
    return f"Tool start: {tool_name} timeout={timeout_seconds or 0}s input_keys={keys}"


def _summarize_tool_finish(tool_name: str | None, result: Any, meta: dict[str, Any]) -> str:
    error_text = (
        _truncate_text(_extract_error_text(result), 240) if _result_has_error(result) else ""
    )
    result_shape = _describe_tool_result_shape(result)
    parts = [
        f"Tool finish: {tool_name}",
        f"status={'error' if meta.get('had_error') else 'ok'}",
        f"result={result_shape}",
    ]
    if meta.get("retries"):
        parts.append(f"retries={meta['retries']}")
    if meta.get("timed_out"):
        parts.append("timed_out=true")
    if meta.get("error_type") and meta.get("error_type") != "none":
        parts.append(f"error_type={meta['error_type']}")
    if error_text:
        parts.append(f"error={error_text}")
    return " ".join(parts)


def _describe_tool_result_shape(result: Any) -> str:
    if isinstance(result, dict):
        keys = sorted(str(key) for key in result)[:8]
        return (
            f"dict(keys={keys}, chars={len(json.dumps(result, ensure_ascii=False, default=str))})"
        )
    if isinstance(result, list):
        return f"list(len={len(result)})"
    if isinstance(result, str):
        return f"str(chars={len(result)})"
    if result is None:
        return "null"
    return type(result).__name__
