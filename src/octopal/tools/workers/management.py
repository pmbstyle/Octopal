from __future__ import annotations

import asyncio
import hashlib
import json
import re
import uuid
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from octopal.runtime.plans import PlanRunService
from octopal.runtime.worker_result_payloads import (
    SYNTHESIZE_WORKER_OUTPUT_CONTEXT_BUDGET,
    summarize_worker_output_for_context,
)
from octopal.runtime.workers.allowed_paths import (
    infer_allowed_paths_from_values as _infer_allowed_paths_from_values,
)
from octopal.tools.registry import ToolSpec
from octopal.utils import utc_now

if TYPE_CHECKING:
    from octopal.runtime.octo.core import Octo

_WORKER_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_MAX_PARALLEL_BATCH = 10
_WORKER_BLOCKED_TOOL_NAMES = {
    "send_file_to_user",
    "self_control",
    "octo_restart_self",
    "octo_check_update",
    "octo_update_self",
}
_WORKER_PERMISSION_ALIASES = {
    "spawn_children": "worker_manage",
}
_ORCHESTRATION_PLAN_KEY = "_orchestration_plan"
_ORCHESTRATION_ITEM_STATUSES = {
    "todo",
    "running",
    "awaiting_worker",
    "awaiting_instruction",
    "completed",
    "failed",
    "blocked",
    "skipped",
    "stopped",
    "missing",
}
_ORCHESTRATION_TERMINAL_STATUSES = {
    "completed",
    "failed",
    "blocked",
    "skipped",
    "stopped",
    "missing",
}
_ALLOWED_PATHS_GUIDANCE = (
    "Workers always keep their own private scratch workspace. "
    "Use allowed_paths only when the worker needs files from Octo's main workspace, "
    "and pass the smallest explicit set that will do the job. "
    "If the task only needs the worker's own scratch space, omit allowed_paths."
)


def _infer_allowed_paths_from_task(task: str) -> list[str] | None:
    return _infer_allowed_paths_from_values(task)


def _worker_context_spec(ctx: dict[str, object]) -> Any | None:
    worker = ctx.get("worker")
    return getattr(worker, "spec", None)


def _worker_context_id(ctx: dict[str, object]) -> str:
    spec = _worker_context_spec(ctx)
    return str(getattr(spec, "id", "") or "").strip()


def _worker_context_is_orchestrator(ctx: dict[str, object]) -> bool:
    spec = _worker_context_spec(ctx)
    tool_names = {str(tool).strip() for tool in getattr(spec, "available_tools", []) or []}
    return bool(tool_names & {"start_child_worker", "start_workers_parallel"})


def _orchestration_access_error(ctx: dict[str, object]) -> str | None:
    if not _worker_context_id(ctx):
        return "orchestration plan tools require a worker context."
    if not _worker_context_is_orchestrator(ctx):
        return "orchestration plan tools are only available to child-spawning workers."
    octo = ctx.get("octo")
    if octo is None or getattr(octo, "store", None) is None:
        return "orchestration plan tools require an active Octo store."
    return None


def _normalize_orchestration_item_id(value: object) -> str:
    text = str(value or "").strip()
    if text:
        return text[:80]
    return f"item-{uuid.uuid4().hex[:8]}"


def _normalize_orchestration_status(value: object, *, default: str = "todo") -> str:
    status = str(value or default).strip().lower()
    if status not in _ORCHESTRATION_ITEM_STATUSES:
        return default
    return status


def _load_orchestration_output(
    octo: Any, worker_id: str
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    worker = octo.store.get_worker(worker_id)
    output = dict(getattr(worker, "output", None) or {}) if worker is not None else {}
    raw_plan = output.get(_ORCHESTRATION_PLAN_KEY)
    plan = dict(raw_plan) if isinstance(raw_plan, dict) else None
    return output, plan


def _save_orchestration_plan(octo: Any, worker_id: str, plan: dict[str, Any]) -> dict[str, Any]:
    output, _ = _load_orchestration_output(octo, worker_id)
    now = utc_now().isoformat()
    plan["updated_at"] = now
    output[_ORCHESTRATION_PLAN_KEY] = plan
    octo.store.update_worker_result(worker_id, output=output)
    return plan


def _plan_items_by_id(plan: dict[str, Any]) -> dict[str, dict[str, Any]]:
    items = plan.get("items")
    if not isinstance(items, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("id") or "").strip()
        if item_id:
            out[item_id] = item
    return out


def _derive_orchestration_plan_status(items: list[dict[str, Any]]) -> str:
    if not items:
        return "planned"
    statuses = {str(item.get("status") or "todo") for item in items}
    if statuses <= {"completed", "skipped"}:
        return "completed"
    if statuses & {"failed", "blocked", "stopped", "missing"}:
        return "needs_attention"
    if statuses & {"running", "awaiting_worker", "awaiting_instruction"}:
        return "running"
    if statuses & {"completed", "skipped"}:
        return "running"
    return "planned"


def _update_orchestration_item(
    *,
    octo: Any,
    worker_id: str,
    item_id: str,
    status: str,
    worker_run_id: str | None = None,
    worker_template_id: str | None = None,
    summary: str | None = None,
    output: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    _, plan = _load_orchestration_output(octo, worker_id)
    if plan is None:
        return {"status": "error", "message": "orchestration plan was not found"}
    items = plan.get("items")
    if not isinstance(items, list):
        return {"status": "error", "message": "orchestration plan has no items"}
    by_id = _plan_items_by_id(plan)
    item = by_id.get(item_id)
    if item is None:
        return {"status": "not_found", "item_id": item_id}
    now = utc_now().isoformat()
    item["status"] = status
    item["updated_at"] = now
    if status in _ORCHESTRATION_TERMINAL_STATUSES:
        item["completed_at"] = now
        if status in {"completed", "skipped"}:
            item.pop("error", None)
    else:
        item.pop("completed_at", None)
        item.pop("summary", None)
        item.pop("output", None)
        item.pop("error", None)
    if worker_run_id:
        item["worker_run_id"] = worker_run_id
    if worker_template_id:
        item["worker_template_id"] = worker_template_id
    if summary is not None:
        item["summary"] = summary
    if output is not None:
        item["output"] = output
    if error is not None:
        item["error"] = error
    plan["status"] = _derive_orchestration_plan_status(
        [item for item in items if isinstance(item, dict)]
    )
    saved = _save_orchestration_plan(octo, worker_id, plan)
    return {"status": "ok", "plan": saved, "item": item}


def _tool_orchestration_plan_create(args: dict[str, object], ctx: dict[str, object]) -> str:
    access_error = _orchestration_access_error(ctx)
    if access_error:
        return json.dumps({"status": "error", "message": access_error}, ensure_ascii=False)
    goal = str(args.get("goal") or "").strip()
    raw_items = args.get("items")
    if not goal:
        return json.dumps({"status": "error", "message": "goal is required"}, ensure_ascii=False)
    if not isinstance(raw_items, list) or not raw_items:
        return json.dumps(
            {"status": "error", "message": "items must be a non-empty array"}, ensure_ascii=False
        )
    now = utc_now().isoformat()
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw_item in enumerate(raw_items[:20]):
        if not isinstance(raw_item, dict):
            continue
        item_id = _normalize_orchestration_item_id(raw_item.get("id") or f"item-{index + 1}")
        if item_id in seen:
            item_id = f"{item_id}-{index + 1}"
        seen.add(item_id)
        title = str(raw_item.get("title") or item_id).strip()[:240] or item_id
        item = {
            "id": item_id,
            "title": title,
            "status": _normalize_orchestration_status(raw_item.get("status")),
            "created_at": now,
            "updated_at": now,
        }
        task = str(raw_item.get("task") or "").strip()
        if task:
            item["task"] = task
        worker_template_id = str(raw_item.get("worker_template_id") or "").strip()
        if worker_template_id:
            item["worker_template_id"] = worker_template_id
        items.append(item)
    if not items:
        return json.dumps(
            {"status": "error", "message": "items must contain at least one object"},
            ensure_ascii=False,
        )
    plan = {
        "goal": goal,
        "status": _derive_orchestration_plan_status(items),
        "items": items,
        "created_at": now,
        "updated_at": now,
    }
    saved = _save_orchestration_plan(ctx["octo"], _worker_context_id(ctx), plan)
    return json.dumps({"status": "ok", "plan": saved}, ensure_ascii=False)


def _tool_orchestration_plan_status(args: dict[str, object], ctx: dict[str, object]) -> str:
    access_error = _orchestration_access_error(ctx)
    if access_error:
        return json.dumps({"status": "error", "message": access_error}, ensure_ascii=False)
    _, plan = _load_orchestration_output(ctx["octo"], _worker_context_id(ctx))
    if plan is None:
        return json.dumps(
            {"status": "not_found", "message": "orchestration plan was not found"},
            ensure_ascii=False,
        )
    return json.dumps({"status": "ok", "plan": plan}, ensure_ascii=False)


def _tool_orchestration_plan_update_item(args: dict[str, object], ctx: dict[str, object]) -> str:
    access_error = _orchestration_access_error(ctx)
    if access_error:
        return json.dumps({"status": "error", "message": access_error}, ensure_ascii=False)
    item_id = str(args.get("item_id") or "").strip()
    if not item_id:
        return json.dumps({"status": "error", "message": "item_id is required"}, ensure_ascii=False)
    status = _normalize_orchestration_status(args.get("status"))
    result = _update_orchestration_item(
        octo=ctx["octo"],
        worker_id=_worker_context_id(ctx),
        item_id=item_id,
        status=status,
        worker_run_id=str(args.get("worker_run_id") or "").strip() or None,
        worker_template_id=str(args.get("worker_template_id") or "").strip() or None,
        summary=str(args.get("summary") or "").strip() or None,
        output=args.get("output") if isinstance(args.get("output"), dict) else None,
        error=str(args.get("error") or "").strip() or None,
    )
    return json.dumps(result, ensure_ascii=False)


def get_worker_tools() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="propose_knowledge",
            description="Propose a fact, decision, or failure lesson for the permanent canonical memory. The Octo will review and potentially add it.",
            parameters={
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "description": "Category of knowledge.",
                        "enum": ["fact", "decision", "failure"],
                    },
                    "content": {
                        "type": "string",
                        "description": "The concise fact or lesson to remember.",
                    },
                },
                "required": ["category", "content"],
                "additionalProperties": False,
            },
            permission="network",
            handler=_tool_propose_knowledge,
        ),
        ToolSpec(
            name="orchestration_plan_create",
            description=(
                "Create or replace the current orchestrator worker's scoped execution plan. "
                "Use this only from workers that coordinate child workers; it is internal worker "
                "state, not a user-facing Octo runtime plan."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "goal": {"type": "string"},
                    "items": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 20,
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "title": {"type": "string"},
                                "task": {"type": "string"},
                                "worker_template_id": {"type": "string"},
                                "status": {
                                    "type": "string",
                                    "enum": sorted(_ORCHESTRATION_ITEM_STATUSES),
                                },
                            },
                            "required": ["id", "title"],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": ["goal", "items"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_orchestration_plan_create,
        ),
        ToolSpec(
            name="orchestration_plan_status",
            description="Inspect the current orchestrator worker's scoped execution plan.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="worker_manage",
            handler=_tool_orchestration_plan_status,
        ),
        ToolSpec(
            name="orchestration_plan_update_item",
            description=(
                "Update one item in the current orchestrator worker's scoped execution plan. "
                "Use this for orchestrator-owned work that is not automatically tied to a child launch."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "item_id": {"type": "string"},
                    "status": {"type": "string", "enum": sorted(_ORCHESTRATION_ITEM_STATUSES)},
                    "worker_run_id": {"type": "string"},
                    "worker_template_id": {"type": "string"},
                    "summary": {"type": "string"},
                    "output": {"type": "object", "additionalProperties": True},
                    "error": {"type": "string"},
                },
                "required": ["item_id", "status"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_orchestration_plan_update_item,
        ),
        ToolSpec(
            name="list_workers",
            description="List available worker templates with their capabilities.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="worker_manage",
            handler=_tool_list_workers,
        ),
        ToolSpec(
            name="start_worker",
            description=(
                "Start one bounded worker task with an explicit worker template ID. Treat the returned "
                "worker run as active execution state for the current task: later collect, verify, or "
                "synthesize its result instead of considering launch itself complete. Use list_workers first "
                f"if you need to choose an executor. {_ALLOWED_PATHS_GUIDANCE}"
            ),
            parameters={
                "type": "object",
                "properties": {
                    "worker_id": {
                        "type": "string",
                        "description": "Worker template ID (e.g., 'web_researcher'). Required; automatic routing is disabled.",
                    },
                    "task": {
                        "type": "string",
                        "description": "Natural language task description for the worker.",
                    },
                    "inputs": {
                        "type": "object",
                        "description": "Task-specific input data.",
                        "additionalProperties": True,
                    },
                    "tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional subset of this template's tools for this task; cannot add tools outside the template contract.",
                    },
                    "timeout_seconds": {
                        "type": "number",
                        "description": "Override default timeout (optional).",
                    },
                    "scheduled_task_id": {
                        "type": "string",
                        "description": "Optional schedule task ID when this worker run comes from check_schedule. Enables reliable execution tracking.",
                    },
                    "plan_run_id": {
                        "type": "string",
                        "description": "Optional runtime plan id when this worker executes a specific durable plan step.",
                    },
                    "plan_step_id": {
                        "type": "string",
                        "description": "Optional runtime plan step id to bind to the launched worker run.",
                    },
                    "required_tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Explicit tool capabilities the selected worker must support.",
                    },
                    "required_permissions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional permissions the selected worker should include.",
                    },
                    "orchestration_item_id": {
                        "type": "string",
                        "description": "Optional scoped orchestration plan item id to bind to this child worker run.",
                    },
                    "required_tool_calls": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Tool calls that must happen before the worker may complete.",
                    },
                    "allowed_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Optional main-workspace paths to share with the worker in addition to its own scratch "
                            "workspace. Use the smallest explicit set needed, for example ['skills/job-search/SKILL.md', "
                            "'experiments/README.md']. Omit this when the worker only needs its own scratch files."
                        ),
                    },
                },
                "required": ["worker_id", "task"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_start_worker,
            is_async=True,
        ),
        ToolSpec(
            name="start_child_worker",
            description=(
                "Start a child worker from inside a worker context with lineage tracking and spawn-policy checks. "
                "The parent worker remains responsible for supervising, answering child instructions, and "
                "synthesizing child results before it completes. "
                f"{_ALLOWED_PATHS_GUIDANCE}"
            ),
            parameters={
                "type": "object",
                "properties": {
                    "worker_id": {
                        "type": "string",
                        "description": "Worker template ID (e.g., 'web_researcher'). Required; automatic routing is disabled.",
                    },
                    "task": {
                        "type": "string",
                        "description": "Natural language task description for the child worker.",
                    },
                    "inputs": {
                        "type": "object",
                        "description": "Task-specific input data.",
                        "additionalProperties": True,
                    },
                    "tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional subset of this template's tools for this task; cannot add tools outside the template contract.",
                    },
                    "timeout_seconds": {
                        "type": "number",
                        "description": "Override default timeout (optional).",
                    },
                    "required_tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Explicit tool capabilities the selected worker must support.",
                    },
                    "required_permissions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional permissions the selected worker should include.",
                    },
                    "required_tool_calls": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Tool calls that must happen before the worker may complete.",
                    },
                    "allowed_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Optional main-workspace paths to share with the child worker in addition to its own "
                            "scratch workspace. Use the smallest explicit set needed, for example "
                            "['skills/job-search/SKILL.md', 'memory/canon/facts.md']. Omit this when the child only "
                            "needs its own scratch files."
                        ),
                    },
                },
                "required": ["worker_id", "task"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_start_child_worker,
            is_async=True,
        ),
        ToolSpec(
            name="start_workers_parallel",
            description=(
                "Launch multiple explicitly selected independent worker tasks in parallel and return run IDs. "
                "Use this when fan-out is faster or safer than serial execution, then keep the returned runs "
                "as active execution state until worker_yield/get_worker_result/synthesize_worker_results "
                "shows the batch is ready, blocked, or needs follow-up. Each worker still gets its own scratch workspace. "
                "For any shared project files, set allowed_paths per task with the smallest explicit path set. "
                "When a task executes a durable runtime plan step, pass plan_run_id and plan_step_id on that task."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "tasks": {
                        "type": "array",
                        "description": "List of tasks to launch. Each item must include worker_id and task, and may include inputs, a subset-only tools override, timeout_seconds, required_tools, required_permissions, required_tool_calls, plan_run_id, plan_step_id, orchestration_item_id, and allowed_paths.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "worker_id": {"type": "string"},
                                "task": {"type": "string"},
                                "inputs": {"type": "object", "additionalProperties": True},
                                "tools": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Optional subset of the selected template's tools; cannot add tools outside the template contract.",
                                },
                                "timeout_seconds": {"type": "number"},
                                "required_tools": {"type": "array", "items": {"type": "string"}},
                                "required_permissions": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                                "orchestration_item_id": {
                                    "type": "string",
                                    "description": "Optional scoped orchestration plan item id to bind to this child worker run.",
                                },
                                "required_tool_calls": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                                "plan_run_id": {
                                    "type": "string",
                                    "description": "Optional runtime plan id when this worker executes a specific durable plan step.",
                                },
                                "plan_step_id": {
                                    "type": "string",
                                    "description": "Optional runtime plan step id to bind to the launched worker run.",
                                },
                                "allowed_paths": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": (
                                        "Optional main-workspace paths to share with this worker in addition to its "
                                        "own scratch workspace. Use the smallest explicit set needed."
                                    ),
                                },
                            },
                            "required": ["worker_id", "task"],
                            "additionalProperties": False,
                        },
                    },
                    "max_parallel": {
                        "type": "number",
                        "description": "Maximum concurrent launches (default 3, max 10).",
                    },
                },
                "required": ["tasks"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_start_workers_parallel,
            is_async=True,
        ),
        ToolSpec(
            name="synthesize_worker_results",
            description=(
                "Synthesize worker outputs into the next execution decision and combined summary, including "
                "completed, failed, missing, and pending runs. Use this after parallel or related worker runs "
                "are ready; if workers are still pending, follow the returned next_best_action instead of "
                "pretending the task is complete."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "worker_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Worker run IDs to synthesize.",
                    }
                },
                "required": ["worker_ids"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_synthesize_worker_results,
        ),
        ToolSpec(
            name="stop_worker",
            description="Stop a running worker by worker_id.",
            parameters={
                "type": "object",
                "properties": {"worker_id": {"type": "string"}},
                "required": ["worker_id"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_stop_worker,
            is_async=True,
        ),
        ToolSpec(
            name="get_worker_status",
            description="Get the current status and details of a specific worker by ID.",
            parameters={
                "type": "object",
                "properties": {
                    "worker_id": {
                        "type": "string",
                        "description": "The worker ID to check.",
                    }
                },
                "required": ["worker_id"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_get_worker_status,
        ),
        ToolSpec(
            name="list_active_workers",
            description="List all active workers (running or completed in the last 10 minutes).",
            parameters={
                "type": "object",
                "properties": {
                    "older_than_minutes": {
                        "type": "number",
                        "description": "Include workers updated in the last N minutes (default: 10).",
                    }
                },
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_list_active_workers,
        ),
        ToolSpec(
            name="worker_session_status",
            description=(
                "Summarize the current worker fabric as Octo's active execution state: active runs, recent "
                "completions/failures, and lineage health hints."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "older_than_minutes": {
                        "type": "number",
                        "description": "Window for active workers (default: 10).",
                    },
                    "recent_limit": {
                        "type": "number",
                        "description": "How many recent workers to inspect for summary (default: 12, max 50).",
                    },
                },
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_worker_session_status,
        ),
        ToolSpec(
            name="worker_yield",
            description=(
                "Assess whether active worker runs should be waited on, collected, or synthesized. Use this "
                "after worker launch or when resuming a task with in-flight workers; follow its mode and "
                "next_best_action as continuation guidance for the same task."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "worker_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional worker run IDs to inspect. If omitted, evaluates the current active worker fabric.",
                    },
                    "lineage_id": {
                        "type": "string",
                        "description": "Optional lineage ID to focus on a parent/child worker tree.",
                    },
                    "older_than_minutes": {
                        "type": "number",
                        "description": "Window for discovering active workers when worker_ids are omitted (default: 10).",
                    },
                },
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_worker_yield,
        ),
        ToolSpec(
            name="answer_worker_instruction",
            description=(
                "Answer a worker that is paused in awaiting_instruction state. "
                "Use this after get_worker_status/get_worker_result or a child resume payload exposes an instruction_request."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "worker_id": {
                        "type": "string",
                        "description": "The paused worker run ID.",
                    },
                    "request_id": {
                        "type": "string",
                        "description": "Instruction request id. Optional when the worker has exactly one active instruction_request in its record.",
                    },
                    "instruction": {
                        "type": "string",
                        "description": "The concrete instruction the worker should use to continue.",
                    },
                },
                "required": ["worker_id", "instruction"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_answer_worker_instruction,
            is_async=True,
        ),
        ToolSpec(
            name="get_worker_result",
            description="Get the result/output of a completed worker by ID.",
            parameters={
                "type": "object",
                "properties": {
                    "worker_id": {
                        "type": "string",
                        "description": "The worker ID to get results from.",
                    }
                },
                "required": ["worker_id"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_get_worker_result,
        ),
        ToolSpec(
            name="get_worker_output_path",
            description="Retrieve a specific part of a worker's output using a dotted path (e.g., 'results.items.0'). Useful for large outputs.",
            parameters={
                "type": "object",
                "properties": {
                    "worker_id": {
                        "type": "string",
                        "description": "The worker ID to check.",
                    },
                    "path": {
                        "type": "string",
                        "description": "Dotted path to the desired data (e.g., 'data.users.0.name').",
                    },
                },
                "required": ["worker_id", "path"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_get_worker_output_path,
        ),
        ToolSpec(
            name="create_worker_template",
            description="Create a new worker template by writing a worker.json file to the workspace. When a worker needs Octopal skills, prefer generic tools like list_skills, use_skill, and run_skill_script instead of hardcoding dynamic skill_<id> tools.",
            parameters={
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Unique worker ID (e.g., 'my_researcher'). Use lowercase with underscores.",
                    },
                    "name": {
                        "type": "string",
                        "description": "Human-readable name (e.g., 'My Researcher').",
                    },
                    "description": {
                        "type": "string",
                        "description": "What this worker does.",
                    },
                    "system_prompt": {
                        "type": "string",
                        "description": "Worker's personality, purpose, and instructions.",
                    },
                    "available_tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Tool names this worker can use (e.g., ['web_search', 'web_fetch']). For Octopal skills, prefer ['list_skills', 'use_skill', 'run_skill_script'] over dynamic skill_<id> tool names.",
                    },
                    "required_permissions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Permissions needed: 'network', 'filesystem_read', 'filesystem_write', 'exec', 'service_read', 'service_control', 'deploy_control', 'db_admin', 'security_audit', 'self_control'.",
                    },
                    "model": {
                        "type": "string",
                        "description": "Optional model override (e.g., 'gpt-4o').",
                    },
                    "max_thinking_steps": {
                        "type": "number",
                        "description": "Max reasoning iterations (default: 10).",
                    },
                    "default_timeout_seconds": {
                        "type": "number",
                        "description": "Default timeout in seconds (default: 300).",
                    },
                    "can_spawn_children": {
                        "type": "boolean",
                        "description": "Whether this worker template can spawn child workers.",
                    },
                    "allowed_child_templates": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Explicit whitelist of child template IDs this worker may spawn.",
                    },
                },
                "required": ["id", "name", "description", "system_prompt"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_create_worker_template,
        ),
        ToolSpec(
            name="update_worker_template",
            description="Update an existing worker template. Reads the worker.json file, modifies the specified fields, and writes it back. For Octopal skills, prefer generic tools like list_skills, use_skill, and run_skill_script over dynamic skill_<id> tool names.",
            parameters={
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Worker ID to update.",
                    },
                    "name": {"type": "string", "description": "New name (optional)."},
                    "description": {"type": "string", "description": "New description (optional)."},
                    "system_prompt": {
                        "type": "string",
                        "description": "New system prompt (optional).",
                    },
                    "available_tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "New tool list (optional). For Octopal skills, prefer ['list_skills', 'use_skill', 'run_skill_script'] over dynamic skill_<id> tool names.",
                    },
                    "required_permissions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "New permissions (optional).",
                    },
                    "model": {"type": "string", "description": "New model override (optional)."},
                    "max_thinking_steps": {
                        "type": "number",
                        "description": "New max steps (optional).",
                    },
                    "default_timeout_seconds": {
                        "type": "number",
                        "description": "New timeout (optional).",
                    },
                    "can_spawn_children": {
                        "type": "boolean",
                        "description": "Enable/disable child spawning (optional).",
                    },
                    "allowed_child_templates": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "New child-template whitelist (optional).",
                    },
                },
                "required": ["id"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_update_worker_template,
        ),
        ToolSpec(
            name="delete_worker_template",
            description="Delete a worker template by removing its directory from the configured workspace workers folder.",
            parameters={
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Worker ID to delete.",
                    }
                },
                "required": ["id"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_delete_worker_template,
        ),
    ]


def _tool_list_workers(args: dict[str, object], ctx: dict[str, object]) -> str:
    """List available worker templates."""
    octo: Octo = ctx["octo"]

    templates = octo.store.list_worker_templates()
    template_list = []
    for t in templates:
        worker_info: dict[str, object] = {
            "worker_id": t.id,
            "name": t.name,
            "description": t.description,
            "tools": t.available_tools,
            "permissions": t.required_permissions,
            "timeout_seconds": t.default_timeout_seconds,
        }
        can_spawn_children = bool(getattr(t, "can_spawn_children", False))
        allowed_child_templates = list(getattr(t, "allowed_child_templates", []))
        if can_spawn_children:
            worker_info["can_spawn_children"] = True
        if allowed_child_templates:
            worker_info["children"] = allowed_child_templates
        template_list.append(worker_info)

    return json.dumps(
        {
            "count": len(template_list),
            "workers": template_list,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _tool_create_worker_template(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Create a new worker template by writing a worker.json file to the workspace."""
    octo: Octo = ctx["octo"]
    base_dir: Path = ctx.get("base_dir", Path("workspace"))

    worker_id = str(args.get("id", "")).strip()
    name = str(args.get("name", "")).strip()
    description = str(args.get("description", "")).strip()
    system_prompt = str(args.get("system_prompt", "")).strip()

    if not worker_id:
        return "create_worker_template error: id is required."
    if not _is_valid_worker_id(worker_id):
        return "create_worker_template error: id must match ^[a-z0-9][a-z0-9_-]*$."
    if not name:
        return "create_worker_template error: name is required."
    if not description:
        return "create_worker_template error: description is required."
    if not system_prompt:
        return "create_worker_template error: system_prompt is required."

    # Check if worker already exists
    existing = octo.store.get_worker_template(worker_id)
    if existing:
        return f"create_worker_template error: worker '{worker_id}' already exists. Use update_worker_template to modify it."

    # Get optional parameters with defaults
    available_tools = (
        args.get("available_tools") if isinstance(args.get("available_tools"), list) else []
    )
    required_permissions = (
        args.get("required_permissions")
        if isinstance(args.get("required_permissions"), list)
        else []
    )
    available_tools = _normalize_str_list(available_tools)
    required_permissions = _infer_required_permissions(available_tools, required_permissions)
    model = str(args.get("model", "")).strip() or None
    max_thinking_steps = (
        int(args.get("max_thinking_steps")) if args.get("max_thinking_steps") else 10
    )
    default_timeout_seconds = (
        int(args.get("default_timeout_seconds")) if args.get("default_timeout_seconds") else 300
    )
    can_spawn_children = bool(args.get("can_spawn_children", False))
    allowed_child_templates = _normalize_str_list(args.get("allowed_child_templates"))

    # Build worker.json content
    worker_config = {
        "id": worker_id,
        "name": name,
        "description": description,
        "system_prompt": system_prompt,
        "available_tools": available_tools,
        "required_permissions": required_permissions,
        "model": model,
        "max_thinking_steps": max_thinking_steps,
        "default_timeout_seconds": default_timeout_seconds,
        "can_spawn_children": can_spawn_children,
        "allowed_child_templates": allowed_child_templates,
    }

    # Write worker.json file
    worker_dir = _resolve_worker_dir(base_dir, worker_id)
    if worker_dir is None:
        return "create_worker_template error: invalid worker id path."
    try:
        worker_dir.mkdir(parents=True, exist_ok=True)
        worker_file = worker_dir / "worker.json"
        worker_file.write_text(json.dumps(worker_config, indent=2), encoding="utf-8")
    except Exception as e:
        return f"create_worker_template error: failed to write worker.json: {e}"

    return json.dumps(
        {
            "status": "created",
            "worker_id": worker_id,
            "name": name,
            "description": description,
            "available_tools": available_tools,
            "required_permissions": required_permissions,
            "can_spawn_children": can_spawn_children,
            "allowed_child_templates": allowed_child_templates,
            "message": f"Worker template '{name}' created successfully at workers/{worker_id}/worker.json",
        },
        ensure_ascii=False,
    )


def _tool_update_worker_template(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Update an existing worker template by modifying its worker.json file."""
    base_dir: Path = ctx.get("base_dir", Path("workspace"))

    worker_id = str(args.get("id", "")).strip()
    if not worker_id:
        return "update_worker_template error: id is required."
    if not _is_valid_worker_id(worker_id):
        return "update_worker_template error: id must match ^[a-z0-9][a-z0-9_-]*$."

    # Read existing worker.json
    worker_dir = _resolve_worker_dir(base_dir, worker_id)
    if worker_dir is None:
        return "update_worker_template error: invalid worker id path."
    worker_file = worker_dir / "worker.json"
    if not worker_file.exists():
        return f"update_worker_template error: worker '{worker_id}' not found. Use create_worker_template to create it."

    try:
        existing_config = json.loads(worker_file.read_text(encoding="utf-8"))
    except Exception as e:
        return f"update_worker_template error: failed to read worker.json: {e}"

    # Update fields if provided
    if args.get("name"):
        existing_config["name"] = str(args.get("name")).strip()
    if args.get("description"):
        existing_config["description"] = str(args.get("description")).strip()
    if args.get("system_prompt"):
        existing_config["system_prompt"] = str(args.get("system_prompt")).strip()
    if isinstance(args.get("available_tools"), list):
        existing_config["available_tools"] = _normalize_str_list(args.get("available_tools"))
    if isinstance(args.get("required_permissions"), list):
        existing_config["required_permissions"] = _normalize_str_list(
            args.get("required_permissions")
        )
    if args.get("model"):
        existing_config["model"] = str(args.get("model")).strip()
    if args.get("max_thinking_steps"):
        existing_config["max_thinking_steps"] = int(args.get("max_thinking_steps"))
    if args.get("default_timeout_seconds"):
        existing_config["default_timeout_seconds"] = int(args.get("default_timeout_seconds"))
    if "can_spawn_children" in args:
        existing_config["can_spawn_children"] = bool(args.get("can_spawn_children"))
    if isinstance(args.get("allowed_child_templates"), list):
        existing_config["allowed_child_templates"] = _normalize_str_list(
            args.get("allowed_child_templates")
        )

    existing_config["available_tools"] = _normalize_str_list(existing_config.get("available_tools"))
    existing_config["required_permissions"] = _infer_required_permissions(
        existing_config.get("available_tools"),
        existing_config.get("required_permissions"),
    )

    # Write updated worker.json
    try:
        worker_file.write_text(json.dumps(existing_config, indent=2), encoding="utf-8")
    except Exception as e:
        return f"update_worker_template error: failed to write worker.json: {e}"

    return json.dumps(
        {
            "status": "updated",
            "worker_id": worker_id,
            "name": existing_config["name"],
            "description": existing_config["description"],
            "can_spawn_children": bool(existing_config.get("can_spawn_children", False)),
            "allowed_child_templates": _normalize_str_list(
                existing_config.get("allowed_child_templates")
            ),
            "message": f"Worker template '{existing_config['name']}' updated successfully at workers/{worker_id}/worker.json",
        },
        ensure_ascii=False,
    )


def _infer_required_permissions(available_tools: object, required_permissions: object) -> list[str]:
    normalized_permissions = _normalize_str_list(required_permissions)
    seen = set(normalized_permissions)

    from octopal.tools.tools import get_tools

    tool_names = set(_normalize_str_list(available_tools))
    for tool in get_tools():
        if str(tool.name).strip().lower() not in tool_names:
            continue
        permission = str(getattr(tool, "permission", "")).strip().lower()
        if not permission or permission in seen:
            continue
        seen.add(permission)
        normalized_permissions.append(permission)

    return normalized_permissions


def _tool_delete_worker_template(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Delete a worker template by removing its directory."""
    import shutil

    base_dir: Path = ctx.get("base_dir", Path("workspace"))

    worker_id = str(args.get("id", "")).strip()
    if not worker_id:
        return "delete_worker_template error: id is required."
    if not _is_valid_worker_id(worker_id):
        return "delete_worker_template error: id must match ^[a-z0-9][a-z0-9_-]*$."

    # Check if worker exists
    worker_dir = _resolve_worker_dir(base_dir, worker_id)
    if worker_dir is None:
        return "delete_worker_template error: invalid worker id path."
    if not worker_dir.exists():
        return f"delete_worker_template error: worker '{worker_id}' not found."

    # Delete the directory
    try:
        shutil.rmtree(worker_dir)
    except Exception as e:
        return f"delete_worker_template error: failed to delete directory: {e}"

    return json.dumps(
        {
            "status": "deleted",
            "worker_id": worker_id,
            "message": f"Worker template '{worker_id}' deleted successfully. Directory workers/{worker_id}/ has been removed.",
        },
        ensure_ascii=False,
    )


async def _tool_start_worker(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Start a worker task (octo or worker context)."""
    return await _start_worker_common(args, ctx, require_worker_context=False)


async def _tool_start_child_worker(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Start a child worker from a worker context only."""
    return await _start_worker_common(args, ctx, require_worker_context=True)


async def _start_worker_common(
    args: dict[str, object],
    ctx: dict[str, object],
    *,
    require_worker_context: bool,
) -> str:
    octo: Octo = ctx["octo"]
    chat_id = int(ctx.get("chat_id") or 0)
    caller_worker = ctx.get("worker")
    if require_worker_context and caller_worker is None:
        return "start_child_worker error: this tool can only be called from a worker context."

    worker_id = str(args.get("worker_id", "")).strip()
    task = str(args.get("task", "")).strip()
    inputs = args.get("inputs") if isinstance(args.get("inputs"), dict) else {}
    tools = args.get("tools") if isinstance(args.get("tools"), list) else None
    required_tools = _normalize_str_list(args.get("required_tools"))
    required_permissions = _normalize_str_list(args.get("required_permissions"))
    required_tool_calls = _normalize_tool_name_list(args.get("required_tool_calls"))
    timeout_seconds = int(args.get("timeout_seconds")) if args.get("timeout_seconds") else None
    scheduled_task_id = str(args.get("scheduled_task_id", "")).strip() or None

    if not task:
        return "start_worker error: task is required."

    resolution = _resolve_worker_for_start(
        octo=octo,
        worker_id=worker_id,
        required_tools=_merge_tool_requirements(required_tools, required_tool_calls),
        required_permissions=required_permissions,
    )
    if isinstance(resolution, str):
        return resolution
    template = resolution["template"]
    worker_id = str(resolution["worker_id"])

    tool_validation_error = _validate_requested_worker_tools(
        requested_tools=tools,
        template_tools=getattr(template, "available_tools", []),
        error_prefix="start_child_worker error" if require_worker_context else "start_worker error",
    )
    if tool_validation_error:
        return tool_validation_error
    required_call_validation_error = _validate_required_tool_calls_available(
        required_tool_calls=required_tool_calls,
        requested_tools=tools,
        template_tools=getattr(template, "available_tools", []),
        error_prefix="start_child_worker error" if require_worker_context else "start_worker error",
    )
    if required_call_validation_error:
        return required_call_validation_error

    plan_binding_error = _validate_plan_step_binding_request(octo=octo, args=args)
    if plan_binding_error:
        return json.dumps(
            {
                "status": "error",
                "worker_template_id": worker_id or None,
                "message": plan_binding_error.get("message") or "plan binding is invalid",
                "followup_required": False,
                "next_best_action": "continue_current_plan",
                "plan_binding": plan_binding_error,
            },
            ensure_ascii=False,
        )

    child_ctx = _extract_child_context(caller_worker)
    if child_ctx is not None:
        policy_error = _validate_child_spawn_policy(
            octo=octo,
            parent_ctx=child_ctx,
            child_template=template,
            explicit_worker_id=worker_id,
        )
        if policy_error:
            return policy_error

    orchestration_binding_error = _validate_orchestration_item_binding_request(
        octo=octo,
        parent_worker_id=child_ctx["run_id"] if child_ctx else None,
        args=args,
    )
    if orchestration_binding_error:
        return json.dumps(
            {
                "status": "error",
                "worker_template_id": worker_id or None,
                "message": orchestration_binding_error.get("message")
                or "orchestration plan binding is invalid",
                "followup_required": False,
                "next_best_action": "continue_current_plan",
                "orchestration_binding": orchestration_binding_error,
            },
            ensure_ascii=False,
        )

    allowed_paths = (
        args.get("allowed_paths")
        if "allowed_paths" in args
        else _infer_allowed_paths_from_values(task, inputs)
    )

    launch = await octo._start_worker_async(
        worker_id=worker_id,
        task=task,
        chat_id=chat_id,
        inputs=inputs,
        tools=tools,
        model=None,
        timeout_seconds=timeout_seconds,
        scheduled_task_id=scheduled_task_id,
        required_tool_calls=required_tool_calls,
        parent_worker_id=child_ctx["run_id"] if child_ctx else None,
        lineage_id=child_ctx["lineage_id"] if child_ctx else None,
        root_task_id=child_ctx["root_task_id"] if child_ctx else None,
        spawn_depth=(child_ctx["spawn_depth"] + 1) if child_ctx else 0,
        allowed_paths=allowed_paths,
    )
    status = str(launch.get("status", "started"))
    launched_worker_id = launch.get("worker_id")
    run_id = launch.get("run_id")
    if status == "started" and launched_worker_id:
        message = f"Worker '{template.name}' started as {launched_worker_id}. Use get_worker_status/get_worker_result with this worker_id."
    elif status == "skipped_duplicate":
        message = "Duplicate worker task detected in this turn; skipped starting a new worker."
    else:
        message = f"Worker start returned status={status}."
    followup_required = status in {"started", "skipped_duplicate"} and bool(
        launched_worker_id or run_id
    )
    next_best_action = "wait_for_worker_progress" if followup_required else "continue_current_plan"
    plan_binding = (
        _bind_plan_step_for_worker_launch(
            octo=octo,
            args=args,
            worker_run_id=str(launched_worker_id or "").strip(),
        )
        if status == "started"
        else _skipped_plan_step_binding(args)
    )
    orchestration_binding = (
        _bind_orchestration_item_for_child_launch(
            octo=octo,
            parent_worker_id=child_ctx["run_id"] if child_ctx else None,
            args=args,
            worker_run_id=str(launched_worker_id or run_id or "").strip(),
            worker_template_id=worker_id,
        )
        if status == "started"
        else None
    )

    return json.dumps(
        {
            "status": status,
            "worker_template_id": worker_id,
            "worker_id": launched_worker_id,
            "run_id": run_id,
            "scheduled_task_id": scheduled_task_id,
            "lineage_id": launch.get("lineage_id"),
            "parent_worker_id": launch.get("parent_worker_id"),
            "root_task_id": launch.get("root_task_id"),
            "spawn_depth": launch.get("spawn_depth"),
            "message": message,
            "followup_required": followup_required,
            "next_best_action": next_best_action,
            **({"plan_binding": plan_binding} if plan_binding else {}),
            **({"orchestration_binding": orchestration_binding} if orchestration_binding else {}),
        },
        ensure_ascii=False,
    )


def _validate_plan_step_binding_request(
    *,
    octo: Octo,
    args: dict[str, object],
) -> dict[str, object] | None:
    plan_run_id = str(args.get("plan_run_id") or "").strip()
    plan_step_id = str(args.get("plan_step_id") or "").strip()
    if not plan_run_id and not plan_step_id:
        return None
    if not plan_run_id or not plan_step_id:
        return {
            "status": "error",
            "run_id": plan_run_id or None,
            "step_id": plan_step_id or None,
            "message": "plan_run_id and plan_step_id must be provided together",
        }
    store = getattr(octo, "store", None)
    if store is None:
        return {
            "status": "error",
            "run_id": plan_run_id,
            "step_id": plan_step_id,
            "message": "Octo store is unavailable",
        }
    service = PlanRunService(store)
    snapshot = service.get_snapshot(plan_run_id)
    if snapshot is None:
        return {
            "status": "not_found",
            "run_id": plan_run_id,
            "step_id": plan_step_id,
            "message": "plan run was not found",
        }
    known_steps = {str(step.get("step_id") or "") for step in snapshot.get("steps") or []}
    if plan_step_id not in known_steps:
        return {
            "status": "not_found",
            "run_id": plan_run_id,
            "step_id": plan_step_id,
            "message": "plan step was not found",
        }
    return None


def _bind_plan_step_for_worker_launch(
    *,
    octo: Octo,
    args: dict[str, object],
    worker_run_id: str,
) -> dict[str, object] | None:
    plan_run_id = str(args.get("plan_run_id") or "").strip()
    plan_step_id = str(args.get("plan_step_id") or "").strip()
    if not plan_run_id and not plan_step_id:
        return None
    if not plan_run_id or not plan_step_id:
        return {
            "status": "error",
            "message": "plan_run_id and plan_step_id must be provided together",
        }
    if not worker_run_id:
        return {
            "status": "error",
            "run_id": plan_run_id,
            "step_id": plan_step_id,
            "message": "worker run id is unavailable",
        }
    store = getattr(octo, "store", None)
    if store is None:
        return {
            "status": "error",
            "run_id": plan_run_id,
            "step_id": plan_step_id,
            "message": "Octo store is unavailable",
        }
    service = PlanRunService(store)
    snapshot = service.get_snapshot(plan_run_id)
    if snapshot is None:
        return {"status": "not_found", "run_id": plan_run_id}
    known_steps = {str(step.get("step_id") or "") for step in snapshot.get("steps") or []}
    if plan_step_id not in known_steps:
        return {"status": "not_found", "run_id": plan_run_id, "step_id": plan_step_id}
    service.bind_worker_step(plan_run_id, plan_step_id, worker_run_id)
    return {
        "status": "ok",
        "run_id": plan_run_id,
        "step_id": plan_step_id,
        "worker_run_id": worker_run_id,
    }


def _skipped_plan_step_binding(args: dict[str, object]) -> dict[str, object] | None:
    plan_run_id = str(args.get("plan_run_id") or "").strip()
    plan_step_id = str(args.get("plan_step_id") or "").strip()
    if not plan_run_id and not plan_step_id:
        return None
    return {
        "status": "skipped",
        "run_id": plan_run_id or None,
        "step_id": plan_step_id or None,
        "message": "worker was not started; plan step was not bound",
    }


def _validate_orchestration_item_binding_request(
    *,
    octo: Octo,
    parent_worker_id: str | None,
    args: dict[str, object],
) -> dict[str, object] | None:
    item_id = str(args.get("orchestration_item_id") or "").strip()
    if not item_id:
        return None
    if not parent_worker_id:
        return {
            "status": "error",
            "item_id": item_id,
            "message": "orchestration_item_id can only be used from a child-spawning worker",
        }
    _, plan = _load_orchestration_output(octo, parent_worker_id)
    if plan is None:
        return {
            "status": "not_found",
            "item_id": item_id,
            "message": "orchestration plan was not found",
        }
    if item_id not in _plan_items_by_id(plan):
        return {
            "status": "not_found",
            "item_id": item_id,
            "message": "orchestration plan item was not found",
        }
    return None


def _bind_orchestration_item_for_child_launch(
    *,
    octo: Octo,
    parent_worker_id: str | None,
    args: dict[str, object],
    worker_run_id: str,
    worker_template_id: str,
) -> dict[str, object] | None:
    item_id = str(args.get("orchestration_item_id") or "").strip()
    if not item_id:
        return None
    if not parent_worker_id or not worker_run_id:
        return {
            "status": "error",
            "item_id": item_id,
            "message": "parent worker id and child worker run id are required",
        }
    return _update_orchestration_item(
        octo=octo,
        worker_id=parent_worker_id,
        item_id=item_id,
        status="awaiting_worker",
        worker_run_id=worker_run_id,
        worker_template_id=worker_template_id,
    )


def sync_orchestration_plan_with_child_batch(
    *,
    octo: Octo,
    parent_worker_id: str,
    child_batch: dict[str, Any],
) -> dict[str, object] | None:
    """Update a parent worker's scoped orchestration plan from child-batch outcomes."""
    if not parent_worker_id or not isinstance(child_batch, dict):
        return None
    _, plan = _load_orchestration_output(octo, parent_worker_id)
    if plan is None:
        return None
    items = plan.get("items")
    if not isinstance(items, list):
        return None
    by_child_id: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        worker_run_id = str(item.get("worker_run_id") or "").strip()
        if worker_run_id:
            by_child_id[worker_run_id] = item
    if not by_child_id:
        return None

    changed = 0
    now = utc_now().isoformat()

    def _apply_outcome(bucket: str, status: str) -> None:
        nonlocal changed
        raw_outcomes = child_batch.get(bucket)
        if not isinstance(raw_outcomes, list):
            return
        for outcome in raw_outcomes:
            if not isinstance(outcome, dict):
                continue
            worker_id = str(outcome.get("worker_id") or "").strip()
            item = by_child_id.get(worker_id)
            if item is None:
                continue
            current_status = str(item.get("status") or "")
            if current_status in _ORCHESTRATION_TERMINAL_STATUSES and current_status == status:
                continue
            item["status"] = status
            item["updated_at"] = now
            if status in _ORCHESTRATION_TERMINAL_STATUSES:
                item["completed_at"] = now
            summary = str(outcome.get("summary") or "").strip()
            if summary:
                item["summary"] = summary
            output = outcome.get("output")
            if isinstance(output, dict):
                item["output"] = output
            error = str(outcome.get("error") or "").strip()
            if error:
                item["error"] = error
            changed += 1

    _apply_outcome("completed", "completed")
    _apply_outcome("failed", "failed")
    _apply_outcome("stopped", "stopped")
    _apply_outcome("missing", "missing")
    _apply_outcome("awaiting_instruction", "awaiting_instruction")

    if not changed:
        return {"status": "unchanged", "updated_count": 0, "plan": plan}
    plan["status"] = _derive_orchestration_plan_status(
        [item for item in items if isinstance(item, dict)]
    )
    saved = _save_orchestration_plan(octo, parent_worker_id, plan)
    return {"status": "ok", "updated_count": changed, "plan": saved}


async def _tool_start_workers_parallel(args: dict[str, object], ctx: dict[str, object]) -> str:
    octo: Octo = ctx["octo"]
    chat_id = int(ctx.get("chat_id") or 0)
    caller_worker = ctx.get("worker")
    child_ctx = _extract_child_context(caller_worker)
    tasks = args.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        return "start_workers_parallel error: tasks must be a non-empty array."
    if len(tasks) > _MAX_PARALLEL_BATCH:
        return f"start_workers_parallel error: max {_MAX_PARALLEL_BATCH} tasks per batch."

    max_parallel = int(args.get("max_parallel") or 3)
    max_parallel = max(1, min(_MAX_PARALLEL_BATCH, max_parallel))
    sem = asyncio.Semaphore(max_parallel)

    async def _launch(item: object, index: int) -> dict[str, object]:
        if not isinstance(item, dict):
            return {"index": index, "status": "error", "error": "task item must be an object"}
        task_text = str(item.get("task", "")).strip()
        if not task_text:
            return {"index": index, "status": "error", "error": "task is required"}

        worker_id = str(item.get("worker_id", "")).strip()
        inputs = item.get("inputs") if isinstance(item.get("inputs"), dict) else {}
        tools = item.get("tools") if isinstance(item.get("tools"), list) else None
        timeout_seconds = int(item.get("timeout_seconds")) if item.get("timeout_seconds") else None
        required_tools = _normalize_str_list(item.get("required_tools"))
        required_permissions = _normalize_str_list(item.get("required_permissions"))
        required_tool_calls = _normalize_tool_name_list(item.get("required_tool_calls"))

        resolution = _resolve_worker_for_start(
            octo=octo,
            worker_id=worker_id,
            required_tools=_merge_tool_requirements(required_tools, required_tool_calls),
            required_permissions=required_permissions,
        )
        if isinstance(resolution, str):
            return {"index": index, "status": "error", "error": resolution}

        selected_worker_id = str(resolution["worker_id"])
        template = resolution["template"]
        tool_validation_error = _validate_requested_worker_tools(
            requested_tools=tools,
            template_tools=getattr(template, "available_tools", []),
            error_prefix="start_workers_parallel error",
        )
        if tool_validation_error:
            return {"index": index, "status": "error", "error": tool_validation_error}
        tool_validation_error = _validate_required_tool_calls_available(
            required_tool_calls=required_tool_calls,
            requested_tools=tools,
            template_tools=getattr(template, "available_tools", []),
            error_prefix="start_workers_parallel error",
        )
        if tool_validation_error:
            return {"index": index, "status": "error", "error": tool_validation_error}
        plan_binding_error = _validate_plan_step_binding_request(octo=octo, args=item)
        if plan_binding_error:
            return {
                "index": index,
                "status": "error",
                "error": plan_binding_error.get("message") or "plan binding is invalid",
                "plan_binding": plan_binding_error,
            }
        if child_ctx is not None:
            policy_error = _validate_child_spawn_policy(
                octo=octo,
                parent_ctx=child_ctx,
                child_template=template,
                explicit_worker_id=selected_worker_id,
            )
            if policy_error:
                return {"index": index, "status": "error", "error": policy_error}
        orchestration_binding_error = _validate_orchestration_item_binding_request(
            octo=octo,
            parent_worker_id=child_ctx["run_id"] if child_ctx else None,
            args=item,
        )
        if orchestration_binding_error:
            return {
                "index": index,
                "status": "error",
                "error": orchestration_binding_error.get("message")
                or "orchestration plan binding is invalid",
                "orchestration_binding": orchestration_binding_error,
            }
        async with sem:
            launch = await octo._start_worker_async(
                worker_id=selected_worker_id,
                task=task_text,
                chat_id=chat_id,
                inputs=inputs,
                tools=tools,
                model=None,
                timeout_seconds=timeout_seconds,
                scheduled_task_id=None,
                required_tool_calls=required_tool_calls,
                parent_worker_id=child_ctx["run_id"] if child_ctx else None,
                lineage_id=child_ctx["lineage_id"] if child_ctx else None,
                root_task_id=child_ctx["root_task_id"] if child_ctx else None,
                spawn_depth=(child_ctx["spawn_depth"] + 1) if child_ctx else 0,
                allowed_paths=(
                    item.get("allowed_paths")
                    if "allowed_paths" in item
                    else _infer_allowed_paths_from_values(task_text, inputs)
                ),
            )

        status = str(launch.get("status", "started"))
        worker_run_id = str(launch.get("worker_id") or launch.get("run_id") or "").strip()
        plan_binding = (
            _bind_plan_step_for_worker_launch(
                octo=octo,
                args=item,
                worker_run_id=worker_run_id,
            )
            if status == "started"
            else _skipped_plan_step_binding(item)
        )
        orchestration_binding = (
            _bind_orchestration_item_for_child_launch(
                octo=octo,
                parent_worker_id=child_ctx["run_id"] if child_ctx else None,
                args=item,
                worker_run_id=worker_run_id,
                worker_template_id=selected_worker_id,
            )
            if status == "started"
            else None
        )
        return {
            "index": index,
            "status": status,
            "worker_id": launch.get("worker_id"),
            "run_id": launch.get("run_id"),
            "worker_template_id": selected_worker_id,
            "worker_template_name": getattr(template, "name", selected_worker_id),
            "lineage_id": launch.get("lineage_id"),
            "parent_worker_id": launch.get("parent_worker_id"),
            "root_task_id": launch.get("root_task_id"),
            "spawn_depth": launch.get("spawn_depth"),
            **({"plan_binding": plan_binding} if plan_binding else {}),
            **({"orchestration_binding": orchestration_binding} if orchestration_binding else {}),
        }

    launches = await asyncio.gather(*[_launch(item, idx) for idx, item in enumerate(tasks)])
    started = sum(
        1 for item in launches if str(item.get("status")) in {"started", "skipped_duplicate"}
    )
    failed = len(launches) - started
    followup_required = started > 0
    return json.dumps(
        {
            "status": "ok" if failed == 0 else "partial",
            "started_count": started,
            "failed_count": failed,
            "max_parallel": max_parallel,
            "launches": launches,
            "followup_required": followup_required,
            "next_best_action": (
                "wait_for_worker_progress" if followup_required else "continue_current_plan"
            ),
        },
        ensure_ascii=False,
    )


def _tool_synthesize_worker_results(args: dict[str, object], ctx: dict[str, object]) -> str:
    octo: Octo = ctx["octo"]
    worker_ids = _normalize_str_list(args.get("worker_ids"))
    if not worker_ids:
        return "synthesize_worker_results error: worker_ids is required."

    completed: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []
    pending: list[dict[str, object]] = []
    missing: list[str] = []
    summary_hashes: set[str] = set()
    ready_progress: list[dict[str, object]] = []
    pending_progress: list[dict[str, object]] = []
    failed_progress: list[dict[str, object]] = []
    missing_progress: list[str] = []

    for wid in worker_ids:
        worker = octo.store.get_worker(wid)
        if not worker:
            missing.append(wid)
            missing_progress.append(wid)
            continue
        status = str(worker.status)
        if status == "completed":
            summary = str(worker.summary or "").strip()
            output_context = summarize_worker_output_for_context(
                worker.output,
                budget=SYNTHESIZE_WORKER_OUTPUT_CONTEXT_BUDGET,
            )
            item = {
                "worker_id": wid,
                "summary": summary,
                "output": output_context.output,
                "output_truncated": output_context.output_truncated,
                "output_preview_text": output_context.output_preview_text,
                "available_keys": output_context.available_keys,
                "output_chars": output_context.output_chars,
                **_worker_timing_fields(worker),
            }
            completed.append(item)
            ready_progress.append(
                {
                    "worker_id": wid,
                    "summary_hash": (
                        hashlib.sha256(summary.encode("utf-8")).hexdigest() if summary else None
                    ),
                }
            )
            if summary:
                summary_hashes.add(hashlib.sha256(summary.encode("utf-8")).hexdigest())
        elif status == "failed":
            error = str(worker.error or "Unknown error")
            item = {
                "worker_id": wid,
                "error": error,
                **_worker_timing_fields(worker),
            }
            failed.append(item)
            failed_progress.append({"worker_id": wid, "error": error})
        else:
            item = {
                "worker_id": wid,
                "status": status,
                **_worker_timing_fields(worker),
            }
            pending.append(item)
            pending_progress.append({"worker_id": wid, "status": status})

    synthesis_lines: list[str] = []
    can_synthesize = len(completed) > 0
    if can_synthesize:
        synthesis_lines.append("Completed worker findings:")
        for item in completed:
            synthesis_lines.append(f"- {item['worker_id']}: {item['summary'] or 'No summary'}")
    elif pending:
        synthesis_lines.append(
            "No completed worker results are ready yet. Do not synthesize yet; wait for worker progress."
        )
    elif failed:
        synthesis_lines.append(
            "No completed worker results are available. Inspect the worker failures instead."
        )
    elif missing:
        synthesis_lines.append(
            "No completed worker results are available. Some worker IDs could not be found."
        )
    else:
        synthesis_lines.append("No completed worker results are available yet.")
    if failed:
        synthesis_lines.append("Failed workers:")
        for item in failed:
            synthesis_lines.append(f"- {item['worker_id']}: {item['error']}")
    if pending:
        synthesis_lines.append("Pending workers:")
        for item in pending:
            synthesis_lines.append(f"- {item['worker_id']}: {item['status']}")
    if missing:
        synthesis_lines.append("Unknown worker IDs:")
        for wid in missing:
            synthesis_lines.append(f"- {wid}")

    conflicting = len(summary_hashes) > 1
    if conflicting:
        synthesis_lines.append(
            "Potential conflict detected: completed workers reported different summaries."
        )

    status = "ready"
    next_best_action = "continue_current_plan"
    followup_required = False
    if not can_synthesize and pending:
        status = "pending"
        next_best_action = "wait_for_worker_progress"
        followup_required = True
    elif can_synthesize and pending:
        status = "partial"
        next_best_action = "synthesize_ready_results"
        followup_required = True
    elif can_synthesize:
        status = "ready"
        next_best_action = "synthesize_ready_results"
    elif failed:
        status = "failed_only"
        next_best_action = "inspect_worker_failures"
    elif missing:
        status = "missing"
        next_best_action = "verify_worker_ids"
    else:
        status = "idle"

    progress_signature = hashlib.sha256(
        json.dumps(
            {
                "worker_ids": worker_ids,
                "ready": ready_progress,
                "pending": pending_progress,
                "failed": failed_progress,
                "missing": missing_progress,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    return json.dumps(
        {
            "status": status,
            "worker_ids": worker_ids,
            "completed_count": len(completed),
            "failed_count": len(failed),
            "pending_count": len(pending),
            "missing_count": len(missing),
            "can_synthesize": can_synthesize,
            "followup_required": followup_required,
            "next_best_action": next_best_action,
            "progress_signature": progress_signature,
            "conflicting_summaries": conflicting,
            "synthesis": "\n".join(synthesis_lines),
            "ready_results": completed,
            "failed_results": failed,
            "pending_results": pending,
            "missing_results": missing,
            "completed": completed,
            "failed": failed,
            "pending": pending,
            "missing": missing,
        },
        ensure_ascii=False,
    )


async def _tool_stop_worker(args: dict[str, object], ctx: dict[str, object]) -> str:
    octo: Octo = ctx["octo"]
    worker_id = str(args.get("worker_id", "")).strip()
    if not worker_id:
        return "stop_worker error: worker_id is required."
    stopped = await octo.runtime.stop_worker(worker_id)
    return json.dumps(
        {"status": "stopped" if stopped else "not_found", "worker_id": worker_id},
        ensure_ascii=False,
    )


async def _tool_answer_worker_instruction(args: dict[str, object], ctx: dict[str, object]) -> str:
    octo: Octo = ctx["octo"]
    worker_id = str(args.get("worker_id", "")).strip()
    request_id = str(args.get("request_id", "") or "").strip()
    instruction = str(args.get("instruction", "") or "").strip()
    if not worker_id:
        return "answer_worker_instruction error: worker_id is required."
    if not instruction:
        return "answer_worker_instruction error: instruction is required."

    worker = octo.store.get_worker(worker_id)
    if worker is None:
        return json.dumps({"status": "not_found", "worker_id": worker_id}, ensure_ascii=False)
    answerer_worker_id = _answerer_worker_id(ctx)
    if answerer_worker_id is not None and not _is_direct_child_worker(
        worker,
        parent_worker_id=answerer_worker_id,
    ):
        return json.dumps(
            {
                "status": "unauthorized",
                "worker_id": worker_id,
                "message": "Only a worker's direct parent can answer its instruction request.",
            },
            ensure_ascii=False,
        )
    if not request_id:
        request = _extract_worker_instruction_request(worker.output)
        request_id = str(request.get("request_id") or "").strip() if request else ""
    if not request_id:
        return json.dumps(
            {
                "status": "missing_request_id",
                "worker_id": worker_id,
                "message": "No active instruction_request found for this worker.",
            },
            ensure_ascii=False,
        )

    runtime = getattr(octo, "runtime", None)
    if runtime is None or not hasattr(runtime, "answer_instruction"):
        return json.dumps(
            {"status": "runtime_unavailable", "worker_id": worker_id, "request_id": request_id},
            ensure_ascii=False,
        )
    answered = await runtime.answer_instruction(
        worker_id=worker_id,
        request_id=request_id,
        instruction=instruction,
        answerer_worker_id=answerer_worker_id,
    )
    return json.dumps(
        {
            "status": "answered" if answered else "not_waiting",
            "worker_id": worker_id,
            "request_id": request_id,
        },
        ensure_ascii=False,
    )


def _answerer_worker_id(ctx: dict[str, object]) -> str | None:
    caller_worker = ctx.get("worker")
    spec = getattr(caller_worker, "spec", None)
    if spec is None:
        return None
    answerer_id = str(getattr(spec, "run_id", "") or getattr(spec, "id", "") or "").strip()
    return answerer_id or None


def _is_direct_child_worker(worker: object, *, parent_worker_id: str) -> bool:
    return str(getattr(worker, "parent_worker_id", "") or "").strip() == parent_worker_id


def _tool_get_worker_status(args: dict[str, object], ctx: dict[str, object]) -> str:
    octo: Octo = ctx["octo"]
    worker_id = str(args.get("worker_id", "")).strip()
    if not worker_id:
        return "get_worker_status error: worker_id is required."

    worker = octo.store.get_worker(worker_id)
    if not worker:
        return json.dumps(
            {
                "status": "not_found",
                "worker_id": worker_id,
                "message": "Worker not found. It may be from an old conversation or never existed.",
            },
            ensure_ascii=False,
        )
    worker = _reconcile_stale_worker_status(octo, worker)

    payload = {
        "status": worker.status,
        "worker_id": worker.id,
        "task": worker.task,
        "lineage_id": worker.lineage_id,
        "parent_worker_id": worker.parent_worker_id,
        "root_task_id": worker.root_task_id,
        "spawn_depth": worker.spawn_depth,
        "summary": worker.summary,
        "error": worker.error,
    }
    instruction_request = _extract_worker_instruction_request(worker.output)
    if instruction_request is not None:
        payload["instruction_request"] = instruction_request
    payload.update(_worker_timing_fields(worker))
    return json.dumps(payload, ensure_ascii=False)


def _tool_list_active_workers(args: dict[str, object], ctx: dict[str, object]) -> str:
    octo: Octo = ctx["octo"]
    older_than_minutes = int(args.get("older_than_minutes") or 10)

    workers = octo.store.get_active_workers(older_than_minutes=older_than_minutes)
    workers = _reconcile_stale_active_workers(octo, workers, older_than_minutes=older_than_minutes)
    worker_list = []
    for w in workers:
        worker_list.append(
            {
                "worker_id": w.id,
                "status": w.status,
                "task": w.task,
                "lineage_id": w.lineage_id,
                "parent_worker_id": w.parent_worker_id,
                "root_task_id": w.root_task_id,
                "spawn_depth": w.spawn_depth,
                "created_at": w.created_at.isoformat(),
                "updated_at": w.updated_at.isoformat(),
                "summary": w.summary,
                "error": w.error,
            }
        )

    return json.dumps(
        {
            "count": len(worker_list),
            "workers": worker_list,
        },
        ensure_ascii=False,
    )


def _tool_worker_session_status(args: dict[str, object], ctx: dict[str, object]) -> str:
    octo: Octo = ctx["octo"]
    older_than_minutes = int(args.get("older_than_minutes") or 10)
    recent_limit = max(1, min(50, int(args.get("recent_limit") or 12)))

    active_workers = octo.store.get_active_workers(older_than_minutes=older_than_minutes)
    active_workers = _reconcile_stale_active_workers(
        octo, active_workers, older_than_minutes=older_than_minutes
    )
    recent_workers = (
        octo.store.list_recent_workers(recent_limit)
        if hasattr(octo.store, "list_recent_workers")
        else octo.store.list_workers()[:recent_limit]
    )

    counts: dict[str, int] = {}
    lineage_counts: dict[str, int] = {}
    for worker in active_workers:
        status = str(worker.status or "unknown")
        counts[status] = counts.get(status, 0) + 1
        lineage_key = str(worker.lineage_id or "standalone")
        lineage_counts[lineage_key] = lineage_counts.get(lineage_key, 0) + 1

    recent_summary: list[dict[str, object]] = []
    for worker in recent_workers[:recent_limit]:
        recent_summary.append(
            {
                "worker_id": worker.id,
                "status": worker.status,
                "task": worker.task,
                "updated_at": worker.updated_at.isoformat(),
                "lineage_id": worker.lineage_id,
                "parent_worker_id": worker.parent_worker_id,
                "spawn_depth": worker.spawn_depth,
                "summary": worker.summary,
                "error": worker.error,
            }
        )

    active_lineages = [
        {"lineage_id": key, "active_workers": count}
        for key, count in sorted(lineage_counts.items(), key=lambda item: (-item[1], item[0]))
    ]

    hints: list[str] = []
    running = (
        counts.get("running", 0)
        + counts.get("started", 0)
        + counts.get("waiting_for_children", 0)
        + counts.get("awaiting_instruction", 0)
    )
    failed_recent = sum(1 for worker in recent_workers if str(worker.status) == "failed")
    if running > 0:
        hints.append(f"{running} worker(s) currently in flight.")
    if failed_recent > 0:
        hints.append(
            f"{failed_recent} recent worker run(s) failed; inspect summaries before retrying."
        )
    if any((worker.spawn_depth or 0) > 0 for worker in active_workers):
        hints.append(
            "Active child-worker lineage detected; prefer synthesis or status checks before spawning more."
        )
    if not hints:
        hints.append("Worker fabric looks quiet and healthy.")

    return json.dumps(
        {
            "status": "ok",
            "older_than_minutes": older_than_minutes,
            "recent_limit": recent_limit,
            "active_count": len(active_workers),
            "status_counts": counts,
            "active_lineages": active_lineages,
            "recent_workers": recent_summary,
            "hints": hints,
        },
        ensure_ascii=False,
    )


def _tool_worker_yield(args: dict[str, object], ctx: dict[str, object]) -> str:
    octo: Octo = ctx["octo"]
    older_than_minutes = int(args.get("older_than_minutes") or 10)
    requested_worker_ids = _normalize_str_list(args.get("worker_ids"))
    lineage_id = str(args.get("lineage_id", "") or "").strip() or None

    workers = _select_workers_for_yield(
        octo=octo,
        requested_worker_ids=requested_worker_ids,
        lineage_id=lineage_id,
        older_than_minutes=older_than_minutes,
    )
    if not workers:
        return json.dumps(
            {
                "status": "idle",
                "mode": "resume",
                "followup_required": False,
                "message": "No matching active worker runs found. Continue normally.",
                "requested_worker_ids": requested_worker_ids,
                "lineage_id": lineage_id,
                "pending_workers": [],
                "completed_workers": [],
                "failed_workers": [],
                "next_best_action": "continue_current_plan",
                "hints": ["Worker fabric is quiet; there is nothing to wait on right now."],
            },
            ensure_ascii=False,
        )

    pending_workers: list[dict[str, object]] = []
    completed_workers: list[dict[str, object]] = []
    failed_workers: list[dict[str, object]] = []

    for worker in workers:
        payload = _serialize_worker_run(worker)
        status = str(getattr(worker, "status", "") or "").strip().lower()
        if status in {"completed"}:
            completed_workers.append(payload)
        elif status in {"failed", "stopped"}:
            failed_workers.append(payload)
        else:
            pending_workers.append(payload)

    followup_required = len(pending_workers) > 0
    all_requested_resolved = bool(requested_worker_ids) and len(workers) == len(
        {str(worker_id).strip() for worker_id in requested_worker_ids if str(worker_id).strip()}
    )
    synthesize_recommended = len(completed_workers) >= 2 and not pending_workers
    collect_results_recommended = len(completed_workers) >= 1 and not synthesize_recommended

    hints: list[str] = []
    if pending_workers:
        hints.append(
            f"{len(pending_workers)} worker run(s) are still in flight; yield and return when they finish."
        )
    if completed_workers:
        hints.append(
            f"{len(completed_workers)} worker run(s) have usable results ready for collection."
        )
    if failed_workers:
        hints.append(
            f"{len(failed_workers)} worker run(s) failed or stopped; inspect summaries before retrying."
        )
    if lineage_id and pending_workers:
        hints.append(
            "Focused lineage still has active children; avoid spawning more work in the same tree."
        )
    if synthesize_recommended:
        hints.append("All requested runs are done; synthesis is the cleanest next step.")
    elif collect_results_recommended:
        hints.append("Result collection is ready; fetch the completed worker output now.")
    elif not hints:
        hints.append("No pending worker work remains; continue with the current plan.")

    next_best_action = "continue_current_plan"
    mode = "resume"
    message = "No active worker waiting is needed."
    if pending_workers:
        next_best_action = "wait_for_worker_progress"
        mode = "yield"
        message = f"Yield now. {len(pending_workers)} worker run(s) are still running."
    elif synthesize_recommended:
        next_best_action = "synthesize_worker_results"
        message = "Parallel worker runs are done. Synthesize their results now."
    elif collect_results_recommended:
        next_best_action = "get_worker_result"
        message = "A worker result is ready. Collect the completed output now."
    elif failed_workers:
        next_best_action = "inspect_worker_failures"
        message = "No more active work is running. Inspect the failed worker summaries."

    return json.dumps(
        {
            "status": "ok",
            "mode": mode,
            "followup_required": followup_required,
            "message": message,
            "requested_worker_ids": requested_worker_ids,
            "all_requested_resolved": all_requested_resolved,
            "lineage_id": lineage_id,
            "pending_count": len(pending_workers),
            "completed_count": len(completed_workers),
            "failed_count": len(failed_workers),
            "pending_workers": pending_workers,
            "completed_workers": completed_workers,
            "failed_workers": failed_workers,
            "next_best_action": next_best_action,
            "synthesize_recommended": synthesize_recommended,
            "collect_results_recommended": collect_results_recommended,
            "hints": hints,
        },
        ensure_ascii=False,
    )


def _reconcile_stale_worker_status(octo: Octo, worker: Any) -> Any:
    runtime = getattr(octo, "runtime", None)
    if not runtime or not hasattr(runtime, "is_worker_running"):
        return worker
    if worker.status not in {"started", "running", "waiting_for_children", "awaiting_instruction"}:
        return worker
    # Small grace window avoids false stale marks during process launch transitions.
    if worker.updated_at >= (utc_now() - timedelta(minutes=2)):
        return worker
    if runtime.is_worker_running(worker.id):
        return worker
    octo.store.update_worker_status(worker.id, "stopped")
    octo.store.update_worker_result(
        worker.id,
        error="Worker process not found in runtime; stale running state reconciled.",
    )
    refreshed = octo.store.get_worker(worker.id)
    return refreshed or worker


def _reconcile_stale_active_workers(
    octo: Octo, workers: list[Any], older_than_minutes: int
) -> list[Any]:
    stale_ids: list[str] = []
    runtime = getattr(octo, "runtime", None)
    if not runtime or not hasattr(runtime, "is_worker_running"):
        return workers
    grace_cutoff = utc_now() - timedelta(minutes=2)
    for worker in workers:
        if worker.status not in {
            "started",
            "running",
            "waiting_for_children",
            "awaiting_instruction",
        }:
            continue
        if worker.updated_at >= grace_cutoff:
            continue
        if runtime.is_worker_running(worker.id):
            continue
        octo.store.update_worker_status(worker.id, "stopped")
        octo.store.update_worker_result(
            worker.id,
            error="Worker process not found in runtime; stale running state reconciled.",
        )
        stale_ids.append(worker.id)
    if not stale_ids:
        return workers
    return octo.store.get_active_workers(older_than_minutes=older_than_minutes)


def _select_workers_for_yield(
    *,
    octo: Octo,
    requested_worker_ids: list[str],
    lineage_id: str | None,
    older_than_minutes: int,
) -> list[Any]:
    workers_by_id: dict[str, Any] = {}

    for worker_id in requested_worker_ids:
        worker = octo.store.get_worker(worker_id)
        if not worker:
            continue
        worker = _reconcile_stale_worker_status(octo, worker)
        workers_by_id[str(worker.id)] = worker

    if lineage_id:
        active_workers = octo.store.get_active_workers(older_than_minutes=older_than_minutes)
        active_workers = _reconcile_stale_active_workers(
            octo,
            active_workers,
            older_than_minutes=older_than_minutes,
        )
        for worker in active_workers:
            if str(getattr(worker, "lineage_id", "") or "").strip() != lineage_id:
                continue
            workers_by_id[str(worker.id)] = worker

        recent_workers = (
            octo.store.list_recent_workers(50)
            if hasattr(octo.store, "list_recent_workers")
            else octo.store.list_workers()[:50]
        )
        for worker in recent_workers:
            if str(getattr(worker, "lineage_id", "") or "").strip() != lineage_id:
                continue
            workers_by_id.setdefault(str(worker.id), worker)

    if not requested_worker_ids and not lineage_id:
        active_workers = octo.store.get_active_workers(older_than_minutes=older_than_minutes)
        active_workers = _reconcile_stale_active_workers(
            octo,
            active_workers,
            older_than_minutes=older_than_minutes,
        )
        for worker in active_workers:
            workers_by_id[str(worker.id)] = worker

    workers = list(workers_by_id.values())
    workers.sort(key=lambda worker: str(getattr(worker, "updated_at", "") or ""), reverse=True)
    return workers


def _serialize_worker_run(worker: Any) -> dict[str, object]:
    payload: dict[str, object] = {
        "worker_id": getattr(worker, "id", None),
        "status": getattr(worker, "status", None),
        "task": getattr(worker, "task", None),
        "lineage_id": getattr(worker, "lineage_id", None),
        "parent_worker_id": getattr(worker, "parent_worker_id", None),
        "spawn_depth": getattr(worker, "spawn_depth", None),
        "updated_at": (
            getattr(worker, "updated_at", None).isoformat()
            if getattr(worker, "updated_at", None) is not None
            else None
        ),
        "summary": getattr(worker, "summary", None),
        "error": getattr(worker, "error", None),
    }
    instruction_request = _extract_worker_instruction_request(getattr(worker, "output", None))
    if instruction_request is not None:
        payload["instruction_request"] = instruction_request
    return payload


def _extract_worker_instruction_request(output: Any) -> dict[str, object] | None:
    if not isinstance(output, dict):
        return None
    request = output.get("instruction_request")
    if not isinstance(request, dict):
        return None
    request_id = str(request.get("request_id") or "").strip()
    question = str(request.get("question") or "").strip()
    if not request_id or not question:
        return None
    return dict(request)


def _worker_timing_fields(worker: Any) -> dict[str, object]:
    now = utc_now()
    created_at = getattr(worker, "created_at", None)
    updated_at = getattr(worker, "updated_at", None)
    payload: dict[str, object] = {
        "created_at": created_at.isoformat() if created_at is not None else None,
        "updated_at": updated_at.isoformat() if updated_at is not None else None,
        "runtime_seconds": None,
        "seconds_since_update": None,
    }
    if created_at is not None:
        payload["runtime_seconds"] = max(0, int((now - created_at).total_seconds()))
    if updated_at is not None:
        payload["seconds_since_update"] = max(0, int((now - updated_at).total_seconds()))
    return payload


def _tool_get_worker_result(args: dict[str, object], ctx: dict[str, object]) -> str:
    octo: Octo = ctx["octo"]
    worker_id = str(args.get("worker_id", "")).strip()
    if not worker_id:
        return "get_worker_result error: worker_id is required."

    worker = octo.store.get_worker(worker_id)
    if not worker:
        return json.dumps(
            {"status": "not_found", "worker_id": worker_id, "message": "Worker not found."},
            ensure_ascii=False,
        )

    if worker.status == "completed":
        payload = {
            "status": "completed",
            "worker_id": worker.id,
            "lineage_id": worker.lineage_id,
            "parent_worker_id": worker.parent_worker_id,
            "root_task_id": worker.root_task_id,
            "spawn_depth": worker.spawn_depth,
            "summary": worker.summary,
            "output": worker.output,
        }
        payload.update(_worker_timing_fields(worker))
        return json.dumps(payload, ensure_ascii=False)
    elif worker.status == "failed":
        payload = {
            "status": "failed",
            "worker_id": worker.id,
            "lineage_id": worker.lineage_id,
            "parent_worker_id": worker.parent_worker_id,
            "root_task_id": worker.root_task_id,
            "spawn_depth": worker.spawn_depth,
            "summary": worker.summary,
            "error": worker.error or "Unknown error",
            "output": worker.output,
        }
        payload.update(_worker_timing_fields(worker))
        return json.dumps(payload, ensure_ascii=False)
    else:
        waiting_message = f"Worker is still {worker.status}. Result not available yet."
        if worker.status == "waiting_for_children":
            waiting_message = "Worker is waiting for child workers to finish before resuming."
        elif worker.status == "awaiting_instruction":
            waiting_message = "Worker is waiting for an instruction before resuming."
        payload = {
            "status": worker.status,
            "worker_id": worker.id,
            "lineage_id": worker.lineage_id,
            "parent_worker_id": worker.parent_worker_id,
            "root_task_id": worker.root_task_id,
            "spawn_depth": worker.spawn_depth,
            "message": waiting_message,
        }
        instruction_request = _extract_worker_instruction_request(worker.output)
        if instruction_request is not None:
            payload["instruction_request"] = instruction_request
        payload.update(_worker_timing_fields(worker))
        return json.dumps(payload, ensure_ascii=False)


def _tool_get_worker_output_path(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Retrieve a specific part of a worker's output using a dotted path."""
    octo: Octo = ctx["octo"]
    worker_id = str(args.get("worker_id", "")).strip()
    path = str(args.get("path", "")).strip()

    if not worker_id:
        return "get_worker_output_path error: worker_id is required."
    if not path:
        return "get_worker_output_path error: path is required."

    worker = octo.store.get_worker(worker_id)
    if not worker:
        return json.dumps({"status": "not_found", "worker_id": worker_id}, ensure_ascii=False)

    if worker.status != "completed":
        return json.dumps(
            {"status": worker.status, "message": "Worker result not available."}, ensure_ascii=False
        )

    output = worker.output or {}
    current = output
    parts = path.split(".")

    for part in parts:
        if isinstance(current, dict):
            if part in current:
                current = current[part]
            else:
                return json.dumps(
                    {"error": f"Path not found: {path} (missing key '{part}')"}, ensure_ascii=False
                )
        elif isinstance(current, list):
            try:
                idx = int(part)
                if 0 <= idx < len(current):
                    current = current[idx]
                else:
                    return json.dumps(
                        {"error": f"Path not found: {path} (index '{idx}' out of range)"},
                        ensure_ascii=False,
                    )
            except ValueError:
                return json.dumps(
                    {"error": f"Path not found: {path} (expected index for list, got '{part}')"},
                    ensure_ascii=False,
                )
        else:
            return json.dumps(
                {
                    "error": f"Path not found: {path} (cannot traverse into non-container type at '{part}')"
                },
                ensure_ascii=False,
            )

    return json.dumps({"worker_id": worker_id, "path": path, "value": current}, ensure_ascii=False)


def _tool_propose_knowledge(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Propose a fact or insight for the canonical memory."""
    category = str(args.get("category", "fact")).lower()
    content = str(args.get("content", ""))
    worker = ctx.get("worker")

    if not content:
        return "Error: Content is required."

    if hasattr(worker, "add_proposal"):
        worker.add_proposal(category, content)
        return f"Proposal logged: [{category}] {content}"

    # Fallback if not running in a worker context with the new SDK
    return f"Proposal logged (text-only): [{category}] {content}"


def _is_valid_worker_id(worker_id: str) -> bool:
    return bool(_WORKER_ID_PATTERN.fullmatch(worker_id))


def _resolve_worker_dir(base_dir: Path, worker_id: str) -> Path | None:
    base = base_dir.resolve()
    workers_root = (base / "workers").resolve()
    candidate = (workers_root / worker_id).resolve()
    try:
        candidate.relative_to(workers_root)
    except ValueError:
        return None
    return candidate


def _extract_child_context(worker_obj: object) -> dict[str, Any] | None:
    if worker_obj is None or not hasattr(worker_obj, "spec"):
        return None
    spec = getattr(worker_obj, "spec", None)
    run_id = str(getattr(spec, "run_id", "") or getattr(spec, "id", "")).strip()
    if not run_id:
        return None
    spawn_depth = int(getattr(spec, "spawn_depth", 0) or 0)
    lineage_id = str(getattr(spec, "lineage_id", "") or run_id).strip()
    root_task_id = str(getattr(spec, "root_task_id", "") or run_id).strip()
    parent_template_id = str(getattr(spec, "template_id", "")).strip()
    effective_permissions = _normalize_str_list(getattr(spec, "effective_permissions", []))
    return {
        "run_id": run_id,
        "spawn_depth": spawn_depth,
        "lineage_id": lineage_id,
        "root_task_id": root_task_id,
        "template_id": parent_template_id,
        "effective_permissions": effective_permissions,
    }


def _validate_child_spawn_policy(
    *,
    octo: Octo,
    parent_ctx: dict[str, Any],
    child_template: object,
    explicit_worker_id: str,
) -> str | None:
    parent_template_id = str(parent_ctx.get("template_id", "")).strip()
    if not parent_template_id:
        return "start_child_worker error: parent worker template is unknown; cannot spawn children."
    parent_template = octo.store.get_worker_template(parent_template_id)
    if not parent_template:
        return f"start_child_worker error: parent template '{parent_template_id}' not found."

    can_spawn = bool(getattr(parent_template, "can_spawn_children", False))
    if not can_spawn:
        return (
            f"start_child_worker error: parent template '{parent_template_id}' cannot spawn children "
            "(set can_spawn_children=true)."
        )

    allowed = set(_normalize_str_list(getattr(parent_template, "allowed_child_templates", [])))
    child_template_id = str(getattr(child_template, "id", explicit_worker_id)).strip()
    if child_template_id not in allowed:
        return (
            f"start_child_worker error: child template '{child_template_id}' is not allowed by parent template "
            f"'{parent_template_id}'."
        )

    return None


def _resolve_worker_for_start(
    *,
    octo: Octo,
    worker_id: str,
    required_tools: list[str] | None = None,
    required_permissions: list[str] | None = None,
) -> dict[str, object] | str:
    if not worker_id:
        return "start_worker error: worker_id is required. Use list_workers to choose an executor."
    if worker_id.lower() in {"auto", "best", "router"}:
        return (
            "start_worker error: automatic worker routing is disabled. "
            "Use list_workers to choose a specific worker_id."
        )

    template = octo.store.get_worker_template(worker_id)
    if not template:
        return f"start_worker error: worker '{worker_id}' not found. Use list_workers to see available workers."
    requirement_error = _validate_template_requirements(
        template,
        required_tools=required_tools,
        required_permissions=required_permissions,
        error_prefix="start_worker error",
    )
    if requirement_error:
        return requirement_error
    return {
        "template": template,
        "worker_id": worker_id,
    }


def _normalize_str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _normalize_worker_permissions(value: object) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in _normalize_str_list(value):
        lowered = item.lower()
        canonical = _WORKER_PERMISSION_ALIASES.get(lowered, lowered)
        if canonical in seen:
            continue
        seen.add(canonical)
        normalized.append(canonical)
    return normalized


def _normalize_tool_name_list(value: object) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for item in _normalize_str_list(value):
        tool_name = item.lower()
        if tool_name in seen:
            continue
        seen.add(tool_name)
        normalized.append(tool_name)
    return normalized


def _merge_tool_requirements(*values: list[str] | None) -> list[str]:
    seen: set[str] = set()
    merged: list[str] = []
    for value in values:
        for item in value or []:
            tool_name = str(item).strip().lower()
            if not tool_name or tool_name in seen:
                continue
            seen.add(tool_name)
            merged.append(tool_name)
    return merged


def _validate_template_requirements(
    template: object,
    *,
    required_tools: list[str] | None,
    required_permissions: list[str] | None,
    error_prefix: str,
) -> str | None:
    available_tools = {str(t).lower() for t in getattr(template, "available_tools", [])}
    permissions = set(_normalize_worker_permissions(getattr(template, "required_permissions", [])))
    missing_tools = [tool for tool in (required_tools or []) if tool.lower() not in available_tools]
    missing_permissions = [
        permission
        for permission in _normalize_worker_permissions(required_permissions or [])
        if permission not in permissions
    ]
    if missing_tools:
        return (
            f"{error_prefix}: worker '{getattr(template, 'id', '')}' does not provide required "
            f"tool(s): {', '.join(missing_tools)}."
        )
    if missing_permissions:
        return (
            f"{error_prefix}: worker '{getattr(template, 'id', '')}' does not provide required "
            f"permission(s): {', '.join(missing_permissions)}."
        )
    return None


def _effective_template_tool_names(value: object) -> list[str]:
    return [
        tool_name
        for tool_name in _normalize_tool_name_list(value)
        if tool_name not in _WORKER_BLOCKED_TOOL_NAMES
    ]


def _validate_requested_worker_tools(
    *,
    requested_tools: object,
    template_tools: object,
    error_prefix: str,
) -> str | None:
    if requested_tools is None:
        return None

    normalized_requested = _normalize_tool_name_list(requested_tools)
    allowed_tools = set(_effective_template_tool_names(template_tools))
    unexpected = sorted(
        tool_name for tool_name in normalized_requested if tool_name not in allowed_tools
    )
    if unexpected:
        return (
            f"{error_prefix}: requested tools exceed template contract "
            f"({', '.join(unexpected)}). Update the worker template instead."
        )
    return None


def _validate_required_tool_calls_available(
    *,
    required_tool_calls: list[str],
    requested_tools: object,
    template_tools: object,
    error_prefix: str,
) -> str | None:
    if not required_tool_calls:
        return None
    effective_tools = (
        _normalize_tool_name_list(requested_tools)
        if requested_tools is not None
        else _effective_template_tool_names(template_tools)
    )
    available = set(effective_tools)
    missing = sorted(tool_name for tool_name in required_tool_calls if tool_name not in available)
    if missing:
        return (
            f"{error_prefix}: required tool call(s) are not available in this worker run "
            f"({', '.join(missing)}). Include them in tools or choose another worker template."
        )
    return None
