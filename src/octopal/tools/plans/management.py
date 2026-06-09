from __future__ import annotations

import json
from typing import Any

from octopal.runtime.plans import PlanRunService
from octopal.tools.registry import ToolSpec
from octopal.utils import utc_now

_PLAN_STEP_KINDS = ["octo", "tool", "worker", "approval", "input", "final"]


def get_plan_tools() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="plan_create",
            description=(
                "Create a durable runtime plan for a concrete user task. Use this when the user asks "
                "for actions that require multiple steps, workers, tools, approvals, or later continuation."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "goal": {
                        "type": "string",
                        "description": "Concrete user-facing goal this plan will complete.",
                    },
                    "steps": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 10,
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "kind": {"type": "string", "enum": _PLAN_STEP_KINDS},
                                "title": {"type": "string"},
                                "task": {"type": "string"},
                                "executor": {
                                    "type": "string",
                                    "description": "Worker id, tool name, or short Octo executor hint.",
                                },
                                "input": {"type": "object", "additionalProperties": True},
                            },
                            "required": ["id", "kind", "title"],
                            "additionalProperties": False,
                        },
                    },
                    "source": {
                        "type": "string",
                        "description": "Optional source label; defaults to adhoc.",
                    },
                    "metadata": {"type": "object", "additionalProperties": True},
                },
                "required": ["goal", "steps"],
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_plan_create,
        ),
        ToolSpec(
            name="plan_status",
            description=(
                "Inspect a durable runtime plan. Omit run_id to list active plans for the current chat."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "run_id": {"type": "string"},
                    "active_only": {
                        "type": "boolean",
                        "description": "When run_id is omitted, list only active plans. Defaults to true.",
                    },
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20},
                },
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_plan_status,
        ),
        ToolSpec(
            name="plan_update_step",
            description=(
                "Update a runtime plan step after completing work, starting work, binding a worker run, "
                "or discovering a blocker. Keep the plan current before answering the user."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "run_id": {"type": "string"},
                    "step_id": {"type": "string"},
                    "status": {
                        "type": "string",
                        "enum": [
                            "running",
                            "awaiting_worker",
                            "awaiting_approval",
                            "awaiting_user",
                            "completed",
                            "failed",
                            "blocked",
                        ],
                    },
                    "worker_run_id": {
                        "type": "string",
                        "description": "Worker run id when status is awaiting_worker.",
                    },
                    "output": {"type": "object", "additionalProperties": True},
                    "error": {"type": "string"},
                },
                "required": ["run_id", "step_id", "status"],
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_plan_update_step,
        ),
    ]


def _service(ctx: dict[str, Any]) -> PlanRunService:
    octo = ctx.get("octo")
    if octo is None or getattr(octo, "store", None) is None:
        raise RuntimeError("Plan tools require an active Octo store.")
    return PlanRunService(octo.store)


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _coerce_chat_id(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _tool_plan_create(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    service = _service(ctx)
    goal = str((args or {}).get("goal") or "").strip()
    if not goal:
        return _json({"status": "error", "message": "goal is required"})
    steps = list((args or {}).get("steps") or [])
    if not steps:
        return _json({"status": "error", "message": "steps must contain at least one step"})
    chat_id = _coerce_chat_id(ctx.get("chat_id"))
    run = service.create_run(
        goal=goal,
        steps=steps,
        chat_id=chat_id,
        source=str((args or {}).get("source") or "adhoc").strip() or "adhoc",
        correlation_id=str(ctx.get("correlation_id") or "").strip() or None,
        metadata=dict((args or {}).get("metadata") or {}),
    )
    owner_id = str(
        getattr(getattr(ctx.get("octo"), "operational_memory", None), "owner_id", "default")
    )
    _link_commitments_to_plan(
        service,
        run.id,
        dict((args or {}).get("metadata") or {}),
        chat_id=run.chat_id,
        owner_id=owner_id,
    )
    snapshot = service.get_snapshot(run.id)
    return _json({"status": "ok", "run_id": run.id, "snapshot": snapshot})


def _tool_plan_status(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    service = _service(ctx)
    run_id = str((args or {}).get("run_id") or "").strip()
    if run_id:
        snapshot = service.get_snapshot(run_id)
        if snapshot is None:
            return _json({"status": "not_found", "run_id": run_id})
        return _json({"status": "ok", "snapshot": snapshot})

    limit = max(1, min(int((args or {}).get("limit") or 10), 20))
    active_only = bool((args or {}).get("active_only", True))
    chat_id = _coerce_chat_id(ctx.get("chat_id")) or 0
    if active_only:
        runs = service.active_runs_for_chat(chat_id, limit=limit)
    else:
        runs = service.store.list_plan_runs(chat_id=chat_id, limit=limit)
    return _json(
        {
            "status": "ok",
            "plans": [run.model_dump(mode="json") for run in runs],
            "count": len(runs),
        }
    )


def _tool_plan_update_step(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    service = _service(ctx)
    run_id = str((args or {}).get("run_id") or "").strip()
    step_id = str((args or {}).get("step_id") or "").strip()
    status = str((args or {}).get("status") or "").strip()
    if not run_id or not step_id or not status:
        return _json({"status": "error", "message": "run_id, step_id, and status are required"})

    snapshot = service.get_snapshot(run_id)
    if snapshot is None:
        return _json({"status": "not_found", "run_id": run_id})
    known_steps = {str(step.get("step_id") or "") for step in snapshot.get("steps") or []}
    if step_id not in known_steps:
        return _json({"status": "not_found", "run_id": run_id, "step_id": step_id})

    if status == "running":
        service.mark_step_running(run_id, step_id)
    elif status == "awaiting_worker":
        worker_run_id = str((args or {}).get("worker_run_id") or "").strip()
        if not worker_run_id:
            return _json(
                {"status": "error", "message": "worker_run_id is required for awaiting_worker"}
            )
        service.bind_worker_step(run_id, step_id, worker_run_id)
    elif status == "completed":
        service.complete_step(run_id, step_id, output=dict((args or {}).get("output") or {}))
    elif status == "failed":
        service.fail_step(run_id, step_id, error=str((args or {}).get("error") or "failed"))
    elif status in {"awaiting_approval", "awaiting_user"}:
        service.store.update_plan_run(run_id, status=status, current_step_id=step_id)
        service.store.update_plan_step(
            run_id,
            step_id,
            status=status,
            output=dict((args or {}).get("output") or {}),
            started_at=utc_now(),
        )
        service.append_event(run_id, f"step.{status}", step_id=step_id)
    elif status == "blocked":
        error = str((args or {}).get("error") or "blocked").strip() or "blocked"
        service.store.update_plan_run(
            run_id,
            status="blocked",
            current_step_id=step_id,
            completed_at=utc_now(),
        )
        service.store.update_plan_step(
            run_id,
            step_id,
            status="blocked",
            error=error,
            completed_at=utc_now(),
        )
        service.append_event(run_id, "step.blocked", step_id=step_id, data={"error": error})
        resolver = getattr(service.store, "resolve_operational_memory_items_for_plan", None)
        if callable(resolver):
            resolver(run_id, status="blocked", resolved_at=utc_now())
    else:
        return _json({"status": "error", "message": f"Unsupported status: {status}"})

    return _json({"status": "ok", "snapshot": service.get_snapshot(run_id)})


def _link_commitments_to_plan(
    service: PlanRunService,
    run_id: str,
    metadata: dict[str, Any],
    *,
    chat_id: int | None,
    owner_id: str,
) -> None:
    raw_ids = metadata.get("commitment_ids")
    if isinstance(raw_ids, str):
        commitment_ids = [raw_ids]
    elif isinstance(raw_ids, list):
        commitment_ids = [str(item) for item in raw_ids if str(item).strip()]
    else:
        commitment_ids = []
    if not commitment_ids:
        return
    if chat_id is None or chat_id == 0:
        return
    list_items = getattr(service.store, "list_operational_memory_items", None)
    updater = getattr(service.store, "update_operational_memory_item", None)
    if not callable(list_items) or not callable(updater):
        return
    active_items = list_items(
        owner_id,
        chat_id=chat_id,
        statuses=["active", "in_progress", "blocked"],
        limit=50,
    )
    allowed_ids = {str(item.id) for item in active_items if item.chat_id == chat_id}
    for commitment_id in commitment_ids[:10]:
        if commitment_id not in allowed_ids:
            continue
        updater(commitment_id, status="in_progress", plan_run_id=run_id)
