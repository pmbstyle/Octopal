from __future__ import annotations

import asyncio
import hashlib
import json
import re
from pathlib import Path
from typing import TYPE_CHECKING

from broodmind.tools.registry import ToolSpec

if TYPE_CHECKING:
    from broodmind.queen.core import Queen

_WORKER_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_TOKEN_RE = re.compile(r"[a-z0-9_]+")
_MAX_PARALLEL_BATCH = 10


def get_worker_tools() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="propose_knowledge",
            description="Propose a fact, decision, or failure lesson for the permanent canonical memory. The Queen will review and potentially add it.",
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
            name="list_workers",
            description="List available worker templates with their capabilities.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="worker_manage",
            handler=_tool_list_workers,
        ),
        ToolSpec(
            name="start_worker",
            description="Start a worker task. If worker_id is omitted or set to 'auto', the worker specialization router selects the best template.",
            parameters={
                "type": "object",
                "properties": {
                    "worker_id": {
                        "type": "string",
                        "description": "Optional worker template ID (e.g., 'web_researcher'). Use 'auto' or omit to route automatically.",
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
                        "description": "Override default tools for this task (optional).",
                    },
                    "model": {
                        "type": "string",
                        "description": "Override model for this task (optional, e.g., 'gpt-4o', 'anthropic/claude-3-opus').",
                    },
                    "timeout_seconds": {
                        "type": "number",
                        "description": "Override default timeout (optional).",
                    },
                    "scheduled_task_id": {
                        "type": "string",
                        "description": "Optional schedule task ID when this worker run comes from check_schedule. Enables reliable execution tracking.",
                    },
                    "required_tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional tool capabilities the selected worker should support.",
                    },
                    "required_permissions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional permissions the selected worker should include.",
                    },
                },
                "required": ["task"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_start_worker,
            is_async=True,
        ),
        ToolSpec(
            name="start_workers_parallel",
            description="Launch multiple worker tasks in parallel and return run IDs plus routing decisions.",
            parameters={
                "type": "object",
                "properties": {
                    "tasks": {
                        "type": "array",
                        "description": "List of tasks to launch. Each item may include worker_id, task, inputs, tools, model, timeout_seconds, required_tools, required_permissions.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "worker_id": {"type": "string"},
                                "task": {"type": "string"},
                                "inputs": {"type": "object", "additionalProperties": True},
                                "tools": {"type": "array", "items": {"type": "string"}},
                                "model": {"type": "string"},
                                "timeout_seconds": {"type": "number"},
                                "required_tools": {"type": "array", "items": {"type": "string"}},
                                "required_permissions": {"type": "array", "items": {"type": "string"}},
                            },
                            "required": ["task"],
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
            description="Synthesize results from multiple workers into one combined summary, including failures and pending runs.",
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
                    }
                },
                "required": ["worker_id", "path"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_get_worker_output_path,
        ),
        ToolSpec(
            name="create_worker_template",
            description="Create a new worker template by writing a worker.json file to the workspace.",
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
                        "description": "Tool names this worker can use (e.g., ['web_search', 'web_fetch']).",
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
                },
                "required": ["id", "name", "description", "system_prompt"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_create_worker_template,
        ),
        ToolSpec(
            name="update_worker_template",
            description="Update an existing worker template. Reads the worker.json file, modifies the specified fields, and writes it back.",
            parameters={
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Worker ID to update.",
                    },
                    "name": {"type": "string", "description": "New name (optional)."},
                    "description": {"type": "string", "description": "New description (optional)."},
                    "system_prompt": {"type": "string", "description": "New system prompt (optional)."},
                    "available_tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "New tool list (optional).",
                    },
                    "required_permissions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "New permissions (optional).",
                    },
                    "model": {"type": "string", "description": "New model override (optional)."},
                    "max_thinking_steps": {"type": "number", "description": "New max steps (optional)."},
                    "default_timeout_seconds": {"type": "number", "description": "New timeout (optional)."},
                },
                "required": ["id"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_update_worker_template,
        ),
        ToolSpec(
            name="delete_worker_template",
            description="Delete a worker template by removing its directory from workspace/workers/.",
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
    queen: Queen = ctx["queen"]

    templates = queen.store.list_worker_templates()
    template_list = []
    for t in templates:
        template_list.append({
            "worker_id": t.id,
            "name": t.name,
            "description": t.description,
            "available_tools": t.available_tools,
            "required_permissions": t.required_permissions,
            "default_timeout_seconds": t.default_timeout_seconds,
        })

    return json.dumps({
        "count": len(template_list),
        "workers": template_list,
    }, ensure_ascii=False)


def _tool_create_worker_template(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Create a new worker template by writing a worker.json file to the workspace."""
    queen: Queen = ctx["queen"]
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
    existing = queen.store.get_worker_template(worker_id)
    if existing:
        return f"create_worker_template error: worker '{worker_id}' already exists. Use update_worker_template to modify it."

    # Get optional parameters with defaults
    available_tools = args.get("available_tools") if isinstance(args.get("available_tools"), list) else []
    required_permissions = args.get("required_permissions") if isinstance(args.get("required_permissions"), list) else []
    model = str(args.get("model", "")).strip() or None
    max_thinking_steps = int(args.get("max_thinking_steps")) if args.get("max_thinking_steps") else 10
    default_timeout_seconds = int(args.get("default_timeout_seconds")) if args.get("default_timeout_seconds") else 300

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

    return json.dumps({
        "status": "created",
        "worker_id": worker_id,
        "name": name,
        "description": description,
        "available_tools": available_tools,
        "required_permissions": required_permissions,
        "message": f"Worker template '{name}' created successfully at workers/{worker_id}/worker.json"
    }, ensure_ascii=False)


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
        existing_config["available_tools"] = args.get("available_tools")
    if isinstance(args.get("required_permissions"), list):
        existing_config["required_permissions"] = args.get("required_permissions")
    if args.get("model"):
        existing_config["model"] = str(args.get("model")).strip()
    if args.get("max_thinking_steps"):
        existing_config["max_thinking_steps"] = int(args.get("max_thinking_steps"))
    if args.get("default_timeout_seconds"):
        existing_config["default_timeout_seconds"] = int(args.get("default_timeout_seconds"))

    # Write updated worker.json
    try:
        worker_file.write_text(json.dumps(existing_config, indent=2), encoding="utf-8")
    except Exception as e:
        return f"update_worker_template error: failed to write worker.json: {e}"

    return json.dumps({
        "status": "updated",
        "worker_id": worker_id,
        "name": existing_config["name"],
        "description": existing_config["description"],
        "message": f"Worker template '{existing_config['name']}' updated successfully at workers/{worker_id}/worker.json"
    }, ensure_ascii=False)


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

    return json.dumps({
        "status": "deleted",
        "worker_id": worker_id,
        "message": f"Worker template '{worker_id}' deleted successfully. Directory workers/{worker_id}/ has been removed."
    }, ensure_ascii=False)


async def _tool_start_worker(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Start a worker task with the specified worker template."""
    queen: Queen = ctx["queen"]
    chat_id = int(ctx.get("chat_id") or 0)

    worker_id = str(args.get("worker_id", "")).strip()
    task = str(args.get("task", "")).strip()
    inputs = args.get("inputs") if isinstance(args.get("inputs"), dict) else {}
    tools = args.get("tools") if isinstance(args.get("tools"), list) else None
    required_tools = _normalize_str_list(args.get("required_tools"))
    required_permissions = _normalize_str_list(args.get("required_permissions"))
    model = str(args.get("model", "")).strip() or None
    timeout_seconds = int(args.get("timeout_seconds")) if args.get("timeout_seconds") else None
    scheduled_task_id = str(args.get("scheduled_task_id", "")).strip() or None

    if not task:
        return "start_worker error: task is required."

    resolution = _resolve_worker_for_start(
        queen=queen,
        worker_id=worker_id,
        task=task,
        required_tools=required_tools,
        required_permissions=required_permissions,
    )
    if isinstance(resolution, str):
        return resolution
    template = resolution["template"]
    worker_id = str(resolution["worker_id"])
    router_used = bool(resolution["router_used"])
    route_reason = str(resolution["router_reason"])
    route_score = float(resolution["router_score"]) if resolution["router_score"] is not None else None

    launch = await queen._start_worker_async(
        worker_id=worker_id,
        task=task,
        chat_id=chat_id,
        inputs=inputs,
        tools=tools,
        model=model,
        timeout_seconds=timeout_seconds,
        scheduled_task_id=scheduled_task_id,
    )
    status = str(launch.get("status", "started"))
    launched_worker_id = launch.get("worker_id")
    run_id = launch.get("run_id")
    if status == "started" and launched_worker_id:
        if router_used:
            message = (
                f"Worker router selected '{template.id}' ({template.name}) and started run {launched_worker_id}. "
                "Use get_worker_status/get_worker_result with this worker_id."
            )
        else:
            message = f"Worker '{template.name}' started as {launched_worker_id}. Use get_worker_status/get_worker_result with this worker_id."
    elif status == "skipped_duplicate":
        message = "Duplicate worker task detected in this turn; skipped starting a new worker."
    else:
        message = f"Worker start returned status={status}."

    return json.dumps({
        "status": status,
        "worker_template_id": worker_id,
        "worker_id": launched_worker_id,
        "run_id": run_id,
        "scheduled_task_id": scheduled_task_id,
        "router_used": router_used,
        "router_reason": route_reason,
        "router_score": route_score,
        "message": message,
    }, ensure_ascii=False)


async def _tool_start_workers_parallel(args: dict[str, object], ctx: dict[str, object]) -> str:
    queen: Queen = ctx["queen"]
    chat_id = int(ctx.get("chat_id") or 0)
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
        model = str(item.get("model", "")).strip() or None
        timeout_seconds = int(item.get("timeout_seconds")) if item.get("timeout_seconds") else None
        required_tools = _normalize_str_list(item.get("required_tools"))
        required_permissions = _normalize_str_list(item.get("required_permissions"))

        resolution = _resolve_worker_for_start(
            queen=queen,
            worker_id=worker_id,
            task=task_text,
            required_tools=required_tools,
            required_permissions=required_permissions,
        )
        if isinstance(resolution, str):
            return {"index": index, "status": "error", "error": resolution}

        selected_worker_id = str(resolution["worker_id"])
        template = resolution["template"]
        async with sem:
            launch = await queen._start_worker_async(
                worker_id=selected_worker_id,
                task=task_text,
                chat_id=chat_id,
                inputs=inputs,
                tools=tools,
                model=model,
                timeout_seconds=timeout_seconds,
                scheduled_task_id=None,
            )

        return {
            "index": index,
            "status": launch.get("status", "started"),
            "worker_id": launch.get("worker_id"),
            "run_id": launch.get("run_id"),
            "worker_template_id": selected_worker_id,
            "worker_template_name": getattr(template, "name", selected_worker_id),
            "router_used": bool(resolution["router_used"]),
            "router_reason": str(resolution["router_reason"]),
            "router_score": resolution["router_score"],
        }

    launches = await asyncio.gather(*[_launch(item, idx) for idx, item in enumerate(tasks)])
    started = sum(1 for item in launches if str(item.get("status")) in {"started", "skipped_duplicate"})
    failed = len(launches) - started
    return json.dumps(
        {
            "status": "ok" if failed == 0 else "partial",
            "started_count": started,
            "failed_count": failed,
            "max_parallel": max_parallel,
            "launches": launches,
        },
        ensure_ascii=False,
    )


def _tool_synthesize_worker_results(args: dict[str, object], ctx: dict[str, object]) -> str:
    queen: Queen = ctx["queen"]
    worker_ids = _normalize_str_list(args.get("worker_ids"))
    if not worker_ids:
        return "synthesize_worker_results error: worker_ids is required."

    completed: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []
    pending: list[dict[str, object]] = []
    missing: list[str] = []
    summary_hashes: set[str] = set()

    for wid in worker_ids:
        worker = queen.store.get_worker(wid)
        if not worker:
            missing.append(wid)
            continue
        status = str(worker.status)
        if status == "completed":
            summary = str(worker.summary or "").strip()
            completed.append({"worker_id": wid, "summary": summary, "output": worker.output})
            if summary:
                summary_hashes.add(hashlib.sha256(summary.encode("utf-8")).hexdigest())
        elif status == "failed":
            failed.append({"worker_id": wid, "error": str(worker.error or "Unknown error")})
        else:
            pending.append({"worker_id": wid, "status": status})

    synthesis_lines: list[str] = []
    if completed:
        synthesis_lines.append("Completed worker findings:")
        for item in completed:
            synthesis_lines.append(f"- {item['worker_id']}: {item['summary'] or 'No summary'}")
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
        synthesis_lines.append("Potential conflict detected: completed workers reported different summaries.")

    return json.dumps(
        {
            "status": "ok",
            "worker_ids": worker_ids,
            "completed_count": len(completed),
            "failed_count": len(failed),
            "pending_count": len(pending),
            "missing_count": len(missing),
            "conflicting_summaries": conflicting,
            "synthesis": "\n".join(synthesis_lines),
            "completed": completed,
            "failed": failed,
            "pending": pending,
            "missing": missing,
        },
        ensure_ascii=False,
    )


async def _tool_stop_worker(args: dict[str, object], ctx: dict[str, object]) -> str:
    queen: Queen = ctx["queen"]
    worker_id = str(args.get("worker_id", "")).strip()
    if not worker_id:
        return "stop_worker error: worker_id is required."
    stopped = await queen.runtime.stop_worker(worker_id)
    return json.dumps({"status": "stopped" if stopped else "not_found", "worker_id": worker_id}, ensure_ascii=False)


def _tool_get_worker_status(args: dict[str, object], ctx: dict[str, object]) -> str:
    queen: Queen = ctx["queen"]
    worker_id = str(args.get("worker_id", "")).strip()
    if not worker_id:
        return "get_worker_status error: worker_id is required."

    worker = queen.store.get_worker(worker_id)
    if not worker:
        return json.dumps({
            "status": "not_found",
            "worker_id": worker_id,
            "message": "Worker not found. It may be from an old conversation or never existed."
        }, ensure_ascii=False)

    return json.dumps({
        "status": worker.status,
        "worker_id": worker.id,
        "task": worker.task,
        "created_at": worker.created_at.isoformat(),
        "updated_at": worker.updated_at.isoformat(),
        "summary": worker.summary,
        "error": worker.error,
    }, ensure_ascii=False)


def _tool_list_active_workers(args: dict[str, object], ctx: dict[str, object]) -> str:
    queen: Queen = ctx["queen"]
    older_than_minutes = int(args.get("older_than_minutes") or 10)

    workers = queen.store.get_active_workers(older_than_minutes=older_than_minutes)
    worker_list = []
    for w in workers:
        worker_list.append({
            "worker_id": w.id,
            "status": w.status,
            "task": w.task,
            "created_at": w.created_at.isoformat(),
            "updated_at": w.updated_at.isoformat(),
            "summary": w.summary,
            "error": w.error,
        })

    return json.dumps({
        "count": len(worker_list),
        "workers": worker_list,
    }, ensure_ascii=False)


def _tool_get_worker_result(args: dict[str, object], ctx: dict[str, object]) -> str:
    queen: Queen = ctx["queen"]
    worker_id = str(args.get("worker_id", "")).strip()
    if not worker_id:
        return "get_worker_result error: worker_id is required."

    worker = queen.store.get_worker(worker_id)
    if not worker:
        return json.dumps({
            "status": "not_found",
            "worker_id": worker_id,
            "message": "Worker not found."
        }, ensure_ascii=False)

    if worker.status == "completed":
        return json.dumps({
            "status": "completed",
            "worker_id": worker.id,
            "summary": worker.summary,
            "output": worker.output,
        }, ensure_ascii=False)
    elif worker.status == "failed":
        return json.dumps({
            "status": "failed",
            "worker_id": worker.id,
            "error": worker.error or "Unknown error",
        }, ensure_ascii=False)
    else:
        return json.dumps({
            "status": worker.status,
            "worker_id": worker.id,
            "message": f"Worker is still {worker.status}. Result not available yet.",
        }, ensure_ascii=False)


def _tool_get_worker_output_path(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Retrieve a specific part of a worker's output using a dotted path."""
    queen: Queen = ctx["queen"]
    worker_id = str(args.get("worker_id", "")).strip()
    path = str(args.get("path", "")).strip()

    if not worker_id:
        return "get_worker_output_path error: worker_id is required."
    if not path:
        return "get_worker_output_path error: path is required."

    worker = queen.store.get_worker(worker_id)
    if not worker:
        return json.dumps({"status": "not_found", "worker_id": worker_id}, ensure_ascii=False)

    if worker.status != "completed":
        return json.dumps({"status": worker.status, "message": "Worker result not available."}, ensure_ascii=False)

    output = worker.output or {}
    current = output
    parts = path.split(".")

    for part in parts:
        if isinstance(current, dict):
            if part in current:
                current = current[part]
            else:
                return json.dumps({"error": f"Path not found: {path} (missing key '{part}')"}, ensure_ascii=False)
        elif isinstance(current, list):
            try:
                idx = int(part)
                if 0 <= idx < len(current):
                    current = current[idx]
                else:
                    return json.dumps({"error": f"Path not found: {path} (index '{idx}' out of range)"}, ensure_ascii=False)
            except ValueError:
                return json.dumps({"error": f"Path not found: {path} (expected index for list, got '{part}')"}, ensure_ascii=False)
        else:
            return json.dumps({"error": f"Path not found: {path} (cannot traverse into non-container type at '{part}')"}, ensure_ascii=False)

    return json.dumps({
        "worker_id": worker_id,
        "path": path,
        "value": current
    }, ensure_ascii=False)


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


def _resolve_worker_for_start(
    *,
    queen: Queen,
    worker_id: str,
    task: str,
    required_tools: list[str] | None = None,
    required_permissions: list[str] | None = None,
) -> dict[str, object] | str:
    if not worker_id or worker_id.lower() in {"auto", "best", "router"}:
        templates = queen.store.list_worker_templates()
        selection = _select_worker_template(
            templates=templates,
            task=task,
            required_tools=required_tools,
            required_permissions=required_permissions,
        )
        if selection is None:
            return "start_worker error: no worker templates are available for routing. Use create_worker_template or provide worker_id."
        template = selection["template"]
        return {
            "template": template,
            "worker_id": template.id,
            "router_used": True,
            "router_reason": selection["reason"],
            "router_score": selection["score"],
        }

    template = queen.store.get_worker_template(worker_id)
    if not template:
        return f"start_worker error: worker '{worker_id}' not found. Use list_workers to see available workers."
    return {
        "template": template,
        "worker_id": worker_id,
        "router_used": False,
        "router_reason": "",
        "router_score": None,
    }


def _normalize_str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _tokenize(text: str) -> set[str]:
    return set(_TOKEN_RE.findall((text or "").lower()))


def _select_worker_template(
    *,
    templates: list[object],
    task: str,
    required_tools: list[str] | None = None,
    required_permissions: list[str] | None = None,
) -> dict[str, object] | None:
    if not templates:
        return None

    required_tools = [t.lower() for t in (required_tools or [])]
    required_permissions = [p.lower() for p in (required_permissions or [])]
    task_tokens = _tokenize(task)
    if not task_tokens:
        task_tokens = {"task"}

    best: dict[str, object] | None = None
    for template in templates:
        score = 0.0
        reasons: list[str] = []
        descriptor = " ".join(
            [
                str(getattr(template, "id", "")),
                str(getattr(template, "name", "")),
                str(getattr(template, "description", "")),
                str(getattr(template, "system_prompt", "")),
            ]
        )
        template_tokens = _tokenize(descriptor)
        overlap = len(task_tokens & template_tokens)
        if overlap:
            score += overlap * 2.0
            reasons.append(f"keyword_overlap={overlap}")

        available_tools = [str(t).lower() for t in getattr(template, "available_tools", [])]
        permissions = [str(p).lower() for p in getattr(template, "required_permissions", [])]

        if required_tools:
            matched_tools = sum(1 for t in required_tools if t in available_tools)
            if matched_tools:
                score += matched_tools * 5.0
                reasons.append(f"required_tools={matched_tools}/{len(required_tools)}")
            else:
                score -= 6.0
                reasons.append("missing_required_tools")
        if required_permissions:
            matched_perms = sum(1 for p in required_permissions if p in permissions)
            if matched_perms:
                score += matched_perms * 4.0
                reasons.append(f"required_permissions={matched_perms}/{len(required_permissions)}")
            else:
                score -= 4.0
                reasons.append("missing_required_permissions")

        if "web" in task_tokens and any("web" in t for t in available_tools):
            score += 3.0
            reasons.append("web_tool_bonus")
        if {"test", "pytest", "unit"} & task_tokens and "test_runner" in str(getattr(template, "id", "")):
            score += 4.0
            reasons.append("test_runner_bonus")
        if {"deploy", "release", "rollback"} & task_tokens and "deploy" in str(getattr(template, "id", "")):
            score += 4.0
            reasons.append("deploy_bonus")
        if {"code", "refactor", "bugfix", "python"} & task_tokens and "coder" in str(getattr(template, "id", "")):
            score += 4.0
            reasons.append("coder_bonus")

        tie_key = str(getattr(template, "id", ""))
        candidate = {
            "template": template,
            "score": score,
            "reason": ", ".join(reasons) if reasons else "default selection",
            "tie_key": tie_key,
        }
        if best is None:
            best = candidate
        else:
            best_score = float(best["score"])
            if score > best_score or (score == best_score and tie_key < str(best["tie_key"])):
                best = candidate

    if best is None:
        return None
    best.pop("tie_key", None)
    return best
