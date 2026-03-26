from __future__ import annotations

import asyncio
import hashlib
import json
import re
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from octopal.tools.registry import ToolSpec
from octopal.utils import utc_now

if TYPE_CHECKING:
    from octopal.runtime.octo.core import Octo

_WORKER_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_TOKEN_RE = re.compile(r"[a-z0-9_]+")
_MAX_PARALLEL_BATCH = 10
_ALLOWED_PATHS_GUIDANCE = (
    "Workers always keep their own private scratch workspace. "
    "Use allowed_paths only when the worker needs files from Octo's main workspace, "
    "and pass the smallest explicit set that will do the job. "
    "If the task only needs the worker's own scratch space, omit allowed_paths."
)


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
            name="list_workers",
            description="List available worker templates with their capabilities.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="worker_manage",
            handler=_tool_list_workers,
        ),
        ToolSpec(
            name="start_worker",
            description=(
                "Start a worker task. If worker_id is omitted or set to 'auto', the worker specialization router "
                f"selects the best template. {_ALLOWED_PATHS_GUIDANCE}"
            ),
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
                "required": ["task"],
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
                f"{_ALLOWED_PATHS_GUIDANCE}"
            ),
            parameters={
                "type": "object",
                "properties": {
                    "worker_id": {
                        "type": "string",
                        "description": "Optional worker template ID (e.g., 'web_researcher'). Use 'auto' or omit to route automatically.",
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
                        "description": "Override default tools for this task (optional).",
                    },
                    "model": {
                        "type": "string",
                        "description": "Override model for this task (optional).",
                    },
                    "timeout_seconds": {
                        "type": "number",
                        "description": "Override default timeout (optional).",
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
                "required": ["task"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_start_child_worker,
            is_async=True,
        ),
        ToolSpec(
            name="start_workers_parallel",
            description=(
                "Launch multiple worker tasks in parallel and return run IDs plus routing decisions. "
                "Each worker still gets its own scratch workspace. "
                "For any shared project files, set allowed_paths per task with the smallest explicit path set."
            ),
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
                                "allowed_paths": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": (
                                        "Optional main-workspace paths to share with this worker in addition to its "
                                        "own scratch workspace. Use the smallest explicit set needed."
                                    ),
                                },
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
            name="worker_session_status",
            description="Summarize the current worker fabric: active runs, recent completions/failures, and lineage health hints.",
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
            description="Assess whether to yield while worker runs are still in flight, or switch to synthesis/result collection when they are ready.",
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
                    "system_prompt": {"type": "string", "description": "New system prompt (optional)."},
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
                    "max_thinking_steps": {"type": "number", "description": "New max steps (optional)."},
                    "default_timeout_seconds": {"type": "number", "description": "New timeout (optional)."},
                    "can_spawn_children": {"type": "boolean", "description": "Enable/disable child spawning (optional)."},
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
        template_list.append({
            "worker_id": t.id,
            "name": t.name,
            "description": t.description,
            "available_tools": t.available_tools,
            "required_permissions": t.required_permissions,
            "default_timeout_seconds": t.default_timeout_seconds,
            "can_spawn_children": bool(getattr(t, "can_spawn_children", False)),
            "allowed_child_templates": list(getattr(t, "allowed_child_templates", [])),
        })

    return json.dumps({
        "count": len(template_list),
        "workers": template_list,
    }, ensure_ascii=False)


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
    available_tools = args.get("available_tools") if isinstance(args.get("available_tools"), list) else []
    required_permissions = args.get("required_permissions") if isinstance(args.get("required_permissions"), list) else []
    available_tools = _normalize_str_list(available_tools)
    required_permissions = _infer_required_permissions(available_tools, required_permissions)
    model = str(args.get("model", "")).strip() or None
    max_thinking_steps = int(args.get("max_thinking_steps")) if args.get("max_thinking_steps") else 10
    default_timeout_seconds = int(args.get("default_timeout_seconds")) if args.get("default_timeout_seconds") else 300
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

    return json.dumps({
        "status": "created",
        "worker_id": worker_id,
        "name": name,
        "description": description,
        "available_tools": available_tools,
        "required_permissions": required_permissions,
        "can_spawn_children": can_spawn_children,
        "allowed_child_templates": allowed_child_templates,
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
        existing_config["available_tools"] = _normalize_str_list(args.get("available_tools"))
    if isinstance(args.get("required_permissions"), list):
        existing_config["required_permissions"] = _normalize_str_list(args.get("required_permissions"))
    if args.get("model"):
        existing_config["model"] = str(args.get("model")).strip()
    if args.get("max_thinking_steps"):
        existing_config["max_thinking_steps"] = int(args.get("max_thinking_steps"))
    if args.get("default_timeout_seconds"):
        existing_config["default_timeout_seconds"] = int(args.get("default_timeout_seconds"))
    if "can_spawn_children" in args:
        existing_config["can_spawn_children"] = bool(args.get("can_spawn_children"))
    if isinstance(args.get("allowed_child_templates"), list):
        existing_config["allowed_child_templates"] = _normalize_str_list(args.get("allowed_child_templates"))

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

    return json.dumps({
        "status": "updated",
        "worker_id": worker_id,
        "name": existing_config["name"],
        "description": existing_config["description"],
        "can_spawn_children": bool(existing_config.get("can_spawn_children", False)),
        "allowed_child_templates": _normalize_str_list(existing_config.get("allowed_child_templates")),
        "message": f"Worker template '{existing_config['name']}' updated successfully at workers/{worker_id}/worker.json"
    }, ensure_ascii=False)


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

    return json.dumps({
        "status": "deleted",
        "worker_id": worker_id,
        "message": f"Worker template '{worker_id}' deleted successfully. Directory workers/{worker_id}/ has been removed."
    }, ensure_ascii=False)


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
    model = str(args.get("model", "")).strip() or None
    timeout_seconds = int(args.get("timeout_seconds")) if args.get("timeout_seconds") else None
    scheduled_task_id = str(args.get("scheduled_task_id", "")).strip() or None

    if not task:
        return "start_worker error: task is required."

    resolution = _resolve_worker_for_start(
        octo=octo,
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

    launch = await octo._start_worker_async(
        worker_id=worker_id,
        task=task,
        chat_id=chat_id,
        inputs=inputs,
        tools=tools,
        model=model,
        timeout_seconds=timeout_seconds,
        scheduled_task_id=scheduled_task_id,
        parent_worker_id=child_ctx["run_id"] if child_ctx else None,
        lineage_id=child_ctx["lineage_id"] if child_ctx else None,
        root_task_id=child_ctx["root_task_id"] if child_ctx else None,
        spawn_depth=(child_ctx["spawn_depth"] + 1) if child_ctx else 0,
        allowed_paths=args.get("allowed_paths") if "allowed_paths" in args else None,
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
        "lineage_id": launch.get("lineage_id"),
        "parent_worker_id": launch.get("parent_worker_id"),
        "root_task_id": launch.get("root_task_id"),
        "spawn_depth": launch.get("spawn_depth"),
        "router_used": router_used,
        "router_reason": route_reason,
        "router_score": route_score,
        "message": message,
    }, ensure_ascii=False)


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
        model = str(item.get("model", "")).strip() or None
        timeout_seconds = int(item.get("timeout_seconds")) if item.get("timeout_seconds") else None
        required_tools = _normalize_str_list(item.get("required_tools"))
        required_permissions = _normalize_str_list(item.get("required_permissions"))

        resolution = _resolve_worker_for_start(
            octo=octo,
            worker_id=worker_id,
            task=task_text,
            required_tools=required_tools,
            required_permissions=required_permissions,
        )
        if isinstance(resolution, str):
            return {"index": index, "status": "error", "error": resolution}

        selected_worker_id = str(resolution["worker_id"])
        template = resolution["template"]
        if child_ctx is not None:
            policy_error = _validate_child_spawn_policy(
                octo=octo,
                parent_ctx=child_ctx,
                child_template=template,
                explicit_worker_id=selected_worker_id,
            )
            if policy_error:
                return {"index": index, "status": "error", "error": policy_error}
        async with sem:
            launch = await octo._start_worker_async(
                worker_id=selected_worker_id,
                task=task_text,
                chat_id=chat_id,
                inputs=inputs,
                tools=tools,
                model=model,
                timeout_seconds=timeout_seconds,
                scheduled_task_id=None,
                parent_worker_id=child_ctx["run_id"] if child_ctx else None,
                lineage_id=child_ctx["lineage_id"] if child_ctx else None,
                root_task_id=child_ctx["root_task_id"] if child_ctx else None,
                spawn_depth=(child_ctx["spawn_depth"] + 1) if child_ctx else 0,
                allowed_paths=item.get("allowed_paths") if "allowed_paths" in item else None,
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
            "lineage_id": launch.get("lineage_id"),
            "parent_worker_id": launch.get("parent_worker_id"),
            "root_task_id": launch.get("root_task_id"),
            "spawn_depth": launch.get("spawn_depth"),
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
    octo: Octo = ctx["octo"]
    worker_ids = _normalize_str_list(args.get("worker_ids"))
    if not worker_ids:
        return "synthesize_worker_results error: worker_ids is required."

    completed: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []
    pending: list[dict[str, object]] = []
    missing: list[str] = []
    summary_hashes: set[str] = set()

    for wid in worker_ids:
        worker = octo.store.get_worker(wid)
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
    octo: Octo = ctx["octo"]
    worker_id = str(args.get("worker_id", "")).strip()
    if not worker_id:
        return "stop_worker error: worker_id is required."
    stopped = await octo.runtime.stop_worker(worker_id)
    return json.dumps({"status": "stopped" if stopped else "not_found", "worker_id": worker_id}, ensure_ascii=False)


def _tool_get_worker_status(args: dict[str, object], ctx: dict[str, object]) -> str:
    octo: Octo = ctx["octo"]
    worker_id = str(args.get("worker_id", "")).strip()
    if not worker_id:
        return "get_worker_status error: worker_id is required."

    worker = octo.store.get_worker(worker_id)
    if not worker:
        return json.dumps({
            "status": "not_found",
            "worker_id": worker_id,
            "message": "Worker not found. It may be from an old conversation or never existed."
        }, ensure_ascii=False)
    worker = _reconcile_stale_worker_status(octo, worker)

    return json.dumps({
        "status": worker.status,
        "worker_id": worker.id,
        "task": worker.task,
        "lineage_id": worker.lineage_id,
        "parent_worker_id": worker.parent_worker_id,
        "root_task_id": worker.root_task_id,
        "spawn_depth": worker.spawn_depth,
        "created_at": worker.created_at.isoformat(),
        "updated_at": worker.updated_at.isoformat(),
        "summary": worker.summary,
        "error": worker.error,
    }, ensure_ascii=False)


def _tool_list_active_workers(args: dict[str, object], ctx: dict[str, object]) -> str:
    octo: Octo = ctx["octo"]
    older_than_minutes = int(args.get("older_than_minutes") or 10)

    workers = octo.store.get_active_workers(older_than_minutes=older_than_minutes)
    workers = _reconcile_stale_active_workers(octo, workers, older_than_minutes=older_than_minutes)
    worker_list = []
    for w in workers:
        worker_list.append({
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
        })

    return json.dumps({
        "count": len(worker_list),
        "workers": worker_list,
    }, ensure_ascii=False)


def _tool_worker_session_status(args: dict[str, object], ctx: dict[str, object]) -> str:
    octo: Octo = ctx["octo"]
    older_than_minutes = int(args.get("older_than_minutes") or 10)
    recent_limit = max(1, min(50, int(args.get("recent_limit") or 12)))

    active_workers = octo.store.get_active_workers(older_than_minutes=older_than_minutes)
    active_workers = _reconcile_stale_active_workers(octo, active_workers, older_than_minutes=older_than_minutes)
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
    running = counts.get("running", 0) + counts.get("started", 0)
    failed_recent = sum(1 for worker in recent_workers if str(worker.status) == "failed")
    if running > 0:
        hints.append(f"{running} worker(s) currently in flight.")
    if failed_recent > 0:
        hints.append(f"{failed_recent} recent worker run(s) failed; inspect summaries before retrying.")
    if any((worker.spawn_depth or 0) > 0 for worker in active_workers):
        hints.append("Active child-worker lineage detected; prefer synthesis or status checks before spawning more.")
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
        hints.append("Focused lineage still has active children; avoid spawning more work in the same tree.")
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
        next_best_action = "append_followup_required"
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
    if worker.status not in {"started", "running"}:
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


def _reconcile_stale_active_workers(octo: Octo, workers: list[Any], older_than_minutes: int) -> list[Any]:
    stale_ids: list[str] = []
    runtime = getattr(octo, "runtime", None)
    if not runtime or not hasattr(runtime, "is_worker_running"):
        return workers
    grace_cutoff = utc_now() - timedelta(minutes=2)
    for worker in workers:
        if worker.status not in {"started", "running"}:
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
    return {
        "worker_id": getattr(worker, "id", None),
        "status": getattr(worker, "status", None),
        "task": getattr(worker, "task", None),
        "lineage_id": getattr(worker, "lineage_id", None),
        "parent_worker_id": getattr(worker, "parent_worker_id", None),
        "spawn_depth": getattr(worker, "spawn_depth", None),
        "updated_at": getattr(worker, "updated_at", None).isoformat()
        if getattr(worker, "updated_at", None) is not None
        else None,
        "summary": getattr(worker, "summary", None),
        "error": getattr(worker, "error", None),
    }


def _tool_get_worker_result(args: dict[str, object], ctx: dict[str, object]) -> str:
    octo: Octo = ctx["octo"]
    worker_id = str(args.get("worker_id", "")).strip()
    if not worker_id:
        return "get_worker_result error: worker_id is required."

    worker = octo.store.get_worker(worker_id)
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
            "lineage_id": worker.lineage_id,
            "parent_worker_id": worker.parent_worker_id,
            "root_task_id": worker.root_task_id,
            "spawn_depth": worker.spawn_depth,
            "summary": worker.summary,
            "output": worker.output,
        }, ensure_ascii=False)
    elif worker.status == "failed":
        return json.dumps({
            "status": "failed",
            "worker_id": worker.id,
            "lineage_id": worker.lineage_id,
            "parent_worker_id": worker.parent_worker_id,
            "root_task_id": worker.root_task_id,
            "spawn_depth": worker.spawn_depth,
            "error": worker.error or "Unknown error",
        }, ensure_ascii=False)
    else:
        return json.dumps({
            "status": worker.status,
            "worker_id": worker.id,
            "lineage_id": worker.lineage_id,
            "parent_worker_id": worker.parent_worker_id,
            "root_task_id": worker.root_task_id,
            "spawn_depth": worker.spawn_depth,
            "message": f"Worker is still {worker.status}. Result not available yet.",
        }, ensure_ascii=False)


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

    parent_permissions = set(_normalize_str_list(parent_ctx.get("effective_permissions", [])))
    child_permissions = set(_normalize_str_list(getattr(child_template, "required_permissions", [])))
    if not child_permissions.issubset(parent_permissions):
        missing = sorted(child_permissions - parent_permissions)
        return (
            "start_child_worker error: child template requests permissions not held by parent "
            f"({', '.join(missing)})."
        )
    return None


def _resolve_worker_for_start(
    *,
    octo: Octo,
    worker_id: str,
    task: str,
    required_tools: list[str] | None = None,
    required_permissions: list[str] | None = None,
) -> dict[str, object] | str:
    if not worker_id or worker_id.lower() in {"auto", "best", "router"}:
        templates = octo.store.list_worker_templates()
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

    template = octo.store.get_worker_template(worker_id)
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
