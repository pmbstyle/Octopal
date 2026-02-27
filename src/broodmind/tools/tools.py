from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from broodmind.memory.memchain import memchain_init, memchain_record, memchain_status, memchain_verify
from broodmind.tools.browser_tools import (
    browser_click,
    browser_close,
    browser_open,
    browser_snapshot,
    browser_type,
)
from broodmind.tools.canon_tools import manage_canon, search_canon
from broodmind.tools.download_file import download_file
from broodmind.tools.exec_run import exec_run
from broodmind.tools.fetch_plan import fetch_plan_tool
from broodmind.tools.filesystem import fs_delete, fs_list, fs_move, fs_read, fs_write
from broodmind.tools.llm_task import run_llm_subtask
from broodmind.tools.skills_tools import get_registered_skill_tools, get_skill_management_tools
from broodmind.tools.ops_tools import (
    artifact_collect,
    config_audit,
    coverage_report,
    db_backup,
    db_maintenance,
    db_query_readonly,
    db_restore,
    docker_compose_control,
    git_ops,
    process_inspect,
    release_snapshot,
    rollback_release,
    secret_scan,
    self_control,
    service_health,
    service_logs,
    test_run,
)
from broodmind.tools.registry import ToolSpec
from broodmind.tools.web_fetch import markdown_new_fetch, web_fetch
from broodmind.tools.web_search import web_search
from broodmind.tools.worker_tools import get_worker_tools
from broodmind.tools.mcp_tools import get_mcp_mgmt_tools

from broodmind.utils import utc_now
import structlog

logger = structlog.get_logger(__name__)

def get_tools(mcp_manager=None) -> list[ToolSpec]:
    tools = [
        ToolSpec(
            name="manage_canon",
            description="Manage canonical memory files (facts.md, decisions.md, failures.md). Only the Queen can use this.",
            parameters={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Action to perform.",
                        "enum": ["list", "read", "write"],
                    },
                    "filename": {
                        "type": "string",
                        "description": "Filename (e.g., 'facts.md'). Required for read/write.",
                    },
                    "content": {
                        "type": "string",
                        "description": "Content to write. Required for write.",
                    },
                    "mode": {
                        "type": "string",
                        "description": "Write mode: 'append' (default) or 'overwrite'.",
                        "enum": ["append", "overwrite"],
                    },
                },
                "required": ["action"],
                "additionalProperties": False,
            },
            permission="canon_manage",
            handler=lambda args, ctx: manage_canon(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="search_canon",
            description="Semantically search for facts and decisions in the canonical memory base.",
            parameters={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query or topic to look for.",
                    },
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            permission="canon_manage",
            handler=lambda args, ctx: search_canon(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="queen_context_health",
            description="Return current context-health metrics and reset decision state for the active chat.",
            parameters={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_queen_context_health,
            is_async=True,
        ),
        ToolSpec(
            name="queen_opportunity_scan",
            description="Generate proactive opportunity cards (impact/effort/confidence/next_action) for the active chat.",
            parameters={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": 5},
                },
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_queen_opportunity_scan,
            is_async=True,
        ),
        ToolSpec(
            name="queen_self_queue_add",
            description="Add a Queen-initiated task into self-driven queue.",
            parameters={
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "task": {"type": "string"},
                    "priority": {"type": "integer", "minimum": 1, "maximum": 5},
                    "source": {"type": "string"},
                    "notes": {"type": "string"},
                },
                "required": ["title", "task"],
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_queen_self_queue_add,
            is_async=True,
        ),
        ToolSpec(
            name="queen_self_queue_list",
            description="List current Queen self-driven queue items.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=_tool_queen_self_queue_list,
            is_async=True,
        ),
        ToolSpec(
            name="queen_self_queue_take",
            description="Claim next pending task from Queen self-driven queue.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=_tool_queen_self_queue_take,
            is_async=True,
        ),
        ToolSpec(
            name="queen_self_queue_update",
            description="Update status of a Queen self-queue item.",
            parameters={
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                    "status": {"type": "string", "enum": ["pending", "claimed", "done", "cancelled"]},
                    "notes": {"type": "string"},
                },
                "required": ["task_id", "status"],
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_queen_self_queue_update,
            is_async=True,
        ),
        ToolSpec(
            name="queen_memchain_status",
            description="Show current memchain integrity status for tracked workspace memory/config files.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=_tool_queen_memchain_status,
            is_async=True,
        ),
        ToolSpec(
            name="queen_memchain_verify",
            description="Verify memchain continuity and detect file drift for tracked workspace memory/config files.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=_tool_queen_memchain_verify,
            is_async=True,
        ),
        ToolSpec(
            name="queen_memchain_record",
            description="Record a new memchain snapshot for tracked workspace memory/config files.",
            parameters={
                "type": "object",
                "properties": {
                    "reason": {"type": "string", "description": "Reason for recording snapshot."},
                },
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_queen_memchain_record,
            is_async=True,
        ),
        ToolSpec(
            name="queen_memchain_init",
            description="Initialize or reinitialize memchain files in workspace memory.",
            parameters={
                "type": "object",
                "properties": {
                    "force": {"type": "boolean", "description": "If true, reinitialize chain files."},
                },
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_queen_memchain_init,
            is_async=True,
        ),
        ToolSpec(
            name="list_schedule",
            description="List all scheduled tasks and their status. Only the Queen can use this.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=lambda args, ctx: "\n".join([f"- {t['name']} (ID: {t['id']}): {t['frequency']}, Last run: {t['last_run_at'] or 'Never'}" for t in ctx["queen"].scheduler.store.get_scheduled_tasks()]),
            is_async=True,
        ),
        ToolSpec(
            name="check_schedule",
            description="Check for tasks that are due to run. Returns machine-readable JSON with due tasks and current UTC time. Only the Queen can use this.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=_tool_check_schedule,
            is_async=True,
        ),
        ToolSpec(
            name="schedule_task",
            description="Add or update a scheduled task. Only the Queen can use this.",
            parameters={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Human-readable name of the task."},
                    "frequency": {"type": "string", "description": "Frequency (e.g., 'Every 30 minutes', 'Daily at 14:00')."},
                    "task": {"type": "string", "description": "The task description for the worker or Queen."},
                    "description": {"type": "string", "description": "Brief description of the task purpose."},
                    "worker_id": {"type": "string", "description": "Optional: Specific worker template ID to use."},
                    "inputs": {"type": "object", "description": "Optional: Inputs for the worker."},
                },
                "required": ["name", "frequency", "task"],
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_schedule_task,
            is_async=True,
        ),
        ToolSpec(
            name="remove_task",
            description="Remove a scheduled task by ID. Only the Queen can use this.",
            parameters={
                "type": "object",
                "properties": {
                    "task_id": {"type": "string", "description": "The ID of the task to remove (e.g., 'check_emails')."},
                },
                "required": ["task_id"],
                "additionalProperties": False,
            },
            permission="self_control",
            handler=lambda args, ctx: (ctx["queen"].scheduler.remove_task(args["task_id"]), "Task removed.")[1],
            is_async=True,
        ),
        ToolSpec(
            name="run_llm_subtask",
            description="Run a generic, JSON-only LLM sub-task. Ideal for tasks requiring structured data generation or analysis based on a prompt.",
            parameters={
                "type": "object",
                "properties": {
                    "prompt": {"type": "string", "description": "The specific instruction or task for the sub-task LLM."},
                    "input": {"type": "object", "description": "Optional JSON-serializable input data for the task."},
                    "schema": {"type": "object", "description": "Optional JSON schema to validate the LLM's output."},
                },
                "required": ["prompt"],
                "additionalProperties": False,
            },
            permission="llm_subtask", # A new permission to control access to this powerful tool
            handler=lambda args, ctx: run_llm_subtask(args, ctx["queen"].provider),
            is_async=True,
        ),
        ToolSpec(
            name="download_file",
            description="Download a file from a URL and save it to the workspace 'downloads' directory.",
            parameters={
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "The URL of the file to download."},
                    "filename": {"type": "string", "description": "Optional: The name to save the file as. If omitted, it will be inferred from the URL."},
                },
                "required": ["url"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: download_file(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="web_search",
            description="Search the web using Brave Search and return a JSON list of results (title, url, snippet).",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query text."},
                    "count": {"type": "integer", "description": "Max results to return (1-10)."},
                    "country": {"type": "string", "description": "Country code for localization (e.g., US, CA)."},
                    "search_lang": {"type": "string", "description": "Search language (e.g., en)."},
                    "ui_lang": {"type": "string", "description": "UI language (e.g., en)."},
                    "freshness": {
                        "type": "string",
                        "description": "Time filter (e.g., 1d, 7d, 30d).",
                    },
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: web_search(args),
            is_async=True,
        ),
        ToolSpec(
            name="web_fetch",
            description="Make an HTTP request and return a JSON payload with status_code, content_type, and snippet.",
            parameters={
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "URL to fetch (http/https only)."},
                    "method": {
                        "type": "string",
                        "description": "HTTP method.",
                        "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"],
                    },
                    "max_chars": {
                        "type": "integer",
                        "description": "Max characters of content to return (200-200000).",
                    },
                    "headers": {
                        "type": "object",
                        "description": "Optional dictionary of custom request headers (e.g. for API tokens).",
                    },
                    "params": {
                        "type": "object",
                        "description": "Optional query string parameters.",
                    },
                    "json": {
                        "type": "object",
                        "description": "Optional JSON request body.",
                    },
                    "body": {
                        "description": "Optional raw request body (string) or object/list.",
                    },
                },
                "required": ["url"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: web_fetch(args),
            is_async=True,
        ),
        ToolSpec(
            name="markdown_new_fetch",
            description="Fetch URL content as markdown via markdown.new. Returns structured JSON with ok/degraded/fallback flags.",
            parameters={
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "URL to fetch (http/https only)."},
                    "method": {
                        "type": "string",
                        "description": "Conversion mode used by markdown.new.",
                        "enum": ["auto", "ai", "browser"],
                    },
                    "retain_images": {
                        "type": "boolean",
                        "description": "Whether markdown.new should keep image URLs in markdown output.",
                    },
                    "max_chars": {
                        "type": "integer",
                        "description": "Max characters of markdown snippet to return (200-200000).",
                    },
                    "timeout_seconds": {
                        "type": "number",
                        "description": "HTTP timeout budget in seconds (5-300).",
                    },
                    "fallback_to_web_fetch": {
                        "type": "boolean",
                        "description": "If true (default), fall back to web_fetch when markdown.new fails or is rate-limited.",
                    },
                },
                "required": ["url"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: markdown_new_fetch(args),
            is_async=True,
        ),
        ToolSpec(
            name="fetch_plan_tool",
            description="Orchestrate URL fetching across markdown_new_fetch, web_fetch, and browser fallback with a traceable execution plan.",
            parameters={
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "URL to fetch (http/https only)."},
                    "goal": {
                        "type": "string",
                        "description": "Fetch goal profile.",
                        "enum": ["quick_summary", "structured_extract", "full_content"],
                    },
                    "prefer_markdown": {
                        "type": "boolean",
                        "description": "Try markdown_new_fetch first (default true).",
                    },
                    "allow_browser": {
                        "type": "boolean",
                        "description": "Allow browser fallback when direct fetch content is insufficient (default true).",
                    },
                    "close_browser": {
                        "type": "boolean",
                        "description": "Close browser session after browser fallback attempt (default true).",
                    },
                    "max_chars": {
                        "type": "integer",
                        "description": "Max characters in returned content snippet (200-200000).",
                    },
                    "min_content_chars": {
                        "type": "integer",
                        "description": "Minimum content threshold for considering an attempt successful.",
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "Total time budget across all fetch attempts (5-300).",
                    },
                },
                "required": ["url"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: fetch_plan_tool(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_open",
            description="Open a URL in an agentic browser. Supports dynamic JavaScript-heavy sites.",
            parameters={
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "URL to open."},
                },
                "required": ["url"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: browser_open(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_snapshot",
            description="Capture an accessibility snapshot of the current page. Provides [ref=eN] tags for interacting with elements.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="network",
            handler=lambda args, ctx: browser_snapshot(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_click",
            description="Click an element in the browser using its ref (e.g. 'e1') from the last snapshot.",
            parameters={
                "type": "object",
                "properties": {
                    "ref": {"type": "string", "description": "Element reference (e.g. 'e1')."},
                },
                "required": ["ref"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: browser_click(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_type",
            description="Type text into an element in the browser using its ref.",
            parameters={
                "type": "object",
                "properties": {
                    "ref": {"type": "string", "description": "Element reference (e.g. 'e1')."},
                    "text": {"type": "string", "description": "Text to type."},
                    "press_enter": {"type": "boolean", "description": "Whether to press Enter after typing."},
                },
                "required": ["ref", "text"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: browser_type(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_close",
            description="Close the browser session for the current chat.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="network",
            handler=lambda args, ctx: browser_close(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="fs_read",
            description="Read a file from the workspace. Returns file contents as text.",
            parameters={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace-relative path to read.",
                    }
                },
                "required": ["path"],
                "additionalProperties": False,
            },
            permission="filesystem_read",
            handler=lambda args, ctx: fs_read(args, ctx["base_dir"]),
            is_async=True,
        ),
        ToolSpec(
            name="fs_write",
            description="Write a file to the workspace. Overwrites if the file exists.",
            parameters={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace-relative path to write.",
                    },
                    "content": {"type": "string", "description": "File contents."},
                },
                "required": ["path", "content"],
                "additionalProperties": False,
            },
            permission="filesystem_write",
            handler=lambda args, ctx: fs_write(args, ctx["base_dir"]),
            is_async=True,
        ),
        ToolSpec(
            name="fs_list",
            description="List entries in a workspace directory.",
            parameters={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace-relative directory to list. Defaults to root.",
                    }
                },
                "additionalProperties": False,
            },
            permission="filesystem_read",
            handler=lambda args, ctx: fs_list(args, ctx["base_dir"]),
            is_async=True,
        ),
        ToolSpec(
            name="fs_move",
            description="Move or rename a file/directory.",
            parameters={
                "type": "object",
                "properties": {
                    "source": {
                        "type": "string",
                        "description": "Workspace-relative source path.",
                    },
                    "destination": {
                        "type": "string",
                        "description": "Workspace-relative destination path.",
                    },
                },
                "required": ["source", "destination"],
                "additionalProperties": False,
            },
            permission="filesystem_write",
            handler=lambda args, ctx: fs_move(args, ctx["base_dir"]),
            is_async=True,
        ),
        ToolSpec(
            name="fs_delete",
            description="Delete a file or directory.",
            parameters={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace-relative path to delete.",
                    }
                },
                "required": ["path"],
                "additionalProperties": False,
            },
            permission="filesystem_write",
            handler=lambda args, ctx: fs_delete(args, ctx["base_dir"]),
            is_async=True,
        ),
        ToolSpec(
            name="exec_run",
            description="Run a shell command in the workspace. Supports blocking execution (default) or background processes.",
            parameters={
                "type": "object",
                "properties": {
                    "command": {"type": "string", "description": "Shell command to execute."},
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "Timeout in seconds for blocking calls.",
                    },
                    "background": {
                        "type": "boolean",
                        "description": "If true, run in background and return session_id.",
                    },
                    "action": {
                        "type": "string",
                        "description": "Action: 'start' (default), 'poll', 'kill', 'write', 'read'.",
                        "enum": ["start", "poll", "kill", "write", "read"],
                    },
                    "session_id": {
                        "type": "string",
                        "description": "ID of background session (required for poll/kill/write/read).",
                    },
                    "input_data": {
                        "type": "string",
                        "description": "Input text to write to stdin (for 'write' action).",
                    },
                },
                "required": [],
                "additionalProperties": False,
            },
            permission="exec",
            handler=lambda args, ctx: exec_run(args, ctx["base_dir"]),
            is_async=True,
        ),
        ToolSpec(
            name="service_health",
            description="Check health for HTTP endpoints, ports, processes, or docker containers.",
            parameters={
                "type": "object",
                "properties": {
                    "mode": {"type": "string", "enum": ["http", "port", "process", "docker"]},
                    "url": {"type": "string"},
                    "host": {"type": "string"},
                    "port": {"type": "integer"},
                    "name": {"type": "string"},
                    "container": {"type": "string"},
                    "timeout_seconds": {"type": "number"},
                },
                "required": ["mode"],
                "additionalProperties": False,
            },
            permission="service_read",
            handler=lambda args, ctx: service_health(args, ctx),
        ),
        ToolSpec(
            name="service_logs",
            description="Fetch logs from docker containers or workspace log files with optional filtering.",
            parameters={
                "type": "object",
                "properties": {
                    "mode": {"type": "string", "enum": ["docker", "file"]},
                    "container": {"type": "string"},
                    "path": {"type": "string"},
                    "lines": {"type": "integer"},
                    "since": {"type": "string"},
                    "grep": {"type": "string"},
                },
                "required": ["mode"],
                "additionalProperties": False,
            },
            permission="service_read",
            handler=lambda args, ctx: service_logs(args, ctx),
        ),
        ToolSpec(
            name="docker_compose_control",
            description="Run allowlisted docker compose actions for service management.",
            parameters={
                "type": "object",
                "properties": {
                    "action": {"type": "string", "enum": ["ps", "up", "down", "restart", "logs", "exec"]},
                    "services": {"type": "array", "items": {"type": "string"}},
                    "compose_file": {"type": "string"},
                    "detach": {"type": "boolean"},
                    "lines": {"type": "integer"},
                    "command": {"type": "string"},
                    "confirm": {"type": "boolean", "description": "Required for destructive actions."},
                    "timeout_seconds": {"type": "integer"},
                },
                "required": ["action"],
                "additionalProperties": False,
            },
            permission="service_control",
            handler=lambda args, ctx: docker_compose_control(args, ctx),
        ),
        ToolSpec(
            name="git_ops",
            description="Run safe repository operations (status/fetch/pull/branch/log/show).",
            parameters={
                "type": "object",
                "properties": {
                    "action": {"type": "string", "enum": ["status", "fetch", "pull", "branch", "log", "show"]},
                    "repo_path": {"type": "string"},
                    "limit": {"type": "integer"},
                    "ref": {"type": "string"},
                    "timeout_seconds": {"type": "integer"},
                },
                "required": ["action"],
                "additionalProperties": False,
            },
            permission="deploy_control",
            handler=lambda args, ctx: git_ops(args, ctx),
        ),
        ToolSpec(
            name="process_inspect",
            description="Inspect system processes or listening ports.",
            parameters={
                "type": "object",
                "properties": {"action": {"type": "string", "enum": ["list", "ports"]}},
                "additionalProperties": False,
            },
            permission="service_read",
            handler=lambda args, ctx: process_inspect(args, ctx),
        ),
        ToolSpec(
            name="db_backup",
            description="Backup SQLite database to the state backup directory.",
            parameters={
                "type": "object",
                "properties": {"db_path": {"type": "string"}},
                "additionalProperties": False,
            },
            permission="db_admin",
            handler=lambda args, ctx: db_backup(args, ctx),
        ),
        ToolSpec(
            name="db_restore",
            description="Restore SQLite database from backup file.",
            parameters={
                "type": "object",
                "properties": {
                    "db_path": {"type": "string"},
                    "backup_path": {"type": "string"},
                    "confirm": {"type": "boolean"},
                },
                "required": ["backup_path"],
                "additionalProperties": False,
            },
            permission="db_admin",
            handler=lambda args, ctx: db_restore(args, ctx),
        ),
        ToolSpec(
            name="db_maintenance",
            description="Run SQLite maintenance operations (integrity_check or vacuum).",
            parameters={
                "type": "object",
                "properties": {"db_path": {"type": "string"}, "action": {"type": "string", "enum": ["integrity_check", "vacuum"]}},
                "additionalProperties": False,
            },
            permission="db_admin",
            handler=lambda args, ctx: db_maintenance(args, ctx),
        ),
        ToolSpec(
            name="db_query_readonly",
            description="Run a read-only SELECT query against SQLite database.",
            parameters={
                "type": "object",
                "properties": {"db_path": {"type": "string"}, "query": {"type": "string"}, "limit": {"type": "integer"}},
                "required": ["query"],
                "additionalProperties": False,
            },
            permission="db_admin",
            handler=lambda args, ctx: db_query_readonly(args, ctx),
        ),
        ToolSpec(
            name="secret_scan",
            description="Scan files for potential secrets or private keys.",
            parameters={
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "additionalProperties": False,
            },
            permission="security_audit",
            handler=lambda args, ctx: secret_scan(args, ctx),
        ),
        ToolSpec(
            name="config_audit",
            description="Audit runtime configuration presence and critical keys.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="security_audit",
            handler=lambda args, ctx: config_audit(args, ctx),
        ),
        ToolSpec(
            name="test_run",
            description="Run allowlisted test/lint commands and return summarized output.",
            parameters={
                "type": "object",
                "properties": {"command": {"type": "string"}, "timeout_seconds": {"type": "integer"}},
                "required": ["command"],
                "additionalProperties": False,
            },
            permission="exec",
            handler=lambda args, ctx: test_run(args, ctx),
        ),
        ToolSpec(
            name="coverage_report",
            description="Read coverage.xml summary if available.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="exec",
            handler=lambda args, ctx: coverage_report(args, ctx),
        ),
        ToolSpec(
            name="artifact_collect",
            description="Collect artifact file list matching a glob pattern.",
            parameters={
                "type": "object",
                "properties": {"pattern": {"type": "string"}},
                "additionalProperties": False,
            },
            permission="filesystem_read",
            handler=lambda args, ctx: artifact_collect(args, ctx),
        ),
        ToolSpec(
            name="release_snapshot",
            description="Create or list release snapshots for rollback planning.",
            parameters={
                "type": "object",
                "properties": {"action": {"type": "string", "enum": ["create", "list"]}, "note": {"type": "string"}},
                "additionalProperties": False,
            },
            permission="deploy_control",
            handler=lambda args, ctx: release_snapshot(args, ctx),
        ),
        ToolSpec(
            name="rollback_release",
            description="Rollback repository checkout to a previous release snapshot commit.",
            parameters={
                "type": "object",
                "properties": {"snapshot_id": {"type": "string"}, "confirm": {"type": "boolean"}},
                "additionalProperties": False,
            },
            permission="deploy_control",
            handler=lambda args, ctx: rollback_release(args, ctx),
        ),
        ToolSpec(
            name="queen_context_reset",
            description="Compact or reset Queen chat context with a structured handoff and wake-up directive.",
            parameters={
                "type": "object",
                "properties": {
                    "mode": {"type": "string", "enum": ["soft", "hard"], "description": "soft keeps bootstrap, hard also resets bootstrap hash."},
                    "reason": {"type": "string", "description": "Why context reset is needed now."},
                    "goal_now": {"type": "string", "description": "Primary goal to keep after reset."},
                    "done": {"type": "array", "items": {"type": "string"}, "description": "Completed items worth preserving."},
                    "open_threads": {"type": "array", "items": {"type": "string"}, "description": "Open threads still unresolved."},
                    "critical_constraints": {"type": "array", "items": {"type": "string"}, "description": "Non-negotiable constraints."},
                    "next_step": {"type": "string", "description": "First step after wake-up."},
                    "current_interest": {"type": "string", "description": "Current focus area."},
                    "pending_human_input": {"type": "string", "description": "Human input currently needed, if any."},
                    "cognitive_state": {"type": "string", "enum": ["focused", "fatigued", "frustrated", "energized"]},
                    "confidence": {"type": "number", "description": "Confidence in handoff quality (0-1)."},
                    "confirm": {"type": "boolean", "description": "Required for hard reset or guarded retries."},
                },
                "required": ["reason"],
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_queen_context_reset,
            is_async=True,
        ),
        ToolSpec(
            name="self_control",
            description="Request supervised self actions (restart/shutdown/reload) or check action status.",
            parameters={
                "type": "object",
                "properties": {
                    "action": {"type": "string", "enum": ["restart_service", "graceful_shutdown", "reload_config", "status"]},
                    "reason": {"type": "string"},
                    "confirm": {"type": "boolean"},
                },
                "required": ["action"],
                "additionalProperties": False,
            },
            permission="self_control",
            handler=lambda args, ctx: self_control(args, ctx),
        ),
    ]
    tools.extend(get_skill_management_tools())
    tools.extend(get_registered_skill_tools())
    tools.extend(get_worker_tools())
    tools.extend(get_mcp_mgmt_tools())
    if mcp_manager:
        mcp_tools = mcp_manager.get_all_tools()
        if mcp_tools:
            logger.info("Injecting %d MCP tools into registry", len(mcp_tools))
            tools.extend(mcp_tools)
    return tools


async def _tool_check_schedule(args, ctx) -> str:
    scheduler = ctx["queen"].scheduler
    due_tasks = scheduler.get_actionable_tasks()
    queen = ctx.get("queen")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    context_health = None
    opportunity_snapshot = None
    self_queue = None
    if queen is not None and hasattr(queen, "get_context_health_snapshot"):
        try:
            context_health = await queen.get_context_health_snapshot(chat_id)
        except Exception:
            context_health = None
    if queen is not None and hasattr(queen, "scan_opportunities"):
        try:
            opportunity_snapshot = await queen.scan_opportunities(chat_id, limit=3)
        except Exception:
            opportunity_snapshot = None
    if queen is not None and hasattr(queen, "get_self_queue"):
        try:
            self_queue = await queen.get_self_queue(chat_id)
        except Exception:
            self_queue = None
    payload = {
        "current_utc": utc_now().isoformat(),
        "due_count": len(due_tasks),
        "context_health": context_health,
        "opportunities": opportunity_snapshot,
        "self_queue": self_queue,
        "due_tasks": [
            {
                "task_id": t.get("id"),
                "name": t.get("name"),
                "frequency": t.get("frequency"),
                "worker_id": t.get("worker_id"),
                "task_text": t.get("task_text"),
                "description": t.get("description"),
                "inputs": t.get("inputs") if isinstance(t.get("inputs"), dict) else {},
                "last_run_at": t.get("last_run_at"),
            }
            for t in due_tasks
        ],
    }
    return json.dumps(payload, ensure_ascii=False)


def _tool_schedule_task(args, ctx) -> str:
    try:
        task_id = ctx["queen"].scheduler.schedule_task(
            name=args["name"],
            frequency=args["frequency"],
            task_text=args["task"],
            description=args.get("description"),
            worker_id=args.get("worker_id"),
            inputs=args.get("inputs"),
        )
    except ValueError as exc:
        return f"schedule_task error: {exc}"

    return json.dumps(
        {
            "status": "scheduled",
            "task_id": task_id,
            "name": args["name"],
            "frequency": args["frequency"],
        },
        ensure_ascii=False,
    )


async def _tool_queen_context_reset(args, ctx) -> str:
    queen = ctx.get("queen")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if queen is None or not hasattr(queen, "request_context_reset"):
        return json.dumps({"status": "error", "message": "queen context reset is unavailable"}, ensure_ascii=False)
    result = await queen.request_context_reset(chat_id, args or {})
    return json.dumps(result, ensure_ascii=False)


async def _tool_queen_context_health(args, ctx) -> str:
    queen = ctx.get("queen")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if queen is None or not hasattr(queen, "get_context_health_snapshot"):
        return json.dumps({"status": "error", "message": "queen context health is unavailable"}, ensure_ascii=False)
    snapshot = await queen.get_context_health_snapshot(chat_id)
    thresholds = (
        queen.get_context_thresholds()
        if hasattr(queen, "get_context_thresholds")
        else {
            "watch": {
                "context_size_estimate": 90000,
                "repetition_score": 0.70,
                "error_streak": 4,
                "no_progress_turns": 6,
            },
            "reset_soon": {
                "context_size_estimate": 150000,
                "repetition_score": 0.82,
                "error_streak": 7,
                "no_progress_turns": 10,
            },
        }
    )
    payload = {
        "status": "ok",
        "chat_id": chat_id,
        "context_health": snapshot,
        "thresholds": thresholds,
    }
    return json.dumps(payload, ensure_ascii=False)


def _workspace_dir() -> Path:
    return Path(os.getenv("BROODMIND_WORKSPACE_DIR", "workspace")).resolve()


async def _tool_queen_memchain_status(args, ctx) -> str:
    payload = await asyncio.to_thread(memchain_status, _workspace_dir())
    return json.dumps(payload, ensure_ascii=False)


async def _tool_queen_memchain_verify(args, ctx) -> str:
    result = await asyncio.to_thread(memchain_verify, _workspace_dir())
    payload = {
        "status": result.status,
        "message": result.message,
        "entries": result.entries,
        "head_hash": result.head_hash,
        "broken_at": result.broken_at,
        "changed_files": result.changed_files or [],
    }
    return json.dumps(payload, ensure_ascii=False)


async def _tool_queen_memchain_record(args, ctx) -> str:
    reason = str((args or {}).get("reason", "queen_manual") or "queen_manual")
    payload = await asyncio.to_thread(
        memchain_record,
        _workspace_dir(),
        reason=reason,
        meta={"source": "queen_tool", "chat_id": int(ctx.get("chat_id", 0) or 0)},
    )
    return json.dumps(payload, ensure_ascii=False)


async def _tool_queen_memchain_init(args, ctx) -> str:
    force = bool((args or {}).get("force", False))
    payload = await asyncio.to_thread(memchain_init, _workspace_dir(), force=force)
    return json.dumps(payload, ensure_ascii=False)


async def _tool_queen_opportunity_scan(args, ctx) -> str:
    queen = ctx.get("queen")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if queen is None or not hasattr(queen, "scan_opportunities"):
        return json.dumps({"status": "error", "message": "queen opportunity scan is unavailable"}, ensure_ascii=False)
    limit = int((args or {}).get("limit", 3) or 3)
    payload = await queen.scan_opportunities(chat_id, limit=limit)
    return json.dumps(payload, ensure_ascii=False)


async def _tool_queen_self_queue_add(args, ctx) -> str:
    queen = ctx.get("queen")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if queen is None or not hasattr(queen, "add_self_queue_item"):
        return json.dumps({"status": "error", "message": "queen self queue is unavailable"}, ensure_ascii=False)
    payload = await queen.add_self_queue_item(chat_id, args or {})
    return json.dumps(payload, ensure_ascii=False)


async def _tool_queen_self_queue_list(args, ctx) -> str:
    queen = ctx.get("queen")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if queen is None or not hasattr(queen, "get_self_queue"):
        return json.dumps({"status": "error", "message": "queen self queue is unavailable"}, ensure_ascii=False)
    items = await queen.get_self_queue(chat_id)
    return json.dumps({"status": "ok", "chat_id": chat_id, "items": items, "count": len(items)}, ensure_ascii=False)


async def _tool_queen_self_queue_take(args, ctx) -> str:
    queen = ctx.get("queen")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if queen is None or not hasattr(queen, "take_next_self_queue_item"):
        return json.dumps({"status": "error", "message": "queen self queue is unavailable"}, ensure_ascii=False)
    payload = await queen.take_next_self_queue_item(chat_id)
    return json.dumps(payload, ensure_ascii=False)


async def _tool_queen_self_queue_update(args, ctx) -> str:
    queen = ctx.get("queen")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if queen is None or not hasattr(queen, "update_self_queue_item"):
        return json.dumps({"status": "error", "message": "queen self queue is unavailable"}, ensure_ascii=False)
    payload = await queen.update_self_queue_item(chat_id, args or {})
    return json.dumps(payload, ensure_ascii=False)
