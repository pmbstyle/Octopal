from __future__ import annotations

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
from broodmind.tools.filesystem import fs_delete, fs_list, fs_move, fs_read, fs_write
from broodmind.tools.llm_task import run_llm_subtask
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
from broodmind.tools.web_fetch import web_fetch
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
            name="list_schedule",
            description="List all scheduled tasks and their status. Only the Queen can use this.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=lambda args, ctx: "\n".join([f"- {t['name']} (ID: {t['id']}): {t['frequency']}, Last run: {t['last_run_at'] or 'Never'}" for t in ctx["queen"].scheduler.store.get_scheduled_tasks()]),
            is_async=True,
        ),
        ToolSpec(
            name="check_schedule",
            description="Check for tasks that are due to run. Returns a list of actionable tasks and current UTC time. Only the Queen can use this.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=lambda args, ctx: f"Current UTC: {utc_now().isoformat()}\n\n" + (
                "No tasks are due at this time." if not ctx["queen"].scheduler.get_actionable_tasks() 
                else "The following tasks are due:\n" + "\n".join([f"### {t['name']}\n- ID: {t['id']}\n- Worker: {t['worker_id']}\n- Task: {t['task_text']}" for t in ctx["queen"].scheduler.get_actionable_tasks()])
            ),
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
            handler=lambda args, ctx: f"Task scheduled with ID: {ctx['queen'].scheduler.schedule_task(name=args['name'], frequency=args['frequency'], task_text=args['task'], description=args.get('description'), worker_id=args.get('worker_id'), inputs=args.get('inputs'))}",
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
    tools.extend(get_worker_tools())
    tools.extend(get_mcp_mgmt_tools())
    if mcp_manager:
        mcp_tools = mcp_manager.get_all_tools()
        if mcp_tools:
            logger.info("Injecting %d MCP tools into registry", len(mcp_tools))
            tools.extend(mcp_tools)
    return tools
