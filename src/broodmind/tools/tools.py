from __future__ import annotations

from broodmind.tools.download_file import download_file
from broodmind.tools.exec_run import exec_run
from broodmind.tools.filesystem import fs_delete, fs_list, fs_move, fs_read, fs_write
from broodmind.tools.llm_task import run_llm_subtask
from broodmind.tools.registry import ToolSpec
from broodmind.tools.web_fetch import web_fetch
from broodmind.tools.web_search import web_search
from broodmind.tools.worker_tools import get_worker_tools


def get_tools() -> list[ToolSpec]:
    tools = [
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
        ),
        ToolSpec(
            name="web_fetch",
            description="Fetch a URL and return a JSON payload with status_code, content_type, and snippet.",
            parameters={
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "URL to fetch (http/https only)."},
                    "max_chars": {
                        "type": "integer",
                        "description": "Max characters of content to return (200-200000).",
                    },
                    "headers": {
                        "type": "object",
                        "description": "Optional dictionary of custom request headers (e.g. for API tokens).",
                    },
                },
                "required": ["url"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: web_fetch(args),
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
                "required": ["command"],
                "additionalProperties": False,
            },
            permission="exec",
            handler=lambda args, ctx: exec_run(args, ctx["base_dir"]),
        ),
    ]
    tools.extend(get_worker_tools())
    return tools
