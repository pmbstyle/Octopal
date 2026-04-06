from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import structlog

from octopal.channels import normalize_user_channel, user_channel_label
from octopal.infrastructure.config.settings import load_settings
from octopal.tools.communication.send_file import send_file_to_user
from octopal.runtime.memory.memchain import (
    memchain_init,
    memchain_record,
    memchain_status,
    memchain_verify,
)
from octopal.runtime.metrics import read_metrics_snapshot
from octopal.runtime.state import is_pid_running, read_status
from octopal.tools.browser.actions import (
    browser_click,
    browser_close,
    browser_extract,
    browser_focus_tab,
    browser_navigate,
    browser_open,
    browser_screenshot,
    browser_snapshot,
    browser_tabs,
    browser_type,
    browser_wait_for,
    browser_workflow,
)
from octopal.tools.connectors.calendar import get_calendar_connector_tools
from octopal.tools.connectors.drive import get_drive_connector_tools
from octopal.tools.connectors.gmail import get_gmail_connector_tools
from octopal.tools.connectors.github import get_github_connector_tools
from octopal.tools.connectors.status import get_connector_status_tools
from octopal.tools.filesystem.download import download_file
from octopal.tools.filesystem.files import fs_delete, fs_list, fs_move, fs_read, fs_write
from octopal.tools.inventory import annotate_tool_specs
from octopal.tools.memory.canon import manage_canon, search_canon
from octopal.tools.memory.experiments import octo_experiment_log
from octopal.tools.ops.exec_run import exec_run
from octopal.tools.registry import ToolSpec
from octopal.tools.skills.management import get_registered_skill_tools, get_skill_management_tools
from octopal.tools.web.fetch import markdown_new_fetch, web_fetch
from octopal.tools.web.plan import fetch_plan_tool
from octopal.tools.web.search import web_search
from octopal.tools.workers.management import get_worker_tools
from octopal.utils import utc_now

logger = structlog.get_logger(__name__)


def _resolve_mcp_manager(ctx: dict[str, Any], fallback: Any) -> Any:
    octo = (ctx or {}).get("octo")
    if octo is not None and getattr(octo, "mcp_manager", None) is not None:
        return octo.mcp_manager
    return fallback


async def _tool_github_review_bundle(args, ctx) -> str:
    manager = _resolve_mcp_manager(ctx, ctx.get("mcp_manager"))
    if manager is None:
        return json.dumps(
            {
                "status": "error",
                "message": "GitHub review bundle is unavailable because no MCP manager is active.",
            },
            ensure_ascii=False,
        )

    owner = str((args or {}).get("owner", "") or "").strip()
    repo = str((args or {}).get("repo", "") or "").strip()
    pull_number = int((args or {}).get("pull_number", 0) or 0)
    file_limit = max(1, min(int((args or {}).get("file_limit", 100) or 100), 100))
    commit_limit = max(1, min(int((args or {}).get("commit_limit", 100) or 100), 100))
    review_limit = max(1, min(int((args or {}).get("review_limit", 100) or 100), 100))
    comment_limit = max(1, min(int((args or {}).get("comment_limit", 100) or 100), 100))
    include_commit_comments = bool((args or {}).get("include_commit_comments", True))

    if not owner or not repo or pull_number <= 0:
        return json.dumps(
            {
                "status": "error",
                "message": "owner, repo, and pull_number are required.",
            },
            ensure_ascii=False,
        )

    async def _call(tool_name: str, tool_args: dict[str, Any]) -> Any:
        result = await manager.call_tool(
            "github-core",
            tool_name,
            tool_args,
            allow_name_fallback=True,
        )
        content_items = getattr(result, "content", None)
        if not content_items:
            return result
        if len(content_items) == 1:
            text = getattr(content_items[0], "text", None)
            if isinstance(text, str):
                return json.loads(text)
        normalized: list[Any] = []
        for item in content_items:
            text = getattr(item, "text", None)
            if isinstance(text, str):
                normalized.append(json.loads(text))
            else:
                normalized.append(item.model_dump() if hasattr(item, "model_dump") else str(item))
        return normalized

    base_args = {"owner": owner, "repo": repo, "pull_number": pull_number}
    pr_payload, readiness_payload, reviews_payload, review_comments_payload, files_payload, commits_payload, convo_payload = await asyncio.gather(
        _call("get_pull_request", base_args),
        _call("get_pull_merge_readiness", base_args),
        _call("list_pull_reviews", {**base_args, "per_page": review_limit}),
        _call("list_pull_review_comments", {**base_args, "per_page": comment_limit}),
        _call("list_pull_files", {**base_args, "per_page": file_limit}),
        _call("list_pull_commits", {**base_args, "per_page": commit_limit}),
        _call("list_issue_comments", {"owner": owner, "repo": repo, "issue_number": pull_number, "per_page": comment_limit}),
    )

    commit_comments: dict[str, Any] = {}
    if include_commit_comments:
        commits = (commits_payload or {}).get("commits") or []
        for commit in commits[:commit_limit]:
            sha = str((commit or {}).get("sha", "") or "").strip()
            if not sha:
                continue
            try:
                commit_comments[sha] = await _call(
                    "list_commit_comments",
                    {"owner": owner, "repo": repo, "commit_sha": sha, "per_page": comment_limit},
                )
            except Exception as exc:
                commit_comments[sha] = {"status": "error", "message": str(exc)}

    payload = {
        "status": "ok",
        "owner": owner,
        "repo": repo,
        "pull_number": pull_number,
        "pull_request": pr_payload,
        "merge_readiness": (readiness_payload or {}).get("merge_readiness"),
        "review_summary": (readiness_payload or {}).get("review_summary"),
        "conversation_comments": (convo_payload or {}).get("comments", []),
        "reviews": (reviews_payload or {}).get("reviews", []),
        "review_comments": (review_comments_payload or {}).get("comments", []),
        "files": (files_payload or {}).get("files", []),
        "commits": (commits_payload or {}).get("commits", []),
        "commit_comments": commit_comments,
        "hints": [
            "Use files.patch and review_comments together when drafting code review feedback.",
            "Use merge_readiness.blocking_reviews and requested_reviewers to understand review state before commenting.",
            "Conversation comments are issue comments on the PR thread; inline code feedback lives in review_comments.",
        ],
    }
    return json.dumps(payload, ensure_ascii=False)


def _tool_catalog_search(args, ctx) -> str:
    query = str((args or {}).get("query", "") or "").strip().lower()
    category_filter = str((args or {}).get("category", "") or "").strip().lower()
    capability_filter = str((args or {}).get("capability", "") or "").strip().lower()
    limit = max(1, min(int((args or {}).get("limit", 12) or 12), 50))

    report = ctx.get("tool_resolution_report")
    if report is not None and hasattr(report, "available_tools"):
        candidates = list(report.available_tools)
    else:
        candidates = list(ctx.get("all_tool_specs") or [])

    active_names = {
        str(getattr(spec, "name", "") or "").strip().lower()
        for spec in (ctx.get("active_tool_specs") or [])
    }

    scored: list[tuple[int, ToolSpec]] = []
    for spec in candidates:
        if str(spec.name).strip().lower() == "tool_catalog_search":
            continue

        metadata = getattr(spec, "metadata", None)
        category = str(getattr(metadata, "category", "") or "").strip().lower()
        capabilities = tuple(getattr(metadata, "capabilities", ()) or ())
        profile_tags = tuple(getattr(metadata, "profile_tags", ()) or ())
        if category_filter and category != category_filter:
            continue
        if capability_filter and capability_filter not in capabilities:
            continue

        score = _tool_catalog_search_score(
            spec,
            query=query,
            category=category,
            capabilities=capabilities,
            profile_tags=profile_tags,
        )
        if query and score <= 0:
            continue
        scored.append((score, spec))

    scored.sort(
        key=lambda item: (
            -item[0],
            0 if str(getattr(item[1], "name", "") or "").strip().lower() not in active_names else 1,
            str(getattr(item[1], "name", "") or ""),
        )
    )

    items = []
    for score, spec in scored[:limit]:
        metadata = getattr(spec, "metadata", None)
        params = spec.parameters if isinstance(spec.parameters, dict) else {}
        properties = params.get("properties") if isinstance(params, dict) else {}
        required = params.get("required") if isinstance(params, dict) else []
        if not isinstance(properties, dict):
            properties = {}
        if not isinstance(required, list):
            required = []

        items.append(
            {
                "name": spec.name,
                "description": spec.description,
                "category": str(getattr(metadata, "category", "") or "misc"),
                "risk": str(getattr(metadata, "risk", "") or "safe"),
                "capabilities": list(getattr(metadata, "capabilities", ()) or ()),
                "profile_tags": list(getattr(metadata, "profile_tags", ()) or ()),
                "required_arguments": [str(item) for item in required if str(item).strip()],
                "argument_names": sorted(str(key) for key in properties.keys()),
                "active_now": str(spec.name).strip().lower() in active_names,
                "score": score,
            }
        )

    return json.dumps(
        {
            "status": "ok",
            "query": query or None,
            "category": category_filter or None,
            "capability": capability_filter or None,
            "count": len(items),
            "results": items,
            "hint": (
                "If the needed tool is not active right now, use this catalog result to decide what tool family you need next."
            ),
        },
        ensure_ascii=False,
    )


def _tool_catalog_search_score(
    spec: ToolSpec,
    *,
    query: str,
    category: str,
    capabilities: tuple[str, ...],
    profile_tags: tuple[str, ...],
) -> int:
    if not query:
        return 1

    name = str(getattr(spec, "name", "") or "").strip().lower()
    description = str(getattr(spec, "description", "") or "").strip().lower()
    query_terms = [term for term in query.replace("-", "_").split() if term]
    if not query_terms:
        query_terms = [query]

    score = 0
    for term in query_terms:
        if name == term:
            score += 120
        elif name.startswith(term):
            score += 80
        elif term in name:
            score += 55

        if category == term:
            score += 35
        elif term and term in category:
            score += 20

        if term in description:
            score += 12

        for capability in capabilities:
            if capability == term:
                score += 30
            elif term in capability:
                score += 16

        for tag in profile_tags:
            if tag == term:
                score += 18
            elif term in tag:
                score += 10

    return score


def get_tools(mcp_manager=None) -> list[ToolSpec]:
    tools = [
        ToolSpec(
            name="send_file_to_user",
            description="Send a local workspace file or a downloaded URL attachment to the active user channel. Only the Octo can use this.",
            parameters={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace-relative or workspace-absolute file path to send.",
                    },
                    "url": {
                        "type": "string",
                        "description": "HTTP(S) URL to download into workspace/tmp before sending.",
                    },
                    "filename": {
                        "type": "string",
                        "description": "Optional filename override when downloading from URL.",
                    },
                    "caption": {
                        "type": "string",
                        "description": "Optional message caption to attach to the file.",
                    },
                },
                "additionalProperties": False,
            },
            permission="self_control",
            handler=send_file_to_user,
            is_async=True,
        ),
        ToolSpec(
            name="manage_canon",
            description="Manage canonical memory files (facts.md, decisions.md, failures.md). Only the Octo can use this.",
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
            name="octo_context_health",
            description="Return current context-health metrics and reset decision state for the active chat.",
            parameters={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_octo_context_health,
            is_async=True,
        ),
        ToolSpec(
            name="tool_catalog_search",
            description=(
                "Search the full catalog of available Octo tools, including tools that may not be active in the current "
                "tool budget. Use this when the visible toolset seems insufficient for the task."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search text such as gmail, calendar, worker, drive, file, or schedule.",
                    },
                    "category": {
                        "type": "string",
                        "description": "Optional category filter such as connectors, browser, workers, or filesystem.",
                    },
                    "capability": {
                        "type": "string",
                        "description": "Optional capability filter such as gmail_read or connector_use.",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50,
                        "description": "Maximum number of matches to return.",
                    },
                },
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_catalog_search,
        ),
        ToolSpec(
            name="github_review_bundle",
            description=(
                "Collect a pull request review bundle from the GitHub connector: PR metadata, merge readiness, "
                "reviews, review comments, changed files, commits, and commit comments."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "pull_number": {"type": "integer", "minimum": 1},
                    "file_limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    "commit_limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    "review_limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    "comment_limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    "include_commit_comments": {"type": "boolean"},
                },
                "required": ["owner", "repo", "pull_number"],
                "additionalProperties": False,
            },
            permission="mcp_exec",
            handler=_tool_github_review_bundle,
            is_async=True,
        ),
        ToolSpec(
            name="octo_opportunity_scan",
            description="Generate proactive opportunity cards (impact/effort/confidence/next_action) for the active chat.",
            parameters={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": 5},
                },
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_octo_opportunity_scan,
            is_async=True,
        ),
        ToolSpec(
            name="octo_self_queue_add",
            description="Add a Octo-initiated task into self-driven queue.",
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
            handler=_tool_octo_self_queue_add,
            is_async=True,
        ),
        ToolSpec(
            name="octo_self_queue_list",
            description="List current Octo self-driven queue items.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=_tool_octo_self_queue_list,
            is_async=True,
        ),
        ToolSpec(
            name="octo_self_queue_take",
            description="Claim next pending task from Octo self-driven queue.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=_tool_octo_self_queue_take,
            is_async=True,
        ),
        ToolSpec(
            name="octo_self_queue_update",
            description="Update status of a Octo self-queue item.",
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
            handler=_tool_octo_self_queue_update,
            is_async=True,
        ),
        ToolSpec(
            name="octo_experiment_log",
            description="Append a compact self-improvement observation or experiment result to workspace/experiments/results.jsonl.",
            parameters={
                "type": "object",
                "properties": {
                    "problem": {"type": "string", "description": "Short description of the repeated inefficiency or issue."},
                    "classification": {
                        "type": "string",
                        "enum": ["behavioral", "system", "unclear"],
                        "description": "Use behavioral for soft inefficiency, system for reproducible technical faults, unclear when not yet classified.",
                    },
                    "source": {
                        "type": "string",
                        "enum": ["failures", "deliberation_audit", "manual_observation", "self_queue", "worker_result"],
                    },
                    "status": {
                        "type": "string",
                        "enum": ["observed", "proposed", "kept", "discarded"],
                    },
                    "evidence": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Short evidence bullets for why this issue is repeating.",
                    },
                    "change_summary": {"type": "string", "description": "Optional short note about the attempted change."},
                    "notes": {"type": "string", "description": "Optional short note or verdict detail."},
                },
                "required": ["problem"],
                "additionalProperties": False,
            },
            permission="self_control",
            handler=octo_experiment_log,
            is_async=True,
        ),
        ToolSpec(
            name="octo_memchain_status",
            description="Show current memchain integrity status for tracked workspace memory/config files.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=_tool_octo_memchain_status,
            is_async=True,
        ),
        ToolSpec(
            name="octo_memchain_verify",
            description="Verify memchain continuity and detect file drift for tracked workspace memory/config files.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=_tool_octo_memchain_verify,
            is_async=True,
        ),
        ToolSpec(
            name="octo_memchain_record",
            description="Record a new memchain snapshot for tracked workspace memory/config files.",
            parameters={
                "type": "object",
                "properties": {
                    "reason": {"type": "string", "description": "Reason for recording snapshot."},
                },
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_octo_memchain_record,
            is_async=True,
        ),
        ToolSpec(
            name="octo_memchain_init",
            description="Initialize or reinitialize memchain files in workspace memory.",
            parameters={
                "type": "object",
                "properties": {
                    "force": {"type": "boolean", "description": "If true, reinitialize chain files."},
                },
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_octo_memchain_init,
            is_async=True,
        ),
        ToolSpec(
            name="list_schedule",
            description="List all scheduled tasks and their status. Only the Octo can use this.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=lambda args, ctx: "\n".join([f"- {t['name']} (ID: {t['id']}): {t['frequency']}, Last run: {t['last_run_at'] or 'Never'}" for t in ctx["octo"].scheduler.store.get_scheduled_tasks()]),
            is_async=True,
        ),
        ToolSpec(
            name="check_schedule",
            description="Check for tasks that are due to run. Returns machine-readable JSON with due tasks and current UTC time. Only the Octo can use this.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=_tool_check_schedule,
            is_async=True,
        ),
        ToolSpec(
            name="scheduler_status",
            description="Summarize scheduler state with due tasks, next-run previews, and hints about what the Octo should do next.",
            parameters={
                "type": "object",
                "properties": {
                    "enabled_only": {
                        "type": "boolean",
                        "description": "If true, only include enabled tasks.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum tasks to include in the preview (default: 20, max: 50).",
                    },
                },
                "additionalProperties": False,
            },
            permission="self_control",
            handler=_tool_scheduler_status,
            is_async=True,
        ),
        ToolSpec(
            name="schedule_task",
            description="Add or update a scheduled task. Only the Octo can use this.",
            parameters={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Human-readable name of the task."},
                    "frequency": {"type": "string", "description": "Frequency (e.g., 'Every 30 minutes', 'Daily at 14:00')."},
                    "task": {"type": "string", "description": "The task description for the worker or Octo."},
                    "description": {"type": "string", "description": "Brief description of the task purpose."},
                    "worker_id": {"type": "string", "description": "Optional: Specific worker template ID to use."},
                    "inputs": {"type": "object", "description": "Optional: Inputs for the worker."},
                    "notify_user": {
                        "type": "string",
                        "enum": ["never", "if_significant", "always"],
                        "description": "When the user should hear about this scheduled task: never, only if significant, or always.",
                    },
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
            description="Remove a scheduled task by ID. Only the Octo can use this.",
            parameters={
                "type": "object",
                "properties": {
                    "task_id": {"type": "string", "description": "The ID of the task to remove (e.g., 'check_emails')."},
                },
                "required": ["task_id"],
                "additionalProperties": False,
            },
            permission="self_control",
            handler=lambda args, ctx: (ctx["octo"].scheduler.remove_task(args["task_id"]), "Task removed.")[1],
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
            handler=_tool_run_llm_subtask,
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
            description="Search the web via the configured provider registry (auto, Brave, or Firecrawl) and return structured JSON results.",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query text."},
                    "provider": {
                        "type": "string",
                        "description": "Optional search provider override.",
                        "enum": ["auto", "brave", "firecrawl"],
                    },
                    "count": {"type": "integer", "description": "Max results to return (1-10)."},
                    "country": {"type": "string", "description": "Country code for localization (e.g., US, CA)."},
                    "search_lang": {"type": "string", "description": "Search language (e.g., en)."},
                    "ui_lang": {"type": "string", "description": "UI language (e.g., en)."},
                    "location": {"type": "string", "description": "Optional location hint for providers that support it."},
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
                    "target_id": {"type": "string", "description": "Optional existing tab target id."},
                    "new_tab": {"type": "boolean", "description": "Open in a new tab before loading the URL."},
                },
                "required": ["url"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: browser_open(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_tabs",
            description="List open browser tabs for this chat session, including the active target id.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="network",
            handler=lambda args, ctx: browser_tabs(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_focus_tab",
            description="Switch the active browser tab using a target id returned by browser_tabs.",
            parameters={
                "type": "object",
                "properties": {
                    "target_id": {"type": "string", "description": "Tab target id (e.g. 't1')."},
                },
                "required": ["target_id"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: browser_focus_tab(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_snapshot",
            description="Capture an accessibility snapshot of the current page. Provides [ref=eN] tags for interacting with elements.",
            parameters={
                "type": "object",
                "properties": {
                    "target_id": {"type": "string", "description": "Optional tab target id to snapshot."},
                },
                "additionalProperties": False,
            },
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
                    "target_id": {"type": "string", "description": "Optional tab target id for the ref lookup."},
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
                    "target_id": {"type": "string", "description": "Optional tab target id for the ref lookup."},
                },
                "required": ["ref", "text"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: browser_type(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_navigate",
            description="Navigate the current or specified browser tab to a new URL.",
            parameters={
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "URL to navigate to."},
                    "target_id": {"type": "string", "description": "Optional tab target id."},
                },
                "required": ["url"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: browser_navigate(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_close",
            description="Close the browser session for the current chat.",
            parameters={
                "type": "object",
                "properties": {
                    "target_id": {"type": "string", "description": "Optional tab target id to close instead of the full session."},
                },
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: browser_close(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_wait_for",
            description="Wait for a browser element ref or visible text to appear before continuing.",
            parameters={
                "type": "object",
                "properties": {
                    "ref": {"type": "string", "description": "Optional element reference from browser_snapshot."},
                    "text": {"type": "string", "description": "Optional visible text to wait for."},
                    "target_id": {"type": "string", "description": "Optional tab target id."},
                    "state": {
                        "type": "string",
                        "description": "Desired locator state.",
                        "enum": ["attached", "detached", "hidden", "visible"],
                    },
                    "timeout_ms": {"type": "integer", "description": "Timeout in milliseconds (default 10000)."},
                },
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: browser_wait_for(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_extract",
            description="Extract visible text from the current page or a specific browser ref.",
            parameters={
                "type": "object",
                "properties": {
                    "ref": {"type": "string", "description": "Optional element reference from browser_snapshot."},
                    "max_chars": {"type": "integer", "description": "Maximum text length to return (100-20000)."},
                    "target_id": {"type": "string", "description": "Optional tab target id."},
                },
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: browser_extract(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_screenshot",
            description="Capture a PNG screenshot of the current or specified browser tab.",
            parameters={
                "type": "object",
                "properties": {
                    "target_id": {"type": "string", "description": "Optional tab target id."},
                    "full_page": {"type": "boolean", "description": "Capture the full page instead of only the viewport."},
                },
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: browser_screenshot(args, ctx),
            is_async=True,
        ),
        ToolSpec(
            name="browser_workflow",
            description="Run a short browser workflow as one tool call by sequencing browser actions like open, tabs, focus_tab, navigate, snapshot, screenshot, click, type, wait_for, extract, and close.",
            parameters={
                "type": "object",
                "properties": {
                    "steps": {
                        "type": "array",
                        "description": "Ordered workflow steps.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "action": {
                                    "type": "string",
                                    "enum": ["open", "tabs", "focus_tab", "navigate", "snapshot", "screenshot", "click", "type", "wait_for", "extract", "close"],
                                },
                                "url": {"type": "string"},
                                "target_id": {"type": "string"},
                                "new_tab": {"type": "boolean"},
                                "ref": {"type": "string"},
                                "text": {"type": "string"},
                                "press_enter": {"type": "boolean"},
                                "full_page": {"type": "boolean"},
                                "state": {"type": "string", "enum": ["attached", "detached", "hidden", "visible"]},
                                "timeout_ms": {"type": "integer"},
                                "max_chars": {"type": "integer"},
                            },
                            "required": ["action"],
                            "additionalProperties": False,
                        },
                    },
                    "stop_on_error": {
                        "type": "boolean",
                        "description": "Stop after the first failed step (default: true).",
                    },
                },
                "required": ["steps"],
                "additionalProperties": False,
            },
            permission="network",
            handler=lambda args, ctx: browser_workflow(args, ctx),
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
            handler=lambda args, ctx: fs_read(args, ctx),
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
            handler=lambda args, ctx: fs_write(args, ctx),
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
            handler=lambda args, ctx: fs_list(args, ctx),
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
            handler=lambda args, ctx: fs_move(args, ctx),
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
            handler=lambda args, ctx: fs_delete(args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("service_health", args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("service_logs", args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("docker_compose_control", args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("git_ops", args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("process_inspect", args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("db_backup", args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("db_restore", args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("db_maintenance", args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("db_query_readonly", args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("secret_scan", args, ctx),
        ),
        ToolSpec(
            name="config_audit",
            description="Audit runtime configuration presence and critical keys.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="security_audit",
            handler=lambda args, ctx: _tool_ops_management("config_audit", args, ctx),
        ),
        ToolSpec(
            name="gateway_status",
            description="Read-only control-plane snapshot for gateway, octo, active channel, exec sessions, and MCP connectivity.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="service_read",
            handler=lambda args, ctx: _tool_gateway_status(args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("test_run", args, ctx),
        ),
        ToolSpec(
            name="coverage_report",
            description="Read coverage.xml summary if available.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="exec",
            handler=lambda args, ctx: _tool_ops_management("coverage_report", args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("artifact_collect", args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("release_snapshot", args, ctx),
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
            handler=lambda args, ctx: _tool_ops_management("rollback_release", args, ctx),
        ),
        ToolSpec(
            name="octo_context_reset",
            description="Compact or reset Octo chat context with a structured handoff and wake-up directive.",
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
            handler=_tool_octo_context_reset,
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
            handler=lambda args, ctx: _tool_ops_management("self_control", args, ctx),
        ),
    ]
    tools.extend(get_skill_management_tools())
    tools.extend(get_registered_skill_tools())
    tools.extend(get_worker_tools())
    tools.extend(get_connector_status_tools())
    tools.extend(get_calendar_connector_tools(mcp_manager))
    tools.extend(get_drive_connector_tools(mcp_manager))
    tools.extend(get_gmail_connector_tools(mcp_manager))
    tools.extend(get_github_connector_tools(mcp_manager))
    tools.extend(_get_mcp_management_tools())
    if mcp_manager:
        mcp_tools = mcp_manager.get_all_tools()
        if mcp_tools:
            logger.info("Injecting %d MCP tools into registry", len(mcp_tools))
            tools.extend(mcp_tools)
    return annotate_tool_specs(tools)


def _tool_run_llm_subtask(args, ctx):
    from octopal.tools.llm.subtask import run_llm_subtask

    return run_llm_subtask(args, ctx["octo"].provider)


def _tool_ops_management(name: str, args, ctx):
    from octopal.tools.ops import management as ops_management

    handler = getattr(ops_management, name)
    return handler(args, ctx)


def _get_mcp_management_tools() -> list[ToolSpec]:
    try:
        from octopal.tools.mcp.management import get_mcp_mgmt_tools
    except ModuleNotFoundError as exc:
        if exc.name == "mcp":
            return _get_fallback_mcp_management_tools()
        raise

    return get_mcp_mgmt_tools()


def _fallback_mcp_unavailable(_args, _ctx) -> str:
    return "Error: MCP dependencies are not installed in this environment."


def _get_fallback_mcp_management_tools() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="mcp_connect",
            description="Connect to an external MCP server.",
            parameters={"type": "object", "properties": {}, "additionalProperties": True},
            permission="self_control",
            handler=_fallback_mcp_unavailable,
            is_async=True,
        ),
        ToolSpec(
            name="mcp_disconnect",
            description="Disconnect from an MCP server.",
            parameters={"type": "object", "properties": {}, "additionalProperties": True},
            permission="self_control",
            handler=_fallback_mcp_unavailable,
            is_async=True,
        ),
        ToolSpec(
            name="mcp_list",
            description="List active MCP servers and their tools.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=_fallback_mcp_unavailable,
        ),
        ToolSpec(
            name="mcp_status",
            description="Show status for all known MCP servers.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=_fallback_mcp_unavailable,
        ),
        ToolSpec(
            name="mcp_call",
            description="Call an MCP tool on a specific server.",
            parameters={"type": "object", "properties": {}, "additionalProperties": True},
            permission="mcp_exec",
            handler=_fallback_mcp_unavailable,
            is_async=True,
        ),
        ToolSpec(
            name="mcp_discover",
            description="Summarize MCP server readiness, exposed tools, and next actions.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="self_control",
            handler=_fallback_mcp_unavailable,
        ),
    ]


async def _tool_check_schedule(args, ctx) -> str:
    scheduler = ctx["octo"].scheduler
    due_tasks = scheduler.get_actionable_tasks()
    octo = ctx.get("octo")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    context_health = None
    opportunity_snapshot = None
    self_queue = None
    if octo is not None and hasattr(octo, "get_context_health_snapshot"):
        try:
            maybe = octo.get_context_health_snapshot(chat_id)
            if asyncio.iscoroutine(maybe):
                context_health = await maybe
            else:
                context_health = maybe
        except Exception:
            context_health = None
    if octo is not None and hasattr(octo, "scan_opportunities"):
        try:
            maybe = octo.scan_opportunities(chat_id, limit=3)
            if asyncio.iscoroutine(maybe):
                opportunity_snapshot = await maybe
            else:
                opportunity_snapshot = maybe
        except Exception:
            opportunity_snapshot = None
    if octo is not None and hasattr(octo, "get_self_queue"):
        try:
            maybe = octo.get_self_queue(chat_id)
            if asyncio.iscoroutine(maybe):
                self_queue = await maybe
            else:
                self_queue = maybe
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
                "notify_user": t.get("notify_user"),
            }
            for t in due_tasks
        ],
    }
    return json.dumps(payload, ensure_ascii=False)


async def _tool_scheduler_status(args, ctx) -> str:
    scheduler = ctx["octo"].scheduler
    enabled_only = bool((args or {}).get("enabled_only", False))
    limit = max(1, min(50, int((args or {}).get("limit") or 20)))
    described = scheduler.describe_tasks(enabled_only=enabled_only)
    preview = described[:limit]
    due_count = sum(1 for task in described if bool(task.get("due_now")))
    disabled_count = sum(1 for task in described if int(task.get("enabled", 1) or 0) != 1)
    hints: list[str] = []
    if due_count > 0:
        hints.append(f"{due_count} scheduled task(s) are due now; run check_schedule or dispatch work.")
    if disabled_count > 0 and not enabled_only:
        hints.append(f"{disabled_count} scheduled task(s) are disabled and will not run until re-enabled.")
    if any(task.get("overdue") for task in described):
        hints.append("At least one scheduled task looks overdue; inspect execution flow or worker failures.")
    if not hints:
        hints.append("Scheduler looks healthy. Use next-run previews to plan follow-up work.")

    next_due = next((task for task in described if task.get("next_run_at")), None)
    payload = {
        "status": "ok",
        "enabled_only": enabled_only,
        "task_count": len(described),
        "due_count": due_count,
        "disabled_count": disabled_count,
        "next_due_task": (
            {
                "task_id": next_due.get("id"),
                "name": next_due.get("name"),
                "next_run_at": next_due.get("next_run_at"),
                "due_now": bool(next_due.get("due_now")),
            }
            if next_due
            else None
        ),
        "tasks": [
            {
                "task_id": task.get("id"),
                "name": task.get("name"),
                "frequency": task.get("frequency"),
                "worker_id": task.get("worker_id"),
                "enabled": bool(int(task.get("enabled", 1) or 0) == 1),
                "due_now": bool(task.get("due_now")),
                "overdue": bool(task.get("overdue")),
                "next_run_at": task.get("next_run_at"),
                "last_run_at": task.get("last_run_at"),
                "description": task.get("description"),
                "notify_user": task.get("notify_user"),
            }
            for task in preview
        ],
        "hints": hints,
    }
    return json.dumps(payload, ensure_ascii=False)


def _tool_schedule_task(args, ctx) -> str:
    try:
        task_id = ctx["octo"].scheduler.schedule_task(
            name=args["name"],
            frequency=args["frequency"],
            task_text=args["task"],
            description=args.get("description"),
            worker_id=args.get("worker_id"),
            inputs=args.get("inputs"),
            notify_user=args.get("notify_user"),
        )
    except ValueError as exc:
        return f"schedule_task error: {exc}"

    return json.dumps(
        {
            "status": "scheduled",
            "task_id": task_id,
            "name": args["name"],
            "frequency": args["frequency"],
            "notify_user": args.get("notify_user", "if_significant"),
        },
        ensure_ascii=False,
    )


def _tool_gateway_status(args, ctx) -> str:
    del args, ctx
    settings = load_settings()
    status_data = read_status(settings) or {}
    metrics = read_metrics_snapshot(settings.state_dir) or {}

    pid = status_data.get("pid")
    running = is_pid_running(pid)
    active_channel = normalize_user_channel(
        str(status_data.get("active_channel", "") or settings.user_channel)
    )
    active_channel_label = user_channel_label(active_channel)
    octo_metrics = metrics.get("octo", {}) if isinstance(metrics, dict) else {}
    telegram_metrics = metrics.get("telegram", {}) if isinstance(metrics, dict) else {}
    whatsapp_metrics = metrics.get("whatsapp", {}) if isinstance(metrics, dict) else {}
    exec_metrics = metrics.get("exec_run", {}) if isinstance(metrics, dict) else {}
    connectivity_metrics = metrics.get("connectivity", {}) if isinstance(metrics, dict) else {}
    active_channel_metrics = whatsapp_metrics if active_channel == "whatsapp" else telegram_metrics

    services = [
        {
            "id": "gateway",
            "status": "ok" if running else "critical",
            "reason": "running" if running else "process is not running",
            "updated_at": status_data.get("last_message_at"),
        },
        {
            "id": "octo",
            "status": _gateway_octo_status(octo_metrics),
            "reason": _gateway_octo_reason(octo_metrics),
            "updated_at": octo_metrics.get("updated_at"),
        },
        {
            "id": active_channel,
            "status": _gateway_channel_status(active_channel, active_channel_metrics),
            "reason": _gateway_channel_reason(active_channel, active_channel_metrics),
            "updated_at": active_channel_metrics.get("updated_at"),
        },
        {
            "id": "mcp",
            "status": "ok" if _mcp_connected_count(connectivity_metrics) > 0 else "warning",
            "reason": _gateway_mcp_reason(connectivity_metrics),
            "updated_at": connectivity_metrics.get("updated_at"),
        },
    ]
    hints: list[str] = []
    if not running:
        hints.append("Gateway process is down; restart the Octopal runtime before expecting channel traffic.")
    if int(octo_metrics.get("followup_queues", 0) or 0) > 0:
        hints.append("Octo follow-up queue is non-empty; check worker/gateway traffic before spawning more work.")
    if active_channel == "telegram" and int(telegram_metrics.get("chat_queues", 0) or 0) > 0:
        hints.append("Telegram queue depth is elevated; outbound delivery may be catching up.")
    if active_channel == "whatsapp" and int(bool(whatsapp_metrics.get("connected", 0))) == 0:
        hints.append("WhatsApp bridge is not connected; expect delivery issues until it reconnects.")
    if _mcp_connected_count(connectivity_metrics) == 0:
        hints.append("No MCP servers are currently connected; tool availability may be reduced.")
    if not hints:
        hints.append("Gateway control plane looks healthy.")

    return json.dumps(
        {
            "status": "ok",
            "running": running,
            "pid": pid,
            "started_at": status_data.get("started_at"),
            "last_heartbeat": status_data.get("last_message_at"),
            "gateway": {
                "host": settings.gateway_host,
                "port": settings.gateway_port,
                "active_channel": active_channel,
                "active_channel_label": active_channel_label,
            },
            "octo": {
                "followup_queues": int(octo_metrics.get("followup_queues", 0) or 0),
                "internal_queues": int(octo_metrics.get("internal_queues", 0) or 0),
                "followup_tasks": int(octo_metrics.get("followup_tasks", 0) or 0),
                "internal_tasks": int(octo_metrics.get("internal_tasks", 0) or 0),
                "thinking_count": int(octo_metrics.get("thinking_count", 0) or 0),
                "updated_at": octo_metrics.get("updated_at"),
            },
            "channel": {
                "id": active_channel,
                "label": active_channel_label,
                "updated_at": active_channel_metrics.get("updated_at"),
                "queue_depth": int(active_channel_metrics.get("chat_queues", 0) or 0)
                if active_channel == "telegram"
                else 0,
                "send_tasks": int(active_channel_metrics.get("send_tasks", 0) or 0)
                if active_channel == "telegram"
                else None,
                "connected": (
                    None
                    if active_channel != "whatsapp" and "connected" not in active_channel_metrics
                    else active_channel_metrics.get("connected")
                ),
                "chat_mappings": active_channel_metrics.get("chat_mappings"),
            },
            "exec": {
                "sessions_running": int(exec_metrics.get("background_sessions_running", 0) or 0),
                "sessions_total": int(exec_metrics.get("background_sessions_total", 0) or 0),
                "updated_at": exec_metrics.get("updated_at"),
            },
            "mcp": {
                "servers_total": _mcp_server_total(connectivity_metrics),
                "servers_connected": _mcp_connected_count(connectivity_metrics),
                "updated_at": connectivity_metrics.get("updated_at"),
            },
            "services": services,
            "hints": hints,
        },
        ensure_ascii=False,
    )


async def _tool_octo_context_reset(args, ctx) -> str:
    octo = ctx.get("octo")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if octo is None or not hasattr(octo, "request_context_reset"):
        return json.dumps({"status": "error", "message": "octo context reset is unavailable"}, ensure_ascii=False)
    result = await octo.request_context_reset(chat_id, args or {})
    return json.dumps(result, ensure_ascii=False)


async def _tool_octo_context_health(args, ctx) -> str:
    octo = ctx.get("octo")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if octo is None or not hasattr(octo, "get_context_health_snapshot"):
        return json.dumps({"status": "error", "message": "octo context health is unavailable"}, ensure_ascii=False)
    snapshot = await octo.get_context_health_snapshot(chat_id)
    thresholds = (
        octo.get_context_thresholds()
        if hasattr(octo, "get_context_thresholds")
        else {
            "watch": {
                "context_size_estimate": 60000,
                "repetition_score": 0.65,
                "error_streak": 3,
                "no_progress_turns": 4,
            },
            "reset_soon": {
                "context_size_estimate": 100000,
                "repetition_score": 0.75,
                "error_streak": 5,
                "no_progress_turns": 7,
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
    return Path(os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")).resolve()


def _gateway_octo_status(octo_metrics: dict[str, object]) -> str:
    followup = int(octo_metrics.get("followup_queues", 0) or 0)
    internal = int(octo_metrics.get("internal_queues", 0) or 0)
    queue_pressure = followup + internal
    if queue_pressure >= 10:
        return "warning"
    return "ok"


def _gateway_octo_reason(octo_metrics: dict[str, object]) -> str:
    followup = int(octo_metrics.get("followup_queues", 0) or 0)
    internal = int(octo_metrics.get("internal_queues", 0) or 0)
    queue_pressure = followup + internal
    if queue_pressure <= 0:
        return "queues clear"
    return f"queue pressure {queue_pressure} (followup={followup}, internal={internal})"


def _gateway_channel_status(channel_id: str, channel_metrics: dict[str, object]) -> str:
    if channel_id == "whatsapp":
        connected = channel_metrics.get("connected")
        if connected in {0}:
            return "critical"
        return "ok" if connected in {1} else "warning"
    queue_depth = int(channel_metrics.get("chat_queues", 0) or 0)
    if queue_depth >= 40:
        return "critical"
    if queue_depth >= 15:
        return "warning"
    return "ok"


def _gateway_channel_reason(channel_id: str, channel_metrics: dict[str, object]) -> str:
    if channel_id == "whatsapp":
        connected = channel_metrics.get("connected")
        mappings = int(channel_metrics.get("chat_mappings", 0) or 0)
        if connected in {0}:
            return "bridge disconnected"
        if connected in {1}:
            return f"connected ({mappings} mapped chat(s))" if mappings > 0 else "connected"
        return "awaiting bridge status"
    queue_depth = int(channel_metrics.get("chat_queues", 0) or 0)
    send_tasks = int(channel_metrics.get("send_tasks", 0) or 0)
    if queue_depth <= 0 and send_tasks <= 0:
        return "healthy"
    return f"queues={queue_depth}, send_tasks={send_tasks}"


def _mcp_server_total(connectivity_metrics: dict[str, object]) -> int:
    servers = connectivity_metrics.get("mcp_servers", {})
    return len(servers) if isinstance(servers, dict) else 0


def _mcp_connected_count(connectivity_metrics: dict[str, object]) -> int:
    servers = connectivity_metrics.get("mcp_servers", {})
    if not isinstance(servers, dict):
        return 0
    connected = 0
    for payload in servers.values():
        if isinstance(payload, dict) and payload.get("connected"):
            connected += 1
    return connected


def _gateway_mcp_reason(connectivity_metrics: dict[str, object]) -> str:
    total = _mcp_server_total(connectivity_metrics)
    connected = _mcp_connected_count(connectivity_metrics)
    if total <= 0:
        return "no configured MCP servers reporting metrics"
    return f"{connected}/{total} server(s) connected"


async def _tool_octo_memchain_status(args, ctx) -> str:
    payload = await asyncio.to_thread(memchain_status, _workspace_dir())
    return json.dumps(payload, ensure_ascii=False)


async def _tool_octo_memchain_verify(args, ctx) -> str:
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


async def _tool_octo_memchain_record(args, ctx) -> str:
    reason = str((args or {}).get("reason", "octo_manual") or "octo_manual")
    payload = await asyncio.to_thread(
        memchain_record,
        _workspace_dir(),
        reason=reason,
        meta={"source": "octo_tool", "chat_id": int(ctx.get("chat_id", 0) or 0)},
    )
    return json.dumps(payload, ensure_ascii=False)


async def _tool_octo_memchain_init(args, ctx) -> str:
    force = bool((args or {}).get("force", False))
    payload = await asyncio.to_thread(memchain_init, _workspace_dir(), force=force)
    return json.dumps(payload, ensure_ascii=False)


async def _tool_octo_opportunity_scan(args, ctx) -> str:
    octo = ctx.get("octo")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if octo is None or not hasattr(octo, "scan_opportunities"):
        return json.dumps({"status": "error", "message": "octo opportunity scan is unavailable"}, ensure_ascii=False)
    limit = int((args or {}).get("limit", 3) or 3)
    payload = await octo.scan_opportunities(chat_id, limit=limit)
    return json.dumps(payload, ensure_ascii=False)


async def _tool_octo_self_queue_add(args, ctx) -> str:
    octo = ctx.get("octo")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if octo is None or not hasattr(octo, "add_self_queue_item"):
        return json.dumps({"status": "error", "message": "octo self queue is unavailable"}, ensure_ascii=False)
    payload = await octo.add_self_queue_item(chat_id, args or {})
    return json.dumps(payload, ensure_ascii=False)


async def _tool_octo_self_queue_list(args, ctx) -> str:
    octo = ctx.get("octo")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if octo is None or not hasattr(octo, "get_self_queue"):
        return json.dumps({"status": "error", "message": "octo self queue is unavailable"}, ensure_ascii=False)
    items = await octo.get_self_queue(chat_id)
    return json.dumps({"status": "ok", "chat_id": chat_id, "items": items, "count": len(items)}, ensure_ascii=False)


async def _tool_octo_self_queue_take(args, ctx) -> str:
    octo = ctx.get("octo")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if octo is None or not hasattr(octo, "take_next_self_queue_item"):
        return json.dumps({"status": "error", "message": "octo self queue is unavailable"}, ensure_ascii=False)
    payload = await octo.take_next_self_queue_item(chat_id)
    return json.dumps(payload, ensure_ascii=False)


async def _tool_octo_self_queue_update(args, ctx) -> str:
    octo = ctx.get("octo")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    if octo is None or not hasattr(octo, "update_self_queue_item"):
        return json.dumps({"status": "error", "message": "octo self queue is unavailable"}, ensure_ascii=False)
    payload = await octo.update_self_queue_item(chat_id, args or {})
    return json.dumps(payload, ensure_ascii=False)
