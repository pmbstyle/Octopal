from __future__ import annotations

import json
from typing import Any

from octopal.tools.metadata import ToolMetadata
from octopal.tools.registry import ToolSpec

_GITHUB_SERVER_ID = "github-core"


def _extract_mcp_payload(result: Any) -> Any:
    content_items = getattr(result, "content", None)
    if not content_items:
        return result

    if len(content_items) == 1:
        item = content_items[0]
        text = getattr(item, "text", None)
        if isinstance(text, str):
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text
        if hasattr(item, "model_dump"):
            return item.model_dump()
        return str(item)

    normalized: list[Any] = []
    for item in content_items:
        text = getattr(item, "text", None)
        if isinstance(text, str):
            try:
                normalized.append(json.loads(text))
            except json.JSONDecodeError:
                normalized.append(text)
            continue
        if hasattr(item, "model_dump"):
            normalized.append(item.model_dump())
            continue
        normalized.append(str(item))
    return normalized


def _resolve_mcp_manager(ctx: dict[str, Any], fallback: Any) -> Any:
    octo = (ctx or {}).get("octo")
    if octo is not None and getattr(octo, "mcp_manager", None) is not None:
        return octo.mcp_manager
    return fallback


async def _github_mcp_proxy(
    remote_tool_name: str,
    args: dict[str, Any],
    ctx: dict[str, Any],
    *,
    fallback_manager: Any,
) -> Any:
    manager = _resolve_mcp_manager(ctx, fallback_manager)
    if manager is None:
        return {
            "ok": False,
            "error": "GitHub tools are unavailable because no MCP manager is active.",
            "hint": "Restart Octopal after authorizing the GitHub connector.",
        }

    try:
        result = await manager.call_tool(
            _GITHUB_SERVER_ID,
            remote_tool_name,
            args or {},
            allow_name_fallback=True,
        )
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "server_id": _GITHUB_SERVER_ID,
            "tool": remote_tool_name,
            "hint": "Check connector status and confirm the GitHub MCP server is connected.",
        }

    return _extract_mcp_payload(result)


def _github_tool(
    *,
    name: str,
    remote_tool_name: str,
    description: str,
    parameters: dict[str, Any],
    fallback_manager: Any,
    capabilities: tuple[str, ...],
) -> ToolSpec:
    return ToolSpec(
        name=name,
        description=description,
        parameters=parameters,
        permission="mcp_exec",
        handler=lambda args, ctx, _remote=remote_tool_name, _manager=fallback_manager: _github_mcp_proxy(
            _remote,
            args,
            ctx,
            fallback_manager=_manager,
        ),
        is_async=True,
        server_id=_GITHUB_SERVER_ID,
        remote_tool_name=remote_tool_name,
        metadata=ToolMetadata(
            category="connectors",
            risk="safe",
            profile_tags=("research", "engineering"),
            capabilities=capabilities,
        ),
    )


def get_github_connector_tools(mcp_manager: Any = None) -> list[ToolSpec]:
    if mcp_manager is None:
        return []

    return [
        _github_tool(
            name="github_get_authenticated_user",
            remote_tool_name="get_authenticated_user",
            description="Get the connected GitHub user profile to confirm which account is active.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            fallback_manager=mcp_manager,
            capabilities=("github_read", "connector_use"),
        ),
        _github_tool(
            name="github_list_repositories",
            remote_tool_name="list_repositories",
            description="List repositories visible to the connected GitHub account.",
            parameters={
                "type": "object",
                "properties": {
                    "visibility": {"type": "string"},
                    "affiliation": {"type": "string"},
                    "sort": {"type": "string"},
                    "direction": {"type": "string"},
                    "per_page": {"type": "integer", "minimum": 1, "maximum": 100},
                    "page": {"type": "integer", "minimum": 1},
                },
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_read", "connector_use"),
        ),
        _github_tool(
            name="github_get_repository",
            remote_tool_name="get_repository",
            description="Read metadata for a specific GitHub repository by owner and repo name.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                },
                "required": ["owner", "repo"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_read", "connector_use"),
        ),
        _github_tool(
            name="github_list_issues",
            remote_tool_name="list_issues",
            description="List issues for a repository, optionally filtered by state, labels, or time.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "state": {"type": "string"},
                    "labels": {"type": "string"},
                    "since": {"type": "string"},
                    "per_page": {"type": "integer", "minimum": 1, "maximum": 100},
                    "page": {"type": "integer", "minimum": 1},
                },
                "required": ["owner", "repo"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_issue_read", "connector_use"),
        ),
        _github_tool(
            name="github_get_issue",
            remote_tool_name="get_issue",
            description="Read a GitHub issue by owner, repo, and issue number.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "issue_number": {"type": "integer", "minimum": 1},
                },
                "required": ["owner", "repo", "issue_number"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_issue_read", "connector_use"),
        ),
        _github_tool(
            name="github_create_issue",
            remote_tool_name="create_issue",
            description="Create a GitHub issue in a repository.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "title": {"type": "string"},
                    "body": {"type": "string"},
                    "assignees": {"type": "array", "items": {"type": "string"}},
                    "labels": {"type": "array", "items": {"type": "string"}},
                    "milestone": {"type": "integer", "minimum": 0},
                },
                "required": ["owner", "repo", "title"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_issue_write", "connector_use"),
        ),
        _github_tool(
            name="github_update_issue",
            remote_tool_name="update_issue",
            description="Update mutable fields on a GitHub issue, including body, labels, assignees, or state.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "issue_number": {"type": "integer", "minimum": 1},
                    "title": {"type": "string"},
                    "body": {"type": "string"},
                    "assignees": {"type": "array", "items": {"type": "string"}},
                    "labels": {"type": "array", "items": {"type": "string"}},
                    "milestone": {"type": "integer", "minimum": 0},
                    "state": {"type": "string"},
                    "state_reason": {"type": "string"},
                },
                "required": ["owner", "repo", "issue_number"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_issue_write", "connector_use"),
        ),
        _github_tool(
            name="github_list_issue_comments",
            remote_tool_name="list_issue_comments",
            description="List conversation comments for a GitHub issue or pull request.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "issue_number": {"type": "integer", "minimum": 1},
                    "per_page": {"type": "integer", "minimum": 1, "maximum": 100},
                    "page": {"type": "integer", "minimum": 1},
                },
                "required": ["owner", "repo", "issue_number"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_issue_read", "connector_use"),
        ),
        _github_tool(
            name="github_create_issue_comment",
            remote_tool_name="create_issue_comment",
            description="Create a conversation comment on a GitHub issue or pull request.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "issue_number": {"type": "integer", "minimum": 1},
                    "body": {"type": "string"},
                },
                "required": ["owner", "repo", "issue_number", "body"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_issue_write", "connector_use"),
        ),
        _github_tool(
            name="github_update_issue_comment",
            remote_tool_name="update_issue_comment",
            description="Update an existing GitHub issue or pull request conversation comment.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "comment_id": {"type": "integer", "minimum": 1},
                    "body": {"type": "string"},
                },
                "required": ["owner", "repo", "comment_id", "body"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_issue_write", "connector_use"),
        ),
        _github_tool(
            name="github_list_pull_requests",
            remote_tool_name="list_pull_requests",
            description="List pull requests for a repository, optionally filtered by state, base, or head branch.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "state": {"type": "string"},
                    "head": {"type": "string"},
                    "base": {"type": "string"},
                    "sort": {"type": "string"},
                    "direction": {"type": "string"},
                    "per_page": {"type": "integer", "minimum": 1, "maximum": 100},
                    "page": {"type": "integer", "minimum": 1},
                },
                "required": ["owner", "repo"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_pr_read", "connector_use"),
        ),
        _github_tool(
            name="github_get_pull_request",
            remote_tool_name="get_pull_request",
            description="Read a GitHub pull request by owner, repo, and pull request number.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "pull_number": {"type": "integer", "minimum": 1},
                },
                "required": ["owner", "repo", "pull_number"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_pr_read", "connector_use"),
        ),
        _github_tool(
            name="github_list_pull_reviews",
            remote_tool_name="list_pull_reviews",
            description="List submitted reviews for a GitHub pull request.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "pull_number": {"type": "integer", "minimum": 1},
                    "per_page": {"type": "integer", "minimum": 1, "maximum": 100},
                    "page": {"type": "integer", "minimum": 1},
                },
                "required": ["owner", "repo", "pull_number"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_pr_read", "connector_use"),
        ),
        _github_tool(
            name="github_list_pull_review_comments",
            remote_tool_name="list_pull_review_comments",
            description="List inline review comments for a GitHub pull request.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "pull_number": {"type": "integer", "minimum": 1},
                    "per_page": {"type": "integer", "minimum": 1, "maximum": 100},
                    "page": {"type": "integer", "minimum": 1},
                },
                "required": ["owner", "repo", "pull_number"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_pr_read", "connector_use"),
        ),
        _github_tool(
            name="github_create_pull_review",
            remote_tool_name="create_pull_review",
            description="Create a GitHub pull request review with COMMENT, APPROVE, or REQUEST_CHANGES.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "pull_number": {"type": "integer", "minimum": 1},
                    "body": {"type": "string"},
                    "event": {"type": "string"},
                    "commit_id": {"type": "string"},
                    "comments": {"type": "array", "items": {"type": "object"}},
                },
                "required": ["owner", "repo", "pull_number"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_pr_write", "connector_use"),
        ),
        _github_tool(
            name="github_list_pull_files",
            remote_tool_name="list_pull_files",
            description="List changed files in a GitHub pull request, including patch hunks when available.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "pull_number": {"type": "integer", "minimum": 1},
                    "per_page": {"type": "integer", "minimum": 1, "maximum": 100},
                    "page": {"type": "integer", "minimum": 1},
                },
                "required": ["owner", "repo", "pull_number"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_pr_read", "connector_use"),
        ),
        _github_tool(
            name="github_list_pull_commits",
            remote_tool_name="list_pull_commits",
            description="List commits included in a GitHub pull request.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "pull_number": {"type": "integer", "minimum": 1},
                    "per_page": {"type": "integer", "minimum": 1, "maximum": 100},
                    "page": {"type": "integer", "minimum": 1},
                },
                "required": ["owner", "repo", "pull_number"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_pr_read", "connector_use"),
        ),
        _github_tool(
            name="github_list_commit_comments",
            remote_tool_name="list_commit_comments",
            description="List commit comments for a specific commit SHA in a repository.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "commit_sha": {"type": "string"},
                    "per_page": {"type": "integer", "minimum": 1, "maximum": 100},
                    "page": {"type": "integer", "minimum": 1},
                },
                "required": ["owner", "repo", "commit_sha"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_pr_read", "connector_use"),
        ),
        _github_tool(
            name="github_get_pull_merge_readiness",
            remote_tool_name="get_pull_merge_readiness",
            description="Summarize pull request review state and merge readiness without merging.",
            parameters={
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                    "pull_number": {"type": "integer", "minimum": 1},
                },
                "required": ["owner", "repo", "pull_number"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("github_pr_read", "connector_use"),
        ),
    ]
