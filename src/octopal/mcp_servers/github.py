from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import httpx
from mcp.server import FastMCP

_GITHUB_API_BASE_URL = "https://api.github.com"
_DEFAULT_HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}


class GitHubConfigError(RuntimeError):
    """Raised when required GitHub MCP configuration is missing."""


class GitHubApiError(RuntimeError):
    """Raised when GitHub returns a structured error response."""

    def __init__(
        self,
        *,
        status_code: int,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.status_code = status_code
        self.details = details or {}
        super().__init__(f"GitHub API {status_code}: {message}")


def _parse_github_api_error(response: httpx.Response) -> GitHubApiError:
    payload: dict[str, Any] = {}
    try:
        payload = response.json()
    except json.JSONDecodeError:
        text = response.text.strip() or response.reason_phrase
        return GitHubApiError(status_code=response.status_code, message=text, details={})

    message = str(payload.get("message") or response.reason_phrase or "Unknown GitHub API error").strip()
    return GitHubApiError(status_code=response.status_code, message=message, details=payload)


def _normalize_user(user: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(user, dict):
        return None
    return {
        "login": user.get("login"),
        "id": user.get("id"),
        "type": user.get("type"),
        "site_admin": user.get("site_admin", False),
        "html_url": user.get("html_url"),
    }


def _normalize_repo(repo: dict[str, Any]) -> dict[str, Any]:
    owner = _normalize_user(repo.get("owner"))
    return {
        "id": repo.get("id"),
        "name": repo.get("name"),
        "full_name": repo.get("full_name"),
        "private": repo.get("private", False),
        "default_branch": repo.get("default_branch"),
        "description": repo.get("description"),
        "html_url": repo.get("html_url"),
        "clone_url": repo.get("clone_url"),
        "ssh_url": repo.get("ssh_url"),
        "visibility": repo.get("visibility"),
        "language": repo.get("language"),
        "fork": repo.get("fork", False),
        "archived": repo.get("archived", False),
        "disabled": repo.get("disabled", False),
        "open_issues_count": repo.get("open_issues_count"),
        "stargazers_count": repo.get("stargazers_count"),
        "watchers_count": repo.get("watchers_count"),
        "forks_count": repo.get("forks_count"),
        "updated_at": repo.get("updated_at"),
        "pushed_at": repo.get("pushed_at"),
        "owner": owner,
    }


def _normalize_issue(issue: dict[str, Any]) -> dict[str, Any]:
    assignees = issue.get("assignees") or []
    labels = issue.get("labels") or []
    return {
        "id": issue.get("id"),
        "number": issue.get("number"),
        "title": issue.get("title"),
        "state": issue.get("state"),
        "state_reason": issue.get("state_reason"),
        "html_url": issue.get("html_url"),
        "comments": issue.get("comments"),
        "created_at": issue.get("created_at"),
        "updated_at": issue.get("updated_at"),
        "closed_at": issue.get("closed_at"),
        "body": issue.get("body"),
        "user": _normalize_user(issue.get("user")),
        "assignees": [_normalize_user(assignee) for assignee in assignees],
        "labels": [
            {
                "name": label.get("name"),
                "color": label.get("color"),
                "description": label.get("description"),
            }
            for label in labels
            if isinstance(label, dict)
        ],
        "pull_request": issue.get("pull_request"),
    }


def _normalize_issue_comment(comment: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": comment.get("id"),
        "node_id": comment.get("node_id"),
        "html_url": comment.get("html_url"),
        "body": comment.get("body"),
        "created_at": comment.get("created_at"),
        "updated_at": comment.get("updated_at"),
        "author_association": comment.get("author_association"),
        "user": _normalize_user(comment.get("user")),
    }


def _normalize_pull_request(pr: dict[str, Any]) -> dict[str, Any]:
    head = pr.get("head") or {}
    base = pr.get("base") or {}
    requested_reviewers = pr.get("requested_reviewers") or []
    requested_teams = pr.get("requested_teams") or []
    return {
        "id": pr.get("id"),
        "number": pr.get("number"),
        "title": pr.get("title"),
        "state": pr.get("state"),
        "draft": pr.get("draft", False),
        "merged": pr.get("merged"),
        "mergeable": pr.get("mergeable"),
        "mergeable_state": pr.get("mergeable_state"),
        "rebaseable": pr.get("rebaseable"),
        "html_url": pr.get("html_url"),
        "created_at": pr.get("created_at"),
        "updated_at": pr.get("updated_at"),
        "closed_at": pr.get("closed_at"),
        "merged_at": pr.get("merged_at"),
        "body": pr.get("body"),
        "comments": pr.get("comments"),
        "review_comments": pr.get("review_comments"),
        "commits": pr.get("commits"),
        "additions": pr.get("additions"),
        "deletions": pr.get("deletions"),
        "changed_files": pr.get("changed_files"),
        "user": _normalize_user(pr.get("user")),
        "requested_reviewers": [_normalize_user(user) for user in requested_reviewers],
        "requested_teams": [
            {
                "id": team.get("id"),
                "name": team.get("name"),
                "slug": team.get("slug"),
            }
            for team in requested_teams
            if isinstance(team, dict)
        ],
        "head": {
            "label": head.get("label"),
            "ref": head.get("ref"),
            "sha": head.get("sha"),
            "repo": _normalize_repo(head.get("repo")) if isinstance(head.get("repo"), dict) else None,
        },
        "base": {
            "label": base.get("label"),
            "ref": base.get("ref"),
            "sha": base.get("sha"),
            "repo": _normalize_repo(base.get("repo")) if isinstance(base.get("repo"), dict) else None,
        },
    }


def _normalize_pull_review(review: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": review.get("id"),
        "node_id": review.get("node_id"),
        "user": _normalize_user(review.get("user")),
        "body": review.get("body"),
        "state": review.get("state"),
        "html_url": review.get("html_url"),
        "submitted_at": review.get("submitted_at"),
        "commit_id": review.get("commit_id"),
        "author_association": review.get("author_association"),
    }


def _normalize_pull_review_comment(comment: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": comment.get("id"),
        "node_id": comment.get("node_id"),
        "path": comment.get("path"),
        "position": comment.get("position"),
        "line": comment.get("line"),
        "side": comment.get("side"),
        "commit_id": comment.get("commit_id"),
        "body": comment.get("body"),
        "html_url": comment.get("html_url"),
        "created_at": comment.get("created_at"),
        "updated_at": comment.get("updated_at"),
        "author_association": comment.get("author_association"),
        "user": _normalize_user(comment.get("user")),
    }


def _normalize_pull_file(file: dict[str, Any]) -> dict[str, Any]:
    return {
        "sha": file.get("sha"),
        "filename": file.get("filename"),
        "status": file.get("status"),
        "additions": file.get("additions"),
        "deletions": file.get("deletions"),
        "changes": file.get("changes"),
        "blob_url": file.get("blob_url"),
        "raw_url": file.get("raw_url"),
        "contents_url": file.get("contents_url"),
        "patch": file.get("patch"),
        "previous_filename": file.get("previous_filename"),
    }


def _normalize_commit(commit_payload: dict[str, Any]) -> dict[str, Any]:
    commit = commit_payload.get("commit") or {}
    author = commit.get("author") or {}
    committer = commit.get("committer") or {}
    verification = commit.get("verification") or {}
    parents = commit_payload.get("parents") or []
    return {
        "sha": commit_payload.get("sha"),
        "html_url": commit_payload.get("html_url"),
        "message": commit.get("message"),
        "author": {
            "name": author.get("name"),
            "email": author.get("email"),
            "date": author.get("date"),
            "user": _normalize_user(commit_payload.get("author")),
        },
        "committer": {
            "name": committer.get("name"),
            "email": committer.get("email"),
            "date": committer.get("date"),
            "user": _normalize_user(commit_payload.get("committer")),
        },
        "parents": [{"sha": parent.get("sha"), "url": parent.get("url")} for parent in parents if isinstance(parent, dict)],
        "comment_count": commit.get("comment_count"),
        "verification": {
            "verified": verification.get("verified"),
            "reason": verification.get("reason"),
            "verified_at": verification.get("verified_at"),
        },
    }


def _normalize_commit_comment(comment: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": comment.get("id"),
        "node_id": comment.get("node_id"),
        "path": comment.get("path"),
        "position": comment.get("position"),
        "line": comment.get("line"),
        "commit_id": comment.get("commit_id"),
        "body": comment.get("body"),
        "html_url": comment.get("html_url"),
        "created_at": comment.get("created_at"),
        "updated_at": comment.get("updated_at"),
        "author_association": comment.get("author_association"),
        "user": _normalize_user(comment.get("user")),
    }


class GitHubApiClient:
    def __init__(self) -> None:
        self._token = self._load_token()
        self._client = httpx.AsyncClient(base_url=_GITHUB_API_BASE_URL, timeout=30.0)

    def _load_token(self) -> str:
        token = os.getenv("GITHUB_TOKEN", "").strip()
        if not token:
            raise GitHubConfigError("Missing GitHub MCP credentials. Expected GITHUB_TOKEN.")
        return token

    async def close(self) -> None:
        await self._client.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        response = await self._client.request(
            method,
            path,
            params=params,
            json=json_body,
            headers={
                **_DEFAULT_HEADERS,
                "Authorization": f"Bearer {self._token}",
            },
        )
        if response.is_error:
            raise _parse_github_api_error(response)
        if not response.content:
            return {}
        return response.json()

    async def get_authenticated_user(self) -> dict[str, Any]:
        payload = await self._request("GET", "/user")
        return {
            "login": payload.get("login"),
            "id": payload.get("id"),
            "name": payload.get("name"),
            "email": payload.get("email"),
            "company": payload.get("company"),
            "html_url": payload.get("html_url"),
            "avatar_url": payload.get("avatar_url"),
        }

    async def list_repositories(
        self,
        *,
        visibility: str | None = None,
        affiliation: str | None = None,
        sort: str = "updated",
        direction: str = "desc",
        per_page: int = 20,
        page: int = 1,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "sort": sort,
            "direction": direction,
            "per_page": max(1, min(per_page, 100)),
            "page": max(1, page),
        }
        if visibility:
            params["visibility"] = visibility
        if affiliation:
            params["affiliation"] = affiliation
        payload = await self._request("GET", "/user/repos", params=params)
        return {"repositories": [_normalize_repo(repo) for repo in payload]}

    async def get_repository(self, *, owner: str, repo: str) -> dict[str, Any]:
        payload = await self._request("GET", f"/repos/{owner}/{repo}")
        return _normalize_repo(payload)

    async def list_issues(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "open",
        labels: str | None = None,
        since: str | None = None,
        per_page: int = 20,
        page: int = 1,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "state": state,
            "per_page": max(1, min(per_page, 100)),
            "page": max(1, page),
        }
        if labels:
            params["labels"] = labels
        if since:
            params["since"] = since
        payload = await self._request("GET", f"/repos/{owner}/{repo}/issues", params=params)
        return {
            "owner": owner,
            "repo": repo,
            "issues": [_normalize_issue(issue) for issue in payload],
        }

    async def get_issue(self, *, owner: str, repo: str, issue_number: int) -> dict[str, Any]:
        payload = await self._request("GET", f"/repos/{owner}/{repo}/issues/{issue_number}")
        return _normalize_issue(payload)

    async def create_issue(
        self,
        *,
        owner: str,
        repo: str,
        title: str,
        body: str | None = None,
        assignees: list[str] | None = None,
        labels: list[str] | None = None,
        milestone: int | None = None,
    ) -> dict[str, Any]:
        request_body: dict[str, Any] = {"title": title}
        if body is not None:
            request_body["body"] = body
        if assignees is not None:
            request_body["assignees"] = assignees
        if labels is not None:
            request_body["labels"] = labels
        if milestone is not None:
            request_body["milestone"] = milestone
        payload = await self._request("POST", f"/repos/{owner}/{repo}/issues", json_body=request_body)
        return _normalize_issue(payload)

    async def update_issue(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        title: str | None = None,
        body: str | None = None,
        assignees: list[str] | None = None,
        labels: list[str] | None = None,
        milestone: int | None = None,
        state: str | None = None,
        state_reason: str | None = None,
    ) -> dict[str, Any]:
        request_body: dict[str, Any] = {}
        if title is not None:
            request_body["title"] = title
        if body is not None:
            request_body["body"] = body
        if assignees is not None:
            request_body["assignees"] = assignees
        if labels is not None:
            request_body["labels"] = labels
        if milestone is not None:
            request_body["milestone"] = milestone
        if state is not None:
            request_body["state"] = state
        if state_reason is not None:
            request_body["state_reason"] = state_reason
        if not request_body:
            raise ValueError("At least one mutable issue field must be provided for update.")
        payload = await self._request(
            "PATCH",
            f"/repos/{owner}/{repo}/issues/{issue_number}",
            json_body=request_body,
        )
        return _normalize_issue(payload)

    async def list_issue_comments(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        per_page: int = 30,
        page: int = 1,
    ) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            f"/repos/{owner}/{repo}/issues/{issue_number}/comments",
            params={"per_page": max(1, min(per_page, 100)), "page": max(1, page)},
        )
        return {
            "owner": owner,
            "repo": repo,
            "issue_number": issue_number,
            "comments": [_normalize_issue_comment(comment) for comment in payload],
        }

    async def create_issue_comment(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        body: str,
    ) -> dict[str, Any]:
        payload = await self._request(
            "POST",
            f"/repos/{owner}/{repo}/issues/{issue_number}/comments",
            json_body={"body": body},
        )
        return _normalize_issue_comment(payload)

    async def update_issue_comment(
        self,
        *,
        owner: str,
        repo: str,
        comment_id: int,
        body: str,
    ) -> dict[str, Any]:
        payload = await self._request(
            "PATCH",
            f"/repos/{owner}/{repo}/issues/comments/{comment_id}",
            json_body={"body": body},
        )
        return _normalize_issue_comment(payload)

    async def list_pull_requests(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "open",
        head: str | None = None,
        base: str | None = None,
        sort: str = "updated",
        direction: str = "desc",
        per_page: int = 20,
        page: int = 1,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "state": state,
            "sort": sort,
            "direction": direction,
            "per_page": max(1, min(per_page, 100)),
            "page": max(1, page),
        }
        if head:
            params["head"] = head
        if base:
            params["base"] = base
        payload = await self._request("GET", f"/repos/{owner}/{repo}/pulls", params=params)
        return {
            "owner": owner,
            "repo": repo,
            "pull_requests": [_normalize_pull_request(pr) for pr in payload],
        }

    async def get_pull_request(self, *, owner: str, repo: str, pull_number: int) -> dict[str, Any]:
        payload = await self._request("GET", f"/repos/{owner}/{repo}/pulls/{pull_number}")
        return _normalize_pull_request(payload)

    async def list_pull_reviews(
        self,
        *,
        owner: str,
        repo: str,
        pull_number: int,
        per_page: int = 30,
        page: int = 1,
    ) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            f"/repos/{owner}/{repo}/pulls/{pull_number}/reviews",
            params={"per_page": max(1, min(per_page, 100)), "page": max(1, page)},
        )
        return {
            "owner": owner,
            "repo": repo,
            "pull_number": pull_number,
            "reviews": [_normalize_pull_review(review) for review in payload],
        }

    async def list_pull_review_comments(
        self,
        *,
        owner: str,
        repo: str,
        pull_number: int,
        per_page: int = 30,
        page: int = 1,
    ) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            f"/repos/{owner}/{repo}/pulls/{pull_number}/comments",
            params={"per_page": max(1, min(per_page, 100)), "page": max(1, page)},
        )
        return {
            "owner": owner,
            "repo": repo,
            "pull_number": pull_number,
            "comments": [_normalize_pull_review_comment(comment) for comment in payload],
        }

    async def create_pull_review(
        self,
        *,
        owner: str,
        repo: str,
        pull_number: int,
        body: str | None = None,
        event: str | None = None,
        commit_id: str | None = None,
        comments: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        request_body: dict[str, Any] = {}
        if body is not None:
            request_body["body"] = body
        if event is not None:
            request_body["event"] = event
        if commit_id is not None:
            request_body["commit_id"] = commit_id
        if comments is not None:
            request_body["comments"] = comments
        payload = await self._request(
            "POST",
            f"/repos/{owner}/{repo}/pulls/{pull_number}/reviews",
            json_body=request_body,
        )
        return _normalize_pull_review(payload)

    async def list_pull_files(
        self,
        *,
        owner: str,
        repo: str,
        pull_number: int,
        per_page: int = 30,
        page: int = 1,
    ) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            f"/repos/{owner}/{repo}/pulls/{pull_number}/files",
            params={"per_page": max(1, min(per_page, 100)), "page": max(1, page)},
        )
        return {
            "owner": owner,
            "repo": repo,
            "pull_number": pull_number,
            "files": [_normalize_pull_file(item) for item in payload],
        }

    async def list_pull_commits(
        self,
        *,
        owner: str,
        repo: str,
        pull_number: int,
        per_page: int = 30,
        page: int = 1,
    ) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            f"/repos/{owner}/{repo}/pulls/{pull_number}/commits",
            params={"per_page": max(1, min(per_page, 100)), "page": max(1, page)},
        )
        return {
            "owner": owner,
            "repo": repo,
            "pull_number": pull_number,
            "commits": [_normalize_commit(item) for item in payload],
        }

    async def list_commit_comments(
        self,
        *,
        owner: str,
        repo: str,
        commit_sha: str,
        per_page: int = 30,
        page: int = 1,
    ) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            f"/repos/{owner}/{repo}/commits/{commit_sha}/comments",
            params={"per_page": max(1, min(per_page, 100)), "page": max(1, page)},
        )
        return {
            "owner": owner,
            "repo": repo,
            "commit_sha": commit_sha,
            "comments": [_normalize_commit_comment(item) for item in payload],
        }

    async def get_pull_merge_readiness(
        self,
        *,
        owner: str,
        repo: str,
        pull_number: int,
    ) -> dict[str, Any]:
        pr = await self.get_pull_request(owner=owner, repo=repo, pull_number=pull_number)
        reviews_payload = await self.list_pull_reviews(owner=owner, repo=repo, pull_number=pull_number)
        reviews = reviews_payload.get("reviews") or []

        latest_by_user: dict[str, dict[str, Any]] = {}
        for review in reviews:
            user = review.get("user") or {}
            login = str(user.get("login") or "").strip().lower()
            if login:
                latest_by_user[login] = review

        state_counts: dict[str, int] = {}
        for review in reviews:
            state = str(review.get("state") or "UNKNOWN").upper()
            state_counts[state] = state_counts.get(state, 0) + 1

        blocking_reviews = [
            review for review in latest_by_user.values() if str(review.get("state") or "").upper() == "CHANGES_REQUESTED"
        ]
        approvals = [
            review for review in latest_by_user.values() if str(review.get("state") or "").upper() == "APPROVED"
        ]

        return {
            "owner": owner,
            "repo": repo,
            "pull_number": pull_number,
            "pull_request": pr,
            "review_summary": {
                "total_reviews": len(reviews),
                "latest_reviews_by_user": list(latest_by_user.values()),
                "state_counts": state_counts,
                "approvals": len(approvals),
                "changes_requested": len(blocking_reviews),
                "comment_only_reviews": state_counts.get("COMMENTED", 0),
            },
            "merge_readiness": {
                "draft": pr.get("draft", False),
                "mergeable": pr.get("mergeable"),
                "mergeable_state": pr.get("mergeable_state"),
                "rebaseable": pr.get("rebaseable"),
                "requested_reviewers": pr.get("requested_reviewers", []),
                "requested_teams": pr.get("requested_teams", []),
                "blocking_reviews": blocking_reviews,
            },
        }


mcp = FastMCP(
    name="Octopal GitHub",
    instructions=(
        "Use these tools to inspect the connected GitHub account, repositories, issues, and pull requests. "
        "Prefer list tools to discover repositories or PR numbers before fetching a single item."
    ),
    log_level="ERROR",
)

_github_client: GitHubApiClient | None = None


def _client() -> GitHubApiClient:
    global _github_client
    if _github_client is None:
        _github_client = GitHubApiClient()
    return _github_client


@mcp.tool(name="get_authenticated_user")
async def get_authenticated_user() -> dict[str, Any]:
    """Return basic information about the connected GitHub user."""
    return await _client().get_authenticated_user()


@mcp.tool(name="list_repositories")
async def list_repositories(
    visibility: str | None = None,
    affiliation: str | None = None,
    sort: str = "updated",
    direction: str = "desc",
    per_page: int = 20,
    page: int = 1,
) -> dict[str, Any]:
    """List repositories visible to the authenticated GitHub user."""
    return await _client().list_repositories(
        visibility=visibility,
        affiliation=affiliation,
        sort=sort,
        direction=direction,
        per_page=per_page,
        page=page,
    )


@mcp.tool(name="get_repository")
async def get_repository(owner: str, repo: str) -> dict[str, Any]:
    """Return metadata for a specific GitHub repository."""
    return await _client().get_repository(owner=owner, repo=repo)


@mcp.tool(name="list_issues")
async def list_issues(
    owner: str,
    repo: str,
    state: str = "open",
    labels: str | None = None,
    since: str | None = None,
    per_page: int = 20,
    page: int = 1,
) -> dict[str, Any]:
    """List issues for a repository."""
    return await _client().list_issues(
        owner=owner,
        repo=repo,
        state=state,
        labels=labels,
        since=since,
        per_page=per_page,
        page=page,
    )


@mcp.tool(name="get_issue")
async def get_issue(owner: str, repo: str, issue_number: int) -> dict[str, Any]:
    """Return a single issue by number."""
    return await _client().get_issue(owner=owner, repo=repo, issue_number=issue_number)


@mcp.tool(name="create_issue")
async def create_issue(
    owner: str,
    repo: str,
    title: str,
    body: str | None = None,
    assignees: list[str] | None = None,
    labels: list[str] | None = None,
    milestone: int | None = None,
) -> dict[str, Any]:
    """Create a GitHub issue."""
    return await _client().create_issue(
        owner=owner,
        repo=repo,
        title=title,
        body=body,
        assignees=assignees,
        labels=labels,
        milestone=milestone,
    )


@mcp.tool(name="update_issue")
async def update_issue(
    owner: str,
    repo: str,
    issue_number: int,
    title: str | None = None,
    body: str | None = None,
    assignees: list[str] | None = None,
    labels: list[str] | None = None,
    milestone: int | None = None,
    state: str | None = None,
    state_reason: str | None = None,
) -> dict[str, Any]:
    """Update mutable fields on a GitHub issue."""
    return await _client().update_issue(
        owner=owner,
        repo=repo,
        issue_number=issue_number,
        title=title,
        body=body,
        assignees=assignees,
        labels=labels,
        milestone=milestone,
        state=state,
        state_reason=state_reason,
    )


@mcp.tool(name="list_issue_comments")
async def list_issue_comments(
    owner: str,
    repo: str,
    issue_number: int,
    per_page: int = 30,
    page: int = 1,
) -> dict[str, Any]:
    """List issue comments for an issue or pull request conversation."""
    return await _client().list_issue_comments(
        owner=owner,
        repo=repo,
        issue_number=issue_number,
        per_page=per_page,
        page=page,
    )


@mcp.tool(name="create_issue_comment")
async def create_issue_comment(owner: str, repo: str, issue_number: int, body: str) -> dict[str, Any]:
    """Create an issue comment. This also works for pull request conversation comments."""
    return await _client().create_issue_comment(owner=owner, repo=repo, issue_number=issue_number, body=body)


@mcp.tool(name="update_issue_comment")
async def update_issue_comment(owner: str, repo: str, comment_id: int, body: str) -> dict[str, Any]:
    """Update an existing issue comment."""
    return await _client().update_issue_comment(owner=owner, repo=repo, comment_id=comment_id, body=body)


@mcp.tool(name="list_pull_requests")
async def list_pull_requests(
    owner: str,
    repo: str,
    state: str = "open",
    head: str | None = None,
    base: str | None = None,
    sort: str = "updated",
    direction: str = "desc",
    per_page: int = 20,
    page: int = 1,
) -> dict[str, Any]:
    """List pull requests for a repository."""
    return await _client().list_pull_requests(
        owner=owner,
        repo=repo,
        state=state,
        head=head,
        base=base,
        sort=sort,
        direction=direction,
        per_page=per_page,
        page=page,
    )


@mcp.tool(name="get_pull_request")
async def get_pull_request(owner: str, repo: str, pull_number: int) -> dict[str, Any]:
    """Return a single pull request by number."""
    return await _client().get_pull_request(owner=owner, repo=repo, pull_number=pull_number)


@mcp.tool(name="list_pull_reviews")
async def list_pull_reviews(
    owner: str,
    repo: str,
    pull_number: int,
    per_page: int = 30,
    page: int = 1,
) -> dict[str, Any]:
    """List review submissions for a pull request."""
    return await _client().list_pull_reviews(
        owner=owner,
        repo=repo,
        pull_number=pull_number,
        per_page=per_page,
        page=page,
    )


@mcp.tool(name="list_pull_review_comments")
async def list_pull_review_comments(
    owner: str,
    repo: str,
    pull_number: int,
    per_page: int = 30,
    page: int = 1,
) -> dict[str, Any]:
    """List inline review comments for a pull request."""
    return await _client().list_pull_review_comments(
        owner=owner,
        repo=repo,
        pull_number=pull_number,
        per_page=per_page,
        page=page,
    )


@mcp.tool(name="create_pull_review")
async def create_pull_review(
    owner: str,
    repo: str,
    pull_number: int,
    body: str | None = None,
    event: str | None = None,
    commit_id: str | None = None,
    comments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Create a pull request review, optionally with APPROVE, REQUEST_CHANGES, or COMMENT event."""
    return await _client().create_pull_review(
        owner=owner,
        repo=repo,
        pull_number=pull_number,
        body=body,
        event=event,
        commit_id=commit_id,
        comments=comments,
    )


@mcp.tool(name="list_pull_files")
async def list_pull_files(
    owner: str,
    repo: str,
    pull_number: int,
    per_page: int = 30,
    page: int = 1,
) -> dict[str, Any]:
    """List changed files in a pull request, including patch hunks when GitHub provides them."""
    return await _client().list_pull_files(
        owner=owner,
        repo=repo,
        pull_number=pull_number,
        per_page=per_page,
        page=page,
    )


@mcp.tool(name="list_pull_commits")
async def list_pull_commits(
    owner: str,
    repo: str,
    pull_number: int,
    per_page: int = 30,
    page: int = 1,
) -> dict[str, Any]:
    """List commits included in a pull request."""
    return await _client().list_pull_commits(
        owner=owner,
        repo=repo,
        pull_number=pull_number,
        per_page=per_page,
        page=page,
    )


@mcp.tool(name="list_commit_comments")
async def list_commit_comments(
    owner: str,
    repo: str,
    commit_sha: str,
    per_page: int = 30,
    page: int = 1,
) -> dict[str, Any]:
    """List commit comments for a specific commit SHA."""
    return await _client().list_commit_comments(
        owner=owner,
        repo=repo,
        commit_sha=commit_sha,
        per_page=per_page,
        page=page,
    )


@mcp.tool(name="get_pull_merge_readiness")
async def get_pull_merge_readiness(owner: str, repo: str, pull_number: int) -> dict[str, Any]:
    """Summarize pull request review state and merge readiness without merging."""
    return await _client().get_pull_merge_readiness(owner=owner, repo=repo, pull_number=pull_number)


def main() -> None:
    try:
        mcp.run()
    finally:
        try:
            if _github_client is not None:
                asyncio.run(_github_client.close())
        except Exception:
            pass


if __name__ == "__main__":
    main()
