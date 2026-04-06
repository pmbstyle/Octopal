from __future__ import annotations

import httpx

from octopal.mcp_servers.github import (
    _normalize_commit,
    _normalize_commit_comment,
    _normalize_issue,
    _normalize_issue_comment,
    _normalize_pull_file,
    _normalize_pull_request,
    _normalize_pull_review,
    _normalize_pull_review_comment,
    _normalize_repo,
    _parse_github_api_error,
)


def test_normalize_repo_keeps_expected_fields() -> None:
    normalized = _normalize_repo(
        {
            "id": 1,
            "name": "demo",
            "full_name": "octo/demo",
            "private": False,
            "default_branch": "main",
            "html_url": "https://github.com/octo/demo",
            "owner": {
                "login": "octo",
                "id": 9,
                "type": "User",
                "site_admin": False,
                "html_url": "https://github.com/octo",
            },
        }
    )

    assert normalized["full_name"] == "octo/demo"
    assert normalized["owner"]["login"] == "octo"


def test_normalize_issue_keeps_labels_and_user() -> None:
    normalized = _normalize_issue(
        {
            "id": 2,
            "number": 17,
            "title": "Fix connector",
            "state": "open",
            "user": {"login": "alice", "id": 10, "type": "User", "site_admin": False},
            "labels": [{"name": "bug", "color": "d73a4a", "description": "Something is broken"}],
        }
    )

    assert normalized["number"] == 17
    assert normalized["user"]["login"] == "alice"
    assert normalized["labels"][0]["name"] == "bug"


def test_normalize_pull_request_keeps_base_and_head_refs() -> None:
    normalized = _normalize_pull_request(
        {
            "id": 3,
            "number": 8,
            "title": "Add connector",
            "state": "open",
            "mergeable_state": "clean",
            "requested_reviewers": [{"login": "carol", "id": 12, "type": "User", "site_admin": False}],
            "head": {"label": "octo:feature", "ref": "feature", "sha": "abc123"},
            "base": {"label": "octo:main", "ref": "main", "sha": "def456"},
            "user": {"login": "bob", "id": 11, "type": "User", "site_admin": False},
        }
    )

    assert normalized["number"] == 8
    assert normalized["head"]["ref"] == "feature"
    assert normalized["base"]["ref"] == "main"
    assert normalized["mergeable_state"] == "clean"
    assert normalized["requested_reviewers"][0]["login"] == "carol"


def test_normalize_issue_comment_keeps_body_and_author() -> None:
    normalized = _normalize_issue_comment(
        {
            "id": 4,
            "body": "Looks good",
            "user": {"login": "dana", "id": 13, "type": "User", "site_admin": False},
        }
    )

    assert normalized["id"] == 4
    assert normalized["body"] == "Looks good"
    assert normalized["user"]["login"] == "dana"


def test_normalize_pull_review_keeps_state_and_user() -> None:
    normalized = _normalize_pull_review(
        {
            "id": 5,
            "state": "APPROVED",
            "body": "Approved",
            "user": {"login": "erin", "id": 14, "type": "User", "site_admin": False},
        }
    )

    assert normalized["state"] == "APPROVED"
    assert normalized["user"]["login"] == "erin"


def test_normalize_pull_review_comment_keeps_path_and_body() -> None:
    normalized = _normalize_pull_review_comment(
        {
            "id": 6,
            "path": "src/app.py",
            "body": "Please rename this",
            "user": {"login": "frank", "id": 15, "type": "User", "site_admin": False},
        }
    )

    assert normalized["path"] == "src/app.py"
    assert normalized["body"] == "Please rename this"
    assert normalized["user"]["login"] == "frank"


def test_normalize_pull_file_keeps_patch_and_stats() -> None:
    normalized = _normalize_pull_file(
        {
            "sha": "abc123",
            "filename": "src/app.py",
            "status": "modified",
            "additions": 5,
            "deletions": 2,
            "changes": 7,
            "patch": "@@ -1,2 +1,5 @@",
        }
    )

    assert normalized["filename"] == "src/app.py"
    assert normalized["changes"] == 7
    assert normalized["patch"] == "@@ -1,2 +1,5 @@"


def test_normalize_commit_keeps_message_and_sha() -> None:
    normalized = _normalize_commit(
        {
            "sha": "def456",
            "html_url": "https://github.com/octo/demo/commit/def456",
            "commit": {
                "message": "Improve worker flow",
                "comment_count": 1,
                "author": {"name": "Alice", "email": "alice@example.com", "date": "2026-04-06T10:00:00Z"},
                "committer": {"name": "Alice", "email": "alice@example.com", "date": "2026-04-06T10:00:00Z"},
                "verification": {"verified": True, "reason": "valid", "verified_at": "2026-04-06T10:00:01Z"},
            },
        }
    )

    assert normalized["sha"] == "def456"
    assert normalized["message"] == "Improve worker flow"
    assert normalized["verification"]["verified"] is True


def test_normalize_commit_comment_keeps_path_and_body() -> None:
    normalized = _normalize_commit_comment(
        {
            "id": 7,
            "path": "src/app.py",
            "body": "Nit: rename this helper",
            "user": {"login": "gina", "id": 16, "type": "User", "site_admin": False},
        }
    )

    assert normalized["id"] == 7
    assert normalized["path"] == "src/app.py"
    assert normalized["user"]["login"] == "gina"


def test_parse_github_api_error_prefers_message_from_json_payload() -> None:
    response = httpx.Response(
        403,
        json={"message": "Resource not accessible by personal access token", "documentation_url": "https://docs.github.com"},
    )

    error = _parse_github_api_error(response)

    assert error.status_code == 403
    assert str(error) == "GitHub API 403: Resource not accessible by personal access token"
