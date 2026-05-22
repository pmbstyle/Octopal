from __future__ import annotations

import json

from typer.testing import CliRunner

from octopal.cli.main import (
    _build_tool_resolution_snapshot,
    app,
)
from octopal.tools.catalog import get_tools

runner = CliRunner()


def test_build_tool_resolution_snapshot_for_octo_applies_policy_and_profile() -> None:
    snapshot = _build_tool_resolution_snapshot(
        get_tools(mcp_manager=None),
        preset="octo",
        profile_name="research",
        include_blocked=True,
    )

    available_names = {row["name"] for row in snapshot["available"]}
    blocked_rows = {row["name"]: row for row in snapshot["blocked"]}

    assert "web_search" in available_names
    assert "fs_read" not in available_names
    assert "fs_read" in blocked_rows
    assert blocked_rows["fs_read"]["reason"] == "blocked_by_allowlist:profile.research"
    assert "web_fetch" in blocked_rows
    assert blocked_rows["web_fetch"]["reason"] == "blocked_by_deny:octo.raw_fetch_denylist"
    assert "exec_run" in blocked_rows
    assert "test_run" in blocked_rows


def test_build_tool_resolution_snapshot_for_octo_blocks_direct_exec_without_profile() -> None:
    snapshot = _build_tool_resolution_snapshot(
        get_tools(mcp_manager=None),
        preset="octo",
        profile_name=None,
        include_blocked=True,
    )

    available_names = {row["name"] for row in snapshot["available"]}
    blocked_rows = {row["name"]: row for row in snapshot["blocked"]}

    assert "exec_run" in available_names
    assert "test_run" not in available_names
    assert blocked_rows["test_run"]["reason"] == "blocked_by_deny:octo.direct_exec_denylist"


def test_tools_resolve_json_outputs_snapshot() -> None:
    result = runner.invoke(
        app, ["tools", "resolve", "--preset", "octo", "--profile", "research", "--json"]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["preset"] == "octo"
    assert payload["profile"] == "research"
    assert payload["available_count"] > 0
    assert any(row["name"] == "web_search" for row in payload["available"])


def test_build_tool_resolution_snapshot_rejects_unknown_preset() -> None:
    try:
        _build_tool_resolution_snapshot(
            get_tools(mcp_manager=None),
            preset="mystery",
            profile_name=None,
            include_blocked=False,
        )
    except Exception as exc:
        assert "Unsupported tools preset" in str(exc)
    else:
        raise AssertionError("Expected unsupported preset to raise")
