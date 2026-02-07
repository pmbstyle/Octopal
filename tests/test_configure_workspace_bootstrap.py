from __future__ import annotations

from pathlib import Path

from broodmind.cli.configure import _ensure_workspace_bootstrap


def test_workspace_bootstrap_creates_required_markdown(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    result = _ensure_workspace_bootstrap(workspace)

    assert (workspace / "AGENTS.md").exists()
    assert (workspace / "USER.md").exists()
    assert (workspace / "SOUL.md").exists()
    assert (workspace / "HEARTBEAT.md").exists()
    assert (workspace / "MEMORY.md").exists()
    assert (workspace / "memory" / "canon" / "facts.md").exists()
    assert (workspace / "memory" / "canon" / "decisions.md").exists()
    assert (workspace / "memory" / "canon" / "failures.md").exists()
    assert (workspace / "workers" / "web_fetcher" / "worker.json").exists()
    assert (workspace / "workers" / "web_researcher" / "worker.json").exists()

    assert (workspace / "USER.md").read_text(encoding="utf-8").strip() == ""
    assert (workspace / "SOUL.md").read_text(encoding="utf-8").strip() == ""
    assert (workspace / "HEARTBEAT.md").read_text(encoding="utf-8").strip() == ""
    agents_content = (workspace / "AGENTS.md").read_text(encoding="utf-8")
    assert "Core Roles" in agents_content
    assert "Safety Rules" in agents_content
    assert "HEARTBEAT_OK" in agents_content
    assert len(result["created_files"]) > 8
    assert any(str(path).startswith("workers/") for path in result["created_files"])


def test_workspace_bootstrap_is_non_destructive(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    user_file = workspace / "USER.md"
    user_file.write_text("existing", encoding="utf-8")

    result = _ensure_workspace_bootstrap(workspace)

    assert user_file.read_text(encoding="utf-8") == "existing"
    assert int(result["skipped_files"]) >= 1
