from __future__ import annotations

import asyncio
from pathlib import Path

from broodmind.cli.configure import _ensure_workspace_bootstrap
from broodmind.runtime.queen.prompt_builder import build_bootstrap_context_prompt


def test_workspace_bootstrap_creates_required_markdown(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    _ensure_workspace_bootstrap(workspace)

    assert (workspace / "AGENTS.md").exists()
    assert (workspace / "USER.md").exists()
    assert (workspace / "SOUL.md").exists()
    assert (workspace / "HEARTBEAT.md").exists()
    assert (workspace / "MEMORY.md").exists()
    assert (workspace / "memory" / "canon" / "facts.md").exists()
    assert (workspace / "memory" / "canon" / "decisions.md").exists()
    assert (workspace / "memory" / "canon" / "failures.md").exists()
    assert (workspace / "experiments" / "README.md").exists()
    assert (workspace / "experiments" / "results.jsonl").exists()
    assert (workspace / "workers" / "web_fetcher" / "worker.json").exists()
    assert (workspace / "workers" / "web_researcher" / "worker.json").exists()

    assert "## User" in (workspace / "USER.md").read_text(encoding="utf-8")
    assert "## Persona" in (workspace / "SOUL.md").read_text(encoding="utf-8")
    assert "# HEARTBEAT" in (workspace / "HEARTBEAT.md").read_text(encoding="utf-8")
    agents_content = (workspace / "AGENTS.md").read_text(encoding="utf-8")
    assert "Core Roles" in agents_content
    assert "Safety Rules" in agents_content
    assert "HEARTBEAT_OK" in agents_content
    assert "Controlled Self-Improvement" in agents_content
    assert "Workers are the default path for external work." in agents_content
    assert "do not reduce `timeout_seconds` below the worker template default" in agents_content
    assert "prefer a worker that can spawn child workers or launch a bounded parallel batch" in agents_content


def test_bootstrap_context_includes_experiments_readme(tmp_path: Path, monkeypatch) -> None:
    workspace = tmp_path / "workspace"
    _ensure_workspace_bootstrap(workspace)
    monkeypatch.setenv("BROODMIND_WORKSPACE_DIR", str(workspace))

    context = asyncio.run(build_bootstrap_context_prompt(store=None, chat_id=123))

    assert 'file name="experiments/README.md"' in context.content
    assert "one active experiment at a time" in context.content


def test_workspace_bootstrap_is_non_destructive(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    user_file = workspace / "USER.md"
    user_file.write_text("existing", encoding="utf-8")

    _ensure_workspace_bootstrap(workspace)

    assert user_file.read_text(encoding="utf-8") == "existing"
