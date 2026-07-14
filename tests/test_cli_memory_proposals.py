from __future__ import annotations

import asyncio
import json
from pathlib import Path

from typer.testing import CliRunner

from octopal.cli import memory_proposals as proposals_cli
from octopal.cli.main import app
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.canon import CanonService
from octopal.runtime.memory.facts import FactsService

runner = CliRunner()


class _Settings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir
        self.memory_owner_id = "default"


def _create_worker_proposal(settings: _Settings) -> str:
    store = SQLiteStore(settings)
    facts = FactsService(store=store, owner_id=settings.memory_owner_id)
    canon = CanonService(
        workspace_dir=settings.workspace_dir,
        store=store,
        embeddings=None,
        facts=facts,
    )

    async def scenario() -> str:
        result = await canon.write_canon(
            "facts",
            "Deployment target is production.\n",
            source_kind="worker",
            source_ref="worker-run-cli",
        )
        return result.removeprefix("Quarantined canon proposal: ")

    return asyncio.run(scenario())


def test_memory_proposals_cli_is_metadata_only_until_show_and_supports_rollback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _Settings(tmp_path / "data", tmp_path / "workspace")
    proposal_id = _create_worker_proposal(settings)
    monkeypatch.setattr(proposals_cli, "load_settings", lambda: settings)

    list_result = runner.invoke(app, ["memory", "proposals", "list", "--json"])
    assert list_result.exit_code == 0
    list_payload = json.loads(list_result.stdout)
    proposal = next(item for item in list_payload["proposals"] if item["id"] == proposal_id)
    assert proposal["source_kind"] == "worker"
    assert proposal["trust_state"] == "quarantined_candidate"
    assert proposal["content_chars"] > 0
    assert "content" not in proposal
    assert "source_ref" not in proposal
    assert proposal["source_ref_present"] is True
    assert len(proposal["source_ref_sha256"]) == 64

    show_result = runner.invoke(
        app,
        ["memory", "proposals", "show", proposal_id, "--json"],
    )
    assert show_result.exit_code == 0
    assert json.loads(show_result.stdout)["proposal"]["content"] == (
        "Deployment target is production.\n"
    )

    confirmation = runner.invoke(
        app,
        ["memory", "proposals", "promote", proposal_id, "--json"],
    )
    assert confirmation.exit_code == 1
    assert json.loads(confirmation.stdout)["error"]["code"] == "confirmation_required"

    promoted = runner.invoke(
        app,
        ["memory", "proposals", "promote", proposal_id, "--yes", "--json"],
    )
    assert promoted.exit_code == 0
    assert json.loads(promoted.stdout) == {
        "filename": "facts.md",
        "proposal_id": proposal_id,
        "rollback_applied": False,
        "trust_state": "trusted",
    }
    assert (
        "Deployment target is production."
        in (settings.workspace_dir / "memory" / "canon" / "facts.md").read_text()
    )

    deprecated = runner.invoke(
        app,
        ["memory", "proposals", "deprecate", proposal_id, "--yes", "--json"],
    )
    assert deprecated.exit_code == 0
    assert json.loads(deprecated.stdout)["rollback_applied"] is True
    assert (
        "Deployment target is production."
        not in (settings.workspace_dir / "memory" / "canon" / "facts.md").read_text()
    )


def test_memory_proposals_cli_rejects_unknown_state(tmp_path: Path, monkeypatch) -> None:
    settings = _Settings(tmp_path / "data", tmp_path / "workspace")
    monkeypatch.setattr(proposals_cli, "load_settings", lambda: settings)

    result = runner.invoke(
        app,
        ["memory", "proposals", "list", "--state", "active", "--json"],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"]["code"] == "invalid_trust_state"


def test_memory_proposals_cli_returns_structured_error_for_invalid_id(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _Settings(tmp_path / "data", tmp_path / "workspace")
    monkeypatch.setattr(proposals_cli, "load_settings", lambda: settings)

    result = runner.invoke(
        app,
        ["memory", "proposals", "show", "../facts.md", "--json"],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"]["code"] == "proposal_not_found"
