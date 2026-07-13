from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from octopal.infrastructure.store.models import ExecutionEpisodeRecord
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.episodes import build_worker_execution_episode
from octopal.runtime.workers.contracts import WorkerResult, WorkerSpec


class _StoreSettings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


def _store(tmp_path: Path) -> SQLiteStore:
    return SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))


def _episode(**updates: object) -> ExecutionEpisodeRecord:
    values = {
        "id": "episode-1",
        "worker_run_id": "worker-1",
        "task_fingerprint": "task-hash",
        "environment_fingerprint": "environment-hash",
        "capability_fingerprint": "capability-hash",
        "result_fingerprint": "result-hash",
        "status": "completed",
        "source_kind": "worker",
        "trust_state": "observed",
        "trajectory_refs": {"worker_record_id": "worker-1"},
        "result_metadata": {"output_keys": ["report"]},
        "verification": {"result_contract_validated": True},
        "provenance": {"content_policy": "metadata_only_v1"},
        "created_at": datetime.now(UTC),
    }
    values.update(updates)
    return ExecutionEpisodeRecord.model_validate(values)


def _spec(secret: str) -> WorkerSpec:
    return WorkerSpec(
        id="worker-1",
        template_id="researcher",
        task=f"Inspect account using {secret}",
        inputs={"api_key": secret},
        system_prompt=f"Never reveal {secret}",
        available_tools=["web_search"],
        mcp_tools=[],
        model="test-model",
        granted_capabilities=[{"type": "network", "token": secret}],
        timeout_seconds=10,
        max_thinking_steps=4,
        run_id="worker-1",
        correlation_id="turn-1",
        effective_permissions=["network"],
    )


def test_sqlite_execution_episodes_are_append_only(tmp_path: Path) -> None:
    store = _store(tmp_path)
    episode = _episode()

    store.add_execution_episode(episode)

    assert store.get_execution_episode(episode.id) == episode
    assert store.list_execution_episodes(worker_run_id="worker-1") == [episode]
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed"):
        store.add_execution_episode(episode)
    with pytest.raises(sqlite3.IntegrityError, match="execution episodes are immutable"):
        store._conn.execute(  # noqa: SLF001 - verifies the database invariant
            "UPDATE execution_episodes SET trust_state = 'trusted' WHERE id = ?",
            (episode.id,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="execution episodes are immutable"):
        store._conn.execute(  # noqa: SLF001 - verifies the database invariant
            "DELETE FROM execution_episodes WHERE id = ?", (episode.id,)
        )


def test_external_episode_cannot_be_created_as_trusted() -> None:
    with pytest.raises(ValidationError, match="cannot directly create a trusted episode"):
        _episode(source_kind="web", trust_state="trusted")

    trusted_runtime_episode = _episode(
        id="runtime-episode",
        source_kind="local_runtime_evidence",
        trust_state="trusted",
    )
    assert trusted_runtime_episode.trust_state == "trusted"


def test_worker_episode_contains_fingerprints_not_secret_values() -> None:
    secret = "sk-live-do-not-copy"
    result = WorkerResult(
        summary=f"Fetched data with {secret}",
        output={
            "report": {"credential": secret},
            secret: "secret used as an output key",
            "verification": {secret: True, "passed": True},
            "_telemetry": {"prompt": secret},
        },
        tools_used=["web_search", secret],
        thinking_steps=2,
    )

    episode = build_worker_execution_episode(
        spec=_spec(secret),
        result=result,
        stored_output=result.output,
        status="completed",
        launcher_kind="TestLauncher",
    )

    serialized = episode.model_dump_json()
    assert secret not in serialized
    assert episode.source_kind == "worker"
    assert episode.trust_state == "observed"
    assert episode.result_metadata["output_key_count"] == 4
    assert len(episode.result_metadata["output_keys_fingerprint"]) == 64
    assert episode.result_metadata["tools_used_count"] == 2
    assert episode.verification["explicit_verification_key_count"] == 2
    assert len(episode.verification["explicit_verification_keys_fingerprint"]) == 64
    assert episode.provenance["content_policy"] == "metadata_only_v1"


def test_internal_output_does_not_count_as_structured_domain_output() -> None:
    result = WorkerResult(
        summary="Coordinator stopped without a domain result",
        output={"_telemetry": {}, "_orchestration_plan": {"status": "pending"}},
    )

    episode = build_worker_execution_episode(
        spec=_spec("not-secret"),
        result=result,
        stored_output=result.output,
        status="completed",
        launcher_kind="TestLauncher",
    )

    assert episode.verification["structured_output_present"] is False
    assert episode.verification["domain_output_key_count"] == 0


def test_stopped_episode_preserves_terminal_status() -> None:
    result = WorkerResult(status="failed", summary="Worker stopped", output={"stopped": True})

    episode = build_worker_execution_episode(
        spec=_spec("not-secret"),
        result=result,
        stored_output=result.output,
        status="stopped",
        launcher_kind="TestLauncher",
    )

    assert episode.status == "stopped"
    assert episode.verification["terminal_status"] == "stopped"
