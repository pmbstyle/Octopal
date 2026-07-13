from __future__ import annotations

import asyncio
import base64
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.exceptions import InvalidTag
from pydantic import ValidationError

from octopal.infrastructure.config.models import LLMConfig
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.store.models import ExecutionEpisodeRecord
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.episode_evidence import (
    EpisodeEvidenceCipher,
    build_encrypted_worker_episode_evidence,
)
from octopal.runtime.memory.episodes import build_worker_execution_episode
from octopal.runtime.workers.contracts import WorkerResult, WorkerSpec
from octopal.runtime.workers.runtime import WorkerRuntime


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


def _encoded_key(byte: bytes = b"k") -> str:
    return base64.urlsafe_b64encode(byte * 32).decode("ascii")


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


def test_episode_evidence_cipher_round_trip_and_tamper_detection() -> None:
    secret = "raw-secret-value"
    cipher = EpisodeEvidenceCipher.from_encoded_key(_encoded_key())

    evidence = cipher.encrypt(
        episode_id="episode-1",
        payload={"episode_id": "episode-1", "secret": secret},
        retention_days=7,
    )

    assert secret.encode() not in evidence.ciphertext
    assert evidence.expires_at - evidence.created_at == timedelta(days=7)
    assert cipher.decrypt(evidence)["secret"] == secret

    tampered = evidence.model_copy(
        update={"ciphertext": evidence.ciphertext[:-1] + bytes([evidence.ciphertext[-1] ^ 1])}
    )
    with pytest.raises(InvalidTag):
        cipher.decrypt(tampered)


@pytest.mark.parametrize(
    "encoded_key",
    ["not base64!", base64.urlsafe_b64encode(b"too-short").decode("ascii")],
)
def test_episode_evidence_cipher_rejects_invalid_keys(encoded_key: str) -> None:
    with pytest.raises(ValueError, match="episode evidence key"):
        EpisodeEvidenceCipher.from_encoded_key(encoded_key)


def test_sqlite_episode_evidence_is_erasable_but_not_mutable(tmp_path: Path) -> None:
    store = _store(tmp_path)
    episode = _episode()
    cipher = EpisodeEvidenceCipher.from_encoded_key(_encoded_key())
    evidence = cipher.encrypt(
        episode_id=episode.id,
        payload={"episode_id": episode.id, "raw": "sensitive"},
        retention_days=30,
    )

    store.add_execution_episode_bundle(episode, evidence)

    assert store.get_execution_episode(episode.id) == episode
    assert store.get_execution_episode_evidence(episode.id) == evidence
    with pytest.raises(sqlite3.IntegrityError, match="evidence is immutable"):
        store._conn.execute(  # noqa: SLF001 - verifies the database invariant
            "UPDATE execution_episode_evidence SET key_id = ? WHERE episode_id = ?",
            ("f" * 16, episode.id),
        )
    store._conn.rollback()  # noqa: SLF001 - clear the failed direct test transaction

    assert store.delete_execution_episode_evidence(episode.id) is True
    assert store.get_execution_episode_evidence(episode.id) is None
    assert store.get_execution_episode(episode.id) == episode


def test_sqlite_episode_evidence_bundle_is_atomic_and_cleanup_preserves_metadata(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    cipher = EpisodeEvidenceCipher.from_encoded_key(_encoded_key())
    episode = _episode()
    mismatched = cipher.encrypt(
        episode_id="different-episode",
        payload={"episode_id": "different-episode"},
        retention_days=1,
    )

    with pytest.raises(ValueError, match="must reference"):
        store.add_execution_episode_bundle(episode, mismatched)
    assert store.get_execution_episode(episode.id) is None

    expired = cipher.encrypt(
        episode_id=episode.id,
        payload={"episode_id": episode.id},
        retention_days=1,
    ).model_copy(update={"expires_at": datetime.now(UTC) - timedelta(seconds=1)})
    store.add_execution_episode_bundle(episode, expired)

    assert store.cleanup_expired_execution_episode_evidence(datetime.now(UTC)) == 1
    assert store.get_execution_episode_evidence(episode.id) is None
    assert store.get_execution_episode(episode.id) == episode


def test_raw_episode_evidence_excludes_provider_api_key() -> None:
    raw_secret = "raw-task-secret"
    provider_api_key = "provider-api-key-must-not-be-captured"
    spec = _spec(raw_secret).model_copy(
        update={
            "llm_config": LLMConfig(
                provider_id="example-provider",
                model="example-model",
                api_key=provider_api_key,
                api_base="https://provider.invalid/v1",
            )
        }
    )
    result = WorkerResult(
        summary="done",
        output={"raw": raw_secret},
        tools_used=["web_search"],
    )
    episode = build_worker_execution_episode(
        spec=spec,
        result=result,
        stored_output=result.output,
        status="completed",
        launcher_kind="TestLauncher",
        evidence_storage="aes256gcm",
    )
    cipher = EpisodeEvidenceCipher.from_encoded_key(_encoded_key())

    evidence = build_encrypted_worker_episode_evidence(
        cipher=cipher,
        episode=episode,
        spec=spec,
        result=result,
        stored_output=result.output,
        retention_days=30,
    )
    payload = cipher.decrypt(evidence)
    serialized = json.dumps(payload, sort_keys=True)

    assert raw_secret in serialized
    assert provider_api_key not in serialized
    assert payload["execution"]["provider_id"] == "example-provider"
    assert payload["execution"]["model"] == "example-model"


def test_runtime_records_encrypted_evidence_only_when_key_is_configured(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    settings = Settings(
        OCTOPAL_EPISODE_EVIDENCE_KEY=_encoded_key(),
        OCTOPAL_EPISODE_EVIDENCE_RETENTION_DAYS=5,
    )
    runtime = WorkerRuntime(
        store=store,
        policy=object(),  # type: ignore[arg-type]
        workspace_dir=tmp_path / "workspace",
        launcher=object(),  # type: ignore[arg-type]
        settings=settings,
    )
    result = WorkerResult(summary="done", output={"raw": "retained"})

    asyncio.run(
        runtime._record_execution_episode(  # noqa: SLF001 - focused persistence integration
            spec=_spec("raw-secret"),
            result=result,
            stored_output=result.output,
            worker_status="completed",
        )
    )

    episodes = store.list_execution_episodes(worker_run_id="worker-1")
    assert len(episodes) == 1
    evidence = store.get_execution_episode_evidence(episodes[0].id)
    assert evidence is not None
    assert episodes[0].provenance["content_policy"] == "metadata_with_encrypted_raw_v1"
    assert runtime._episode_evidence_cipher is not None  # noqa: SLF001
    assert runtime._episode_evidence_cipher.decrypt(evidence)["result"]["output"] == {
        "raw": "retained"
    }
    assert "OCTOPAL_EPISODE_EVIDENCE_KEY" not in runtime._build_worker_env(  # noqa: SLF001
        _spec("raw-secret")
    )


def test_settings_hide_episode_evidence_key_from_dumps() -> None:
    settings = Settings(OCTOPAL_EPISODE_EVIDENCE_KEY=_encoded_key())

    assert settings.episode_evidence_key == _encoded_key()
    assert "episode_evidence_key" not in settings.model_dump()
