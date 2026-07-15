from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from typer.testing import CliRunner

from octopal.cli.main import app
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.store.models import ExecutionEpisodeRecord
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.episode_evidence import EpisodeEvidenceCipher

runner = CliRunner()
_RAW_SECRET = "raw-evidence-secret-value"


def _encoded_key(byte: bytes = b"k") -> str:
    return base64.urlsafe_b64encode(byte * 32).decode("ascii")


def _seed_episode(
    tmp_path: Path,
    monkeypatch,
    *,
    expired: bool = False,
) -> tuple[SQLiteStore, str]:
    state_dir = tmp_path / "data"
    workspace_dir = tmp_path / "workspace"
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "storage": {
                    "state_dir": str(state_dir),
                    "workspace_dir": str(workspace_dir),
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OCTOPAL_EPISODE_EVIDENCE_KEY", _encoded_key())
    store = SQLiteStore(
        Settings(
            OCTOPAL_STATE_DIR=state_dir,
            OCTOPAL_WORKSPACE_DIR=workspace_dir,
        )
    )
    now = datetime.now(UTC)
    episode = ExecutionEpisodeRecord(
        id="episode-cli-test",
        worker_run_id="worker-cli-test",
        task_fingerprint="task-fingerprint",
        environment_fingerprint="environment-fingerprint",
        capability_fingerprint="capability-fingerprint",
        result_fingerprint="result-fingerprint",
        status="completed",
        source_kind="worker",
        trust_state="observed",
        trajectory_refs={"worker_record_id": "worker-cli-test"},
        result_metadata={"output_key_count": 1},
        verification={"result_contract_validated": True},
        provenance={"content_policy": "metadata_with_encrypted_raw_v1"},
        created_at=now,
    )
    cipher = EpisodeEvidenceCipher.from_encoded_key(_encoded_key())
    evidence = cipher.encrypt(
        episode_id=episode.id,
        payload={"episode_id": episode.id, "result": {"secret": _RAW_SECRET}},
        retention_days=30,
    )
    if expired:
        evidence = evidence.model_copy(update={"expires_at": now - timedelta(seconds=1)})
    store.add_execution_episode_bundle(episode, evidence)
    return store, episode.id


def test_episodes_list_and_default_show_never_load_raw_evidence(tmp_path, monkeypatch) -> None:
    store, episode_id = _seed_episode(tmp_path, monkeypatch)

    list_result = runner.invoke(app, ["episodes", "list", "--json"])
    show_result = runner.invoke(app, ["episodes", "show", episode_id, "--json"])

    assert list_result.exit_code == 0
    list_payload = json.loads(list_result.stdout)
    assert list_payload["episodes"][0]["id"] == episode_id
    assert list_payload["episodes"][0]["evidence"]["available"] is True
    assert _RAW_SECRET not in list_result.stdout
    assert _encoded_key() not in list_result.stdout

    assert show_result.exit_code == 0
    show_payload = json.loads(show_result.stdout)
    assert show_payload["episode"]["id"] == episode_id
    assert show_payload["evidence"]["available"] is True
    assert "raw_evidence" not in show_payload
    assert _RAW_SECRET not in show_result.stdout
    assert store.get_execution_episode_evidence_metadata(episode_id) is not None

    purge_result = runner.invoke(app, ["episodes", "purge-storage", "--json"])
    assert purge_result.exit_code == 0
    assert json.loads(purge_result.stdout) == {
        "live_evidence_deleted": False,
        "secure_purge_complete": True,
    }
    assert store.get_execution_episode_evidence(episode_id) is not None


def test_episodes_show_requires_key_before_revealing_raw_evidence(tmp_path, monkeypatch) -> None:
    _store, episode_id = _seed_episode(tmp_path, monkeypatch)
    monkeypatch.delenv("OCTOPAL_EPISODE_EVIDENCE_KEY")

    result = runner.invoke(
        app,
        ["episodes", "show", episode_id, "--reveal-evidence", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "evidence_key_missing"
    assert _RAW_SECRET not in result.stdout


def test_episodes_show_reveals_raw_evidence_only_with_explicit_flag(tmp_path, monkeypatch) -> None:
    _store, episode_id = _seed_episode(tmp_path, monkeypatch)

    result = runner.invoke(
        app,
        ["episodes", "show", episode_id, "--reveal-evidence", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["raw_evidence"]["result"]["secret"] == _RAW_SECRET
    audit = _store.list_audit(limit=1)[0]
    assert audit.event_type == "execution_episode_evidence_revealed"
    assert audit.data == {"episode_id": episode_id, "interface": "cli"}
    assert _RAW_SECRET not in audit.model_dump_json()


def test_episodes_show_erases_expired_evidence_instead_of_revealing_it(
    tmp_path, monkeypatch
) -> None:
    store, episode_id = _seed_episode(tmp_path, monkeypatch, expired=True)

    result = runner.invoke(
        app,
        ["episodes", "show", episode_id, "--reveal-evidence", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "evidence_expired"
    assert _RAW_SECRET not in result.stdout
    assert store.get_execution_episode_evidence(episode_id) is None
    audit = store.list_audit(limit=1)[0]
    assert audit.event_type == "execution_episode_evidence_expired_erased"


def test_episodes_erase_evidence_requires_confirmation_and_preserves_metadata(
    tmp_path, monkeypatch
) -> None:
    store, episode_id = _seed_episode(tmp_path, monkeypatch)

    unconfirmed = runner.invoke(
        app,
        ["episodes", "erase-evidence", episode_id, "--json"],
    )

    assert unconfirmed.exit_code == 1
    assert json.loads(unconfirmed.stdout)["error"]["code"] == "confirmation_required"
    assert store.get_execution_episode_evidence(episode_id) is not None

    erased = runner.invoke(
        app,
        ["episodes", "erase-evidence", episode_id, "--yes", "--json"],
    )

    assert erased.exit_code == 0
    payload = json.loads(erased.stdout)
    assert payload == {
        "episode_id": episode_id,
        "evidence_deleted": True,
        "metadata_preserved": True,
    }
    assert store.get_execution_episode_evidence(episode_id) is None
    assert store.get_execution_episode(episode_id) is not None
    audit = store.list_audit(limit=1)[0]
    assert audit.event_type == "execution_episode_evidence_erased"
    assert audit.data == {"episode_id": episode_id, "interface": "cli"}
