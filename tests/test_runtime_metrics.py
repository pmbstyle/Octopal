from __future__ import annotations

import json
from pathlib import Path

from octopal.runtime.metrics import read_metrics_snapshot, update_component_gauges


def test_update_component_gauges_writes_and_merges_metrics(tmp_path: Path, monkeypatch) -> None:
    state_dir = tmp_path / "state"
    monkeypatch.setenv("OCTOPAL_STATE_DIR", str(state_dir))

    update_component_gauges("octo", {"queued": 3})
    update_component_gauges("workers", {"active": 2})

    snapshot = read_metrics_snapshot(state_dir)

    assert snapshot["octo"]["queued"] == 3
    assert snapshot["workers"]["active"] == 2
    assert snapshot["octo"]["updated_at"]
    assert snapshot["workers"]["updated_at"]


def test_update_component_gauges_ignores_empty_component(tmp_path: Path, monkeypatch) -> None:
    state_dir = tmp_path / "state"
    monkeypatch.setenv("OCTOPAL_STATE_DIR", str(state_dir))

    update_component_gauges("", {"queued": 3})

    assert not (state_dir / "runtime_metrics.json").exists()


def test_read_metrics_snapshot_returns_empty_for_missing_invalid_and_non_dict(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = state_dir / "runtime_metrics.json"

    assert read_metrics_snapshot(state_dir) == {}

    metrics_path.write_text("{broken", encoding="utf-8")
    assert read_metrics_snapshot(state_dir) == {}

    metrics_path.write_text(json.dumps(["not", "a", "dict"]), encoding="utf-8")
    assert read_metrics_snapshot(state_dir) == {}


def test_update_component_gauges_recovers_from_corrupt_existing_file(
    tmp_path: Path, monkeypatch
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = state_dir / "runtime_metrics.json"
    metrics_path.write_text("{broken", encoding="utf-8")
    monkeypatch.setenv("OCTOPAL_STATE_DIR", str(state_dir))

    update_component_gauges("gateway", {"clients": 5})

    snapshot = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert snapshot["gateway"]["clients"] == 5
    assert "updated_at" in snapshot["gateway"]
