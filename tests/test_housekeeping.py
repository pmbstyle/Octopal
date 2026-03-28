from __future__ import annotations

import os
import time
from pathlib import Path

from octopal.runtime.housekeeping import (
    cleanup_ephemeral_worker_dirs,
    cleanup_workspace_tmp,
    remove_tree_with_retries,
    rotate_canon_events,
)


def _touch(path: Path, text: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_cleanup_workspace_tmp_removes_old_files(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    old_file = workspace / "tmp" / "old.txt"
    new_file = workspace / "tmp" / "new.txt"
    _touch(old_file, "old")
    _touch(new_file, "new")

    # Make old file older than 72h.
    old_ts = time.time() - (72 * 3600)
    os.utime(old_file, (old_ts, old_ts))

    result = cleanup_workspace_tmp(workspace, retention_hours=24)
    assert result.deleted_files >= 1
    assert not old_file.exists()
    assert new_file.exists()


def test_cleanup_ephemeral_worker_dirs_removes_old_uuid_dirs_only(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    stale_worker = workspace / "workers" / "657aa2f0-7e40-4666-98dd-941fd435692a"
    template_worker = workspace / "workers" / "coder"
    fresh_worker = workspace / "workers" / "30e3a19a-6998-44b2-9a7b-4dc328f9b482"

    _touch(stale_worker / "data" / "note.txt", "stale")
    _touch(template_worker / "worker.json", '{"id":"coder"}')
    _touch(fresh_worker / "spec.json", "{}")

    old_ts = time.time() - (72 * 3600)
    os.utime(stale_worker / "data" / "note.txt", (old_ts, old_ts))
    os.utime(stale_worker / "data", (old_ts, old_ts))
    os.utime(stale_worker, (old_ts, old_ts))

    result = cleanup_ephemeral_worker_dirs(workspace, retention_minutes=15)

    assert result.deleted_dirs == 1
    assert not stale_worker.exists()
    assert template_worker.exists()
    assert fresh_worker.exists()


def test_remove_tree_with_retries_retries_permission_errors(tmp_path: Path) -> None:
    target = tmp_path / "worker"
    target.mkdir()

    calls = {"count": 0}

    def _flaky_rmtree(path: Path, onerror=None) -> None:
        assert path == target
        calls["count"] += 1
        if calls["count"] < 3:
            raise PermissionError("directory is busy")

    import octopal.runtime.housekeeping as housekeeping_mod

    original_rmtree = housekeeping_mod.shutil.rmtree
    housekeeping_mod.shutil.rmtree = _flaky_rmtree
    try:
        removed = remove_tree_with_retries(target, retries=3, base_delay_seconds=0)
    finally:
        housekeeping_mod.shutil.rmtree = original_rmtree

    assert removed is True
    assert calls["count"] == 3


def test_rotate_canon_events_bootstraps_snapshot_and_keeps_archives(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    canon_dir = workspace / "memory" / "canon"
    canon_dir.mkdir(parents=True, exist_ok=True)
    (canon_dir / "facts.md").write_text("# Facts\n\nA\n", encoding="utf-8")
    (canon_dir / "decisions.md").write_text("# Decisions\n\nB\n", encoding="utf-8")
    events = canon_dir / "events.jsonl"
    events.write_text('{"ts":"x","filename":"facts.md","mode":"append","content":"c"}\n' * 30, encoding="utf-8")

    # Seed old archives to validate pruning.
    _touch(canon_dir / "events.20250101010101.jsonl", "old1")
    _touch(canon_dir / "events.20250101010102.jsonl", "old2")

    result = rotate_canon_events(
        workspace,
        max_bytes=50,
        keep_archives=2,
    )

    assert result.rotated is True
    assert result.archived_file is not None
    assert result.bootstrap_entries >= 2
    assert events.exists()
    rebuilt = events.read_text(encoding="utf-8")
    assert '"mode": "overwrite"' in rebuilt
    archives = sorted(canon_dir.glob("events.*.jsonl"))
    assert len(archives) <= 2
