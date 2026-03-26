from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from octopal.tools.filesystem.download import download_file
from octopal.tools.filesystem.files import fs_delete, fs_list, fs_read, fs_write


def _ensure_symlink_supported(tmp_path: Path) -> None:
    probe_target = tmp_path / "probe_target"
    probe_target.mkdir(parents=True, exist_ok=True)
    probe_link = tmp_path / "probe_link"
    try:
        probe_link.symlink_to(probe_target, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlink not supported in this environment: {exc}")
    finally:
        try:
            if probe_link.exists() or probe_link.is_symlink():
                probe_link.unlink()
        except OSError:
            pass


def test_fs_write_rejects_path_traversal(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    result = fs_write({"path": "../escape.txt", "content": "nope"}, workspace)
    assert result.startswith("fs_write error:")
    assert "outside workspace" in result


def test_fs_write_rejects_symlink_escape(tmp_path: Path) -> None:
    _ensure_symlink_supported(tmp_path)

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "outside"
    outside.mkdir(parents=True, exist_ok=True)
    link_dir = workspace / "linked"
    link_dir.symlink_to(outside, target_is_directory=True)

    result = fs_write({"path": "linked/pwn.txt", "content": "x"}, workspace)
    assert result.startswith("fs_write error:")
    assert "outside workspace" in result
    assert not (outside / "pwn.txt").exists()


def test_fs_delete_unlinks_symlink_without_touching_target(tmp_path: Path) -> None:
    _ensure_symlink_supported(tmp_path)

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    outside_file = tmp_path / "outside.txt"
    outside_file.write_text("safe", encoding="utf-8")
    link_file = workspace / "link.txt"
    link_file.symlink_to(outside_file, target_is_directory=False)

    result = fs_delete({"path": "link.txt"}, workspace)
    assert result == "fs_delete ok"
    assert outside_file.exists()
    assert not link_file.exists()


def test_download_file_rejects_filename_with_directories(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    payload = asyncio.run(
        download_file(
            {"url": "https://example.com/file.txt", "filename": "../escape.txt"},
            {"base_dir": workspace},
        )
    )
    assert "filename must not contain directory components" in payload


def test_fs_tools_keep_worker_scratch_and_allow_explicit_shared_paths(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    worker_dir = workspace / "workers" / "worker-1"
    shared_dir = workspace / "src"
    worker_dir.mkdir(parents=True, exist_ok=True)
    shared_dir.mkdir(parents=True, exist_ok=True)

    (worker_dir / "notes.txt").write_text("local", encoding="utf-8")
    (shared_dir / "shared.txt").write_text("shared", encoding="utf-8")
    (worker_dir / "src").mkdir(parents=True, exist_ok=True)
    (worker_dir / "src" / "scratch_only.txt").write_text("scratch", encoding="utf-8")

    ctx = {
        "base_dir": worker_dir,
        "workspace_root": workspace,
        "worker": SimpleNamespace(spec=SimpleNamespace(allowed_paths=["src"], id="worker-1")),
    }

    assert fs_read({"path": "notes.txt"}, ctx) == "local"
    assert fs_read({"path": "src/shared.txt"}, ctx) == "shared"
    assert fs_list({"path": "src"}, ctx) == "shared.txt"

    assert fs_write({"path": "draft.md", "content": "worker-only"}, ctx) == "fs_write ok"
    assert (worker_dir / "draft.md").read_text(encoding="utf-8") == "worker-only"

    assert fs_write({"path": "src/generated.txt", "content": "from-worker"}, ctx) == "fs_write ok"
    assert (shared_dir / "generated.txt").read_text(encoding="utf-8") == "from-worker"


def test_fs_tools_without_allowed_paths_stay_in_worker_workspace(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    worker_dir = workspace / "workers" / "worker-1"
    shared_dir = workspace / "src"
    worker_dir.mkdir(parents=True, exist_ok=True)
    shared_dir.mkdir(parents=True, exist_ok=True)

    ctx = {
        "base_dir": worker_dir,
        "workspace_root": workspace,
        "worker": SimpleNamespace(spec=SimpleNamespace(allowed_paths=None, id="worker-1")),
    }

    assert fs_write({"path": "src/local_only.txt", "content": "scratch"}, ctx) == "fs_write ok"
    assert (worker_dir / "src" / "local_only.txt").read_text(encoding="utf-8") == "scratch"
    assert not (shared_dir / "local_only.txt").exists()
