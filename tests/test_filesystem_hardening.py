from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from broodmind.tools.download_file import download_file
from broodmind.tools.filesystem import fs_delete, fs_write


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
