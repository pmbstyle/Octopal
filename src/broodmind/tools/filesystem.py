from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from broodmind.tools.path_safety import WorkspacePathError, resolve_workspace_path


def fs_read(args: dict[str, Any], base_dir: Path) -> str:
    path = str(args.get("path", "")).strip()
    try:
        target = resolve_workspace_path(base_dir, path, must_exist=True)
        return target.read_text(encoding="utf-8")
    except WorkspacePathError as exc:
        return f"fs_read error: {exc}."
    except Exception as exc:
        return f"fs_read error: {exc}"


def fs_write(args: dict[str, Any], base_dir: Path) -> str:
    path = str(args.get("path", "")).strip()
    content = str(args.get("content", ""))
    try:
        target = resolve_workspace_path(base_dir, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return "fs_write ok"
    except WorkspacePathError as exc:
        return f"fs_write error: {exc}."
    except Exception as exc:
        return f"fs_write error: {exc}"


def fs_list(args: dict[str, Any], base_dir: Path) -> str:
    path = str(args.get("path", "")).strip() or "."
    try:
        target = resolve_workspace_path(base_dir, path, must_exist=True)
        if not target.is_dir():
            return "fs_list error: path is not a directory."
        entries = sorted([p.name for p in target.iterdir()])
        return "\n".join(entries)
    except WorkspacePathError as exc:
        return f"fs_list error: {exc}."
    except Exception as exc:
        return f"fs_list error: {exc}"


def fs_move(args: dict[str, Any], base_dir: Path) -> str:
    source = str(args.get("source", "")).strip()
    destination = str(args.get("destination", "")).strip()
    if not source:
        return "fs_move error: source is required."
    if not destination:
        return "fs_move error: destination is required."
    try:
        src = resolve_workspace_path(base_dir, source, must_exist=True)
        dst = resolve_workspace_path(base_dir, destination)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
        return "fs_move ok"
    except WorkspacePathError as exc:
        return f"fs_move error: {exc}."
    except Exception as exc:
        return f"fs_move error: {exc}"


def fs_delete(args: dict[str, Any], base_dir: Path) -> str:
    path = str(args.get("path", "")).strip()
    try:
        target = resolve_workspace_path(base_dir, path, must_exist=True, allow_final_symlink=True)
        if target.is_symlink():
            target.unlink()
            return "fs_delete ok"
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
        return "fs_delete ok"
    except WorkspacePathError as exc:
        return f"fs_delete error: {exc}."
    except Exception as exc:
        return f"fs_delete error: {exc}"
