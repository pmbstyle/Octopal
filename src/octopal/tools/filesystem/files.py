from __future__ import annotations

import shutil
from pathlib import Path, PurePosixPath
from typing import Any

from octopal.tools.filesystem.path_safety import WorkspacePathError, resolve_workspace_path


def _get_paths(ctx: dict[str, Any] | Path) -> tuple[Path, Path, list[str] | None]:
    if isinstance(ctx, Path):
        return ctx, ctx, None

    worker_dir = Path(ctx["base_dir"])
    workspace_root = Path(ctx.get("workspace_root") or worker_dir)
    worker = ctx.get("worker")
    allowed_paths = getattr(worker.spec, "allowed_paths", None) if worker and hasattr(worker, "spec") else None

    return workspace_root, worker_dir, list(allowed_paths) if allowed_paths is not None else None


def _normalized_parts(raw_path: str) -> tuple[str, ...]:
    normalized = str(PurePosixPath(str(raw_path or "").replace("\\", "/")))
    return tuple(part for part in normalized.split("/") if part and part != ".")


def _is_shared_workspace_path(
    path: str,
    *,
    workspace_root: Path,
    allowed_paths: list[str] | None,
) -> bool:
    if not allowed_paths:
        return False

    requested_parts = _normalized_parts(path)
    if not requested_parts or ".." in requested_parts:
        return False

    for allowed in allowed_paths:
        allowed_parts = _normalized_parts(allowed)
        if not allowed_parts:
            continue
        candidate = workspace_root / Path(*allowed_parts)
        exact_match = requested_parts == allowed_parts
        prefix_match = requested_parts[: len(allowed_parts)] == allowed_parts
        if candidate.exists() and candidate.is_file():
            if exact_match:
                return True
            continue
        if prefix_match:
            return True
    return False


def _resolve_tool_path(
    raw_path: str,
    *,
    workspace_root: Path,
    worker_dir: Path,
    allowed_paths: list[str] | None,
    must_exist: bool = False,
    allow_final_symlink: bool = False,
) -> Path:
    target_root = worker_dir
    target_allowlist = None
    if _is_shared_workspace_path(raw_path, workspace_root=workspace_root, allowed_paths=allowed_paths):
        target_root = workspace_root
        target_allowlist = allowed_paths
    return resolve_workspace_path(
        target_root,
        raw_path,
        must_exist=must_exist,
        allow_final_symlink=allow_final_symlink,
        allowed_paths=target_allowlist,
    )


def fs_read(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    path = str(args.get("path", "")).strip()
    workspace_root, worker_dir, allowed_paths = _get_paths(ctx)
    try:
        target = _resolve_tool_path(
            path,
            workspace_root=workspace_root,
            worker_dir=worker_dir,
            allowed_paths=allowed_paths,
            must_exist=True,
        )
        return target.read_text(encoding="utf-8")
    except WorkspacePathError as exc:
        return f"fs_read error: {exc}."
    except Exception as exc:
        return f"fs_read error: {exc}"


def fs_write(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    path = str(args.get("path", "")).strip()
    content = str(args.get("content", ""))
    workspace_root, worker_dir, allowed_paths = _get_paths(ctx)
    try:
        target = _resolve_tool_path(
            path,
            workspace_root=workspace_root,
            worker_dir=worker_dir,
            allowed_paths=allowed_paths,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return "fs_write ok"
    except WorkspacePathError as exc:
        return f"fs_write error: {exc}."
    except Exception as exc:
        return f"fs_write error: {exc}"


def fs_list(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    path = str(args.get("path", "")).strip() or "."
    workspace_root, worker_dir, allowed_paths = _get_paths(ctx)
    try:
        target = _resolve_tool_path(
            path,
            workspace_root=workspace_root,
            worker_dir=worker_dir,
            allowed_paths=allowed_paths,
            must_exist=True,
        )
        if not target.is_dir():
            return "fs_list error: path is not a directory."
        entries = sorted([p.name for p in target.iterdir()])
        return "\n".join(entries)
    except WorkspacePathError as exc:
        return f"fs_list error: {exc}."
    except Exception as exc:
        return f"fs_list error: {exc}"


def fs_move(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    source = str(args.get("source", "")).strip()
    destination = str(args.get("destination", "")).strip()
    workspace_root, worker_dir, allowed_paths = _get_paths(ctx)
    if not source:
        return "fs_move error: source is required."
    if not destination:
        return "fs_move error: destination is required."
    try:
        src = _resolve_tool_path(
            source,
            workspace_root=workspace_root,
            worker_dir=worker_dir,
            allowed_paths=allowed_paths,
            must_exist=True,
        )
        dst = _resolve_tool_path(
            destination,
            workspace_root=workspace_root,
            worker_dir=worker_dir,
            allowed_paths=allowed_paths,
        )
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
        return "fs_move ok"
    except WorkspacePathError as exc:
        return f"fs_move error: {exc}."
    except Exception as exc:
        return f"fs_move error: {exc}"


def fs_delete(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    path = str(args.get("path", "")).strip()
    workspace_root, worker_dir, allowed_paths = _get_paths(ctx)
    try:
        target = _resolve_tool_path(
            path,
            workspace_root=workspace_root,
            worker_dir=worker_dir,
            allowed_paths=allowed_paths,
            must_exist=True,
            allow_final_symlink=True,
        )
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
