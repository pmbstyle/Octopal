from __future__ import annotations

import os
from pathlib import Path


class WorkspacePathError(ValueError):
    """Raised when a user-supplied path is unsafe for workspace operations."""


def resolve_workspace_path(
    base_dir: Path,
    user_path: str,
    *,
    must_exist: bool = False,
    allow_final_symlink: bool = False,
    allowed_paths: list[str] | None = None,
) -> Path:
    raw = str(user_path or "").strip()
    if not raw:
        raise WorkspacePathError("path is required")
    if "\x00" in raw:
        raise WorkspacePathError("path contains null byte")

    root = base_dir.resolve()
    candidate = Path(os.path.normpath(str(root / raw)))
    _assert_within(root, candidate)

    check_target = candidate
    if allow_final_symlink and candidate.exists() and candidate.is_symlink():
        check_target = candidate.parent
    _assert_existing_ancestor_within(root, check_target)

    if allowed_paths is not None:
        # Resolve all allowed path prefixes
        allowed_resolved = []
        for p in allowed_paths:
            p_path = (root / p).resolve(strict=False)
            allowed_resolved.append(p_path)
            
        # Check if candidate is within any of the allowed paths
        # or if it's the worker's own specific directory which isn't part of allowed_paths but should be implicit
        # Actually, worker's directory check is handled implicitly if it's not restricted or we just check allowed_paths
        
        # We need to resolve candidate strictly if it exists, or loosely if it doesn't
        resolved_candidate = candidate.resolve(strict=False)
        is_allowed = False
        for allowed_root in allowed_resolved:
            if resolved_candidate == allowed_root or allowed_root in resolved_candidate.parents:
                is_allowed = True
                break
                
        if not is_allowed:
            raise WorkspacePathError(f"access denied: path '{raw}' is outside allowed paths")

    if must_exist and not candidate.exists():
        raise WorkspacePathError("path does not exist")

    if candidate.exists():
        if candidate.is_symlink():
            if not allow_final_symlink:
                raise WorkspacePathError("final path cannot be a symlink")
            return candidate
        resolved_existing = candidate.resolve(strict=True)
        _assert_within(root, resolved_existing)
        return resolved_existing

    return candidate


def is_within_workspace(base_dir: Path, target: Path) -> bool:
    try:
        root = base_dir.resolve()
        resolved = target.resolve(strict=False)
        _assert_within(root, resolved)
        _assert_existing_ancestor_within(root, resolved)
        return True
    except Exception:
        return False


def _assert_within(root: Path, target: Path) -> None:
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise WorkspacePathError("path outside workspace") from exc


def _assert_existing_ancestor_within(root: Path, candidate: Path) -> None:
    ancestor = candidate
    while not ancestor.exists():
        parent = ancestor.parent
        if parent == ancestor:
            break
        ancestor = parent
    if ancestor.exists():
        resolved_ancestor = ancestor.resolve(strict=True)
        _assert_within(root, resolved_ancestor)
