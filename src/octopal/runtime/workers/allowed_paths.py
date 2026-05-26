from __future__ import annotations

import os
import re
from collections.abc import Mapping, Sequence
from pathlib import Path

_PATH_TOKEN_RE = re.compile(r"(?P<path>(?:[A-Za-z]:[\\/])?(?:[\w.@()+-]+[\\/])+[\w.@()+-]+)")


def _workspace_path(workspace_dir: Path | None = None) -> Path:
    if workspace_dir is not None:
        return Path(workspace_dir).resolve()
    return Path(os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")).resolve()


def _workspace_relative_path(
    raw_path: object,
    *,
    workspace_dir: Path | None = None,
    require_exists: bool = False,
) -> str | None:
    raw = str(raw_path or "").strip().strip("`'\".,;:)")
    if not raw:
        return None

    workspace = _workspace_path(workspace_dir)
    path = Path(raw)
    if path.is_absolute():
        try:
            resolved = path.resolve()
            rel_path = resolved.relative_to(workspace)
        except (OSError, ValueError):
            return None
    else:
        rel_path = path
        try:
            resolved = (workspace / rel_path).resolve()
            rel_path = resolved.relative_to(workspace)
        except (OSError, ValueError):
            return None

    if require_exists and not (workspace / rel_path).exists():
        return None
    return rel_path.as_posix()


def _existing_parent_workspace_path(
    raw_path: object,
    *,
    workspace_dir: Path | None = None,
) -> str | None:
    raw = str(raw_path or "").strip().strip("`'\".,;:)")
    if not raw:
        return None
    rel = _workspace_relative_path(raw, workspace_dir=workspace_dir)
    if not rel:
        return None
    rel_path = Path(rel)
    if not rel_path.suffix:
        return None

    workspace = _workspace_path(workspace_dir)
    parent = rel_path.parent
    while str(parent) not in {"", "."}:
        if (workspace / parent).is_dir():
            return parent.as_posix()
        parent = parent.parent
    return None


def normalize_allowed_paths(
    value: object,
    *,
    workspace_dir: Path | None = None,
) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        raw_items = [value]
    elif isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        return None

    seen: set[str] = set()
    normalized: list[str] = []
    for item in raw_items:
        rel = _workspace_relative_path(item, workspace_dir=workspace_dir)
        if not rel or rel in seen:
            continue
        seen.add(rel)
        normalized.append(rel)
    return normalized or None


def infer_allowed_paths_from_task(
    task: str,
    *,
    workspace_dir: Path | None = None,
) -> list[str] | None:
    return infer_allowed_paths_from_values(task, workspace_dir=workspace_dir)


def infer_allowed_paths_from_values(
    *values: object,
    workspace_dir: Path | None = None,
) -> list[str] | None:
    seen: set[str] = set()
    inferred: list[str] = []
    for raw_path in _iter_path_candidates(values):
        rel = _workspace_relative_path(
            raw_path,
            workspace_dir=workspace_dir,
            require_exists=True,
        )
        if not rel:
            rel = _existing_parent_workspace_path(raw_path, workspace_dir=workspace_dir)
        if not rel or rel in seen:
            continue
        seen.add(rel)
        inferred.append(rel)
    return inferred or None


def _iter_path_candidates(value: object) -> list[str]:
    candidates: list[str] = []

    def visit(item: object) -> None:
        if item is None:
            return
        if isinstance(item, str):
            for match in _PATH_TOKEN_RE.finditer(item):
                if _is_url_path_match(item, match.start()):
                    continue
                candidates.append(match.group("path"))
            return
        if isinstance(item, Mapping):
            for nested in item.values():
                visit(nested)
            return
        if isinstance(item, Sequence) and not isinstance(item, (bytes, bytearray)):
            for nested in item:
                visit(nested)

    visit(value)
    return candidates


def _is_url_path_match(text: str, start: int) -> bool:
    return start >= 3 and text[start - 3 : start] == "://"
