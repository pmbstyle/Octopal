"""Lossless, worker-run-local storage for tool results that do not fit inline."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_HANDLE_RE = re.compile(r"source-\d{4}-[0-9a-f]{12}\Z")


@dataclass(frozen=True)
class ToolResultReference:
    """Opaque reference to one immutable result captured during a worker run."""

    handle: str
    tool_name: str
    char_count: int
    sha256: str


class ToolResultStore:
    """Keep complete JSON-compatible tool results available for the active worker.

    Tool output is not an evidence archive and follows the worker directory lifecycle.
    It exists so prompt compaction can never become data loss during an active run.
    """

    def __init__(self, worker_dir: Path) -> None:
        self._root = worker_dir / "tool-results"
        self._references: dict[str, ToolResultReference] = {}
        self._paths: dict[str, Path] = {}
        self._sequence = 0
        self._load_index()

    def store(self, tool_name: str, result: Any) -> ToolResultReference | None:
        """Persist a JSON-compatible result exactly enough for later worker reads."""

        content = _serialize_result(result)
        if content is None:
            return None
        digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
        self._sequence += 1
        handle = f"source-{self._sequence:04d}-{digest[:12]}"
        reference = ToolResultReference(
            handle=handle,
            tool_name=str(tool_name or "unknown"),
            char_count=len(content),
            sha256=digest,
        )
        self._root.mkdir(parents=True, exist_ok=True)
        path = self._root / f"{handle}.json"
        path.write_text(content, encoding="utf-8")
        self._references[handle] = reference
        self._paths[handle] = path
        self._write_index()
        return reference

    def read(self, handle: str, *, offset: int, length: int) -> dict[str, Any]:
        """Return the exact requested character range, never an implicit preview."""

        reference, content = self._resolve(handle)
        if offset < 0:
            raise ValueError("offset must be non-negative")
        if length <= 0:
            raise ValueError("length must be positive")
        end = min(offset + length, len(content))
        return {
            "handle": reference.handle,
            "tool_name": reference.tool_name,
            "offset": offset,
            "length": end - offset if offset < len(content) else 0,
            "total_chars": reference.char_count,
            "sha256": reference.sha256,
            "content": content[offset:end],
            "end_of_content": end >= len(content),
        }

    def search(self, handle: str, *, query: str, max_matches: int) -> dict[str, Any]:
        """Locate exact character ranges without replacing source content with excerpts."""

        reference, content = self._resolve(handle)
        normalized_query = str(query or "").strip()
        if not normalized_query:
            raise ValueError("query must not be empty")
        if max_matches <= 0:
            raise ValueError("max_matches must be positive")
        matches: list[dict[str, int]] = []
        for match in re.finditer(re.escape(normalized_query), content, flags=re.IGNORECASE):
            matches.append({"offset": match.start(), "length": match.end() - match.start()})
            if len(matches) >= max_matches:
                break
        return {
            "handle": reference.handle,
            "tool_name": reference.tool_name,
            "query": normalized_query,
            "total_chars": reference.char_count,
            "sha256": reference.sha256,
            "matches": matches,
        }

    def _resolve(self, handle: str) -> tuple[ToolResultReference, str]:
        normalized_handle = str(handle or "").strip()
        reference = self._references.get(normalized_handle)
        path = self._paths.get(normalized_handle)
        if reference is None or path is None:
            raise ValueError("unknown source handle")
        content = path.read_text(encoding="utf-8")
        actual_digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
        if actual_digest != reference.sha256:
            raise RuntimeError("source content integrity check failed")
        return reference, content

    def _load_index(self) -> None:
        index_path = self._index_path
        if not index_path.is_file():
            return
        try:
            payload = json.loads(index_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        if not isinstance(payload, dict):
            return
        try:
            self._sequence = max(0, int(payload.get("sequence") or 0))
        except (TypeError, ValueError):
            self._sequence = 0
        entries = payload.get("entries")
        if not isinstance(entries, list):
            return
        for raw_entry in entries:
            if not isinstance(raw_entry, dict):
                continue
            handle = str(raw_entry.get("handle") or "").strip()
            tool_name = str(raw_entry.get("tool_name") or "").strip()
            sha256 = str(raw_entry.get("sha256") or "").strip()
            try:
                char_count = int(raw_entry.get("char_count"))
            except (TypeError, ValueError):
                continue
            path = self._root / f"{handle}.json"
            if (
                not _HANDLE_RE.fullmatch(handle)
                or not tool_name
                or len(sha256) != 64
                or char_count < 0
                or not path.is_file()
            ):
                continue
            self._references[handle] = ToolResultReference(
                handle=handle,
                tool_name=tool_name,
                char_count=char_count,
                sha256=sha256,
            )
            self._paths[handle] = path

    def _write_index(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "sequence": self._sequence,
            "entries": [
                {
                    "handle": reference.handle,
                    "tool_name": reference.tool_name,
                    "char_count": reference.char_count,
                    "sha256": reference.sha256,
                }
                for reference in self._references.values()
            ],
        }
        temporary_path = self._root / ".index.tmp"
        temporary_path.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
        )
        temporary_path.replace(self._index_path)

    @property
    def _index_path(self) -> Path:
        return self._root / "index.json"


def _serialize_result(result: Any) -> str | None:
    if isinstance(result, str):
        return result
    try:
        return json.dumps(result, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError):
        return None
