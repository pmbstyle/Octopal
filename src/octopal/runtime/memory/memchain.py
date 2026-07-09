from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

TRACKED_RELATIVE_PATHS = [
    "memory/canon/facts.md",
    "memory/canon/decisions.md",
    "memory/canon/failures.md",
    "MEMORY.md",
    "SOUL.md",
    "USER.md",
    "config/mcp.json",
]


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _read_text_or_missing(path: Path) -> tuple[bool, str]:
    if not path.exists() or not path.is_file():
        return False, ""
    try:
        return True, path.read_text(encoding="utf-8")
    except Exception:
        return False, ""


def _tracked_file_hashes(workspace_dir: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for rel in TRACKED_RELATIVE_PATHS:
        full = workspace_dir / rel
        exists, content = _read_text_or_missing(full)
        payload = f"exists={int(exists)}\n{content}" if exists else "exists=0\n"
        hashes[rel] = _sha256_text(payload)
    return hashes


def _snapshot_hash(file_hashes: dict[str, str]) -> str:
    canonical = json.dumps(file_hashes, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return _sha256_text(canonical)


def _chain_files(workspace_dir: Path) -> tuple[Path, Path]:
    memory_dir = workspace_dir / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    return memory_dir / "memchain.jsonl", memory_dir / "memchain_head.txt"


@dataclass
class MemChainVerifyResult:
    status: str
    message: str
    entries: int
    head_hash: str
    broken_at: int | None = None
    changed_files: list[str] | None = None


def _iter_chain_entries(chain_path: Path) -> list[dict[str, Any]]:
    if not chain_path.exists():
        return []
    entries: list[dict[str, Any]] = []
    for line in chain_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                entries.append(obj)
        except Exception:
            continue
    return entries


def _compute_entry_hash(entry: dict[str, Any]) -> str:
    payload = {
        "index": int(entry.get("index", 0)),
        "ts": str(entry.get("ts", "")),
        "prev_hash": str(entry.get("prev_hash", "")),
        "snapshot_hash": str(entry.get("snapshot_hash", "")),
        "file_hashes": entry.get("file_hashes", {}),
        "reason": str(entry.get("reason", "")),
        "meta": entry.get("meta", {}),
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return _sha256_text(canonical)


def memchain_init(workspace_dir: Path, *, force: bool = False) -> dict[str, Any]:
    chain_path, head_path = _chain_files(workspace_dir)
    if force:
        chain_path.write_text("", encoding="utf-8")
        head_path.write_text("", encoding="utf-8")
    if chain_path.exists() and chain_path.stat().st_size > 0 and not force:
        status = memchain_status(workspace_dir)
        return {"status": "exists", **status}
    return memchain_record(workspace_dir, reason="init", meta={"source": "cli"})


def memchain_record(
    workspace_dir: Path, *, reason: str = "manual", meta: dict[str, Any] | None = None
) -> dict[str, Any]:
    chain_path, head_path = _chain_files(workspace_dir)
    entries = _iter_chain_entries(chain_path)
    prev_hash = str(entries[-1].get("entry_hash", "")) if entries else ""
    file_hashes = _tracked_file_hashes(workspace_dir)
    snapshot_hash = _snapshot_hash(file_hashes)
    entry = {
        "index": len(entries) + 1,
        "ts": _utc_now_iso(),
        "prev_hash": prev_hash,
        "snapshot_hash": snapshot_hash,
        "file_hashes": file_hashes,
        "reason": reason,
        "meta": meta or {},
    }
    entry["entry_hash"] = _compute_entry_hash(entry)
    with chain_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
    head_path.write_text(str(entry["entry_hash"]), encoding="utf-8")
    return {
        "status": "ok",
        "entries": len(entries) + 1,
        "head_hash": str(entry["entry_hash"]),
        "snapshot_hash": snapshot_hash,
        "chain_path": str(chain_path),
        "head_path": str(head_path),
    }


def memchain_verify(workspace_dir: Path) -> MemChainVerifyResult:
    chain_path, head_path = _chain_files(workspace_dir)
    entries = _iter_chain_entries(chain_path)
    if not entries:
        return MemChainVerifyResult(
            status="missing",
            message="memchain is not initialized",
            entries=0,
            head_hash="",
        )

    prev = ""
    for idx, entry in enumerate(entries, start=1):
        if str(entry.get("prev_hash", "")) != prev:
            return MemChainVerifyResult(
                status="broken",
                message="prev_hash mismatch",
                entries=len(entries),
                head_hash=str(entries[-1].get("entry_hash", "")),
                broken_at=idx,
            )
        expected = _compute_entry_hash(entry)
        actual = str(entry.get("entry_hash", ""))
        if expected != actual:
            return MemChainVerifyResult(
                status="broken",
                message="entry_hash mismatch",
                entries=len(entries),
                head_hash=str(entries[-1].get("entry_hash", "")),
                broken_at=idx,
            )
        prev = actual

    head_disk = head_path.read_text(encoding="utf-8").strip() if head_path.exists() else ""
    if head_disk and head_disk != prev:
        return MemChainVerifyResult(
            status="broken",
            message="head hash mismatch",
            entries=len(entries),
            head_hash=prev,
        )

    latest = entries[-1]
    current_hashes = _tracked_file_hashes(workspace_dir)
    current_snapshot = _snapshot_hash(current_hashes)
    changed_files = [
        rel
        for rel, h in current_hashes.items()
        if h != str((latest.get("file_hashes") or {}).get(rel, ""))
    ]
    if current_snapshot != str(latest.get("snapshot_hash", "")):
        return MemChainVerifyResult(
            status="drift",
            message="tracked files changed since last record",
            entries=len(entries),
            head_hash=prev,
            changed_files=changed_files,
        )

    return MemChainVerifyResult(
        status="ok",
        message="chain verified",
        entries=len(entries),
        head_hash=prev,
    )


def memchain_status(workspace_dir: Path) -> dict[str, Any]:
    result = memchain_verify(workspace_dir)
    return {
        "status": result.status,
        "message": result.message,
        "entries": result.entries,
        "head_hash": result.head_hash,
        "broken_at": result.broken_at,
        "changed_files": result.changed_files or [],
    }
