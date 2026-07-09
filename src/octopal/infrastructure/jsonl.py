from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class JsonlRepairReport:
    repaired: bool
    dropped_lines: int
    backup_path: str | None = None


def read_jsonl_dicts(
    path: Path, *, repair: bool = True
) -> tuple[list[dict[str, Any]], JsonlRepairReport]:
    if not path.exists():
        return [], JsonlRepairReport(repaired=False, dropped_lines=0)

    text = path.read_text(encoding="utf-8", errors="ignore")
    rows: list[dict[str, Any]] = []
    dropped = 0

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except Exception:
            dropped += 1
            continue
        if isinstance(item, dict):
            rows.append(item)
        else:
            dropped += 1

    if dropped == 0 or not repair:
        return rows, JsonlRepairReport(repaired=False, dropped_lines=dropped)

    backup = f"{path}.bak-{time.time_ns()}"
    tmp = path.with_suffix(f"{path.suffix}.tmp")
    payload = "".join(json.dumps(item, ensure_ascii=False) + "\n" for item in rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.replace(backup)
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(path)
    return rows, JsonlRepairReport(repaired=True, dropped_lines=dropped, backup_path=backup)
