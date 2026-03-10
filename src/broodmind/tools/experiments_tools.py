from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from uuid import uuid4

from broodmind.jsonl_guard import read_jsonl_dicts
from broodmind.utils import utc_now


_MAX_TEXT_LEN = 280
_MAX_EVIDENCE_ITEMS = 6


async def queen_experiment_log(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Append a compact Queen self-improvement observation to experiments/results.jsonl."""
    workspace_dir = Path(ctx.get("base_dir") or Path("workspace")).resolve()
    experiments_dir = workspace_dir / "experiments"
    experiments_dir.mkdir(parents=True, exist_ok=True)

    readme_path = experiments_dir / "README.md"
    if not readme_path.exists():
        readme_path.write_text(
            "# Controlled Self-Improvement\n\n"
            "Use this folder for rare, low-risk behavior experiments.\n",
            encoding="utf-8",
        )

    results_path = experiments_dir / "results.jsonl"
    if results_path.exists():
        # Best-effort repair if the file was manually edited into an invalid state.
        read_jsonl_dicts(results_path, repair=True)
    else:
        results_path.write_text("", encoding="utf-8")

    problem = _clean_text(args.get("problem"), required=True, field="problem")
    classification = _clean_enum(
        args.get("classification"),
        allowed={"behavioral", "system", "unclear"},
        default="behavioral",
    )
    source = _clean_enum(
        args.get("source"),
        allowed={"failures", "deliberation_audit", "manual_observation", "self_queue", "worker_result"},
        default="manual_observation",
    )
    status = _clean_enum(
        args.get("status"),
        allowed={"observed", "proposed", "kept", "discarded"},
        default="observed",
    )
    evidence = _clean_evidence(args.get("evidence"))
    change_summary = _clean_text(args.get("change_summary"), required=False, field="change_summary")
    notes = _clean_text(args.get("notes"), required=False, field="notes")

    entry_id = f"{utc_now().strftime('%Y%m%dT%H%M%SZ')}_{_slugify(problem)[:32]}_{uuid4().hex[:6]}"
    entry = {
        "id": entry_id,
        "ts": utc_now().isoformat(),
        "classification": classification,
        "source": source,
        "status": status,
        "problem": problem,
        "evidence": evidence,
    }
    if change_summary:
        entry["change_summary"] = change_summary
    if notes:
        entry["notes"] = notes

    with results_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False))
        handle.write("\n")

    return f"Experiment entry logged: {entry_id}"


def _clean_text(value: Any, *, required: bool, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        if required:
            raise ValueError(f"{field} is required")
        return ""
    text = re.sub(r"\s+", " ", text)
    return text[:_MAX_TEXT_LEN]


def _clean_enum(value: Any, *, allowed: set[str], default: str) -> str:
    candidate = str(value or "").strip().lower()
    return candidate if candidate in allowed else default


def _clean_evidence(value: Any) -> list[str]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    cleaned: list[str] = []
    for raw in items[:_MAX_EVIDENCE_ITEMS]:
        text = _clean_text(raw, required=False, field="evidence")
        if text:
            cleaned.append(text)
    return cleaned


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "experiment"
