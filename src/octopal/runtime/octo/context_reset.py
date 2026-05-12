from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def estimate_repetition_score(entries: list[Any]) -> float:
    if not entries:
        return 0.0
    sample = entries[:40]
    values = [_normalize_compact(getattr(entry, "content", "")) for entry in sample]
    values = [value for value in values if value]
    if not values:
        return 0.0
    unique = len(set(values))
    return max(0.0, min(1.0, 1.0 - (unique / max(1, len(values)))))


def estimate_error_streak(entries: list[Any]) -> int:
    streak = 0
    for entry in entries[:20]:
        text = _normalize_compact(getattr(entry, "content", ""))
        if not text:
            continue
        if any(token in text for token in ("error", "failed", "exception", "unable", "timeout")):
            streak += 1
            continue
        break
    return streak


def persist_context_reset_files(workspace_dir: Path, handoff: dict[str, Any]) -> dict[str, str]:
    memory_dir = workspace_dir / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    handoff_json_path = memory_dir / "handoff.json"
    handoff_md_path = memory_dir / "handoff.md"
    audit_md_path = memory_dir / "context-audit.md"
    audit_jsonl_path = memory_dir / "context-audit.jsonl"

    handoff_json_path.write_text(
        json.dumps(handoff, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    handoff_md_path.write_text(render_handoff_markdown(handoff), encoding="utf-8")
    append_context_audit_markdown(audit_md_path, handoff)
    with audit_jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(handoff, ensure_ascii=False) + "\n")

    return {
        "handoff_json": str(handoff_json_path),
        "handoff_md": str(handoff_md_path),
        "audit_md": str(audit_md_path),
        "audit_jsonl": str(audit_jsonl_path),
    }


def render_handoff_markdown(handoff: dict[str, Any]) -> str:
    lines = [
        "# Octo Handoff",
        "",
        f"- created_at: {handoff.get('created_at', '')}",
        f"- mode: {handoff.get('mode', 'soft')}",
        f"- reason: {handoff.get('reason', '')}",
        f"- confidence: {handoff.get('confidence', 0.0)}",
        f"- cognitive_state: {handoff.get('cognitive_state', 'focused')}",
        "",
        "## Goal Now",
        handoff.get("goal_now", "") or "-",
        "",
        "## Next Step",
        handoff.get("next_step", "") or "-",
        "",
        "## Current Interest",
        handoff.get("current_interest", "") or "-",
        "",
        "## Pending Human Input",
        handoff.get("pending_human_input", "") or "-",
        "",
        "## Done",
    ]
    done = handoff.get("done") or []
    lines.extend([f"- {item}" for item in done] or ["-"])
    lines.extend(["", "## Open Threads"])
    open_threads = handoff.get("open_threads") or []
    lines.extend([f"- {item}" for item in open_threads] or ["-"])
    lines.extend(["", "## Critical Constraints"])
    constraints = handoff.get("critical_constraints") or []
    lines.extend([f"- {item}" for item in constraints] or ["-"])
    lines.extend(["", "## Health Snapshot"])
    health = handoff.get("health_snapshot") or {}
    for key in (
        "context_size_estimate",
        "repetition_score",
        "error_streak",
        "no_progress_turns",
        "resets_since_progress",
        "overload_score",
    ):
        lines.append(f"- {key}: {health.get(key, 0)}")
    return "\n".join(lines).strip() + "\n"


def append_context_audit_markdown(path: Path, handoff: dict[str, Any]) -> None:
    timestamp = str(handoff.get("created_at", ""))
    mode = str(handoff.get("mode", "soft"))
    reason = str(handoff.get("reason", ""))
    confidence = str(handoff.get("confidence", ""))
    health = handoff.get("health_snapshot") or {}
    section = (
        f"\n## {timestamp} | mode={mode}\n"
        f"- reason: {reason}\n"
        f"- confidence: {confidence}\n"
        f"- context_size_estimate: {health.get('context_size_estimate', 0)}\n"
        f"- repetition_score: {health.get('repetition_score', 0)}\n"
        f"- no_progress_turns: {health.get('no_progress_turns', 0)}\n"
        f"- overload_score: {health.get('overload_score', 0)}\n"
    )
    existing = path.read_text(encoding="utf-8") if path.exists() else "# Context Reset Audit\n"
    path.write_text(existing.rstrip() + section + "\n", encoding="utf-8")


def build_wakeup_message(handoff: dict[str, Any], handoff_path: str) -> str:
    goal_now = str(handoff.get("goal_now", "") or "").strip()
    next_step = str(handoff.get("next_step", "") or "").strip()
    return (
        "You woke up after a context reset.\n"
        f"Handoff goal: {goal_now}\n"
        f"Suggested next step: {next_step}\n"
        f"Handoff file: {handoff_path}\n"
        "Choose one mode now: continue / clarify / replan."
    )


def build_restart_resume_message(resume: dict[str, Any]) -> str:
    handoff = resume.get("handoff") if isinstance(resume.get("handoff"), dict) else {}
    files = resume.get("files") if isinstance(resume.get("files"), dict) else {}
    update_status = resume.get("update") if isinstance(resume.get("update"), dict) else {}
    goal_now = str(handoff.get("goal_now", "") or "").strip()
    next_step = str(handoff.get("next_step", "") or "").strip()
    reason = str(handoff.get("reason", "") or "").strip()
    source = str(handoff.get("source", "") or "").strip()
    handoff_path = str(files.get("handoff_md", "") or "").strip()
    if source == "octo_update_self":
        return (
            "You woke up after a supervised self update and restart.\n"
            f"Update reason: {reason}\n"
            f"Version before update: {update_status.get('local_version') or 'unknown'}\n"
            f"Latest version seen before update: {update_status.get('latest_version') or 'unknown'}\n"
            f"Handoff goal: {goal_now}\n"
            f"Suggested next step: {next_step}\n"
            f"Handoff file: {handoff_path}\n"
            "Check runtime health and control acknowledgements, then tell the user whether update and restart completed."
        )
    return (
        "You woke up after a supervised self restart.\n"
        f"Restart reason: {reason}\n"
        f"Handoff goal: {goal_now}\n"
        f"Suggested next step: {next_step}\n"
        f"Handoff file: {handoff_path}\n"
        "Tell the user briefly that the restart completed, then continue, clarify, or replan."
    )


def _normalize_compact(value: str) -> str:
    lowered = (value or "").strip().lower()
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered
