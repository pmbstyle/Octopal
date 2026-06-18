from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from octopal.runtime.octo.delivery import resolve_user_delivery
from octopal.runtime.octo.tool_selection import _DURABLE_WORKSPACE_ROOTS
from octopal.runtime.worker_result_payloads import (
    ROUTE_WORKER_OUTPUT_CONTEXT_BUDGET,
    summarize_worker_output_for_context,
)
from octopal.runtime.workers.contracts import WorkerResult

_LEGACY_WORKER_ARTIFACT_KEYS = ("report_path", "output_path", "path", "file")


@dataclass(frozen=True)
class _WorkerArtifactSummary:
    durable_paths: list[str]
    scratch_paths: list[str]
    primary_report_path: str | None
    unsafe_legacy_paths: list[str]

    @property
    def has_user_visible_artifact(self) -> bool:
        return bool(self.primary_report_path or self.durable_paths)

    def to_payload(self) -> dict[str, Any]:
        return {
            "durable_paths": list(self.durable_paths),
            "scratch_paths": list(self.scratch_paths),
            "primary_report_path": self.primary_report_path,
            "unsafe_legacy_paths": list(self.unsafe_legacy_paths),
            "has_user_visible_artifact": self.has_user_visible_artifact,
        }


def _normalize_worker_result_entry(
    item: tuple[str, WorkerResult] | tuple[str, str, WorkerResult],
) -> tuple[str, str, WorkerResult]:
    if len(item) == 2:
        task_text, result = item
        return "", task_text, result
    worker_id, task_text, result = item
    return str(worker_id or "").strip(), task_text, result


def _normalize_worker_artifact_path(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    raw = value.strip().replace("\\", "/")
    if not raw or "\x00" in raw:
        return None
    while raw.startswith("./"):
        raw = raw[2:]
    return raw.strip() or None


def _extract_worker_artifact_paths(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    seen: set[str] = set()
    paths: list[str] = []
    for item in value:
        normalized = _normalize_worker_artifact_path(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        paths.append(normalized)
    return paths


def _is_durable_workspace_artifact_path(path: str) -> bool:
    normalized = _normalize_worker_artifact_path(path)
    if not normalized:
        return False

    workspace_root = Path(os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")).resolve()
    candidate = Path(normalized)
    if candidate.is_absolute():
        try:
            relative = candidate.resolve(strict=False).relative_to(workspace_root)
        except ValueError:
            return False
        parts = relative.parts
    else:
        parts = tuple(part for part in Path(normalized).parts if part not in ("", "."))

    return bool(parts) and parts[0] in _DURABLE_WORKSPACE_ROOTS


def _summarize_worker_artifacts(result: WorkerResult) -> _WorkerArtifactSummary:
    output = result.output if isinstance(result.output, dict) else {}
    durable_paths = [
        path
        for path in _extract_worker_artifact_paths(output.get("durable_paths"))
        if _is_durable_workspace_artifact_path(path)
    ]
    scratch_paths = _extract_worker_artifact_paths(output.get("scratch_paths"))

    report_path = _normalize_worker_artifact_path(output.get("report_path"))
    primary_report_path: str | None = None
    if report_path and (
        report_path in durable_paths
        or (Path(report_path).is_absolute() and _is_durable_workspace_artifact_path(report_path))
    ):
        primary_report_path = report_path
        if report_path not in durable_paths:
            durable_paths.append(report_path)
    elif durable_paths:
        primary_report_path = durable_paths[0]

    unsafe_legacy_paths: list[str] = []
    for key in _LEGACY_WORKER_ARTIFACT_KEYS:
        normalized = _normalize_worker_artifact_path(output.get(key))
        if normalized and normalized not in durable_paths and normalized not in unsafe_legacy_paths:
            unsafe_legacy_paths.append(normalized)
    for item in _extract_worker_artifact_paths(output.get("files")):
        if item not in durable_paths and item not in unsafe_legacy_paths:
            unsafe_legacy_paths.append(item)

    return _WorkerArtifactSummary(
        durable_paths=durable_paths,
        scratch_paths=scratch_paths,
        primary_report_path=primary_report_path,
        unsafe_legacy_paths=unsafe_legacy_paths,
    )


def _build_worker_result_payload(worker_id: str, task_text: str, result: WorkerResult) -> dict[str, Any]:
    artifact_summary = _summarize_worker_artifacts(result)
    output_context = summarize_worker_output_for_context(
        result.output,
        budget=ROUTE_WORKER_OUTPUT_CONTEXT_BUDGET,
    )

    payload = {
        "status": result.status,
        "worker_id": worker_id,
        "task": task_text,
        "summary": result.summary,
        "output": output_context.output,
        "output_preview_text": output_context.output_preview_text,
        "output_truncated": output_context.output_truncated,
        "available_keys": output_context.available_keys,
        "output_chars": output_context.output_chars,
        "artifact_summary": artifact_summary.to_payload(),
        "questions": result.questions,
        "knowledge_proposals": [p.model_dump() for p in result.knowledge_proposals],
        "tools_used": result.tools_used,
    }
    return payload


def should_send_worker_followup(text: str) -> bool:
    """Determine if a worker follow-up should be sent to the user."""
    return resolve_user_delivery(text).user_visible


def should_force_worker_followup(result: WorkerResult) -> bool:
    """Return True when a completed worker result is substantive enough to surface."""
    summary = (result.summary or "").strip()
    if not summary:
        return False

    artifact_summary = _summarize_worker_artifacts(result)

    if len(summary) >= 160:
        return True

    if result.questions or result.knowledge_proposals:
        return True

    if len(result.tools_used or []) >= 2:
        return True

    if artifact_summary.has_user_visible_artifact:
        return True

    output = result.output
    if isinstance(output, dict):
        interesting_keys = {"report", "results", "items", "jobs", "posts", "articles"}
        if interesting_keys.intersection(output.keys()):
            return True

    return False


def build_forced_worker_followup(result: WorkerResult) -> str:
    """Build a concise Octo-style fallback when routing suppresses a useful update."""
    lead = _build_generic_worker_completion_message(result)
    if lead == "Task finished.":
        return ""

    if len(lead) > 700:
        lead = lead[:697].rstrip() + "..."

    parts = [lead]
    if result.questions:
        questions = [q.strip() for q in result.questions[:3] if q and q.strip()]
        if questions:
            parts.append("\n".join(f"- {question}" for question in questions))
    return "\n\n".join(parts).strip()


def _build_generic_worker_completion_message(result: WorkerResult) -> str:
    artifact_summary = _summarize_worker_artifacts(result)
    if artifact_summary.primary_report_path:
        return f"Task finished. Output is ready in `{artifact_summary.primary_report_path}`."
    if len(artifact_summary.durable_paths) >= 2:
        return f"Task finished. Created {len(artifact_summary.durable_paths)} durable file(s)."
    if result.questions:
        return "Task finished. I need your input on the next step."
    return "Task finished."
