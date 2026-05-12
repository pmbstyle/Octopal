from __future__ import annotations

from typing import Any


def _normalize_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        items = [chunk.strip() for chunk in value.split("\n")]
    else:
        return []
    normalized: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            normalized.append(text)
    return normalized[:20]


def _serialize_worker_record(worker_record: Any) -> dict[str, Any] | None:
    if worker_record is None:
        return None
    if hasattr(worker_record, "model_dump"):
        try:
            return worker_record.model_dump(mode="json")
        except TypeError:
            return worker_record.model_dump()
    if isinstance(worker_record, dict):
        return dict(worker_record)
    return None


def _is_active_worker_status(status: Any) -> bool:
    return str(status or "").strip().lower() in {
        "started",
        "running",
        "waiting_for_children",
        "awaiting_instruction",
    }
