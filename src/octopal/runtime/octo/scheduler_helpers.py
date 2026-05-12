from __future__ import annotations

from typing import Any


def _empty_scheduler_metric_counters() -> dict[str, int]:
    return {
        "ticks_total": 0,
        "failures_total": 0,
        "started_total": 0,
        "completed_total": 0,
        "duplicates_total": 0,
        "rejected_by_policy_total": 0,
        "errors_total": 0,
    }


def _coerce_positive_chat_id(value: Any) -> int | None:
    try:
        chat_id = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return chat_id if chat_id > 0 else None
