from __future__ import annotations

import hashlib
import json
import os
from typing import Any

_DEFAULT_TOOL_LOOP_WARNING_THRESHOLD = 8
_DEFAULT_TOOL_LOOP_CRITICAL_THRESHOLD = 12
_DEFAULT_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD = 30


def _stable_json(value: Any) -> str:
    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":")
        )
    except Exception:
        return repr(value)


def _hash_tool_call(tool_name: str, params: Any) -> str:
    payload = f"{(tool_name or '').strip().lower()}:{_stable_json(params)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _hash_tool_outcome(result: Any, meta: dict[str, Any]) -> str:
    payload = {
        "result": result,
        "timed_out": bool(meta.get("timed_out")),
        "had_error": bool(meta.get("had_error")),
    }
    return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()


def _tool_no_progress_streak(
    history: list[dict[str, str]],
    *,
    tool_name: str,
    args_hash: str,
) -> tuple[int, str | None]:
    streak = 0
    latest_result_hash: str | None = None
    for record in reversed(history):
        if record.get("tool_name") != tool_name or record.get("args_hash") != args_hash:
            continue
        record_result = record.get("result_hash")
        if not record_result:
            continue
        if latest_result_hash is None:
            latest_result_hash = record_result
            streak = 1
            continue
        if record_result != latest_result_hash:
            break
        streak += 1
    return streak, latest_result_hash


def _detect_tool_loop(
    history: list[dict[str, str]],
    *,
    tool_name: str,
    args_hash: str,
    warning_threshold: int = _DEFAULT_TOOL_LOOP_WARNING_THRESHOLD,
    critical_threshold: int = _DEFAULT_TOOL_LOOP_CRITICAL_THRESHOLD,
    global_breaker_threshold: int = _DEFAULT_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD,
    global_breaker_count: int | None = None,
) -> dict[str, Any] | None:
    effective_count = (
        int(global_breaker_count) if isinstance(global_breaker_count, int) else len(history)
    )
    if effective_count >= global_breaker_threshold:
        return {
            "detector": "global_circuit_breaker",
            "level": "critical",
            "count": effective_count,
            "message": "Too many tool calls in one run without completion.",
        }

    streak, result_hash = _tool_no_progress_streak(
        history, tool_name=tool_name, args_hash=args_hash
    )
    if result_hash is None:
        return None
    if streak >= critical_threshold:
        return {
            "detector": "known_poll_no_progress",
            "level": "critical",
            "count": streak,
            "message": f"Repeated '{tool_name}' calls with no progress.",
        }
    if streak >= warning_threshold:
        return {
            "detector": "known_poll_no_progress",
            "level": "warning",
            "count": streak,
            "message": f"Potential tool loop detected for '{tool_name}'.",
        }
    return None


def _resolve_tool_loop_thresholds() -> dict[str, int]:
    warning = _parse_positive_int_env(
        "OCTOPAL_TOOL_LOOP_WARNING_THRESHOLD",
        _DEFAULT_TOOL_LOOP_WARNING_THRESHOLD,
    )
    critical = _parse_positive_int_env(
        "OCTOPAL_TOOL_LOOP_CRITICAL_THRESHOLD",
        _DEFAULT_TOOL_LOOP_CRITICAL_THRESHOLD,
    )
    global_breaker = _parse_positive_int_env(
        "OCTOPAL_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD",
        _DEFAULT_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD,
    )
    if critical <= warning:
        critical = warning + 1
    if global_breaker <= critical:
        global_breaker = critical + 1
    return {
        "warning": warning,
        "critical": critical,
        "global_breaker": global_breaker,
    }


def _parse_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default
