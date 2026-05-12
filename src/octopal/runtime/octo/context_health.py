from __future__ import annotations

from typing import Any

from octopal.runtime.octo.runtime_config import _env_float, _env_int

_RESET_CONFIRM_THRESHOLD = 2
_RESET_CONFIDENCE_MIN = 0.7

_WATCH_THRESHOLDS = {
    "context_size_estimate": _env_int("OCTOPAL_CONTEXT_WATCH_SIZE", 150000, minimum=5000),
    "repetition_score": _env_float(
        "OCTOPAL_CONTEXT_WATCH_REPETITION", 0.65, minimum=0.0, maximum=1.0
    ),
    "error_streak": _env_int("OCTOPAL_CONTEXT_WATCH_ERROR_STREAK", 3, minimum=1),
    "no_progress_turns": _env_int("OCTOPAL_CONTEXT_WATCH_NO_PROGRESS", 4, minimum=1),
}
_RESET_SOON_THRESHOLDS = {
    "context_size_estimate": _env_int("OCTOPAL_CONTEXT_RESET_SOON_SIZE", 250000, minimum=5000),
    "repetition_score": _env_float(
        "OCTOPAL_CONTEXT_RESET_SOON_REPETITION", 0.75, minimum=0.0, maximum=1.0
    ),
    "error_streak": _env_int("OCTOPAL_CONTEXT_RESET_SOON_ERROR_STREAK", 5, minimum=1),
    "no_progress_turns": _env_int("OCTOPAL_CONTEXT_RESET_SOON_NO_PROGRESS", 7, minimum=1),
}

# Keep RESET_SOON at or above WATCH thresholds, even with custom env values.
_RESET_SOON_THRESHOLDS["context_size_estimate"] = max(
    int(_RESET_SOON_THRESHOLDS["context_size_estimate"]),
    int(_WATCH_THRESHOLDS["context_size_estimate"]),
)
_RESET_SOON_THRESHOLDS["repetition_score"] = max(
    float(_RESET_SOON_THRESHOLDS["repetition_score"]),
    float(_WATCH_THRESHOLDS["repetition_score"]),
)
_RESET_SOON_THRESHOLDS["error_streak"] = max(
    int(_RESET_SOON_THRESHOLDS["error_streak"]),
    int(_WATCH_THRESHOLDS["error_streak"]),
)
_RESET_SOON_THRESHOLDS["no_progress_turns"] = max(
    int(_RESET_SOON_THRESHOLDS["no_progress_turns"]),
    int(_WATCH_THRESHOLDS["no_progress_turns"]),
)


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _coerce_int(
    value: Any,
    default: int,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    try:
        result = int(value)
    except Exception:
        result = default
    if minimum is not None:
        result = max(minimum, result)
    if maximum is not None:
        result = min(maximum, result)
    return result


def _watch_conditions(
    *,
    context_size_estimate: int,
    repetition_score: float,
    error_streak: int,
    no_progress_turns: int,
) -> list[bool]:
    return [
        context_size_estimate >= int(_WATCH_THRESHOLDS["context_size_estimate"]),
        repetition_score >= float(_WATCH_THRESHOLDS["repetition_score"]),
        error_streak >= int(_WATCH_THRESHOLDS["error_streak"]),
        no_progress_turns >= int(_WATCH_THRESHOLDS["no_progress_turns"]),
    ]


def _is_reset_soon_severe(
    *,
    context_size_estimate: int,
    repetition_score: float,
    error_streak: int,
    no_progress_turns: int,
) -> bool:
    return (
        context_size_estimate >= int(_RESET_SOON_THRESHOLDS["context_size_estimate"])
        or repetition_score >= float(_RESET_SOON_THRESHOLDS["repetition_score"])
        or error_streak >= int(_RESET_SOON_THRESHOLDS["error_streak"])
        or no_progress_turns >= int(_RESET_SOON_THRESHOLDS["no_progress_turns"])
    )


def _is_progress_reply(current_norm: str, prior_norm: str) -> bool:
    if not current_norm:
        return False
    if current_norm == prior_norm:
        return False
    if len(current_norm) < 24:
        return False
    stalled_markers = (
        "please try again",
        "i cannot",
        "i can't",
        "unable to",
        "still working on it",
        "no update",
    )
    return not any(marker in current_norm for marker in stalled_markers)
