from __future__ import annotations

from octopal.runtime.temporal_context import format_temporal_context_prompt

_CONTEXT = {
    "local_date": "2026-05-15",
    "local_time": "09:30:00",
    "local_datetime": "2026-05-15T09:30:00+00:00",
    "local_weekday": "Friday",
    "local_timezone": "UTC",
    "utc_date": "2026-05-15",
    "utc_time": "09:30:00",
    "utc_datetime": "2026-05-15T09:30:00+00:00",
    "utc_weekday": "Friday",
}


def test_temporal_context_formats_worker_prompt() -> None:
    prompt = format_temporal_context_prompt(_CONTEXT)

    assert "Temporal context:" in prompt
    assert "Current local date: 2026-05-15" in prompt
    assert "Current UTC datetime: 2026-05-15T09:30:00+00:00" in prompt
    assert "relative dates" in prompt
