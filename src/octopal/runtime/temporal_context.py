from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from octopal.utils import utc_now


def build_temporal_context(now: datetime | None = None) -> dict[str, Any]:
    utc_dt = now or utc_now()
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=UTC)
    utc_dt = utc_dt.astimezone(UTC)
    local_dt = utc_dt.astimezone()
    timezone_name = local_dt.tzname() or local_dt.strftime("%z") or "local"
    return {
        "current_date": local_dt.date().isoformat(),
        "current_time": local_dt.strftime("%H:%M:%S"),
        "current_datetime": local_dt.isoformat(),
        "current_weekday": local_dt.strftime("%A"),
        "local_date": local_dt.date().isoformat(),
        "local_time": local_dt.strftime("%H:%M:%S"),
        "local_datetime": local_dt.isoformat(),
        "local_weekday": local_dt.strftime("%A"),
        "local_timezone": timezone_name,
        "utc_date": utc_dt.date().isoformat(),
        "utc_time": utc_dt.strftime("%H:%M:%S"),
        "utc_datetime": utc_dt.isoformat(),
        "utc_weekday": utc_dt.strftime("%A"),
    }


def format_temporal_context_prompt(context: Mapping[str, Any] | None = None) -> str:
    ctx = dict(context or build_temporal_context())
    return (
        "Temporal context:\n"
        f"- Current local date: {ctx['local_date']} ({ctx['local_weekday']})\n"
        f"- Current local time: {ctx['local_time']} ({ctx['local_timezone']})\n"
        f"- Current local datetime: {ctx['local_datetime']}\n"
        f"- Current UTC datetime: {ctx['utc_datetime']}\n"
        "- Treat relative dates like today, yesterday, tomorrow, this morning, and this "
        "week relative to this context unless the task gives a more specific date."
    )

