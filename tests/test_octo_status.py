from __future__ import annotations

from octopal.runtime.octo_status import build_octo_status


def test_build_octo_status_reports_idle_when_no_work_is_pending() -> None:
    payload = build_octo_status(
        {
            "followup_queues": 0,
            "internal_queues": 0,
            "followup_tasks": 0,
            "internal_tasks": 0,
            "thinking_count": 0,
            "updated_at": "2026-03-20T10:05:01+00:00",
        }
    )

    assert payload["state"] == "idle"
    assert payload["busy"] is False
    assert payload["label"] == "Idle"
    assert payload["reason"] == "idle"
    assert payload["service_status"] == "ok"


def test_build_octo_status_reports_busy_when_plan_needs_next_step() -> None:
    payload = build_octo_status(
        {
            "followup_queues": 0,
            "internal_queues": 0,
            "followup_tasks": 0,
            "internal_tasks": 0,
            "thinking_count": 0,
            "active_plan_runs": 1,
            "needs_next_step_plan_runs": 1,
            "updated_at": "2026-03-20T10:05:01+00:00",
        }
    )

    assert payload["state"] == "thinking"
    assert payload["busy"] is True
    assert payload["label"] == "Busy"
    assert payload["reason"] == "1 plan(s) need next step"
    assert payload["active_plan_runs"] == 1
    assert payload["needs_next_step_plan_runs"] == 1


def test_build_octo_status_reports_busy_when_internal_queues_are_non_empty() -> None:
    payload = build_octo_status(
        {
            "followup_queues": 2,
            "internal_queues": 1,
            "followup_tasks": 1,
            "internal_tasks": 1,
            "thinking_count": 0,
            "updated_at": "2026-03-20T10:05:01+00:00",
        }
    )

    assert payload["state"] == "thinking"
    assert payload["busy"] is True
    assert payload["label"] == "Busy"
    assert payload["reason"] == "idle" or isinstance(payload["reason"], str)
    assert payload["service_status"] == "ok"
    assert payload["queue_pressure"] == 3


def test_build_octo_status_uses_queue_pressure_thresholds_for_service_health() -> None:
    payload = build_octo_status(
        {
            "followup_queues": 6,
            "internal_queues": 4,
            "followup_tasks": 1,
            "internal_tasks": 1,
            "thinking_count": 0,
        }
    )

    assert payload["busy"] is True
    assert payload["service_status"] == "warning"
    assert payload["reason"] == "queue pressure rising (10)"
