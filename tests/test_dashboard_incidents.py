from __future__ import annotations

from datetime import UTC, datetime

from octopal.gateway.dashboard import _build_incidents
from octopal.infrastructure.store.models import WorkerRecord


def _worker(worker_id: str, status: str) -> WorkerRecord:
    now = datetime.now(UTC)
    return WorkerRecord(
        id=worker_id,
        status=status,
        task="task",
        granted_caps=[],
        created_at=now,
        updated_at=now,
    )


def test_build_incidents_prioritizes_critical_entries() -> None:
    services = [
        {
            "id": "gateway",
            "name": "Gateway",
            "status": "critical",
            "reason": "process is not running",
            "updated_at": "2026-03-01T00:00:00+00:00",
        },
        {
            "id": "octo",
            "name": "Octo",
            "status": "warning",
            "reason": "queue pressure rising",
            "updated_at": "2026-03-01T00:00:05+00:00",
        },
    ]
    workers = [_worker("w1", "failed"), _worker("w2", "failed"), _worker("w3", "completed")]
    logs = [
        {
            "service": "gateway",
            "level": "error",
            "event": "gateway timeout to provider",
            "timestamp": "2026-03-01T00:00:10+00:00",
        },
        {
            "service": "gateway",
            "level": "error",
            "event": "gateway timeout to provider",
            "timestamp": "2026-03-01T00:00:12+00:00",
        },
    ]

    result = _build_incidents(
        services=services,
        recent_workers=workers,
        logs=logs,
        control_pending=0,
        queue_depth=5,
    )
    assert result["summary"]["open"] >= 1
    assert result["summary"]["critical"] >= 1
    top = result["items"][0]
    assert top["severity"] == "critical"


def test_build_incidents_limits_to_top_five() -> None:
    services = [
        {"id": f"s{i}", "name": f"S{i}", "status": "warning", "reason": "warn", "updated_at": ""}
        for i in range(10)
    ]
    result = _build_incidents(
        services=services,
        recent_workers=[],
        logs=[],
        control_pending=0,
        queue_depth=0,
    )
    assert len(result["items"]) == 5
