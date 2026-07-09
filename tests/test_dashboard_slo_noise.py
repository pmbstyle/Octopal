from __future__ import annotations

from datetime import UTC, datetime, timedelta

from octopal.gateway.dashboard import _build_noise_control, _build_slo_metrics
from octopal.infrastructure.store.models import WorkerRecord


def _worker(
    worker_id: str, status: str, updated_at: datetime, template_id: str = "coder"
) -> WorkerRecord:
    return WorkerRecord(
        id=worker_id,
        status=status,
        task="task",
        granted_caps=[],
        created_at=updated_at - timedelta(minutes=5),
        updated_at=updated_at,
        template_id=template_id,
    )


def test_build_slo_metrics_produces_expected_keys() -> None:
    now = datetime.now(UTC)
    services = [
        {"id": "gateway", "status": "ok"},
        {"id": "octo", "status": "warning"},
        {"id": "telegram", "status": "ok"},
        {"id": "exec_run", "status": "ok"},
    ]
    workers = [
        _worker("f1", "failed", now),
        _worker("c1", "completed", now + timedelta(minutes=10)),
    ]
    slo = _build_slo_metrics(
        active_channel="telegram",
        services=services,
        log_health={"error_rate_5m": 0.015},
        recent_workers=workers,
    )
    assert "uptime_pct" in slo
    assert "burn_rate" in slo
    assert "mttr_minutes" in slo
    assert slo["burn_rate"]["value"] > 1.0


def test_build_noise_control_reduces_duplicates() -> None:
    logs = [
        {"service": "gateway", "level": "error", "event": "timeout to provider"},
        {"service": "gateway", "level": "error", "event": "timeout to provider"},
        {"service": "gateway", "level": "warning", "event": "queue backlog high"},
        {"service": "octo", "level": "warning", "event": "queue backlog high"},
    ]
    noise = _build_noise_control(logs=logs)
    assert noise["raw_alerts"] == 4
    assert noise["deduped_alerts"] < noise["raw_alerts"]
    assert noise["reduction_pct"] > 0
