from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

from octopal.gateway.dashboard import (
    _clear_control_queue_requests,
    _execute_dashboard_action,
    _select_retry_target,
)
from octopal.infrastructure.store.models import WorkerRecord
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.self_control import SELF_UPDATE_ACTION, SELF_UPDATE_REQUESTED_BY


def _worker(worker_id: str, status: str) -> WorkerRecord:
    now = datetime.now(UTC)
    return WorkerRecord(
        id=worker_id,
        status=status,
        task="demo task",
        granted_caps=[],
        created_at=now,
        updated_at=now,
    )


def test_select_retry_target_prefers_requested_failed_worker() -> None:
    workers = [_worker("w1", "failed"), _worker("w2", "failed")]
    picked = _select_retry_target(workers, requested_worker_id="w2")
    assert picked is not None
    assert picked.id == "w2"


def test_select_retry_target_requires_requested_worker_id() -> None:
    workers = [_worker("w1", "failed"), _worker("w2", "failed")]
    assert _select_retry_target(workers, requested_worker_id=None) is None


def test_clear_control_queue_requests_acks_only_pending(tmp_path) -> None:
    state_dir = tmp_path
    req_file = state_dir / "control_requests.jsonl"
    ack_file = state_dir / "control_acks.jsonl"
    req_file.write_text(
        "\n".join(
            [
                json.dumps({"request_id": "r1", "action": "restart_service"}),
                json.dumps({"request_id": "r2", "action": "reload_config"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    ack_file.write_text(json.dumps({"request_id": "r1", "status": "ok"}) + "\n", encoding="utf-8")

    cleared = _clear_control_queue_requests(state_dir, actor="tester")
    assert cleared == 1

    lines = [line for line in ack_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 2
    last = json.loads(lines[-1])
    assert last["request_id"] == "r2"
    assert last["status"] == "cleared"


def test_dashboard_request_self_update_appends_control_request(tmp_path, monkeypatch) -> None:
    class Settings:
        state_dir = tmp_path / "data"
        workspace_dir = tmp_path / "workspace"

    monkeypatch.setattr(
        "octopal.gateway.dashboard._dashboard_update_status",
        lambda: {
            "status": "ok",
            "local_version": "2026.05.03",
            "latest_version": "2026.05.04",
            "update_available": True,
            "can_update": True,
        },
    )

    settings = Settings()
    store = SQLiteStore(settings)
    result = asyncio.run(
        _execute_dashboard_action(
            app=object(),
            settings=settings,
            store=store,
            action="request_self_update",
            worker_id=None,
            reason="apply latest release",
            requested_by="dashboard",
        )
    )

    assert result["status"] == "ok"
    assert result["request"]["action"] == SELF_UPDATE_ACTION
    assert result["request"]["requested_by"] == SELF_UPDATE_REQUESTED_BY

    req_file = settings.state_dir / "control_requests.jsonl"
    lines = [line for line in req_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 1
    request = json.loads(lines[0])
    assert request["metadata"]["source"] == "dashboard"
    assert request["metadata"]["update"]["latest_version"] == "2026.05.04"
