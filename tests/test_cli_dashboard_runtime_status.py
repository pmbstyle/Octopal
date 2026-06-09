from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from octopal.cli import main as cli_main


class _EmptyStore:
    def get_active_workers(self, older_than_minutes: int = 5) -> list:
        return []

    def list_recent_workers(self, limit: int) -> list:
        return []

    def count_workers_created_since(self, _since) -> int:
        return 0


class _StoreWithPlanBoundWorker(_EmptyStore):
    def __init__(self) -> None:
        self.worker = SimpleNamespace(
            id="worker-plan-1",
            status="running",
            task="Patch parser",
            updated_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
            created_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
            summary="",
            error="",
            tools_used=[],
        )
        self.step = SimpleNamespace(
            run_id="plan-1",
            step_id="patch",
            status="awaiting_worker",
            title="Patch parser",
            kind="worker",
        )

    def get_active_workers(self, older_than_minutes: int = 5) -> list:
        return [self.worker]

    def list_recent_workers(self, limit: int) -> list:
        return [self.worker]

    def count_workers_created_since(self, _since) -> int:
        return 1

    def get_plan_step_by_worker_run_id(self, worker_id: str):
        if worker_id == self.worker.id:
            return self.step
        return None


def test_dashboard_snapshot_uses_discovered_runtime_pid_when_status_pid_is_stale(
    monkeypatch, tmp_path
) -> None:
    settings = SimpleNamespace(state_dir=tmp_path, user_channel="telegram")
    launcher = SimpleNamespace(
        configured_launcher="same_env",
        effective_launcher="same_env",
        available=True,
        reason="",
    )

    monkeypatch.setattr(
        cli_main,
        "read_status",
        lambda _settings: {
            "pid": 111,
            "started_at": "2026-01-01T00:00:00+00:00",
            "active_channel": "Telegram",
        },
    )
    monkeypatch.setattr(cli_main, "is_octopal_runtime_pid", lambda pid: False)
    monkeypatch.setattr(cli_main, "list_octopal_runtime_pids", lambda: [222])
    monkeypatch.setattr(cli_main, "read_metrics_snapshot", lambda _state_dir: {})
    monkeypatch.setattr(cli_main, "get_worker_launcher_status", lambda _settings: launcher)

    snapshot = cli_main._build_dashboard_snapshot(settings, last=8, store=_EmptyStore())

    assert snapshot["system"]["running"] is True
    assert snapshot["system"]["pid"] == 222


def test_dashboard_snapshot_ignores_stale_status_pid_without_runtime_process(
    monkeypatch, tmp_path
) -> None:
    settings = SimpleNamespace(state_dir=tmp_path, user_channel="telegram")
    launcher = SimpleNamespace(
        configured_launcher="same_env",
        effective_launcher="same_env",
        available=True,
        reason="",
    )

    monkeypatch.setattr(
        cli_main,
        "read_status",
        lambda _settings: {
            "pid": 111,
            "started_at": "2026-01-01T00:00:00+00:00",
            "active_channel": "Telegram",
        },
    )
    monkeypatch.setattr(cli_main, "is_octopal_runtime_pid", lambda pid: False)
    monkeypatch.setattr(cli_main, "list_octopal_runtime_pids", lambda: [])
    monkeypatch.setattr(cli_main, "read_metrics_snapshot", lambda _state_dir: {})
    monkeypatch.setattr(cli_main, "get_worker_launcher_status", lambda _settings: launcher)

    snapshot = cli_main._build_dashboard_snapshot(settings, last=8, store=_EmptyStore())

    assert snapshot["system"]["running"] is False
    assert snapshot["system"]["pid"] is None


def test_dashboard_snapshot_exposes_plan_binding_for_recent_worker(monkeypatch, tmp_path) -> None:
    settings = SimpleNamespace(state_dir=tmp_path, user_channel="telegram")
    launcher = SimpleNamespace(
        configured_launcher="same_env",
        effective_launcher="same_env",
        available=True,
        reason="",
    )

    monkeypatch.setattr(
        cli_main,
        "read_status",
        lambda _settings: {
            "pid": None,
            "started_at": "2026-01-01T00:00:00+00:00",
            "active_channel": "Telegram",
        },
    )
    monkeypatch.setattr(cli_main, "is_octopal_runtime_pid", lambda pid: False)
    monkeypatch.setattr(cli_main, "list_octopal_runtime_pids", lambda: [])
    monkeypatch.setattr(cli_main, "read_metrics_snapshot", lambda _state_dir: {})
    monkeypatch.setattr(cli_main, "get_worker_launcher_status", lambda _settings: launcher)

    snapshot = cli_main._build_dashboard_snapshot(
        settings,
        last=8,
        store=_StoreWithPlanBoundWorker(),
    )

    assert snapshot["workers"]["recent"][0]["plan_binding"] == {
        "run_id": "plan-1",
        "step_id": "patch",
        "status": "awaiting_worker",
        "title": "Patch parser",
        "kind": "worker",
    }
