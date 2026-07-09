from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import octopal.tools.catalog as tool_catalog


def test_gateway_status_summarizes_runtime_and_channel_state(monkeypatch, tmp_path: Path) -> None:
    settings = SimpleNamespace(
        state_dir=tmp_path / "data",
        workspace_dir=tmp_path / "workspace",
        gateway_host="127.0.0.1",
        gateway_port=8123,
        user_channel="telegram",
    )
    monkeypatch.setattr(tool_catalog, "load_settings", lambda: settings)
    monkeypatch.setattr(
        tool_catalog,
        "get_worker_launcher_status",
        lambda _settings: SimpleNamespace(
            configured_launcher="docker",
            effective_launcher="docker",
            available=True,
            reason="Docker worker runtime is ready.",
            docker_cli_path="/usr/bin/docker",
            docker_daemon_reachable=True,
            docker_image_present=True,
        ),
    )
    monkeypatch.setattr(
        tool_catalog,
        "read_status",
        lambda _settings: {
            "pid": 4321,
            "started_at": "2026-03-20T10:00:00+00:00",
            "last_message_at": "2026-03-20T10:05:00+00:00",
            "last_user_message_at": "2026-03-20T10:05:00+00:00",
            "last_internal_heartbeat_at": "2026-03-20T10:06:00+00:00",
            "last_scheduler_tick_at": "2026-03-20T10:06:30+00:00",
            "last_scheduler_tick_status": "ok",
            "status_updated_at": "2026-03-20T10:06:31+00:00",
            "active_channel": "Telegram",
        },
    )
    monkeypatch.setattr(tool_catalog, "is_pid_running", lambda pid: pid == 4321)
    monkeypatch.setattr(
        tool_catalog,
        "read_metrics_snapshot",
        lambda _state_dir: {
            "octo": {
                "followup_queues": 2,
                "internal_queues": 1,
                "followup_tasks": 1,
                "internal_tasks": 1,
                "thinking_count": 0,
                "updated_at": "2026-03-20T10:05:01+00:00",
            },
            "telegram": {
                "chat_queues": 4,
                "send_tasks": 2,
                "updated_at": "2026-03-20T10:05:02+00:00",
            },
            "exec_run": {
                "background_sessions_running": 1,
                "background_sessions_total": 3,
                "updated_at": "2026-03-20T10:05:03+00:00",
            },
            "scheduler": {
                "running": True,
                "last_tick_status": "idle",
                "last_due_count": 2,
                "last_dispatch_started": 1,
                "last_dispatch_duplicates": 0,
                "last_dispatch_rejected_by_policy": 1,
                "last_dispatch_errors": 0,
                "last_policy_reasons": {"missing_worker_id": 1},
                "ticks_total": 8,
                "failures_total": 0,
                "updated_at": "2026-03-20T10:05:03+00:00",
            },
            "connectivity": {
                "mcp_servers": {
                    "docs": {"connected": True},
                    "github": {"connected": False},
                },
                "updated_at": "2026-03-20T10:05:04+00:00",
            },
        },
    )

    payload = json.loads(tool_catalog._tool_gateway_status({}, {}))

    assert payload["status"] == "ok"
    assert payload["running"] is True
    assert payload["last_heartbeat"] == "2026-03-20T10:06:00+00:00"
    assert payload["last_user_message_at"] == "2026-03-20T10:05:00+00:00"
    assert payload["last_scheduler_tick_at"] == "2026-03-20T10:06:30+00:00"
    assert payload["gateway"]["active_channel"] == "telegram"
    assert payload["octo"]["state"] == "thinking"
    assert payload["octo"]["busy"] is True
    assert payload["octo"]["followup_queues"] == 2
    assert payload["channel"]["queue_depth"] == 4
    assert payload["exec"]["sessions_running"] == 1
    assert payload["worker_launcher"] == {
        "configured": "docker",
        "effective": "docker",
        "available": True,
        "reason": "Docker worker runtime is ready.",
        "docker_cli_path": "/usr/bin/docker",
        "docker_daemon_reachable": True,
        "docker_image_present": True,
    }
    assert payload["scheduler"]["last_dispatch_started"] == 1
    assert any(
        service["id"] == "scheduler" and service["status"] == "warning"
        for service in payload["services"]
    )
    assert any(
        service["id"] == "worker-launcher"
        and service["status"] == "ok"
        and service["metrics"]["effective"] == "docker"
        for service in payload["services"]
    )
    assert payload["mcp"]["servers_connected"] == 1
    assert any(
        service["id"] == "gateway" and service["status"] == "ok" for service in payload["services"]
    )
    assert any("rejected by policy" in hint for hint in payload["hints"])
    assert any("follow-up queue" in hint for hint in payload["hints"])


def test_gateway_status_treats_missing_scheduler_metrics_as_not_reporting(
    monkeypatch, tmp_path: Path
) -> None:
    settings = SimpleNamespace(
        state_dir=tmp_path / "data",
        workspace_dir=tmp_path / "workspace",
        gateway_host="127.0.0.1",
        gateway_port=8123,
        user_channel="telegram",
    )
    monkeypatch.setattr(tool_catalog, "load_settings", lambda: settings)
    monkeypatch.setattr(
        tool_catalog,
        "get_worker_launcher_status",
        lambda _settings: SimpleNamespace(
            configured_launcher="docker",
            effective_launcher="docker",
            available=True,
            reason="Docker worker runtime is ready.",
            docker_cli_path="/usr/bin/docker",
            docker_daemon_reachable=True,
            docker_image_present=True,
        ),
    )
    monkeypatch.setattr(
        tool_catalog,
        "read_status",
        lambda _settings: {
            "pid": 4321,
            "started_at": "2026-03-20T10:00:00+00:00",
            "last_message_at": "2026-03-20T10:05:00+00:00",
            "last_user_message_at": "2026-03-20T10:05:00+00:00",
            "last_internal_heartbeat_at": "2026-03-20T10:06:00+00:00",
            "last_scheduler_tick_at": "2026-03-20T10:06:30+00:00",
            "last_scheduler_tick_status": "ok",
            "status_updated_at": "2026-03-20T10:06:31+00:00",
            "active_channel": "Telegram",
        },
    )
    monkeypatch.setattr(tool_catalog, "is_pid_running", lambda pid: pid == 4321)
    monkeypatch.setattr(
        tool_catalog,
        "read_metrics_snapshot",
        lambda _state_dir: {
            "octo": {
                "followup_queues": 0,
                "internal_queues": 0,
                "followup_tasks": 0,
                "internal_tasks": 0,
                "thinking_count": 0,
                "updated_at": "2026-03-20T10:05:01+00:00",
            },
            "telegram": {
                "chat_queues": 0,
                "send_tasks": 0,
                "updated_at": "2026-03-20T10:05:02+00:00",
            },
            "connectivity": {"mcp_servers": {}, "updated_at": "2026-03-20T10:05:04+00:00"},
        },
    )

    payload = json.loads(tool_catalog._tool_gateway_status({}, {}))

    scheduler_service = next(
        service for service in payload["services"] if service["id"] == "scheduler"
    )
    assert scheduler_service["status"] == "ok"
    assert scheduler_service["reason"] == "scheduler metrics unavailable"


def test_gateway_status_warns_when_docker_launcher_falls_back(monkeypatch, tmp_path: Path) -> None:
    settings = SimpleNamespace(
        state_dir=tmp_path / "data",
        workspace_dir=tmp_path / "workspace",
        gateway_host="127.0.0.1",
        gateway_port=8123,
        user_channel="telegram",
    )
    monkeypatch.setattr(tool_catalog, "load_settings", lambda: settings)
    monkeypatch.setattr(
        tool_catalog,
        "get_worker_launcher_status",
        lambda _settings: SimpleNamespace(
            configured_launcher="docker",
            effective_launcher="same_env",
            available=False,
            reason="Docker daemon is unavailable.",
            docker_cli_path="/usr/bin/docker",
            docker_daemon_reachable=False,
            docker_image_present=None,
        ),
    )
    monkeypatch.setattr(
        tool_catalog,
        "read_status",
        lambda _settings: {
            "pid": 4321,
            "started_at": "2026-03-20T10:00:00+00:00",
            "last_internal_heartbeat_at": "2026-03-20T10:06:00+00:00",
            "status_updated_at": "2026-03-20T10:06:31+00:00",
            "active_channel": "Telegram",
        },
    )
    monkeypatch.setattr(tool_catalog, "is_pid_running", lambda pid: pid == 4321)
    monkeypatch.setattr(
        tool_catalog,
        "read_metrics_snapshot",
        lambda _state_dir: {
            "octo": {"updated_at": "2026-03-20T10:05:01+00:00"},
            "telegram": {"updated_at": "2026-03-20T10:05:02+00:00"},
            "connectivity": {"mcp_servers": {"docs": {"connected": True}}},
        },
    )

    payload = json.loads(tool_catalog._tool_gateway_status({}, {}))

    assert payload["worker_launcher"]["configured"] == "docker"
    assert payload["worker_launcher"]["effective"] == "same_env"
    assert payload["worker_launcher"]["available"] is False
    launcher_service = next(
        service for service in payload["services"] if service["id"] == "worker-launcher"
    )
    assert launcher_service["status"] == "warning"
    assert launcher_service["metrics"]["docker_daemon_reachable"] is False
    assert any("Docker worker launcher is not active" in hint for hint in payload["hints"])
