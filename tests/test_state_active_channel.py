from __future__ import annotations

import json
from types import SimpleNamespace

from octopal.runtime.state import (
    mark_runtime_running,
    resolve_runtime_status_display,
    update_last_internal_heartbeat,
    update_last_message,
    update_last_scheduler_tick,
    write_start_status,
)


def test_write_start_status_persists_active_channel(tmp_path) -> None:
    settings = SimpleNamespace(state_dir=tmp_path, user_channel="whatsapp")
    write_start_status(settings)

    payload = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert payload["active_channel"] == "WhatsApp"
    assert payload["phase"] == "starting"
    assert payload["last_user_message_at"] is None
    assert payload["last_internal_heartbeat_at"] is None
    assert payload["last_scheduler_tick_at"] is None
    assert payload["status_updated_at"]


def test_write_start_status_persists_desktop_active_channel(tmp_path) -> None:
    settings = SimpleNamespace(state_dir=tmp_path, user_channel="desktop")
    write_start_status(settings)

    payload = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert payload["active_channel"] == "Desktop"


def test_mark_runtime_running_updates_phase(tmp_path) -> None:
    settings = SimpleNamespace(state_dir=tmp_path, user_channel="telegram")
    write_start_status(settings)

    mark_runtime_running(settings)

    payload = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert payload["phase"] == "running"
    assert payload["status_updated_at"]


def test_status_tracks_user_heartbeat_and_scheduler_separately(tmp_path) -> None:
    settings = SimpleNamespace(state_dir=tmp_path, user_channel="telegram")
    write_start_status(settings)

    update_last_message(settings)
    after_user = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert after_user["last_user_message_at"]
    assert after_user["last_message_at"] == after_user["last_user_message_at"]
    assert after_user["last_internal_heartbeat_at"] is None
    assert after_user["last_scheduler_tick_at"] is None

    update_last_internal_heartbeat(settings)
    after_heartbeat = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert after_heartbeat["last_internal_heartbeat_at"]
    assert after_heartbeat["last_user_message_at"] == after_user["last_user_message_at"]

    update_last_scheduler_tick(settings, status="ok")
    after_scheduler = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert after_scheduler["last_scheduler_tick_at"]
    assert after_scheduler["last_scheduler_tick_status"] == "ok"
    assert (
        after_scheduler["last_internal_heartbeat_at"]
        == after_heartbeat["last_internal_heartbeat_at"]
    )


def test_resolve_runtime_status_display_uses_starting_phase() -> None:
    status_text, status_color = resolve_runtime_status_display(
        status_data={"phase": "starting"},
        pid_running=True,
    )
    assert status_text == "STARTING"
    assert status_color == "yellow"
