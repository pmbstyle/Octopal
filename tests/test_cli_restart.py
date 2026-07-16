from __future__ import annotations

from pathlib import Path

import pytest
import typer

from octopal.cli import main


class _Settings:
    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir


def test_restart_preflights_worker_launcher_before_stopping(tmp_path: Path, monkeypatch) -> None:
    events: list[str] = []
    settings = _Settings(tmp_path / "data")

    monkeypatch.setattr(main, "load_settings", lambda: settings)
    monkeypatch.setattr(
        main,
        "_ensure_worker_launcher_ready",
        lambda _settings: events.append("launcher_ready"),
    )
    monkeypatch.setattr(main, "stop", lambda: events.append("stopped"))
    monkeypatch.setattr(
        main,
        "start",
        lambda *, foreground: events.append(f"started:{foreground}"),
    )
    monkeypatch.setattr(main.time, "sleep", lambda _seconds: None)

    main.restart(foreground=False)

    assert events == ["launcher_ready", "stopped", "started:False"]


def test_restart_keeps_runtime_running_when_launcher_preflight_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    events: list[str] = []
    settings = _Settings(tmp_path / "data")

    monkeypatch.setattr(main, "load_settings", lambda: settings)

    def fail_preflight(_settings) -> None:
        events.append("launcher_failed")
        raise typer.Exit(code=1)

    monkeypatch.setattr(main, "_ensure_worker_launcher_ready", fail_preflight)
    monkeypatch.setattr(main, "stop", lambda: events.append("stopped"))

    with pytest.raises(typer.Exit):
        main.restart(foreground=False)

    assert events == ["launcher_failed"]
