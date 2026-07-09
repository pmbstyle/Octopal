from __future__ import annotations

import time

from octopal.cli.main import _is_webapp_build_stale, _schedule_webapp_build
from octopal.infrastructure.config.settings import Settings


def test_webapp_build_is_stale_when_dist_missing(tmp_path) -> None:
    webapp_dir = tmp_path / "webapp"
    webapp_dir.mkdir()
    (webapp_dir / "src").mkdir()
    (webapp_dir / "src" / "main.tsx").write_text("export {};\n", encoding="utf-8")
    dist_dir = webapp_dir / "dist"

    assert _is_webapp_build_stale(webapp_dir, dist_dir) is True


def test_webapp_build_not_stale_when_dist_newer(tmp_path) -> None:
    webapp_dir = tmp_path / "webapp"
    src_dir = webapp_dir / "src"
    dist_dir = webapp_dir / "dist"
    src_dir.mkdir(parents=True)
    dist_dir.mkdir(parents=True)

    source_file = src_dir / "main.tsx"
    source_file.write_text("console.log('a');\n", encoding="utf-8")
    time.sleep(0.01)
    (dist_dir / "index.html").write_text("<html></html>\n", encoding="utf-8")

    assert _is_webapp_build_stale(webapp_dir, dist_dir) is False


def test_webapp_build_stale_when_source_newer_than_dist(tmp_path) -> None:
    webapp_dir = tmp_path / "webapp"
    src_dir = webapp_dir / "src"
    dist_dir = webapp_dir / "dist"
    src_dir.mkdir(parents=True)
    dist_dir.mkdir(parents=True)

    source_file = src_dir / "main.tsx"
    source_file.write_text("console.log('a');\n", encoding="utf-8")
    (dist_dir / "index.html").write_text("<html></html>\n", encoding="utf-8")
    time.sleep(0.01)
    source_file.write_text("console.log('b');\n", encoding="utf-8")

    assert _is_webapp_build_stale(webapp_dir, dist_dir) is True


def test_schedule_webapp_build_starts_background_thread_when_stale(tmp_path, monkeypatch) -> None:
    settings = Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WEBAPP_ENABLED=True,
    )
    webapp_dir = tmp_path / "webapp"
    dist_dir = webapp_dir / "dist"
    webapp_dir.mkdir()

    captured: dict[str, object] = {}

    class DummyThread:
        def __init__(self, *, target=None, kwargs=None, name=None, daemon=None):
            captured["target"] = target
            captured["kwargs"] = kwargs
            captured["name"] = name
            captured["daemon"] = daemon
            captured["started"] = False

        def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr(
        "octopal.cli.main._resolve_webapp_paths", lambda _settings: (webapp_dir, dist_dir)
    )
    monkeypatch.setattr("octopal.cli.main.shutil.which", lambda _name: "/usr/bin/npm")
    monkeypatch.setattr(
        "octopal.cli.main._is_webapp_build_stale", lambda _webapp_dir, _dist_dir: True
    )
    monkeypatch.setattr("octopal.cli.main.threading.Thread", DummyThread)

    _schedule_webapp_build(settings)

    assert captured["started"] is True
    assert captured["target"].__name__ == "_build_webapp_assets"
    assert captured["kwargs"] == {"settings": settings, "fail_hard": False}
    assert captured["name"] == "octopal-webapp-build"
    assert captured["daemon"] is True


def test_schedule_webapp_build_skips_thread_when_assets_are_fresh(tmp_path, monkeypatch) -> None:
    settings = Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WEBAPP_ENABLED=True,
    )
    webapp_dir = tmp_path / "webapp"
    dist_dir = webapp_dir / "dist"
    webapp_dir.mkdir()

    called = {"thread": False}

    class DummyThread:
        def __init__(self, *args, **kwargs):
            called["thread"] = True

        def start(self) -> None:
            called["thread"] = True

    monkeypatch.setattr(
        "octopal.cli.main._resolve_webapp_paths", lambda _settings: (webapp_dir, dist_dir)
    )
    monkeypatch.setattr("octopal.cli.main.shutil.which", lambda _name: "/usr/bin/npm")
    monkeypatch.setattr(
        "octopal.cli.main._is_webapp_build_stale", lambda _webapp_dir, _dist_dir: False
    )
    monkeypatch.setattr("octopal.cli.main.threading.Thread", DummyThread)

    _schedule_webapp_build(settings)

    assert called["thread"] is False
