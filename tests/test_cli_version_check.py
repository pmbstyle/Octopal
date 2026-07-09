from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from octopal import __version__
from octopal.cli.main import (
    _VERSION_CHECK_TTL_SECONDS,
    _get_latest_release_info,
    _is_remote_version_newer,
    _maybe_warn_about_newer_release,
    _normalize_release_version,
)
from octopal.infrastructure.config.settings import Settings


def _build_settings(tmp_path) -> Settings:
    return Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
    )


def test_normalize_release_version_strips_v_prefix() -> None:
    assert _normalize_release_version("v2026.04.15") == "2026.04.15"
    assert _normalize_release_version("2026.04.15.1") == "2026.04.15.1"
    assert _normalize_release_version("release-2026.04.15") is None


def test_remote_version_newer_compares_date_based_versions() -> None:
    assert _is_remote_version_newer("2026.04.14", "2026.04.15") is True
    assert _is_remote_version_newer("2026.04.14", "2026.04.14.1") is True
    assert _is_remote_version_newer("2026.04.14.1", "2026.04.14") is False
    assert _is_remote_version_newer("2026.04.14", "2026.04.14") is False


def test_get_latest_release_info_uses_fresh_cache(tmp_path, monkeypatch) -> None:
    settings = _build_settings(tmp_path)
    cache_path = settings.state_dir / "version_check.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(
            {
                "repo": "owner/repo",
                "version": "2026.04.15",
                "url": "https://example.test/releases/tag/v2026.04.15",
                "checked_at": datetime.now(UTC).isoformat(),
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("octopal.cli.main._detect_release_repo_slug", lambda: "owner/repo")
    monkeypatch.setattr(
        "octopal.cli.main._fetch_latest_release_info_from_github",
        lambda _repo_slug: (_ for _ in ()).throw(AssertionError("network fetch should not run")),
    )

    assert _get_latest_release_info(settings) == (
        "2026.04.15",
        "https://example.test/releases/tag/v2026.04.15",
    )


def test_get_latest_release_info_refreshes_stale_cache(tmp_path, monkeypatch) -> None:
    settings = _build_settings(tmp_path)
    cache_path = settings.state_dir / "version_check.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(
            {
                "repo": "owner/repo",
                "version": "2026.04.14",
                "url": "https://example.test/releases/tag/v2026.04.14",
                "checked_at": (
                    datetime.now(UTC) - timedelta(seconds=_VERSION_CHECK_TTL_SECONDS + 1)
                ).isoformat(),
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("octopal.cli.main._detect_release_repo_slug", lambda: "owner/repo")
    monkeypatch.setattr(
        "octopal.cli.main._fetch_latest_release_info_from_github",
        lambda _repo_slug: ("2026.04.15", "https://example.test/releases/tag/v2026.04.15"),
    )

    assert _get_latest_release_info(settings) == (
        "2026.04.15",
        "https://example.test/releases/tag/v2026.04.15",
    )


def test_warns_when_new_release_is_available(tmp_path, monkeypatch) -> None:
    settings = _build_settings(tmp_path)
    printed: list[str] = []
    remote_version = f"{__version__}.1"
    remote_url = f"https://example.test/releases/tag/v{remote_version}"

    monkeypatch.setattr(
        "octopal.cli.main._get_latest_release_info",
        lambda _settings: (remote_version, remote_url),
    )
    monkeypatch.setattr(
        "octopal.cli.main.console.print",
        lambda message="", *args, **kwargs: printed.append(str(message)),
    )

    _maybe_warn_about_newer_release(settings)

    assert any("Update available:" in line for line in printed)
    assert any(remote_version in line for line in printed)
