from __future__ import annotations

from pathlib import Path

import octopal.browser.managed as managed
import octopal.browser.pinchtab as pinchtab
from octopal.infrastructure.config.settings import Settings


def test_managed_web_defaults_enable_auto_stack() -> None:
    settings = Settings()

    assert settings.webclaw_enabled is True
    assert settings.webclaw_prefer_local is True
    assert settings.browser_backend == "auto"
    assert settings.pinchtab_managed is True
    assert settings.pinchtab_image == "pinchtab/pinchtab:0.11.0"


def test_prepare_managed_web_runtime_activates_healthy_sidecar(tmp_path: Path, monkeypatch) -> None:
    settings = Settings(
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKER_LAUNCHER="docker",
    )
    monkeypatch.setattr(managed, "_prepare_host_webclaw", lambda _settings: None)
    monkeypatch.setattr(
        managed,
        "_ensure_managed_pinchtab",
        lambda _settings: (
            managed.ManagedPinchTabStatus(
                "ready",
                "Managed PinchTab is healthy.",
                base_url="http://127.0.0.1:19867",
                worker_base_url="http://host.docker.internal:19867",
                container_name="octopal-pinchtab-test",
                image="pinchtab/pinchtab:0.11.0",
            ),
            "server-secret",
        ),
    )

    status = managed.prepare_managed_web_runtime(settings)

    assert status.status == "ready"
    assert settings.browser_backend == "pinchtab"
    assert settings.pinchtab_base_url == "http://127.0.0.1:19867"
    assert settings.pinchtab_worker_base_url == "http://host.docker.internal:19867"
    assert settings.pinchtab_token == "server-secret"
    assert "server-secret" not in (
        tmp_path / "state" / "managed-web" / "pinchtab" / "status.json"
    ).read_text(encoding="utf-8")


def test_prepare_managed_web_runtime_falls_back_to_playwright(tmp_path: Path, monkeypatch) -> None:
    settings = Settings(OCTOPAL_STATE_DIR=tmp_path / "state")
    monkeypatch.setattr(managed, "_prepare_host_webclaw", lambda _settings: None)

    def fail(_settings):
        raise RuntimeError("Docker is unavailable")

    monkeypatch.setattr(managed, "_ensure_managed_pinchtab", fail)

    status = managed.prepare_managed_web_runtime(settings)

    assert status.status == "degraded"
    assert settings.browser_backend == "playwright"
    assert "Docker is unavailable" in status.detail


def test_prepare_managed_web_runtime_preserves_external_pinchtab(monkeypatch) -> None:
    settings = Settings(
        OCTOPAL_BROWSER_BACKEND="pinchtab",
        OCTOPAL_PINCHTAB_TOKEN="external-secret",
        OCTOPAL_PINCHTAB_BASE_URL="http://pinchtab.internal:9867",
    )
    monkeypatch.setattr(managed, "_prepare_host_webclaw", lambda _settings: None)

    status = managed.prepare_managed_web_runtime(settings)

    assert status.status == "external"
    assert settings.browser_backend == "pinchtab"
    assert settings.pinchtab_base_url == "http://pinchtab.internal:9867"


def test_runtime_binding_uses_resolved_managed_pinchtab_settings() -> None:
    settings = Settings(
        OCTOPAL_BROWSER_BACKEND="pinchtab",
        OCTOPAL_PINCHTAB_BASE_URL="http://127.0.0.1:19867",
        OCTOPAL_PINCHTAB_TOKEN="managed-secret",
    )

    pinchtab.configure_pinchtab_backend(settings)
    try:
        backend = pinchtab.get_pinchtab_backend()
        assert backend is not None
        assert backend._base_url == "http://127.0.0.1:19867"
        assert backend._token == "managed-secret"
    finally:
        pinchtab.configure_pinchtab_backend(None)
