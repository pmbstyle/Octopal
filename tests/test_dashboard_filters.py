from __future__ import annotations

from octopal.gateway.dashboard import (
    DashboardFilters,
    _build_filters,
    _normalize_log_entry,
)
from octopal.infrastructure.config.settings import Settings


def _settings(tmp_path):
    return Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
    )


def test_build_filters_normalizes_unknown_values(tmp_path) -> None:
    settings = _settings(tmp_path)
    filters = _build_filters(settings, window_minutes=17, service="weird", environment="")
    assert filters.window_minutes == 60
    assert filters.service == "all"
    assert filters.environment == "all"


def test_build_filters_accepts_whatsapp_service(tmp_path) -> None:
    settings = _settings(tmp_path)
    filters = _build_filters(settings, window_minutes=60, service="whatsapp", environment="all")
    assert filters.service == "whatsapp"


def test_normalize_log_entry_respects_service_filter() -> None:
    filters = DashboardFilters(window_minutes=60, service="telegram", environment="all")
    line = (
        '{"timestamp":"2026-03-01T00:00:00+00:00","level":"info","event":"telegram message queued"}'
    )
    entry = _normalize_log_entry(line, filters=filters)
    assert entry is not None
    assert entry["service"] == "telegram"

    blocked = _normalize_log_entry(
        '{"timestamp":"2026-03-01T00:00:00+00:00","level":"info","event":"octo loop tick"}',
        filters=filters,
    )
    assert blocked is None


def test_normalize_log_entry_detects_whatsapp_service(tmp_path) -> None:
    filters = DashboardFilters(window_minutes=60, service="whatsapp", environment="all")
    line = '{"timestamp":"2026-03-01T00:00:00+00:00","level":"info","event":"whatsapp bridge connected"}'
    entry = _normalize_log_entry(line, filters=filters)
    assert entry is not None
    assert entry["service"] == "whatsapp"
