from __future__ import annotations

import asyncio

import pytest

from octopal.cli.configure import _collect_connector_next_steps
from octopal.infrastructure.config.models import ConnectorInstanceConfig, OctopalConfig
from octopal.infrastructure.connectors.manager import ConnectorManager


def _build_manager(config: OctopalConfig) -> ConnectorManager:
    return ConnectorManager(config=config.connectors, mcp_manager=None, octo_config=config)


def test_google_connector_configure_requires_cli_enabled_connector() -> None:
    config = OctopalConfig()
    manager = _build_manager(config)
    connector = manager.get_connector("google")

    with pytest.raises(RuntimeError, match="not enabled"):
        asyncio.run(connector.configure({"client_id": "id", "client_secret": "secret"}))


def test_google_connector_status_rejects_unsupported_services() -> None:
    config = OctopalConfig()
    config.connectors.instances["google"] = ConnectorInstanceConfig(
        enabled=True,
        settings={"enabled_services": ["gmail", "calendar"]},
    )
    manager = _build_manager(config)
    connector = manager.get_connector("google")

    status = asyncio.run(connector.get_status())

    assert status["status"] == "unsupported_service_configuration"
    assert "calendar" in status["message"]


def test_google_connector_status_requires_reauth_for_newly_enabled_services() -> None:
    config = OctopalConfig()
    config.connectors.instances["google"] = ConnectorInstanceConfig(
        enabled=True,
        settings={
            "enabled_services": ["gmail"],
            "authorized_services": [],
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "refresh",
        },
    )
    manager = _build_manager(config)
    connector = manager.get_connector("google")

    status = asyncio.run(connector.get_status())

    assert status["status"] == "needs_reauth"
    assert "gmail" in status["message"]


def test_collect_connector_next_steps_prompts_for_auth_when_google_added() -> None:
    previous = OctopalConfig()
    current = OctopalConfig()
    current.connectors.instances["google"] = ConnectorInstanceConfig(
        enabled=True,
        settings={"enabled_services": ["gmail"]},
    )

    lines = _collect_connector_next_steps(current, previous)

    assert any("octopal connector auth google" in line for line in lines)
    assert any("octopal connector status" in line for line in lines)
    assert any("octopal restart" in line for line in lines)
