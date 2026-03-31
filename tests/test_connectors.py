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
        enabled_services=["gmail", "calendar"],
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
        enabled_services=["gmail"],
        credentials={"client_id": "id", "client_secret": "secret"},
        auth={"authorized_services": [], "refresh_token": "refresh"},
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
        enabled_services=["gmail"],
    )

    lines = _collect_connector_next_steps(current, previous)

    assert any("octopal connector auth google" in line for line in lines)
    assert any("octopal connector status" in line for line in lines)
    assert any("octopal restart" in line for line in lines)


def test_google_connector_disconnect_clears_auth_state_but_keeps_client_credentials() -> None:
    config = OctopalConfig()
    config.connectors.instances["google"] = ConnectorInstanceConfig(
        enabled=True,
        enabled_services=["gmail"],
        credentials={"client_id": "client-id", "client_secret": "client-secret"},
        auth={
            "authorized_services": ["gmail"],
            "refresh_token": "refresh-token",
            "access_token": "access-token",
        },
    )
    manager = _build_manager(config)
    connector = manager.get_connector("google")

    result = asyncio.run(connector.disconnect())

    assert result["status"] == "success"
    instance = config.connectors.instances["google"]
    assert instance.auth.refresh_token is None
    assert instance.auth.access_token is None
    assert instance.auth.authorized_services == []
    assert instance.credentials.client_id == "client-id"
    assert instance.credentials.client_secret == "client-secret"
