from __future__ import annotations

import asyncio
import sys
import types

import pytest

from octopal.cli.configure import _collect_connector_next_steps
from octopal.infrastructure.config.models import ConnectorInstanceConfig, OctopalConfig
from octopal.infrastructure.connectors.google import _oauthlib_insecure_transport_for_localhost
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


def test_connector_manager_reconciles_ready_google_connector_into_running_mcp() -> None:
    config = OctopalConfig()
    config.connectors.instances["google"] = ConnectorInstanceConfig(
        enabled=True,
        enabled_services=["gmail"],
        credentials={"client_id": "client-id", "client_secret": "client-secret"},
        auth={"authorized_services": ["gmail"], "refresh_token": "refresh-token"},
    )

    class _MCP:
        def __init__(self) -> None:
            self.connected: list[str] = []
            self.disconnected: list[str] = []

        async def connect_server(self, mcp_config):
            self.connected.append(mcp_config.id)
            return []

        async def disconnect_server(self, server_id: str, *, intentional: bool = True):
            self.disconnected.append(server_id)

    mcp = _MCP()
    manager = ConnectorManager(config=config.connectors, mcp_manager=mcp, octo_config=config)

    asyncio.run(manager.load_and_start_all())

    assert mcp.connected == ["google-gmail"]
    assert mcp.disconnected == []


def test_connector_manager_uses_internal_gmail_mcp_server_and_env_names() -> None:
    config = OctopalConfig()
    config.connectors.instances["google"] = ConnectorInstanceConfig(
        enabled=True,
        enabled_services=["gmail"],
        credentials={"client_id": "client-id", "client_secret": "client-secret"},
        auth={"authorized_services": ["gmail"], "refresh_token": "refresh-token"},
    )

    class _MCP:
        def __init__(self) -> None:
            self.configs = []

        async def connect_server(self, mcp_config):
            self.configs.append(mcp_config)
            return []

        async def disconnect_server(self, server_id: str, *, intentional: bool = True):
            return None

    mcp = _MCP()
    manager = ConnectorManager(config=config.connectors, mcp_manager=mcp, octo_config=config)

    asyncio.run(manager.load_and_start_all())

    assert len(mcp.configs) == 1
    gmail_cfg = mcp.configs[0]
    assert gmail_cfg.command == sys.executable
    assert gmail_cfg.args == ["-m", "octopal.mcp_servers.gmail"]
    assert gmail_cfg.env["GMAIL_CLIENT_ID"] == "client-id"
    assert gmail_cfg.env["GMAIL_CLIENT_SECRET"] == "client-secret"
    assert gmail_cfg.env["GMAIL_REFRESH_TOKEN"] == "refresh-token"


def test_connector_manager_continues_startup_when_ready_google_connector_fails_to_start() -> None:
    config = OctopalConfig()
    config.connectors.instances["google"] = ConnectorInstanceConfig(
        enabled=True,
        enabled_services=["gmail"],
        credentials={"client_id": "client-id", "client_secret": "client-secret"},
        auth={"authorized_services": ["gmail"], "refresh_token": "refresh-token"},
    )

    class _MCP:
        def __init__(self) -> None:
            self.connected: list[str] = []
            self.disconnected: list[str] = []

        async def connect_server(self, mcp_config):
            self.connected.append(mcp_config.id)
            raise RuntimeError("boom")

        async def disconnect_server(self, server_id: str, *, intentional: bool = True):
            self.disconnected.append(server_id)

    mcp = _MCP()
    manager = ConnectorManager(config=config.connectors, mcp_manager=mcp, octo_config=config)

    asyncio.run(manager.load_and_start_all())

    assert mcp.connected == ["google-gmail"]
    assert mcp.disconnected == ["google-gmail"]


def test_connector_manager_reconciles_not_ready_google_connector_into_disconnected_mcp() -> None:
    config = OctopalConfig()
    config.connectors.instances["google"] = ConnectorInstanceConfig(
        enabled=True,
        enabled_services=["gmail"],
        credentials={"client_id": "client-id", "client_secret": "client-secret"},
    )

    class _MCP:
        def __init__(self) -> None:
            self.connected: list[str] = []
            self.disconnected: list[str] = []

        async def connect_server(self, mcp_config):
            self.connected.append(mcp_config.id)
            return []

        async def disconnect_server(self, server_id: str, *, intentional: bool = True):
            self.disconnected.append(server_id)

    mcp = _MCP()
    manager = ConnectorManager(config=config.connectors, mcp_manager=mcp, octo_config=config)

    asyncio.run(manager.load_and_start_all())

    assert mcp.connected == []
    assert mcp.disconnected == ["google-gmail"]


def test_oauthlib_insecure_transport_context_is_temporary(monkeypatch) -> None:
    monkeypatch.delenv("OAUTHLIB_INSECURE_TRANSPORT", raising=False)

    with _oauthlib_insecure_transport_for_localhost():
        assert __import__("os").environ["OAUTHLIB_INSECURE_TRANSPORT"] == "1"

    assert "OAUTHLIB_INSECURE_TRANSPORT" not in __import__("os").environ


def test_google_manual_authorize_requires_matching_state(monkeypatch) -> None:
    config = OctopalConfig()
    config.connectors.instances["google"] = ConnectorInstanceConfig(
        enabled=True,
        enabled_services=["gmail"],
        credentials={"client_id": "client-id", "client_secret": "client-secret"},
    )
    manager = _build_manager(config)
    connector = manager.get_connector("google")
    connector._pending_manual_auth = {  # type: ignore[attr-defined]
        "redirect_uri": "http://localhost",
        "code_verifier": "verifier",
        "state": "expected-state",
    }
    fake_flow_module = types.ModuleType("google_auth_oauthlib.flow")

    class _FakeInstalledAppFlow:
        @classmethod
        def from_client_config(cls, *args, **kwargs):
            return cls()

    fake_flow_module.InstalledAppFlow = _FakeInstalledAppFlow
    monkeypatch.setitem(sys.modules, "google_auth_oauthlib.flow", fake_flow_module)

    result = asyncio.run(
        connector.complete_manual_authorize("http://localhost/?state=wrong-state&code=abc")
    )

    assert result["error"] == "State mismatch while completing Google authorization."
