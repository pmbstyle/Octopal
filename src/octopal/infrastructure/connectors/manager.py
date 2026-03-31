from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

from octopal.infrastructure.config.models import ConnectorsConfig
from octopal.infrastructure.connectors.google import GoogleConnector

if TYPE_CHECKING:
    from octopal.infrastructure.config.models import OctopalConfig
    from octopal.infrastructure.mcp.manager import MCPManager

logger = structlog.get_logger(__name__)

class ConnectorManager:
    def __init__(
        self,
        config: ConnectorsConfig,
        mcp_manager: MCPManager | None,
        octo_config: OctopalConfig,
    ):
        self.config = config
        self.mcp_manager = mcp_manager
        self.octo_config = octo_config
        self.connectors = {
            "google": GoogleConnector(self)
        }

    def get_connector(self, name: str):
        return self.connectors.get(name)

    async def disconnect_connector(self, name: str, *, forget_credentials: bool = False) -> dict[str, Any]:
        connector = self.get_connector(name)
        if connector is None:
            raise RuntimeError(f"Unknown connector '{name}'.")
        return await connector.disconnect(forget_credentials=forget_credentials)

    def save_config(self) -> None:
        """Save the overall Octopal config."""
        from octopal.infrastructure.config.settings import save_config
        save_config(self.octo_config)

    async def get_all_statuses(self) -> dict[str, dict[str, Any]]:
        statuses = {}
        for name, connector in self.connectors.items():
            statuses[name] = await connector.get_status()
        return statuses

    async def load_and_start_all(self) -> None:
        """Reconcile runtime-managed integrations for all connectors."""
        for name in self.connectors:
            await self.reconcile_connector_runtime(name)

    async def reconcile_connector_runtime(self, name: str) -> None:
        connector = self.get_connector(name)
        if connector is None:
            return

        status = await connector.get_status()
        if status.get("status") == "ready":
            logger.info("Starting connector", name=name)
            await connector.start()
            return

        logger.info(
            "Connector not ready; disconnecting managed runtime integrations",
            name=name,
            status=status.get("status"),
        )
        await self._disconnect_managed_servers(connector)

    async def _disconnect_managed_servers(self, connector) -> None:
        if self.mcp_manager is None:
            return
        for server_id in connector.managed_server_ids():
            try:
                await self.mcp_manager.disconnect_server(server_id, intentional=True)
            except Exception:
                logger.warning(
                    "Failed to disconnect managed MCP server for connector",
                    connector=connector.name,
                    server_id=server_id,
                    exc_info=True,
                )
