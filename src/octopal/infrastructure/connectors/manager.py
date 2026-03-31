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
    def __init__(self, config: ConnectorsConfig, mcp_manager: MCPManager, octo_config: OctopalConfig):
        self.config = config
        self.mcp_manager = mcp_manager
        self.octo_config = octo_config
        self.connectors = {
            "google": GoogleConnector(self)
        }

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
        """Start all enabled connectors."""
        for name, config in self.config.instances.items():
            if config.enabled and name in self.connectors:
                status = await self.connectors[name].get_status()
                if status["status"] == "ready":
                    logger.info("Starting connector", name=name)
                    # For Google, starting means ensuring MCP servers are registered
                    await self.connectors[name]._register_mcp_server()
