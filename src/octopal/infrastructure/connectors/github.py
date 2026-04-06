from __future__ import annotations

import sys
from typing import Any

import httpx
import structlog

from octopal.infrastructure.connectors.base import Connector

logger = structlog.get_logger(__name__)


class GitHubConnector(Connector):
    _SUPPORTED_SERVICES = ("repos", "issues", "pull_requests")
    _SERVER_ID = "github-core"

    def __init__(self, manager: Any):
        self.manager = manager

    @property
    def name(self) -> str:
        return "github"

    def _get_config(self):
        return self.manager.config.instances.get(self.name)

    def supported_services(self) -> list[str]:
        return list(self._SUPPORTED_SERVICES)

    def managed_server_ids(self) -> list[str]:
        return [self._SERVER_ID] if self._get_enabled_services() else []

    def _get_enabled_services(self) -> list[str]:
        config = self._get_config()
        if not config:
            return []
        enabled = [str(service).strip().lower() for service in config.enabled_services if str(service).strip()]
        deduped: list[str] = []
        for service in enabled:
            if service not in deduped:
                deduped.append(service)
        return deduped

    def _get_authorized_services(self) -> list[str]:
        config = self._get_config()
        if not config:
            return []
        return [
            str(service).strip().lower()
            for service in config.auth.authorized_services
            if str(service).strip()
        ]

    def _unsupported_enabled_services(self) -> list[str]:
        supported = set(self.supported_services())
        return [service for service in self._get_enabled_services() if service not in supported]

    async def get_status(self) -> dict[str, Any]:
        config = self._get_config()
        if not config or not config.enabled:
            return {"status": "disabled"}

        enabled_services = self._get_enabled_services()
        unsupported_services = self._unsupported_enabled_services()
        if unsupported_services:
            return {
                "status": "unsupported_service_configuration",
                "message": (
                    "GitHub connector is configured with unsupported services: "
                    f"{', '.join(unsupported_services)}. Re-run `octopal configure` and keep only supported GitHub services."
                ),
                "services": enabled_services,
                "supported_services": self.supported_services(),
            }

        if not enabled_services:
            return {
                "status": "misconfigured",
                "message": "GitHub connector is enabled but no supported services are selected.",
                "services": [],
                "supported_services": self.supported_services(),
            }

        if not config.auth.access_token:
            return {
                "status": "needs_auth",
                "message": "Connector configured but needs a GitHub personal access token.",
                "services": enabled_services,
                "supported_services": self.supported_services(),
            }

        authorized_services = set(self._get_authorized_services())
        enabled_service_set = set(enabled_services)
        if not enabled_service_set.issubset(authorized_services):
            missing = sorted(enabled_service_set - authorized_services)
            return {
                "status": "needs_reauth",
                "message": (
                    "GitHub connector needs re-authorization for newly enabled services: "
                    f"{', '.join(missing)}"
                ),
                "services": enabled_services,
                "supported_services": self.supported_services(),
            }

        return {
            "status": "ready",
            "message": f"GitHub connector is ready with services: {', '.join(enabled_services)}",
            "services": enabled_services,
            "supported_services": self.supported_services(),
        }

    async def configure(self, settings: dict[str, Any]) -> None:
        config = self._get_config()
        if not config:
            raise RuntimeError(
                "GitHub connector is not enabled. Run `octopal configure` and enable GitHub first."
            )
        if not config.enabled:
            raise RuntimeError(
                "GitHub connector is disabled. Re-run `octopal configure` to enable it before authorizing."
            )

        token = settings.get("token")
        if token is not None:
            config.auth.access_token = str(token).strip() or None
            if config.auth.access_token is None:
                config.auth.authorized_services = []
        self.manager.save_config()

    async def authorize(self) -> dict[str, Any]:
        config = self._get_config()
        if not config:
            return {"error": "GitHub connector is not enabled. Run `octopal configure` first."}
        if not config.enabled:
            return {"error": "GitHub connector is disabled. Run `octopal configure` to enable it first."}

        unsupported_services = self._unsupported_enabled_services()
        if unsupported_services:
            return {
                "error": (
                    "Unsupported GitHub services are enabled: "
                    f"{', '.join(unsupported_services)}. Re-run `octopal configure` and keep only supported GitHub services."
                )
            }

        enabled_services = self._get_enabled_services()
        if not enabled_services:
            return {"error": "No supported GitHub services are enabled. Re-run `octopal configure`."}

        token = str(config.auth.access_token or "").strip()
        if not token:
            return {"error": "Missing GitHub personal access token."}

        try:
            async with httpx.AsyncClient(base_url="https://api.github.com", timeout=20.0) as client:
                response = await client.get(
                    "/user",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": "2022-11-28",
                    },
                )
            if response.status_code == 401:
                return {"error": "GitHub rejected the token. Check that the personal access token is valid."}
            if response.is_error:
                detail = response.text.strip() or response.reason_phrase
                return {"error": f"GitHub authorization check failed: {detail}"}
        except Exception as exc:
            logger.exception("Failed to authorize GitHub connector")
            return {"error": f"Failed to authorize GitHub connector: {exc}"}

        config.auth.authorized_services = enabled_services
        config.auth.last_error = None
        self.manager.save_config()

        if self.manager.mcp_manager is not None:
            await self.start()

        return {
            "status": "success",
            "message": f"GitHub connector authorized for {', '.join(enabled_services)}.",
        }

    async def start(self) -> None:
        if self.manager.mcp_manager is None:
            return

        config = self._get_config()
        if not config or not config.enabled:
            return

        enabled_services = self._get_enabled_services()
        if not enabled_services or not config.auth.access_token:
            return

        from octopal.infrastructure.mcp.manager import MCPServerConfig

        github_cfg = MCPServerConfig(
            id=self._SERVER_ID,
            name="GitHub Connector",
            command=sys.executable,
            args=["-m", "octopal.mcp_servers.github"],
            env={
                "GITHUB_TOKEN": config.auth.access_token,
                "GITHUB_ENABLED_SERVICES": ",".join(enabled_services),
            },
            transport="stdio",
        )
        await self.manager.mcp_manager.connect_server(github_cfg)

    async def disconnect(self, *, forget_credentials: bool = False) -> dict[str, Any]:
        config = self._get_config()
        if not config:
            return {"status": "noop", "message": "GitHub connector is not configured."}

        if self.manager.mcp_manager is not None:
            for server_id in self.managed_server_ids():
                try:
                    await self.manager.mcp_manager.disconnect_server(server_id, intentional=True)
                except Exception:
                    logger.warning(
                        "Failed to disconnect MCP server for GitHub connector",
                        server_id=server_id,
                        exc_info=True,
                    )

        config.auth.refresh_token = None
        config.auth.access_token = None
        config.auth.authorized_services = []
        config.auth.last_error = None
        self.manager.save_config()

        return {
            "status": "success",
            "message": "GitHub connector disconnected and stored access credentials were removed.",
        }
