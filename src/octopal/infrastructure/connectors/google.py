from __future__ import annotations

from typing import Any

import structlog

from octopal.infrastructure.connectors.base import Connector

logger = structlog.get_logger(__name__)


class GoogleConnector(Connector):
    _SUPPORTED_SERVICES = ("gmail",)

    def __init__(self, manager: Any):
        self.manager = manager
        self._service_scopes = {
            "gmail": "https://www.googleapis.com/auth/gmail.modify",
        }

    @property
    def name(self) -> str:
        return "google"

    def _get_config(self):
        return self.manager.config.instances.get(self.name)

    def supported_services(self) -> list[str]:
        return list(self._SUPPORTED_SERVICES)

    def _get_enabled_services(self) -> list[str]:
        config = self._get_config()
        if not config:
            return []
        raw_services = config.settings.get("enabled_services", list(self._SUPPORTED_SERVICES))
        if not isinstance(raw_services, list):
            return []
        enabled = [str(service).strip().lower() for service in raw_services if str(service).strip()]
        deduped: list[str] = []
        for service in enabled:
            if service not in deduped:
                deduped.append(service)
        return deduped

    def _get_authorized_services(self) -> list[str]:
        config = self._get_config()
        if not config:
            return []
        raw_services = config.settings.get("authorized_services", [])
        if not isinstance(raw_services, list):
            return []
        return [str(service).strip().lower() for service in raw_services if str(service).strip()]

    def _unsupported_enabled_services(self) -> list[str]:
        supported = set(self.supported_services())
        return [service for service in self._get_enabled_services() if service not in supported]

    def _get_scopes(self) -> list[str]:
        enabled = self._get_enabled_services()
        return [self._service_scopes[svc] for svc in enabled if svc in self._service_scopes]

    async def get_status(self) -> dict[str, Any]:
        config = self._get_config()
        if not config or not config.enabled:
            return {"status": "disabled"}

        settings = config.settings
        enabled_services = self._get_enabled_services()
        unsupported_services = self._unsupported_enabled_services()
        if unsupported_services:
            return {
                "status": "unsupported_service_configuration",
                "message": (
                    "Google connector is configured with unsupported services: "
                    f"{', '.join(unsupported_services)}. Re-run `octopal configure` and keep only Gmail."
                ),
                "services": enabled_services,
                "supported_services": self.supported_services(),
            }

        if not enabled_services:
            return {
                "status": "misconfigured",
                "message": "Google connector is enabled but no supported services are selected.",
                "services": [],
                "supported_services": self.supported_services(),
            }

        if not settings.get("client_id") or not settings.get("client_secret"):
            return {
                "status": "not_configured",
                "message": "Missing client_id or client_secret",
                "services": enabled_services,
                "supported_services": self.supported_services(),
            }

        if not settings.get("refresh_token"):
            return {
                "status": "needs_auth",
                "message": "Connector configured but needs CLI authorization.",
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
                    "Google connector needs re-authorization for newly enabled services: "
                    f"{', '.join(missing)}"
                ),
                "services": enabled_services,
                "supported_services": self.supported_services(),
            }

        return {
            "status": "ready",
            "message": f"Google connector is ready with services: {', '.join(enabled_services)}",
            "services": enabled_services,
            "supported_services": self.supported_services(),
        }

    async def configure(self, settings: dict[str, Any]) -> None:
        """Update auth-related settings for an already-enabled connector."""
        config = self._get_config()
        if not config:
            raise RuntimeError(
                "Google connector is not enabled. Run `octopal configure` and enable Google first."
            )
        if not config.enabled:
            raise RuntimeError(
                "Google connector is disabled. Re-run `octopal configure` to enable it before authorizing."
            )

        allowed_keys = {"client_id", "client_secret"}
        for key, value in settings.items():
            if key in allowed_keys:
                config.settings[key] = value
        self.manager.save_config()

    async def authorize(self) -> dict[str, Any]:
        """Run OAuth2 authorization for the enabled Google services."""
        try:
            from google_auth_oauthlib.flow import InstalledAppFlow
        except ImportError:
            return {
                "error": (
                    "google-auth-oauthlib is not installed. Install project dependencies again "
                    "before authorizing Google."
                )
            }

        config = self._get_config()
        if not config:
            return {"error": "Google connector is not enabled. Run `octopal configure` first."}
        if not config.enabled:
            return {"error": "Google connector is disabled. Run `octopal configure` to enable it first."}

        settings = config.settings
        client_id = settings.get("client_id")
        client_secret = settings.get("client_secret")

        if not client_id or not client_secret:
            return {"error": "Missing client_id or client_secret in Google connector settings."}

        unsupported_services = self._unsupported_enabled_services()
        if unsupported_services:
            return {
                "error": (
                    "Unsupported Google services are enabled: "
                    f"{', '.join(unsupported_services)}. Re-run `octopal configure` and keep only Gmail."
                )
            }

        scopes = self._get_scopes()
        if not scopes:
            return {"error": "No supported Google services are enabled. Re-run `octopal configure`."}

        client_config = {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        }

        try:
            flow = InstalledAppFlow.from_client_config(client_config, scopes=scopes)
            credentials = flow.run_local_server(
                host="127.0.0.1",
                port=0,
                authorization_prompt_message=(
                    "Open this URL in your browser to authorize Octopal:\n{url}\n"
                ),
                success_message="Google authorization complete. You can close this tab.",
                open_browser=True,
                access_type="offline",
                prompt="consent",
            )

            settings["refresh_token"] = credentials.refresh_token
            settings["token"] = credentials.token
            settings["authorized_services"] = self._get_enabled_services()
            self.manager.save_config()

            if self.manager.mcp_manager is not None:
                await self.start()

            return {
                "status": "success",
                "message": (
                    "Google connector authorized for "
                    f"{', '.join(self._get_enabled_services())}."
                ),
            }
        except Exception as e:
            logger.exception("Failed to authorize Google connector")
            return {"error": f"Failed to authorize Google connector: {e}"}

    async def start(self) -> None:
        """Start the Gmail MCP server when the connector is ready."""
        if self.manager.mcp_manager is None:
            return

        config = self._get_config()
        if not config or not config.enabled:
            return

        settings = config.settings
        enabled_services = self._get_enabled_services()
        if "gmail" not in enabled_services:
            return

        from octopal.infrastructure.mcp.manager import MCPServerConfig

        common_env = {
            "GOOGLE_CLIENT_ID": settings.get("client_id"),
            "GOOGLE_CLIENT_SECRET": settings.get("client_secret"),
            "GOOGLE_REFRESH_TOKEN": settings.get("refresh_token"),
        }

        if "gmail" in enabled_services:
            gmail_cfg = MCPServerConfig(
                id="google-gmail",
                name="Gmail Connector",
                command="npx",
                args=["-y", "@modelcontextprotocol/server-gmail"],
                env=common_env,
                transport="stdio"
            )
            await self.manager.mcp_manager.connect_server(gmail_cfg)
