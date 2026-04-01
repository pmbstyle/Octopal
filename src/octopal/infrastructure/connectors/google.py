from __future__ import annotations

import os
import sys
import webbrowser
from contextlib import contextmanager
from typing import Any
from urllib.parse import parse_qs, urlparse

import structlog

from octopal.infrastructure.connectors.base import Connector

logger = structlog.get_logger(__name__)


@contextmanager
def _oauthlib_insecure_transport_for_localhost():
    """Allow oauthlib to parse localhost callback URLs in CLI/manual flows."""
    original = os.environ.get("OAUTHLIB_INSECURE_TRANSPORT")
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    try:
        yield
    finally:
        if original is None:
            os.environ.pop("OAUTHLIB_INSECURE_TRANSPORT", None)
        else:
            os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = original


class GoogleConnector(Connector):
    _SUPPORTED_SERVICES = ("gmail",)
    _MCP_SERVER_IDS = ("google-gmail",)

    def __init__(self, manager: Any):
        self.manager = manager
        self._pending_manual_auth: dict[str, str] | None = None
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

    def managed_server_ids(self) -> list[str]:
        return list(self._MCP_SERVER_IDS)

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

    def _get_scopes(self) -> list[str]:
        enabled = self._get_enabled_services()
        return [self._service_scopes[svc] for svc in enabled if svc in self._service_scopes]

    def _build_client_config(self, client_id: str, client_secret: str) -> dict[str, dict[str, str]]:
        return {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        }

    def _store_credentials(self, credentials) -> None:
        config = self._get_config()
        if config is None:
            raise RuntimeError("Google connector config disappeared during authorization.")
        config.auth.refresh_token = credentials.refresh_token
        config.auth.access_token = credentials.token
        config.auth.authorized_services = self._get_enabled_services()
        config.auth.last_error = None
        self.manager.save_config()

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

        if not config.credentials.client_id or not config.credentials.client_secret:
            return {
                "status": "not_configured",
                "message": "Missing client_id or client_secret",
                "services": enabled_services,
                "supported_services": self.supported_services(),
            }

        if not config.auth.refresh_token:
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

        for key, value in settings.items():
            if key == "client_id":
                config.credentials.client_id = str(value) if value is not None else None
            elif key == "client_secret":
                config.credentials.client_secret = str(value) if value is not None else None
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

        client_id = config.credentials.client_id
        client_secret = config.credentials.client_secret

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

        client_config = self._build_client_config(client_id, client_secret)

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
            self._store_credentials(credentials)

            if self.manager.mcp_manager is not None:
                await self.start()

            return {
                "status": "success",
                "message": (
                    "Google connector authorized for "
                    f"{', '.join(self._get_enabled_services())}."
                ),
            }
        except webbrowser.Error as e:
            if "could not locate runnable browser" in str(e).lower():
                return {
                    "status": "manual_required",
                    "message": "No runnable browser found on this machine.",
                    "error": str(e),
                }
            logger.exception("Failed to authorize Google connector")
            return {"error": f"Failed to authorize Google connector: {e}"}
        except Exception as e:
            logger.exception("Failed to authorize Google connector")
            return {"error": f"Failed to authorize Google connector: {e}"}

    async def begin_manual_authorize(self) -> dict[str, str]:
        """Return an auth URL for a headless/manual OAuth flow."""
        try:
            from google_auth_oauthlib.flow import InstalledAppFlow
        except ImportError as exc:
            raise RuntimeError("google-auth-oauthlib is not installed.") from exc

        config = self._get_config()
        if not config:
            raise RuntimeError("Google connector is not enabled. Run `octopal configure` first.")

        client_id = config.credentials.client_id
        client_secret = config.credentials.client_secret
        if not client_id or not client_secret:
            raise RuntimeError("Missing client_id or client_secret in Google connector settings.")

        scopes = self._get_scopes()
        if not scopes:
            raise RuntimeError("No supported Google services are enabled. Re-run `octopal configure`.")

        flow = InstalledAppFlow.from_client_config(
            self._build_client_config(client_id, client_secret),
            scopes=scopes,
        )
        flow.redirect_uri = "http://localhost"
        auth_url, state = flow.authorization_url(access_type="offline", prompt="consent")
        self._pending_manual_auth = {
            "redirect_uri": flow.redirect_uri,
            "code_verifier": str(flow.code_verifier or ""),
            "state": str(state or ""),
        }
        return {"auth_url": auth_url, "redirect_uri": flow.redirect_uri}

    async def complete_manual_authorize(self, authorization_response: str) -> dict[str, Any]:
        """Complete a manual OAuth flow from a pasted redirect URL or auth code."""
        try:
            from google_auth_oauthlib.flow import InstalledAppFlow
        except ImportError:
            return {"error": "google-auth-oauthlib is not installed."}

        config = self._get_config()
        if not config:
            return {"error": "Google connector is not enabled. Run `octopal configure` first."}

        client_id = config.credentials.client_id
        client_secret = config.credentials.client_secret
        if not client_id or not client_secret:
            return {"error": "Missing client_id or client_secret in Google connector settings."}

        flow = InstalledAppFlow.from_client_config(
            self._build_client_config(client_id, client_secret),
            scopes=self._get_scopes(),
        )
        pending = dict(self._pending_manual_auth or {})
        flow.redirect_uri = str(pending.get("redirect_uri") or "http://localhost")
        flow.code_verifier = pending.get("code_verifier") or None

        try:
            with _oauthlib_insecure_transport_for_localhost():
                if authorization_response.startswith("http://") or authorization_response.startswith("https://"):
                    parsed = urlparse(authorization_response)
                    params = parse_qs(parsed.query)
                    returned_state = str((params.get("state") or [""])[0] or "")
                    expected_state = str(pending.get("state") or "")
                    if expected_state and returned_state and returned_state != expected_state:
                        return {"error": "State mismatch while completing Google authorization."}
                    auth_code = str((params.get("code") or [""])[0] or "")
                    if not auth_code:
                        return {"error": "Authorization response URL did not contain a code."}
                    flow.fetch_token(code=auth_code)
                else:
                    flow.fetch_token(code=authorization_response)
            self._store_credentials(flow.credentials)
            self._pending_manual_auth = None

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
            logger.exception("Failed to complete manual Google authorization")
            return {"error": f"Failed to complete manual Google authorization: {e}"}

    async def start(self) -> None:
        """Start the Gmail MCP server when the connector is ready."""
        if self.manager.mcp_manager is None:
            return

        config = self._get_config()
        if not config or not config.enabled:
            return

        enabled_services = self._get_enabled_services()
        if "gmail" not in enabled_services:
            return

        from octopal.infrastructure.mcp.manager import MCPServerConfig

        common_env = {
            "GMAIL_CLIENT_ID": config.credentials.client_id,
            "GMAIL_CLIENT_SECRET": config.credentials.client_secret,
            "GMAIL_REFRESH_TOKEN": config.auth.refresh_token,
        }

        if "gmail" in enabled_services:
            gmail_cfg = MCPServerConfig(
                id="google-gmail",
                name="Gmail Connector",
                command=sys.executable,
                args=["-m", "octopal.mcp_servers.gmail"],
                env=common_env,
                transport="stdio"
            )
            await self.manager.mcp_manager.connect_server(gmail_cfg)

    async def disconnect(self, *, forget_credentials: bool = False) -> dict[str, Any]:
        """Disconnect Gmail integration and clear authorization state."""
        config = self._get_config()
        if not config:
            return {"status": "noop", "message": "Google connector is not configured."}

        if self.manager.mcp_manager is not None:
            for server_id in self._MCP_SERVER_IDS:
                try:
                    await self.manager.mcp_manager.disconnect_server(server_id, intentional=True)
                except Exception:
                    logger.warning("Failed to disconnect MCP server for Google connector", server_id=server_id, exc_info=True)

        config.auth.refresh_token = None
        config.auth.access_token = None
        config.auth.authorized_services = []
        config.auth.last_error = None
        if forget_credentials:
            config.credentials.client_id = None
            config.credentials.client_secret = None

        self.manager.save_config()

        if forget_credentials:
            return {
                "status": "success",
                "message": "Google connector disconnected and stored client credentials were removed.",
            }
        return {
            "status": "success",
            "message": "Google connector disconnected. Client credentials were kept for easy re-authorization.",
        }
