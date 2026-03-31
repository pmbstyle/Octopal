from __future__ import annotations

import json
from typing import Any

import structlog

from octopal.infrastructure.connectors.base import Connector

logger = structlog.get_logger(__name__)

class GoogleConnector(Connector):
    def __init__(self, manager: Any):
        self.manager = manager
        self._scopes = [
            "https://www.googleapis.com/auth/gmail.modify",
            "https://www.googleapis.com/auth/calendar",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/documents",
        ]

    @property
    def name(self) -> str:
        return "google"

    async def get_status(self) -> dict[str, Any]:
        config = self.manager.config.instances.get(self.name)
        if not config or not config.enabled:
            return {"status": "disabled"}
        
        settings = config.settings
        if not settings.get("client_id") or not settings.get("client_secret"):
            return {"status": "not_configured", "message": "Missing client_id or client_secret"}
        
        if not settings.get("refresh_token"):
            return {"status": "needs_auth", "message": "Connector configured but needs user authorization"}
        
        return {"status": "ready", "message": "Google connector is ready"}

    async def configure(self, settings: dict[str, Any]) -> None:
        """Update settings for the Google connector."""
        config = self.manager.config.instances.get(self.name)
        if not config:
            from octopal.infrastructure.config.models import ConnectorInstanceConfig
            config = ConnectorInstanceConfig(enabled=True)
            self.manager.config.instances[self.name] = config
        
        # Merge settings
        config.settings.update(settings)
        config.enabled = True
        
        # Save config
        self.manager.save_config()

    async def setup(self) -> dict[str, Any]:
        """Start OAuth2 flow."""
        try:
            from google_auth_oauthlib.flow import Flow
        except ImportError:
            return {"error": "google-auth-oauthlib is not installed. Please install it to use the Google connector."}

        config = self.manager.config.instances.get(self.name)
        if not config:
            return {"error": "Google connector not configured. Call configure first."}
        
        settings = config.settings
        client_id = settings.get("client_id")
        client_secret = settings.get("client_secret")
        
        if not client_id or not client_secret:
            return {"error": "Missing client_id or client_secret in Google connector settings."}

        client_config = {
            "web": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        }

        flow = Flow.from_client_config(
            client_config,
            scopes=self._scopes,
            redirect_uri="urn:ietf:wg:oauth:2.0:oob" # Out-of-band for CLI/Bot
        )

        auth_url, _ = flow.authorization_url(prompt='consent', access_type='offline')
        
        return {
            "auth_url": auth_url,
            "message": "Please visit the URL above to authorize Octo to access your Google services. After authorizing, you will receive a code. Provide this code to complete the setup."
        }

    async def complete_setup(self, data: dict[str, Any]) -> dict[str, Any]:
        """Complete OAuth2 flow with auth code."""
        try:
            from google_auth_oauthlib.flow import Flow
        except ImportError:
            return {"error": "google-auth-oauthlib is not installed."}

        auth_code = data.get("auth_code")
        if not auth_code:
            return {"error": "Missing auth_code."}

        config = self.manager.config.instances.get(self.name)
        settings = config.settings
        
        client_config = {
            "web": {
                "client_id": settings.get("client_id"),
                "client_secret": settings.get("client_secret"),
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        }

        flow = Flow.from_client_config(
            client_config,
            scopes=self._scopes,
            redirect_uri="urn:ietf:wg:oauth:2.0:oob"
        )

        try:
            flow.fetch_token(code=auth_code)
            credentials = flow.credentials
            
            # Store tokens
            settings["refresh_token"] = credentials.refresh_token
            settings["token"] = credentials.token
            
            # Save config
            self.manager.save_config()
            
            # Register MCP server automatically
            await self._register_mcp_server()
            
            return {"status": "success", "message": "Google connector setup complete and MCP server registered."}
        except Exception as e:
            logger.exception("Failed to complete Google connector setup")
            return {"error": f"Failed to exchange code for tokens: {e}"}

    async def _register_mcp_server(self) -> None:
        """Register the Google MCP server in mcp_servers.json."""
        config = self.manager.config.instances.get(self.name)
        settings = config.settings
        
        # We can use the community-maintained Google MCP server
        # This requires node/npx.
        
        server_id = "google-connector"
        mcp_config = {
            "name": "Google Connector (Gmail, Calendar, Drive)",
            "command": "npx",
            "args": ["-y", "@modelcontextprotocol/server-google-drive"], # Note: this is just one, we might want a combined one or multiple
            "env": {
                "GOOGLE_CLIENT_ID": settings.get("client_id"),
                "GOOGLE_CLIENT_SECRET": settings.get("client_secret"),
                "GOOGLE_REFRESH_TOKEN": settings.get("refresh_token"),
            }
        }
        
        # Actually there are separate servers for Gmail and Drive.
        # For simplicity in this prototype, let's just register Drive.
        # In a real implementation, we'd register multiple or a combined one.
        
        from octopal.infrastructure.mcp.manager import MCPServerConfig
        
        cfg = MCPServerConfig(
            id=server_id,
            name=mcp_config["name"],
            command=mcp_config["command"],
            args=mcp_config["args"],
            env=mcp_config["env"],
            transport="stdio"
        )
        
        await self.manager.mcp_manager.connect_server(cfg)
        
        # Also register Gmail if possible
        gmail_mcp_config = {
            "id": "gmail-connector",
            "name": "Gmail Connector",
            "command": "npx",
            "args": ["-y", "@modelcontextprotocol/server-gmail"],
            "env": {
                "GOOGLE_CLIENT_ID": settings.get("client_id"),
                "GOOGLE_CLIENT_SECRET": settings.get("client_secret"),
                "GOOGLE_REFRESH_TOKEN": settings.get("refresh_token"),
            }
        }
        
        cfg_gmail = MCPServerConfig(
            id=gmail_mcp_config["id"],
            name=gmail_mcp_config["name"],
            command=gmail_mcp_config["command"],
            args=gmail_mcp_config["args"],
            env=gmail_mcp_config["env"],
            transport="stdio"
        )
        await self.manager.mcp_manager.connect_server(cfg_gmail)
