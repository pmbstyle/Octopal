from __future__ import annotations

from urllib.parse import urljoin

from octopal.infrastructure.config.models import A2AConfig


def build_agent_card(config: A2AConfig, *, base_url: str) -> dict[str, object]:
    root_url = (config.public_base_url or base_url).rstrip("/") + "/"
    interface_url = urljoin(root_url, "a2a/v1")
    return {
        "name": config.agent_name,
        "description": config.agent_description,
        "version": "1.0.0",
        "supportedInterfaces": [
            {
                "url": interface_url,
                "protocolBinding": "HTTP+JSON",
                "protocolVersion": config.protocol_version,
            }
        ],
        "capabilities": {
            "streaming": False,
            "pushNotifications": False,
            "extendedAgentCard": False,
        },
        "securitySchemes": {
            "peerBearer": {
                "type": "http",
                "scheme": "bearer",
                "description": "Invite-only peer token configured in Octopal.",
            }
        },
        "securityRequirements": [{"peerBearer": []}],
        "defaultInputModes": [
            "text/plain",
            "application/json",
            "application/octet-stream",
            "image/png",
            "image/jpeg",
            "application/pdf",
        ],
        "defaultOutputModes": ["text/plain", "application/json"],
        "skills": [
            {
                "id": "peer-chat",
                "name": "Trusted Peer Chat",
                "description": (
                    "Accepts text, structured data, and file parts from authenticated "
                    "trusted peer agents and routes them through Octopal policy."
                ),
                "tags": ["chat", "agent-to-agent", "trusted-peer"],
                "examples": ["Send a private note or task payload to this Octopal instance."],
                "inputModes": [
                    "text/plain",
                    "application/json",
                    "application/octet-stream",
                    "image/png",
                    "image/jpeg",
                    "application/pdf",
                ],
                "outputModes": ["text/plain", "application/json"],
            }
        ],
    }
