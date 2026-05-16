from __future__ import annotations

from typing import Any
from uuid import uuid4

import httpx

from octopal.infrastructure.config.models import A2AConfig, A2APeerConfig


class A2AClientError(RuntimeError):
    pass


async def send_peer_message(
    config: A2AConfig,
    *,
    peer_id: str,
    text: str,
    context_id: str | None = None,
    timeout_seconds: float = 60.0,
) -> dict[str, Any]:
    peer = config.peers.get(peer_id)
    if peer is None or not peer.enabled:
        raise A2AClientError(f"A2A peer {peer_id!r} is not configured or enabled.")
    if "chat" not in {item.strip().lower() for item in peer.capabilities}:
        raise A2AClientError(f"A2A peer {peer_id!r} does not allow chat.")
    endpoint = _message_send_endpoint(peer)
    token = str(peer.token or "").strip()
    if not token:
        raise A2AClientError(f"A2A peer {peer_id!r} has no bearer token configured.")

    payload = {
        "message": {
            "role": "ROLE_USER",
            "parts": [{"text": text}],
            "messageId": f"octopal-message-{uuid4().hex}",
            "contextId": context_id or f"octopal-peer-{peer_id}",
        }
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "A2A-Version": config.protocol_version,
    }
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        response = await client.post(endpoint, headers=headers, json=payload)
    if response.status_code >= 400:
        raise A2AClientError(
            f"A2A peer {peer_id!r} returned HTTP {response.status_code}: {response.text[:500]}"
        )
    data = response.json()
    if not isinstance(data, dict):
        raise A2AClientError(f"A2A peer {peer_id!r} returned a non-object response.")
    return data


def _message_send_endpoint(peer: A2APeerConfig) -> str:
    base_url = str(peer.base_url or "").strip()
    if not base_url:
        card_url = str(peer.agent_card_url or "").strip()
        suffix = "/.well-known/agent-card.json"
        if card_url.endswith(suffix):
            base_url = card_url[: -len(suffix)] + "/a2a/v1"
    if not base_url:
        raise A2AClientError("A2A peer requires base_url or agent_card_url.")
    return base_url.rstrip("/") + "/message:send"
