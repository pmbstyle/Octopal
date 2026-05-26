from __future__ import annotations

import base64
from typing import Any
from uuid import uuid4

import httpx

from octopal.infrastructure.config.models import A2AConfig, A2APeerConfig
from octopal.interop.a2a.capabilities import (
    A2A_CAPABILITY_CHAT,
    missing_peer_capabilities,
    outbound_required_capabilities,
)


class A2AClientError(RuntimeError):
    pass


async def send_peer_message(
    config: A2AConfig,
    *,
    peer_id: str,
    text: str | None = None,
    data: Any = None,
    file_urls: list[dict[str, Any]] | None = None,
    raw_files: list[dict[str, Any]] | None = None,
    context_id: str | None = None,
    timeout_seconds: float = 60.0,
) -> dict[str, Any]:
    peer = config.peers.get(peer_id)
    if peer is None or not peer.enabled:
        raise A2AClientError(f"A2A peer {peer_id!r} is not configured or enabled.")
    missing = missing_peer_capabilities(
        peer,
        outbound_required_capabilities(data=data, file_urls=file_urls, raw_files=raw_files),
    )
    if missing == [A2A_CAPABILITY_CHAT]:
        raise A2AClientError(f"A2A peer {peer_id!r} does not allow chat.")
    if missing:
        raise A2AClientError(
            f"A2A peer {peer_id!r} does not allow required capabilities: " f"{', '.join(missing)}."
        )
    endpoint = _message_send_endpoint(peer)
    token = str(peer.token or "").strip()
    if not token:
        raise A2AClientError(f"A2A peer {peer_id!r} has no bearer token configured.")

    parts = _build_message_parts(
        text=text,
        data=data,
        file_urls=file_urls,
        raw_files=raw_files,
    )
    if not parts:
        raise A2AClientError("A2A message requires text, data, file_urls, or raw_files.")

    payload = {
        "message": {
            "role": "ROLE_USER",
            "parts": parts,
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


def _build_message_parts(
    *,
    text: str | None,
    data: Any,
    file_urls: list[dict[str, Any]] | None,
    raw_files: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = []
    text_value = str(text or "").strip()
    if text_value:
        parts.append({"text": text_value, "mediaType": "text/plain"})
    if data is not None:
        parts.append({"data": data, "mediaType": "application/json"})
    for item in file_urls or []:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        part: dict[str, Any] = {"url": url}
        _copy_optional_part_fields(part, item)
        parts.append(part)
    for item in raw_files or []:
        if not isinstance(item, dict):
            continue
        raw = str(item.get("raw") or "").strip()
        if not raw:
            binary = item.get("bytes")
            if isinstance(binary, bytes):
                raw = base64.b64encode(binary).decode("ascii")
        if not raw:
            continue
        part = {"raw": raw}
        _copy_optional_part_fields(part, item)
        parts.append(part)
    return parts


def _copy_optional_part_fields(target: dict[str, Any], source: dict[str, Any]) -> None:
    filename = str(source.get("filename") or "").strip()
    if filename:
        target["filename"] = filename
    media_type = str(source.get("media_type") or source.get("mediaType") or "").strip()
    if media_type:
        target["mediaType"] = media_type
    metadata = source.get("metadata")
    if isinstance(metadata, dict) and metadata:
        target["metadata"] = metadata


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
