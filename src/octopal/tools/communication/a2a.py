from __future__ import annotations

import json
from typing import Any

from octopal.infrastructure.config.models import A2AConfig
from octopal.interop.a2a.client import A2AClientError, send_peer_message


async def a2a_send_message(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    peer_id = str((args or {}).get("peer_id") or "").strip()
    text = str((args or {}).get("text") or "").strip()
    context_id = str((args or {}).get("context_id") or "").strip() or None
    if not peer_id:
        return _json({"status": "error", "message": "peer_id is required."})
    if not text:
        return _json({"status": "error", "message": "text is required."})

    config = _resolve_a2a_config(ctx)
    if not config.enabled:
        return _json({"status": "error", "message": "A2A interop is disabled."})
    try:
        payload = await send_peer_message(config, peer_id=peer_id, text=text, context_id=context_id)
    except A2AClientError as exc:
        return _json({"status": "error", "message": str(exc)})
    except Exception as exc:
        return _json({"status": "error", "message": f"A2A request failed: {exc}"})
    return _json(
        {
            "status": "ok",
            "peer_id": peer_id,
            "context_id": context_id or f"octopal-peer-{peer_id}",
            "response": payload,
        }
    )


def a2a_list_peers(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    config = _resolve_a2a_config(ctx)
    peers: list[dict[str, Any]] = []
    for peer_id, peer in sorted(config.peers.items()):
        if not peer.enabled:
            continue
        peers.append(
            {
                "peer_id": peer_id,
                "name": peer.name or peer_id,
                "capabilities": list(peer.capabilities),
                "trust_level": peer.trust_level,
                "has_base_url": bool(str(peer.base_url or "").strip()),
                "has_agent_card_url": bool(str(peer.agent_card_url or "").strip()),
            }
        )
    return _json(
        {
            "status": "ok",
            "enabled": config.enabled,
            "count": len(peers),
            "peers": peers,
        }
    )


def _resolve_a2a_config(ctx: dict[str, Any]) -> A2AConfig:
    octo = (ctx or {}).get("octo")
    runtime_settings = getattr(getattr(octo, "runtime", None), "settings", None)
    candidate = getattr(runtime_settings, "a2a", None)
    if isinstance(candidate, A2AConfig):
        return candidate
    config_obj = getattr(runtime_settings, "config_obj", None)
    candidate = getattr(config_obj, "a2a", None)
    if isinstance(candidate, A2AConfig):
        return candidate
    return A2AConfig()


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)
