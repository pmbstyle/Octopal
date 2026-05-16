from __future__ import annotations

import secrets
from dataclasses import dataclass

from fastapi import HTTPException, Request

from octopal.infrastructure.config.models import A2AConfig, A2APeerConfig


@dataclass(frozen=True)
class AuthenticatedPeer:
    peer_id: str
    config: A2APeerConfig


def require_a2a_enabled(config: A2AConfig) -> None:
    if not config.enabled:
        raise HTTPException(status_code=404, detail="A2A interop is not enabled")


def authenticate_peer(request: Request, config: A2AConfig) -> AuthenticatedPeer:
    require_a2a_enabled(config)
    authorization = str(request.headers.get("authorization") or "").strip()
    scheme, _, credential = authorization.partition(" ")
    if scheme.lower() != "bearer" or not credential.strip():
        raise HTTPException(status_code=401, detail="Missing A2A peer bearer token")

    provided = credential.strip()
    for peer_id, peer in config.peers.items():
        expected = str(peer.token or "").strip()
        if peer.enabled and expected and secrets.compare_digest(provided, expected):
            return AuthenticatedPeer(peer_id=peer_id, config=peer)
    raise HTTPException(status_code=403, detail="Invalid A2A peer token")


def require_peer_capability(peer: AuthenticatedPeer, capability: str) -> None:
    capabilities = {str(item).strip().lower() for item in peer.config.capabilities}
    if capability.strip().lower() not in capabilities:
        raise HTTPException(status_code=403, detail=f"A2A peer lacks {capability!r} capability")

