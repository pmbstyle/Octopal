from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from octopal.infrastructure.config.models import A2APeerConfig

A2A_CAPABILITY_CHAT = "chat"
A2A_CAPABILITY_DATA = "data"
A2A_CAPABILITY_FILES_RAW = "files:raw"
A2A_CAPABILITY_FILES_URL = "files:url"


def normalize_capabilities(capabilities: Iterable[Any]) -> set[str]:
    return {str(item).strip().lower() for item in capabilities if str(item).strip()}


def missing_peer_capabilities(
    peer: A2APeerConfig,
    required: Iterable[str],
) -> list[str]:
    allowed = normalize_capabilities(peer.capabilities)
    return [capability for capability in required if capability not in allowed]


def outbound_required_capabilities(
    *,
    data: Any,
    file_urls: list[dict[str, Any]] | None,
    raw_files: list[dict[str, Any]] | None,
) -> list[str]:
    required = [A2A_CAPABILITY_CHAT]
    if data is not None:
        required.append(A2A_CAPABILITY_DATA)
    if file_urls:
        required.append(A2A_CAPABILITY_FILES_URL)
    if raw_files:
        required.append(A2A_CAPABILITY_FILES_RAW)
    return _dedupe(required)


def message_required_capabilities(parts: Iterable[Any]) -> list[str]:
    required = [A2A_CAPABILITY_CHAT]
    for part in parts:
        if getattr(part, "data", None) is not None:
            required.append(A2A_CAPABILITY_DATA)
        if getattr(part, "url", None) is not None:
            required.append(A2A_CAPABILITY_FILES_URL)
        if getattr(part, "raw", None) is not None:
            required.append(A2A_CAPABILITY_FILES_RAW)
    return _dedupe(required)


def _dedupe(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result
