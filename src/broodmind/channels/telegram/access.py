from __future__ import annotations

import structlog

logger = structlog.get_logger(__name__)


def parse_allowed_chat_ids(raw: str) -> set[int]:
    allowed: set[int] = set()
    for part in (raw or "").split(","):
        candidate = part.strip()
        if not candidate:
            continue
        try:
            allowed.add(int(candidate))
        except ValueError:
            logger.warning("Ignoring invalid allowed chat ID", value=candidate)
    return allowed


def is_allowed_chat(chat_id: int, allowed_chat_ids: set[int]) -> bool:
    return not allowed_chat_ids or chat_id in allowed_chat_ids
