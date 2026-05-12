from __future__ import annotations

import re

import structlog

from octopal.infrastructure.providers.base import InferenceProvider, Message
from octopal.runtime.octo.router import _complete_text, normalize_plain_text
from octopal.runtime.scheduler.service import normalize_notify_user_policy
from octopal.utils import (
    extract_heartbeat_user_visible_message,
    has_no_user_response_suffix,
    is_control_response,
)

logger = structlog.get_logger(__name__)

_SCHEDULED_OCTO_CONTROL_DONE = "SCHEDULED_TASK_DONE"
_SCHEDULED_OCTO_CONTROL_BLOCKED = "SCHEDULED_TASK_BLOCKED"
_SCHEDULED_OCTO_CONTROL_BLOCKED_MARKERS = (
    "bounded `octo_control` route",
    "bounded octo_control route",
    "requires external network access",
    "cannot be performed from the bounded",
    "no workers may be launched",
    "no direct tools available",
    "requires a worker",
)


def _coerce_control_plane_reply(text: str) -> str:
    """Normalize internal control-plane replies to a strict channel-safe token."""
    value = normalize_plain_text(text or "")
    if is_control_response(value):
        return value
    if has_no_user_response_suffix(value):
        return "NO_USER_RESPONSE"
    return "HEARTBEAT_OK"


def _has_scheduler_idle_suffix(text: str) -> bool:
    value = (text or "").strip()
    if not value:
        return False
    trimmed = re.sub(r"[^\w]+$", "", value).strip()
    normalized = re.sub(r"[\s_-]+", "", trimmed).upper()
    return normalized.endswith("SCHEDULERIDLE")


async def _normalize_heartbeat_delivery_reply(provider: InferenceProvider | None, text: str) -> str:
    """Normalize heartbeat output to the explicit delivery contract."""
    raw_value = str(text or "")
    explicit = extract_heartbeat_user_visible_message(raw_value)
    if explicit:
        return explicit
    value = normalize_plain_text(raw_value)
    if is_control_response(value) or has_no_user_response_suffix(value):
        return _coerce_control_plane_reply(value)
    if provider is None:
        return _coerce_control_plane_reply(value)

    rewrite_prompt = (
        "Rewrite the draft heartbeat reply into the strict heartbeat delivery contract.\n"
        "Return exactly one of:\n"
        "- HEARTBEAT_OK\n"
        "- NO_USER_RESPONSE\n"
        "- <user_visible>...</user_visible>\n"
        "Use <user_visible> only for a completed result that is explicitly user-facing.\n"
        "Do not include planning, self-talk, tool notes, or any extra text outside the wrapper."
    )
    try:
        rewritten = await _complete_text(
            provider,
            [
                Message(role="system", content=rewrite_prompt),
                Message(role="user", content=f"<draft>\n{value}\n</draft>"),
            ],
            context="heartbeat_delivery_rewrite",
        )
    except Exception:
        logger.debug("Heartbeat delivery rewrite failed", exc_info=True)
        return _coerce_control_plane_reply(value)

    explicit = extract_heartbeat_user_visible_message(rewritten)
    if explicit:
        return explicit
    return _coerce_control_plane_reply(rewritten)


def _looks_like_scheduled_octo_control_route_block(text: str) -> bool:
    value = normalize_plain_text(text or "").casefold()
    if not value:
        return False
    return any(marker in value for marker in _SCHEDULED_OCTO_CONTROL_BLOCKED_MARKERS)


def _coerce_scheduled_octo_control_reply(text: str) -> str:
    raw_value = str(text or "")
    explicit = extract_heartbeat_user_visible_message(raw_value)
    if explicit:
        return explicit
    value = normalize_plain_text(raw_value)
    normalized_upper = value.strip().upper()
    if normalized_upper == _SCHEDULED_OCTO_CONTROL_DONE:
        return _SCHEDULED_OCTO_CONTROL_DONE
    if normalized_upper == _SCHEDULED_OCTO_CONTROL_BLOCKED:
        return _SCHEDULED_OCTO_CONTROL_BLOCKED
    if normalized_upper == "NO_USER_RESPONSE" or has_no_user_response_suffix(value):
        return "NO_USER_RESPONSE"
    return "NO_USER_RESPONSE"


async def _normalize_scheduled_octo_control_reply(
    provider: InferenceProvider | None,
    text: str,
) -> str:
    raw_value = str(text or "")
    explicit = extract_heartbeat_user_visible_message(raw_value)
    if explicit:
        return explicit
    value = normalize_plain_text(raw_value)
    normalized_upper = value.strip().upper()
    if normalized_upper == _SCHEDULED_OCTO_CONTROL_DONE:
        return _SCHEDULED_OCTO_CONTROL_DONE
    if normalized_upper == _SCHEDULED_OCTO_CONTROL_BLOCKED:
        return _SCHEDULED_OCTO_CONTROL_BLOCKED
    if normalized_upper == "NO_USER_RESPONSE" or has_no_user_response_suffix(value):
        return "NO_USER_RESPONSE"
    if provider is None:
        return _coerce_scheduled_octo_control_reply(value)

    rewrite_prompt = (
        "Rewrite the draft scheduled Octo control reply into the strict completion contract.\n"
        "Return exactly one of:\n"
        "- SCHEDULED_TASK_DONE\n"
        "- SCHEDULED_TASK_BLOCKED\n"
        "- NO_USER_RESPONSE\n"
        "- <user_visible>...</user_visible>\n"
        "Use SCHEDULED_TASK_DONE only if the task completed successfully with no user-visible update.\n"
        "Use SCHEDULED_TASK_BLOCKED when the task cannot complete from the bounded route because it needs workers, external access, or unavailable tools.\n"
        "Use <user_visible> only for a concise completed user-facing update.\n"
        "Use NO_USER_RESPONSE if the task did not complete or there is no completion signal.\n"
        "Do not include any extra text outside the token or wrapper."
    )
    try:
        rewritten = await _complete_text(
            provider,
            [
                Message(role="system", content=rewrite_prompt),
                Message(role="user", content=f"<draft>\n{value}\n</draft>"),
            ],
            context="scheduled_octo_control_delivery_rewrite",
        )
    except Exception:
        logger.debug("Scheduled Octo control delivery rewrite failed", exc_info=True)
        return _coerce_scheduled_octo_control_reply(value)

    return _coerce_scheduled_octo_control_reply(rewritten)


def _normalize_scheduled_octo_control_notify_policy(notify_user: str | None) -> str:
    policy = normalize_notify_user_policy(notify_user)
    if policy == "if_significant":
        return "never"
    return policy
