from __future__ import annotations

import json
from typing import Any

from octopal.infrastructure.providers.base import Message
from octopal.runtime.octo.route_loop_helpers import normalize_plain_text
from octopal.runtime.octo.route_planning import _extract_json_object
from octopal.utils import (
    sanitize_user_facing_text_preserving_reaction,
    should_suppress_user_delivery,
)


def _messages_include_tool_call(messages: list[Message | dict[str, Any]], tool_name: str) -> bool:
    normalized = str(tool_name or "").strip().lower()
    if not normalized:
        return False
    for message in messages:
        if isinstance(message, dict):
            if str(message.get("name") or "").strip().lower() == normalized:
                return True
            tool_calls = message.get("tool_calls") or []
        else:
            if str(getattr(message, "name", "") or "").strip().lower() == normalized:
                return True
            tool_calls = getattr(message, "tool_calls", None) or []
        if not isinstance(tool_calls, list):
            continue
        for call in tool_calls:
            if not isinstance(call, dict):
                continue
            name = str((call.get("function") or {}).get("name") or "").strip().lower()
            if name == normalized:
                return True
    return False


def _normalize_worker_followup_reply(raw: str) -> str:
    value = normalize_plain_text(raw or "")
    if not value:
        return "NO_USER_RESPONSE"
    if should_suppress_user_delivery(value):
        return "NO_USER_RESPONSE"

    payload = _extract_json_object(value)
    if isinstance(payload, dict):
        if bool(payload.get("no_user_response")):
            return "NO_USER_RESPONSE"
        response = payload.get("user_response")
        if response is None:
            response = payload.get("response")
        if response is None:
            response = payload.get("message")
        response_text = sanitize_user_facing_text_preserving_reaction(str(response or ""))
        if response_text and not should_suppress_user_delivery(response_text):
            return response_text
        return "NO_USER_RESPONSE"

    cleaned = sanitize_user_facing_text_preserving_reaction(value)
    if should_suppress_user_delivery(cleaned):
        return "NO_USER_RESPONSE"
    return cleaned


def _normalize_proactive_reply(raw: str) -> str:
    value = normalize_plain_text(raw or "")
    if not value or should_suppress_user_delivery(value):
        return "NO_USER_RESPONSE"
    payload = _extract_json_object(value)
    if not isinstance(payload, dict):
        return "NO_USER_RESPONSE"

    decision = str(payload.get("decision", "noop") or "noop").strip().lower()
    if decision not in {"noop", "queue", "claim", "execute", "repair", "blocked"}:
        decision = "noop"
    risk = str(payload.get("risk", "low") or "low").strip().lower()
    if risk not in {"low", "medium", "high"}:
        risk = "low"
    try:
        confidence = float(payload.get("confidence", 0.0) or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    normalized = {
        "decision": decision,
        "confidence": confidence,
        "risk": risk,
        "requires_user_input": bool(payload.get("requires_user_input")),
        "selected_item_id": payload.get("selected_item_id") or None,
        "queued_item_id": payload.get("queued_item_id") or None,
        "reason": str(payload.get("reason", "") or "").strip()[:500],
    }
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True)


def _looks_like_tool_error(text: str) -> bool:
    lowered = text.lower()
    return " error" in lowered or "failed" in lowered
