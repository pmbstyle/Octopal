from __future__ import annotations

import json
import re
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

    jsonish_response = _extract_jsonish_worker_user_response(value)
    if jsonish_response is not None:
        return jsonish_response

    cleaned = sanitize_user_facing_text_preserving_reaction(value)
    if should_suppress_user_delivery(cleaned):
        return "NO_USER_RESPONSE"
    return cleaned


def _extract_jsonish_worker_user_response(value: str) -> str | None:
    if not re.search(r'"(?:user_response|no_user_response)"\s*:', value):
        return None

    response_match = re.search(r'"user_response"\s*:\s*', value, flags=re.IGNORECASE)
    if response_match is None:
        if _jsonish_no_user_response_is_true(value):
            return "NO_USER_RESPONSE"
        return None

    value_start = response_match.end()
    while value_start < len(value) and value[value_start].isspace():
        value_start += 1

    if value[value_start : value_start + 4].lower() == "null":
        if _jsonish_no_user_response_is_true(value[value_start + 4 :]):
            return "NO_USER_RESPONSE"
        return "NO_USER_RESPONSE"

    if value_start >= len(value) or value[value_start] != '"':
        return None

    content_start = value_start + 1
    delimiter = re.search(
        r'"\s*,\s*"(?:no_user_response|actions_taken|reason|response|message)"\s*:',
        value[content_start:],
        flags=re.DOTALL | re.IGNORECASE,
    )
    if delimiter is not None:
        content_end = content_start + delimiter.start()
    else:
        string_match = re.match(r'"(?:\\.|[^"\\])*"', value[value_start:], flags=re.DOTALL)
        if string_match is None:
            return None
        content_end = value_start + len(string_match.group(0)) - 1

    if _jsonish_no_user_response_is_true(value[content_end + 1 :]):
        return "NO_USER_RESPONSE"

    response = _decode_jsonish_string_inner(value[content_start:content_end])

    response_text = sanitize_user_facing_text_preserving_reaction(str(response or ""))
    if response_text and not should_suppress_user_delivery(response_text):
        return response_text
    return "NO_USER_RESPONSE"


def _jsonish_no_user_response_is_true(value: str) -> bool:
    return bool(re.search(r'"no_user_response"\s*:\s*true\b', value, flags=re.IGNORECASE))


def _decode_jsonish_string_inner(value: str) -> str:
    repaired = value.translate(str.maketrans({"\n": r"\n", "\r": r"\r", "\t": r"\t"}))
    repaired = re.sub(r'(?<!\\)"', r'\\"', repaired)
    try:
        decoded = json.loads(f'"{repaired}"')
    except json.JSONDecodeError:
        return value
    return str(decoded)


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
