from __future__ import annotations

import json
import re
from typing import Any

from octopal.tools.registry import ToolSpec
from octopal.utils import sanitize_user_facing_text

_TEXTUAL_TOOL_NAME_RE = re.compile(r"^(?:mcp__[\w-]+__)?[a-z][a-z0-9_]{1,63}$", re.IGNORECASE)
_TEXTUAL_TOOL_PREVIEW_RE = re.compile(
    r"^(?P<tool>(?:mcp__[\w-]+__)?[a-z][a-z0-9_]{1,63})(?P<rest>(?:,\s*[a-z_][a-z0-9_ -]{0,31}:\s*[^,\n]{1,200})+)$",
    re.IGNORECASE,
)


def normalize_plain_text(text: str) -> str:
    return sanitize_user_facing_text(text or "")


def _tool_result_payload_error_type(result: Any) -> str | None:
    payload = _parse_tool_result_payload(result)
    if not isinstance(payload, dict):
        return None

    status = str(payload.get("status") or "").strip().lower()
    if status in {"error", "failed", "failure"}:
        return str(payload.get("error_type") or "structured_error_status")

    state = str(payload.get("state") or "").strip().lower()
    if state in {"error", "failed", "failure"}:
        return str(payload.get("error_type") or "structured_error_state")

    if payload.get("ok") is False:
        return str(payload.get("error_type") or "structured_not_ok")

    if _has_meaningful_error_value(payload.get("error")):
        return str(payload.get("error_type") or "structured_error_field")

    if _has_meaningful_error_value(payload.get("errors")):
        return str(payload.get("error_type") or "structured_errors_field")

    return None


def _parse_tool_result_payload(result: Any) -> Any:
    if isinstance(result, str):
        try:
            return json.loads(result)
        except Exception:
            return None
    return result


def _has_meaningful_error_value(value: Any) -> bool:
    if value is None or value is False:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _is_context_overflow_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "maximum context length",
            "input tokens exceeds",
            "context length",
            "too many tokens",
        )
    )


def _exception_chain_text(exc: Exception) -> str:
    parts: list[str] = []
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen and len(parts) < 8:
        seen.add(id(current))
        text = str(current).strip()
        if text:
            parts.append(text)
        current = current.__cause__ or current.__context__
    return " | ".join(parts)


def _is_transient_provider_error(exc: Exception) -> bool:
    text = _exception_chain_text(exc).lower()
    return any(
        marker in text
        for marker in (
            "timeout",
            "timed out",
            "sockettimeout",
            "apitimeouterror",
            "rate limit",
            "ratelimit",
            "429",
            "502",
            "503",
            "504",
            "service unavailable",
            "connection error",
            "connection reset",
            "client has been closed",
            "apiconnectionerror",
            "temporary",
            "temporarily unavailable",
        )
    )


def _recover_textual_tool_call(content: str, tools: list[ToolSpec]) -> dict[str, Any] | None:
    """Recover a malformed tool invocation when the model emits tool syntax as plain text."""
    raw = normalize_plain_text(content or "")
    if not raw or "\n" in raw or len(raw) > 300:
        return None

    trimmed = re.sub(r"^[\s\W_]+", "", raw, flags=re.UNICODE)
    trimmed = re.sub(r"[\s\W_]+$", "", trimmed, flags=re.UNICODE).strip()
    if not trimmed:
        return None

    tool_by_name = {str(spec.name).lower(): spec for spec in tools}

    if _TEXTUAL_TOOL_NAME_RE.fullmatch(trimmed):
        spec = tool_by_name.get(trimmed.lower())
        if spec is None:
            return None
        required = _required_tool_fields(spec)
        if required:
            return None
        return {
            "id": f"recovered-{spec.name}",
            "type": "function",
            "function": {"name": spec.name, "arguments": "{}"},
        }

    match = _TEXTUAL_TOOL_PREVIEW_RE.fullmatch(trimmed)
    if not match:
        return None

    spec = tool_by_name.get(str(match.group("tool") or "").lower())
    if spec is None:
        return None

    args = _parse_textual_tool_preview_args(match.group("rest") or "", spec)
    if args is None:
        return None

    required = _required_tool_fields(spec)
    if any(field not in args for field in required):
        return None

    return {
        "id": f"recovered-{spec.name}",
        "type": "function",
        "function": {"name": spec.name, "arguments": json.dumps(args, ensure_ascii=False)},
    }


def _parse_textual_tool_preview_args(preview: str, spec: ToolSpec) -> dict[str, Any] | None:
    args: dict[str, Any] = {}
    properties = (
        ((spec.parameters or {}).get("properties") or {})
        if isinstance(spec.parameters, dict)
        else {}
    )
    alias_map = {"file": "path"}

    for chunk in preview.split(","):
        piece = chunk.strip()
        if not piece or ":" not in piece:
            continue
        key_raw, value_raw = piece.split(":", 1)
        key = key_raw.strip().lower().replace(" ", "_")
        value = value_raw.strip()
        if not key or not value:
            continue
        key = alias_map.get(key, key)
        if properties and key not in properties:
            return None
        args[key] = value

    return args or None


def _required_tool_fields(spec: ToolSpec) -> set[str]:
    params = spec.parameters if isinstance(spec.parameters, dict) else {}
    required = params.get("required") or []
    if not isinstance(required, list):
        return set()
    return {str(item) for item in required if str(item).strip()}
