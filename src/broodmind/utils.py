from __future__ import annotations

import json
import re
import subprocess
from datetime import UTC, datetime

_TEXTUAL_TOOL_NAME_RE = re.compile(r"^(?:mcp__[\w-]+__)?[a-z][a-z0-9_]{1,63}$", re.IGNORECASE)
_TEXTUAL_TOOL_PREVIEW_RE = re.compile(
    r"^(?P<tool>(?:mcp__[\w-]+__)?[a-z][a-z0-9_]{1,63})(?P<rest>(?:,\s*[a-z_][a-z0-9_ -]{0,31}:\s*[^,\n]{1,200})+)$",
    re.IGNORECASE,
)
_REACT_TAG_RE = re.compile(r"<react>(.*?)</react>", re.IGNORECASE | re.DOTALL)
_THINK_BLOCK_RE = re.compile(r"<think>.*?</think>", re.IGNORECASE | re.DOTALL)
_THINK_TAG_RE = re.compile(r"</?think>", re.IGNORECASE)
_TOOL_RESULT_LINE_RE = re.compile(
    r"(?:^|\n)\s*Tool result \([^)]+\):\s*(?:\{.*?\}|\[.*?\]|.+?)(?=\n|$)",
    re.IGNORECASE | re.DOTALL,
)
_TECHNICAL_FAILURE_RE = re.compile(
    r"remote mcp tool response schema is incompatible|mcp_schema_mismatch|schema mismatch",
    re.IGNORECASE,
)
_REACTION_MAPPING = {
    "✅": "👍",
    "✔️": "👍",
    "❌": "👎",
    "✖️": "👎",
    "🚀": "⚡",
    "⚠️": "🤨",
    "ℹ️": "🤔",
}

def get_tailscale_ips() -> list[str]:
    """Retrieve all available Tailscale IPs in the tailnet using JSON output."""
    try:
        # tailscale status --json provides a detailed list of all nodes and their IPs.
        out = subprocess.check_output(["tailscale", "status", "--json"], text=True, stderr=subprocess.DEVNULL)
        data = json.loads(out)
        ips = []

        # Add self IPs
        if "Self" in data and "TailscaleIPs" in data["Self"]:
            ips.extend(data["Self"]["TailscaleIPs"])

        # Add peer IPs
        if "Peer" in data:
            for peer in data["Peer"].values():
                if "TailscaleIPs" in peer:
                    ips.extend(peer["TailscaleIPs"])

        return list(set(ips))  # Unique IPs
    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError):
        return []


def utc_now() -> datetime:
    return datetime.now(UTC)


def normalize_reaction_emoji(emoji: str) -> str:
    return _REACTION_MAPPING.get((emoji or "").strip(), (emoji or "").strip())


def extract_reaction_and_strip(text: str) -> tuple[str | None, str]:
    match = _REACT_TAG_RE.search(text or "")
    if not match:
        return None, text or ""
    emoji = (match.group(1) or "").strip() or None
    cleaned = _REACT_TAG_RE.sub("", text or "").strip()
    return emoji, cleaned


def strip_reaction_tags(text: str) -> str:
    return _REACT_TAG_RE.sub("", text or "").strip()


def sanitize_user_facing_text(text: str) -> str:
    """Remove reasoning/tool traces and collapse raw machine payloads into safe text."""
    value = strip_reaction_tags(text or "")
    if not value:
        return ""

    cleaned = value.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = _THINK_BLOCK_RE.sub("", cleaned)
    cleaned = _THINK_TAG_RE.sub("", cleaned)
    cleaned = _TOOL_RESULT_LINE_RE.sub("", cleaned)
    cleaned = cleaned.strip()
    if not cleaned:
        return ""

    payload = _try_parse_json_object(cleaned)
    if isinstance(payload, dict):
        normalized = _normalize_machine_payload_for_user(payload)
        if normalized is not None:
            cleaned = normalized.strip()

    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned


def _try_parse_json_object(text: str) -> dict | None:
    value = (text or "").strip()
    if not value.startswith("{") or not value.endswith("}"):
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _normalize_machine_payload_for_user(payload: dict) -> str | None:
    keys = set(payload.keys())

    if {"worker_id", "lineage_id", "root_task_id"}.intersection(keys):
        return ""

    if payload.get("type") == "result":
        summary = str(payload.get("summary", "") or "").strip()
        return summary

    status = str(payload.get("status", "") or "").strip().lower()
    if status in {"running", "queued"} and "worker_id" in keys:
        return ""

    if status in {"completed", "failed"} and "summary" in keys:
        return str(payload.get("summary", "") or "").strip()

    return None


def is_technical_delivery_noise(text: str) -> bool:
    value = sanitize_user_facing_text(text or "")
    if not value:
        return True
    if value.startswith("{") and value.endswith("}"):
        return True
    return bool(_TECHNICAL_FAILURE_RE.search(value))


def is_heartbeat_ok(text: str) -> bool:
    """Check if the text contains HEARTBEAT_OK (case-insensitive) and is exactly one line."""
    value = (text or "").strip()
    if not value:
        return False
    # Must contain HEARTBEAT_OK and have no internal newlines
    return "HEARTBEAT_OK" in value.upper() and "\n" not in value


def is_control_response(text: str) -> bool:
    """Check if the text is a system control message like HEARTBEAT_OK or NO_USER_RESPONSE."""
    value = (text or "").strip()
    if not value:
        return True

    if is_heartbeat_ok(value):
        return True

    # Check for NO_USER_RESPONSE variations
    normalized = value.upper().replace("_", "").replace(" ", "")
    return normalized == "NOUSERRESPONSE"


def has_no_user_response_suffix(text: str) -> bool:
    """Return True when text ends with NO_USER_RESPONSE (allowing spacing/underscore variations)."""
    value = (text or "").strip()
    if not value:
        return False
    # Trim trailing formatting/punctuation wrappers (e.g., **NO_USER_RESPONSE**).
    trimmed = re.sub(r"[^\w]+$", "", value).strip()
    normalized = re.sub(r"[\s_-]+", "", trimmed).upper()
    return normalized.endswith("NOUSERRESPONSE")


def has_heartbeat_ok_edge(text: str) -> bool:
    """Return True when text starts or ends with HEARTBEAT_OK (spacing/underscore/punctuation tolerant)."""
    value = (text or "").strip()
    if not value:
        return False
    # Trim leading/trailing wrappers so formatted control tokens are still recognized.
    trimmed = re.sub(r"^[^\w]+", "", value)
    trimmed = re.sub(r"[^\w]+$", "", trimmed).strip()
    if not trimmed:
        return False
    normalized = re.sub(r"[\s_-]+", "", trimmed).upper()
    return normalized.startswith("HEARTBEATOK") or normalized.endswith("HEARTBEATOK")


def looks_like_textual_tool_invocation(text: str) -> bool:
    value = (text or "").strip()
    if not value or "\n" in value or len(value) > 300:
        return False
    if is_control_response(value):
        return False
    if has_heartbeat_ok_edge(value):
        return False
    if has_no_user_response_suffix(value):
        return False

    trimmed = re.sub(r"^[\s\W_]+", "", value, flags=re.UNICODE)
    trimmed = re.sub(r"[\s\W_]+$", "", trimmed, flags=re.UNICODE).strip()
    if not trimmed:
        return False

    return bool(
        _TEXTUAL_TOOL_NAME_RE.fullmatch(trimmed)
        or _TEXTUAL_TOOL_PREVIEW_RE.fullmatch(trimmed)
    )


def should_suppress_user_delivery(text: str) -> bool:
    """Guard rail for outbound channels: suppress control/system-only payloads."""
    value = sanitize_user_facing_text(text or "")
    if not value:
        return True
    if is_control_response(value):
        return True
    if has_heartbeat_ok_edge(value):
        return True
    if has_no_user_response_suffix(value):
        return True
    if is_technical_delivery_noise(value):
        return True
    return bool(looks_like_textual_tool_invocation(value))
