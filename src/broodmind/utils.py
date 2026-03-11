from __future__ import annotations

from datetime import UTC, datetime
import re


import subprocess

import json

_TEXTUAL_TOOL_NAME_RE = re.compile(r"^(?:mcp__[\w-]+__)?[a-z][a-z0-9_]{1,63}$", re.IGNORECASE)
_TEXTUAL_TOOL_PREVIEW_RE = re.compile(
    r"^(?P<tool>(?:mcp__[\w-]+__)?[a-z][a-z0-9_]{1,63})(?P<rest>(?:,\s*[a-z_][a-z0-9_ -]{0,31}:\s*[^,\n]{1,200})+)$",
    re.IGNORECASE,
)

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
    if normalized == "NOUSERRESPONSE":
        return True
        
    return False


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
    value = (text or "").strip()
    if not value:
        return True
    if is_control_response(value):
        return True
    if has_heartbeat_ok_edge(value):
        return True
    if has_no_user_response_suffix(value):
        return True
    if looks_like_textual_tool_invocation(value):
        return True
    return False
