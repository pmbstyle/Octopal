from __future__ import annotations

from typing import Any

from octopal.runtime.intents.types import ActionIntent


def format_approval_message(intent: ActionIntent) -> str:
    summary = _approval_summary(intent)
    details = _approval_details(intent)
    reason = _approval_reason(intent)
    risk = _human_risk(intent.risk)

    lines = [
        "Approval needed",
        "",
        f"What Octopal wants to do: {summary}",
    ]
    if details:
        lines.extend(["", *details])
    lines.extend(
        [
            "",
            f"Risk: {risk}",
            f"Why this paused: {reason}",
            "",
            "Use the buttons below to approve or deny.",
        ]
    )
    return "\n".join(lines)


def approval_display_payload(intent: ActionIntent) -> dict[str, Any]:
    return {
        "summary": _approval_summary(intent),
        "details": _approval_details(intent),
        "reason": _approval_reason(intent),
        "risk_label": _human_risk(intent.risk),
        "message": format_approval_message(intent),
    }


def _approval_summary(intent: ActionIntent) -> str:
    intent_type = intent.type
    payload = intent.payload

    if intent_type == "exec.run":
        action = str(payload.get("action", "start") or "start").strip().lower()
        if action == "write":
            return "send input to a running shell command"
        if action == "stop":
            return "stop a running shell command"
        return "run a shell command"
    if intent_type == "http.post":
        return "send data to a web service"
    if intent_type == "email.send":
        return "send an email"
    if intent_type == "payment.send":
        return "send a payment"
    if intent_type == "file.write":
        return "write to a file"
    if intent_type == "mcp_call":
        tool_name = str(payload.get("tool_name", "") or "").strip()
        return f"call the connector tool {tool_name}" if tool_name else "call a connector tool"
    if intent_type == "desktop.control":
        action = str(payload.get("action", "") or "").strip()
        return f"control the desktop with {action}" if action else "control the desktop"
    return _humanize_token(intent_type)


def _approval_details(intent: ActionIntent) -> list[str]:
    payload = intent.payload
    if intent.type == "exec.run":
        command = str(payload.get("command", "") or payload.get("input_data", "") or "").strip()
        if command:
            return [f"Command: {_clip(command, 900)}"]
        return []
    if intent.type in {"http.get", "http.post"}:
        url = str(payload.get("url", "") or "").strip()
        return [f"URL: {_clip(url, 500)}"] if url else []
    if intent.type in {"file.read", "file.write"}:
        path = str(payload.get("path", "") or "").strip()
        return [f"Path: {_clip(path, 500)}"] if path else []
    if intent.type == "email.send":
        details = []
        to = str(payload.get("to", "") or "").strip()
        subject = str(payload.get("subject", "") or "").strip()
        if to:
            details.append(f"To: {_clip(to, 300)}")
        if subject:
            details.append(f"Subject: {_clip(subject, 300)}")
        return details
    if intent.type == "payment.send":
        amount = str(payload.get("amount", "") or "").strip()
        currency = str(payload.get("currency", "") or "").strip()
        recipient = str(payload.get("recipient", "") or "").strip()
        details = []
        if amount or currency:
            amount_label = " ".join(part for part in [amount, currency] if part)
            details.append(f"Amount: {_clip(amount_label, 300)}")
        if recipient:
            details.append(f"Recipient: {_clip(recipient, 300)}")
        return details
    if intent.type == "desktop.control":
        details = []
        action = str(payload.get("action", "") or "").strip()
        pid = str(payload.get("pid", "") or "").strip()
        window_id = str(payload.get("window_id", "") or "").strip()
        element_index = str(payload.get("element_index", "") or "").strip()
        text_preview = str(payload.get("text_preview", "") or "").strip()
        if action:
            details.append(f"Action: {_clip(action, 120)}")
        if pid:
            details.append(f"PID: {_clip(pid, 80)}")
        if window_id:
            details.append(f"Window: {_clip(window_id, 80)}")
        if element_index:
            details.append(f"Element: {_clip(element_index, 80)}")
        if text_preview:
            details.append(f"Text preview: {_clip(text_preview, 300)}")
        return details
    return []


def _approval_reason(intent: ActionIntent) -> str:
    reason = str(intent.payload.get("reason", "") or "").strip()
    if reason:
        return _clip(_humanize_reason(reason), 500)
    if intent.requires_approval:
        return "this action requires explicit approval"
    return f"policy marked this as {intent.risk} risk"


def _human_risk(risk: str) -> str:
    normalized = str(risk or "unknown").strip().lower()
    return {
        "low": "Low",
        "medium": "Medium",
        "high": "High",
        "critical": "Critical",
    }.get(normalized, _humanize_token(normalized))


def _humanize_reason(reason: str) -> str:
    cleaned = reason.strip()
    replacements = {
        "uses dangerous command": "uses the sensitive command",
        "writes to a device path": "writes directly to a device path",
        "interactive input looks dangerous": "input to the command looks sensitive",
    }
    for needle, replacement in replacements.items():
        if cleaned.lower().startswith(needle):
            return replacement + cleaned[len(needle) :]
    return cleaned


def _humanize_token(value: str) -> str:
    cleaned = str(value or "unknown").replace("_", " ").replace(".", " ").strip()
    return cleaned[:1].upper() + cleaned[1:] if cleaned else "Unknown"


def _clip(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "..."
