from __future__ import annotations

from octopal.runtime.intents.approval_format import (
    approval_display_payload,
    format_approval_message,
)
from octopal.runtime.intents.types import ActionIntent


def _intent(intent_type: str, payload: dict) -> ActionIntent:
    return ActionIntent(
        id="intent-1",
        type=intent_type,
        payload=payload,
        payload_hash="hash",
        risk="high",
        requires_approval=True,
        worker_id="octo",
    )


def test_exec_approval_message_is_human_readable() -> None:
    message = format_approval_message(
        _intent(
            "exec.run",
            {
                "action": "start",
                "command": "sudo true",
                "background": False,
                "reason": "uses dangerous command `sudo`",
            },
        )
    )

    assert "Approval needed" in message
    assert "What Octopal wants to do: run a shell command" in message
    assert "Command: sudo true" in message
    assert "Risk: High" in message
    assert "Payload:" not in message
    assert "{'action'" not in message


def test_approval_display_payload_includes_message_and_summary() -> None:
    display = approval_display_payload(
        _intent(
            "email.send",
            {"to": "slava@example.com", "subject": "Hello", "body": "Hi"},
        )
    )

    assert display["summary"] == "send an email"
    assert "To: slava@example.com" in display["message"]
    assert display["risk_label"] == "High"
