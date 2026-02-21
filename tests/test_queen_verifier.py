from __future__ import annotations

from broodmind.providers.base import Message
from broodmind.queen.router import (
    _build_insufficient_evidence_response,
    _messages_to_text,
    _normalize_verification_payload,
)


def test_normalize_verification_payload_parses_fields() -> None:
    payload = _normalize_verification_payload(
        {
            "verdict": "revised",
            "response": "Safer answer",
            "missing_evidence": ["source link"],
            "confidence": 0.8,
        }
    )
    assert payload is not None
    assert payload["verdict"] == "revised"
    assert payload["response"] == "Safer answer"
    assert payload["missing_evidence"] == ["source link"]
    assert payload["confidence"] == 0.8


def test_normalize_verification_payload_rejects_invalid_verdict() -> None:
    assert _normalize_verification_payload({"verdict": "maybe"}) is None


def test_build_insufficient_evidence_response_uses_missing_evidence() -> None:
    text = _build_insufficient_evidence_response(
        {"response": "", "missing_evidence": ["a timestamped source"]},
        "draft",
    )
    assert "missing enough evidence" in text.lower()
    assert "timestamped source" in text


def test_messages_to_text_serializes_message_objects_and_dicts() -> None:
    text = _messages_to_text(
        [
            Message(role="system", content="rules"),
            {"role": "assistant", "content": "draft"},
            {"role": "tool", "content": {"ok": True}},
        ]
    )
    assert "system: rules" in text
    assert "assistant: draft" in text
    assert "tool: {'ok': True}" in text or '"ok": true' in text
