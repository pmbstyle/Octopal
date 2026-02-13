from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from typing import Any

from broodmind.intents.types import ActionIntent, IntentRequest


@dataclass(frozen=True)
class IntentSpec:
    risk: str
    requires_approval: bool
    payload_schema: dict[str, Any]


INTENT_REGISTRY: dict[str, IntentSpec] = {
    "http.get": IntentSpec(
        risk="low",
        requires_approval=False,
        payload_schema={
            "required": ["url"],
            "optional": ["headers"],
            "types": {"url": "str", "headers": "dict[str,str]"},
        },
    ),
    "http.post": IntentSpec(
        risk="medium",
        requires_approval=True,
        payload_schema={
            "required": ["url"],
            "optional": ["headers", "body", "json"],
            "types": {
                "url": "str",
                "headers": "dict[str,str]",
                "body": "str",
                "json": "dict",
            },
        },
    ),
    "mcp_call": IntentSpec(
        risk="medium",
        requires_approval=False,
        payload_schema={
            "required": ["server_id", "tool_name", "arguments"],
            "optional": [],
            "types": {
                "server_id": "str",
                "tool_name": "str",
                "arguments": "dict",
            },
        },
    ),
    "file.read": IntentSpec(
        risk="low",
        requires_approval=False,
        payload_schema={
            "required": ["path"],
            "optional": [],
            "types": {"path": "str"},
        },
    ),
    "file.write": IntentSpec(
        risk="medium",
        requires_approval=False,
        payload_schema={
            "required": ["path", "content"],
            "optional": ["mode"],
            "types": {"path": "str", "content": "str", "mode": "str"},
        },
    ),
    "email.send": IntentSpec(
        risk="high",
        requires_approval=True,
        payload_schema={
            "required": ["to", "subject", "body"],
            "optional": ["cc", "bcc", "attachments"],
            "types": {
                "to": "str",
                "subject": "str",
                "body": "str",
                "cc": "list[str]",
                "bcc": "list[str]",
                "attachments": "list",
            },
        },
    ),
    "payment.send": IntentSpec(
        risk="critical",
        requires_approval=True,
        payload_schema={
            "required": ["amount", "currency", "recipient"],
            "optional": ["memo"],
            "types": {
                "amount": "Decimal",
                "currency": "str",
                "recipient": "str",
                "memo": "str",
            },
        },
    ),
}


class IntentValidationError(ValueError):
    pass


def canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def normalize_payload(intent_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    if intent_type not in INTENT_REGISTRY:
        raise IntentValidationError(f"Unknown intent type: {intent_type}")

    schema = INTENT_REGISTRY[intent_type].payload_schema
    required = set(schema.get("required", []))
    optional = set(schema.get("optional", []))
    allowed = required | optional

    unknown = set(payload.keys()) - allowed
    if unknown:
        raise IntentValidationError(f"Unknown fields: {sorted(unknown)}")

    missing = required - set(payload.keys())
    if missing:
        raise IntentValidationError(f"Missing required fields: {sorted(missing)}")

    types = schema.get("types", {})
    normalized: dict[str, Any] = {}
    for key, value in payload.items():
        expected = types.get(key)
        normalized[key] = _validate_and_coerce(value, expected)

    return normalized


def validate_intent(request: IntentRequest, worker_id: str, intent_id: str) -> ActionIntent:
    spec = INTENT_REGISTRY.get(request.type)
    if not spec:
        raise IntentValidationError(f"Unknown intent type: {request.type}")

    normalized_payload = normalize_payload(request.type, request.payload)
    payload_hash = sha256(canonical_json(normalized_payload).encode("utf-8")).hexdigest()

    return ActionIntent(
        id=intent_id,
        type=request.type,
        payload=normalized_payload,
        payload_hash=payload_hash,
        risk=spec.risk,
        requires_approval=spec.requires_approval,
        worker_id=worker_id,
    )


def _validate_and_coerce(value: Any, expected: str | None) -> Any:
    if expected is None:
        return value
    if expected == "str":
        if not isinstance(value, str):
            raise IntentValidationError("Expected string")
        return value
    if expected == "list[str]":
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            raise IntentValidationError("Expected list[str]")
        return value
    if expected == "dict[str,str]":
        if not isinstance(value, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in value.items()
        ):
            raise IntentValidationError("Expected dict[str,str]")
        return value
    if expected == "dict":
        if not isinstance(value, dict):
            raise IntentValidationError("Expected dict")
        return value
    if expected == "Decimal":
        try:
            return Decimal(str(value))
        except Exception as exc:
            raise IntentValidationError("Expected Decimal") from exc
    return value
