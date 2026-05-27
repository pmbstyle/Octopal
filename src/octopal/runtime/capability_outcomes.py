from __future__ import annotations

import json
from typing import Any, Literal, TypedDict, cast

CAPABILITY_OUTCOME_KEY = "capability_outcome"

CapabilityOutcomeKind = Literal[
    "needs_approval",
    "needs_continuation",
    "needs_worker",
    "needs_user_input",
    "policy_denied",
]


class CapabilityOutcome(TypedDict, total=False):
    kind: CapabilityOutcomeKind
    reason: str
    next_action: str
    tool: str
    missing_tool: str
    policy_reason: str
    question: str
    details: dict[str, Any]


def capability_outcome(
    kind: CapabilityOutcomeKind,
    *,
    reason: str,
    next_action: str,
    tool: str | None = None,
    missing_tool: str | None = None,
    policy_reason: str | None = None,
    question: str | None = None,
    details: dict[str, Any] | None = None,
) -> CapabilityOutcome:
    outcome: CapabilityOutcome = {
        "kind": kind,
        "reason": str(reason or "").strip(),
        "next_action": str(next_action or "").strip(),
    }
    if tool:
        outcome["tool"] = str(tool).strip()
    if missing_tool:
        outcome["missing_tool"] = str(missing_tool).strip()
    if policy_reason:
        outcome["policy_reason"] = str(policy_reason).strip()
    if question:
        outcome["question"] = str(question).strip()
    if details:
        outcome["details"] = dict(details)
    return outcome


def extract_capability_outcome(value: Any) -> CapabilityOutcome | None:
    payload = value
    if isinstance(value, str):
        try:
            payload = json.loads(value)
        except Exception:
            return None
    if not isinstance(payload, dict):
        return None
    outcome = payload.get(CAPABILITY_OUTCOME_KEY)
    if not isinstance(outcome, dict):
        return None
    kind = str(outcome.get("kind") or "").strip()
    if kind not in _VALID_OUTCOME_KINDS:
        return None
    return cast(CapabilityOutcome, dict(outcome))


_VALID_OUTCOME_KINDS = {
    "needs_approval",
    "needs_continuation",
    "needs_worker",
    "needs_user_input",
    "policy_denied",
}
