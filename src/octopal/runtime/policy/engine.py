from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from datetime import timedelta
from uuid import uuid4

from octopal.runtime.intents.registry import normalize_payload
from octopal.runtime.intents.types import ActionIntent
from octopal.runtime.policy.capabilities import DEFAULT_CAPABILITY_WHITELIST
from octopal.runtime.policy.permits import ApprovalRequirement, Permit
from octopal.runtime.workers.contracts import Capability
from octopal.utils import utc_now


@dataclass
class PolicyEngine:
    whitelist: dict[str, list[str]] = None

    def __post_init__(self) -> None:
        if self.whitelist is None:
            self.whitelist = DEFAULT_CAPABILITY_WHITELIST

    def grant_capabilities(self, requested: list[Capability]) -> list[Capability]:
        granted: list[Capability] = []
        for cap in requested:
            allowed_scopes = self.whitelist.get(cap.type, [])
            if not allowed_scopes:
                continue
            if _scope_allowed(cap.scope, allowed_scopes):
                granted.append(cap)
        return granted

    def check_intent(self, intent: ActionIntent) -> ApprovalRequirement:
        if intent.requires_approval or intent.risk in {"high", "critical"}:
            return ApprovalRequirement(
                requires_approval=True,
                reason=f"risk={intent.risk}",
            )
        return ApprovalRequirement(requires_approval=False)

    def issue_permit(self, intent: ActionIntent, worker_id: str) -> Permit:
        """Issue a permit for a verified/approved intent."""
        normalized = normalize_payload(intent.type, intent.payload)
        payload_hash = _hash_payload(normalized)

        return Permit(
            id=str(uuid4()),
            intent_id="auto",  # Placeholder if not linked to a persisted intent record yet
            intent_type=intent.type,
            worker_id=worker_id,
            payload_hash=payload_hash,
            expires_at=utc_now() + timedelta(minutes=5),
        )


def _scope_allowed(scope: str, allowed_scopes: list[str]) -> bool:
    for allowed in allowed_scopes:
        if allowed == "*":
            return True
        if fnmatch.fnmatch(scope, allowed):
            return True
    return False


def _hash_payload(payload: dict) -> str:
    import hashlib

    from octopal.runtime.intents.registry import canonical_json

    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
