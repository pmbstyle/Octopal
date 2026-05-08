from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from octopal.runtime.scheduler.service import normalize_notify_user_policy
from octopal.runtime.workers.contracts import WorkerResult
from octopal.utils import (
    sanitize_user_facing_text_preserving_reaction,
    should_suppress_user_delivery,
)


class DeliveryMode(StrEnum):
    SILENT = "silent"
    DEFERRED = "deferred"
    IMMEDIATE = "immediate"


@dataclass(frozen=True)
class DeliveryDecision:
    mode: DeliveryMode
    text: str
    reason: str
    followup_required: bool = False

    @property
    def user_visible(self) -> bool:
        return self.mode in {DeliveryMode.DEFERRED, DeliveryMode.IMMEDIATE}


def resolve_user_delivery(
    text: str,
    *,
    followup_required: bool = False,
) -> DeliveryDecision:
    value = sanitize_user_facing_text_preserving_reaction(str(text or ""))
    if should_suppress_user_delivery(value):
        return DeliveryDecision(
            mode=DeliveryMode.SILENT,
            text=value,
            reason="control_or_empty",
            followup_required=False,
        )
    return DeliveryDecision(
        mode=DeliveryMode.IMMEDIATE,
        text=value,
        reason="user_visible",
        followup_required=followup_required,
    )


def resolve_worker_followup_delivery(
    text: str,
    *,
    result: WorkerResult,
    pending_closure: bool,
    suppress_followup: bool,
    should_force: bool,
    notify_user: str | None = None,
    forced_text_factory,
) -> DeliveryDecision:
    notify_policy = normalize_notify_user_policy(notify_user)
    decision = resolve_user_delivery(text)
    has_failure = _result_has_blocking_failure(result)

    if notify_policy == "never" and not has_failure:
        return DeliveryDecision(
            mode=DeliveryMode.SILENT,
            text=decision.text,
            reason="scheduled_notify_never",
        )

    if notify_policy == "always":
        if not decision.user_visible:
            forced_text = forced_text_factory(result)
            forced_decision = resolve_user_delivery(forced_text)
            if forced_decision.user_visible:
                return DeliveryDecision(
                    mode=DeliveryMode.DEFERRED if suppress_followup else DeliveryMode.IMMEDIATE,
                    text=forced_decision.text,
                    reason="scheduled_notify_always",
                )
        else:
            return DeliveryDecision(
                mode=DeliveryMode.DEFERRED if suppress_followup else DeliveryMode.IMMEDIATE,
                text=decision.text,
                reason="scheduled_notify_always",
            )

    if not decision.user_visible and (should_force or pending_closure):
        forced_text = forced_text_factory(result)
        forced_decision = resolve_user_delivery(forced_text)
        if forced_decision.user_visible:
            return DeliveryDecision(
                mode=DeliveryMode.DEFERRED if suppress_followup else DeliveryMode.IMMEDIATE,
                text=forced_decision.text,
                reason="forced_substantive_followup",
            )

    if not decision.user_visible:
        return DeliveryDecision(
            mode=DeliveryMode.SILENT,
            text=decision.text,
            reason="no_user_response",
        )

    if suppress_followup:
        return DeliveryDecision(
            mode=DeliveryMode.DEFERRED,
            text=decision.text,
            reason="suppressed_turn_followup",
        )

    return DeliveryDecision(
        mode=DeliveryMode.IMMEDIATE,
        text=decision.text,
        reason="user_visible_followup",
    )


def _result_has_blocking_failure(result: WorkerResult) -> bool:
    summary = str(result.summary or "").strip().lower()
    if "error" in summary or "failed" in summary:
        return True
    output = result.output
    if not isinstance(output, dict):
        return False
    raw_error = str(output.get("error") or "").strip()
    if raw_error:
        return True
    status = str(output.get("status") or "").strip().lower()
    return status in {"error", "failed", "failure"}
