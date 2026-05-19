from __future__ import annotations

import sys
from datetime import timedelta
from typing import Any

import structlog

from octopal.infrastructure.logging import correlation_id_var
from octopal.runtime.octo.context_health import (
    _RESET_SOON_THRESHOLDS,
    _WATCH_THRESHOLDS,
)
from octopal.runtime.octo.context_reset import _normalize_compact
from octopal.utils import utc_now

logger = structlog.get_logger(__name__)

_PENDING_CONVERSATIONAL_CLOSURE_TTL_SECONDS = 3600
_HEARTBEAT_USER_VISIBLE_COOLDOWN_SECONDS = 300


def _runtime_value(name: str, default: Any) -> Any:
    core_module = sys.modules.get("octopal.runtime.octo.core")
    if core_module is None:
        return default
    return getattr(core_module, name, default)


def _pending_conversational_closure_ttl_seconds() -> int:
    return int(
        _runtime_value(
            "_PENDING_CONVERSATIONAL_CLOSURE_TTL_SECONDS",
            _PENDING_CONVERSATIONAL_CLOSURE_TTL_SECONDS,
        )
    )


def _heartbeat_user_visible_cooldown_seconds() -> int:
    return int(
        _runtime_value(
            "_HEARTBEAT_USER_VISIBLE_COOLDOWN_SECONDS",
            _HEARTBEAT_USER_VISIBLE_COOLDOWN_SECONDS,
        )
    )


class OctoTurnStateMixin:
    def peek_context_wakeup(self, chat_id: int) -> str:
        pending = self._pending_wakeup_by_chat or {}
        return str(pending.get(chat_id, "") or "")

    def has_pending_conversational_closure(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        self._prune_pending_conversational_closures()
        pending = self._pending_conversational_closure_by_correlation
        if pending is None:
            return False
        return correlation_id in pending

    def mark_pending_conversational_closure(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        self._prune_pending_conversational_closures()
        pending = self._pending_conversational_closure_by_correlation
        if pending is None:
            pending = {}
            self._pending_conversational_closure_by_correlation = pending
        pending[correlation_id] = utc_now()

    def clear_pending_conversational_closure(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        pending = self._pending_conversational_closure_by_correlation
        if pending is None:
            return
        pending.pop(correlation_id, None)

    def mark_structured_followup_required(self, correlation_id: str | None = None) -> None:
        if not correlation_id:
            correlation_id = str(correlation_id_var.get() or "").strip() or None
        if not correlation_id:
            return
        self._prune_structured_followup_required()
        hints = self._structured_followup_required_by_correlation
        if hints is None:
            hints = {}
            self._structured_followup_required_by_correlation = hints
        hints[correlation_id] = utc_now()

    def consume_structured_followup_required(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        self._prune_structured_followup_required()
        hints = self._structured_followup_required_by_correlation
        if hints is None:
            return False
        return correlation_id in hints and bool(hints.pop(correlation_id, None))

    def clear_structured_followup_required(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        hints = self._structured_followup_required_by_correlation
        if hints is None:
            return
        hints.pop(correlation_id, None)

    def _prune_structured_followup_required(self) -> None:
        hints = self._structured_followup_required_by_correlation
        if hints is None:
            return
        if not hints:
            return
        cutoff = utc_now() - timedelta(seconds=_pending_conversational_closure_ttl_seconds())
        expired = [
            correlation_id
            for correlation_id, created_at in hints.items()
            if not created_at or created_at < cutoff
        ]
        for correlation_id in expired:
            hints.pop(correlation_id, None)

    def _prune_pending_conversational_closures(self) -> None:
        pending = self._pending_conversational_closure_by_correlation
        if pending is None:
            return
        if not pending:
            return
        cutoff = utc_now() - timedelta(seconds=_pending_conversational_closure_ttl_seconds())
        expired = [
            correlation_id
            for correlation_id, created_at in pending.items()
            if not created_at or created_at < cutoff
        ]
        for correlation_id in expired:
            pending.pop(correlation_id, None)

    def suppress_turn_followups(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        self._prune_suppressed_followups()
        suppressed = self._suppressed_followups_by_correlation
        if suppressed is None:
            suppressed = {}
            self._suppressed_followups_by_correlation = suppressed
        suppressed[correlation_id] = utc_now()

    def suppress_channel_followups(
        self,
        correlation_id: str | None,
        *,
        reason: str | None = None,
    ) -> None:
        if not correlation_id:
            return
        self._prune_channel_followup_suppressions()
        suppressed = self._channel_followups_suppressed_by_correlation
        if suppressed is None:
            suppressed = {}
            self._channel_followups_suppressed_by_correlation = suppressed
        suppressed[correlation_id] = {"created_at": utc_now(), "reason": str(reason or "")}

    def mark_user_turn_active(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        active = self._active_user_turns_by_correlation
        if active is None:
            active = {}
            self._active_user_turns_by_correlation = active
        active[correlation_id] = utc_now()

    def mark_user_turn_inactive(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        active = self._active_user_turns_by_correlation
        if active is None:
            return
        active.pop(correlation_id, None)

    def has_active_user_turn(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        active = self._active_user_turns_by_correlation
        if active is None:
            return False
        return correlation_id in active

    def should_suppress_turn_followups(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        self._prune_suppressed_followups()
        suppressed = self._suppressed_followups_by_correlation
        if suppressed is None:
            return False
        return correlation_id in suppressed

    def should_suppress_channel_followups(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        self._prune_channel_followup_suppressions()
        suppressed = self._channel_followups_suppressed_by_correlation
        if suppressed is None:
            return False
        return correlation_id in suppressed

    def channel_followup_suppression_reason(self, correlation_id: str | None) -> str:
        if not correlation_id:
            return ""
        self._prune_channel_followup_suppressions()
        suppressed = self._channel_followups_suppressed_by_correlation
        if not suppressed:
            return ""
        value = suppressed.get(correlation_id)
        if isinstance(value, dict):
            return str(value.get("reason") or "")
        return ""

    def clear_suppressed_turn_followups(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        suppressed = self._suppressed_followups_by_correlation
        if suppressed is None:
            return
        suppressed.pop(correlation_id, None)

    def register_worker_correlation(self, run_id: str, correlation_id: str | None) -> None:
        if not run_id or not correlation_id:
            return
        self._worker_correlation_by_run_id[run_id] = correlation_id
        self._active_workers_by_correlation.setdefault(correlation_id, set()).add(run_id)

    def register_worker_chat(self, run_id: str, chat_id: int) -> None:
        if not run_id:
            return
        self._worker_chat_by_run_id[run_id] = int(chat_id or 0)

    def get_worker_chat_id(self, run_id: str) -> int:
        if not run_id:
            return 0
        value = self._worker_chat_by_run_id.get(run_id)
        if value is not None:
            return int(value or 0)
        worker = None
        try:
            worker = self.store.get_worker(run_id)
        except Exception:
            logger.debug(
                "Failed to resolve worker chat id from store",
                worker_id=run_id,
                exc_info=True,
            )
        return int(getattr(worker, "chat_id", 0) or 0) if worker is not None else 0

    def has_active_workers_for_correlation(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        return bool(self._active_workers_by_correlation.get(correlation_id))

    def mark_internal_result_pending(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        pending = self._pending_internal_results_by_correlation
        pending[correlation_id] = int(pending.get(correlation_id, 0)) + 1

    def mark_internal_result_processed(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        pending = self._pending_internal_results_by_correlation
        remaining = int(pending.get(correlation_id, 0)) - 1
        if remaining <= 0:
            pending.pop(correlation_id, None)
            return
        pending[correlation_id] = remaining

    def has_pending_internal_results_for_correlation(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        return int(self._pending_internal_results_by_correlation.get(correlation_id, 0)) > 0

    def should_flush_worker_followups(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return True
        return (
            not self.has_active_user_turn(correlation_id)
            and not self.has_active_workers_for_correlation(correlation_id)
            and not self.has_pending_internal_results_for_correlation(correlation_id)
        )

    def _prune_suppressed_followups(self) -> None:
        suppressed = self._suppressed_followups_by_correlation
        if not suppressed:
            return
        cutoff = utc_now() - timedelta(seconds=_pending_conversational_closure_ttl_seconds())
        expired = [
            correlation_id
            for correlation_id, created_at in suppressed.items()
            if not created_at or created_at < cutoff
        ]
        for correlation_id in expired:
            suppressed.pop(correlation_id, None)

    def _prune_channel_followup_suppressions(self) -> None:
        suppressed = self._channel_followups_suppressed_by_correlation
        if not suppressed:
            return
        cutoff = utc_now() - timedelta(seconds=_pending_conversational_closure_ttl_seconds())
        expired = []
        for correlation_id, value in suppressed.items():
            created_at = value.get("created_at") if isinstance(value, dict) else value
            if not created_at or created_at < cutoff:
                expired.append(correlation_id)
        for correlation_id in expired:
            suppressed.pop(correlation_id, None)

    def clear_context_wakeup(self, chat_id: int) -> None:
        pending = self._pending_wakeup_by_chat or {}
        pending.pop(chat_id, None)

    def note_user_visible_delivery(self, chat_id: int, text: str) -> None:
        normalized = _normalize_compact(text)
        if normalized:
            self._last_reply_norm_by_chat[chat_id] = normalized
        self._last_user_visible_delivery_at_by_chat[chat_id] = utc_now()

    def should_suppress_heartbeat_delivery(self, chat_id: int, text: str) -> bool:
        cooldown_seconds = _heartbeat_user_visible_cooldown_seconds()
        if cooldown_seconds <= 0:
            return False
        delivered_at = (self._last_user_visible_delivery_at_by_chat or {}).get(chat_id)
        if delivered_at is None:
            return False
        try:
            elapsed = (utc_now() - delivered_at).total_seconds()
        except Exception:
            return False
        if elapsed < 0:
            return False
        suppress = elapsed < cooldown_seconds
        if suppress:
            logger.info(
                "Suppressing heartbeat delivery after recent visible message",
                chat_id=chat_id,
                cooldown_seconds=cooldown_seconds,
                elapsed_seconds=round(elapsed, 2),
                text_len=len(text or ""),
            )
        return suppress

    def get_context_thresholds(self) -> dict[str, dict[str, float | int]]:
        return {
            "watch": dict(_WATCH_THRESHOLDS),
            "reset_soon": dict(_RESET_SOON_THRESHOLDS),
        }
