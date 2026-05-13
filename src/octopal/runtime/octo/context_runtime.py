from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import structlog

from octopal.infrastructure.logging import correlation_id_var
from octopal.infrastructure.observability.base import now_ms
from octopal.infrastructure.observability.helpers import summarize_exception
from octopal.infrastructure.store.models import AuditEvent
from octopal.runtime.memory.memchain import memchain_record
from octopal.runtime.octo.background_tracing import (
    _finish_background_trace_context,
    _start_background_trace_context,
)
from octopal.runtime.octo.context_health import (
    _RESET_CONFIDENCE_MIN,
    _RESET_CONFIRM_THRESHOLD,
    _RESET_SOON_THRESHOLDS,
    _WATCH_THRESHOLDS,
    _coerce_float,
    _is_reset_soon_severe,
    _watch_conditions,
)
from octopal.runtime.octo.context_reset import (
    build_wakeup_message as _build_wakeup_message,
)
from octopal.runtime.octo.context_reset import (
    estimate_error_streak as _estimate_error_streak,
)
from octopal.runtime.octo.context_reset import (
    estimate_repetition_score as _estimate_repetition_score,
)
from octopal.runtime.octo.context_reset import (
    persist_context_reset_files as _persist_context_reset_files,
)
from octopal.runtime.octo.worker_records import _normalize_string_list
from octopal.utils import utc_now

logger = structlog.get_logger(__name__)


class OctoContextRuntimeMixin:
    async def get_context_health_snapshot(self, chat_id: int) -> dict[str, Any]:
        trace_started_at_ms = now_ms()
        previous_snapshot = dict((self._context_health_by_chat or {}).get(chat_id, {}))
        trace_metadata: dict[str, Any] = {
            "chat_id": chat_id,
            "previous_context_health": str(previous_snapshot.get("context_health") or ""),
        }
        trace_ctx, trace_token, is_root_trace = await _start_background_trace_context(
            self.trace_sink,
            name="context.health",
            chat_id=chat_id,
            correlation_id=str(correlation_id_var.get() or "").strip() or None,
            metadata=trace_metadata,
        )
        trace_status = "ok"
        trace_output: dict[str, Any] | None = None
        try:
            recent_entries_all = await asyncio.to_thread(
                self.store.list_memory_entries_by_chat, chat_id, 120
            )
            recent_entries = [
                entry
                for entry in recent_entries_all
                if not bool((entry.metadata or {}).get("heartbeat"))
            ]
            entry_count = len(recent_entries)
            context_size_estimate = sum(len(e.content or "") for e in recent_entries)
            repetition_score = _estimate_repetition_score(recent_entries)
            error_streak = _estimate_error_streak(recent_entries)
            no_progress_turns = int((self._no_progress_turns_by_chat or {}).get(chat_id, 0))
            resets_since_progress = int(
                (self._reset_streak_without_progress_by_chat or {}).get(chat_id, 0)
            )
            overload_score = min(
                1.0,
                (context_size_estimate / float(_WATCH_THRESHOLDS["context_size_estimate"]))
                + (repetition_score * 0.9)
                + (min(8, error_streak) / 10.0)
                + (min(12, no_progress_turns) / 12.0),
            )
            watch_conditions = _watch_conditions(
                context_size_estimate=context_size_estimate,
                repetition_score=repetition_score,
                error_streak=error_streak,
                no_progress_turns=no_progress_turns,
            )
            watch_signal_count = sum(1 for cond in watch_conditions if cond)
            watch_escalation_streak = int(
                (self._watch_escalation_streak_by_chat or {}).get(chat_id, 0)
            )
            if watch_signal_count >= 2:
                watch_escalation_streak += 1
            else:
                watch_escalation_streak = 0
            self._watch_escalation_streak_by_chat[chat_id] = watch_escalation_streak
            severe = _is_reset_soon_severe(
                context_size_estimate=context_size_estimate,
                repetition_score=repetition_score,
                error_streak=error_streak,
                no_progress_turns=no_progress_turns,
            )
            context_health = (
                "RESET_SOON"
                if (severe or watch_escalation_streak >= 2)
                else ("WATCH" if watch_signal_count > 0 else "OK")
            )
            snapshot = {
                "chat_id": chat_id,
                "entry_count": entry_count,
                "context_size_estimate": context_size_estimate,
                "repetition_score": round(repetition_score, 3),
                "error_streak": error_streak,
                "no_progress_turns": no_progress_turns,
                "resets_since_progress": resets_since_progress,
                "overload_score": round(overload_score, 3),
                "watch_signal_count": watch_signal_count,
                "watch_escalation_streak": watch_escalation_streak,
                "context_health": context_health,
                "updated_at": utc_now().isoformat(),
            }
            self._context_health_by_chat[chat_id] = snapshot
            trace_output = {
                "context_health": context_health,
                "entry_count": entry_count,
                "context_size_estimate": context_size_estimate,
                "repetition_score": round(repetition_score, 3),
                "error_streak": error_streak,
                "no_progress_turns": no_progress_turns,
                "resets_since_progress": resets_since_progress,
                "overload_score": round(overload_score, 3),
                "watch_signal_count": watch_signal_count,
                "watch_escalation_streak": watch_escalation_streak,
            }
            previous_health = str(previous_snapshot.get("context_health") or "")
            if previous_health and previous_health != context_health and trace_ctx is not None:
                await self.trace_sink.annotate(
                    trace_ctx,
                    name="context.health.changed",
                    metadata={
                        "from_state": previous_health,
                        "to_state": context_health,
                        "chat_id": chat_id,
                    },
                )
            return snapshot
        except Exception as exc:
            trace_status = "error"
            trace_metadata.update(summarize_exception(exc))
            raise
        finally:
            trace_metadata["duration_ms"] = round(now_ms() - trace_started_at_ms, 2)
            await _finish_background_trace_context(
                self.trace_sink,
                trace_ctx,
                trace_token,
                is_root_trace=is_root_trace,
                status=trace_status,
                output=trace_output,
                metadata=trace_metadata,
            )

    async def build_heartbeat_context_hint(self, chat_id: int) -> str:
        snap = await self.get_context_health_snapshot(chat_id)
        return (
            "Context health metrics:\n"
            f"- context_size_estimate={snap['context_size_estimate']}\n"
            f"- repetition_score={snap['repetition_score']}\n"
            f"- error_streak={snap['error_streak']}\n"
            f"- no_progress_turns={snap['no_progress_turns']}\n"
            f"- resets_since_progress={snap['resets_since_progress']}\n"
            f"- overload_score={snap['overload_score']}\n"
            f"- watch_signal_count={snap['watch_signal_count']}\n"
            f"- watch_escalation_streak={snap['watch_escalation_streak']}\n"
            f"- context_health={snap['context_health']}\n"
            "Decision thresholds:\n"
            f"- WATCH if any: size>={_WATCH_THRESHOLDS['context_size_estimate']}, repetition>={_WATCH_THRESHOLDS['repetition_score']:.2f}, "
            f"error_streak>={_WATCH_THRESHOLDS['error_streak']}, no_progress>={_WATCH_THRESHOLDS['no_progress_turns']}.\n"
            f"- RESET_SOON if any: size>={_RESET_SOON_THRESHOLDS['context_size_estimate']}, repetition>={_RESET_SOON_THRESHOLDS['repetition_score']:.2f}, "
            f"error_streak>={_RESET_SOON_THRESHOLDS['error_streak']}, no_progress>={_RESET_SOON_THRESHOLDS['no_progress_turns']}.\n"
            "- Also RESET_SOON if 2+ WATCH signals persist for 2+ heartbeats.\n"
            "If context_health is RESET_SOON, call `octo_context_reset` with mode='soft' and a concise handoff."
        )

    def _register_progress(self, chat_id: int, reason: str) -> None:
        self._no_progress_turns_by_chat[chat_id] = 0
        self._reset_streak_without_progress_by_chat[chat_id] = 0
        self._progress_revision_by_chat[chat_id] = (
            int(self._progress_revision_by_chat.get(chat_id, 0)) + 1
        )
        logger.debug("Registered progress", chat_id=chat_id, reason=reason)

    async def request_context_reset(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        trace_started_at_ms = now_ms()
        mode = str(args.get("mode", "soft") or "soft").strip().lower()
        if mode not in {"soft", "hard"}:
            mode = "soft"

        reason = str(args.get("reason", "") or "").strip() or "context overloaded"
        confidence = _coerce_float(args.get("confidence"), default=0.8)
        confirm = bool(args.get("confirm", False))
        trace_metadata: dict[str, Any] = {
            "chat_id": chat_id,
            "mode": mode,
            "reason": reason,
            "confidence": confidence,
            "confirm": confirm,
        }
        trace_ctx, trace_token, is_root_trace = await _start_background_trace_context(
            self.trace_sink,
            name="context.reset",
            chat_id=chat_id,
            correlation_id=str(correlation_id_var.get() or "").strip() or None,
            metadata=trace_metadata,
        )
        trace_status = "ok"
        trace_output: dict[str, Any] | None = None
        try:
            health = await self.get_context_health_snapshot(chat_id)

            progress_rev = int(self._progress_revision_by_chat.get(chat_id, 0))
            last_reset_rev = int(self._last_reset_progress_revision_by_chat.get(chat_id, -1))
            no_progress_since_last_reset = progress_rev <= last_reset_rev
            current_streak = int(self._reset_streak_without_progress_by_chat.get(chat_id, 0))
            proposed_streak = (current_streak + 1) if no_progress_since_last_reset else 1

            requires_confirm_reasons: list[str] = []
            if mode == "hard":
                requires_confirm_reasons.append("hard_reset")
            if confidence < _RESET_CONFIDENCE_MIN:
                requires_confirm_reasons.append("low_confidence_handoff")
            if proposed_streak >= _RESET_CONFIRM_THRESHOLD:
                requires_confirm_reasons.append("repeated_reset_without_progress")
            trace_metadata["requires_confirmation_for"] = list(requires_confirm_reasons)
            if requires_confirm_reasons and not confirm:
                if trace_ctx is not None:
                    await self.trace_sink.annotate(
                        trace_ctx,
                        name="context.reset_requested",
                        metadata={
                            "status": "needs_confirmation",
                            "requires_confirmation_for": list(requires_confirm_reasons),
                            "chat_id": chat_id,
                        },
                    )
                trace_output = {
                    "status": "needs_confirmation",
                    "requires_confirmation_for": list(requires_confirm_reasons),
                    "health_before": health,
                }
                return {
                    "status": "needs_confirmation",
                    "mode": mode,
                    "reason": reason,
                    "confidence": confidence,
                    "requires_confirmation_for": requires_confirm_reasons,
                    "message": (
                        "Reset blocked until confirmation. Re-run octo_context_reset with confirm=true "
                        "to proceed."
                    ),
                    "health": health,
                }

            handoff = {
                "chat_id": chat_id,
                "created_at": utc_now().isoformat(),
                "mode": mode,
                "reason": reason,
                "confidence": confidence,
                "goal_now": str(args.get("goal_now", "") or "").strip(),
                "done": _normalize_string_list(args.get("done")),
                "open_threads": _normalize_string_list(args.get("open_threads")),
                "critical_constraints": _normalize_string_list(args.get("critical_constraints")),
                "next_step": str(args.get("next_step", "") or "").strip(),
                "current_interest": str(args.get("current_interest", "") or "").strip(),
                "pending_human_input": str(args.get("pending_human_input", "") or "").strip(),
                "cognitive_state": str(args.get("cognitive_state", "") or "focused")
                .strip()
                .lower(),
                "health_snapshot": health,
            }
            if not handoff["goal_now"]:
                handoff["goal_now"] = "Continue current task with focused context."
            if not handoff["next_step"]:
                handoff["next_step"] = "Review handoff and choose: continue, clarify, or replan."

            try:
                if trace_ctx is not None:
                    await self.trace_sink.annotate(
                        trace_ctx,
                        name="context.reset_requested",
                        metadata={
                            "status": "executing",
                            "mode": mode,
                            "chat_id": chat_id,
                        },
                    )
                workspace_dir = Path(os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")).resolve()
                file_info = await asyncio.to_thread(
                    _persist_context_reset_files, workspace_dir, handoff
                )
                reflection_entry: dict[str, Any] | None = None
                if self.reflection is not None:
                    try:
                        record = await asyncio.to_thread(
                            self.reflection.record_context_reset,
                            chat_id,
                            handoff,
                        )
                        reflection_entry = {
                            "id": record.id,
                            "kind": record.kind,
                            "summary": record.summary,
                        }
                    except Exception:
                        logger.warning(
                            "Reflection record failed during context reset",
                            chat_id=chat_id,
                            exc_info=True,
                        )
                memchain_info: dict[str, Any] | None = None
                try:
                    memchain_info = await asyncio.to_thread(
                        memchain_record,
                        workspace_dir,
                        reason="context_reset",
                        meta={"mode": mode, "chat_id": chat_id, "source": "octo_context_reset"},
                    )
                except Exception as exc:
                    logger.warning(
                        "Memchain record failed during context reset",
                        chat_id=chat_id,
                        error=str(exc),
                    )

                deleted_entries = await asyncio.to_thread(
                    self.store.delete_memory_entries_by_chat,
                    chat_id,
                    0,
                )
                if mode == "hard":
                    await asyncio.to_thread(
                        self.store.set_chat_bootstrap_hash, chat_id, "", utc_now()
                    )

                self._last_reply_norm_by_chat.pop(chat_id, None)
                self._last_reset_progress_revision_by_chat[chat_id] = progress_rev
                self._reset_streak_without_progress_by_chat[chat_id] = proposed_streak
                self._pending_wakeup_by_chat[chat_id] = _build_wakeup_message(
                    handoff, file_info["handoff_md"]
                )
                self._no_progress_turns_by_chat[chat_id] = 0

                await asyncio.to_thread(
                    self.store.append_audit,
                    AuditEvent(
                        id=str(uuid4()),
                        ts=utc_now(),
                        level="info",
                        event_type="octo.context_reset",
                        data={
                            "chat_id": chat_id,
                            "mode": mode,
                            "reason": reason,
                            "confidence": confidence,
                            "deleted_entries": deleted_entries,
                            "requires_confirmation_for": requires_confirm_reasons,
                            "health_snapshot": health,
                            "files": file_info,
                            "reflection": reflection_entry or {},
                            "memchain": memchain_info or {},
                        },
                    ),
                )
                if trace_ctx is not None:
                    await self.trace_sink.annotate(
                        trace_ctx,
                        name="context.reset_completed",
                        metadata={
                            "mode": mode,
                            "deleted_entries": deleted_entries,
                            "handoff_written": bool(file_info.get("handoff_md")),
                            "reflection_written": bool(reflection_entry),
                            "memchain_written": bool(memchain_info),
                            "chat_id": chat_id,
                        },
                    )
                trace_output = {
                    "status": "reset_complete",
                    "mode": mode,
                    "deleted_entries": deleted_entries,
                    "health_before": health,
                    "handoff_written": bool(file_info.get("handoff_md")),
                    "reflection_written": bool(reflection_entry),
                    "memchain_written": bool(memchain_info),
                }
                return {
                    "status": "reset_complete",
                    "mode": mode,
                    "deleted_entries": deleted_entries,
                    "handoff": handoff,
                    "files": file_info,
                    "reflection": reflection_entry or {},
                    "memchain": memchain_info or {},
                    "health_before": health,
                    "requires_confirmation_for": requires_confirm_reasons,
                    "message": "Context reset completed. Wake-up handoff is queued for the next turn.",
                }
            except Exception as exc:
                trace_status = "error"
                trace_metadata.update(summarize_exception(exc))
                raise
        except Exception as exc:
            trace_status = "error"
            trace_metadata.update(summarize_exception(exc))
            raise
        finally:
            trace_metadata["duration_ms"] = round(now_ms() - trace_started_at_ms, 2)
            await _finish_background_trace_context(
                self.trace_sink,
                trace_ctx,
                trace_token,
                is_root_trace=is_root_trace,
                status=trace_status,
                output=trace_output,
                metadata=trace_metadata,
            )
