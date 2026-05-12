from __future__ import annotations

from typing import Any

import structlog

from octopal.infrastructure.observability.base import now_ms
from octopal.infrastructure.observability.helpers import safe_preview, summarize_exception
from octopal.runtime.octo.background_tracing import (
    _finish_background_trace_context,
    _start_background_trace_context,
)
from octopal.runtime.octo.delivery import resolve_user_delivery

logger = structlog.get_logger(__name__)


async def _send_worker_followup(
    octo: Any,
    chat_id: int,
    correlation_id: str | None,
    text: str,
    *,
    batched_count: int = 1,
) -> None:
    trace_started_at_ms = now_ms()
    trace_metadata: dict[str, Any] = {
        "delivery_channel": "internal",
        "delivery_source": "worker_followup",
        "correlation_id": correlation_id,
        "batched_count": batched_count,
        "text_preview": safe_preview(text, limit=240),
        "text_len": len(text or ""),
    }
    trace_ctx, trace_token, is_root_trace = await _start_background_trace_context(
        octo.trace_sink,
        name="channel.delivery",
        chat_id=chat_id,
        correlation_id=correlation_id,
        metadata=trace_metadata,
    )
    trace_status = "ok"
    trace_output: dict[str, Any] | None = None
    try:
        decision = resolve_user_delivery(text)
        trace_metadata.update(
            {
                "delivery_mode": decision.mode,
                "user_visible": decision.user_visible,
                "suppressed_reason": decision.reason,
            }
        )
        if not decision.user_visible:
            trace_output = {"status": "suppressed", "reason": decision.reason}
            logger.info(
                "Internal worker follow-up skipped", chat_id=chat_id, reason=decision.reason
            )
            return
        if octo.internal_send:
            await octo.internal_send(chat_id, decision.text)
            octo.note_user_visible_delivery(chat_id, decision.text)
            octo.clear_pending_conversational_closure(correlation_id)
            trace_output = {
                "status": "sent",
                "message_len": len(decision.text),
                "batched_count": batched_count,
            }
            logger.info(
                "Internal worker follow-up sent",
                chat_id=chat_id,
                text_len=len(decision.text),
                batched_count=batched_count,
            )
            await octo.memory.add_message(
                "assistant",
                decision.text,
                {
                    "chat_id": chat_id,
                    "worker_followup": True,
                    "batched_count": batched_count,
                },
            )
            return
        trace_status = "error"
        trace_metadata["error_type"] = "no_sender_attached"
        trace_output = {"status": "dropped", "reason": "no_sender_attached"}
        logger.info(
            "Worker follow-up produced but no sender attached",
            chat_id=chat_id,
            text_len=len(text),
            batched_count=batched_count,
        )
    except Exception as exc:
        trace_status = "error"
        trace_metadata.update(summarize_exception(exc))
        trace_output = {"status": "failed"}
        raise
    finally:
        trace_metadata["duration_ms"] = round(now_ms() - trace_started_at_ms, 2)
        await _finish_background_trace_context(
            octo.trace_sink,
            trace_ctx,
            trace_token,
            is_root_trace=is_root_trace,
            status=trace_status,
            output=trace_output,
            metadata=trace_metadata,
        )


async def _send_scheduler_control_update(
    octo: Any,
    chat_id: int,
    task_id: str | None,
    text: str,
) -> None:
    trace_started_at_ms = now_ms()
    trace_metadata: dict[str, Any] = {
        "delivery_channel": "internal",
        "delivery_source": "scheduler_octo_control",
        "scheduled_task_id": task_id,
        "text_preview": safe_preview(text, limit=240),
        "text_len": len(text or ""),
    }
    trace_ctx, trace_token, is_root_trace = await _start_background_trace_context(
        octo.trace_sink,
        name="channel.delivery",
        chat_id=chat_id,
        correlation_id=None,
        metadata=trace_metadata,
    )
    trace_status = "ok"
    trace_output: dict[str, Any] | None = None
    try:
        decision = resolve_user_delivery(text)
        trace_metadata.update(
            {
                "delivery_mode": decision.mode,
                "user_visible": decision.user_visible,
                "suppressed_reason": decision.reason,
            }
        )
        if not decision.user_visible:
            trace_output = {"status": "suppressed", "reason": decision.reason}
            logger.info(
                "Scheduled Octo control update skipped",
                chat_id=chat_id,
                task_id=task_id,
                reason=decision.reason,
            )
            return
        if octo.internal_send:
            await octo.internal_send(chat_id, decision.text)
            octo.note_user_visible_delivery(chat_id, decision.text)
            trace_output = {
                "status": "sent",
                "message_len": len(decision.text),
            }
            logger.info(
                "Scheduled Octo control update sent",
                chat_id=chat_id,
                task_id=task_id,
                text_len=len(decision.text),
            )
            await octo.memory.add_message(
                "assistant",
                decision.text,
                {
                    "chat_id": chat_id,
                    "background_delivery": True,
                    "scheduler_octo_control": True,
                    "scheduled_task_id": task_id,
                },
            )
            return
        trace_status = "error"
        trace_metadata["error_type"] = "no_sender_attached"
        trace_output = {"status": "dropped", "reason": "no_sender_attached"}
        logger.info(
            "Scheduled Octo control update produced but no sender attached",
            chat_id=chat_id,
            task_id=task_id,
            text_len=len(text),
        )
    except Exception as exc:
        trace_status = "error"
        trace_metadata.update(summarize_exception(exc))
        trace_output = {"status": "failed"}
        raise
    finally:
        trace_metadata["duration_ms"] = round(now_ms() - trace_started_at_ms, 2)
        await _finish_background_trace_context(
            octo.trace_sink,
            trace_ctx,
            trace_token,
            is_root_trace=is_root_trace,
            status=trace_status,
            output=trace_output,
            metadata=trace_metadata,
        )
