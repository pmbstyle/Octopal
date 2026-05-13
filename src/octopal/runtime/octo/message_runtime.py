from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import structlog

from octopal.infrastructure.logging import correlation_id_var
from octopal.infrastructure.observability.base import (
    bind_trace_context,
    now_ms,
    reset_trace_context,
)
from octopal.infrastructure.observability.helpers import (
    hash_payload,
    safe_preview,
    summarize_exception,
)
from octopal.runtime.octo import followup_pipeline as _followup_pipeline
from octopal.runtime.octo.context_health import _is_progress_reply
from octopal.runtime.octo.context_reset import _normalize_compact
from octopal.runtime.octo.control_plane import (
    RouteMode,
    RouteRequest,
    resolve_turn_route_mode,
)
from octopal.runtime.octo.control_replies import (
    _coerce_control_plane_reply,
    _normalize_heartbeat_delivery_reply,
)
from octopal.runtime.octo.delivery import resolve_user_delivery
from octopal.runtime.octo.prompt_builder import (
    build_bootstrap_context_prompt as _default_build_bootstrap_context_prompt,
)
from octopal.runtime.octo.reply import OctoReply
from octopal.runtime.octo.router import route_heartbeat as _default_route_heartbeat
from octopal.runtime.octo.router import route_or_reply as _default_route_or_reply
from octopal.runtime.state import update_last_internal_heartbeat
from octopal.utils import (
    extract_reaction_and_strip,
    sanitize_user_facing_text_preserving_reaction,
    utc_now,
)

logger = structlog.get_logger(__name__)

_discard_worker_followup_batch = _followup_pipeline._discard_worker_followup_batch
_schedule_worker_followup_flush = _followup_pipeline._schedule_worker_followup_flush


def _build_user_memory_content(
    text: str,
    *,
    images: list[str] | None = None,
    saved_file_paths: list[str] | None = None,
) -> str:
    trimmed = (text or "").strip()
    has_images = bool(images)
    normalized_paths = [str(path).strip() for path in (saved_file_paths or []) if str(path).strip()]
    if not has_images and not normalized_paths:
        return trimmed

    parts: list[str] = []
    if trimmed:
        parts.append(trimmed)
    elif has_images:
        parts.append("User uploaded image attachment(s).")
    else:
        parts.append("User uploaded file attachment(s).")

    attachment_lines: list[str] = []
    if has_images:
        attachment_lines.append(f"- image_count={len(images or [])}")
    if normalized_paths:
        attachment_lines.append("- saved_file_paths:")
        attachment_lines.extend(f"  - {path}" for path in normalized_paths)

    if attachment_lines:
        parts.append("Attachments received:\n" + "\n".join(attachment_lines))
    return "\n\n".join(parts)


def _core_callable(name: str, default: Callable[..., Any]) -> Callable[..., Any]:
    core_module = sys.modules.get("octopal.runtime.octo.core")
    if core_module is not None:
        candidate = getattr(core_module, name, None)
        if callable(candidate):
            return candidate
    return default


class OctoMessageRuntimeMixin:
    async def handle_message(
        self,
        text: str,
        chat_id: int,
        approval_requester=None,
        show_typing: bool = True,
        is_ws: bool = False,
        images: list[str] | None = None,
        saved_file_paths: list[str] | None = None,
        persist_to_memory: bool = True,
        track_progress: bool = True,
        include_wakeup: bool = True,
        background_delivery: bool = False,
    ) -> OctoReply:
        if not is_ws and self._ws_active:
            logger.info("Ignoring Telegram message while WebSocket is active", chat_id=chat_id)
            return OctoReply(
                immediate="I'm currently active on WebSocket. Please use the WebSocket client or wait until it's closed.",
                followup=None,
            )
        correlation_token = None
        correlation_id = correlation_id_var.get()
        trace_bind_token = None
        trace_ctx = None
        trace_started_at_ms = now_ms()
        trace_status = "ok"
        trace_output: dict[str, Any] | None = None
        trace_metadata: dict[str, Any] = {
            "channel": "ws" if is_ws else "telegram",
            "message_kind": "heartbeat" if not track_progress else "user",
            "text_len": len(text),
            "has_images": bool(images),
            "has_files": bool(saved_file_paths),
            "persist_to_memory": persist_to_memory,
            "track_progress": track_progress,
            "background_delivery": background_delivery,
        }
        wants_followup = False
        finalized_visible_reply = False
        route_request = RouteRequest(
            mode=resolve_turn_route_mode(
                track_progress=track_progress,
                background_delivery=background_delivery,
            ),
            user_text=text,
            chat_id=chat_id,
            show_typing=show_typing,
            include_wakeup=include_wakeup,
            track_progress=track_progress,
            background_delivery=background_delivery,
        )
        trace_metadata["route_mode"] = route_request.mode.value
        if not correlation_id:
            correlation_id = f"turn-{uuid4()}"
            correlation_token = correlation_id_var.set(correlation_id)

        try:
            session_id = f"{'ws' if is_ws else 'chat'}:{chat_id}"
            if self.trace_sink is not None:
                trace_ctx = await self.trace_sink.start_trace(
                    name="octo.turn",
                    trace_id=correlation_id,
                    root_trace_id=correlation_id,
                    session_id=session_id,
                    chat_id=chat_id,
                    input={
                        "text_preview": safe_preview(text, limit=160),
                        "text_hash": hash_payload(text),
                    },
                    metadata=trace_metadata,
                )
                trace_bind_token = bind_trace_context(trace_ctx)
            self.mark_user_turn_active(correlation_id)
            if callable(approval_requester):
                self._approval_requesters[chat_id] = approval_requester
            logger.info(
                "Handling message",
                chat_id=chat_id,
                is_ws=is_ws,
                has_images=bool(images),
                route_mode=route_request.mode.value,
            )
            logger.debug("Received message text", text_len=len(text), text=text[:500])
            if not track_progress:
                self.suppress_turn_followups(correlation_id)
            if persist_to_memory:
                user_memory_content = _build_user_memory_content(
                    text,
                    images=images,
                    saved_file_paths=saved_file_paths,
                )
                await self.memory.add_message(
                    "user",
                    user_memory_content,
                    {
                        "chat_id": chat_id,
                        "has_images": bool(images),
                        "has_files": bool(saved_file_paths),
                        "saved_file_paths": list(saved_file_paths or []),
                        "heartbeat": not track_progress,
                        "fact_candidate": False,
                    },
                )
            bootstrap_context = None
            if route_request.mode is RouteMode.HEARTBEAT:
                reply_text = await _core_callable("route_heartbeat", _default_route_heartbeat)(
                    self,
                    chat_id,
                    text,
                    show_typing=show_typing,
                    include_wakeup=include_wakeup,
                )
            else:
                bootstrap_context = await _core_callable(
                    "build_bootstrap_context_prompt",
                    _default_build_bootstrap_context_prompt,
                )(self.store, chat_id)
                trace_metadata["bootstrap_chars"] = len(bootstrap_context.content)
                if bootstrap_context.files:
                    files_summary = ", ".join(
                        [f"{name} ({size} chars)" for name, size in bootstrap_context.files]
                    )
                    logger.debug(
                        "Octo bootstrap files",
                        route_mode=route_request.mode.value,
                        files=files_summary,
                        file_count=len(bootstrap_context.files),
                        total_chars=len(bootstrap_context.content),
                        hash=bootstrap_context.hash,
                    )
                route_kwargs: dict[str, Any] = {
                    "show_typing": show_typing,
                    "images": images,
                    "saved_file_paths": saved_file_paths,
                    "include_wakeup": include_wakeup,
                    "route_mode": route_request.mode,
                }
                route_or_reply = _core_callable("route_or_reply", _default_route_or_reply)
                while True:
                    try:
                        reply_text = await route_or_reply(
                            self,
                            self.provider,
                            self.memory,
                            text,
                            chat_id,
                            bootstrap_context.content,
                            **route_kwargs,
                        )
                        break
                    except TypeError as exc:
                        # Backward-compatible fallback for monkeypatched tests/extensions using older signatures.
                        msg = str(exc)
                        if "unexpected keyword argument" not in msg:
                            raise
                        removed = False
                        for key in list(route_kwargs.keys()):
                            if f"'{key}'" in msg:
                                route_kwargs.pop(key, None)
                                removed = True
                                break
                        if not removed:
                            raise
            initial_reaction_emoji, _ = extract_reaction_and_strip(reply_text or "")
            wants_followup = self.consume_structured_followup_required(correlation_id)
            if not track_progress:
                wants_followup = False
                if background_delivery:
                    reply_text = await _normalize_heartbeat_delivery_reply(
                        self.provider, reply_text
                    )
                else:
                    reply_text = _coerce_control_plane_reply(reply_text)
                if route_request.mode is RouteMode.HEARTBEAT:
                    runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
                    if runtime_settings is not None:
                        await asyncio.to_thread(
                            update_last_internal_heartbeat,
                            runtime_settings,
                        )
            logger.info("Octo response ready")
            if track_progress:
                reply_norm = _normalize_compact(reply_text)
                prior_reply = self._last_reply_norm_by_chat.get(chat_id, "")
                if _is_progress_reply(reply_norm, prior_reply):
                    self._register_progress(chat_id, "assistant_response")
                else:
                    self._no_progress_turns_by_chat[chat_id] = (
                        int(self._no_progress_turns_by_chat.get(chat_id, 0)) + 1
                    )
                self._last_reply_norm_by_chat[chat_id] = reply_norm
            if wants_followup:
                self.mark_pending_conversational_closure(correlation_id)
            try:
                await self.get_context_health_snapshot(chat_id)
            except Exception:
                logger.debug(
                    "Failed to refresh context health snapshot",
                    chat_id=chat_id,
                    exc_info=True,
                )
            if include_wakeup:
                self.clear_context_wakeup(chat_id)
            if bootstrap_context is not None and bootstrap_context.hash:
                await asyncio.to_thread(
                    self.store.set_chat_bootstrap_hash,
                    chat_id,
                    bootstrap_context.hash,
                    utc_now(),
                )
            immediate_text = sanitize_user_facing_text_preserving_reaction(reply_text)
            reaction_emoji, _ = extract_reaction_and_strip(reply_text or "")
            reaction_emoji = reaction_emoji or initial_reaction_emoji
            delivery = resolve_user_delivery(immediate_text, followup_required=wants_followup)
            logger.debug(
                "OctoReply prepared for channel delivery",
                chat_id=chat_id,
                route_mode=route_request.mode.value,
                has_react_tag="<react>" in immediate_text.lower(),
                reaction=reaction_emoji,
                delivery_mode=delivery.mode,
            )
            if not persist_to_memory and delivery.user_visible:
                await self.memory.add_message(
                    "assistant",
                    delivery.text,
                    {
                        "chat_id": chat_id,
                        "background_delivery": True,
                        "heartbeat": not track_progress,
                    },
                )
            elif persist_to_memory and delivery.user_visible:
                await self.memory.add_message(
                    "assistant",
                    delivery.text,
                    {
                        "chat_id": chat_id,
                        "heartbeat": not track_progress,
                    },
                )
            if delivery.user_visible and track_progress:
                self.note_user_visible_delivery(chat_id, delivery.text)
                if not delivery.followup_required:
                    finalized_visible_reply = True
                    self.suppress_turn_followups(correlation_id)
                    logger.info(
                        "Suppressing worker follow-ups after final in-turn reply",
                        chat_id=chat_id,
                    )
            trace_output = {
                "delivery_mode": delivery.mode,
                "followup_required": delivery.followup_required,
                "user_visible": delivery.user_visible,
                "reaction": reaction_emoji,
            }
            return OctoReply(
                immediate=delivery.text,
                followup=None,
                followup_required=delivery.followup_required,
                reaction=reaction_emoji,
                delivery_mode=delivery.mode,
            )
        except Exception as exc:
            trace_status = "error"
            trace_metadata.update(summarize_exception(exc))
            raise
        finally:
            self.mark_user_turn_inactive(correlation_id)
            if track_progress and not finalized_visible_reply:
                self.clear_suppressed_turn_followups(correlation_id)
            if finalized_visible_reply:
                dropped = _discard_worker_followup_batch(
                    chat_id,
                    correlation_id,
                    only_if_created_during_active_turn=True,
                )
                if dropped:
                    logger.info(
                        "Dropped worker follow-up after final in-turn reply",
                        chat_id=chat_id,
                    )
            pending_followup_work = self.has_active_workers_for_correlation(
                correlation_id
            ) or self.has_pending_internal_results_for_correlation(correlation_id)
            if wants_followup and pending_followup_work and not finalized_visible_reply:
                _schedule_worker_followup_flush(self, chat_id, correlation_id)
            else:
                _discard_worker_followup_batch(
                    chat_id,
                    correlation_id,
                    only_if_created_during_active_turn=True,
                )
            self.clear_structured_followup_required(correlation_id)
            if trace_ctx is not None and self.trace_sink is not None:
                finish_meta = dict(trace_metadata)
                finish_meta.update(
                    {
                        "duration_ms": round(now_ms() - trace_started_at_ms, 2),
                        "wants_followup": wants_followup,
                        "finalized_visible_reply": finalized_visible_reply,
                        "active_workers_for_correlation": self.has_active_workers_for_correlation(
                            correlation_id
                        ),
                        "pending_internal_results": self.has_pending_internal_results_for_correlation(
                            correlation_id
                        ),
                    }
                )
                await self.trace_sink.finish_trace(
                    trace_ctx,
                    status=trace_status,
                    output=trace_output,
                    metadata=finish_meta,
                )
            if trace_bind_token is not None:
                reset_trace_context(trace_bind_token)
            if correlation_token is not None:
                correlation_id_var.reset(correlation_token)
