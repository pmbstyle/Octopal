from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from octopal.infrastructure.observability.helpers import safe_preview
from octopal.infrastructure.providers.base import Message
from octopal.runtime.octo.route_loop_helpers import normalize_plain_text
from octopal.utils import should_suppress_user_delivery

logger = structlog.get_logger(__name__)


def _log_system_prompt(messages: list[Message], label: str) -> None:
    system_lengths = [len(m.content) for m in messages if m.role == "system" and m.content]
    if system_lengths:
        logger.debug(
            "Octo system prompt",
            label=label,
            parts=len(system_lengths),
            total_chars=sum(system_lengths),
        )


def _build_partial_callback(
    *,
    octo: Any,
    chat_id: int,
) -> Callable[[str], Awaitable[None]] | None:
    if chat_id <= 0 or not getattr(octo, "is_ws_active", False):
        return None
    sender = getattr(octo, "emit_ws_progress", None)
    if not callable(sender):
        sender = getattr(octo, "internal_progress_send", None)
    if not callable(sender):
        return None

    async def _on_partial(text: str) -> None:
        clean = normalize_plain_text(text or "")
        if not clean:
            return
        if should_suppress_user_delivery(clean):
            return
        try:
            await sender(chat_id, "partial", clean, {})
        except Exception:
            logger.debug("Failed to emit partial stream", chat_id=chat_id, exc_info=True)

    return _on_partial


async def _emit_octo_tool_use_event(
    *,
    octo: Any,
    chat_id: int,
    tool_name: str,
    args: dict[str, Any],
) -> None:
    if chat_id <= 0 or not getattr(octo, "is_ws_active", False):
        return
    sender = getattr(octo, "emit_ws_progress", None)
    if not callable(sender):
        return

    normalized_name = str(tool_name or "").strip()
    if not normalized_name:
        return
    display_name = normalized_name
    if normalized_name in {"start_worker", "start_child_worker"}:
        worker_label = str(args.get("worker_id") or "").strip()
        display_name = f"{worker_label} worker" if worker_label else "worker"
        text = f"Octo starting {display_name}"
    elif normalized_name == "start_workers_parallel":
        task_count = len(args.get("tasks") or []) if isinstance(args.get("tasks"), list) else 0
        text = f"Octo starting {task_count or 'multiple'} workers"
    else:
        text = f"Octo using {display_name}"

    try:
        await sender(
            chat_id,
            "tool_start",
            text,
            {
                "tool_name": normalized_name,
                "args_preview": safe_preview(args, limit=240),
            },
        )
    except Exception:
        logger.debug(
            "Failed to emit Octo tool use event",
            chat_id=chat_id,
            tool_name=normalized_name,
            exc_info=True,
        )
