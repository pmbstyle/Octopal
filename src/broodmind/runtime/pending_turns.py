from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class PendingTurn:
    chat_id: int
    text_parts: list[str] = field(default_factory=list)
    images: list[str] = field(default_factory=list)
    saved_file_paths: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    timer_task: asyncio.Task[None] | None = None

    def merged_text(self) -> str:
        return "\n\n".join(part for part in self.text_parts if part.strip()).strip()


class PendingTurnAggregator:
    def __init__(
        self,
        *,
        grace_seconds: float,
        flush_callback: Callable[[int, str, list[str], list[str], dict[str, Any]], Awaitable[None]],
    ) -> None:
        self.grace_seconds = max(0.0, float(grace_seconds))
        self._flush_callback = flush_callback
        self._pending_by_chat: dict[int, PendingTurn] = {}
        self._guard = asyncio.Lock()

    async def submit(
        self,
        *,
        chat_id: int,
        text: str,
        images: list[str] | None = None,
        saved_file_paths: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        payload_images = list(images or [])
        payload_paths = list(saved_file_paths or [])
        payload_meta = dict(metadata or {})

        async with self._guard:
            pending = self._pending_by_chat.get(chat_id)
            if pending is None:
                pending = PendingTurn(chat_id=chat_id)
                self._pending_by_chat[chat_id] = pending

            if text.strip():
                pending.text_parts.append(text.strip())
            if payload_images:
                pending.images.extend(payload_images)
            if payload_paths:
                pending.saved_file_paths.extend(payload_paths)
            if payload_meta:
                pending.metadata.update(payload_meta)

            if pending.timer_task and not pending.timer_task.done():
                pending.timer_task.cancel()

            if self.grace_seconds <= 0:
                pending.timer_task = None
                should_flush_now = True
            else:
                pending.timer_task = asyncio.create_task(self._sleep_then_flush(chat_id))
                should_flush_now = False

        if should_flush_now:
            await self._flush(chat_id)

    async def stop(self) -> None:
        async with self._guard:
            tasks = [
                pending.timer_task
                for pending in self._pending_by_chat.values()
                if pending.timer_task and not pending.timer_task.done()
            ]
            self._pending_by_chat.clear()

        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _sleep_then_flush(self, chat_id: int) -> None:
        try:
            await asyncio.sleep(self.grace_seconds)
            await self._flush(chat_id)
        except asyncio.CancelledError:
            raise

    async def _flush(self, chat_id: int) -> None:
        pending: PendingTurn | None = None
        async with self._guard:
            pending = self._pending_by_chat.pop(chat_id, None)

        if pending is None:
            return

        text = pending.merged_text()
        if not text and not pending.images:
            logger.debug("Skipping empty pending turn flush", chat_id=chat_id)
            return

        logger.debug(
            "Flushing pending user turn",
            chat_id=chat_id,
            text_parts=len(pending.text_parts),
            images=len(pending.images),
            saved_paths=len(pending.saved_file_paths),
        )
        try:
            await self._flush_callback(
                chat_id,
                text,
                list(pending.images),
                list(pending.saved_file_paths),
                dict(pending.metadata),
            )
        except Exception:
            logger.exception("Pending turn flush failed", chat_id=chat_id)
