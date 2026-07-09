from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

PendingTurnKey = tuple[int, str]
FlushCallback = Callable[[int, str, list[str], list[str], dict[str, Any]], Awaitable[None]]
TerminalFailureCallback = Callable[
    [int, str, list[str], list[str], dict[str, Any], Exception], Awaitable[None]
]


@dataclass(slots=True)
class PendingTurn:
    chat_id: int
    sender_id: str = ""
    text_parts: list[str] = field(default_factory=list)
    images: list[str] = field(default_factory=list)
    saved_file_paths: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    timer_task: asyncio.Task[None] | None = None
    failed_attempts: int = 0

    def merged_text(self) -> str:
        return "\n\n".join(part for part in self.text_parts if part.strip()).strip()


class PendingTurnAggregator:
    def __init__(
        self,
        *,
        grace_seconds: float,
        flush_callback: FlushCallback,
        terminal_failure_callback: TerminalFailureCallback | None = None,
        retry_seconds: float = 1.0,
        max_retry_seconds: float = 30.0,
        max_flush_attempts: int = 3,
    ) -> None:
        self.grace_seconds = max(0.0, float(grace_seconds))
        self.retry_seconds = max(0.01, float(retry_seconds))
        self.max_retry_seconds = max(self.retry_seconds, float(max_retry_seconds))
        self.max_flush_attempts = max(1, int(max_flush_attempts))
        self._flush_callback = flush_callback
        self._terminal_failure_callback = terminal_failure_callback
        self._pending_by_key: dict[PendingTurnKey, PendingTurn] = {}
        self._guard = asyncio.Lock()
        self._stopped = False

    async def submit(
        self,
        *,
        chat_id: int,
        sender_id: str | int | None = None,
        text: str,
        images: list[str] | None = None,
        saved_file_paths: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        payload_images = list(images or [])
        payload_paths = list(saved_file_paths or [])
        payload_meta = dict(metadata or {})
        normalized_sender_id = str(sender_id or "").strip()
        key = (chat_id, normalized_sender_id)

        async with self._guard:
            if self._stopped:
                raise RuntimeError("Pending turn aggregator is stopped")
            pending = self._pending_by_key.get(key)
            if pending is None:
                pending = PendingTurn(chat_id=chat_id, sender_id=normalized_sender_id)
                self._pending_by_key[key] = pending

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
                pending.timer_task = asyncio.create_task(
                    self._sleep_then_flush(key, self.grace_seconds)
                )
                should_flush_now = False

        if should_flush_now:
            await self._flush(key)

    async def stop(self) -> None:
        async with self._guard:
            self._stopped = True
            tasks = [
                pending.timer_task
                for pending in self._pending_by_key.values()
                if pending.timer_task and not pending.timer_task.done()
            ]
            self._pending_by_key.clear()

        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task

    async def _sleep_then_flush(self, key: PendingTurnKey, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            await self._flush(key)
        except asyncio.CancelledError:
            raise

    async def _flush(self, key: PendingTurnKey) -> None:
        pending: PendingTurn | None = None
        async with self._guard:
            pending = self._pending_by_key.pop(key, None)

        if pending is None:
            return

        chat_id = pending.chat_id
        text = pending.merged_text()
        if not text and not pending.images and not pending.saved_file_paths:
            logger.debug("Skipping empty pending turn flush", chat_id=chat_id)
            return

        logger.debug(
            "Flushing pending user turn",
            chat_id=chat_id,
            sender_id=pending.sender_id or None,
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
        except Exception as exc:
            pending.failed_attempts += 1
            if pending.failed_attempts >= self.max_flush_attempts:
                logger.exception(
                    "Pending turn flush failed permanently",
                    chat_id=chat_id,
                    sender_id=pending.sender_id or None,
                    attempts=pending.failed_attempts,
                )
                await self._notify_terminal_failure(pending, text, exc)
                return

            retry_delay = min(
                self.max_retry_seconds,
                self.retry_seconds * (2.0 ** min(pending.failed_attempts - 1, 16)),
            )
            logger.exception(
                "Pending turn flush failed; scheduling retry",
                chat_id=chat_id,
                sender_id=pending.sender_id or None,
                attempt=pending.failed_attempts,
                max_attempts=self.max_flush_attempts,
                retry_seconds=retry_delay,
            )
            await self._requeue_failed(key, pending, retry_delay=retry_delay)

    async def _requeue_failed(
        self, key: PendingTurnKey, failed: PendingTurn, *, retry_delay: float
    ) -> None:
        async with self._guard:
            if self._stopped:
                return

            newer = self._pending_by_key.get(key)
            if newer is not None and newer.timer_task and not newer.timer_task.done():
                newer.timer_task.cancel()

            pending = _merge_pending_turns(failed, newer) if newer is not None else failed
            pending.timer_task = asyncio.create_task(self._sleep_then_flush(key, retry_delay))
            self._pending_by_key[key] = pending

    async def _notify_terminal_failure(
        self, pending: PendingTurn, text: str, exc: Exception
    ) -> None:
        if self._terminal_failure_callback is None or self._stopped:
            return
        try:
            await self._terminal_failure_callback(
                pending.chat_id,
                text,
                list(pending.images),
                list(pending.saved_file_paths),
                dict(pending.metadata),
                exc,
            )
        except Exception:
            logger.exception(
                "Pending turn terminal failure notification failed",
                chat_id=pending.chat_id,
                sender_id=pending.sender_id or None,
            )


def _merge_pending_turns(older: PendingTurn, newer: PendingTurn) -> PendingTurn:
    return PendingTurn(
        chat_id=older.chat_id,
        sender_id=older.sender_id,
        text_parts=[*older.text_parts, *newer.text_parts],
        images=[*older.images, *newer.images],
        saved_file_paths=[*older.saved_file_paths, *newer.saved_file_paths],
        metadata={**older.metadata, **newer.metadata},
        failed_attempts=max(older.failed_attempts, newer.failed_attempts),
    )
