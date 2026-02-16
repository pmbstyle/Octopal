from __future__ import annotations

import asyncio
import re
import structlog
import uuid
from typing import Any, NamedTuple

import telegramify_markdown
from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, Message

from broodmind.config.settings import Settings
from broodmind.logging_config import correlation_id_var
from broodmind.queen.core import Queen, QueenReply
from broodmind.runtime_metrics import update_component_gauges
from broodmind.state import update_last_message
from broodmind.telegram.approvals import ApprovalManager
from broodmind.utils import is_heartbeat_ok

logger = structlog.get_logger(__name__)


class QueuedMessage(NamedTuple):
    text: str
    reply_to_message_id: int | None = None


_CHAT_LOCKS: dict[int, asyncio.Lock] = {}
_CHAT_QUEUES: dict[int, asyncio.Queue[QueuedMessage]] = {}
_CHAT_SEND_TASKS: dict[int, asyncio.Task] = {}
_TYPING_TASKS: dict[int, asyncio.Task] = {}
_TYPING_STOP_EVENTS: dict[int, asyncio.Event] = {}
_TYPING_REFS: dict[int, int] = {}
_TYPING_LOCK: asyncio.Lock | None = None
_SEND_IDLE_TIMEOUT_SECONDS = 300.0
_TELEGRAM_PARSE_MODE: str | None = None


def _publish_runtime_metrics() -> None:
    update_component_gauges(
        "telegram",
        {
            "chat_locks": len(_CHAT_LOCKS),
            "chat_queues": len(_CHAT_QUEUES),
            "send_tasks": len(_CHAT_SEND_TASKS),
            "typing_tasks": len(_TYPING_TASKS),
        },
    )


def register_handlers(
    dp: Dispatcher, queen: Queen, approvals: ApprovalManager, settings: Settings, bot: Bot
) -> None:
    global _TELEGRAM_PARSE_MODE, _TYPING_LOCK
    _TELEGRAM_PARSE_MODE = _normalize_parse_mode(settings.telegram_parse_mode)
    if _TYPING_LOCK is None:
        _TYPING_LOCK = asyncio.Lock()

    async def _internal_send(chat_id: int, text: str) -> None:
        await _enqueue_send(bot, chat_id, text)

    async def _internal_progress_send(
        chat_id: int,
        state: str,
        text: str,
        meta: dict[str, object],
    ) -> None:
        # Progress events are for internal tracking/logging only.
        # User-facing updates are handled by the Queen after worker completion or via her immediate replies.
        logger.info("Worker progress event", chat_id=chat_id, state=state, text=text)

    async def _internal_typing_control(chat_id: int, active: bool) -> None:
        async with _TYPING_LOCK:
            if active:
                count = _TYPING_REFS.get(chat_id, 0) + 1
                _TYPING_REFS[chat_id] = count
                if count == 1:
                    stop_event = asyncio.Event()
                    _TYPING_STOP_EVENTS[chat_id] = stop_event
                    _TYPING_TASKS[chat_id] = asyncio.create_task(_typing_loop_by_id(bot, chat_id, stop_event))
            else:
                count = _TYPING_REFS.get(chat_id, 0) - 1
                if count <= 0:
                    _TYPING_REFS.pop(chat_id, None)
                    stop_event = _TYPING_STOP_EVENTS.pop(chat_id, None)
                    if stop_event:
                        stop_event.set()
                    task = _TYPING_TASKS.pop(chat_id, None)
                    if task and not task.done():
                        task.cancel()
                else:
                    _TYPING_REFS[chat_id] = count
        _publish_runtime_metrics()

    queen.internal_send = _internal_send
    queen.internal_progress_send = _internal_progress_send
    queen.internal_typing_control = _internal_typing_control

    @dp.message()
    async def handle_message(message: Message) -> None:
        # Generate a unique ID for this request chain
        correlation_id = f"msg-{uuid.uuid4()}"
        correlation_id_var.set(correlation_id)

        if not message.text:
            return
        logger.debug("Incoming message from chat_id=%s", message.chat.id)
        lock = _CHAT_LOCKS.setdefault(message.chat.id, asyncio.Lock())
        _publish_runtime_metrics()
        async with lock:
            try:
                reply = await queen.handle_message(message.text, message.chat.id)
            except Exception:
                logger.exception("Failed to handle message")
                # Avoid leaking technical error details to the user.
                # The Queen's internal failure logs will capture the detail.
                return

            if isinstance(reply, QueenReply):
                update_last_message(settings)
                if reply.immediate and not is_heartbeat_ok(reply.immediate):
                    # Reply with quote/reply to the current message
                    await _enqueue_send(message.bot, message.chat.id, reply.immediate, reply_to_message_id=message.message_id)
                return

        update_last_message(settings)
        if reply and not is_heartbeat_ok(str(reply)):
            await _enqueue_send(message.bot, message.chat.id, str(reply), reply_to_message_id=message.message_id)

    @dp.callback_query()
    async def handle_callback(query: CallbackQuery) -> None:
        data = query.data or ""
        if data.startswith("approve:"):
            intent_id = data.split(":", 1)[1]
            approvals.resolve(intent_id, True)
            await query.answer("Intent Approved")
            if query.message:
                await query.message.edit_reply_markup(reply_markup=None)
            return
        if data.startswith("deny:"):
            intent_id = data.split(":", 1)[1]
            approvals.resolve(intent_id, False)
            await query.answer("Intent Denied")
            if query.message:
                await query.message.edit_reply_markup(reply_markup=None)


async def _send_chunked(bot: Bot, chat_id: int, text: str, reply_to_message_id: int | None = None, limit: int = 4000) -> None:
    chunks = _chunk_text(text, limit)
    for i, chunk in enumerate(chunks):
        # Only the first chunk should be a reply to the original message
        rid = reply_to_message_id if i == 0 else None
        await _send_message_safe(bot, chat_id, chunk, reply_to_message_id=rid)


async def _send_message_safe(bot: Bot, chat_id: int, text: str, reply_to_message_id: int | None = None) -> None:
    parse_mode = _TELEGRAM_PARSE_MODE
    outbound = text
    if parse_mode == "MarkdownV2":
        outbound = _prepare_markdown_v2(text)
    
    try:
        if not parse_mode:
            await bot.send_message(chat_id, text, reply_to_message_id=reply_to_message_id)
        else:
            await bot.send_message(chat_id, outbound, parse_mode=parse_mode, reply_to_message_id=reply_to_message_id)
    except TelegramBadRequest as exc:
        # Formatting mismatch should not drop the message for the user.
        logger.warning(
            "Telegram parse failed; retrying without parse_mode (parse_mode=%s, error=%s)",
            parse_mode,
            exc,
        )
        await bot.send_message(chat_id, text, reply_to_message_id=reply_to_message_id)


async def _enqueue_send(bot: Bot, chat_id: int, text: str, reply_to_message_id: int | None = None) -> None:
    queue = _CHAT_QUEUES.get(chat_id)
    if not queue:
        queue = asyncio.Queue()
        _CHAT_QUEUES[chat_id] = queue

    # If the task is missing or has finished, create a new one.
    if chat_id not in _CHAT_SEND_TASKS or _CHAT_SEND_TASKS[chat_id].done():
        _CHAT_SEND_TASKS[chat_id] = asyncio.create_task(_sender_loop(bot, chat_id, queue))
    _publish_runtime_metrics()

    await queue.put(QueuedMessage(text=text, reply_to_message_id=reply_to_message_id))


async def _sender_loop(bot: Bot, chat_id: int, queue: asyncio.Queue[QueuedMessage]) -> None:
    while True:
        try:
            # Wait for a new message, but with a timeout.
            msg = await asyncio.wait_for(queue.get(), timeout=_SEND_IDLE_TIMEOUT_SECONDS)
        except TimeoutError:
            # Queue has been empty for the timeout duration, so this worker can exit.
            break

        try:
            await _send_chunked(bot, chat_id, msg.text, reply_to_message_id=msg.reply_to_message_id)
        except Exception:
            logger.exception("Failed to send queued message")
        finally:
            queue.task_done()

    # The task is now finished, remove it from the registry so a new one can be created later.
    _CHAT_SEND_TASKS.pop(chat_id, None)
    # Drop idle queue to avoid unbounded per-chat growth over long runtimes.
    if queue.empty():
        _CHAT_QUEUES.pop(chat_id, None)
    _publish_runtime_metrics()
    logger.debug("Sender loop for chat_id=%s finished due to inactivity.", chat_id)


async def _typing_loop_by_id(bot: Bot, chat_id: int, stop: asyncio.Event) -> None:
    try:
        while not stop.is_set():
            await bot.send_chat_action(chat_id, action="typing")
            try:
                await asyncio.wait_for(stop.wait(), timeout=4.0)
            except TimeoutError:
                continue
    except Exception:
        logger.debug("Typing indicator failed", chat_id=chat_id, exc_info=True)


def _chunk_text(text: str, limit: int) -> list[str]:
    if len(text) <= limit:
        return [text]
    parts: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            parts.append(remaining)
            break
        cut = remaining.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit
        parts.append(remaining[:cut].strip())
        remaining = remaining[cut:].lstrip()
    return [p for p in parts if p]


def _normalize_parse_mode(raw: str | None) -> str | None:
    value = (raw or "").strip()
    if not value:
        return None
    lowered = value.lower()
    if lowered == "markdownv2":
        return "MarkdownV2"
    if lowered == "html":
        return "HTML"
    if lowered in {"markdown", "markdownv1"}:
        return "Markdown"
    logger.warning("Unknown BROODMIND_TELEGRAM_PARSE_MODE value; using plain text (value=%s)", value)
    return None


def _prepare_markdown_v2(text: str) -> str:
    """Robust MarkdownV2 sanitizer using telegramify-markdown."""
    if not text:
        return ""
    return telegramify_markdown.markdownify(text)

