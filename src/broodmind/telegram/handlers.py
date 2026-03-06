from __future__ import annotations

import base64
import io
import re
import structlog
import uuid
import asyncio
import contextlib
import time
from dataclasses import dataclass
from typing import Any, NamedTuple

import telegramify_markdown
from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, Message, ReactionTypeEmoji

from broodmind.config.settings import Settings
from broodmind.logging_config import correlation_id_var
from broodmind.queen.core import Queen, QueenReply
from broodmind.runtime_metrics import update_component_gauges
from broodmind.state import update_last_message
from broodmind.telegram.access import is_allowed_chat, parse_allowed_chat_ids
from broodmind.telegram.approvals import ApprovalManager
from broodmind.utils import should_suppress_user_delivery, utc_now

logger = structlog.get_logger(__name__)


class QueuedMessage(NamedTuple):
    text: str
    reply_to_message_id: int | None = None


@dataclass
class ProgressPreviewState:
    message_id: int | None = None
    pending_text: str | None = None
    last_sent_text: str = ""
    last_sent_at: float = 0.0
    flush_task: asyncio.Task | None = None


# ... (rest of global vars remain same, just ensuring imports are correct)
_CHAT_LOCKS: dict[int, asyncio.Lock] = {}
_CHAT_QUEUES: dict[int, asyncio.Queue[QueuedMessage]] = {}
_CHAT_SEND_TASKS: dict[int, asyncio.Task] = {}
_TYPING_TASKS: dict[int, asyncio.Task] = {}
_TYPING_STOP_EVENTS: dict[int, asyncio.Event] = {}
_TYPING_REFS: dict[int, int] = {}
_TYPING_LOCK: asyncio.Lock | None = None
_PROGRESS_LOCKS: dict[int, asyncio.Lock] = {}
_PROGRESS_PREVIEWS: dict[int, ProgressPreviewState] = {}
_SEND_IDLE_TIMEOUT_SECONDS = 300.0
_TELEGRAM_PARSE_MODE: str | None = None
_PROGRESS_THROTTLE_SECONDS = 1.0
_PROGRESS_TEXT_LIMIT = 3900
_PROGRESS_PROMOTE_MAX_AGE_SECONDS = 180.0


def _publish_runtime_metrics() -> None:
    update_component_gauges(
        "telegram",
        {
            "chat_locks": len(_CHAT_LOCKS),
            "chat_queues": len(_CHAT_QUEUES),
            "send_tasks": len(_CHAT_SEND_TASKS),
            "typing_tasks": len(_TYPING_TASKS),
            "progress_previews": len(_PROGRESS_PREVIEWS),
        },
    )


_REACTION_MAPPING = {
    "✅": "👍",
    "✔️": "👍",
    "❌": "👎",
    "✖️": "👎",
    "🚀": "⚡",
    "⚠️": "🤨",
    "ℹ️": "🤔",
}


_REACT_TAG_RE = re.compile(r"<react>(.*?)</react>", re.IGNORECASE | re.DOTALL)


def _normalize_reaction(emoji: str) -> str:
    # Handle both raw emoji and potential mapping
    return _REACTION_MAPPING.get(emoji.strip(), emoji.strip())


def _extract_reaction_and_strip(text: str) -> tuple[str | None, str]:
    match = _REACT_TAG_RE.search(text or "")
    if not match:
        return None, text or ""
    emoji = (match.group(1) or "").strip() or None
    cleaned = _REACT_TAG_RE.sub("", text or "").strip()
    return emoji, cleaned


def _strip_reaction_tags(text: str) -> str:
    return _REACT_TAG_RE.sub("", text or "").strip()


def register_handlers(
    dp: Dispatcher, queen: Queen, approvals: ApprovalManager, settings: Settings, bot: Bot
) -> None:
    global _TELEGRAM_PARSE_MODE, _TYPING_LOCK
    _TELEGRAM_PARSE_MODE = _normalize_parse_mode(settings.telegram_parse_mode)
    if _TYPING_LOCK is None:
        _TYPING_LOCK = asyncio.Lock()
    allowed_chat_ids = parse_allowed_chat_ids(settings.allowed_telegram_chat_ids)

    async def _reject_unauthorized_message(message: Message) -> None:
        logger.warning("Rejected Telegram message from unauthorized chat", chat_id=message.chat.id)
        await message.answer("This chat is not authorized to use BroodMind.")

    async def _internal_send(chat_id: int, text: str) -> None:
        if should_suppress_user_delivery(text):
            logger.debug("Suppressed control response for Telegram delivery", chat_id=chat_id)
            return
        await _enqueue_send(bot, chat_id, text)

    async def _internal_progress_send(
        chat_id: int,
        state: str,
        text: str,
        meta: dict[str, object],
    ) -> None:
        # Telegram user-facing preview stream should reflect only model partial text.
        # Worker lifecycle events remain internal logs to avoid noisy status messages.
        if state != "partial":
            logger.info("Worker progress event", chat_id=chat_id, state=state, text=text)
            return

        logger.debug("LLM partial stream update", chat_id=chat_id, text_len=len(text or ""))
        preview_text = _format_progress_preview(state=state, text=text, meta=meta)
        if not preview_text:
            return
        await _schedule_progress_preview(bot, chat_id, preview_text)

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

    # Re-initialize the Queen's default (Telegram) output hooks if needed
    queen._tg_send = _internal_send
    queen._tg_progress = _internal_progress_send
    queen._tg_typing = _internal_typing_control

    import importlib.metadata

    @dp.message(Command("help"))
    async def cmd_help(message: Message) -> None:
        if not is_allowed_chat(message.chat.id, allowed_chat_ids):
            await _reject_unauthorized_message(message)
            return
        help_text = (
            "Available commands:\n"
            "/help - Show this help message\n"
            "/status - Show system status\n"
            "/workers - List available worker templates\n"
            "/memory [limit] - Show memory usage stats\n"
            "/version - Show bot version"
        )
        await message.answer(help_text)

    @dp.message(Command("version"))
    async def cmd_version(message: Message) -> None:
        if not is_allowed_chat(message.chat.id, allowed_chat_ids):
            await _reject_unauthorized_message(message)
            return
        try:
            version = importlib.metadata.version("broodmind")
        except importlib.metadata.PackageNotFoundError:
            version = "0.2.0-dev"
        await message.answer(f"BroodMind v{version}")

    @dp.message(Command("status"))
    async def cmd_status(message: Message) -> None:
        if not is_allowed_chat(message.chat.id, allowed_chat_ids):
            await _reject_unauthorized_message(message)
            return
        active_workers = await asyncio.to_thread(queen.store.get_active_workers)
        status_text = (
            f"**System Status**\n"
            f"Thinking: {'Yes' if queen._thinking_count > 0 else 'No'}\n"
            f"Active Workers: {len(active_workers)}\n"
            f"Current Time: {utc_now().isoformat()}\n"
        )
        if active_workers:
            status_text += "\n**Running Workers:**\n"
            for w in active_workers:
                status_text += f"- RunID: {w.id}\n  Task: {w.task[:50]}...\n"
        await message.answer(status_text, parse_mode="Markdown")

    @dp.message(Command("workers"))
    async def cmd_workers(message: Message) -> None:
        if not is_allowed_chat(message.chat.id, allowed_chat_ids):
            await _reject_unauthorized_message(message)
            return
        templates = await asyncio.to_thread(queen.store.list_worker_templates)
        if not templates:
            await message.answer("No worker templates found.")
            return
        
        text = "**Available Workers:**\n\n"
        for t in templates:
            text += f"**{t.worker_id}**\n{t.description or 'No description'}\n\n"
        await message.answer(text, parse_mode="Markdown")

    @dp.message(Command("memory"))
    async def cmd_memory(message: Message, command: CommandObject) -> None:
        if not is_allowed_chat(message.chat.id, allowed_chat_ids):
            await _reject_unauthorized_message(message)
            return
        limit = 300
        if command.args and command.args.isdigit():
            limit = int(command.args)
            limit = max(50, min(limit, 1000))

        entries = await asyncio.to_thread(queen.store.list_memory_entries, limit=limit)
        
        unique_chats = set()
        role_counts = {}
        for e in entries:
            role_counts[e.role] = role_counts.get(e.role, 0) + 1
            if e.metadata and "chat_id" in e.metadata:
                unique_chats.add(e.metadata["chat_id"])

        text = (
            f"**Memory Snapshot** (Sample: {len(entries)})\n"
            f"Unique Chats: {len(unique_chats)}\n"
            f"Role Distribution:\n"
        )
        for role, count in role_counts.items():
            text += f"- {role}: {count}\n"
        
        await message.answer(text, parse_mode="Markdown")

    @dp.message()
    async def handle_message(message: Message) -> None:
        if not is_allowed_chat(message.chat.id, allowed_chat_ids):
            await _reject_unauthorized_message(message)
            return
        # Generate a unique ID for this request chain
        correlation_id = f"msg-{uuid.uuid4()}"
        correlation_id_var.set(correlation_id)

        # 1. Extract text and images
        text = message.text or message.caption or ""
        images: list[str] = []

        if message.photo:
            try:
                # Use the largest available photo size
                photo = message.photo[-1]
                logger.debug("Downloading photo", file_id=photo.file_id, width=photo.width, height=photo.height)
                
                with io.BytesIO() as buffer:
                    await bot.download(photo, destination=buffer)
                    buffer.seek(0)
                    b64_data = base64.b64encode(buffer.read()).decode("utf-8")
                    # Assume JPEG for Telegram photos
                    images.append(f"data:image/jpeg;base64,{b64_data}")
            except Exception:
                logger.exception("Failed to process image from Telegram")
                # Continue processing even if image fails, just treat as text-only (or empty)

        if not text and not images:
            return

        logger.debug("Incoming message", chat_id=message.chat.id, has_images=bool(images))
        lock = _CHAT_LOCKS.setdefault(message.chat.id, asyncio.Lock())
        _publish_runtime_metrics()

        async with lock:
            # 2. Silent Mode Check
            # If text starts with "! " or "> ", treat as a silent memory entry.
            if text.startswith("! ") or text.startswith("> "):
                clean_text = text[2:].strip()
                if clean_text:
                    await queen.memory.add_message(
                        "user", 
                        f"[SILENT LOG] {clean_text}", 
                        {"chat_id": message.chat.id, "silent": True, "has_images": bool(images)}
                    )
                    # We don't store images deep in memory yet, but we acknowledge receipt.
                    try:
                        await message.react([ReactionTypeEmoji(emoji="✍\ufe0f")])
                    except Exception as exc:
                        logger.debug("Failed to react to silent message", error=str(exc))
                return

            # 3. Normal Queen Processing
            try:
                reply = await queen.handle_message(text, message.chat.id, images=images)
            except Exception:
                logger.exception("Failed to handle message")
                return

            if isinstance(reply, QueenReply):
                update_last_message(settings)
                
                final_text = reply.immediate or ""
                
                # 4. Reaction Parsing
                emoji, final_text = _extract_reaction_and_strip(final_text)
                if emoji:
                    mapped_emoji = _normalize_reaction(emoji)
                    try:
                        await message.react([ReactionTypeEmoji(emoji=mapped_emoji)])
                    except Exception as exc:
                        logger.warning("Failed to apply reaction", emoji=emoji, mapped=mapped_emoji, error=str(exc))

                if not should_suppress_user_delivery(final_text):
                    # Reply with quote/reply to the current message
                    await _enqueue_send(message.bot, message.chat.id, final_text, reply_to_message_id=message.message_id)
                return

        update_last_message(settings)
        # Fallback for non-QueenReply results (shouldn't happen with current core types, but for safety)
        if not should_suppress_user_delivery(str(reply)):
            await _enqueue_send(message.bot, message.chat.id, str(reply), reply_to_message_id=message.message_id)

    @dp.callback_query()
    async def handle_callback(query: CallbackQuery) -> None:
        query_chat_id = query.message.chat.id if query.message and query.message.chat else None
        if query_chat_id is None or not is_allowed_chat(query_chat_id, allowed_chat_ids):
            logger.warning("Rejected Telegram callback from unauthorized chat", chat_id=query_chat_id)
            await query.answer("Unauthorized", show_alert=True)
            return
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
    text = _strip_reaction_tags(text)
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
    if should_suppress_user_delivery(text):
        logger.debug("Suppressed control response before queueing", chat_id=chat_id)
        return

    promoted = await _promote_progress_preview_to_final(
        bot,
        chat_id,
        text,
    )
    if promoted:
        return

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


def _get_progress_lock(chat_id: int) -> asyncio.Lock:
    lock = _PROGRESS_LOCKS.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        _PROGRESS_LOCKS[chat_id] = lock
    return lock


def _format_progress_preview(state: str, text: str, meta: dict[str, object]) -> str:
    state_label = (state or "working").strip().lower()
    body = (text or "").strip()
    if state_label == "partial":
        return body[:_PROGRESS_TEXT_LIMIT].rstrip()
    label_map = {
        "queued": "queued",
        "worker_started": "started",
        "running": "running",
        "completed": "completed",
        "failed": "failed",
        "duplicate": "duplicate",
        "child_stopped": "child stopped",
    }
    label = label_map.get(state_label, state_label or "working")
    if not body:
        worker_id = str(meta.get("worker_id") or "").strip()
        body = f"State update ({worker_id})" if worker_id else "State update"
    rendered = f"[{label}] {body}"
    return rendered[:_PROGRESS_TEXT_LIMIT].rstrip()


async def _schedule_progress_preview(bot: Bot, chat_id: int, preview_text: str) -> None:
    lock = _get_progress_lock(chat_id)
    async with lock:
        state = _PROGRESS_PREVIEWS.get(chat_id)
        if state is None:
            state = ProgressPreviewState()
            _PROGRESS_PREVIEWS[chat_id] = state
        state.pending_text = preview_text
        task = state.flush_task
        if task is None or task.done():
            state.flush_task = asyncio.create_task(_progress_flush_loop(bot, chat_id))
    _publish_runtime_metrics()


async def _progress_flush_loop(bot: Bot, chat_id: int) -> None:
    try:
        while True:
            lock = _get_progress_lock(chat_id)
            async with lock:
                state = _PROGRESS_PREVIEWS.get(chat_id)
                if state is None:
                    return
                pending = (state.pending_text or "").strip()
                if not pending:
                    state.flush_task = None
                    return
                wait_s = max(0.0, _PROGRESS_THROTTLE_SECONDS - (time.monotonic() - state.last_sent_at))
            if wait_s > 0:
                await asyncio.sleep(wait_s)
            async with lock:
                state = _PROGRESS_PREVIEWS.get(chat_id)
                if state is None:
                    return
                pending = (state.pending_text or "").strip()
                if not pending:
                    state.flush_task = None
                    return
                state.pending_text = None
                if pending == state.last_sent_text:
                    continue
                try:
                    message_id = state.message_id
                    if message_id is None:
                        sent = await bot.send_message(chat_id, pending)
                        state.message_id = getattr(sent, "message_id", None)
                    else:
                        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=pending)
                    state.last_sent_text = pending
                    state.last_sent_at = time.monotonic()
                except Exception:
                    logger.debug("Progress preview update failed", chat_id=chat_id, exc_info=True)
    finally:
        lock = _PROGRESS_LOCKS.get(chat_id)
        if lock is None:
            return
        async with lock:
            state = _PROGRESS_PREVIEWS.get(chat_id)
            if state is not None:
                state.flush_task = None


async def _promote_progress_preview_to_final(bot: Bot, chat_id: int, final_text: str) -> bool:
    lock = _get_progress_lock(chat_id)
    async with lock:
        state = _PROGRESS_PREVIEWS.get(chat_id)
        if state is None:
            return False
        task = state.flush_task
        state.flush_task = None
        state.pending_text = None
        message_id = state.message_id

    if task and not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    if message_id is None:
        async with lock:
            _PROGRESS_PREVIEWS.pop(chat_id, None)
            _PROGRESS_LOCKS.pop(chat_id, None)
        _publish_runtime_metrics()
        return False

    # Never promote very old previews to final text:
    # editing a stale message can surface a new answer at an old timestamp.
    preview_age_s = float("inf")
    if state.last_sent_at > 0:
        preview_age_s = max(0.0, time.monotonic() - state.last_sent_at)
    if preview_age_s > _PROGRESS_PROMOTE_MAX_AGE_SECONDS:
        logger.info(
            "Skipping stale progress promote",
            chat_id=chat_id,
            message_id=message_id,
            preview_age_s=round(preview_age_s, 1),
            max_age_s=_PROGRESS_PROMOTE_MAX_AGE_SECONDS,
        )
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
        async with lock:
            _PROGRESS_PREVIEWS.pop(chat_id, None)
            _PROGRESS_LOCKS.pop(chat_id, None)
        _publish_runtime_metrics()
        return False

    clean_text = _strip_reaction_tags(final_text or "").strip()
    if not clean_text or len(clean_text) > 4000:
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
        async with lock:
            _PROGRESS_PREVIEWS.pop(chat_id, None)
            _PROGRESS_LOCKS.pop(chat_id, None)
        _publish_runtime_metrics()
        return False

    parse_mode = _TELEGRAM_PARSE_MODE
    outbound = clean_text
    if parse_mode == "MarkdownV2":
        outbound = _prepare_markdown_v2(clean_text)

    promoted = False
    try:
        if parse_mode:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=outbound,
                parse_mode=parse_mode,
            )
        else:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=clean_text)
        promoted = True
    except TelegramBadRequest as exc:
        logger.warning(
            "Progress final promote parse failed; retrying plain text (parse_mode=%s, error=%s)",
            parse_mode,
            exc,
        )
        with contextlib.suppress(Exception):
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=clean_text)
            promoted = True
    except Exception:
        logger.debug("Progress final promote failed", chat_id=chat_id, exc_info=True)

    if not promoted:
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id=chat_id, message_id=message_id)

    async with lock:
        _PROGRESS_PREVIEWS.pop(chat_id, None)
        _PROGRESS_LOCKS.pop(chat_id, None)
    _publish_runtime_metrics()
    return promoted
