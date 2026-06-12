from __future__ import annotations

import asyncio
import base64
import io
import mimetypes
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any, NamedTuple

import structlog
import telegramify_markdown
from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, FSInputFile, Message, ReactionTypeEmoji

from octopal.channels.group_addressing import decide_group_addressing, load_recent_group_context
from octopal.channels.group_observation import record_passive_group_observation
from octopal.channels.telegram.access import is_allowed_chat, parse_allowed_chat_ids
from octopal.channels.telegram.approvals import ApprovalManager
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.logging import correlation_id_var
from octopal.infrastructure.observability.base import TraceSink
from octopal.infrastructure.observability.helpers import safe_preview, summarize_exception
from octopal.runtime.metrics import read_metrics_snapshot, update_component_gauges
from octopal.runtime.octo.core import Octo, OctoReply
from octopal.runtime.octo.delivery import resolve_user_delivery
from octopal.runtime.octo_status import build_octo_status
from octopal.runtime.pending_turns import PendingTurnAggregator
from octopal.runtime.state import update_last_message
from octopal.utils import (
    escape_html,
    extract_edge_reaction_fallback,
    extract_reaction_and_strip,
    normalize_reaction_emoji,
    sanitize_user_facing_text,
    strip_reaction_tags,
    utc_now,
)

logger = structlog.get_logger(__name__)


class QueuedMessage(NamedTuple):
    text: str
    reply_to_message_id: int | None = None
    delivery_source: str = "telegram"
    correlation_id: str | None = None
    trace_sink: TraceSink | None = None


_CHAT_LOCKS: dict[int, asyncio.Lock] = {}
_CHAT_QUEUES: dict[int, asyncio.Queue[QueuedMessage]] = {}
_CHAT_SEND_TASKS: dict[int, asyncio.Task] = {}
_TYPING_TASKS: dict[int, asyncio.Task] = {}
_TYPING_STOP_EVENTS: dict[int, asyncio.Event] = {}
_TYPING_REFS: dict[int, int] = {}
_TYPING_LOCK: asyncio.Lock | None = None
_SEND_IDLE_TIMEOUT_SECONDS = 300.0
_INBOUND_MESSAGE_DEDUP_TTL_SECONDS = 300.0
_INBOUND_PAYLOAD_DEDUP_TTL_SECONDS = 120.0
_TELEGRAM_PARSE_MODE: str | None = None
_TELEGRAM_TRACE_SINK: TraceSink | None = None
_PENDING_TURNS: PendingTurnAggregator | None = None
_RECENT_INBOUND_MESSAGE_IDS: dict[tuple[int, int], float] = {}
_RECENT_INBOUND_PAYLOADS: dict[tuple[int, int | str, str], float] = {}
_TELEGRAM_IMAGE_DOCUMENT_MIME_PREFIXES = ("image/",)
_TELEGRAM_IMAGE_DOCUMENT_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}


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


def _prune_recent_inbound_messages(now: float | None = None) -> None:
    if not _RECENT_INBOUND_MESSAGE_IDS:
        return
    current = time.monotonic() if now is None else now
    cutoff = current - _INBOUND_MESSAGE_DEDUP_TTL_SECONDS
    expired = [key for key, seen_at in _RECENT_INBOUND_MESSAGE_IDS.items() if seen_at < cutoff]
    for key in expired:
        _RECENT_INBOUND_MESSAGE_IDS.pop(key, None)


def _is_duplicate_inbound_message(chat_id: int, message_id: int) -> bool:
    if message_id <= 0:
        return False
    now = time.monotonic()
    _prune_recent_inbound_messages(now)
    message_key = (chat_id, message_id)
    if message_key in _RECENT_INBOUND_MESSAGE_IDS:
        return True
    _RECENT_INBOUND_MESSAGE_IDS[message_key] = now
    return False


def _prune_recent_inbound_payloads(now: float | None = None) -> None:
    if not _RECENT_INBOUND_PAYLOADS:
        return
    current = time.monotonic() if now is None else now
    cutoff = current - _INBOUND_PAYLOAD_DEDUP_TTL_SECONDS
    expired = [key for key, seen_at in _RECENT_INBOUND_PAYLOADS.items() if seen_at < cutoff]
    for key in expired:
        _RECENT_INBOUND_PAYLOADS.pop(key, None)


def _build_inbound_message_fingerprint(
    text: str,
    photo_ids: list[str] | None = None,
    attachment_ids: list[str] | None = None,
) -> str:
    normalized_text = re.sub(r"\s+", " ", (text or "").strip()).casefold()
    normalized_photos = [
        photo_id.strip() for photo_id in (photo_ids or []) if photo_id and photo_id.strip()
    ]
    normalized_attachments = [
        attachment_id.strip()
        for attachment_id in (attachment_ids or [])
        if attachment_id and attachment_id.strip()
    ]
    return (
        f"text={normalized_text}|photos={'|'.join(normalized_photos)}|"
        f"attachments={'|'.join(normalized_attachments)}"
    )


def _telegram_attachment_suffix(file_name: str | None, mime_type: str | None, fallback: str) -> str:
    suffix = Path(file_name or "").suffix.lower()
    if suffix:
        return suffix
    guessed = mimetypes.guess_extension(str(mime_type or "").split(";", 1)[0].strip().lower())
    if guessed:
        return guessed
    return fallback


def _is_telegram_image_attachment(file_name: str | None, mime_type: str | None) -> bool:
    normalized_mime = str(mime_type or "").strip().lower()
    if any(normalized_mime.startswith(prefix) for prefix in _TELEGRAM_IMAGE_DOCUMENT_MIME_PREFIXES):
        return True
    return Path(file_name or "").suffix.lower() in _TELEGRAM_IMAGE_DOCUMENT_SUFFIXES


def _persist_telegram_media_payload(
    *,
    workspace_dir: Path,
    binary: bytes,
    source: str,
    file_name: str | None = None,
    mime_type: str | None = None,
    default_suffix: str = ".bin",
) -> str:
    media_dir = workspace_dir / "tmp" / source
    media_dir.mkdir(parents=True, exist_ok=True)
    suffix = _telegram_attachment_suffix(file_name, mime_type, default_suffix)
    safe_stem = Path(file_name or "").stem.strip() or source.rstrip("s")
    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", safe_stem).strip("._") or source.rstrip("s")
    file_path = (media_dir / f"{safe_stem}_{uuid.uuid4()}{suffix}").resolve()
    file_path.write_bytes(binary)
    return str(file_path)


def _build_telegram_media_artifacts(
    *,
    binary: bytes,
    file_name: str | None,
    mime_type: str | None,
    workspace_dir: Path,
    source: str,
    default_suffix: str,
) -> tuple[list[str], list[str]]:
    saved_path = _persist_telegram_media_payload(
        workspace_dir=workspace_dir,
        binary=binary,
        source=source,
        file_name=file_name,
        mime_type=mime_type,
        default_suffix=default_suffix,
    )
    if _is_telegram_image_attachment(file_name, mime_type):
        normalized_mime = str(mime_type or "").strip().lower() or "image/jpeg"
        return [f"data:{normalized_mime};base64,{base64.b64encode(binary).decode('utf-8')}"], [
            saved_path
        ]
    return [], [saved_path]


def _is_duplicate_inbound_payload(chat_id: int, sender_id: int | None, fingerprint: str) -> bool:
    normalized = (fingerprint or "").strip()
    if not normalized or normalized == "text=|photos=|attachments=":
        return False
    now = time.monotonic()
    _prune_recent_inbound_payloads(now)
    sender_key: int | str = sender_id if sender_id is not None else "unknown"
    payload_key = (chat_id, sender_key, normalized)
    if payload_key in _RECENT_INBOUND_PAYLOADS:
        return True
    _RECENT_INBOUND_PAYLOADS[payload_key] = now
    return False


def register_handlers(
    dp: Dispatcher, octo: Octo, approvals: ApprovalManager, settings: Settings, bot: Bot
) -> None:
    global _TELEGRAM_PARSE_MODE, _TELEGRAM_TRACE_SINK, _TYPING_LOCK, _PENDING_TURNS
    _TELEGRAM_PARSE_MODE = _normalize_parse_mode(settings.telegram_parse_mode)
    _TELEGRAM_TRACE_SINK = octo.trace_sink
    if _TYPING_LOCK is None:
        _TYPING_LOCK = asyncio.Lock()
    _PENDING_TURNS = PendingTurnAggregator(
        grace_seconds=getattr(settings, "user_message_grace_seconds", 5.0),
        flush_callback=_flush_pending_turn_factory(octo, settings, bot),
    )
    allowed_chat_ids = parse_allowed_chat_ids(settings.allowed_telegram_chat_ids)

    async def _reject_unauthorized_message(message: Message) -> None:
        logger.warning("Rejected Telegram message from unauthorized chat", chat_id=message.chat.id)
        await message.answer("This chat is not authorized to use Octopal.")

    async def _internal_send(chat_id: int, text: str) -> None:
        decision = resolve_user_delivery(text)
        if not decision.user_visible:
            logger.debug("Suppressed control response for Telegram delivery", chat_id=chat_id)
            return
        await _enqueue_send(bot, chat_id, decision.text)

    async def _internal_send_file(chat_id: int, file_path: str, caption: str | None = None) -> None:
        await _send_file_safe(bot, chat_id, file_path, caption=caption)

    async def _internal_progress_send(
        chat_id: int,
        state: str,
        text: str,
        meta: dict[str, object],
    ) -> None:
        logger.info("Worker progress event", chat_id=chat_id, state=state, text=text)

    async def _internal_worker_event_send(
        chat_id: int, event: str, payload: dict[str, Any]
    ) -> None:
        logger.info("Worker event", chat_id=chat_id, event=event, payload=payload)

    async def _internal_typing_control(chat_id: int, active: bool) -> None:
        async with _TYPING_LOCK:
            if active:
                count = _TYPING_REFS.get(chat_id, 0) + 1
                _TYPING_REFS[chat_id] = count
                if count == 1:
                    stop_event = asyncio.Event()
                    _TYPING_STOP_EVENTS[chat_id] = stop_event
                    _TYPING_TASKS[chat_id] = asyncio.create_task(
                        _typing_loop_by_id(bot, chat_id, stop_event)
                    )
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

    octo.internal_send = _internal_send
    octo.internal_send_file = _internal_send_file
    octo.internal_progress_send = _internal_progress_send
    octo.internal_worker_event_send = _internal_worker_event_send
    octo.internal_typing_control = _internal_typing_control

    # Re-initialize the Octo's default (Telegram) output hooks if needed
    octo._tg_send = _internal_send
    octo._tg_send_file = _internal_send_file
    octo._tg_progress = _internal_progress_send
    octo._tg_worker_event = _internal_worker_event_send
    octo._tg_typing = _internal_typing_control

    import importlib.metadata

    @dp.message(Command("help"))
    async def cmd_help(message: Message, command: CommandObject) -> None:
        if not is_allowed_chat(message.chat.id, allowed_chat_ids):
            await _reject_unauthorized_message(message)
            return
        if not await _telegram_group_command_should_run(
            message, command=command, settings=settings, octo=octo, bot=bot
        ):
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
    async def cmd_version(message: Message, command: CommandObject) -> None:
        if not is_allowed_chat(message.chat.id, allowed_chat_ids):
            await _reject_unauthorized_message(message)
            return
        if not await _telegram_group_command_should_run(
            message, command=command, settings=settings, octo=octo, bot=bot
        ):
            return
        try:
            version = importlib.metadata.version("octopal")
        except importlib.metadata.PackageNotFoundError:
            version = "0.2.0-dev"
        await message.answer(f"Octopal v{version}")

    @dp.message(Command("status"))
    async def cmd_status(message: Message, command: CommandObject) -> None:
        if not is_allowed_chat(message.chat.id, allowed_chat_ids):
            await _reject_unauthorized_message(message)
            return
        if not await _telegram_group_command_should_run(
            message, command=command, settings=settings, octo=octo, bot=bot
        ):
            return
        active_workers = await asyncio.to_thread(octo.store.get_active_workers)
        metrics = read_metrics_snapshot(settings.state_dir)
        octo_status = build_octo_status((metrics or {}).get("octo", {}))
        status_text = (
            f"**System Status**\n"
            f"Octo: {octo_status['label']}\n"
            f"Reason: {octo_status['reason']}\n"
            f"Active Workers: {len(active_workers)}\n"
            f"Current Time: {utc_now().isoformat()}\n"
        )
        if octo_status["busy"]:
            status_text += (
                f"Follow-up Queues: {octo_status['followup_queues']}\n"
                f"Internal Queues: {octo_status['internal_queues']}\n"
            )
        if active_workers:
            status_text += "\n**Running Workers:**\n"
            for w in active_workers:
                status_text += f"- RunID: {w.id}\n  Task: {w.task[:50]}...\n"
        await message.answer(status_text, parse_mode="Markdown")

    @dp.message(Command("workers"))
    async def cmd_workers(message: Message, command: CommandObject) -> None:
        if not is_allowed_chat(message.chat.id, allowed_chat_ids):
            await _reject_unauthorized_message(message)
            return
        if not await _telegram_group_command_should_run(
            message, command=command, settings=settings, octo=octo, bot=bot
        ):
            return
        templates = await asyncio.to_thread(octo.store.list_worker_templates)
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
        if not await _telegram_group_command_should_run(
            message, command=command, settings=settings, octo=octo, bot=bot
        ):
            return
        limit = 300
        if command.args and command.args.isdigit():
            limit = int(command.args)
            limit = max(50, min(limit, 1000))

        entries = await asyncio.to_thread(octo.store.list_memory_entries, limit=limit)

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
        if _is_duplicate_inbound_message(message.chat.id, message.message_id):
            logger.info(
                "Skipping duplicate Telegram inbound message",
                chat_id=message.chat.id,
                message_id=message.message_id,
            )
            return
        if _telegram_bot_authored_message_should_skip(message, bot):
            logger.info(
                "Skipping bot-authored Telegram inbound message",
                chat_id=message.chat.id,
                message_id=message.message_id,
            )
            return
        text = message.text or message.caption or ""
        photo_ids = [
            str(getattr(photo, "file_unique_id", "") or "").strip()
            for photo in (message.photo or [])
        ]
        attachment_ids = [
            str(getattr(getattr(message, field, None), "file_unique_id", "") or "").strip()
            for field in ("document", "video", "audio", "voice", "animation")
            if getattr(getattr(message, field, None), "file_unique_id", None)
        ]
        inbound_fingerprint = _build_inbound_message_fingerprint(text, photo_ids, attachment_ids)
        if _is_duplicate_inbound_payload(
            message.chat.id,
            getattr(message.from_user, "id", None),
            inbound_fingerprint,
        ):
            logger.info(
                "Skipping duplicate Telegram inbound payload",
                chat_id=message.chat.id,
                message_id=message.message_id,
            )
            return
        # Generate a unique ID for this request chain
        correlation_id = f"msg-{uuid.uuid4()}"
        correlation_id_var.set(correlation_id)

        # 1. Extract text and images
        images: list[str] = []
        saved_file_paths: list[str] = []

        if message.photo:
            try:
                # Use the largest available photo size
                photo = message.photo[-1]
                logger.debug(
                    "Downloading photo",
                    file_id=photo.file_id,
                    width=photo.width,
                    height=photo.height,
                )

                with io.BytesIO() as buffer:
                    await bot.download(photo, destination=buffer)
                    payload = buffer.getvalue()

                workspace_dir = Path(os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")).resolve()
                img_dir = workspace_dir / "tmp" / "telegram_images"
                img_dir.mkdir(parents=True, exist_ok=True)
                file_path = (img_dir / f"img_{uuid.uuid4()}.jpg").resolve()
                file_path.write_bytes(payload)
                saved_file_paths.append(str(file_path))

                b64_data = base64.b64encode(payload).decode("utf-8")
                # Assume JPEG for Telegram photos
                images.append(f"data:image/jpeg;base64,{b64_data}")
            except Exception:
                logger.exception("Failed to process image from Telegram")
                # Continue processing even if image fails, just treat as text-only (or empty)

        attachment_specs = [
            ("document", ".bin", "telegram_files"),
            ("video", ".mp4", "telegram_files"),
            ("audio", ".mp3", "telegram_files"),
            ("voice", ".ogg", "telegram_files"),
            ("animation", ".gif", "telegram_files"),
        ]
        workspace_dir = Path(os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")).resolve()
        for attr_name, default_suffix, source in attachment_specs:
            attachment = getattr(message, attr_name, None)
            if not attachment:
                continue
            try:
                with io.BytesIO() as buffer:
                    await bot.download(attachment, destination=buffer)
                    payload = buffer.getvalue()
                media_images, media_paths = _build_telegram_media_artifacts(
                    binary=payload,
                    file_name=getattr(attachment, "file_name", None),
                    mime_type=getattr(attachment, "mime_type", None),
                    workspace_dir=workspace_dir,
                    source=source,
                    default_suffix=default_suffix,
                )
                images.extend(media_images)
                saved_file_paths.extend(media_paths)
            except Exception:
                logger.exception("Failed to process Telegram attachment", attachment_type=attr_name)

        if not text and not images and not saved_file_paths:
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
                    await octo.memory.add_message(
                        "user",
                        f"[SILENT LOG] {clean_text}",
                        {"chat_id": message.chat.id, "silent": True, "has_images": bool(images)},
                    )
                    # We don't store images deep in memory yet, but we acknowledge receipt.
                    try:
                        await message.react([ReactionTypeEmoji(emoji="✍\ufe0f")])
                    except Exception as exc:
                        logger.debug("Failed to react to silent message", error=str(exc))
                return

            # 3. Queue into pending turn buffer and wait for the grace window to settle.
            assert _PENDING_TURNS is not None
            await _PENDING_TURNS.submit(
                chat_id=message.chat.id,
                text=text,
                images=images,
                saved_file_paths=saved_file_paths,
                metadata={
                    "reply_to_message_id": message.message_id,
                    "is_group_chat": _is_telegram_group_chat(message),
                    "reply_to_agent": _telegram_reply_targets_this_bot(message, bot),
                    "sender_label": _telegram_sender_label(message),
                },
            )
            return

    @dp.callback_query()
    async def handle_callback(query: CallbackQuery) -> None:
        query_chat_id = query.message.chat.id if query.message and query.message.chat else None
        if query_chat_id is None or not is_allowed_chat(query_chat_id, allowed_chat_ids):
            logger.warning(
                "Rejected Telegram callback from unauthorized chat", chat_id=query_chat_id
            )
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


async def _send_chunked(
    bot: Bot,
    chat_id: int,
    text: str,
    reply_to_message_id: int | None = None,
    limit: int = 4000,
    *,
    delivery_source: str = "telegram",
    correlation_id: str | None = None,
    trace_sink: TraceSink | None = None,
) -> None:
    chunks = _chunk_text(text, limit)
    chunk_count = len(chunks)
    for i, chunk in enumerate(chunks):
        # Only the first chunk should be a reply to the original message
        rid = reply_to_message_id if i == 0 else None
        await _send_message_safe(
            bot,
            chat_id,
            chunk,
            reply_to_message_id=rid,
            delivery_source=delivery_source,
            correlation_id=correlation_id,
            trace_sink=trace_sink,
            chunk_index=i,
            chunk_count=chunk_count,
        )


async def _send_message_safe(
    bot: Bot,
    chat_id: int,
    text: str,
    reply_to_message_id: int | None = None,
    *,
    delivery_source: str = "telegram",
    correlation_id: str | None = None,
    trace_sink: TraceSink | None = None,
    chunk_index: int = 0,
    chunk_count: int = 1,
) -> None:
    trace_started_at_ms = time.perf_counter() * 1000.0
    trace_ctx = None
    trace_status = "ok"
    trace_output: dict[str, Any] | None = None
    trace_metadata: dict[str, Any] = {
        "delivery_channel": "telegram",
        "delivery_source": delivery_source,
        "correlation_id": correlation_id,
        "chunk_index": chunk_index,
        "chunk_count": chunk_count,
        "reply_to_message_id": reply_to_message_id,
        "text_preview": safe_preview(text, limit=240),
        "text_len": len(text or ""),
    }
    if trace_sink is not None:
        trace_id = f"channel-delivery-{uuid.uuid4().hex}"
        root_trace_id = str(correlation_id or trace_id)
        trace_ctx = await trace_sink.start_trace(
            name="channel.delivery",
            trace_id=trace_id,
            root_trace_id=root_trace_id,
            session_id=f"chat:{chat_id}",
            chat_id=chat_id,
            metadata=trace_metadata,
        )

    try:
        sanitized = sanitize_user_facing_text(strip_reaction_tags(text))
        trace_metadata["sanitized_len"] = len(sanitized or "")
        if not sanitized:
            trace_output = {"status": "suppressed", "reason": "empty_after_sanitize"}
            logger.debug("Suppressed empty message after Telegram sanitization", chat_id=chat_id)
            return

        preferred_parse_mode = _TELEGRAM_PARSE_MODE or "MarkdownV2"
        outbound = sanitized

        if preferred_parse_mode == "MarkdownV2":
            outbound = _prepare_markdown_v2(sanitized)
        elif preferred_parse_mode == "HTML":
            outbound = escape_html(sanitized)

        try:
            await bot.send_message(
                chat_id,
                outbound,
                parse_mode=preferred_parse_mode,
                reply_to_message_id=reply_to_message_id,
            )
            trace_output = {
                "status": "sent",
                "parse_mode": preferred_parse_mode,
                "fallback_used": False,
            }
        except TelegramBadRequest as exc:
            trace_metadata["fallback_parse_mode"] = "HTML"
            trace_metadata["initial_parse_mode"] = preferred_parse_mode
            logger.warning(
                "Telegram parse failed; retrying with HTML fallback (parse_mode=%s, error=%s)",
                preferred_parse_mode,
                exc,
            )
            try:
                await bot.send_message(
                    chat_id,
                    escape_html(sanitized),
                    parse_mode="HTML",
                    reply_to_message_id=reply_to_message_id,
                )
                trace_output = {
                    "status": "sent",
                    "parse_mode": "HTML",
                    "fallback_used": True,
                }
            except TelegramBadRequest as html_exc:
                trace_metadata["fallback_parse_mode_2"] = "plain_text"
                logger.warning(
                    "Telegram HTML parse failed; retrying without parse_mode (error=%s)",
                    html_exc,
                )
                await bot.send_message(chat_id, sanitized, reply_to_message_id=reply_to_message_id)
                trace_output = {
                    "status": "sent",
                    "parse_mode": None,
                    "fallback_used": True,
                }
    except Exception as exc:
        trace_status = "error"
        trace_metadata.update(summarize_exception(exc))
        trace_output = {"status": "failed"}
        raise
    finally:
        if trace_ctx is not None and trace_sink is not None:
            finish_meta = dict(trace_metadata)
            finish_meta["duration_ms"] = round(
                (time.perf_counter() * 1000.0) - trace_started_at_ms, 2
            )
            await trace_sink.finish_trace(
                trace_ctx,
                status=trace_status,
                output=trace_output,
                metadata=finish_meta,
            )


async def _send_file_safe(
    bot: Bot, chat_id: int, file_path: str, caption: str | None = None
) -> None:
    clean_caption = sanitize_user_facing_text(strip_reaction_tags(caption or "")) or None
    input_file = FSInputFile(file_path)
    media_kind = _detect_telegram_media_kind(file_path)
    if media_kind == "image":
        await bot.send_photo(
            chat_id,
            photo=input_file,
            caption=clean_caption,
        )
        return
    if media_kind == "animation":
        await bot.send_animation(
            chat_id,
            animation=input_file,
            caption=clean_caption,
        )
        return
    if media_kind == "video":
        await bot.send_video(
            chat_id,
            video=input_file,
            caption=clean_caption,
        )
        return
    if media_kind == "audio":
        await bot.send_audio(
            chat_id,
            audio=input_file,
            caption=clean_caption,
        )
        return
    await bot.send_document(
        chat_id,
        document=input_file,
        caption=clean_caption,
    )


def _detect_telegram_media_kind(file_path: str) -> str:
    mime_type, _ = mimetypes.guess_type(file_path)
    suffix = Path(file_path).suffix.lower()
    if suffix == ".gif" or mime_type == "image/gif":
        return "animation"
    if mime_type and mime_type.lower().startswith("image/"):
        return "image"
    if mime_type and mime_type.lower().startswith("video/"):
        return "video"
    if mime_type and mime_type.lower().startswith("audio/"):
        return "audio"
    return "document"


async def _enqueue_send(
    bot: Bot,
    chat_id: int,
    text: str,
    reply_to_message_id: int | None = None,
    *,
    delivery_source: str = "telegram",
    correlation_id: str | None = None,
    trace_sink: TraceSink | None = None,
) -> None:
    decision = resolve_user_delivery(text)
    if not decision.user_visible:
        logger.debug("Suppressed control response before queueing", chat_id=chat_id)
        return
    active_trace_sink = trace_sink or _TELEGRAM_TRACE_SINK
    active_correlation_id = correlation_id or (str(correlation_id_var.get() or "").strip() or None)

    queue = _CHAT_QUEUES.get(chat_id)
    if not queue:
        queue = asyncio.Queue()
        _CHAT_QUEUES[chat_id] = queue

    # If the task is missing or has finished, create a new one.
    if chat_id not in _CHAT_SEND_TASKS or _CHAT_SEND_TASKS[chat_id].done():
        _CHAT_SEND_TASKS[chat_id] = asyncio.create_task(_sender_loop(bot, chat_id, queue))
    _publish_runtime_metrics()

    await queue.put(
        QueuedMessage(
            text=decision.text,
            reply_to_message_id=reply_to_message_id,
            delivery_source=delivery_source,
            correlation_id=active_correlation_id,
            trace_sink=active_trace_sink,
        )
    )


async def _sender_loop(bot: Bot, chat_id: int, queue: asyncio.Queue[QueuedMessage]) -> None:
    while True:
        try:
            # Wait for a new message, but with a timeout.
            msg = await asyncio.wait_for(queue.get(), timeout=_SEND_IDLE_TIMEOUT_SECONDS)
        except TimeoutError:
            # Queue has been empty for the timeout duration, so this worker can exit.
            break

        try:
            await _send_chunked(
                bot,
                chat_id,
                msg.text,
                reply_to_message_id=msg.reply_to_message_id,
                delivery_source=msg.delivery_source,
                correlation_id=msg.correlation_id,
                trace_sink=msg.trace_sink,
            )
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


def _flush_pending_turn_factory(
    octo: Octo,
    settings: Settings,
    bot: Bot,
):
    async def _flush_pending_turn(
        chat_id: int,
        text: str,
        images: list[str],
        saved_file_paths: list[str],
        metadata: dict[str, Any],
    ) -> None:
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        reply_to_message_id = metadata.get("reply_to_message_id")
        is_group_chat = bool(metadata.get("is_group_chat"))
        if is_group_chat:
            provider = getattr(octo, "provider", None)
            recent_context = await load_recent_group_context(octo, chat_id=chat_id)
            decision = await decide_group_addressing(
                provider=provider,
                settings=settings,
                channel="telegram",
                chat_id=chat_id,
                text=text,
                has_attachments=bool(images or saved_file_paths),
                reply_to_agent=bool(metadata.get("reply_to_agent")),
                sender_label=str(metadata.get("sender_label", "") or "") or None,
                recent_context=recent_context,
            )
            if not decision.should_process:
                await record_passive_group_observation(
                    octo,
                    channel="telegram",
                    chat_id=chat_id,
                    text=text,
                    images=images,
                    saved_file_paths=saved_file_paths,
                    sender_label=str(metadata.get("sender_label", "") or "") or None,
                    addressing_action=decision.action,
                    addressing_reason=decision.reason,
                )
                logger.info(
                    "Observed non-addressed Telegram group message",
                    chat_id=chat_id,
                    reason=decision.reason,
                    confidence=decision.confidence,
                )
                return
            source_context = {
                "source_channel": "telegram",
                "chat_kind": "group",
                "addressing_action": decision.action,
                "addressing_reason": decision.reason,
            }
        else:
            source_context = {
                "source_channel": "telegram",
                "chat_kind": "private",
            }

        # Immediate feedback
        if reply_to_message_id is not None:
            try:
                await bot.set_message_reaction(
                    chat_id=chat_id,
                    message_id=reply_to_message_id,
                    reaction=[ReactionTypeEmoji(emoji="🤔")],
                )
            except Exception:
                logger.debug("Failed to set thinking reaction", chat_id=chat_id, exc_info=True)

        async with lock:
            try:
                reply = await octo.handle_message(
                    text,
                    chat_id,
                    images=images,
                    saved_file_paths=saved_file_paths,
                    source_channel="telegram",
                    source_context=source_context,
                )
            except Exception:
                logger.exception("Failed to handle aggregated message", chat_id=chat_id)
                await _enqueue_send(
                    bot,
                    chat_id,
                    "I received your message, but something broke on my side. Please try again.",
                    reply_to_message_id=reply_to_message_id,
                )
                return

            if isinstance(reply, OctoReply):
                update_last_message(settings)
                final_text = reply.immediate or ""

                tagged_emoji, final_text = extract_reaction_and_strip(final_text)
                inferred_emoji = None
                if not tagged_emoji and not getattr(reply, "reaction", None):
                    inferred_emoji, final_text = extract_edge_reaction_fallback(final_text)
                    if inferred_emoji:
                        logger.debug(
                            "Inferred terminal reaction from plain-text edge emoji",
                            chat_id=chat_id,
                            message_id=reply_to_message_id,
                            emoji=inferred_emoji,
                        )
                effective_emoji = tagged_emoji or getattr(reply, "reaction", None) or inferred_emoji
                if effective_emoji:
                    logger.debug(
                        "Detected terminal reaction in octo reply",
                        chat_id=chat_id,
                        message_id=reply_to_message_id,
                        emoji=effective_emoji,
                        reply_reaction=getattr(reply, "reaction", None),
                    )
                elif reply_to_message_id is not None:
                    logger.debug(
                        "No terminal reaction tag found in octo reply",
                        chat_id=chat_id,
                        message_id=reply_to_message_id,
                        reply_reaction=getattr(reply, "reaction", None),
                    )
                if effective_emoji and reply_to_message_id is not None:
                    mapped_emoji = normalize_reaction_emoji(effective_emoji)
                    try:
                        await bot.set_message_reaction(
                            chat_id=chat_id,
                            message_id=reply_to_message_id,
                            reaction=[ReactionTypeEmoji(emoji=mapped_emoji)],
                        )
                        logger.debug(
                            "Applied terminal reaction to Telegram message",
                            chat_id=chat_id,
                            message_id=reply_to_message_id,
                            requested_emoji=effective_emoji,
                            applied_emoji=mapped_emoji,
                        )
                    except Exception:
                        logger.warning(
                            "Failed to set terminal reaction",
                            chat_id=chat_id,
                            emoji=effective_emoji,
                            exc_info=True,
                        )

                if final_text:
                    await _enqueue_send(
                        bot, chat_id, final_text, reply_to_message_id=reply_to_message_id
                    )
                return

        update_last_message(settings)
        decision = resolve_user_delivery(str(reply))
        if decision.user_visible:
            await _enqueue_send(
                bot, chat_id, decision.text, reply_to_message_id=reply_to_message_id
            )

    return _flush_pending_turn


def _is_telegram_group_chat(message: Message) -> bool:
    chat_type = str(getattr(getattr(message, "chat", None), "type", "") or "").lower()
    return chat_type in {"group", "supergroup"}


def _telegram_reply_targets_this_bot(message: Message, bot: Bot) -> bool:
    reply_to = getattr(message, "reply_to_message", None)
    reply_from = getattr(reply_to, "from_user", None)
    if reply_from is None:
        return False
    bot_id = getattr(bot, "id", None)
    reply_from_id = getattr(reply_from, "id", None)
    if bot_id is not None and reply_from_id is not None:
        return int(bot_id) == int(reply_from_id)
    return False


def _telegram_message_from_this_bot(message: Message, bot: Bot) -> bool:
    sender = getattr(message, "from_user", None)
    bot_id = getattr(bot, "id", None)
    sender_id = getattr(sender, "id", None)
    if bot_id is None or sender_id is None:
        return False
    return int(bot_id) == int(sender_id)


def _telegram_bot_authored_message_should_skip(message: Message, bot: Bot) -> bool:
    if not bool(getattr(getattr(message, "from_user", None), "is_bot", False)):
        return False
    return not (
        _is_telegram_group_chat(message) and not _telegram_message_from_this_bot(message, bot)
    )


async def _telegram_group_command_should_run(
    message: Message,
    *,
    command: CommandObject | None,
    settings: Settings,
    octo: Octo,
    bot: Bot,
) -> bool:
    if not _is_telegram_group_chat(message):
        return True
    if _telegram_command_targets_this_bot(command, bot):
        return True
    provider = getattr(octo, "provider", None)
    recent_context = await load_recent_group_context(octo, chat_id=message.chat.id)
    decision = await decide_group_addressing(
        provider=provider,
        settings=settings,
        channel="telegram",
        chat_id=message.chat.id,
        text=message.text or message.caption or "",
        reply_to_agent=_telegram_reply_targets_this_bot(message, bot),
        sender_label=_telegram_sender_label(message) or None,
        recent_context=recent_context,
    )
    if decision.should_process:
        return True
    logger.info(
        "Ignoring non-addressed Telegram group command",
        chat_id=message.chat.id,
        command=getattr(command, "command", None),
        reason=decision.reason,
        confidence=decision.confidence,
    )
    return False


def _telegram_command_targets_this_bot(command: CommandObject | None, bot: Bot) -> bool:
    mention = str(getattr(command, "mention", "") or "").strip().lstrip("@").casefold()
    if not mention:
        return False
    username = _telegram_bot_username(bot)
    return bool(username and mention == username.casefold())


def _telegram_bot_username(bot: Bot) -> str:
    for attr in ("username", "bot_username"):
        value = str(getattr(bot, attr, "") or "").strip()
        if value:
            return value.lstrip("@")
    me = getattr(bot, "_me", None)
    value = str(getattr(me, "username", "") or "").strip()
    return value.lstrip("@")


def _telegram_sender_label(message: Message) -> str:
    user = getattr(message, "from_user", None)
    parts = [
        str(getattr(user, "full_name", "") or "").strip(),
        str(getattr(user, "username", "") or "").strip(),
    ]
    return " / ".join(part for part in parts if part)


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
    logger.warning("Unknown OCTOPAL_TELEGRAM_PARSE_MODE value; using plain text (value=%s)", value)
    return None


def _prepare_markdown_v2(text: str) -> str:
    """Convert common markdown into Telegram-safe MarkdownV2."""
    if not text:
        return ""
    return telegramify_markdown.markdownify(text)
