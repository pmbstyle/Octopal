from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from typing import Any

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from broodmind.config.settings import Settings
from broodmind.logging_config import correlation_id_var
from broodmind.queen.core import Queen, QueenReply
from broodmind.runtime_metrics import update_component_gauges
from broodmind.state import is_pid_running, read_status, update_last_message
from broodmind.telegram.approvals import ApprovalManager

logger = logging.getLogger(__name__)
_CHAT_LOCKS: dict[int, asyncio.Lock] = {}
_CHAT_QUEUES: dict[int, asyncio.Queue[str]] = {}
_CHAT_SEND_TASKS: dict[int, asyncio.Task] = {}
_SEND_IDLE_TIMEOUT_SECONDS = 300.0
_PROGRESS_COALESCE_SECONDS = 8.0
_CHAT_LAST_PROGRESS: dict[int, tuple[str, float]] = {}
_WORKER_CB_PREFIX = "worker:"
_WORKER_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$")
_TELEGRAM_PARSE_MODE: str | None = None


def _publish_runtime_metrics() -> None:
    update_component_gauges(
        "telegram",
        {
            "chat_locks": len(_CHAT_LOCKS),
            "chat_queues": len(_CHAT_QUEUES),
            "send_tasks": len(_CHAT_SEND_TASKS),
        },
    )


def register_handlers(
    dp: Dispatcher, queen: Queen, approvals: ApprovalManager, settings: Settings, bot: Bot
) -> None:
    global _TELEGRAM_PARSE_MODE
    _TELEGRAM_PARSE_MODE = _normalize_parse_mode(settings.telegram_parse_mode)

    async def _internal_send(chat_id: int, text: str) -> None:
        await _enqueue_send(bot, chat_id, text)
    async def _internal_progress_send(
        chat_id: int,
        state: str,
        text: str,
        meta: dict[str, object],
    ) -> None:
        # Keep worker lifecycle noise out of chat; only surface failure/duplicate events.
        if state in {"failed", "duplicate"}:
            await _enqueue_progress(bot, chat_id, state, text, meta)

    queen.internal_send = _internal_send
    queen.internal_progress_send = _internal_progress_send

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
            if await _handle_command(message, queen, settings):
                return
            typing_stop = asyncio.Event()
            typing_task = asyncio.create_task(_typing_loop(message, typing_stop))
            try:
                reply = await queen.handle_message(message.text, message.chat.id)
            except Exception as exc:
                logger.exception("Failed to handle message")
                response = f"Error: {exc}"
                update_last_message(settings)
                typing_stop.set()
                await _enqueue_send(message.bot, message.chat.id, response)
                return
            finally:
                typing_stop.set()
                if not typing_task.done():
                    typing_task.cancel()

            if isinstance(reply, QueenReply):
                update_last_message(settings)
                await _enqueue_send(message.bot, message.chat.id, reply.immediate)
                if reply.followup:
                    async def _send_followup(task):
                        try:
                            result_text = await task
                            if result_text and result_text.strip():
                                await _enqueue_send(message.bot, message.chat.id, result_text)
                        except Exception as exc:
                            logger.exception("Failed to send followup")
                            await _enqueue_send(
                                message.bot,
                                message.chat.id,
                                f"Worker error: {exc}",
                            )
                    asyncio.create_task(_send_followup(reply.followup))
                return

        update_last_message(settings)
        await _enqueue_send(message.bot, message.chat.id, str(reply))

    @dp.callback_query()
    async def handle_callback(query: CallbackQuery) -> None:
        data = query.data or ""
        lock = _CHAT_LOCKS.setdefault(query.message.chat.id if query.message else query.from_user.id, asyncio.Lock())
        _publish_runtime_metrics()
        async with lock:
            if data.startswith(_WORKER_CB_PREFIX):
                await _handle_worker_callback(query, data, queen)
                return
        if data.startswith("approve:"):
            intent_id = data.split(":", 1)[1]
            approvals.resolve(intent_id, True)
            await query.answer("Approved")
            if query.message:
                await query.message.edit_reply_markup(reply_markup=None)
            return
        if data.startswith("deny:"):
            intent_id = data.split(":", 1)[1]
            approvals.resolve(intent_id, False)
            await query.answer("Denied")
            if query.message:
                await query.message.edit_reply_markup(reply_markup=None)


async def _handle_command(message: Message, queen: Queen, settings: Settings) -> bool:
    text = (message.text or "").strip()
    if not text.startswith("/"):
        return False

    command_token = text.split(maxsplit=1)[0]
    command = command_token.split("@", 1)[0].lower()
    args = text[len(command_token):].strip()

    if command == "/help":
        await _enqueue_send(
            message.bot,
            message.chat.id,
            (
                "Available commands:\n"
                "/help - Show this help\n"
                "/status - Show bot/runtime status\n"
                "/workers - Show templates and recent active workers\n"
                "/memory - Show memory snapshot stats"
            ),
        )
        update_last_message(settings)
        return True

    if command == "/status":
        status_data = read_status(settings)
        pid = status_data.get("pid") if status_data else None
        running = is_pid_running(pid)
        active_workers = await asyncio.to_thread(queen.store.get_active_workers, 10)
        reply = (
            f"System: {'RUNNING' if running else 'STOPPED'}\n"
            f"PID: {pid or 'N/A'}\n"
            f"Last heartbeat: {status_data.get('last_message_at') if status_data else 'Never'}\n"
            f"Active/recent workers: {len(active_workers)}"
        )
        await _enqueue_send(message.bot, message.chat.id, reply)
        update_last_message(settings)
        return True

    if command == "/workers":
        templates = await asyncio.to_thread(queen.store.list_worker_templates)
        active_workers = await asyncio.to_thread(queen.store.get_active_workers, 30)
        worker_lines = [f"- {t.id}: {t.name}" for t in templates[:10]]
        if len(templates) > 10:
            worker_lines.append(f"... and {len(templates) - 10} more")
        active_lines = [f"- {w.id[:8]} {w.status}: {w.task[:60]}" for w in active_workers[:5]]
        if len(active_workers) > 5:
            active_lines.append(f"... and {len(active_workers) - 5} more")
        reply = (
            f"Worker templates ({len(templates)}):\n"
            + ("\n".join(worker_lines) if worker_lines else "No templates found.")
            + "\n\nRecent workers:\n"
            + ("\n".join(active_lines) if active_lines else "No active/recent workers.")
        )
        await _enqueue_send(message.bot, message.chat.id, reply)
        update_last_message(settings)
        return True

    if command == "/memory":
        limit = 300
        if args.isdigit():
            limit = max(50, min(1000, int(args)))
        entries = await asyncio.to_thread(queen.store.list_memory_entries, limit)
        by_role: dict[str, int] = {}
        chat_ids: set[int] = set()
        for entry in entries:
            by_role[entry.role] = by_role.get(entry.role, 0) + 1
            chat_id = entry.metadata.get("chat_id") if entry.metadata else None
            if isinstance(chat_id, int):
                chat_ids.add(chat_id)
        reply = (
            f"Memory snapshot (latest {limit} entries):\n"
            f"- Total: {len(entries)}\n"
            f"- Unique chats in snapshot: {len(chat_ids)}\n"
            f"- Roles: {json.dumps(by_role, ensure_ascii=False)}"
        )
        await _enqueue_send(message.bot, message.chat.id, reply)
        update_last_message(settings)
        return True

    return False


async def _send_chunked(bot: Bot, chat_id: int, text: str, limit: int = 4000) -> None:
    chunks = _chunk_text(text, limit)
    for chunk in chunks:
        await _send_message_safe(bot, chat_id, chunk)


async def _send_message_safe(bot: Bot, chat_id: int, text: str) -> None:
    parse_mode = _TELEGRAM_PARSE_MODE
    if not parse_mode:
        await bot.send_message(chat_id, text)
        return
    try:
        await bot.send_message(chat_id, text, parse_mode=parse_mode)
    except TelegramBadRequest as exc:
        # Formatting mismatch should not drop the message for the user.
        logger.warning(
            "Telegram parse failed; retrying without parse_mode (parse_mode=%s, error=%s)",
            parse_mode,
            exc,
        )
        await bot.send_message(chat_id, text)


async def _enqueue_send(bot: Bot, chat_id: int, text: str) -> None:
    queue = _CHAT_QUEUES.get(chat_id)
    if not queue:
        queue = asyncio.Queue()
        _CHAT_QUEUES[chat_id] = queue

    # If the task is missing or has finished, create a new one.
    if chat_id not in _CHAT_SEND_TASKS or _CHAT_SEND_TASKS[chat_id].done():
        _CHAT_SEND_TASKS[chat_id] = asyncio.create_task(_sender_loop(bot, chat_id, queue))
    _publish_runtime_metrics()

    await queue.put(text)


async def _sender_loop(bot: Bot, chat_id: int, queue: asyncio.Queue[str]) -> None:
    while True:
        try:
            # Wait for a new message, but with a timeout.
            text = await asyncio.wait_for(queue.get(), timeout=_SEND_IDLE_TIMEOUT_SECONDS)
        except TimeoutError:
            # Queue has been empty for the timeout duration, so this worker can exit.
            break

        try:
            await _send_chunked(bot, chat_id, text)
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


async def _enqueue_progress(
    bot: Bot,
    chat_id: int,
    state: str,
    text: str,
    meta: dict[str, object] | None = None,
) -> None:
    now = time.monotonic()
    last = _CHAT_LAST_PROGRESS.get(chat_id)
    if last and last[0] == state and (now - last[1]) < _PROGRESS_COALESCE_SECONDS:
        return
    _CHAT_LAST_PROGRESS[chat_id] = (state, now)
    await _enqueue_send(bot, chat_id, _format_progress_text(state, text, meta or {}))


def _format_progress_text(state: str, text: str, meta: dict[str, object]) -> str:
    worker_id = str(meta.get("worker_id", "")).strip()
    if state == "queued":
        return f"Queued. {text}"
    if state == "running":
        return f"Running. {text}"
    if state == "completed":
        return f"Completed. {text}"
    if state == "failed":
        return f"Failed. {text}"
    if state == "duplicate":
        return text
    if state == "worker_started" and worker_id:
        return f"Worker ready: {worker_id}"
    return text



async def _typing_loop(message: Message, stop: asyncio.Event) -> None:
    try:
        while not stop.is_set():
            await message.bot.send_chat_action(message.chat.id, action="typing")
            try:
                await asyncio.wait_for(stop.wait(), timeout=4.0)
            except TimeoutError:
                continue
    except Exception:
        logger.debug("Typing indicator failed", exc_info=True)


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


def _worker_controls_keyboard(worker_id: str, *, can_stop: bool) -> InlineKeyboardMarkup:
    row = [
        InlineKeyboardButton(
            text="Refresh",
            callback_data=f"{_WORKER_CB_PREFIX}refresh:{worker_id}",
        ),
        InlineKeyboardButton(
            text="Get result",
            callback_data=f"{_WORKER_CB_PREFIX}result:{worker_id}",
        ),
    ]
    if can_stop:
        row.append(
            InlineKeyboardButton(
                text="Stop",
                callback_data=f"{_WORKER_CB_PREFIX}stop:{worker_id}",
            )
        )
    return InlineKeyboardMarkup(inline_keyboard=[row])


def _is_safe_worker_id(worker_id: str) -> bool:
    return bool(worker_id and _WORKER_ID_RE.match(worker_id))


def _format_worker_status_text(worker: Any) -> str:
    if not worker:
        return "Worker not found."
    summary = worker.summary or "-"
    error = worker.error or "-"
    return (
        f"Worker: {worker.id}\n"
        f"Status: {worker.status}\n"
        f"Task: {worker.task}\n"
        f"Updated: {worker.updated_at.isoformat()}\n"
        f"Summary: {summary}\n"
        f"Error: {error}"
    )


def _format_worker_result_text(worker: Any) -> str:
    if not worker:
        return "Worker not found."
    if worker.status == "completed":
        output_text = json.dumps(worker.output, ensure_ascii=False) if worker.output else "{}"
        if len(output_text) > 2000:
            output_text = output_text[:2000] + "...[truncated]"
        return (
            f"Worker: {worker.id}\n"
            f"Status: completed\n"
            f"Summary: {worker.summary or '-'}\n"
            f"Output: {output_text}"
        )
    if worker.status == "failed":
        return (
            f"Worker: {worker.id}\n"
            f"Status: failed\n"
            f"Error: {worker.error or 'Unknown error'}"
        )
    return f"Worker: {worker.id}\nStatus: {worker.status}\nResult is not available yet."


async def _handle_worker_callback(query: CallbackQuery, data: str, queen: Queen) -> None:
    parts = data.split(":", 2)
    if len(parts) != 3:
        await query.answer("Invalid worker callback.", show_alert=True)
        return
    _, action, worker_id = parts
    worker_id = worker_id.strip()
    if not _is_safe_worker_id(worker_id):
        await query.answer("Invalid worker id.", show_alert=True)
        return

    if action == "refresh":
        worker = await asyncio.to_thread(queen.store.get_worker, worker_id)
        if not worker:
            await query.answer("Worker not found (may have been cleaned up).", show_alert=True)
            if query.message:
                await query.message.edit_reply_markup(reply_markup=None)
            return
        text = _format_worker_status_text(worker)
        await query.answer("Status refreshed.")
        if query.message:
            try:
                await query.message.edit_text(
                    text,
                    reply_markup=_worker_controls_keyboard(
                        worker_id,
                        can_stop=worker.status in {"started", "running"},
                    ),
                )
            except Exception:
                logger.debug("Failed to edit refresh callback message", exc_info=True)
        return

    if action == "result":
        worker = await asyncio.to_thread(queen.store.get_worker, worker_id)
        if not worker:
            await query.answer("Worker not found (may have been cleaned up).", show_alert=True)
            if query.message:
                await query.message.edit_reply_markup(reply_markup=None)
            return
        text = _format_worker_result_text(worker)
        await query.answer("Result fetched.")
        if query.message:
            try:
                await query.message.edit_text(
                    text,
                    reply_markup=_worker_controls_keyboard(
                        worker_id,
                        can_stop=worker.status in {"started", "running"},
                    ),
                )
            except Exception:
                logger.debug("Failed to edit result callback message", exc_info=True)
        return

    if action == "stop":
        stopped = await queen.runtime.stop_worker(worker_id)
        worker = await asyncio.to_thread(queen.store.get_worker, worker_id)
        if not worker:
            await query.answer("Worker not found (may have been cleaned up).", show_alert=True)
            if query.message:
                await query.message.edit_reply_markup(reply_markup=None)
            return
        text = _format_worker_status_text(worker)
        await query.answer("Worker stopped." if stopped else "Worker not running.")
        if query.message:
            try:
                await query.message.edit_text(
                    text,
                    reply_markup=_worker_controls_keyboard(worker_id, can_stop=False),
                )
            except Exception:
                logger.debug("Failed to edit stop callback message", exc_info=True)
        return

    await query.answer("Unknown worker action.", show_alert=True)
