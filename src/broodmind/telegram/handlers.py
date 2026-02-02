from __future__ import annotations

import logging
import asyncio

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from broodmind.config.settings import Settings
from broodmind.queen.core import Queen, QueenReply
from broodmind.state import update_last_message
from broodmind.telegram.approvals import ApprovalManager

logger = logging.getLogger(__name__)
_CHAT_LOCKS: dict[int, asyncio.Lock] = {}
_CHAT_QUEUES: dict[int, asyncio.Queue[str]] = {}
_CHAT_SEND_TASKS: dict[int, asyncio.Task] = {}


def register_handlers(
    dp: Dispatcher, queen: Queen, approvals: ApprovalManager, settings: Settings, bot: Bot
) -> None:
    async def _internal_send(chat_id: int, text: str) -> None:
        await _enqueue_send(bot, chat_id, text)

    queen.internal_send = _internal_send
    
    @dp.message()
    async def handle_message(message: Message) -> None:
        if not message.text:
            return
        logger.info("Incoming message")
        lock = _CHAT_LOCKS.setdefault(message.chat.id, asyncio.Lock())
        async with lock:
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


async def _send_chunked(bot: Bot, chat_id: int, text: str, limit: int = 4000) -> None:
    chunks = _chunk_text(text, limit)
    for chunk in chunks:
        await bot.send_message(chat_id, chunk)


async def _enqueue_send(bot: Bot, chat_id: int, text: str) -> None:
    queue = _CHAT_QUEUES.get(chat_id)
    if not queue:
        queue = asyncio.Queue()
        _CHAT_QUEUES[chat_id] = queue

    # If the task is missing or has finished, create a new one.
    if chat_id not in _CHAT_SEND_TASKS or _CHAT_SEND_TASKS[chat_id].done():
        _CHAT_SEND_TASKS[chat_id] = asyncio.create_task(_sender_loop(bot, chat_id, queue))
        
    await queue.put(text)


async def _sender_loop(bot: Bot, chat_id: int, queue: asyncio.Queue[str]) -> None:
    while True:
        try:
            # Wait for a new message, but with a timeout.
            text = await asyncio.wait_for(queue.get(), timeout=300.0)  # 5-minute timeout
        except asyncio.TimeoutError:
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
    logger.debug("Sender loop for chat_id=%s finished due to inactivity.", chat_id)



async def _typing_loop(message: Message, stop: asyncio.Event) -> None:
    try:
        while not stop.is_set():
            await message.bot.send_chat_action(message.chat.id, action="typing")
            try:
                await asyncio.wait_for(stop.wait(), timeout=4.0)
            except asyncio.TimeoutError:
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
