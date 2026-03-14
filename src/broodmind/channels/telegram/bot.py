from __future__ import annotations

import asyncio
import os

import structlog
from aiogram import Bot, Dispatcher

from broodmind.app_runtime import build_queen
from broodmind.config.settings import Settings
from broodmind.queen.core import Queen, QueenReply
from broodmind.channels.telegram.approvals import ApprovalManager
from broodmind.channels.telegram.handlers import register_handlers
from broodmind.utils import is_heartbeat_ok, is_control_response

logger = structlog.get_logger(__name__)


def build_dispatcher(settings: Settings, bot: Bot) -> Dispatcher:
    os.environ.setdefault("BROODMIND_STATE_DIR", str(settings.state_dir))
    os.environ.setdefault("BROODMIND_WORKSPACE_DIR", str(settings.workspace_dir))
    queen = build_queen(settings)
    queen.approvals = ApprovalManager(bot=bot)

    dp = Dispatcher()
    register_handlers(dp, queen, queen.approvals, settings, bot)
    return dp, queen


async def _heartbeat_poker(queen: Queen, interval_seconds: int, chat_id: int):
    """Periodically triggers the queen's heartbeat logic."""
    logger.info("Starting application-level heartbeat with interval=%ss", interval_seconds)
    while True:
        await asyncio.sleep(interval_seconds)
        logger.info("Triggering internal heartbeat for chat_id=%s", chat_id)
        try:
            context_hint = await queen.build_heartbeat_context_hint(chat_id)
            heartbeat_prompt = (
                "This is a heartbeat trigger. Use `check_schedule` to identify and execute any due tasks.\n\n"
                f"{context_hint}"
            )
            reply = await queen.handle_message(
                heartbeat_prompt,
                chat_id,
                show_typing=False,
                persist_to_memory=False,
                track_progress=False,
                include_wakeup=False,
            )
            # Heartbeat replies are control-plane responses; don't send them to Telegram chat.
            if isinstance(reply, QueenReply):
                text = (reply.immediate or "").strip()
                if is_control_response(text):
                    logger.debug("Heartbeat processed successfully (control response acknowledged)", response=text)
                elif not text:
                    logger.warning("Heartbeat produced empty response")
                else:
                    logger.warning(
                        "Heartbeat produced non-control text; suppressing delivery to Telegram",
                        chat_id=chat_id,
                        preview=text[:200],
                    )
        except Exception:
            logger.exception("Internal heartbeat execution failed")


async def run_bot(settings: Settings, existing_queen: Queen | None = None) -> None:
    bot = Bot(token=settings.telegram_bot_token)
    if existing_queen:
        dp = Dispatcher()
        from broodmind.channels.telegram.approvals import ApprovalManager
        from broodmind.channels.telegram.handlers import register_handlers

        approvals = ApprovalManager(bot=bot)
        # Update queen's approval bot
        existing_queen.approvals = approvals
        register_handlers(dp, existing_queen, approvals, settings, bot)
        queen = existing_queen
    else:
        dp, queen = build_dispatcher(settings, bot)

    # Parse allowed chat IDs from settings
    allowed_chat_ids = []
    if settings.allowed_telegram_chat_ids:
        try:
            allowed_chat_ids = [
                int(cid.strip()) for cid in settings.allowed_telegram_chat_ids.split(",") if cid.strip()
            ]
        except ValueError:
            logger.error("Invalid ALLOWED_TELEGRAM_CHAT_IDS format - must be comma-separated integers")

    # Initialize queen system before starting polling
    logger.info("Initializing queen system")
    await queen.initialize_system(bot, allowed_chat_ids=allowed_chat_ids)
    logger.info("Queen system initialization complete")

    # Start application-level heartbeat if configured
    heartbeat_task = None
    if settings.heartbeat_interval_seconds > 0 and allowed_chat_ids:
        heartbeat_task = asyncio.create_task(
            _heartbeat_poker(
                queen,
                settings.heartbeat_interval_seconds,
                allowed_chat_ids[0],  # Use the first allowed chat as the context
            )
        )

    logger.info("Starting Telegram polling")
    try:
        await dp.start_polling(bot)
    finally:
        logger.info("Shutting down bot session")
        await bot.session.close()

        if heartbeat_task:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                logger.info("Stopped internal heartbeat task.")

        logger.info("Stopping queen background tasks")
        await queen.stop_background_tasks()
