from __future__ import annotations

import asyncio
import os

import structlog
from aiogram import Bot, Dispatcher

from octopal.channels.telegram.approvals import ApprovalManager
from octopal.channels.telegram.handlers import register_handlers
from octopal.infrastructure.config.settings import Settings
from octopal.runtime.app import build_octo
from octopal.runtime.octo.core import Octo, OctoReply
from octopal.runtime.octo.delivery import DeliveryMode
from octopal.utils import is_control_response

logger = structlog.get_logger(__name__)


def build_dispatcher(settings: Settings, bot: Bot) -> Dispatcher:
    os.environ.setdefault("OCTOPAL_STATE_DIR", str(settings.state_dir))
    os.environ.setdefault("OCTOPAL_WORKSPACE_DIR", str(settings.workspace_dir))
    octo = build_octo(settings)
    octo.approvals = ApprovalManager(bot=bot)

    dp = Dispatcher()
    register_handlers(dp, octo, octo.approvals, settings, bot)
    return dp, octo


async def _heartbeat_poker(octo: Octo, interval_seconds: int, chat_id: int):
    """Periodically triggers the octo's heartbeat logic."""
    logger.info("Starting application-level heartbeat with interval=%ss", interval_seconds)
    loop = asyncio.get_running_loop()
    next_tick = loop.time() + interval_seconds
    while True:
        await asyncio.sleep(max(0.0, next_tick - loop.time()))
        logger.info("Triggering internal heartbeat for chat_id=%s", chat_id)
        try:
            context_hint = await octo.build_heartbeat_context_hint(chat_id)
            heartbeat_prompt = (
                "This is a heartbeat trigger. Use `check_schedule` to identify and execute any due tasks.\n\n"
                f"{context_hint}"
            )
            reply = await octo.handle_message(
                heartbeat_prompt,
                chat_id,
                show_typing=False,
                persist_to_memory=False,
                track_progress=False,
                include_wakeup=False,
                background_delivery=True,
                source_channel="telegram",
            )
            if isinstance(reply, OctoReply):
                text = (reply.immediate or "").strip()
                if reply.delivery_mode == DeliveryMode.IMMEDIATE and text:
                    if octo.should_suppress_heartbeat_delivery(chat_id, text):
                        logger.info(
                            "Heartbeat user-visible update suppressed due to recent delivery",
                            chat_id=chat_id,
                            text_len=len(text),
                        )
                    elif octo.internal_send:
                        await octo.internal_send(chat_id, text)
                        await octo.emit_ws_chat_event(
                            direction="outbound",
                            role="assistant",
                            channel="telegram",
                            chat_id=chat_id,
                            text=text,
                            meta={"delivery_source": "heartbeat"},
                        )
                        octo.note_user_visible_delivery(chat_id, text)
                        logger.info(
                            "Heartbeat delivered user-visible update",
                            chat_id=chat_id,
                            text_len=len(text),
                        )
                    else:
                        logger.warning(
                            "Heartbeat produced user-visible update but no sender is attached",
                            chat_id=chat_id,
                            preview=text[:200],
                        )
                elif is_control_response(text):
                    logger.debug(
                        "Heartbeat processed successfully (control response acknowledged)",
                        response=text,
                    )
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
        finally:
            next_tick += interval_seconds
            now = loop.time()
            if next_tick < now:
                next_tick = now + interval_seconds


async def run_bot(settings: Settings, existing_octo: Octo | None = None) -> None:
    bot = Bot(token=settings.telegram_bot_token)
    if existing_octo:
        dp = Dispatcher()
        from octopal.channels.telegram.approvals import ApprovalManager
        from octopal.channels.telegram.handlers import register_handlers

        approvals = ApprovalManager(bot=bot)
        # Update octo's approval bot
        existing_octo.approvals = approvals
        register_handlers(dp, existing_octo, approvals, settings, bot)
        octo = existing_octo
    else:
        dp, octo = build_dispatcher(settings, bot)

    # Parse allowed chat IDs from settings
    allowed_chat_ids = []
    if settings.allowed_telegram_chat_ids:
        try:
            allowed_chat_ids = [
                int(cid.strip())
                for cid in settings.allowed_telegram_chat_ids.split(",")
                if cid.strip()
            ]
        except ValueError:
            logger.error(
                "Invalid ALLOWED_TELEGRAM_CHAT_IDS format - must be comma-separated integers"
            )

    # Initialize octo system before starting polling
    logger.info("Initializing octo system")
    await octo.initialize_system(bot, allowed_chat_ids=allowed_chat_ids)
    logger.info("Octo system initialization complete")

    # Start application-level heartbeat if configured
    heartbeat_task = None
    if settings.heartbeat_interval_seconds > 0 and allowed_chat_ids:
        heartbeat_task = asyncio.create_task(
            _heartbeat_poker(
                octo,
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

        logger.info("Stopping octo background tasks")
        await octo.stop_background_tasks()
