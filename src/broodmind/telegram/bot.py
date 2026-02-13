from __future__ import annotations

import asyncio
import os

import structlog
from aiogram import Bot, Dispatcher

from broodmind.config.settings import Settings
from broodmind.mcp.manager import MCPManager
from broodmind.memory.canon import CanonService
from broodmind.memory.service import MemoryService
from broodmind.policy.engine import PolicyEngine
from broodmind.providers.litellm_provider import LiteLLMProvider
from broodmind.providers.openai_embeddings import OpenAIEmbeddingsProvider
from broodmind.queen.core import Queen, QueenReply
from broodmind.store.sqlite import SQLiteStore
from broodmind.telegram.approvals import ApprovalManager
from broodmind.telegram.handlers import register_handlers
from broodmind.utils import is_heartbeat_ok
from broodmind.workers.launcher_factory import build_launcher
from broodmind.workers.runtime import WorkerRuntime

logger = structlog.get_logger(__name__)


def build_dispatcher(settings: Settings, bot: Bot) -> Dispatcher:
    os.environ.setdefault("BROODMIND_STATE_DIR", str(settings.state_dir))

    # Use unified LiteLLM provider (supports both OpenRouter and z.ai)
    provider = LiteLLMProvider(settings)

    store = SQLiteStore(settings)

    # Initialize default worker templates
    from broodmind.workers.templates import initialize_templates
    initialize_templates(store)

    policy = PolicyEngine()
    launcher = build_launcher(settings)
    mcp_manager = MCPManager(workspace_dir=settings.workspace_dir)
    runtime = WorkerRuntime(
        store=store,
        policy=policy,
        workspace_dir=settings.workspace_dir,
        launcher=launcher,
        mcp_manager=mcp_manager,
    )
    approvals = ApprovalManager(bot=bot)
    embeddings = None
    if settings.openai_api_key:
        embeddings = OpenAIEmbeddingsProvider(settings)
    memory = MemoryService(
        store=store,
        embeddings=embeddings,
        top_k=settings.memory_top_k,
        min_score=settings.memory_min_score,
        max_chars=settings.memory_max_chars,
    )
    canon = CanonService(
        workspace_dir=settings.workspace_dir,
        store=store,
        embeddings=embeddings
    )
    mcp_manager = MCPManager(workspace_dir=settings.workspace_dir)
    queen = Queen(
        provider=provider,
        store=store,
        policy=policy,
        runtime=runtime,
        approvals=approvals,
        memory=memory,
        canon=canon,
        mcp_manager=mcp_manager,
    )

    dp = Dispatcher()
    register_handlers(dp, queen, approvals, settings, bot)
    return dp, queen


async def _heartbeat_poker(queen: Queen, interval_seconds: int, chat_id: int):
    """Periodically triggers the queen's heartbeat logic."""
    logger.info("Starting application-level heartbeat with interval=%ss", interval_seconds)
    while True:
        await asyncio.sleep(interval_seconds)
        logger.info("Triggering internal heartbeat for chat_id=%s", chat_id)
        try:
            heartbeat_prompt = (
                "This is a heartbeat trigger. Check your scheduled tasks in `workspace/HEARTBEAT.md` "
                "and execute any tasks whose conditions are met."
            )
            reply = await queen.handle_message(heartbeat_prompt, chat_id, show_typing=False)
            # Heartbeat replies are control-plane responses; don't send them to Telegram chat.
            if isinstance(reply, QueenReply):
                text = (reply.immediate or "").strip()
                if is_heartbeat_ok(text):
                    logger.debug("Heartbeat processed successfully (HEARTBEAT_OK acknowledged)")
                elif not text:
                    logger.warning("Heartbeat produced empty response (no HEARTBEAT_OK)")
                else:
                    logger.info(
                        "Heartbeat produced non-ACK text (suppressed from chat, chat_id=%s, preview=%s)",
                        chat_id,
                        text[:200],
                    )
        except Exception:
            logger.exception("Internal heartbeat execution failed")


async def run_bot(settings: Settings) -> None:
    bot = Bot(token=settings.telegram_bot_token)
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
