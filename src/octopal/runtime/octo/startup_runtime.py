from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import structlog

from octopal.runtime.octo.context_reset import (
    build_restart_resume_message as _build_restart_resume_message,
)
from octopal.runtime.octo.router import (
    route_internal_maintenance as _default_route_internal_maintenance,
)
from octopal.runtime.octo.scheduler_helpers import _coerce_positive_chat_id
from octopal.runtime.self_control import (
    mark_restart_resume_consumed,
    read_pending_restart_resume,
)
from octopal.utils import should_suppress_user_delivery

logger = structlog.get_logger(__name__)


def _core_callable(name: str, default: Callable[..., Any]) -> Callable[..., Any]:
    core_module = sys.modules.get("octopal.runtime.octo.core")
    if core_module is not None:
        candidate = getattr(core_module, name, None)
        if callable(candidate):
            return candidate
    return default


class OctoStartupRuntimeMixin:
    async def initialize_system(self, bot=None, allowed_chat_ids: list[int] | None = None) -> None:
        system_chat_id = 0
        logger.info("Octo waking up")
        self.start_background_tasks()

        # Load and connect MCP servers
        if self.mcp_manager:
            await self.mcp_manager.load_and_connect_all()

        # Load and start connectors
        if self.connector_manager:
            await self.connector_manager.load_and_start_all()

        restart_resume = None
        runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
        if runtime_settings is not None:
            restart_resume = await asyncio.to_thread(
                read_pending_restart_resume,
                Path(runtime_settings.state_dir),
            )
            if restart_resume and restart_resume.get("consumed_at"):
                restart_resume = None

        wake_up_prompt = (
            "You are waking up. Inspect runtime health and available workers internally. "
            "Use only bounded control-plane tools if needed, but never output a tool name or tool syntax as your final answer. "
            "Then produce a short friendly startup status message for the user in plain language."
        )
        if restart_resume:
            wake_up_prompt += "\n\n" + _build_restart_resume_message(restart_resume)
        original_send = self.internal_send
        chat_ids = [
            chat_id
            for item in (allowed_chat_ids or [])
            if (chat_id := _coerce_positive_chat_id(item)) is not None
        ]
        self._scheduled_delivery_chat_ids = list(chat_ids)
        if chat_ids and (bot or callable(original_send)):
            logger.info("Octo will send initialization message", count=len(chat_ids))
            logger.debug("Allowed chat_ids", chat_ids=chat_ids)

            async def send_to_allowed_chats(chat_id, text):
                for target_chat_id in chat_ids:
                    try:
                        if callable(original_send):
                            # Reuse the active channel send pipeline when one is attached.
                            await original_send(target_chat_id, text)
                        else:
                            await bot.send_message(chat_id=target_chat_id, text=text)
                        logger.debug("Sent initialization message", chat_id=target_chat_id)
                    except Exception as e:
                        logger.warning("Failed to send to chat_id", chat_id=target_chat_id, error=e)

            self.internal_send = send_to_allowed_chats
        else:
            logger.warning(
                "No allowed user channel recipients configured; octo will not send ready message."
            )
            self.internal_send = None
        try:
            result = await _core_callable(
                "route_internal_maintenance",
                _default_route_internal_maintenance,
            )(
                self,
                system_chat_id,
                wake_up_prompt,
            )
            if should_suppress_user_delivery(result):
                result = "Octo is online. Initialization is complete and I am ready for your tasks."
            logger.info(
                "Octo wake up complete",
                result_preview=f"{result[:60]}..." if result else "empty",
            )

            # Send the Octo's own response to allowed chats if configured.
            if result and self.internal_send and chat_ids:
                try:
                    await self.internal_send(system_chat_id, result)
                    logger.info("Octo initialization response sent")
                except Exception as e:
                    logger.warning("Failed to send octo initialization response", error=e)
            if restart_resume and runtime_settings is not None:
                await asyncio.to_thread(
                    mark_restart_resume_consumed,
                    Path(runtime_settings.state_dir),
                )
        except Exception:
            logger.exception("Octo failed to complete wake-up task")
        finally:
            self.internal_send = original_send
