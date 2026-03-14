from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from broodmind.intents.types import ActionIntent


@dataclass
class ApprovalManager:
    bot: Bot | None
    timeout_seconds: int = 60
    _pending: dict[str, asyncio.Future] = field(default_factory=dict)

    async def request_approval(self, chat_id: int, intent: ActionIntent) -> bool:
        if self.bot is None:
            raise RuntimeError("ApprovalManager requires a bot for Telegram approvals.")
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending[intent.id] = future

        text = (
            "Approval required:\n"
            f"Intent: {intent.type}\n"
            f"Risk: {intent.risk}\n"
            f"Payload: {intent.payload}"
        )
        await self.bot.send_message(
            chat_id,
            text,
            reply_markup=_build_keyboard(intent.id),
        )
        try:
            return await asyncio.wait_for(future, timeout=self.timeout_seconds)
        except TimeoutError:
            if intent.id in self._pending:
                self._pending.pop(intent.id, None)
            return False

    def resolve(self, intent_id: str, approved: bool) -> bool:
        future = self._pending.pop(intent_id, None)
        if not future or future.done():
            return False
        future.set_result(approved)
        return True


def _build_keyboard(intent_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Approve", callback_data=f"approve:{intent_id}"),
                InlineKeyboardButton(text="Deny", callback_data=f"deny:{intent_id}"),
            ]
        ]
    )
