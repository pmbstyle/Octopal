from __future__ import annotations

import asyncio
import sys
import types

from octopal.infrastructure.config.settings import Settings
from octopal.runtime.octo.core import OctoReply
from octopal.utils import (
    extract_edge_reaction_fallback,
    extract_reaction_and_strip,
    strip_reaction_tags,
)

if "telegramify_markdown" not in sys.modules:
    sys.modules["telegramify_markdown"] = types.SimpleNamespace(markdownify=lambda text: text)

from octopal.channels.telegram import handlers as telegram_handlers
from octopal.channels.telegram.handlers import _flush_pending_turn_factory


def test_extract_reaction_and_strip_removes_tag() -> None:
    emoji, text = extract_reaction_and_strip("<react>👍</react> Hello there")
    assert emoji == "👍"
    assert text == "Hello there"


def test_extract_reaction_and_strip_handles_zero_width_noise_in_tag() -> None:
    emoji, text = extract_reaction_and_strip("<react>\u200b👍</react>")
    assert emoji == "👍"
    assert text == ""


def test_strip_reaction_tags_removes_unknown_react_markup() -> None:
    cleaned = strip_reaction_tags("Text <react>not-an-emoji</react> remains")
    assert "<react>" not in cleaned
    assert "</react>" not in cleaned
    assert "Text" in cleaned
    assert "remains" in cleaned


def test_extract_edge_reaction_fallback_handles_short_confirmation_text() -> None:
    emoji, text = extract_edge_reaction_fallback("Поставила! 👻")
    assert emoji == "👻"
    assert text == "Поставила!"


def test_telegram_uses_reply_reaction_fallback_when_immediate_loses_tag(tmp_path) -> None:
    class DummyOcto:
        async def handle_message(
            self, text: str, chat_id: int, images=None, saved_file_paths=None, **kwargs
        ):
            return OctoReply(
                immediate="Поставила! Посмотрим, появится ли 👻",
                followup=None,
                followup_required=False,
                reaction="👍",
            )

    class DummyBot:
        def __init__(self) -> None:
            self.reactions: list[tuple[int, int, str]] = []
            self.messages: list[tuple[int, str, int | None]] = []

        async def set_message_reaction(self, chat_id: int, message_id: int, reaction):
            self.reactions.append((chat_id, message_id, reaction[0].emoji))

        async def send_message(
            self, chat_id: int, text: str, parse_mode=None, reply_to_message_id=None
        ):
            self.messages.append((chat_id, text, reply_to_message_id))
            return None

    queued_messages: list[tuple[int, str, int | None]] = []

    async def fake_enqueue_send(
        bot, chat_id: int, text: str, reply_to_message_id: int | None = None
    ) -> None:
        queued_messages.append((chat_id, text, reply_to_message_id))

    original_enqueue = telegram_handlers._enqueue_send
    telegram_handlers._enqueue_send = fake_enqueue_send

    settings = Settings(
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_TELEGRAM_PARSE_MODE="MarkdownV2",
    )
    bot = DummyBot()
    flush = _flush_pending_turn_factory(DummyOcto(), settings, bot)

    try:

        async def scenario() -> None:
            await flush(
                211619002,
                "hello",
                [],
                [],
                {"reply_to_message_id": 4740},
            )

        asyncio.run(scenario())
    finally:
        telegram_handlers._enqueue_send = original_enqueue

    assert bot.reactions == [
        (211619002, 4740, "🤔"),
        (211619002, 4740, "👍"),
    ]
    assert queued_messages == [
        (211619002, "Поставила! Посмотрим, появится ли 👻", 4740),
    ]


def test_telegram_infers_reaction_from_short_text_edge_emoji(tmp_path) -> None:
    class DummyOcto:
        async def handle_message(
            self, text: str, chat_id: int, images=None, saved_file_paths=None, **kwargs
        ):
            return OctoReply(
                immediate="Поставила! 👻",
                followup=None,
                followup_required=False,
                reaction=None,
            )

    class DummyBot:
        def __init__(self) -> None:
            self.reactions: list[tuple[int, int, str]] = []
            self.messages: list[tuple[int, str, int | None]] = []

        async def set_message_reaction(self, chat_id: int, message_id: int, reaction):
            self.reactions.append((chat_id, message_id, reaction[0].emoji))

        async def send_message(
            self, chat_id: int, text: str, parse_mode=None, reply_to_message_id=None
        ):
            self.messages.append((chat_id, text, reply_to_message_id))
            return None

    queued_messages: list[tuple[int, str, int | None]] = []

    async def fake_enqueue_send(
        bot, chat_id: int, text: str, reply_to_message_id: int | None = None
    ) -> None:
        queued_messages.append((chat_id, text, reply_to_message_id))

    original_enqueue = telegram_handlers._enqueue_send
    telegram_handlers._enqueue_send = fake_enqueue_send

    settings = Settings(
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_TELEGRAM_PARSE_MODE="MarkdownV2",
    )
    bot = DummyBot()
    flush = _flush_pending_turn_factory(DummyOcto(), settings, bot)

    try:

        async def scenario() -> None:
            await flush(
                211619002,
                "hello",
                [],
                [],
                {"reply_to_message_id": 4741},
            )

        asyncio.run(scenario())
    finally:
        telegram_handlers._enqueue_send = original_enqueue

    assert bot.reactions == [
        (211619002, 4741, "🤔"),
        (211619002, 4741, "👻"),
    ]
    assert queued_messages == [
        (211619002, "Поставила!", 4741),
    ]
