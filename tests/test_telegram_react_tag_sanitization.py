from __future__ import annotations

import asyncio
import sys
import types
from types import SimpleNamespace

from aiogram.filters import CommandObject

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
    emoji, text = extract_edge_reaction_fallback("Set it! 👻")
    assert emoji == "👻"
    assert text == "Set it!"


def test_telegram_uses_reply_reaction_fallback_when_immediate_loses_tag(tmp_path) -> None:
    class DummyOcto:
        async def handle_message(
            self, text: str, chat_id: int, images=None, saved_file_paths=None, **kwargs
        ):
            return OctoReply(
                immediate="Set it! Let us see if it appears 👻",
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
        (211619002, "Set it! Let us see if it appears 👻", 4740),
    ]


def test_telegram_infers_reaction_from_short_text_edge_emoji(tmp_path) -> None:
    class DummyOcto:
        async def handle_message(
            self, text: str, chat_id: int, images=None, saved_file_paths=None, **kwargs
        ):
            return OctoReply(
                immediate="Set it! 👻",
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
        (211619002, "Set it!", 4741),
    ]


def test_telegram_group_message_ignored_before_reaction_or_octo_call(tmp_path) -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            return '{"action":"ignore","confidence":0.97,"reason":"addressed to another agent"}'

    class DummyMemory:
        def __init__(self) -> None:
            self.messages: list[tuple[str, str, dict]] = []

        async def add_message(self, role: str, content: str, metadata: dict) -> None:
            self.messages.append((role, content, metadata))

    class DummyOcto:
        provider = DummyProvider()

        def __init__(self) -> None:
            self.calls = 0
            self.memory = DummyMemory()

        async def handle_message(
            self, text: str, chat_id: int, images=None, saved_file_paths=None, **kwargs
        ):
            self.calls += 1
            return OctoReply(
                immediate="should not happen",
                followup=None,
                followup_required=False,
                reaction=None,
            )

    class DummyBot:
        def __init__(self) -> None:
            self.reactions: list[tuple[int, int, str]] = []

        async def set_message_reaction(self, chat_id: int, message_id: int, reaction):
            self.reactions.append((chat_id, message_id, reaction[0].emoji))

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
    octo = DummyOcto()
    bot = DummyBot()
    flush = _flush_pending_turn_factory(octo, settings, bot)

    try:

        async def scenario() -> None:
            await flush(
                -100211619002,
                "Bob, what is the status?",
                [],
                [],
                {
                    "reply_to_message_id": 4742,
                    "is_group_chat": True,
                    "reply_to_agent": False,
                    "sender_label": "Slava",
                },
            )

        asyncio.run(scenario())
    finally:
        telegram_handlers._enqueue_send = original_enqueue

    assert octo.calls == 0
    assert bot.reactions == []
    assert queued_messages == []
    assert len(octo.memory.messages) == 1
    role, content, metadata = octo.memory.messages[0]
    assert role == "system"
    assert "Observed group-chat message." in content
    assert "Bob, what is the status?" in content
    assert metadata["passive_group_observation"] is True
    assert metadata["conversation_scope"] == "default"
    assert metadata["chat_kind"] == "group"
    assert metadata["addressing_reason"] == "addressed to another agent"


def test_telegram_plain_group_command_is_gated(tmp_path) -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            return '{"action":"ignore","confidence":0.98,"reason":"ambient command"}'

    message = SimpleNamespace(
        chat=SimpleNamespace(id=-100211619002, type="supergroup"),
        text="/status",
        caption=None,
        from_user=SimpleNamespace(full_name="Slava", username="slava"),
        reply_to_message=None,
    )
    octo = SimpleNamespace(provider=DummyProvider())
    settings = Settings(
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
    )
    bot = SimpleNamespace(id=123456, username="AliceBot")

    async def scenario() -> bool:
        return await telegram_handlers._telegram_group_command_should_run(
            message,
            command=CommandObject(command="status"),
            settings=settings,
            octo=octo,
            bot=bot,
        )

    assert asyncio.run(scenario()) is False


def test_telegram_targeted_group_command_skips_provider_gate(tmp_path) -> None:
    class FailingProvider:
        async def complete(self, messages, **kwargs):
            raise AssertionError("targeted command should not need provider classification")

    message = SimpleNamespace(
        chat=SimpleNamespace(id=-100211619002, type="supergroup"),
        text="/status@AliceBot",
        caption=None,
        from_user=SimpleNamespace(full_name="Slava", username="slava"),
        reply_to_message=None,
    )
    octo = SimpleNamespace(provider=FailingProvider())
    settings = Settings(
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
    )
    bot = SimpleNamespace(id=123456, username="AliceBot")

    async def scenario() -> bool:
        return await telegram_handlers._telegram_group_command_should_run(
            message,
            command=CommandObject(command="status", mention="AliceBot"),
            settings=settings,
            octo=octo,
            bot=bot,
        )

    assert asyncio.run(scenario()) is True


def test_telegram_reply_to_unknown_bot_does_not_bypass_group_gate(tmp_path) -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            return '{"action":"ignore","confidence":0.98,"reason":"other bot"}'

    message = SimpleNamespace(
        chat=SimpleNamespace(id=-100211619002, type="supergroup"),
        text="/status",
        caption=None,
        from_user=SimpleNamespace(full_name="Slava", username="slava"),
        reply_to_message=SimpleNamespace(
            from_user=SimpleNamespace(is_bot=True, id=999999, username="OtherBot")
        ),
    )
    octo = SimpleNamespace(provider=DummyProvider())
    settings = Settings(
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
    )
    bot = SimpleNamespace(id=123456, username="AliceBot")

    async def scenario() -> bool:
        return await telegram_handlers._telegram_group_command_should_run(
            message,
            command=CommandObject(command="status"),
            settings=settings,
            octo=octo,
            bot=bot,
        )

    assert asyncio.run(scenario()) is False
