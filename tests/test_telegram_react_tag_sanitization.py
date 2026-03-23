from __future__ import annotations

import asyncio
import sys
import types

from broodmind.infrastructure.config.settings import Settings
from broodmind.runtime.queen.core import QueenReply
from broodmind.utils import extract_reaction_and_strip, strip_reaction_tags

if "telegramify_markdown" not in sys.modules:
    sys.modules["telegramify_markdown"] = types.SimpleNamespace(markdownify=lambda text: text)

from broodmind.channels.telegram.handlers import _flush_pending_turn_factory


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


def test_telegram_uses_reply_reaction_fallback_when_immediate_loses_tag(tmp_path) -> None:
    class DummyQueen:
        async def handle_message(self, text: str, chat_id: int, images=None, saved_file_paths=None):
            return QueenReply(
                immediate="Поставила! Посмотрим, появится ли 👻",
                followup=None,
                followup_required=False,
                reaction="👍",
            )

    class DummyBot:
        def __init__(self) -> None:
            self.reactions: list[tuple[int, int, str]] = []

        async def set_message_reaction(self, chat_id: int, message_id: int, reaction):
            self.reactions.append((chat_id, message_id, reaction[0].emoji))

        async def send_message(self, chat_id: int, text: str, parse_mode=None, reply_to_message_id=None):
            return None

    settings = Settings(
        BROODMIND_STATE_DIR=tmp_path / "state",
        BROODMIND_WORKSPACE_DIR=tmp_path / "workspace",
        BROODMIND_TELEGRAM_PARSE_MODE="MarkdownV2",
    )
    bot = DummyBot()
    flush = _flush_pending_turn_factory(DummyQueen(), settings, bot)

    async def scenario() -> None:
        await flush(
            211619002,
            "hello",
            [],
            [],
            {"reply_to_message_id": 4740},
        )

    asyncio.run(scenario())

    assert bot.reactions == [
        (211619002, 4740, "🤔"),
        (211619002, 4740, "👍"),
    ]
