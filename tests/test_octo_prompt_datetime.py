from __future__ import annotations

import asyncio
from datetime import datetime

from octopal.runtime.octo.prompt_builder import build_control_plane_prompt, build_octo_prompt


class DummyMemory:
    async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
        return []

    async def get_recent_history(self, chat_id: int, limit: int = 20):
        return []


class DummyCanon:
    def get_tier1_context(self):
        return ""


def _datetime_messages(messages) -> list[str]:
    return [
        message.content
        for message in messages
        if isinstance(message.content, str) and message.content.startswith("Current date/time: ")
    ]


def _assert_current_datetime_message(content: str) -> None:
    prefix = "Current date/time: "
    assert content.startswith(prefix)
    parsed = datetime.fromisoformat(content.removeprefix(prefix))
    assert parsed.tzinfo is not None


def test_build_octo_prompt_includes_current_datetime_system_message() -> None:
    async def scenario() -> None:
        messages = await build_octo_prompt(
            store=object(),
            memory=DummyMemory(),
            canon=DummyCanon(),
            user_text="what happened first?",
            chat_id=123,
            bootstrap_context="",
        )

        datetime_messages = _datetime_messages(messages)
        assert len(datetime_messages) == 1
        _assert_current_datetime_message(datetime_messages[0])

    asyncio.run(scenario())


def test_build_octo_prompt_does_not_add_voice_rules_for_websocket() -> None:
    async def scenario() -> None:
        messages = await build_octo_prompt(
            store=object(),
            memory=DummyMemory(),
            canon=DummyCanon(),
            user_text="hello from desktop",
            chat_id=123,
            bootstrap_context="",
            is_ws=True,
        )

        system_text = "\n".join(
            str(message.content)
            for message in messages
            if message.role == "system" and isinstance(message.content, str)
        )
        assert "VOICE COMMUNICATION MODE" not in system_text
        assert "Voice (STT/TTS)" not in system_text

    asyncio.run(scenario())


def test_build_control_plane_prompt_includes_current_datetime_system_message() -> None:
    async def scenario() -> None:
        messages = await build_control_plane_prompt(
            user_text="scheduled tick",
            chat_id=123,
            mode_label="scheduler",
        )

        datetime_messages = _datetime_messages(messages)
        assert len(datetime_messages) == 1
        _assert_current_datetime_message(datetime_messages[0])

    asyncio.run(scenario())


def test_build_control_plane_prompt_uses_compact_system_prompt() -> None:
    async def scenario() -> None:
        messages = await build_control_plane_prompt(
            user_text="scheduled tick",
            chat_id=123,
            mode_label="scheduler",
            mode_rules="Return SCHEDULER_IDLE.",
        )

        system_text = "\n".join(
            str(message.content)
            for message in messages
            if message.role == "system" and isinstance(message.content, str)
        )

        assert len(system_text) < 2500
        assert "Runtime execution contract: scheduler." in system_text
        assert "not a user-facing capability story" in system_text
        assert "bounded operational route" not in system_text
        assert "Return SCHEDULER_IDLE." in system_text
        assert "Available worker templates" not in system_text
        assert "Canonical Memory Management" not in system_text

    asyncio.run(scenario())
