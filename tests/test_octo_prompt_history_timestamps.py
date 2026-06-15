from __future__ import annotations

import asyncio

from octopal.runtime.octo.prompt_builder import build_octo_prompt


class DummyCanon:
    def get_tier1_context(self):
        return ""


def test_build_octo_prompt_adds_recent_history_timestamps_as_metadata() -> None:
    class DummyMemory:
        async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
            return []

        async def get_recent_history(self, chat_id: int, limit: int = 20):
            return [
                ("user", "start deployment", "2026-04-28T10:00:00+00:00"),
                ("assistant", "deployment started", "2026-04-28T10:01:00+00:00"),
            ]

    async def scenario() -> None:
        messages = await build_octo_prompt(
            store=object(),
            memory=DummyMemory(),
            canon=DummyCanon(),
            user_text="what happened after deployment started?",
            chat_id=123,
            bootstrap_context="",
        )

        contents = [message.content for message in messages if isinstance(message.content, str)]
        joined = "\n".join(contents)
        assert "Recent conversation message metadata:" in joined
        assert "[1] role=user sent_at=2026-04-28T10:00:00+00:00" in joined
        assert "[2] role=assistant sent_at=2026-04-28T10:01:00+00:00" in joined
        assert "not text written by the user or assistant" in joined
        assert "Do not quote or restate sent_at values" in joined
        assert "start deployment" in contents
        assert "deployment started" in contents
        assert "Sent at:" not in joined

    asyncio.run(scenario())


def test_build_octo_prompt_deduplicates_current_user_turn_with_timestamped_history() -> None:
    class DummyMemory:
        async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
            return []

        async def get_recent_history(self, chat_id: int, limit: int = 20):
            return [
                ("assistant", "I can check that.", "2026-04-28T10:01:00+00:00"),
                ("user", "check status", "2026-04-28T10:02:00+00:00"),
            ]

    async def scenario() -> None:
        messages = await build_octo_prompt(
            store=object(),
            memory=DummyMemory(),
            canon=DummyCanon(),
            user_text="check status",
            chat_id=123,
            bootstrap_context="",
        )

        contents = [message.content for message in messages if isinstance(message.content, str)]
        joined = "\n".join(contents)
        assert contents.count("check status") == 1
        assert "sent_at=2026-04-28T10:02:00+00:00" not in joined
        assert "Sent at:" not in joined

    asyncio.run(scenario())


def test_build_octo_prompt_passes_conversation_scope_and_group_context() -> None:
    class DummyMemory:
        def __init__(self) -> None:
            self.seen_scope: str | None = None

        async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
            return []

        async def get_recent_history(
            self,
            chat_id: int,
            limit: int = 20,
            *,
            conversation_scope: str | None = None,
        ):
            self.seen_scope = conversation_scope
            return []

    memory = DummyMemory()

    async def scenario() -> None:
        messages = await build_octo_prompt(
            store=object(),
            memory=memory,
            canon=DummyCanon(),
            user_text="can you see this group message?",
            chat_id=-100,
            bootstrap_context="",
            conversation_scope="default",
            channel_context={
                "source_channel": "telegram",
                "chat_kind": "group",
                "addressing_action": "respond_self",
            },
        )

        contents = [message.content for message in messages if isinstance(message.content, str)]
        joined = "\n".join(contents)
        assert memory.seen_scope == "default"
        assert "source_channel=telegram" in joined
        assert "chat_kind=group" in joined
        assert "group_addressing_action=respond_self" in joined
        assert "valid group-chat turn" in joined
        assert "normal conversation context" in joined

    asyncio.run(scenario())
