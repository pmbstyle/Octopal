from __future__ import annotations

import asyncio

from broodmind.runtime.queen.prompt_builder import build_queen_prompt


def test_build_queen_prompt_includes_saved_image_paths_in_user_text() -> None:
    class DummyMemory:
        async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
            return []

        async def get_recent_history(self, chat_id: int, limit: int = 20):
            return []

    class DummyCanon:
        def get_tier1_context(self):
            return ""

    async def scenario() -> None:
        messages = await build_queen_prompt(
            store=object(),
            memory=DummyMemory(),
            canon=DummyCanon(),
            user_text="what is in this image?",
            chat_id=123,
            bootstrap_context="",
            images=["data:image/jpeg;base64,SGVsbG8="],
            saved_file_paths=["/tmp/telegram_images/img_test.jpg"],
        )
        user_message = messages[-1]
        assert isinstance(user_message.content, list)
        first_block = user_message.content[0]
        assert first_block["type"] == "text"
        assert "/tmp/telegram_images/img_test.jpg" in first_block["text"]
        assert "saved locally for tool-based inspection" in first_block["text"]

    asyncio.run(scenario())


def test_build_queen_prompt_includes_worker_first_guardrails() -> None:
    class DummyMemory:
        async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
            return []

        async def get_recent_history(self, chat_id: int, limit: int = 20):
            return []

    class DummyCanon:
        def get_tier1_context(self):
            return ""

    async def scenario() -> None:
        messages = await build_queen_prompt(
            store=object(),
            memory=DummyMemory(),
            canon=DummyCanon(),
            user_text="check heartbeat",
            chat_id=123,
            bootstrap_context="",
        )
        system_message = messages[0]
        assert isinstance(system_message.content, str)
        assert "Workers are the default execution unit for external work." in system_message.content
        assert "Treat direct Queen-side network or MCP access as emergency-only fallback." in system_message.content
        assert "For scheduled or network-heavy work, never lower `timeout_seconds` below the worker template default" in system_message.content
        assert "prefer a capable parent worker that can spawn child workers or use `start_workers_parallel`" in system_message.content

    asyncio.run(scenario())


def test_build_queen_prompt_includes_tool_policy_summary() -> None:
    class DummyMemory:
        async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
            return []

        async def get_recent_history(self, chat_id: int, limit: int = 20):
            return []

    class DummyCanon:
        def get_tier1_context(self):
            return ""

    async def scenario() -> None:
        messages = await build_queen_prompt(
            store=object(),
            memory=DummyMemory(),
            canon=DummyCanon(),
            user_text="inspect tools",
            chat_id=123,
            bootstrap_context="",
            tool_policy_summary=(
                "Tool policy contract:\n"
                "- Use safe tools by default.\n"
                "- If a tool is blocked by policy, do not repeat the same call."
            ),
        )
        contents = [str(msg.content) for msg in messages if isinstance(msg.content, str)]
        merged = "\n".join(contents)
        assert "Tool policy contract:" in merged
        assert "Use safe tools by default." in merged
        assert "do not repeat the same call" in merged

    asyncio.run(scenario())
