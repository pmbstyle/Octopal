from __future__ import annotations

import asyncio

from octopal.runtime.octo.prompt_builder import build_octo_prompt


def test_build_octo_prompt_includes_saved_image_paths_in_user_text() -> None:
    class DummyMemory:
        async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
            return []

        async def get_recent_history(self, chat_id: int, limit: int = 20):
            return []

    class DummyCanon:
        def get_tier1_context(self):
            return ""

    async def scenario() -> None:
        messages = await build_octo_prompt(
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
        assert "also saved locally for continuity" in first_block["text"]
        assert "If your current model can inspect image inputs" in first_block["text"]

    asyncio.run(scenario())


def test_build_octo_prompt_includes_saved_file_paths_without_images() -> None:
    class DummyMemory:
        async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
            return []

        async def get_recent_history(self, chat_id: int, limit: int = 20):
            return []

    class DummyCanon:
        def get_tier1_context(self):
            return ""

    async def scenario() -> None:
        messages = await build_octo_prompt(
            store=object(),
            memory=DummyMemory(),
            canon=DummyCanon(),
            user_text="please inspect this file",
            chat_id=123,
            bootstrap_context="",
            images=[],
            saved_file_paths=["/tmp/uploads/report.pdf"],
        )
        user_message = messages[-1]
        assert isinstance(user_message.content, str)
        assert "/tmp/uploads/report.pdf" in user_message.content
        assert "Files received and saved locally" in user_message.content

    asyncio.run(scenario())


def test_build_octo_prompt_includes_worker_first_guardrails() -> None:
    class DummyMemory:
        async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
            return []

        async def get_recent_history(self, chat_id: int, limit: int = 20):
            return []

    class DummyCanon:
        def get_tier1_context(self):
            return ""

    async def scenario() -> None:
        messages = await build_octo_prompt(
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
        assert "Treat direct Octo-side network or MCP access as emergency-only fallback." in system_message.content
        assert "For scheduled or network-heavy work, never lower `timeout_seconds` below the worker template default" in system_message.content
        assert "prefer a capable parent worker that can spawn child workers or use `start_workers_parallel`" in system_message.content

    asyncio.run(scenario())


def test_build_octo_prompt_includes_tool_policy_summary() -> None:
    class DummyMemory:
        async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
            return []

        async def get_recent_history(self, chat_id: int, limit: int = 20):
            return []

    class DummyCanon:
        def get_tier1_context(self):
            return ""

    async def scenario() -> None:
        messages = await build_octo_prompt(
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


def test_build_octo_prompt_uses_facets_aware_memory_getter_when_available() -> None:
    class DummyMemory:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[str] | None]] = []

        async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
            raise AssertionError("facets-aware getter should be preferred when available")

        async def get_context_by_facets(
            self,
            user_text: str,
            *,
            exclude_chat_id: int | None = None,
            memory_facets: list[str] | None = None,
        ):
            self.calls.append((user_text, memory_facets))
            return ["assistant: We decided to use uv."]

        async def get_recent_history(self, chat_id: int, limit: int = 20):
            return []

    class DummyCanon:
        def get_tier1_context(self):
            return ""

    memory = DummyMemory()

    async def scenario() -> None:
        messages = await build_octo_prompt(
            store=object(),
            memory=memory,
            canon=DummyCanon(),
            user_text="why did we decide to use uv?",
            chat_id=123,
            bootstrap_context="",
        )
        contents = [str(msg.content) for msg in messages if isinstance(msg.content, str)]
        merged = "\n".join(contents)
        assert "We decided to use uv." in merged

    asyncio.run(scenario())
    assert memory.calls == [("why did we decide to use uv?", ["decision"])]


def test_build_octo_prompt_includes_compact_facts_context_when_available() -> None:
    class DummyMemory:
        async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
            return []

        async def get_context_by_facets(
            self,
            user_text: str,
            *,
            exclude_chat_id: int | None = None,
            memory_facets: list[str] | None = None,
        ):
            return []

        async def get_recent_history(self, chat_id: int, limit: int = 20):
            return []

    class DummyCanon:
        def get_tier1_context(self):
            return ""

    class DummyFacts:
        def get_relevant_facts(self, query: str, *, memory_facets: list[str] | None = None, limit: int = 3):
            return ["primary installer is uv (decisions.md)"]

    async def scenario() -> None:
        messages = await build_octo_prompt(
            store=object(),
            memory=DummyMemory(),
            canon=DummyCanon(),
            user_text="what did we decide about installer?",
            chat_id=123,
            bootstrap_context="",
            facts=DummyFacts(),
        )
        merged = "\n".join(str(message.content) for message in messages if isinstance(message.content, str))
        assert "<facts>" in merged
        assert "primary installer is uv" in merged

    asyncio.run(scenario())
