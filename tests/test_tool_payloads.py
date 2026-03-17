from __future__ import annotations

import asyncio

from broodmind.infrastructure.providers.base import Message
from broodmind.runtime.tool_payloads import render_tool_result_for_llm


def test_render_tool_result_compacts_large_nested_payload() -> None:
    payload = {
        "status": "ok",
        "items": [{"id": idx, "body": "x" * 400} for idx in range(40)],
        "notes": "y" * 2_500,
    }

    rendered = render_tool_result_for_llm(payload)

    assert rendered.was_compacted is True
    assert len(rendered.text) <= 4000
    assert '"status": "ok"' in rendered.text
    assert "__broodmind_compaction__" in rendered.text
    assert "truncated" in rendered.text


def test_render_tool_result_parses_json_strings_before_compacting() -> None:
    raw = '{"items": [' + ",".join('{"value":"' + ("z" * 300) + '"}' for _ in range(30)) + "]}"

    rendered = render_tool_result_for_llm(raw)

    assert rendered.was_compacted is True
    assert rendered.text.startswith("{")
    assert "__broodmind_compaction__" in rendered.text


def test_route_compacts_tool_messages_before_next_tool_round(monkeypatch) -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.tool_calls = 0
            self.seen_tool_messages: list[dict] = []

        async def complete(self, messages, **kwargs):
            return "Final answer"

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.tool_calls += 1
            tool_messages = []
            for msg in messages:
                role = msg.get("role") if isinstance(msg, dict) else getattr(msg, "role", None)
                if str(role) == "tool":
                    tool_messages.append(msg)
            if tool_messages:
                self.seen_tool_messages = tool_messages
            if self.tool_calls == 1:
                return {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {"name": "dummy_tool", "arguments": "{}"},
                        }
                    ],
                }
            return {"content": "I finished the check and have the result.", "tool_calls": []}

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyQueen:
        store = object()
        canon = object()
        internal_progress_send = None
        is_ws_active = False

        async def set_typing(self, chat_id: int, active: bool) -> None:
            return None

        async def set_thinking(self, active: bool) -> None:
            return None

        def peek_context_wakeup(self, chat_id: int) -> str:
            return ""

    async def fake_build_queen_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    def dummy_tool(args, ctx):
        return {
            "status": "ok",
            "results": [{"idx": idx, "body": "payload-" + ("x" * 300)} for idx in range(40)],
        }

    import broodmind.runtime.queen.router as router
    from broodmind.tools.registry import ToolSpec

    def fake_get_queen_tools(queen, chat_id):
        return (
            [
                ToolSpec(
                    name="dummy_tool",
                    description="dummy",
                    parameters={"type": "object", "properties": {}},
                    permission="exec",
                    handler=dummy_tool,
                )
            ],
            {"queen": queen, "chat_id": chat_id},
        )

    monkeypatch.setattr(router, "build_queen_prompt", fake_build_queen_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)
    monkeypatch.setattr(router, "_get_queen_tools", fake_get_queen_tools)

    async def scenario() -> None:
        provider = DummyProvider()
        response = await router.route_or_reply(
            DummyQueen(),
            provider,
            DummyMemory(),
            "check this",
            123,
            "",
        )
        assert response == "I finished the check and have the result."
        assert provider.tool_calls == 2
        assert provider.seen_tool_messages
        assert len(provider.seen_tool_messages[0]["content"]) <= 4000
        assert "compacted" in provider.seen_tool_messages[0]["content"]

    asyncio.run(scenario())
