from __future__ import annotations

import asyncio

from broodmind.infrastructure.providers.base import Message


def test_route_includes_policy_block_result_for_blocked_tool_call(monkeypatch) -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.calls = 0
            self.seen_policy_block = False

        async def complete(self, messages, **kwargs):
            return "The risky tool is blocked, so I will use a safer path."

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {"name": "exec_run", "arguments": "{}"},
                        }
                    ],
                }
            tool_messages = []
            for msg in messages:
                if isinstance(msg, dict):
                    role = msg.get("role")
                    content = msg.get("content", "")
                else:
                    role = getattr(msg, "role", None)
                    content = getattr(msg, "content", "")
                if role == "tool":
                    tool_messages.append(content)
            self.seen_policy_block = any("policy_block" in str(content) for content in tool_messages)
            return {"content": "The risky tool is blocked, so I will use a safer path.", "tool_calls": []}

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

    def fake_get_queen_tools(queen, chat_id):
        from broodmind.tools.diagnostics import resolve_tool_diagnostics
        from broodmind.tools.registry import ToolSpec

        safe_tool = ToolSpec(
            name="web_search",
            description="search",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="network",
            handler=lambda args, ctx: {"ok": True},
        )
        blocked_tool = ToolSpec(
            name="exec_run",
            description="exec",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="exec",
            handler=lambda args, ctx: {"ok": True},
        )
        report = resolve_tool_diagnostics(
            [safe_tool, blocked_tool],
            permissions={"network": True, "exec": False},
        )
        return [safe_tool], {"queen": queen, "chat_id": chat_id, "tool_resolution_report": report}

    import broodmind.runtime.queen.router as router

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
        assert response == "The risky tool is blocked, so I will use a safer path."
        assert provider.seen_policy_block is True

    asyncio.run(scenario())
