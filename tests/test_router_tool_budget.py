from __future__ import annotations

import asyncio

from broodmind.infrastructure.providers.base import Message
from broodmind.runtime.queen.router import (
    _budget_tool_specs,
    _finalize_response,
    _recover_textual_tool_call,
    _sanitize_messages_for_complete,
    _shrink_tool_specs_for_retry,
)
from broodmind.tools.registry import ToolSpec
from broodmind.tools.tools import get_tools


def test_budget_keeps_internal_worker_and_scheduler_tools() -> None:
    all_tools = get_tools(mcp_manager=None)
    budgeted = _budget_tool_specs(all_tools, max_count=8)
    names = {spec.name for spec in budgeted}

    must_keep = {
        "check_schedule",
        "start_worker",
        "get_worker_status",
        "get_worker_result",
        "list_workers",
        "list_active_workers",
        "schedule_task",
    }
    assert must_keep.issubset(names)


def test_shrink_retry_keeps_start_worker() -> None:
    all_tools = get_tools(mcp_manager=None)
    shrunk = _shrink_tool_specs_for_retry(all_tools)
    names = {spec.name for spec in shrunk}
    assert "start_worker" in names


def test_route_falls_back_when_tool_run_ends_with_empty_response(monkeypatch) -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.tool_calls = 0

        async def complete(self, messages, **kwargs):
            return "I checked it and I'm still working through the result."

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.tool_calls += 1
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
            return {"content": "", "tool_calls": []}

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyQueen:
        store = object()
        canon = object()
        internal_progress_send = None
        is_ws_active = False

        def __init__(self) -> None:
            self.thinking_states: list[bool] = []
            self.typing_states: list[tuple[int, bool]] = []

        async def set_typing(self, chat_id: int, active: bool) -> None:
            self.typing_states.append((chat_id, active))

        async def set_thinking(self, active: bool) -> None:
            self.thinking_states.append(active)

        def peek_context_wakeup(self, chat_id: int) -> str:
            return ""

    async def fake_build_queen_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_plan(provider, messages, has_tools):
        return {"mode": "execute", "steps": ["run dummy tool"], "response": ""}

    def dummy_tool(args, ctx):
        return {"ok": True}

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

    import broodmind.runtime.queen.router as router

    monkeypatch.setattr(router, "build_queen_prompt", fake_build_queen_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)
    monkeypatch.setattr(router, "_get_queen_tools", fake_get_queen_tools)

    async def scenario() -> None:
        provider = DummyProvider()
        queen = DummyQueen()
        response = await router.route_or_reply(
            queen,
            provider,
            DummyMemory(),
            "check this",
            123,
            "",
        )
        assert response == "I checked it and I'm still working through the result."
        assert provider.tool_calls == 2

    asyncio.run(scenario())


def test_route_retries_image_message_with_saved_file_paths(monkeypatch, tmp_path) -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.tool_calls = 0
            self.last_retry_messages = None

        async def complete(self, messages, **kwargs):
            return "I could not use tools, but I preserved the image locally and explained the limitation."

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.tool_calls += 1
            if self.tool_calls == 1:
                raise RuntimeError("OpenAIException - Invalid API parameter. {'error': {'code': '1210'}}")
            self.last_retry_messages = messages
            return {"content": "I inspected the saved image path via tools.", "tool_calls": []}

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
        return [
            Message(
                role="user",
                content=[
                    {"type": "text", "text": str(kwargs["user_text"])},
                    {"type": "image_url", "image_url": {"url": kwargs["images"][0]}},
                ],
            )
        ]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    import broodmind.runtime.queen.router as router

    monkeypatch.setattr(router, "build_queen_prompt", fake_build_queen_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)
    monkeypatch.setenv("BROODMIND_WORKSPACE_DIR", str(tmp_path))

    async def scenario() -> None:
        provider = DummyProvider()
        response = await router.route_or_reply(
            DummyQueen(),
            provider,
            DummyMemory(),
            "what is in this image?",
            123,
            "",
            images=["data:image/jpeg;base64,SGVsbG8="],
        )
        assert response == "I inspected the saved image path via tools."
        assert provider.tool_calls == 2
        assert provider.last_retry_messages is not None
        last_message = provider.last_retry_messages[-1]
        assert last_message["role"] == "user"
        assert "saved locally for tool-based inspection" in last_message["content"]
        assert str(tmp_path) in last_message["content"]

    asyncio.run(scenario())


def test_route_retries_with_fewer_tools_after_invalid_tool_payload(monkeypatch) -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.tool_counts: list[int] = []

        async def complete(self, messages, **kwargs):
            return "Fallback text should not be used."

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.tool_counts.append(len(tools))
            if len(tools) > 12:
                raise RuntimeError("OpenAIException - Invalid API parameter. {'error': {'code': '1210'}}")
            return {"content": "Recovered after shrinking tool set.", "tool_calls": []}

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
        tools = [
            ToolSpec(
                name=f"dummy_tool_{idx}",
                description="dummy",
                parameters={"type": "object", "properties": {}},
                permission="exec",
                handler=lambda args, ctx: {"ok": True},
            )
            for idx in range(20)
        ]
        return tools, {"queen": queen, "chat_id": chat_id}

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
        assert response == "Recovered after shrinking tool set."
        assert provider.tool_counts[:2] == [20, 14]
        assert provider.tool_counts[-1] == 12

    asyncio.run(scenario())


def test_route_passes_saved_file_paths_into_prompt(monkeypatch) -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            return "Looks good."

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

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

    captured_kwargs = {}

    async def fake_build_queen_prompt(**kwargs):
        captured_kwargs.update(kwargs)
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    import broodmind.runtime.queen.router as router

    monkeypatch.setattr(router, "build_queen_prompt", fake_build_queen_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)

    async def scenario() -> None:
        response = await router.route_or_reply(
            DummyQueen(),
            DummyProvider(),
            DummyMemory(),
            "what is in this image?",
            123,
            "",
            images=["data:image/jpeg;base64,SGVsbG8="],
            saved_file_paths=["/tmp/telegram_images/img_test.jpg"],
        )
        assert response == "Looks good."
        assert captured_kwargs["saved_file_paths"] == ["/tmp/telegram_images/img_test.jpg"]

    asyncio.run(scenario())


def test_plain_completion_does_not_stream_for_telegram(monkeypatch) -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            return "Final reply"

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("telegram path should not use streaming partials")

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyQueen:
        store = object()
        canon = object()
        is_ws_active = False
        internal_progress_send = object()

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

    import broodmind.runtime.queen.router as router

    monkeypatch.setattr(router, "build_queen_prompt", fake_build_queen_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)

    async def scenario() -> None:
        response = await router.route_or_reply(
            DummyQueen(),
            DummyProvider(),
            DummyMemory(),
            "hello",
            123,
            "",
        )
        assert response == "Final reply"

    asyncio.run(scenario())


def test_plain_completion_can_stream_for_websocket(monkeypatch) -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            raise AssertionError("websocket path should prefer streaming partials")

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            await on_partial("partial text")
            return "Final reply"

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyQueen:
        store = object()
        canon = object()
        is_ws_active = True

        def __init__(self) -> None:
            self.progress: list[tuple[str, str]] = []

        async def set_typing(self, chat_id: int, active: bool) -> None:
            return None

        async def set_thinking(self, active: bool) -> None:
            return None

        def peek_context_wakeup(self, chat_id: int) -> str:
            return ""

        async def internal_progress_send(self, chat_id: int, state: str, text: str, meta: dict) -> None:
            self.progress.append((state, text))

    async def fake_build_queen_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    import broodmind.runtime.queen.router as router

    monkeypatch.setattr(router, "build_queen_prompt", fake_build_queen_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)

    async def scenario() -> None:
        queen = DummyQueen()
        response = await router.route_or_reply(
            queen,
            DummyProvider(),
            DummyMemory(),
            "hello",
            123,
            "",
        )
        assert response == "Final reply"
        assert queen.progress == [("partial", "partial text")]

    asyncio.run(scenario())


def test_recover_textual_tool_name_without_args() -> None:
    spec = ToolSpec(
        name="check_schedule",
        description="check schedule",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
        permission="self_control",
        handler=lambda args, ctx: {"ok": True},
    )

    recovered = _recover_textual_tool_call("check_schedule", [spec])
    assert recovered is not None
    assert recovered["function"]["name"] == "check_schedule"
    assert recovered["function"]["arguments"] == "{}"


def test_recover_textual_tool_preview_with_file_alias() -> None:
    spec = ToolSpec(
        name="fs_read",
        description="read file",
        parameters={
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
            "additionalProperties": False,
        },
        permission="filesystem_read",
        handler=lambda args, ctx: {"ok": True},
    )

    recovered = _recover_textual_tool_call("fs_read, file: memory/2026-03-11.md", [spec])
    assert recovered is not None
    assert recovered["function"]["name"] == "fs_read"
    assert recovered["function"]["arguments"] == '{"path": "memory/2026-03-11.md"}'


def test_do_not_recover_human_text_wrapped_around_tool_name() -> None:
    spec = ToolSpec(
        name="check_schedule",
        description="check schedule",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
        permission="self_control",
        handler=lambda args, ctx: {"ok": True},
    )

    assert _recover_textual_tool_call("Checking schedule... check_schedule", [spec]) is None


def test_sanitize_messages_keeps_tool_results_for_plain_fallback() -> None:
    sanitized = _sanitize_messages_for_complete(
        [
            {"role": "system", "content": "Use tool results."},
            {"role": "assistant", "content": "", "tool_calls": [{"id": "call-1"}]},
            {"role": "tool", "name": "check_schedule", "content": '{"status":"ok","tasks":[]}'},
        ]
    )

    tool_summary = next(msg for msg in sanitized if msg["role"] == "assistant" and "Tool result" in msg["content"])
    assert "Tool result (check_schedule)" in tool_summary["content"]
    assert '"status": "ok"' in tool_summary["content"]


def test_finalize_response_rewrites_bare_tool_name() -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            assert any("collapsed into a tool invocation" in str(m.get("content", "")) for m in messages)
            return "I checked the worker list and the system is ready."

    async def scenario() -> None:
        result = await _finalize_response(
            DummyProvider(),
            [Message(role="system", content="Rewrite if needed.")],
            "list_workers",
            internal_followup=False,
        )
        assert result == "I checked the worker list and the system is ready."

    asyncio.run(scenario())


def test_finalize_response_returns_no_user_response_when_rewrite_still_bad() -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            return "check_schedule"

    async def scenario() -> None:
        result = await _finalize_response(
            DummyProvider(),
            [Message(role="system", content="Rewrite if needed.")],
            "list_workers",
            internal_followup=True,
        )
        assert result == "NO_USER_RESPONSE"

    asyncio.run(scenario())


def test_finalize_response_preserves_control_token_without_rewrite() -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            raise AssertionError("control token should not trigger rewrite")

    async def scenario() -> None:
        result = await _finalize_response(
            DummyProvider(),
            [Message(role="system", content="Rewrite if needed.")],
            "NO_USER_RESPONSE",
            internal_followup=True,
        )
        assert result == "NO_USER_RESPONSE"

    asyncio.run(scenario())
