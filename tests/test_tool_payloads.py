from __future__ import annotations

import asyncio

from octopal.infrastructure.providers.base import Message
from octopal.runtime.capability_outcomes import CAPABILITY_OUTCOME_KEY
from octopal.runtime.tool_payloads import render_tool_result_for_llm


def test_render_tool_result_compacts_large_nested_payload() -> None:
    payload = {
        "status": "ok",
        "items": [{"id": idx, "body": "x" * 800} for idx in range(100)],
        "notes": "y" * 5_000,
    }

    rendered = render_tool_result_for_llm(payload)

    assert rendered.was_compacted is True
    assert len(rendered.text) <= 32000
    assert "[tool_result_summary type=dict" in rendered.text
    assert '"status":"ok"' in rendered.text
    assert "__octopal_compaction__" in rendered.text
    assert "truncated" in rendered.text


def test_render_tool_result_surfaces_capability_outcome() -> None:
    payload = {
        "type": "tool_unavailable",
        CAPABILITY_OUTCOME_KEY: {
            "kind": "needs_continuation",
            "next_action": "Call octo_continue_from_control_route with one concrete continuation task.",
        },
    }

    rendered = render_tool_result_for_llm(payload)

    assert "[capability_outcome kind=needs_continuation" in rendered.text
    assert "octo_continue_from_control_route" in rendered.text


def test_render_tool_result_parses_json_strings_before_compacting() -> None:
    raw = '{"items": [' + ",".join('{"value":"' + ("z" * 1000) + '"}' for _ in range(150)) + "]}"

    rendered = render_tool_result_for_llm(raw)

    assert rendered.was_compacted is True
    assert rendered.text.startswith("{")
    assert "__octopal_compaction__" in rendered.text


def test_render_tool_result_parses_small_json_string_without_counting_as_compacted() -> None:
    raw = '{"returncode":0,"stdout":"ok","stderr":""}'

    rendered = render_tool_result_for_llm(raw, tool_name="exec_run")

    assert rendered.was_compacted is False
    assert '"returncode":0' in rendered.text
    assert "__octopal_compaction__" not in rendered.text


def test_render_tool_result_preserves_raw_fs_read_json_text() -> None:
    raw = '{\n  "id": "demo_worker",\n  "name": "Demo Worker"\n}'

    rendered = render_tool_result_for_llm(raw, tool_name="fs_read")

    assert rendered.was_compacted is False
    assert rendered.text == raw.strip()
    assert "__octopal_compaction__" not in rendered.text


def test_render_tool_result_preserves_raw_manage_canon_text() -> None:
    raw = '{"decision":"keep raw canon reads as text"}'

    rendered = render_tool_result_for_llm(raw, tool_name="manage_canon")

    assert rendered.was_compacted is False
    assert rendered.text == raw
    assert "__octopal_compaction__" not in rendered.text


def test_render_tool_result_preserves_raw_drive_file_content_field() -> None:
    payload = {
        "ok": True,
        "file": {"id": "123", "name": "settings.json"},
        "content": '{"featureFlags":{"raw":true}}',
        "text_length": 31,
    }

    rendered = render_tool_result_for_llm(payload, tool_name="drive_read_text_file")

    assert rendered.was_compacted is False
    assert '"content":"{\\"featureFlags\\":{\\"raw\\":true}}"' in rendered.text
    assert "__octopal_compaction__" not in rendered.text


def test_render_tool_result_preserves_larger_fetch_snippet_for_worker_tools() -> None:
    payload = {
        "ok": True,
        "source": "basic_fetch",
        "snippet": "x" * 20_000,
    }

    rendered = render_tool_result_for_llm(payload, tool_name="web_fetch")

    assert rendered.was_compacted is False
    assert len(rendered.text) > 20_000
    assert "truncated" not in rendered.text


def test_render_tool_result_default_budget_still_compacts_large_fetch_snippet() -> None:
    payload = {
        "ok": True,
        "source": "basic_fetch",
        "snippet": "x" * 20_000,
    }

    rendered = render_tool_result_for_llm(payload)

    assert rendered.was_compacted is True
    assert "truncated" in rendered.text


def test_render_tool_result_preserves_requested_skill_guidance() -> None:
    guidance = "skill guidance\n" + ("x" * 80_000)
    payload = {
        "skill_id": "writer",
        "truncated": False,
        "guidance": guidance,
    }

    rendered = render_tool_result_for_llm(payload, tool_name="use_skill")

    assert rendered.was_compacted is False
    assert len(rendered.text) > 80_000
    assert '"truncated":false' in rendered.text
    assert "skill guidance" in rendered.text
    assert "__octopal_compaction__" not in rendered.text


def test_render_tool_result_preserves_dynamic_skill_guidance() -> None:
    payload = {
        "skill_id": "writer",
        "truncated": False,
        "guidance": "x" * 80_000,
    }

    rendered = render_tool_result_for_llm(payload, tool_name="skill_writer")

    assert rendered.was_compacted is False
    assert len(rendered.text) > 80_000
    assert "__octopal_compaction__" not in rendered.text


def test_render_tool_result_preserves_larger_mcp_thread_payload() -> None:
    payload = {
        "thread_id": "thr_123",
        "messages": [
            {
                "id": f"msg_{idx}",
                "subject": "Forward Future",
                "body": "x" * 1800,
            }
            for idx in range(12)
        ],
    }

    rendered = render_tool_result_for_llm(payload, tool_name="mcp_agentmail_get_thread")

    assert rendered.was_compacted is False
    assert len(rendered.text) > 20_000
    assert "truncated" not in rendered.text


def test_render_tool_result_preserves_more_items_for_content_heavy_mcp_lists() -> None:
    payload = {
        "threads": [
            {"id": f"thr_{idx}", "snippet": "hello world", "subject": f"Subject {idx}"}
            for idx in range(60)
        ]
    }

    rendered = render_tool_result_for_llm(payload, tool_name="mcp_agentmail_list_threads")

    assert rendered.was_compacted is False
    assert "more list items omitted" not in rendered.text


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

    class DummyOcto:
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

    async def fake_build_octo_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    def dummy_tool(args, ctx):
        return {
            "status": "ok",
            "results": [{"idx": idx, "body": "payload-" + ("x" * 800)} for idx in range(120)],
        }

    import octopal.runtime.octo.router as router
    from octopal.tools.registry import ToolSpec

    def fake_get_octo_tools(octo, chat_id):
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
            {"octo": octo, "chat_id": chat_id},
        )

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)
    monkeypatch.setattr(router, "_get_octo_tools", fake_get_octo_tools)

    async def scenario() -> None:
        provider = DummyProvider()
        response = await router.route_or_reply(
            DummyOcto(),
            provider,
            DummyMemory(),
            "check this",
            123,
            "",
        )
        assert response == "I finished the check and have the result."
        assert provider.tool_calls == 2
        assert provider.seen_tool_messages
        assert len(provider.seen_tool_messages[0]["content"]) <= 32000
        assert "[tool_result_summary type=dict" in provider.seen_tool_messages[0]["content"]
        assert "compacted" in provider.seen_tool_messages[0]["content"]

    asyncio.run(scenario())


def test_render_tool_result_includes_path_hints_for_compacted_payload() -> None:
    payload = {
        "status": "ok",
        "report_path": "reports/out.md",
        "items": [{"id": idx, "body": "x" * 1200} for idx in range(90)],
    }

    rendered = render_tool_result_for_llm(payload)

    assert rendered.was_compacted is True
    assert "[tool_result_paths reports/out.md]" in rendered.text
