from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.providers import codex_provider
from octopal.infrastructure.providers.base import Message
from octopal.infrastructure.providers.codex_provider import CodexAppServerError, CodexProvider
from octopal.runtime.octo.router import _complete_route_with_tools, route_or_reply
from octopal.tools.registry import ToolSpec


class _FakeCodexClient:
    instances: list[_FakeCodexClient] = []
    next_thread = 1
    fail_resume_once = False
    fail_turn = False
    active_turns = 0
    max_active_turns = 0

    def __init__(self, command: str, args: list[str], env: dict[str, str]) -> None:
        del command, args, env
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.thread_id = ""
        self.turn_event_index = 0
        self.turn_active = False
        type(self).instances.append(self)

    async def start(self) -> None:
        return None

    async def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float,
    ) -> Any:
        del timeout
        payload = dict(params or {})
        self.calls.append((method, payload))
        if method == "thread/start":
            self.thread_id = f"thread-{type(self).next_thread}"
            type(self).next_thread += 1
            return {"thread": {"id": self.thread_id}}
        if method == "thread/resume":
            if type(self).fail_resume_once:
                type(self).fail_resume_once = False
                raise CodexAppServerError("missing thread")
            self.thread_id = str(payload["threadId"])
            return {"thread": {"id": self.thread_id}}
        if method == "turn/start":
            self.turn_event_index = 0
            self.turn_active = True
            type(self).active_turns += 1
            type(self).max_active_turns = max(type(self).max_active_turns, type(self).active_turns)
            return {"turn": {"id": f"turn-{len(type(self).instances)}"}}
        raise AssertionError(f"unexpected request: {method}")

    async def next_event(self, timeout: float) -> tuple[str, dict[str, Any]]:
        del timeout
        await asyncio.sleep(0.01)
        if type(self).fail_turn:
            self._finish_turn()
            raise CodexAppServerError("turn failed")
        self.turn_event_index += 1
        if self.turn_event_index == 1:
            return (
                "notification",
                {
                    "method": "item/agentMessage/delta",
                    "params": {"threadId": self.thread_id, "delta": "ok"},
                },
            )
        self._finish_turn()
        return (
            "notification",
            {"method": "turn/completed", "params": {"threadId": self.thread_id}},
        )

    async def close(self) -> None:
        self._finish_turn()

    def _finish_turn(self) -> None:
        if self.turn_active:
            self.turn_active = False
            type(self).active_turns -= 1


@pytest.fixture(autouse=True)
def _fake_codex_client(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeCodexClient.instances = []
    _FakeCodexClient.next_thread = 1
    _FakeCodexClient.fail_resume_once = False
    _FakeCodexClient.fail_turn = False
    _FakeCodexClient.active_turns = 0
    _FakeCodexClient.max_active_turns = 0
    monkeypatch.setattr(codex_provider, "_CodexAppServerClient", _FakeCodexClient)


def _settings(state_dir: Path, *, model: str = "gpt-5.4") -> Settings:
    return Settings(
        OCTOPAL_STATE_DIR=state_dir,
        OCTOPAL_LITELLM_PROVIDER_ID="codex",
        OCTOPAL_LITELLM_MODEL=model,
    )


def _tool(name: str = "lookup") -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": name,
            "parameters": {"type": "object", "properties": {}},
        },
    }


def _call(client: _FakeCodexClient, method: str) -> dict[str, Any]:
    return next(payload for candidate, payload in client.calls if candidate == method)


def test_persistent_session_resumes_after_provider_restart_with_only_new_input(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        first = CodexProvider(_settings(tmp_path))
        await first.complete_with_tools(
            [
                {"role": "system", "content": "developer and memory context"},
                {"role": "user", "content": "first request"},
            ],
            tools=[_tool()],
            codex_session_key="telegram:primary:101",
        )

        created = _FakeCodexClient.instances[0]
        assert _call(created, "thread/start")["ephemeral"] is False
        assert _call(created, "thread/start")["developerInstructions"] == (
            "developer and memory context"
        )

        restarted = CodexProvider(_settings(tmp_path))
        await restarted.complete_with_tools(
            [
                {"role": "system", "content": "repacked memory must not be resent"},
                {"role": "user", "content": "first request"},
                {"role": "assistant", "content": "first answer"},
                {"role": "user", "content": "second request"},
            ],
            tools=[_tool()],
            codex_session_key="telegram:primary:101",
        )

        resumed = _FakeCodexClient.instances[1]
        assert _call(resumed, "thread/resume")["threadId"] == "thread-1"
        assert "developerInstructions" not in _call(resumed, "thread/resume")
        assert _call(resumed, "turn/start")["input"] == [
            {"type": "text", "text": "USER:\nsecond request"}
        ]

        registry = json.loads((tmp_path / "codex_sessions.json").read_text())
        assert len(registry["sessions"]) == 1
        assert "telegram:primary:101" not in json.dumps(registry)

    asyncio.run(scenario())


def test_tool_catalog_change_starts_a_fresh_persistent_thread(tmp_path: Path) -> None:
    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        messages = [{"role": "user", "content": "request"}]
        await provider.complete_with_tools(
            messages,
            tools=[_tool("lookup")],
            codex_session_key="telegram:primary:102",
        )
        await provider.complete_with_tools(
            messages,
            tools=[_tool("write")],
            codex_session_key="telegram:primary:102",
        )

        second = _FakeCodexClient.instances[1]
        assert [method for method, _ in second.calls] == ["thread/start", "turn/start"]
        assert _call(second, "thread/start")["ephemeral"] is False
        assert _call(second, "thread/start")["dynamicTools"][0]["name"] == "write"

    asyncio.run(scenario())


def test_warm_tool_loop_sends_only_the_appended_tool_result(tmp_path: Path) -> None:
    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        initial_messages = [
            {"role": "system", "content": "large initial context"},
            {"role": "user", "content": "look this up"},
        ]
        await provider.complete_with_tools(
            initial_messages,
            tools=[_tool()],
            codex_session_key="telegram:default:108",
        )
        await provider.complete_with_tools(
            [
                *initial_messages,
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {"name": "lookup", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "name": "lookup",
                    "tool_call_id": "call-1",
                    "content": "fresh tool result",
                },
            ],
            tools=[_tool()],
            codex_session_key="telegram:default:108",
        )

        resumed = _FakeCodexClient.instances[1]
        assert _call(resumed, "turn/start")["input"] == [
            {"type": "text", "text": "TOOL:\nfresh tool result"}
        ]

    asyncio.run(scenario())


def test_mismatched_prompt_prefix_keeps_terminal_tool_result(tmp_path: Path) -> None:
    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        await provider.complete_with_tools(
            [
                {"role": "system", "content": "initial context"},
                {"role": "user", "content": "look this up"},
            ],
            tools=[_tool()],
            codex_session_key="telegram:default:109",
        )
        await provider.complete_with_tools(
            [
                {"role": "system", "content": "changed packed context"},
                {"role": "user", "content": "look this up"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {"name": "lookup", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "name": "lookup",
                    "tool_call_id": "call-1",
                    "content": "terminal tool result",
                },
            ],
            tools=[_tool()],
            codex_session_key="telegram:default:109",
        )

        resumed = _FakeCodexClient.instances[1]
        assert _call(resumed, "thread/resume")["threadId"] == "thread-1"
        assert _call(resumed, "turn/start")["input"] == [
            {
                "type": "text",
                "text": "USER:\nlook this up\n\nTOOL:\nterminal tool result",
            }
        ]

    asyncio.run(scenario())


def test_resume_failure_falls_back_to_full_context_and_replaces_mapping(tmp_path: Path) -> None:
    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        await provider.complete_with_tools(
            [{"role": "user", "content": "first"}],
            tools=[_tool()],
            codex_session_key="desktop:primary:103",
        )
        _FakeCodexClient.fail_resume_once = True
        await provider.complete_with_tools(
            [
                {"role": "system", "content": "full current context"},
                {"role": "user", "content": "second"},
            ],
            tools=[_tool()],
            codex_session_key="desktop:primary:103",
        )

        fallback = _FakeCodexClient.instances[1]
        assert [method for method, _ in fallback.calls] == [
            "thread/resume",
            "thread/start",
            "turn/start",
        ]
        assert _call(fallback, "thread/start")["developerInstructions"] == ("full current context")
        registry = json.loads((tmp_path / "codex_sessions.json").read_text())
        assert next(iter(registry["sessions"].values()))["thread_id"] == "thread-2"

    asyncio.run(scenario())


def test_concurrent_turns_for_one_session_are_serialized(tmp_path: Path) -> None:
    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        await asyncio.gather(
            provider.complete_with_tools(
                [{"role": "user", "content": "one"}],
                tools=[_tool()],
                codex_session_key="telegram:primary:104",
            ),
            provider.complete_with_tools(
                [{"role": "user", "content": "two"}],
                tools=[_tool()],
                codex_session_key="telegram:primary:104",
            ),
        )
        assert _FakeCodexClient.max_active_turns == 1
        assert "thread/resume" in [method for method, _ in _FakeCodexClient.instances[1].calls]

    asyncio.run(scenario())


def test_distinct_conversation_keys_never_share_a_thread(tmp_path: Path) -> None:
    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        await provider.complete_with_tools(
            [{"role": "user", "content": "telegram request"}],
            tools=[_tool()],
            codex_session_key="telegram:default:107",
        )
        await provider.complete_with_tools(
            [{"role": "user", "content": "desktop request"}],
            tools=[_tool()],
            codex_session_key="desktop:default:107",
        )

        assert [method for method, _ in _FakeCodexClient.instances[0].calls] == [
            "thread/start",
            "turn/start",
        ]
        assert [method for method, _ in _FakeCodexClient.instances[1].calls] == [
            "thread/start",
            "turn/start",
        ]
        registry = json.loads((tmp_path / "codex_sessions.json").read_text())
        assert len(registry["sessions"]) == 2

    asyncio.run(scenario())


def test_ephemeral_calls_and_failed_first_turn_do_not_create_session_state(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        await provider.complete([{"role": "user", "content": "isolated helper"}])
        assert _call(_FakeCodexClient.instances[0], "thread/start")["ephemeral"] is True
        assert not (tmp_path / "codex_sessions.json").exists()

        _FakeCodexClient.fail_turn = True
        with pytest.raises(CodexAppServerError, match="turn failed"):
            await provider.complete_with_tools(
                [{"role": "user", "content": "failed"}],
                tools=[_tool()],
                codex_session_key="telegram:primary:105",
            )
        assert not (tmp_path / "codex_sessions.json").exists()

    asyncio.run(scenario())


def test_main_octo_route_passes_the_scoped_session_key_to_provider() -> None:
    class _Provider:
        def __init__(self) -> None:
            self.tool_kwargs: dict[str, object] = {}

        async def complete_with_tools(
            self, messages, *, tools, tool_choice="auto", **kwargs: object
        ) -> dict[str, Any]:
            del messages, tools, tool_choice
            self.tool_kwargs = dict(kwargs)
            return {"content": "The result is ready.", "tool_calls": []}

        async def complete(self, messages, **kwargs: object) -> str:
            del messages, kwargs
            return '{"verdict":"final","confidence":1.0,"reason":"complete"}'

    class _Octo:
        trace_sink = None

    async def scenario() -> None:
        provider = _Provider()
        result = await _complete_route_with_tools(
            octo=_Octo(),
            provider=provider,
            messages=[{"role": "user", "content": "finish the request"}],
            tool_specs=[
                ToolSpec(
                    name="lookup",
                    description="lookup",
                    parameters={"type": "object", "properties": {}},
                    permission="read",
                    handler=lambda: None,
                )
            ],
            ctx={
                "chat_id": 106,
                "codex_session_key": "telegram:default:106",
            },
            internal_followup=False,
            user_text="finish the request",
            images=None,
            allow_tool_catalog_expansion=False,
        )
        assert result == "The result is ready."
        assert provider.tool_kwargs == {"codex_session_key": "telegram:default:106"}

    asyncio.run(scenario())


def test_main_conversation_planner_reply_passes_scoped_session_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Provider:
        def __init__(self) -> None:
            self.complete_kwargs: list[dict[str, object]] = []

        async def complete(self, messages, **kwargs: object) -> str:
            del messages
            self.complete_kwargs.append(dict(kwargs))
            return '{"mode":"reply","steps":[],"response":"Hello from Alice."}'

    class _Memory:
        async def add_message(self, role, content, metadata=None) -> None:
            del role, content, metadata

    class _Octo:
        store = object()
        canon = object()
        is_ws_active = False
        internal_progress_send = None
        trace_sink = None

        async def set_typing(self, chat_id: int, active: bool) -> None:
            del chat_id, active

        async def set_thinking(self, active: bool) -> None:
            del active

        def peek_context_wakeup(self, chat_id: int) -> str:
            del chat_id
            return ""

    async def fake_build_octo_prompt(**kwargs: object) -> list[Message]:
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def no_action_retry(**kwargs: object) -> bool:
        del kwargs
        return False

    async def finalize_response(**kwargs: object) -> str:
        return str(kwargs["response_text"])

    import octopal.runtime.octo.router as router

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_needs_action_or_blocked_retry", no_action_retry)
    monkeypatch.setattr(router, "_finalize_response", finalize_response)
    monkeypatch.setattr(
        router,
        "_get_octo_tools",
        lambda octo, chat_id: ([], {"octo": octo, "chat_id": chat_id}),
    )

    async def scenario() -> None:
        provider = _Provider()
        response = await route_or_reply(
            _Octo(),
            provider,
            _Memory(),
            "hello",
            211619002,
            "",
            conversation_scope="primary",
            channel_context={"source_channel": "telegram"},
        )

        assert response == "Hello from Alice."
        assert provider.complete_kwargs == [{"codex_session_key": "telegram:primary:211619002"}]

    asyncio.run(scenario())
