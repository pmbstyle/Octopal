from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest

from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.providers import codex_provider
from octopal.infrastructure.providers.base import Message
from octopal.infrastructure.providers.codex_provider import CodexAppServerError, CodexProvider
from octopal.runtime.octo.router import _complete_route_with_tools, route_or_reply
from octopal.tools.registry import ToolSpec

_RealCodexAppServerClient = codex_provider._CodexAppServerClient


class _FakeCodexClient:
    instances: list[_FakeCodexClient] = []
    next_thread = 1
    fail_resume_once: BaseException | None = None
    hang_resume_once = False
    fail_turn = False
    active_turns = 0
    max_active_turns = 0

    def __init__(self, command: str, args: list[str], env: dict[str, str]) -> None:
        del command, args, env
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.thread_id = ""
        self.turn_event_index = 0
        self.turn_active = False
        self.closed = False
        self.request_timeouts: list[tuple[str, float]] = []
        self.event_timeouts: list[float] = []
        self.responses: list[tuple[int | str, dict[str, Any]]] = []
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
        self.request_timeouts.append((method, timeout))
        payload = dict(params or {})
        self.calls.append((method, payload))
        if method == "thread/start":
            self.thread_id = f"thread-{type(self).next_thread}"
            type(self).next_thread += 1
            return {"thread": {"id": self.thread_id}}
        if method == "thread/resume":
            if type(self).hang_resume_once:
                type(self).hang_resume_once = False
                await asyncio.wait_for(asyncio.Future(), timeout=timeout)
            if type(self).fail_resume_once is not None:
                error = type(self).fail_resume_once
                type(self).fail_resume_once = None
                raise error
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
        self.event_timeouts.append(timeout)
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
        self.closed = True
        self._finish_turn()

    async def respond(self, request_id: int | str, result: dict[str, Any]) -> None:
        self.responses.append((request_id, result))

    def _finish_turn(self) -> None:
        if self.turn_active:
            self.turn_active = False
            type(self).active_turns -= 1


@pytest.fixture(autouse=True)
def _fake_codex_client(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeCodexClient.instances = []
    _FakeCodexClient.next_thread = 1
    _FakeCodexClient.fail_resume_once = None
    _FakeCodexClient.hang_resume_once = False
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


def test_configuration_reset_reports_only_changed_redacted_component(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[dict[str, Any]] = []

    def record_state(state: str, session_ref: str, **fields: Any) -> None:
        events.append({"state": state, "session_ref": session_ref, **fields})

    monkeypatch.setattr(codex_provider, "_log_session_state", record_state)

    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        messages = [{"role": "user", "content": "request"}]
        await provider.complete_with_tools(
            messages,
            tools=[_tool("lookup")],
            codex_session_key="telegram:primary:112",
        )
        await provider.complete_with_tools(
            messages,
            tools=[_tool("write")],
            codex_session_key="telegram:primary:112",
        )

        reset = next(event for event in events if event.get("reason") == "configuration_changed")
        assert reset["phase"] == "executor"
        assert reset["changed_components"] == ["tool_catalog"]
        assert reset["tool_count"] == 1
        assert len(reset["tool_digest"]) == 64
        assert "request" not in json.dumps(reset)

    asyncio.run(scenario())


def test_configuration_components_rotate_only_for_relevant_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_kwargs = {
        "model": "model-a",
        "cwd": "/workspace/a",
        "effort": "high",
        "dynamic_tools": codex_provider._tools_to_dynamic_tools([_tool("lookup")]),
    }
    base = codex_provider._session_configuration(**base_kwargs)

    for component, override in (
        ("model", {"model": "model-b"}),
        ("cwd", {"cwd": "/workspace/b"}),
        ("reasoning_effort", {"effort": "low"}),
        (
            "tool_catalog",
            {"dynamic_tools": codex_provider._tools_to_dynamic_tools([_tool("write")])},
        ),
    ):
        changed = codex_provider._session_configuration(**{**base_kwargs, **override})
        assert codex_provider._changed_config_components(base.components, changed.components) == [
            component
        ]

    monkeypatch.setattr(codex_provider, "_codex_command", lambda: "/other/bin/codex")
    command_changed = codex_provider._session_configuration(**base_kwargs)
    assert codex_provider._changed_config_components(
        base.components, command_changed.components
    ) == ["command"]

    monkeypatch.setattr(codex_provider, "_codex_args", lambda: ["app-server", "--flag"])
    args_changed = codex_provider._session_configuration(**base_kwargs)
    assert codex_provider._changed_config_components(
        command_changed.components, args_changed.components
    ) == ["args"]


def test_request_telemetry_redacts_stderr_content(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _Logger:
        def warning(self, event: str, **fields: Any) -> None:
            captured.update({"event": event, **fields})

        def debug(self, event: str, **fields: Any) -> None:
            captured.update({"event": event, **fields})

    monkeypatch.setattr(codex_provider, "logger", _Logger())
    codex_provider._log_app_server_request(
        "thread/resume",
        outcome="failed",
        elapsed_ms=30_000,
        write_wait_ms=10,
        wire_wait_ms=29_990,
        process_exit_code=7,
        stderr_tail=(
            "fatal login required token=top-secret at "
            "/Users/private-user/.config/provider/state.json"
        ),
        error="TimeoutError",
        exception=TimeoutError("request included private prompt"),
        protocol_error_count=2,
    )

    assert captured["request_stage"] == "thread/resume"
    assert captured["stderr_category"] == "fatal"
    assert len(captured["stderr_digest"]) == 12
    assert captured["failure_reason"] == "request_timeout"
    assert captured["failure_type"] == "timeout"
    assert captured["failure_category"] == "request"
    assert captured["process_exit_kind"] == "nonzero"
    assert captured["process_exit_signal"] is None
    assert captured["likely_diagnostic_categories"] == [
        "authentication",
        "runtime_failure",
    ]
    assert captured["protocol_error_count"] == 2
    serialized = json.dumps(captured)
    assert "top-secret" not in serialized
    assert "private-user" not in serialized
    assert "private prompt" not in serialized
    assert captured["write_wait_ms"] == 10
    assert captured["wire_wait_ms"] == 29_990


@pytest.mark.parametrize(
    ("reason", "transport_type", "category", "exit_code", "exit_kind", "signal"),
    [
        ("child_exit_nonzero", "child_exit", "process", 7, "nonzero", None),
        ("child_exit_signal", "child_exit", "process", -9, "signal", 9),
        ("stdout_eof", "eof", "stream", None, "running_or_unknown", None),
        ("invalid_json", "protocol_error", "protocol", None, "running_or_unknown", None),
    ],
)
def test_transport_telemetry_distinguishes_redacted_failure_types(
    monkeypatch: pytest.MonkeyPatch,
    reason: str,
    transport_type: str,
    category: str,
    exit_code: int | None,
    exit_kind: str,
    signal: int | None,
) -> None:
    captured: dict[str, Any] = {}

    class _Logger:
        def warning(self, event: str, **fields: Any) -> None:
            captured.update({"event": event, **fields})

    monkeypatch.setattr(codex_provider, "logger", _Logger())
    codex_provider._log_app_server_transport(
        reason=reason,
        transport_type=transport_type,
        category=category,
        process_exit_code=exit_code,
        stderr_tail=(
            "permission denied: /Users/private-user/workspace; " "authorization=Bearer top-secret"
        ),
        protocol_error_count=3,
        primary_failure=True,
    )

    assert captured["transport_reason"] == reason
    assert captured["transport_type"] == transport_type
    assert captured["transport_category"] == category
    assert captured["process_exit_code"] == exit_code
    assert captured["process_exit_kind"] == exit_kind
    assert captured["process_exit_signal"] == signal
    assert captured["likely_diagnostic_categories"] == ["permission"]
    assert captured["protocol_error_count"] == 3
    assert len(captured["stderr_digest"]) == 12
    serialized = json.dumps(captured)
    assert "private-user" not in serialized
    assert "top-secret" not in serialized


def test_protocol_error_and_eof_are_logged_without_stdout_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[dict[str, Any]] = []

    class _Logger:
        def warning(self, event: str, **fields: Any) -> None:
            events.append({"event": event, **fields})

    class _Process:
        def __init__(self) -> None:
            self.stdout = asyncio.StreamReader()
            self.returncode: int | None = None

    monkeypatch.setattr(codex_provider, "logger", _Logger())

    async def scenario() -> None:
        client = _RealCodexAppServerClient("unused", [], {})
        process = _Process()
        client._process = process  # type: ignore[assignment]
        process.stdout.feed_data(b'{"malformed":"prompt=private-message token=top-secret"\n')
        process.stdout.feed_data(b'["private-message", "top-secret"]\n')
        process.stdout.feed_eof()
        await client._read_stdout()

        assert [event["transport_reason"] for event in events] == [
            "invalid_json",
            "invalid_message_shape",
            "stdout_eof",
        ]
        assert events[0]["transport_type"] == "protocol_error"
        assert events[0]["protocol_error_count"] == 1
        assert events[0]["primary_failure"] is False
        assert events[1]["transport_type"] == "protocol_error"
        assert events[1]["protocol_error_count"] == 2
        assert events[2]["transport_type"] == "eof"
        assert events[2]["protocol_error_count"] == 2
        assert events[2]["primary_failure"] is True
        serialized = json.dumps(events)
        assert "private-message" not in serialized
        assert "top-secret" not in serialized

    asyncio.run(scenario())


def test_first_transport_failure_remains_authoritative_while_exit_is_observed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[dict[str, Any]] = []

    class _Logger:
        def warning(self, event: str, **fields: Any) -> None:
            events.append({"event": event, **fields})

    monkeypatch.setattr(codex_provider, "logger", _Logger())
    client = _RealCodexAppServerClient("unused", [], {})
    client._fail_transport(
        CodexAppServerError("stdout closed"),
        reason="stdout_eof",
        transport_type="eof",
        category="stream",
        process_exit_code=None,
    )
    client._fail_transport(
        CodexAppServerError("exited with code 7"),
        reason="child_exit_nonzero",
        transport_type="child_exit",
        category="process",
        process_exit_code=7,
    )

    assert client._transport_reason == "stdout_eof"
    assert client._transport_type == "eof"
    assert str(client._transport_error) == "stdout closed"
    assert [event["primary_failure"] for event in events] == [True, False]
    assert events[1]["transport_reason"] == "child_exit_nonzero"
    assert events[1]["process_exit_code"] == 7


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


@pytest.mark.parametrize(
    "resume_error",
    [
        TimeoutError("resume timed out"),
        CodexAppServerError("codex app-server stdout closed"),
    ],
)
def test_resume_failure_uses_fresh_client_and_replaces_mapping(
    tmp_path: Path, resume_error: BaseException
) -> None:
    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        await provider.complete_with_tools(
            [{"role": "user", "content": "first"}],
            tools=[_tool()],
            codex_session_key="desktop:primary:103",
        )
        _FakeCodexClient.fail_resume_once = resume_error
        await provider.complete_with_tools(
            [
                {"role": "system", "content": "full current context"},
                {"role": "user", "content": "second"},
            ],
            tools=[_tool()],
            codex_session_key="desktop:primary:103",
        )

        failed_resume = _FakeCodexClient.instances[1]
        fallback = _FakeCodexClient.instances[2]
        assert [method for method, _ in failed_resume.calls] == ["thread/resume"]
        assert failed_resume.closed is True
        assert [method for method, _ in fallback.calls] == ["thread/start", "turn/start"]
        assert _call(fallback, "thread/start")["developerInstructions"] == ("full current context")
        registry = json.loads((tmp_path / "codex_sessions.json").read_text())
        assert next(iter(registry["sessions"].values()))["thread_id"] == "thread-2"

    asyncio.run(scenario())


def test_hung_resume_times_out_then_recovers_on_fresh_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(codex_provider, "CODEX_CONTROL_REQUEST_TIMEOUT_SECONDS", 0.01)

    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        await provider.complete_with_tools(
            [{"role": "user", "content": "first"}],
            tools=[_tool()],
            codex_session_key="desktop:primary:113",
        )
        _FakeCodexClient.hang_resume_once = True
        await provider.complete_with_tools(
            [{"role": "user", "content": "second"}],
            tools=[_tool()],
            codex_session_key="desktop:primary:113",
        )

        failed_resume = _FakeCodexClient.instances[1]
        fallback = _FakeCodexClient.instances[2]
        assert failed_resume.closed is True
        assert [method for method, _ in failed_resume.calls] == ["thread/resume"]
        assert [method for method, _ in fallback.calls] == ["thread/start", "turn/start"]
        registry = json.loads((tmp_path / "codex_sessions.json").read_text())
        assert next(iter(registry["sessions"].values()))["thread_id"] == "thread-2"

    asyncio.run(scenario())


def test_control_requests_use_short_timeout_and_turn_events_use_long_idle_timeout(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        await provider.complete_with_tools(
            [{"role": "user", "content": "request"}],
            tools=[_tool()],
            codex_session_key="telegram:primary:110",
        )

        client = _FakeCodexClient.instances[0]
        assert client.request_timeouts == [
            ("thread/start", codex_provider.CODEX_CONTROL_REQUEST_TIMEOUT_SECONDS),
            ("turn/start", codex_provider.CODEX_CONTROL_REQUEST_TIMEOUT_SECONDS),
        ]
        assert client.event_timeouts == [
            codex_provider.CODEX_TURN_EVENT_IDLE_TIMEOUT_SECONDS,
            codex_provider.CODEX_TURN_EVENT_IDLE_TIMEOUT_SECONDS,
        ]

    asyncio.run(scenario())


def test_app_server_eof_fails_initialize_without_waiting_for_request_timeout() -> None:
    async def scenario() -> None:
        client = _RealCodexAppServerClient(
            sys.executable,
            ["-c", "import sys; sys.stdin.readline()"],
            dict(os.environ),
        )
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        with pytest.raises(CodexAppServerError, match="app-server"):
            await asyncio.wait_for(client.start(), timeout=3.0)
        assert loop.time() - started_at < 2.0

    asyncio.run(scenario())


def test_simultaneous_request_and_terminal_notification_are_preserved() -> None:
    request = {
        "id": 7,
        "method": "item/permissions/requestApproval",
        "params": {},
    }
    completed = {"method": "turn/completed", "params": {"threadId": "thread-1"}}

    class _PrequeuedClient(_RealCodexAppServerClient):
        def __init__(self) -> None:
            super().__init__("unused", [], {})
            self.responses: list[tuple[int | str, dict[str, Any]]] = []

        async def respond(self, request_id: int | str, result: dict[str, Any]) -> None:
            self.responses.append((request_id, result))

    async def prequeued_client() -> _PrequeuedClient:
        client = _PrequeuedClient()
        await client._requests.put(request)
        await client._notifications.put(completed)
        client._transport_error = CodexAppServerError("transport closed after terminal event")
        client._transport_failed.set()
        return client

    async def scenario() -> None:
        client = await prequeued_client()
        assert await client.next_event(0.1) == ("request", request)
        assert await client.next_event(0.1) == ("notification", completed)
        with pytest.raises(CodexAppServerError, match="transport closed"):
            await client.next_event(0.1)

        collecting_client = await prequeued_client()
        result = await asyncio.wait_for(
            codex_provider._collect_turn(
                collecting_client,
                thread_id="thread-1",
                turn_id=None,
                on_partial=None,
            ),
            timeout=0.5,
        )
        assert result == {"content": "", "tool_calls": [], "terminal": True}
        assert collecting_client.responses == [(7, {"permissions": {}, "scope": "turn"})]

    asyncio.run(scenario())


def test_next_event_cancellation_cleans_up_child_waiters() -> None:
    async def scenario() -> None:
        client = _RealCodexAppServerClient("unused", [], {})
        waiter = asyncio.create_task(client.next_event(60.0))

        for _ in range(10):
            if (
                client._requests._getters
                and client._notifications._getters
                and client._transport_failed._waiters
            ):
                break
            await asyncio.sleep(0)
        assert len(client._requests._getters) == 1
        assert len(client._notifications._getters) == 1
        assert len(client._transport_failed._waiters) == 1

        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        assert not client._requests._getters
        assert not client._notifications._getters
        assert not client._transport_failed._waiters

    asyncio.run(scenario())


def test_next_event_cancellation_preserves_simultaneously_ready_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = {
        "id": 7,
        "method": "item/permissions/requestApproval",
        "params": {},
    }
    completed = {"method": "turn/completed", "params": {"threadId": "thread-1"}}

    async def scenario() -> None:
        client = _RealCodexAppServerClient("unused", [], {})
        await client._requests.put(request)
        await client._notifications.put(completed)
        client._transport_error = CodexAppServerError("transport closed after terminal event")
        client._transport_failed.set()

        real_wait = asyncio.wait
        ready = asyncio.Event()
        block_first_wait = True

        async def wait_after_children_complete(
            futures: Any,
            *,
            timeout: float | None = None,
            return_when: str = asyncio.ALL_COMPLETED,
        ) -> tuple[set[asyncio.Future[Any]], set[asyncio.Future[Any]]]:
            nonlocal block_first_wait
            done, pending = await real_wait(
                futures,
                timeout=timeout,
                return_when=return_when,
            )
            if block_first_wait:
                block_first_wait = False
                ready.set()
                await asyncio.Future()
            return done, pending

        monkeypatch.setattr(codex_provider.asyncio, "wait", wait_after_children_complete)
        waiter = asyncio.create_task(client.next_event(60.0))
        await asyncio.wait_for(ready.wait(), timeout=0.5)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        assert await client.next_event(0.1) == ("request", request)
        assert await client.next_event(0.1) == ("notification", completed)
        with pytest.raises(CodexAppServerError, match="transport closed"):
            await client.next_event(0.1)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("success", "tool_content", "item_status"),
    [(True, "lookup result", "completed"), (False, "lookup failed", "failed")],
)
def test_dynamic_tool_result_uses_native_request_lifecycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    success: bool,
    tool_content: str,
    item_status: str,
) -> None:
    class _NativeToolClient(_FakeCodexClient):
        async def next_event(self, timeout: float) -> tuple[str, dict[str, Any]]:
            self.event_timeouts.append(timeout)
            self.turn_event_index += 1
            events = [
                (
                    "request",
                    {
                        "id": 9,
                        "method": "item/tool/call",
                        "params": {
                            "threadId": self.thread_id,
                            "turnId": "turn-1",
                            "callId": "call-lookup",
                            "tool": "lookup",
                            "arguments": {"query": "safe"},
                        },
                    },
                ),
                (
                    "notification",
                    {
                        "method": "item/completed",
                        "params": {
                            "threadId": self.thread_id,
                            "turnId": "turn-1",
                            "item": {
                                "id": "call-lookup",
                                "type": "dynamicToolCall",
                                "status": item_status,
                            },
                        },
                    },
                ),
                (
                    "notification",
                    {
                        "method": "item/agentMessage/delta",
                        "params": {"threadId": self.thread_id, "delta": "final answer"},
                    },
                ),
                (
                    "notification",
                    {
                        "method": "turn/completed",
                        "params": {
                            "threadId": self.thread_id,
                            "turn": {"status": "completed"},
                        },
                    },
                ),
            ]
            if self.turn_event_index == len(events):
                self._finish_turn()
            return events[self.turn_event_index - 1]

    monkeypatch.setattr(codex_provider, "_CodexAppServerClient", _NativeToolClient)
    executions: list[dict[str, Any]] = []

    async def execute(call: dict[str, Any]) -> dict[str, Any]:
        executions.append(call)
        return {"success": success, "content": tool_content}

    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        result = await provider.complete_with_tools(
            [{"role": "user", "content": "use lookup"}],
            tools=[_tool()],
            codex_session_key="telegram:primary:native",
            tool_executor=execute,
        )

        client = _NativeToolClient.instances[0]
        assert len(executions) == 1
        assert executions[0]["id"] == "call-lookup"
        assert client.responses == [
            (
                9,
                {
                    "success": success,
                    "contentItems": [{"type": "inputText", "text": tool_content}],
                },
            )
        ]
        assert "turn/interrupt" not in [method for method, _ in client.calls]
        assert client.event_timeouts == [codex_provider.CODEX_TURN_EVENT_IDLE_TIMEOUT_SECONDS] * 4
        assert result["content"] == "final answer"
        assert result["tool_results_submitted"] is True
        assert result["tool_calls"][0]["id"] == "call-lookup"
        registry = json.loads((tmp_path / "codex_sessions.json").read_text())
        assert len(registry["sessions"]) == 1

    asyncio.run(scenario())


def test_transport_loss_after_native_tool_execution_is_not_replayable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _ToolThenEofClient(_FakeCodexClient):
        async def next_event(self, timeout: float) -> tuple[str, dict[str, Any]]:
            self.event_timeouts.append(timeout)
            if self.turn_event_index == 0:
                self.turn_event_index = 1
                return (
                    "request",
                    {
                        "id": 9,
                        "method": "item/tool/call",
                        "params": {
                            "threadId": self.thread_id,
                            "turnId": "turn-1",
                            "callId": "call-lookup",
                            "tool": "lookup",
                            "arguments": {"query": "safe"},
                        },
                    },
                )
            self._finish_turn()
            raise CodexAppServerError("stdout closed")

    monkeypatch.setattr(codex_provider, "_CodexAppServerClient", _ToolThenEofClient)

    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        executions = 0

        async def execute(call: dict[str, Any]) -> dict[str, Any]:
            nonlocal executions
            assert call["id"] == "call-lookup"
            executions += 1
            return {"success": True, "content": "actual result"}

        with pytest.raises(
            codex_provider.CodexToolResultTransportError,
            match="automatic replay disabled",
        ):
            await provider.complete_with_tools(
                [{"role": "user", "content": "use lookup"}],
                tools=[_tool()],
                codex_session_key="telegram:primary:transport",
                codex_request_lane="interactive",
                tool_executor=execute,
            )

        assert executions == 1
        assert _ToolThenEofClient.instances[0].event_timeouts == [
            codex_provider.CODEX_TURN_EVENT_IDLE_TIMEOUT_SECONDS,
            codex_provider.CODEX_TURN_EVENT_IDLE_TIMEOUT_SECONDS,
        ]
        assert _ToolThenEofClient.instances[0].responses[0][1]["success"] is True
        assert "turn/interrupt" not in [
            method for method, _ in _ToolThenEofClient.instances[0].calls
        ]
        registry_path = tmp_path / "codex_sessions.json"
        assert not registry_path.exists() or json.loads(registry_path.read_text())["sessions"] == {}

    asyncio.run(scenario())


def test_cancellation_interrupts_active_turn_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    interrupt_seen = asyncio.Event()

    class _HangingClient(_FakeCodexClient):
        async def next_event(self, timeout: float) -> tuple[str, dict[str, Any]]:
            del timeout
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        async def request(
            self,
            method: str,
            params: dict[str, Any] | None = None,
            *,
            timeout: float,
        ) -> Any:
            if method == "turn/interrupt":
                self.calls.append((method, dict(params or {})))
                interrupt_seen.set()
                return {}
            return await super().request(method, params, timeout=timeout)

    monkeypatch.setattr(codex_provider, "_CodexAppServerClient", _HangingClient)

    async def scenario() -> None:
        provider = CodexProvider(_settings(Path("unused")))
        task = asyncio.create_task(
            provider.complete_with_tools(
                [{"role": "user", "content": "wait"}],
                tools=[_tool()],
            )
        )
        while not _HangingClient.instances or not _HangingClient.instances[0].turn_active:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(interrupt_seen.wait(), timeout=0.5)
        client = _HangingClient.instances[0]
        assert [method for method, _ in client.calls].count("turn/interrupt") == 1

    asyncio.run(scenario())


def test_interactive_and_background_lanes_use_independent_sessions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_events: list[tuple[str, str, str]] = []

    def capture_session_state(state: str, session_ref: str, **kwargs: object) -> None:
        del session_ref
        session_events.append((state, str(kwargs["phase"]), str(kwargs["lane"])))

    monkeypatch.setattr(codex_provider, "_log_session_state", capture_session_state)

    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        kwargs = {
            "tools": [_tool()],
            "codex_session_key": "telegram:primary:lane",
        }
        await provider.complete_with_tools(
            [{"role": "user", "content": "interactive"}],
            codex_request_lane="interactive",
            **kwargs,
        )
        await provider.complete_with_tools(
            [{"role": "user", "content": "background"}],
            codex_request_lane="background",
            **kwargs,
        )

        assert [method for method, _ in _FakeCodexClient.instances[1].calls] == [
            "thread/start",
            "turn/start",
        ]
        registry = json.loads((tmp_path / "codex_sessions.json").read_text())
        assert len(registry["sessions"]) == 2
        assert session_events == [
            ("created", "executor", "interactive"),
            ("created", "executor", "background"),
        ]

    asyncio.run(scenario())


def test_admission_reserves_capacity_for_interactive_work() -> None:
    async def scenario() -> None:
        controller = codex_provider._CodexAdmissionController(capacity=2, background_capacity=1)
        background_release = asyncio.Event()
        interactive_entered = asyncio.Event()
        second_background_entered = asyncio.Event()

        async def hold_background(entered: asyncio.Event) -> None:
            async with controller.acquire("background"):
                entered.set()
                await background_release.wait()

        first_entered = asyncio.Event()
        first = asyncio.create_task(hold_background(first_entered))
        await first_entered.wait()
        second = asyncio.create_task(hold_background(second_background_entered))

        async def run_interactive() -> None:
            async with controller.acquire("interactive"):
                interactive_entered.set()

        interactive = asyncio.create_task(run_interactive())
        await asyncio.wait_for(interactive_entered.wait(), timeout=0.5)
        assert not second_background_entered.is_set()
        background_release.set()
        await asyncio.gather(first, second, interactive)

    asyncio.run(scenario())


def test_admission_fairness_serves_aged_background_after_interactive_burst(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(codex_provider, "CODEX_BACKGROUND_MAX_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(codex_provider, "CODEX_INTERACTIVE_BURST_LIMIT", 1)

    async def scenario() -> None:
        controller = codex_provider._CodexAdmissionController(capacity=1, background_capacity=1)
        order: list[str] = []
        async with controller.acquire("interactive"):
            pass

        async def enter(lane: str) -> None:
            async with controller.acquire(lane):
                order.append(lane)

        background = asyncio.create_task(enter("background"))
        interactive = asyncio.create_task(enter("interactive"))
        await asyncio.gather(background, interactive)
        assert order == ["background", "interactive"]

    asyncio.run(scenario())


def test_admission_serves_waiting_interactive_before_next_background() -> None:
    async def scenario() -> None:
        controller = codex_provider._CodexAdmissionController(capacity=1, background_capacity=1)
        release_active = asyncio.Event()
        release_interactive = asyncio.Event()
        active_entered = asyncio.Event()
        order: list[str] = []

        async def active_background() -> None:
            async with controller.acquire("background"):
                active_entered.set()
                await release_active.wait()

        async def enter(lane: str) -> None:
            async with controller.acquire(lane):
                order.append(lane)
                if lane == "interactive":
                    await release_interactive.wait()

        active = asyncio.create_task(active_background())
        await active_entered.wait()
        queued_background = asyncio.create_task(enter("background"))
        interactive = asyncio.create_task(enter("interactive"))
        await asyncio.sleep(0)
        release_active.set()
        while not order:
            await asyncio.sleep(0)

        assert order == ["interactive"]
        release_interactive.set()
        await asyncio.gather(active, queued_background, interactive)
        assert order == ["interactive", "background"]

    asyncio.run(scenario())


def test_failed_fallback_turn_does_not_persist_replacement_mapping(tmp_path: Path) -> None:
    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        await provider.complete_with_tools(
            [{"role": "user", "content": "first"}],
            tools=[_tool()],
            codex_session_key="desktop:primary:111",
        )

        _FakeCodexClient.fail_resume_once = TimeoutError("resume timed out")
        _FakeCodexClient.fail_turn = True
        with pytest.raises(CodexAppServerError, match="turn failed"):
            await provider.complete_with_tools(
                [{"role": "user", "content": "second"}],
                tools=[_tool()],
                codex_session_key="desktop:primary:111",
            )

        registry = json.loads((tmp_path / "codex_sessions.json").read_text())
        assert registry["sessions"] == {}
        assert _FakeCodexClient.instances[1].closed is True
        assert _FakeCodexClient.instances[2].closed is True

    asyncio.run(scenario())


def test_cancelled_resumed_turn_drops_mapping_before_releasing_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    turn_started = asyncio.Event()

    class _HangingTurnClient(_FakeCodexClient):
        hang = False

        async def request(
            self,
            method: str,
            params: dict[str, Any] | None = None,
            *,
            timeout: float,
        ) -> Any:
            result = await super().request(method, params, timeout=timeout)
            if method == "turn/start" and type(self).hang:
                turn_started.set()
            return result

        async def next_event(self, timeout: float) -> tuple[str, dict[str, Any]]:
            if type(self).hang:
                await asyncio.Event().wait()
            return await super().next_event(timeout)

    monkeypatch.setattr(codex_provider, "_CodexAppServerClient", _HangingTurnClient)

    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        await provider.complete_with_tools(
            [{"role": "user", "content": "first"}],
            tools=[_tool()],
            codex_session_key="telegram:primary:cancelled",
        )
        _HangingTurnClient.hang = True
        task = asyncio.create_task(
            provider.complete_with_tools(
                [{"role": "user", "content": "second"}],
                tools=[_tool()],
                codex_session_key="telegram:primary:cancelled",
            )
        )
        await asyncio.wait_for(turn_started.wait(), timeout=0.5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        registry = json.loads((tmp_path / "codex_sessions.json").read_text())
        assert registry["sessions"] == {}

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


def test_main_route_executes_native_tool_once_and_uses_terminal_answer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _RouteToolClient(_FakeCodexClient):
        async def next_event(self, timeout: float) -> tuple[str, dict[str, Any]]:
            self.event_timeouts.append(timeout)
            self.turn_event_index += 1
            events = [
                (
                    "request",
                    {
                        "id": 41,
                        "method": "item/tool/call",
                        "params": {
                            "threadId": self.thread_id,
                            "turnId": "turn-route",
                            "callId": "call-route",
                            "tool": "lookup",
                            "arguments": {"query": "one"},
                        },
                    },
                ),
                (
                    "notification",
                    {
                        "method": "item/completed",
                        "params": {
                            "threadId": self.thread_id,
                            "turnId": "turn-route",
                            "item": {
                                "id": "call-route",
                                "type": "dynamicToolCall",
                                "status": "completed",
                            },
                        },
                    },
                ),
                (
                    "notification",
                    {
                        "method": "item/agentMessage/delta",
                        "params": {"threadId": self.thread_id, "delta": "native final"},
                    },
                ),
                (
                    "notification",
                    {
                        "method": "turn/completed",
                        "params": {
                            "threadId": self.thread_id,
                            "turn": {"status": "completed"},
                        },
                    },
                ),
            ]
            if self.turn_event_index == len(events):
                self._finish_turn()
            return events[self.turn_event_index - 1]

    monkeypatch.setattr(codex_provider, "_CodexAppServerClient", _RouteToolClient)
    import octopal.runtime.octo.router as router

    async def no_retry(**kwargs: object) -> bool:
        del kwargs
        return False

    async def finalize(**kwargs: object) -> str:
        return str(kwargs["response_text"])

    monkeypatch.setattr(router, "_needs_autonomous_recovery_retry", no_retry)
    monkeypatch.setattr(router, "_finalize_response", finalize)
    tool_runs = 0

    def lookup(args: dict[str, Any], ctx: dict[str, object]) -> dict[str, Any]:
        nonlocal tool_runs
        assert args == {"query": "one"}
        assert ctx["chat_id"] == 301
        tool_runs += 1
        return {"value": "actual"}

    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        messages: list[Message | dict[str, Any]] = [Message(role="user", content="look it up")]
        reply = await _complete_route_with_tools(
            octo=type("Octo", (), {"trace_sink": None})(),
            provider=provider,
            messages=messages,
            tool_specs=[
                ToolSpec(
                    name="lookup",
                    description="lookup",
                    parameters={"type": "object", "properties": {}},
                    permission="read",
                    handler=lookup,
                )
            ],
            ctx={
                "chat_id": 301,
                "codex_session_key": "telegram:primary:301",
                "codex_request_lane": "background",
            },
            internal_followup=False,
            user_text="look it up",
            images=None,
            allow_tool_catalog_expansion=False,
        )

        assert reply == "native final"
        assert tool_runs == 1
        assert len(_RouteToolClient.instances) == 1
        client = _RouteToolClient.instances[0]
        assert client.responses[0][1]["success"] is True
        assert "actual" in client.responses[0][1]["contentItems"][0]["text"]
        assert "turn/interrupt" not in [method for method, _ in client.calls]
        assert any(
            isinstance(message, dict)
            and message.get("role") == "tool"
            and "actual" in str(message.get("content"))
            for message in messages
        )

    asyncio.run(scenario())


def test_native_control_continuation_runs_after_provider_turn_releases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import octopal.runtime.octo.router as router

    events: list[str] = []
    continuation_runs = 0

    class _Provider:
        provider_id = "codex"

        def __init__(self) -> None:
            self.calls = 0
            self.turn_active = False

        async def complete_with_tools(
            self,
            messages: list[Message | dict[str, Any]],
            *,
            tools: list[dict[str, Any]],
            tool_choice: str = "auto",
            **kwargs: object,
        ) -> dict[str, Any]:
            del tools, tool_choice
            self.calls += 1
            events.append(f"provider-{self.calls}-start")
            if self.calls == 1:
                self.turn_active = True
                call = {
                    "id": "call-continue",
                    "type": "function",
                    "function": {
                        "name": "octo_continue_from_control_route",
                        "arguments": json.dumps({"task": "finish the worker result"}),
                    },
                }
                executor = kwargs.get("tool_executor")
                assert callable(executor)
                execution = await executor(call)
                assert continuation_runs == 0
                assert "deferred" in str(execution.get("content"))
                events.append("provider-1-tool-response")
                self.turn_active = False
                events.append("provider-1-end")
                return {
                    "content": "continuation accepted",
                    "tool_calls": [call],
                    "tool_results_submitted": True,
                }

            assert self.turn_active is False
            assert any(
                isinstance(message, dict)
                and message.get("role") == "tool"
                and "continued" in str(message.get("content"))
                for message in messages
            )
            events.append("provider-2-end")
            return {"content": "NO_USER_RESPONSE", "tool_calls": []}

    provider = _Provider()

    async def continue_route(args: dict[str, Any], ctx: dict[str, object]) -> str:
        nonlocal continuation_runs
        assert args == {"task": "finish the worker result"}
        assert ctx["chat_id"] == 303
        assert provider.turn_active is False
        continuation_runs += 1
        events.append("continuation")
        return json.dumps({"status": "continued", "delivered": False, "notify_user": False})

    async def no_retry(**kwargs: object) -> bool:
        del kwargs
        return False

    async def finalize(**kwargs: object) -> str:
        return str(kwargs["response_text"])

    monkeypatch.setattr(router, "_needs_autonomous_recovery_retry", no_retry)
    monkeypatch.setattr(router, "_finalize_response", finalize)

    async def scenario() -> None:
        reply = await asyncio.wait_for(
            _complete_route_with_tools(
                octo=type("Octo", (), {"trace_sink": None})(),
                provider=provider,
                messages=[Message(role="user", content="process worker result")],
                tool_specs=[
                    ToolSpec(
                        name="octo_continue_from_control_route",
                        description="continue through the normal route",
                        parameters={"type": "object", "properties": {}},
                        permission="self_control",
                        handler=continue_route,
                        is_async=True,
                    )
                ],
                ctx={"chat_id": 303, "codex_request_lane": "background"},
                internal_followup=True,
                user_text="process worker result",
                images=None,
                allow_tool_catalog_expansion=False,
            ),
            timeout=1.0,
        )

        assert reply == "NO_USER_RESPONSE"
        assert provider.calls == 2
        assert continuation_runs == 1
        assert events == [
            "provider-1-start",
            "provider-1-tool-response",
            "provider-1-end",
            "continuation",
            "provider-2-start",
            "provider-2-end",
        ]

    asyncio.run(scenario())


def test_deferred_native_continuation_is_not_replayed_after_transport_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import octopal.runtime.octo.router as router

    continuation_runs = 0

    class _Provider:
        provider_id = "codex"

        def __init__(self) -> None:
            self.calls = 0

        async def complete_with_tools(
            self,
            messages: list[Message | dict[str, Any]],
            *,
            tools: list[dict[str, Any]],
            tool_choice: str = "auto",
            **kwargs: object,
        ) -> dict[str, Any]:
            del tools, tool_choice
            self.calls += 1
            if self.calls == 1:
                call = {
                    "id": "call-continue-lost-response",
                    "type": "function",
                    "function": {
                        "name": "octo_continue_from_control_route",
                        "arguments": json.dumps({"task": "finish once"}),
                    },
                }
                executor = kwargs.get("tool_executor")
                assert callable(executor)
                execution = await executor(call)
                assert "deferred" in str(execution.get("content"))
                raise codex_provider.CodexToolResultTransportError("transport closed")

            assert continuation_runs == 1
            assert any(
                isinstance(message, dict)
                and message.get("role") == "tool"
                and "continued" in str(message.get("content"))
                for message in messages
            )
            return {"content": "NO_USER_RESPONSE", "tool_calls": []}

    async def continue_route(args: dict[str, Any], ctx: dict[str, object]) -> str:
        nonlocal continuation_runs
        assert args == {"task": "finish once"}
        assert ctx["chat_id"] == 304
        continuation_runs += 1
        return json.dumps({"status": "continued"})

    async def no_retry(**kwargs: object) -> bool:
        del kwargs
        return False

    async def finalize(**kwargs: object) -> str:
        return str(kwargs["response_text"])

    monkeypatch.setattr(router, "_needs_autonomous_recovery_retry", no_retry)
    monkeypatch.setattr(router, "_finalize_response", finalize)

    async def scenario() -> None:
        provider = _Provider()
        reply = await asyncio.wait_for(
            _complete_route_with_tools(
                octo=type("Octo", (), {"trace_sink": None})(),
                provider=provider,
                messages=[Message(role="user", content="process worker result")],
                tool_specs=[
                    ToolSpec(
                        name="octo_continue_from_control_route",
                        description="continue through the normal route",
                        parameters={"type": "object", "properties": {}},
                        permission="self_control",
                        handler=continue_route,
                        is_async=True,
                    )
                ],
                ctx={"chat_id": 304, "codex_request_lane": "background"},
                internal_followup=True,
                user_text="process worker result",
                images=None,
                allow_tool_catalog_expansion=False,
            ),
            timeout=1.0,
        )

        assert reply == "NO_USER_RESPONSE"
        assert provider.calls == 2
        assert continuation_runs == 1

    asyncio.run(scenario())


def test_deferred_native_continuation_survives_provider_turn_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import octopal.runtime.octo.router as router

    acknowledged = asyncio.Event()
    continuation_finished = asyncio.Event()
    continuation_runs = 0

    class _Provider:
        provider_id = "codex"
        turn_active = False

        async def complete_with_tools(
            self,
            messages: list[Message | dict[str, Any]],
            *,
            tools: list[dict[str, Any]],
            tool_choice: str = "auto",
            **kwargs: object,
        ) -> dict[str, Any]:
            del messages, tools, tool_choice
            self.turn_active = True
            call = {
                "id": "call-continue-cancelled-turn",
                "type": "function",
                "function": {
                    "name": "octo_continue_from_control_route",
                    "arguments": json.dumps({"task": "finish after cancellation"}),
                },
            }
            executor = kwargs.get("tool_executor")
            assert callable(executor)
            execution = await executor(call)
            assert "deferred" in str(execution.get("content"))
            acknowledged.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.turn_active = False

    provider = _Provider()

    async def continue_route(args: dict[str, Any], ctx: dict[str, object]) -> str:
        nonlocal continuation_runs
        assert args == {"task": "finish after cancellation"}
        assert ctx["chat_id"] == 305
        assert provider.turn_active is False
        continuation_runs += 1
        continuation_finished.set()
        return json.dumps({"status": "continued"})

    async def no_retry(**kwargs: object) -> bool:
        del kwargs
        return False

    monkeypatch.setattr(router, "_needs_autonomous_recovery_retry", no_retry)

    async def scenario() -> None:
        route = asyncio.create_task(
            _complete_route_with_tools(
                octo=type("Octo", (), {"trace_sink": None})(),
                provider=provider,
                messages=[Message(role="user", content="process worker result")],
                tool_specs=[
                    ToolSpec(
                        name="octo_continue_from_control_route",
                        description="continue through the normal route",
                        parameters={"type": "object", "properties": {}},
                        permission="self_control",
                        handler=continue_route,
                        is_async=True,
                    )
                ],
                ctx={"chat_id": 305, "codex_request_lane": "background"},
                internal_followup=True,
                user_text="process worker result",
                images=None,
                allow_tool_catalog_expansion=False,
            )
        )
        await asyncio.wait_for(acknowledged.wait(), timeout=1.0)
        route.cancel()
        with pytest.raises(asyncio.CancelledError):
            await route
        await asyncio.wait_for(continuation_finished.wait(), timeout=1.0)
        await asyncio.sleep(0)

        assert continuation_runs == 1

    asyncio.run(scenario())


def test_main_route_recovers_transport_after_tool_without_replaying_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _RecoveringToolClient(_FakeCodexClient):
        async def next_event(self, timeout: float) -> tuple[str, dict[str, Any]]:
            if type(self).instances.index(self) > 0:
                return await super().next_event(timeout)
            self.event_timeouts.append(timeout)
            if self.turn_event_index == 0:
                self.turn_event_index = 1
                return (
                    "request",
                    {
                        "id": 51,
                        "method": "item/tool/call",
                        "params": {
                            "threadId": self.thread_id,
                            "turnId": "turn-recover",
                            "callId": "call-recover",
                            "tool": "write_once",
                            "arguments": {"value": "same"},
                        },
                    },
                )
            self._finish_turn()
            raise CodexAppServerError("stdout closed")

    monkeypatch.setattr(codex_provider, "_CodexAppServerClient", _RecoveringToolClient)
    import octopal.runtime.octo.router as router

    async def no_retry(**kwargs: object) -> bool:
        del kwargs
        return False

    async def finalize(**kwargs: object) -> str:
        return str(kwargs["response_text"])

    monkeypatch.setattr(router, "_needs_autonomous_recovery_retry", no_retry)
    monkeypatch.setattr(router, "_finalize_response", finalize)
    tool_runs = 0

    def write_once(args: dict[str, Any], ctx: dict[str, object]) -> dict[str, Any]:
        nonlocal tool_runs
        del ctx
        assert args == {"value": "same"}
        tool_runs += 1
        return {"status": "written"}

    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        messages: list[Message | dict[str, Any]] = [Message(role="user", content="write it")]
        reply = await _complete_route_with_tools(
            octo=type("Octo", (), {"trace_sink": None})(),
            provider=provider,
            messages=messages,
            tool_specs=[
                ToolSpec(
                    name="write_once",
                    description="write",
                    parameters={"type": "object", "properties": {}},
                    permission="exec",
                    handler=write_once,
                )
            ],
            ctx={
                "chat_id": 302,
                "codex_session_key": "telegram:primary:302",
                "codex_request_lane": "background",
            },
            internal_followup=False,
            user_text="write it",
            images=None,
            allow_tool_catalog_expansion=False,
        )

        assert reply == "ok"
        assert tool_runs == 1
        assert len(_RecoveringToolClient.instances) == 2
        recovered_input = _call(_RecoveringToolClient.instances[1], "turn/start")["input"]
        assert "TOOL:\n" in recovered_input[0]["text"]
        assert "written" in recovered_input[0]["text"]

    asyncio.run(scenario())


def test_provider_tool_bridge_preserves_result_when_pending_execution_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import octopal.runtime.octo.router as router

    started = asyncio.Event()
    release = asyncio.Event()
    executions = 0

    async def fail_after_start(**kwargs: object) -> tuple[Any, dict[str, Any]]:
        nonlocal executions
        del kwargs
        executions += 1
        started.set()
        await release.wait()
        raise RuntimeError("bridge infrastructure failed")

    monkeypatch.setattr(router, "_handle_tool_call_at_most_once", fail_after_start)
    bridge = router._ProviderToolExecutorBridge(
        provider=type("Provider", (), {"provider_id": "codex"})(),
        tools=list,
        ctx={},
        ledger={},
        recovery_generation=lambda: 0,
    )
    call = {
        "id": "call-ambiguous",
        "type": "function",
        "function": {"name": "write_once", "arguments": "{}"},
    }

    async def scenario() -> None:
        execution = asyncio.create_task(bridge.execute(call))
        await started.wait()
        messages: list[Message | dict[str, Any]] = []
        append = asyncio.create_task(bridge.append_unconsumed(messages))
        await asyncio.sleep(0)
        release.set()

        with pytest.raises(RuntimeError, match="bridge infrastructure failed"):
            await execution
        await append

        assert executions == 1
        assert bridge.has_activity is True
        assert messages[0] == {"role": "assistant", "tool_calls": [call]}
        assert messages[1]["role"] == "tool"
        assert messages[1]["tool_call_id"] == "call-ambiguous"
        assert "ambiguous_tool_execution" in str(messages[1]["content"])

    asyncio.run(scenario())


def test_provider_tool_bridge_preserves_deferred_failure_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import octopal.runtime.octo.router as router

    executions = 0

    async def fail_deferred(**kwargs: object) -> tuple[Any, dict[str, Any]]:
        nonlocal executions
        del kwargs
        executions += 1
        raise RuntimeError("deferred bridge infrastructure failed")

    monkeypatch.setattr(router, "_handle_tool_call_at_most_once", fail_deferred)
    bridge = router._ProviderToolExecutorBridge(
        provider=type("Provider", (), {"provider_id": "codex"})(),
        tools=list,
        ctx={},
        ledger={},
        recovery_generation=lambda: 0,
    )
    call = {
        "id": "call-deferred-ambiguous",
        "type": "function",
        "function": {
            "name": "octo_continue_from_control_route",
            "arguments": "{}",
        },
    }

    async def scenario() -> None:
        execution = await bridge.execute(call)
        assert execution["success"] is True
        assert "deferred" in str(execution["content"])

        messages: list[Message | dict[str, Any]] = []
        await bridge.append_unconsumed(messages)

        assert executions == 1
        assert messages[0] == {"role": "assistant", "tool_calls": [call]}
        assert messages[1]["role"] == "tool"
        assert messages[1]["tool_call_id"] == "call-deferred-ambiguous"
        assert "ambiguous_tool_execution" in str(messages[1]["content"])

    asyncio.run(scenario())


def test_provider_tool_bridge_reuses_completed_deferred_result_without_replay() -> None:
    import octopal.runtime.octo.router as router

    executions = 0

    async def continue_route(args: dict[str, Any], ctx: dict[str, object]) -> str:
        nonlocal executions
        del ctx
        assert args == {"task": "finish once"}
        executions += 1
        return json.dumps({"status": "continued"})

    bridge = router._ProviderToolExecutorBridge(
        provider=type("Provider", (), {"provider_id": "codex"})(),
        tools=lambda: [
            ToolSpec(
                name="octo_continue_from_control_route",
                description="continue through the normal route",
                parameters={"type": "object", "properties": {}},
                permission="self_control",
                handler=continue_route,
                is_async=True,
            )
        ],
        ctx={},
        ledger={},
        recovery_generation=lambda: 0,
    )
    first_call = {
        "id": "call-continue-first",
        "type": "function",
        "function": {
            "name": "octo_continue_from_control_route",
            "arguments": json.dumps({"task": "finish once"}),
        },
    }
    repeated_call = {**first_call, "id": "call-continue-repeated"}

    async def scenario() -> None:
        first_execution = await bridge.execute(first_call)
        assert "deferred" in str(first_execution["content"])
        assert await bridge.drain_deferred() == 1
        assert bridge.take(first_call) is not None

        repeated_execution = await bridge.execute(repeated_call)

        assert repeated_execution["success"] is True
        assert "continued" in str(repeated_execution["content"])
        assert bridge.take(repeated_call) is not None
        assert executions == 1

    asyncio.run(scenario())


def test_main_route_reuses_independent_planner_and_executor_sessions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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

    tool_spec = ToolSpec(
        name="lookup",
        description="lookup",
        parameters={"type": "object", "properties": {}},
        permission="read",
        handler=lambda: None,
    )
    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_needs_action_or_blocked_retry", no_action_retry)
    monkeypatch.setattr(router, "_finalize_response", finalize_response)
    monkeypatch.setattr(
        router,
        "_get_octo_tools",
        lambda octo, chat_id: ([tool_spec], {"octo": octo, "chat_id": chat_id}),
    )

    async def scenario() -> None:
        provider = CodexProvider(_settings(tmp_path))
        for text in ("first", "second"):
            assert (
                await route_or_reply(
                    _Octo(),
                    provider,
                    _Memory(),
                    text,
                    211619002,
                    "",
                    conversation_scope="primary",
                    channel_context={"source_channel": "telegram"},
                )
                == "ok"
            )

        planner_created, executor_created, planner_resumed, executor_resumed = (
            _FakeCodexClient.instances
        )
        assert _call(planner_created, "thread/start").get("dynamicTools") is None
        assert _call(executor_created, "thread/start")["dynamicTools"][0]["name"] == "lookup"
        assert _call(planner_resumed, "thread/resume")["threadId"] == "thread-1"
        assert _call(executor_resumed, "thread/resume")["threadId"] == "thread-2"
        registry = json.loads((tmp_path / "codex_sessions.json").read_text())
        assert len(registry["sessions"]) == 2

    asyncio.run(scenario())


def test_main_conversation_planner_reply_passes_scoped_session_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Provider:
        provider_id = "codex"

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
        background_response = await route_or_reply(
            _Octo(),
            provider,
            _Memory(),
            "internal follow-up",
            211619002,
            "",
            conversation_scope="primary",
            channel_context={"source_channel": "telegram"},
            background_delivery=True,
        )

        assert response == "Hello from Alice."
        assert background_response == "Hello from Alice."
        assert provider.complete_kwargs == [
            {
                "codex_session_key": "telegram:primary:211619002",
                "codex_request_lane": "interactive",
            },
            {
                "codex_session_key": "telegram:primary:211619002",
                "codex_request_lane": "background",
            },
        ]

    asyncio.run(scenario())


def test_public_telegram_turn_and_continuation_keep_session_identity_and_lanes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from octopal.channels.telegram import handlers as telegram_handlers
    from octopal.runtime.octo.core import Octo
    from octopal.runtime.octo.prompt_builder import BootstrapContext
    from octopal.tools.catalog import _tool_octo_continue_from_control_route

    class _Provider:
        provider_id = "codex"

        def __init__(self) -> None:
            self.complete_kwargs: list[dict[str, object]] = []

        async def complete(self, messages, **kwargs: object) -> str:
            del messages
            self.complete_kwargs.append(dict(kwargs))
            return json.dumps(
                {
                    "mode": "reply",
                    "steps": [],
                    "response": f"Alice response {len(self.complete_kwargs)}",
                }
            )

    class _Memory:
        async def add_message(self, role, content, metadata=None) -> None:
            del role, content, metadata

    class _Bot:
        async def set_message_reaction(self, **kwargs: object) -> None:
            del kwargs

    async def fake_bootstrap_context(store: object, chat_id: int) -> BootstrapContext:
        del store, chat_id
        return BootstrapContext(content="", hash="", files=[])

    async def fake_build_octo_prompt(**kwargs: object) -> list[Message]:
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def no_action_retry(**kwargs: object) -> bool:
        del kwargs
        return False

    async def finalize_response(**kwargs: object) -> str:
        return str(kwargs["response_text"])

    queued: list[str] = []

    async def fake_enqueue_send(
        bot: object,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
    ) -> None:
        del bot, chat_id, reply_to_message_id
        queued.append(text)

    import octopal.runtime.octo.core as octo_core
    import octopal.runtime.octo.router as router

    monkeypatch.setattr(octo_core, "build_bootstrap_context_prompt", fake_bootstrap_context)
    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_needs_action_or_blocked_retry", no_action_retry)
    monkeypatch.setattr(router, "_finalize_response", finalize_response)
    monkeypatch.setattr(
        router,
        "_get_octo_tools",
        lambda octo, chat_id: ([], {"octo": octo, "chat_id": chat_id}),
    )
    monkeypatch.setattr(telegram_handlers, "_enqueue_send", fake_enqueue_send)

    provider = _Provider()
    sent: list[str] = []

    async def internal_send(chat_id: int, text: str) -> None:
        del chat_id
        sent.append(text)

    octo = Octo(
        provider=provider,
        store=object(),
        policy=object(),
        runtime=object(),
        approvals=object(),
        memory=_Memory(),
        canon=object(),
        internal_send=internal_send,
    )
    settings = _settings(tmp_path)
    flush = telegram_handlers._flush_pending_turn_factory(octo, settings, _Bot())
    session_key = "telegram:default:211619002"

    async def scenario() -> None:
        await flush(
            211619002,
            "hello",
            [],
            [],
            {"reply_to_message_id": 42, "is_group_chat": False},
        )
        continuation = await _tool_octo_continue_from_control_route(
            {"task": "finish the existing request", "notify_user": True},
            {
                "octo": octo,
                "chat_id": 211619002,
                "codex_session_key": session_key,
            },
        )
        assert json.loads(continuation)["status"] == "continued"

    asyncio.run(scenario())

    assert provider.complete_kwargs == [
        {"codex_session_key": session_key, "codex_request_lane": "interactive"},
        {"codex_session_key": session_key, "codex_request_lane": "background"},
    ]
    assert queued == ["Alice response 1"]
    assert sent == ["Alice response 2"]
