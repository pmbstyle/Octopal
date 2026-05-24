from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from octopal.infrastructure.logging import correlation_id_var
from octopal.runtime.octo import mcp_long_tasks
from octopal.runtime.octo.mcp_long_tasks import maybe_track_mcp_long_task
from octopal.runtime.octo.router import _handle_octo_tool_call
from octopal.tools.registry import ToolSpec


class DummyMemory:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, dict]] = []

    async def add_message(self, role: str, text: str, metadata: dict) -> None:
        self.messages.append((role, text, metadata))


class DummyMCPManager:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []

    async def call_tool(self, server_id: str, tool_name: str, args: dict):
        self.calls.append((server_id, tool_name, args))
        task_id = args.get("task_id") or args.get("job_id")
        if tool_name == "get_phone_task_status":
            payload = {"status": "completed", "task_id": task_id}
        else:
            payload = {"result": "NASA says hello from X."}
        content = SimpleNamespace(model_dump=lambda: {"type": "text", "text": json.dumps(payload)})
        return SimpleNamespace(content=[content])


class DummyOcto:
    def __init__(self) -> None:
        self.mcp_manager = DummyMCPManager()
        self.memory = DummyMemory()
        self.sent: list[tuple[int, str]] = []
        self.marked: list[str | None] = []
        self.cleared: list[str | None] = []
        self._pending_mcp_long_tasks = {}

    def mark_structured_followup_required(self, correlation_id: str | None = None) -> None:
        self.marked.append(correlation_id)

    def clear_pending_conversational_closure(self, correlation_id: str | None) -> None:
        self.cleared.append(correlation_id)

    async def internal_send(self, chat_id: int, text: str) -> None:
        self.sent.append((chat_id, text))


@pytest.mark.asyncio
async def test_phone_start_schedules_result_poll_and_marks_followup(monkeypatch) -> None:
    monkeypatch.setattr(mcp_long_tasks, "_MCP_LONG_TASK_INITIAL_DELAY_SECONDS", 0)
    monkeypatch.setattr(mcp_long_tasks, "_MCP_LONG_TASK_POLL_INTERVAL_SECONDS", 0)

    octo = DummyOcto()
    tracked = maybe_track_mcp_long_task(
        octo=octo,
        chat_id=123,
        correlation_id="turn-1",
        tool_name="mcp_glm_cellphone_start_phone_task",
        args={"query": "NASA X"},
        result=[{"type": "text", "text": '{"task_id":"phone-1","status":"running"}'}],
        server_id="glm_cellphone",
        remote_tool_name="start_phone_task",
    )

    assert tracked is True
    assert octo.marked == ["turn-1"]

    await asyncio.sleep(0.01)

    assert octo.mcp_manager.calls == [
        ("glm_cellphone", "get_phone_task_status", {"task_id": "phone-1"}),
        ("glm_cellphone", "get_phone_task_result", {"task_id": "phone-1"}),
    ]
    assert octo.sent == [(123, "NASA says hello from X.")]
    assert octo.memory.messages == [
        (
            "assistant",
            "NASA says hello from X.",
            {
                "chat_id": 123,
                "background_delivery": True,
                "mcp_long_task": True,
                "correlation_id": "turn-1",
            },
        )
    ]
    assert octo._pending_mcp_long_tasks == {}


@pytest.mark.asyncio
async def test_raw_mcp_status_call_schedules_result_poll(monkeypatch) -> None:
    monkeypatch.setattr(mcp_long_tasks, "_MCP_LONG_TASK_INITIAL_DELAY_SECONDS", 0)
    monkeypatch.setattr(mcp_long_tasks, "_MCP_LONG_TASK_POLL_INTERVAL_SECONDS", 0)

    octo = DummyOcto()
    tracked = maybe_track_mcp_long_task(
        octo=octo,
        chat_id=123,
        correlation_id="turn-raw",
        tool_name="mcp_call",
        args={
            "server_id": "glm_cellphone",
            "tool_name": "get_phone_task_status",
            "arguments": {"task_id": "phone-raw"},
        },
        result='{"status":"running"}',
    )

    assert tracked is True
    await asyncio.sleep(0.01)

    assert octo.mcp_manager.calls[0] == (
        "glm_cellphone",
        "get_phone_task_status",
        {"task_id": "phone-raw"},
    )
    assert octo.sent == [(123, "NASA says hello from X.")]


@pytest.mark.asyncio
async def test_phone_start_uses_job_id_for_glm_cellphone(monkeypatch) -> None:
    monkeypatch.setattr(mcp_long_tasks, "_MCP_LONG_TASK_INITIAL_DELAY_SECONDS", 0)
    monkeypatch.setattr(mcp_long_tasks, "_MCP_LONG_TASK_POLL_INTERVAL_SECONDS", 0)

    octo = DummyOcto()
    tracked = maybe_track_mcp_long_task(
        octo=octo,
        chat_id=123,
        correlation_id="turn-job",
        tool_name="mcp_call",
        args={
            "server_id": "glm_cellphone",
            "tool_name": "start_phone_task",
            "arguments": {"task": "Open X"},
        },
        result='{"job_id":"job-1","status":"queued"}',
    )

    assert tracked is True
    await asyncio.sleep(0.01)

    assert octo.mcp_manager.calls == [
        ("glm_cellphone", "get_phone_task_status", {"job_id": "job-1"}),
        ("glm_cellphone", "get_phone_task_result", {"job_id": "job-1"}),
    ]
    assert octo.sent == [(123, "NASA says hello from X.")]


@pytest.mark.asyncio
async def test_router_tracks_generated_phone_tool_result(monkeypatch) -> None:
    monkeypatch.setattr(mcp_long_tasks, "_MCP_LONG_TASK_INITIAL_DELAY_SECONDS", 0)
    monkeypatch.setattr(mcp_long_tasks, "_MCP_LONG_TASK_POLL_INTERVAL_SECONDS", 0)

    async def _handler(_args, _ctx):
        return [{"type": "text", "text": '{"task_id":"phone-router","status":"running"}'}]

    octo = DummyOcto()
    spec = ToolSpec(
        name="mcp_glm_cellphone_start_phone_task",
        description="start phone",
        parameters={"type": "object", "properties": {}},
        permission="mcp_exec",
        handler=_handler,
        is_async=True,
        server_id="glm_cellphone",
        remote_tool_name="start_phone_task",
    )
    call = {
        "function": {
            "name": "mcp_glm_cellphone_start_phone_task",
            "arguments": "{}",
        }
    }

    token = correlation_id_var.set("turn-router")
    try:
        result, meta = await _handle_octo_tool_call(
            call,
            [spec],
            {"octo": octo, "chat_id": 123},
        )
    finally:
        correlation_id_var.reset(token)

    assert result == [{"type": "text", "text": '{"task_id":"phone-router","status":"running"}'}]
    assert meta == {"timed_out": False, "had_error": False}
    assert octo.marked == ["turn-router"]

    await asyncio.sleep(0.01)

    assert octo.sent == [(123, "NASA says hello from X.")]


def test_plain_text_phone_result_is_not_json_quoted() -> None:
    assert mcp_long_tasks._format_result_text("plain result") == "plain result"
