from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from octopal.runtime.intents.types import IntentRequest
from octopal.runtime.tool_errors import ToolBridgeError
from octopal.runtime.workers.contracts import WorkerResult, WorkerSpec
from octopal.worker_sdk.intents import http_get
from octopal.worker_sdk.protocol import VALID_MESSAGE_TYPES
from octopal.worker_sdk.worker import Worker


def _worker() -> Worker:
    return Worker(
        spec=WorkerSpec(
            id="writer",
            template_id="writer",
            template_name="Writer",
            task="Write a summary",
            inputs={},
            system_prompt="Be concise",
            available_tools=["fs_read"],
            granted_capabilities=[],
            timeout_seconds=30,
            max_thinking_steps=5,
        )
    )


def test_http_get_builds_expected_intent_request() -> None:
    request = http_get("https://example.com", headers={"Accept": "application/json"})

    assert request == IntentRequest(
        type="http.get",
        payload={"url": "https://example.com", "headers": {"Accept": "application/json"}},
    )


def test_valid_message_types_exposes_expected_protocol_surface() -> None:
    assert "await_children" in VALID_MESSAGE_TYPES
    assert "resume_children" in VALID_MESSAGE_TYPES
    assert "instruction_request" in VALID_MESSAGE_TYPES
    assert "resume_instruction" in VALID_MESSAGE_TYPES
    assert "intent_request" in VALID_MESSAGE_TYPES
    assert "octo_tool_result" in VALID_MESSAGE_TYPES
    assert "programmatic_read_batch" in VALID_MESSAGE_TYPES
    assert "programmatic_read_batch_result" in VALID_MESSAGE_TYPES
    assert "shutdown" in VALID_MESSAGE_TYPES


def test_worker_from_spec_file_loads_worker_spec(tmp_path: Path) -> None:
    spec_path = tmp_path / "worker.json"
    spec_path.write_text(
        json.dumps(
            {
                "id": "writer",
                "template_id": "writer",
                "task": "Summarize",
                "inputs": {"topic": "news"},
                "system_prompt": "Be concise",
                "available_tools": ["fs_read"],
                "granted_capabilities": [],
                "timeout_seconds": 30,
                "max_thinking_steps": 5,
            }
        ),
        encoding="utf-8",
    )

    worker = Worker.from_spec_file(str(spec_path))

    assert worker.spec.id == "writer"
    assert worker.spec.inputs == {"topic": "news"}


def test_worker_log_and_complete_emit_expected_messages(monkeypatch) -> None:
    worker = _worker()
    sent: list[dict] = []

    async def _fake_write_message(payload: dict) -> None:
        sent.append(payload)

    monkeypatch.setattr(worker, "_write_message", _fake_write_message)

    async def _scenario() -> None:
        await worker.log("info", "hello")
        await worker.complete(WorkerResult(summary="done", output={"ok": True}))

    asyncio.run(_scenario())

    assert sent == [
        {"type": "log", "level": "info", "message": "hello"},
        {
            "type": "result",
            "result": {
                "status": "completed",
                "summary": "done",
                "output": {"ok": True},
                "questions": [],
                "knowledge_proposals": [],
                "thinking_steps": 0,
                "tools_used": [],
            },
        },
    ]


def test_worker_add_proposal_records_structured_knowledge() -> None:
    worker = _worker()

    worker.add_proposal("fact", "Service health is green")

    assert len(worker.knowledge_proposals) == 1
    assert worker.knowledge_proposals[0].category == "fact"
    assert worker.knowledge_proposals[0].content == "Service health is green"


def test_worker_await_children_emits_suspend_request_and_returns_resume_payload(
    monkeypatch,
) -> None:
    worker = _worker()
    sent: list[dict] = []

    async def _fake_write_message(payload: dict) -> None:
        sent.append(payload)

    async def _fake_read_message() -> dict:
        return {
            "type": "resume_children",
            "child_batch": {
                "worker_ids": ["child-1"],
                "status": "completed",
                "completed": [{"worker_id": "child-1", "status": "completed", "summary": "done"}],
            },
        }

    monkeypatch.setattr(worker, "_write_message", _fake_write_message)
    monkeypatch.setattr(worker, "_read_message", _fake_read_message)

    payload = asyncio.run(worker.await_children(["child-1"]))

    assert sent == [{"type": "await_children", "worker_ids": ["child-1"]}]
    assert payload["status"] == "completed"
    assert payload["completed"][0]["worker_id"] == "child-1"


def test_worker_request_instruction_emits_request_and_returns_resume_payload(monkeypatch) -> None:
    worker = _worker()
    sent: list[dict] = []
    request_id = ""

    async def _fake_write_message(payload: dict) -> None:
        nonlocal request_id
        sent.append(payload)
        request_id = str(payload["request_id"])

    async def _fake_read_message() -> dict:
        return {
            "type": "resume_instruction",
            "request_id": request_id,
            "status": "answered",
            "instruction": "Use the narrow path.",
        }

    monkeypatch.setattr(worker, "_write_message", _fake_write_message)
    monkeypatch.setattr(worker, "_read_message", _fake_read_message)

    payload = asyncio.run(
        worker.request_instruction(
            question="Which path?",
            context={"paths": ["narrow", "broad"]},
            target="parent",
            timeout_seconds=45,
        )
    )

    assert sent[0]["type"] == "instruction_request"
    assert sent[0]["target"] == "parent"
    assert sent[0]["question"] == "Which path?"
    assert sent[0]["timeout_seconds"] == 45
    assert payload["status"] == "answered"
    assert payload["instruction"] == "Use the narrow path."


def test_worker_programmatic_read_batch_round_trips_correlated_response(monkeypatch) -> None:
    worker = _worker()
    sent: list[dict] = []
    request_id = ""

    async def _fake_write_message(payload: dict) -> None:
        nonlocal request_id
        sent.append(payload)
        request_id = str(payload["request_id"])

    async def _fake_read_message() -> dict:
        return {
            "type": "programmatic_read_batch_result",
            "request_id": request_id,
            "ok": True,
            "remaining_calls": 1,
            "result": {"completed_count": 1, "results": [{"call_id": "call-1"}]},
        }

    monkeypatch.setattr(worker, "_write_message", _fake_write_message)
    monkeypatch.setattr(worker, "_read_message", _fake_read_message)

    result = asyncio.run(
        worker.programmatic_read_batch(
            [{"call_id": "call-1", "tool_name": "web_search", "arguments": {}}]
        )
    )

    assert sent[0]["type"] == "programmatic_read_batch"
    assert sent[0]["request_id"].startswith("prb-")
    assert result["completed_count"] == 1


def test_worker_programmatic_read_batch_raises_classified_bridge_error(monkeypatch) -> None:
    worker = _worker()
    request_id = ""

    async def _fake_write_message(payload: dict) -> None:
        nonlocal request_id
        request_id = str(payload["request_id"])

    async def _fake_read_message() -> dict:
        return {
            "type": "programmatic_read_batch_result",
            "request_id": request_id,
            "ok": False,
            "remaining_calls": 0,
            "error": {
                "code": "call_budget_exhausted",
                "message": "Programmatic read call budget is exhausted",
                "details": {},
            },
        }

    monkeypatch.setattr(worker, "_write_message", _fake_write_message)
    monkeypatch.setattr(worker, "_read_message", _fake_read_message)

    with pytest.raises(ToolBridgeError) as exc_info:
        asyncio.run(worker.programmatic_read_batch([]))

    assert exc_info.value.bridge == "programmatic_read"
    assert exc_info.value.classification == "call_budget_exhausted"
    assert str(exc_info.value) == "call_budget_exhausted"


def test_worker_request_intent_returns_permit_when_hash_matches(monkeypatch) -> None:
    worker = _worker()
    sent: list[dict] = []
    request = IntentRequest(type="http.get", payload={"url": "https://example.com"})

    async def _fake_write_message(payload: dict) -> None:
        sent.append(payload)

    async def _fake_read_message() -> dict:
        return {
            "type": "permit",
            "permit": {
                "id": "permit-1",
                "intent_id": "intent-1",
                "intent_type": "http.get",
                "worker_id": "writer",
                "payload_hash": (
                    worker._hash_payload({"url": "https://example.com"})
                    if hasattr(worker, "_hash_payload")
                    else None
                ),
                "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
                "one_time": True,
                "consumed": False,
            },
        }

    from octopal.worker_sdk import worker as worker_module

    async def _fake_read_message_bound() -> dict:
        return {
            "type": "permit",
            "permit": {
                "id": "permit-1",
                "intent_id": "intent-1",
                "intent_type": "http.get",
                "worker_id": "writer",
                "payload_hash": worker_module._hash_payload({"url": "https://example.com"}),
                "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
                "one_time": True,
                "consumed": False,
            },
        }

    monkeypatch.setattr(worker, "_write_message", _fake_write_message)
    monkeypatch.setattr(worker, "_read_message", _fake_read_message_bound)

    permit = asyncio.run(worker.request_intent(request))

    assert sent == [
        {
            "type": "intent_request",
            "intent": {"type": "http.get", "payload": {"url": "https://example.com"}},
        }
    ]
    assert permit.id == "permit-1"
    assert permit.worker_id == "writer"


def test_worker_request_intent_rejects_denied_or_mismatched_response(monkeypatch) -> None:
    worker = _worker()
    request = IntentRequest(type="http.get", payload={"url": "https://example.com"})

    async def _noop_write(payload: dict) -> None:
        return None

    async def _denied_read() -> dict:
        return {"type": "permit_denied", "reason": "not allowed"}

    monkeypatch.setattr(worker, "_write_message", _noop_write)
    monkeypatch.setattr(worker, "_read_message", _denied_read)

    try:
        asyncio.run(worker.request_intent(request))
    except RuntimeError as exc:
        assert "Intent denied: not allowed" in str(exc)
    else:
        raise AssertionError("Expected permit denial")

    async def _wrong_hash_read() -> dict:
        return {
            "type": "permit",
            "permit": {
                "id": "permit-1",
                "intent_id": "intent-1",
                "intent_type": "http.get",
                "worker_id": "writer",
                "payload_hash": "wrong",
                "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
                "one_time": True,
                "consumed": False,
            },
        }

    monkeypatch.setattr(worker, "_read_message", _wrong_hash_read)

    try:
        asyncio.run(worker.request_intent(request))
    except RuntimeError as exc:
        assert "payload hash mismatch" in str(exc).lower()
    else:
        raise AssertionError("Expected payload hash mismatch")


def test_worker_notify_intent_executed_emits_verified_payload(monkeypatch) -> None:
    worker = _worker()
    sent: list[dict] = []

    async def _fake_write_message(payload: dict) -> None:
        sent.append(payload)

    monkeypatch.setattr(worker, "_write_message", _fake_write_message)

    asyncio.run(
        worker.notify_intent_executed(
            intent_id="intent-1",
            permit_id="permit-1",
            intent_type="http.get",
            executed_payload={"url": "https://example.com"},
            success=True,
            result="ok",
        )
    )

    payload = sent[0]
    assert payload["type"] == "intent_executed"
    assert payload["payload_hash_verified"] is True
    assert payload["worker_id"] == "writer"
    assert payload["result"] == "ok"


def test_worker_tool_bridge_calls_handle_success_and_errors(monkeypatch) -> None:
    worker = _worker()
    sent: list[dict] = []

    async def _fake_write_message(payload: dict) -> None:
        sent.append(payload)

    async def _mcp_success() -> dict:
        return {"type": "mcp_result", "result": {"items": 3}}

    monkeypatch.setattr(worker, "_write_message", _fake_write_message)
    monkeypatch.setattr(worker, "_read_message", _mcp_success)

    mcp_result = asyncio.run(worker.call_mcp_tool("demo", "search", {"q": "docs"}))
    assert mcp_result == {"items": 3}
    assert sent[0] == {
        "type": "mcp_call",
        "server_id": "demo",
        "tool_name": "search",
        "arguments": {"q": "docs"},
    }

    async def _octo_success() -> dict:
        return {"type": "octo_tool_result", "ok": True, "result": {"status": "ok"}}

    monkeypatch.setattr(worker, "_read_message", _octo_success)
    octo_result = asyncio.run(worker.call_octo_tool("manage_canon", {"action": "list"}))
    assert octo_result == {"status": "ok"}

    async def _mcp_error() -> dict:
        return {
            "type": "error",
            "message": "tool failed",
            "bridge": "mcp",
            "classification": "schema_mismatch",
            "retryable": False,
            "server_id": "demo",
            "tool_name": "search",
        }

    monkeypatch.setattr(worker, "_read_message", _mcp_error)
    try:
        asyncio.run(worker.call_mcp_tool("demo", "search", {"q": "docs"}))
    except ToolBridgeError as exc:
        assert "tool failed" in str(exc)
        assert exc.bridge == "mcp"
        assert exc.classification == "schema_mismatch"
        assert exc.retryable is False
    else:
        raise AssertionError("Expected MCP error")

    async def _octo_error() -> dict:
        return {"type": "octo_tool_result", "ok": False, "error": "denied"}

    monkeypatch.setattr(worker, "_read_message", _octo_error)
    try:
        asyncio.run(worker.call_octo_tool("manage_canon", {"action": "list"}))
    except RuntimeError as exc:
        assert "denied" in str(exc)
    else:
        raise AssertionError("Expected Octo tool bridge failure")
