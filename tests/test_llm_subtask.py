from __future__ import annotations

import asyncio
import json

from broodmind.tools.llm import subtask as subtask_module


class _ProviderStub:
    def __init__(self, response: str | Exception) -> None:
        self._response = response
        self.calls: list[list] = []

    async def complete(self, messages, **kwargs):
        self.calls.append(messages)
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


def test_run_llm_subtask_requires_prompt() -> None:
    result = asyncio.run(subtask_module.run_llm_subtask({}, _ProviderStub("{}")))
    payload = json.loads(result)
    assert "prompt" in payload["error"]


def test_run_llm_subtask_rejects_non_serializable_input() -> None:
    provider = _ProviderStub("{}")
    result = asyncio.run(
        subtask_module.run_llm_subtask({"prompt": "summarize", "input": {"bad": {1, 2}}}, provider)
    )
    payload = json.loads(result)
    assert "JSON serializable" in payload["error"]
    assert provider.calls == []


def test_run_llm_subtask_returns_provider_json_and_builds_messages() -> None:
    provider = _ProviderStub('{"status":"ok","count":2}')

    result = asyncio.run(
        subtask_module.run_llm_subtask(
            {"prompt": "summarize", "input": {"items": ["a", "b"]}},
            provider,
        )
    )

    assert json.loads(result) == {"status": "ok", "count": 2}
    assert len(provider.calls) == 1
    messages = provider.calls[0]
    assert messages[0].role == "system"
    assert "JSON-only function" in messages[0].content
    assert messages[1].role == "user"
    assert "Task: summarize" in messages[1].content
    assert '"items"' in messages[1].content


def test_run_llm_subtask_reports_invalid_json_from_provider() -> None:
    provider = _ProviderStub("not-json")
    result = asyncio.run(subtask_module.run_llm_subtask({"prompt": "summarize"}, provider))
    payload = json.loads(result)
    assert "invalid JSON" in payload["error"]


def test_run_llm_subtask_validates_schema_when_available(monkeypatch) -> None:
    class _SchemaModule:
        class ValidationError(Exception):
            def __init__(self, message: str) -> None:
                self.message = message

        @staticmethod
        def validate(*, instance, schema) -> None:
            if "status" not in instance:
                raise _SchemaModule.ValidationError("'status' is required")

    monkeypatch.setattr(subtask_module, "jsonschema", _SchemaModule)

    provider = _ProviderStub('{"count":2}')
    result = asyncio.run(
        subtask_module.run_llm_subtask(
            {
                "prompt": "summarize",
                "schema": {
                    "type": "object",
                    "required": ["status"],
                },
            },
            provider,
        )
    )
    payload = json.loads(result)
    assert "failed schema validation" in payload["error"]
    assert "status" in payload["error"]


def test_run_llm_subtask_reports_missing_jsonschema_dependency(monkeypatch) -> None:
    monkeypatch.setattr(subtask_module, "jsonschema", None)
    provider = _ProviderStub('{"status":"ok"}')

    result = asyncio.run(
        subtask_module.run_llm_subtask(
            {"prompt": "summarize", "schema": {"type": "object"}},
            provider,
        )
    )
    payload = json.loads(result)
    assert "jsonschema" in payload["error"]
    assert provider.calls == []


def test_run_llm_subtask_wraps_provider_exception() -> None:
    provider = _ProviderStub(RuntimeError("provider boom"))
    result = asyncio.run(subtask_module.run_llm_subtask({"prompt": "summarize"}, provider))
    payload = json.loads(result)
    assert "unexpected error occurred" in payload["error"]
    assert "provider boom" in payload["error"]
