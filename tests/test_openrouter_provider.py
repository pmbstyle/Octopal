from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx

from broodmind.infrastructure.config.settings import Settings
from broodmind.infrastructure.providers.base import Message
from broodmind.infrastructure.providers.openrouter_provider import (
    OpenRouterProvider,
    _extract_content,
    _extract_tool_calls,
    _serialize_message,
    _truncate,
)


def _settings(**overrides) -> Settings:
    defaults = dict(
        telegram_bot_token="test-token",
        llm_provider="openrouter",
        litellm_num_retries=0,
        litellm_timeout=30.0,
        litellm_fallbacks=None,
        litellm_drop_params=True,
        litellm_caching=False,
        openrouter_api_key="router-key",
        openrouter_model="anthropic/claude-sonnet-4",
        openrouter_base_url="https://openrouter.ai/api/v1/",
        openrouter_timeout=30.0,
        zai_api_key=None,
        zai_model="glm-5",
        zai_base_url="https://api.z.ai/api/coding/paas/v4",
        zai_chat_path="/chat/completions",
        zai_timeout_seconds=45.0,
        zai_connect_timeout_seconds=15.0,
        zai_accept_language="en-US,en",
        debug_prompts=False,
    )
    defaults.update(overrides)
    return Settings.model_construct(**defaults)


class _ResponseStub:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.request = httpx.Request("POST", "https://openrouter.ai/api/v1/chat/completions")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "boom",
                request=self.request,
                response=httpx.Response(self.status_code, request=self.request),
            )

    def json(self) -> dict:
        return self._payload


class _AsyncClientStub:
    response_payload: dict = {}
    response_status: int = 200
    captured_calls: list[dict] = []

    def __init__(self, *, base_url: str, timeout: httpx.Timeout) -> None:
        self.base_url = base_url
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def post(self, path: str, *, headers: dict, json: dict):
        self.__class__.captured_calls.append(
            {
                "base_url": self.base_url,
                "timeout_read": self.timeout.read,
                "timeout_connect": self.timeout.connect,
                "path": path,
                "headers": headers,
                "json": json,
            }
        )
        return _ResponseStub(self.__class__.response_payload, self.__class__.response_status)


def test_serialize_message_supports_dataclass_and_dict() -> None:
    msg = Message(role="user", content="hello")

    assert _serialize_message(msg) == {"role": "user", "content": "hello"}
    assert _serialize_message({"role": "assistant", "content": "ok"}) == {
        "role": "assistant",
        "content": "ok",
    }


def test_extract_content_and_tool_calls_handle_common_shapes() -> None:
    tool_call = {"id": "call-1", "function": {"name": "search", "arguments": "{}"}}
    response = {"choices": [{"message": {"content": "done", "tool_calls": tool_call}}]}

    assert _extract_content(response) == "done"
    assert _extract_tool_calls(response) == [tool_call]
    assert _extract_content({"choices": "bad-shape"}) == ""
    assert _extract_tool_calls({"choices": "bad-shape"}) == []


def test_truncate_short_and_long_text() -> None:
    assert _truncate("short") == "short"
    long_text = "x" * 450
    truncated = _truncate(long_text)
    assert truncated.startswith("x" * 400)
    assert "[truncated 450 bytes]" in truncated


def test_complete_posts_expected_payload(monkeypatch) -> None:
    _AsyncClientStub.response_payload = {"choices": [{"message": {"content": "router-ok"}}]}
    _AsyncClientStub.response_status = 200
    _AsyncClientStub.captured_calls = []
    monkeypatch.setattr("broodmind.infrastructure.providers.openrouter_provider.httpx.AsyncClient", _AsyncClientStub)

    provider = OpenRouterProvider(_settings())
    result = asyncio.run(provider.complete([Message(role="user", content="hello")], temperature=0.7))

    assert result == "router-ok"
    assert len(_AsyncClientStub.captured_calls) == 1
    call = _AsyncClientStub.captured_calls[0]
    assert call["base_url"] == "https://openrouter.ai/api/v1"
    assert call["path"] == "/chat/completions"
    assert call["headers"]["Authorization"] == "Bearer router-key"
    assert call["json"]["model"] == "anthropic/claude-sonnet-4"
    assert call["json"]["temperature"] == 0.7
    assert call["json"]["messages"] == [{"role": "user", "content": "hello"}]


def test_complete_with_tools_returns_content_and_tool_calls(monkeypatch) -> None:
    tool_call = {"id": "call-1", "function": {"name": "search", "arguments": "{}"}}
    _AsyncClientStub.response_payload = {
        "choices": [{"message": {"content": "need-tool", "tool_calls": [tool_call]}}]
    }
    _AsyncClientStub.response_status = 200
    _AsyncClientStub.captured_calls = []
    monkeypatch.setattr("broodmind.infrastructure.providers.openrouter_provider.httpx.AsyncClient", _AsyncClientStub)

    provider = OpenRouterProvider(_settings())
    result = asyncio.run(
        provider.complete_with_tools(
            [{"role": "user", "content": "search docs"}],
            tools=[{"function": {"name": "search"}}],
            tool_choice="required",
            temperature=0.1,
        )
    )

    assert result == {"content": "need-tool", "tool_calls": [tool_call]}
    call = _AsyncClientStub.captured_calls[0]
    assert call["json"]["tool_choice"] == "required"
    assert call["json"]["tools"] == [{"function": {"name": "search"}}]
    assert call["json"]["temperature"] == 0.1


def test_complete_stream_invokes_partial_callback() -> None:
    provider = OpenRouterProvider(_settings())
    seen: list[str] = []

    async def _fake_complete(messages, **kwargs):
        return "streamed"

    async def _on_partial(text: str) -> None:
        seen.append(text)

    provider.complete = _fake_complete  # type: ignore[method-assign]
    result = asyncio.run(provider.complete_stream([{"role": "user", "content": "hi"}], on_partial=_on_partial))

    assert result == "streamed"
    assert seen == ["streamed"]


def test_complete_raises_runtime_error_for_http_failure(monkeypatch) -> None:
    _AsyncClientStub.response_payload = {"error": "bad"}
    _AsyncClientStub.response_status = 401
    _AsyncClientStub.captured_calls = []
    monkeypatch.setattr("broodmind.infrastructure.providers.openrouter_provider.httpx.AsyncClient", _AsyncClientStub)

    provider = OpenRouterProvider(_settings())

    try:
        asyncio.run(provider.complete([{"role": "user", "content": "hello"}]))
    except RuntimeError as exc:
        assert "OpenRouter request failed" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError for HTTP failure")


def test_complete_requires_api_key() -> None:
    provider = OpenRouterProvider(_settings(openrouter_api_key=""))

    try:
        asyncio.run(provider.complete([{"role": "user", "content": "hello"}]))
    except RuntimeError as exc:
        assert "OPENROUTER_API_KEY is not set" in str(exc)
    else:
        raise AssertionError("Expected missing key failure")
