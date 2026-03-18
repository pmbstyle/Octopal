from __future__ import annotations

import asyncio
from types import SimpleNamespace

from broodmind.infrastructure.config.settings import Settings
from broodmind.infrastructure.providers.litellm_provider import LiteLLMProvider, _serialize_message


def _settings() -> Settings:
    return Settings.model_construct(
        telegram_bot_token="test-token",
        llm_provider="litellm",
        litellm_num_retries=0,
        litellm_timeout=30.0,
        litellm_max_concurrency=2,
        litellm_rate_limit_max_retries=2,
        litellm_rate_limit_base_delay_seconds=1.0,
        litellm_rate_limit_max_delay_seconds=30.0,
        litellm_fallbacks=None,
        litellm_drop_params=True,
        litellm_caching=False,
        openrouter_api_key=None,
        openrouter_model="anthropic/claude-sonnet-4",
        openrouter_base_url="https://openrouter.ai/api/v1",
        openrouter_timeout=30.0,
        zai_api_key="z-test",
        zai_model="glm-5",
        zai_base_url="https://api.z.ai/api/coding/paas/v4",
        zai_chat_path="/chat/completions",
        zai_timeout_seconds=45.0,
        zai_connect_timeout_seconds=15.0,
        zai_accept_language="en-US,en",
        debug_prompts=False,
    )


def _response(content: str):
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=content))]
    )


def test_complete_normalizes_system_only_payload_to_include_user(monkeypatch) -> None:
    captured: list[list[dict[str, str]]] = []

    async def _fake_acompletion(**kwargs):
        captured.append(kwargs["messages"])
        return _response("ok")

    monkeypatch.setattr("broodmind.infrastructure.providers.litellm_provider.acompletion", _fake_acompletion)
    provider = LiteLLMProvider(_settings())

    result = asyncio.run(provider.complete([{"role": "system", "content": "You are verifier"}]))

    assert result == "ok"
    assert captured
    sent = captured[0]
    assert any(msg.get("role") == "user" for msg in sent)


def test_complete_retries_with_strict_payload_on_1214(monkeypatch) -> None:
    captured: list[list[dict[str, str]]] = []
    calls = {"n": 0}

    async def _fake_acompletion(**kwargs):
        calls["n"] += 1
        captured.append(kwargs["messages"])
        if calls["n"] == 1:
            raise RuntimeError("Error code: 400 - {'error': {'code': '1214', 'message': 'The messages parameter is illegal.'}}")
        return _response("ok-after-retry")

    monkeypatch.setattr("broodmind.infrastructure.providers.litellm_provider.acompletion", _fake_acompletion)
    provider = LiteLLMProvider(_settings())

    result = asyncio.run(provider.complete([{"role": "system", "content": "Verifier prompt"}]))

    assert result == "ok-after-retry"
    assert calls["n"] == 2
    assert len(captured) == 2
    assert captured[1][0]["role"] == "user"
    assert "system:" in captured[1][0]["content"].lower()


def test_serialize_message_coerces_null_tool_call_content() -> None:
    serialized = _serialize_message(
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "call-1",
                    "type": "function",
                    "function": {"name": "dummy_tool", "arguments": "{}"},
                }
            ],
        }
    )

    assert serialized["content"] == ""
    assert serialized["tool_calls"][0]["function"]["name"] == "dummy_tool"


def test_complete_retries_once_when_client_was_closed(monkeypatch) -> None:
    calls = {"n": 0}

    async def _fake_acompletion(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("Cannot send a request, as the client has been closed.")
        return _response("ok-after-client-retry")

    monkeypatch.setattr("broodmind.infrastructure.providers.litellm_provider.acompletion", _fake_acompletion)
    provider = LiteLLMProvider(_settings())

    result = asyncio.run(provider.complete([{"role": "user", "content": "hello"}]))

    assert result == "ok-after-client-retry"
    assert calls["n"] == 2


def test_complete_records_shared_cooldown_after_rate_limit(monkeypatch) -> None:
    calls = {"n": 0}
    sleeps: list[float] = []
    clock = {"now": 100.0}

    async def _fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        clock["now"] += delay

    async def _fake_acompletion(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("Error code: 429 - {'error': {'message': 'Rate limit reached for requests'}}")
        return _response("ok-after-rate-limit")

    monkeypatch.setattr("broodmind.infrastructure.providers.litellm_provider.acompletion", _fake_acompletion)
    monkeypatch.setattr("broodmind.infrastructure.providers.litellm_provider.asyncio.sleep", _fake_sleep)
    monkeypatch.setattr(
        "broodmind.infrastructure.providers.litellm_provider._compute_rate_limit_delay",
        lambda **kwargs: 1.0,
    )
    monkeypatch.setattr(LiteLLMProvider, "_now", lambda self: clock["now"])
    LiteLLMProvider._rate_limit_cooldowns.clear()
    provider = LiteLLMProvider(_settings())

    result = asyncio.run(provider.complete([{"role": "user", "content": "hello"}]))

    assert result == "ok-after-rate-limit"
    assert calls["n"] == 2
    assert sleeps == [1.0]
    key = provider._shared_rate_limit_key()
    assert LiteLLMProvider._rate_limit_cooldowns[key] == 101.0


def test_complete_respects_shared_cooldown_from_other_instance(monkeypatch) -> None:
    sleeps: list[float] = []
    clock = {"now": 50.0}

    async def _fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        clock["now"] += delay

    async def _fake_acompletion(**kwargs):
        return _response("ok")

    monkeypatch.setattr("broodmind.infrastructure.providers.litellm_provider.acompletion", _fake_acompletion)
    monkeypatch.setattr("broodmind.infrastructure.providers.litellm_provider.asyncio.sleep", _fake_sleep)
    monkeypatch.setattr(LiteLLMProvider, "_now", lambda self: clock["now"])
    LiteLLMProvider._rate_limit_cooldowns.clear()

    provider_a = LiteLLMProvider(_settings())
    provider_b = LiteLLMProvider(_settings())
    provider_a._record_shared_rate_limit_cooldown(3.5)

    result = asyncio.run(provider_b.complete([{"role": "user", "content": "hello"}]))

    assert result == "ok"
    assert sleeps == [3.5]


def test_complete_with_tools_downgrades_response_format_to_json_object(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    async def _fake_acompletion(**kwargs):
        calls.append(dict(kwargs))
        response_format = kwargs.get("response_format")
        if isinstance(response_format, dict) and response_format.get("type") == "json_schema":
            raise RuntimeError(
                "Provider returned error: response_format is invalid, recommended val is: must be text or json_object"
            )
        return {
            "choices": [
                {
                    "message": {
                        "content": "{\"type\":\"result\",\"summary\":\"ok\"}",
                        "tool_calls": [],
                    }
                }
            ]
        }

    monkeypatch.setattr("broodmind.infrastructure.providers.litellm_provider.acompletion", _fake_acompletion)
    LiteLLMProvider._tool_response_format_modes.clear()
    provider = LiteLLMProvider(_settings())

    result = asyncio.run(
        provider.complete_with_tools(
            [{"role": "user", "content": "hello"}],
            tools=[{"type": "function", "function": {"name": "dummy"}}],
            response_format={"type": "json_schema", "json_schema": {"name": "worker_result", "schema": {"type": "object"}}},
        )
    )

    assert result["content"]
    assert len(calls) == 2
    assert calls[0]["response_format"]["type"] == "json_schema"
    assert calls[1]["response_format"]["type"] == "json_object"
    assert LiteLLMProvider._tool_response_format_modes[provider._tool_response_format_key()] == "json_object"


def test_complete_with_tools_reuses_cached_response_format_mode(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    async def _fake_acompletion(**kwargs):
        calls.append(dict(kwargs))
        return {
            "choices": [
                {
                    "message": {
                        "content": "{\"type\":\"result\",\"summary\":\"ok\"}",
                        "tool_calls": [],
                    }
                }
            ]
        }

    monkeypatch.setattr("broodmind.infrastructure.providers.litellm_provider.acompletion", _fake_acompletion)
    LiteLLMProvider._tool_response_format_modes.clear()
    provider = LiteLLMProvider(_settings())
    LiteLLMProvider._tool_response_format_modes[provider._tool_response_format_key()] = "json_object"

    asyncio.run(
        provider.complete_with_tools(
            [{"role": "user", "content": "hello"}],
            tools=[{"type": "function", "function": {"name": "dummy"}}],
            response_format={"type": "json_schema", "json_schema": {"name": "worker_result", "schema": {"type": "object"}}},
        )
    )

    assert len(calls) == 1
    assert calls[0]["response_format"]["type"] == "json_object"


def test_complete_with_tools_downgrades_to_no_response_format_when_needed(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    async def _fake_acompletion(**kwargs):
        calls.append(dict(kwargs))
        response_format = kwargs.get("response_format")
        if response_format is not None:
            raise RuntimeError("response_format unsupported")
        return {
            "choices": [
                {
                    "message": {
                        "content": "{\"type\":\"result\",\"summary\":\"ok\"}",
                        "tool_calls": [],
                    }
                }
            ]
        }

    monkeypatch.setattr("broodmind.infrastructure.providers.litellm_provider.acompletion", _fake_acompletion)
    LiteLLMProvider._tool_response_format_modes.clear()
    provider = LiteLLMProvider(_settings())

    asyncio.run(
        provider.complete_with_tools(
            [{"role": "user", "content": "hello"}],
            tools=[{"type": "function", "function": {"name": "dummy"}}],
            response_format={"type": "json_schema", "json_schema": {"name": "worker_result", "schema": {"type": "object"}}},
        )
    )

    assert [call.get("response_format", {"type": "none"}).get("type", "none") if isinstance(call.get("response_format"), dict) else "none" for call in calls] == [
        "json_schema",
        "json_object",
        "none",
    ]
    assert LiteLLMProvider._tool_response_format_modes[provider._tool_response_format_key()] == "none"
