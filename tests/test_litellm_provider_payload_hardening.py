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
