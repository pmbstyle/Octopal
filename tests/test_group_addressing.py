from __future__ import annotations

import json
from types import SimpleNamespace

from octopal.channels.group_addressing import (
    decide_group_addressing,
    resolve_group_addressing_identity,
)


class _FakeProvider:
    def __init__(self, payload: dict | str) -> None:
        self.payload = payload
        self.messages = []

    async def complete(self, messages, **kwargs):
        self.messages.append(messages)
        if isinstance(self.payload, str):
            return self.payload
        return json.dumps(self.payload)


def _settings(**kwargs) -> SimpleNamespace:
    defaults = {
        "group_addressing_enabled": True,
        "group_agent_name": "Alice",
        "group_agent_aliases": "Alice,AliceBot",
        "group_collective_aliases": "Octopals,agents",
        "a2a": SimpleNamespace(agent_name="Fallback"),
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_resolve_group_addressing_identity_uses_configured_values_only() -> None:
    identity = resolve_group_addressing_identity(_settings())

    assert identity.agent_name == "Alice"
    assert identity.agent_aliases == ["Alice", "AliceBot"]
    assert identity.collective_aliases == ["Octopals", "agents"]


def test_group_addressing_uses_provider_decision_for_group_message() -> None:
    provider = _FakeProvider(
        {"action": "respond_all_agents", "confidence": 0.91, "reason": "collective request"}
    )

    async def scenario():
        return await decide_group_addressing(
            provider=provider,
            settings=_settings(),
            channel="telegram",
            chat_id=-100,
            text="Octopals, update yourselves",
            sender_label="Slava",
        )

    import asyncio

    decision = asyncio.run(scenario())

    assert decision.action == "respond_all_agents"
    assert decision.should_process is True
    assert provider.messages


def test_group_addressing_reply_to_agent_continues_without_provider() -> None:
    async def scenario():
        return await decide_group_addressing(
            provider=None,
            settings=_settings(),
            channel="telegram",
            chat_id=-100,
            text="yes",
            reply_to_agent=True,
        )

    import asyncio

    decision = asyncio.run(scenario())

    assert decision.action == "continue_thread"
    assert decision.should_process is True


def test_group_addressing_is_conservative_without_provider() -> None:
    async def scenario():
        return await decide_group_addressing(
            provider=None,
            settings=_settings(),
            channel="whatsapp",
            chat_id=100,
            text="Alice, what is the status?",
        )

    import asyncio

    decision = asyncio.run(scenario())

    assert decision.action == "ignore"
    assert decision.should_process is False
