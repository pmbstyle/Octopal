from __future__ import annotations

import json
from types import SimpleNamespace

from octopal.channels.group_addressing import (
    decide_group_addressing,
    load_recent_group_context,
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


def _semantic_review(**overrides) -> dict:
    review = {
        "is_direct_request_to_this_agent": True,
        "adds_new_information_or_decision_point": True,
        "would_reply_change_conversation_state": True,
        "loop_risk": "low",
        "silence_is_better": False,
    }
    review.update(overrides)
    return review


def test_resolve_group_addressing_identity_uses_configured_values_only() -> None:
    identity = resolve_group_addressing_identity(_settings())

    assert identity.agent_name == "Alice"
    assert identity.agent_aliases == ["Alice", "AliceBot"]
    assert identity.collective_aliases == ["Octopals", "agents"]


def test_group_addressing_uses_provider_decision_for_group_message() -> None:
    provider = _FakeProvider(
        {
            "action": "respond_all_agents",
            "confidence": 0.91,
            "reason": "collective request",
            "semantic_review": _semantic_review(),
        }
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


def test_group_addressing_preserves_semantic_review() -> None:
    provider = _FakeProvider(
        {
            "action": "ignore",
            "confidence": 0.96,
            "reason": "reply would keep agents talking to themselves",
            "semantic_review": {
                "is_direct_request_to_this_agent": True,
                "adds_new_information_or_decision_point": False,
                "would_reply_change_conversation_state": False,
                "loop_risk": "high",
                "silence_is_better": True,
                "ignored_extra": "nope",
            },
        }
    )

    async def scenario():
        return await decide_group_addressing(
            provider=provider,
            settings=_settings(),
            channel="telegram",
            chat_id=-100,
            text="Alice, any final thought?",
            sender_label="BobBot",
        )

    import asyncio

    decision = asyncio.run(scenario())

    assert decision.action == "ignore"
    assert decision.semantic_review == {
        "is_direct_request_to_this_agent": True,
        "adds_new_information_or_decision_point": False,
        "would_reply_change_conversation_state": False,
        "loop_risk": "high",
        "silence_is_better": True,
    }


def test_group_addressing_clamps_contradictory_loop_review_to_ignore() -> None:
    provider = _FakeProvider(
        {
            "action": "respond_self",
            "confidence": 0.97,
            "reason": "direct question to Alice",
            "semantic_review": {
                "is_direct_request_to_this_agent": True,
                "adds_new_information_or_decision_point": False,
                "would_reply_change_conversation_state": False,
                "loop_risk": "high",
                "silence_is_better": True,
            },
        }
    )

    async def scenario():
        return await decide_group_addressing(
            provider=provider,
            settings=_settings(),
            channel="telegram",
            chat_id=-100,
            text="Alice, anything else?",
            sender_label="BobBot",
        )

    import asyncio

    decision = asyncio.run(scenario())

    assert decision.action == "ignore"
    assert decision.should_process is False
    assert decision.confidence == 0.65
    assert "semantic review says silence is better" in decision.reason


def test_group_addressing_fails_closed_without_required_semantic_review() -> None:
    provider = _FakeProvider(
        {"action": "respond_self", "confidence": 0.91, "reason": "direct request"}
    )

    async def scenario():
        return await decide_group_addressing(
            provider=provider,
            settings=_settings(),
            channel="telegram",
            chat_id=-100,
            text="Alice, status?",
            sender_label="Slava",
        )

    import asyncio

    decision = asyncio.run(scenario())

    assert decision.action == "ignore"
    assert decision.should_process is False
    assert decision.confidence == 0.5
    assert "missing required semantic review" in decision.reason


def test_group_addressing_reply_to_agent_requires_provider_review() -> None:
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

    assert decision.action == "ignore"
    assert decision.should_process is False
    assert "no provider is available for loop-safe review" in decision.reason


def test_group_addressing_reply_to_agent_is_provider_signal_not_bypass() -> None:
    provider = _FakeProvider(
        {
            "action": "continue_thread",
            "confidence": 0.92,
            "reason": "explicit reply advances the thread",
            "semantic_review": _semantic_review(),
        }
    )

    async def scenario():
        return await decide_group_addressing(
            provider=provider,
            settings=_settings(),
            channel="telegram",
            chat_id=-100,
            text="yes, deploy it",
            reply_to_agent=True,
        )

    import asyncio

    decision = asyncio.run(scenario())

    assert decision.action == "continue_thread"
    payload = json.loads(provider.messages[-1][-1].content)
    assert payload["reply_to_agent"] is True


def test_group_addressing_passes_recent_context_to_provider() -> None:
    provider = _FakeProvider(
        {
            "action": "continue_thread",
            "confidence": 0.88,
            "reason": "follow-up",
            "semantic_review": _semantic_review(),
        }
    )

    async def scenario():
        return await decide_group_addressing(
            provider=provider,
            settings=_settings(),
            channel="telegram",
            chat_id=-100,
            text="And what about the deploy?",
            sender_label="Slava",
            recent_context=[
                ("user", "Alice, check the release status", "2026-06-11T10:00:00+00:00"),
                ("assistant", "The release is building now.", "2026-06-11T10:01:00+00:00"),
            ],
        )

    import asyncio

    decision = asyncio.run(scenario())

    assert decision.action == "continue_thread"
    payload = json.loads(provider.messages[-1][-1].content)
    assert payload["recent_context"] == [
        {
            "role": "user",
            "content": "Alice, check the release status",
            "created_at": "2026-06-11T10:00:00+00:00",
        },
        {
            "role": "assistant",
            "content": "The release is building now.",
            "created_at": "2026-06-11T10:01:00+00:00",
        },
    ]
    assert payload["agent_loop_guard"] == {
        "enabled": True,
        "reply_to_other_agents_only_for_substantive_new_work": True,
    }


def test_group_addressing_prompt_includes_agent_loop_guard() -> None:
    provider = _FakeProvider({"action": "ignore", "confidence": 0.93, "reason": "loop risk"})

    async def scenario():
        return await decide_group_addressing(
            provider=provider,
            settings=_settings(),
            channel="telegram",
            chat_id=-100,
            text="Alice, anything else to add?",
            sender_label="BobBot",
            recent_context=[
                (
                    "assistant",
                    "I already covered the deployment status.",
                    "2026-06-11T10:01:00+00:00",
                ),
                (
                    "system",
                    "Observed group-chat message.\n\nSender: BobBot\n\nMessage:\nThanks, Alice.",
                    "2026-06-11T10:02:00+00:00",
                ),
            ],
        )

    import asyncio

    decision = asyncio.run(scenario())

    system_prompt = provider.messages[-1][0].content
    assert decision.action == "ignore"
    assert "Avoid agent-to-agent loops" in system_prompt
    assert "semantic_review must be an object" in system_prompt
    assert "would_reply_change_conversation_state" in system_prompt
    assert "silence_is_better" in system_prompt
    assert "substantive question" in system_prompt
    assert "multiple agent/assistant turns since the last clear human turn" in system_prompt
    assert "agents talking to themselves" in system_prompt


def test_load_recent_group_context_reads_memory_history() -> None:
    class _Memory:
        async def get_recent_history(self, chat_id, limit=6, **kwargs):
            assert chat_id == -100
            assert limit == 8
            assert kwargs == {}
            return [
                ("user", "Alice, status?", "2026-06-11T10:00:00+00:00"),
                ("assistant", "Ready.", "2026-06-11T10:01:00+00:00"),
            ]

    async def scenario():
        return await load_recent_group_context(SimpleNamespace(memory=_Memory()), chat_id=-100)

    import asyncio

    history = asyncio.run(scenario())

    assert history == [
        ("user", "Alice, status?", "2026-06-11T10:00:00+00:00"),
        ("assistant", "Ready.", "2026-06-11T10:01:00+00:00"),
    ]


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
