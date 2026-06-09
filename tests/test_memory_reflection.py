from __future__ import annotations

import asyncio
from pathlib import Path

from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.reflection import ReflectionService
from octopal.runtime.octo.core import Octo
from octopal.runtime.octo.prompt_builder import build_octo_prompt


class _StoreSettings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


def test_reflection_service_builds_wakeup_context(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    reflection = ReflectionService(store=store, owner_id="default")
    reflection.record_context_reset(
        42,
        {
            "reason": "context overloaded",
            "goal_now": "Finish memory rollout.",
            "next_step": "Run the targeted tests.",
            "open_threads": ["facts layer"],
            "critical_constraints": ["do not break live agent"],
            "health_snapshot": {"context_size_estimate": 1234},
        },
    )

    text = reflection.build_wakeup_context(42)
    assert "Recent reflection relevant to this wake-up:" in text
    assert "Finish memory rollout." in text
    assert "Run the targeted tests." in text


def test_build_octo_prompt_includes_reflection_on_wakeup(tmp_path: Path) -> None:
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    reflection = ReflectionService(store=store, owner_id="default")
    reflection.record_context_reset(
        5,
        {
            "reason": "context overloaded",
            "goal_now": "Resume the task.",
            "next_step": "Check the latest state first.",
            "open_threads": [],
            "critical_constraints": [],
            "health_snapshot": {},
        },
    )

    class DummyMemory:
        async def get_context(self, user_text: str, exclude_chat_id: int | None = None):
            return []

        async def get_recent_history(self, chat_id: int, limit: int = 20):
            return []

    class DummyCanon:
        def get_tier1_context(self):
            return ""

    async def scenario() -> None:
        messages = await build_octo_prompt(
            store=store,
            memory=DummyMemory(),
            canon=DummyCanon(),
            user_text="continue",
            chat_id=5,
            bootstrap_context="",
            wake_notice="You woke up after a reset.",
            reflection=reflection,
        )
        merged = "\n".join(
            str(message.content) for message in messages if isinstance(message.content, str)
        )
        assert "Wake-up directive after context reset:" in merged
        assert "Recent reflection relevant to this wake-up:" in merged
        assert "Resume the task." in merged

    asyncio.run(scenario())


def test_octo_context_reset_records_reflection_entry(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(tmp_path / "workspace"))
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))
    reflection = ReflectionService(store=store, owner_id="default")

    class DummyMemory:
        async def add_message(self, role: str, content: str, metadata: dict):
            return None

    octo = Octo(
        provider=object(),
        store=store,
        policy=object(),
        runtime=object(),
        approvals=object(),
        memory=DummyMemory(),
        canon=object(),
        reflection=reflection,
    )

    async def scenario() -> None:
        result = await octo.request_context_reset(
            99,
            {
                "mode": "soft",
                "reason": "context overloaded",
                "goal_now": "Resume testing.",
                "next_step": "Review the latest handoff.",
            },
        )
        assert result["status"] == "reset_complete"

    asyncio.run(scenario())
    entries = store.list_octo_diary_entries("default", chat_id=99, limit=10)
    assert len(entries) == 1
    assert entries[0].kind == "context_reset"
    assert "Resume testing." in entries[0].summary
    handoff_text = (tmp_path / "workspace" / "memory" / "handoff.md").read_text(encoding="utf-8")
    assert "- chat_id: 99" in handoff_text
    assert "- context_health: OK" in handoff_text


def test_context_reset_default_reason_does_not_claim_overload_when_health_is_ok(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(tmp_path / "workspace"))
    store = SQLiteStore(_StoreSettings(tmp_path / "data", tmp_path / "workspace"))

    class DummyMemory:
        async def add_message(self, role: str, content: str, metadata: dict):
            return None

    octo = Octo(
        provider=object(),
        store=store,
        policy=object(),
        runtime=object(),
        approvals=object(),
        memory=DummyMemory(),
        canon=object(),
    )

    async def scenario() -> None:
        result = await octo.request_context_reset(
            77,
            {
                "mode": "soft",
                "goal_now": "Resume testing.",
                "next_step": "Review the latest handoff.",
            },
        )
        assert result["status"] == "reset_complete"
        assert result["handoff"]["reason"] == "context reset requested"
        assert result["handoff"]["health_snapshot"]["context_health"] == "OK"

    asyncio.run(scenario())
    handoff_text = (tmp_path / "workspace" / "memory" / "handoff.md").read_text(encoding="utf-8")
    assert "- reason: context reset requested" in handoff_text
    assert "- context_health: OK" in handoff_text
