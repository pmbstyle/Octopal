from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from octopal.infrastructure.store.base import Store
from octopal.infrastructure.store.models import OctoDiaryEntryRecord
from octopal.utils import utc_now


@dataclass
class ReflectionService:
    store: Store
    owner_id: str = "default"

    def record_context_reset(self, chat_id: int, handoff: dict[str, Any]) -> OctoDiaryEntryRecord:
        created_at = utc_now()
        goal_now = str(handoff.get("goal_now", "") or "").strip()
        next_step = str(handoff.get("next_step", "") or "").strip()
        reason = str(handoff.get("reason", "") or "").strip() or "context reset"
        summary_parts = [f"reset: {reason}"]
        if goal_now:
            summary_parts.append(f"goal: {goal_now}")
        if next_step:
            summary_parts.append(f"next: {next_step}")
        summary = " | ".join(summary_parts)[:600]

        record = OctoDiaryEntryRecord(
            id=str(uuid.uuid4()),
            owner_id=self.owner_id,
            chat_id=chat_id,
            kind="context_reset",
            summary=summary,
            details={
                "reason": reason,
                "goal_now": goal_now,
                "next_step": next_step,
                "open_threads": list(handoff.get("open_threads") or []),
                "critical_constraints": list(handoff.get("critical_constraints") or []),
                "health_snapshot": dict(handoff.get("health_snapshot") or {}),
            },
            created_at=created_at,
        )
        self.store.add_octo_diary_entry(record)
        return record

    def list_recent(self, chat_id: int | None = None, limit: int = 5) -> list[OctoDiaryEntryRecord]:
        return self.store.list_octo_diary_entries(
            self.owner_id,
            chat_id=chat_id,
            limit=limit,
        )

    def build_wakeup_context(self, chat_id: int, limit: int = 2, max_chars: int = 600) -> str:
        context, _ = self.build_wakeup_context_with_ids(
            chat_id,
            limit=limit,
            max_chars=max_chars,
        )
        return context

    def build_wakeup_context_with_ids(
        self, chat_id: int, limit: int = 2, max_chars: int = 600
    ) -> tuple[str, list[str]]:
        entries = self.list_recent(chat_id=chat_id, limit=limit)
        if not entries:
            return "", []
        lines = ["Recent reflection relevant to this wake-up:"]
        selected_ids: list[str] = []
        for entry in reversed(entries):
            candidate = "\n".join([*lines, f"- {entry.summary}"])
            if len(candidate) <= max_chars:
                lines.append(f"- {entry.summary}")
                selected_ids.append(f"octo_diary:{entry.id}")
                continue
            suffix = "\n...[reflection truncated]..."
            remaining = max(0, max_chars - len("\n".join(lines)) - len(suffix) - 1)
            if remaining:
                lines.append(f"- {entry.summary}"[:remaining].rstrip())
                selected_ids.append(f"octo_diary:{entry.id}")
            return "\n".join(lines).rstrip() + suffix, selected_ids
        return "\n".join(lines), selected_ids
