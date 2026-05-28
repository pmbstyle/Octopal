from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from octopal.infrastructure.store.base import Store
from octopal.infrastructure.store.models import (
    PlanEventRecord,
    PlanRunRecord,
    PlanStepRecord,
)
from octopal.utils import utc_now

PLAN_ACTIVE_STATUSES = {"planned", "running", "needs_next_step", "awaiting_worker"}
PLAN_TERMINAL_STATUSES = {"completed", "failed", "cancelled", "blocked"}
STEP_ACTIVE_STATUSES = {"running", "awaiting_worker", "awaiting_approval", "awaiting_input"}
STEP_TERMINAL_STATUSES = {"completed", "failed", "skipped", "cancelled"}


@dataclass(frozen=True)
class PlanStepSpec:
    id: str
    kind: str
    title: str
    task: str | None = None
    executor: str | None = None
    input: dict[str, Any] | None = None


class PlanRunService:
    """Small persistence-first helper for Octo's runtime plans.

    This service intentionally does not execute tools or workers. It owns the durable
    state contract that an executor, worker follow-up, or watchdog can safely resume.
    """

    def __init__(self, store: Store) -> None:
        self.store = store

    def create_run(
        self,
        *,
        goal: str,
        steps: list[dict[str, Any] | PlanStepSpec],
        chat_id: int | None = None,
        source: str = "adhoc",
        correlation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PlanRunRecord:
        normalized_steps = self._normalize_steps(steps)
        now = utc_now()
        run_id = f"plan-{uuid4()}"
        current_step_id = normalized_steps[0].id if normalized_steps else None
        plan_payload = {
            "goal": goal,
            "steps": [
                {
                    "id": step.id,
                    "kind": step.kind,
                    "title": step.title,
                    **({"task": step.task} if step.task else {}),
                    **({"executor": step.executor} if step.executor else {}),
                }
                for step in normalized_steps
            ],
        }
        run = PlanRunRecord(
            id=run_id,
            goal=goal,
            status="planned",
            chat_id=chat_id,
            source=source,
            correlation_id=correlation_id,
            current_step_id=current_step_id,
            plan=plan_payload,
            metadata=dict(metadata or {}),
            created_at=now,
            updated_at=now,
        )
        step_records = [
            PlanStepRecord(
                run_id=run_id,
                step_id=step.id,
                seq=index,
                kind=step.kind,
                title=step.title,
                status="pending",
                task=step.task,
                executor=step.executor,
                input=dict(step.input or {}),
                output={},
                created_at=now,
                updated_at=now,
            )
            for index, step in enumerate(normalized_steps)
        ]
        self.store.create_plan_run(run, step_records)
        self.append_event(
            run_id,
            "plan.created",
            data={
                "goal": goal,
                "step_count": len(step_records),
                "source": source,
            },
        )
        return run

    def get_snapshot(self, run_id: str) -> dict[str, Any] | None:
        run = self.store.get_plan_run(run_id)
        if run is None:
            return None
        steps = self.store.get_plan_steps(run_id)
        next_step = self._next_actionable_step(steps)
        return {
            "run": run.model_dump(mode="json"),
            "steps": [step.model_dump(mode="json") for step in steps],
            "next_step": next_step.model_dump(mode="json") if next_step else None,
        }

    def mark_step_running(self, run_id: str, step_id: str) -> None:
        now = utc_now()
        self.store.update_plan_run(run_id, status="running", current_step_id=step_id)
        self.store.update_plan_step(run_id, step_id, status="running", started_at=now)
        self.append_event(run_id, "step.started", step_id=step_id)

    def bind_worker_step(self, run_id: str, step_id: str, worker_run_id: str) -> None:
        self.store.update_plan_run(run_id, status="awaiting_worker", current_step_id=step_id)
        self.store.update_plan_step(
            run_id,
            step_id,
            status="awaiting_worker",
            worker_run_id=worker_run_id,
            started_at=utc_now(),
        )
        self.append_event(
            run_id,
            "step.awaiting_worker",
            step_id=step_id,
            data={"worker_run_id": worker_run_id},
        )

    def complete_step(
        self,
        run_id: str,
        step_id: str,
        *,
        output: dict[str, Any] | None = None,
    ) -> None:
        now = utc_now()
        self.store.update_plan_step(
            run_id,
            step_id,
            status="completed",
            output=dict(output or {}),
            completed_at=now,
        )
        steps = self.store.get_plan_steps(run_id)
        next_step = self._next_actionable_step(steps)
        if next_step is None:
            self.store.update_plan_run(
                run_id,
                status="completed",
                current_step_id=step_id,
                completed_at=now,
            )
            self.append_event(run_id, "plan.completed", step_id=step_id)
            return
        self.store.update_plan_run(
            run_id,
            status="needs_next_step",
            current_step_id=next_step.step_id,
        )
        self.append_event(
            run_id,
            "step.completed",
            step_id=step_id,
            data={"next_step_id": next_step.step_id},
        )

    def fail_step(self, run_id: str, step_id: str, *, error: str) -> None:
        now = utc_now()
        self.store.update_plan_step(
            run_id,
            step_id,
            status="failed",
            error=error,
            completed_at=now,
        )
        self.store.update_plan_run(
            run_id,
            status="failed",
            current_step_id=step_id,
            completed_at=now,
        )
        self.append_event(run_id, "step.failed", step_id=step_id, data={"error": error})

    def append_event(
        self,
        run_id: str,
        event_type: str,
        *,
        step_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> PlanEventRecord:
        event = PlanEventRecord(
            id=f"plan-event-{uuid4()}",
            run_id=run_id,
            step_id=step_id,
            event_type=event_type,
            data=dict(data or {}),
            created_at=utc_now(),
        )
        self.store.append_plan_event(event)
        return event

    def active_runs_for_chat(self, chat_id: int, *, limit: int = 10) -> list[PlanRunRecord]:
        return self.store.list_plan_runs(
            chat_id=chat_id,
            statuses=sorted(PLAN_ACTIVE_STATUSES),
            limit=limit,
        )

    def _normalize_steps(self, steps: list[dict[str, Any] | PlanStepSpec]) -> list[PlanStepSpec]:
        normalized: list[PlanStepSpec] = []
        seen: set[str] = set()
        for index, raw in enumerate(steps):
            if isinstance(raw, PlanStepSpec):
                step = raw
            else:
                raw_id = str(raw.get("id") or "").strip()
                step_id = raw_id or f"step_{index + 1}"
                step = PlanStepSpec(
                    id=step_id,
                    kind=str(raw.get("kind") or "octo").strip() or "octo",
                    title=str(raw.get("title") or raw.get("task") or step_id).strip(),
                    task=str(raw.get("task") or "").strip() or None,
                    executor=str(raw.get("executor") or raw.get("worker_id") or "").strip() or None,
                    input=dict(raw.get("input") or raw.get("inputs") or {}),
                )
            if step.id in seen:
                raise ValueError(f"Duplicate plan step id: {step.id}")
            if not step.id.strip():
                raise ValueError("Plan step id cannot be empty")
            seen.add(step.id)
            normalized.append(step)
        if not normalized:
            raise ValueError("Plan requires at least one step")
        return normalized

    def _next_actionable_step(self, steps: list[PlanStepRecord]) -> PlanStepRecord | None:
        for step in steps:
            if step.status == "pending":
                return step
        return None
