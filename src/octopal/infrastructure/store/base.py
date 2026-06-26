from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from octopal.infrastructure.store.models import (
    AuditEvent,
    IntentRecord,
    MemoryEntry,
    MemoryFactRecord,
    MemoryFactSourceRecord,
    OctoDiaryEntryRecord,
    OperationalMemoryItemRecord,
    PermitRecord,
    PlanEventRecord,
    PlanRunRecord,
    PlanStepRecord,
    WorkerRecord,
    WorkerTemplateRecord,
)

UNSET = object()


class Store(Protocol):
    def create_worker(self, record: WorkerRecord) -> None: ...

    def update_worker_status(self, worker_id: str, status: str) -> None: ...

    def update_worker_result(
        self,
        worker_id: str,
        summary: str | None = None,
        output: dict[str, Any] | None = None,
        error: str | None = None,
        tools_used: list[str] | None = None,
    ) -> None: ...

    def get_worker(self, worker_id: str) -> WorkerRecord | None: ...

    def get_active_workers(self, older_than_minutes: int = 10) -> list[WorkerRecord]: ...

    def cleanup_old_workers(
        self, keep_recent_hours: int = 24, keep_completed_count: int = 100
    ) -> int: ...

    def list_workers(self) -> list[WorkerRecord]: ...

    def upsert_worker_template(self, record: WorkerTemplateRecord) -> None: ...

    def list_worker_templates(self) -> list[WorkerTemplateRecord]: ...

    def get_worker_template(self, template_id: str) -> WorkerTemplateRecord | None: ...

    def delete_worker_template(self, template_id: str) -> None: ...

    def save_intent(self, record: IntentRecord) -> None: ...

    def update_intent_status(self, intent_id: str, status: str) -> None: ...

    def create_permit(self, record: PermitRecord) -> None: ...

    def consume_permit_atomic(self, permit_id: str, now: datetime) -> bool: ...

    def get_permit(self, permit_id: str, now: datetime) -> PermitRecord | None: ...

    def append_audit(self, event: AuditEvent) -> None: ...

    def list_audit(self, limit: int = 100) -> list[AuditEvent]: ...

    def list_audit_for_correlation(
        self, correlation_id: str, limit: int = 100
    ) -> list[AuditEvent]: ...

    def get_audit(self, event_id: str) -> AuditEvent | None: ...

    def add_memory_entry(self, entry: MemoryEntry) -> None: ...

    def list_memory_entries(self, limit: int = 200) -> list[MemoryEntry]: ...

    def list_memory_entries_for_owner(
        self, owner_id: str, limit: int = 200
    ) -> list[MemoryEntry]: ...

    def list_memory_entries_by_chat(self, chat_id: int, limit: int = 50) -> list[MemoryEntry]: ...

    def search_memory_entries_lexical(
        self,
        owner_id: str,
        query: str,
        limit: int = 80,
        exclude_chat_id: int | None = None,
    ) -> list[MemoryEntry]: ...

    def cleanup_old_memory(self, keep_days: int = 30, keep_count: int = 1000) -> int: ...
    def delete_memory_entries_by_chat(self, chat_id: int, keep_recent: int = 0) -> int: ...

    def upsert_memory_fact(self, record: MemoryFactRecord) -> None: ...

    def list_memory_facts(
        self,
        owner_id: str,
        *,
        limit: int = 100,
        status: str | None = None,
        subject: str | None = None,
        key: str | None = None,
        source_kind: str | None = None,
        source_ref: str | None = None,
    ) -> list[MemoryFactRecord]: ...

    def invalidate_memory_fact(
        self, fact_id: str, valid_to: datetime, status: str = "invalidated"
    ) -> None: ...

    def add_memory_fact_source(self, record: MemoryFactSourceRecord) -> None: ...

    def list_memory_fact_sources(self, fact_id: str) -> list[MemoryFactSourceRecord]: ...

    def add_octo_diary_entry(self, record: OctoDiaryEntryRecord) -> None: ...

    def list_octo_diary_entries(
        self,
        owner_id: str,
        *,
        chat_id: int | None = None,
        limit: int = 20,
    ) -> list[OctoDiaryEntryRecord]: ...

    def upsert_operational_memory_item(self, record: OperationalMemoryItemRecord) -> None: ...

    def list_operational_memory_items(
        self,
        owner_id: str,
        *,
        chat_id: int | None = None,
        statuses: list[str] | None = None,
        kinds: list[str] | None = None,
        limit: int = 50,
    ) -> list[OperationalMemoryItemRecord]: ...

    def update_operational_memory_item(
        self,
        item_id: str,
        *,
        status: str | None = None,
        plan_run_id: str | None = None,
        plan_step_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        resolved_at: datetime | None = None,
    ) -> None: ...

    def resolve_operational_memory_items_for_plan(
        self,
        plan_run_id: str,
        *,
        status: str,
        resolved_at: datetime,
    ) -> int: ...

    def create_plan_run(self, run: PlanRunRecord, steps: list[PlanStepRecord]) -> None: ...

    def update_plan_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        current_step_id: str | None = None,
        plan: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        completed_at: datetime | None | object = UNSET,
    ) -> None: ...

    def get_plan_run(self, run_id: str) -> PlanRunRecord | None: ...

    def list_plan_runs(
        self,
        *,
        chat_id: int | None = None,
        statuses: list[str] | None = None,
        limit: int = 50,
    ) -> list[PlanRunRecord]: ...

    def get_plan_steps(self, run_id: str) -> list[PlanStepRecord]: ...

    def get_plan_step_by_worker_run_id(
        self,
        worker_run_id: str,
        *,
        chat_id: int | None = None,
    ) -> PlanStepRecord | None: ...

    def update_plan_step(
        self,
        run_id: str,
        step_id: str,
        *,
        status: str | None = None,
        worker_run_id: str | None = None,
        output: dict[str, Any] | None = None,
        error: str | None | object = UNSET,
        started_at: datetime | None = None,
        completed_at: datetime | None | object = UNSET,
    ) -> None: ...

    def append_plan_event(self, event: PlanEventRecord) -> None: ...

    def list_plan_events(self, run_id: str, limit: int = 100) -> list[PlanEventRecord]: ...

    def is_chat_bootstrapped(self, chat_id: int) -> bool: ...

    def mark_chat_bootstrapped(self, chat_id: int, ts: datetime) -> None: ...

    def get_chat_bootstrap_hash(self, chat_id: int) -> str | None: ...

    def set_chat_bootstrap_hash(self, chat_id: int, bootstrap_hash: str, ts: datetime) -> None: ...

    def upsert_scheduled_task(
        self,
        task_id: str,
        name: str,
        frequency: str,
        task_text: str,
        description: str | None = None,
        worker_id: str | None = None,
        inputs: dict | None = None,
        enabled: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> None: ...

    def update_task_last_run(self, task_id: str, ts: datetime) -> None: ...

    def update_scheduled_task_metadata(
        self,
        task_id: str,
        metadata: dict[str, Any] | None,
    ) -> None: ...

    def get_scheduled_tasks(self, enabled_only: bool = False) -> list[dict[str, Any]]: ...

    def delete_scheduled_task(self, task_id: str) -> None: ...
