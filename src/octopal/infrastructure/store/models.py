from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class WorkerRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    status: str
    task: str
    granted_caps: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    # Worker results (populated when completed)
    summary: str | None = None
    output: dict[str, Any] | None = None
    error: str | None = None
    tools_used: list[str] = Field(default_factory=list)
    lineage_id: str | None = None
    parent_worker_id: str | None = None
    root_task_id: str | None = None
    spawn_depth: int = 0
    template_id: str | None = None
    template_name: str | None = None


class IntentRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    worker_id: str
    type: str
    payload: dict[str, Any]
    payload_hash: str
    risk: str
    requires_approval: bool
    status: str
    created_at: datetime


class WorkerTemplateRecord(BaseModel):
    """Worker template - pre-defined agent with system prompt."""

    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    description: str
    system_prompt: str  # Worker's personality and purpose
    available_tools: list[str]  # Tool names this worker can use
    required_permissions: list[str]  # ["network", "fs_read", "fs_write", "exec"]
    model: str | None = None  # Optional model override
    max_thinking_steps: int = 10
    default_timeout_seconds: int = 300
    can_spawn_children: bool = False
    allowed_child_templates: list[str] = Field(default_factory=list)
    allowed_paths: list[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class PermitRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    intent_id: str
    intent_type: str
    worker_id: str
    payload_hash: str
    expires_at: datetime
    consumed_at: datetime | None = None
    created_at: datetime


class AuditEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    ts: datetime
    correlation_id: str | None = None
    level: Literal["debug", "info", "warning", "error", "critical"]
    event_type: str
    data: dict[str, Any] = Field(default_factory=dict)


class MemoryEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    role: str
    content: str
    embedding: list[float] | None = None
    created_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class MemoryFactRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    owner_id: str
    subject: str
    key: str
    value_text: str
    value_json: dict[str, Any] | None = None
    fact_type: str
    confidence: float
    status: str
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    facets: list[str] = Field(default_factory=list)
    source_kind: str | None = None
    source_ref: str | None = None
    created_at: datetime
    updated_at: datetime


class MemoryFactSourceRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    fact_id: str
    memory_entry_uuid: str | None = None
    canon_filename: str | None = None
    source_note: str | None = None
    created_at: datetime


class OctoDiaryEntryRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    owner_id: str
    chat_id: int | None = None
    kind: str
    summary: str
    details: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class OperationalMemoryItemRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    owner_id: str
    chat_id: int | None = None
    kind: str
    statement: str
    next_action: str | None = None
    status: str
    priority: int = 2
    confidence: float = 0.5
    source_kind: str | None = None
    source_ref: str | None = None
    plan_run_id: str | None = None
    plan_step_id: str | None = None
    evidence: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None = None


class PlanRunRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    goal: str
    status: str
    chat_id: int | None = None
    source: str = "adhoc"
    correlation_id: str | None = None
    current_step_id: str | None = None
    plan: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None


class PlanStepRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str
    step_id: str
    seq: int
    kind: str
    title: str
    status: str
    task: str | None = None
    executor: str | None = None
    worker_run_id: str | None = None
    input: dict[str, Any] = Field(default_factory=dict)
    output: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None


class PlanEventRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    run_id: str
    event_type: str
    step_id: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
