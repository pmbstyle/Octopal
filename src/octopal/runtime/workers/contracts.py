from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from octopal.infrastructure.config.models import LLMConfig


class WorkerTemplate(BaseModel):
    """Pre-defined worker agent with system prompt and tools."""

    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    description: str
    system_prompt: str  # Worker's personality and purpose
    available_tools: list[str]  # Tool names this worker can use
    required_permissions: list[str]  # ["network", "fs_read", "fs_write", "exec"]
    model: str | None = None  # Optional model override for this worker
    max_thinking_steps: int = 10
    default_timeout_seconds: int = 300
    can_spawn_children: bool = False
    allowed_child_templates: list[str] = Field(default_factory=list)
    allowed_paths: list[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class TaskRequest(BaseModel):
    """Task from Octo to worker."""

    model_config = ConfigDict(frozen=True)

    worker_id: str  # Which worker template to use
    task: str  # Natural language task description
    inputs: dict[str, Any] = Field(default_factory=dict)  # Task-specific inputs
    tools: list[str] | None = None  # Override default tools if needed
    required_tool_calls: list[str] = Field(default_factory=list)
    timeout_seconds: int | None = None  # Override default timeout
    run_id: str | None = None  # Optional caller-provided execution id
    correlation_id: str | None = None
    parent_worker_id: str | None = None
    lineage_id: str | None = None
    root_task_id: str | None = None
    spawn_depth: int = 0
    allowed_paths: list[str] | None = None  # Restricted workspace paths the worker can access


class WorkerSpec(BaseModel):
    """Simplified worker specification for runtime."""

    model_config = ConfigDict(frozen=True)

    id: str
    template_id: str = ""
    template_name: str | None = None
    task: str
    inputs: dict[str, Any]
    system_prompt: str
    available_tools: list[str]
    required_tool_calls: list[str] = Field(default_factory=list)
    mcp_tools: list[dict[str, Any]] = Field(default_factory=list)
    model: str | None = None
    llm_config: LLMConfig | None = None
    granted_capabilities: list[dict[str, Any]]  # From policy engine
    timeout_seconds: int
    max_thinking_steps: int
    run_id: str = ""
    lifecycle: str = "ephemeral"
    correlation_id: str | None = None
    parent_worker_id: str | None = None
    lineage_id: str | None = None
    root_task_id: str | None = None
    spawn_depth: int = 0
    effective_permissions: list[str] = Field(default_factory=list)
    allowed_paths: list[str] | None = None


class KnowledgeProposal(BaseModel):
    """Proposal for canonical memory."""

    model_config = ConfigDict(frozen=True)

    category: str  # "fact", "decision", "failure"
    content: str


class WorkerResult(BaseModel):
    """Worker result with optional questions for Octo."""

    model_config = ConfigDict(frozen=True)

    status: Literal["completed", "failed", "awaiting_instruction"] = "completed"
    summary: str
    output: dict[str, Any] | None = None
    questions: list[str] = Field(default_factory=list)  # Questions for Octo
    knowledge_proposals: list[KnowledgeProposal] = Field(default_factory=list)
    thinking_steps: int = 0
    tools_used: list[str] = Field(default_factory=list)


class WorkerInstructionRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    request_id: str
    worker_id: str
    target: Literal["octo", "parent"] = "octo"
    question: str
    context: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 120
    created_at: datetime


class ChildWorkerOutcome(BaseModel):
    model_config = ConfigDict(frozen=True)

    worker_id: str
    status: str
    summary: str | None = None
    output: dict[str, Any] | None = None
    error: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ChildBatchResume(BaseModel):
    model_config = ConfigDict(frozen=True)

    worker_ids: list[str] = Field(default_factory=list)
    completed_count: int = 0
    failed_count: int = 0
    stopped_count: int = 0
    missing_count: int = 0
    awaiting_instruction_count: int = 0
    status: str = "completed"
    completed: list[ChildWorkerOutcome] = Field(default_factory=list)
    failed: list[ChildWorkerOutcome] = Field(default_factory=list)
    stopped: list[ChildWorkerOutcome] = Field(default_factory=list)
    missing: list[ChildWorkerOutcome] = Field(default_factory=list)
    awaiting_instruction: list[ChildWorkerOutcome] = Field(default_factory=list)


class Capability(BaseModel):
    """Permission capability (kept for policy engine compatibility)."""

    model_config = ConfigDict(frozen=True)

    type: str
    scope: str
    read_only: bool = False
