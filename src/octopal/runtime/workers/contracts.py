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
    created_at: datetime
    updated_at: datetime


class TaskRequest(BaseModel):
    """Task from Octo to worker."""
    model_config = ConfigDict(frozen=True)

    worker_id: str  # Which worker template to use
    task: str  # Natural language task description
    inputs: dict[str, Any] = Field(default_factory=dict)  # Task-specific inputs
    tools: list[str] | None = None  # Override default tools if needed
    model: str | None = None  # Override model for this task
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

    status: Literal["completed", "failed"] = "completed"
    summary: str
    output: dict[str, Any] | None = None
    questions: list[str] = Field(default_factory=list)  # Questions for Octo
    knowledge_proposals: list[KnowledgeProposal] = Field(default_factory=list)
    thinking_steps: int = 0
    tools_used: list[str] = Field(default_factory=list)


class Capability(BaseModel):
    """Permission capability (kept for policy engine compatibility)."""
    model_config = ConfigDict(frozen=True)

    type: str
    scope: str
    read_only: bool = False
