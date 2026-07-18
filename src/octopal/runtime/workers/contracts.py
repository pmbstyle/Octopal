from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from octopal.infrastructure.config.models import LLMConfig
from octopal.infrastructure.store.models import AdaptationContext, ProceduralRecipeContext
from octopal.runtime.memory.influence import require_complete_memory_influence_ids


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
    max_thinking_steps: int | None = None  # Override template reasoning-step budget
    run_id: str | None = None  # Optional caller-provided execution id
    correlation_id: str | None = None
    parent_worker_id: str | None = None
    lineage_id: str | None = None
    root_task_id: str | None = None
    spawn_depth: int = 0
    allowed_paths: list[str] | None = None  # Restricted workspace paths the worker can access
    programmatic_read_call_budget: int = Field(default=0, ge=0, le=16, strict=True)
    memory_influence_ids: list[str] = Field(default_factory=list, max_length=128)
    idempotency_key: str | None = None

    @field_validator("memory_influence_ids", mode="before")
    @classmethod
    def validate_memory_influence_ids(cls, value: object) -> list[str]:
        if not isinstance(value, (list, tuple)):
            return []
        return require_complete_memory_influence_ids(value)


class WorkerInferenceBudget(BaseModel):
    """Fail-closed inference budget attached to an isolated worker run."""

    model_config = ConfigDict(frozen=True)

    pricing_model: str = Field(min_length=1)
    max_llm_calls: int = Field(gt=0)
    max_tool_calls: int = Field(gt=0)
    max_total_tokens: int = Field(gt=0)
    max_cost_microusd: int = Field(gt=0)
    input_cost_microusd_per_million_tokens: int = Field(ge=0)
    completion_cost_microusd_per_million_tokens: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_budget(self) -> Self:
        if not self.pricing_model.strip():
            raise ValueError("pricing_model must not be blank")
        if (
            self.input_cost_microusd_per_million_tokens == 0
            and self.completion_cost_microusd_per_million_tokens == 0
        ):
            raise ValueError("at least one token cost rate must be non-zero")
        return self


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
    strict_thinking_budget: bool = False
    inference_budget: WorkerInferenceBudget | None = None
    run_id: str = ""
    lifecycle: str = "ephemeral"
    correlation_id: str | None = None
    parent_worker_id: str | None = None
    lineage_id: str | None = None
    root_task_id: str | None = None
    spawn_depth: int = 0
    effective_permissions: list[str] = Field(default_factory=list)
    allowed_paths: list[str] | None = None
    programmatic_read_call_budget: int = Field(default=0, ge=0, le=16, strict=True)
    memory_influence_ids: list[str] = Field(default_factory=list, max_length=128)
    procedural_recipes: list[ProceduralRecipeContext] = Field(default_factory=list, max_length=1)
    adaptations: list[AdaptationContext] = Field(default_factory=list, max_length=1)
    idempotency_key: str | None = None

    @field_validator("memory_influence_ids", mode="before")
    @classmethod
    def validate_memory_influence_ids(cls, value: object) -> list[str]:
        if not isinstance(value, (list, tuple)):
            return []
        return require_complete_memory_influence_ids(value)

    @model_validator(mode="after")
    def validate_inference_budget(self) -> Self:
        if self.adaptations and self.lifecycle != "benchmark":
            raise ValueError("adaptation contexts are restricted to benchmark lifecycle")
        if self.inference_budget is None:
            return self
        if not self.strict_thinking_budget:
            raise ValueError("inference_budget requires strict_thinking_budget")
        if self.max_thinking_steps <= 0 or self.max_thinking_steps > 6:
            raise ValueError("budgeted max_thinking_steps must be between 1 and 6")
        if self.inference_budget.max_llm_calls > 6:
            raise ValueError("budgeted max_llm_calls must not exceed 6")
        if self.inference_budget.max_tool_calls > 6:
            raise ValueError("budgeted max_tool_calls must not exceed 6")
        if self.programmatic_read_call_budget > self.inference_budget.max_tool_calls:
            raise ValueError(
                "programmatic_read_call_budget must not exceed budgeted max_tool_calls"
            )
        return self


class KnowledgeProposal(BaseModel):
    """Proposal for canonical memory."""

    model_config = ConfigDict(frozen=True)

    category: str  # "fact", "decision", "failure"
    content: str


class WorkerResult(BaseModel):
    """Worker result.

    `completed` and `failed` are final result states. `awaiting_instruction`
    is a runtime pause state produced by the instruction-request channel, not
    a valid final JSON status from an agent worker.
    """

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
