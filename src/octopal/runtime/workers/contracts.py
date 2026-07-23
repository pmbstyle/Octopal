from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from octopal.infrastructure.config.models import LLMConfig
from octopal.infrastructure.store.models import AdaptationContext, ProceduralRecipeContext
from octopal.runtime.memory.influence import require_complete_memory_influence_ids
from octopal.runtime.memory.retrieval import MemoryRetrievalTrace


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
    outcome_verification: WorkspaceFileVerificationContract | None = None
    programmatic_read_call_budget: int = Field(default=0, ge=0, le=16, strict=True)
    memory_influence_ids: list[str] = Field(default_factory=list, max_length=128)
    memory_retrievals: list[MemoryRetrievalTrace] = Field(default_factory=list, max_length=20)
    idempotency_key: str | None = None

    @field_validator("memory_influence_ids", mode="before")
    @classmethod
    def validate_memory_influence_ids(cls, value: object) -> list[str]:
        if not isinstance(value, (list, tuple)):
            return []
        return require_complete_memory_influence_ids(value)

    @model_validator(mode="after")
    def validate_memory_retrievals(self) -> Self:
        selected_ids = set(self.memory_influence_ids)
        if any(retrieval.memory_id not in selected_ids for retrieval in self.memory_retrievals):
            raise ValueError("memory retrieval traces must reference selected memory influence ids")
        _require_unique_ranked_memory_retrievals(self.memory_retrievals)
        return self


class WorkspaceFileVerificationContract(BaseModel):
    """Host-side postcondition for a worker that must produce one workspace file.

    The worker does not attest to this condition: the runtime checks the path after
    the worker exits.  Paths are intentionally workspace-relative so the same task
    contract works with native and Docker workers on every supported platform.
    """

    model_config = ConfigDict(frozen=True)

    kind: Literal["workspace_file"] = "workspace_file"
    artifact_path: str = Field(min_length=1, max_length=1024)
    min_bytes: int = Field(default=1, ge=0, le=64 * 1024 * 1024)
    max_bytes: int = Field(default=64 * 1024 * 1024, ge=0, le=64 * 1024 * 1024)
    expected_sha256: str | None = Field(default=None, pattern=r"^[a-fA-F0-9]{64}$")

    @field_validator("artifact_path")
    @classmethod
    def validate_artifact_path(cls, value: str) -> str:
        normalized = value.strip().replace("\\", "/")
        if not normalized or "\x00" in normalized:
            raise ValueError("artifact_path must be a non-empty workspace-relative path")
        if normalized.startswith("/") or PureWindowsPath(normalized).is_absolute():
            raise ValueError("artifact_path must be workspace-relative")
        if any(part == ".." for part in normalized.split("/")):
            raise ValueError("artifact_path must not traverse outside the workspace")
        if normalized.endswith("/") or PurePosixPath(normalized).parent == PurePosixPath("."):
            raise ValueError("artifact_path must name a file inside a workspace directory")
        return normalized

    @field_validator("expected_sha256")
    @classmethod
    def normalize_expected_sha256(cls, value: str | None) -> str | None:
        return value.lower() if value else None

    @model_validator(mode="after")
    def validate_size_range(self) -> Self:
        if self.min_bytes > self.max_bytes:
            raise ValueError("min_bytes must not exceed max_bytes")
        return self


class WorkspaceFileVerificationEvidence(BaseModel):
    """Metadata-only evidence produced by the host outcome verifier."""

    model_config = ConfigDict(frozen=True)

    verifier: Literal["workspace_file"] = "workspace_file"
    status: Literal["passed", "failed", "not_run"]
    expected_postcondition: Literal["workspace_file_exists"] = "workspace_file_exists"
    verification_method: Literal["host_filesystem_stat_sha256"] = "host_filesystem_stat_sha256"
    artifact_path_fingerprint: str
    observed_exists: bool
    observed_regular_file: bool
    observed_size_bytes: int | None = None
    observed_sha256: str | None = None
    unresolved_gaps: list[str] = Field(default_factory=list)


def ensure_outcome_artifact_is_shared(
    allowed_paths: list[str] | None,
    outcome_verification: WorkspaceFileVerificationContract | Mapping[str, Any] | None,
) -> list[str] | None:
    """Grant a file contract access to its smallest shared workspace directory."""

    if isinstance(outcome_verification, WorkspaceFileVerificationContract):
        artifact_path = outcome_verification.artifact_path
    elif isinstance(outcome_verification, Mapping):
        artifact_path = str(outcome_verification.get("artifact_path") or "").strip()
        artifact_path = artifact_path.replace("\\", "/")
    else:
        return allowed_paths

    if not artifact_path or PurePosixPath(artifact_path).parent == PurePosixPath("."):
        return allowed_paths
    required_path = PurePosixPath(artifact_path).parent.as_posix()

    normalized_allowed = [
        str(path).strip().replace("\\", "/").strip("/")
        for path in allowed_paths or []
        if str(path).strip()
    ]
    if any(
        allowed == "." or artifact_path == allowed or artifact_path.startswith(f"{allowed}/")
        for allowed in normalized_allowed
    ):
        return list(allowed_paths or []) or None
    return [*(allowed_paths or []), required_path]


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
    outcome_verification: WorkspaceFileVerificationContract | None = None
    programmatic_read_call_budget: int = Field(default=0, ge=0, le=16, strict=True)
    memory_influence_ids: list[str] = Field(default_factory=list, max_length=128)
    memory_retrievals: list[MemoryRetrievalTrace] = Field(default_factory=list, max_length=20)
    procedural_recipes: list[ProceduralRecipeContext] = Field(default_factory=list, max_length=1)
    adaptations: list[AdaptationContext] = Field(default_factory=list, max_length=1)
    idempotency_key: str | None = None

    @model_validator(mode="before")
    @classmethod
    def share_outcome_artifact(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        outcome_verification = value.get("outcome_verification")
        allowed_paths = ensure_outcome_artifact_is_shared(
            value.get("allowed_paths"), outcome_verification
        )
        if allowed_paths == value.get("allowed_paths"):
            return value
        return {**value, "allowed_paths": allowed_paths}

    @field_validator("memory_influence_ids", mode="before")
    @classmethod
    def validate_memory_influence_ids(cls, value: object) -> list[str]:
        if not isinstance(value, (list, tuple)):
            return []
        return require_complete_memory_influence_ids(value)

    @model_validator(mode="after")
    def validate_memory_retrievals(self) -> Self:
        selected_ids = set(self.memory_influence_ids)
        if any(retrieval.memory_id not in selected_ids for retrieval in self.memory_retrievals):
            raise ValueError("memory retrieval traces must reference selected memory influence ids")
        _require_unique_ranked_memory_retrievals(self.memory_retrievals)
        return self

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


def _require_unique_ranked_memory_retrievals(
    retrievals: list[MemoryRetrievalTrace],
) -> None:
    memory_ids = [retrieval.memory_id for retrieval in retrievals]
    if len(memory_ids) != len(set(memory_ids)):
        raise ValueError("memory retrieval traces must reference unique memory entries")
    ranks = [retrieval.rank for retrieval in retrievals]
    if ranks != list(range(1, len(retrievals) + 1)):
        raise ValueError("memory retrieval trace ranks must be contiguous and ordered")


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
