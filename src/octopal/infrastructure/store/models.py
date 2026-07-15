from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

MemoryInfluenceId = Annotated[
    str,
    StringConstraints(
        pattern=(
            r"^(?:canon_event|memory_fact|memory_entry|octo_diary|operational_memory):"
            r"[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$"
        )
    ),
]

MemoryOrigin = Literal[
    "direct_user",
    "assistant_inference",
    "local_runtime_evidence",
    "worker",
    "connector",
    "mcp",
    "web",
    "document",
    "imported_canon",
]
MemoryTrustState = Literal[
    "observed",
    "quarantined_candidate",
    "corroborated",
    "trusted",
    "superseded",
    "deprecated",
]
ExecutionEpisodeSource = MemoryOrigin
ExecutionEpisodeTrustState = MemoryTrustState
ProceduralRecipeStatus = Literal["candidate", "active", "deprecated"]
RecipeText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
ProceduralRecipeId = Annotated[str, StringConstraints(pattern=r"^recipe_[a-f0-9]{64}$")]

_UNTRUSTED_MEMORY_ORIGINS = {
    "assistant_inference",
    "worker",
    "connector",
    "mcp",
    "web",
    "document",
}
_PROCEDURAL_RECIPE_DEFINITION_FIELDS = (
    "applicability_conditions",
    "required_capabilities",
    "required_permissions",
    "strategy_steps",
    "verification_contract",
    "known_failures",
    "invalidating_conditions",
)


def procedural_recipe_definition_fingerprint(value: Mapping[str, Any]) -> str:
    payload = {field: value.get(field) for field in _PROCEDURAL_RECIPE_DEFINITION_FIELDS}
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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


class ExecutionEpisodeRecord(BaseModel):
    """Immutable, metadata-only evidence index for one terminal execution."""

    model_config = ConfigDict(frozen=True)

    id: str
    worker_run_id: str
    task_fingerprint: str
    environment_fingerprint: str
    capability_fingerprint: str
    result_fingerprint: str
    status: Literal["completed", "failed", "stopped"]
    source_kind: ExecutionEpisodeSource
    trust_state: ExecutionEpisodeTrustState
    correlation_id: str | None = None
    template_id: str | None = None
    model: str | None = None
    trajectory_refs: dict[str, Any] = Field(default_factory=dict)
    result_metadata: dict[str, Any] = Field(default_factory=dict)
    verification: dict[str, Any] = Field(default_factory=dict)
    provenance: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime

    @model_validator(mode="after")
    def reject_external_trust_escalation(self) -> ExecutionEpisodeRecord:
        if self.trust_state == "trusted" and self.source_kind in _UNTRUSTED_MEMORY_ORIGINS:
            raise ValueError(
                f"source_kind '{self.source_kind}' cannot directly create a trusted episode"
            )
        return self


class ExecutionEpisodeEvidenceRecord(BaseModel):
    """Encrypted raw evidence that may be erased without rewriting episode metadata."""

    model_config = ConfigDict(frozen=True)

    episode_id: str
    algorithm: Literal["AES-256-GCM"]
    key_id: str = Field(min_length=16, max_length=64)
    nonce: bytes = Field(min_length=12, max_length=12)
    ciphertext: bytes = Field(min_length=16)
    created_at: datetime
    expires_at: datetime


class ExecutionEpisodeEvidenceMetadata(BaseModel):
    """Non-secret envelope fields for operator inspection without loading ciphertext."""

    model_config = ConfigDict(frozen=True)

    episode_id: str
    algorithm: Literal["AES-256-GCM"]
    key_id: str = Field(min_length=16, max_length=64)
    created_at: datetime
    expires_at: datetime


class ProceduralRecipeRecord(BaseModel):
    """Operator-promoted procedural memory linked to immutable execution episodes."""

    model_config = ConfigDict(frozen=True)

    id: ProceduralRecipeId
    intent_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    definition_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    applicability_conditions: list[RecipeText] = Field(default_factory=list, max_length=16)
    required_capabilities: list[RecipeText] = Field(default_factory=list, max_length=32)
    required_permissions: list[RecipeText] = Field(default_factory=list, max_length=32)
    strategy_steps: list[RecipeText] = Field(min_length=1, max_length=20)
    verification_contract: dict[str, Any]
    known_failures: list[RecipeText] = Field(default_factory=list, max_length=16)
    invalidating_conditions: list[RecipeText] = Field(default_factory=list, max_length=16)
    source_episode_ids: list[str] = Field(min_length=1, max_length=16)
    success_count: int = Field(ge=1)
    failure_count: int = Field(default=0, ge=0)
    status: ProceduralRecipeStatus
    last_validated_at: datetime
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_recipe_payload(self) -> ProceduralRecipeRecord:
        if len(set(self.source_episode_ids)) != len(self.source_episode_ids):
            raise ValueError("source episode ids must be unique")
        if not self.verification_contract:
            raise ValueError("verification contract must not be empty")
        if len(json.dumps(self.verification_contract, ensure_ascii=False, default=str)) > 8000:
            raise ValueError("verification contract exceeds 8000 characters")
        if self.definition_fingerprint != procedural_recipe_definition_fingerprint(
            self.model_dump(mode="json")
        ):
            raise ValueError("procedural recipe definition fingerprint does not match")
        return self


class ProceduralRecipeContext(BaseModel):
    """Bounded advisory recipe context passed to one isolated worker run."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: ProceduralRecipeId
    evaluation_id: str | None = Field(default=None, pattern=r"^recipe_eval_[a-f0-9]{64}$")
    definition_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    applicability_conditions: list[RecipeText] = Field(default_factory=list, max_length=16)
    required_capabilities: list[RecipeText] = Field(default_factory=list, max_length=32)
    required_permissions: list[RecipeText] = Field(default_factory=list, max_length=32)
    strategy_steps: list[RecipeText] = Field(min_length=1, max_length=20)
    verification_contract: dict[str, Any]
    known_failures: list[RecipeText] = Field(default_factory=list, max_length=16)
    invalidating_conditions: list[RecipeText] = Field(default_factory=list, max_length=16)

    @model_validator(mode="after")
    def validate_context_payload(self) -> ProceduralRecipeContext:
        if not self.verification_contract:
            raise ValueError("verification contract must not be empty")
        if len(json.dumps(self.model_dump(mode="json"), ensure_ascii=False)) > 16_000:
            raise ValueError("procedural recipe context exceeds 16000 characters")
        if self.definition_fingerprint != procedural_recipe_definition_fingerprint(
            self.model_dump(mode="json")
        ):
            raise ValueError("procedural recipe definition fingerprint does not match")
        return self


class ProceduralRecipeEvaluationRecord(BaseModel):
    """Immutable metadata-only comparison of baseline and recipe benchmark results."""

    model_config = ConfigDict(frozen=True)

    id: str = Field(pattern=r"^recipe_eval_[a-f0-9]{64}$")
    recipe_id: ProceduralRecipeId
    baseline_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    candidate_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    scenario_set_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    common_trial_count: int = Field(ge=2)
    baseline_success_rate: float = Field(ge=0, le=1)
    candidate_success_rate: float = Field(ge=0, le=1)
    regression_count: int = Field(ge=0)
    improvement_count: int = Field(ge=0)
    passed: bool
    created_at: datetime


class IntentRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    worker_id: str
    type: str
    payload: dict[str, Any]
    payload_hash: str
    risk: str
    requires_approval: bool
    memory_influence_ids: list[MemoryInfluenceId] = Field(default_factory=list, max_length=128)
    procedural_recipe_ids: list[ProceduralRecipeId] = Field(default_factory=list, max_length=8)
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
    trust_state: MemoryTrustState
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    facets: list[str] = Field(default_factory=list)
    source_kind: MemoryOrigin
    source_ref: str | None = None
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def reject_external_trust_escalation(self) -> MemoryFactRecord:
        if self.trust_state == "trusted" and self.source_kind in _UNTRUSTED_MEMORY_ORIGINS:
            raise ValueError(
                f"source_kind '{self.source_kind}' cannot directly create a trusted memory fact"
            )
        return self


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
