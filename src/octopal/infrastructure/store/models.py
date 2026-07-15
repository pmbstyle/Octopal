from __future__ import annotations

import hashlib
import json
import re
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
AdaptationKind = Literal["prompt", "tool_description", "routing", "recipe"]
AdaptationStatus = Literal["candidate", "active", "retired"]
MCPTaskProtocol = Literal["extension", "legacy"]
MCPTaskRemoteStatus = Literal[
    "working",
    "input_required",
    "completed",
    "failed",
    "cancelled",
]
MCPTaskRuntimeStatus = Literal[
    "running",
    "awaiting_instruction",
    "completed",
    "failed",
    "stopped",
]
RecipeText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
ProceduralRecipeId = Annotated[str, StringConstraints(pattern=r"^recipe_[a-f0-9]{64}$")]
AdaptationClusterId = Annotated[str, StringConstraints(pattern=r"^adapt_cluster_[a-f0-9]{64}$")]
AdaptationCandidateId = Annotated[str, StringConstraints(pattern=r"^adapt_[a-f0-9]{64}$")]

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


def adaptation_artifact_fingerprint(
    kind: AdaptationKind,
    target: str,
    change: Mapping[str, Any],
) -> str:
    encoded = json.dumps(
        {"kind": kind, "target": target, "change": change},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def adaptation_candidate_definition_fingerprint(value: Mapping[str, Any]) -> str:
    payload = {
        field: value.get(field)
        for field in (
            "kind",
            "target",
            "hypothesis",
            "change",
            "source_cluster_ids",
            "parent_id",
        )
    }
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


class AdaptationFailureClusterRecord(BaseModel):
    """Immutable metadata-only cluster of recurrent benchmark failures."""

    model_config = ConfigDict(frozen=True)

    id: AdaptationClusterId
    signature: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_summary_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    failure_categories: list[str] = Field(min_length=1, max_length=32)
    scenario_ids: list[str] = Field(min_length=1, max_length=256)
    task_fingerprints: list[str] = Field(min_length=1, max_length=256)
    trial_refs: list[str] = Field(min_length=2, max_length=512)
    trial_count: int = Field(ge=2, le=512)
    created_at: datetime

    @model_validator(mode="after")
    def validate_cluster(self) -> AdaptationFailureClusterRecord:
        if self.trial_count != len(self.trial_refs):
            raise ValueError("adaptation cluster trial count does not match its references")
        if len(set(self.trial_refs)) != len(self.trial_refs):
            raise ValueError("adaptation cluster trial references must be unique")
        if self.failure_categories != sorted(set(self.failure_categories)):
            raise ValueError("adaptation cluster failure categories must be sorted and unique")
        if self.scenario_ids != sorted(set(self.scenario_ids)):
            raise ValueError("adaptation cluster scenario ids must be sorted and unique")
        if self.task_fingerprints != sorted(set(self.task_fingerprints)) or any(
            not re.fullmatch(r"[a-f0-9]{64}", value) for value in self.task_fingerprints
        ):
            raise ValueError("adaptation cluster task fingerprints must be sorted and valid")
        payload = self.model_dump(mode="json", exclude={"id", "trial_count", "created_at"})
        expected_id = hashlib.sha256(
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        ).hexdigest()
        if self.id != f"adapt_cluster_{expected_id}":
            raise ValueError("adaptation cluster id does not match its definition")
        return self


class AdaptationCandidateRecord(BaseModel):
    """Versioned operator-authored change candidate; never self-promotes."""

    model_config = ConfigDict(frozen=True)

    id: AdaptationCandidateId
    family_id: str = Field(pattern=r"^adapt_family_[a-f0-9]{64}$")
    version: int = Field(ge=1)
    kind: AdaptationKind
    target: str = Field(pattern=r"^(worker|tool):[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    artifact_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    definition_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    hypothesis: str = Field(min_length=1, max_length=2000)
    change: dict[str, Any]
    source_cluster_ids: list[AdaptationClusterId] = Field(min_length=1, max_length=16)
    parent_id: AdaptationCandidateId | None = None
    status: AdaptationStatus
    evaluation_id: str | None = Field(default=None, pattern=r"^adapt_eval_[a-f0-9]{64}$")
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_candidate(self) -> AdaptationCandidateRecord:
        if len(set(self.source_cluster_ids)) != len(self.source_cluster_ids):
            raise ValueError("adaptation source cluster ids must be unique")
        if self.artifact_fingerprint != adaptation_artifact_fingerprint(
            self.kind,
            self.target,
            self.change,
        ):
            raise ValueError("adaptation artifact fingerprint does not match")
        expected_definition = adaptation_candidate_definition_fingerprint(
            self.model_dump(mode="json")
        )
        if self.definition_fingerprint != expected_definition:
            raise ValueError("adaptation definition fingerprint does not match")
        if self.id != f"adapt_{expected_definition}":
            raise ValueError("adaptation candidate id does not match its definition")
        expected_family = hashlib.sha256(
            json.dumps(
                [self.kind, self.target],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        if self.family_id != f"adapt_family_{expected_family}":
            raise ValueError("adaptation family id does not match its target")
        if self.status == "active" and self.evaluation_id is None:
            raise ValueError("active adaptation candidates require an evaluation id")
        return self


class AdaptationContext(BaseModel):
    """Bounded benchmark-only application context for one candidate artifact."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: AdaptationCandidateId
    kind: AdaptationKind
    target: str = Field(pattern=r"^(worker|tool):[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    artifact_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    change: dict[str, Any]

    @model_validator(mode="after")
    def validate_context(self) -> AdaptationContext:
        if self.artifact_fingerprint != adaptation_artifact_fingerprint(
            self.kind,
            self.target,
            self.change,
        ):
            raise ValueError("adaptation context fingerprint does not match")
        if len(json.dumps(self.change, ensure_ascii=False, default=str)) > 16_000:
            raise ValueError("adaptation context change exceeds 16000 characters")
        return self


class AdaptationEvaluationRecord(BaseModel):
    """Immutable held-out baseline/candidate comparison."""

    model_config = ConfigDict(frozen=True)

    id: str = Field(pattern=r"^adapt_eval_[a-f0-9]{64}$")
    candidate_id: AdaptationCandidateId
    baseline_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    candidate_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    scenario_set_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    common_trial_count: int = Field(ge=2)
    distinct_scenario_count: int = Field(ge=2)
    baseline_success_rate: float = Field(ge=0, le=1)
    candidate_success_rate: float = Field(ge=0, le=1)
    success_rate_delta: float = Field(ge=-1, le=1)
    regression_count: int = Field(ge=0)
    improvement_count: int = Field(ge=0)
    held_out: bool
    passed: bool
    created_at: datetime

    @model_validator(mode="after")
    def validate_evaluation(self) -> AdaptationEvaluationRecord:
        if self.passed and (
            not self.held_out
            or self.regression_count != 0
            or self.improvement_count < 1
            or self.success_rate_delta <= 0
        ):
            raise ValueError("passing adaptation evaluation violates promotion criteria")
        payload = self.model_dump(mode="json", exclude={"id", "created_at"})
        expected_id = hashlib.sha256(
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        ).hexdigest()
        if self.id != f"adapt_eval_{expected_id}":
            raise ValueError("adaptation evaluation id does not match its comparison")
        return self


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


class MCPTaskRecord(BaseModel):
    """Durable client-side handle for one native MCP task."""

    model_config = ConfigDict(frozen=True)

    id: str
    server_id: str
    task_id: str = Field(repr=False)
    protocol: MCPTaskProtocol
    remote_status: MCPTaskRemoteStatus
    runtime_status: MCPTaskRuntimeStatus
    tool_name: str
    auth_context_id: str
    correlation_id: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    worker_run_id: str | None = None
    chat_id: int | None = None
    chat_turn_id: str | None = None
    plan_run_id: str | None = None
    plan_step_id: str | None = None
    status_message: str | None = None
    ttl_ms: int | None = None
    poll_interval_ms: int | None = None
    input_requests: dict[str, Any] = Field(default_factory=dict)
    responded_input_keys: list[str] = Field(default_factory=list)
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    remote_created_at: datetime
    remote_updated_at: datetime
    created_at: datetime
    updated_at: datetime


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
