from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, cast
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from octopal.infrastructure.store.base import Store
from octopal.infrastructure.store.models import (
    AuditEvent,
    ExecutionEpisodeRecord,
    ProceduralRecipeRecord,
    RecipeText,
)
from octopal.utils import utc_now

_RECIPE_STATUSES = {"candidate", "active", "deprecated"}


class ProceduralRecipeCandidate(BaseModel):
    """Bounded operator- or evaluator-supplied recipe definition."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    applicability_conditions: list[RecipeText] = Field(default_factory=list, max_length=16)
    required_capabilities: list[RecipeText] = Field(default_factory=list, max_length=32)
    required_permissions: list[RecipeText] = Field(default_factory=list, max_length=32)
    strategy_steps: list[RecipeText] = Field(min_length=1, max_length=20)
    verification_contract: dict[str, Any]
    known_failures: list[RecipeText] = Field(default_factory=list, max_length=16)
    invalidating_conditions: list[RecipeText] = Field(default_factory=list, max_length=16)
    source_episode_ids: list[str] = Field(min_length=1, max_length=16)

    def bounded_payload(self) -> dict[str, Any]:
        payload = cast(dict[str, Any], self.model_dump(mode="json"))
        if len(json.dumps(payload, ensure_ascii=False, default=str)) > 32_000:
            raise ValueError("recipe candidate exceeds 32000 characters")
        return payload


@dataclass
class ProceduralRecipeService:
    store: Store

    def create_candidate(
        self, definition: ProceduralRecipeCandidate, *, actor_ref: str = "operator_cli"
    ) -> ProceduralRecipeRecord:
        payload = definition.bounded_payload()
        source_episode_ids = sorted(definition.source_episode_ids)
        payload["source_episode_ids"] = source_episode_ids
        episodes = self._load_eligible_episodes(source_episode_ids)
        intent_fingerprint = self._common_episode_fingerprint(episodes, "task_fingerprint")
        definition_fingerprint = _fingerprint(
            {key: value for key, value in payload.items() if key != "source_episode_ids"}
        )
        recipe_id = f"recipe_{_fingerprint(payload)}"
        existing = self.store.get_procedural_recipe(recipe_id)
        if existing is not None:
            return existing
        now = utc_now()
        record = ProceduralRecipeRecord(
            id=recipe_id,
            intent_fingerprint=intent_fingerprint,
            definition_fingerprint=definition_fingerprint,
            applicability_conditions=definition.applicability_conditions,
            required_capabilities=definition.required_capabilities,
            required_permissions=definition.required_permissions,
            strategy_steps=definition.strategy_steps,
            verification_contract=definition.verification_contract,
            known_failures=definition.known_failures,
            invalidating_conditions=definition.invalidating_conditions,
            source_episode_ids=source_episode_ids,
            success_count=len(episodes),
            failure_count=0,
            status="candidate",
            last_validated_at=max(episode.created_at for episode in episodes),
            created_at=now,
            updated_at=now,
        )
        created = self.store.add_procedural_recipe_with_audit(
            record,
            _audit_event(
                "procedural_recipe_candidate_created",
                record,
                actor_ref=actor_ref,
            ),
        )
        if not created:
            existing = self.store.get_procedural_recipe(record.id)
            if existing is None:
                raise RuntimeError("recipe creation conflicted without a stored record")
            return existing
        return record

    def promote(self, recipe_id: str, *, actor_ref: str = "operator_cli") -> ProceduralRecipeRecord:
        record = self._get_required(recipe_id)
        if record.status != "candidate":
            raise ValueError(f"recipe cannot be promoted from {record.status}")
        episodes = self._load_eligible_episodes(record.source_episode_ids)
        if len(episodes) < 2:
            raise ValueError("recipe promotion requires at least two verified successful episodes")
        self._common_episode_fingerprint(episodes, "task_fingerprint")
        self._common_episode_fingerprint(episodes, "capability_fingerprint")
        active = self.store.list_procedural_recipes(status="active", limit=1000)
        if any(item.intent_fingerprint == record.intent_fingerprint for item in active):
            raise ValueError("an active recipe already exists for this intent fingerprint")
        now = utc_now()
        transitioned = self.store.transition_procedural_recipe_with_audit(
            record.id,
            expected_statuses=["candidate"],
            new_status="active",
            updated_at=now,
            event=_audit_event(
                "procedural_recipe_promoted",
                record,
                actor_ref=actor_ref,
            ),
        )
        if not transitioned:
            raise RuntimeError("recipe changed during promotion")
        return self._get_required(record.id)

    def deprecate(
        self, recipe_id: str, *, actor_ref: str = "operator_cli"
    ) -> ProceduralRecipeRecord:
        record = self._get_required(recipe_id)
        if record.status == "deprecated":
            raise ValueError("recipe is already deprecated")
        now = utc_now()
        transitioned = self.store.transition_procedural_recipe_with_audit(
            record.id,
            expected_statuses=["candidate", "active"],
            new_status="deprecated",
            updated_at=now,
            event=_audit_event(
                "procedural_recipe_deprecated",
                record,
                actor_ref=actor_ref,
            ),
        )
        if not transitioned:
            raise RuntimeError("recipe changed during deprecation")
        return self._get_required(record.id)

    def get(self, recipe_id: str) -> ProceduralRecipeRecord | None:
        normalized = str(recipe_id or "").strip()
        if not normalized.startswith("recipe_") or len(normalized) != 71:
            return None
        return self.store.get_procedural_recipe(normalized)

    def list_recipes(
        self, *, status: str | None = None, limit: int = 100
    ) -> list[ProceduralRecipeRecord]:
        if status is not None and status not in _RECIPE_STATUSES:
            raise ValueError("invalid procedural recipe status")
        return self.store.list_procedural_recipes(status=status, limit=limit)

    def _get_required(self, recipe_id: str) -> ProceduralRecipeRecord:
        record = self.get(recipe_id)
        if record is None:
            raise ValueError("procedural recipe not found")
        return record

    def _load_eligible_episodes(self, episode_ids: list[str]) -> list[ExecutionEpisodeRecord]:
        if len(set(episode_ids)) != len(episode_ids):
            raise ValueError("source episode ids must be unique")
        episodes: list[ExecutionEpisodeRecord] = []
        for episode_id in episode_ids:
            episode = self.store.get_execution_episode(episode_id)
            if episode is None:
                raise ValueError("source execution episode not found")
            _require_verified_success(episode)
            episodes.append(episode)
        self._common_episode_fingerprint(episodes, "task_fingerprint")
        return episodes

    @staticmethod
    def _common_episode_fingerprint(episodes: list[ExecutionEpisodeRecord], field: str) -> str:
        values = {str(getattr(episode, field)) for episode in episodes}
        if len(values) != 1:
            raise ValueError(f"source episodes do not share one {field}")
        return next(iter(values))


def recipe_metadata_payload(record: ProceduralRecipeRecord) -> dict[str, Any]:
    return {
        "id": record.id,
        "intent_fingerprint": record.intent_fingerprint,
        "definition_fingerprint": record.definition_fingerprint,
        "status": record.status,
        "source_episode_count": len(record.source_episode_ids),
        "source_episode_ids_fingerprint": _fingerprint(sorted(record.source_episode_ids)),
        "strategy_step_count": len(record.strategy_steps),
        "applicability_condition_count": len(record.applicability_conditions),
        "required_capability_count": len(record.required_capabilities),
        "required_permission_count": len(record.required_permissions),
        "known_failure_count": len(record.known_failures),
        "invalidating_condition_count": len(record.invalidating_conditions),
        "success_count": record.success_count,
        "failure_count": record.failure_count,
        "last_validated_at": record.last_validated_at.isoformat(),
        "created_at": record.created_at.isoformat(),
        "updated_at": record.updated_at.isoformat(),
    }


def _require_verified_success(episode: ExecutionEpisodeRecord) -> None:
    verification = episode.verification
    graders = verification.get("grader_results")
    graders_passed = (
        bool(graders)
        and isinstance(graders, list)
        and all(
            isinstance(item, dict)
            and (item.get("passed") is True or item.get("status") == "passed")
            for item in graders
        )
    )
    if episode.status != "completed":
        raise ValueError("source episode is not completed")
    if episode.trust_state not in {"observed", "corroborated", "trusted"}:
        raise ValueError("source episode trust state is not eligible for procedural memory")
    if verification.get("result_contract_validated") is not True:
        raise ValueError("source episode result contract was not validated")
    if verification.get("structured_output_present") is not True:
        raise ValueError("source episode has no structured output evidence")
    if verification.get("explicit_verification_present") is not True and not graders_passed:
        raise ValueError("source episode has no successful verification evidence")


def _audit_event(event_type: str, record: ProceduralRecipeRecord, *, actor_ref: str) -> AuditEvent:
    return AuditEvent(
        id=f"audit_{uuid4().hex}",
        ts=utc_now(),
        correlation_id=record.id,
        level="info",
        event_type=event_type,
        data={
            "recipe_id": record.id,
            "intent_fingerprint": record.intent_fingerprint,
            "definition_fingerprint": record.definition_fingerprint,
            "source_episode_count": len(record.source_episode_ids),
            "actor": "operator",
            "actor_ref_fingerprint": _fingerprint(str(actor_ref or "operator_cli")[:120]),
        },
    )


def _fingerprint(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
