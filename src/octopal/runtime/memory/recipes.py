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
    ProceduralRecipeContext,
    ProceduralRecipeEvaluationRecord,
    ProceduralRecipeRecord,
    RecipeText,
    procedural_recipe_definition_fingerprint,
)
from octopal.runtime.memory.episodes import (
    worker_capability_fingerprint,
    worker_task_fingerprint,
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
        self,
        definition: ProceduralRecipeCandidate,
        *,
        actor_ref: str = "operator_cli",
        include_matching: bool = False,
    ) -> ProceduralRecipeRecord:
        payload = definition.bounded_payload()
        source_episode_ids = (
            self._expand_matching_episode_ids(definition.source_episode_ids)
            if include_matching
            else sorted(definition.source_episode_ids)
        )
        payload["source_episode_ids"] = source_episode_ids
        episodes = self._load_eligible_episodes(source_episode_ids)
        intent_fingerprint = self._common_episode_fingerprint(episodes, "task_fingerprint")
        definition_fingerprint = procedural_recipe_definition_fingerprint(payload)
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
        evaluation = self.store.get_latest_procedural_recipe_evaluation(record.id)
        if evaluation is None or not evaluation.passed:
            raise ValueError("recipe promotion requires a passing held-out evaluation")
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
                evaluation_id=evaluation.id,
            ),
        )
        if not transitioned:
            raise RuntimeError("recipe changed during promotion")
        return self._get_required(record.id)

    def evaluate(
        self,
        recipe_id: str,
        *,
        baseline: dict[str, Any],
        candidate: dict[str, Any],
        actor_ref: str = "operator_cli",
    ) -> ProceduralRecipeEvaluationRecord:
        from octopal.runtime.workers.bench import compare_worker_bench_to_baseline

        recipe = self._get_required(recipe_id)
        if recipe.status == "deprecated":
            raise ValueError("deprecated recipes cannot be evaluated")
        baseline_trials = _evaluation_trial_index(baseline, label="baseline")
        candidate_trials = _evaluation_trial_index(candidate, label="candidate")
        if set(baseline_trials) != set(candidate_trials):
            raise ValueError(
                "baseline and candidate evaluations must have identical trial coverage"
            )
        scenario_ids = {scenario_id for scenario_id, _ in baseline_trials}
        if len(scenario_ids) < 2:
            raise ValueError("held-out evaluation requires at least two distinct scenarios")
        if any(_trial_recipe_ids(trial) for trial in baseline_trials.values()):
            raise ValueError("baseline evaluation must not contain procedural recipes")
        if any(_trial_recipe_ids(trial) != {recipe.id} for trial in candidate_trials.values()):
            raise ValueError(
                "candidate evaluation must apply only the target recipe to every trial"
            )
        expected_definition = {recipe.id: recipe.definition_fingerprint}
        if any(
            _trial_recipe_definitions(trial) != expected_definition
            for trial in candidate_trials.values()
        ):
            raise ValueError("candidate evaluation recipe definition fingerprint does not match")

        comparison = compare_worker_bench_to_baseline(summary=candidate, baseline=baseline)
        rates = comparison.get("success_rate")
        if not isinstance(rates, dict):
            raise ValueError("evaluation comparison did not produce success rates")
        baseline_rate = rates.get("baseline")
        candidate_rate = rates.get("current")
        if not isinstance(baseline_rate, (int, float)) or not isinstance(
            candidate_rate, (int, float)
        ):
            raise ValueError("all held-out trials must have deterministic grades")
        regressions = comparison.get("regressions")
        improvements = comparison.get("improvements")
        if not isinstance(regressions, list) or not isinstance(improvements, list):
            raise ValueError("evaluation comparison is incomplete")
        passed = (
            not bool(comparison.get("coverage_changed"))
            and not regressions
            and float(candidate_rate) >= float(baseline_rate)
            and float(candidate_rate) > 0
        )
        trial_keys = sorted(f"{scenario_id}:{trial}" for scenario_id, trial in baseline_trials)
        now = utc_now()
        evaluation_payload = {
            "recipe_id": recipe.id,
            "baseline_fingerprint": _fingerprint(_evaluation_projection(baseline_trials)),
            "candidate_fingerprint": _fingerprint(_evaluation_projection(candidate_trials)),
            "scenario_set_fingerprint": _fingerprint(trial_keys),
            "common_trial_count": len(trial_keys),
            "baseline_success_rate": float(baseline_rate),
            "candidate_success_rate": float(candidate_rate),
            "regression_count": len(regressions),
            "improvement_count": len(improvements),
            "passed": passed,
        }
        evaluation = ProceduralRecipeEvaluationRecord(
            id=f"recipe_eval_{_fingerprint(evaluation_payload)}",
            created_at=now,
            **evaluation_payload,
        )
        created = self.store.add_procedural_recipe_evaluation_with_audit(
            evaluation,
            _evaluation_audit_event(evaluation, recipe, actor_ref=actor_ref),
        )
        if not created:
            existing = self.store.get_procedural_recipe_evaluation(evaluation.id)
            if existing is None:
                raise RuntimeError("recipe evaluation conflicted without a stored record")
            return existing
        return evaluation

    def resolve_for_worker(
        self,
        *,
        task: str,
        inputs: dict[str, Any],
        granted_capabilities: list[dict[str, Any]],
        effective_permissions: list[str],
        available_tools: list[str],
        mcp_tools: list[dict[str, Any]],
    ) -> list[ProceduralRecipeContext]:
        intent_fingerprint = worker_task_fingerprint(task, inputs)
        capability_fingerprint = worker_capability_fingerprint(
            granted_capabilities=granted_capabilities,
            effective_permissions=effective_permissions,
            available_tools=available_tools,
            mcp_tools=mcp_tools,
        )
        active = self.store.list_procedural_recipes(status="active", limit=1000)
        available_capabilities = {
            str(item.get("type") or "").strip().lower()
            for item in granted_capabilities
            if isinstance(item, dict)
        }
        available_capabilities.update(str(name).strip().lower() for name in available_tools)
        available_capabilities.update(
            str(item.get("name") or "").strip().lower()
            for item in mcp_tools
            if isinstance(item, dict)
        )
        available_permissions = {
            str(permission).strip().lower() for permission in effective_permissions
        }
        for recipe in active:
            if recipe.intent_fingerprint != intent_fingerprint:
                continue
            if recipe.failure_count > 0:
                continue
            evaluation = self.store.get_latest_procedural_recipe_evaluation(recipe.id)
            if evaluation is None or not evaluation.passed:
                continue
            source = self.store.get_execution_episode(recipe.source_episode_ids[0])
            if source is None or source.capability_fingerprint != capability_fingerprint:
                continue
            required_capabilities = {
                str(value).strip().lower() for value in recipe.required_capabilities
            }
            required_permissions = {
                str(value).strip().lower() for value in recipe.required_permissions
            }
            if not required_capabilities.issubset(available_capabilities):
                continue
            if not required_permissions.issubset(available_permissions):
                continue
            return [_recipe_context(recipe, evaluation)]
        return []

    def record_outcome(self, recipe_id: str, episode: ExecutionEpisodeRecord) -> bool:
        recipe = self._get_required(recipe_id)
        if recipe.status == "candidate":
            raise ValueError("candidate recipes cannot record runtime outcomes")
        raw_recipe_ids = episode.provenance.get("procedural_recipe_ids")
        if not isinstance(raw_recipe_ids, list) or recipe.id not in raw_recipe_ids:
            raise ValueError("execution episode does not attribute the procedural recipe")
        succeeded = _is_verified_success(episode)
        return self.store.record_procedural_recipe_outcome_with_audit(
            recipe.id,
            episode_id=episode.id,
            succeeded=succeeded,
            validated_at=utc_now(),
            event=_outcome_audit_event(recipe, episode, succeeded=succeeded),
        )

    def latest_evaluation(self, recipe_id: str) -> ProceduralRecipeEvaluationRecord | None:
        recipe = self._get_required(recipe_id)
        return self.store.get_latest_procedural_recipe_evaluation(recipe.id)

    def context_for_evaluation(self, recipe_id: str) -> ProceduralRecipeContext:
        recipe = self._get_required(recipe_id)
        if recipe.status == "deprecated":
            raise ValueError("deprecated recipes cannot be evaluated")
        return _recipe_context(recipe, None)

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

    def _expand_matching_episode_ids(self, episode_ids: list[str]) -> list[str]:
        seeds = self._load_eligible_episodes(episode_ids)
        task_fingerprint = self._common_episode_fingerprint(seeds, "task_fingerprint")
        capability_fingerprint = self._common_episode_fingerprint(seeds, "capability_fingerprint")
        selected = list(dict.fromkeys(episode_ids))
        matching = self.store.list_execution_episodes_for_task(
            task_fingerprint,
            capability_fingerprint=capability_fingerprint,
            limit=16,
        )
        for episode in matching:
            if episode.id in selected:
                continue
            try:
                _require_verified_success(episode)
            except ValueError:
                continue
            if len(selected) >= 16:
                break
            selected.append(episode.id)
        return sorted(selected)

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


def recipe_evaluation_payload(record: ProceduralRecipeEvaluationRecord) -> dict[str, Any]:
    return cast(dict[str, Any], record.model_dump(mode="json"))


def _require_verified_success(episode: ExecutionEpisodeRecord) -> None:
    verification = episode.verification
    graders = verification.get("grader_results")
    legacy_verification_passed = verification.get("explicit_verification_present") is True or (
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
    if verification.get("verified") is True:
        return
    if verification.get("outcome_contract_present") is True:
        raise ValueError("source episode has no successful verification evidence from host")
    if not legacy_verification_passed:
        raise ValueError("source episode has no successful verification evidence")


def _is_verified_success(episode: ExecutionEpisodeRecord) -> bool:
    try:
        _require_verified_success(episode)
    except ValueError:
        return False
    return True


def _recipe_context(
    recipe: ProceduralRecipeRecord,
    evaluation: ProceduralRecipeEvaluationRecord | None,
) -> ProceduralRecipeContext:
    return ProceduralRecipeContext(
        id=recipe.id,
        evaluation_id=evaluation.id if evaluation is not None else None,
        definition_fingerprint=recipe.definition_fingerprint,
        applicability_conditions=recipe.applicability_conditions,
        required_capabilities=recipe.required_capabilities,
        required_permissions=recipe.required_permissions,
        strategy_steps=recipe.strategy_steps,
        verification_contract=recipe.verification_contract,
        known_failures=recipe.known_failures,
        invalidating_conditions=recipe.invalidating_conditions,
    )


def _evaluation_trial_index(
    summary: dict[str, Any], *, label: str
) -> dict[tuple[str, int], dict[str, Any]]:
    raw_trials = summary.get("scenarios")
    if not isinstance(raw_trials, list) or not raw_trials:
        raise ValueError(f"{label} evaluation must contain non-empty scenarios")
    result: dict[tuple[str, int], dict[str, Any]] = {}
    for raw_trial in raw_trials:
        if not isinstance(raw_trial, dict):
            raise ValueError(f"{label} evaluation contains an invalid trial")
        scenario_id = str(raw_trial.get("scenario_id") or "").strip()
        trial = raw_trial.get("trial")
        grade = raw_trial.get("grade")
        if not scenario_id or not isinstance(trial, int) or trial < 1:
            raise ValueError(f"{label} evaluation contains an invalid trial identity")
        if not isinstance(grade, dict) or not isinstance(grade.get("passed"), bool):
            raise ValueError(f"{label} evaluation contains an ungraded trial")
        key = (scenario_id, trial)
        if key in result:
            raise ValueError(f"{label} evaluation contains duplicate trials")
        result[key] = raw_trial
    return result


def _trial_recipe_ids(trial: dict[str, Any]) -> set[str]:
    manifest = trial.get("context_manifest")
    if not isinstance(manifest, dict):
        return set()
    memory = manifest.get("memory")
    if not isinstance(memory, dict):
        return set()
    recipe_ids = memory.get("recipe_ids")
    if not isinstance(recipe_ids, list):
        return set()
    return {str(value) for value in recipe_ids}


def _trial_recipe_definitions(trial: dict[str, Any]) -> dict[str, str]:
    manifest = trial.get("context_manifest")
    if not isinstance(manifest, dict):
        return {}
    memory = manifest.get("memory")
    if not isinstance(memory, dict):
        return {}
    raw_definitions = memory.get("recipe_definition_fingerprints")
    if not isinstance(raw_definitions, dict):
        return {}
    return {str(recipe_id): str(fingerprint) for recipe_id, fingerprint in raw_definitions.items()}


def _evaluation_projection(trials: dict[tuple[str, int], dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "scenario_id": key[0],
            "trial": key[1],
            "status": trial.get("status"),
            "returncode": trial.get("returncode"),
            "grade": trial.get("grade"),
            "recipe_ids": sorted(_trial_recipe_ids(trial)),
            "recipe_definition_fingerprints": _trial_recipe_definitions(trial),
        }
        for key, trial in sorted(trials.items())
    ]


def _audit_event(
    event_type: str,
    record: ProceduralRecipeRecord,
    *,
    actor_ref: str,
    evaluation_id: str | None = None,
) -> AuditEvent:
    data: dict[str, Any] = {
        "recipe_id": record.id,
        "intent_fingerprint": record.intent_fingerprint,
        "definition_fingerprint": record.definition_fingerprint,
        "source_episode_count": len(record.source_episode_ids),
        "actor": "operator",
        "actor_ref_fingerprint": _fingerprint(str(actor_ref or "operator_cli")[:120]),
    }
    if evaluation_id is not None:
        data["evaluation_id"] = evaluation_id
    return AuditEvent(
        id=f"audit_{uuid4().hex}",
        ts=utc_now(),
        correlation_id=record.id,
        level="info",
        event_type=event_type,
        data=data,
    )


def _evaluation_audit_event(
    evaluation: ProceduralRecipeEvaluationRecord,
    recipe: ProceduralRecipeRecord,
    *,
    actor_ref: str,
) -> AuditEvent:
    return AuditEvent(
        id=f"audit_{uuid4().hex}",
        ts=utc_now(),
        correlation_id=recipe.id,
        level="info" if evaluation.passed else "warning",
        event_type="procedural_recipe_evaluated",
        data={
            "recipe_id": recipe.id,
            "evaluation_id": evaluation.id,
            "common_trial_count": evaluation.common_trial_count,
            "baseline_success_rate": evaluation.baseline_success_rate,
            "candidate_success_rate": evaluation.candidate_success_rate,
            "regression_count": evaluation.regression_count,
            "improvement_count": evaluation.improvement_count,
            "passed": evaluation.passed,
            "actor": "operator",
            "actor_ref_fingerprint": _fingerprint(str(actor_ref or "operator_cli")[:120]),
        },
    )


def _outcome_audit_event(
    recipe: ProceduralRecipeRecord,
    episode: ExecutionEpisodeRecord,
    *,
    succeeded: bool,
) -> AuditEvent:
    return AuditEvent(
        id=f"audit_{uuid4().hex}",
        ts=utc_now(),
        correlation_id=recipe.id,
        level="info" if succeeded else "warning",
        event_type="procedural_recipe_outcome_recorded",
        data={
            "recipe_id": recipe.id,
            "episode_id": episode.id,
            "succeeded": succeeded,
            "actor": "worker_runtime",
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
