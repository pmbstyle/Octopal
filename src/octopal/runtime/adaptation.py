from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator

from octopal.infrastructure.store.base import Store
from octopal.infrastructure.store.models import (
    AdaptationCandidateId,
    AdaptationCandidateRecord,
    AdaptationContext,
    AdaptationEvaluationRecord,
    AdaptationFailureClusterRecord,
    AdaptationKind,
    AuditEvent,
    ProceduralRecipeContext,
    adaptation_artifact_fingerprint,
    adaptation_candidate_definition_fingerprint,
)
from octopal.runtime.workers.bench import compare_worker_bench_to_baseline
from octopal.utils import utc_now

_ADAPTATION_KINDS = {"prompt", "tool_description", "routing", "recipe"}
_SAFE_LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class AdaptationCandidateDefinition(BaseModel):
    """Bounded, operator-authored hypothesis and structured artifact change."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: AdaptationKind
    target: str = Field(pattern=r"^(worker|tool):[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    hypothesis: str = Field(min_length=1, max_length=2000)
    change: dict[str, Any]
    source_cluster_ids: list[str] = Field(min_length=1, max_length=16)
    parent_id: AdaptationCandidateId | None = None

    @model_validator(mode="after")
    def validate_definition(self) -> AdaptationCandidateDefinition:
        if len(set(self.source_cluster_ids)) != len(self.source_cluster_ids):
            raise ValueError("adaptation source cluster ids must be unique")
        _validate_change(self.kind, self.target, self.change)
        payload = self.model_dump(mode="json")
        if len(json.dumps(payload, ensure_ascii=False, default=str)) > 24_000:
            raise ValueError("adaptation candidate exceeds 24000 characters")
        return self


@dataclass
class AdaptationService:
    store: Store

    def cluster_failures(
        self,
        summary: dict[str, Any],
        *,
        actor_ref: str = "operator_cli",
    ) -> list[AdaptationFailureClusterRecord]:
        trials = _validated_trials(summary)
        source_fingerprint = _fingerprint(
            [
                _failure_projection(trial)
                for _key, trial in sorted(trials.items())
                if _trial_failed(trial)
            ]
        )
        groups: dict[str, list[tuple[tuple[str, int], dict[str, Any]]]] = defaultdict(list)
        signatures: dict[str, dict[str, Any]] = {}
        for key, trial in trials.items():
            if not _trial_failed(trial):
                continue
            signature_payload = _failure_signature_payload(trial)
            signature = _fingerprint(signature_payload)
            signatures[signature] = signature_payload
            groups[signature].append((key, trial))

        records: list[AdaptationFailureClusterRecord] = []
        for signature, members in sorted(groups.items()):
            if len(members) < 2:
                continue
            scenario_ids = sorted({key[0] for key, _trial in members})
            task_fingerprints = sorted({_trial_task_fingerprint(trial) for _key, trial in members})
            trial_refs = sorted(f"{key[0]}#{key[1]}" for key, _trial in members)
            payload = {
                "signature": signature,
                "source_summary_fingerprint": source_fingerprint,
                "failure_categories": signatures[signature]["failure_categories"],
                "scenario_ids": scenario_ids,
                "task_fingerprints": task_fingerprints,
                "trial_refs": trial_refs,
            }
            record = AdaptationFailureClusterRecord(
                id=f"adapt_cluster_{_fingerprint(payload)}",
                trial_count=len(trial_refs),
                created_at=utc_now(),
                **payload,
            )
            created = self.store.add_adaptation_failure_cluster_with_audit(
                record,
                _audit_event(
                    "adaptation_failure_cluster_created",
                    actor_ref=actor_ref,
                    correlation_id=record.id,
                    data={
                        "cluster_id": record.id,
                        "signature": record.signature,
                        "trial_count": record.trial_count,
                        "scenario_count": len(record.scenario_ids),
                        "failure_categories": record.failure_categories,
                    },
                ),
            )
            if not created:
                existing = self.store.get_adaptation_failure_cluster(record.id)
                if existing is None:
                    raise RuntimeError("adaptation cluster conflicted without a stored record")
                record = existing
            records.append(record)
        return records

    def create_candidate(
        self,
        definition: AdaptationCandidateDefinition,
        *,
        actor_ref: str = "operator_cli",
    ) -> AdaptationCandidateRecord:
        clusters = []
        for cluster_id in definition.source_cluster_ids:
            cluster = self.store.get_adaptation_failure_cluster(cluster_id)
            if cluster is None:
                raise ValueError("adaptation source cluster not found")
            clusters.append(cluster)
        if not clusters:
            raise ValueError("adaptation candidate requires source clusters")

        family_id = f"adapt_family_{_fingerprint([definition.kind, definition.target])}"
        family = self.store.list_adaptation_candidates(
            kind=definition.kind,
            target=definition.target,
            limit=1000,
        )
        parent = None
        if definition.parent_id is not None:
            parent = self.store.get_adaptation_candidate(definition.parent_id)
            if parent is None or parent.family_id != family_id:
                raise ValueError("adaptation parent must belong to the same artifact family")
        else:
            parent = next((item for item in family if item.status == "active"), None)

        definition_payload = definition.model_dump(mode="json")
        definition_payload["parent_id"] = parent.id if parent is not None else None
        definition_fingerprint = adaptation_candidate_definition_fingerprint(definition_payload)
        artifact_fingerprint = adaptation_artifact_fingerprint(
            definition.kind,
            definition.target,
            definition.change,
        )
        now = utc_now()
        record = AdaptationCandidateRecord(
            id=f"adapt_{definition_fingerprint}",
            family_id=family_id,
            version=max((item.version for item in family), default=0) + 1,
            kind=definition.kind,
            target=definition.target,
            artifact_fingerprint=artifact_fingerprint,
            definition_fingerprint=definition_fingerprint,
            hypothesis=definition.hypothesis,
            change=definition.change,
            source_cluster_ids=cast(list[Any], definition.source_cluster_ids),
            parent_id=parent.id if parent is not None else None,
            status="candidate",
            created_at=now,
            updated_at=now,
        )
        created = self.store.add_adaptation_candidate_with_audit(
            record,
            _audit_event(
                "adaptation_candidate_created",
                actor_ref=actor_ref,
                correlation_id=record.id,
                data={
                    "candidate_id": record.id,
                    "family_id": record.family_id,
                    "version": record.version,
                    "kind": record.kind,
                    "target": record.target,
                    "artifact_fingerprint": record.artifact_fingerprint,
                    "source_cluster_ids": list(record.source_cluster_ids),
                },
            ),
        )
        if not created:
            existing = self.store.get_adaptation_candidate(record.id)
            if existing is None:
                raise RuntimeError("adaptation candidate version changed during creation")
            return existing
        return record

    def evaluate(
        self,
        candidate_id: str,
        *,
        baseline: dict[str, Any],
        candidate: dict[str, Any],
        actor_ref: str = "operator_cli",
    ) -> AdaptationEvaluationRecord:
        record = self._get_candidate(candidate_id)
        if record.status == "retired":
            raise ValueError("retired adaptation candidates cannot be evaluated")
        baseline_trials = _validated_trials(baseline)
        candidate_trials = _validated_trials(candidate)
        if set(baseline_trials) != set(candidate_trials):
            raise ValueError("baseline and candidate must have identical trial coverage")
        if any(
            _trial_task_fingerprint(baseline_trials[key])
            != _trial_task_fingerprint(candidate_trials[key])
            for key in baseline_trials
        ):
            raise ValueError("baseline and candidate task coverage differs")
        scenario_ids = {scenario_id for scenario_id, _trial in baseline_trials}
        if len(scenario_ids) < 2:
            raise ValueError("adaptation evaluation requires at least two distinct scenarios")
        source_scenarios: set[str] = set()
        source_task_fingerprints: set[str] = set()
        for cluster_id in record.source_cluster_ids:
            cluster = self.store.get_adaptation_failure_cluster(cluster_id)
            if cluster is None:
                raise ValueError("adaptation source cluster no longer exists")
            source_scenarios.update(cluster.scenario_ids)
            source_task_fingerprints.update(cluster.task_fingerprints)
        evaluation_task_fingerprints = {
            _trial_task_fingerprint(trial) for trial in baseline_trials.values()
        }
        held_out = not bool(
            (source_scenarios & scenario_ids)
            or (source_task_fingerprints & evaluation_task_fingerprints)
        )
        if not held_out:
            raise ValueError(
                "adaptation evaluation scenarios must be held out from source clusters"
            )

        if any(_trial_adaptation(trial) is not None for trial in baseline_trials.values()):
            raise ValueError("baseline trials must not contain adaptation context")
        expected = {
            "id": record.id,
            "kind": record.kind,
            "target": record.target,
            "artifact_fingerprint": record.artifact_fingerprint,
        }
        if any(_trial_adaptation(trial) != expected for trial in candidate_trials.values()):
            raise ValueError("candidate trials must apply the exact target adaptation")
        if any(
            not isinstance(trial.get("grade"), dict)
            or not isinstance(trial["grade"].get("passed"), bool)
            for trial in (*baseline_trials.values(), *candidate_trials.values())
        ):
            raise ValueError("all adaptation trials must have deterministic grades")

        comparison = compare_worker_bench_to_baseline(summary=candidate, baseline=baseline)
        rates = comparison.get("success_rate")
        regressions = comparison.get("regressions")
        improvements = comparison.get("improvements")
        if (
            not isinstance(rates, dict)
            or not isinstance(regressions, list)
            or not isinstance(improvements, list)
        ):
            raise ValueError("adaptation comparison is incomplete")
        baseline_rate = rates.get("baseline")
        candidate_rate = rates.get("current")
        if not isinstance(baseline_rate, (int, float)) or not isinstance(
            candidate_rate, (int, float)
        ):
            raise ValueError("adaptation success rates are unavailable")
        rate_delta = float(candidate_rate) - float(baseline_rate)
        passed = (
            not bool(comparison.get("coverage_changed"))
            and held_out
            and not regressions
            and bool(improvements)
            and rate_delta > 0
        )
        trial_keys = [
            {
                "scenario_id": scenario_id,
                "trial": trial,
                "task_fingerprint": _trial_task_fingerprint(baseline_trials[(scenario_id, trial)]),
            }
            for scenario_id, trial in sorted(baseline_trials)
        ]
        payload = {
            "candidate_id": record.id,
            "baseline_fingerprint": _fingerprint(_evaluation_projection(baseline_trials)),
            "candidate_fingerprint": _fingerprint(_evaluation_projection(candidate_trials)),
            "scenario_set_fingerprint": _fingerprint(trial_keys),
            "common_trial_count": len(trial_keys),
            "distinct_scenario_count": len(scenario_ids),
            "baseline_success_rate": float(baseline_rate),
            "candidate_success_rate": float(candidate_rate),
            "success_rate_delta": round(rate_delta, 6),
            "regression_count": len(regressions),
            "improvement_count": len(improvements),
            "held_out": held_out,
            "passed": passed,
        }
        evaluation = AdaptationEvaluationRecord(
            id=f"adapt_eval_{_fingerprint(payload)}",
            created_at=utc_now(),
            **payload,
        )
        created = self.store.add_adaptation_evaluation_with_audit(
            evaluation,
            _audit_event(
                "adaptation_candidate_evaluated",
                actor_ref=actor_ref,
                correlation_id=record.id,
                data={
                    "candidate_id": record.id,
                    "evaluation_id": evaluation.id,
                    "passed": evaluation.passed,
                    "held_out": evaluation.held_out,
                    "common_trial_count": evaluation.common_trial_count,
                    "distinct_scenario_count": evaluation.distinct_scenario_count,
                    "regression_count": evaluation.regression_count,
                    "improvement_count": evaluation.improvement_count,
                    "success_rate_delta": evaluation.success_rate_delta,
                },
            ),
        )
        if not created:
            existing = self.store.get_adaptation_evaluation(evaluation.id)
            if existing is None:
                raise RuntimeError("adaptation evaluation conflicted without a stored record")
            return existing
        return evaluation

    def promote(
        self,
        candidate_id: str,
        *,
        actor_ref: str = "operator_cli",
    ) -> AdaptationCandidateRecord:
        record = self._get_candidate(candidate_id)
        if record.status != "candidate":
            raise ValueError(f"adaptation cannot be promoted from {record.status}")
        evaluation = self.store.get_latest_adaptation_evaluation(record.id)
        if evaluation is None or not evaluation.passed:
            raise ValueError("adaptation promotion requires a passing held-out evaluation")
        activated = self.store.activate_adaptation_candidate_with_audit(
            record.id,
            expected_statuses=["candidate"],
            evaluation_id=evaluation.id,
            updated_at=utc_now(),
            event=_audit_event(
                "adaptation_candidate_promoted",
                actor_ref=actor_ref,
                correlation_id=record.id,
                data={
                    "candidate_id": record.id,
                    "family_id": record.family_id,
                    "version": record.version,
                    "evaluation_id": evaluation.id,
                },
            ),
        )
        if not activated:
            raise RuntimeError("adaptation changed during promotion")
        return self._get_candidate(record.id)

    def rollback(
        self,
        candidate_id: str,
        *,
        actor_ref: str = "operator_cli",
    ) -> AdaptationCandidateRecord:
        target = self._get_candidate(candidate_id)
        if target.status != "retired":
            raise ValueError("rollback target must be a retired adaptation version")
        evaluation = self.store.get_latest_adaptation_evaluation(target.id)
        if evaluation is None or not evaluation.passed:
            raise ValueError("rollback target must retain a passing held-out evaluation")
        activated = self.store.activate_adaptation_candidate_with_audit(
            target.id,
            expected_statuses=["retired"],
            evaluation_id=evaluation.id,
            updated_at=utc_now(),
            event=_audit_event(
                "adaptation_candidate_rolled_back",
                actor_ref=actor_ref,
                correlation_id=target.id,
                data={
                    "candidate_id": target.id,
                    "family_id": target.family_id,
                    "version": target.version,
                    "evaluation_id": evaluation.id,
                },
            ),
        )
        if not activated:
            raise RuntimeError("adaptation family changed during rollback")
        return self._get_candidate(target.id)

    def context_for_evaluation(self, candidate_id: str) -> AdaptationContext:
        record = self._get_candidate(candidate_id)
        if record.status == "retired":
            raise ValueError("retired adaptation candidates cannot be evaluated")
        return AdaptationContext(
            id=record.id,
            kind=record.kind,
            target=record.target,
            artifact_fingerprint=record.artifact_fingerprint,
            change=record.change,
        )

    def get_candidate(self, candidate_id: str) -> AdaptationCandidateRecord | None:
        normalized = str(candidate_id or "").strip()
        if not normalized.startswith("adapt_") or len(normalized) != 70:
            return None
        return self.store.get_adaptation_candidate(normalized)

    def list_candidates(
        self,
        *,
        kind: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[AdaptationCandidateRecord]:
        normalized_kind = str(kind).strip().lower() if kind is not None else None
        if normalized_kind is not None and normalized_kind not in _ADAPTATION_KINDS:
            raise ValueError("invalid adaptation kind")
        if status is not None and status not in {"candidate", "active", "retired"}:
            raise ValueError("invalid adaptation status")
        return self.store.list_adaptation_candidates(
            kind=normalized_kind,
            status=status,
            limit=limit,
        )

    def latest_evaluation(self, candidate_id: str) -> AdaptationEvaluationRecord | None:
        record = self._get_candidate(candidate_id)
        return self.store.get_latest_adaptation_evaluation(record.id)

    def bandit_readiness(self) -> dict[str, Any]:
        active = self.store.list_adaptation_candidates(status="active", limit=1000)
        evaluated = [
            evaluation
            for item in active
            if (evaluation := self.store.get_latest_adaptation_evaluation(item.id)) is not None
        ]
        return {
            "enabled": False,
            "mode": "offline_candidates_only",
            "active_candidate_count": len(active),
            "passing_evaluation_count": sum(1 for item in evaluated if item.passed),
            "requirements": {
                "minimum_distinct_scenarios_per_arm": 20,
                "complete_token_cost_accounting": True,
                "explicit_online_experiment_approval": True,
                "bounded_exploration_and_rollback": True,
            },
            "reason": (
                "Contextual-bandit routing is deliberately unavailable until scenario coverage, "
                "complete usage accounting, and a separately approved online experiment exist."
            ),
        }

    def _get_candidate(self, candidate_id: str) -> AdaptationCandidateRecord:
        record = self.get_candidate(candidate_id)
        if record is None:
            raise ValueError("adaptation candidate not found")
        return record


def adaptation_candidate_metadata(record: AdaptationCandidateRecord) -> dict[str, Any]:
    return {
        "id": record.id,
        "family_id": record.family_id,
        "version": record.version,
        "kind": record.kind,
        "target": record.target,
        "artifact_fingerprint": record.artifact_fingerprint,
        "definition_fingerprint": record.definition_fingerprint,
        "source_cluster_ids": list(record.source_cluster_ids),
        "parent_id": record.parent_id,
        "status": record.status,
        "evaluation_id": record.evaluation_id,
        "hypothesis_chars": len(record.hypothesis),
        "change_keys": sorted(record.change),
        "created_at": record.created_at.isoformat(),
        "updated_at": record.updated_at.isoformat(),
    }


def adaptation_evaluation_payload(record: AdaptationEvaluationRecord) -> dict[str, Any]:
    return record.model_dump(mode="json")


def _validate_change(kind: AdaptationKind, target: str, change: dict[str, Any]) -> None:
    target_kind, _, _target_name = target.partition(":")
    if kind == "prompt":
        _require_exact_keys(change, {"append_instruction"})
        _bounded_text(change.get("append_instruction"), field="append_instruction", maximum=4000)
        if target_kind != "worker":
            raise ValueError("prompt adaptations must target a worker")
        return
    if kind == "tool_description":
        _require_exact_keys(change, {"append_description"})
        _bounded_text(change.get("append_description"), field="append_description", maximum=2000)
        if target_kind != "tool":
            raise ValueError("tool-description adaptations must target a tool")
        return
    if kind == "routing":
        _require_exact_keys(change, {"max_thinking_steps"})
        value = change.get("max_thinking_steps")
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 30:
            raise ValueError("routing max_thinking_steps must be an integer from 1 to 30")
        if target_kind != "worker":
            raise ValueError("routing adaptations must target a worker")
        return
    if kind == "recipe":
        _require_exact_keys(change, {"procedural_recipe"})
        ProceduralRecipeContext.model_validate(change.get("procedural_recipe"))
        if target_kind != "worker":
            raise ValueError("recipe adaptations must target a worker")
        return
    raise ValueError("unsupported adaptation kind")


def _require_exact_keys(value: dict[str, Any], expected: set[str]) -> None:
    if set(value) != expected:
        raise ValueError(f"adaptation change must contain exactly: {', '.join(sorted(expected))}")


def _bounded_text(value: Any, *, field: str, maximum: int) -> str:
    text = str(value or "").strip()
    if not text or len(text) > maximum:
        raise ValueError(f"adaptation {field} must contain 1 to {maximum} characters")
    return text


def _validated_trials(summary: dict[str, Any]) -> dict[tuple[str, int], dict[str, Any]]:
    raw = summary.get("scenarios")
    if not isinstance(raw, list):
        raise ValueError("benchmark summary must contain a scenarios list")
    trials: dict[tuple[str, int], dict[str, Any]] = {}
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"benchmark scenario {index} must be an object")
        scenario_id = str(item.get("scenario_id") or "").strip()
        if not scenario_id or len(scenario_id) > 128:
            raise ValueError(f"benchmark scenario {index} has an invalid id")
        trial = item.get("trial", 1)
        if isinstance(trial, bool) or not isinstance(trial, int) or trial <= 0:
            raise ValueError(f"benchmark scenario {scenario_id} has an invalid trial")
        key = (scenario_id, trial)
        if key in trials:
            raise ValueError(f"benchmark contains duplicate trial {scenario_id}#{trial}")
        _trial_task_fingerprint(item)
        trials[key] = item
    return trials


def _trial_failed(trial: dict[str, Any]) -> bool:
    if trial.get("returncode") not in (None, 0):
        return True
    if str(trial.get("status") or "").strip().lower() in {"failed", "timeout", "missing_result"}:
        return True
    grade = trial.get("grade")
    return isinstance(grade, dict) and grade.get("passed") is False


def _failure_categories(trial: dict[str, Any]) -> list[str]:
    categories: set[str] = set()
    if trial.get("returncode") not in (None, 0):
        categories.add("execution:nonzero_returncode")
    status = str(trial.get("status") or "").strip().lower()
    if status in {"failed", "timeout", "missing_result"}:
        categories.add(f"execution:{status}")
    grade = trial.get("grade")
    if isinstance(grade, dict):
        assertions = grade.get("assertions")
        if isinstance(assertions, list):
            for assertion in assertions:
                if isinstance(assertion, dict) and assertion.get("passed") is False:
                    grader_type = _safe_label(assertion.get("type"), fallback="unknown")
                    categories.add(f"grader:{grader_type}")
    return sorted(categories or {"execution:failed"})


def _failure_signature_payload(trial: dict[str, Any]) -> dict[str, Any]:
    manifest = trial.get("context_manifest")
    manifest_obj = manifest if isinstance(manifest, dict) else {}
    task = manifest_obj.get("task")
    task_obj = task if isinstance(task, dict) else {}
    raw_tools = trial.get("tools_used")
    tools = raw_tools if isinstance(raw_tools, list) else []
    return {
        "failure_categories": _failure_categories(trial),
        "status": str(trial.get("status") or "unknown").strip().lower(),
        "template_id": _safe_label(task_obj.get("template_id"), fallback="unknown"),
        "model": _safe_label(task_obj.get("model"), fallback="unknown"),
        "tools_used": sorted({_safe_label(item, fallback="unknown") for item in tools[:64]}),
    }


def _failure_projection(trial: dict[str, Any]) -> dict[str, Any]:
    return {
        "scenario_id": str(trial.get("scenario_id") or ""),
        "trial": trial.get("trial", 1),
        "task_fingerprint": _trial_task_fingerprint(trial),
        **_failure_signature_payload(trial),
    }


def _trial_adaptation(trial: dict[str, Any]) -> dict[str, Any] | None:
    manifest = trial.get("context_manifest")
    if not isinstance(manifest, dict):
        return None
    adaptation = manifest.get("adaptation")
    if not isinstance(adaptation, dict) or int(adaptation.get("count") or 0) == 0:
        return None
    return {
        "id": adaptation.get("id"),
        "kind": adaptation.get("kind"),
        "target": adaptation.get("target"),
        "artifact_fingerprint": adaptation.get("artifact_fingerprint"),
    }


def _trial_task_fingerprint(trial: dict[str, Any]) -> str:
    manifest = trial.get("context_manifest")
    task = manifest.get("task") if isinstance(manifest, dict) else None
    value = task.get("task_fingerprint") if isinstance(task, dict) else None
    fingerprint = str(value or "").strip().lower()
    if not re.fullmatch(r"[a-f0-9]{64}", fingerprint):
        raise ValueError("benchmark trial is missing a valid task fingerprint")
    return fingerprint


def _evaluation_projection(
    trials: dict[tuple[str, int], dict[str, Any]],
) -> list[dict[str, Any]]:
    projection = []
    for key, trial in sorted(trials.items()):
        grade = trial.get("grade")
        projection.append(
            {
                "scenario_id": key[0],
                "trial": key[1],
                "status": trial.get("status"),
                "returncode": trial.get("returncode"),
                "task_fingerprint": _trial_task_fingerprint(trial),
                "grade_passed": grade.get("passed") if isinstance(grade, dict) else None,
                "adaptation": _trial_adaptation(trial),
            }
        )
    return projection


def _audit_event(
    event_type: str,
    *,
    actor_ref: str,
    correlation_id: str,
    data: dict[str, Any],
) -> AuditEvent:
    return AuditEvent(
        id=str(uuid.uuid4()),
        ts=utc_now(),
        correlation_id=correlation_id,
        level="info",
        event_type=event_type,
        data={"actor_ref": _fingerprint(str(actor_ref or "operator"))[:16], **data},
    )


def _fingerprint(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _safe_label(value: Any, *, fallback: str) -> str:
    text = str(value or "").strip()
    return text if _SAFE_LABEL_RE.fullmatch(text) else fallback
