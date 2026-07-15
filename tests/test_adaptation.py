from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from octopal.cli import adaptation as adaptation_cli
from octopal.cli.main import app
from octopal.infrastructure.store.models import (
    AdaptationContext,
    procedural_recipe_definition_fingerprint,
)
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.adaptation import (
    AdaptationCandidateDefinition,
    AdaptationService,
    adaptation_candidate_metadata,
)
from octopal.runtime.workers.agent_worker import (
    _adaptation_manifest,
    _apply_tool_description_adaptation,
    _build_adaptation_prompt,
)
from octopal.runtime.workers.bench import (
    WorkerBenchScenario,
    build_worker_spec,
    load_scenarios_file,
)
from octopal.runtime.workers.contracts import WorkerSpec
from octopal.tools.registry import ToolSpec


class _StoreSettings:
    def __init__(self, root: Path) -> None:
        self.state_dir = root / "data"
        self.workspace_dir = root / "workspace"


def _store(tmp_path: Path) -> SQLiteStore:
    return SQLiteStore(_StoreSettings(tmp_path))


def _trial(
    scenario_id: str,
    *,
    passed: bool,
    trial: int = 1,
    adaptation: AdaptationContext | None = None,
    task_key: str | None = None,
) -> dict:
    manifest = {
        "task": {
            "template_id": "demo",
            "model": "test-model",
            "task_fingerprint": hashlib.sha256((task_key or scenario_id).encode()).hexdigest(),
        },
        "adaptation": (
            {
                "count": 1,
                "id": adaptation.id,
                "kind": adaptation.kind,
                "target": adaptation.target,
                "artifact_fingerprint": adaptation.artifact_fingerprint,
            }
            if adaptation is not None
            else {"count": 0}
        ),
    }
    return {
        "scenario_id": scenario_id,
        "trial": trial,
        "status": "completed" if passed else "failed",
        "returncode": 0,
        "tools_used": ["read_data"],
        "context_manifest": manifest,
        "grade": {
            "passed": passed,
            "assertion_count": 1,
            "passed_count": 1 if passed else 0,
            "assertions": [
                {
                    "type": "structured_output",
                    "passed": passed,
                    "evidence": {"domain_key_count": 1 if passed else 0},
                }
            ],
        },
    }


def _summary(*trials: dict) -> dict:
    return {"scenarios": list(trials)}


def _source_summary() -> dict:
    return _summary(
        _trial("train_a", passed=False),
        _trial("train_b", passed=False),
        _trial("unrelated", passed=True),
    )


def _candidate_definition(cluster_id: str, instruction: str = "Verify structured output."):
    return AdaptationCandidateDefinition(
        kind="prompt",
        target="worker:demo",
        hypothesis="Missing verification guidance causes false completion.",
        change={"append_instruction": instruction},
        source_cluster_ids=[cluster_id],
    )


def _recipe_context_payload() -> dict:
    payload = {
        "applicability_conditions": ["The fixture is available."],
        "required_capabilities": [],
        "required_permissions": [],
        "strategy_steps": ["Inspect the fixture."],
        "verification_contract": {"required": ["result"]},
        "known_failures": [],
        "invalidating_conditions": [],
    }
    return {
        "id": "recipe_" + "a" * 64,
        "evaluation_id": None,
        "definition_fingerprint": procedural_recipe_definition_fingerprint(payload),
        **payload,
    }


def _create_candidate(service: AdaptationService):
    clusters = service.cluster_failures(_source_summary())
    assert len(clusters) == 1
    return service.create_candidate(_candidate_definition(clusters[0].id)), clusters[0]


def _passing_evaluation(service: AdaptationService, candidate_id: str):
    context = service.context_for_evaluation(candidate_id)
    baseline = _summary(
        _trial("heldout_a", passed=False),
        _trial("heldout_b", passed=True),
    )
    candidate = _summary(
        _trial("heldout_a", passed=True, adaptation=context),
        _trial("heldout_b", passed=True, adaptation=context),
    )
    return service.evaluate(candidate_id, baseline=baseline, candidate=candidate)


def _worker_spec(adaptation: AdaptationContext) -> WorkerSpec:
    return WorkerSpec(
        id="worker-1",
        template_id="demo",
        task="Test adaptation",
        inputs={},
        system_prompt="Base prompt",
        available_tools=["read_data"],
        granted_capabilities=[],
        timeout_seconds=30,
        max_thinking_steps=5,
        lifecycle="benchmark",
        adaptations=[adaptation],
    )


def test_failure_clustering_is_recurrent_metadata_only_and_idempotent(tmp_path: Path) -> None:
    store = _store(tmp_path)
    service = AdaptationService(store)

    first = service.cluster_failures(_source_summary())
    second = service.cluster_failures(_source_summary())

    assert len(first) == 1
    assert second == first
    cluster = first[0]
    assert cluster.failure_categories == ["execution:failed", "grader:structured_output"]
    assert cluster.scenario_ids == ["train_a", "train_b"]
    assert cluster.task_fingerprints == sorted(
        hashlib.sha256(value.encode()).hexdigest() for value in ("train_a", "train_b")
    )
    assert cluster.trial_count == 2
    assert "summary" not in cluster.model_dump()
    events = store.list_audit_for_correlation(cluster.id)
    assert [event.event_type for event in events] == ["adaptation_failure_cluster_created"]


def test_single_failure_does_not_create_recurrent_cluster(tmp_path: Path) -> None:
    service = AdaptationService(_store(tmp_path))

    assert service.cluster_failures(_summary(_trial("only", passed=False))) == []


@pytest.mark.parametrize(
    ("kind", "target", "change"),
    [
        ("prompt", "worker:demo", {"append_instruction": "Check the result."}),
        ("tool_description", "tool:read_data", {"append_description": "Use for records."}),
        ("routing", "worker:demo", {"max_thinking_steps": 8}),
    ],
)
def test_candidate_change_contracts_accept_bounded_supported_shapes(
    kind: str,
    target: str,
    change: dict,
) -> None:
    definition = AdaptationCandidateDefinition(
        kind=kind,
        target=target,
        hypothesis="Measured recurrent failure.",
        change=change,
        source_cluster_ids=["adapt_cluster_" + "a" * 64],
    )

    assert definition.kind == kind


def test_candidate_change_contracts_reject_broad_or_ambiguous_mutations() -> None:
    with pytest.raises(ValueError, match="exactly"):
        AdaptationCandidateDefinition(
            kind="prompt",
            target="worker:demo",
            hypothesis="Bad shape.",
            change={"append_instruction": "x", "replace_everything": True},
            source_cluster_ids=["adapt_cluster_" + "a" * 64],
        )
    with pytest.raises(ValueError, match="integer from 1 to 30"):
        AdaptationCandidateDefinition(
            kind="routing",
            target="worker:demo",
            hypothesis="Bad budget.",
            change={"max_thinking_steps": True},
            source_cluster_ids=["adapt_cluster_" + "a" * 64],
        )


def test_candidate_lifecycle_requires_heldout_improvement_and_supports_rollback(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    service = AdaptationService(store)
    first, cluster = _create_candidate(service)

    with pytest.raises(ValueError, match="passing held-out"):
        service.promote(first.id)

    first_evaluation = _passing_evaluation(service, first.id)
    assert first_evaluation.passed is True
    assert first_evaluation.improvement_count == 1
    assert first_evaluation.regression_count == 0
    active_first = service.promote(first.id)
    assert active_first.status == "active"
    assert active_first.evaluation_id == first_evaluation.id

    second = service.create_candidate(
        _candidate_definition(cluster.id, "Verify output and cite the checked field.")
    )
    assert second.version == 2
    assert second.parent_id == first.id
    second_evaluation = _passing_evaluation(service, second.id)
    service.promote(second.id)
    assert service.get_candidate(first.id).status == "retired"  # type: ignore[union-attr]
    assert service.get_candidate(second.id).status == "active"  # type: ignore[union-attr]

    rolled_back = service.rollback(first.id)
    assert rolled_back.status == "active"
    assert rolled_back.evaluation_id == first_evaluation.id
    assert service.get_candidate(second.id).status == "retired"  # type: ignore[union-attr]
    assert second_evaluation.passed is True


def test_evaluation_rejects_training_overlap_and_forged_candidate_context(tmp_path: Path) -> None:
    service = AdaptationService(_store(tmp_path))
    record, _cluster = _create_candidate(service)
    context = service.context_for_evaluation(record.id)

    with pytest.raises(ValueError, match="held out"):
        service.evaluate(
            record.id,
            baseline=_summary(_trial("train_a", passed=False), _trial("train_b", passed=True)),
            candidate=_summary(
                _trial("train_a", passed=True, adaptation=context),
                _trial("train_b", passed=True, adaptation=context),
            ),
        )

    with pytest.raises(ValueError, match="held out"):
        service.evaluate(
            record.id,
            baseline=_summary(
                _trial("renamed_a", passed=False, task_key="train_a"),
                _trial("renamed_b", passed=True, task_key="train_b"),
            ),
            candidate=_summary(
                _trial("renamed_a", passed=True, adaptation=context, task_key="train_a"),
                _trial("renamed_b", passed=True, adaptation=context, task_key="train_b"),
            ),
        )

    forged = context.model_copy(update={"id": "adapt_" + "f" * 64})
    with pytest.raises(ValueError, match="exact target adaptation"):
        service.evaluate(
            record.id,
            baseline=_summary(
                _trial("heldout_a", passed=False),
                _trial("heldout_b", passed=True),
            ),
            candidate=_summary(
                _trial("heldout_a", passed=True, adaptation=forged),
                _trial("heldout_b", passed=True, adaptation=forged),
            ),
        )

    with pytest.raises(ValueError, match="baseline trials must not"):
        service.evaluate(
            record.id,
            baseline=_summary(
                _trial("heldout_a", passed=False, adaptation=context),
                _trial("heldout_b", passed=True, adaptation=context),
            ),
            candidate=_summary(
                _trial("heldout_a", passed=True, adaptation=context),
                _trial("heldout_b", passed=True, adaptation=context),
            ),
        )


def test_evaluation_rejects_changed_or_missing_task_fingerprints(tmp_path: Path) -> None:
    service = AdaptationService(_store(tmp_path))
    record, _cluster = _create_candidate(service)
    context = service.context_for_evaluation(record.id)
    baseline = _summary(
        _trial("heldout_a", passed=False),
        _trial("heldout_b", passed=True),
    )
    changed_task = _summary(
        _trial("heldout_a", passed=True, adaptation=context, task_key="different"),
        _trial("heldout_b", passed=True, adaptation=context),
    )

    with pytest.raises(ValueError, match="task coverage differs"):
        service.evaluate(record.id, baseline=baseline, candidate=changed_task)

    missing_task = _trial("heldout_a", passed=False)
    del missing_task["context_manifest"]["task"]["task_fingerprint"]
    with pytest.raises(ValueError, match="valid task fingerprint"):
        service.evaluate(
            record.id,
            baseline=_summary(missing_task, _trial("heldout_b", passed=True)),
            candidate=_summary(
                _trial("heldout_a", passed=True, adaptation=context),
                _trial("heldout_b", passed=True, adaptation=context),
            ),
        )


def test_evaluation_rejects_any_ungraded_trial(tmp_path: Path) -> None:
    service = AdaptationService(_store(tmp_path))
    record, _cluster = _create_candidate(service)
    context = service.context_for_evaluation(record.id)
    ungraded = _trial("heldout_a", passed=False)
    ungraded["grade"]["passed"] = None

    with pytest.raises(ValueError, match="deterministic grades"):
        service.evaluate(
            record.id,
            baseline=_summary(ungraded, _trial("heldout_b", passed=True)),
            candidate=_summary(
                _trial("heldout_a", passed=True, adaptation=context),
                _trial("heldout_b", passed=True, adaptation=context),
            ),
        )


def test_non_improving_comparison_is_recorded_but_cannot_promote(tmp_path: Path) -> None:
    service = AdaptationService(_store(tmp_path))
    record, _cluster = _create_candidate(service)
    context = service.context_for_evaluation(record.id)
    baseline = _summary(_trial("heldout_a", passed=True), _trial("heldout_b", passed=True))
    candidate = _summary(
        _trial("heldout_a", passed=True, adaptation=context),
        _trial("heldout_b", passed=True, adaptation=context),
    )

    evaluation = service.evaluate(record.id, baseline=baseline, candidate=candidate)

    assert evaluation.passed is False
    assert evaluation.success_rate_delta == 0
    with pytest.raises(ValueError, match="passing held-out"):
        service.promote(record.id)


def test_evaluation_record_rejects_forged_passing_state_or_identity(tmp_path: Path) -> None:
    service = AdaptationService(_store(tmp_path))
    record, _cluster = _create_candidate(service)
    evaluation = _passing_evaluation(service, record.id)
    payload = evaluation.model_dump(mode="json")

    payload["held_out"] = False
    with pytest.raises(ValueError, match="violates promotion criteria"):
        type(evaluation).model_validate(payload)

    payload = evaluation.model_dump(mode="json")
    payload["id"] = "adapt_eval_" + "f" * 64
    with pytest.raises(ValueError, match="id does not match"):
        type(evaluation).model_validate(payload)


def test_adaptation_rows_are_immutable_and_list_metadata_hides_artifact(tmp_path: Path) -> None:
    store = _store(tmp_path)
    service = AdaptationService(store)
    record, cluster = _create_candidate(service)
    metadata = adaptation_candidate_metadata(record)

    assert "change" not in metadata
    assert "hypothesis" not in metadata
    assert metadata["change_keys"] == ["append_instruction"]
    with pytest.raises(sqlite3.IntegrityError, match="definition is immutable"):
        store._conn.execute(  # type: ignore[attr-defined]
            "UPDATE adaptation_candidates SET hypothesis = 'rewritten' WHERE id = ?",
            (record.id,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="cannot be deleted"):
        store._conn.execute(  # type: ignore[attr-defined]
            "DELETE FROM adaptation_failure_clusters WHERE id = ?",
            (cluster.id,),
        )


def test_prompt_tool_and_routing_adaptations_apply_only_in_benchmark_spec(tmp_path: Path) -> None:
    service = AdaptationService(_store(tmp_path))
    prompt_record, cluster = _create_candidate(service)
    prompt_context = service.context_for_evaluation(prompt_record.id)
    template = {
        "name": "Demo",
        "system_prompt": "Base prompt",
        "available_tools": ["read_data"],
        "required_permissions": ["fs_read"],
        "max_thinking_steps": 5,
        "default_timeout_seconds": 30,
    }
    prompt_spec = build_worker_spec(
        scenario=WorkerBenchScenario(
            id="prompt_eval",
            template_id="demo",
            task="Test",
            inputs={},
            adaptations=(prompt_context,),
        ),
        template=template,
        run_id="prompt-run",
    )
    assert prompt_spec["system_prompt"] == "Base prompt"
    assert prompt_spec["adaptations"][0]["id"] == prompt_record.id
    assert "Verify structured output." in _build_adaptation_prompt(
        WorkerSpec.model_validate(prompt_spec)
    )

    routing_record = service.create_candidate(
        AdaptationCandidateDefinition(
            kind="routing",
            target="worker:demo",
            hypothesis="The bounded run needs one more reasoning step.",
            change={"max_thinking_steps": 8},
            source_cluster_ids=[cluster.id],
        )
    )
    routing_spec = build_worker_spec(
        scenario=WorkerBenchScenario(
            id="routing_eval",
            template_id="demo",
            task="Test",
            inputs={},
            adaptations=(service.context_for_evaluation(routing_record.id),),
        ),
        template=template,
        run_id="routing-run",
    )
    assert routing_spec["max_thinking_steps"] == 8
    assert routing_spec["strict_thinking_budget"] is True

    recipe_record = service.create_candidate(
        AdaptationCandidateDefinition(
            kind="recipe",
            target="worker:demo",
            hypothesis="The task benefits from an evaluated procedural strategy.",
            change={"procedural_recipe": _recipe_context_payload()},
            source_cluster_ids=[cluster.id],
        )
    )
    recipe_spec = build_worker_spec(
        scenario=WorkerBenchScenario(
            id="recipe_eval",
            template_id="demo",
            task="Test",
            inputs={},
            adaptations=(service.context_for_evaluation(recipe_record.id),),
        ),
        template=template,
        run_id="recipe-run",
    )
    assert recipe_spec["procedural_recipes"][0]["id"] == "recipe_" + "a" * 64

    tool_record = service.create_candidate(
        AdaptationCandidateDefinition(
            kind="tool_description",
            target="tool:read_data",
            hypothesis="The tool boundary is unclear.",
            change={"append_description": "Use only for read-only records."},
            source_cluster_ids=[cluster.id],
        )
    )
    tool_context = service.context_for_evaluation(tool_record.id)
    tool = ToolSpec(
        name="read_data",
        description="Read records.",
        parameters={"type": "object"},
        permission="fs_read",
        handler=lambda _args, _ctx: None,
    )
    adapted = _apply_tool_description_adaptation([tool], _worker_spec(tool_context))
    assert adapted[0].description == "Read records.\nUse only for read-only records."
    assert tool.description == "Read records."


def test_context_manifest_contains_only_candidate_identity(tmp_path: Path) -> None:
    service = AdaptationService(_store(tmp_path))
    record, _cluster = _create_candidate(service)
    context = service.context_for_evaluation(record.id)

    manifest = _adaptation_manifest(_worker_spec(context))

    assert manifest == {
        "count": 1,
        "id": record.id,
        "kind": "prompt",
        "target": "worker:demo",
        "artifact_fingerprint": record.artifact_fingerprint,
    }
    assert "instruction" not in json.dumps(manifest)


def test_adaptation_context_is_rejected_outside_benchmark_lifecycle(tmp_path: Path) -> None:
    service = AdaptationService(_store(tmp_path))
    record, _cluster = _create_candidate(service)
    context = service.context_for_evaluation(record.id)

    with pytest.raises(ValueError, match="restricted to benchmark lifecycle"):
        WorkerSpec(
            id="production-worker",
            template_id="demo",
            task="Do production work",
            inputs={},
            system_prompt="Base prompt",
            available_tools=[],
            granted_capabilities=[],
            timeout_seconds=30,
            max_thinking_steps=5,
            lifecycle="ephemeral",
            adaptations=[context],
        )


def test_suite_validation_rejects_target_mismatch_and_conflicting_routing(tmp_path: Path) -> None:
    from octopal.infrastructure.store.models import adaptation_artifact_fingerprint

    change = {"max_thinking_steps": 8}
    context = AdaptationContext(
        id="adapt_" + "a" * 64,
        kind="routing",
        target="worker:other",
        artifact_fingerprint=adaptation_artifact_fingerprint("routing", "worker:other", change),
        change=change,
    )
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(
        json.dumps(
            {
                "version": 1,
                "scenarios": [
                    {
                        "id": "case",
                        "template_id": "demo",
                        "task": "Test",
                        "max_thinking_steps": 5,
                        "graders": [{"type": "terminal_status", "expected": "completed"}],
                        "adaptations": [context.model_dump(mode="json")],
                    }
                ],
            }
        )
    )

    with pytest.raises(ValueError, match="must target worker:demo"):
        load_scenarios_file(suite_path)


def test_bandit_readiness_is_explicitly_disabled(tmp_path: Path) -> None:
    readiness = AdaptationService(_store(tmp_path)).bandit_readiness()

    assert readiness["enabled"] is False
    assert readiness["mode"] == "offline_candidates_only"
    assert readiness["requirements"]["complete_token_cost_accounting"] is True


def test_adaptation_cli_cluster_propose_evaluate_promote_and_rollback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _StoreSettings(tmp_path)
    monkeypatch.setattr(adaptation_cli, "load_settings", lambda: settings)
    runner = CliRunner()
    source_path = tmp_path / "source.json"
    source_path.write_text(json.dumps(_source_summary()))

    clustered = runner.invoke(app, ["adaptation", "cluster", str(source_path), "--json"])
    assert clustered.exit_code == 0, clustered.stdout
    cluster_id = json.loads(clustered.stdout)["clusters"][0]["id"]
    definition_path = tmp_path / "definition.json"
    definition_path.write_text(
        json.dumps(_candidate_definition(cluster_id).model_dump(mode="json"))
    )

    proposed = runner.invoke(app, ["adaptation", "propose", str(definition_path), "--json"])
    assert proposed.exit_code == 0, proposed.stdout
    candidate_id = json.loads(proposed.stdout)["candidate"]["id"]
    listed = runner.invoke(app, ["adaptation", "list", "--json"])
    assert listed.exit_code == 0
    assert "change" not in json.loads(listed.stdout)["candidates"][0]
    context_result = runner.invoke(app, ["adaptation", "context", candidate_id, "--json"])
    context = AdaptationContext.model_validate(json.loads(context_result.stdout)["adaptations"][0])

    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    baseline_path.write_text(
        json.dumps(_summary(_trial("heldout_a", passed=False), _trial("heldout_b", passed=True)))
    )
    candidate_path.write_text(
        json.dumps(
            _summary(
                _trial("heldout_a", passed=True, adaptation=context),
                _trial("heldout_b", passed=True, adaptation=context),
            )
        )
    )
    evaluated = runner.invoke(
        app,
        [
            "adaptation",
            "evaluate",
            candidate_id,
            str(baseline_path),
            str(candidate_path),
            "--json",
        ],
    )
    assert evaluated.exit_code == 0, evaluated.stdout
    assert json.loads(evaluated.stdout)["evaluation"]["passed"] is True
    confirmation = runner.invoke(app, ["adaptation", "promote", candidate_id, "--json"])
    assert confirmation.exit_code == 1
    promoted = runner.invoke(app, ["adaptation", "promote", candidate_id, "--yes", "--json"])
    assert promoted.exit_code == 0, promoted.stdout
    assert json.loads(promoted.stdout)["candidate"]["status"] == "active"
    readiness = runner.invoke(app, ["adaptation", "bandit-readiness", "--json"])
    assert readiness.exit_code == 0
    assert json.loads(readiness.stdout)["enabled"] is False


def test_adaptation_cli_rejects_invalid_definition_without_echoing_content(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _StoreSettings(tmp_path)
    monkeypatch.setattr(adaptation_cli, "load_settings", lambda: settings)
    definition_path = tmp_path / "invalid.json"
    definition_path.write_text('{"secret_value":"do-not-echo"}')

    result = CliRunner().invoke(app, ["adaptation", "propose", str(definition_path), "--json"])

    assert result.exit_code == 1
    assert "do-not-echo" not in result.stdout
    assert json.loads(result.stdout)["error"]["code"] == "invalid_adaptation_definition"


def test_schema_reopen_preserves_adaptation_records(tmp_path: Path) -> None:
    first = _store(tmp_path)
    service = AdaptationService(first)
    record, cluster = _create_candidate(service)
    first._conn.close()  # type: ignore[attr-defined]

    reopened = _store(tmp_path)

    assert reopened.get_adaptation_candidate(record.id) == record
    assert reopened.get_adaptation_failure_cluster(cluster.id) == cluster
    assert datetime.fromisoformat(record.created_at.isoformat()).tzinfo == UTC
