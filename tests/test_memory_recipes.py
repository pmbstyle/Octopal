from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from typer.testing import CliRunner

from octopal.cli import memory_recipes as recipes_cli
from octopal.cli.main import app
from octopal.infrastructure.store.models import (
    AuditEvent,
    ExecutionEpisodeRecord,
    IntentRecord,
    ProceduralRecipeContext,
    procedural_recipe_definition_fingerprint,
)
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.episodes import (
    worker_capability_fingerprint,
    worker_task_fingerprint,
)
from octopal.runtime.memory.recipes import (
    ProceduralRecipeCandidate,
    ProceduralRecipeService,
    recipe_metadata_payload,
)

runner = CliRunner()


class _Settings:
    def __init__(self, state_dir: Path, workspace_dir: Path) -> None:
        self.state_dir = state_dir
        self.workspace_dir = workspace_dir


def _store(tmp_path: Path) -> SQLiteStore:
    return SQLiteStore(_Settings(tmp_path / "data", tmp_path / "workspace"))


def _episode(
    episode_id: str,
    *,
    task_fingerprint: str = "a" * 64,
    capability_fingerprint: str = "b" * 64,
    status: str = "completed",
    verified: bool = True,
    outcome_contract_present: bool | None = None,
    trust_state: str = "observed",
    created_at: datetime | None = None,
) -> ExecutionEpisodeRecord:
    verification: dict[str, object] = {
        "result_contract_validated": True,
        "structured_output_present": True,
        "explicit_verification_present": verified,
        "grader_results": [],
    }
    if outcome_contract_present is not None:
        verification["verified"] = verified
        verification["outcome_contract_present"] = outcome_contract_present
    return ExecutionEpisodeRecord.model_validate(
        {
            "id": episode_id,
            "worker_run_id": f"worker-{episode_id}",
            "task_fingerprint": task_fingerprint,
            "environment_fingerprint": "c" * 64,
            "capability_fingerprint": capability_fingerprint,
            "result_fingerprint": "d" * 64,
            "status": status,
            "source_kind": "worker",
            "trust_state": trust_state,
            "trajectory_refs": {"worker_record_id": f"worker-{episode_id}"},
            "result_metadata": {},
            "verification": verification,
            "provenance": {"content_policy": "metadata_only_v1"},
            "created_at": created_at or datetime.now(UTC),
        }
    )


def _candidate(*episode_ids: str) -> ProceduralRecipeCandidate:
    return ProceduralRecipeCandidate(
        applicability_conditions=["The deployment target is a local test environment."],
        required_capabilities=["filesystem_read"],
        required_permissions=["filesystem_read"],
        strategy_steps=["Inspect current state.", "Apply the bounded change.", "Run checks."],
        verification_contract={"required_checks": ["pytest"], "no_regressions": True},
        known_failures=["Do not continue when the workspace is dirty."],
        invalidating_conditions=["The target is production."],
        source_episode_ids=list(episode_ids),
    )


def _bench_result(
    recipe_id: str | None,
    *,
    definition_fingerprint: str | None = None,
    passed: bool = True,
    scenario_id: str = "held-out",
) -> dict[str, object]:
    scenarios = []
    for current_scenario_id in (scenario_id, f"{scenario_id}-variant"):
        scenarios.append(
            {
                "scenario_id": current_scenario_id,
                "trial": 1,
                "status": "completed",
                "returncode": 0,
                "grade": {
                    "passed": passed,
                    "assertion_count": 1,
                    "passed_count": int(passed),
                    "assertions": [],
                },
                "context_manifest": {
                    "memory": {
                        "recipe_ids": [recipe_id] if recipe_id else [],
                        "recipe_definition_fingerprints": (
                            {recipe_id: definition_fingerprint}
                            if recipe_id and definition_fingerprint
                            else {}
                        ),
                    }
                },
            }
        )
    return {"scenarios": scenarios}


def _record_passing_evaluation(service: ProceduralRecipeService, recipe_id: str) -> None:
    recipe = service.get(recipe_id)
    assert recipe is not None
    evaluation = service.evaluate(
        recipe_id,
        baseline=_bench_result(None),
        candidate=_bench_result(
            recipe_id,
            definition_fingerprint=recipe.definition_fingerprint,
        ),
    )
    assert evaluation.passed is True


def test_recipe_candidate_is_episode_backed_metadata_only_until_explicit_show(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    episode = _episode("episode-1")
    store.add_execution_episode(episode)
    service = ProceduralRecipeService(store)

    record = service.create_candidate(_candidate(episode.id))

    assert record.status == "candidate"
    assert record.intent_fingerprint == episode.task_fingerprint
    assert record.success_count == 1
    assert service.get(record.id) == record
    metadata = recipe_metadata_payload(record)
    assert metadata["strategy_step_count"] == 3
    assert metadata["source_episode_count"] == 1
    assert "strategy_steps" not in metadata
    assert "source_episode_ids" not in metadata
    audits = store.list_audit_for_correlation(record.id)
    assert [event.event_type for event in audits] == ["procedural_recipe_candidate_created"]
    assert audits[0].data["actor"] == "operator"
    assert "actor_ref" not in audits[0].data
    assert len(audits[0].data["actor_ref_fingerprint"]) == 64


def test_recipe_candidate_deduplicates_source_order_and_atomic_retries(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _episode("episode-1")
    second = _episode("episode-2")
    store.add_execution_episode(first)
    store.add_execution_episode(second)
    service = ProceduralRecipeService(store)

    record = service.create_candidate(_candidate(first.id, second.id))
    duplicate = service.create_candidate(_candidate(second.id, first.id))
    duplicate_event = AuditEvent(
        id="duplicate-candidate-audit",
        ts=datetime.now(UTC),
        level="info",
        event_type="procedural_recipe_candidate_created",
    )

    assert duplicate.id == record.id
    assert duplicate.source_episode_ids == [first.id, second.id]
    assert store.add_procedural_recipe_with_audit(record, duplicate_event) is False
    assert [event.event_type for event in store.list_audit_for_correlation(record.id)] == [
        "procedural_recipe_candidate_created"
    ]


def test_recipe_candidate_rejects_unknown_definition_fields() -> None:
    payload = _candidate("episode-1").model_dump(mode="json")
    payload["required_permisisons"] = ["filesystem_write"]

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        ProceduralRecipeCandidate.model_validate(payload)


def test_recipe_context_rejects_a_claimed_fingerprint_for_changed_instructions() -> None:
    definition = _candidate("episode-1").model_dump(mode="json")
    definition.pop("source_episode_ids")
    fingerprint = procedural_recipe_definition_fingerprint(definition)
    definition["strategy_steps"] = ["Different unreviewed instructions."]

    with pytest.raises(ValueError, match="definition fingerprint does not match"):
        ProceduralRecipeContext(
            id=f"recipe_{'a' * 64}",
            definition_fingerprint=fingerprint,
            **definition,
        )


def test_recipe_promotion_requires_recurrent_verified_success_and_supports_deprecation(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    first = _episode("episode-1", created_at=datetime.now(UTC) - timedelta(minutes=1))
    second = _episode("episode-2")
    store.add_execution_episode(first)
    store.add_execution_episode(second)
    service = ProceduralRecipeService(store)

    single = service.create_candidate(_candidate(first.id))
    with pytest.raises(ValueError, match="at least two"):
        service.promote(single.id)

    recurrent = service.create_candidate(_candidate(first.id, second.id))
    with pytest.raises(ValueError, match="held-out evaluation"):
        service.promote(recurrent.id)
    _record_passing_evaluation(service, recurrent.id)
    active = service.promote(recurrent.id)
    assert active.status == "active"
    assert active.success_count == 2
    assert active.last_validated_at == second.created_at
    promoted_audit = store.list_audit_for_correlation(active.id)[-1]
    latest_evaluation = service.latest_evaluation(active.id)
    assert latest_evaluation is not None
    assert promoted_audit.data["evaluation_id"] == latest_evaluation.id
    deprecated = service.deprecate(active.id)
    assert deprecated.status == "deprecated"
    assert [event.event_type for event in store.list_audit_for_correlation(active.id)] == [
        "procedural_recipe_candidate_created",
        "procedural_recipe_evaluated",
        "procedural_recipe_promoted",
        "procedural_recipe_deprecated",
    ]


@pytest.mark.parametrize(
    ("episodes", "message"),
    [
        ([_episode("episode-failed", status="failed")], "not completed"),
        ([_episode("episode-unverified", verified=False)], "no successful verification"),
        (
            [
                _episode(
                    "episode-failed-outcome-contract",
                    verified=False,
                    outcome_contract_present=True,
                )
            ],
            "from host",
        ),
        (
            [_episode("episode-quarantined", trust_state="quarantined_candidate")],
            "trust state",
        ),
        (
            [
                _episode("episode-a", capability_fingerprint="1" * 64),
                _episode("episode-b", capability_fingerprint="2" * 64),
            ],
            "capability_fingerprint",
        ),
    ],
)
def test_recipe_gate_rejects_ineligible_sources(
    tmp_path: Path, episodes: list[ExecutionEpisodeRecord], message: str
) -> None:
    store = _store(tmp_path)
    for episode in episodes:
        store.add_execution_episode(episode)
    service = ProceduralRecipeService(store)
    if len(episodes) == 1:
        with pytest.raises(ValueError, match=message):
            service.create_candidate(_candidate(episodes[0].id))
        return
    record = service.create_candidate(_candidate(*(episode.id for episode in episodes)))
    with pytest.raises(ValueError, match=message):
        service.promote(record.id)


def test_recipe_definition_is_immutable_and_transition_audit_is_atomic(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _episode("episode-1")
    second = _episode("episode-2")
    store.add_execution_episode(first)
    store.add_execution_episode(second)
    record = ProceduralRecipeService(store).create_candidate(_candidate(first.id, second.id))

    with pytest.raises(sqlite3.IntegrityError, match="definition is immutable"):
        store._conn.execute(  # noqa: SLF001 - database invariant
            "UPDATE procedural_recipes SET strategy_steps_json = '[]' WHERE id = ?",
            (record.id,),
        )
    store._conn.rollback()  # noqa: SLF001 - clear failed direct transaction
    with pytest.raises(sqlite3.IntegrityError, match="cannot be deleted"):
        store._conn.execute(  # noqa: SLF001 - database invariant
            "DELETE FROM procedural_recipes WHERE id = ?", (record.id,)
        )
    store._conn.rollback()  # noqa: SLF001 - clear failed direct transaction

    refreshed_at = datetime.now(UTC)
    store._conn.execute(  # noqa: SLF001 - future revalidation state is mutable
        """
        UPDATE procedural_recipes
        SET success_count = ?, failure_count = ?, last_validated_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (3, 1, refreshed_at.isoformat(), refreshed_at.isoformat(), record.id),
    )
    store._conn.commit()  # noqa: SLF001 - direct invariant test
    refreshed = store.get_procedural_recipe(record.id)
    assert refreshed is not None
    assert (refreshed.success_count, refreshed.failure_count) == (3, 1)

    duplicate_audit = AuditEvent(
        id="duplicate-audit",
        ts=datetime.now(UTC),
        level="info",
        event_type="seed",
    )
    store.append_audit(duplicate_audit)
    with pytest.raises(sqlite3.IntegrityError):
        store.transition_procedural_recipe_with_audit(
            record.id,
            expected_statuses=["candidate"],
            new_status="active",
            updated_at=datetime.now(UTC),
            event=duplicate_audit,
        )
    unchanged = store.get_procedural_recipe(record.id)
    assert unchanged is not None
    assert unchanged.status == "candidate"


def test_database_allows_only_one_active_recipe_per_intent(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _episode("episode-1")
    second = _episode("episode-2")
    store.add_execution_episode(first)
    store.add_execution_episode(second)
    service = ProceduralRecipeService(store)
    first_recipe = service.create_candidate(_candidate(first.id, second.id))
    alternate = _candidate(first.id, second.id).model_copy(
        update={"strategy_steps": ["Use a distinct bounded strategy."]}
    )
    second_recipe = service.create_candidate(alternate)

    _record_passing_evaluation(service, first_recipe.id)
    assert service.promote(first_recipe.id).status == "active"
    transitioned = store.transition_procedural_recipe_with_audit(
        second_recipe.id,
        expected_statuses=["candidate"],
        new_status="active",
        updated_at=datetime.now(UTC),
        event=AuditEvent(
            id="second-promotion",
            ts=datetime.now(UTC),
            level="info",
            event_type="procedural_recipe_promoted",
        ),
    )

    assert transitioned is False
    still_candidate = store.get_procedural_recipe(second_recipe.id)
    assert still_candidate is not None
    assert still_candidate.status == "candidate"
    assert store.list_audit_for_correlation(second_recipe.id)[0].event_type == (
        "procedural_recipe_candidate_created"
    )


def test_recipe_consolidation_adds_only_matching_verified_episodes(tmp_path: Path) -> None:
    store = _store(tmp_path)
    episodes = [
        _episode("episode-1"),
        _episode("episode-2"),
        _episode("episode-3"),
        _episode("episode-failed", status="failed"),
        _episode("episode-other", capability_fingerprint="e" * 64),
    ]
    for episode in episodes:
        store.add_execution_episode(episode)

    record = ProceduralRecipeService(store).create_candidate(
        _candidate("episode-1"), include_matching=True
    )

    assert record.source_episode_ids == ["episode-1", "episode-2", "episode-3"]
    assert record.success_count == 3


def test_recipe_evaluation_rejects_regression_and_unattributed_candidate(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _episode("episode-1")
    second = _episode("episode-2")
    store.add_execution_episode(first)
    store.add_execution_episode(second)
    service = ProceduralRecipeService(store)
    recipe = service.create_candidate(_candidate(first.id, second.id))

    with pytest.raises(ValueError, match="target recipe"):
        service.evaluate(
            recipe.id,
            baseline=_bench_result(None),
            candidate=_bench_result(None),
        )
    with pytest.raises(ValueError, match="definition fingerprint"):
        service.evaluate(
            recipe.id,
            baseline=_bench_result(None),
            candidate=_bench_result(
                recipe.id,
                definition_fingerprint="f" * 64,
            ),
        )

    passed = service.evaluate(
        recipe.id,
        baseline=_bench_result(None, passed=True),
        candidate=_bench_result(
            recipe.id,
            definition_fingerprint=recipe.definition_fingerprint,
            passed=True,
        ),
    )
    failed = service.evaluate(
        recipe.id,
        baseline=_bench_result(None, passed=True),
        candidate=_bench_result(
            recipe.id,
            definition_fingerprint=recipe.definition_fingerprint,
            passed=False,
        ),
    )
    assert failed.passed is False
    assert failed.regression_count == 2
    repeated = service.evaluate(
        recipe.id,
        baseline=_bench_result(None, passed=True),
        candidate=_bench_result(
            recipe.id,
            definition_fingerprint=recipe.definition_fingerprint,
            passed=True,
        ),
    )
    assert repeated.id == passed.id
    assert service.latest_evaluation(recipe.id) == failed
    with pytest.raises(ValueError, match="held-out evaluation"):
        service.promote(recipe.id)


def test_active_evaluated_recipe_resolves_fail_closed_for_exact_worker_context(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    task = "Inspect the bounded fixture."
    inputs = {"path": "fixture.txt"}
    granted_capabilities: list[dict[str, object]] = []
    permissions = ["filesystem_read"]
    tools = ["filesystem_read"]
    task_fingerprint = worker_task_fingerprint(task, inputs)
    capability_fingerprint = worker_capability_fingerprint(
        granted_capabilities=granted_capabilities,
        effective_permissions=permissions,
        available_tools=tools,
        mcp_tools=[],
    )
    first = _episode(
        "episode-1",
        task_fingerprint=task_fingerprint,
        capability_fingerprint=capability_fingerprint,
    )
    second = _episode(
        "episode-2",
        task_fingerprint=task_fingerprint,
        capability_fingerprint=capability_fingerprint,
    )
    store.add_execution_episode(first)
    store.add_execution_episode(second)
    service = ProceduralRecipeService(store)
    recipe = service.create_candidate(_candidate(first.id, second.id))
    candidate_outcome = _episode("episode-candidate-outcome").model_copy(
        update={"provenance": {"procedural_recipe_ids": [recipe.id]}}
    )
    store.add_execution_episode(candidate_outcome)
    with pytest.raises(ValueError, match="candidate recipes"):
        service.record_outcome(recipe.id, candidate_outcome)
    _record_passing_evaluation(service, recipe.id)
    service.promote(recipe.id)

    resolved = service.resolve_for_worker(
        task=task,
        inputs=inputs,
        granted_capabilities=granted_capabilities,
        effective_permissions=permissions,
        available_tools=tools,
        mcp_tools=[],
    )

    assert [item.id for item in resolved] == [recipe.id]
    assert resolved[0].evaluation_id is not None
    assert (
        service.resolve_for_worker(
            task="Different task",
            inputs=inputs,
            granted_capabilities=granted_capabilities,
            effective_permissions=permissions,
            available_tools=tools,
            mcp_tools=[],
        )
        == []
    )
    assert (
        service.resolve_for_worker(
            task=task,
            inputs=inputs,
            granted_capabilities=granted_capabilities,
            effective_permissions=[],
            available_tools=tools,
            mcp_tools=[],
        )
        == []
    )
    failed_outcome = _episode(
        "episode-active-failure",
        task_fingerprint=task_fingerprint,
        capability_fingerprint=capability_fingerprint,
        status="failed",
    ).model_copy(update={"provenance": {"procedural_recipe_ids": [recipe.id]}})
    store.add_execution_episode(failed_outcome)
    assert service.record_outcome(recipe.id, failed_outcome) is True
    assert (
        service.resolve_for_worker(
            task=task,
            inputs=inputs,
            granted_capabilities=granted_capabilities,
            effective_permissions=permissions,
            available_tools=tools,
            mcp_tools=[],
        )
        == []
    )


def test_recipe_outcomes_are_idempotent_and_update_validation_counters(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _episode("episode-1")
    second = _episode("episode-2")
    store.add_execution_episode(first)
    store.add_execution_episode(second)
    service = ProceduralRecipeService(store)
    recipe = service.create_candidate(_candidate(first.id, second.id))
    _record_passing_evaluation(service, recipe.id)
    service.promote(recipe.id)
    outcome = _episode("episode-outcome").model_copy(
        update={"provenance": {"procedural_recipe_ids": [recipe.id]}}
    )
    store.add_execution_episode(outcome)

    assert service.record_outcome(recipe.id, outcome) is True
    assert service.record_outcome(recipe.id, outcome) is False
    updated = service.get(recipe.id)
    assert updated is not None
    assert updated.success_count == 3
    assert updated.failure_count == 0
    assert [event.event_type for event in store.list_audit_for_correlation(recipe.id)][
        -1
    ] == "procedural_recipe_outcome_recorded"

    latest_evaluation = service.latest_evaluation(recipe.id)
    assert latest_evaluation is not None
    with pytest.raises(sqlite3.IntegrityError, match="evaluations are immutable"):
        store._conn.execute(  # noqa: SLF001 - database invariant
            "UPDATE procedural_recipe_evaluations SET passed = 0 WHERE id = ?",
            (latest_evaluation.id,),
        )
    store._conn.rollback()  # noqa: SLF001 - clear failed direct transaction
    with pytest.raises(sqlite3.IntegrityError, match="outcomes are immutable"):
        store._conn.execute(  # noqa: SLF001 - database invariant
            "UPDATE procedural_recipe_outcomes SET succeeded = 0 WHERE episode_id = ?",
            (outcome.id,),
        )
    store._conn.rollback()  # noqa: SLF001 - clear failed direct transaction


def test_intent_recipe_provenance_column_migrates_existing_database(tmp_path: Path) -> None:
    state_dir = tmp_path / "data"
    state_dir.mkdir(parents=True)
    database = state_dir / "octopal.db"
    connection = sqlite3.connect(database)
    connection.execute("""
        CREATE TABLE intents (
            id TEXT PRIMARY KEY,
            worker_id TEXT NOT NULL,
            type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            risk TEXT NOT NULL,
            requires_approval INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """)
    connection.commit()
    connection.close()

    store = SQLiteStore(_Settings(state_dir, tmp_path / "workspace"))
    recipe_id = f"recipe_{'a' * 64}"
    store.save_intent(
        IntentRecord(
            id="intent-recipe",
            worker_id="worker-1",
            type="filesystem_write",
            payload={"path": "fixture.txt"},
            payload_hash="payload-hash",
            risk="high",
            requires_approval=True,
            procedural_recipe_ids=[recipe_id],
            status="pending",
            created_at=datetime.now(UTC),
        )
    )

    row = store._conn.execute(  # noqa: SLF001 - migration assertion
        "SELECT memory_influence_ids_json, procedural_recipe_ids_json FROM intents"
    ).fetchone()
    assert json.loads(row["memory_influence_ids_json"]) == []
    assert json.loads(row["procedural_recipe_ids_json"]) == [recipe_id]


def test_memory_recipes_cli_propose_inspect_promote_and_deprecate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _Settings(tmp_path / "data", tmp_path / "workspace")
    store = SQLiteStore(settings)
    first = _episode("episode-cli-1")
    second = _episode("episode-cli-2")
    store.add_execution_episode(first)
    store.add_execution_episode(second)
    definition_path = tmp_path / "recipe.json"
    definition_path.write_text(
        json.dumps(_candidate(first.id, second.id).model_dump(mode="json")),
        encoding="utf-8",
    )
    monkeypatch.setattr(recipes_cli, "load_settings", lambda: settings)

    proposed = runner.invoke(app, ["memory", "recipes", "propose", str(definition_path), "--json"])
    assert proposed.exit_code == 0
    recipe_id = json.loads(proposed.stdout)["recipe"]["id"]

    listed = runner.invoke(app, ["memory", "recipes", "list", "--json"])
    assert listed.exit_code == 0
    listed_recipe = json.loads(listed.stdout)["recipes"][0]
    assert listed_recipe["id"] == recipe_id
    assert "strategy_steps" not in listed_recipe
    assert "Inspect current state." not in listed.stdout

    shown = runner.invoke(app, ["memory", "recipes", "show", recipe_id, "--json"])
    assert shown.exit_code == 0
    assert json.loads(shown.stdout)["recipe"]["strategy_steps"][0] == ("Inspect current state.")
    assert json.loads(shown.stdout)["latest_evaluation"] is None

    context = runner.invoke(app, ["memory", "recipes", "context", recipe_id, "--json"])
    assert context.exit_code == 0
    context_payload = json.loads(context.stdout)["procedural_recipes"][0]
    assert context_payload["id"] == recipe_id

    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    baseline_path.write_text(json.dumps(_bench_result(None)), encoding="utf-8")
    candidate_path.write_text(
        json.dumps(
            _bench_result(
                recipe_id,
                definition_fingerprint=context_payload["definition_fingerprint"],
            )
        ),
        encoding="utf-8",
    )
    evaluated = runner.invoke(
        app,
        [
            "memory",
            "recipes",
            "evaluate",
            recipe_id,
            str(baseline_path),
            str(candidate_path),
            "--json",
        ],
    )
    assert evaluated.exit_code == 0
    assert json.loads(evaluated.stdout)["evaluation"]["passed"] is True

    confirmation = runner.invoke(app, ["memory", "recipes", "promote", recipe_id, "--json"])
    assert confirmation.exit_code == 1
    assert json.loads(confirmation.stdout)["error"]["code"] == "confirmation_required"

    promoted = runner.invoke(app, ["memory", "recipes", "promote", recipe_id, "--yes", "--json"])
    assert promoted.exit_code == 0
    assert json.loads(promoted.stdout)["status"] == "active"

    deprecated = runner.invoke(
        app, ["memory", "recipes", "deprecate", recipe_id, "--yes", "--json"]
    )
    assert deprecated.exit_code == 0
    assert json.loads(deprecated.stdout)["status"] == "deprecated"
