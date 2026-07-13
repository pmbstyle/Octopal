from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from octopal.infrastructure.config.models import OctopalConfig
from octopal.runtime.workers.bench import (
    WorkerBenchIsolation,
    WorkerBenchScenario,
    _prepare_live_artifacts_dir,
    _prepare_worker_bench_isolation,
    _resolved_pricing_model,
    _run_scenarios,
    _worker_bench_env,
    aggregate_worker_bench_summaries,
    build_worker_spec,
    compare_worker_bench_to_baseline,
    grade_worker_messages,
    load_scenarios_file,
    main,
    parse_worker_jsonl,
    preflight_worker_bench,
    run_worker_bench,
    select_scenarios,
    summarize_worker_messages,
)
from octopal.runtime.workers.contracts import WorkerInferenceBudget, WorkerSpec


def _budgeted_live_scenario(
    *, scenario_id: str = "web_search_ranked_pricing"
) -> WorkerBenchScenario:
    model = "glm-test"
    return WorkerBenchScenario(
        id=scenario_id,
        template_id="web_search_ranked",
        task="Run a bounded live test",
        inputs={},
        provider_id="zai",
        model=model,
        max_thinking_steps=4,
        inference_budget=WorkerInferenceBudget(
            pricing_model="openai/glm-test",
            max_llm_calls=4,
            max_total_tokens=20_000,
            max_cost_microusd=20_000,
            input_cost_microusd_per_million_tokens=200_000,
            completion_cost_microusd_per_million_tokens=1_000_000,
        ),
    )


def _live_source_config() -> OctopalConfig:
    return OctopalConfig.model_validate(
        {
            "llm": {
                "provider_id": "zai",
                "model": "glm-test",
                "api_key": "test-provider-key",
            },
            "search": {"brave_api_key": "test-search-key"},
        }
    )


def _write_live_template(workspace_dir: Path, *, tools: list[str] | None = None) -> None:
    template_dir = workspace_dir / "workers" / "web_search_ranked"
    template_dir.mkdir(parents=True)
    (template_dir / "worker.json").write_text(
        json.dumps(
            {
                "name": "Web Search Ranked",
                "system_prompt": "Search carefully.",
                "available_tools": tools or ["web_search"],
                "required_permissions": ["network"],
                "max_thinking_steps": 4,
                "default_timeout_seconds": 30,
            }
        ),
        encoding="utf-8",
    )


def test_select_scenarios_rejects_unknown_id() -> None:
    try:
        select_scenarios(["missing"])
    except ValueError as exc:
        assert "Unknown scenario" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_build_worker_spec_uses_template_contract() -> None:
    scenario = WorkerBenchScenario(
        id="example",
        template_id="web_search_ranked",
        task="Search docs",
        inputs={"query": "Octopal"},
    )
    template = {
        "name": "Web Search Ranked",
        "system_prompt": "Search carefully.",
        "available_tools": ["web_search"],
        "required_permissions": ["network"],
        "default_timeout_seconds": 180,
        "max_thinking_steps": 6,
    }

    spec = build_worker_spec(scenario=scenario, template=template, run_id="bench-example")

    assert spec["id"] == "bench-example"
    assert spec["template_id"] == "web_search_ranked"
    assert spec["task"] == "Search docs"
    assert spec["inputs"] == {"query": "Octopal"}
    assert spec["available_tools"] == ["web_search"]
    assert spec["effective_permissions"] == ["network"]
    assert spec["timeout_seconds"] == 180
    assert spec["max_thinking_steps"] == 6
    assert spec["strict_thinking_budget"] is False


def test_build_worker_spec_applies_strict_live_budget() -> None:
    scenario = _budgeted_live_scenario(scenario_id="strict-live")

    spec = build_worker_spec(
        scenario=scenario,
        template={"name": "Bounded worker", "max_thinking_steps": 12},
        run_id="bench-strict-live",
    )

    assert spec["model"] == "glm-test"
    assert spec["llm_config"] == {"provider_id": "zai", "model": "glm-test"}
    assert spec["max_thinking_steps"] == 4
    assert spec["strict_thinking_budget"] is True
    assert spec["inference_budget"]["max_total_tokens"] == 20_000
    assert spec["inference_budget"]["max_llm_calls"] == 4
    assert spec["inference_budget"]["max_cost_microusd"] == 20_000
    assert WorkerSpec.model_validate(spec).strict_thinking_budget is True


def test_parse_worker_jsonl_skips_non_json_lines() -> None:
    messages, errors = parse_worker_jsonl('{"type":"log"}\nnot-json\n[1]\n')

    assert messages == [{"type": "log"}]
    assert errors == ["not-json", "[1]"]


def test_summarize_worker_messages_extracts_telemetry() -> None:
    result = {
        "type": "result",
        "result": {
            "status": "completed",
            "summary": "done",
            "thinking_steps": 2,
            "tools_used": ["web_search"],
            "output": {
                "_telemetry": {
                    "llm_calls": 2,
                    "tool_calls": 1,
                    "llm_latency_ms_total": 1234,
                    "tool_latency_ms_total": 55,
                    "tokens": {
                        "prompt_tokens": 100,
                        "completion_tokens": 25,
                        "total_tokens": 125,
                    },
                    "context": {
                        "system_prompt_chars": 1000,
                        "task_prompt_chars": 80,
                        "tool_count": 2,
                        "tool_schema_chars": 500,
                        "llm_input_chars_peak": 2000,
                        "llm_input_chars_total": 3500,
                        "tool_result_raw_chars_total": 900,
                        "tool_result_rendered_chars_total": 700,
                        "tool_result_truncated_chars_total": 200,
                        "tool_result_rendered_chars_by_tool": {"web_search": 700},
                    },
                    "context_manifest": {
                        "version": 1,
                        "tools": {"active_names": ["web_search"]},
                    },
                    "inference_budget": {
                        "max_llm_calls": 4,
                        "max_total_tokens": 5000,
                        "max_cost_microusd": 20000,
                        "estimated_cost_microusd": 75,
                        "accounting_complete": True,
                        "provider_attempts": 2,
                        "exhausted_reason": None,
                        "last_request_max_tokens": 900,
                    },
                }
            },
        },
    }
    stdout = "\n".join(
        [
            json.dumps({"type": "log", "level": "info", "message": "start"}),
            json.dumps(result),
        ]
    )

    summary = summarize_worker_messages(
        scenario_id="example",
        returncode=0,
        elapsed_ms=3456,
        stdout=stdout,
        stderr="",
    )

    assert summary["scenario_id"] == "example"
    assert summary["status"] == "completed"
    assert summary["tokens"]["prompt_tokens"] == 100
    assert summary["latency_ms"]["llm_total"] == 1234
    assert summary["context"]["tool_schema_chars"] == 500
    assert summary["context"]["tool_result_rendered_chars_by_tool"] == {"web_search": 700}
    assert summary["context_manifest"]["tools"]["active_names"] == ["web_search"]
    assert summary["inference_budget"]["max_llm_calls"] == 4
    assert summary["inference_budget"]["estimated_cost_microusd"] == 75
    assert summary["inference_budget"]["accounting_complete"] is True
    assert summary["inference_budget"]["provider_attempts"] == 2
    assert summary["message_count"] == 2
    assert summary["log_count"] == 1


def test_run_scenarios_omits_artifact_paths_for_ephemeral_runs(monkeypatch, tmp_path) -> None:
    workspace = tmp_path / "workspace"
    template_dir = workspace / "workers" / "web_search_ranked"
    template_dir.mkdir(parents=True)
    (template_dir / "worker.json").write_text(
        json.dumps(
            {
                "name": "Web Search Ranked",
                "system_prompt": "Search carefully.",
                "available_tools": ["web_search"],
                "required_permissions": ["network"],
            }
        ),
        encoding="utf-8",
    )

    class _Proc:
        returncode = 0
        stdout = json.dumps({"type": "result", "result": {"status": "completed", "summary": "ok"}})
        stderr = ""

    monkeypatch.setattr(
        "octopal.runtime.workers.bench.subprocess.run", lambda *_args, **_kwargs: _Proc()
    )

    summaries = _run_scenarios(
        workspace_dir=workspace,
        scenarios=[
            WorkerBenchScenario(
                id="web_search_ranked_pricing",
                template_id="web_search_ranked",
                task="Search docs",
                inputs={},
            )
        ],
        artifacts_dir=tmp_path / "artifacts",
        timeout_seconds=30,
        env={},
        include_artifacts=False,
    )

    assert summaries[0]["status"] == "completed"
    assert "artifacts" not in summaries[0]


def test_load_scenarios_file_defaults_external_scenarios_to_replay_only(tmp_path) -> None:
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(
        json.dumps(
            {
                "version": 1,
                "scenarios": [
                    {
                        "id": "structured-result",
                        "template_id": "example",
                        "task": "Return a structured result",
                        "inputs": {},
                        "graders": [
                            {"type": "terminal_status", "expected": "completed"},
                            {"type": "structured_output"},
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    scenarios = load_scenarios_file(suite_path)

    assert scenarios[0].id == "structured-result"
    assert scenarios[0].live_allowed is False
    assert scenarios[0].graders[1]["type"] == "structured_output"


def test_load_scenarios_file_requires_complete_live_budget_contract(tmp_path) -> None:
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(
        json.dumps(
            {
                "version": 1,
                "scenarios": [
                    {
                        "id": "bounded-live",
                        "template_id": "example",
                        "task": "Run one bounded live trial",
                        "provider_id": "zai",
                        "model": "glm-test",
                        "max_thinking_steps": 4,
                        "live_allowed": True,
                        "live_budget": {
                            "pricing_model": "openai/glm-test",
                            "max_llm_calls": 4,
                            "max_total_tokens": 20000,
                            "max_cost_usd": "0.02",
                            "input_cost_per_million_tokens_usd": "0.20",
                            "completion_cost_per_million_tokens_usd": "1.00",
                        },
                        "graders": [{"type": "terminal_status", "expected": "completed"}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    scenario = load_scenarios_file(suite_path)[0]

    assert scenario.live_allowed is True
    assert scenario.provider_id == "zai"
    assert scenario.model == "glm-test"
    assert scenario.max_thinking_steps == 4
    assert scenario.inference_budget is not None
    assert scenario.inference_budget.max_cost_microusd == 20_000


def test_load_scenarios_file_rejects_live_scenario_without_budget(tmp_path) -> None:
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(
        json.dumps(
            {
                "version": 1,
                "scenarios": [
                    {
                        "id": "unsafe-live",
                        "template_id": "example",
                        "task": "Run without a budget",
                        "provider_id": "zai",
                        "model": "glm-test",
                        "max_thinking_steps": 4,
                        "live_allowed": True,
                        "graders": [{"type": "terminal_status", "expected": "completed"}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    try:
        load_scenarios_file(suite_path)
    except ValueError as exc:
        assert "must declare live_budget" in str(exc)
    else:
        raise AssertionError("expected missing live budget rejection")


def test_load_scenarios_file_rejects_unknown_live_provider(tmp_path) -> None:
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(
        json.dumps(
            {
                "version": 1,
                "scenarios": [
                    {
                        "id": "unknown-provider",
                        "template_id": "example",
                        "task": "Reject unknown provider",
                        "provider_id": "not-registered",
                        "model": "model-x",
                        "max_thinking_steps": 2,
                        "live_allowed": True,
                        "live_budget": {
                            "pricing_model": "provider/model-x",
                            "max_llm_calls": 2,
                            "max_total_tokens": 1000,
                            "max_cost_usd": "0.01",
                            "input_cost_per_million_tokens_usd": "1.00",
                            "completion_cost_per_million_tokens_usd": "1.00",
                        },
                        "graders": [{"type": "terminal_status", "expected": "completed"}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    try:
        load_scenarios_file(suite_path)
    except ValueError as exc:
        assert "provider_id must be one of" in str(exc)
    else:
        raise AssertionError("expected unknown live provider rejection")


def test_load_scenarios_file_rejects_path_like_ids(tmp_path) -> None:
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(
        json.dumps(
            {
                "version": 1,
                "scenarios": [
                    {
                        "id": "../../escape",
                        "template_id": "example",
                        "task": "Unsafe artifact id",
                        "graders": [{"type": "terminal_status", "expected": "completed"}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    try:
        load_scenarios_file(suite_path)
    except ValueError as exc:
        assert "cannot start with punctuation" in str(exc)
    else:
        raise AssertionError("expected unsafe scenario id rejection")


def test_grade_worker_messages_reports_assertion_evidence() -> None:
    scenario = WorkerBenchScenario(
        id="example",
        template_id="example",
        task="Return a structured result",
        inputs={},
        graders=(
            {"type": "terminal_status", "expected": "completed"},
            {"type": "structured_output"},
            {"type": "required_output_path", "path": "report.title"},
            {"type": "required_tool", "tool": "web_search"},
            {"type": "forbidden_tool", "tool": "send_file_to_user"},
            {"type": "max_tool_calls", "maximum": 2},
            {"type": "max_thinking_steps", "maximum": 4},
        ),
    )
    stdout = json.dumps(
        {
            "type": "result",
            "result": {
                "status": "completed",
                "thinking_steps": 2,
                "tools_used": ["web_search"],
                "output": {
                    "report": {"title": "Evidence"},
                    "_telemetry": {"tool_calls": 1},
                },
            },
        }
    )

    grade = grade_worker_messages(scenario=scenario, stdout=stdout)

    assert grade["passed"] is True
    assert grade["passed_count"] == 7
    assert grade["assertions"][2]["evidence"]["path"] == "report.title"
    assert grade["assertions"][2]["evidence"]["value_type"] == "str"
    assert "Evidence" not in json.dumps(grade)


def test_structured_output_grader_rejects_internal_metadata_only() -> None:
    scenario = WorkerBenchScenario(
        id="internal-only",
        template_id="unused",
        task="Require domain output",
        inputs={},
        graders=({"type": "structured_output"},),
    )
    stdout = json.dumps(
        {
            "type": "result",
            "result": {
                "status": "completed",
                "output": {"_telemetry": {}, "_orchestration_plan": {"status": "pending"}},
            },
        }
    )

    grade = grade_worker_messages(scenario=scenario, stdout=stdout)

    assert grade["passed"] is False
    assert grade["assertions"][0]["evidence"]["domain_keys"] == []


def test_telemetry_context_and_false_completion_graders_are_explicit() -> None:
    scenario = WorkerBenchScenario(
        id="telemetry-contract",
        template_id="unused",
        task="Validate telemetry contract",
        inputs={},
        graders=(
            {"type": "required_telemetry_path", "path": "tokens.total_tokens"},
            {"type": "required_context_manifest_path", "path": "tools.active_names"},
            {"type": "no_false_completion"},
            {"type": "max_stdout_parse_errors", "maximum": 0},
        ),
    )
    stdout = "not-json\n" + json.dumps(
        {
            "type": "result",
            "result": {
                "status": "completed",
                "output": {
                    "_telemetry": {
                        "tokens": {"total_tokens": 42},
                        "context_manifest": {"tools": {"active_names": ["web_search"]}},
                    }
                },
            },
        }
    )

    grade = grade_worker_messages(scenario=scenario, stdout=stdout)

    assert grade["passed"] is False
    assert grade["assertions"][0]["passed"] is True
    assert grade["assertions"][0]["evidence"]["value_type"] == "int"
    assert grade["assertions"][1]["passed"] is True
    assert grade["assertions"][2]["passed"] is False
    assert grade["assertions"][2]["evidence"]["signature_present"] is True
    assert grade["assertions"][3]["evidence"] == {"actual": 1, "maximum": 0}


def test_required_telemetry_path_rejects_null_usage_accounting() -> None:
    scenario = WorkerBenchScenario(
        id="null-telemetry",
        template_id="unused",
        task="Require actual usage accounting",
        inputs={},
        graders=({"type": "required_telemetry_path", "path": "tokens.total_tokens"},),
    )
    stdout = json.dumps(
        {
            "type": "result",
            "result": {
                "status": "completed",
                "output": {"report": {}, "_telemetry": {"tokens": {"total_tokens": None}}},
            },
        }
    )

    grade = grade_worker_messages(scenario=scenario, stdout=stdout)

    assert grade["passed"] is False
    assert grade["assertions"][0]["evidence"] == {
        "path": "tokens.total_tokens",
        "found": True,
        "value_type": "NoneType",
        "value_chars": 4,
    }


def test_aggregate_worker_bench_summaries_reports_metrics_and_failures() -> None:
    summaries = [
        {
            "scenario_id": "stable",
            "trial": 1,
            "status": "completed",
            "returncode": None,
            "thinking_steps": 2,
            "tool_calls": 1,
            "tokens": {"total_tokens": 100},
            "context": {"tool_schema_chars": 500},
            "inference_budget": {"estimated_cost_microusd": 40},
            "grade": {
                "passed": True,
                "assertion_count": 2,
                "passed_count": 2,
                "assertions": [],
            },
        },
        {
            "scenario_id": "regressed",
            "trial": 1,
            "status": "completed",
            "returncode": None,
            "thinking_steps": 4,
            "tool_calls": 3,
            "tokens": {"total_tokens": 200},
            "context": {"tool_schema_chars": 700},
            "inference_budget": {"estimated_cost_microusd": 80},
            "grade": {
                "passed": False,
                "assertion_count": 2,
                "passed_count": 1,
                "assertions": [{"type": "max_tool_calls", "passed": False, "evidence": {}}],
            },
        },
    ]

    aggregate = aggregate_worker_bench_summaries(summaries)

    assert aggregate["success_rate"] == 0.5
    assert aggregate["assertion_pass_rate"] == 0.75
    assert aggregate["multi_trial"]["pass_at_k"] == 0.5
    assert aggregate["distributions"]["thinking_steps"] == {
        "count": 2,
        "min": 2,
        "max": 4,
        "mean": 3.0,
        "p50": 2,
        "p95": 4,
    }
    assert aggregate["distributions"]["total_tokens"]["mean"] == 150.0
    assert aggregate["distributions"]["estimated_cost_microusd"]["mean"] == 60.0
    assert aggregate["failure_categories"]["grader:max_tool_calls"] == {
        "trial_count": 1,
        "scenario_ids": ["regressed"],
    }


def test_aggregate_success_rate_ignores_successful_ungraded_trials() -> None:
    aggregate = aggregate_worker_bench_summaries(
        [
            {
                "scenario_id": "telemetry-only",
                "trial": 1,
                "status": "completed",
                "returncode": 0,
                "grade": {"passed": None, "assertion_count": 0, "passed_count": 0},
            }
        ]
    )

    assert aggregate["success_rate"] is None
    assert aggregate["multi_trial"]["pass_at_k"] is None


def test_compare_worker_bench_to_baseline_detects_only_changed_outcomes() -> None:
    def trial(scenario_id: str, passed: bool) -> dict[str, object]:
        return {
            "scenario_id": scenario_id,
            "trial": 1,
            "status": "completed",
            "returncode": None,
            "grade": {
                "passed": passed,
                "assertion_count": 1,
                "passed_count": int(passed),
                "assertions": [],
            },
        }

    baseline = {"scenarios": [trial("stable", True), trial("known-failure", False)]}
    current = {
        "scenarios": [
            trial("stable", False),
            trial("known-failure", False),
            trial("new", True),
        ]
    }

    comparison = compare_worker_bench_to_baseline(summary=current, baseline=baseline)

    assert comparison["regression_detected"] is True
    assert comparison["coverage_changed"] is True
    assert comparison["regressions"] == [
        {
            "scenario_id": "stable",
            "trial": 1,
            "baseline_outcome": "passed",
            "current_outcome": "failed",
        }
    ]
    assert comparison["unchanged_trial_count"] == 1
    assert comparison["new_trials"] == [{"scenario_id": "new", "trial": 1}]
    assert comparison["success_rate"] == {
        "baseline": 0.5,
        "current": 0.0,
        "delta": -0.5,
    }
    assert comparison["overall_success_rate"] == {
        "baseline": 0.5,
        "current": 0.3333,
    }


def test_compare_worker_bench_treats_new_failed_trial_as_regression() -> None:
    passed = {
        "scenario_id": "stable",
        "trial": 1,
        "status": "completed",
        "grade": {"passed": True, "assertions": []},
    }
    failed = {
        "scenario_id": "new-failure",
        "trial": 1,
        "status": "completed",
        "grade": {"passed": False, "assertions": []},
    }

    comparison = compare_worker_bench_to_baseline(
        summary={"scenarios": [passed, failed]},
        baseline={"scenarios": [passed]},
    )

    assert comparison["regression_detected"] is True
    assert comparison["regressions"] == [
        {
            "scenario_id": "new-failure",
            "trial": 1,
            "baseline_outcome": "missing",
            "current_outcome": "failed",
        }
    ]


def test_replay_mode_does_not_load_provider_settings(monkeypatch, tmp_path) -> None:
    scenario = WorkerBenchScenario(
        id="offline",
        template_id="unused",
        task="Replay only",
        inputs={},
        graders=({"type": "terminal_status", "expected": "completed"},),
        live_allowed=False,
    )
    replay_dir = tmp_path / "replay"
    replay_dir.mkdir()
    (replay_dir / "offline.out.jsonl").write_text(
        json.dumps(
            {
                "type": "result",
                "result": {"status": "completed", "summary": "offline result"},
            }
        ),
        encoding="utf-8",
    )

    def fail_if_loaded(_path):
        raise AssertionError("replay must not load config or provider credentials")

    monkeypatch.setattr("octopal.runtime.workers.bench._load_bench_source_config", fail_if_loaded)

    summary = run_worker_bench(
        workspace_dir=tmp_path / "unused-workspace",
        scenarios=[scenario],
        execution_mode="replay",
        replay_dir=replay_dir,
    )

    assert summary["execution_mode"] == "replay"
    assert summary["graded_trials"] == 1
    assert summary["passed_trials"] == 1
    assert summary["failed_trials"] == 0
    assert summary["scenarios"][0]["grade"]["passed"] is True
    assert summary["scenarios"][0]["returncode"] is None
    assert summary["scenarios"][0]["elapsed_ms"] is None


def test_live_preflight_accepts_only_declared_read_only_tools(tmp_path) -> None:
    _write_live_template(tmp_path)

    report = preflight_worker_bench(
        workspace_dir=tmp_path,
        scenarios=[_budgeted_live_scenario()],
        source_config=_live_source_config(),
    )

    assert report["passed"] is True
    assert report["errors"] == []
    assert report["scenarios"][0]["tools"] == [
        {
            "name": "web_search",
            "permission": "network",
            "risk": "safe",
            "owner": "core",
            "read_only": True,
            "capabilities": ["network_fetch", "search"],
        }
    ]


def test_live_preflight_rejects_network_tool_without_read_only_declaration(tmp_path) -> None:
    _write_live_template(tmp_path, tools=["web_fetch"])

    report = preflight_worker_bench(
        workspace_dir=tmp_path,
        scenarios=[_budgeted_live_scenario()],
        source_config=_live_source_config(),
    )

    assert report["passed"] is False
    assert "tool 'web_fetch' is not declared as a safe core read-only tool" in " ".join(
        report["errors"]
    )


def test_live_preflight_only_does_not_execute_worker(monkeypatch, tmp_path) -> None:
    _write_live_template(tmp_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(_live_source_config().model_dump(mode="json")), encoding="utf-8"
    )

    def fail_if_executed(**_kwargs):
        raise AssertionError("preflight-only must not execute a worker")

    monkeypatch.setattr("octopal.runtime.workers.bench._run_scenarios", fail_if_executed)

    summary = run_worker_bench(
        workspace_dir=tmp_path,
        scenarios=[_budgeted_live_scenario()],
        config_path=config_path,
        preflight_only=True,
    )

    assert summary["preflight_only"] is True
    assert summary["preflight"]["passed"] is True
    assert summary["trial_count"] == 0


def test_isolated_worker_config_strips_shared_runtime_credentials(tmp_path) -> None:
    source_payload = _live_source_config().model_dump(mode="json")
    source_payload.update(
        {
            "connectors": {
                "instances": {
                    "gmail": {
                        "enabled": True,
                        "auth": {"access_token": "shared-access-token"},
                    }
                }
            },
            "browser": {"pinchtab_token": "shared-browser-token"},
            "search": {
                "brave_api_key": "test-search-key",
                "firecrawl_api_key": "unused-fallback-key",
            },
            "storage": {"state_dir": "shared-state", "workspace_dir": "shared-workspace"},
        }
    )
    source_config = OctopalConfig.model_validate(source_payload)

    isolation = _prepare_worker_bench_isolation(
        root_dir=tmp_path / "isolation",
        source_config=source_config,
        provider_id="zai",
        model="glm-test",
    )
    isolated_payload = json.loads(isolation.config_path.read_text(encoding="utf-8"))

    assert isolated_payload["connectors"]["instances"] == {}
    assert isolated_payload["browser"]["pinchtab_token"] is None
    assert isolated_payload["search"] == {
        "brave_api_key": "test-search-key",
        "firecrawl_api_key": None,
    }
    assert isolated_payload["storage"] == {
        "state_dir": str(isolation.state_dir),
        "workspace_dir": str(isolation.workspace_dir),
    }
    serialized = json.dumps(isolated_payload)
    assert "shared-access-token" not in serialized
    assert "shared-browser-token" not in serialized
    assert isolation.config_path.stat().st_mode & 0o777 == 0o600


def test_worker_bench_env_does_not_inherit_unapproved_secrets(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("UNRELATED_SECRET", "must-not-leak")
    root = tmp_path / "isolation"
    isolation = WorkerBenchIsolation(
        root_dir=root,
        workspace_dir=root / "workspace",
        state_dir=root / "state",
        browser_profile_dir=root / "browser",
        home_dir=root / "home",
        config_path=root / "config.json",
    )

    env = _worker_bench_env(_live_source_config(), isolation)

    assert "UNRELATED_SECRET" not in env
    assert env["BRAVE_API_KEY"] == "test-search-key"
    assert env["OCTOPAL_CONFIG_FILE"] == str(isolation.config_path)
    assert env["OCTOPAL_WORKSPACE_DIR"] == str(isolation.workspace_dir)
    assert env["OCTOPAL_STATE_DIR"] == str(isolation.state_dir)


def test_worker_bench_env_selects_only_one_search_credential(tmp_path) -> None:
    source_payload = _live_source_config().model_dump(mode="json")
    source_payload["search"] = {
        "brave_api_key": "preferred-search-key",
        "firecrawl_api_key": "fallback-search-key",
    }
    root = tmp_path / "isolation"
    isolation = WorkerBenchIsolation(
        root_dir=root,
        workspace_dir=root / "workspace",
        state_dir=root / "state",
        browser_profile_dir=root / "browser",
        home_dir=root / "home",
        config_path=root / "config.json",
    )

    env = _worker_bench_env(OctopalConfig.model_validate(source_payload), isolation)

    assert env["BRAVE_API_KEY"] == "preferred-search-key"
    assert "FIRECRAWL_API_KEY" not in env


def test_resolved_pricing_model_honors_always_prefix_provider() -> None:
    assert (
        _resolved_pricing_model(
            provider_id="openrouter",
            model="x-ai/grok-4.3",
            model_prefix=None,
        )
        == "openrouter/x-ai/grok-4.3"
    )


def test_live_mode_enforces_trial_cap_before_loading_settings(monkeypatch, tmp_path) -> None:
    def fail_if_loaded(_path):
        raise AssertionError("trial validation must happen before loading config")

    monkeypatch.setattr("octopal.runtime.workers.bench._load_bench_source_config", fail_if_loaded)

    try:
        run_worker_bench(workspace_dir=tmp_path, trials=4)
    except ValueError as exc:
        assert "capped at 3 total trials" in str(exc)
    else:
        raise AssertionError("expected live trial cap error")


def test_live_mode_requires_explicit_config_path() -> None:
    try:
        run_worker_bench(workspace_dir=Path("."), scenarios=[_budgeted_live_scenario()])
    except ValueError as exc:
        assert "config_path is required" in str(exc)
    else:
        raise AssertionError("expected explicit live config error")


def test_live_artifacts_directory_must_be_empty(tmp_path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    (artifacts_dir / "old-result.json").write_text("{}", encoding="utf-8")
    isolation = WorkerBenchIsolation(
        root_dir=tmp_path / "isolation",
        workspace_dir=tmp_path / "isolation" / "workspace",
        state_dir=tmp_path / "isolation" / "state",
        browser_profile_dir=tmp_path / "isolation" / "browser",
        home_dir=tmp_path / "isolation" / "home",
        config_path=tmp_path / "isolation" / "config.json",
    )

    try:
        _prepare_live_artifacts_dir(requested=artifacts_dir, isolation=isolation)
    except ValueError as exc:
        assert "must be empty" in str(exc)
    else:
        raise AssertionError("expected non-empty artifact directory error")


def test_live_mode_caps_total_runs_across_scenarios(monkeypatch, tmp_path) -> None:
    scenarios = [
        WorkerBenchScenario(
            id=f"scenario-{index}",
            template_id="unused",
            task="Live test",
            inputs={},
        )
        for index in range(2)
    ]

    def fail_if_loaded(_path):
        raise AssertionError("total run validation must happen before loading config")

    monkeypatch.setattr("octopal.runtime.workers.bench._load_bench_source_config", fail_if_loaded)

    try:
        run_worker_bench(workspace_dir=tmp_path, scenarios=scenarios, trials=2)
    except ValueError as exc:
        assert "requested 4" in str(exc)
    else:
        raise AssertionError("expected total live run cap error")


def test_live_mode_caps_total_declared_token_budget(monkeypatch, tmp_path) -> None:
    base = _budgeted_live_scenario()
    assert base.inference_budget is not None
    scenarios = [
        replace(
            base,
            id=f"bounded-{index}",
            inference_budget=base.inference_budget.model_copy(update={"max_total_tokens": 40_000}),
        )
        for index in range(3)
    ]

    def fail_if_loaded(_path):
        raise AssertionError("invocation budget validation must happen before loading config")

    monkeypatch.setattr("octopal.runtime.workers.bench._load_bench_source_config", fail_if_loaded)

    try:
        run_worker_bench(workspace_dir=tmp_path, scenarios=scenarios)
    except ValueError as exc:
        assert "token budgets exceed the invocation safety cap" in str(exc)
    else:
        raise AssertionError("expected invocation token budget cap error")


def test_live_mode_rejects_replay_only_external_scenario(monkeypatch, tmp_path) -> None:
    scenario = WorkerBenchScenario(
        id="offline",
        template_id="unused",
        task="Replay only",
        inputs={},
        graders=({"type": "terminal_status", "expected": "completed"},),
        live_allowed=False,
    )

    def fail_if_loaded(_path):
        raise AssertionError("scenario safety validation must happen before loading config")

    monkeypatch.setattr("octopal.runtime.workers.bench._load_bench_source_config", fail_if_loaded)

    try:
        run_worker_bench(workspace_dir=tmp_path, scenarios=[scenario])
    except ValueError as exc:
        assert "live execution is disabled" in str(exc)
    else:
        raise AssertionError("expected live execution safety error")


def test_live_summary_counts_ungraded_execution_failure(monkeypatch, tmp_path) -> None:
    source_config = _live_source_config()
    monkeypatch.setattr(
        "octopal.runtime.workers.bench._load_bench_source_config", lambda _path: source_config
    )
    monkeypatch.setattr(
        "octopal.runtime.workers.bench.preflight_worker_bench",
        lambda **_kwargs: {"passed": True, "errors": []},
    )
    monkeypatch.setattr(
        "octopal.runtime.workers.bench._run_scenarios",
        lambda **_kwargs: [
            {
                "scenario_id": "web_search_ranked_pricing",
                "status": "missing_result",
                "returncode": 2,
                "grade": {"passed": None},
            }
        ],
    )

    summary = run_worker_bench(
        workspace_dir=tmp_path,
        scenarios=[_budgeted_live_scenario()],
        config_path=tmp_path / "config.json",
    )

    assert summary["graded_trials"] == 0
    assert summary["failed_trials"] == 1


def test_execution_failure_is_not_also_counted_as_passed(monkeypatch, tmp_path) -> None:
    source_config = _live_source_config()
    monkeypatch.setattr(
        "octopal.runtime.workers.bench._load_bench_source_config", lambda _path: source_config
    )
    monkeypatch.setattr(
        "octopal.runtime.workers.bench.preflight_worker_bench",
        lambda **_kwargs: {"passed": True, "errors": []},
    )
    monkeypatch.setattr(
        "octopal.runtime.workers.bench._run_scenarios",
        lambda **_kwargs: [
            {
                "scenario_id": "web_search_ranked_pricing",
                "status": "completed",
                "returncode": 2,
                "grade": {"passed": True},
            }
        ],
    )

    summary = run_worker_bench(
        workspace_dir=tmp_path,
        scenarios=[_budgeted_live_scenario()],
        config_path=tmp_path / "config.json",
    )

    assert summary["graded_trials"] == 1
    assert summary["passed_trials"] == 0
    assert summary["failed_trials"] == 1


def test_cli_returns_nonzero_when_a_replay_grade_fails(tmp_path, capsys) -> None:
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(
        json.dumps(
            {
                "version": 1,
                "scenarios": [
                    {
                        "id": "failure",
                        "template_id": "unused",
                        "task": "Detect missing output",
                        "graders": [{"type": "structured_output"}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    replay_dir = tmp_path / "replay"
    replay_dir.mkdir()
    (replay_dir / "failure.out.jsonl").write_text(
        json.dumps(
            {
                "type": "result",
                "result": {"status": "completed", "output": {"_telemetry": {}}},
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--suite",
            str(suite_path),
            "--mode",
            "replay",
            "--replay-dir",
            str(replay_dir),
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert output["graded_trials"] == 1
    assert output["failed_trials"] == 1


def test_cli_baseline_gate_accepts_an_unchanged_known_failure(tmp_path, capsys) -> None:
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(
        json.dumps(
            {
                "version": 1,
                "scenarios": [
                    {
                        "id": "known-failure",
                        "template_id": "unused",
                        "task": "Keep a known failure visible",
                        "graders": [{"type": "structured_output"}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    replay_dir = tmp_path / "replay"
    replay_dir.mkdir()
    (replay_dir / "known-failure.out.jsonl").write_text(
        json.dumps(
            {
                "type": "result",
                "result": {"status": "completed", "output": {"_telemetry": {}}},
            }
        ),
        encoding="utf-8",
    )
    baseline_path = tmp_path / "baseline.json"

    initial_exit = main(
        [
            "--suite",
            str(suite_path),
            "--mode",
            "replay",
            "--replay-dir",
            str(replay_dir),
            "--out",
            str(baseline_path),
        ]
    )
    capsys.readouterr()
    comparison_exit = main(
        [
            "--suite",
            str(suite_path),
            "--mode",
            "replay",
            "--replay-dir",
            str(replay_dir),
            "--baseline",
            str(baseline_path),
        ]
    )

    comparison = json.loads(capsys.readouterr().out)["baseline_comparison"]
    assert initial_exit == 1
    assert comparison_exit == 0
    assert comparison["regression_detected"] is False
    assert comparison["success_rate"]["delta"] == 0.0
