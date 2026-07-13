from __future__ import annotations

import json
from types import SimpleNamespace

from octopal.runtime.workers.bench import (
    WorkerBenchScenario,
    _run_scenarios,
    build_worker_spec,
    grade_worker_messages,
    load_scenarios_file,
    main,
    parse_worker_jsonl,
    run_worker_bench,
    select_scenarios,
    summarize_worker_messages,
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

    def fail_if_loaded():
        raise AssertionError("replay must not load settings or provider credentials")

    monkeypatch.setattr("octopal.runtime.workers.bench.load_settings", fail_if_loaded)

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


def test_live_mode_enforces_trial_cap_before_loading_settings(monkeypatch, tmp_path) -> None:
    def fail_if_loaded():
        raise AssertionError("trial validation must happen before loading settings")

    monkeypatch.setattr("octopal.runtime.workers.bench.load_settings", fail_if_loaded)

    try:
        run_worker_bench(workspace_dir=tmp_path, trials=4)
    except ValueError as exc:
        assert "capped at 3 total trials" in str(exc)
    else:
        raise AssertionError("expected live trial cap error")


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

    def fail_if_loaded():
        raise AssertionError("total run validation must happen before loading settings")

    monkeypatch.setattr("octopal.runtime.workers.bench.load_settings", fail_if_loaded)

    try:
        run_worker_bench(workspace_dir=tmp_path, scenarios=scenarios, trials=2)
    except ValueError as exc:
        assert "requested 4" in str(exc)
    else:
        raise AssertionError("expected total live run cap error")


def test_live_mode_rejects_replay_only_external_scenario(monkeypatch, tmp_path) -> None:
    scenario = WorkerBenchScenario(
        id="offline",
        template_id="unused",
        task="Replay only",
        inputs={},
        graders=({"type": "terminal_status", "expected": "completed"},),
        live_allowed=False,
    )

    def fail_if_loaded():
        raise AssertionError("scenario safety validation must happen before loading settings")

    monkeypatch.setattr("octopal.runtime.workers.bench.load_settings", fail_if_loaded)

    try:
        run_worker_bench(workspace_dir=tmp_path, scenarios=[scenario])
    except ValueError as exc:
        assert "live execution is disabled" in str(exc)
    else:
        raise AssertionError("expected live execution safety error")


def test_live_summary_counts_ungraded_execution_failure(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "octopal.runtime.workers.bench.load_settings", lambda: SimpleNamespace(config_obj=None)
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

    summary = run_worker_bench(workspace_dir=tmp_path)

    assert summary["graded_trials"] == 0
    assert summary["failed_trials"] == 1


def test_execution_failure_is_not_also_counted_as_passed(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "octopal.runtime.workers.bench.load_settings", lambda: SimpleNamespace(config_obj=None)
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

    summary = run_worker_bench(workspace_dir=tmp_path)

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
