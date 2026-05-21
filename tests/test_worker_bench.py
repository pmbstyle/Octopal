from __future__ import annotations

import json

from octopal.runtime.workers.bench import (
    WorkerBenchScenario,
    _run_scenarios,
    build_worker_spec,
    parse_worker_jsonl,
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
