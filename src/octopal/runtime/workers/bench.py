from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from octopal.infrastructure.config.settings import load_settings


@dataclass(frozen=True)
class WorkerBenchScenario:
    id: str
    template_id: str
    task: str
    inputs: dict[str, Any]
    graders: tuple[dict[str, Any], ...] = ()
    live_allowed: bool = True


_SUPPORTED_GRADER_TYPES = {
    "forbidden_tool",
    "max_thinking_steps",
    "max_tool_calls",
    "required_output_path",
    "required_tool",
    "structured_output",
    "terminal_status",
}
_MAX_LIVE_RUNS = 3
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


DEFAULT_SCENARIOS: tuple[WorkerBenchScenario, ...] = (
    WorkerBenchScenario(
        id="web_search_ranked_pricing",
        template_id="web_search_ranked",
        task=(
            "Search for the current OpenAI API pricing page and return the top 3 relevant "
            "official or high-quality sources. Do not fetch full pages."
        ),
        inputs={"query": "OpenAI API pricing latest official", "max_results": 3},
    ),
)


def scenario_ids() -> list[str]:
    return [scenario.id for scenario in DEFAULT_SCENARIOS]


def select_scenarios(selected: list[str] | None) -> list[WorkerBenchScenario]:
    return select_scenarios_from(DEFAULT_SCENARIOS, selected)


def select_scenarios_from(
    scenarios: tuple[WorkerBenchScenario, ...] | list[WorkerBenchScenario],
    selected: list[str] | None,
) -> list[WorkerBenchScenario]:
    scenarios = list(scenarios)
    if not selected:
        return scenarios

    by_id = {scenario.id: scenario for scenario in scenarios}
    missing = [item for item in selected if item not in by_id]
    if missing:
        available = ", ".join(sorted(by_id))
        raise ValueError(f"Unknown scenario(s): {', '.join(missing)}. Available: {available}")
    return [by_id[item] for item in selected]


def load_scenarios_file(path: Path) -> list[WorkerBenchScenario]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise ValueError(f"Expected worker bench suite version 1 at {path}")
    raw_scenarios = payload.get("scenarios")
    if not isinstance(raw_scenarios, list) or not raw_scenarios:
        raise ValueError(f"Worker bench suite at {path} must contain non-empty scenarios")

    scenarios: list[WorkerBenchScenario] = []
    seen_ids: set[str] = set()
    for index, raw in enumerate(raw_scenarios):
        if not isinstance(raw, dict):
            raise ValueError(f"Scenario {index} in {path} must be an object")
        scenario_id = _required_identifier(raw.get("id"), field=f"scenarios[{index}].id")
        if scenario_id in seen_ids:
            raise ValueError(f"Duplicate worker bench scenario id: {scenario_id}")
        seen_ids.add(scenario_id)
        graders = _validate_graders(raw.get("graders", []), scenario_id=scenario_id)
        inputs = raw.get("inputs", {})
        if not isinstance(inputs, dict):
            raise ValueError(f"Scenario {scenario_id} inputs must be an object")
        live_allowed = raw.get("live_allowed", False)
        if not isinstance(live_allowed, bool):
            raise ValueError(f"Scenario {scenario_id} live_allowed must be a boolean")
        scenarios.append(
            WorkerBenchScenario(
                id=scenario_id,
                template_id=_required_identifier(
                    raw.get("template_id"), field=f"scenario {scenario_id} template_id"
                ),
                task=_required_text(raw.get("task"), field=f"scenario {scenario_id} task"),
                inputs=dict(inputs),
                graders=tuple(graders),
                live_allowed=live_allowed,
            )
        )
    return scenarios


def build_worker_spec(
    *,
    scenario: WorkerBenchScenario,
    template: dict[str, Any],
    run_id: str,
) -> dict[str, Any]:
    available_tools = _str_list(template.get("available_tools"))
    spec: dict[str, Any] = {
        "id": run_id,
        "template_id": scenario.template_id,
        "template_name": template.get("name") or scenario.template_id,
        "task": scenario.task,
        "inputs": scenario.inputs,
        "system_prompt": str(template.get("system_prompt") or ""),
        "available_tools": available_tools,
        "granted_capabilities": [],
        "timeout_seconds": int(template.get("default_timeout_seconds") or 300),
        "max_thinking_steps": int(template.get("max_thinking_steps") or 10),
        "run_id": run_id,
        "lifecycle": "ephemeral",
        "effective_permissions": _str_list(template.get("required_permissions")),
    }
    model = str(template.get("model") or "").strip()
    if model:
        spec["model"] = model
    return spec


def parse_worker_jsonl(text: str) -> tuple[list[dict[str, Any]], list[str]]:
    messages: list[dict[str, Any]] = []
    parse_errors: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            parse_errors.append(stripped[:240])
            continue
        if isinstance(payload, dict):
            messages.append(payload)
        else:
            parse_errors.append(stripped[:240])
    return messages, parse_errors


def summarize_worker_messages(
    *,
    scenario_id: str,
    returncode: int | None,
    elapsed_ms: int | None,
    stdout: str,
    stderr: str,
) -> dict[str, Any]:
    messages, parse_errors = parse_worker_jsonl(stdout)
    result = next(
        (message.get("result") for message in messages if message.get("type") == "result"), None
    )
    logs = [message for message in messages if message.get("type") == "log"]
    instruction_requests = [
        message for message in messages if message.get("type") == "instruction_request"
    ]
    if not isinstance(result, dict):
        return {
            "scenario_id": scenario_id,
            "status": "missing_result",
            "returncode": returncode,
            "elapsed_ms": elapsed_ms,
            "message_count": len(messages),
            "log_count": len(logs),
            "instruction_request_count": len(instruction_requests),
            "stdout_parse_errors": len(parse_errors),
            "stderr_chars": len(stderr),
        }

    raw_output = result.get("output")
    output: dict[str, Any] = raw_output if isinstance(raw_output, dict) else {}
    raw_telemetry = output.get("_telemetry")
    telemetry: dict[str, Any] = raw_telemetry if isinstance(raw_telemetry, dict) else {}
    raw_context = telemetry.get("context")
    context: dict[str, Any] = raw_context if isinstance(raw_context, dict) else {}
    raw_tokens = telemetry.get("tokens")
    tokens: dict[str, Any] = raw_tokens if isinstance(raw_tokens, dict) else {}
    raw_manifest = telemetry.get("context_manifest")
    context_manifest: dict[str, Any] = raw_manifest if isinstance(raw_manifest, dict) else {}

    return {
        "scenario_id": scenario_id,
        "status": result.get("status"),
        "summary": result.get("summary"),
        "returncode": returncode,
        "elapsed_ms": elapsed_ms,
        "thinking_steps": result.get("thinking_steps"),
        "tools_used": result.get("tools_used") or [],
        "llm_calls": telemetry.get("llm_calls"),
        "tool_calls": telemetry.get("tool_calls"),
        "tokens": {
            "prompt_tokens": _int_or_none(tokens.get("prompt_tokens")),
            "completion_tokens": _int_or_none(tokens.get("completion_tokens")),
            "total_tokens": _int_or_none(tokens.get("total_tokens")),
        },
        "latency_ms": {
            "llm_total": _int_or_none(telemetry.get("llm_latency_ms_total")),
            "tool_total": _int_or_none(telemetry.get("tool_latency_ms_total")),
        },
        "context": {
            "system_prompt_chars": _int_or_none(context.get("system_prompt_chars")),
            "task_prompt_chars": _int_or_none(context.get("task_prompt_chars")),
            "tool_count": _int_or_none(context.get("tool_count")),
            "tool_schema_chars": _int_or_none(context.get("tool_schema_chars")),
            "llm_input_chars_peak": _int_or_none(context.get("llm_input_chars_peak")),
            "llm_input_chars_total": _int_or_none(context.get("llm_input_chars_total")),
            "tool_result_raw_chars_total": _int_or_none(context.get("tool_result_raw_chars_total")),
            "tool_result_rendered_chars_total": _int_or_none(
                context.get("tool_result_rendered_chars_total")
            ),
            "tool_result_truncated_chars_total": _int_or_none(
                context.get("tool_result_truncated_chars_total")
            ),
            "tool_result_rendered_chars_by_tool": context.get("tool_result_rendered_chars_by_tool")
            or {},
        },
        "context_manifest": context_manifest,
        "message_count": len(messages),
        "log_count": len(logs),
        "instruction_request_count": len(instruction_requests),
        "stdout_parse_errors": len(parse_errors),
        "stderr_chars": len(stderr),
    }


def grade_worker_messages(
    *,
    scenario: WorkerBenchScenario,
    stdout: str,
) -> dict[str, Any]:
    messages, _parse_errors = parse_worker_jsonl(stdout)
    result = next(
        (message.get("result") for message in messages if message.get("type") == "result"), None
    )
    result_obj = result if isinstance(result, dict) else {}
    output = result_obj.get("output")
    output_obj = output if isinstance(output, dict) else {}
    tools_used = _str_list(result_obj.get("tools_used"))
    telemetry = output_obj.get("_telemetry")
    telemetry_obj = telemetry if isinstance(telemetry, dict) else {}

    assertions: list[dict[str, Any]] = []
    for grader in scenario.graders:
        grader_type = str(grader["type"])
        passed = False
        evidence: dict[str, Any]
        if grader_type == "terminal_status":
            expected = grader.get("expected")
            expected_values = (
                [str(item) for item in expected] if isinstance(expected, list) else [str(expected)]
            )
            actual = result_obj.get("status")
            passed = actual in expected_values
            evidence = {"actual": actual, "expected": expected_values}
        elif grader_type == "structured_output":
            domain_keys = sorted(key for key in output_obj if key != "_telemetry")
            passed = bool(domain_keys)
            evidence = {"domain_keys": domain_keys}
        elif grader_type == "required_output_path":
            path = str(grader.get("path") or "")
            found, value = _resolve_json_path(output_obj, path)
            passed = found
            evidence = {
                "path": path,
                "found": found,
                "value_type": type(value).__name__ if found else None,
                "value_chars": _serialized_value_chars(value) if found else None,
            }
        elif grader_type == "required_tool":
            tool = str(grader.get("tool") or "")
            passed = tool in tools_used
            evidence = {"tool": tool, "tools_used": tools_used}
        elif grader_type == "forbidden_tool":
            tool = str(grader.get("tool") or "")
            passed = tool not in tools_used
            evidence = {"tool": tool, "tools_used": tools_used}
        elif grader_type == "max_tool_calls":
            maximum = int(grader["maximum"])
            actual = _int_or_none(telemetry_obj.get("tool_calls"))
            passed = actual is not None and actual <= maximum
            evidence = {"actual": actual, "maximum": maximum}
        elif grader_type == "max_thinking_steps":
            maximum = int(grader["maximum"])
            actual = _int_or_none(result_obj.get("thinking_steps"))
            passed = actual is not None and actual <= maximum
            evidence = {"actual": actual, "maximum": maximum}
        else:  # pragma: no cover - scenario validation rejects this before execution
            evidence = {"error": f"unsupported grader type: {grader_type}"}
        assertions.append(
            {
                "type": grader_type,
                "passed": passed,
                "evidence": evidence,
            }
        )

    passed_count = sum(1 for assertion in assertions if assertion["passed"])
    return {
        "passed": passed_count == len(assertions) if assertions else None,
        "assertion_count": len(assertions),
        "passed_count": passed_count,
        "assertions": assertions,
    }


def run_worker_bench(
    *,
    workspace_dir: Path,
    scenario_names: list[str] | None = None,
    scenarios: list[WorkerBenchScenario] | None = None,
    artifacts_dir: Path | None = None,
    timeout_seconds: int = 300,
    execution_mode: str = "live",
    replay_dir: Path | None = None,
    trials: int = 1,
) -> dict[str, Any]:
    if execution_mode not in {"live", "replay"}:
        raise ValueError("execution_mode must be 'live' or 'replay'")
    if trials <= 0:
        raise ValueError("trials must be greater than zero")
    available_scenarios = list(scenarios) if scenarios is not None else list(DEFAULT_SCENARIOS)
    selected_scenarios = select_scenarios_from(available_scenarios, scenario_names)
    if execution_mode == "live":
        requested_live_runs = len(selected_scenarios) * trials
        if requested_live_runs > _MAX_LIVE_RUNS:
            raise ValueError(
                f"live worker bench runs are capped at {_MAX_LIVE_RUNS} total trials per "
                f"invocation; requested {requested_live_runs}"
            )
        blocked = [scenario.id for scenario in selected_scenarios if not scenario.live_allowed]
        if blocked:
            raise ValueError(
                "live execution is disabled for scenario(s): " + ", ".join(sorted(blocked))
            )
    elif replay_dir is None:
        raise ValueError("replay_dir is required for replay mode")

    started_at = datetime.now(UTC)
    summaries: list[dict[str, Any]] = []

    if execution_mode == "replay":
        assert replay_dir is not None
        summaries = _replay_scenarios(
            scenarios=selected_scenarios,
            replay_dir=replay_dir,
            trials=trials,
        )
    else:
        settings = load_settings()
        if artifacts_dir is None:
            with tempfile.TemporaryDirectory(prefix="octopal-worker-bench-") as tmp:
                summaries = _run_scenarios(
                    workspace_dir=workspace_dir,
                    scenarios=selected_scenarios,
                    artifacts_dir=Path(tmp),
                    timeout_seconds=timeout_seconds,
                    env=_worker_bench_env(settings, workspace_dir),
                    include_artifacts=False,
                    trials=trials,
                )
        else:
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            summaries = _run_scenarios(
                workspace_dir=workspace_dir,
                scenarios=selected_scenarios,
                artifacts_dir=artifacts_dir,
                timeout_seconds=timeout_seconds,
                env=_worker_bench_env(settings, workspace_dir),
                include_artifacts=True,
                trials=trials,
            )

    finished_at = datetime.now(UTC)
    graded_trials = sum(1 for item in summaries if item.get("grade", {}).get("passed") is not None)
    passed_trials = sum(1 for item in summaries if item.get("grade", {}).get("passed") is True)
    failed_trials = sum(1 for item in summaries if item.get("grade", {}).get("passed") is False)
    return {
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "workspace_dir": str(workspace_dir),
        "execution_mode": execution_mode,
        "scenario_count": len(selected_scenarios),
        "trial_count": len(summaries),
        "graded_trials": graded_trials,
        "passed_trials": passed_trials,
        "failed_trials": failed_trials,
        "scenarios": summaries,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run repeatable worker telemetry benchmarks.")
    parser.add_argument(
        "--workspace",
        default=os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace"),
        help="Workspace directory with worker templates.",
    )
    parser.add_argument(
        "--scenario",
        action="append",
        help="Scenario id to run. Repeat to run multiple. Default: all scenarios.",
    )
    parser.add_argument("--suite", help="Optional versioned JSON scenario suite.")
    parser.add_argument(
        "--mode",
        choices=("live", "replay"),
        default="live",
        help="Execute workers or grade saved JSONL without provider/tool calls.",
    )
    parser.add_argument("--replay-dir", help="Directory containing saved scenario JSONL files.")
    parser.add_argument(
        "--trials",
        type=int,
        default=1,
        help=f"Trials per scenario. Live mode is capped at {_MAX_LIVE_RUNS} total runs.",
    )
    parser.add_argument(
        "--artifacts-dir",
        help="Optional directory to keep generated worker specs and JSONL output.",
    )
    parser.add_argument("--out", help="Optional path to write the JSON summary.")
    parser.add_argument("--timeout", type=int, default=300, help="Per-scenario timeout in seconds.")
    args = parser.parse_args(argv)

    try:
        scenarios = load_scenarios_file(Path(args.suite)) if args.suite else None
        summary = run_worker_bench(
            workspace_dir=Path(args.workspace),
            scenario_names=args.scenario,
            scenarios=scenarios,
            artifacts_dir=Path(args.artifacts_dir) if args.artifacts_dir else None,
            timeout_seconds=args.timeout,
            execution_mode=args.mode,
            replay_dir=Path(args.replay_dir) if args.replay_dir else None,
            trials=args.trials,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    text = json.dumps(summary, ensure_ascii=False, indent=2)
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text + "\n", encoding="utf-8")
    sys.stdout.write(text + "\n")
    return 1 if summary["failed_trials"] else 0


def _run_scenarios(
    *,
    workspace_dir: Path,
    scenarios: list[WorkerBenchScenario],
    artifacts_dir: Path,
    timeout_seconds: int,
    env: dict[str, str],
    include_artifacts: bool,
    trials: int = 1,
) -> list[dict[str, Any]]:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    summaries: list[dict[str, Any]] = []
    for scenario in scenarios:
        template_path = workspace_dir / "workers" / scenario.template_id / "worker.json"
        template = _read_json_object(template_path)
        for trial in range(1, trials + 1):
            artifact_stem = _artifact_stem(scenario.id, trial, trials)
            run_id = f"bench-{scenario.id}-{int(time.time())}-{trial}"
            spec = build_worker_spec(scenario=scenario, template=template, run_id=run_id)
            spec_path = artifacts_dir / f"{artifact_stem}.spec.json"
            stdout_path = artifacts_dir / f"{artifact_stem}.out.jsonl"
            stderr_path = artifacts_dir / f"{artifact_stem}.err.txt"
            spec_path.write_text(json.dumps(spec, ensure_ascii=False, indent=2), encoding="utf-8")

            started = time.perf_counter()
            try:
                proc = subprocess.run(
                    [sys.executable, "-m", "octopal.runtime.workers.entrypoint", str(spec_path)],
                    cwd=Path.cwd(),
                    env=env,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    text=True,
                    timeout=timeout_seconds,
                    check=False,
                )
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                stdout_path.write_text(proc.stdout, encoding="utf-8")
                stderr_path.write_text(proc.stderr, encoding="utf-8")
                summary = summarize_worker_messages(
                    scenario_id=scenario.id,
                    returncode=proc.returncode,
                    elapsed_ms=elapsed_ms,
                    stdout=proc.stdout,
                    stderr=proc.stderr,
                )
                summary["grade"] = grade_worker_messages(scenario=scenario, stdout=proc.stdout)
            except subprocess.TimeoutExpired as exc:
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                stdout = _coerce_output_text(exc.stdout)
                stderr = _coerce_output_text(exc.stderr)
                stdout_path.write_text(stdout, encoding="utf-8")
                stderr_path.write_text(stderr, encoding="utf-8")
                summary = {
                    "scenario_id": scenario.id,
                    "status": "timeout",
                    "returncode": None,
                    "elapsed_ms": elapsed_ms,
                    "timeout_seconds": timeout_seconds,
                    "grade": grade_worker_messages(scenario=scenario, stdout=stdout),
                }
            summary["execution_mode"] = "live"
            summary["trial"] = trial
            if include_artifacts:
                summary["artifacts"] = {
                    "spec": str(spec_path),
                    "stdout": str(stdout_path),
                    "stderr": str(stderr_path),
                }
            summaries.append(summary)
    return summaries


def _replay_scenarios(
    *,
    scenarios: list[WorkerBenchScenario],
    replay_dir: Path,
    trials: int,
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for scenario in scenarios:
        for trial in range(1, trials + 1):
            artifact_stem = _artifact_stem(scenario.id, trial, trials)
            stdout_path = replay_dir / f"{artifact_stem}.out.jsonl"
            stderr_path = replay_dir / f"{artifact_stem}.err.txt"
            stdout = stdout_path.read_text(encoding="utf-8")
            stderr = stderr_path.read_text(encoding="utf-8") if stderr_path.exists() else ""
            summary = summarize_worker_messages(
                scenario_id=scenario.id,
                returncode=None,
                elapsed_ms=None,
                stdout=stdout,
                stderr=stderr,
            )
            summary.update(
                {
                    "execution_mode": "replay",
                    "trial": trial,
                    "grade": grade_worker_messages(scenario=scenario, stdout=stdout),
                    "artifacts": {
                        "stdout": str(stdout_path),
                        "stderr": str(stderr_path) if stderr_path.exists() else None,
                    },
                }
            )
            summaries.append(summary)
    return summaries


def _worker_bench_env(settings: Any, workspace_dir: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["OCTOPAL_WORKSPACE_DIR"] = str(workspace_dir)
    config_obj = getattr(settings, "config_obj", None)
    brave_api_key = getattr(getattr(config_obj, "search", None), "brave_api_key", None) or getattr(
        settings, "brave_api_key", None
    )
    firecrawl_api_key = getattr(
        getattr(config_obj, "search", None), "firecrawl_api_key", None
    ) or getattr(settings, "firecrawl_api_key", None)
    if brave_api_key and not env.get("BRAVE_API_KEY"):
        env["BRAVE_API_KEY"] = str(brave_api_key)
    if firecrawl_api_key and not env.get("FIRECRAWL_API_KEY"):
        env["FIRECRAWL_API_KEY"] = str(firecrawl_api_key)
    return env


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _validate_graders(value: Any, *, scenario_id: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"Scenario {scenario_id} must define at least one grader")
    graders: list[dict[str, Any]] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, dict):
            raise ValueError(f"Scenario {scenario_id} grader {index} must be an object")
        grader_type = _required_text(
            raw.get("type"), field=f"scenario {scenario_id} grader {index} type"
        )
        if grader_type not in _SUPPORTED_GRADER_TYPES:
            supported = ", ".join(sorted(_SUPPORTED_GRADER_TYPES))
            raise ValueError(
                f"Scenario {scenario_id} grader {index} has unsupported type "
                f"'{grader_type}'. Supported: {supported}"
            )
        if grader_type == "terminal_status":
            expected = raw.get("expected")
            if not isinstance(expected, str | list) or not expected:
                raise ValueError(f"Scenario {scenario_id} terminal_status grader requires expected")
            if isinstance(expected, list) and not all(
                isinstance(item, str) and item.strip() for item in expected
            ):
                raise ValueError(
                    f"Scenario {scenario_id} terminal_status expected must contain strings"
                )
        elif grader_type == "required_output_path":
            _required_text(raw.get("path"), field=f"scenario {scenario_id} grader path")
        elif grader_type in {"required_tool", "forbidden_tool"}:
            _required_text(raw.get("tool"), field=f"scenario {scenario_id} grader tool")
        elif grader_type in {"max_tool_calls", "max_thinking_steps"}:
            maximum = raw.get("maximum")
            if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 0:
                raise ValueError(
                    f"Scenario {scenario_id} {grader_type} maximum must be a non-negative integer"
                )
        graders.append(dict(raw))
    return graders


def _required_text(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _required_identifier(value: Any, *, field: str) -> str:
    text = _required_text(value, field=field)
    if not _SAFE_ID_RE.fullmatch(text):
        raise ValueError(
            f"{field} must use only letters, numbers, dot, underscore, or hyphen and cannot "
            "start with punctuation"
        )
    return text


def _resolve_json_path(payload: dict[str, Any], path: str) -> tuple[bool, Any]:
    current: Any = payload
    for part in path.split("."):
        if not part or not isinstance(current, dict) or part not in current:
            return False, None
        current = current[part]
    return True, current


def _serialized_value_chars(value: Any) -> int:
    try:
        return len(json.dumps(value, ensure_ascii=False, default=str))
    except Exception:
        return len(str(value))


def _artifact_stem(scenario_id: str, trial: int, trials: int) -> str:
    if trials == 1:
        return scenario_id
    return f"{scenario_id}.trial-{trial}"


def _str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item).strip()]


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return int(value)
    return None


def _coerce_output_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)
