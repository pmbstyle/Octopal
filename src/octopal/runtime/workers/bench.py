from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from math import ceil
from pathlib import Path
from typing import Any

from octopal.infrastructure.config.models import (
    BrowserRuntimeConfig,
    LiteLLMRuntimeConfig,
    LLMConfig,
    OctopalConfig,
    SearchConfig,
    StorageConfig,
)
from octopal.infrastructure.providers.catalog import (
    get_provider_catalog_entry,
    list_registered_provider_ids,
)
from octopal.runtime.workers.contracts import WorkerInferenceBudget
from octopal.tools.catalog import get_tools


@dataclass(frozen=True)
class WorkerBenchScenario:
    id: str
    template_id: str
    task: str
    inputs: dict[str, Any]
    graders: tuple[dict[str, Any], ...] = ()
    live_allowed: bool = True
    provider_id: str | None = None
    model: str | None = None
    max_thinking_steps: int | None = None
    inference_budget: WorkerInferenceBudget | None = None


_SUPPORTED_GRADER_TYPES = {
    "forbidden_tool",
    "max_stdout_parse_errors",
    "max_thinking_steps",
    "max_tool_calls",
    "no_false_completion",
    "required_context_manifest_path",
    "required_output_path",
    "required_telemetry_path",
    "required_tool",
    "structured_output",
    "terminal_status",
}
_MAX_LIVE_RUNS = 3
_MAX_LIVE_RUN_TOTAL_TOKENS = 50_000
_MAX_LIVE_RUN_COST_MICROUSD = 100_000
_MAX_LIVE_INVOCATION_TOTAL_TOKENS = 100_000
_MAX_LIVE_INVOCATION_COST_MICROUSD = 200_000
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_BENCH_ENV_PASSTHROUGH = (
    "LANG",
    "LC_ALL",
    "PATH",
    "PYTHONPATH",
    "SSL_CERT_FILE",
    "VIRTUAL_ENV",
)


@dataclass(frozen=True)
class WorkerBenchIsolation:
    root_dir: Path
    workspace_dir: Path
    state_dir: Path
    browser_profile_dir: Path
    home_dir: Path
    config_path: Path


DEFAULT_SCENARIOS: tuple[WorkerBenchScenario, ...] = (
    WorkerBenchScenario(
        id="web_search_ranked_pricing",
        template_id="web_search_ranked",
        task=(
            "Search for the current OpenAI API pricing page and return the top 3 relevant "
            "official or high-quality sources. Do not fetch full pages."
        ),
        inputs={"query": "OpenAI API pricing latest official", "max_results": 3},
        live_allowed=False,
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
        provider_id = _optional_text(raw.get("provider_id"))
        model = _optional_text(raw.get("model"))
        max_thinking_steps = _optional_positive_int(
            raw.get("max_thinking_steps"),
            field=f"scenario {scenario_id} max_thinking_steps",
        )
        inference_budget = _load_live_budget(
            raw.get("live_budget"),
            scenario_id=scenario_id,
        )
        if live_allowed:
            _validate_live_scenario_contract(
                scenario_id=scenario_id,
                provider_id=provider_id,
                model=model,
                max_thinking_steps=max_thinking_steps,
                inference_budget=inference_budget,
            )
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
                provider_id=provider_id,
                model=model,
                max_thinking_steps=max_thinking_steps,
                inference_budget=inference_budget,
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
        "max_thinking_steps": (
            scenario.max_thinking_steps
            if scenario.max_thinking_steps is not None
            else int(template.get("max_thinking_steps") or 10)
        ),
        "strict_thinking_budget": scenario.max_thinking_steps is not None,
        "run_id": run_id,
        "lifecycle": "ephemeral",
        "effective_permissions": _str_list(template.get("required_permissions")),
    }
    model = str(scenario.model or template.get("model") or "").strip()
    if model:
        spec["model"] = model
    if scenario.provider_id:
        spec["llm_config"] = {
            "provider_id": scenario.provider_id,
            "model": model or None,
        }
    if scenario.inference_budget is not None:
        spec["inference_budget"] = scenario.inference_budget.model_dump(mode="json")
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
    raw_budget = telemetry.get("inference_budget")
    budget: dict[str, Any] = raw_budget if isinstance(raw_budget, dict) else {}

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
        "inference_budget": _summarize_inference_budget(budget),
        "message_count": len(messages),
        "log_count": len(logs),
        "instruction_request_count": len(instruction_requests),
        "stdout_parse_errors": len(parse_errors),
        "stderr_chars": len(stderr),
    }


def _summarize_inference_budget(budget: dict[str, Any]) -> dict[str, Any]:
    if not budget:
        return {}
    return {
        "max_llm_calls": _int_or_none(budget.get("max_llm_calls")),
        "max_tool_calls": _int_or_none(budget.get("max_tool_calls")),
        "max_total_tokens": _int_or_none(budget.get("max_total_tokens")),
        "max_cost_microusd": _int_or_none(budget.get("max_cost_microusd")),
        "estimated_cost_microusd": _int_or_none(budget.get("estimated_cost_microusd")),
        "accounting_complete": budget.get("accounting_complete"),
        "provider_attempts": _int_or_none(budget.get("provider_attempts")),
        "tool_calls_denied": _int_or_none(budget.get("tool_calls_denied")),
        "exhausted_reason": budget.get("exhausted_reason"),
        "last_request_max_tokens": _int_or_none(budget.get("last_request_max_tokens")),
    }


def grade_worker_messages(
    *,
    scenario: WorkerBenchScenario,
    stdout: str,
) -> dict[str, Any]:
    messages, parse_errors = parse_worker_jsonl(stdout)
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
            domain_keys = sorted(key for key in output_obj if not str(key).startswith("_"))
            passed = bool(domain_keys)
            evidence = {"domain_keys": domain_keys}
        elif grader_type == "required_output_path":
            path = str(grader.get("path") or "")
            found, value = _resolve_json_path(output_obj, path)
            passed = found
            evidence = _path_evidence(path=path, found=found, value=value)
        elif grader_type == "required_telemetry_path":
            path = str(grader.get("path") or "")
            found, value = _resolve_json_path(telemetry_obj, path)
            passed = found and value is not None
            evidence = _path_evidence(path=path, found=found, value=value)
        elif grader_type == "required_context_manifest_path":
            manifest = telemetry_obj.get("context_manifest")
            manifest_obj = manifest if isinstance(manifest, dict) else {}
            path = str(grader.get("path") or "")
            found, value = _resolve_json_path(manifest_obj, path)
            passed = found and value is not None
            evidence = _path_evidence(path=path, found=found, value=value)
        elif grader_type == "no_false_completion":
            actual_status = result_obj.get("status")
            domain_keys = sorted(key for key in output_obj if not str(key).startswith("_"))
            signature_present = actual_status == "completed" and not domain_keys
            passed = not signature_present
            evidence = {
                "status": actual_status,
                "domain_key_count": len(domain_keys),
                "signature_present": signature_present,
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
        elif grader_type == "max_stdout_parse_errors":
            maximum = int(grader["maximum"])
            actual = len(parse_errors)
            passed = actual <= maximum
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
    config_path: Path | None = None,
    preflight_only: bool = False,
) -> dict[str, Any]:
    if execution_mode not in {"live", "replay"}:
        raise ValueError("execution_mode must be 'live' or 'replay'")
    if trials <= 0:
        raise ValueError("trials must be greater than zero")
    available_scenarios = list(scenarios) if scenarios is not None else list(DEFAULT_SCENARIOS)
    selected_scenarios = select_scenarios_from(available_scenarios, scenario_names)
    if not selected_scenarios:
        raise ValueError("worker bench requires at least one selected scenario")
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
        for scenario in selected_scenarios:
            _validate_live_scenario_contract(
                scenario_id=scenario.id,
                provider_id=scenario.provider_id,
                model=scenario.model,
                max_thinking_steps=scenario.max_thinking_steps,
                inference_budget=scenario.inference_budget,
            )
        invocation_token_budget = (
            sum(
                scenario.inference_budget.max_total_tokens
                for scenario in selected_scenarios
                if scenario.inference_budget is not None
            )
            * trials
        )
        invocation_cost_budget = (
            sum(
                scenario.inference_budget.max_cost_microusd
                for scenario in selected_scenarios
                if scenario.inference_budget is not None
            )
            * trials
        )
        if invocation_token_budget > _MAX_LIVE_INVOCATION_TOTAL_TOKENS:
            raise ValueError(
                "live worker bench token budgets exceed the invocation safety cap of "
                f"{_MAX_LIVE_INVOCATION_TOTAL_TOKENS}"
            )
        if invocation_cost_budget > _MAX_LIVE_INVOCATION_COST_MICROUSD:
            raise ValueError(
                "live worker bench cost budgets exceed the invocation safety cap of "
                f"{_MAX_LIVE_INVOCATION_COST_MICROUSD} micro-USD"
            )
        if config_path is None:
            raise ValueError("config_path is required for live worker bench execution")
    elif replay_dir is None:
        raise ValueError("replay_dir is required for replay mode")
    elif preflight_only:
        raise ValueError("preflight_only is only available in live mode")

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
        assert config_path is not None
        source_config = _load_bench_source_config(config_path)
        preflight = preflight_worker_bench(
            workspace_dir=workspace_dir,
            scenarios=selected_scenarios,
            source_config=source_config,
        )
        if not preflight["passed"]:
            raise ValueError(
                "live worker bench preflight failed: " + "; ".join(preflight["errors"])
            )
        if preflight_only:
            finished_at = datetime.now(UTC)
            return {
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "workspace_dir": str(workspace_dir),
                "execution_mode": "live",
                "preflight_only": True,
                "preflight": preflight,
                "scenario_count": len(selected_scenarios),
                "trial_count": 0,
                "graded_trials": 0,
                "passed_trials": 0,
                "failed_trials": 0,
                "success_rate": None,
                "assertion_pass_rate": None,
                "multi_trial": {},
                "distributions": {},
                "failure_categories": {},
                "scenarios": [],
            }

        with tempfile.TemporaryDirectory(prefix="octopal-worker-bench-") as tmp:
            isolation = _prepare_worker_bench_isolation(
                root_dir=Path(tmp),
                source_config=source_config,
                provider_id=str(selected_scenarios[0].provider_id),
                model=str(selected_scenarios[0].model),
            )
            run_artifacts_dir, include_artifacts = _prepare_live_artifacts_dir(
                requested=artifacts_dir,
                isolation=isolation,
            )
            summaries = _run_scenarios(
                workspace_dir=workspace_dir,
                scenarios=selected_scenarios,
                artifacts_dir=run_artifacts_dir,
                timeout_seconds=timeout_seconds,
                env=_worker_bench_env(source_config, isolation),
                include_artifacts=include_artifacts,
                trials=trials,
            )

    finished_at = datetime.now(UTC)
    graded_trials = sum(1 for item in summaries if item.get("grade", {}).get("passed") is not None)
    failed_trials = sum(1 for item in summaries if _trial_failed(item))
    passed_trials = sum(
        1
        for item in summaries
        if item.get("grade", {}).get("passed") is True and not _trial_failed(item)
    )
    aggregate = aggregate_worker_bench_summaries(summaries)
    result = {
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "workspace_dir": str(workspace_dir),
        "execution_mode": execution_mode,
        "scenario_count": len(selected_scenarios),
        "trial_count": len(summaries),
        "graded_trials": graded_trials,
        "passed_trials": passed_trials,
        "failed_trials": failed_trials,
        "success_rate": aggregate["success_rate"],
        "assertion_pass_rate": aggregate["assertion_pass_rate"],
        "multi_trial": aggregate["multi_trial"],
        "distributions": aggregate["distributions"],
        "failure_categories": aggregate["failure_categories"],
        "scenarios": summaries,
    }
    if execution_mode == "live":
        result["preflight"] = preflight
    return result


def aggregate_worker_bench_summaries(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    outcomes = [_trial_outcome(summary) for summary in summaries]
    evaluated_trial_count = sum(1 for outcome in outcomes if outcome != "ungraded")
    passed_trials = sum(1 for outcome in outcomes if outcome == "passed")
    assertion_count = 0
    passed_assertions = 0
    scenario_outcomes: dict[str, list[str]] = defaultdict(list)
    failure_categories: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"trial_count": 0, "scenario_ids": set()}
    )

    for summary in summaries:
        scenario_id = str(summary.get("scenario_id") or "unknown")
        outcome = _trial_outcome(summary)
        scenario_outcomes[scenario_id].append(outcome)
        grade = summary.get("grade")
        if isinstance(grade, dict):
            assertion_count += int(grade.get("assertion_count") or 0)
            passed_assertions += int(grade.get("passed_count") or 0)
        if outcome == "failed":
            for category in _trial_failure_categories(summary):
                failure_categories[category]["trial_count"] += 1
                failure_categories[category]["scenario_ids"].add(scenario_id)

    scenario_count = len(scenario_outcomes)
    evaluated_scenario_count = sum(
        1 for outcomes in scenario_outcomes.values() if any(item != "ungraded" for item in outcomes)
    )
    pass_at_k_count = sum(1 for outcomes in scenario_outcomes.values() if "passed" in outcomes)
    pass_all_k_count = sum(
        1 for outcomes in scenario_outcomes.values() if all(item == "passed" for item in outcomes)
    )
    trial_sizes = [len(outcomes) for outcomes in scenario_outcomes.values()]

    metric_paths = {
        "elapsed_ms": "elapsed_ms",
        "thinking_steps": "thinking_steps",
        "llm_calls": "llm_calls",
        "tool_calls": "tool_calls",
        "prompt_tokens": "tokens.prompt_tokens",
        "completion_tokens": "tokens.completion_tokens",
        "total_tokens": "tokens.total_tokens",
        "llm_latency_ms": "latency_ms.llm_total",
        "tool_latency_ms": "latency_ms.tool_total",
        "system_prompt_chars": "context.system_prompt_chars",
        "task_prompt_chars": "context.task_prompt_chars",
        "tool_count": "context.tool_count",
        "tool_schema_chars": "context.tool_schema_chars",
        "llm_input_chars_peak": "context.llm_input_chars_peak",
        "llm_input_chars_total": "context.llm_input_chars_total",
        "tool_result_raw_chars_total": "context.tool_result_raw_chars_total",
        "tool_result_rendered_chars_total": "context.tool_result_rendered_chars_total",
        "tool_result_truncated_chars_total": "context.tool_result_truncated_chars_total",
        "estimated_cost_microusd": "inference_budget.estimated_cost_microusd",
    }
    distributions: dict[str, dict[str, int | float]] = {}
    for name, path in metric_paths.items():
        values = [
            value
            for summary in summaries
            if (value := _numeric_path_value(summary, path)) is not None
        ]
        if values:
            distributions[name] = _distribution(values)

    return {
        "success_rate": _rate(passed_trials, evaluated_trial_count),
        "assertion_pass_rate": _rate(passed_assertions, assertion_count),
        "multi_trial": {
            "scenario_count": scenario_count,
            "evaluated_scenario_count": evaluated_scenario_count,
            "min_trials_per_scenario": min(trial_sizes) if trial_sizes else 0,
            "max_trials_per_scenario": max(trial_sizes) if trial_sizes else 0,
            "pass_at_k": _rate(pass_at_k_count, evaluated_scenario_count),
            "pass_all_k": _rate(pass_all_k_count, evaluated_scenario_count),
        },
        "distributions": distributions,
        "failure_categories": {
            category: {
                "trial_count": data["trial_count"],
                "scenario_ids": sorted(data["scenario_ids"]),
            }
            for category, data in sorted(failure_categories.items())
        },
    }


def compare_worker_bench_to_baseline(
    *, summary: dict[str, Any], baseline: dict[str, Any]
) -> dict[str, Any]:
    current_trials = _summary_trial_index(summary, label="current summary")
    baseline_trials = _summary_trial_index(baseline, label="baseline summary")
    current_keys = set(current_trials)
    baseline_keys = set(baseline_trials)
    common_keys = current_keys & baseline_keys

    regressions: list[dict[str, Any]] = []
    improvements: list[dict[str, Any]] = []
    changed: list[dict[str, Any]] = []
    unchanged_count = 0
    for key in sorted(common_keys):
        baseline_outcome = _trial_outcome(baseline_trials[key])
        current_outcome = _trial_outcome(current_trials[key])
        if baseline_outcome == current_outcome:
            unchanged_count += 1
            continue
        item = _outcome_change(
            key=key,
            baseline_outcome=baseline_outcome,
            current_outcome=current_outcome,
        )
        changed.append(item)
        if baseline_outcome == "passed" and current_outcome != "passed":
            regressions.append(item)
        elif baseline_outcome != "passed" and current_outcome == "passed":
            improvements.append(item)

    new_keys = current_keys - baseline_keys
    for key in sorted(new_keys):
        current_outcome = _trial_outcome(current_trials[key])
        if current_outcome == "failed":
            regressions.append(
                _outcome_change(
                    key=key,
                    baseline_outcome="missing",
                    current_outcome=current_outcome,
                )
            )

    current_aggregate = aggregate_worker_bench_summaries(
        [current_trials[key] for key in sorted(common_keys)]
    )
    baseline_aggregate = aggregate_worker_bench_summaries(
        [baseline_trials[key] for key in sorted(common_keys)]
    )
    overall_current_aggregate = aggregate_worker_bench_summaries(list(current_trials.values()))
    overall_baseline_aggregate = aggregate_worker_bench_summaries(list(baseline_trials.values()))
    current_rate = current_aggregate["success_rate"]
    baseline_rate = baseline_aggregate["success_rate"]
    rate_delta = (
        round(float(current_rate) - float(baseline_rate), 4)
        if current_rate is not None and baseline_rate is not None
        else None
    )

    return {
        "regression_detected": bool(regressions),
        "coverage_changed": current_keys != baseline_keys,
        "common_trial_count": len(common_keys),
        "unchanged_trial_count": unchanged_count,
        "success_rate": {
            "baseline": baseline_rate,
            "current": current_rate,
            "delta": rate_delta,
        },
        "overall_success_rate": {
            "baseline": overall_baseline_aggregate["success_rate"],
            "current": overall_current_aggregate["success_rate"],
        },
        "regressions": regressions,
        "improvements": improvements,
        "other_outcome_changes": [
            item for item in changed if item not in regressions and item not in improvements
        ],
        "new_trials": [_trial_identity(key) for key in sorted(new_keys)],
        "missing_trials": [_trial_identity(key) for key in sorted(baseline_keys - current_keys)],
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
    parser.add_argument(
        "--config",
        help="Explicit source config for live mode. A sanitized temporary copy is used by workers.",
    )
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="Validate live provider, budgets, tools, templates, and isolation without execution.",
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
    parser.add_argument(
        "--baseline",
        help="Optional prior JSON summary to compare by scenario and trial outcome.",
    )
    parser.add_argument("--timeout", type=int, default=300, help="Per-scenario timeout in seconds.")
    args = parser.parse_args(argv)

    try:
        scenarios = load_scenarios_file(Path(args.suite)) if args.suite else None
        baseline = _read_json_object(Path(args.baseline)) if args.baseline else None
        summary = run_worker_bench(
            workspace_dir=Path(args.workspace),
            scenario_names=args.scenario,
            scenarios=scenarios,
            artifacts_dir=Path(args.artifacts_dir) if args.artifacts_dir else None,
            timeout_seconds=args.timeout,
            execution_mode=args.mode,
            replay_dir=Path(args.replay_dir) if args.replay_dir else None,
            trials=args.trials,
            config_path=Path(args.config) if args.config else None,
            preflight_only=args.preflight_only,
        )
        if baseline is not None:
            summary["baseline_comparison"] = compare_worker_bench_to_baseline(
                summary=summary,
                baseline=baseline,
            )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    text = json.dumps(summary, ensure_ascii=False, indent=2)
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text + "\n", encoding="utf-8")
    sys.stdout.write(text + "\n")
    comparison = summary.get("baseline_comparison")
    if isinstance(comparison, dict):
        return 1 if comparison.get("regression_detected") else 0
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


def preflight_worker_bench(
    *,
    workspace_dir: Path,
    scenarios: list[WorkerBenchScenario],
    source_config: OctopalConfig,
) -> dict[str, Any]:
    errors: list[str] = []
    scenario_reports: list[dict[str, Any]] = []
    all_tools_by_name = {tool.name.strip().lower(): tool for tool in get_tools(mcp_manager=None)}
    source_provider_id = str(source_config.llm.provider_id or "").strip().lower()
    selected_search_provider, _ = _selected_search_credential(source_config)
    configured_search_providers = [selected_search_provider] if selected_search_provider else []

    for scenario in scenarios:
        template_path = workspace_dir / "workers" / scenario.template_id / "worker.json"
        try:
            template = _read_json_object(template_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"scenario {scenario.id} template cannot be loaded: {exc}")
            continue

        provider_id = str(scenario.provider_id or "").strip().lower()
        model = str(scenario.model or "").strip()
        expected_pricing_model = _resolved_pricing_model(
            provider_id=provider_id,
            model=model,
            model_prefix=source_config.llm.model_prefix,
        )
        scenario_errors: list[str] = []
        if provider_id != source_provider_id:
            scenario_errors.append(
                f"provider {provider_id!r} does not match source config provider "
                f"{source_provider_id!r}"
            )
        provider_entry = get_provider_catalog_entry(provider_id)
        if provider_entry.requires_api_key and not str(source_config.llm.api_key or "").strip():
            scenario_errors.append(f"source config has no API key for provider {provider_id!r}")
        if (
            scenario.inference_budget is not None
            and scenario.inference_budget.pricing_model != expected_pricing_model
        ):
            scenario_errors.append(
                "live_budget pricing_model must match the resolved model "
                f"{expected_pricing_model!r}"
            )

        required_permissions = {
            permission.strip().lower()
            for permission in _str_list(template.get("required_permissions"))
        }
        tool_reports: list[dict[str, Any]] = []
        available_tools = _str_list(template.get("available_tools"))
        if not available_tools:
            scenario_errors.append("template must expose at least one explicitly read-only tool")
        for raw_name in available_tools:
            normalized_name = raw_name.strip().lower()
            tool = all_tools_by_name.get(normalized_name)
            if tool is None:
                scenario_errors.append(f"template references unknown tool {raw_name!r}")
                continue
            metadata = tool.metadata
            if not metadata.read_only or metadata.risk != "safe" or metadata.owner != "core":
                scenario_errors.append(
                    f"tool {raw_name!r} is not declared as a safe core read-only tool"
                )
            permission = str(tool.permission or "").strip().lower()
            if permission not in required_permissions:
                scenario_errors.append(
                    f"tool {raw_name!r} requires undeclared permission {permission!r}"
                )
            if "search" in metadata.capabilities and not configured_search_providers:
                scenario_errors.append(
                    f"tool {raw_name!r} requires a configured isolated search provider"
                )
            tool_reports.append(
                {
                    "name": tool.name,
                    "permission": permission,
                    "risk": metadata.risk,
                    "owner": metadata.owner,
                    "read_only": metadata.read_only,
                    "capabilities": list(metadata.capabilities),
                }
            )

        if scenario_errors:
            errors.extend(f"scenario {scenario.id}: {message}" for message in scenario_errors)
        scenario_reports.append(
            {
                "id": scenario.id,
                "template_id": scenario.template_id,
                "provider_id": provider_id,
                "model": model,
                "pricing_model": expected_pricing_model,
                "credential_present": bool(str(source_config.llm.api_key or "").strip()),
                "tools": tool_reports,
                "passed": not scenario_errors,
            }
        )

    return {
        "passed": not errors,
        "errors": errors,
        "scenarios": scenario_reports,
        "configured_search_providers": configured_search_providers,
        "isolation": {
            "temporary_workspace": True,
            "temporary_state": True,
            "temporary_config": True,
            "temporary_browser_profile": True,
            "minimal_environment": True,
            "connectors_disabled": True,
            "observability_disabled": True,
        },
    }


def _load_bench_source_config(path: Path) -> OctopalConfig:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise ValueError(f"live worker bench config does not exist: {resolved}")
    try:
        return OctopalConfig.model_validate(_read_json_object(resolved))
    except Exception as exc:
        raise ValueError(f"invalid live worker bench config {resolved}: {exc}") from exc


def _resolved_pricing_model(*, provider_id: str, model: str, model_prefix: str | None) -> str:
    entry = get_provider_catalog_entry(provider_id)
    prefix = str(model_prefix or entry.model_prefix or "").strip()
    if not prefix:
        return model
    if entry.always_prefix_model:
        return model if model.startswith(f"{prefix}/") else f"{prefix}/{model}"
    if "/" in model:
        return model
    return f"{prefix}/{model}"


def _selected_search_credential(source_config: OctopalConfig) -> tuple[str | None, str | None]:
    brave_key = str(source_config.search.brave_api_key or "").strip()
    if brave_key:
        return "brave", brave_key
    firecrawl_key = str(source_config.search.firecrawl_api_key or "").strip()
    if firecrawl_key:
        return "firecrawl", firecrawl_key
    return None, None


def _prepare_worker_bench_isolation(
    *,
    root_dir: Path,
    source_config: OctopalConfig,
    provider_id: str,
    model: str,
) -> WorkerBenchIsolation:
    workspace_dir = root_dir / "workspace"
    state_dir = root_dir / "state"
    browser_profile_dir = root_dir / "browser-profile"
    home_dir = root_dir / "home"
    config_path = root_dir / "config.json"
    for directory in (workspace_dir, state_dir, browser_profile_dir, home_dir, root_dir / "tmp"):
        directory.mkdir(parents=True, exist_ok=True)

    search_provider, search_key = _selected_search_credential(source_config)
    isolated_search = SearchConfig(
        brave_api_key=search_key if search_provider == "brave" else None,
        firecrawl_api_key=search_key if search_provider == "firecrawl" else None,
    )
    isolated_config = OctopalConfig(
        llm=LLMConfig(
            provider_id=provider_id,
            model=model,
            api_key=source_config.llm.api_key,
            api_base=source_config.llm.api_base,
            model_prefix=source_config.llm.model_prefix,
        ),
        litellm=LiteLLMRuntimeConfig(
            num_retries=0,
            timeout=source_config.litellm.timeout,
            fallbacks=None,
            drop_params=source_config.litellm.drop_params,
            caching=False,
            max_concurrency=1,
            rate_limit_max_retries=0,
            rate_limit_base_delay_seconds=source_config.litellm.rate_limit_base_delay_seconds,
            rate_limit_max_delay_seconds=source_config.litellm.rate_limit_max_delay_seconds,
        ),
        storage=StorageConfig(state_dir=state_dir, workspace_dir=workspace_dir),
        search=isolated_search,
        web=source_config.web.model_copy(deep=True),
        browser=BrowserRuntimeConfig(
            backend="playwright",
            pinchtab_managed=False,
            pinchtab_fallback_to_playwright=False,
            pinchtab_token=None,
            pinchtab_session=None,
        ),
    )
    config_path.write_text(
        json.dumps(isolated_config.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if os.name != "nt":
        os.chmod(config_path, 0o600)
    return WorkerBenchIsolation(
        root_dir=root_dir,
        workspace_dir=workspace_dir,
        state_dir=state_dir,
        browser_profile_dir=browser_profile_dir,
        home_dir=home_dir,
        config_path=config_path,
    )


def _prepare_live_artifacts_dir(
    *, requested: Path | None, isolation: WorkerBenchIsolation
) -> tuple[Path, bool]:
    if requested is None:
        artifacts_dir = isolation.root_dir / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        return artifacts_dir, False
    artifacts_dir = requested.expanduser().resolve()
    if artifacts_dir.exists() and any(artifacts_dir.iterdir()):
        raise ValueError(f"live artifacts_dir must be empty: {artifacts_dir}")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    return artifacts_dir, True


def _worker_bench_env(
    source_config: OctopalConfig, isolation: WorkerBenchIsolation
) -> dict[str, str]:
    env = {name: os.environ[name] for name in _BENCH_ENV_PASSTHROUGH if os.environ.get(name)}
    env.update(
        {
            "HOME": str(isolation.home_dir),
            "TMPDIR": str(isolation.root_dir / "tmp"),
            "XDG_CACHE_HOME": str(isolation.root_dir / "cache"),
            "XDG_CONFIG_HOME": str(isolation.root_dir / "config-home"),
            "OCTOPAL_CONFIG_FILE": str(isolation.config_path),
            "OCTOPAL_WORKSPACE_DIR": str(isolation.workspace_dir),
            "OCTOPAL_STATE_DIR": str(isolation.state_dir),
            "OCTOPAL_PINCHTAB_OWNERSHIP_FILE": str(
                isolation.browser_profile_dir / "pinchtab-ownership.json"
            ),
        }
    )
    search_provider, search_key = _selected_search_credential(source_config)
    if search_provider == "brave" and search_key:
        env["BRAVE_API_KEY"] = search_key
    if search_provider == "firecrawl" and search_key:
        env["FIRECRAWL_API_KEY"] = search_key
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
        elif grader_type in {
            "required_output_path",
            "required_telemetry_path",
            "required_context_manifest_path",
        }:
            _required_text(raw.get("path"), field=f"scenario {scenario_id} grader path")
        elif grader_type in {"required_tool", "forbidden_tool"}:
            _required_text(raw.get("tool"), field=f"scenario {scenario_id} grader tool")
        elif grader_type in {
            "max_tool_calls",
            "max_thinking_steps",
            "max_stdout_parse_errors",
        }:
            maximum = raw.get("maximum")
            if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 0:
                raise ValueError(
                    f"Scenario {scenario_id} {grader_type} maximum must be a non-negative integer"
                )
        graders.append(dict(raw))
    return graders


def _load_live_budget(value: Any, *, scenario_id: str) -> WorkerInferenceBudget | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f"Scenario {scenario_id} live_budget must be an object")
    pricing_model = _required_text(
        value.get("pricing_model"), field=f"scenario {scenario_id} live_budget pricing_model"
    )
    max_total_tokens = _optional_positive_int(
        value.get("max_total_tokens"),
        field=f"scenario {scenario_id} live_budget max_total_tokens",
    )
    if max_total_tokens is None:
        raise ValueError(f"Scenario {scenario_id} live_budget max_total_tokens is required")
    max_llm_calls = _optional_positive_int(
        value.get("max_llm_calls"),
        field=f"scenario {scenario_id} live_budget max_llm_calls",
    )
    if max_llm_calls is None:
        raise ValueError(f"Scenario {scenario_id} live_budget max_llm_calls is required")
    if max_llm_calls > 6:
        raise ValueError(
            f"Scenario {scenario_id} live_budget max_llm_calls exceeds the safety cap of 6"
        )
    max_tool_calls = _optional_positive_int(
        value.get("max_tool_calls"),
        field=f"scenario {scenario_id} live_budget max_tool_calls",
    )
    if max_tool_calls is None:
        raise ValueError(f"Scenario {scenario_id} live_budget max_tool_calls is required")
    if max_tool_calls > 6:
        raise ValueError(
            f"Scenario {scenario_id} live_budget max_tool_calls exceeds the safety cap of 6"
        )
    max_cost_microusd = _usd_to_microusd(
        value.get("max_cost_usd"),
        field=f"scenario {scenario_id} live_budget max_cost_usd",
        allow_zero=False,
    )
    input_rate = _usd_to_microusd(
        value.get("input_cost_per_million_tokens_usd"),
        field=(f"scenario {scenario_id} live_budget input_cost_per_million_tokens_usd"),
        allow_zero=True,
    )
    completion_rate = _usd_to_microusd(
        value.get("completion_cost_per_million_tokens_usd"),
        field=(f"scenario {scenario_id} live_budget completion_cost_per_million_tokens_usd"),
        allow_zero=True,
    )
    if input_rate == 0 and completion_rate == 0:
        raise ValueError(f"Scenario {scenario_id} live_budget must declare a non-zero token rate")
    return WorkerInferenceBudget(
        pricing_model=pricing_model,
        max_llm_calls=max_llm_calls,
        max_tool_calls=max_tool_calls,
        max_total_tokens=max_total_tokens,
        max_cost_microusd=max_cost_microusd,
        input_cost_microusd_per_million_tokens=input_rate,
        completion_cost_microusd_per_million_tokens=completion_rate,
    )


def _validate_live_scenario_contract(
    *,
    scenario_id: str,
    provider_id: str | None,
    model: str | None,
    max_thinking_steps: int | None,
    inference_budget: WorkerInferenceBudget | None,
) -> None:
    if not provider_id:
        raise ValueError(f"Live scenario {scenario_id} must declare an explicit provider_id")
    if provider_id.strip().lower() == "codex":
        raise ValueError(
            f"Live scenario {scenario_id} cannot use codex because it lacks token accounting"
        )
    supported_provider_ids = set(list_registered_provider_ids(include_custom=False)) - {"codex"}
    if provider_id.strip().lower() not in supported_provider_ids:
        supported = ", ".join(sorted(supported_provider_ids))
        raise ValueError(f"Live scenario {scenario_id} provider_id must be one of: {supported}")
    if not model:
        raise ValueError(f"Live scenario {scenario_id} must declare an explicit model")
    if max_thinking_steps is None:
        raise ValueError(
            f"Live scenario {scenario_id} must declare max_thinking_steps between 1 and 6"
        )
    if max_thinking_steps > 6:
        raise ValueError(
            f"Live scenario {scenario_id} max_thinking_steps exceeds the initial safety cap of 6"
        )
    if inference_budget is None:
        raise ValueError(f"Live scenario {scenario_id} must declare live_budget")
    if inference_budget.max_total_tokens > _MAX_LIVE_RUN_TOTAL_TOKENS:
        raise ValueError(
            f"Live scenario {scenario_id} max_total_tokens exceeds the safety cap of "
            f"{_MAX_LIVE_RUN_TOTAL_TOKENS}"
        )
    if inference_budget.max_cost_microusd > _MAX_LIVE_RUN_COST_MICROUSD:
        raise ValueError(f"Live scenario {scenario_id} max_cost_usd exceeds the safety cap of 0.10")


def _usd_to_microusd(value: Any, *, field: str, allow_zero: bool) -> int:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{field} is required")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a decimal number") from exc
    if not amount.is_finite() or amount < 0 or (not allow_zero and amount == 0):
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{field} must be a finite {qualifier} decimal number")
    return int((amount * Decimal(1_000_000)).to_integral_value(rounding=ROUND_CEILING))


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_positive_int(value: Any, *, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return int(value)


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


def _path_evidence(*, path: str, found: bool, value: Any) -> dict[str, Any]:
    return {
        "path": path,
        "found": found,
        "value_type": type(value).__name__ if found else None,
        "value_chars": _serialized_value_chars(value) if found else None,
    }


def _serialized_value_chars(value: Any) -> int:
    try:
        return len(json.dumps(value, ensure_ascii=False, default=str))
    except Exception:
        return len(str(value))


def _artifact_stem(scenario_id: str, trial: int, trials: int) -> str:
    if trials == 1:
        return scenario_id
    return f"{scenario_id}.trial-{trial}"


def _trial_failed(summary: dict[str, Any]) -> bool:
    grade = summary.get("grade")
    grade_passed = grade.get("passed") if isinstance(grade, dict) else None
    if grade_passed is False:
        return True
    returncode = summary.get("returncode")
    if returncode not in (None, 0):
        return True
    status = str(summary.get("status") or "").strip().lower()
    if status in {"missing_result", "timeout"}:
        return True
    return grade_passed is None and status == "failed"


def _trial_outcome(summary: dict[str, Any]) -> str:
    if _trial_failed(summary):
        return "failed"
    grade = summary.get("grade")
    if isinstance(grade, dict) and grade.get("passed") is True:
        return "passed"
    return "ungraded"


def _trial_failure_categories(summary: dict[str, Any]) -> set[str]:
    categories: set[str] = set()
    returncode = summary.get("returncode")
    if returncode not in (None, 0):
        categories.add("execution:nonzero_returncode")
    status = str(summary.get("status") or "").strip().lower()
    if status in {"missing_result", "timeout"}:
        categories.add(f"execution:{status}")
    grade = summary.get("grade")
    if isinstance(grade, dict):
        assertions = grade.get("assertions")
        if isinstance(assertions, list):
            for assertion in assertions:
                if isinstance(assertion, dict) and assertion.get("passed") is False:
                    grader_type = str(assertion.get("type") or "unknown")
                    categories.add(f"grader:{grader_type}")
    if not categories:
        categories.add("execution:failed")
    return categories


def _numeric_path_value(payload: dict[str, Any], path: str) -> int | float | None:
    found, value = _resolve_json_path(payload, path)
    if not found or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return float(value)
    return None


def _distribution(values: list[int | float]) -> dict[str, int | float]:
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "max": ordered[-1],
        "mean": round(sum(ordered) / len(ordered), 3),
        "p50": _nearest_rank(ordered, 0.50),
        "p95": _nearest_rank(ordered, 0.95),
    }


def _nearest_rank(values: list[int | float], percentile: float) -> int | float:
    index = max(0, ceil(percentile * len(values)) - 1)
    return values[index]


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _summary_trial_index(
    summary: dict[str, Any], *, label: str
) -> dict[tuple[str, int], dict[str, Any]]:
    raw_scenarios = summary.get("scenarios")
    if not isinstance(raw_scenarios, list):
        raise ValueError(f"{label} must contain a scenarios list")
    indexed: dict[tuple[str, int], dict[str, Any]] = {}
    for index, raw in enumerate(raw_scenarios):
        if not isinstance(raw, dict):
            raise ValueError(f"{label} scenario {index} must be an object")
        scenario_id = _required_identifier(
            raw.get("scenario_id"), field=f"{label} scenario {index} scenario_id"
        )
        trial = raw.get("trial", 1)
        if isinstance(trial, bool) or not isinstance(trial, int) or trial <= 0:
            raise ValueError(f"{label} scenario {scenario_id} trial must be a positive integer")
        key = (scenario_id, trial)
        if key in indexed:
            raise ValueError(f"{label} contains duplicate trial {scenario_id}#{trial}")
        indexed[key] = raw
    return indexed


def _outcome_change(
    *, key: tuple[str, int], baseline_outcome: str, current_outcome: str
) -> dict[str, Any]:
    return {
        **_trial_identity(key),
        "baseline_outcome": baseline_outcome,
        "current_outcome": current_outcome,
    }


def _trial_identity(key: tuple[str, int]) -> dict[str, Any]:
    scenario_id, trial = key
    return {"scenario_id": scenario_id, "trial": trial}


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
