from __future__ import annotations

import argparse
import json
import os
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
    scenarios = list(DEFAULT_SCENARIOS)
    if not selected:
        return scenarios

    by_id = {scenario.id: scenario for scenario in scenarios}
    missing = [item for item in selected if item not in by_id]
    if missing:
        available = ", ".join(sorted(by_id))
        raise ValueError(f"Unknown scenario(s): {', '.join(missing)}. Available: {available}")
    return [by_id[item] for item in selected]


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
    returncode: int,
    elapsed_ms: int,
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

    output = result.get("output") if isinstance(result.get("output"), dict) else {}
    telemetry = output.get("_telemetry") if isinstance(output, dict) else {}
    if not isinstance(telemetry, dict):
        telemetry = {}
    context = telemetry.get("context") if isinstance(telemetry.get("context"), dict) else {}
    tokens = telemetry.get("tokens") if isinstance(telemetry.get("tokens"), dict) else {}

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
        "message_count": len(messages),
        "log_count": len(logs),
        "instruction_request_count": len(instruction_requests),
        "stdout_parse_errors": len(parse_errors),
        "stderr_chars": len(stderr),
    }


def run_worker_bench(
    *,
    workspace_dir: Path,
    scenario_names: list[str] | None = None,
    artifacts_dir: Path | None = None,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    settings = load_settings()
    scenarios = select_scenarios(scenario_names)
    started_at = datetime.now(UTC)
    summaries: list[dict[str, Any]] = []

    if artifacts_dir is None:
        with tempfile.TemporaryDirectory(prefix="octopal-worker-bench-") as tmp:
            summaries = _run_scenarios(
                workspace_dir=workspace_dir,
                scenarios=scenarios,
                artifacts_dir=Path(tmp),
                timeout_seconds=timeout_seconds,
                env=_worker_bench_env(settings, workspace_dir),
                include_artifacts=False,
            )
    else:
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        summaries = _run_scenarios(
            workspace_dir=workspace_dir,
            scenarios=scenarios,
            artifacts_dir=artifacts_dir,
            timeout_seconds=timeout_seconds,
            env=_worker_bench_env(settings, workspace_dir),
            include_artifacts=True,
        )

    finished_at = datetime.now(UTC)
    return {
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "workspace_dir": str(workspace_dir),
        "scenario_count": len(summaries),
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
        choices=scenario_ids(),
        help="Scenario id to run. Repeat to run multiple. Default: all scenarios.",
    )
    parser.add_argument(
        "--artifacts-dir",
        help="Optional directory to keep generated worker specs and JSONL output.",
    )
    parser.add_argument("--out", help="Optional path to write the JSON summary.")
    parser.add_argument("--timeout", type=int, default=300, help="Per-scenario timeout in seconds.")
    args = parser.parse_args(argv)

    summary = run_worker_bench(
        workspace_dir=Path(args.workspace),
        scenario_names=args.scenario,
        artifacts_dir=Path(args.artifacts_dir) if args.artifacts_dir else None,
        timeout_seconds=args.timeout,
    )
    text = json.dumps(summary, ensure_ascii=False, indent=2)
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text + "\n", encoding="utf-8")
    sys.stdout.write(text + "\n")
    return 0


def _run_scenarios(
    *,
    workspace_dir: Path,
    scenarios: list[WorkerBenchScenario],
    artifacts_dir: Path,
    timeout_seconds: int,
    env: dict[str, str],
    include_artifacts: bool,
) -> list[dict[str, Any]]:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    summaries: list[dict[str, Any]] = []
    for scenario in scenarios:
        template_path = workspace_dir / "workers" / scenario.template_id / "worker.json"
        template = _read_json_object(template_path)
        run_id = f"bench-{scenario.id}-{int(time.time())}"
        spec = build_worker_spec(scenario=scenario, template=template, run_id=run_id)
        spec_path = artifacts_dir / f"{scenario.id}.spec.json"
        stdout_path = artifacts_dir / f"{scenario.id}.out.jsonl"
        stderr_path = artifacts_dir / f"{scenario.id}.err.txt"
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
            }
        if include_artifacts:
            summary["artifacts"] = {
                "spec": str(spec_path),
                "stdout": str(stdout_path),
                "stderr": str(stderr_path),
            }
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
