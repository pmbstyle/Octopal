from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from pydantic import BaseModel, ConfigDict, Field, model_validator

from octopal.infrastructure.config.models import OctopalConfig
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.providers.base import InferenceProvider, Message
from octopal.infrastructure.providers.litellm_provider import LiteLLMProvider
from octopal.infrastructure.providers.profile_resolver import resolve_litellm_profile
from octopal.tools.catalog import get_tools
from octopal.tools.registry import ToolSpec

_MAX_CASES = 3
_MAX_TOTAL_TOKENS = 30_000
_MAX_COST_MICROUSD = 30_000
_MAX_OUTPUT_TOKENS = 512
_MAX_PROMPT_CHARS = 2_000
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SYSTEM_PROMPT = (
    "You are evaluating one tool schema. Follow the user request by calling the only exposed "
    "tool exactly once. Do not claim that the tool ran and do not provide a prose answer."
)
_MALFORMED_ARGUMENT_OUTCOMES = frozenset(
    {"arguments_invalid_json", "arguments_non_object", "arguments_schema_invalid"}
)


class ToolCallBenchCase(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    tool: str
    prompt: str = Field(min_length=1, max_length=_MAX_PROMPT_CHARS)


class ToolCallBenchBudget(BaseModel):
    model_config = ConfigDict(frozen=True)

    pricing_model: str = Field(min_length=1)
    max_calls: int = Field(gt=0, le=_MAX_CASES, strict=True)
    max_total_tokens: int = Field(gt=0, le=_MAX_TOTAL_TOKENS, strict=True)
    max_cost_usd: Decimal = Field(gt=0)
    input_cost_per_million_tokens_usd: Decimal = Field(ge=0)
    output_cost_per_million_tokens_usd: Decimal = Field(ge=0)
    max_output_tokens: int = Field(gt=0, le=_MAX_OUTPUT_TOKENS, strict=True)

    @model_validator(mode="after")
    def validate_rates_and_cost(self) -> ToolCallBenchBudget:
        amounts = (
            self.max_cost_usd,
            self.input_cost_per_million_tokens_usd,
            self.output_cost_per_million_tokens_usd,
        )
        if any(not amount.is_finite() for amount in amounts):
            raise ValueError("budget amounts must be finite")
        if (
            self.input_cost_per_million_tokens_usd == 0
            and self.output_cost_per_million_tokens_usd == 0
        ):
            raise ValueError("at least one token cost rate must be non-zero")
        if self.max_cost_microusd > _MAX_COST_MICROUSD:
            raise ValueError("max_cost_usd exceeds the hard safety cap of 0.03 USD")
        return self

    @property
    def max_cost_microusd(self) -> int:
        return _usd_to_microusd(self.max_cost_usd)

    @property
    def input_rate_microusd(self) -> int:
        return _usd_to_microusd(self.input_cost_per_million_tokens_usd)

    @property
    def output_rate_microusd(self) -> int:
        return _usd_to_microusd(self.output_cost_per_million_tokens_usd)


class ToolCallBenchSuite(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: int = Field(default=1, ge=1, le=1, strict=True)
    provider_id: str = Field(min_length=1)
    model: str = Field(min_length=1)
    variant: str = Field(pattern=r"^(baseline|examples)$")
    live_budget: ToolCallBenchBudget
    cases: tuple[ToolCallBenchCase, ...] = Field(min_length=1, max_length=_MAX_CASES)

    @model_validator(mode="after")
    def validate_case_contract(self) -> ToolCallBenchSuite:
        ids = [case.id for case in self.cases]
        if any(not _SAFE_ID_RE.fullmatch(case_id) for case_id in ids):
            raise ValueError("case ids must be safe identifiers")
        if len(ids) != len(set(ids)):
            raise ValueError("case ids must be unique")
        if self.live_budget.max_calls != len(self.cases):
            raise ValueError("live_budget max_calls must equal the number of cases")
        return self


@dataclass(frozen=True)
class ToolCallBenchPreflight:
    passed: bool
    errors: list[str]
    report: dict[str, Any]
    tools_by_case: dict[str, ToolSpec]


ProviderFactory = Callable[[OctopalConfig], InferenceProvider]


def load_tool_call_bench_suite(path: Path) -> ToolCallBenchSuite:
    payload = _read_json_object(path)
    return ToolCallBenchSuite.model_validate(payload)


def load_tool_call_bench_config(path: Path) -> OctopalConfig:
    try:
        return OctopalConfig.model_validate(_read_json_object(path))
    except Exception as exc:
        raise ValueError("invalid tool-call bench config") from exc


def preflight_tool_call_bench(
    *,
    suite: ToolCallBenchSuite,
    source_config: OctopalConfig,
) -> ToolCallBenchPreflight:
    errors: list[str] = []
    settings = _isolated_provider_settings(source_config)
    profile = resolve_litellm_profile(settings, config_override=source_config.llm)
    if profile.provider_id != suite.provider_id.strip().lower():
        errors.append("suite provider_id does not match the source config")
    if profile.raw_model != suite.model.strip():
        errors.append("suite model does not match the source config")
    if profile.model != suite.live_budget.pricing_model.strip():
        errors.append("live_budget pricing_model does not match the resolved provider model")
    if profile.requires_api_key and not str(profile.api_key or "").strip():
        errors.append("source config has no API key for the selected provider")

    tools_by_name = {tool.name: tool for tool in get_tools(mcp_manager=None)}
    tools_by_case: dict[str, ToolSpec] = {}
    case_reports: list[dict[str, Any]] = []
    worst_case_tokens = 0
    worst_case_cost = 0
    for case in suite.cases:
        tool = tools_by_name.get(case.tool)
        if tool is None:
            errors.append(f"case {case.id} references unknown tool {case.tool!r}")
            continue
        if suite.variant == "baseline" and tool.usage_examples:
            errors.append(f"case {case.id} baseline tool already has usage examples")
        if suite.variant == "examples" and not tool.usage_examples:
            errors.append(f"case {case.id} examples variant has no usage examples")
        try:
            Draft202012Validator.check_schema(tool.parameters)
        except Exception:
            errors.append(f"case {case.id} tool schema is not valid JSON Schema")
            continue
        tools_by_case[case.id] = tool
        messages = _case_messages(case)
        input_ceiling = _request_input_token_ceiling(messages, [tool.to_openai_tool()])
        call_token_ceiling = input_ceiling + suite.live_budget.max_output_tokens
        call_cost_ceiling = _estimate_cost_microusd(
            prompt_tokens=input_ceiling,
            completion_tokens=suite.live_budget.max_output_tokens,
            budget=suite.live_budget,
        )
        worst_case_tokens += call_token_ceiling
        worst_case_cost += call_cost_ceiling
        case_reports.append(
            {
                "id": case.id,
                "tool": tool.name,
                "schema_chars": len(
                    json.dumps(tool.to_openai_tool(), ensure_ascii=False, separators=(",", ":"))
                ),
                "usage_example_count": len(tool.usage_examples),
                "usage_example_schema_chars": _usage_example_schema_chars(tool),
                "usage_example_evidence": tool.usage_example_evidence,
                "input_token_ceiling": input_ceiling,
                "call_token_ceiling": call_token_ceiling,
                "call_cost_ceiling_microusd": call_cost_ceiling,
            }
        )

    if worst_case_tokens > suite.live_budget.max_total_tokens:
        errors.append("worst-case token ceiling exceeds live_budget max_total_tokens")
    if worst_case_cost > suite.live_budget.max_cost_microusd:
        errors.append("worst-case cost ceiling exceeds live_budget max_cost_usd")

    report = {
        "passed": not errors,
        "errors": errors,
        "provider_id": profile.provider_id,
        "model": profile.raw_model,
        "pricing_model": profile.model,
        "credential_present": bool(str(profile.api_key or "").strip()),
        "variant": suite.variant,
        "case_count": len(suite.cases),
        "cases": case_reports,
        "budget": {
            "max_calls": suite.live_budget.max_calls,
            "max_total_tokens": suite.live_budget.max_total_tokens,
            "max_cost_microusd": suite.live_budget.max_cost_microusd,
            "max_output_tokens": suite.live_budget.max_output_tokens,
            "worst_case_tokens": worst_case_tokens,
            "worst_case_cost_microusd": worst_case_cost,
        },
        "safety": {
            "tools_execute": False,
            "strict_single_attempt": True,
            "tool_choice_required": True,
            "raw_prompts_retained": False,
            "raw_arguments_retained": False,
            "provider_errors_stop_run": True,
            "missing_usage_stops_run": True,
        },
    }
    return ToolCallBenchPreflight(
        passed=not errors,
        errors=errors,
        report=report,
        tools_by_case=tools_by_case,
    )


async def run_tool_call_bench(
    *,
    suite: ToolCallBenchSuite,
    source_config: OctopalConfig,
    preflight_only: bool = False,
    provider_factory: ProviderFactory | None = None,
) -> dict[str, Any]:
    preflight = preflight_tool_call_bench(suite=suite, source_config=source_config)
    if not preflight.passed:
        raise ValueError("tool-call bench preflight failed: " + "; ".join(preflight.errors))
    if preflight_only:
        return {"status": "preflight_ok", "preflight": preflight.report}

    provider = (provider_factory or _build_provider)(source_config)
    results: list[dict[str, Any]] = []
    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_tokens = 0
    estimated_cost_microusd = 0
    stop_reason: str | None = None

    for case in suite.cases:
        tool = preflight.tools_by_case[case.id]
        case_report = next(item for item in preflight.report["cases"] if item["id"] == case.id)
        if (
            total_tokens + int(case_report["call_token_ceiling"])
            > suite.live_budget.max_total_tokens
        ):
            stop_reason = "token_budget_preflight_exhausted"
            break
        if (
            estimated_cost_microusd + int(case_report["call_cost_ceiling_microusd"])
            > suite.live_budget.max_cost_microusd
        ):
            stop_reason = "cost_budget_preflight_exhausted"
            break
        try:
            response = await provider.complete_with_tools(
                _case_messages(case),
                tools=[tool.to_openai_tool()],
                tool_choice="required",
                temperature=0.0,
                max_tokens=suite.live_budget.max_output_tokens,
                strict_single_attempt=True,
            )
        except Exception as exc:
            results.append(
                {
                    "case_id": case.id,
                    "tool": tool.name,
                    "outcome": "provider_error",
                    "error_type": type(exc).__name__,
                }
            )
            stop_reason = "provider_error"
            break

        usage = _parse_usage(response.get("usage") if isinstance(response, dict) else None)
        if usage is None:
            results.append(
                {
                    "case_id": case.id,
                    "tool": tool.name,
                    "outcome": "usage_missing",
                }
            )
            stop_reason = "usage_missing"
            break
        total_prompt_tokens += usage["prompt_tokens"]
        total_completion_tokens += usage["completion_tokens"]
        total_tokens += usage["total_tokens"]
        call_cost = _estimate_cost_microusd(
            prompt_tokens=usage["prompt_tokens"],
            completion_tokens=usage["completion_tokens"],
            budget=suite.live_budget,
        )
        estimated_cost_microusd += call_cost
        result = _grade_tool_call_response(case=case, tool=tool, response=response)
        if (
            result.get("outcome") == "tool_call_missing"
            and usage["completion_tokens"] >= suite.live_budget.max_output_tokens
        ):
            result["outcome"] = "output_budget_exhausted"
        result["usage"] = usage
        result["estimated_cost_microusd"] = call_cost
        results.append(result)
        if total_tokens > suite.live_budget.max_total_tokens:
            stop_reason = "token_budget_exceeded"
            break
        if estimated_cost_microusd > suite.live_budget.max_cost_microusd:
            stop_reason = "cost_budget_exceeded"
            break

    malformed_count = sum(
        1 for result in results if result.get("outcome") in _MALFORMED_ARGUMENT_OUTCOMES
    )
    expected_tool_call_count = sum(
        1
        for result in results
        if result.get("outcome") in _MALFORMED_ARGUMENT_OUTCOMES.union({"valid"})
    )
    valid_count = sum(1 for result in results if result.get("outcome") == "valid")
    status = "completed" if stop_reason is None and len(results) == len(suite.cases) else "stopped"
    return {
        "status": status,
        "stop_reason": stop_reason,
        "provider_id": suite.provider_id,
        "model": suite.model,
        "pricing_model": suite.live_budget.pricing_model,
        "variant": suite.variant,
        "case_count": len(suite.cases),
        "attempted_cases": len(results),
        "results": results,
        "metrics": {
            "provider_attempts": len(results),
            "valid_call_count": valid_count,
            "call_success_rate": valid_count / len(suite.cases),
            "malformed_argument_count": malformed_count,
            "malformed_argument_rate": (
                malformed_count / expected_tool_call_count if expected_tool_call_count else None
            ),
            "prompt_tokens": total_prompt_tokens,
            "completion_tokens": total_completion_tokens,
            "total_tokens": total_tokens,
            "estimated_cost_microusd": estimated_cost_microusd,
            "usage_accounting_complete": all("usage" in result for result in results),
        },
        "preflight": preflight.report,
    }


def _grade_tool_call_response(
    *,
    case: ToolCallBenchCase,
    tool: ToolSpec,
    response: Any,
) -> dict[str, Any]:
    tool_calls = response.get("tool_calls") if isinstance(response, dict) else None
    if not isinstance(tool_calls, list) or not tool_calls:
        return {"case_id": case.id, "tool": tool.name, "outcome": "tool_call_missing"}
    if len(tool_calls) != 1:
        return {
            "case_id": case.id,
            "tool": tool.name,
            "outcome": "multiple_tool_calls",
            "tool_call_count": len(tool_calls),
        }
    function = tool_calls[0].get("function") if isinstance(tool_calls[0], dict) else None
    if not isinstance(function, dict) or str(function.get("name") or "") != tool.name:
        return {"case_id": case.id, "tool": tool.name, "outcome": "wrong_tool"}
    raw_arguments = function.get("arguments", "")
    try:
        arguments = json.loads(raw_arguments) if isinstance(raw_arguments, str) else raw_arguments
    except Exception:
        return {
            "case_id": case.id,
            "tool": tool.name,
            "outcome": "arguments_invalid_json",
        }
    if not isinstance(arguments, dict):
        return {
            "case_id": case.id,
            "tool": tool.name,
            "outcome": "arguments_non_object",
        }
    errors = sorted(Draft202012Validator(tool.parameters).iter_errors(arguments), key=str)
    if errors:
        return {
            "case_id": case.id,
            "tool": tool.name,
            "outcome": "arguments_schema_invalid",
            "validation_errors": [
                {
                    "path": [str(item) for item in error.absolute_path],
                    "validator": str(error.validator or "unknown"),
                }
                for error in errors[:5]
            ],
        }
    return {
        "case_id": case.id,
        "tool": tool.name,
        "outcome": "valid",
    }


def _case_messages(case: ToolCallBenchCase) -> list[Message]:
    return [
        Message(role="system", content=_SYSTEM_PROMPT),
        Message(role="user", content=case.prompt),
    ]


def _request_input_token_ceiling(messages: list[Message], tools: list[dict[str, Any]]) -> int:
    payload = {
        "messages": [message.to_dict() for message in messages],
        "tools": tools,
    }
    serialized = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    structural_overhead = 512 + 32 * (len(messages) + len(tools))
    return max(1, len(serialized) + structural_overhead)


def _usage_example_schema_chars(tool: ToolSpec) -> int:
    if not tool.usage_examples:
        return 0
    with_examples = len(
        json.dumps(tool.to_openai_tool(), ensure_ascii=False, separators=(",", ":"))
    )
    description_without_examples = tool.to_openai_tool()
    description_without_examples["function"]["description"] = tool.description
    without_examples = len(
        json.dumps(description_without_examples, ensure_ascii=False, separators=(",", ":"))
    )
    return max(0, with_examples - without_examples)


def _parse_usage(value: Any) -> dict[str, int] | None:
    if not isinstance(value, dict):
        return None
    parsed: dict[str, int] = {}
    for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
        item = value.get(key)
        if isinstance(item, bool) or not isinstance(item, int | float):
            return None
        if item < 0 or int(item) != item:
            return None
        parsed[key] = int(item)
    if parsed["total_tokens"] <= 0:
        return None
    if parsed["total_tokens"] < parsed["prompt_tokens"] + parsed["completion_tokens"]:
        return None
    return parsed


def _estimate_cost_microusd(
    *,
    prompt_tokens: int,
    completion_tokens: int,
    budget: ToolCallBenchBudget,
) -> int:
    numerator = (
        prompt_tokens * budget.input_rate_microusd + completion_tokens * budget.output_rate_microusd
    )
    return _ceil_div(numerator, 1_000_000)


def _isolated_provider_settings(source_config: OctopalConfig) -> Settings:
    settings = Settings(
        _env_file=None,
        LITELLM_NUM_RETRIES=0,
        LITELLM_FALLBACKS=None,
        LITELLM_CACHING=False,
        LITELLM_MAX_CONCURRENCY=1,
        LITELLM_RATE_LIMIT_MAX_RETRIES=0,
        OCTOPAL_DEBUG_PROMPTS=False,
        OCTOPAL_OBSERVABILITY_ENABLED=False,
    )
    settings.config_obj = OctopalConfig(llm=source_config.llm.model_copy(deep=True))
    return settings


def _build_provider(source_config: OctopalConfig) -> InferenceProvider:
    settings = _isolated_provider_settings(source_config)
    return LiteLLMProvider(settings, config=source_config.llm)


def _usd_to_microusd(value: Decimal) -> int:
    try:
        converted = (value * Decimal(1_000_000)).to_integral_value(rounding=ROUND_CEILING)
    except (InvalidOperation, OverflowError, ValueError) as exc:
        raise ValueError("invalid USD amount") from exc
    return int(converted)


def _ceil_div(numerator: int, denominator: int) -> int:
    if numerator <= 0:
        return 0
    return (numerator + denominator - 1) // denominator


def _read_json_object(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise ValueError(f"JSON file does not exist: {resolved}")
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON file must contain an object: {resolved}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    resolved = path.expanduser().resolve()
    _ensure_output_available(resolved)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _ensure_output_available(path: Path) -> None:
    if path.exists():
        raise ValueError(f"output file already exists: {path}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Measure tool-call argument quality without executing tools."
    )
    parser.add_argument("--suite", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--preflight-only", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.out is not None:
        _ensure_output_available(args.out.expanduser().resolve())
    suite = load_tool_call_bench_suite(args.suite)
    source_config = load_tool_call_bench_config(args.config)
    result = asyncio.run(
        run_tool_call_bench(
            suite=suite,
            source_config=source_config,
            preflight_only=bool(args.preflight_only),
        )
    )
    if args.out is not None:
        _write_json(args.out, result)
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    if result.get("status") == "stopped":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
