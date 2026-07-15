from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest
from pydantic import ValidationError

from octopal.infrastructure.config.models import LLMConfig, OctopalConfig
from octopal.runtime.tool_call_bench import (
    ToolCallBenchBudget,
    ToolCallBenchCase,
    ToolCallBenchSuite,
    _write_json,
    load_tool_call_bench_config,
    main,
    preflight_tool_call_bench,
    run_tool_call_bench,
)


def _config() -> OctopalConfig:
    return OctopalConfig(
        llm=LLMConfig(
            provider_id="minimax",
            model="MiniMax-M3",
            api_key="test-key",
            api_base="https://api.minimax.io/v1",
        )
    )


def _budget(*, max_calls: int = 1) -> ToolCallBenchBudget:
    return ToolCallBenchBudget(
        pricing_model="minimax/MiniMax-M3",
        max_calls=max_calls,
        max_total_tokens=20_000,
        max_cost_usd=Decimal("0.03"),
        input_cost_per_million_tokens_usd=Decimal("0.6"),
        output_cost_per_million_tokens_usd=Decimal("2.4"),
        max_output_tokens=128,
    )


def _suite(*, cases: tuple[ToolCallBenchCase, ...] | None = None) -> ToolCallBenchSuite:
    selected = cases or (
        ToolCallBenchCase(
            id="read_report",
            tool="fs_read",
            prompt="Read the report at PRIVATE_REPORT_MARKER and call the tool once.",
        ),
    )
    return ToolCallBenchSuite(
        provider_id="minimax",
        model="MiniMax-M3",
        variant="baseline",
        live_budget=_budget(max_calls=len(selected)),
        cases=selected,
    )


class _FakeProvider:
    provider_id = "minimax"
    model_id = "minimax/MiniMax-M3"

    def __init__(self, responses: list[dict]) -> None:
        self.responses = list(responses)
        self.calls = 0

    async def complete(self, messages, **kwargs):
        raise AssertionError("plain completion is not allowed")

    async def complete_stream(self, messages, *, on_partial, **kwargs):
        raise AssertionError("stream completion is not allowed")

    async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
        self.calls += 1
        assert len(tools) == 1
        assert tool_choice == "required"
        assert kwargs["strict_single_attempt"] is True
        return self.responses.pop(0)


def _response(*, tool: str = "fs_read", arguments: object, usage: dict | None = None) -> dict:
    return {
        "content": "ignored raw model content",
        "tool_calls": [
            {
                "id": "call-1",
                "type": "function",
                "function": {"name": tool, "arguments": arguments},
            }
        ],
        "usage": usage or {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120},
    }


def test_preflight_is_budgeted_and_retains_no_prompt_content() -> None:
    preflight = preflight_tool_call_bench(suite=_suite(), source_config=_config())

    assert preflight.passed is True
    assert preflight.report["budget"]["worst_case_tokens"] <= 20_000
    assert preflight.report["budget"]["worst_case_cost_microusd"] <= 30_000
    assert preflight.report["safety"]["tools_execute"] is False
    assert "PRIVATE_REPORT_MARKER" not in json.dumps(preflight.report)


def test_run_grades_valid_call_without_retaining_arguments() -> None:
    provider = _FakeProvider([_response(arguments='{"path":"PRIVATE_REPORT_MARKER"}')])

    result = asyncio.run(
        run_tool_call_bench(
            suite=_suite(),
            source_config=_config(),
            provider_factory=lambda _config: provider,
        )
    )

    assert result["status"] == "completed"
    assert result["metrics"]["valid_call_count"] == 1
    assert result["metrics"]["malformed_argument_rate"] == 0.0
    assert result["metrics"]["usage_accounting_complete"] is True
    assert result["results"][0]["outcome"] == "valid"
    assert "PRIVATE_REPORT_MARKER" not in json.dumps(result)


def test_run_grades_schema_error_without_retaining_invalid_value() -> None:
    provider = _FakeProvider([_response(arguments='{"wrong":"PRIVATE_INVALID_VALUE"}')])

    result = asyncio.run(
        run_tool_call_bench(
            suite=_suite(),
            source_config=_config(),
            provider_factory=lambda _config: provider,
        )
    )

    case_result = result["results"][0]
    assert case_result["outcome"] == "arguments_schema_invalid"
    assert {item["validator"] for item in case_result["validation_errors"]} == {
        "required",
        "additionalProperties",
    }
    assert result["metrics"]["malformed_argument_rate"] == 1.0
    assert "PRIVATE_INVALID_VALUE" not in json.dumps(result)


def test_missing_tool_call_at_completion_cap_is_censored() -> None:
    provider = _FakeProvider(
        [
            {
                "content": "unfinished reasoning",
                "tool_calls": [],
                "usage": {
                    "prompt_tokens": 100,
                    "completion_tokens": 128,
                    "total_tokens": 228,
                },
            }
        ]
    )

    result = asyncio.run(
        run_tool_call_bench(
            suite=_suite(),
            source_config=_config(),
            provider_factory=lambda _config: provider,
        )
    )

    assert result["results"][0]["outcome"] == "output_budget_exhausted"
    assert result["metrics"]["malformed_argument_rate"] is None


def test_missing_usage_stops_before_next_provider_call() -> None:
    cases = (
        ToolCallBenchCase(id="first", tool="fs_read", prompt="Read reports/one.md."),
        ToolCallBenchCase(id="second", tool="fs_read", prompt="Read reports/two.md."),
    )
    response = _response(arguments='{"path":"reports/one.md"}')
    response["usage"] = {}
    provider = _FakeProvider([response])

    result = asyncio.run(
        run_tool_call_bench(
            suite=_suite(cases=cases),
            source_config=_config(),
            provider_factory=lambda _config: provider,
        )
    )

    assert result["status"] == "stopped"
    assert result["stop_reason"] == "usage_missing"
    assert result["attempted_cases"] == 1
    assert provider.calls == 1


def test_preflight_only_never_builds_provider() -> None:
    result = asyncio.run(
        run_tool_call_bench(
            suite=_suite(),
            source_config=_config(),
            preflight_only=True,
            provider_factory=lambda _config: (_ for _ in ()).throw(
                AssertionError("provider must not be built")
            ),
        )
    )

    assert result["status"] == "preflight_ok"


def test_suite_requires_exact_call_budget() -> None:
    with pytest.raises(ValidationError, match="max_calls must equal"):
        ToolCallBenchSuite(
            provider_id="minimax",
            model="MiniMax-M3",
            variant="baseline",
            live_budget=_budget(max_calls=2),
            cases=(ToolCallBenchCase(id="one", tool="fs_read", prompt="Read one file."),),
        )


@pytest.mark.parametrize("max_cost", [Decimal("Infinity"), Decimal("0.031")])
def test_budget_rejects_non_finite_or_excessive_cost(max_cost: Decimal) -> None:
    with pytest.raises(ValidationError):
        ToolCallBenchBudget(
            pricing_model="minimax/MiniMax-M3",
            max_calls=1,
            max_total_tokens=20_000,
            max_cost_usd=max_cost,
            input_cost_per_million_tokens_usd=Decimal("0.6"),
            output_cost_per_million_tokens_usd=Decimal("2.4"),
            max_output_tokens=128,
        )


def test_result_writer_refuses_to_overwrite_artifact(tmp_path) -> None:
    path = tmp_path / "result.json"
    _write_json(path, {"status": "first"})

    with pytest.raises(ValueError, match="already exists"):
        _write_json(path, {"status": "second"})

    assert json.loads(path.read_text(encoding="utf-8")) == {"status": "first"}


def test_cli_rejects_existing_output_before_loading_inputs(tmp_path) -> None:
    output = tmp_path / "existing.json"
    output.write_text("{}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="already exists"):
        main(
            [
                "--suite",
                str(tmp_path / "missing-suite.json"),
                "--config",
                str(tmp_path / "missing-config.json"),
                "--out",
                str(output),
            ]
        )


def test_invalid_config_error_retains_no_secret_content(tmp_path) -> None:
    path = tmp_path / "invalid-config.json"
    path.write_text(
        json.dumps({"llm": {"provider_id": [], "api_key": "PRIVATE_CONFIG_SECRET"}}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc_info:
        load_tool_call_bench_config(path)

    assert str(exc_info.value) == "invalid tool-call bench config"
    assert "PRIVATE_CONFIG_SECRET" not in str(exc_info.value)
