from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Iterator
from typing import Any

import pytest

import octopal.tools.web.search as search_mod
from octopal.tools.catalog import get_tools
from octopal.tools.metadata import ProgrammaticReadContract, ToolMetadata
from octopal.tools.programmatic_execution import (
    ProgrammaticReadBatchError,
    ProgrammaticReadBatchLimits,
    ProgrammaticReadCall,
    execute_programmatic_read_batch,
)
from octopal.tools.registry import ToolSpec


def _tool(
    name: str,
    handler: Callable[[dict[str, Any], dict[str, Any]], Any],
    *,
    max_parallel_calls: int = 2,
) -> ToolSpec:
    return ToolSpec(
        name=name,
        description="Read a value",
        parameters={
            "type": "object",
            "properties": {
                "value": {"type": "integer"},
                "delay": {"type": "number", "minimum": 0},
            },
            "required": ["value"],
            "additionalProperties": False,
        },
        permission="network",
        handler=handler,
        is_async=True,
        metadata=ToolMetadata(
            category="web",
            read_only=True,
            programmatic_read=ProgrammaticReadContract(
                idempotent=True,
                max_parallel_calls=max_parallel_calls,
                result_shape="json_object",
                max_result_bytes=1_024,
            ),
        ),
    )


def _call(call_id: str, tool_name: str, value: int) -> ProgrammaticReadCall:
    return ProgrammaticReadCall(
        call_id=call_id,
        tool_name=tool_name,
        arguments={"value": value},
    )


def test_batch_limits_reject_unbounded_values() -> None:
    with pytest.raises(ValueError, match="max_calls"):
        ProgrammaticReadBatchLimits(max_calls=17)
    with pytest.raises(ValueError, match="max_parallel_calls"):
        ProgrammaticReadBatchLimits(max_parallel_calls=9)
    with pytest.raises(ValueError, match="call_timeout_seconds"):
        ProgrammaticReadBatchLimits(call_timeout_seconds=61)
    with pytest.raises(ValueError, match="max_argument_bytes"):
        ProgrammaticReadBatchLimits(max_argument_bytes=256_001)
    with pytest.raises(ValueError, match="max_parallel_calls"):
        ProgrammaticReadBatchLimits(max_parallel_calls=1.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="call_timeout_seconds"):
        ProgrammaticReadBatchLimits(call_timeout_seconds=True)


@pytest.mark.asyncio
async def test_batch_preflight_is_atomic_when_later_arguments_are_invalid() -> None:
    invocations = 0

    async def handler(args: dict[str, Any], _ctx: dict[str, Any]) -> str:
        nonlocal invocations
        invocations += 1
        return json.dumps({"value": args["value"]})

    tool = _tool("lookup", handler)
    calls = [
        _call("first", "lookup", 1),
        ProgrammaticReadCall(
            call_id="second",
            tool_name="lookup",
            arguments={"value": "secret-invalid-value"},
        ),
    ]

    with pytest.raises(ProgrammaticReadBatchError) as exc_info:
        await execute_programmatic_read_batch([tool], calls)

    assert exc_info.value.code == "arguments_invalid"
    assert exc_info.value.details == ("call_id=second", "tool_name=lookup")
    assert "secret-invalid-value" not in str(exc_info.value)
    assert invocations == 0


@pytest.mark.asyncio
async def test_batch_rejects_too_many_calls_before_execution() -> None:
    invocations = 0

    async def handler(args: dict[str, Any], _ctx: dict[str, Any]) -> str:
        nonlocal invocations
        invocations += 1
        return json.dumps({"value": args["value"]})

    tool = _tool("lookup", handler)
    calls = [_call(f"call-{index}", "lookup", index) for index in range(3)]

    with pytest.raises(ProgrammaticReadBatchError) as exc_info:
        await execute_programmatic_read_batch(
            [tool], calls, limits=ProgrammaticReadBatchLimits(max_calls=2)
        )

    assert exc_info.value.code == "batch_call_limit_exceeded"
    assert invocations == 0


@pytest.mark.asyncio
async def test_batch_reads_at_most_one_call_past_the_limit() -> None:
    yielded = 0

    async def handler(args: dict[str, Any], _ctx: dict[str, Any]) -> str:
        return json.dumps({"value": args["value"]})

    def calls() -> Iterator[ProgrammaticReadCall]:
        nonlocal yielded
        for index in range(100):
            yielded += 1
            yield _call(f"call-{index}", "lookup", index)

    with pytest.raises(ProgrammaticReadBatchError) as exc_info:
        await execute_programmatic_read_batch(
            [_tool("lookup", handler)],
            calls(),
            limits=ProgrammaticReadBatchLimits(max_calls=2),
        )

    assert exc_info.value.code == "batch_call_limit_exceeded"
    assert yielded == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("arguments", "code"),
    [
        ({"value": float("nan")}, "arguments_not_json"),
        ({"value": 1, "extra": "x" * 100}, "arguments_too_large"),
    ],
)
async def test_batch_rejects_non_json_or_oversized_arguments(
    arguments: dict[str, Any], code: str
) -> None:
    async def handler(_args: dict[str, Any], _ctx: dict[str, Any]) -> str:
        return '{"value":1}'

    tool = _tool("lookup", handler)
    limits = ProgrammaticReadBatchLimits(max_argument_bytes=32)

    with pytest.raises(ProgrammaticReadBatchError) as exc_info:
        await execute_programmatic_read_batch(
            [tool],
            [ProgrammaticReadCall("call-1", "lookup", arguments)],
            limits=limits,
        )

    assert exc_info.value.code == code


@pytest.mark.asyncio
async def test_batch_enforces_global_and_per_tool_parallelism_and_preserves_order() -> None:
    active_total = 0
    max_active_total = 0
    active_by_tool = {"alpha": 0, "beta": 0}
    max_active_by_tool = {"alpha": 0, "beta": 0}

    def handler_for(tool_name: str) -> Callable[[dict[str, Any], dict[str, Any]], Any]:
        async def handler(args: dict[str, Any], _ctx: dict[str, Any]) -> str:
            nonlocal active_total, max_active_total
            active_total += 1
            active_by_tool[tool_name] += 1
            max_active_total = max(max_active_total, active_total)
            max_active_by_tool[tool_name] = max(
                max_active_by_tool[tool_name], active_by_tool[tool_name]
            )
            try:
                await asyncio.sleep(0.02)
                return json.dumps({"value": args["value"]})
            finally:
                active_by_tool[tool_name] -= 1
                active_total -= 1

        return handler

    tools = [
        _tool("alpha", handler_for("alpha"), max_parallel_calls=2),
        _tool("beta", handler_for("beta"), max_parallel_calls=2),
    ]
    calls = [
        _call("a-1", "alpha", 1),
        _call("a-2", "alpha", 2),
        _call("a-3", "alpha", 3),
        _call("b-1", "beta", 4),
        _call("b-2", "beta", 5),
        _call("b-3", "beta", 6),
    ]

    result = await execute_programmatic_read_batch(
        tools,
        calls,
        limits=ProgrammaticReadBatchLimits(
            max_calls=6,
            max_parallel_calls=3,
            call_timeout_seconds=1,
        ),
    )

    assert result.completed_count == 6
    assert result.failed_count == 0
    assert result.timed_out_count == 0
    assert [item.call_id for item in result.results] == [call.call_id for call in calls]
    assert [item.value for item in result.results] == [
        {"value": 1},
        {"value": 2},
        {"value": 3},
        {"value": 4},
        {"value": 5},
        {"value": 6},
    ]
    assert max_active_total == 3
    assert max_active_by_tool == {"alpha": 2, "beta": 2}


@pytest.mark.asyncio
async def test_batch_times_out_once_without_retrying() -> None:
    invocations = 0
    cancelled = asyncio.Event()

    async def handler(_args: dict[str, Any], _ctx: dict[str, Any]) -> str:
        nonlocal invocations
        invocations += 1
        try:
            await asyncio.sleep(1)
        finally:
            cancelled.set()
        return '{"value":1}'

    result = await execute_programmatic_read_batch(
        [_tool("lookup", handler)],
        [_call("call-1", "lookup", 1)],
        limits=ProgrammaticReadBatchLimits(call_timeout_seconds=0.01),
    )

    assert result.completed_count == 0
    assert result.timed_out_count == 1
    assert result.results[0].error_code == "call_timed_out"
    assert invocations == 1
    assert cancelled.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["result_invalid_json", "handler_error"])
async def test_batch_failure_results_do_not_expose_handler_or_result_payload(
    mode: str,
) -> None:
    async def handler(_args: dict[str, Any], _ctx: dict[str, Any]) -> str:
        if mode == "handler_error":
            raise RuntimeError("secret-handler-error")
        return "secret-invalid-json"

    result = await execute_programmatic_read_batch(
        [_tool("lookup", handler)],
        [_call("call-1", "lookup", 1)],
    )

    call_result = result.results[0]
    assert call_result.status == "failed"
    assert call_result.error_code == mode
    assert "secret" not in repr(call_result)


@pytest.mark.asyncio
async def test_batch_rejects_sync_handler_before_execution() -> None:
    invocations = 0

    def handler(_args: dict[str, Any], _ctx: dict[str, Any]) -> str:
        nonlocal invocations
        invocations += 1
        return '{"value":1}'

    with pytest.raises(ProgrammaticReadBatchError) as exc_info:
        await execute_programmatic_read_batch(
            [_tool("lookup", handler)],
            [_call("call-1", "lookup", 7)],
        )

    assert exc_info.value.code == "handler_not_cancellable"
    assert invocations == 0


@pytest.mark.asyncio
async def test_catalog_web_search_runs_through_cancellable_batch_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_search(args: dict[str, Any]) -> dict[str, Any]:
        return {
            "ok": True,
            "source": "brave_search",
            "provider": "brave",
            "query": args["query"],
            "count": 1,
            "results": [{"title": "Octopal"}],
        }

    monkeypatch.setattr(search_mod, "run_search_async", fake_search)
    web_search = next(tool for tool in get_tools(mcp_manager=None) if tool.name == "web_search")

    result = await execute_programmatic_read_batch(
        [web_search],
        [
            ProgrammaticReadCall(
                call_id="search-1",
                tool_name="web_search",
                arguments={"query": "Octopal"},
            )
        ],
        limits=ProgrammaticReadBatchLimits(call_timeout_seconds=1),
    )

    assert result.completed_count == 1
    assert isinstance(result.results[0].value, dict)
    assert result.results[0].value["results"] == [{"title": "Octopal"}]


@pytest.mark.asyncio
async def test_catalog_web_search_timeout_cancels_async_provider_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancelled = asyncio.Event()

    async def slow_search(_args: dict[str, Any]) -> dict[str, Any]:
        try:
            await asyncio.sleep(1)
        finally:
            cancelled.set()
        return {"ok": True, "results": []}

    monkeypatch.setattr(search_mod, "run_search_async", slow_search)
    web_search = next(tool for tool in get_tools(mcp_manager=None) if tool.name == "web_search")

    result = await execute_programmatic_read_batch(
        [web_search],
        [ProgrammaticReadCall("search-1", "web_search", {"query": "Octopal"})],
        limits=ProgrammaticReadBatchLimits(call_timeout_seconds=0.01),
    )

    assert result.timed_out_count == 1
    assert result.results[0].error_code == "call_timed_out"
    assert cancelled.is_set()
