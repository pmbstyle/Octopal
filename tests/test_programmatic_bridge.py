from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import ValidationError

from octopal.runtime.workers.contracts import (
    WorkerInferenceBudget,
    WorkerSpec,
)
from octopal.runtime.workers.programmatic_bridge import (
    handle_programmatic_read_bridge_request,
    safe_programmatic_read_request_id,
)
from octopal.tools.metadata import ProgrammaticReadContract, ToolMetadata
from octopal.tools.registry import ToolSpec


def _spec(
    *,
    available_tools: list[str] | None = None,
    effective_permissions: list[str] | None = None,
    call_budget: int = 2,
    inference_budget: WorkerInferenceBudget | None = None,
    strict_thinking_budget: bool = False,
) -> WorkerSpec:
    return WorkerSpec(
        id="worker-1",
        template_id="research",
        task="Research",
        inputs={},
        system_prompt="Research carefully",
        available_tools=available_tools if available_tools is not None else ["lookup"],
        granted_capabilities=[],
        timeout_seconds=30,
        max_thinking_steps=3,
        effective_permissions=(
            effective_permissions if effective_permissions is not None else ["network"]
        ),
        programmatic_read_call_budget=call_budget,
        inference_budget=inference_budget,
        strict_thinking_budget=strict_thinking_budget,
    )


def _tool(
    handler: Any,
    *,
    name: str = "lookup",
    permission: str = "network",
    metadata: ToolMetadata | None = None,
) -> ToolSpec:
    return ToolSpec(
        name=name,
        description="Lookup",
        parameters={
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": False,
        },
        permission=permission,
        handler=handler,
        is_async=True,
        metadata=metadata
        or ToolMetadata(
            category="web",
            read_only=True,
            programmatic_read=ProgrammaticReadContract(
                idempotent=True,
                max_parallel_calls=2,
                result_shape="json_object",
                max_result_bytes=4_096,
            ),
        ),
    )


def _payload(*, tool_name: str = "lookup", count: int = 1) -> dict[str, Any]:
    return {
        "type": "programmatic_read_batch",
        "request_id": "request-1",
        "calls": [
            {
                "call_id": f"call-{index}",
                "tool_name": tool_name,
                "arguments": {"query": f"query-{index}"},
            }
            for index in range(count)
        ],
    }


def test_worker_spec_disables_programmatic_bridge_by_default() -> None:
    spec = _spec(call_budget=0)

    assert spec.programmatic_read_call_budget == 0


def test_request_id_is_echoed_only_when_protocol_safe() -> None:
    assert safe_programmatic_read_request_id({"request_id": "prb-123"}) == "prb-123"
    assert safe_programmatic_read_request_id({"request_id": "secret\nnext-line"}) == ""
    assert safe_programmatic_read_request_id({"request_id": 123}) == ""


@pytest.mark.asyncio
async def test_bridge_stays_disabled_when_runtime_remaining_budget_is_larger() -> None:
    invocations = 0

    async def handler(_args: dict[str, Any], _ctx: dict[str, Any]) -> str:
        nonlocal invocations
        invocations += 1
        return "{}"

    outcome = await handle_programmatic_read_bridge_request(
        spec=_spec(call_budget=0),
        payload=_payload(),
        remaining_calls=99,
        tools=[_tool(handler)],
    )

    assert outcome.response["error"]["code"] == "call_budget_exhausted"
    assert outcome.response["remaining_calls"] == 0
    assert outcome.consumed_calls == 0
    assert invocations == 0


def test_budgeted_worker_rejects_programmatic_budget_above_tool_budget() -> None:
    inference_budget = WorkerInferenceBudget(
        pricing_model="minimax/MiniMax-M3",
        max_llm_calls=2,
        max_tool_calls=1,
        max_total_tokens=10_000,
        max_cost_microusd=10_000,
        input_cost_microusd_per_million_tokens=300_000,
        completion_cost_microusd_per_million_tokens=1_200_000,
    )

    with pytest.raises(ValidationError, match="programmatic_read_call_budget"):
        _spec(
            call_budget=2,
            inference_budget=inference_budget,
            strict_thinking_budget=True,
        )


@pytest.mark.asyncio
async def test_bridge_executes_only_inventory_and_permission_bound_tool() -> None:
    invocations = 0

    async def handler(args: dict[str, Any], _ctx: dict[str, Any]) -> str:
        nonlocal invocations
        invocations += 1
        return json.dumps({"query": args["query"]})

    outcome = await handle_programmatic_read_bridge_request(
        spec=_spec(call_budget=2),
        payload=_payload(count=2),
        remaining_calls=99,
        tools=[_tool(handler)],
    )

    assert outcome.consumed_calls == 2
    assert outcome.response["ok"] is True
    assert outcome.response["remaining_calls"] == 0
    assert outcome.response["result"]["completed_count"] == 2
    assert [item["value"] for item in outcome.response["result"]["results"]] == [
        {"query": "query-0"},
        {"query": "query-1"},
    ]
    assert invocations == 2


@pytest.mark.asyncio
async def test_bridge_rejects_tool_outside_worker_inventory_and_consumes_reservation() -> None:
    invocations = 0

    async def handler(_args: dict[str, Any], _ctx: dict[str, Any]) -> str:
        nonlocal invocations
        invocations += 1
        return "{}"

    outcome = await handle_programmatic_read_bridge_request(
        spec=_spec(available_tools=["other"], call_budget=2),
        payload=_payload(),
        remaining_calls=2,
        tools=[_tool(handler)],
    )

    assert outcome.response["ok"] is False
    assert outcome.response["error"]["code"] == "tool_not_in_worker_inventory"
    assert outcome.response["remaining_calls"] == 1
    assert outcome.consumed_calls == 1
    assert invocations == 0


@pytest.mark.asyncio
async def test_bridge_rejects_missing_worker_permission_before_handler() -> None:
    invocations = 0

    async def handler(_args: dict[str, Any], _ctx: dict[str, Any]) -> str:
        nonlocal invocations
        invocations += 1
        return "{}"

    outcome = await handle_programmatic_read_bridge_request(
        spec=_spec(effective_permissions=["filesystem_read"]),
        payload=_payload(),
        remaining_calls=2,
        tools=[_tool(handler)],
    )

    assert outcome.response["error"]["code"] == "tool_permission_not_granted"
    assert invocations == 0


@pytest.mark.asyncio
async def test_bridge_still_applies_programmatic_contract_after_inventory_binding() -> None:
    invocations = 0

    async def handler(_args: dict[str, Any], _ctx: dict[str, Any]) -> str:
        nonlocal invocations
        invocations += 1
        return "{}"

    outcome = await handle_programmatic_read_bridge_request(
        spec=_spec(effective_permissions=["mcp_exec"]),
        payload=_payload(),
        remaining_calls=2,
        tools=[
            _tool(
                handler,
                permission="mcp_exec",
                metadata=ToolMetadata(
                    category="mcp",
                    owner="mcp",
                    read_only=True,
                    programmatic_read=ProgrammaticReadContract(
                        idempotent=True,
                        max_parallel_calls=1,
                        result_shape="json_object",
                        max_result_bytes=1_024,
                    ),
                ),
            )
        ],
    )

    assert outcome.response["ok"] is False
    assert outcome.response["error"]["code"] == "tool_not_eligible"
    assert invocations == 0


@pytest.mark.asyncio
async def test_bridge_exhausts_remaining_budget_before_oversized_batch() -> None:
    invocations = 0

    async def handler(_args: dict[str, Any], _ctx: dict[str, Any]) -> str:
        nonlocal invocations
        invocations += 1
        return "{}"

    outcome = await handle_programmatic_read_bridge_request(
        spec=_spec(call_budget=2),
        payload=_payload(count=2),
        remaining_calls=1,
        tools=[_tool(handler)],
    )

    assert outcome.response["error"]["code"] == "call_budget_exhausted"
    assert outcome.response["remaining_calls"] == 0
    assert outcome.consumed_calls == 1
    assert invocations == 0
