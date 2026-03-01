from __future__ import annotations

import asyncio
import time
from pathlib import Path

from broodmind.tools.registry import ToolSpec
from broodmind.worker_sdk.worker import Worker
from broodmind.workers.agent_worker import (
    _auto_tune_max_steps,
    _classify_tool_error,
    _detect_tool_loop,
    _execute_tool,
    _extract_mcp_identity,
    _hash_tool_call,
    _hash_tool_outcome,
    _parse_tool_arguments,
    _resolve_tool_loop_thresholds,
    _tool_no_progress_streak,
)
from broodmind.workers.contracts import WorkerSpec
from broodmind.workers.runtime import _call_mcp_with_name_fallback, _extract_mcp_tool_identity


def _dummy_worker() -> Worker:
    spec = WorkerSpec(
        id="w1",
        task="t",
        inputs={},
        system_prompt="s",
        available_tools=[],
        mcp_tools=[],
        model=None,
        granted_capabilities=[],
        timeout_seconds=60,
        max_thinking_steps=5,
        run_id="r1",
        lifecycle="ephemeral",
        correlation_id=None,
    )
    return Worker(spec=spec)


def test_parse_tool_arguments_is_defensive() -> None:
    assert _parse_tool_arguments({"a": 1}) == {"a": 1}
    assert _parse_tool_arguments('{"a": 1}') == {"a": 1}
    assert _parse_tool_arguments("[1,2]") == {"_arg": [1, 2]}
    assert _parse_tool_arguments("{bad}") == {"_raw": "{bad}"}
    assert _parse_tool_arguments(None) == {}


def test_extract_mcp_identity_prefers_explicit_metadata() -> None:
    data = {
        "name": "mcp_demo_tool",
        "server_id": "demo_server",
        "remote_tool_name": "query_docs",
    }
    assert _extract_mcp_identity(data) == ("demo_server", "query_docs")


def test_extract_mcp_tool_identity_uses_longest_server_prefix() -> None:
    server_id, remote_name = _extract_mcp_tool_identity(
        "mcp_demo_server_query_docs",
        ["demo", "demo-server"],
    )
    assert server_id == "demo-server"
    assert remote_name == "query_docs"


def test_call_mcp_with_name_fallback_retries_not_found_variant() -> None:
    class FakeSession:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def call_tool(self, tool_name: str, arguments: dict) -> dict:
            self.calls.append(tool_name)
            if tool_name == "list_threads":
                raise RuntimeError("Tool list_threads not found")
            if tool_name == "list-threads":
                return {"ok": True}
            raise RuntimeError("unexpected")

    async def scenario() -> tuple[dict, list[str]]:
        session = FakeSession()
        result = await _call_mcp_with_name_fallback(session, "list_threads", {})
        return result, session.calls

    result, calls = asyncio.run(scenario())
    assert result["ok"] is True
    assert calls == ["list_threads", "list-threads"]


def test_execute_tool_sync_handler_does_not_block_event_loop() -> None:
    worker = _dummy_worker()

    def slow_sync_handler(args, ctx):
        time.sleep(0.2)
        return {"ok": True, "args": args}

    tool = ToolSpec(
        name="slow_sync",
        description="slow",
        parameters={"type": "object"},
        permission="filesystem_read",
        handler=slow_sync_handler,
        is_async=False,
    )

    async def scenario() -> tuple[float, dict, dict]:
        start = time.perf_counter()
        task = asyncio.create_task(
            _execute_tool(
                "slow_sync",
                {"x": 1},
                Path("."),
                worker,
                {"slow_sync": tool},
                timeout_seconds=2,
            )
        )
        await asyncio.sleep(0.05)
        mid = time.perf_counter() - start
        result, meta = await task
        return mid, result, meta

    mid_elapsed, result, meta = asyncio.run(scenario())
    assert mid_elapsed < 0.15
    assert result["ok"] is True
    assert meta["had_error"] is False


def test_execute_tool_timeout_returns_error() -> None:
    worker = _dummy_worker()

    async def slow_async_handler(args, ctx):
        await asyncio.sleep(1.2)
        return {"ok": True}

    tool = ToolSpec(
        name="slow_async",
        description="slow",
        parameters={"type": "object"},
        permission="filesystem_read",
        handler=slow_async_handler,
        is_async=True,
    )

    async def scenario():
        return await _execute_tool(
            "slow_async",
            {},
            Path("."),
            worker,
            {"slow_async": tool},
            timeout_seconds=0,
        )

    # timeout_seconds=0 means no timeout (backward compatible)
    result, _meta = asyncio.run(scenario())
    assert result["ok"] is True

    async def scenario_timeout():
        return await _execute_tool(
            "slow_async",
            {},
            Path("."),
            worker,
            {"slow_async": tool},
            timeout_seconds=1,
        )

    timeout_result, timeout_meta = asyncio.run(scenario_timeout())
    assert "error" in timeout_result
    assert "timed out" in timeout_result["error"].lower()
    assert timeout_meta["timed_out"] is True
    assert timeout_meta["retries"] >= 1


def test_tool_error_classification() -> None:
    assert _classify_tool_error("connection timeout while fetching") == "transient"
    assert _classify_tool_error("permission denied by policy") == "permanent"


def test_auto_tune_max_steps_increases_for_web_and_mcp() -> None:
    tuned = _auto_tune_max_steps(8, ["web_search", "mcp_demo_read"], "Research worker")
    assert tuned > 8


def test_tool_call_hash_is_stable_for_key_order() -> None:
    h1 = _hash_tool_call("process", {"action": "poll", "id": 1})
    h2 = _hash_tool_call("process", {"id": 1, "action": "poll"})
    assert h1 == h2


def test_tool_no_progress_streak_counts_same_outcome() -> None:
    history = [
        {"tool_name": "process", "args_hash": "a", "result_hash": "x"},
        {"tool_name": "process", "args_hash": "a", "result_hash": "x"},
        {"tool_name": "process", "args_hash": "a", "result_hash": "x"},
    ]
    count, latest = _tool_no_progress_streak(history, tool_name="process", args_hash="a")
    assert count == 3
    assert latest == "x"


def test_detect_tool_loop_warning_and_critical_thresholds() -> None:
    history_warning = [
        {"tool_name": "process", "args_hash": "a", "result_hash": "x"} for _ in range(8)
    ]
    warning = _detect_tool_loop(history_warning, tool_name="process", args_hash="a")
    assert warning is not None
    assert warning["level"] == "warning"

    history_critical = [
        {"tool_name": "process", "args_hash": "a", "result_hash": "x"} for _ in range(12)
    ]
    critical = _detect_tool_loop(history_critical, tool_name="process", args_hash="a")
    assert critical is not None
    assert critical["level"] == "critical"


def test_detect_tool_loop_global_circuit_breaker() -> None:
    history = [{"tool_name": "any", "args_hash": str(i), "result_hash": str(i)} for i in range(30)]
    state = _detect_tool_loop(history, tool_name="any", args_hash="29")
    assert state is not None
    assert state["detector"] == "global_circuit_breaker"


def test_tool_outcome_hash_changes_on_error_state() -> None:
    ok_hash = _hash_tool_outcome({"status": "ok"}, {"had_error": False, "timed_out": False})
    err_hash = _hash_tool_outcome({"status": "ok"}, {"had_error": True, "timed_out": False})
    assert ok_hash != err_hash


def test_resolve_tool_loop_thresholds_from_env(monkeypatch) -> None:
    monkeypatch.setenv("BROODMIND_TOOL_LOOP_WARNING_THRESHOLD", "5")
    monkeypatch.setenv("BROODMIND_TOOL_LOOP_CRITICAL_THRESHOLD", "9")
    monkeypatch.setenv("BROODMIND_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD", "20")
    thresholds = _resolve_tool_loop_thresholds()
    assert thresholds == {"warning": 5, "critical": 9, "global_breaker": 20}


def test_resolve_tool_loop_thresholds_normalizes_invalid_order(monkeypatch) -> None:
    monkeypatch.setenv("BROODMIND_TOOL_LOOP_WARNING_THRESHOLD", "10")
    monkeypatch.setenv("BROODMIND_TOOL_LOOP_CRITICAL_THRESHOLD", "10")
    monkeypatch.setenv("BROODMIND_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD", "1")
    thresholds = _resolve_tool_loop_thresholds()
    assert thresholds["warning"] == 10
    assert thresholds["critical"] == 11
    assert thresholds["global_breaker"] == 12


def test_resolve_tool_loop_thresholds_ignores_bad_values(monkeypatch) -> None:
    monkeypatch.setenv("BROODMIND_TOOL_LOOP_WARNING_THRESHOLD", "oops")
    monkeypatch.setenv("BROODMIND_TOOL_LOOP_CRITICAL_THRESHOLD", "0")
    monkeypatch.setenv("BROODMIND_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD", "-3")
    thresholds = _resolve_tool_loop_thresholds()
    assert thresholds["warning"] >= 1
    assert thresholds["critical"] > thresholds["warning"]
    assert thresholds["global_breaker"] > thresholds["critical"]
