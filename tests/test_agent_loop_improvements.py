from __future__ import annotations

import asyncio
import time
from pathlib import Path

from octopal.runtime.tool_errors import ToolBridgeError
from octopal.runtime.workers.agent_worker import (
    _auto_tune_max_steps,
    _build_inference_unavailable_result,
    _classify_tool_error,
    _detect_orchestration_stall,
    _detect_tool_loop,
    _execute_tool,
    _extract_error_text,
    _extract_mcp_identity,
    _extract_tool_progress_key,
    _hash_tool_call,
    _hash_tool_outcome,
    _is_upstream_unavailable_error,
    _maybe_wait_for_orchestration_poll_window,
    _meaningful_tool_history_size,
    _parse_tool_arguments,
    _resolve_orchestration_poll_throttle_seconds,
    _resolve_tool_loop_thresholds,
    _result_has_error,
    _tool_progress_streak,
    execute_agent_task,
)
from octopal.runtime.workers.contracts import WorkerSpec
from octopal.runtime.workers.runtime import (
    _call_mcp_with_name_fallback,
    _extract_mcp_tool_identity,
    _validate_worker_local_tool_call,
)
from octopal.tools.registry import ToolSpec
from octopal.worker_sdk.worker import Worker


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


def test_plain_tool_error_strings_are_treated_as_errors() -> None:
    result = "run_skill_script error: skill 'job-search' has no scripts directory."
    assert _result_has_error(result) is True
    assert "no scripts directory" in _extract_error_text(result)


def test_execute_tool_preserves_structured_bridge_error_metadata() -> None:
    worker = _dummy_worker()

    async def broken_handler(args, ctx):
        raise ToolBridgeError(
            "schema mismatch",
            bridge="mcp",
            classification="schema_mismatch",
            retryable=False,
            server_id="demo",
            tool_name="get_thread",
        )

    tool = ToolSpec(
        name="mcp_demo_get_thread",
        description="broken",
        parameters={"type": "object"},
        permission="network",
        handler=broken_handler,
        is_async=True,
    )

    async def scenario():
        return await _execute_tool(
            "mcp_demo_get_thread",
            {},
            Path("."),
            worker,
            {"mcp_demo_get_thread": tool},
            timeout_seconds=5,
        )

    result, meta = asyncio.run(scenario())
    assert result["error"] == "schema mismatch"
    assert meta["had_error"] is True
    assert meta["error_type"] == "permanent"
    assert meta["error_bridge"] == "mcp"
    assert meta["error_classification"] == "schema_mismatch"


def test_auto_tune_max_steps_increases_for_web_and_mcp() -> None:
    tuned = _auto_tune_max_steps(8, ["web_search", "mcp_demo_read"], "Research worker")
    assert tuned > 8


def test_tool_call_hash_is_stable_for_key_order() -> None:
    h1 = _hash_tool_call("process", {"action": "poll", "id": 1})
    h2 = _hash_tool_call("process", {"id": 1, "action": "poll"})
    assert h1 == h2


def test_tool_progress_streak_counts_same_progress_key() -> None:
    history = [
        {
            "tool_name": "get_worker_result",
            "args_hash": "a",
            "result_hash": "x",
            "progress_key": None,
        },
        {
            "tool_name": "synthesize_worker_results",
            "args_hash": "b",
            "result_hash": "1",
            "progress_key": "sig-1",
            "observed_at": 100.0,
        },
        {
            "tool_name": "get_worker_result",
            "args_hash": "c",
            "result_hash": "y",
            "progress_key": None,
        },
        {
            "tool_name": "synthesize_worker_results",
            "args_hash": "b",
            "result_hash": "2",
            "progress_key": "sig-1",
            "observed_at": 108.0,
        },
        {
            "tool_name": "synthesize_worker_results",
            "args_hash": "b",
            "result_hash": "3",
            "progress_key": "sig-1",
            "observed_at": 116.0,
        },
    ]
    streak = _tool_progress_streak(
        history,
        tool_name="synthesize_worker_results",
        progress_key="sig-1",
    )
    assert streak["count"] == 3
    assert streak["elapsed_seconds"] == 16.0


def test_extract_tool_progress_key_reads_synthesize_signature() -> None:
    assert (
        _extract_tool_progress_key(
            "synthesize_worker_results",
            {"progress_signature": "sig-1"},
        )
        == "sig-1"
    )
    assert _extract_tool_progress_key("get_worker_result", {"progress_signature": "sig-1"}) is None


def test_extract_tool_progress_key_reads_synthesize_signature_from_json_string() -> None:
    assert (
        _extract_tool_progress_key(
            "synthesize_worker_results",
            '{"status":"pending","progress_signature":"sig-2","pending_count":2}',
        )
        == "sig-2"
    )


def test_extract_tool_progress_key_reads_worker_result_signature_ignoring_runtime_noise() -> None:
    first = _extract_tool_progress_key(
        "get_worker_result",
        {
            "status": "running",
            "worker_id": "w1",
            "updated_at": "2026-04-16T22:02:26Z",
            "runtime_seconds": 3,
            "seconds_since_update": 1,
        },
    )
    second = _extract_tool_progress_key(
        "get_worker_result",
        {
            "status": "running",
            "worker_id": "w1",
            "updated_at": "2026-04-16T22:02:26Z",
            "runtime_seconds": 9,
            "seconds_since_update": 7,
        },
    )
    assert first == second == "w1:running:2026-04-16T22:02:26Z"


def test_upstream_unavailable_error_detects_529_overload() -> None:
    assert _is_upstream_unavailable_error(
        "LiteLLM completion with tools failed: overloaded_error http_code 529 under high load"
    )


def test_meaningful_tool_history_size_dedupes_repeated_worker_polls_without_progress() -> None:
    history = [
        {
            "tool_name": "get_worker_result",
            "args_hash": "worker-a",
            "result_hash": "r1",
            "progress_key": "w1:running:t1",
        },
        {
            "tool_name": "get_worker_result",
            "args_hash": "worker-b",
            "result_hash": "r2",
            "progress_key": "w2:running:t1",
        },
        {
            "tool_name": "get_worker_result",
            "args_hash": "worker-a",
            "result_hash": "r3",
            "progress_key": "w1:running:t1",
        },
        {
            "tool_name": "get_worker_result",
            "args_hash": "worker-b",
            "result_hash": "r4",
            "progress_key": "w2:running:t1",
        },
        {
            "tool_name": "get_worker_result",
            "args_hash": "worker-a",
            "result_hash": "r5",
            "progress_key": "w1:completed:t2",
        },
    ]
    assert _meaningful_tool_history_size(history) == 3


def test_resolve_orchestration_poll_throttle_seconds_from_env(monkeypatch) -> None:
    monkeypatch.setenv("OCTOPAL_ORCHESTRATION_POLL_THROTTLE_SECONDS", "7")
    assert _resolve_orchestration_poll_throttle_seconds() == 7


def test_maybe_wait_for_orchestration_poll_window_throttles_recent_child_poll(
    monkeypatch,
) -> None:
    worker = _dummy_worker()
    sleep_calls: list[float] = []
    log_messages: list[str] = []

    async def _fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    async def _fake_log(level: str, message: str) -> None:
        log_messages.append(f"{level}:{message}")

    monkeypatch.setenv("OCTOPAL_ORCHESTRATION_POLL_THROTTLE_SECONDS", "3")
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.asyncio.sleep", _fake_sleep)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.time.monotonic", lambda: 101.0)
    monkeypatch.setattr(worker, "log", _fake_log)

    args_hash = _hash_tool_call("get_worker_result", {"worker_id": "child-1"})
    outcome = asyncio.run(
        _maybe_wait_for_orchestration_poll_window(
            worker,
            [
                {
                    "tool_name": "get_worker_result",
                    "args_hash": args_hash,
                    "observed_at": 100.0,
                }
            ],
            tool_name="get_worker_result",
            tool_input={"worker_id": "child-1"},
        )
    )

    assert outcome["step_exempt"] is True
    assert outcome["args_hash"] == args_hash
    assert outcome["waited_seconds"] == 2.0
    assert sleep_calls == [2.0]
    assert any("Throttling get_worker_result poll" in message for message in log_messages)


def test_build_inference_unavailable_result_marks_retryable_failure() -> None:
    worker = _dummy_worker()
    result = _build_inference_unavailable_result(
        worker=worker,
        telemetry={"llm_calls": 0},
        error_text="provider overloaded 529",
        thinking_steps=2,
        tools_used=["web_search"],
        partial=True,
    )
    assert result.status == "failed"
    assert "partially completed" in result.summary.lower()
    assert result.output is not None
    assert result.output["retryable"] is True
    assert result.output["reason"] == "inference_upstream_unavailable"


def test_detect_orchestration_stall_warns_and_breaks_on_repeated_no_progress() -> None:
    history = [
        {
            "tool_name": "synthesize_worker_results",
            "args_hash": "same",
            "result_hash": "r1",
            "progress_key": "sig-1",
            "observed_at": 100.0,
        },
        {
            "tool_name": "get_worker_result",
            "args_hash": "worker-1",
            "result_hash": "running",
            "progress_key": None,
        },
        {
            "tool_name": "synthesize_worker_results",
            "args_hash": "same",
            "result_hash": "r2",
            "progress_key": "sig-1",
            "observed_at": 118.0,
        },
    ]
    warning = _detect_orchestration_stall(
        history,
        tool_name="synthesize_worker_results",
        tool_result={
            "pending_count": 2,
            "pending_results": [{"worker_id": "w1", "runtime_seconds": 2}],
        },
        progress_key="sig-1",
    )
    assert warning is not None
    assert warning["level"] == "warning"
    assert warning["elapsed_seconds"] == 18.0

    history.append(
        {
            "tool_name": "synthesize_worker_results",
            "args_hash": "same",
            "result_hash": "r3",
            "progress_key": "sig-1",
            "observed_at": 134.0,
        }
    )
    critical = _detect_orchestration_stall(
        history,
        tool_name="synthesize_worker_results",
        tool_result={
            "pending_count": 2,
            "pending_results": [{"worker_id": "w1", "runtime_seconds": 2}],
        },
        progress_key="sig-1",
    )
    assert critical is not None
    assert critical["level"] == "critical"
    assert critical["elapsed_seconds"] == 34.0


def test_detect_orchestration_stall_handles_json_string_tool_results() -> None:
    history = [
        {
            "tool_name": "synthesize_worker_results",
            "args_hash": "same",
            "result_hash": "r1",
            "progress_key": "sig-json",
            "observed_at": 100.0,
        },
        {
            "tool_name": "synthesize_worker_results",
            "args_hash": "same",
            "result_hash": "r2",
            "progress_key": "sig-json",
            "observed_at": 118.0,
        },
    ]
    warning = _detect_orchestration_stall(
        history,
        tool_name="synthesize_worker_results",
        tool_result='{"status":"pending","pending_count":2,"progress_signature":"sig-json","pending_results":[{"worker_id":"w1","runtime_seconds":2}]}',
        progress_key="sig-json",
    )
    assert warning is not None
    assert warning["level"] == "warning"


def test_detect_orchestration_stall_ignores_short_repeat_windows() -> None:
    history = [
        {
            "tool_name": "synthesize_worker_results",
            "args_hash": "same",
            "result_hash": "r1",
            "progress_key": "sig-fresh",
            "observed_at": 100.0,
        },
        {
            "tool_name": "synthesize_worker_results",
            "args_hash": "same",
            "result_hash": "r2",
            "progress_key": "sig-fresh",
            "observed_at": 103.0,
        },
        {
            "tool_name": "synthesize_worker_results",
            "args_hash": "same",
            "result_hash": "r3",
            "progress_key": "sig-fresh",
            "observed_at": 108.0,
        },
    ]
    state = _detect_orchestration_stall(
        history,
        tool_name="synthesize_worker_results",
        tool_result={
            "pending_count": 2,
            "pending_results": [{"worker_id": "w1", "runtime_seconds": 45}],
        },
        progress_key="sig-fresh",
    )
    assert state is None


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
    monkeypatch.setenv("OCTOPAL_TOOL_LOOP_WARNING_THRESHOLD", "5")
    monkeypatch.setenv("OCTOPAL_TOOL_LOOP_CRITICAL_THRESHOLD", "9")
    monkeypatch.setenv("OCTOPAL_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD", "20")
    thresholds = _resolve_tool_loop_thresholds()
    assert thresholds == {"warning": 5, "critical": 9, "global_breaker": 20}


def test_resolve_tool_loop_thresholds_normalizes_invalid_order(monkeypatch) -> None:
    monkeypatch.setenv("OCTOPAL_TOOL_LOOP_WARNING_THRESHOLD", "10")
    monkeypatch.setenv("OCTOPAL_TOOL_LOOP_CRITICAL_THRESHOLD", "10")
    monkeypatch.setenv("OCTOPAL_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD", "1")
    thresholds = _resolve_tool_loop_thresholds()
    assert thresholds["warning"] == 10
    assert thresholds["critical"] == 11
    assert thresholds["global_breaker"] == 12


def test_resolve_tool_loop_thresholds_ignores_bad_values(monkeypatch) -> None:
    monkeypatch.setenv("OCTOPAL_TOOL_LOOP_WARNING_THRESHOLD", "oops")
    monkeypatch.setenv("OCTOPAL_TOOL_LOOP_CRITICAL_THRESHOLD", "0")
    monkeypatch.setenv("OCTOPAL_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD", "-3")
    thresholds = _resolve_tool_loop_thresholds()
    assert thresholds["warning"] >= 1
    assert thresholds["critical"] > thresholds["warning"]
    assert thresholds["global_breaker"] > thresholds["critical"]


def test_execute_agent_task_counts_completed_cycles_not_raw_llm_calls(
    monkeypatch, tmp_path: Path
) -> None:
    worker = _dummy_worker()

    async def _noop_log(level: str, message: str) -> None:
        return None

    monkeypatch.setattr(worker, "log", _noop_log)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.load_settings", lambda: object())
    monkeypatch.setattr(
        "octopal.runtime.workers.agent_worker.build_inference_provider",
        lambda settings, model=None, config=None: object(),
    )

    tool = ToolSpec(
        name="echo",
        description="echo",
        parameters={"type": "object"},
        permission="filesystem_read",
        handler=lambda args, ctx: {"ok": True},
        is_async=False,
    )
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.get_tools", lambda: [tool])

    responses = iter(
        [
            {"content": ""},
            {"content": ""},
            {
                "tool_calls": [
                    {
                        "id": "call-1",
                        "function": {"name": "echo", "arguments": '{"value": 1}'},
                    }
                ]
            },
            {"content": '{"type":"result","summary":"done"}'},
        ]
    )

    async def _fake_call_llm(provider, messages, tools):
        return next(responses)

    async def _fake_execute_tool(
        tool_name,
        tool_input,
        workspace_root,
        worker_dir,
        worker_obj,
        tool_map,
        *,
        timeout_seconds=None,
    ):
        return {"ok": True}, {
            "retries": 0,
            "timed_out": False,
            "had_error": False,
            "error_type": "none",
        }

    monkeypatch.setattr("octopal.runtime.workers.agent_worker._call_llm", _fake_call_llm)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker._execute_tool", _fake_execute_tool)

    result = asyncio.run(execute_agent_task(worker, tmp_path, tmp_path))

    assert result.summary == "done"
    assert result.thinking_steps == 2
    assert result.tools_used == ["echo"]


def test_execute_agent_task_stops_after_repeated_empty_turns(monkeypatch, tmp_path: Path) -> None:
    worker = _dummy_worker()

    async def _noop_log(level: str, message: str) -> None:
        return None

    monkeypatch.setattr(worker, "log", _noop_log)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.load_settings", lambda: object())
    monkeypatch.setattr(
        "octopal.runtime.workers.agent_worker.build_inference_provider",
        lambda settings, model=None, config=None: object(),
    )
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.get_tools", lambda: [])

    async def _fake_call_llm(provider, messages, tools):
        return {"content": ""}

    monkeypatch.setattr("octopal.runtime.workers.agent_worker._call_llm", _fake_call_llm)

    result = asyncio.run(execute_agent_task(worker, tmp_path, tmp_path))

    assert result.summary == "Task stopped after 3 empty turns without progress"
    assert result.thinking_steps == 0
    assert isinstance(result.output, dict)
    assert result.output["reason"] == "empty_turn_limit"
    assert result.output["_telemetry"]["empty_turns"] == 3


def test_execute_agent_task_does_not_charge_step_for_throttled_poll_round(
    monkeypatch,
    tmp_path: Path,
) -> None:
    worker = _dummy_worker()

    async def _noop_log(level: str, message: str) -> None:
        return None

    monkeypatch.setattr(worker, "log", _noop_log)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.load_settings", lambda: object())
    monkeypatch.setattr(
        "octopal.runtime.workers.agent_worker.build_inference_provider",
        lambda settings, model=None, config=None: object(),
    )

    tool = ToolSpec(
        name="get_worker_result",
        description="poll child",
        parameters={"type": "object"},
        permission="worker_manage",
        handler=lambda args, ctx: {"worker_id": args.get("worker_id"), "status": "running"},
        is_async=False,
    )
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.get_tools", lambda: [tool])

    responses = iter(
        [
            {
                "tool_calls": [
                    {
                        "id": "call-1",
                        "function": {
                            "name": "get_worker_result",
                            "arguments": '{"worker_id": "child-1"}',
                        },
                    }
                ]
            },
            {"content": '{"type":"result","summary":"done"}'},
        ]
    )

    async def _fake_call_llm(provider, messages, tools):
        return next(responses)

    async def _fake_execute_tool(
        tool_name,
        tool_input,
        workspace_root,
        worker_dir,
        worker_obj,
        tool_map,
        *,
        timeout_seconds=None,
    ):
        return {
            "worker_id": tool_input["worker_id"],
            "status": "running",
            "updated_at": "2026-04-16T21:12:52Z",
        }, {"retries": 0, "timed_out": False, "had_error": False, "error_type": "none"}

    async def _fake_wait_for_poll_window(worker_obj, history, *, tool_name, tool_input):
        return {
            "step_exempt": True,
            "waited_seconds": 1.0,
            "args_hash": _hash_tool_call(str(tool_name or ""), tool_input or {}),
        }

    monkeypatch.setattr("octopal.runtime.workers.agent_worker._call_llm", _fake_call_llm)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker._execute_tool", _fake_execute_tool)
    monkeypatch.setattr(
        "octopal.runtime.workers.agent_worker._maybe_wait_for_orchestration_poll_window",
        _fake_wait_for_poll_window,
    )

    result = asyncio.run(execute_agent_task(worker, tmp_path, tmp_path))

    assert result.summary == "done"
    assert result.thinking_steps == 1
    assert result.tools_used == ["get_worker_result"]


def test_execute_agent_task_injects_request_instruction_without_parent_answer_tool(
    monkeypatch,
    tmp_path: Path,
) -> None:
    worker = _dummy_worker()

    async def _noop_log(level: str, message: str) -> None:
        return None

    monkeypatch.setattr(worker, "log", _noop_log)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.load_settings", lambda: object())
    monkeypatch.setattr(
        "octopal.runtime.workers.agent_worker.build_inference_provider",
        lambda settings, model=None, config=None: object(),
    )

    tool = ToolSpec(
        name="answer_worker_instruction",
        description="answer child",
        parameters={"type": "object"},
        permission="worker_manage",
        handler=lambda args, ctx: {"status": "answered"},
        is_async=False,
    )
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.get_tools", lambda: [tool])

    responses = iter(
        [
            {"content": '{"type":"result","summary":"done"}'},
        ]
    )

    async def _fake_call_llm(provider, messages, tools):
        tool_names = {tool.name for tool in tools}
        assert "request_instruction" in tool_names
        assert "answer_worker_instruction" not in tool_names
        system_prompt = str(messages[0]["content"])
        assert "Temporal context:" in system_prompt
        assert "Current local date:" in system_prompt
        assert "Worker coordination:" in system_prompt
        assert "Parent-worker coordination:" not in system_prompt
        assert "normal tool calls" in system_prompt
        assert "JSON tool_use" in system_prompt
        return next(responses)

    async def _fake_execute_tool(
        tool_name,
        tool_input,
        workspace_root,
        worker_dir,
        worker_obj,
        tool_map,
        *,
        timeout_seconds=None,
    ):
        return {"status": "answered"}, {
            "retries": 0,
            "timed_out": False,
            "had_error": False,
            "error_type": "none",
        }

    monkeypatch.setattr("octopal.runtime.workers.agent_worker._call_llm", _fake_call_llm)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker._execute_tool", _fake_execute_tool)

    result = asyncio.run(execute_agent_task(worker, tmp_path, tmp_path))

    assert result.summary == "done"
    assert result.thinking_steps == 1
    assert result.tools_used == []


def test_worker_runtime_allows_injected_parent_instruction_answer_tool() -> None:
    parent_spec = _dummy_worker().spec.model_copy(
        update={
            "available_tools": ["start_child_worker"],
            "effective_permissions": ["worker_manage"],
        }
    )
    childless_spec = _dummy_worker().spec.model_copy(
        update={
            "available_tools": ["get_worker_result"],
            "effective_permissions": ["worker_manage"],
        }
    )

    assert (
        _validate_worker_local_tool_call(
            spec=parent_spec,
            tool_name="answer_worker_instruction",
            permission="worker_manage",
        )
        is None
    )
    assert _validate_worker_local_tool_call(
        spec=childless_spec,
        tool_name="answer_worker_instruction",
        permission="worker_manage",
    ) == "Worker tool 'answer_worker_instruction' is not allowed by this worker spec."


def test_execute_agent_task_does_not_charge_step_for_parent_instruction_answer(
    monkeypatch,
    tmp_path: Path,
) -> None:
    worker = Worker(
        spec=_dummy_worker().spec.model_copy(
            update={"available_tools": ["start_child_worker"]}
        )
    )

    async def _noop_log(level: str, message: str) -> None:
        return None

    monkeypatch.setattr(worker, "log", _noop_log)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.load_settings", lambda: object())
    monkeypatch.setattr(
        "octopal.runtime.workers.agent_worker.build_inference_provider",
        lambda settings, model=None, config=None: object(),
    )

    tools = [
        ToolSpec(
            name="start_child_worker",
            description="spawn child",
            parameters={"type": "object"},
            permission="worker_manage",
            handler=lambda args, ctx: {"status": "started"},
            is_async=False,
        ),
        ToolSpec(
            name="answer_worker_instruction",
            description="answer child",
            parameters={"type": "object"},
            permission="worker_manage",
            handler=lambda args, ctx: {"status": "answered"},
            is_async=False,
        ),
    ]
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.get_tools", lambda: tools)

    responses = iter(
        [
            {
                "tool_calls": [
                    {
                        "id": "call-1",
                        "function": {
                            "name": "answer_worker_instruction",
                            "arguments": (
                                '{"worker_id": "child-1", "instruction": "continue"}'
                            ),
                        },
                    }
                ]
            },
            {"content": '{"type":"result","summary":"done"}'},
        ]
    )

    async def _fake_call_llm(provider, messages, tools):
        tool_names = {tool.name for tool in tools}
        assert "request_instruction" in tool_names
        assert "answer_worker_instruction" in tool_names
        system_prompt = str(messages[0]["content"])
        assert "Parent-worker coordination:" in system_prompt
        assert "normal tool calls" in system_prompt
        return next(responses)

    async def _fake_execute_tool(
        tool_name,
        tool_input,
        workspace_root,
        worker_dir,
        worker_obj,
        tool_map,
        *,
        timeout_seconds=None,
    ):
        return {"status": "answered"}, {
            "retries": 0,
            "timed_out": False,
            "had_error": False,
            "error_type": "none",
        }

    async def _fake_await_children(worker_ids: list[str]) -> dict:
        raise AssertionError("answering an existing instruction should not spawn a new child wait")

    monkeypatch.setattr("octopal.runtime.workers.agent_worker._call_llm", _fake_call_llm)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker._execute_tool", _fake_execute_tool)
    monkeypatch.setattr(worker, "await_children", _fake_await_children)

    result = asyncio.run(execute_agent_task(worker, tmp_path, tmp_path))

    assert result.summary == "done"
    assert result.thinking_steps == 1
    assert result.tools_used == ["answer_worker_instruction"]


def test_execute_agent_task_suspends_until_runtime_resumes_child_batch(
    monkeypatch,
    tmp_path: Path,
) -> None:
    worker = Worker(
        spec=WorkerSpec(
            id="w-join",
            task="coordinate child work",
            inputs={},
            system_prompt="s",
            available_tools=["start_child_worker"],
            mcp_tools=[],
            model=None,
            granted_capabilities=[],
            timeout_seconds=60,
            max_thinking_steps=5,
            run_id="r-join",
            lifecycle="ephemeral",
            correlation_id=None,
        )
    )
    log_messages: list[str] = []

    async def _fake_log(level: str, message: str) -> None:
        log_messages.append(f"{level}:{message}")

    monkeypatch.setattr(worker, "log", _fake_log)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.load_settings", lambda: object())
    monkeypatch.setattr(
        "octopal.runtime.workers.agent_worker.build_inference_provider",
        lambda settings, model=None, config=None: object(),
    )

    tools = [
        ToolSpec(
            name="start_child_worker",
            description="spawn child",
            parameters={"type": "object"},
            permission="worker_manage",
            handler=lambda args, ctx: {"status": "started"},
            is_async=False,
        ),
        ToolSpec(
            name="answer_worker_instruction",
            description="answer child",
            parameters={"type": "object"},
            permission="worker_manage",
            handler=lambda args, ctx: {"status": "answered"},
            is_async=False,
        ),
    ]
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.get_tools", lambda: tools)

    call_state = {"llm_calls": 0}
    executed_tools: list[tuple[str | None, dict]] = []

    async def _fake_call_llm(provider, messages, tools):
        tool_names = {tool.name for tool in tools}
        assert "request_instruction" in tool_names
        assert "answer_worker_instruction" in tool_names
        assert "Parent-worker coordination:" in str(messages[0]["content"])
        call_state["llm_calls"] += 1
        if call_state["llm_calls"] == 1:
            return {
                "tool_calls": [
                    {
                        "id": "call-1",
                        "function": {
                            "name": "start_child_worker",
                            "arguments": '{"worker_id":"coder","task":"do child task"}',
                        },
                    }
                ]
            }
        join_notes = [
            str(message.get("content") or "")
            for message in messages
            if message.get("role") == "user"
            and "Runtime child-batch resume" in str(message.get("content") or "")
        ]
        assert any("Runtime child-batch resume" in note for note in join_notes)
        assert any("child summary" in note for note in join_notes)
        return {"content": '{"type":"result","summary":"done","output":{"joined":true}}'}

    async def _fake_execute_tool(
        tool_name,
        tool_input,
        workspace_root,
        worker_dir,
        worker_obj,
        tool_map,
        *,
        timeout_seconds=None,
    ):
        executed_tools.append((tool_name, dict(tool_input)))
        if tool_name == "start_child_worker":
            return (
                '{"status":"started","worker_id":"child-1","run_id":"child-1"}',
                {"retries": 0, "timed_out": False, "had_error": False, "error_type": "none"},
            )
        raise AssertionError(f"Unexpected tool: {tool_name}")

    async def _fake_await_children(worker_ids: list[str]) -> dict:
        assert worker_ids == ["child-1"]
        return {
            "worker_ids": ["child-1"],
            "status": "completed",
            "completed_count": 1,
            "failed_count": 0,
            "stopped_count": 0,
            "missing_count": 0,
            "completed": [
                {"worker_id": "child-1", "status": "completed", "summary": "child summary", "output": {"ok": True}}
            ],
            "failed": [],
            "stopped": [],
            "missing": [],
        }

    monkeypatch.setattr("octopal.runtime.workers.agent_worker._call_llm", _fake_call_llm)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker._execute_tool", _fake_execute_tool)
    monkeypatch.setattr(worker, "await_children", _fake_await_children)

    result = asyncio.run(execute_agent_task(worker, tmp_path, tmp_path))

    assert result.summary == "done"
    assert result.output is not None
    assert result.output["joined"] is True
    assert call_state["llm_calls"] == 2
    assert [name for name, _ in executed_tools] == ["start_child_worker"]
    assert any("Suspending parent worker until child batch completes" in message for message in log_messages)
    assert any("Resuming parent worker after child batch update" in message for message in log_messages)


def test_execute_agent_task_reawaits_children_after_instruction_answer(
    monkeypatch,
    tmp_path: Path,
) -> None:
    worker = Worker(
        spec=WorkerSpec(
            id="w-parent-answer",
            task="coordinate child work",
            inputs={},
            system_prompt="s",
            available_tools=["start_child_worker"],
            mcp_tools=[],
            model=None,
            granted_capabilities=[],
            timeout_seconds=60,
            max_thinking_steps=5,
            run_id="r-parent-answer",
            lifecycle="ephemeral",
            correlation_id=None,
        )
    )

    async def _noop_log(level: str, message: str) -> None:
        return None

    monkeypatch.setattr(worker, "log", _noop_log)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.load_settings", lambda: object())
    monkeypatch.setattr(
        "octopal.runtime.workers.agent_worker.build_inference_provider",
        lambda settings, model=None, config=None: object(),
    )

    tools = [
        ToolSpec(
            name="start_child_worker",
            description="spawn child",
            parameters={"type": "object"},
            permission="worker_manage",
            handler=lambda args, ctx: {"status": "started"},
            is_async=False,
        ),
        ToolSpec(
            name="answer_worker_instruction",
            description="answer child",
            parameters={"type": "object"},
            permission="worker_manage",
            handler=lambda args, ctx: {"status": "answered"},
            is_async=False,
        ),
    ]
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.get_tools", lambda: tools)

    call_state = {"llm_calls": 0}
    executed_tools: list[str | None] = []

    async def _fake_call_llm(provider, messages, tools):
        call_state["llm_calls"] += 1
        if call_state["llm_calls"] == 1:
            return {
                "tool_calls": [
                    {
                        "id": "call-1",
                        "function": {
                            "name": "start_child_worker",
                            "arguments": '{"worker_id":"coder","task":"do child task"}',
                        },
                    }
                ]
            }
        if call_state["llm_calls"] == 2:
            notes = [
                str(message.get("content") or "")
                for message in messages
                if message.get("role") == "user"
            ]
            assert any("Workers awaiting instruction" in note for note in notes)
            assert any("request_id=req-1" in note for note in notes)
            return {
                "tool_calls": [
                    {
                        "id": "call-2",
                        "function": {
                            "name": "answer_worker_instruction",
                            "arguments": (
                                '{"worker_id":"child-1","request_id":"req-1",'
                                '"instruction":"Use the narrow path."}'
                            ),
                        },
                    }
                ]
            }
        notes = [
            str(message.get("content") or "")
            for message in messages
            if message.get("role") == "user"
        ]
        assert any("child completed after answer" in note for note in notes)
        return {"content": '{"type":"result","summary":"done","output":{"joined":true}}'}

    async def _fake_execute_tool(
        tool_name,
        tool_input,
        workspace_root,
        worker_dir,
        worker_obj,
        tool_map,
        *,
        timeout_seconds=None,
    ):
        executed_tools.append(tool_name)
        if tool_name == "start_child_worker":
            return (
                '{"status":"started","worker_id":"child-1","run_id":"child-1"}',
                {"retries": 0, "timed_out": False, "had_error": False, "error_type": "none"},
            )
        if tool_name == "answer_worker_instruction":
            return (
                {"status": "answered", "worker_id": tool_input["worker_id"]},
                {"retries": 0, "timed_out": False, "had_error": False, "error_type": "none"},
            )
        raise AssertionError(f"Unexpected tool: {tool_name}")

    child_batches = iter(
        [
            {
                "worker_ids": ["child-1"],
                "status": "awaiting_instruction",
                "completed_count": 0,
                "failed_count": 0,
                "stopped_count": 0,
                "missing_count": 0,
                "awaiting_instruction_count": 1,
                "completed": [],
                "failed": [],
                "stopped": [],
                "missing": [],
                "awaiting_instruction": [
                    {
                        "worker_id": "child-1",
                        "status": "awaiting_instruction",
                        "summary": "Awaiting instruction: choose path",
                        "output": {
                            "instruction_request": {
                                "request_id": "req-1",
                                "question": "Which path?",
                            }
                        },
                    }
                ],
            },
            {
                "worker_ids": ["child-1"],
                "status": "completed",
                "completed_count": 1,
                "failed_count": 0,
                "stopped_count": 0,
                "missing_count": 0,
                "completed": [
                    {
                        "worker_id": "child-1",
                        "status": "completed",
                        "summary": "child completed after answer",
                        "output": {"ok": True},
                    }
                ],
                "failed": [],
                "stopped": [],
                "missing": [],
            },
        ]
    )
    awaited_batches: list[list[str]] = []

    async def _fake_await_children(worker_ids: list[str]) -> dict:
        awaited_batches.append(worker_ids)
        return next(child_batches)

    monkeypatch.setattr("octopal.runtime.workers.agent_worker._call_llm", _fake_call_llm)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker._execute_tool", _fake_execute_tool)
    monkeypatch.setattr(worker, "await_children", _fake_await_children)

    result = asyncio.run(execute_agent_task(worker, tmp_path, tmp_path))

    assert result.summary == "done"
    assert result.output is not None
    assert result.output["joined"] is True
    assert call_state["llm_calls"] == 3
    assert executed_tools == ["start_child_worker", "answer_worker_instruction"]
    assert awaited_batches == [["child-1"], ["child-1"]]
    assert result.thinking_steps == 2


def test_execute_agent_task_skips_redundant_get_worker_result_for_joined_child(
    monkeypatch,
    tmp_path: Path,
) -> None:
    worker = Worker(
        spec=WorkerSpec(
            id="w-guardrail",
            task="coordinate child work",
            inputs={},
            system_prompt="s",
            available_tools=["start_child_worker", "get_worker_result"],
            mcp_tools=[],
            model=None,
            granted_capabilities=[],
            timeout_seconds=60,
            max_thinking_steps=5,
            run_id="r-guardrail",
            lifecycle="ephemeral",
            correlation_id=None,
        )
    )
    log_messages: list[str] = []

    async def _fake_log(level: str, message: str) -> None:
        log_messages.append(f"{level}:{message}")

    monkeypatch.setattr(worker, "log", _fake_log)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.load_settings", lambda: object())
    monkeypatch.setattr(
        "octopal.runtime.workers.agent_worker.build_inference_provider",
        lambda settings, model=None, config=None: object(),
    )

    tools = [
        ToolSpec(
            name="start_child_worker",
            description="spawn child",
            parameters={"type": "object"},
            permission="worker_manage",
            handler=lambda args, ctx: {"status": "started"},
            is_async=False,
        ),
        ToolSpec(
            name="get_worker_result",
            description="get child result",
            parameters={"type": "object"},
            permission="worker_manage",
            handler=lambda args, ctx: {"status": "completed"},
            is_async=False,
        ),
    ]
    monkeypatch.setattr("octopal.runtime.workers.agent_worker.get_tools", lambda: tools)

    responses = iter(
        [
            {
                "tool_calls": [
                    {
                        "id": "call-1",
                        "function": {
                            "name": "start_child_worker",
                            "arguments": '{"worker_id":"coder","task":"do child task"}',
                        },
                    }
                ]
            },
            {
                "tool_calls": [
                    {
                        "id": "call-2",
                        "function": {
                            "name": "get_worker_result",
                            "arguments": '{"worker_id":"child-1"}',
                        },
                    }
                ]
            },
            {"content": '{"type":"result","summary":"done","output":{"guardrail":true}}'},
        ]
    )

    async def _fake_call_llm(provider, messages, tools):
        return next(responses)

    executed_tools: list[str] = []

    async def _fake_execute_tool(
        tool_name,
        tool_input,
        workspace_root,
        worker_dir,
        worker_obj,
        tool_map,
        *,
        timeout_seconds=None,
    ):
        executed_tools.append(str(tool_name))
        if tool_name == "start_child_worker":
            return (
                '{"status":"started","worker_id":"child-1","run_id":"child-1"}',
                {"retries": 0, "timed_out": False, "had_error": False, "error_type": "none"},
            )
        raise AssertionError(f"Unexpected real tool execution: {tool_name}")

    async def _fake_await_children(worker_ids: list[str]) -> dict:
        assert worker_ids == ["child-1"]
        return {
            "worker_ids": ["child-1"],
            "status": "completed",
            "completed_count": 1,
            "failed_count": 0,
            "stopped_count": 0,
            "missing_count": 0,
            "completed": [
                {"worker_id": "child-1", "status": "completed", "summary": "child summary", "output": {"ok": True}}
            ],
            "failed": [],
            "stopped": [],
            "missing": [],
        }

    monkeypatch.setattr("octopal.runtime.workers.agent_worker._call_llm", _fake_call_llm)
    monkeypatch.setattr("octopal.runtime.workers.agent_worker._execute_tool", _fake_execute_tool)
    monkeypatch.setattr(worker, "await_children", _fake_await_children)

    result = asyncio.run(execute_agent_task(worker, tmp_path, tmp_path))

    assert result.summary == "done"
    assert result.output is not None
    assert result.output["guardrail"] is True
    assert executed_tools == ["start_child_worker"]
    assert any("Skipping redundant get_worker_result for already-joined child worker: child-1" in message for message in log_messages)
