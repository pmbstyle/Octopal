"""
Simplified Worker - Agent with tools and system prompt

Workers are pre-defined agents that:
- Have a system prompt defining their purpose
- Have access to specific tools
- Can reason and perform multi-step operations
- Can ask Queen questions when needed
"""
from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import random
import time
import traceback
from pathlib import Path
from typing import Any

import structlog

from broodmind.infrastructure.config.settings import load_settings
from broodmind.infrastructure.providers.litellm_provider import LiteLLMProvider
from broodmind.runtime.tool_errors import ToolBridgeError
from broodmind.runtime.tool_payloads import render_tool_result_for_llm
from broodmind.runtime.workers.contracts import WorkerResult
from broodmind.tools.registry import ToolPolicy, ToolPolicyPipelineStep, apply_tool_policy_pipeline
from broodmind.tools.tools import get_tools
from broodmind.worker_sdk.worker import Worker

_LOG_MAX_CHARS = 2000
_MAX_TOOL_ITERS = 10
_DEFAULT_TOOL_TIMEOUT_SECONDS = 60
_DEFAULT_MAX_STEP_CAP = 30
_DEFAULT_TOOL_LOOP_WARNING_THRESHOLD = 8
_DEFAULT_TOOL_LOOP_CRITICAL_THRESHOLD = 12
_DEFAULT_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD = 30
_TRANSIENT_ERROR_HINTS = (
    "timeout",
    "timed out",
    "rate limit",
    "429",
    "500",
    "502",
    "503",
    "504",
    "connection",
    "temporarily",
    "unavailable",
    "service unavailable",
    "backend down",
    "bad gateway",
    "overloaded",
    "try again",
    "econnreset",
    "network",
)
_PERMANENT_ERROR_HINTS = (
    "permission",
    "denied",
    "unknown tool",
    "invalid",
    "validation",
    "required",
    "not found",
)
_UPSTREAM_UNAVAILABLE_HINTS = (
    "500",
    "502",
    "503",
    "504",
    "service unavailable",
    "backend down",
    "bad gateway",
    "gateway timeout",
)
_SYSTEMIC_TOOL_ERROR_CLASSIFICATIONS = {"schema_mismatch"}
_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "result"},
        "status": {"type": "string", "enum": ["completed", "failed"]},
        "summary": {"type": "string"},
        "output": {"type": ["object", "array", "string", "number", "boolean", "null"]},
        "questions": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["type", "summary"],
    "additionalProperties": True,
}
logger = structlog.get_logger(__name__)
_QUEEN_PROXY_TOOLS = {
    "list_workers",
    "start_worker",
    "start_child_worker",
    "start_workers_parallel",
    "synthesize_worker_results",
    "stop_worker",
    "get_worker_status",
    "list_active_workers",
    "get_worker_result",
    "get_worker_output_path",
    "create_worker_template",
    "update_worker_template",
    "delete_worker_template",
}


def _stable_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    except Exception:
        return repr(value)


def _hash_tool_call(tool_name: str, params: Any) -> str:
    payload = f"{(tool_name or '').strip().lower()}:{_stable_json(params)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _hash_tool_outcome(result: Any, meta: dict[str, Any]) -> str:
    payload = {
        "result": result,
        "timed_out": bool(meta.get("timed_out")),
        "had_error": bool(meta.get("had_error")),
    }
    return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()


def _tool_no_progress_streak(
    history: list[dict[str, str]],
    *,
    tool_name: str,
    args_hash: str,
) -> tuple[int, str | None]:
    streak = 0
    latest_result_hash: str | None = None
    for record in reversed(history):
        if record.get("tool_name") != tool_name or record.get("args_hash") != args_hash:
            continue
        record_result = record.get("result_hash")
        if not record_result:
            continue
        if latest_result_hash is None:
            latest_result_hash = record_result
            streak = 1
            continue
        if record_result != latest_result_hash:
            break
        streak += 1
    return streak, latest_result_hash


def _detect_tool_loop(
    history: list[dict[str, str]],
    *,
    tool_name: str,
    args_hash: str,
    warning_threshold: int = _DEFAULT_TOOL_LOOP_WARNING_THRESHOLD,
    critical_threshold: int = _DEFAULT_TOOL_LOOP_CRITICAL_THRESHOLD,
    global_breaker_threshold: int = _DEFAULT_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD,
) -> dict[str, Any] | None:
    if len(history) >= global_breaker_threshold:
        return {
            "detector": "global_circuit_breaker",
            "level": "critical",
            "count": len(history),
            "message": "Too many tool calls in one run without completion.",
        }

    streak, result_hash = _tool_no_progress_streak(history, tool_name=tool_name, args_hash=args_hash)
    if result_hash is None:
        return None
    if streak >= critical_threshold:
        return {
            "detector": "known_poll_no_progress",
            "level": "critical",
            "count": streak,
            "message": f"Repeated '{tool_name}' calls with no progress.",
        }
    if streak >= warning_threshold:
        return {
            "detector": "known_poll_no_progress",
            "level": "warning",
            "count": streak,
            "message": f"Potential tool loop detected for '{tool_name}'.",
        }
    return None


def _resolve_tool_loop_thresholds() -> dict[str, int]:
    warning = _parse_positive_int_env(
        "BROODMIND_TOOL_LOOP_WARNING_THRESHOLD",
        _DEFAULT_TOOL_LOOP_WARNING_THRESHOLD,
    )
    critical = _parse_positive_int_env(
        "BROODMIND_TOOL_LOOP_CRITICAL_THRESHOLD",
        _DEFAULT_TOOL_LOOP_CRITICAL_THRESHOLD,
    )
    global_breaker = _parse_positive_int_env(
        "BROODMIND_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD",
        _DEFAULT_TOOL_LOOP_GLOBAL_BREAKER_THRESHOLD,
    )
    if critical <= warning:
        critical = warning + 1
    if global_breaker <= critical:
        global_breaker = critical + 1
    return {
        "warning": warning,
        "critical": critical,
        "global_breaker": global_breaker,
    }


def _parse_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


async def run_agent_worker(spec_path: str) -> None:
    """Main entry point for simplified agent worker."""
    from broodmind.infrastructure.logging import correlation_id_var

    worker = Worker.from_spec_file(spec_path)
    base_dir = Path(spec_path).parent

    # Set the correlation ID for this worker's context
    if worker.spec.correlation_id:
        correlation_id_var.set(worker.spec.correlation_id)

    await worker.log(
        "info",
        f"AgentWorker start: id={worker.spec.id} run_id={worker.spec.run_id}",
    )

    try:
        result = await execute_agent_task(worker, base_dir)
        await worker.complete(result)
    except Exception as exc:
        error_text = str(exc)
        await worker.log("error", f"AgentWorker failed: id={worker.spec.id} error={error_text}")
        await worker.complete(
            WorkerResult(
                status="failed",
                summary=f"Worker failed: {error_text}",
                output={
                    "error": error_text,
                    "traceback": _truncate_text(traceback.format_exc(), 4000),
                },
            )
        )


async def execute_agent_task(worker: Worker, base_dir: Path) -> WorkerResult:
    """Execute the agent's task with tools."""
    spec = worker.spec

    # Initialize LLM provider from settings
    settings = load_settings()
    provider = LiteLLMProvider(settings, model=spec.model)

    # Build system prompt with tool descriptions
    available_tools = get_tools()
    # Filter tools by name from worker spec
    filtered_tools = apply_tool_policy_pipeline(
        available_tools,
        [
            ToolPolicyPipelineStep(
                label="worker.available_tools",
                policy=ToolPolicy(allow=list(spec.available_tools or [])),
            )
        ],
    )
    filtered_tools = _with_queen_tool_proxies(filtered_tools, worker)

    # Add MCP tools from spec
    from broodmind.tools.registry import ToolSpec
    for mcp_tool_data in spec.mcp_tools:
        # Generate a proxy handler for this MCP tool.
        identity = _extract_mcp_identity(mcp_tool_data)
        if identity is None:
            await worker.log(
                "warning",
                f"Skipping MCP tool with invalid identity: {mcp_tool_data.get('name', '<unknown>')}",
            )
            continue
        s_id, t_name = identity

        async def mcp_proxy_handler(args: dict, ctx: dict, s_id=s_id, t_name=t_name):
            w = ctx.get("worker")
            return await w.call_mcp_tool(s_id, t_name, args)

        mcp_spec = ToolSpec(
            name=mcp_tool_data["name"],
            description=mcp_tool_data["description"],
            parameters=mcp_tool_data["parameters"],
            permission=mcp_tool_data["permission"],
            handler=mcp_proxy_handler,
            is_async=True,
        )
        filtered_tools.append(mcp_spec)

    tool_descriptions = "\n".join(
        f"- {t.name}: {t.description}" for t in filtered_tools
    )

    system_prompt = f"""{spec.system_prompt}

Available tools:
{tool_descriptions}

When you need to use a tool, the system will automatically call it for you. Just indicate what you want to do in your response.

When you have completed the task, respond with:
{{
  "type": "result",
  "summary": "Brief summary of what you did",
  "output": {{...}}  // Optional structured output
}}

If you need clarification from the Queen, include:
{{
  "type": "result",
  "summary": "...",
  "questions": ["question1", "question2"]
}}
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Task: {spec.task}\n\nInputs: {json.dumps(spec.inputs, indent=2)}"},
    ]

    tools_used = []
    thinking_steps = 0
    tool_map = {t.name: t for t in filtered_tools}
    loop_start = asyncio.get_running_loop().time()
    effective_max_steps = _auto_tune_max_steps(spec.max_thinking_steps, spec.available_tools, spec.system_prompt)
    telemetry: dict[str, Any] = {
        "max_thinking_steps_configured": spec.max_thinking_steps,
        "max_thinking_steps_effective": effective_max_steps,
        "llm_calls": 0,
        "llm_latency_ms_total": 0,
        "tool_calls": 0,
        "tool_latency_ms_total": 0,
        "tool_retries": 0,
        "tool_timeouts": 0,
        "tool_errors": 0,
        "tool_result_truncations": 0,
        "tokens": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }
    upstream_failures: dict[str, int] = {}
    successful_tool_calls = 0
    tool_call_history: list[dict[str, str]] = []
    tool_loop_thresholds = _resolve_tool_loop_thresholds()

    for _iteration in range(effective_max_steps):
        thinking_steps += 1

        llm_start = time.perf_counter()
        response = await _call_llm(provider, messages, filtered_tools)
        telemetry["llm_calls"] += 1
        telemetry["llm_latency_ms_total"] += int((time.perf_counter() - llm_start) * 1000)
        usage = response.get("usage") or {}
        if isinstance(usage, dict):
            for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
                value = usage.get(key)
                if isinstance(value, int | float):
                    telemetry["tokens"][key] += int(value)
        await worker.log("debug", f"LLM response: {response}")

        # Handle OpenAI-style tool_calls
        tool_calls = response.get("tool_calls", [])
        if tool_calls:
            content = response.get("content", "")
            assistant_msg: dict[str, Any] = {"role": "assistant", "tool_calls": tool_calls}
            if content:
                assistant_msg["content"] = content
            messages.append(assistant_msg)

            # Process tool calls
            for tool_call in tool_calls:
                function = tool_call.get("function", {})
                tool_name = function.get("name")
                tool_input = _parse_tool_arguments(function.get("arguments", "{}"))
                tool_call_id = tool_call.get("id", "") or ""

                await worker.log("info", f"Using tool: {tool_name}")

                # Execute tool
                elapsed = asyncio.get_running_loop().time() - loop_start
                remaining_budget = max(1, spec.timeout_seconds - int(elapsed))
                tool_timeout = min(_DEFAULT_TOOL_TIMEOUT_SECONDS, remaining_budget)
                tool_start = time.perf_counter()
                tool_result, tool_meta = await _execute_tool(
                    tool_name,
                    tool_input,
                    base_dir,
                    worker,
                    tool_map,
                    timeout_seconds=tool_timeout,
                )
                telemetry["tool_calls"] += 1
                telemetry["tool_latency_ms_total"] += int((time.perf_counter() - tool_start) * 1000)
                telemetry["tool_retries"] += int(tool_meta.get("retries", 0))
                if tool_meta.get("timed_out"):
                    telemetry["tool_timeouts"] += 1
                if tool_meta.get("had_error"):
                    telemetry["tool_errors"] += 1
                else:
                    successful_tool_calls += 1
                tools_used.append(tool_name)
                args_hash = _hash_tool_call(str(tool_name or ""), tool_input)
                result_hash = _hash_tool_outcome(tool_result, tool_meta)
                tool_call_history.append(
                    {
                        "tool_name": str(tool_name or ""),
                        "args_hash": args_hash,
                        "result_hash": result_hash,
                    }
                )
                loop_state = _detect_tool_loop(
                    tool_call_history,
                    tool_name=str(tool_name or ""),
                    args_hash=args_hash,
                    warning_threshold=tool_loop_thresholds["warning"],
                    critical_threshold=tool_loop_thresholds["critical"],
                    global_breaker_threshold=tool_loop_thresholds["global_breaker"],
                )
                if loop_state is not None:
                    if loop_state["level"] == "warning":
                        await worker.log(
                            "warning",
                            (
                                f"Tool loop warning ({loop_state['detector']}): "
                                f"{loop_state['message']} count={loop_state['count']}"
                            ),
                        )
                    else:
                        await worker.log(
                            "warning",
                            (
                                f"Tool loop breaker ({loop_state['detector']}): "
                                f"{loop_state['message']} count={loop_state['count']}"
                            ),
                        )
                        return WorkerResult(
                            summary=(
                                "Task stopped to prevent an infinite tool loop. "
                                "Please refine the task or provide additional constraints."
                            ),
                            output=_attach_telemetry(
                                {
                                    "degraded": True,
                                    "reason": "tool_loop_detected",
                                    "loop": loop_state,
                                },
                                telemetry,
                            ),
                            knowledge_proposals=worker.knowledge_proposals,
                            thinking_steps=thinking_steps,
                            tools_used=tools_used,
                        )

                if tool_meta.get("had_error"):
                    error_text = _extract_error_text(tool_result)
                    if _is_systemic_tool_bridge_failure(tool_meta):
                        return WorkerResult(
                            status="failed",
                            summary="Task failed: remote MCP tool response schema is incompatible.",
                            output=_attach_telemetry(
                                {
                                    "degraded": True,
                                    "reason": "mcp_schema_mismatch",
                                    "failed_tool": tool_name,
                                    "bridge": tool_meta.get("error_bridge"),
                                    "error_classification": tool_meta.get("error_classification"),
                                    "error": _truncate_text(error_text, 500),
                                },
                                telemetry,
                            ),
                            knowledge_proposals=worker.knowledge_proposals,
                            thinking_steps=thinking_steps,
                            tools_used=tools_used,
                        )
                    if _is_upstream_unavailable_error(error_text):
                        signature = f"{tool_name}:{_upstream_error_bucket(error_text)}"
                        upstream_failures[signature] = upstream_failures.get(signature, 0) + 1
                        if upstream_failures[signature] >= 2 and successful_tool_calls == 0:
                            return WorkerResult(
                                summary=(
                                    "Task partially completed with degraded state: "
                                    "upstream service is currently unavailable."
                                ),
                                output=_attach_telemetry(
                                    {
                                        "degraded": True,
                                        "reason": "upstream_unavailable",
                                        "failed_tool": tool_name,
                                        "error": _truncate_text(error_text, 500),
                                    },
                                    telemetry,
                                ),
                                knowledge_proposals=worker.knowledge_proposals,
                                thinking_steps=thinking_steps,
                                tools_used=tools_used,
                            )

                # Add tool result message
                rendered_tool_result = render_tool_result_for_llm(tool_result)
                if rendered_tool_result.was_compacted:
                    telemetry["tool_result_truncations"] += 1
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": rendered_tool_result.text,
                })
        else:
            # No tool calls, check if this is a completion
            content = str(response.get("content", "") or "").strip()

            # Try to parse structured JSON result, including fenced JSON blocks.
            result_block = _extract_result_block(content)
            if result_block is not None:
                return WorkerResult(
                    status=str(result_block.get("status", "completed")) if result_block.get("status") in {"completed", "failed"} else "completed",
                    summary=str(result_block.get("summary", "Task completed")).strip() or "Task completed",
                    output=_attach_telemetry(result_block.get("output"), telemetry),
                    questions=result_block.get("questions", []),
                    knowledge_proposals=worker.knowledge_proposals,
                    thinking_steps=thinking_steps,
                    tools_used=tools_used,
                )

            # If model produced plain text with no tool call, treat it as completion.
            if content:
                return WorkerResult(
                    summary=content,
                    output=_attach_telemetry(None, telemetry),
                    knowledge_proposals=worker.knowledge_proposals,
                    thinking_steps=thinking_steps,
                    tools_used=tools_used,
                )

            # If we get here without tool_calls or structured result, we've hit the thinking limit
            if thinking_steps >= effective_max_steps:
                return WorkerResult(
                    summary=f"Task incomplete after {thinking_steps} thinking steps",
                    output=_attach_telemetry(None, telemetry),
                    knowledge_proposals=worker.knowledge_proposals,
                    thinking_steps=thinking_steps,
                    tools_used=tools_used,
                )
            # Continue to next iteration
            continue

    # Max iterations reached without completion
    return WorkerResult(
        summary=f"Task incomplete after {thinking_steps} thinking steps",
        output=_attach_telemetry(None, telemetry),
        knowledge_proposals=worker.knowledge_proposals,
        thinking_steps=thinking_steps,
        tools_used=tools_used,
    )


def _extract_result_block(content: str) -> dict[str, Any] | None:
    if not content:
        return None

    candidates = [content]
    stripped = content.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3 and lines[-1].strip() == "```":
            body = "\n".join(lines[1:-1]).strip()
            candidates.append(body)

    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(payload, dict):
            if payload.get("type") == "result":
                return payload
            if "summary" in payload:
                normalized = dict(payload)
                normalized.setdefault("type", "result")
                if _is_valid_result_payload(normalized):
                    return normalized
    return None


async def _call_llm(
    provider: LiteLLMProvider,
    messages: list[dict],
    tools: list,
) -> dict:
    """Call LLM with tools using the centralized provider."""
    # Build OpenAI-style tools format
    openai_tools = [
        {
            "type": "function",
            "function": {
                "name": t.name,
                "description": t.description,
                "parameters": t.parameters,
            },
        }
        for t in tools
    ]

    response_format = {
        "type": "json_schema",
        "json_schema": {"name": "worker_result", "schema": _RESULT_SCHEMA},
    }
    # Provider handles adaptive response_format downgrade when a route does not
    # support schema-constrained outputs.
    response = await provider.complete_with_tools(
        messages=messages,
        tools=openai_tools if openai_tools else [],
        tool_choice="auto",
        response_format=response_format,
    )

    # Return in expected format: {"content": "...", "tool_calls": [...]}
    return response


async def _execute_tool(
    tool_name: str | None,
    tool_input: dict,
    base_dir: Path,
    worker: Worker,
    tool_map: dict[str, Any],
    *,
    timeout_seconds: int | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Execute a tool by name."""
    if not tool_name or tool_name not in tool_map:
        return {"error": f"Unknown tool: {tool_name}"}, {
            "retries": 0,
            "timed_out": False,
            "had_error": True,
            "error_type": "permanent",
        }

    tool = tool_map[tool_name]

    try:
        # Tool handlers expect (args, ctx) where ctx is a dict
        # Filesystem tools need base_dir in context, others don't
        # worker instance is needed for intent requests
        ctx = {"base_dir": base_dir, "worker": worker}

        # Use tool.is_async to determine if it needs to be awaited
        async def _run_tool() -> Any:
            if tool.is_async:
                if inspect.iscoroutinefunction(tool.handler):
                    return await tool.handler(tool_input, ctx)
                maybe_result = tool.handler(tool_input, ctx)
                if inspect.isawaitable(maybe_result):
                    return await maybe_result
                return maybe_result
            # Run sync handlers in a thread to keep loop responsive.
            return await asyncio.to_thread(tool.handler, tool_input, ctx)

        max_attempts = 3 if _is_tool_retryable(tool_name, tool) else 1
        retries = 0
        for attempt in range(max_attempts):
            try:
                if timeout_seconds and timeout_seconds > 0:
                    result = await asyncio.wait_for(_run_tool(), timeout=timeout_seconds)
                else:
                    result = await _run_tool()
            except TimeoutError:
                error_text = f"Tool timed out after {timeout_seconds}s: {tool_name}"
                if attempt < max_attempts - 1:
                    retries += 1
                    await asyncio.sleep(_retry_backoff(attempt))
                    continue
                return {"error": error_text}, {
                    "retries": retries,
                    "timed_out": True,
                    "had_error": True,
                    "error_type": "transient",
                }
            except Exception as exc:
                await worker.log("error", f"Tool execution failed: {tool_name}: {exc}")
                error_text = str(exc)
                error_info = _tool_error_info(
                    error_text,
                    classification=exc.classification if isinstance(exc, ToolBridgeError) else None,
                    bridge=exc.bridge if isinstance(exc, ToolBridgeError) else None,
                    retryable=exc.retryable if isinstance(exc, ToolBridgeError) else None,
                )
                error_type = str(error_info["error_type"])
                if attempt < max_attempts - 1 and error_type == "transient":
                    retries += 1
                    await asyncio.sleep(_retry_backoff(attempt))
                    continue
                return {"error": error_text}, {
                    "retries": retries,
                    "timed_out": False,
                    "had_error": True,
                    **error_info,
                }

            if _result_has_error(result):
                error_text = _extract_error_text(result)
                classification = None
                bridge = None
                retryable = None
                if isinstance(result, dict):
                    if isinstance(result.get("classification"), str):
                        classification = result["classification"]
                    if isinstance(result.get("bridge"), str):
                        bridge = result["bridge"]
                    if isinstance(result.get("retryable"), bool):
                        retryable = result["retryable"]
                error_info = _tool_error_info(
                    error_text,
                    classification=classification,
                    bridge=bridge,
                    retryable=retryable,
                )
                error_type = str(error_info["error_type"])
                if attempt < max_attempts - 1 and error_type == "transient":
                    retries += 1
                    await asyncio.sleep(_retry_backoff(attempt))
                    continue
                return result, {
                    "retries": retries,
                    "timed_out": False,
                    "had_error": True,
                    **error_info,
                }

            return result, {
                "retries": retries,
                "timed_out": False,
                "had_error": False,
                "error_type": "none",
            }
    except Exception as exc:
        await worker.log("error", f"Tool execution failed: {tool_name}: {exc}")
        error_info = _tool_error_info(
            str(exc),
            classification=exc.classification if isinstance(exc, ToolBridgeError) else None,
            bridge=exc.bridge if isinstance(exc, ToolBridgeError) else None,
            retryable=exc.retryable if isinstance(exc, ToolBridgeError) else None,
        )
        return {"error": str(exc)}, {
            "retries": 0,
            "timed_out": False,
            "had_error": True,
            **error_info,
        }


def _with_queen_tool_proxies(tools: list[Any], worker: Worker) -> list[Any]:
    proxied: list[Any] = []
    for tool in tools:
        if getattr(tool, "name", "") not in _QUEEN_PROXY_TOOLS:
            proxied.append(tool)
            continue
        proxied.append(_make_queen_proxy_tool(tool, worker))
    return proxied


def _make_queen_proxy_tool(tool: Any, worker: Worker) -> Any:
    from broodmind.tools.registry import ToolSpec

    async def _proxy_handler(args: dict[str, Any], ctx: dict[str, Any]) -> Any:
        return await worker.call_queen_tool(tool.name, args)

    return ToolSpec(
        name=tool.name,
        description=tool.description,
        parameters=tool.parameters,
        permission=tool.permission,
        handler=_proxy_handler,
        is_async=True,
    )


def _extract_mcp_identity(mcp_tool_data: dict[str, Any]) -> tuple[str, str] | None:
    """Extract MCP server/tool identity from explicit metadata or legacy names."""
    server_id = mcp_tool_data.get("server_id")
    remote_tool_name = mcp_tool_data.get("remote_tool_name")
    if isinstance(server_id, str) and server_id and isinstance(remote_tool_name, str) and remote_tool_name:
        return server_id, remote_tool_name

    name = str(mcp_tool_data.get("name", ""))
    if not name.startswith("mcp_"):
        return None
    # Legacy fallback: mcp_<safe_server_id>_<safe_tool_name>. This may be ambiguous
    # when both include underscores, but keeps compatibility for older specs.
    parts = name.split("_")
    if len(parts) < 3:
        return None
    return parts[1], "_".join(parts[2:])


def _parse_tool_arguments(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
            return {"_arg": parsed}
        except json.JSONDecodeError:
            return {"_raw": value}
    return {}


def _truncate_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + f"...[truncated {len(text) - max_chars} chars]"


def _is_valid_result_payload(payload: dict[str, Any]) -> bool:
    try:
        from jsonschema import ValidationError, validate

        validate(instance=payload, schema=_RESULT_SCHEMA)
        return True
    except ValidationError:
        return False
    except Exception:
        return "summary" in payload


def _classify_tool_error(text: str) -> str:
    lowered = (text or "").lower()
    if any(token in lowered for token in _TRANSIENT_ERROR_HINTS):
        return "transient"
    if any(token in lowered for token in _PERMANENT_ERROR_HINTS):
        return "permanent"
    return "unknown"


def _tool_error_info(
    error_text: str,
    *,
    classification: str | None = None,
    bridge: str | None = None,
    retryable: bool | None = None,
) -> dict[str, Any]:
    if retryable is True:
        error_type = "transient"
    elif retryable is False:
        error_type = "permanent"
    else:
        error_type = _classify_tool_error(error_text)
    return {
        "error_type": error_type,
        "error_classification": classification or "unknown",
        "error_bridge": bridge or "tool",
        "retryable": retryable,
    }


def _is_systemic_tool_bridge_failure(tool_meta: dict[str, Any]) -> bool:
    return (
        tool_meta.get("error_bridge") == "mcp"
        and tool_meta.get("error_classification") in _SYSTEMIC_TOOL_ERROR_CLASSIFICATIONS
    )


def _result_has_error(result: Any) -> bool:
    if isinstance(result, dict):
        return isinstance(result.get("error"), str) and bool(result.get("error"))
    if isinstance(result, str):
        lowered = result.lower()
        return "error" in lowered or "failed" in lowered
    return False


def _extract_error_text(result: Any) -> str:
    if isinstance(result, dict) and isinstance(result.get("error"), str):
        return result["error"]
    if isinstance(result, str):
        return result
    return str(result)


def _is_upstream_unavailable_error(text: str) -> bool:
    lowered = (text or "").lower()
    return any(token in lowered for token in _UPSTREAM_UNAVAILABLE_HINTS)


def _upstream_error_bucket(text: str) -> str:
    lowered = (text or "").lower()
    for token in _UPSTREAM_UNAVAILABLE_HINTS:
        if token in lowered:
            return token
    return "upstream_unavailable"


def _retry_backoff(attempt: int) -> float:
    base = 0.25 * (2**attempt)
    jitter = random.uniform(0.0, 0.2)
    return min(2.0, base + jitter)


def _is_tool_retryable(tool_name: str, tool: Any) -> bool:
    permission = getattr(tool, "permission", "")
    if permission in {"filesystem_write", "service_control", "deploy_control"}:
        return False
    read_like_prefixes = ("get_", "list_", "read_", "web_", "search_", "mcp_")
    return tool_name.startswith(read_like_prefixes) or permission in {"network", "filesystem_read", "service_read"}


def _auto_tune_max_steps(base_steps: int, available_tools: list[str], system_prompt: str) -> int:
    tuned = max(3, int(base_steps))
    tool_set = set(available_tools)
    if any(name.startswith("mcp_") or "web" in name for name in tool_set):
        tuned += 3
    if any(name in {"exec_run", "test_run", "docker_compose_control", "deploy_manager"} for name in tool_set):
        tuned += 2
    if "writer" in system_prompt.lower() and len(tool_set) <= 2:
        tuned -= 2
    return max(3, min(_DEFAULT_MAX_STEP_CAP, tuned))


def _attach_telemetry(output: Any, telemetry: dict[str, Any]) -> dict[str, Any]:
    payload = output if isinstance(output, dict) else {}
    payload["_telemetry"] = telemetry
    return payload
