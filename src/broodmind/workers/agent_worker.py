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
import inspect
import json
import random
import structlog
import time
from pathlib import Path
from typing import Any

from broodmind.config.settings import load_settings
from broodmind.providers.litellm_provider import LiteLLMProvider
from broodmind.tools.tools import get_tools
from broodmind.worker_sdk.worker import Worker
from broodmind.workers.contracts import WorkerResult

_LOG_MAX_CHARS = 2000
_MAX_TOOL_ITERS = 10
_MAX_TOOL_RESULT_CHARS = 12_000
_DEFAULT_TOOL_TIMEOUT_SECONDS = 60
_DEFAULT_MAX_STEP_CAP = 30
_TRANSIENT_ERROR_HINTS = (
    "timeout",
    "timed out",
    "rate limit",
    "429",
    "connection",
    "temporarily",
    "unavailable",
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
_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "result"},
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


async def run_agent_worker(spec_path: str) -> None:
    """Main entry point for simplified agent worker."""
    from broodmind.logging_config import correlation_id_var

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
        logger.exception("AgentWorker failed: id=%s", worker.spec.id)
        await worker.complete(
            WorkerResult(
                summary=f"Worker failed: {exc}",
                output={"error": str(exc)},
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
    filtered_tools = [t for t in available_tools if t.name in spec.available_tools]
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
                if isinstance(value, (int, float)):
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
                tools_used.append(tool_name)

                # Add tool result message
                tool_result_text = (
                    tool_result
                    if isinstance(tool_result, str)
                    else json.dumps(tool_result, ensure_ascii=False, default=str)
                )
                if len(tool_result_text) > _MAX_TOOL_RESULT_CHARS:
                    telemetry["tool_result_truncations"] += 1
                tool_result_text = _truncate_text(tool_result_text, _MAX_TOOL_RESULT_CHARS)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": tool_result_text,
                })
        else:
            # No tool calls, check if this is a completion
            content = str(response.get("content", "") or "").strip()

            # Try to parse structured JSON result, including fenced JSON blocks.
            result_block = _extract_result_block(content)
            if result_block is not None:
                return WorkerResult(
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
    # Use provider's complete_with_tools method.
    try:
        response = await provider.complete_with_tools(
            messages=messages,
            tools=openai_tools if openai_tools else [],
            tool_choice="auto",
            response_format=response_format,
        )
    except Exception as exc:
        err = str(exc).lower()
        unsupported_markers = ("response_format", "unsupported", "not supported", "invalid_request_error")
        if any(marker in err for marker in unsupported_markers):
            response = await provider.complete_with_tools(
                messages=messages,
                tools=openai_tools if openai_tools else [],
                tool_choice="auto",
            )
        else:
            raise

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
            except asyncio.TimeoutError:
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
                error_type = _classify_tool_error(error_text)
                if attempt < max_attempts - 1 and error_type == "transient":
                    retries += 1
                    await asyncio.sleep(_retry_backoff(attempt))
                    continue
                return {"error": error_text}, {
                    "retries": retries,
                    "timed_out": False,
                    "had_error": True,
                    "error_type": error_type,
                }

            if _result_has_error(result):
                error_text = _extract_error_text(result)
                error_type = _classify_tool_error(error_text)
                if attempt < max_attempts - 1 and error_type == "transient":
                    retries += 1
                    await asyncio.sleep(_retry_backoff(attempt))
                    continue
                return result, {
                    "retries": retries,
                    "timed_out": False,
                    "had_error": True,
                    "error_type": error_type,
                }

            return result, {
                "retries": retries,
                "timed_out": False,
                "had_error": False,
                "error_type": "none",
            }
    except Exception as exc:
        await worker.log("error", f"Tool execution failed: {tool_name}: {exc}")
        return {"error": str(exc)}, {
            "retries": 0,
            "timed_out": False,
            "had_error": True,
            "error_type": "unknown",
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
