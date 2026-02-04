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
import json
import logging
from pathlib import Path
from typing import Any

from broodmind.config.settings import load_settings
from broodmind.providers.litellm_provider import LiteLLMProvider
from broodmind.tools.tools import get_tools
from broodmind.workers.contracts import WorkerResult, WorkerSpec

_LOG_MAX_CHARS = 2000
_MAX_TOOL_ITERS = 10
logger = logging.getLogger(__name__)


async def run_agent_worker(spec_path: str) -> None:
    """Main entry point for simplified agent worker."""
    from broodmind.worker_sdk.worker import Worker
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
            WorkerResult(summary=f"Worker failed: {exc}", error=str(exc))
        )


async def execute_agent_task(worker: Worker, base_dir: Path) -> WorkerResult:
    """Execute the agent's task with tools."""
    spec = worker.spec

    # Initialize LLM provider from settings
    settings = load_settings()
    provider = LiteLLMProvider(settings)

    # Build system prompt with tool descriptions
    available_tools = get_tools()
    # Filter tools by name from worker spec
    filtered_tools = [t for t in available_tools if t.name in spec.available_tools]

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

    for _iteration in range(spec.max_thinking_steps):
        thinking_steps += 1

        response = await _call_llm(provider, messages, filtered_tools)
        await worker.log("debug", f"LLM response: {response}")

        # Handle OpenAI-style tool_calls
        tool_calls = response.get("tool_calls", [])
        if tool_calls:
            # Process tool calls
            for tool_call in tool_calls:
                function = tool_call.get("function", {})
                tool_name = function.get("name")
                tool_input_str = function.get("arguments", "{}")
                tool_input = json.loads(tool_input_str) if isinstance(tool_input_str, str) else tool_input_str

                await worker.log("info", f"Using tool: {tool_name}")

                # Execute tool
                tool_result = await _execute_tool(tool_name, tool_input, base_dir)
                tools_used.append(tool_name)

                # Add assistant message with tool call
                messages.append({
                    "role": "assistant",
                    "tool_calls": [tool_call],
                })
                # Add tool result message
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.get("id", ""),
                    "content": json.dumps(tool_result) if not isinstance(tool_result, str) else tool_result,
                })
        else:
            # No tool calls, check if this is a completion
            content = response.get("content", "")

            # Try to parse as JSON for structured result
            if content:
                try:
                    result_block = json.loads(content)
                    if isinstance(result_block, dict) and result_block.get("type") == "result":
                        return WorkerResult(
                            summary=result_block.get("summary", "Task completed"),
                            output=result_block.get("output"),
                            questions=result_block.get("questions", []),
                            thinking_steps=thinking_steps,
                            tools_used=tools_used,
                        )
                except (json.JSONDecodeError, TypeError):
                    # Not JSON or not structured, treat as text completion
                    pass

            # If we get here without tool_calls or structured result, we've hit the thinking limit
            if thinking_steps >= spec.max_thinking_steps:
                return WorkerResult(
                    summary=f"Task incomplete after {thinking_steps} thinking steps",
                    thinking_steps=thinking_steps,
                    tools_used=tools_used,
                )
            # Continue to next iteration
            continue

    # Max iterations reached without completion
    return WorkerResult(
        summary=f"Task incomplete after {thinking_steps} thinking steps",
        thinking_steps=thinking_steps,
        tools_used=tools_used,
    )


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

    # Use provider's complete_with_tools method
    response = await provider.complete_with_tools(
        messages=messages,
        tools=openai_tools if openai_tools else [],
        tool_choice="auto",
    )

    # Return in expected format: {"content": "...", "tool_calls": [...]}
    return response


async def _execute_tool(tool_name: str, tool_input: dict, base_dir: Path) -> Any:
    """Execute a tool by name."""
    available_tools = get_tools()
    tool_map = {t.name: t for t in available_tools}

    if tool_name not in tool_map:
        return {"error": f"Unknown tool: {tool_name}"}

    tool = tool_map[tool_name]

    try:
        # Tool handlers expect (args, ctx) where ctx is a dict
        # Filesystem tools need base_dir in context, others don't
        ctx = {"base_dir": str(base_dir)}

        # Check if handler is async or sync
        import asyncio
        handler_result = tool.handler(tool_input, ctx)
        if asyncio.iscoroutine(handler_result):
            result = await handler_result
        else:
            result = handler_result
        return result
    except Exception as exc:
        logger.exception("Tool execution failed: %s", tool_name)
        return {"error": str(exc)}
