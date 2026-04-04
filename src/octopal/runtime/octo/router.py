from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import structlog

from octopal.infrastructure.providers.base import InferenceProvider, Message
from octopal.runtime.memory.service import MemoryService
from octopal.runtime.octo.delivery import resolve_user_delivery
from octopal.runtime.octo.prompt_builder import (
    build_bootstrap_context_prompt,
    build_octo_prompt,
)
from octopal.runtime.tool_loop import (
    _detect_tool_loop,
    _hash_tool_call,
    _hash_tool_outcome,
    _resolve_tool_loop_thresholds,
)
from octopal.runtime.tool_payloads import render_tool_result_for_llm
from octopal.runtime.workers.contracts import WorkerResult
from octopal.tools.diagnostics import ToolResolutionReport, resolve_tool_diagnostics
from octopal.tools.registry import ToolPolicy, ToolPolicyPipelineStep, ToolSpec
from octopal.tools.tools import get_tools
from octopal.utils import (
    extract_reaction_and_strip,
    looks_like_textual_tool_invocation,
    sanitize_user_facing_text,
    sanitize_user_facing_text_preserving_reaction,
    should_suppress_user_delivery,
)

logger = structlog.get_logger(__name__)
_MAX_PLAN_STEPS = 10
_MAX_VERIFY_CONTEXT_CHARS = 20000
_DEFAULT_MAX_TOOL_COUNT = 64
_MIN_TOOL_COUNT_ON_OVERFLOW = 12
_CATALOG_TOOL_EXPANSION_LIMIT = 12
_MANDATORY_OCTO_TOOL_NAMES = {
    "fs_list",
    "fs_read",
    "fs_write",
    "fs_move",
    "fs_delete",
}
_PRIORITY_TOOL_NAMES = {
    "octo_context_reset",
    "octo_context_health",
    "tool_catalog_search",
    "octo_experiment_log",
    "check_schedule",
    "start_worker",
    "get_worker_result",
    "get_worker_output_path",
    "worker_yield",
    "gateway_status",
    "mcp_discover",
    "manage_canon",
}
_ALWAYS_INCLUDE_TOOL_NAMES = {
    # Octo self-control baseline
    "octo_context_reset",
    "octo_context_health",
    "check_schedule",
    "scheduler_status",
    "tool_catalog_search",
    # Scheduler control loop
    "list_schedule",
    "schedule_task",
    "remove_task",
    # Worker lifecycle essentials
    "list_workers",
    "start_worker",
    "start_child_worker",
    "start_workers_parallel",
    "get_worker_status",
    "list_active_workers",
    "worker_session_status",
    "worker_yield",
    "get_worker_result",
    "get_worker_output_path",
    "stop_worker",
    "send_file_to_user",
    # Octo must always be able to inspect and mutate its workspace.
    "fs_list",
    "fs_read",
    "fs_write",
    "fs_move",
    "fs_delete",
}
_TEXTUAL_TOOL_NAME_RE = re.compile(r"^(?:mcp__[\w-]+__)?[a-z][a-z0-9_]{1,63}$", re.IGNORECASE)
_TEXTUAL_TOOL_PREVIEW_RE = re.compile(
    r"^(?P<tool>(?:mcp__[\w-]+__)?[a-z][a-z0-9_]{1,63})(?P<rest>(?:,\s*[a-z_][a-z0-9_ -]{0,31}:\s*[^,\n]{1,200})+)$",
    re.IGNORECASE,
)


def _is_vision_tool_compatibility_error(exc: Exception) -> bool:
    err = str(exc).lower()
    return (
        "invalid api parameter" in err
        or "'code': '1210'" in err
        or '"code": "1210"' in err
    )


def _is_invalid_tool_payload_error(exc: Exception) -> bool:
    err = _exception_chain_text(exc).lower()
    return (
        "invalid api parameter" in err
        or "'code': '1210'" in err
        or '"code": "1210"' in err
        or "tool_choice" in err
        or "tools parameter" in err
    )


def _build_saved_image_fallback_text(user_text: str, saved_paths: list[str]) -> str:
    intro = user_text.strip() or "Please inspect the attached image."
    path_lines = "\n".join(f"- {path}" for path in saved_paths)
    return (
        f"{intro}\n\n"
        "[SYSTEM NOTE: The user sent image attachments. Direct multimodal processing was rejected by the active "
        "provider/model combination, so the images were saved locally for tool-based inspection.\n"
        f"{path_lines}\n"
        "Use any available filesystem, MCP, or image-analysis tools to inspect those files before answering. "
        "If no such tools are available, explain that clearly and ask the user for a brief description.]"
    )


async def route_or_reply(
    octo: Any,
    provider: InferenceProvider,
    memory: MemoryService,
    user_text: str,
    chat_id: int,
    bootstrap_context: str,
    *,
    internal_followup: bool = False,
    show_typing: bool = True,
    images: list[str] | None = None,
    saved_file_paths: list[str] | None = None,
    include_wakeup: bool = True,
) -> str:
    """Core routing logic: decide whether to use tools or reply to user."""
    # Internal chat_id (<= 0) should not trigger typing indicators.
    if chat_id > 0 and show_typing:
        await octo.set_typing(chat_id, True)

    await octo.set_thinking(True)
    try:
        partial_callback = _build_partial_callback(octo=octo, chat_id=chat_id)
        is_ws = getattr(octo, "is_ws_active", False)
        wake_notice = ""
        if include_wakeup and hasattr(octo, "peek_context_wakeup"):
            wake_notice = str(octo.peek_context_wakeup(chat_id) or "")
        mcp_manager = getattr(octo, "mcp_manager", None)
        if mcp_manager is not None:
            try:
                await mcp_manager.ensure_configured_servers_connected()
            except Exception:
                logger.warning("Failed to refresh configured MCP servers before routing", exc_info=True)

        octo_tools, ctx = _get_octo_tools(octo, chat_id)
        logger.info("Octo tools fetched: count=%d", len(octo_tools))
        tool_policy_summary = _build_octo_tool_policy_summary(
            octo_tools,
            ctx.get("tool_resolution_report"),
        )
        messages = await build_octo_prompt(
            store=octo.store,
            memory=memory,
            canon=octo.canon,
            user_text=user_text,
            chat_id=chat_id,
            bootstrap_context=bootstrap_context,
            is_ws=is_ws,
            images=images,
            saved_file_paths=saved_file_paths,
            wake_notice=wake_notice,
            tool_policy_summary=tool_policy_summary,
        )
        if not internal_followup:
            messages.append(
                Message(
                    role="system",
                    content=(
                        "If you are sending an interim update and you must return with a later result without waiting "
                        "for another user message, append exactly FOLLOWUP_REQUIRED on its own final line. "
                        "Do not use FOLLOWUP_REQUIRED for final/completed answers."
                    ),
                )
            )
        _log_system_prompt(messages, "route")

        plan = await _build_plan(provider, messages, bool(octo_tools))
        if plan:
            await _persist_plan(memory, chat_id, plan)
            logger.info(
                "Octo plan ready",
                mode=plan["mode"],
                steps=len(plan.get("steps", [])),
            )
            if plan["mode"] == "reply":
                return await _finalize_response(
                    provider=provider,
                    messages=messages,
                    response_text=str(plan.get("response", "")),
                    internal_followup=internal_followup,
                )
            plan_steps = plan.get("steps", [])
            if plan_steps:
                plan_block = "\n".join([f"{idx + 1}. {step}" for idx, step in enumerate(plan_steps)])
                messages.append(
                    Message(
                        role="system",
                        content=(
                            "Execution plan generated by planner. Execute steps in order and recover gracefully from failures.\n"
                            "<execution_plan>\n"
                            f"{plan_block}\n"
                            "</execution_plan>"
                        ),
                    )
                )
        tool_capable = getattr(provider, "complete_with_tools", None)

        if callable(tool_capable):
            active_tool_specs = list(octo_tools)
            tools = [spec.to_openai_tool() for spec in active_tool_specs]
            last_error: str | None = None
            had_tool_calls = False
            transient_tool_failures = 0
            tool_call_history: list[dict[str, str]] = []
            tool_loop_thresholds = _resolve_tool_loop_thresholds()
            max_attempts = 10
            vision_tool_fallback_used = False

            for _ in range(max_attempts):
                try:
                    result = await provider.complete_with_tools(messages, tools=tools, tool_choice="auto")
                except Exception as e:
                    # If we have images, this might be a multi-modal conflict (e.g. z.ai GLM-4 doesn't support tools + vision).
                    # Fallback strategy: Save images to disk and retry with a text-only prompt pointing to the files.
                    if images and not vision_tool_fallback_used and _is_vision_tool_compatibility_error(e):
                        logger.warning("Vision+Tools failed; attempting save-to-disk fallback", error=str(e))
                        try:
                            saved_paths = []
                            workspace_dir = Path(os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")).resolve()
                            img_dir = workspace_dir / "tmp" / "telegram_images"
                            img_dir.mkdir(parents=True, exist_ok=True)

                            for _idx, img_data in enumerate(images):
                                # expect data:image/jpeg;base64,....
                                if "," in img_data:
                                    header, b64_str = img_data.split(",", 1)
                                    ext = ".jpg"
                                    if "png" in header:
                                        ext = ".png"
                                    elif "webp" in header:
                                        ext = ".webp"
                                else:
                                    b64_str = img_data
                                    ext = ".jpg"  # assume jpg

                                file_name = f"img_{uuid.uuid4()}{ext}"
                                file_path = img_dir / file_name
                                with open(file_path, "wb") as f:
                                    f.write(base64.b64decode(b64_str))
                                saved_paths.append(str(file_path))

                            fallback_text = _build_saved_image_fallback_text(user_text, saved_paths)

                            logger.info("Retrying with text-only fallback and saved images", count=len(saved_paths))
                            messages[-1] = {"role": "user", "content": fallback_text}
                            images = None
                            vision_tool_fallback_used = True
                            continue

                        except Exception as fallback_exc:
                            logger.error("Fallback save-and-retry failed", error=str(fallback_exc))
                            return "I see you sent an image, but I am unable to process it. My current model configuration might not support vision, and I could not save it for tool analysis."
                    if vision_tool_fallback_used:
                        logger.warning(
                            "Tool-enabled retry after image save failed; falling back to plain text completion",
                            error=_exception_chain_text(e)[:500],
                        )
                        messages.append(
                            Message(
                                role="system",
                                content=(
                                    "Tool calling failed even after converting the image request into a text-only "
                                    "instruction with local file paths. Reply without tools. Be transparent that the "
                                    "image files were saved locally but could not be inspected automatically."
                                ),
                            )
                        )
                        fallback_text = await _complete_text(
                            provider,
                            messages,
                            context="saved_image_tool_retry_failed",
                        )
                        return await _finalize_response(
                            provider=provider,
                            messages=messages,
                            response_text=fallback_text,
                            internal_followup=internal_followup,
                        )
                    if _is_context_overflow_error(e) and len(active_tool_specs) > _MIN_TOOL_COUNT_ON_OVERFLOW:
                        prior_count = len(active_tool_specs)
                        active_tool_specs = _shrink_tool_specs_for_retry(active_tool_specs)
                        tools = [spec.to_openai_tool() for spec in active_tool_specs]
                        logger.warning(
                            "Retrying completion with fewer tools after context overflow",
                            previous_tool_count=prior_count,
                            reduced_tool_count=len(active_tool_specs),
                        )
                        continue
                    if _is_invalid_tool_payload_error(e) and len(active_tool_specs) > _MIN_TOOL_COUNT_ON_OVERFLOW:
                        prior_count = len(active_tool_specs)
                        active_tool_specs = _shrink_tool_specs_for_retry(active_tool_specs)
                        tools = [spec.to_openai_tool() for spec in active_tool_specs]
                        logger.warning(
                            "Retrying completion with fewer tools after provider rejected tool payload",
                            previous_tool_count=prior_count,
                            reduced_tool_count=len(active_tool_specs),
                            provider_id=getattr(provider, "provider_id", "unknown"),
                            error=_exception_chain_text(e)[:500],
                        )
                        continue
                    if _is_transient_provider_error(e):
                        transient_tool_failures += 1
                        if transient_tool_failures >= 3:
                            logger.warning(
                                "Tool completion repeatedly failed with transient provider errors; falling back to plain completion",
                                failures=transient_tool_failures,
                                error=_exception_chain_text(e)[:500],
                            )
                            messages.append(
                                Message(
                                    role="system",
                                    content=(
                                        "Tool calling is temporarily unavailable due to provider instability. "
                                        "Reply without tools, summarize status, and ask for retry only if needed."
                                    ),
                                )
                            )
                            fallback_text = await _complete_text(
                                provider,
                                messages,
                                context="transient_tool_error_fallback",
                            )
                            return await _finalize_response(
                                provider=provider,
                                messages=messages,
                                response_text=fallback_text,
                                internal_followup=internal_followup,
                            )
                        delay_s = min(4.0, 0.8 * (2 ** (transient_tool_failures - 1)))
                        logger.warning(
                            "Transient provider error during tool completion; retrying",
                            failure_count=transient_tool_failures,
                            retry_delay_s=round(delay_s, 2),
                            error=_exception_chain_text(e)[:500],
                        )
                        await asyncio.sleep(delay_s)
                        continue
                    raise e

                content_raw = result.get("content", "")
                tool_calls = result.get("tool_calls") or []
                if not tool_calls and content_raw:
                    recovered_call = _recover_textual_tool_call(content_raw, active_tool_specs)
                    if recovered_call is not None:
                        logger.warning(
                            "Recovered textual tool invocation from model content",
                            tool_name=recovered_call.get("function", {}).get("name"),
                            raw_content=str(content_raw)[:200],
                        )
                        tool_calls = [recovered_call]

                if tool_calls:
                    had_tool_calls = True
                    assistant_msg: dict[str, Any] = {"role": "assistant", "tool_calls": tool_calls}
                    if content_raw:
                        assistant_msg["content"] = content_raw
                    messages.append(assistant_msg)

                    for call in tool_calls:
                        tool_result, tool_meta = await _handle_octo_tool_call(call, active_tool_specs, ctx)
                        expanded_names: list[str] = []
                        if str(call.get("function", {}).get("name") or "") == "tool_catalog_search":
                            active_tool_specs, expanded_names = _expand_active_tool_specs_from_catalog_result(
                                tool_result,
                                active_tool_specs=active_tool_specs,
                                ctx=ctx,
                            )
                            tools = [spec.to_openai_tool() for spec in active_tool_specs]
                            if expanded_names:
                                messages.append(
                                    Message(
                                        role="system",
                                        content=(
                                            "Tool catalog expansion complete. The following tools are now active for this turn:\n"
                                            + "\n".join(f"- {name}" for name in expanded_names)
                                            + "\nUse them directly if they fit the task."
                                        ),
                                    )
                                )
                        tool_result_text = render_tool_result_for_llm(tool_result).text
                        loop_state = _record_octo_tool_call(
                            tool_call_history,
                            call=call,
                            tool_result=tool_result,
                            tool_meta=tool_meta,
                            thresholds=tool_loop_thresholds,
                        )
                        messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": call.get("id"),
                                "name": call.get("function", {}).get("name"),
                                "content": tool_result_text,
                            }
                        )
                        if loop_state is not None:
                            logger.warning(
                                "Octo tool loop detected",
                                detector=loop_state["detector"],
                                level=loop_state["level"],
                                count=loop_state["count"],
                                message=loop_state["message"],
                            )
                            if loop_state["level"] == "critical":
                                messages.append(
                                    Message(
                                        role="system",
                                        content=(
                                            "Tool loop breaker triggered. Do not call more tools in this turn. "
                                            "Summarize what happened, what is blocked, and what the next best step is."
                                        ),
                                    )
                                )
                                fallback_text = await _complete_text(
                                    provider,
                                    messages,
                                    context="octo_tool_loop_breaker",
                                )
                                return await _finalize_response(
                                    provider=provider,
                                    messages=messages,
                                    response_text=fallback_text,
                                    internal_followup=internal_followup,
                                )
                        if "error" in tool_result_text.lower() or "failed" in tool_result_text.lower():
                            last_error = tool_result_text
                    continue

                if content_raw:
                    logger.debug("Octo output", output=content_raw)
                    return await _finalize_response(
                        provider=provider,
                        messages=messages,
                        response_text=content_raw,
                        internal_followup=internal_followup,
                    )

                if had_tool_calls:
                    logger.warning(
                        "Tool execution completed without a final assistant response; falling back to text completion",
                    )
                    messages.append(
                        Message(
                            role="system",
                            content=(
                                "You have already used tools for this turn, but your last response was empty. "
                                "Reply to the user now with a concise plain-language status update or answer."
                            ),
                        )
                    )
                    fallback_text = await _complete_text(
                        provider,
                        messages,
                        context="empty_tool_response_fallback",
                    )
                    return await _finalize_response(
                        provider=provider,
                        messages=messages,
                        response_text=fallback_text,
                        internal_followup=internal_followup,
                    )

                return await _finalize_response(
                    provider=provider,
                    messages=messages,
                    response_text=content_raw,
                    internal_followup=internal_followup,
                )

            if had_tool_calls:
                if internal_followup:
                    return "NO_USER_RESPONSE"
                # Force a final response without tools to explain progress.
                messages.append(
                    Message(
                        role="system",
                        content="You have reached the tool call limit for this turn. Summarize what you have initiated and let the user know you are processing their request.",
                    )
                )
                final_resp = await _complete_text(
                    provider,
                    messages,
                    context="tool_limit_fallback",
                )
                return await _finalize_response(
                    provider=provider,
                    messages=messages,
                    response_text=final_resp,
                    internal_followup=internal_followup,
                )

            if last_error and _looks_like_tool_error(last_error):
                if internal_followup:
                    return "NO_USER_RESPONSE"
                messages.append(
                    Message(
                        role="system",
                        content=f"A tool call failed: {last_error}. Explain the problem to the user naturally and ask for guidance if needed.",
                    )
                )
                final_resp = await _complete_text(
                    provider,
                    messages,
                    context="tool_error_fallback",
                )
                return await _finalize_response(
                    provider=provider,
                    messages=messages,
                    response_text=final_resp,
                    internal_followup=internal_followup,
                )

            return ""

        response_raw = await _complete_text(
            provider,
            messages,
            context="plain_completion",
            on_partial=partial_callback,
        )
        logger.debug("Octo output", output=response_raw)
        return await _finalize_response(
            provider=provider,
            messages=messages,
            response_text=response_raw,
            internal_followup=internal_followup,
        )
    except Exception:
        logger.exception("Error in route_or_reply")
        raise
    finally:
        await octo.set_thinking(False)
        if chat_id > 0 and show_typing:
            logger.debug("Toggling typing indicator off", chat_id=chat_id)
            await octo.set_typing(chat_id, False)


async def route_worker_result_back_to_octo(
    octo: Any,
    chat_id: int,
    task_text: str,
    result: WorkerResult,
) -> str:
    """Decide next steps after a worker completes its task."""
    output_summary = result.output
    output_truncated = False
    available_keys = []

    if isinstance(result.output, dict):
        available_keys = list(result.output.keys())
        if len(json.dumps(result.output)) > 64000:
            output_summary = {k: f"<{type(v).__name__}>" for k, v in result.output.items()}
            output_truncated = True

    payload = {
        "task": task_text,
        "summary": result.summary,
        "output": output_summary,
        "output_truncated": output_truncated,
        "available_keys": available_keys,
        "questions": result.questions,
        "knowledge_proposals": [p.model_dump() for p in result.knowledge_proposals],
        "tools_used": result.tools_used,
    }
    payload_json = json.dumps(payload, ensure_ascii=False)

    worker_result_prompt = (
        "Worker completed. Decide and execute next action based on this payload.\n"
        "<worker_result>\n"
        f"{payload_json}\n"
        "</worker_result>\n\n"
        "Interpretation rules:\n"
        "- `summary` is internal worker/runtime text and is not user-facing by default.\n"
        "- Never forward transport/debug/auth/orchestration text to the user.\n"
        "- If you answer the user, write a clean Octo response in plain language.\n\n"
        "If the output is truncated and you need specific details, use `get_worker_output_path`.\n"
        "If there are knowledge_proposals, review them and use `manage_canon` to save them if valid.\n"
        "If a user-facing response is required now, provide it in plain text.\n"
        "If no user-facing response is needed, return exactly: NO_USER_RESPONSE"
    )

    bootstrap_context = await build_bootstrap_context_prompt(octo.store, chat_id)
    reply_text = await route_or_reply(
        octo,
        octo.provider,
        octo.memory,
        worker_result_prompt,
        chat_id,
        bootstrap_context.content,
        internal_followup=True,
    )
    return normalize_plain_text(reply_text)


def should_send_worker_followup(text: str) -> bool:
    """Determine if a worker follow-up should be sent to the user."""
    return resolve_user_delivery(text).user_visible


def should_force_worker_followup(result: WorkerResult) -> bool:
    """Return True when a completed worker result is substantive enough to surface."""
    summary = (result.summary or "").strip()
    if not summary:
        return False

    if len(summary) >= 160:
        return True

    if result.questions or result.knowledge_proposals:
        return True

    if len(result.tools_used or []) >= 2:
        return True

    output = result.output
    if isinstance(output, dict):
        interesting_keys = {
            "path",
            "file",
            "files",
            "report",
            "report_path",
            "output_path",
            "results",
            "items",
            "jobs",
            "posts",
            "articles",
        }
        if interesting_keys.intersection(output.keys()):
            return True

    return False


def build_forced_worker_followup(result: WorkerResult) -> str:
    """Build a concise Octo-style fallback when routing suppresses a useful update."""
    lead = _build_generic_worker_completion_message(result)
    if lead == "Task finished.":
        return ""

    if len(lead) > 700:
        lead = lead[:697].rstrip() + "..."

    parts = [lead]
    if result.questions:
        questions = [q.strip() for q in result.questions[:3] if q and q.strip()]
        if questions:
            parts.append("\n".join(f"- {question}" for question in questions))
    return "\n\n".join(parts).strip()


def _build_generic_worker_completion_message(result: WorkerResult) -> str:
    output = result.output if isinstance(result.output, dict) else {}
    for key in ("report_path", "output_path", "path", "file"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            return f"Task finished. Output is ready in `{value.strip()}`."
    files = output.get("files")
    if isinstance(files, list):
        visible_files = [str(item).strip() for item in files if str(item).strip()]
        if visible_files:
            return f"Task finished. Created {len(visible_files)} file(s)."
    if result.questions:
        return "Task finished. I need your input on the next step."
    return "Task finished."


def normalize_plain_text(text: str) -> str:
    return sanitize_user_facing_text(text or "")


def _looks_like_tool_error(text: str) -> bool:
    lowered = text.lower()
    return " error" in lowered or "failed" in lowered


def _log_system_prompt(messages: list[Message], label: str) -> None:
    system_lengths = [len(m.content) for m in messages if m.role == "system" and m.content]
    if system_lengths:
        logger.debug(
            "Octo system prompt",
            label=label,
            parts=len(system_lengths),
            total_chars=sum(system_lengths),
        )


def _get_octo_tools(octo: Any, chat_id: int) -> tuple[list[ToolSpec], dict[str, object]]:
    perms = {
        "filesystem_read": True,
        "filesystem_write": True,
        "worker_manage": True,
        "llm_subtask": True,
        "canon_manage": True,
        "network": True,
        "exec": True,
        "service_read": True,
        "service_control": True,
        "deploy_control": True,
        "db_admin": True,
        "security_audit": True,
        "self_control": True,
        "mcp_exec": True,
        "skill_use": True,
        "skill_exec": True,
        "skill_manage": True,
    }
    ctx = {
        "base_dir": Path(os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")).resolve(),
        "octo": octo,
        "chat_id": chat_id
    }
    mcp_manager = getattr(octo, "mcp_manager", None)
    policy_steps = [
        ToolPolicyPipelineStep(
            label="octo.raw_fetch_denylist",
            policy=ToolPolicy(deny=["web_fetch", "markdown_new_fetch", "fetch_plan_tool"]),
        )
    ]
    all_tools = get_tools(mcp_manager=mcp_manager)
    resolution_report = resolve_tool_diagnostics(
        all_tools,
        permissions=perms,
        profile_name=os.getenv("OCTOPAL_OCTO_TOOL_PROFILE"),
        policy_pipeline_steps=policy_steps,
    )
    tool_specs = _ensure_mandatory_octo_tools(
        list(resolution_report.available_tools),
        all_tools,
    )
    max_tools = _env_int("OCTOPAL_OCTO_MAX_TOOL_COUNT", _DEFAULT_MAX_TOOL_COUNT, minimum=8)
    tool_specs = _budget_tool_specs(tool_specs, max_count=max_tools)
    ctx["active_tool_specs"] = tool_specs
    ctx["tool_resolution_report"] = resolution_report
    ctx["all_tool_specs"] = all_tools
    return tool_specs, ctx


def _ensure_mandatory_octo_tools(active_tools: list[ToolSpec], all_tools: list[ToolSpec]) -> list[ToolSpec]:
    by_name = {str(spec.name): spec for spec in active_tools}
    for spec in all_tools:
        name = str(spec.name)
        if name in _MANDATORY_OCTO_TOOL_NAMES and name not in by_name:
            by_name[name] = spec
    return list(by_name.values())


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return max(minimum, value)


def _is_context_overflow_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "maximum context length",
            "input tokens exceeds",
            "context length",
            "too many tokens",
        )
    )


def _exception_chain_text(exc: Exception) -> str:
    parts: list[str] = []
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen and len(parts) < 8:
        seen.add(id(current))
        text = str(current).strip()
        if text:
            parts.append(text)
        current = current.__cause__ or current.__context__
    return " | ".join(parts)


def _is_transient_provider_error(exc: Exception) -> bool:
    text = _exception_chain_text(exc).lower()
    return any(
        marker in text
        for marker in (
            "timeout",
            "timed out",
            "sockettimeout",
            "apitimeouterror",
            "rate limit",
            "ratelimit",
            "429",
            "502",
            "503",
            "504",
            "service unavailable",
            "connection error",
            "connection reset",
            "client has been closed",
            "apiconnectionerror",
            "temporary",
            "temporarily unavailable",
        )
    )


def _tool_priority(spec: ToolSpec) -> tuple[int, str]:
    name = str(getattr(spec, "name", "") or "")
    if name in _PRIORITY_TOOL_NAMES:
        return (0, name)
    if _is_connector_tool(spec):
        return (1, name)
    return (2, name)


def _is_connector_tool(spec: ToolSpec) -> bool:
    metadata = getattr(spec, "metadata", None)
    category = str(getattr(metadata, "category", "") or "").strip().lower()
    if category == "connectors":
        return True

    name = str(getattr(spec, "name", "") or "").strip().lower()
    return name.startswith(("gmail_", "calendar_", "drive_", "connector_"))


def _budget_tool_specs(tool_specs: list[ToolSpec], *, max_count: int) -> list[ToolSpec]:
    if len(tool_specs) <= max_count:
        return tool_specs
    prioritized = sorted(tool_specs, key=_tool_priority)
    always = [spec for spec in prioritized if str(getattr(spec, "name", "")) in _ALWAYS_INCLUDE_TOOL_NAMES]

    selected: list[ToolSpec] = list(always)
    selected_names = {str(getattr(spec, "name", "")) for spec in selected}
    remaining_budget = max_count - len(selected)

    if remaining_budget > 0:
        for spec in prioritized:
            name = str(getattr(spec, "name", ""))
            if name in selected_names:
                continue
            selected.append(spec)
            selected_names.add(name)
            if len(selected) >= max_count:
                break

    return selected


def _shrink_tool_specs_for_retry(tool_specs: list[ToolSpec]) -> list[ToolSpec]:
    if len(tool_specs) <= _MIN_TOOL_COUNT_ON_OVERFLOW:
        return tool_specs
    reduced = max(_MIN_TOOL_COUNT_ON_OVERFLOW, int(len(tool_specs) * 0.7))
    return _budget_tool_specs(tool_specs, max_count=reduced)


def _expand_active_tool_specs_from_catalog_result(
    tool_result: Any,
    *,
    active_tool_specs: list[ToolSpec],
    ctx: dict[str, object],
) -> tuple[list[ToolSpec], list[str]]:
    payload = tool_result if isinstance(tool_result, dict) else {}
    if isinstance(tool_result, str):
        try:
            parsed = json.loads(tool_result)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            payload = parsed
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results:
        return active_tool_specs, []

    all_specs = list(ctx.get("all_tool_specs") or [])
    by_name = {str(getattr(spec, "name", "") or ""): spec for spec in all_specs}
    selected = list(active_tool_specs)
    selected_names = {str(getattr(spec, "name", "") or "") for spec in selected}

    expanded_names: list[str] = []
    for item in results:
        if len(expanded_names) >= _CATALOG_TOOL_EXPANSION_LIMIT:
            break
        if not isinstance(item, dict):
            continue
        if bool(item.get("active_now")):
            continue
        name = str(item.get("name", "") or "").strip()
        if not name or name in selected_names:
            continue
        spec = by_name.get(name)
        if spec is None:
            continue
        selected.append(spec)
        selected_names.add(name)
        expanded_names.append(name)

    if expanded_names:
        ctx["active_tool_specs"] = selected
    return selected, expanded_names


async def _handle_octo_tool_call(
    call: dict,
    tools: list[ToolSpec],
    ctx: dict[str, object],
) -> tuple[Any, dict[str, Any]]:
    function = call.get("function") or {}
    name = function.get("name")
    args_raw = function.get("arguments", "{}")
    try:
        args = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
    except Exception:
        args = {}

    logger.debug("Octo tool call", tool_name=name, args=args)
    for spec in tools:
        if spec.name == name:
            try:
                if spec.is_async:
                    import inspect

                    result = spec.handler(args, ctx)
                    if inspect.isawaitable(result):
                        result = await result
                else:
                    result = await asyncio.to_thread(spec.handler, args, ctx)
            except Exception as exc:
                logger.exception("Octo tool execution failed", tool_name=name)
                return {
                    "error": f"Tool execution failed: {name}: {exc}"
                }, {"timed_out": False, "had_error": True}
            logger.debug("Octo tool result", tool_name=name, result_preview=f"{str(result)[:200]}...")
            return result, {"timed_out": False, "had_error": False}
    blocked_payload = _resolve_octo_policy_block(tool_name=str(name or ""), ctx=ctx)
    if blocked_payload is not None:
        return blocked_payload, {"timed_out": False, "had_error": True, "error_type": "policy_block"}
    return {"error": f"Unknown tool: {name}"}, {"timed_out": False, "had_error": True}


def _record_octo_tool_call(
    history: list[dict[str, str]],
    *,
    call: dict[str, Any],
    tool_result: Any,
    tool_meta: dict[str, Any],
    thresholds: dict[str, int],
) -> dict[str, Any] | None:
    function = call.get("function") or {}
    tool_name = str(function.get("name") or "")
    args_raw = function.get("arguments", "{}")
    try:
        tool_args = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
    except Exception:
        tool_args = {}

    args_hash = _hash_tool_call(tool_name, tool_args)
    result_hash = _hash_tool_outcome(tool_result, tool_meta)
    history.append(
        {
            "tool_name": tool_name,
            "args_hash": args_hash,
            "result_hash": result_hash,
        }
    )
    return _detect_tool_loop(
        history,
        tool_name=tool_name,
        args_hash=args_hash,
        warning_threshold=thresholds["warning"],
        critical_threshold=thresholds["critical"],
        global_breaker_threshold=thresholds["global_breaker"],
    )


def _build_octo_tool_policy_summary(
    active_tools: list[ToolSpec],
    report: ToolResolutionReport | None,
) -> str:
    available_counts = {"safe": 0, "guarded": 0, "dangerous": 0}
    for spec in active_tools:
        available_counts[str(spec.metadata.risk)] = available_counts.get(str(spec.metadata.risk), 0) + 1

    blocked_dangerous = 0
    blocked_guarded = 0
    if report is not None:
        for entry in report.blocked_tools:
            risk = str(entry.tool.metadata.risk)
            if risk == "dangerous":
                blocked_dangerous += 1
            elif risk == "guarded":
                blocked_guarded += 1

    return (
        "Tool policy contract:\n"
        "- Use safe tools by default.\n"
        "- Use guarded tools only when they materially advance the task.\n"
        "- Do not choose dangerous tools as the first path, even if available.\n"
        "- If a tool is blocked by policy, do not repeat the same call; choose a safer alternative or explain the constraint.\n"
        "- Do not bypass a blocked tool with an equivalent risky workaround.\n"
        "Current tool policy snapshot:\n"
        f"- active_safe={available_counts['safe']}\n"
        f"- active_guarded={available_counts['guarded']}\n"
        f"- active_dangerous={available_counts['dangerous']}\n"
        f"- blocked_guarded={blocked_guarded}\n"
        f"- blocked_dangerous={blocked_dangerous}"
    )


def _resolve_octo_policy_block(tool_name: str, ctx: dict[str, object]) -> dict[str, Any] | None:
    normalized_name = str(tool_name or "").strip().lower()
    if not normalized_name:
        return None

    report = ctx.get("tool_resolution_report")
    if not isinstance(report, ToolResolutionReport):
        return None

    for entry in report.blocked_tools:
        if str(entry.tool.name).strip().lower() != normalized_name:
            continue
        return {
            "type": "policy_block",
            "tool": entry.tool.name,
            "reason": entry.reasons[0] if entry.reasons else "blocked_by_policy",
            "risk": entry.tool.metadata.risk,
            "message": f"Tool '{entry.tool.name}' is blocked by the current Octo tool policy.",
            "hint": _policy_block_hint(entry.tool),
        }
    return None


def _policy_block_hint(tool: ToolSpec) -> str:
    risk = str(tool.metadata.risk)
    if risk == "dangerous":
        return "Try a safer read-only or worker-driven path first, then explain what remains blocked."
    if risk == "guarded":
        return "Use a lower-risk alternative if one exists, or explain why the guarded path matters."
    return "Use another available tool path."


def _recover_textual_tool_call(content: str, tools: list[ToolSpec]) -> dict[str, Any] | None:
    """Recover a malformed tool invocation when the model emits tool syntax as plain text."""
    raw = normalize_plain_text(content or "")
    if not raw or "\n" in raw or len(raw) > 300:
        return None

    trimmed = re.sub(r"^[\s\W_]+", "", raw, flags=re.UNICODE)
    trimmed = re.sub(r"[\s\W_]+$", "", trimmed, flags=re.UNICODE).strip()
    if not trimmed:
        return None

    tool_by_name = {str(spec.name).lower(): spec for spec in tools}

    if _TEXTUAL_TOOL_NAME_RE.fullmatch(trimmed):
        spec = tool_by_name.get(trimmed.lower())
        if spec is None:
            return None
        required = _required_tool_fields(spec)
        if required:
            return None
        return {
            "id": f"recovered-{spec.name}",
            "type": "function",
            "function": {"name": spec.name, "arguments": "{}"},
        }

    match = _TEXTUAL_TOOL_PREVIEW_RE.fullmatch(trimmed)
    if not match:
        return None

    spec = tool_by_name.get(str(match.group("tool") or "").lower())
    if spec is None:
        return None

    args = _parse_textual_tool_preview_args(match.group("rest") or "", spec)
    if args is None:
        return None

    required = _required_tool_fields(spec)
    if any(field not in args for field in required):
        return None

    return {
        "id": f"recovered-{spec.name}",
        "type": "function",
        "function": {"name": spec.name, "arguments": json.dumps(args, ensure_ascii=False)},
    }


def _parse_textual_tool_preview_args(preview: str, spec: ToolSpec) -> dict[str, Any] | None:
    args: dict[str, Any] = {}
    properties = ((spec.parameters or {}).get("properties") or {}) if isinstance(spec.parameters, dict) else {}
    alias_map = {"file": "path"}

    for chunk in preview.split(","):
        piece = chunk.strip()
        if not piece or ":" not in piece:
            continue
        key_raw, value_raw = piece.split(":", 1)
        key = key_raw.strip().lower().replace(" ", "_")
        value = value_raw.strip()
        if not key or not value:
            continue
        key = alias_map.get(key, key)
        if properties and key not in properties:
            return None
        args[key] = value

    return args or None


def _required_tool_fields(spec: ToolSpec) -> set[str]:
    params = spec.parameters if isinstance(spec.parameters, dict) else {}
    required = params.get("required") or []
    if not isinstance(required, list):
        return set()
    return {str(item) for item in required if str(item).strip()}


async def _build_plan(
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    has_tools: bool,
) -> dict[str, Any] | None:
    planning_prompt = (
        "Create a brief execution plan for this turn. Return JSON only with keys: "
        '{"mode":"execute|reply","steps":["..."],"response":"..."}.\n'
        "- Use mode=reply when no tools/workers are needed and a direct answer is sufficient.\n"
        "- Use mode=execute when tools/workers are needed; provide 1-8 concrete steps.\n"
        "- If mode=reply, include response.\n"
        "- If mode=execute, response is optional."
    )
    planner_messages = list(messages) + [Message(role="system", content=planning_prompt)]
    try:
        raw = await _complete_text(provider, planner_messages, context="planner")
    except Exception:
        logger.debug("Planner step skipped due to provider error", exc_info=True)
        return None

    payload = _extract_json_object(raw)
    if not isinstance(payload, dict):
        return None
    return _normalize_plan_payload(payload, has_tools)


async def _persist_plan(memory: MemoryService, chat_id: int, plan: dict[str, Any]) -> None:
    mode = str(plan.get("mode", "execute"))
    steps = [str(step) for step in plan.get("steps", []) if str(step).strip()]
    response = str(plan.get("response", "")).strip()
    plan_summary = (
        f"Planner mode={mode}; steps={len(steps)}"
        + (f"; response_len={len(response)}" if response else "")
    )
    try:
        await memory.add_message(
            "system",
            plan_summary,
            {
                "chat_id": chat_id,
                "planner": True,
                "mode": mode,
                "steps": steps,
            },
        )
    except Exception:
        logger.debug("Failed to persist planner trace", exc_info=True)


def _extract_json_object(raw: str) -> dict[str, Any] | None:
    if not raw:
        return None
    candidates = [raw.strip()]
    stripped = raw.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3 and lines[-1].strip() == "```":
            candidates.append("\n".join(lines[1:-1]).strip())
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            continue
    return None


def _normalize_plan_payload(payload: dict[str, Any], has_tools: bool) -> dict[str, Any] | None:
    mode = str(payload.get("mode", "execute")).strip().lower()
    steps_raw = payload.get("steps")
    steps: list[str] = []
    if isinstance(steps_raw, list):
        steps = [str(step).strip() for step in steps_raw if str(step).strip()]
    response = str(payload.get("response", "")).strip()

    if mode not in {"reply", "execute"}:
        mode = "execute"

    if mode == "reply":
        if not response:
            return None
        return {"mode": "reply", "response": response, "steps": []}

    # If no tools are available and planner requested execute, degrade to reply when possible.
    if not has_tools and response:
        return {"mode": "reply", "response": response, "steps": []}

    if not steps:
        return None
    return {"mode": "execute", "steps": steps[:_MAX_PLAN_STEPS], "response": response}


async def _finalize_response(
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    response_text: str,
    *,
    internal_followup: bool,
) -> str:
    cleaned = sanitize_user_facing_text_preserving_reaction(response_text or "")
    if not cleaned:
        return cleaned
    _, cleaned_visible_text = extract_reaction_and_strip(cleaned)
    if looks_like_textual_tool_invocation(cleaned_visible_text):
        logger.warning("Final response collapsed to textual tool invocation; attempting rewrite", preview=cleaned[:120])
        rewrite_messages = list(messages)
        rewrite_messages.append(
            Message(
                role="system",
                content=(
                    "Your previous draft collapsed into a tool invocation or tool syntax. "
                    "Rewrite it now as a plain-language final response. "
                    "Do not output a tool name by itself. Do not output tool syntax. "
                    "If no user-visible response is needed, return exactly NO_USER_RESPONSE."
                ),
            )
        )
        rewritten = sanitize_user_facing_text_preserving_reaction(
            await _complete_text(
                provider,
                rewrite_messages,
                context="rewrite_textual_tool_invocation",
            )
        )
        _, rewritten_visible_text = extract_reaction_and_strip(rewritten)
        if rewritten and not looks_like_textual_tool_invocation(rewritten_visible_text):
            return rewritten
        return "NO_USER_RESPONSE"
    return cleaned


async def _verify_final_response(
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    candidate: str,
) -> str:
    context = _messages_to_text(messages)
    prompt = (
        "You are a strict response verifier. Compare the assistant draft response against the evidence context.\n"
        "Return JSON only with keys:\n"
        '{"verdict":"approved|revised|insufficient_evidence","response":"...","missing_evidence":["..."],"confidence":0.0}\n'
        "Rules:\n"
        "- approved: draft is well-supported.\n"
        "- revised: rewrite conservatively to match evidence.\n"
        "- insufficient_evidence: if claims are not backed; provide a short user-facing follow-up request.\n"
        "- Do not invent new facts."
        "\n\n<EVIDENCE>\n"
        f"{context}\n"
        "</EVIDENCE>\n\n"
        "<DRAFT_RESPONSE>\n"
        f"{candidate}\n"
        "</DRAFT_RESPONSE>"
    )
    try:
        raw = await _complete_text(
            provider,
            [Message(role="system", content=prompt)],
            context="verifier",
        )
    except Exception:
        logger.debug("Verifier step skipped due to provider error", exc_info=True)
        return candidate

    payload = _extract_json_object(raw)
    normalized = _normalize_verification_payload(payload)
    if not normalized:
        return candidate

    verdict = normalized["verdict"]
    confidence = normalized["confidence"]
    if verdict == "approved" and confidence >= 0.45:
        return candidate
    if verdict == "revised" and normalized["response"]:
        return normalized["response"]
    if verdict == "insufficient_evidence":
        return _build_insufficient_evidence_response(normalized, candidate)
    return candidate


def _messages_to_text(messages: list[Message | dict[str, Any]], max_chars: int = _MAX_VERIFY_CONTEXT_CHARS) -> str:
    lines: list[str] = []
    for msg in messages[-14:]:
        if isinstance(msg, Message):
            role = msg.role
            content = msg.content
        else:
            role = str(msg.get("role", "unknown"))
            content = msg.get("content", "")
        if isinstance(content, list):
            safe_content = json.dumps(content, ensure_ascii=False)
        else:
            safe_content = str(content)
        if safe_content:
            lines.append(f"{role}: {safe_content}")
    merged = "\n".join(lines)
    if len(merged) > max_chars:
        return merged[-max_chars:]
    return merged


def _normalize_verification_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    verdict = str(payload.get("verdict", "")).strip().lower()
    if verdict not in {"approved", "revised", "insufficient_evidence"}:
        return None
    response = str(payload.get("response", "")).strip()
    missing = payload.get("missing_evidence") or []
    if not isinstance(missing, list):
        missing = []
    missing = [str(item).strip() for item in missing if str(item).strip()]
    confidence_raw = payload.get("confidence", 0.0)
    try:
        confidence = float(confidence_raw)
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    return {
        "verdict": verdict,
        "response": response,
        "missing_evidence": missing,
        "confidence": confidence,
    }


def _build_insufficient_evidence_response(payload: dict[str, Any], candidate: str) -> str:
    response = payload.get("response", "").strip()
    if response:
        lower = response.lower()
        technical_markers = (
            "provided evidence",
            "cannot confidently verify",
            "draft includes details",
            "not supported by",
        )
        if not any(marker in lower for marker in technical_markers):
            return response
    missing = payload.get("missing_evidence") or []
    if missing:
        return (
            "I may be missing enough evidence to confirm this fully. "
            f"Could you share or confirm: {missing[0]}?"
        )
    return (
        "I may be missing enough evidence to give a confident answer yet. "
        "If you want, I can run an additional targeted check."
    )


async def _complete_text(
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    *,
    context: str,
    on_partial: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    sanitized = _sanitize_messages_for_complete(messages)
    try:
        if callable(on_partial):
            stream_callable = getattr(provider, "complete_stream", None)
            if callable(stream_callable):
                return await stream_callable(sanitized, on_partial=on_partial)
        text = await provider.complete(sanitized)
        if callable(on_partial) and text:
            try:
                await on_partial(text)
            except Exception:
                logger.debug("Partial callback failed on non-stream completion", context=context, exc_info=True)
        return text
    except Exception:
        logger.debug(
            "Text completion failed after sanitization",
            context=context,
            message_shape=_message_shape(sanitized),
            exc_info=True,
        )
        raise


def _sanitize_messages_for_complete(messages: list[Message | dict[str, Any]]) -> list[dict[str, str]]:
    sanitized: list[dict[str, str]] = []
    for msg in messages:
        role: str
        content: Any
        tool_name = ""
        if isinstance(msg, Message):
            role = msg.role
            content = msg.content
        else:
            role = str(msg.get("role", "assistant"))
            content = msg.get("content", "")
            if role == "tool":
                tool_name = str(msg.get("name", "") or msg.get("tool_name", "") or "")

        normalized_role = role if role in {"system", "user", "assistant"} else "assistant"
        if role == "tool":
            normalized_content = _coerce_tool_message_to_text(content, tool_name=tool_name)
            if not normalized_content:
                continue
            sanitized.append({"role": "assistant", "content": normalized_content})
            continue

        normalized_content = _coerce_content_to_text(content)
        if not normalized_content:
            continue

        sanitized.append({"role": normalized_role, "content": normalized_content})

    if not sanitized:
        sanitized.append({"role": "user", "content": "Continue."})
    elif not any(msg.get("role") == "user" for msg in sanitized):
        sanitized.append(
            {
                "role": "user",
                "content": "Please follow the instructions above and provide the best supported response.",
            }
        )
    return sanitized


def _coerce_content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_parts: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type", "")).lower()
            if item_type == "text":
                text_val = str(item.get("text", "")).strip()
                if text_val:
                    text_parts.append(text_val)
            elif item_type == "image_url":
                text_parts.append("[image omitted for text-only completion]")
        return "\n".join(text_parts).strip()
    try:
        return json.dumps(content, ensure_ascii=False)
    except Exception:
        return str(content or "")


def _coerce_tool_message_to_text(content: Any, *, tool_name: str = "") -> str:
    rendered = render_tool_result_for_llm(content, max_chars=16000).text
    if not rendered:
        return ""
    label = tool_name.strip() or "tool"
    return f"Tool result ({label}): {rendered}"


def _message_shape(messages: list[dict[str, str]]) -> list[dict[str, Any]]:
    shape: list[dict[str, Any]] = []
    for msg in messages[:24]:
        content = msg.get("content", "")
        shape.append(
            {
                "role": msg.get("role"),
                "content_type": type(content).__name__,
                "content_len": len(content) if isinstance(content, str) else None,
            }
        )
    return shape


def _build_partial_callback(
    *,
    octo: Any,
    chat_id: int,
) -> Callable[[str], Awaitable[None]] | None:
    if chat_id <= 0 or not getattr(octo, "is_ws_active", False):
        return None
    sender = getattr(octo, "internal_progress_send", None)
    if not callable(sender):
        return None

    async def _on_partial(text: str) -> None:
        clean = normalize_plain_text(text or "")
        if not clean:
            return
        if should_suppress_user_delivery(clean):
            return
        try:
            await sender(chat_id, "partial", clean, {})
        except Exception:
            logger.debug("Failed to emit partial stream", chat_id=chat_id, exc_info=True)

    return _on_partial
