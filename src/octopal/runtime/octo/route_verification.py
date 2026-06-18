from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from octopal.infrastructure.providers.base import InferenceProvider, Message
from octopal.runtime.octo.route_loop_helpers import normalize_plain_text
from octopal.runtime.octo.route_planning import _extract_json_object
from octopal.utils import (
    extract_reaction_and_strip,
    looks_like_textual_tool_invocation,
    sanitize_user_facing_text_preserving_reaction,
    should_suppress_user_delivery,
)

logger = structlog.get_logger(__name__)

_MAX_VERIFY_CONTEXT_CHARS = 20000
_CompleteTextFn = Callable[..., Awaitable[str]]


async def _finalize_response(
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    response_text: str,
    *,
    internal_followup: bool,
    preserve_user_visible_wrapper: bool = False,
    complete_text_fn: _CompleteTextFn,
) -> str:
    del internal_followup
    cleaned = (
        _sanitize_control_plane_contract_text(response_text or "")
        if preserve_user_visible_wrapper
        else sanitize_user_facing_text_preserving_reaction(response_text or "")
    )
    if not cleaned:
        return cleaned
    _, cleaned_visible_text = extract_reaction_and_strip(cleaned)
    if looks_like_textual_tool_invocation(cleaned_visible_text):
        logger.warning(
            "Final response collapsed to textual tool invocation; attempting rewrite",
            preview=cleaned[:120],
        )
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
            await complete_text_fn(
                provider,
                rewrite_messages,
                context="rewrite_textual_tool_invocation",
            )
        )
        _, rewritten_visible_text = extract_reaction_and_strip(rewritten)
        if rewritten and not looks_like_textual_tool_invocation(rewritten_visible_text):
            return rewritten
        return "NO_USER_RESPONSE"
    if _messages_include_runtime_state_context(messages):
        cleaned = await _review_runtime_state_user_response(
            provider=provider,
            messages=messages,
            candidate=cleaned,
            complete_text_fn=complete_text_fn,
        )
    return cleaned


def _sanitize_control_plane_contract_text(text: str) -> str:
    """Strip hidden/tool traces while preserving explicit control-plane wrappers."""
    if not text:
        return ""
    cleaned = str(text).replace("\r\n", "\n").replace("\r", "\n")
    cleaned = re.sub(r"<think>.*?</think>", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(
        r"<(tool_call|tool_code|tool_result).*?>.*?</\1>",
        "",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL,
    )
    cleaned = re.sub(
        r"(?:^|\n)\s*Tool result \([^)]+\):\s*(?:\{.*?\}|\[.*?\]|.+?)(?=\n|$)",
        "",
        cleaned,
        flags=re.DOTALL,
    )
    cleaned = re.sub(
        r"</?(?:tool_call|tool_code|tool_result|step|plan|thought).*?>",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned


async def _needs_action_or_blocked_retry(
    *,
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    candidate: str,
    complete_text_fn: _CompleteTextFn,
) -> bool:
    if not normalize_plain_text(candidate or "") or should_suppress_user_delivery(candidate):
        return False
    prompt = (
        "Classify whether the draft assistant response is safe to deliver as the final answer for this turn.\n"
        "Return JSON only with this shape:\n"
        '{"verdict":"final|requires_runtime_action_state","confidence":0.0,"reason":"short"}\n'
        "Use requires_runtime_action_state only when the draft tells the user that work will be done, is being done, "
        "or will be followed up later, while the evidence contains no completed tool call, worker launch, queued task, "
        "schedule change, or explicit blocked/clarifying answer. Use final for direct answers, questions, refusal/blocked "
        "answers, status summaries grounded in evidence, and normal conversational replies. Do not classify from keywords; "
        "judge the speech act and whether runtime state already supports it.\n\n"
        "<EVIDENCE>\n"
        f"{_messages_to_text(messages)}\n"
        "</EVIDENCE>\n\n"
        "<DRAFT_RESPONSE>\n"
        f"{candidate}\n"
        "</DRAFT_RESPONSE>"
    )
    try:
        raw = await complete_text_fn(
            provider,
            [Message(role="system", content=prompt)],
            context="action_state_verifier",
        )
    except Exception:
        logger.debug("Action-state verifier skipped due to provider error", exc_info=True)
        return False

    payload = _extract_json_object(raw)
    if not isinstance(payload, dict):
        return False
    verdict = str(payload.get("verdict") or "").strip().lower()
    try:
        confidence = float(payload.get("confidence", 0.0) or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    return verdict == "requires_runtime_action_state" and confidence >= 0.55


async def _needs_autonomous_recovery_retry(
    *,
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    candidate: str,
    complete_text_fn: _CompleteTextFn,
) -> bool:
    if not normalize_plain_text(candidate or "") or should_suppress_user_delivery(candidate):
        return False
    if len(re.findall(r"(?m)^\s*(?:\d+[\).]|[-*])\s+\S", candidate or "")) < 2:
        return False
    prompt = (
        "Classify whether a draft assistant response improperly delegates recoverable execution "
        "choices to the user after tools have already run or an execution plan is active.\n"
        "Return JSON only with this shape:\n"
        '{"verdict":"final|requires_autonomous_recovery","confidence":0.0,"reason":"short"}\n'
        "Use requires_autonomous_recovery when the draft lists alternative implementation/retry/"
        "diagnostic paths and asks the user to choose, continue, retry, or approve a generic next "
        "attempt, while the evidence shows the assistant still has safe available actions it can "
        "take itself. Prefer autonomous recovery for bounded retries, smaller scoped workers, "
        "state inspection, durable note/plan updates, or choosing a conservative default.\n"
        "Use final when the draft gives a completed result, a real blocked state, or asks for "
        "human-only input: missing credentials, destructive/risky approval, policy permission, "
        "private preference, ambiguous product choice with no safe default, or an external state "
        "change the assistant cannot perform. Do not classify from banned words; judge the speech "
        "act and whether the next step is recoverable without the user.\n\n"
        "<EVIDENCE>\n"
        f"{_messages_to_text(messages)}\n"
        "</EVIDENCE>\n\n"
        "<DRAFT_RESPONSE>\n"
        f"{candidate}\n"
        "</DRAFT_RESPONSE>"
    )
    try:
        raw = await complete_text_fn(
            provider,
            [Message(role="system", content=prompt)],
            context="autonomous_recovery_verifier",
        )
    except Exception:
        logger.debug("Autonomous-recovery verifier skipped due to provider error", exc_info=True)
        return False

    payload = _extract_json_object(raw)
    if not isinstance(payload, dict):
        return False
    verdict = str(payload.get("verdict") or "").strip().lower()
    try:
        confidence = float(payload.get("confidence", 0.0) or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    return verdict == "requires_autonomous_recovery" and confidence >= 0.55


def _messages_include_runtime_state_context(messages: list[Message | dict[str, Any]]) -> bool:
    runtime_tool_names = {
        "plan_create",
        "plan_status",
        "plan_update_step",
        "octo_continue_from_control_route",
        "execute_self_queue_item",
        "octo_self_queue_add",
        "octo_self_queue_update",
    }
    for message in messages[-12:]:
        if isinstance(message, dict):
            role = str(message.get("role") or "").strip().lower()
            name = str(message.get("name") or "").strip().lower()
            tool_calls = message.get("tool_calls") or []
        else:
            role = str(getattr(message, "role", "") or "").strip().lower()
            name = str(getattr(message, "name", "") or "").strip().lower()
            tool_calls = getattr(message, "tool_calls", None) or []
        if role == "tool" and name in runtime_tool_names:
            return True
        if not isinstance(tool_calls, list):
            continue
        for call in tool_calls:
            if not isinstance(call, dict):
                continue
            call_name = str((call.get("function") or {}).get("name") or "").strip().lower()
            if call_name in runtime_tool_names:
                return True
    return False


def _messages_include_execution_plan(messages: list[Message | dict[str, Any]]) -> bool:
    for message in messages[-12:]:
        if isinstance(message, dict):
            content = str(message.get("content") or "")
        else:
            content = str(getattr(message, "content", "") or "")
        if "<execution_plan>" in content and "</execution_plan>" in content:
            return True
    return False


async def _review_runtime_state_user_response(
    *,
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    candidate: str,
    complete_text_fn: _CompleteTextFn,
) -> str:
    if not normalize_plain_text(candidate or "") or should_suppress_user_delivery(candidate):
        return candidate
    prompt = (
        "Review a draft user-facing response that was generated after runtime-state tools were used.\n"
        "Return JSON only with this shape:\n"
        '{"verdict":"approved|revised","response":"...","confidence":0.0,"reason":"short"}\n'
        "Approve if the draft is a clean user-facing answer. Revise only when runtime/debug "
        "state, tool bookkeeping, plan metadata, or execution-contract narration is exposed "
        "as part of the answer. Preserve all useful user-level facts, corrections, results, "
        "and next actions; remove only service-level scaffolding. Do not classify from banned "
        "words alone; judge whether the text is useful to the user or internal machinery leaking.\n\n"
        "<EVIDENCE>\n"
        f"{_messages_to_text(messages)}\n"
        "</EVIDENCE>\n\n"
        "<DRAFT_RESPONSE>\n"
        f"{candidate}\n"
        "</DRAFT_RESPONSE>"
    )
    try:
        raw = await complete_text_fn(
            provider,
            [Message(role="system", content=prompt)],
            context="runtime_state_response_review",
        )
    except Exception:
        logger.debug("Runtime-state response review skipped", exc_info=True)
        return candidate

    payload = _extract_json_object(raw)
    if not isinstance(payload, dict):
        return candidate
    verdict = str(payload.get("verdict") or "").strip().lower()
    try:
        confidence = float(payload.get("confidence", 0.0) or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    response = sanitize_user_facing_text_preserving_reaction(str(payload.get("response") or ""))
    if verdict == "revised" and confidence >= 0.55 and response:
        return response
    return candidate


async def _verify_final_response(
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    candidate: str,
    *,
    complete_text_fn: _CompleteTextFn,
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
        raw = await complete_text_fn(
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


def _messages_to_text(
    messages: list[Message | dict[str, Any]], max_chars: int = _MAX_VERIFY_CONTEXT_CHARS
) -> str:
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
