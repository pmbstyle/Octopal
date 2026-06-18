from __future__ import annotations

import json
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from octopal.infrastructure.observability.helpers import safe_preview
from octopal.infrastructure.providers.base import InferenceProvider, Message
from octopal.runtime.octo.route_loop_helpers import _parse_tool_result_payload
from octopal.runtime.octo.route_planning import _extract_json_object
from octopal.runtime.octo.route_verification import _messages_to_text
from octopal.tools.registry import ToolSpec

logger = structlog.get_logger(__name__)

_CompleteTextFn = Callable[..., Awaitable[str]]
_HandleToolCallFn = Callable[
    [dict[str, Any], list[ToolSpec], dict[str, Any]],
    Awaitable[tuple[Any, dict[str, Any]]],
]
_FindActiveToolSpecFn = Callable[[str, list[ToolSpec]], ToolSpec | None]


async def _worker_followup_requires_autonomous_continuation(
    *,
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    worker_results_payload: str,
    reply_text: str,
    complete_text_fn: _CompleteTextFn,
) -> bool:
    prompt = (
        "Classify whether a worker-result follow-up output may be delivered to the user, "
        "or whether the runtime must continue the task autonomously first.\n"
        "Return JSON only with this shape:\n"
        '{"verdict":"final|requires_continuation|requires_user_input|no_user_response",'
        '"confidence":0.0,"reason":"short"}\n'
        "Use requires_continuation when the draft is not a final task result, not a direct "
        "question for missing user input, and instead represents pending runtime work that "
        "should be completed by the broader route. Judge the speech act and evidence, not "
        "individual words or banned phrases. Use final for grounded results, real blocked "
        "states, and normal user-facing questions. Use requires_user_input only when the next "
        "safe step truly needs the user's decision or missing data.\n\n"
        "<WORKER_RESULTS>\n"
        f"{worker_results_payload[:12000]}\n"
        "</WORKER_RESULTS>\n\n"
        "<EVIDENCE>\n"
        f"{_messages_to_text(messages)}\n"
        "</EVIDENCE>\n\n"
        "<DRAFT_FOLLOWUP>\n"
        f"{reply_text}\n"
        "</DRAFT_FOLLOWUP>"
    )
    try:
        raw = await complete_text_fn(
            provider,
            [Message(role="system", content=prompt)],
            context="worker_followup_autonomy_verifier",
        )
    except Exception:
        logger.debug("Worker follow-up autonomy verifier skipped", exc_info=True)
        return False

    payload = _extract_json_object(raw)
    if not isinstance(payload, dict):
        return False
    verdict = str(payload.get("verdict") or "").strip().lower()
    try:
        confidence = float(payload.get("confidence", 0.0) or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    return verdict == "requires_continuation" and confidence >= 0.55


async def _continue_worker_followup_autonomously(
    *,
    chat_id: int,
    tool_specs: list[ToolSpec],
    ctx: dict[str, Any],
    worker_result_prompt: str,
    reply_text: str,
    find_active_tool_spec_fn: _FindActiveToolSpecFn,
    handle_tool_call_fn: _HandleToolCallFn,
) -> bool:
    continuation_spec = find_active_tool_spec_fn("octo_continue_from_control_route", tool_specs)
    if continuation_spec is None:
        logger.warning("Worker follow-up needed autonomous continuation but tool is unavailable")
        return False

    task = (
        "Continue the original user request autonomously from the worker updates. "
        "The worker-result follow-up draft was not a final answer because more runtime action "
        "is needed. Complete the remaining work end-to-end, repair recoverable issues that "
        "arise during execution, and ask the user only if missing input or approval is required."
    )
    context_summary = (
        "Worker-result follow-up payload:\n"
        f"{safe_preview(worker_result_prompt, limit=12000)}\n\n"
        "Rejected non-final follow-up draft:\n"
        f"{safe_preview(reply_text, limit=3000)}"
    )
    synthetic_call = {
        "id": f"worker-followup-continuation-{uuid.uuid4().hex[:12]}",
        "type": "function",
        "function": {
            "name": continuation_spec.name,
            "arguments": json.dumps(
                {
                    "task": task,
                    "context_summary": context_summary,
                    "notify_user": True,
                },
                ensure_ascii=False,
            ),
        },
    }
    result, meta = await handle_tool_call_fn(synthetic_call, tool_specs, ctx)
    payload = _parse_tool_result_payload(result)
    status = ""
    if isinstance(payload, dict):
        status = str(payload.get("status") or "").strip().lower()
    if not meta.get("had_error") and status == "continued":
        logger.info("Worker follow-up continued autonomously", chat_id=chat_id)
        return True
    logger.warning(
        "Worker follow-up autonomous continuation failed",
        chat_id=chat_id,
        status=status or None,
        had_error=bool(meta.get("had_error")),
    )
    return False
