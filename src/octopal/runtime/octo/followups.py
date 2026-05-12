from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass

from octopal.runtime.octo.delivery import _result_has_blocking_failure
from octopal.runtime.octo.router import build_forced_worker_followup
from octopal.runtime.scheduler.service import normalize_notify_user_policy
from octopal.runtime.workers.contracts import WorkerResult
from octopal.utils import sanitize_user_facing_text, should_suppress_user_delivery


@dataclass
class _PendingWorkerFollowupBatch:
    texts: list[str]
    items: list[_PendingWorkerFollowupItem]
    task: asyncio.Task | None = None
    loop: asyncio.AbstractEventLoop | None = None
    created_during_active_turn: bool = False


@dataclass(frozen=True)
class _PendingWorkerFollowupItem:
    worker_id: str
    task_text: str
    result: WorkerResult
    notify_user: str | None = None


def _build_worker_result_timeout_followup(result: WorkerResult) -> str:
    """Return a minimal user-facing fallback when Octo routing times out."""
    lead = "Worker finished, but the follow-up routing step timed out."

    lines = [lead]
    if result.questions:
        lines.append("")
        lines.append("Open questions:")
        lines.extend(f"- {question}" for question in result.questions[:3] if str(question).strip())

    return "\n".join(lines).strip()


def _build_worker_result_batch_timeout_followup(items: list[_PendingWorkerFollowupItem]) -> str:
    if len(items) == 1:
        return _build_worker_result_timeout_followup(items[0].result)

    lines = [f"{len(items)} worker tasks finished, but the follow-up routing step timed out."]
    questions: list[str] = []
    for item in items:
        for question in item.result.questions[:3]:
            value = str(question).strip()
            if value and value not in questions:
                questions.append(value)
    if questions:
        lines.append("")
        lines.append("Open questions:")
        lines.extend(f"- {question}" for question in questions[:5])
    return "\n".join(lines).strip()


def _is_instruction_request_result(result: WorkerResult) -> bool:
    if str(result.status or "").strip().lower() == "awaiting_instruction":
        return True
    output = result.output if isinstance(result.output, dict) else {}
    return isinstance(output.get("instruction_request"), dict)


def _instruction_request_question(result: WorkerResult) -> str:
    output = result.output if isinstance(result.output, dict) else {}
    request = output.get("instruction_request")
    if isinstance(request, dict):
        question = str(request.get("question") or "").strip()
        if question:
            return question
    if result.questions:
        question = str(result.questions[0] or "").strip()
        if question:
            return question
    return str(result.summary or "").strip()


def _build_worker_followup_batch_result(items: list[_PendingWorkerFollowupItem]) -> WorkerResult:
    summaries = [
        str(item.result.summary or "").strip()
        for item in items
        if str(item.result.summary or "").strip()
    ]
    questions: list[str] = []
    knowledge_proposals = []
    tools_used: list[str] = []
    has_failure = False
    for item in items:
        if _result_has_blocking_failure(item.result):
            has_failure = True
        for question in item.result.questions:
            value = str(question).strip()
            if value and value not in questions:
                questions.append(value)
        for proposal in item.result.knowledge_proposals:
            if proposal not in knowledge_proposals:
                knowledge_proposals.append(proposal)
        for tool_name in item.result.tools_used:
            value = str(tool_name).strip()
            if value and value not in tools_used:
                tools_used.append(value)
    summary = "\n\n".join(summaries)
    if has_failure and "failed" not in summary.lower():
        summary = f"{summary}\n\nAt least one worker failed.".strip()
    return WorkerResult(
        status="failed" if has_failure else "completed",
        summary=summary,
        output={"status": "failed" if has_failure else "completed", "batched_count": len(items)},
        questions=questions,
        knowledge_proposals=knowledge_proposals,
        tools_used=tools_used,
    )


def _build_forced_worker_followup_batch_item(result: WorkerResult) -> str:
    forced_text = build_forced_worker_followup(result).strip()
    if forced_text:
        return forced_text

    summary = sanitize_user_facing_text(result.summary or "").strip()
    if not summary:
        return ""

    summary = re.sub(r"^(?:worker completed|completed)\s*:\s*", "", summary, flags=re.IGNORECASE)
    summary = re.sub(r"\s+", " ", summary).strip(" -")
    if not summary or should_suppress_user_delivery(summary):
        return ""
    if len(summary) > 240:
        summary = summary[:237].rstrip() + "..."
    return summary


def _build_forced_worker_followup_batch(items: list[_PendingWorkerFollowupItem]) -> str:
    if len(items) == 1:
        return build_forced_worker_followup(items[0].result)

    synthetic = _build_worker_followup_batch_result(items)
    if synthetic.questions:
        return "Tasks finished. I need your input on the next step."
    item_summaries: list[str] = []
    for item in items:
        summary = _build_forced_worker_followup_batch_item(item.result)
        if summary and summary not in item_summaries:
            item_summaries.append(summary)
    if item_summaries:
        lead = (
            f"Completed {len(items)} worker tasks, but at least one needs attention:"
            if synthetic.status == "failed"
            else f"Completed {len(items)} worker tasks:"
        )
        bullets = "\n".join(f"- {summary}" for summary in item_summaries[:3])
        return f"{lead}\n{bullets}".strip()
    if synthetic.status == "failed":
        return f"Completed {len(items)} worker tasks, but at least one needs attention."
    return f"Completed {len(items)} worker tasks. The results are ready."


def _combine_worker_followup_notify_policy(items: list[_PendingWorkerFollowupItem]) -> str | None:
    policies = [normalize_notify_user_policy(item.notify_user) for item in items]
    if any(policy == "always" for policy in policies):
        return "always"
    if policies and all(policy == "never" for policy in policies):
        return "never"
    return "if_significant"
