from __future__ import annotations

import asyncio
import json
import sys
from typing import Any

import structlog

from octopal.infrastructure.logging import correlation_id_var
from octopal.infrastructure.observability.base import now_ms
from octopal.infrastructure.observability.helpers import safe_preview, summarize_exception
from octopal.runtime.metrics import read_metrics_snapshot, update_component_gauges
from octopal.runtime.octo.background_tracing import (
    _finish_background_trace_context,
    _start_background_trace_context,
)
from octopal.runtime.octo.delivery import (
    _result_has_blocking_failure,
    resolve_user_delivery,
    resolve_worker_followup_delivery,
)
from octopal.runtime.octo.followup_delivery import _send_worker_followup
from octopal.runtime.octo.followup_text import _merge_worker_followup_texts
from octopal.runtime.octo.followups import (
    _build_forced_worker_followup_batch,
    _build_worker_followup_batch_result,
    _build_worker_result_batch_timeout_followup,
    _combine_worker_followup_notify_policy,
    _instruction_request_question,
    _is_instruction_request_result,
    _PendingWorkerFollowupBatch,
    _PendingWorkerFollowupItem,
)
from octopal.runtime.octo.router import (
    route_worker_results_back_to_octo as _default_route_worker_results_back_to_octo,
)
from octopal.runtime.octo.router import (
    should_force_worker_followup,
)
from octopal.runtime.octo.runtime_config import _env_int
from octopal.runtime.plans import PlanRunService
from octopal.runtime.plans.service import PLAN_ACTIVE_STORAGE_STATUSES
from octopal.runtime.scheduler.service import normalize_notify_user_policy
from octopal.runtime.workers.contracts import WorkerResult

logger = structlog.get_logger(__name__)

_FOLLOWUP_QUEUES: dict[int, asyncio.Queue] = {}
_FOLLOWUP_TASKS: dict[int, asyncio.Task] = {}
_INTERNAL_QUEUES: dict[int, asyncio.Queue] = {}
_INTERNAL_TASKS: dict[int, asyncio.Task] = {}
_WORKER_FOLLOWUP_BATCHES: dict[tuple[int, str], _PendingWorkerFollowupBatch] = {}
_QUEUE_IDLE_TIMEOUT_SECONDS = 300.0
# Worker-result follow-up can include multiple provider retries and a fallback
# pass through Octo, so it needs a wider budget than a single LLM request.
_WORKER_RESULT_ROUTING_TIMEOUT_SECONDS = 900.0
_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS = float(
    _env_int(
        "OCTOPAL_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS",
        8,
        minimum=1,
    )
)


def _runtime_value(name: str, default: Any) -> Any:
    core_module = sys.modules.get("octopal.runtime.octo.core")
    if core_module is None:
        return default
    return getattr(core_module, name, default)


def _queue_idle_timeout_seconds() -> float:
    return float(_runtime_value("_QUEUE_IDLE_TIMEOUT_SECONDS", _QUEUE_IDLE_TIMEOUT_SECONDS))


def _worker_result_routing_timeout_seconds() -> float:
    return float(
        _runtime_value(
            "_WORKER_RESULT_ROUTING_TIMEOUT_SECONDS",
            _WORKER_RESULT_ROUTING_TIMEOUT_SECONDS,
        )
    )


def _worker_followup_batch_window_seconds() -> float:
    return float(
        _runtime_value(
            "_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS",
            _WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS,
        )
    )


async def _route_worker_results_back_to_octo(*args: Any, **kwargs: Any) -> str:
    route_func = _runtime_value(
        "route_worker_results_back_to_octo",
        _default_route_worker_results_back_to_octo,
    )
    try:
        return await route_func(*args, **kwargs)
    except TypeError as exc:
        if "notify_user" not in kwargs or "unexpected keyword argument 'notify_user'" not in str(
            exc
        ):
            raise
        retry_kwargs = dict(kwargs)
        retry_kwargs.pop("notify_user", None)
        return await route_func(*args, **retry_kwargs)


def _runtime_plan_metrics(octo: Any | None = None) -> dict[str, int]:
    if octo is None:
        current = read_metrics_snapshot().get("octo", {})
        if not isinstance(current, dict):
            return {}
        return {
            key: int(current.get(key, 0) or 0)
            for key in ("active_plan_runs", "needs_next_step_plan_runs")
            if key in current
        }

    store = getattr(octo, "store", None)
    if store is None:
        return {"active_plan_runs": 0, "needs_next_step_plan_runs": 0}
    try:
        active_runs = store.list_plan_runs(
            statuses=sorted(PLAN_ACTIVE_STORAGE_STATUSES),
            limit=1000,
        )
    except Exception:
        logger.debug("Failed to collect runtime plan metrics", exc_info=True)
        return {}
    return {
        "active_plan_runs": len(active_runs),
        "needs_next_step_plan_runs": sum(
            1 for run in active_runs if str(run.status).strip() == "needs_next_step"
        ),
    }


def _publish_runtime_metrics(thinking_count: int = 0, *, octo: Any | None = None) -> None:
    gauges = {
        "followup_queues": len(_FOLLOWUP_QUEUES),
        "followup_tasks": len(_FOLLOWUP_TASKS),
        "internal_queues": len(_INTERNAL_QUEUES),
        "internal_tasks": len(_INTERNAL_TASKS),
        "thinking_count": thinking_count,
    }
    gauges.update(_runtime_plan_metrics(octo))
    update_component_gauges("octo", gauges)


async def _followup_worker(chat_id: int, queue: asyncio.Queue) -> None:
    while True:
        try:
            future, coro = await asyncio.wait_for(
                queue.get(), timeout=_queue_idle_timeout_seconds()
            )
        except TimeoutError:
            break
        try:
            result = await coro
            if not future.cancelled():
                future.set_result(result)
        except Exception as exc:
            if not future.cancelled():
                future.set_exception(exc)
        finally:
            queue.task_done()
    _FOLLOWUP_TASKS.pop(chat_id, None)
    if queue.empty():
        _FOLLOWUP_QUEUES.pop(chat_id, None)
    _publish_runtime_metrics()


def _enqueue_followup(chat_id: int, coro) -> asyncio.Future[str]:
    loop = asyncio.get_running_loop()
    queue = _FOLLOWUP_QUEUES.get(chat_id)
    if queue is not None and getattr(queue, "_loop", None) not in (None, loop):
        _FOLLOWUP_QUEUES.pop(chat_id, None)
        prior_task = _FOLLOWUP_TASKS.pop(chat_id, None)
        if prior_task and not prior_task.done():
            prior_task.cancel()
        queue = None
    if not queue:
        queue = asyncio.Queue()
        _FOLLOWUP_QUEUES[chat_id] = queue
    if chat_id not in _FOLLOWUP_TASKS or _FOLLOWUP_TASKS[chat_id].done():
        _FOLLOWUP_TASKS[chat_id] = asyncio.create_task(_followup_worker(chat_id, queue))
    _publish_runtime_metrics()
    future: asyncio.Future[str] = loop.create_future()
    queue.put_nowait((future, coro))
    return future


async def _flush_worker_followup_batch(octo: Any, chat_id: int, correlation_id: str) -> None:
    trace_started_at_ms = now_ms()
    trace_metadata: dict[str, Any] = {
        "correlation_id": correlation_id,
        "batch_window_seconds": _worker_followup_batch_window_seconds(),
        "routing_timeout_seconds": _worker_result_routing_timeout_seconds(),
    }
    trace_ctx, trace_token, is_root_trace = await _start_background_trace_context(
        octo.trace_sink,
        name="worker.followup",
        chat_id=chat_id,
        correlation_id=correlation_id,
        metadata=trace_metadata,
    )
    trace_status = "ok"
    trace_output: dict[str, Any] | None = None
    correlation_token = correlation_id_var.set(correlation_id)
    try:
        await asyncio.sleep(_worker_followup_batch_window_seconds())
        batch_key = (chat_id, correlation_id)
        batch = _WORKER_FOLLOWUP_BATCHES.pop(batch_key, None)
        if batch is None:
            trace_output = {"status": "empty_batch"}
            return
        is_current = getattr(octo, "is_correlation_current_for_chat", None)
        if callable(is_current) and not is_current(chat_id, correlation_id):
            octo.clear_pending_conversational_closure(correlation_id)
            trace_output = {"status": "suppressed", "reason": "stale_chat_turn_epoch"}
            logger.info(
                "Internal worker follow-up skipped",
                chat_id=chat_id,
                correlation_id=correlation_id,
                reason="stale_chat_turn_epoch",
            )
            return
        batched_count = len(batch.texts) + len(batch.items)
        trace_metadata.update(
            {
                "batched_count": batched_count,
                "text_count": len(batch.texts),
                "worker_result_count": len(batch.items),
                "created_during_active_turn": batch.created_during_active_turn,
            }
        )
        merged_texts = list(batch.texts)

        if batch.items:
            notify_user = _combine_worker_followup_notify_policy(batch.items)
            try:
                batched_text = await asyncio.wait_for(
                    _route_worker_results_back_to_octo(
                        octo,
                        chat_id,
                        [(item.worker_id, item.task_text, item.result) for item in batch.items],
                        notify_user=notify_user,
                    ),
                    timeout=_worker_result_routing_timeout_seconds(),
                )
            except TimeoutError:
                trace_metadata["routing_timed_out"] = True
                logger.warning(
                    "Worker-result batch routing timed out",
                    chat_id=chat_id,
                    batched_count=len(batch.items),
                )
                batched_text = _build_worker_result_batch_timeout_followup(batch.items)

            pending_closure = octo.has_pending_conversational_closure(correlation_id)
            synthetic_result = _build_worker_followup_batch_result(batch.items)
            trace_metadata.update(
                {
                    "pending_closure": pending_closure,
                    "notify_user_policy_resolved": notify_user,
                    "synthetic_result_status": synthetic_result.status,
                    "questions_count": len(synthetic_result.questions),
                }
            )
            delivery = resolve_worker_followup_delivery(
                batched_text,
                result=synthetic_result,
                pending_closure=pending_closure,
                suppress_followup=octo.should_suppress_turn_followups(correlation_id),
                should_force=any(should_force_worker_followup(item.result) for item in batch.items),
                notify_user=notify_user,
                forced_text_factory=lambda _result: _build_forced_worker_followup_batch(
                    batch.items
                ),
            )
            if delivery.reason == "forced_substantive_followup":
                logger.info(
                    "Forcing substantive batched worker follow-up",
                    chat_id=chat_id,
                    batched_count=len(batch.items),
                )
            if delivery.user_visible:
                trace_metadata["delivery_reason"] = delivery.reason
                merged_texts.append(delivery.text)
            else:
                octo.clear_pending_conversational_closure(correlation_id)
                trace_output = {"status": "suppressed", "reason": delivery.reason}
                if not merged_texts:
                    logger.info(
                        "Internal worker follow-up skipped",
                        chat_id=chat_id,
                        reason=delivery.reason,
                        batched_count=batched_count,
                    )
                    return

        final_text = _merge_worker_followup_texts(merged_texts)
        if not final_text:
            octo.clear_pending_conversational_closure(correlation_id)
            trace_output = {"status": "suppressed", "reason": "empty_batched_followup"}
            logger.info(
                "Internal worker follow-up skipped",
                chat_id=chat_id,
                reason="empty_batched_followup",
                batched_count=batched_count,
            )
            return
        trace_output = {
            "status": "ready_to_send",
            "final_text_preview": safe_preview(final_text, limit=240),
            "final_text_len": len(final_text),
            "batched_count": batched_count,
        }
        await _send_worker_followup(
            octo,
            chat_id,
            correlation_id,
            final_text,
            batched_count=batched_count,
        )
    except asyncio.CancelledError:
        trace_status = "error"
        trace_metadata["error_type"] = "CancelledError"
        trace_output = {"status": "cancelled"}
        raise
    except Exception as exc:
        trace_status = "error"
        trace_metadata.update(summarize_exception(exc))
        trace_output = {"status": "failed"}
        logger.exception("Failed to flush batched worker follow-up", chat_id=chat_id)
    finally:
        trace_metadata["duration_ms"] = round(now_ms() - trace_started_at_ms, 2)
        await _finish_background_trace_context(
            octo.trace_sink,
            trace_ctx,
            trace_token,
            is_root_trace=is_root_trace,
            status=trace_status,
            output=trace_output,
            metadata=trace_metadata,
        )
        correlation_id_var.reset(correlation_token)


def _schedule_worker_followup_flush(octo: Any, chat_id: int, correlation_id: str | None) -> None:
    if not correlation_id:
        return
    is_current = getattr(octo, "is_correlation_current_for_chat", None)
    if callable(is_current) and not is_current(chat_id, correlation_id):
        _discard_worker_followup_batch(chat_id, correlation_id)
        return
    if octo.should_suppress_channel_followups(correlation_id):
        _discard_worker_followup_batch(chat_id, correlation_id)
        return
    batch_key = (chat_id, correlation_id)
    batch = _WORKER_FOLLOWUP_BATCHES.get(batch_key)
    if batch is None:
        return
    if octo.should_suppress_turn_followups(correlation_id):
        existing_task = batch.task
        if existing_task and not existing_task.done():
            existing_task.cancel()
        batch.task = None
        return
    if not octo.should_flush_worker_followups(correlation_id):
        existing_task = batch.task
        if existing_task and not existing_task.done():
            existing_task.cancel()
        batch.task = None
        return
    existing_task = batch.task
    if existing_task and not existing_task.done():
        existing_task.cancel()
    batch.task = asyncio.create_task(_flush_worker_followup_batch(octo, chat_id, correlation_id))


def _discard_worker_followup_batch(
    chat_id: int,
    correlation_id: str | None,
    *,
    only_if_created_during_active_turn: bool = False,
) -> bool:
    if not correlation_id:
        return False
    batch_key = (chat_id, correlation_id)
    batch = _WORKER_FOLLOWUP_BATCHES.get(batch_key)
    if batch is None:
        return False
    if only_if_created_during_active_turn and not batch.created_during_active_turn:
        return False
    task = batch.task
    if task and not task.done():
        task.cancel()
    _WORKER_FOLLOWUP_BATCHES.pop(batch_key, None)
    return True


async def _enqueue_batched_worker_followup(
    octo: Any,
    chat_id: int,
    correlation_id: str | None,
    text: str | None = None,
    *,
    worker_id: str = "",
    task_text: str | None = None,
    result: WorkerResult | None = None,
    notify_user: str | None = None,
) -> None:
    is_current = getattr(octo, "is_correlation_current_for_chat", None)
    if callable(is_current) and not is_current(chat_id, correlation_id):
        octo.clear_pending_conversational_closure(correlation_id)
        logger.info(
            "Internal worker follow-up skipped",
            chat_id=chat_id,
            correlation_id=correlation_id,
            reason="stale_chat_turn_epoch",
        )
        return
    if octo.should_suppress_channel_followups(correlation_id):
        octo.clear_pending_conversational_closure(correlation_id)
        logger.info(
            "Internal worker follow-up skipped",
            chat_id=chat_id,
            correlation_id=correlation_id,
            reason=octo.channel_followup_suppression_reason(correlation_id)
            or "channel_followups_suppressed",
        )
        return
    if not correlation_id:
        if text is not None:
            await _send_worker_followup(octo, chat_id, correlation_id, text)
            return
        if task_text is not None and result is not None:
            routed_text = await _route_worker_results_back_to_octo(
                octo,
                chat_id,
                [(str(worker_id or "").strip(), task_text, result)],
                notify_user=notify_user,
            )
            decision = resolve_user_delivery(routed_text)
            if decision.user_visible:
                await _send_worker_followup(octo, chat_id, correlation_id, decision.text)
        return

    loop = asyncio.get_running_loop()
    batch_key = (chat_id, correlation_id)
    batch = _WORKER_FOLLOWUP_BATCHES.get(batch_key)
    if batch is not None and batch.loop not in (None, loop):
        prior_task = batch.task
        if prior_task and not prior_task.done():
            prior_task.cancel()
        _WORKER_FOLLOWUP_BATCHES.pop(batch_key, None)
        batch = None
    if batch is None:
        batch = _PendingWorkerFollowupBatch(texts=[], items=[], loop=loop)
        _WORKER_FOLLOWUP_BATCHES[batch_key] = batch
    if text is not None:
        batch.texts.append(text)
    if task_text is not None and result is not None:
        batch.items.append(
            _PendingWorkerFollowupItem(
                worker_id=str(worker_id or "").strip(),
                task_text=task_text,
                result=result,
                notify_user=notify_user,
            )
        )
    if octo.has_active_user_turn(correlation_id):
        batch.created_during_active_turn = True
    _schedule_worker_followup_flush(octo, chat_id, correlation_id)


async def _internal_worker(octo: Any, chat_id: int, queue: asyncio.Queue) -> None:
    """Process completed worker results.

    Worker results are logged and stored in memory but NOT automatically sent to the user.
    The octo decides what to communicate based on worker results.
    """
    while True:
        correlation_id: str | None = None
        correlation_token = None
        try:
            item = await asyncio.wait_for(queue.get(), timeout=_queue_idle_timeout_seconds())
            notify_user = None
            worker_id = ""
            if len(item) == 5:
                worker_id, task_text, result, correlation_id, notify_user = item
            elif len(item) == 4:
                task_text, result, correlation_id, notify_user = item
            else:
                task_text, result, correlation_id = item
        except TimeoutError:
            break
        try:
            if correlation_id:
                correlation_token = correlation_id_var.set(correlation_id)
            logger.info(
                "Processing internal worker result",
                chat_id=chat_id,
                correlation_id=correlation_id,
                summary_len=len(result.summary or ""),
            )
            # Add worker result to memory for context, but don't auto-send
            if result.summary:
                memory_prefix = (
                    "Worker requested instruction"
                    if _is_instruction_request_result(result)
                    else "Worker completed"
                )
                await octo.memory.add_message(
                    "system",
                    f"{memory_prefix}: {result.summary}",
                    {"worker_result": True, "task": task_text, "chat_id": chat_id},
                )
            output_error = ""
            if isinstance(result.output, dict):
                raw_error = result.output.get("error")
                if raw_error is not None:
                    output_error = str(raw_error).strip()
            if output_error:
                await octo.memory.add_message(
                    "system",
                    f"Worker error: {output_error}",
                    {"worker_result": True, "task": task_text, "chat_id": chat_id},
                )
            synced_plan = _sync_runtime_plan_with_worker_result(
                octo,
                chat_id,
                worker_id=worker_id,
                result=result,
            )
            continuation_scheduled = _schedule_runtime_plan_continuation(
                octo,
                chat_id,
                synced_plan,
                worker_id=worker_id,
                task_text=task_text,
                result=result,
                notify_user=notify_user,
                correlation_id=correlation_id,
            )
            if _is_instruction_request_result(result):
                await _route_instruction_request_to_octo(
                    octo,
                    chat_id,
                    worker_id=worker_id,
                    task_text=task_text,
                    result=result,
                    correlation_id=correlation_id,
                )
            # System/internal chat (chat_id == 0) should never emit user-facing follow-ups.
            # Negative chat IDs are valid for channels such as Telegram groups.
            elif chat_id == 0:
                logger.info("Skipping user follow-up for internal chat", chat_id=chat_id)
            elif octo.should_suppress_channel_followups(correlation_id):
                octo.clear_pending_conversational_closure(correlation_id)
                logger.info(
                    "Internal worker follow-up skipped",
                    chat_id=chat_id,
                    correlation_id=correlation_id,
                    reason=octo.channel_followup_suppression_reason(correlation_id)
                    or "channel_followups_suppressed",
                )
            elif continuation_scheduled:
                octo.clear_pending_conversational_closure(correlation_id)
                logger.info(
                    "Internal worker follow-up skipped",
                    chat_id=chat_id,
                    correlation_id=correlation_id,
                    reason="runtime_plan_continuation_scheduled",
                )
            else:
                notify_policy = normalize_notify_user_policy(notify_user)
                if notify_policy == "never" and not _result_has_blocking_failure(result):
                    octo.clear_pending_conversational_closure(correlation_id)
                    logger.info(
                        "Internal worker follow-up skipped",
                        chat_id=chat_id,
                        reason="scheduled_notify_never",
                    )
                else:
                    await _enqueue_batched_worker_followup(
                        octo,
                        chat_id,
                        correlation_id,
                        worker_id=worker_id,
                        task_text=task_text,
                        result=result,
                        notify_user=notify_user,
                    )
                    logger.info(
                        "Internal worker follow-up deferred",
                        chat_id=chat_id,
                        reason="batched_worker_results",
                    )
            logger.debug("Worker result processed", summary_len=len(result.summary or ""))
        except Exception:
            logger.exception("Failed to process internal worker result")
        finally:
            octo.mark_internal_result_processed(correlation_id)
            if octo.should_flush_worker_followups(correlation_id):
                octo.clear_suppressed_turn_followups(correlation_id)
            _schedule_worker_followup_flush(octo, chat_id, correlation_id)
            if correlation_token is not None:
                correlation_id_var.reset(correlation_token)
            queue.task_done()
    _INTERNAL_TASKS.pop(chat_id, None)
    if queue.empty():
        _INTERNAL_QUEUES.pop(chat_id, None)
    _publish_runtime_metrics(octo=octo)


def _sync_runtime_plan_with_worker_result(
    octo: Any,
    chat_id: int,
    *,
    worker_id: str,
    result: WorkerResult,
) -> dict[str, Any] | None:
    store = getattr(octo, "store", None)
    normalized_worker_id = str(worker_id or "").strip()
    if store is None or not normalized_worker_id:
        return None
    service = PlanRunService(store)
    try:
        step = service.update_worker_step_result(
            worker_run_id=normalized_worker_id,
            chat_id=chat_id if chat_id != 0 else None,
            result_status=str(getattr(result, "status", "completed") or "completed"),
            summary=str(getattr(result, "summary", "") or ""),
            output=getattr(result, "output", None),
            questions=list(getattr(result, "questions", []) or []),
            tools_used=list(getattr(result, "tools_used", []) or []),
        )
    except Exception:
        logger.debug(
            "Failed to sync runtime plan with worker result",
            chat_id=chat_id,
            worker_id=normalized_worker_id,
            exc_info=True,
        )
        return None
    if step is not None:
        snapshot = service.get_snapshot(step.run_id)
        logger.info(
            "Synced runtime plan step with worker result",
            chat_id=chat_id,
            worker_id=normalized_worker_id,
            plan_run_id=step.run_id,
            plan_step_id=step.step_id,
            worker_result_status=getattr(result, "status", None),
        )
        _publish_runtime_metrics(octo=octo)
        return {"step": step, "snapshot": snapshot}
    _publish_runtime_metrics(octo=octo)
    return None


def _schedule_runtime_plan_continuation(
    octo: Any,
    chat_id: int,
    synced_plan: dict[str, Any] | None,
    *,
    worker_id: str,
    task_text: str,
    result: WorkerResult,
    notify_user: str | None,
    correlation_id: str | None,
) -> bool:
    continuation = _build_runtime_plan_continuation(
        synced_plan,
        worker_id=worker_id,
        result=result,
        notify_user=notify_user,
    )
    if continuation is None:
        return False

    notify_policy = str(continuation["notify_policy"])
    task = asyncio.create_task(
        _run_runtime_plan_continuation(
            octo,
            chat_id,
            continuation["args"],
            notify_policy=notify_policy,
        )
    )

    def _log_done(done: asyncio.Task) -> None:
        fallback_reason = ""
        try:
            payload = done.result()
        except Exception:
            fallback_reason = "exception"
            logger.exception(
                "Runtime plan continuation failed",
                chat_id=chat_id,
                plan_run_id=continuation["run_id"],
                plan_step_id=continuation["step_id"],
            )
            payload = {"status": "error"}
        else:
            status = str(payload.get("status") if isinstance(payload, dict) else "").strip()
            if status != "continued":
                fallback_reason = f"status={status or 'unknown'}"
        logger.info(
            "Runtime plan continuation finished",
            chat_id=chat_id,
            plan_run_id=continuation["run_id"],
            plan_step_id=continuation["step_id"],
            status=payload.get("status") if isinstance(payload, dict) else None,
            delivered=payload.get("delivered") if isinstance(payload, dict) else None,
            fallback_reason=fallback_reason or None,
        )
        if fallback_reason:
            asyncio.create_task(
                _enqueue_runtime_plan_continuation_fallback(
                    octo,
                    chat_id,
                    correlation_id,
                    worker_id=worker_id,
                    task_text=task_text,
                    result=result,
                    notify_user=notify_user,
                    reason=fallback_reason,
                )
            )

    task.add_done_callback(_log_done)
    logger.info(
        "Scheduled runtime plan continuation",
        chat_id=chat_id,
        plan_run_id=continuation["run_id"],
        plan_step_id=continuation["step_id"],
        notify_user=continuation["notify_user"],
        notify_policy=notify_policy,
    )
    return True


def _build_runtime_plan_continuation(
    synced_plan: dict[str, Any] | None,
    *,
    worker_id: str,
    result: WorkerResult,
    notify_user: str | None,
) -> dict[str, Any] | None:
    if not synced_plan:
        return None
    snapshot = synced_plan.get("snapshot")
    if not isinstance(snapshot, dict):
        return None
    run = snapshot.get("run") if isinstance(snapshot.get("run"), dict) else {}
    next_step = snapshot.get("next_step")
    if not isinstance(next_step, dict):
        return None
    if str(run.get("status") or "").strip().lower() != "needs_next_step":
        return None

    run_id = str(run.get("id") or "").strip()
    step_id = str(next_step.get("step_id") or next_step.get("id") or "").strip()
    if not run_id or not step_id:
        return None
    step_kind = str(next_step.get("kind") or "").strip() or "octo"
    title = str(next_step.get("title") or step_id).strip()
    task_text = str(next_step.get("task") or title).strip()
    executor = str(next_step.get("executor") or "").strip()
    worker_summary = str(getattr(result, "summary", "") or "").strip()
    policy = normalize_notify_user_policy(notify_user) if notify_user is not None else "always"

    continuation_task = (
        f"Continue durable runtime plan `{run_id}` from current step `{step_id}`.\n"
        f"Step kind: {step_kind}.\n"
        f"Step title: {title}.\n"
        f"Step task: {task_text}.\n" + (f"Executor hint: {executor}.\n" if executor else "") + "\n"
        "First inspect the stored plan with `plan_status` if needed. Execute this exact current "
        "step and keep the runtime plan state current with `plan_update_step` before returning. "
        "For a worker step, start the worker with the matching `plan_run_id` and `plan_step_id`. "
        "For an octo/tool/final step, perform the runtime work and then mark the step completed, "
        "blocked, failed, awaiting_user, or awaiting_approval."
    )
    context_summary = (
        f"Previous worker `{worker_id}` returned status={getattr(result, 'status', None)!r}. "
        f"Summary: {worker_summary or '(empty)'}"
    )
    return {
        "run_id": run_id,
        "step_id": step_id,
        "notify_policy": policy,
        "notify_user": policy in {"always", "if_significant"},
        "args": {
            "task": continuation_task,
            "context_summary": context_summary,
            "notify_user": policy in {"always", "if_significant"},
        },
    }


async def _run_runtime_plan_continuation(
    octo: Any,
    chat_id: int,
    args: dict[str, Any],
    *,
    notify_policy: str,
) -> dict[str, Any]:
    from octopal.tools.catalog import _tool_octo_continue_from_control_route

    # Bounded control routes cannot reliably distinguish "significant" from
    # routine output, so preserve the existing scheduled-control contract and
    # downgrade that mode to silent delivery.
    control_notify_policy = "never" if notify_policy in {"never", "if_significant"} else "always"
    try:
        payload = await _tool_octo_continue_from_control_route(
            args,
            {
                "octo": octo,
                "chat_id": chat_id,
                "control_route_notify_user": control_notify_policy,
            },
        )
        try:
            parsed = json.loads(payload)
        except Exception:
            return {"status": "unknown", "raw": str(payload)}
        return parsed if isinstance(parsed, dict) else {"status": "unknown", "raw": parsed}
    finally:
        _publish_runtime_metrics(octo=octo)


async def _enqueue_runtime_plan_continuation_fallback(
    octo: Any,
    chat_id: int,
    correlation_id: str | None,
    *,
    worker_id: str,
    task_text: str,
    result: WorkerResult,
    notify_user: str | None,
    reason: str,
) -> None:
    if chat_id == 0:
        logger.info(
            "Runtime plan continuation fallback skipped",
            chat_id=chat_id,
            reason="internal_chat",
        )
        return
    is_current = getattr(octo, "is_correlation_current_for_chat", None)
    if callable(is_current) and not is_current(chat_id, correlation_id):
        octo.clear_pending_conversational_closure(correlation_id)
        logger.info(
            "Runtime plan continuation fallback skipped",
            chat_id=chat_id,
            reason="stale_chat_turn_epoch",
        )
        return
    if octo.should_suppress_channel_followups(correlation_id):
        logger.info(
            "Runtime plan continuation fallback skipped",
            chat_id=chat_id,
            reason=octo.channel_followup_suppression_reason(correlation_id)
            or "channel_followups_suppressed",
        )
        return

    notify_policy = normalize_notify_user_policy(notify_user)
    if notify_policy == "never" and not _result_has_blocking_failure(result):
        logger.info(
            "Runtime plan continuation fallback skipped",
            chat_id=chat_id,
            reason="scheduled_notify_never",
        )
        return

    await _enqueue_batched_worker_followup(
        octo,
        chat_id,
        correlation_id,
        worker_id=worker_id,
        task_text=task_text,
        result=result,
        notify_user=notify_user,
    )
    logger.info(
        "Runtime plan continuation fallback queued worker follow-up",
        chat_id=chat_id,
        worker_id=worker_id,
        reason=reason,
    )


async def _route_instruction_request_to_octo(
    octo: Any,
    chat_id: int,
    *,
    worker_id: str,
    task_text: str,
    result: WorkerResult,
    correlation_id: str | None,
) -> None:
    logger.info(
        "Routing worker instruction request to Octo",
        chat_id=chat_id,
        worker_id=worker_id,
        correlation_id=correlation_id,
    )
    try:
        reply_text = await asyncio.wait_for(
            _route_worker_results_back_to_octo(
                octo,
                chat_id,
                [(worker_id, task_text, result)],
            ),
            timeout=_worker_result_routing_timeout_seconds(),
        )
    except TimeoutError:
        logger.warning(
            "Worker instruction request routing timed out",
            chat_id=chat_id,
            worker_id=worker_id,
        )
        reply_text = _instruction_request_question(result)

    decision = resolve_user_delivery(reply_text)
    if not decision.user_visible:
        logger.info(
            "Worker instruction request handled without user follow-up",
            chat_id=chat_id,
            worker_id=worker_id,
            reason=decision.reason,
        )
        return
    if chat_id == 0:
        logger.info(
            "Skipping user-visible worker instruction follow-up for internal chat",
            chat_id=chat_id,
            worker_id=worker_id,
        )
        return
    if octo.should_suppress_channel_followups(correlation_id):
        logger.info(
            "Skipping user-visible worker instruction follow-up for suppressed channel",
            chat_id=chat_id,
            worker_id=worker_id,
            correlation_id=correlation_id,
            reason=octo.channel_followup_suppression_reason(correlation_id)
            or "channel_followups_suppressed",
        )
        return
    await _send_worker_followup(octo, chat_id, correlation_id, decision.text, batched_count=1)


def _enqueue_internal_result(
    octo: Any,
    chat_id: int,
    worker_id: str,
    task_text: str,
    result: WorkerResult,
    *,
    correlation_id: str | None,
    notify_user: str | None = None,
) -> None:
    loop = asyncio.get_running_loop()
    queue = _INTERNAL_QUEUES.get(chat_id)
    if queue is not None and getattr(queue, "_loop", None) not in (None, loop):
        _INTERNAL_QUEUES.pop(chat_id, None)
        prior_task = _INTERNAL_TASKS.pop(chat_id, None)
        if prior_task and not prior_task.done():
            prior_task.cancel()
        queue = None
    if not queue:
        queue = asyncio.Queue()
        _INTERNAL_QUEUES[chat_id] = queue
    if chat_id not in _INTERNAL_TASKS or _INTERNAL_TASKS[chat_id].done():
        _INTERNAL_TASKS[chat_id] = asyncio.create_task(_internal_worker(octo, chat_id, queue))
    octo.mark_internal_result_pending(correlation_id)
    queue.put_nowait((str(worker_id or "").strip(), task_text, result, correlation_id, notify_user))
    logger.info("Queued internal worker result", chat_id=chat_id, queue_size=queue.qsize())
    _publish_runtime_metrics(octo=octo)
