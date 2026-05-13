from __future__ import annotations

import asyncio
import sys
from typing import Any

import structlog

from octopal.infrastructure.observability.base import now_ms
from octopal.infrastructure.observability.helpers import safe_preview, summarize_exception
from octopal.runtime.metrics import update_component_gauges
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
    return await route_func(*args, **kwargs)


def _publish_runtime_metrics(thinking_count: int = 0) -> None:
    update_component_gauges(
        "octo",
        {
            "followup_queues": len(_FOLLOWUP_QUEUES),
            "followup_tasks": len(_FOLLOWUP_TASKS),
            "internal_queues": len(_INTERNAL_QUEUES),
            "internal_tasks": len(_INTERNAL_TASKS),
            "thinking_count": thinking_count,
        },
    )


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
    try:
        await asyncio.sleep(_worker_followup_batch_window_seconds())
        batch_key = (chat_id, correlation_id)
        batch = _WORKER_FOLLOWUP_BATCHES.pop(batch_key, None)
        if batch is None:
            trace_output = {"status": "empty_batch"}
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
            try:
                batched_text = await asyncio.wait_for(
                    _route_worker_results_back_to_octo(
                        octo,
                        chat_id,
                        [(item.worker_id, item.task_text, item.result) for item in batch.items],
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
            notify_user = _combine_worker_followup_notify_policy(batch.items)
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


def _schedule_worker_followup_flush(octo: Any, chat_id: int, correlation_id: str | None) -> None:
    if not correlation_id:
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
    if not correlation_id:
        if text is not None:
            await _send_worker_followup(octo, chat_id, correlation_id, text)
            return
        if task_text is not None and result is not None:
            routed_text = await _route_worker_results_back_to_octo(
                octo,
                chat_id,
                [(str(worker_id or "").strip(), task_text, result)],
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
            logger.info(
                "Processing internal worker result",
                chat_id=chat_id,
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
            if _is_instruction_request_result(result):
                await _route_instruction_request_to_octo(
                    octo,
                    chat_id,
                    worker_id=worker_id,
                    task_text=task_text,
                    result=result,
                    correlation_id=correlation_id,
                )
            # System/internal chat (chat_id <= 0) should never emit user-facing follow-ups.
            elif chat_id <= 0:
                logger.info("Skipping user follow-up for internal chat", chat_id=chat_id)
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
            queue.task_done()
    _INTERNAL_TASKS.pop(chat_id, None)
    if queue.empty():
        _INTERNAL_QUEUES.pop(chat_id, None)
    _publish_runtime_metrics()


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
    if chat_id <= 0:
        logger.info(
            "Skipping user-visible worker instruction follow-up for internal chat",
            chat_id=chat_id,
            worker_id=worker_id,
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
    _publish_runtime_metrics()
