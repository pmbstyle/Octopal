from __future__ import annotations

import asyncio
import json
import os
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import structlog
from broodmind.intents.types import ActionIntent
from broodmind.memory.canon import CanonService
from broodmind.memory.memchain import memchain_record
from broodmind.memory.service import MemoryService
from broodmind.scheduler.service import SchedulerService
from broodmind.mcp.manager import MCPManager
from broodmind.policy.engine import PolicyEngine
from broodmind.providers.base import InferenceProvider
from broodmind.browser.manager import get_browser_manager
from broodmind.housekeeping import cleanup_workspace_tmp, rotate_canon_events
from broodmind.logging_config import correlation_id_var
from broodmind.queen.prompt_builder import (
    build_bootstrap_context_prompt,
    build_queen_prompt,
)
from broodmind.queen.router import (
    build_forced_worker_followup,
    normalize_plain_text,
    route_or_reply,
    route_worker_result_back_to_queen,
    should_force_worker_followup,
    should_send_worker_followup,
)
from broodmind.runtime_metrics import update_component_gauges
from broodmind.store.base import Store
from broodmind.store.models import AuditEvent
from broodmind.telegram.approvals import ApprovalManager
from broodmind.utils import is_control_response, should_suppress_user_delivery, utc_now
from broodmind.workers.contracts import TaskRequest, WorkerResult
from broodmind.workers.runtime import WorkerRuntime

logger = structlog.get_logger(__name__)
_FOLLOWUP_QUEUES: dict[int, asyncio.Queue] = {}
_FOLLOWUP_TASKS: dict[int, asyncio.Task] = {}
_INTERNAL_QUEUES: dict[int, asyncio.Queue] = {}
_INTERNAL_TASKS: dict[int, asyncio.Task] = {}
_QUEUE_IDLE_TIMEOUT_SECONDS = 300.0
# Worker-result follow-up can include multiple provider retries and a fallback
# pass through Queen, so it needs a wider budget than a single LLM request.
_WORKER_RESULT_ROUTING_TIMEOUT_SECONDS = 900.0
_RESET_CONFIRM_THRESHOLD = 2
_RESET_CONFIDENCE_MIN = 0.7
def _build_worker_result_timeout_followup(result: WorkerResult) -> str:
    """Return a minimal user-facing fallback when Queen routing times out."""
    summary = (result.summary or "").strip()
    if not summary:
        summary = "Worker finished, but the follow-up routing step timed out."

    lines = [summary]
    if result.questions:
        lines.append("")
        lines.append("Open questions:")
        lines.extend(f"- {question}" for question in result.questions[:3] if str(question).strip())

    return "\n".join(lines).strip()


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return max(minimum, value)


def _env_float(name: str, default: float, *, minimum: float = 0.0, maximum: float = 1.0) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return min(maximum, max(minimum, value))


_PENDING_CONVERSATIONAL_CLOSURE_TTL_SECONDS = _env_int(
    "BROODMIND_PENDING_CONVERSATIONAL_CLOSURE_TTL_SECONDS",
    3600,
    minimum=60,
)


_WATCH_THRESHOLDS = {
    "context_size_estimate": _env_int("BROODMIND_CONTEXT_WATCH_SIZE", 60000, minimum=5000),
    "repetition_score": _env_float("BROODMIND_CONTEXT_WATCH_REPETITION", 0.65, minimum=0.0, maximum=1.0),
    "error_streak": _env_int("BROODMIND_CONTEXT_WATCH_ERROR_STREAK", 3, minimum=1),
    "no_progress_turns": _env_int("BROODMIND_CONTEXT_WATCH_NO_PROGRESS", 4, minimum=1),
}
_RESET_SOON_THRESHOLDS = {
    "context_size_estimate": _env_int("BROODMIND_CONTEXT_RESET_SOON_SIZE", 100000, minimum=5000),
    "repetition_score": _env_float("BROODMIND_CONTEXT_RESET_SOON_REPETITION", 0.75, minimum=0.0, maximum=1.0),
    "error_streak": _env_int("BROODMIND_CONTEXT_RESET_SOON_ERROR_STREAK", 5, minimum=1),
    "no_progress_turns": _env_int("BROODMIND_CONTEXT_RESET_SOON_NO_PROGRESS", 7, minimum=1),
}

# Keep RESET_SOON at or above WATCH thresholds, even with custom env values.
_RESET_SOON_THRESHOLDS["context_size_estimate"] = max(
    int(_RESET_SOON_THRESHOLDS["context_size_estimate"]),
    int(_WATCH_THRESHOLDS["context_size_estimate"]),
)
_RESET_SOON_THRESHOLDS["repetition_score"] = max(
    float(_RESET_SOON_THRESHOLDS["repetition_score"]),
    float(_WATCH_THRESHOLDS["repetition_score"]),
)
_RESET_SOON_THRESHOLDS["error_streak"] = max(
    int(_RESET_SOON_THRESHOLDS["error_streak"]),
    int(_WATCH_THRESHOLDS["error_streak"]),
)
_RESET_SOON_THRESHOLDS["no_progress_turns"] = max(
    int(_RESET_SOON_THRESHOLDS["no_progress_turns"]),
    int(_WATCH_THRESHOLDS["no_progress_turns"]),
)


def _publish_runtime_metrics(thinking_count: int = 0) -> None:
    update_component_gauges(
        "queen",
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
            future, coro = await asyncio.wait_for(queue.get(), timeout=_QUEUE_IDLE_TIMEOUT_SECONDS)
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


async def _internal_worker(queen: Queen, chat_id: int, queue: asyncio.Queue) -> None:
    """Process completed worker results.

    Worker results are logged and stored in memory but NOT automatically sent to the user.
    The queen decides what to communicate based on worker results.
    """
    while True:
        try:
            task_text, result, correlation_id = await asyncio.wait_for(queue.get(), timeout=_QUEUE_IDLE_TIMEOUT_SECONDS)
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
                await queen.memory.add_message(
                    "system",
                    f"Worker completed: {result.summary}",
                    {"worker_result": True, "task": task_text, "chat_id": chat_id}
                )
            output_error = ""
            if isinstance(result.output, dict):
                raw_error = result.output.get("error")
                if raw_error is not None:
                    output_error = str(raw_error).strip()
            if output_error:
                await queen.memory.add_message(
                    "system",
                    f"Worker error: {output_error}",
                    {"worker_result": True, "task": task_text, "chat_id": chat_id}
                )
            # System/internal chat (chat_id <= 0) should never emit user-facing follow-ups.
            if chat_id <= 0:
                logger.info("Skipping user follow-up for internal chat", chat_id=chat_id)
            else:
                # Always route worker result back through Queen decision logic.
                # User delivery is a separate concern from internal decision-making.
                try:
                    final_text = await asyncio.wait_for(
                        route_worker_result_back_to_queen(queen, chat_id, task_text, result),
                        timeout=_WORKER_RESULT_ROUTING_TIMEOUT_SECONDS,
                    )
                except TimeoutError:
                    logger.warning("Worker-result routing timed out", chat_id=chat_id)
                    final_text = _build_worker_result_timeout_followup(result)

                pending_closure = queen.has_pending_conversational_closure(correlation_id)
                if (
                    not should_send_worker_followup(final_text)
                    and (should_force_worker_followup(result) or pending_closure)
                ):
                    logger.info("Forcing substantive worker follow-up", chat_id=chat_id)
                    final_text = build_forced_worker_followup(result)

                if should_send_worker_followup(final_text):
                    if queen.internal_send:
                        await queen.internal_send(chat_id, final_text)
                        queen.clear_pending_conversational_closure(correlation_id)
                        logger.info("Internal worker follow-up sent", chat_id=chat_id, text_len=len(final_text))
                        await queen.memory.add_message(
                            "assistant",
                            final_text,
                            {"chat_id": chat_id, "worker_followup": True},
                        )
                    else:
                        logger.info(
                            "Worker follow-up produced but no sender attached",
                            chat_id=chat_id,
                            text_len=len(final_text),
                        )
                else:
                    logger.info("Internal worker follow-up skipped", chat_id=chat_id, reason="no_user_response")
            logger.debug("Worker result processed", summary_len=len(result.summary or ""))
        except Exception:
            logger.exception("Failed to process internal worker result")
        finally:
            queue.task_done()
    _INTERNAL_TASKS.pop(chat_id, None)
    if queue.empty():
        _INTERNAL_QUEUES.pop(chat_id, None)
    _publish_runtime_metrics()


def _enqueue_internal_result(
    queen: Queen,
    chat_id: int,
    task_text: str,
    result: WorkerResult,
    *,
    correlation_id: str | None,
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
        _INTERNAL_TASKS[chat_id] = asyncio.create_task(_internal_worker(queen, chat_id, queue))
    queue.put_nowait((task_text, result, correlation_id))
    logger.info("Queued internal worker result", chat_id=chat_id, queue_size=queue.qsize())
    _publish_runtime_metrics()


@dataclass
class Queen:
    provider: InferenceProvider
    store: Store
    policy: PolicyEngine
    runtime: WorkerRuntime
    approvals: ApprovalManager
    memory: MemoryService
    canon: CanonService
    scheduler: SchedulerService | None = None
    mcp_manager: MCPManager | None = None
    internal_send: callable | None = None
    internal_progress_send: callable | None = None
    internal_typing_control: callable | None = None
    _cleanup_task: asyncio.Task | None = None
    _metrics_task: asyncio.Task | None = None
    _recent_tasks: set[str] = None  # Track tasks in current conversation to detect duplicates
    _approval_requesters: dict[int, Callable[[Any], Awaitable[bool]]] | None = None
    _thinking_count: int = 0
    _ws_active: bool = False
    _ws_owner: str | None = None
    _tg_send: callable | None = None
    _tg_progress: callable | None = None
    _tg_typing: callable | None = None
    _spawn_limits: dict[str, int] | None = None
    _worker_children: dict[str, set[str]] | None = None
    _worker_lineage: dict[str, str] | None = None
    _worker_depth: dict[str, int] | None = None
    _lineage_children_total: dict[str, int] | None = None
    _lineage_children_active: dict[str, set[str]] | None = None
    _housekeeping_cfg: dict[str, int] | None = None
    _pending_wakeup_by_chat: dict[int, str] | None = None
    _context_health_by_chat: dict[int, dict[str, Any]] | None = None
    _last_reply_norm_by_chat: dict[int, str] | None = None
    _pending_conversational_closure_by_correlation: dict[str, Any] | None = None
    _no_progress_turns_by_chat: dict[int, int] | None = None
    _progress_revision_by_chat: dict[int, int] | None = None
    _reset_streak_without_progress_by_chat: dict[int, int] | None = None
    _last_reset_progress_revision_by_chat: dict[int, int] | None = None
    _watch_escalation_streak_by_chat: dict[int, int] | None = None
    _self_queue_by_chat: dict[int, list[dict[str, Any]]] | None = None
    _last_opportunities_by_chat: dict[int, list[dict[str, Any]]] | None = None

    def __post_init__(self):
        if self._recent_tasks is None:
            self._recent_tasks = set()
        if self._approval_requesters is None:
            self._approval_requesters = {}
        if self._worker_children is None:
            self._worker_children = {}
        if self._worker_lineage is None:
            self._worker_lineage = {}
        if self._worker_depth is None:
            self._worker_depth = {}
        if self._lineage_children_total is None:
            self._lineage_children_total = {}
        if self._lineage_children_active is None:
            self._lineage_children_active = {}
        if self._pending_wakeup_by_chat is None:
            self._pending_wakeup_by_chat = {}
        if self._context_health_by_chat is None:
            self._context_health_by_chat = {}
        if self._last_reply_norm_by_chat is None:
            self._last_reply_norm_by_chat = {}
        if self._pending_conversational_closure_by_correlation is None:
            self._pending_conversational_closure_by_correlation = {}
        if self._no_progress_turns_by_chat is None:
            self._no_progress_turns_by_chat = {}
        if self._progress_revision_by_chat is None:
            self._progress_revision_by_chat = {}
        if self._reset_streak_without_progress_by_chat is None:
            self._reset_streak_without_progress_by_chat = {}
        if self._last_reset_progress_revision_by_chat is None:
            self._last_reset_progress_revision_by_chat = {}
        if self._watch_escalation_streak_by_chat is None:
            self._watch_escalation_streak_by_chat = {}
        if self._self_queue_by_chat is None:
            self._self_queue_by_chat = {}
        if self._last_opportunities_by_chat is None:
            self._last_opportunities_by_chat = {}
        if self._spawn_limits is None:
            max_depth = _env_int("BROODMIND_WORKER_MAX_SPAWN_DEPTH", 2, minimum=0)
            max_total = _env_int("BROODMIND_WORKER_MAX_CHILDREN_TOTAL", 20, minimum=1)
            max_concurrent = _env_int("BROODMIND_WORKER_MAX_CHILDREN_CONCURRENT", 10, minimum=1)
            self._spawn_limits = {
                "max_depth": max_depth,
                "max_children_total": max_total,
                "max_children_concurrent": max_concurrent,
            }
        if self._housekeeping_cfg is None:
            self._housekeeping_cfg = {
                "tmp_retention_hours": _env_int(
                    "BROODMIND_WORKSPACE_TMP_RETENTION_HOURS", 48, minimum=1
                ),
                "canon_events_max_bytes": _env_int(
                    "BROODMIND_CANON_EVENTS_MAX_BYTES", 2_000_000, minimum=1024
                ),
                "canon_events_keep_archives": _env_int(
                    "BROODMIND_CANON_EVENTS_KEEP_ARCHIVES", 7, minimum=1
                ),
            }
        self._restore_worker_registry_state()
        self._thinking_count = 0
        self._tg_send = self.internal_send
        self._tg_progress = self.internal_progress_send
        self._tg_typing = self.internal_typing_control

    @property
    def is_ws_active(self) -> bool:
        return self._ws_active

    def set_output_channel(
        self,
        is_ws: bool,
        send: callable | None = None,
        progress: callable | None = None,
        typing: callable | None = None,
        owner_id: str | None = None,
    ) -> bool:
        """Switch between Telegram and WebSocket output channels."""
        if is_ws:
            if self._ws_active and self._ws_owner and owner_id and self._ws_owner != owner_id:
                logger.warning(
                    "Rejected WebSocket channel switch due to existing owner",
                    current_owner=self._ws_owner,
                    attempted_owner=owner_id,
                )
                return False
        else:
            if self._ws_owner and owner_id and self._ws_owner != owner_id:
                logger.warning(
                    "Rejected output channel reset from non-owner",
                    current_owner=self._ws_owner,
                    attempted_owner=owner_id,
                )
                return False

        self._ws_active = is_ws
        if is_ws:
            self.internal_send = send
            self.internal_progress_send = progress
            self.internal_typing_control = typing
            self._ws_owner = owner_id or "ws-default"
            logger.info("Queen switched to WebSocket output channel")
        else:
            self.internal_send = self._tg_send
            self.internal_progress_send = self._tg_progress
            self.internal_typing_control = self._tg_typing
            self._ws_owner = None
            logger.info("Queen switched to Telegram output channel")
        
        # Update system status file if possible
        try:
            from broodmind.config.settings import load_settings
            from broodmind.state import read_status, _status_path
            import json
            settings = load_settings()
            status_data = read_status(settings) or {}
            status_data["active_channel"] = "WebSocket" if is_ws else "Telegram"
            _status_path(settings).write_text(json.dumps(status_data, indent=2), encoding="utf-8")
        except Exception:
            logger.debug("Failed to update status file with active channel", exc_info=True)
        return True

    async def set_thinking(self, active: bool) -> None:
        """Toggle global thinking indicator."""
        if active:
            self._thinking_count += 1
        else:
            self._thinking_count = max(0, self._thinking_count - 1)
        _publish_runtime_metrics(self._thinking_count)

    async def set_typing(self, chat_id: int, active: bool):
        """Toggle typing indicator for a specific chat."""
        if self.internal_typing_control:
            try:
                await self.internal_typing_control(chat_id, active)
            except Exception:
                logger.debug("Failed to set typing status", chat_id=chat_id, active=active, exc_info=True)

    async def _periodic_cleanup(self, interval_seconds: int):
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                deleted = await asyncio.to_thread(self.store.cleanup_old_workers)
                if deleted > 0:
                    logger.info("Periodic cleanup complete", deleted_workers=deleted)

                cfg = self._housekeeping_cfg or {}
                tmp_result = await asyncio.to_thread(
                    cleanup_workspace_tmp,
                    self.canon.workspace_dir,
                    retention_hours=int(cfg.get("tmp_retention_hours", 48)),
                )
                if tmp_result.deleted_files or tmp_result.deleted_dirs or tmp_result.errors:
                    logger.info(
                        "Workspace tmp cleanup complete",
                        deleted_files=tmp_result.deleted_files,
                        deleted_dirs=tmp_result.deleted_dirs,
                        errors=tmp_result.errors,
                    )

                rotate_result = await asyncio.to_thread(
                    rotate_canon_events,
                    self.canon.workspace_dir,
                    max_bytes=int(cfg.get("canon_events_max_bytes", 2_000_000)),
                    keep_archives=int(cfg.get("canon_events_keep_archives", 7)),
                )
                if rotate_result.rotated or rotate_result.deleted_archives:
                    logger.info(
                        "Canon events rotation complete",
                        rotated=rotate_result.rotated,
                        archived_file=rotate_result.archived_file,
                        deleted_archives=rotate_result.deleted_archives,
                        bootstrap_entries=rotate_result.bootstrap_entries,
                    )
            except Exception:
                logger.exception("Periodic worker cleanup failed")

    async def _periodic_metrics_publish(self, interval_seconds: int):
        from broodmind.runtime_metrics import update_component_gauges
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await asyncio.to_thread(self._reconcile_stale_worker_records)
                mcp_status = {}
                if self.mcp_manager:
                    mcp_status = self.mcp_manager.get_server_statuses()
                
                update_component_gauges(
                    "connectivity",
                    {
                        "mcp_servers": mcp_status
                    }
                )
            except Exception:
                logger.debug("Failed to publish periodic metrics", exc_info=True)

    def _reconcile_stale_worker_records(self) -> None:
        """Normalize stale DB worker states that no longer exist in runtime."""
        runtime = self.runtime
        if not runtime or not hasattr(runtime, "is_worker_running"):
            return
        workers = self.store.get_active_workers(older_than_minutes=120)
        if not workers:
            return
        grace_cutoff = utc_now() - timedelta(minutes=2)
        reconciled = 0
        for worker in workers:
            if worker.status not in {"started", "running"}:
                continue
            if worker.updated_at >= grace_cutoff:
                continue
            if runtime.is_worker_running(worker.id):
                continue
            self.store.update_worker_status(worker.id, "stopped")
            self.store.update_worker_result(
                worker.id,
                error="Worker process not found in runtime; stale running state reconciled.",
            )
            self._mark_worker_inactive(worker.id)
            reconciled += 1
        if reconciled > 0:
            logger.info("Reconciled stale worker records", reconciled_workers=reconciled)

    def _restore_worker_registry_state(self) -> None:
        """Restore lineage/child bookkeeping from persisted workers."""
        if not hasattr(self.store, "list_workers"):
            return
        try:
            workers = list(self.store.list_workers() or [])
        except Exception:
            logger.debug("Skipping worker registry restore: list_workers failed", exc_info=True)
            return
        if not workers:
            return

        self._worker_children.clear()
        self._worker_lineage.clear()
        self._worker_depth.clear()
        self._lineage_children_total.clear()
        self._lineage_children_active.clear()

        worker_by_id: dict[str, Any] = {}
        for worker in workers:
            run_id = str(getattr(worker, "id", "") or "").strip()
            if not run_id:
                continue
            worker_by_id[run_id] = worker
            lineage_id = str(getattr(worker, "lineage_id", "") or run_id).strip() or run_id
            depth = max(0, int(getattr(worker, "spawn_depth", 0) or 0))
            self._worker_lineage[run_id] = lineage_id
            self._worker_depth[run_id] = depth

        orphan_reconciled = 0
        for run_id, worker in worker_by_id.items():
            parent_worker_id = str(getattr(worker, "parent_worker_id", "") or "").strip()
            if not parent_worker_id:
                continue
            if parent_worker_id not in worker_by_id:
                if _is_active_worker_status(getattr(worker, "status", "")):
                    self.store.update_worker_status(run_id, "stopped")
                    self.store.update_worker_result(
                        run_id,
                        error=(
                            "Orphaned child worker reconciled during startup: "
                            "parent worker record is missing."
                        ),
                    )
                    orphan_reconciled += 1
                continue
            lineage_id = self._worker_lineage.get(run_id, run_id)
            self._worker_children.setdefault(parent_worker_id, set()).add(run_id)
            self._lineage_children_total[lineage_id] = (
                int(self._lineage_children_total.get(lineage_id, 0)) + 1
            )
            if _is_active_worker_status(getattr(worker, "status", "")):
                self._lineage_children_active.setdefault(lineage_id, set()).add(run_id)

        stale_reconciled = self._reconcile_startup_stale_workers(worker_by_id)
        if orphan_reconciled or stale_reconciled:
            logger.info(
                "Restored worker registry state",
                workers_seen=len(worker_by_id),
                orphan_reconciled=orphan_reconciled,
                stale_reconciled=stale_reconciled,
            )

    def _reconcile_startup_stale_workers(self, worker_by_id: dict[str, Any]) -> int:
        runtime = self.runtime
        if not runtime or not hasattr(runtime, "is_worker_running"):
            return 0
        reconciled = 0
        for run_id, worker in worker_by_id.items():
            if not _is_active_worker_status(getattr(worker, "status", "")):
                continue
            if runtime.is_worker_running(run_id):
                continue
            self.store.update_worker_status(run_id, "stopped")
            self.store.update_worker_result(
                run_id,
                error="Worker process not found in runtime during startup reconciliation.",
            )
            self._mark_worker_inactive(run_id)
            reconciled += 1
        return reconciled

    def start_background_tasks(self, cleanup_interval_seconds: int = 3600):
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._periodic_cleanup(cleanup_interval_seconds))
            logger.info("Started periodic worker cleanup task")
        if self._metrics_task is None or self._metrics_task.done():
            self._metrics_task = asyncio.create_task(self._periodic_metrics_publish(10))
            logger.info("Started periodic metrics publishing task")

    async def stop_background_tasks(self):
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                logger.info("Stopped periodic worker cleanup task")
        
        if self._metrics_task and not self._metrics_task.done():
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                logger.info("Stopped periodic metrics publishing task")
        
        # Shutdown MCP sessions
        if self.mcp_manager:
            await self.mcp_manager.shutdown()
        
        # Shutdown browser sessions
        await get_browser_manager().shutdown()

    async def initialize_system(self, bot=None, allowed_chat_ids: list[int] | None = None) -> None:
        system_chat_id = 0
        logger.info("Queen waking up")
        self.start_background_tasks()
        
        # Load and connect MCP servers
        if self.mcp_manager:
            await self.mcp_manager.load_and_connect_all()
        
        wake_up_prompt = (
            "You are waking up. Read AGENTS.md and inspect available workers internally. "
            "Use tools if needed, but never output a tool name or tool syntax as your final answer. "
            "Then produce a short friendly startup status message for the user in plain language."
        )
        original_send = self.internal_send
        chat_ids = allowed_chat_ids or []
        if chat_ids and (bot or callable(original_send)):
            logger.info("Queen will send initialization message", count=len(chat_ids))
            logger.debug("Allowed chat_ids", chat_ids=chat_ids)
            async def send_to_allowed_chats(chat_id, text):
                for target_chat_id in chat_ids:
                    try:
                        if callable(original_send):
                            # Reuse the active channel send pipeline when one is attached.
                            await original_send(target_chat_id, text)
                        else:
                            await bot.send_message(chat_id=target_chat_id, text=text)
                        logger.debug("Sent initialization message", chat_id=target_chat_id)
                    except Exception as e:
                        logger.warning("Failed to send to chat_id", chat_id=target_chat_id, error=e)
            self.internal_send = send_to_allowed_chats
        else:
            logger.warning("No allowed user channel recipients configured; queen will not send ready message.")
            self.internal_send = None
        try:
            bootstrap_context = await build_bootstrap_context_prompt(self.store, system_chat_id)
            result = await route_or_reply(
                self,
                self.provider,
                self.memory,
                wake_up_prompt,
                system_chat_id,
                bootstrap_context.content,
            )
            if should_suppress_user_delivery(result):
                result = (
                    "Queen is online. Initialization is complete and I am ready for your tasks."
                )
            logger.info("Queen wake up complete", result_preview=f"{result[:60]}..." if result else "empty")
            
            # Send the Queen's own response to allowed chats if configured.
            if result and self.internal_send and chat_ids:
                try:
                    await self.internal_send(system_chat_id, result)
                    logger.info("Queen initialization response sent")
                except Exception as e:
                    logger.warning("Failed to send queen initialization response", error=e)
        except Exception:
            logger.exception("Queen failed to complete wake-up task")
        finally:
            self.internal_send = original_send

    def peek_context_wakeup(self, chat_id: int) -> str:
        pending = self._pending_wakeup_by_chat or {}
        return str(pending.get(chat_id, "") or "")

    def has_pending_conversational_closure(self, correlation_id: str | None) -> bool:
        if not correlation_id:
            return False
        self._prune_pending_conversational_closures()
        pending = self._pending_conversational_closure_by_correlation or {}
        return correlation_id in pending

    def mark_pending_conversational_closure(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        self._prune_pending_conversational_closures()
        pending = self._pending_conversational_closure_by_correlation or {}
        pending[correlation_id] = utc_now()

    def clear_pending_conversational_closure(self, correlation_id: str | None) -> None:
        if not correlation_id:
            return
        pending = self._pending_conversational_closure_by_correlation or {}
        pending.pop(correlation_id, None)

    def _prune_pending_conversational_closures(self) -> None:
        pending = self._pending_conversational_closure_by_correlation or {}
        if not pending:
            return
        cutoff = utc_now() - timedelta(seconds=_PENDING_CONVERSATIONAL_CLOSURE_TTL_SECONDS)
        expired = [
            correlation_id
            for correlation_id, created_at in pending.items()
            if not created_at or created_at < cutoff
        ]
        for correlation_id in expired:
            pending.pop(correlation_id, None)

    def clear_context_wakeup(self, chat_id: int) -> None:
        pending = self._pending_wakeup_by_chat or {}
        pending.pop(chat_id, None)

    def get_context_thresholds(self) -> dict[str, dict[str, float | int]]:
        return {
            "watch": dict(_WATCH_THRESHOLDS),
            "reset_soon": dict(_RESET_SOON_THRESHOLDS),
        }

    async def get_self_queue(self, chat_id: int) -> list[dict[str, Any]]:
        await self._ensure_self_queue_loaded(chat_id)
        queue = list((self._self_queue_by_chat or {}).get(chat_id, []))
        return [dict(item) for item in queue]

    async def _ensure_self_queue_loaded(self, chat_id: int) -> None:
        if chat_id in self._self_queue_by_chat:
            return
        loaded = await asyncio.to_thread(_load_self_queue, _workspace_dir(), chat_id)
        self._self_queue_by_chat[chat_id] = loaded

    async def add_self_queue_item(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        await self._ensure_self_queue_loaded(chat_id)
        title = str((args or {}).get("title", "") or "").strip()
        task = str((args or {}).get("task", "") or "").strip()
        if not title or not task:
            return {"status": "error", "message": "title and task are required"}
        priority = max(1, min(5, int((args or {}).get("priority", 3) or 3)))
        source = str((args or {}).get("source", "queen") or "queen").strip()[:64]
        queue = self._self_queue_by_chat.setdefault(chat_id, [])
        item = {
            "task_id": str(uuid4()),
            "title": title,
            "task": task,
            "priority": priority,
            "source": source,
            "status": "pending",
            "created_at": utc_now().isoformat(),
            "updated_at": utc_now().isoformat(),
            "notes": str((args or {}).get("notes", "") or "").strip(),
        }
        queue.append(item)
        queue.sort(key=lambda i: (-int(i.get("priority", 3)), str(i.get("created_at", ""))))
        await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
        return {"status": "ok", "item": item, "queue_size": len(queue)}

    async def take_next_self_queue_item(self, chat_id: int) -> dict[str, Any]:
        await self._ensure_self_queue_loaded(chat_id)
        queue = self._self_queue_by_chat.setdefault(chat_id, [])
        for item in queue:
            if str(item.get("status", "pending")) == "pending":
                item["status"] = "claimed"
                item["updated_at"] = utc_now().isoformat()
                await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
                return {"status": "ok", "item": dict(item)}
        return {"status": "empty", "message": "no pending self-queue items"}

    async def update_self_queue_item(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        await self._ensure_self_queue_loaded(chat_id)
        task_id = str((args or {}).get("task_id", "") or "").strip()
        new_status = str((args or {}).get("status", "") or "").strip().lower()
        if not task_id or new_status not in {"pending", "claimed", "done", "cancelled"}:
            return {"status": "error", "message": "task_id and valid status are required"}
        queue = self._self_queue_by_chat.setdefault(chat_id, [])
        for item in queue:
            if str(item.get("task_id", "")) == task_id:
                item["status"] = new_status
                if "notes" in (args or {}):
                    item["notes"] = str((args or {}).get("notes", "") or "").strip()
                item["updated_at"] = utc_now().isoformat()
                await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
                return {"status": "ok", "item": dict(item)}
        return {"status": "not_found", "task_id": task_id}

    async def scan_opportunities(self, chat_id: int, limit: int = 3) -> dict[str, Any]:
        health = await self.get_context_health_snapshot(chat_id)
        await self._ensure_self_queue_loaded(chat_id)
        queue = self._self_queue_by_chat.setdefault(chat_id, [])
        opportunities: list[dict[str, Any]] = []

        context_size = int(health.get("context_size_estimate", 0) or 0)
        repetition = float(health.get("repetition_score", 0.0) or 0.0)
        no_progress = int(health.get("no_progress_turns", 0) or 0)
        resets_since_progress = int(health.get("resets_since_progress", 0) or 0)
        context_health = str(health.get("context_health", "OK") or "OK")

        if context_health == "RESET_SOON":
            opportunities.append(
                _build_opportunity_card(
                    kind="stability",
                    title="Context compaction before quality drops",
                    why_now=f"context_health={context_health}, size={context_size}",
                    impact="high",
                    effort="low",
                    confidence=0.93,
                    next_action="Call queen_context_reset(mode='soft') with concise handoff.",
                )
            )
        if no_progress >= 3 or repetition >= 0.70:
            opportunities.append(
                _build_opportunity_card(
                    kind="momentum",
                    title="Break stagnation with a replan cycle",
                    why_now=f"no_progress_turns={no_progress}, repetition_score={round(repetition, 3)}",
                    impact="high",
                    effort="medium",
                    confidence=0.82,
                    next_action="Create 1 focused task in self-queue and execute immediately.",
                )
            )
        if resets_since_progress >= 1:
            opportunities.append(
                _build_opportunity_card(
                    kind="recovery",
                    title="Post-reset recovery check",
                    why_now=f"resets_since_progress={resets_since_progress}",
                    impact="medium",
                    effort="low",
                    confidence=0.78,
                    next_action="Audit open threads and lock one measurable next step.",
                )
            )
        if not opportunities:
            opportunities.append(
                _build_opportunity_card(
                    kind="improvement",
                    title="Proactive improvement pass",
                    why_now="system is stable; use spare cycle for compounding gains",
                    impact="medium",
                    effort="medium",
                    confidence=0.74,
                    next_action="Pick one automation or cleanup task and add it to self-queue.",
                )
            )

        opportunities = opportunities[: max(1, min(limit, 5))]
        self._last_opportunities_by_chat[chat_id] = [dict(item) for item in opportunities]
        await asyncio.to_thread(_persist_last_opportunities, _workspace_dir(), chat_id, opportunities)
        pending_count = sum(1 for item in queue if str(item.get("status", "pending")) == "pending")
        return {
            "status": "ok",
            "chat_id": chat_id,
            "opportunities": opportunities,
            "queue_pending": pending_count,
            "generated_at": utc_now().isoformat(),
        }

    async def get_context_health_snapshot(self, chat_id: int) -> dict[str, Any]:
        recent_entries_all = await asyncio.to_thread(self.store.list_memory_entries_by_chat, chat_id, 120)
        recent_entries = [
            entry
            for entry in recent_entries_all
            if not bool((entry.metadata or {}).get("heartbeat"))
        ]
        entry_count = len(recent_entries)
        context_size_estimate = sum(len(e.content or "") for e in recent_entries)
        repetition_score = _estimate_repetition_score(recent_entries)
        error_streak = _estimate_error_streak(recent_entries)
        no_progress_turns = int((self._no_progress_turns_by_chat or {}).get(chat_id, 0))
        resets_since_progress = int((self._reset_streak_without_progress_by_chat or {}).get(chat_id, 0))
        overload_score = min(
            1.0,
            (context_size_estimate / float(_WATCH_THRESHOLDS["context_size_estimate"]))
            + (repetition_score * 0.9)
            + (min(8, error_streak) / 10.0)
            + (min(12, no_progress_turns) / 12.0),
        )
        watch_conditions = _watch_conditions(
            context_size_estimate=context_size_estimate,
            repetition_score=repetition_score,
            error_streak=error_streak,
            no_progress_turns=no_progress_turns,
        )
        watch_signal_count = sum(1 for cond in watch_conditions if cond)
        watch_escalation_streak = int((self._watch_escalation_streak_by_chat or {}).get(chat_id, 0))
        if watch_signal_count >= 2:
            watch_escalation_streak += 1
        else:
            watch_escalation_streak = 0
        self._watch_escalation_streak_by_chat[chat_id] = watch_escalation_streak
        severe = _is_reset_soon_severe(
            context_size_estimate=context_size_estimate,
            repetition_score=repetition_score,
            error_streak=error_streak,
            no_progress_turns=no_progress_turns,
        )
        context_health = "RESET_SOON" if (severe or watch_escalation_streak >= 2) else ("WATCH" if watch_signal_count > 0 else "OK")
        snapshot = {
            "chat_id": chat_id,
            "entry_count": entry_count,
            "context_size_estimate": context_size_estimate,
            "repetition_score": round(repetition_score, 3),
            "error_streak": error_streak,
            "no_progress_turns": no_progress_turns,
            "resets_since_progress": resets_since_progress,
            "overload_score": round(overload_score, 3),
            "watch_signal_count": watch_signal_count,
            "watch_escalation_streak": watch_escalation_streak,
            "context_health": context_health,
            "updated_at": utc_now().isoformat(),
        }
        self._context_health_by_chat[chat_id] = snapshot
        return snapshot

    async def build_heartbeat_context_hint(self, chat_id: int) -> str:
        snap = await self.get_context_health_snapshot(chat_id)
        return (
            "Context health metrics:\n"
            f"- context_size_estimate={snap['context_size_estimate']}\n"
            f"- repetition_score={snap['repetition_score']}\n"
            f"- error_streak={snap['error_streak']}\n"
            f"- no_progress_turns={snap['no_progress_turns']}\n"
            f"- resets_since_progress={snap['resets_since_progress']}\n"
            f"- overload_score={snap['overload_score']}\n"
            f"- watch_signal_count={snap['watch_signal_count']}\n"
            f"- watch_escalation_streak={snap['watch_escalation_streak']}\n"
            f"- context_health={snap['context_health']}\n"
            "Decision thresholds:\n"
            f"- WATCH if any: size>={_WATCH_THRESHOLDS['context_size_estimate']}, repetition>={_WATCH_THRESHOLDS['repetition_score']:.2f}, "
            f"error_streak>={_WATCH_THRESHOLDS['error_streak']}, no_progress>={_WATCH_THRESHOLDS['no_progress_turns']}.\n"
            f"- RESET_SOON if any: size>={_RESET_SOON_THRESHOLDS['context_size_estimate']}, repetition>={_RESET_SOON_THRESHOLDS['repetition_score']:.2f}, "
            f"error_streak>={_RESET_SOON_THRESHOLDS['error_streak']}, no_progress>={_RESET_SOON_THRESHOLDS['no_progress_turns']}.\n"
            "- Also RESET_SOON if 2+ WATCH signals persist for 2+ heartbeats.\n"
            "If context_health is RESET_SOON, call `queen_context_reset` with mode='soft' and a concise handoff."
        )

    def _register_progress(self, chat_id: int, reason: str) -> None:
        self._no_progress_turns_by_chat[chat_id] = 0
        self._reset_streak_without_progress_by_chat[chat_id] = 0
        self._progress_revision_by_chat[chat_id] = int(self._progress_revision_by_chat.get(chat_id, 0)) + 1
        logger.debug("Registered progress", chat_id=chat_id, reason=reason)

    async def request_context_reset(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        mode = str(args.get("mode", "soft") or "soft").strip().lower()
        if mode not in {"soft", "hard"}:
            mode = "soft"

        reason = str(args.get("reason", "") or "").strip() or "context overloaded"
        confidence = _coerce_float(args.get("confidence"), default=0.8)
        confirm = bool(args.get("confirm", False))
        health = await self.get_context_health_snapshot(chat_id)

        progress_rev = int(self._progress_revision_by_chat.get(chat_id, 0))
        last_reset_rev = int(self._last_reset_progress_revision_by_chat.get(chat_id, -1))
        no_progress_since_last_reset = progress_rev <= last_reset_rev
        current_streak = int(self._reset_streak_without_progress_by_chat.get(chat_id, 0))
        proposed_streak = (current_streak + 1) if no_progress_since_last_reset else 1

        requires_confirm_reasons: list[str] = []
        if mode == "hard":
            requires_confirm_reasons.append("hard_reset")
        if confidence < _RESET_CONFIDENCE_MIN:
            requires_confirm_reasons.append("low_confidence_handoff")
        if proposed_streak >= _RESET_CONFIRM_THRESHOLD:
            requires_confirm_reasons.append("repeated_reset_without_progress")
        if requires_confirm_reasons and not confirm:
            return {
                "status": "needs_confirmation",
                "mode": mode,
                "reason": reason,
                "confidence": confidence,
                "requires_confirmation_for": requires_confirm_reasons,
                "message": (
                    "Reset blocked until confirmation. Re-run queen_context_reset with confirm=true "
                    "to proceed."
                ),
                "health": health,
            }

        handoff = {
            "chat_id": chat_id,
            "created_at": utc_now().isoformat(),
            "mode": mode,
            "reason": reason,
            "confidence": confidence,
            "goal_now": str(args.get("goal_now", "") or "").strip(),
            "done": _normalize_string_list(args.get("done")),
            "open_threads": _normalize_string_list(args.get("open_threads")),
            "critical_constraints": _normalize_string_list(args.get("critical_constraints")),
            "next_step": str(args.get("next_step", "") or "").strip(),
            "current_interest": str(args.get("current_interest", "") or "").strip(),
            "pending_human_input": str(args.get("pending_human_input", "") or "").strip(),
            "cognitive_state": str(args.get("cognitive_state", "") or "focused").strip().lower(),
            "health_snapshot": health,
        }
        if not handoff["goal_now"]:
            handoff["goal_now"] = "Continue current task with focused context."
        if not handoff["next_step"]:
            handoff["next_step"] = "Review handoff and choose: continue, clarify, or replan."

        workspace_dir = Path(os.getenv("BROODMIND_WORKSPACE_DIR", "workspace")).resolve()
        file_info = await asyncio.to_thread(_persist_context_reset_files, workspace_dir, handoff)
        memchain_info: dict[str, Any] | None = None
        try:
            memchain_info = await asyncio.to_thread(
                memchain_record,
                workspace_dir,
                reason="context_reset",
                meta={"mode": mode, "chat_id": chat_id, "source": "queen_context_reset"},
            )
        except Exception as exc:
            logger.warning("Memchain record failed during context reset", chat_id=chat_id, error=str(exc))

        deleted_entries = await asyncio.to_thread(
            self.store.delete_memory_entries_by_chat,
            chat_id,
            0,
        )
        if mode == "hard":
            await asyncio.to_thread(self.store.set_chat_bootstrap_hash, chat_id, "", utc_now())

        self._last_reply_norm_by_chat.pop(chat_id, None)
        self._last_reset_progress_revision_by_chat[chat_id] = progress_rev
        self._reset_streak_without_progress_by_chat[chat_id] = proposed_streak
        self._pending_wakeup_by_chat[chat_id] = _build_wakeup_message(handoff, file_info["handoff_md"])
        self._no_progress_turns_by_chat[chat_id] = 0

        await asyncio.to_thread(
            self.store.append_audit,
            AuditEvent(
                id=str(uuid4()),
                ts=utc_now(),
                level="info",
                event_type="queen.context_reset",
                data={
                    "chat_id": chat_id,
                    "mode": mode,
                    "reason": reason,
                    "confidence": confidence,
                    "deleted_entries": deleted_entries,
                    "requires_confirmation_for": requires_confirm_reasons,
                    "health_snapshot": health,
                    "files": file_info,
                    "memchain": memchain_info or {},
                },
            ),
        )

        return {
            "status": "reset_complete",
            "mode": mode,
            "deleted_entries": deleted_entries,
            "handoff": handoff,
            "files": file_info,
            "memchain": memchain_info or {},
            "health_before": health,
            "requires_confirmation_for": requires_confirm_reasons,
            "message": "Context reset completed. Wake-up handoff is queued for the next turn.",
        }

    async def handle_message(
        self,
        text: str,
        chat_id: int,
        approval_requester=None,
        show_typing: bool = True,
        is_ws: bool = False,
        images: list[str] | None = None,
        persist_to_memory: bool = True,
        track_progress: bool = True,
        include_wakeup: bool = True,
    ) -> QueenReply:
        if not is_ws and self._ws_active:
            logger.info("Ignoring Telegram message while WebSocket is active", chat_id=chat_id)
            return QueenReply(
                immediate="I'm currently active on WebSocket. Please use the WebSocket client or wait until it's closed.",
                followup=None,
            )
        correlation_token = None
        correlation_id = correlation_id_var.get()
        if not correlation_id:
            correlation_id = f"turn-{uuid4()}"
            correlation_token = correlation_id_var.set(correlation_id)

        try:
            # Clear recent tasks at the start of each new user message
            self._recent_tasks.clear()
            if callable(approval_requester):
                self._approval_requesters[chat_id] = approval_requester
            logger.info("Handling message", chat_id=chat_id, is_ws=is_ws, has_images=bool(images))
            logger.debug("Received message text", text_len=len(text), text=text[:500])
            if persist_to_memory:
                await self.memory.add_message(
                    "user",
                    text,
                    {"chat_id": chat_id, "has_images": bool(images), "heartbeat": not track_progress},
                )
            bootstrap_context = await build_bootstrap_context_prompt(self.store, chat_id)
            if bootstrap_context.files:
                files_summary = ", ".join([f"{name} ({size} chars)" for name, size in bootstrap_context.files])
                logger.debug("Queen bootstrap files", files=files_summary, hash=bootstrap_context.hash)
            route_kwargs: dict[str, Any] = {
                "show_typing": show_typing,
                "images": images,
                "include_wakeup": include_wakeup,
            }
            while True:
                try:
                    reply_text = await route_or_reply(
                        self,
                        self.provider,
                        self.memory,
                        text,
                        chat_id,
                        bootstrap_context.content,
                        **route_kwargs,
                    )
                    break
                except TypeError as exc:
                    # Backward-compatible fallback for monkeypatched tests/extensions using older signatures.
                    msg = str(exc)
                    if "unexpected keyword argument" not in msg:
                        raise
                    removed = False
                    for key in list(route_kwargs.keys()):
                        if f"'{key}'" in msg:
                            route_kwargs.pop(key, None)
                            removed = True
                            break
                    if not removed:
                        raise
            reply_text, wants_followup = _extract_followup_required_marker(reply_text)
            logger.info("Queen response ready")
            if persist_to_memory:
                await self.memory.add_message("assistant", reply_text, {"chat_id": chat_id, "heartbeat": not track_progress})
            if track_progress:
                reply_norm = _normalize_compact(reply_text)
                prior_reply = self._last_reply_norm_by_chat.get(chat_id, "")
                if _is_progress_reply(reply_norm, prior_reply):
                    self._register_progress(chat_id, "assistant_response")
                else:
                    self._no_progress_turns_by_chat[chat_id] = int(self._no_progress_turns_by_chat.get(chat_id, 0)) + 1
                self._last_reply_norm_by_chat[chat_id] = reply_norm
            if wants_followup:
                self.mark_pending_conversational_closure(correlation_id)
            try:
                await self.get_context_health_snapshot(chat_id)
            except Exception:
                logger.debug("Failed to refresh context health snapshot", chat_id=chat_id, exc_info=True)
            if include_wakeup:
                self.clear_context_wakeup(chat_id)
            if bootstrap_context.hash:
                await asyncio.to_thread(
                    self.store.set_chat_bootstrap_hash, chat_id, bootstrap_context.hash, utc_now()
                )
            return QueenReply(
                immediate=normalize_plain_text(reply_text),
                followup=None,
                followup_required=wants_followup,
            )
        finally:
            if correlation_token is not None:
                correlation_id_var.reset(correlation_token)

    async def _start_worker_async(
        self,
        worker_id: str,
        task: str,
        chat_id: int,
        inputs: dict[str, Any] | None,
        tools: list[str] | None,
        model: str | None,
        timeout_seconds: int | None,
        scheduled_task_id: str | None = None,
        parent_worker_id: str | None = None,
        lineage_id: str | None = None,
        root_task_id: str | None = None,
        spawn_depth: int = 0,
    ) -> dict[str, Any]:
        if parent_worker_id:
            violation = self._check_child_spawn_limits(
                lineage_id=lineage_id,
                spawn_depth=spawn_depth,
            )
            if violation:
                return {
                    "status": "rejected",
                    "reason": violation,
                    "worker_id": None,
                    "run_id": None,
                }

        # Create a task signature for duplicate detection
        schedule_sig = scheduled_task_id or "-"
        parent_sig = parent_worker_id or "-"
        task_signature = f"{worker_id}:{schedule_sig}:{parent_sig}:{task[:100]}"  # Keep duplicate detection strict per schedule/task pair.
        if task_signature in self._recent_tasks:
            logger.warning("Duplicate worker task detected, skipping", worker_id=worker_id, task_prefix=task[:50])
            skipped_id = f"skipped-duplicate-{uuid4().hex[:8]}"
            await self._emit_progress(
                chat_id,
                "duplicate",
                "Duplicate worker request detected; skipping duplicate launch.",
                {"worker_template_id": worker_id},
            )
            return {
                "status": "skipped_duplicate",
                "run_id": skipped_id,
                "worker_id": None,
            }

        self._recent_tasks.add(task_signature)

        run_id = str(uuid4())
        effective_lineage_id = lineage_id or run_id
        effective_root_task_id = root_task_id or run_id
        effective_spawn_depth = max(0, int(spawn_depth))
        self._register_worker_lineage(
            run_id=run_id,
            lineage_id=effective_lineage_id,
            spawn_depth=effective_spawn_depth,
            parent_worker_id=parent_worker_id,
        )
        await self._emit_progress(
            chat_id,
            "queued",
            f"Queued worker '{worker_id}' as {run_id}.",
            {
                "worker_id": run_id,
                "worker_template_id": worker_id,
                "lineage_id": effective_lineage_id,
                "parent_worker_id": parent_worker_id,
                "spawn_depth": effective_spawn_depth,
            },
        )
        task_request = TaskRequest(
            worker_id=worker_id,
            task=task,
            inputs=inputs or {},
            tools=tools,
            model=model,
            timeout_seconds=timeout_seconds,
            run_id=run_id,
            correlation_id=correlation_id_var.get(),
            parent_worker_id=parent_worker_id,
            lineage_id=effective_lineage_id,
            root_task_id=effective_root_task_id,
            spawn_depth=effective_spawn_depth,
        )

        requester = self._approval_requesters.get(chat_id)
        if requester is None and getattr(self.approvals, "bot", None):
            async def _telegram_requester(intent: ActionIntent) -> bool:
                return await self.approvals.request_approval(chat_id, intent)

            requester = _telegram_requester

        async def _runner() -> None:
            failed = False
            try:
                await self._emit_progress(
                    chat_id,
                    "running",
                    f"Worker {run_id} is running.",
                    {"worker_id": run_id, "worker_template_id": worker_id},
                )
                result = await self.runtime.run_task(task_request, approval_requester=requester)
                worker_record = await asyncio.to_thread(self.store.get_worker, run_id)
                worker_status = getattr(worker_record, "status", None)
                failed = worker_status in {"failed", "stopped"}
                if scheduled_task_id and self.scheduler:
                    if not failed:
                        self.scheduler.mark_executed(scheduled_task_id)
                        logger.info(
                            "Marked scheduled task as executed after worker completion",
                            task_id=scheduled_task_id,
                            run_id=run_id,
                            worker_status=worker_status,
                        )
                    else:
                        logger.warning(
                            "Skipped scheduled task execution mark due to non-completed worker state",
                            task_id=scheduled_task_id,
                            run_id=run_id,
                            worker_status=worker_status,
                        )
                if not failed:
                    self._register_progress(chat_id, "worker_completed")
                await self._emit_progress(
                    chat_id,
                    "completed",
                    f"Worker {run_id} completed.",
                    {"worker_id": run_id, "worker_template_id": worker_id},
                )
            except Exception as exc:
                failed = True
                result = WorkerResult(summary=f"Worker error: {exc}", output={"error": str(exc)})
                await self._emit_progress(
                    chat_id,
                    "failed",
                    f"Worker {run_id} failed: {exc}",
                    {"worker_id": run_id, "worker_template_id": worker_id},
                )
            if failed:
                await self._cleanup_orphan_children(
                    parent_run_id=run_id,
                    chat_id=chat_id,
                    reason="parent_failed",
                )
            self._mark_worker_inactive(run_id)
            _enqueue_internal_result(
                self,
                chat_id,
                task,
                result,
                correlation_id=task_request.correlation_id,
            )
        asyncio.create_task(_runner())
        await self._emit_progress(
            chat_id,
            "worker_started",
            f"Worker started: {run_id}",
            {
                "worker_id": run_id,
                "worker_template_id": worker_id,
                "lineage_id": effective_lineage_id,
                "parent_worker_id": parent_worker_id,
                "spawn_depth": effective_spawn_depth,
            },
        )
        return {
            "status": "started",
            "run_id": run_id,
            "worker_id": run_id,
            "lineage_id": effective_lineage_id,
            "parent_worker_id": parent_worker_id,
            "root_task_id": effective_root_task_id,
            "spawn_depth": effective_spawn_depth,
        }

    def _check_child_spawn_limits(self, *, lineage_id: str | None, spawn_depth: int) -> str | None:
        limits = self._spawn_limits or {}
        max_depth = int(limits.get("max_depth", 0))
        max_children_total = int(limits.get("max_children_total", 1))
        max_children_concurrent = int(limits.get("max_children_concurrent", 1))

        if spawn_depth > max_depth:
            return f"spawn depth {spawn_depth} exceeds max depth {max_depth}"

        if not lineage_id:
            return None

        total_started = int((self._lineage_children_total or {}).get(lineage_id, 0))
        if total_started >= max_children_total:
            return (
                f"lineage child limit reached ({total_started}/{max_children_total}); "
                "cannot spawn more child workers"
            )

        active = len((self._lineage_children_active or {}).get(lineage_id, set()))
        if active >= max_children_concurrent:
            return (
                f"lineage concurrent child limit reached ({active}/{max_children_concurrent}); "
                "wait for running children to complete"
            )
        return None

    def _register_worker_lineage(
        self,
        *,
        run_id: str,
        lineage_id: str,
        spawn_depth: int,
        parent_worker_id: str | None,
    ) -> None:
        self._worker_lineage[run_id] = lineage_id
        self._worker_depth[run_id] = spawn_depth
        if parent_worker_id:
            self._worker_children.setdefault(parent_worker_id, set()).add(run_id)
            self._lineage_children_total[lineage_id] = int(self._lineage_children_total.get(lineage_id, 0)) + 1
            active = self._lineage_children_active.setdefault(lineage_id, set())
            active.add(run_id)

    def _mark_worker_inactive(self, run_id: str) -> None:
        lineage_id = self._worker_lineage.get(run_id)
        if not lineage_id:
            return
        active = self._lineage_children_active.get(lineage_id)
        if active and run_id in active:
            active.discard(run_id)
            if not active:
                self._lineage_children_active.pop(lineage_id, None)

    async def _cleanup_orphan_children(self, *, parent_run_id: str, chat_id: int, reason: str) -> None:
        child_ids = sorted(self._worker_children.get(parent_run_id, set()))
        if not child_ids:
            return
        for child_id in child_ids:
            stopped = await self.runtime.stop_worker(child_id)
            self._mark_worker_inactive(child_id)
            if stopped:
                await self._emit_progress(
                    chat_id,
                    "child_stopped",
                    f"Stopped orphan child worker {child_id} ({reason}).",
                    {"worker_id": child_id, "reason": reason, "parent_worker_id": parent_run_id},
                )

    async def _emit_progress(
        self,
        chat_id: int,
        state: str,
        text: str,
        meta: dict[str, Any] | None = None,
    ) -> None:
        sender = self.internal_progress_send
        if not sender:
            return
        try:
            await sender(chat_id, state, text, meta or {})
        except Exception:
            logger.debug("Progress emit failed", exc_info=True)


def _normalize_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        items = [chunk.strip() for chunk in value.split("\n")]
    else:
        return []
    normalized: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            normalized.append(text)
    return normalized[:20]


def _is_active_worker_status(status: Any) -> bool:
    return str(status or "").strip().lower() in {"started", "running"}


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _watch_conditions(
    *,
    context_size_estimate: int,
    repetition_score: float,
    error_streak: int,
    no_progress_turns: int,
) -> list[bool]:
    return [
        context_size_estimate >= int(_WATCH_THRESHOLDS["context_size_estimate"]),
        repetition_score >= float(_WATCH_THRESHOLDS["repetition_score"]),
        error_streak >= int(_WATCH_THRESHOLDS["error_streak"]),
        no_progress_turns >= int(_WATCH_THRESHOLDS["no_progress_turns"]),
    ]


def _is_reset_soon_severe(
    *,
    context_size_estimate: int,
    repetition_score: float,
    error_streak: int,
    no_progress_turns: int,
) -> bool:
    return (
        context_size_estimate >= int(_RESET_SOON_THRESHOLDS["context_size_estimate"])
        or repetition_score >= float(_RESET_SOON_THRESHOLDS["repetition_score"])
        or error_streak >= int(_RESET_SOON_THRESHOLDS["error_streak"])
        or no_progress_turns >= int(_RESET_SOON_THRESHOLDS["no_progress_turns"])
    )


def _normalize_compact(value: str) -> str:
    lowered = (value or "").strip().lower()
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered


def _is_progress_reply(current_norm: str, prior_norm: str) -> bool:
    if not current_norm:
        return False
    if current_norm == prior_norm:
        return False
    if len(current_norm) < 24:
        return False
    stalled_markers = (
        "please try again",
        "i cannot",
        "i can't",
        "unable to",
        "still working on it",
        "no update",
    )
    if any(marker in current_norm for marker in stalled_markers):
        return False
    return True


_FOLLOWUP_REQUIRED_MARKER = "FOLLOWUP_REQUIRED"
_FOLLOWUP_REQUIRED_MARKER_NORMALIZED = "FOLLOWUPREQUIRED"


def _extract_followup_required_marker(text: str) -> tuple[str, bool]:
    value = normalize_plain_text(text or "")
    if not value:
        return value, False

    trimmed = re.sub(r"[^\w]+$", "", value).strip()
    normalized = re.sub(r"[\s_-]+", "", trimmed).upper()
    if normalized.endswith(_FOLLOWUP_REQUIRED_MARKER_NORMALIZED):
        cleaned = re.sub(r"(?is)(?:\n|\r|\s)*[*_`<>-]*FOLLOWUP[\s_-]*REQUIRED[*_`<>-]*\s*$", "", value).strip()
        return cleaned, True
    return value, False


def _workspace_dir() -> Path:
    return Path(os.getenv("BROODMIND_WORKSPACE_DIR", "workspace")).resolve()


def _build_opportunity_card(
    *,
    kind: str,
    title: str,
    why_now: str,
    impact: str,
    effort: str,
    confidence: float,
    next_action: str,
) -> dict[str, Any]:
    return {
        "opportunity_id": str(uuid4()),
        "kind": kind,
        "title": title,
        "why_now": why_now,
        "impact": impact,
        "effort": effort,
        "confidence": round(max(0.0, min(confidence, 1.0)), 2),
        "next_action": next_action,
        "created_at": utc_now().isoformat(),
    }


def _persist_self_queue(workspace_dir: Path, chat_id: int, queue: list[dict[str, Any]]) -> str:
    memory_dir = workspace_dir / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    path = memory_dir / f"self-queue-{chat_id}.json"
    path.write_text(json.dumps(queue, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _load_self_queue(workspace_dir: Path, chat_id: int) -> list[dict[str, Any]]:
    path = (workspace_dir / "memory" / f"self-queue-{chat_id}.json").resolve()
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    items: list[dict[str, Any]] = []
    for item in payload:
        if isinstance(item, dict) and str(item.get("task_id", "")).strip():
            items.append(dict(item))
    items.sort(key=lambda i: (-int(i.get("priority", 3) or 3), str(i.get("created_at", ""))))
    return items


def _persist_last_opportunities(workspace_dir: Path, chat_id: int, opportunities: list[dict[str, Any]]) -> str:
    memory_dir = workspace_dir / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    path = memory_dir / f"opportunities-{chat_id}.json"
    path.write_text(json.dumps(opportunities, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _estimate_repetition_score(entries: list[Any]) -> float:
    if not entries:
        return 0.0
    sample = entries[:40]
    values = [_normalize_compact(getattr(entry, "content", "")) for entry in sample]
    values = [v for v in values if v]
    if not values:
        return 0.0
    unique = len(set(values))
    return max(0.0, min(1.0, 1.0 - (unique / max(1, len(values)))))


def _estimate_error_streak(entries: list[Any]) -> int:
    streak = 0
    for entry in entries[:20]:
        text = _normalize_compact(getattr(entry, "content", ""))
        if not text:
            continue
        if any(token in text for token in ("error", "failed", "exception", "unable", "timeout")):
            streak += 1
            continue
        break
    return streak


def _persist_context_reset_files(workspace_dir: Path, handoff: dict[str, Any]) -> dict[str, str]:
    memory_dir = workspace_dir / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    handoff_json_path = memory_dir / "handoff.json"
    handoff_md_path = memory_dir / "handoff.md"
    audit_md_path = memory_dir / "context-audit.md"
    audit_jsonl_path = memory_dir / "context-audit.jsonl"

    handoff_json_path.write_text(json.dumps(handoff, ensure_ascii=False, indent=2), encoding="utf-8")
    handoff_md_path.write_text(_render_handoff_markdown(handoff), encoding="utf-8")
    _append_context_audit_markdown(audit_md_path, handoff)
    with audit_jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(handoff, ensure_ascii=False) + "\n")

    return {
        "handoff_json": str(handoff_json_path),
        "handoff_md": str(handoff_md_path),
        "audit_md": str(audit_md_path),
        "audit_jsonl": str(audit_jsonl_path),
    }


def _render_handoff_markdown(handoff: dict[str, Any]) -> str:
    lines = [
        "# Queen Handoff",
        "",
        f"- created_at: {handoff.get('created_at', '')}",
        f"- mode: {handoff.get('mode', 'soft')}",
        f"- reason: {handoff.get('reason', '')}",
        f"- confidence: {handoff.get('confidence', 0.0)}",
        f"- cognitive_state: {handoff.get('cognitive_state', 'focused')}",
        "",
        "## Goal Now",
        handoff.get("goal_now", "") or "-",
        "",
        "## Next Step",
        handoff.get("next_step", "") or "-",
        "",
        "## Current Interest",
        handoff.get("current_interest", "") or "-",
        "",
        "## Pending Human Input",
        handoff.get("pending_human_input", "") or "-",
        "",
        "## Done",
    ]
    done = handoff.get("done") or []
    lines.extend([f"- {item}" for item in done] or ["-"])
    lines.extend(["", "## Open Threads"])
    open_threads = handoff.get("open_threads") or []
    lines.extend([f"- {item}" for item in open_threads] or ["-"])
    lines.extend(["", "## Critical Constraints"])
    constraints = handoff.get("critical_constraints") or []
    lines.extend([f"- {item}" for item in constraints] or ["-"])
    lines.extend(["", "## Health Snapshot"])
    health = handoff.get("health_snapshot") or {}
    for key in (
        "context_size_estimate",
        "repetition_score",
        "error_streak",
        "no_progress_turns",
        "resets_since_progress",
        "overload_score",
    ):
        lines.append(f"- {key}: {health.get(key, 0)}")
    return "\n".join(lines).strip() + "\n"


def _append_context_audit_markdown(path: Path, handoff: dict[str, Any]) -> None:
    timestamp = str(handoff.get("created_at", ""))
    mode = str(handoff.get("mode", "soft"))
    reason = str(handoff.get("reason", ""))
    confidence = str(handoff.get("confidence", ""))
    health = handoff.get("health_snapshot") or {}
    section = (
        f"\n## {timestamp} | mode={mode}\n"
        f"- reason: {reason}\n"
        f"- confidence: {confidence}\n"
        f"- context_size_estimate: {health.get('context_size_estimate', 0)}\n"
        f"- repetition_score: {health.get('repetition_score', 0)}\n"
        f"- no_progress_turns: {health.get('no_progress_turns', 0)}\n"
        f"- overload_score: {health.get('overload_score', 0)}\n"
    )
    if path.exists():
        existing = path.read_text(encoding="utf-8")
    else:
        existing = "# Context Reset Audit\n"
    path.write_text(existing.rstrip() + section + "\n", encoding="utf-8")


def _build_wakeup_message(handoff: dict[str, Any], handoff_path: str) -> str:
    goal_now = str(handoff.get("goal_now", "") or "").strip()
    next_step = str(handoff.get("next_step", "") or "").strip()
    return (
        "You woke up after a context reset.\n"
        f"Handoff goal: {goal_now}\n"
        f"Suggested next step: {next_step}\n"
        f"Handoff file: {handoff_path}\n"
        "Choose one mode now: continue / clarify / replan."
    )


@dataclass
class QueenReply:
    immediate: str
    followup: asyncio.Task[str] | None
    followup_required: bool = False
