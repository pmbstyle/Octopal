from __future__ import annotations

import asyncio
import re
import sys
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import structlog

from octopal.infrastructure.logging import correlation_id_var
from octopal.infrastructure.observability.base import (
    bind_trace_context,
    get_current_trace_context,
    now_ms,
    reset_trace_context,
)
from octopal.infrastructure.observability.helpers import (
    safe_preview,
    summarize_exception,
)
from octopal.runtime.intents.types import ActionIntent
from octopal.runtime.octo import followup_pipeline as _followup_pipeline
from octopal.runtime.octo.worker_records import _serialize_worker_record
from octopal.runtime.octo.worker_timeouts import _resolve_worker_timeout_seconds
from octopal.runtime.workers.allowed_paths import infer_allowed_paths_from_values
from octopal.runtime.workers.contracts import TaskRequest, WorkerResult

logger = structlog.get_logger(__name__)

_default_enqueue_internal_result = _followup_pipeline._enqueue_internal_result
_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_URL_RE = re.compile(r"https?://[^\s<>()\"']+")
_WHITESPACE_RE = re.compile(r"\s+")


def _core_callable(name: str, default: Callable[..., Any]) -> Callable[..., Any]:
    core_module = sys.modules.get("octopal.runtime.octo.core")
    if core_module is not None:
        candidate = getattr(core_module, name, None)
        if callable(candidate):
            return candidate
    return default


def _merge_allowed_paths(*values: object) -> list[str] | None:
    merged: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, list):
            continue
        for item in value:
            path = str(item or "").strip()
            if not path or path in seen:
                continue
            seen.add(path)
            merged.append(path)
    return merged or None


def _build_worker_task_signature(
    *,
    worker_id: str,
    scheduled_task_id: str | None,
    parent_worker_id: str | None,
    task: str,
) -> str:
    schedule_sig = scheduled_task_id or "-"
    parent_sig = parent_worker_id or "-"
    normalized_task = _WHITESPACE_RE.sub(" ", str(task or "").strip().lower())
    return f"{worker_id}:{schedule_sig}:{parent_sig}:{normalized_task[:240]}"


def _build_worker_cross_scope_signature(
    *,
    worker_id: str,
    scheduled_task_id: str | None,
    parent_worker_id: str | None,
    task: str,
) -> str | None:
    schedule_sig = scheduled_task_id or "-"
    parent_sig = parent_worker_id or "-"
    normalized_task = _WHITESPACE_RE.sub(" ", str(task or "").strip().lower())
    resource_tokens = _extract_worker_resource_tokens(normalized_task)
    if not resource_tokens:
        return None
    resource_sig = "|".join(sorted(dict.fromkeys(resource_tokens)))
    return f"{worker_id}:{schedule_sig}:{parent_sig}:{resource_sig}"


def _extract_worker_resource_tokens(normalized_task: str) -> list[str]:
    resource_tokens: list[str] = [match.lower() for match in _UUID_RE.findall(normalized_task)]
    if not resource_tokens:
        resource_tokens.extend(
            match.rstrip(".,;:)]}") for match in _URL_RE.findall(normalized_task)
        )
    return resource_tokens


class OctoWorkerDispatchMixin:
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
        allowed_paths: list[str] | None = None,
        required_tool_calls: list[str] | None = None,
    ) -> dict[str, Any]:
        trace_sink = self.trace_sink
        parent_trace_ctx = get_current_trace_context()
        dispatch_trace_ctx = None
        dispatch_started_at_ms = now_ms()
        dispatch_trace_status = "ok"
        dispatch_trace_output: dict[str, Any] | None = None
        dispatch_trace_metadata: dict[str, Any] = {
            "worker_template_id": worker_id,
            "task_preview": safe_preview(task, limit=240),
            "chat_id": chat_id,
            "scheduled_task_id": scheduled_task_id,
            "parent_worker_id": parent_worker_id,
            "lineage_id": lineage_id,
            "root_task_id": root_task_id,
            "spawn_depth": spawn_depth,
            "allowed_paths_count": len(allowed_paths or []),
            "required_tool_calls_count": len(required_tool_calls or []),
        }
        if trace_sink is not None and parent_trace_ctx is not None:
            dispatch_trace_ctx = await trace_sink.start_span(
                parent_trace_ctx,
                name="worker.dispatch",
                metadata=dispatch_trace_metadata,
            )
        if parent_worker_id:
            violation = self._check_child_spawn_limits(
                lineage_id=lineage_id,
                spawn_depth=spawn_depth,
            )
            if violation:
                dispatch_trace_status = "error"
                dispatch_trace_metadata["rejected_reason"] = violation
                if dispatch_trace_ctx is not None and trace_sink is not None:
                    finish_meta = dict(dispatch_trace_metadata)
                    finish_meta["duration_ms"] = round(now_ms() - dispatch_started_at_ms, 2)
                    await trace_sink.finish_span(
                        dispatch_trace_ctx,
                        status=dispatch_trace_status,
                        output={"status": "rejected"},
                        metadata=finish_meta,
                    )
                return {
                    "status": "rejected",
                    "reason": violation,
                    "worker_id": None,
                    "run_id": None,
                }

        task_signature = _build_worker_task_signature(
            worker_id=worker_id,
            scheduled_task_id=scheduled_task_id,
            parent_worker_id=parent_worker_id,
            task=task,
        )
        cross_scope_signature = _build_worker_cross_scope_signature(
            worker_id=worker_id,
            scheduled_task_id=scheduled_task_id,
            parent_worker_id=parent_worker_id,
            task=task,
        )
        correlation_id = correlation_id_var.get()
        if not self._reserve_recent_task(
            chat_id=chat_id,
            correlation_id=correlation_id,
            task_signature=task_signature,
            cross_scope_signature=cross_scope_signature,
        ):
            logger.warning(
                "Duplicate worker task detected, skipping",
                worker_id=worker_id,
                task_prefix=task[:50],
            )
            skipped_id = f"skipped-duplicate-{uuid4().hex[:8]}"
            await self._emit_progress(
                chat_id,
                "duplicate",
                "Duplicate worker request detected; skipping duplicate launch.",
                {"worker_template_id": worker_id},
            )
            dispatch_trace_output = {
                "status": "skipped_duplicate",
                "run_id": skipped_id,
            }
            if dispatch_trace_ctx is not None and trace_sink is not None:
                finish_meta = dict(dispatch_trace_metadata)
                finish_meta["duration_ms"] = round(now_ms() - dispatch_started_at_ms, 2)
                await trace_sink.finish_span(
                    dispatch_trace_ctx,
                    status=dispatch_trace_status,
                    output=dispatch_trace_output,
                    metadata=finish_meta,
                )
            return {
                "status": "skipped_duplicate",
                "run_id": skipped_id,
                "worker_id": None,
            }
        try:
            run_id = str(uuid4())
            effective_lineage_id = lineage_id or run_id
            effective_root_task_id = root_task_id or run_id
            effective_spawn_depth = max(0, int(spawn_depth))
            template = None
            if hasattr(self.store, "get_worker_template"):
                try:
                    template = await asyncio.to_thread(self.store.get_worker_template, worker_id)
                except Exception:
                    logger.debug(
                        "Failed to load worker template for timeout resolution",
                        worker_template_id=worker_id,
                        exc_info=True,
                    )
            resolved_timeout_seconds, timeout_meta = _resolve_worker_timeout_seconds(
                explicit_timeout_seconds=timeout_seconds,
                template=template,
                task=task,
                tools=tools,
                scheduled_task_id=scheduled_task_id,
            )
            scheduler_workspace_dir = getattr(self.scheduler, "workspace_dir", None)
            template_inferred_allowed_paths = infer_allowed_paths_from_values(
                getattr(template, "system_prompt", ""),
                workspace_dir=scheduler_workspace_dir,
            )
            effective_allowed_paths = _merge_allowed_paths(
                getattr(template, "allowed_paths", None),
                template_inferred_allowed_paths,
                allowed_paths,
            )
            dispatch_trace_metadata.update(
                {
                    "run_id": run_id,
                    "lineage_id": effective_lineage_id,
                    "root_task_id": effective_root_task_id,
                    "spawn_depth": effective_spawn_depth,
                    "timeout_seconds": resolved_timeout_seconds,
                    "timeout_source": timeout_meta.get("source"),
                    "allowed_paths_count": len(effective_allowed_paths or []),
                }
            )
            if scheduled_task_id and self.scheduler:
                scheduled_task = self.scheduler.get_task(scheduled_task_id)
                if scheduled_task is not None:
                    self._scheduled_notify_user_by_run_id[run_id] = str(
                        scheduled_task.get("notify_user") or "if_significant"
                    )
            self._register_worker_lineage(
                run_id=run_id,
                lineage_id=effective_lineage_id,
                spawn_depth=effective_spawn_depth,
                parent_worker_id=parent_worker_id,
            )
            logger.info(
                "Resolved worker timeout",
                worker_template_id=worker_id,
                run_id=run_id,
                timeout_seconds=resolved_timeout_seconds,
                timeout_source=timeout_meta.get("source"),
                timeout_reasons=timeout_meta.get("reasons"),
            )
            await self._emit_progress(
                chat_id,
                "queued",
                f"Queued {worker_id} worker.",
                {
                    "worker_id": run_id,
                    "worker_template_id": worker_id,
                    "lineage_id": effective_lineage_id,
                    "parent_worker_id": parent_worker_id,
                    "spawn_depth": effective_spawn_depth,
                },
            )
            await self._emit_worker_event(
                chat_id,
                "worker_queued",
                {
                    "run_id": run_id,
                    "worker_template_id": worker_id,
                    "task": task,
                    "lineage_id": effective_lineage_id,
                    "parent_worker_id": parent_worker_id,
                    "spawn_depth": effective_spawn_depth,
                    "timeout_seconds": resolved_timeout_seconds,
                },
            )
            task_request = TaskRequest(
                worker_id=worker_id,
                task=task,
                inputs=inputs or {},
                tools=tools,
                required_tool_calls=required_tool_calls or [],
                timeout_seconds=resolved_timeout_seconds,
                run_id=run_id,
                correlation_id=correlation_id,
                parent_worker_id=parent_worker_id,
                lineage_id=effective_lineage_id,
                root_task_id=effective_root_task_id,
                spawn_depth=effective_spawn_depth,
                allowed_paths=effective_allowed_paths,
            )
            self.register_worker_correlation(run_id, correlation_id)
            self.register_worker_chat(run_id, chat_id)

            requester = self._approval_requesters.get(chat_id)
            if requester is None and getattr(self.approvals, "bot", None):

                async def _telegram_requester(intent: ActionIntent) -> bool:
                    return await self.approvals.request_approval(chat_id, intent)

                requester = _telegram_requester

            async def _runner() -> None:
                nonlocal dispatch_trace_output, dispatch_trace_status
                failed = False
                runner_trace_token = (
                    bind_trace_context(dispatch_trace_ctx)
                    if dispatch_trace_ctx is not None
                    else None
                )
                try:
                    await self._emit_progress(
                        chat_id,
                        "running",
                        f"{worker_id} worker is running.",
                        {"worker_id": run_id, "worker_template_id": worker_id},
                    )
                    await self._emit_worker_event(
                        chat_id,
                        "worker_running",
                        {
                            "run_id": run_id,
                            "worker_template_id": worker_id,
                            "task": task,
                        },
                    )
                    result = await self.runtime.run_task(task_request, approval_requester=requester)
                    worker_record = await asyncio.to_thread(self.store.get_worker, run_id)
                    worker_status = getattr(worker_record, "status", None)
                    normalized_result_status = (
                        str(getattr(result, "status", "completed") or "completed").strip().lower()
                    )
                    if worker_status is None and normalized_result_status == "failed":
                        worker_status = "failed"
                    failed = (
                        worker_status in {"failed", "stopped"}
                        or normalized_result_status == "failed"
                    )
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
                    progress_state = "completed"
                    progress_text = f"{worker_id} worker completed."
                    if failed:
                        normalized_status = str(worker_status or "failed").strip().lower()
                        progress_state = "stopped" if normalized_status == "stopped" else "failed"
                        progress_text = f"{worker_id} worker {normalized_status}."
                    else:
                        self._register_progress(chat_id, "worker_completed")
                    dispatch_trace_output = {
                        "status": progress_state,
                        "worker_status": worker_status,
                        "result_status": normalized_result_status,
                        "summary_preview": safe_preview(
                            getattr(result, "summary", None), limit=240
                        ),
                        "summary_len": len(str(getattr(result, "summary", "") or "")),
                        "questions_count": len(getattr(result, "questions", []) or []),
                        "tools_used": list(getattr(result, "tools_used", []) or []),
                    }
                    if failed:
                        dispatch_trace_status = "error"
                    await self._emit_progress(
                        chat_id,
                        progress_state,
                        progress_text,
                        {
                            "worker_id": run_id,
                            "worker_template_id": worker_id,
                            "worker_status": worker_status,
                        },
                    )
                    await self._emit_worker_event(
                        chat_id,
                        "worker_finished" if not failed else "worker_failed",
                        {
                            "run_id": run_id,
                            "worker_template_id": worker_id,
                            "worker_status": worker_status,
                            "result_summary": getattr(result, "summary", None),
                            "worker": _serialize_worker_record(worker_record),
                        },
                    )
                except Exception as exc:
                    failed = True
                    result = WorkerResult(
                        summary=f"Worker error: {exc}", output={"error": str(exc)}
                    )
                    dispatch_trace_status = "error"
                    dispatch_trace_metadata.update(summarize_exception(exc))
                    dispatch_trace_output = {
                        "status": "failed",
                        "summary_preview": safe_preview(result.summary, limit=240),
                        "summary_len": len(result.summary),
                        "questions_count": 0,
                        "tools_used": [],
                    }
                    await self._emit_progress(
                        chat_id,
                        "failed",
                        f"{worker_id} worker failed: {exc}",
                        {"worker_id": run_id, "worker_template_id": worker_id},
                    )
                    await self._emit_worker_event(
                        chat_id,
                        "worker_failed",
                        {
                            "run_id": run_id,
                            "worker_template_id": worker_id,
                            "error": str(exc),
                        },
                    )
                finally:
                    self._release_recent_task(
                        chat_id=chat_id,
                        correlation_id=correlation_id,
                        task_signature=task_signature,
                        cross_scope_signature=cross_scope_signature,
                    )
                    if dispatch_trace_ctx is not None and trace_sink is not None:
                        finish_meta = dict(dispatch_trace_metadata)
                        finish_meta["duration_ms"] = round(now_ms() - dispatch_started_at_ms, 2)
                        await trace_sink.finish_span(
                            dispatch_trace_ctx,
                            status=dispatch_trace_status,
                            output=dispatch_trace_output,
                            metadata=finish_meta,
                        )
                    if runner_trace_token is not None:
                        reset_trace_context(runner_trace_token)
                if failed:
                    await self._cleanup_orphan_children(
                        parent_run_id=run_id,
                        chat_id=chat_id,
                        reason="parent_failed",
                    )
                scheduled_notify_user = self._scheduled_notify_user_by_run_id.pop(run_id, None)
                self._mark_worker_inactive(run_id)
                enqueue_internal_result = _core_callable(
                    "_enqueue_internal_result", _default_enqueue_internal_result
                )
                enqueue_internal_result(
                    self,
                    chat_id,
                    run_id,
                    task,
                    result,
                    correlation_id=task_request.correlation_id,
                    notify_user=scheduled_notify_user,
                )

            asyncio.create_task(_runner())
            await self._emit_progress(
                chat_id,
                "worker_started",
                f"{worker_id} worker started.",
                {
                    "worker_id": run_id,
                    "worker_template_id": worker_id,
                    "lineage_id": effective_lineage_id,
                    "parent_worker_id": parent_worker_id,
                    "spawn_depth": effective_spawn_depth,
                },
            )
            await self._emit_worker_event(
                chat_id,
                "worker_started",
                {
                    "run_id": run_id,
                    "worker_template_id": worker_id,
                    "task": task,
                    "lineage_id": effective_lineage_id,
                    "parent_worker_id": parent_worker_id,
                    "spawn_depth": effective_spawn_depth,
                    "timeout_seconds": resolved_timeout_seconds,
                },
            )
            dispatch_trace_output = {
                "status": "started",
                "run_id": run_id,
                "lineage_id": effective_lineage_id,
                "timeout_seconds": resolved_timeout_seconds,
            }
            return {
                "status": "started",
                "run_id": run_id,
                "worker_id": run_id,
                "lineage_id": effective_lineage_id,
                "parent_worker_id": parent_worker_id,
                "root_task_id": effective_root_task_id,
                "spawn_depth": effective_spawn_depth,
            }
        except Exception:
            self._release_recent_task(
                chat_id=chat_id,
                correlation_id=correlation_id,
                task_signature=task_signature,
                cross_scope_signature=cross_scope_signature,
            )
            dispatch_trace_status = "error"
            dispatch_trace_output = dispatch_trace_output or {"status": "failed"}
            if dispatch_trace_ctx is not None and trace_sink is not None:
                finish_meta = dict(dispatch_trace_metadata)
                finish_meta["duration_ms"] = round(now_ms() - dispatch_started_at_ms, 2)
                await trace_sink.finish_span(
                    dispatch_trace_ctx,
                    status=dispatch_trace_status,
                    output=dispatch_trace_output,
                    metadata=finish_meta,
                )
            raise

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
            self._lineage_children_total[lineage_id] = (
                int(self._lineage_children_total.get(lineage_id, 0)) + 1
            )
            active = self._lineage_children_active.setdefault(lineage_id, set())
            active.add(run_id)

    def _mark_worker_inactive(self, run_id: str) -> None:
        correlation_id = self._worker_correlation_by_run_id.pop(run_id, None)
        self._worker_chat_by_run_id.pop(run_id, None)
        self._scheduled_notify_user_by_run_id.pop(run_id, None)
        if correlation_id:
            active_by_correlation = self._active_workers_by_correlation.get(correlation_id)
            if active_by_correlation and run_id in active_by_correlation:
                active_by_correlation.discard(run_id)
                if not active_by_correlation:
                    self._active_workers_by_correlation.pop(correlation_id, None)
        lineage_id = self._worker_lineage.get(run_id)
        if not lineage_id:
            return
        active = self._lineage_children_active.get(lineage_id)
        if active and run_id in active:
            active.discard(run_id)
            if not active:
                self._lineage_children_active.pop(lineage_id, None)

    async def _cleanup_orphan_children(
        self, *, parent_run_id: str, chat_id: int, reason: str
    ) -> None:
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
        payload = meta or {}
        sender = self.internal_progress_send
        if sender:
            try:
                await sender(chat_id, state, text, payload)
            except Exception:
                logger.debug("Progress emit failed", exc_info=True)
        await self.emit_ws_progress(chat_id, state, text, payload)

    async def _emit_worker_event(
        self,
        chat_id: int,
        event: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        event_payload = payload or {}
        sender = self.internal_worker_event_send
        if sender:
            try:
                await sender(chat_id, event, event_payload)
            except Exception:
                logger.debug("Worker event emit failed", exc_info=True)
        await self.emit_ws_worker_event(chat_id, event, event_payload)
