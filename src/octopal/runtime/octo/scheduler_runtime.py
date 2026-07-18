from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from typing import Any

import structlog

from octopal.infrastructure.observability.base import now_ms
from octopal.infrastructure.observability.helpers import (
    safe_preview,
    summarize_exception,
)
from octopal.runtime.metrics import update_component_gauges
from octopal.runtime.octo.background_tracing import (
    _finish_background_trace_context,
    _start_background_trace_context,
)
from octopal.runtime.octo.control_plane import RouteMode
from octopal.runtime.octo.scheduler_helpers import _empty_scheduler_metric_counters
from octopal.runtime.state import update_last_scheduler_tick as _default_update_last_scheduler_tick

logger = structlog.get_logger(__name__)


def _core_callable(name: str, default: Callable[..., Any]) -> Callable[..., Any]:
    core_module = sys.modules.get("octopal.runtime.octo.core")
    if core_module is not None:
        candidate = getattr(core_module, name, None)
        if callable(candidate):
            return candidate
    return default


class OctoSchedulerRuntimeMixin:
    def _publish_scheduler_metrics(
        self,
        *,
        running: bool,
        interval_seconds: int | None = None,
        max_tasks: int | None = None,
        last_tick_status: str | None = None,
        due_count: int | None = None,
        result_preview: str | None = None,
        dispatch_summary: dict[str, int] | None = None,
    ) -> None:
        counters = self._scheduler_metric_counters or _empty_scheduler_metric_counters()
        payload: dict[str, Any] = {
            "running": bool(running),
            "configured": self.scheduler is not None,
            **counters,
        }
        resolved_interval = interval_seconds
        if resolved_interval is None:
            resolved_interval = self._scheduler_interval_seconds
        resolved_max_tasks = max_tasks
        if resolved_max_tasks is None:
            resolved_max_tasks = self._scheduler_max_tasks
        if resolved_interval is not None:
            payload["interval_seconds"] = int(resolved_interval)
        if resolved_max_tasks is not None:
            payload["max_tasks"] = int(resolved_max_tasks)
        if last_tick_status is not None:
            payload["last_tick_status"] = str(last_tick_status)
        if due_count is not None:
            payload["last_due_count"] = int(due_count)
        if result_preview is not None:
            payload["last_result_preview"] = str(result_preview)
        if dispatch_summary is not None:
            payload["last_dispatch_attempted"] = int(dispatch_summary.get("attempted") or 0)
            payload["last_dispatch_started"] = int(dispatch_summary.get("started") or 0)
            payload["last_dispatch_completed"] = int(dispatch_summary.get("completed") or 0)
            payload["last_dispatch_duplicates"] = int(dispatch_summary.get("duplicates") or 0)
            payload["last_dispatch_rejected_by_policy"] = int(
                dispatch_summary.get("rejected_by_policy") or 0
            )
            payload["last_dispatch_errors"] = int(dispatch_summary.get("errors") or 0)
            payload["last_policy_reasons"] = dict(dispatch_summary.get("policy_reasons") or {})
        update_component_gauges("scheduler", payload)

    async def _run_scheduler_tick_once(self, *, chat_id: int = 0, max_tasks: int = 10) -> None:
        if self.scheduler is None:
            return
        trace_started_at_ms = now_ms()
        trace_metadata: dict[str, Any] = {
            "route_mode": RouteMode.SCHEDULER.value,
            "chat_id": chat_id,
            "dry_run": False,
            "max_tasks": max_tasks,
        }
        trace_ctx, trace_token, is_root_trace = await _start_background_trace_context(
            self.trace_sink,
            name="octo.scheduler_tick",
            chat_id=chat_id,
            correlation_id=None,
            metadata=trace_metadata,
        )
        trace_status = "ok"
        trace_output: dict[str, Any] | None = None
        try:
            dispatch_summary = await self._dispatch_due_scheduled_tasks_once(
                chat_id=chat_id,
                max_tasks=max_tasks,
            )
            due_count = int(dispatch_summary.get("due_count") or 0)
            trace_metadata.update(
                {
                    "due_count": due_count,
                    "dispatch_mode": "deterministic_due_tasks",
                    "dispatch_started": int(dispatch_summary.get("started") or 0),
                    "dispatch_completed": int(dispatch_summary.get("completed") or 0),
                    "dispatch_duplicates": int(dispatch_summary.get("duplicates") or 0),
                    "dispatch_rejected_by_policy": int(
                        dispatch_summary.get("rejected_by_policy") or 0
                    ),
                    "dispatch_policy_reasons": dict(dispatch_summary.get("policy_reasons") or {}),
                    "dispatch_errors": int(dispatch_summary.get("errors") or 0),
                }
            )
            counters = self._scheduler_metric_counters or _empty_scheduler_metric_counters()
            counters["ticks_total"] = int(counters.get("ticks_total", 0) or 0) + 1
            counters["started_total"] = int(counters.get("started_total", 0) or 0) + int(
                dispatch_summary.get("started") or 0
            )
            counters["completed_total"] = int(counters.get("completed_total", 0) or 0) + int(
                dispatch_summary.get("completed") or 0
            )
            counters["duplicates_total"] = int(counters.get("duplicates_total", 0) or 0) + int(
                dispatch_summary.get("duplicates") or 0
            )
            counters["rejected_by_policy_total"] = int(
                counters.get("rejected_by_policy_total", 0) or 0
            ) + int(dispatch_summary.get("rejected_by_policy") or 0)
            counters["errors_total"] = int(counters.get("errors_total", 0) or 0) + int(
                dispatch_summary.get("errors") or 0
            )
            self._scheduler_metric_counters = counters
            status = "idle" if due_count == 0 else "dispatched"
            self._publish_scheduler_metrics(
                running=True,
                last_tick_status=status,
                due_count=due_count,
                result_preview="deterministic due-task dispatch",
                dispatch_summary=dispatch_summary,
            )
            trace_output = {
                "status": status,
                "due_count": due_count,
                "dispatch": dispatch_summary,
            }
            logger.debug(
                "Scheduler tick complete",
                due_count=due_count,
                dispatch=dispatch_summary,
            )
        except Exception as exc:
            counters = self._scheduler_metric_counters or _empty_scheduler_metric_counters()
            counters["ticks_total"] = int(counters.get("ticks_total", 0) or 0) + 1
            counters["failures_total"] = int(counters.get("failures_total", 0) or 0) + 1
            self._scheduler_metric_counters = counters
            self._publish_scheduler_metrics(
                running=True,
                last_tick_status="failed",
                result_preview=safe_preview(str(exc), limit=160),
            )
            trace_status = "error"
            trace_metadata.update(summarize_exception(exc))
            trace_output = {"status": "failed"}
            logger.exception("Scheduler tick failed")
        finally:
            runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
            if runtime_settings is not None:
                update_last_scheduler_tick = _core_callable(
                    "update_last_scheduler_tick",
                    _default_update_last_scheduler_tick,
                )
                await asyncio.to_thread(
                    update_last_scheduler_tick,
                    runtime_settings,
                    status=trace_status,
                )
            trace_metadata["duration_ms"] = round(now_ms() - trace_started_at_ms, 2)
            await _finish_background_trace_context(
                self.trace_sink,
                trace_ctx,
                trace_token,
                is_root_trace=is_root_trace,
                status=trace_status,
                output=trace_output,
                metadata=trace_metadata,
            )
