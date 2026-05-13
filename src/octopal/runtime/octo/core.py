from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import structlog

from octopal.browser.manager import get_browser_manager as get_browser_manager
from octopal.channels.telegram.approvals import ApprovalManager
from octopal.infrastructure.connectors.manager import ConnectorManager
from octopal.infrastructure.logging import correlation_id_var
from octopal.infrastructure.mcp.manager import MCPManager
from octopal.infrastructure.observability.base import (
    TraceSink,
    bind_trace_context,
    now_ms,
    reset_trace_context,
)
from octopal.infrastructure.observability.helpers import (
    hash_payload,
    safe_preview,
    summarize_exception,
)
from octopal.infrastructure.observability.noop import NoopTraceSink
from octopal.infrastructure.providers.base import InferenceProvider
from octopal.infrastructure.store.base import Store
from octopal.runtime.memory.canon import CanonService
from octopal.runtime.memory.facts import FactsService
from octopal.runtime.memory.reflection import ReflectionService
from octopal.runtime.memory.service import MemoryService
from octopal.runtime.octo import (
    followup_delivery as _followup_delivery,
)
from octopal.runtime.octo import (
    followup_pipeline as _followup_pipeline,
)
from octopal.runtime.octo import (
    followup_text as _followup_text,
)
from octopal.runtime.octo import (
    followups as _followups,
)
from octopal.runtime.octo import (
    router as _octo_router,
)
from octopal.runtime.octo.background_runtime import OctoBackgroundRuntimeMixin
from octopal.runtime.octo.context_health import (
    _is_progress_reply,
)
from octopal.runtime.octo.context_reset import _normalize_compact as _normalize_compact
from octopal.runtime.octo.context_reset import (
    build_restart_resume_message as _build_restart_resume_message,
)
from octopal.runtime.octo.context_runtime import OctoContextRuntimeMixin
from octopal.runtime.octo.control_plane import (
    RouteMode,
    RouteRequest,
    resolve_turn_route_mode,
)
from octopal.runtime.octo.control_replies import (
    _SCHEDULED_OCTO_CONTROL_BLOCKED,
    _SCHEDULED_OCTO_CONTROL_DONE,
    _coerce_control_plane_reply,
    _looks_like_scheduled_octo_control_route_block,
    _normalize_heartbeat_delivery_reply,
    _normalize_scheduled_octo_control_notify_policy,
    _normalize_scheduled_octo_control_reply,
)
from octopal.runtime.octo.delivery import (
    DeliveryMode,
    resolve_user_delivery,
)
from octopal.runtime.octo.prompt_builder import (
    build_bootstrap_context_prompt,
)
from octopal.runtime.octo.reply import OctoReply
from octopal.runtime.octo.router import (
    route_heartbeat,
    route_internal_maintenance,
    route_or_reply,
    route_scheduled_octo_control,
)
from octopal.runtime.octo.router import route_proactive_tick as route_proactive_tick
from octopal.runtime.octo.router import route_scheduler_tick as route_scheduler_tick
from octopal.runtime.octo.runtime_config import _env_flag, _env_int
from octopal.runtime.octo.runtime_config import _env_float as _env_float
from octopal.runtime.octo.scheduler_helpers import (
    _coerce_positive_chat_id,
    _empty_scheduler_metric_counters,
)
from octopal.runtime.octo.scheduler_runtime import OctoSchedulerRuntimeMixin
from octopal.runtime.octo.self_lifecycle import OctoSelfLifecycleMixin
from octopal.runtime.octo.self_queue import OctoSelfQueueMixin
from octopal.runtime.octo.turn_state import OctoTurnStateMixin
from octopal.runtime.octo.worker_dispatch import OctoWorkerDispatchMixin
from octopal.runtime.octo.worker_registry import OctoWorkerRegistryMixin
from octopal.runtime.octo.workspace_paths import _workspace_dir as _workspace_dir
from octopal.runtime.policy.engine import PolicyEngine
from octopal.runtime.scheduler.service import (
    SCHEDULED_TASK_BLOCKED_REASON_KEY,
    SCHEDULED_TASK_BLOCKED_UNTIL_KEY,
    SCHEDULED_TASK_DELIVERY_CHAT_ID_KEY,
    SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY,
    SCHEDULED_TASK_TARGET_CHAT_ID_KEY,
    SchedulerService,
    normalize_notify_user_policy,
    parse_scheduled_task_blocked_until,
)
from octopal.runtime.self_control import (
    check_update_status as check_update_status,
)
from octopal.runtime.self_control import (
    mark_restart_resume_consumed,
    read_pending_restart_resume,
)
from octopal.runtime.state import update_last_internal_heartbeat
from octopal.runtime.workers.contracts import (
    WorkerInstructionRequest,
    WorkerResult,
    WorkerSpec,
)
from octopal.runtime.workers.runtime import WorkerRuntime
from octopal.utils import (
    extract_reaction_and_strip,
    sanitize_user_facing_text_preserving_reaction,
    should_suppress_user_delivery,
    utc_now,
)

logger = structlog.get_logger(__name__)
_FOLLOWUP_QUEUES = _followup_pipeline._FOLLOWUP_QUEUES
_FOLLOWUP_TASKS = _followup_pipeline._FOLLOWUP_TASKS
_INTERNAL_QUEUES = _followup_pipeline._INTERNAL_QUEUES
_INTERNAL_TASKS = _followup_pipeline._INTERNAL_TASKS
_WORKER_FOLLOWUP_BATCHES = _followup_pipeline._WORKER_FOLLOWUP_BATCHES
_QUEUE_IDLE_TIMEOUT_SECONDS = _followup_pipeline._QUEUE_IDLE_TIMEOUT_SECONDS
_WORKER_RESULT_ROUTING_TIMEOUT_SECONDS = (
    _followup_pipeline._WORKER_RESULT_ROUTING_TIMEOUT_SECONDS
)
_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS = (
    _followup_pipeline._WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS
)
_publish_runtime_metrics = _followup_pipeline._publish_runtime_metrics
_followup_worker = _followup_pipeline._followup_worker
_enqueue_followup = _followup_pipeline._enqueue_followup
_flush_worker_followup_batch = _followup_pipeline._flush_worker_followup_batch
_schedule_worker_followup_flush = _followup_pipeline._schedule_worker_followup_flush
_discard_worker_followup_batch = _followup_pipeline._discard_worker_followup_batch
_enqueue_batched_worker_followup = _followup_pipeline._enqueue_batched_worker_followup
_internal_worker = _followup_pipeline._internal_worker
_route_instruction_request_to_octo = _followup_pipeline._route_instruction_request_to_octo
_enqueue_internal_result = _followup_pipeline._enqueue_internal_result
_send_scheduler_control_update = _followup_delivery._send_scheduler_control_update
_send_worker_followup = _followup_delivery._send_worker_followup
_merge_worker_followup_texts = _followup_text._merge_worker_followup_texts
route_worker_results_back_to_octo = _octo_router.route_worker_results_back_to_octo
_PendingWorkerFollowupBatch = _followups._PendingWorkerFollowupBatch
_PendingWorkerFollowupItem = _followups._PendingWorkerFollowupItem
_build_forced_worker_followup_batch = _followups._build_forced_worker_followup_batch
_build_worker_result_timeout_followup = _followups._build_worker_result_timeout_followup


_PENDING_CONVERSATIONAL_CLOSURE_TTL_SECONDS = _env_int(
    "OCTOPAL_PENDING_CONVERSATIONAL_CLOSURE_TTL_SECONDS",
    3600,
    minimum=60,
)
_HEARTBEAT_USER_VISIBLE_COOLDOWN_SECONDS = _env_int(
    "OCTOPAL_HEARTBEAT_USER_VISIBLE_COOLDOWN_SECONDS",
    300,
    minimum=0,
)
_RECENT_WORKER_TASK_TTL_SECONDS = float(
    _env_int(
        "OCTOPAL_RECENT_WORKER_TASK_TTL_SECONDS",
        1800,
        minimum=60,
    )
)
_SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS = float(
    _env_int(
        "OCTOPAL_SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS",
        1800,
        minimum=0,
    )
)
_PROACTIVE_TICK_ENABLED = _env_flag("OCTOPAL_PROACTIVE_TICK_ENABLED", True)
_PROACTIVE_TICK_MIN_INTERVAL_SECONDS = float(
    _env_int("OCTOPAL_PROACTIVE_TICK_MIN_INTERVAL_SECONDS", 21600, minimum=0)
)


@dataclass
class Octo(
    OctoTurnStateMixin,
    OctoSelfQueueMixin,
    OctoSelfLifecycleMixin,
    OctoWorkerDispatchMixin,
    OctoWorkerRegistryMixin,
    OctoBackgroundRuntimeMixin,
    OctoSchedulerRuntimeMixin,
    OctoContextRuntimeMixin,
):
    provider: InferenceProvider
    store: Store
    policy: PolicyEngine
    runtime: WorkerRuntime
    approvals: ApprovalManager
    memory: MemoryService
    canon: CanonService
    facts: FactsService | None = None
    reflection: ReflectionService | None = None
    scheduler: SchedulerService | None = None
    mcp_manager: MCPManager | None = None
    connector_manager: ConnectorManager | None = None
    trace_sink: TraceSink | None = None
    internal_send: callable | None = None
    internal_send_file: callable | None = None
    internal_progress_send: callable | None = None
    internal_worker_event_send: callable | None = None
    internal_typing_control: callable | None = None
    _cleanup_task: asyncio.Task | None = None
    _metrics_task: asyncio.Task | None = None
    _scheduler_task: asyncio.Task | None = None
    _self_control_task: asyncio.Task | None = None
    _scheduler_metric_counters: dict[str, int] | None = None
    _scheduler_interval_seconds: int | None = None
    _scheduler_max_tasks: int | None = None
    _scheduled_octo_control_backoff_by_task: dict[str, tuple[float, str]] | None = None
    _recent_tasks: dict[tuple[int, str, str], float] = (
        None  # Track in-flight worker launches per chat/correlation scope
    )
    _approval_requesters: dict[int, Callable[[Any], Awaitable[bool]]] | None = None
    _thinking_count: int = 0
    _ws_active: bool = False
    _ws_owner: str | None = None
    _tg_send: callable | None = None
    _tg_send_file: callable | None = None
    _tg_progress: callable | None = None
    _tg_worker_event: callable | None = None
    _tg_typing: callable | None = None
    _scheduled_delivery_chat_ids: list[int] | None = None
    _spawn_limits: dict[str, int] | None = None
    _worker_children: dict[str, set[str]] | None = None
    _worker_lineage: dict[str, str] | None = None
    _worker_depth: dict[str, int] | None = None
    _lineage_children_total: dict[str, int] | None = None
    _lineage_children_active: dict[str, set[str]] | None = None
    _worker_correlation_by_run_id: dict[str, str] | None = None
    _worker_chat_by_run_id: dict[str, int] | None = None
    _scheduled_notify_user_by_run_id: dict[str, str] | None = None
    _active_workers_by_correlation: dict[str, set[str]] | None = None
    _pending_internal_results_by_correlation: dict[str, int] | None = None
    _housekeeping_cfg: dict[str, int] | None = None
    _pending_wakeup_by_chat: dict[int, str] | None = None
    _context_health_by_chat: dict[int, dict[str, Any]] | None = None
    _last_reply_norm_by_chat: dict[int, str] | None = None
    _last_user_visible_delivery_at_by_chat: dict[int, Any] | None = None
    _pending_conversational_closure_by_correlation: dict[str, Any] | None = None
    _structured_followup_required_by_correlation: dict[str, Any] | None = None
    _suppressed_followups_by_correlation: dict[str, Any] | None = None
    _no_progress_turns_by_chat: dict[int, int] | None = None
    _progress_revision_by_chat: dict[int, int] | None = None
    _reset_streak_without_progress_by_chat: dict[int, int] | None = None
    _last_reset_progress_revision_by_chat: dict[int, int] | None = None
    _watch_escalation_streak_by_chat: dict[int, int] | None = None
    _self_queue_by_chat: dict[int, list[dict[str, Any]]] | None = None
    _last_opportunities_by_chat: dict[int, list[dict[str, Any]]] | None = None
    _last_proactive_tick_at_by_chat: dict[int, datetime] | None = None
    _active_user_turns_by_correlation: dict[str, Any] | None = None

    def __post_init__(self):
        if self.trace_sink is None:
            self.trace_sink = NoopTraceSink()
        if self._scheduled_octo_control_backoff_by_task is None:
            self._scheduled_octo_control_backoff_by_task = {}
        if self._recent_tasks is None:
            self._recent_tasks = {}
        if self._approval_requesters is None:
            self._approval_requesters = {}
        if self._scheduled_delivery_chat_ids is None:
            self._scheduled_delivery_chat_ids = []
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
        if self._worker_correlation_by_run_id is None:
            self._worker_correlation_by_run_id = {}
        if self._worker_chat_by_run_id is None:
            self._worker_chat_by_run_id = {}
        if self._scheduled_notify_user_by_run_id is None:
            self._scheduled_notify_user_by_run_id = {}
        if self._active_workers_by_correlation is None:
            self._active_workers_by_correlation = {}
        if self._pending_internal_results_by_correlation is None:
            self._pending_internal_results_by_correlation = {}
        if self._pending_wakeup_by_chat is None:
            self._pending_wakeup_by_chat = {}
        if self._context_health_by_chat is None:
            self._context_health_by_chat = {}
        if self._last_reply_norm_by_chat is None:
            self._last_reply_norm_by_chat = {}
        if self._last_user_visible_delivery_at_by_chat is None:
            self._last_user_visible_delivery_at_by_chat = {}
        if self._pending_conversational_closure_by_correlation is None:
            self._pending_conversational_closure_by_correlation = {}
        if self._structured_followup_required_by_correlation is None:
            self._structured_followup_required_by_correlation = {}
        if self._suppressed_followups_by_correlation is None:
            self._suppressed_followups_by_correlation = {}
        if self._active_user_turns_by_correlation is None:
            self._active_user_turns_by_correlation = {}
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
        if self._last_proactive_tick_at_by_chat is None:
            self._last_proactive_tick_at_by_chat = {}
        if self._spawn_limits is None:
            max_depth = _env_int("OCTOPAL_WORKER_MAX_SPAWN_DEPTH", 2, minimum=0)
            max_total = _env_int("OCTOPAL_WORKER_MAX_CHILDREN_TOTAL", 20, minimum=1)
            max_concurrent = _env_int("OCTOPAL_WORKER_MAX_CHILDREN_CONCURRENT", 10, minimum=1)
            self._spawn_limits = {
                "max_depth": max_depth,
                "max_children_total": max_total,
                "max_children_concurrent": max_concurrent,
            }
        if self._housekeeping_cfg is None:
            self._housekeeping_cfg = {
                "tmp_retention_hours": _env_int(
                    "OCTOPAL_WORKSPACE_TMP_RETENTION_HOURS", 48, minimum=1
                ),
                "worker_dir_retention_minutes": _env_int(
                    "OCTOPAL_WORKER_DIR_RETENTION_MINUTES", 15, minimum=1
                ),
                "canon_events_max_bytes": _env_int(
                    "OCTOPAL_CANON_EVENTS_MAX_BYTES", 2_000_000, minimum=1024
                ),
                "canon_events_keep_archives": _env_int(
                    "OCTOPAL_CANON_EVENTS_KEEP_ARCHIVES", 7, minimum=1
                ),
            }
        if self._scheduler_metric_counters is None:
            self._scheduler_metric_counters = _empty_scheduler_metric_counters()
        if self._scheduler_interval_seconds is None:
            self._scheduler_interval_seconds = None
        if self._scheduler_max_tasks is None:
            self._scheduler_max_tasks = None
        self._restore_worker_registry_state()
        self._thinking_count = 0
        self._tg_send = self.internal_send
        self._tg_send_file = self.internal_send_file
        self._tg_progress = self.internal_progress_send
        self._tg_worker_event = self.internal_worker_event_send
        self._tg_typing = self.internal_typing_control

    def _reserve_recent_task(
        self,
        *,
        chat_id: int,
        correlation_id: str | None,
        task_signature: str,
    ) -> bool:
        self._prune_recent_tasks()
        scope_id = str(correlation_id or f"chat:{chat_id}")
        key = (chat_id, scope_id, task_signature)
        if key in self._recent_tasks:
            return False
        self._recent_tasks[key] = time.monotonic()
        return True

    def _release_recent_task(
        self,
        *,
        chat_id: int,
        correlation_id: str | None,
        task_signature: str,
    ) -> None:
        scope_id = str(correlation_id or f"chat:{chat_id}")
        self._recent_tasks.pop((chat_id, scope_id, task_signature), None)

    def _prune_recent_tasks(self) -> None:
        now = time.monotonic()
        cutoff = now - _RECENT_WORKER_TASK_TTL_SECONDS
        stale_keys = [key for key, seen_at in self._recent_tasks.items() if seen_at < cutoff]
        for key in stale_keys:
            self._recent_tasks.pop(key, None)

    @property
    def is_ws_active(self) -> bool:
        return self._ws_active

    def set_output_channel(
        self,
        is_ws: bool,
        send: callable | None = None,
        send_file: callable | None = None,
        progress: callable | None = None,
        worker_event: callable | None = None,
        typing: callable | None = None,
        owner_id: str | None = None,
        force: bool = False,
    ) -> bool:
        """Switch between Telegram and WebSocket output channels."""
        if is_ws:
            if self._ws_active and self._ws_owner and owner_id and self._ws_owner != owner_id:
                if force:
                    logger.warning(
                        "Forcing WebSocket channel takeover",
                        current_owner=self._ws_owner,
                        new_owner=owner_id,
                    )
                else:
                    logger.warning(
                        "Rejected WebSocket channel switch due to existing owner",
                        current_owner=self._ws_owner,
                        attempted_owner=owner_id,
                    )
                    return False
        else:
            if self._ws_owner and owner_id and self._ws_owner != owner_id:
                if force:
                    logger.warning(
                        "Forcing output channel reset from non-owner",
                        current_owner=self._ws_owner,
                        attempted_owner=owner_id,
                    )
                else:
                    logger.warning(
                        "Rejected output channel reset from non-owner",
                        current_owner=self._ws_owner,
                        attempted_owner=owner_id,
                    )
                    return False

        self._ws_active = is_ws
        if is_ws:
            self.internal_send = send
            self.internal_send_file = send_file
            self.internal_progress_send = progress
            self.internal_worker_event_send = worker_event
            self.internal_typing_control = typing
            self._ws_owner = owner_id or "ws-default"
            logger.info("Octo switched to WebSocket output channel")
        else:
            self.internal_send = self._tg_send
            self.internal_send_file = self._tg_send_file
            self.internal_progress_send = self._tg_progress
            self.internal_worker_event_send = self._tg_worker_event
            self.internal_typing_control = self._tg_typing
            self._ws_owner = None
            logger.info("Octo switched to Telegram output channel")

        # Update system status file if possible
        try:
            import json

            from octopal.infrastructure.config.settings import load_settings
            from octopal.runtime.state import _status_path, read_status

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
                logger.debug(
                    "Failed to set typing status", chat_id=chat_id, active=active, exc_info=True
                )
    def _get_scheduled_octo_control_backoff(self, task_id: str) -> tuple[float, str] | None:
        task_id_value = str(task_id or "").strip()
        if not task_id_value:
            return None
        backoff_map = self._scheduled_octo_control_backoff_by_task
        if not isinstance(backoff_map, dict):
            return None
        entry = backoff_map.get(task_id_value)
        if not entry:
            return None
        deadline, reason = entry
        remaining = float(deadline) - time.monotonic()
        if remaining <= 0:
            backoff_map.pop(task_id_value, None)
            return None
        return remaining, str(reason or "").strip() or "runtime_backoff"

    def _set_scheduled_octo_control_backoff(self, task_id: str, *, reason: str) -> None:
        task_id_value = str(task_id or "").strip()
        if not task_id_value or _SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS <= 0:
            return
        backoff_map = self._scheduled_octo_control_backoff_by_task
        if backoff_map is None:
            backoff_map = {}
            self._scheduled_octo_control_backoff_by_task = backoff_map
        backoff_map[task_id_value] = (
            time.monotonic() + _SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS,
            str(reason or "").strip() or "runtime_backoff",
        )

    def _get_persisted_scheduled_octo_control_backoff(
        self,
        task: dict[str, Any],
    ) -> tuple[float, str] | None:
        metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
        if not isinstance(metadata, dict):
            return None
        blocked_until = parse_scheduled_task_blocked_until(metadata)
        if blocked_until is None:
            return None
        remaining = (blocked_until - utc_now()).total_seconds()
        if remaining <= 0:
            self._update_scheduled_octo_control_backoff_metadata(
                task,
                blocked_until=None,
                reason=None,
            )
            return None
        reason = (
            str(metadata.get(SCHEDULED_TASK_BLOCKED_REASON_KEY) or "").strip() or "blocked_by_route"
        )
        return remaining, reason

    def _update_scheduled_octo_control_backoff_metadata(
        self,
        task: dict[str, Any],
        *,
        blocked_until: datetime | None,
        reason: str | None,
    ) -> None:
        scheduler = self.scheduler
        store = getattr(scheduler, "store", None)
        update_metadata = getattr(store, "update_scheduled_task_metadata", None)
        if scheduler is None or not callable(update_metadata):
            return
        task_id = str(task.get("id") or "").strip()
        if not task_id:
            return
        metadata = (
            dict(task.get("metadata") or {}) if isinstance(task.get("metadata"), dict) else {}
        )
        if blocked_until is None:
            metadata.pop(SCHEDULED_TASK_BLOCKED_UNTIL_KEY, None)
        else:
            metadata[SCHEDULED_TASK_BLOCKED_UNTIL_KEY] = blocked_until.isoformat()
        reason_value = str(reason or "").strip()
        if reason_value:
            metadata[SCHEDULED_TASK_BLOCKED_REASON_KEY] = reason_value
        else:
            metadata.pop(SCHEDULED_TASK_BLOCKED_REASON_KEY, None)
        if reason_value == "blocked_by_route":
            metadata[SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY] = "worker"
        elif blocked_until is None:
            metadata.pop(SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY, None)
        try:
            update_metadata(task_id, metadata or None)
        except Exception:
            logger.exception(
                "Failed to persist scheduled Octo control backoff metadata",
                task_id=task_id or None,
            )
            return
        task["metadata"] = metadata
        task["blocked_until"] = metadata.get(SCHEDULED_TASK_BLOCKED_UNTIL_KEY)
        task["blocked_reason"] = metadata.get(SCHEDULED_TASK_BLOCKED_REASON_KEY)
        task["suggested_execution_mode"] = metadata.get(SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY)

    def _resolve_scheduled_task_delivery_chat_id(
        self,
        task: dict[str, Any],
        *,
        requested_chat_id: int = 0,
    ) -> tuple[int | None, str]:
        notify_user = normalize_notify_user_policy(task.get("notify_user"))
        if notify_user == "never":
            return 0, "notify_never"

        metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
        explicit = _coerce_positive_chat_id(
            task.get("delivery_chat_id")
            or metadata.get(SCHEDULED_TASK_DELIVERY_CHAT_ID_KEY)
            or metadata.get(SCHEDULED_TASK_TARGET_CHAT_ID_KEY)
        )
        if explicit is not None:
            return explicit, "task_metadata"

        requested = _coerce_positive_chat_id(requested_chat_id)
        if requested is not None:
            return requested, "request_context"

        configured = [
            chat_id
            for item in (self._scheduled_delivery_chat_ids or [])
            if (chat_id := _coerce_positive_chat_id(item)) is not None
        ]
        unique_configured = list(dict.fromkeys(configured))
        if len(unique_configured) == 1:
            return unique_configured[0], "single_configured_recipient"

        return None, "missing_delivery_target"

    def _resolve_scheduler_delivery_chat_id(
        self,
        *,
        requested_chat_id: int = 0,
    ) -> tuple[int | None, str]:
        requested = _coerce_positive_chat_id(requested_chat_id)
        if requested is not None:
            return requested, "request_context"

        configured = [
            chat_id
            for item in (self._scheduled_delivery_chat_ids or [])
            if (chat_id := _coerce_positive_chat_id(item)) is not None
        ]
        unique_configured = list(dict.fromkeys(configured))
        if len(unique_configured) == 1:
            return unique_configured[0], "single_configured_recipient"

        return None, "missing_delivery_target"

    async def _dispatch_due_scheduled_tasks_once(
        self,
        *,
        chat_id: int = 0,
        max_tasks: int = 10,
    ) -> dict[str, Any]:
        scheduler = self.scheduler
        summary: dict[str, Any] = {
            "due_count": 0,
            "attempted": 0,
            "started": 0,
            "completed": 0,
            "duplicates": 0,
            "rejected_by_policy": 0,
            "policy_reasons": {},
            "errors": 0,
        }
        if scheduler is None:
            return summary

        due_tasks = list(scheduler.get_actionable_tasks() or [])
        summary["due_count"] = len(due_tasks)
        for task in due_tasks[: max(1, int(max_tasks))]:
            task_id = str(task.get("id") or "").strip()
            execution_mode = str(task.get("execution_mode") or "").strip().lower()
            worker_id = str(task.get("worker_id") or "").strip()
            task_text = str(task.get("task_text") or "").strip()
            inputs = task.get("inputs") if isinstance(task.get("inputs"), dict) else {}
            dispatch_chat_id, delivery_target_source = self._resolve_scheduled_task_delivery_chat_id(
                task,
                requested_chat_id=chat_id,
            )
            if dispatch_chat_id is None:
                summary["rejected_by_policy"] += 1
                reason = "missing_delivery_target"
                policy_reasons = summary.setdefault("policy_reasons", {})
                if isinstance(policy_reasons, dict):
                    policy_reasons[reason] = int(policy_reasons.get(reason, 0) or 0) + 1
                logger.warning(
                    "Rejected scheduled task by delivery policy",
                    task_id=task_id or None,
                    notify_user=task.get("notify_user"),
                    delivery_target_source=delivery_target_source,
                )
                continue
            if execution_mode == "octo_control":
                persisted_backoff = self._get_persisted_scheduled_octo_control_backoff(task)
                if persisted_backoff is not None:
                    remaining_seconds, backoff_reason = persisted_backoff
                    logger.info(
                        "Skipping scheduled Octo control task during persisted backoff",
                        task_id=task_id or None,
                        chat_id=dispatch_chat_id,
                        backoff_reason=backoff_reason,
                        cooldown_seconds=round(remaining_seconds, 1),
                    )
                    continue
                backoff = self._get_scheduled_octo_control_backoff(task_id)
                if backoff is not None:
                    remaining_seconds, backoff_reason = backoff
                    logger.info(
                        "Skipping scheduled Octo control task during runtime backoff",
                        task_id=task_id or None,
                        chat_id=dispatch_chat_id,
                        backoff_reason=backoff_reason,
                        cooldown_seconds=round(remaining_seconds, 1),
                    )
                    continue
                summary["attempted"] += 1
                try:
                    result = await self._run_scheduled_octo_control_task_once(
                        task=task,
                        chat_id=dispatch_chat_id,
                    )
                except Exception:
                    summary["errors"] += 1
                    logger.exception(
                        "Scheduled Octo control task failed",
                        task_id=task_id or None,
                    )
                    continue
                if bool(result.get("completed")):
                    summary["completed"] += 1
                elif str(result.get("status") or "").strip().lower() == "failed":
                    summary["errors"] += 1
                continue
            if not worker_id or not task_text:
                summary["rejected_by_policy"] += 1
                reason = "missing_worker_id" if not worker_id else "missing_task_text"
                policy_reasons = summary.setdefault("policy_reasons", {})
                if isinstance(policy_reasons, dict):
                    policy_reasons[reason] = int(policy_reasons.get(reason, 0) or 0) + 1
                logger.warning(
                    "Rejected scheduled task by dispatch policy",
                    task_id=task_id or None,
                    policy_reason=reason,
                    worker_id=worker_id or None,
                    has_task_text=bool(task_text),
                )
                continue

            summary["attempted"] += 1
            try:
                result = await self._start_worker_async(
                    worker_id=worker_id,
                    task=task_text,
                    chat_id=dispatch_chat_id,
                    inputs=inputs,
                    tools=None,
                    model=None,
                    timeout_seconds=None,
                    scheduled_task_id=task_id or None,
                )
            except Exception:
                summary["errors"] += 1
                logger.exception(
                    "Scheduled task dispatch failed",
                    task_id=task_id or None,
                    worker_id=worker_id,
                )
                continue

            status = str(result.get("status") or "").strip().lower()
            if status == "started":
                summary["started"] += 1
            elif status == "skipped_duplicate":
                summary["duplicates"] += 1
            elif status in {"rejected", "failed"}:
                summary["errors"] += 1

        return summary

    async def _run_scheduled_octo_control_task_once(
        self,
        *,
        task: dict[str, Any],
        chat_id: int = 0,
    ) -> dict[str, Any]:
        scheduler = self.scheduler
        task_id = str(task.get("id") or "").strip()
        notify_user = _normalize_scheduled_octo_control_notify_policy(task.get("notify_user"))
        reply_text = await route_scheduled_octo_control(
            self,
            task,
            chat_id=chat_id,
        )
        normalized_reply = await _normalize_scheduled_octo_control_reply(self.provider, reply_text)
        route_blocked = normalized_reply == _SCHEDULED_OCTO_CONTROL_BLOCKED or (
            normalized_reply == "NO_USER_RESPONSE"
            and _looks_like_scheduled_octo_control_route_block(reply_text)
        )
        if route_blocked:
            self._set_scheduled_octo_control_backoff(task_id, reason="blocked_by_route")
            self._update_scheduled_octo_control_backoff_metadata(
                task,
                blocked_until=utc_now()
                + timedelta(seconds=_SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS),
                reason="blocked_by_route",
            )
            logger.warning(
                "Scheduled Octo control task blocked by bounded route",
                task_id=task_id or None,
                chat_id=chat_id,
                raw_reply_preview=safe_preview(reply_text, limit=200),
                cooldown_seconds=_SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS,
            )
            return {
                "status": "failed",
                "completed": False,
                "reason": "blocked_by_route",
                "cooldown_seconds": _SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS,
            }
        if normalized_reply == "NO_USER_RESPONSE":
            logger.warning(
                "Scheduled Octo control task missing explicit completion signal",
                task_id=task_id or None,
                chat_id=chat_id,
                raw_reply_preview=safe_preview(reply_text, limit=200),
            )
            return {
                "status": "failed",
                "completed": False,
                "reason": "missing_completion_signal",
            }
        if normalized_reply == _SCHEDULED_OCTO_CONTROL_DONE:
            self._update_scheduled_octo_control_backoff_metadata(
                task,
                blocked_until=None,
                reason=None,
            )
            if scheduler is not None and task_id:
                scheduler.mark_executed(task_id)
            logger.info(
                "Scheduled Octo control task completed silently",
                task_id=task_id or None,
                notify_user=notify_user,
                chat_id=chat_id,
            )
            return {
                "status": "completed",
                "completed": True,
                "user_visible_sent": False,
                "delivery_mode": DeliveryMode.SILENT,
            }
        delivery = resolve_user_delivery(normalized_reply)
        user_visible_sent = False
        if delivery.user_visible and notify_user != "never":
            await _send_scheduler_control_update(
                self,
                chat_id,
                task_id or None,
                delivery.text,
            )
            user_visible_sent = True
        elif delivery.user_visible:
            logger.info(
                "Scheduled Octo control update suppressed by notify policy",
                task_id=task_id or None,
                notify_user=notify_user,
                chat_id=chat_id,
            )
        if scheduler is not None and task_id:
            scheduler.mark_executed(task_id)
        logger.info(
            "Scheduled Octo control task completed",
            task_id=task_id or None,
            notify_user=notify_user,
            chat_id=chat_id,
            user_visible_sent=user_visible_sent,
            delivery_mode=delivery.mode,
        )
        return {
            "status": "completed",
            "completed": True,
            "user_visible_sent": user_visible_sent,
            "delivery_mode": delivery.mode,
        }

    async def initialize_system(self, bot=None, allowed_chat_ids: list[int] | None = None) -> None:
        system_chat_id = 0
        logger.info("Octo waking up")
        self.start_background_tasks()

        # Load and connect MCP servers
        if self.mcp_manager:
            await self.mcp_manager.load_and_connect_all()

        # Load and start connectors
        if self.connector_manager:
            await self.connector_manager.load_and_start_all()

        restart_resume = None
        runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
        if runtime_settings is not None:
            restart_resume = await asyncio.to_thread(
                read_pending_restart_resume,
                Path(runtime_settings.state_dir),
            )
            if restart_resume and restart_resume.get("consumed_at"):
                restart_resume = None

        wake_up_prompt = (
            "You are waking up. Inspect runtime health and available workers internally. "
            "Use only bounded control-plane tools if needed, but never output a tool name or tool syntax as your final answer. "
            "Then produce a short friendly startup status message for the user in plain language."
        )
        if restart_resume:
            wake_up_prompt += "\n\n" + _build_restart_resume_message(restart_resume)
        original_send = self.internal_send
        chat_ids = [
            chat_id
            for item in (allowed_chat_ids or [])
            if (chat_id := _coerce_positive_chat_id(item)) is not None
        ]
        self._scheduled_delivery_chat_ids = list(chat_ids)
        if chat_ids and (bot or callable(original_send)):
            logger.info("Octo will send initialization message", count=len(chat_ids))
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
            logger.warning(
                "No allowed user channel recipients configured; octo will not send ready message."
            )
            self.internal_send = None
        try:
            result = await route_internal_maintenance(
                self,
                system_chat_id,
                wake_up_prompt,
            )
            if should_suppress_user_delivery(result):
                result = "Octo is online. Initialization is complete and I am ready for your tasks."
            logger.info(
                "Octo wake up complete", result_preview=f"{result[:60]}..." if result else "empty"
            )

            # Send the Octo's own response to allowed chats if configured.
            if result and self.internal_send and chat_ids:
                try:
                    await self.internal_send(system_chat_id, result)
                    logger.info("Octo initialization response sent")
                except Exception as e:
                    logger.warning("Failed to send octo initialization response", error=e)
            if restart_resume and runtime_settings is not None:
                await asyncio.to_thread(
                    mark_restart_resume_consumed,
                    Path(runtime_settings.state_dir),
                )
        except Exception:
            logger.exception("Octo failed to complete wake-up task")
        finally:
            self.internal_send = original_send

    async def handle_worker_instruction_request(
        self,
        *,
        spec: WorkerSpec,
        request: WorkerInstructionRequest,
    ) -> None:
        if request.target != "octo":
            return
        chat_id = self.get_worker_chat_id(spec.id)
        result = WorkerResult(
            status="awaiting_instruction",
            summary=f"Worker {spec.id} requested instruction: {request.question}",
            output={
                "status": "awaiting_instruction",
                "instruction_request": request.model_dump(mode="json"),
            },
            questions=[request.question],
        )
        _enqueue_internal_result(
            self,
            chat_id,
            spec.id,
            spec.task,
            result,
            correlation_id=spec.correlation_id,
            notify_user=None,
        )
        await self._emit_worker_event(
            chat_id,
            "worker_awaiting_instruction",
            {
                "run_id": spec.id,
                "worker_template_id": spec.template_id,
                "instruction_request": request.model_dump(mode="json"),
            },
        )
    async def handle_message(
        self,
        text: str,
        chat_id: int,
        approval_requester=None,
        show_typing: bool = True,
        is_ws: bool = False,
        images: list[str] | None = None,
        saved_file_paths: list[str] | None = None,
        persist_to_memory: bool = True,
        track_progress: bool = True,
        include_wakeup: bool = True,
        background_delivery: bool = False,
    ) -> OctoReply:
        if not is_ws and self._ws_active:
            logger.info("Ignoring Telegram message while WebSocket is active", chat_id=chat_id)
            return OctoReply(
                immediate="I'm currently active on WebSocket. Please use the WebSocket client or wait until it's closed.",
                followup=None,
            )
        correlation_token = None
        correlation_id = correlation_id_var.get()
        trace_bind_token = None
        trace_ctx = None
        trace_started_at_ms = now_ms()
        trace_status = "ok"
        trace_output: dict[str, Any] | None = None
        trace_metadata: dict[str, Any] = {
            "channel": "ws" if is_ws else "telegram",
            "message_kind": "heartbeat" if not track_progress else "user",
            "text_len": len(text),
            "has_images": bool(images),
            "has_files": bool(saved_file_paths),
            "persist_to_memory": persist_to_memory,
            "track_progress": track_progress,
            "background_delivery": background_delivery,
        }
        wants_followup = False
        finalized_visible_reply = False
        route_request = RouteRequest(
            mode=resolve_turn_route_mode(
                track_progress=track_progress,
                background_delivery=background_delivery,
            ),
            user_text=text,
            chat_id=chat_id,
            show_typing=show_typing,
            include_wakeup=include_wakeup,
            track_progress=track_progress,
            background_delivery=background_delivery,
        )
        trace_metadata["route_mode"] = route_request.mode.value
        if not correlation_id:
            correlation_id = f"turn-{uuid4()}"
            correlation_token = correlation_id_var.set(correlation_id)

        try:
            session_id = f"{'ws' if is_ws else 'chat'}:{chat_id}"
            if self.trace_sink is not None:
                trace_ctx = await self.trace_sink.start_trace(
                    name="octo.turn",
                    trace_id=correlation_id,
                    root_trace_id=correlation_id,
                    session_id=session_id,
                    chat_id=chat_id,
                    input={
                        "text_preview": safe_preview(text, limit=160),
                        "text_hash": hash_payload(text),
                    },
                    metadata=trace_metadata,
                )
                trace_bind_token = bind_trace_context(trace_ctx)
            self.mark_user_turn_active(correlation_id)
            if callable(approval_requester):
                self._approval_requesters[chat_id] = approval_requester
            logger.info(
                "Handling message",
                chat_id=chat_id,
                is_ws=is_ws,
                has_images=bool(images),
                route_mode=route_request.mode.value,
            )
            logger.debug("Received message text", text_len=len(text), text=text[:500])
            if not track_progress:
                self.suppress_turn_followups(correlation_id)
            if persist_to_memory:
                await self.memory.add_message(
                    "user",
                    text,
                    {
                        "chat_id": chat_id,
                        "has_images": bool(images),
                        "heartbeat": not track_progress,
                    },
                )
            bootstrap_context = None
            if route_request.mode is RouteMode.HEARTBEAT:
                reply_text = await route_heartbeat(
                    self,
                    chat_id,
                    text,
                    show_typing=show_typing,
                    include_wakeup=include_wakeup,
                )
            else:
                bootstrap_context = await build_bootstrap_context_prompt(self.store, chat_id)
                trace_metadata["bootstrap_chars"] = len(bootstrap_context.content)
                if bootstrap_context.files:
                    files_summary = ", ".join(
                        [f"{name} ({size} chars)" for name, size in bootstrap_context.files]
                    )
                    logger.debug(
                        "Octo bootstrap files",
                        route_mode=route_request.mode.value,
                        files=files_summary,
                        file_count=len(bootstrap_context.files),
                        total_chars=len(bootstrap_context.content),
                        hash=bootstrap_context.hash,
                    )
                route_kwargs: dict[str, Any] = {
                    "show_typing": show_typing,
                    "images": images,
                    "saved_file_paths": saved_file_paths,
                    "include_wakeup": include_wakeup,
                    "route_mode": route_request.mode,
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
            initial_reaction_emoji, _ = extract_reaction_and_strip(reply_text or "")
            wants_followup = self.consume_structured_followup_required(correlation_id)
            if not track_progress:
                wants_followup = False
                if background_delivery:
                    reply_text = await _normalize_heartbeat_delivery_reply(
                        self.provider, reply_text
                    )
                else:
                    reply_text = _coerce_control_plane_reply(reply_text)
                if route_request.mode is RouteMode.HEARTBEAT:
                    runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
                    if runtime_settings is not None:
                        await asyncio.to_thread(
                            update_last_internal_heartbeat,
                            runtime_settings,
                        )
            logger.info("Octo response ready")
            if track_progress:
                reply_norm = _normalize_compact(reply_text)
                prior_reply = self._last_reply_norm_by_chat.get(chat_id, "")
                if _is_progress_reply(reply_norm, prior_reply):
                    self._register_progress(chat_id, "assistant_response")
                else:
                    self._no_progress_turns_by_chat[chat_id] = (
                        int(self._no_progress_turns_by_chat.get(chat_id, 0)) + 1
                    )
                self._last_reply_norm_by_chat[chat_id] = reply_norm
            if wants_followup:
                self.mark_pending_conversational_closure(correlation_id)
            try:
                await self.get_context_health_snapshot(chat_id)
            except Exception:
                logger.debug(
                    "Failed to refresh context health snapshot", chat_id=chat_id, exc_info=True
                )
            if include_wakeup:
                self.clear_context_wakeup(chat_id)
            if bootstrap_context is not None and bootstrap_context.hash:
                await asyncio.to_thread(
                    self.store.set_chat_bootstrap_hash, chat_id, bootstrap_context.hash, utc_now()
                )
            immediate_text = sanitize_user_facing_text_preserving_reaction(reply_text)
            reaction_emoji, _ = extract_reaction_and_strip(reply_text or "")
            reaction_emoji = reaction_emoji or initial_reaction_emoji
            delivery = resolve_user_delivery(immediate_text, followup_required=wants_followup)
            logger.debug(
                "OctoReply prepared for channel delivery",
                chat_id=chat_id,
                route_mode=route_request.mode.value,
                has_react_tag="<react>" in immediate_text.lower(),
                reaction=reaction_emoji,
                delivery_mode=delivery.mode,
            )
            if not persist_to_memory and delivery.user_visible:
                await self.memory.add_message(
                    "assistant",
                    delivery.text,
                    {
                        "chat_id": chat_id,
                        "background_delivery": True,
                        "heartbeat": not track_progress,
                    },
                )
            elif persist_to_memory and delivery.user_visible:
                await self.memory.add_message(
                    "assistant",
                    delivery.text,
                    {
                        "chat_id": chat_id,
                        "heartbeat": not track_progress,
                    },
                )
            if delivery.user_visible and track_progress:
                self.note_user_visible_delivery(chat_id, delivery.text)
                if not delivery.followup_required:
                    finalized_visible_reply = True
                    self.suppress_turn_followups(correlation_id)
                    logger.info(
                        "Suppressing worker follow-ups after final in-turn reply",
                        chat_id=chat_id,
                    )
            trace_output = {
                "delivery_mode": delivery.mode,
                "followup_required": delivery.followup_required,
                "user_visible": delivery.user_visible,
                "reaction": reaction_emoji,
            }
            return OctoReply(
                immediate=delivery.text,
                followup=None,
                followup_required=delivery.followup_required,
                reaction=reaction_emoji,
                delivery_mode=delivery.mode,
            )
        except Exception as exc:
            trace_status = "error"
            trace_metadata.update(summarize_exception(exc))
            raise
        finally:
            self.mark_user_turn_inactive(correlation_id)
            if track_progress and not finalized_visible_reply:
                self.clear_suppressed_turn_followups(correlation_id)
            if finalized_visible_reply:
                dropped = _discard_worker_followup_batch(
                    chat_id,
                    correlation_id,
                    only_if_created_during_active_turn=True,
                )
                if dropped:
                    logger.info(
                        "Dropped worker follow-up after final in-turn reply",
                        chat_id=chat_id,
                    )
            pending_followup_work = self.has_active_workers_for_correlation(
                correlation_id
            ) or self.has_pending_internal_results_for_correlation(correlation_id)
            if wants_followup and pending_followup_work and not finalized_visible_reply:
                _schedule_worker_followup_flush(self, chat_id, correlation_id)
            else:
                _discard_worker_followup_batch(
                    chat_id,
                    correlation_id,
                    only_if_created_during_active_turn=True,
                )
            self.clear_structured_followup_required(correlation_id)
            if trace_ctx is not None and self.trace_sink is not None:
                finish_meta = dict(trace_metadata)
                finish_meta.update(
                    {
                        "duration_ms": round(now_ms() - trace_started_at_ms, 2),
                        "wants_followup": wants_followup,
                        "finalized_visible_reply": finalized_visible_reply,
                        "active_workers_for_correlation": self.has_active_workers_for_correlation(
                            correlation_id
                        ),
                        "pending_internal_results": self.has_pending_internal_results_for_correlation(
                            correlation_id
                        ),
                    }
                )
                await self.trace_sink.finish_trace(
                    trace_ctx,
                    status=trace_status,
                    output=trace_output,
                    metadata=finish_meta,
                )
            if trace_bind_token is not None:
                reset_trace_context(trace_bind_token)
            if correlation_token is not None:
                correlation_id_var.reset(correlation_token)
