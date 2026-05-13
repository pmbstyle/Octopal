from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import structlog

from octopal.browser.manager import get_browser_manager as get_browser_manager
from octopal.channels.telegram.approvals import ApprovalManager
from octopal.infrastructure.connectors.manager import ConnectorManager
from octopal.infrastructure.logging import correlation_id_var as correlation_id_var
from octopal.infrastructure.mcp.manager import MCPManager
from octopal.infrastructure.observability.base import (
    TraceSink,
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
from octopal.runtime.octo.context_runtime import OctoContextRuntimeMixin
from octopal.runtime.octo.control_replies import (
    _coerce_control_plane_reply as _coerce_control_plane_reply,
)
from octopal.runtime.octo.control_replies import (
    _looks_like_scheduled_octo_control_route_block as _looks_like_scheduled_octo_control_route_block,
)
from octopal.runtime.octo.message_runtime import OctoMessageRuntimeMixin
from octopal.runtime.octo.output_runtime import OctoOutputRuntimeMixin
from octopal.runtime.octo.prompt_builder import (
    build_bootstrap_context_prompt as build_bootstrap_context_prompt,
)
from octopal.runtime.octo.recent_task_runtime import OctoRecentTaskRuntimeMixin
from octopal.runtime.octo.reply import OctoReply as OctoReply
from octopal.runtime.octo.router import route_heartbeat as route_heartbeat
from octopal.runtime.octo.router import route_internal_maintenance as route_internal_maintenance
from octopal.runtime.octo.router import route_or_reply as route_or_reply
from octopal.runtime.octo.router import route_proactive_tick as route_proactive_tick
from octopal.runtime.octo.router import route_scheduled_octo_control as route_scheduled_octo_control
from octopal.runtime.octo.router import route_scheduler_tick as route_scheduler_tick
from octopal.runtime.octo.runtime_config import _env_flag, _env_int
from octopal.runtime.octo.scheduled_runtime import OctoScheduledRuntimeMixin
from octopal.runtime.octo.scheduler_helpers import (
    _empty_scheduler_metric_counters,
)
from octopal.runtime.octo.scheduler_runtime import OctoSchedulerRuntimeMixin
from octopal.runtime.octo.self_lifecycle import OctoSelfLifecycleMixin
from octopal.runtime.octo.self_queue import OctoSelfQueueMixin
from octopal.runtime.octo.startup_runtime import OctoStartupRuntimeMixin
from octopal.runtime.octo.turn_state import OctoTurnStateMixin
from octopal.runtime.octo.worker_dispatch import OctoWorkerDispatchMixin
from octopal.runtime.octo.worker_instruction_runtime import OctoWorkerInstructionRuntimeMixin
from octopal.runtime.octo.worker_registry import OctoWorkerRegistryMixin
from octopal.runtime.octo.workspace_paths import _workspace_dir as _workspace_dir
from octopal.runtime.policy.engine import PolicyEngine
from octopal.runtime.scheduler.service import (
    SchedulerService,
)
from octopal.runtime.self_control import (
    check_update_status as check_update_status,
)
from octopal.runtime.workers.runtime import WorkerRuntime

logger = structlog.get_logger(__name__)
_FOLLOWUP_QUEUES = _followup_pipeline._FOLLOWUP_QUEUES
_FOLLOWUP_TASKS = _followup_pipeline._FOLLOWUP_TASKS
_INTERNAL_QUEUES = _followup_pipeline._INTERNAL_QUEUES
_INTERNAL_TASKS = _followup_pipeline._INTERNAL_TASKS
_WORKER_FOLLOWUP_BATCHES = _followup_pipeline._WORKER_FOLLOWUP_BATCHES
_QUEUE_IDLE_TIMEOUT_SECONDS = _followup_pipeline._QUEUE_IDLE_TIMEOUT_SECONDS
_WORKER_RESULT_ROUTING_TIMEOUT_SECONDS = _followup_pipeline._WORKER_RESULT_ROUTING_TIMEOUT_SECONDS
_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS = _followup_pipeline._WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS
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
    OctoScheduledRuntimeMixin,
    OctoOutputRuntimeMixin,
    OctoStartupRuntimeMixin,
    OctoWorkerInstructionRuntimeMixin,
    OctoRecentTaskRuntimeMixin,
    OctoMessageRuntimeMixin,
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
