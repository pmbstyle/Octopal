from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
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
from octopal.runtime.octo.context_runtime import OctoContextRuntimeMixin
from octopal.runtime.octo.control_plane import (
    RouteMode,
    RouteRequest,
    resolve_turn_route_mode,
)
from octopal.runtime.octo.control_replies import (
    _coerce_control_plane_reply,
    _normalize_heartbeat_delivery_reply,
)
from octopal.runtime.octo.control_replies import (
    _looks_like_scheduled_octo_control_route_block as _looks_like_scheduled_octo_control_route_block,
)
from octopal.runtime.octo.delivery import resolve_user_delivery
from octopal.runtime.octo.output_runtime import OctoOutputRuntimeMixin
from octopal.runtime.octo.prompt_builder import (
    build_bootstrap_context_prompt,
)
from octopal.runtime.octo.reply import OctoReply
from octopal.runtime.octo.router import (
    route_heartbeat,
    route_or_reply,
)
from octopal.runtime.octo.router import route_internal_maintenance as route_internal_maintenance
from octopal.runtime.octo.router import route_proactive_tick as route_proactive_tick
from octopal.runtime.octo.router import route_scheduled_octo_control as route_scheduled_octo_control
from octopal.runtime.octo.router import route_scheduler_tick as route_scheduler_tick
from octopal.runtime.octo.runtime_config import _env_flag, _env_int
from octopal.runtime.octo.runtime_config import _env_float as _env_float
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
from octopal.runtime.octo.worker_registry import OctoWorkerRegistryMixin
from octopal.runtime.octo.workspace_paths import _workspace_dir as _workspace_dir
from octopal.runtime.policy.engine import PolicyEngine
from octopal.runtime.scheduler.service import (
    SchedulerService,
)
from octopal.runtime.self_control import (
    check_update_status as check_update_status,
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
    utc_now,
)

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
    OctoScheduledRuntimeMixin,
    OctoOutputRuntimeMixin,
    OctoStartupRuntimeMixin,
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
