from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import structlog

from broodmind.intents.types import ActionIntent
from broodmind.memory.canon import CanonService
from broodmind.memory.service import MemoryService
from broodmind.scheduler.service import SchedulerService
from broodmind.mcp.manager import MCPManager
from broodmind.policy.engine import PolicyEngine
from broodmind.providers.base import InferenceProvider
from broodmind.browser.manager import get_browser_manager
from broodmind.queen.prompt_builder import (
    build_bootstrap_context_prompt,
    build_queen_prompt,
)
from broodmind.queen.router import (
    normalize_plain_text,
    route_or_reply,
    route_worker_result_back_to_queen,
    should_send_worker_followup,
)
from broodmind.runtime_metrics import update_component_gauges
from broodmind.store.base import Store
from broodmind.telegram.approvals import ApprovalManager
from broodmind.utils import utc_now
from broodmind.workers.contracts import TaskRequest, WorkerResult
from broodmind.workers.runtime import WorkerRuntime

logger = structlog.get_logger(__name__)
_FOLLOWUP_QUEUES: dict[int, asyncio.Queue] = {}
_FOLLOWUP_TASKS: dict[int, asyncio.Task] = {}
_INTERNAL_QUEUES: dict[int, asyncio.Queue] = {}
_INTERNAL_TASKS: dict[int, asyncio.Task] = {}
_QUEUE_IDLE_TIMEOUT_SECONDS = 300.0


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
            task_text, result = await asyncio.wait_for(queue.get(), timeout=_QUEUE_IDLE_TIMEOUT_SECONDS)
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
                        timeout=180.0,
                    )
                except TimeoutError:
                    logger.warning("Worker-result routing timed out", chat_id=chat_id)
                    final_text = "NO_USER_RESPONSE"

                if should_send_worker_followup(final_text):
                    if queen.internal_send:
                        await queen.internal_send(chat_id, final_text)
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


def _enqueue_internal_result(queen: Queen, chat_id: int, task_text: str, result: WorkerResult) -> None:
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
    queue.put_nowait((task_text, result))
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

    def __post_init__(self):
        if self._recent_tasks is None:
            self._recent_tasks = set()
        if self._approval_requesters is None:
            self._approval_requesters = {}
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
            except Exception:
                logger.exception("Periodic worker cleanup failed")

    async def _periodic_metrics_publish(self, interval_seconds: int):
        from broodmind.runtime_metrics import update_component_gauges
        while True:
            await asyncio.sleep(interval_seconds)
            try:
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
        
        wake_up_prompt = "You are waking up. Your first task is to read AGENTS.md and then list available workers."
        original_send = self.internal_send
        chat_ids = allowed_chat_ids or []
        if chat_ids and bot:
            logger.info("Queen will send initialization message", count=len(chat_ids))
            logger.debug("Allowed chat_ids", chat_ids=chat_ids)
            async def send_to_allowed_chats(chat_id, text):
                for target_chat_id in chat_ids:
                    try:
                        if callable(original_send):
                            # Reuse Telegram send pipeline (chunking, parse_mode, escaping).
                            await original_send(target_chat_id, text)
                        else:
                            await bot.send_message(chat_id=target_chat_id, text=text)
                        logger.debug("Sent initialization message", chat_id=target_chat_id)
                    except Exception as e:
                        logger.warning("Failed to send to chat_id", chat_id=target_chat_id, error=e)
            self.internal_send = send_to_allowed_chats
        else:
            logger.warning("No ALLOWED_TELEGRAM_CHAT_IDS configured; queen will not send ready message.")
            self.internal_send = None
        try:
            bootstrap_context = await build_bootstrap_context_prompt(self.store, system_chat_id)
            result = await route_or_reply(
                self, self.provider, self.memory, wake_up_prompt, system_chat_id, bootstrap_context.content
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

    async def handle_message(
        self,
        text: str,
        chat_id: int,
        approval_requester=None,
        show_typing: bool = True,
        is_ws: bool = False,
        images: list[str] | None = None,
    ) -> QueenReply:
        if not is_ws and self._ws_active:
            logger.info("Ignoring Telegram message while WebSocket is active", chat_id=chat_id)
            return QueenReply(
                immediate="I'm currently active on WebSocket. Please use the WebSocket client or wait until it's closed.",
                followup=None,
            )

        # Clear recent tasks at the start of each new user message
        self._recent_tasks.clear()
        if callable(approval_requester):
            self._approval_requesters[chat_id] = approval_requester
        logger.info("Handling message", chat_id=chat_id, is_ws=is_ws, has_images=bool(images))
        logger.debug("Received message text", text_len=len(text), text=text[:500])
        await self.memory.add_message("user", text, {"chat_id": chat_id, "has_images": bool(images)})
        bootstrap_context = await build_bootstrap_context_prompt(self.store, chat_id)
        if bootstrap_context.files:
            files_summary = ", ".join([f"{name} ({size} chars)" for name, size in bootstrap_context.files])
            logger.debug("Queen bootstrap files", files=files_summary, hash=bootstrap_context.hash)
        try:
            reply_text = await route_or_reply(
                self,
                self.provider,
                self.memory,
                text,
                chat_id,
                bootstrap_context.content,
                show_typing=show_typing,
                images=images,
            )
        except TypeError as exc:
            # Backward-compatible fallback for monkeypatched tests/extensions using the old signature.
            if "unexpected keyword argument 'images'" not in str(exc):
                raise
            reply_text = await route_or_reply(
                self, self.provider, self.memory, text, chat_id, bootstrap_context.content, show_typing=show_typing
            )
        logger.info("Queen response ready")
        await self.memory.add_message("assistant", reply_text, {"chat_id": chat_id})
        if bootstrap_context.hash:
            await asyncio.to_thread(
                self.store.set_chat_bootstrap_hash, chat_id, bootstrap_context.hash, utc_now()
            )
        return QueenReply(immediate=normalize_plain_text(reply_text), followup=None)

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
    ) -> dict[str, Any]:
        from broodmind.logging_config import correlation_id_var

        # Create a task signature for duplicate detection
        schedule_sig = scheduled_task_id or "-"
        task_signature = f"{worker_id}:{schedule_sig}:{task[:100]}"  # Keep duplicate detection strict per schedule/task pair.
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
        await self._emit_progress(
            chat_id,
            "queued",
            f"Queued worker '{worker_id}' as {run_id}.",
            {"worker_id": run_id, "worker_template_id": worker_id},
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
        )

        requester = self._approval_requesters.get(chat_id)
        if requester is None and getattr(self.approvals, "bot", None):
            async def _telegram_requester(intent: ActionIntent) -> bool:
                return await self.approvals.request_approval(chat_id, intent)

            requester = _telegram_requester

        async def _runner() -> None:
            try:
                await self._emit_progress(
                    chat_id,
                    "running",
                    f"Worker {run_id} is running.",
                    {"worker_id": run_id, "worker_template_id": worker_id},
                )
                result = await self.runtime.run_task(task_request, approval_requester=requester)
                if scheduled_task_id and self.scheduler:
                    worker_record = await asyncio.to_thread(self.store.get_worker, run_id)
                    if worker_record and worker_record.status == "completed":
                        self.scheduler.mark_executed(scheduled_task_id)
                        logger.info(
                            "Marked scheduled task as executed after worker completion",
                            task_id=scheduled_task_id,
                            run_id=run_id,
                        )
                    else:
                        logger.warning(
                            "Skipped scheduled task execution mark due to non-completed worker state",
                            task_id=scheduled_task_id,
                            run_id=run_id,
                            worker_status=getattr(worker_record, "status", None),
                        )
                await self._emit_progress(
                    chat_id,
                    "completed",
                    f"Worker {run_id} completed.",
                    {"worker_id": run_id, "worker_template_id": worker_id},
                )
            except Exception as exc:
                result = WorkerResult(summary=f"Worker error: {exc}", output={"error": str(exc)})
                await self._emit_progress(
                    chat_id,
                    "failed",
                    f"Worker {run_id} failed: {exc}",
                    {"worker_id": run_id, "worker_template_id": worker_id},
                )
            _enqueue_internal_result(self, chat_id, task, result)
        asyncio.create_task(_runner())
        await self._emit_progress(
            chat_id,
            "worker_started",
            f"Worker started: {run_id}",
            {"worker_id": run_id, "worker_template_id": worker_id},
        )
        return {"status": "started", "run_id": run_id, "worker_id": run_id}

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


@dataclass
class QueenReply:
    immediate: str
    followup: asyncio.Task[str] | None
