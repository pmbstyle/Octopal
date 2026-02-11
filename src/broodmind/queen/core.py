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
from broodmind.policy.engine import PolicyEngine
from broodmind.providers.base import InferenceProvider
from broodmind.queen.prompt_builder import (
    build_bootstrap_context_prompt,
    build_queen_prompt,
)
from broodmind.runtime_metrics import update_component_gauges
from broodmind.store.base import Store
from broodmind.telegram.approvals import ApprovalManager
from broodmind.tools.registry import ToolSpec, filter_tools
from broodmind.tools.tools import get_tools
from broodmind.utils import utc_now
from broodmind.workers.contracts import TaskRequest, WorkerResult
from broodmind.workers.runtime import WorkerRuntime

logger = structlog.get_logger(__name__)
_FOLLOWUP_QUEUES: dict[int, asyncio.Queue] = {}
_FOLLOWUP_TASKS: dict[int, asyncio.Task] = {}
_INTERNAL_QUEUES: dict[int, asyncio.Queue] = {}
_INTERNAL_TASKS: dict[int, asyncio.Task] = {}
_QUEUE_IDLE_TIMEOUT_SECONDS = 300.0


def _publish_runtime_metrics() -> None:
    update_component_gauges(
        "queen",
        {
            "followup_queues": len(_FOLLOWUP_QUEUES),
            "followup_tasks": len(_FOLLOWUP_TASKS),
            "internal_queues": len(_INTERNAL_QUEUES),
            "internal_tasks": len(_INTERNAL_TASKS),
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
    queue = _FOLLOWUP_QUEUES.get(chat_id)
    if not queue:
        queue = asyncio.Queue()
        _FOLLOWUP_QUEUES[chat_id] = queue
    if chat_id not in _FOLLOWUP_TASKS or _FOLLOWUP_TASKS[chat_id].done():
        _FOLLOWUP_TASKS[chat_id] = asyncio.create_task(_followup_worker(chat_id, queue))
    _publish_runtime_metrics()
    loop = asyncio.get_running_loop()
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
            # Always route worker result back through Queen decision logic.
            # User delivery is a separate concern from internal decision-making.
            try:
                final_text = await asyncio.wait_for(
                    _route_worker_result_back_to_queen(queen, chat_id, task_text, result),
                    timeout=45.0,
                )
            except TimeoutError:
                logger.warning("Worker-result routing timed out", chat_id=chat_id)
                final_text = "NO_USER_RESPONSE"

            if _should_send_worker_followup(final_text):
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
    queue = _INTERNAL_QUEUES.get(chat_id)
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
    internal_send: callable | None = None
    internal_progress_send: callable | None = None
    _cleanup_task: asyncio.Task | None = None
    _recent_tasks: set[str] = None  # Track tasks in current conversation to detect duplicates
    _approval_requesters: dict[int, Callable[[Any], Awaitable[bool]]] | None = None

    def __post_init__(self):
        if self._recent_tasks is None:
            self._recent_tasks = set()
        if self._approval_requesters is None:
            self._approval_requesters = {}

    async def _periodic_cleanup(self, interval_seconds: int):
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                deleted = await asyncio.to_thread(self.store.cleanup_old_workers)
                if deleted > 0:
                    logger.info("Periodic cleanup complete", deleted_workers=deleted)
            except Exception:
                logger.exception("Periodic worker cleanup failed")

    def start_background_tasks(self, cleanup_interval_seconds: int = 3600):
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._periodic_cleanup(cleanup_interval_seconds))
            logger.info("Started periodic worker cleanup task")

    async def stop_background_tasks(self):
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                logger.info("Stopped periodic worker cleanup task")

    async def initialize_system(self, bot=None, allowed_chat_ids: list[int] | None = None) -> None:
        system_chat_id = 0
        logger.info("Queen waking up")
        self.start_background_tasks()
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
            result = await _route_or_reply(
                self, self.provider, self.memory, wake_up_prompt, system_chat_id, bootstrap_context.content
            )
            logger.info("Queen wake up complete", result_preview=f"{result[:60]}..." if result else "empty")
            # Only send result if we have valid chat IDs (chat_id=0 is invalid)
            if self.internal_send and result and chat_ids:
                try:
                    await self.internal_send(system_chat_id, result)
                    logger.info("Queen ready message sent")
                except Exception as e:
                    logger.warning("Failed to send queen ready message", error=e)
        finally:
            self.internal_send = original_send

    async def handle_message(
        self,
        text: str,
        chat_id: int,
        approval_requester=None,
    ) -> QueenReply:
        # Clear recent tasks at the start of each new user message
        self._recent_tasks.clear()
        if callable(approval_requester):
            self._approval_requesters[chat_id] = approval_requester
        logger.info("Handling message", chat_id=chat_id)
        logger.debug("Received message text", text_len=len(text), text=text[:500])
        await self.memory.add_message("user", text, {"chat_id": chat_id})
        bootstrap_context = await build_bootstrap_context_prompt(self.store, chat_id)
        if bootstrap_context.files:
            files_summary = ", ".join([f"{name} ({size} chars)" for name, size in bootstrap_context.files])
            logger.debug("Queen bootstrap files", files=files_summary, hash=bootstrap_context.hash)
        reply_text = await _route_or_reply(
            self, self.provider, self.memory, text, chat_id, bootstrap_context.content
        )
        logger.info("Queen response ready")
        await self.memory.add_message("assistant", reply_text, {"chat_id": chat_id})
        if bootstrap_context.hash:
            await asyncio.to_thread(
                self.store.set_chat_bootstrap_hash, chat_id, bootstrap_context.hash, utc_now()
            )
        return QueenReply(immediate=_normalize_plain_text(reply_text), followup=None)

    async def _start_worker_async(
        self,
        worker_id: str,
        task: str,
        chat_id: int,
        inputs: dict[str, Any] | None,
        tools: list[str] | None,
        model: str | None,
        timeout_seconds: int | None,
    ) -> dict[str, Any]:
        from broodmind.logging_config import correlation_id_var

        # Create a task signature for duplicate detection
        task_signature = f"{worker_id}:{task[:100]}"  # First 100 chars is enough to detect duplicates
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


def _normalize_plain_text(text: str) -> str:
    cleaned = text.replace("\r\n", "\n").replace("\r", "\n")
    return cleaned.strip()


def _looks_like_tool_error(text: str) -> bool:
    lowered = text.lower()
    return " error" in lowered or "failed" in lowered


def _should_send_worker_followup(text: str) -> bool:
    value = (text or "").strip()
    if not value:
        return False
    if value.upper() in {"NO_USER_RESPONSE", "HEARTBEAT_OK"}:
        return False
    return True


async def _route_worker_result_back_to_queen(
    queen: Queen,
    chat_id: int,
    task_text: str,
    result: WorkerResult,
) -> str:
    payload = {
        "task": task_text,
        "summary": result.summary,
        "output": result.output,
        "questions": result.questions,
        "thinking_steps": result.thinking_steps,
        "tools_used": result.tools_used,
    }
    payload_json = json.dumps(payload, ensure_ascii=False)
    if len(payload_json) > 12000:
        payload_json = payload_json[:12000] + "...[truncated]"

    worker_result_prompt = (
        "Worker completed. Decide and execute next action based on this payload.\n"
        "<worker_result>\n"
        f"{payload_json}\n"
        "</worker_result>\n\n"
        "If a user-facing response is required now, provide it in plain text.\n"
        "If no user-facing response is needed, return exactly: NO_USER_RESPONSE"
    )
    bootstrap_context = await build_bootstrap_context_prompt(queen.store, chat_id)
    reply_text = await _route_or_reply(
        queen,
        queen.provider,
        queen.memory,
        worker_result_prompt,
        chat_id,
        bootstrap_context.content,
    )
    return _normalize_plain_text(reply_text)


async def _route_or_reply(queen: Queen, provider: InferenceProvider, memory: MemoryService, user_text: str, chat_id: int, bootstrap_context: str) -> str:
    messages = await build_queen_prompt(
        store=queen.store, memory=memory, canon=queen.canon, user_text=user_text, chat_id=chat_id, bootstrap_context=bootstrap_context
    )
    _log_system_prompt(messages, "route")
    queen_tools, ctx = _get_queen_tools(queen, chat_id)
    tool_capable = getattr(provider, "complete_with_tools", None)
    if callable(tool_capable):
        tools = [spec.to_openai_tool() for spec in queen_tools]
        last_error: str | None = None
        had_tool_calls = False
        max_attempts = 10
        for _ in range(max_attempts):
            result = await provider.complete_with_tools(messages, tools=tools, tool_choice="auto")
            content_raw = result.get("content", "")
            tool_calls = result.get("tool_calls") or []
            if tool_calls:
                had_tool_calls = True
                assistant_msg: dict[str, Any] = {"role": "assistant", "tool_calls": tool_calls}
                if content_raw:
                    assistant_msg["content"] = content_raw
                messages.append(assistant_msg)
                for call in tool_calls:
                    tool_result = await _handle_queen_tool_call(call, queen_tools, ctx)
                    tool_result_text = (
                        tool_result
                        if isinstance(tool_result, str)
                        else json.dumps(tool_result, ensure_ascii=False)
                    )
                    messages.append(
                        {"role": "tool", "tool_call_id": call.get("id"), "content": tool_result_text}
                    )
                    if "error" in tool_result_text.lower() or "failed" in tool_result_text.lower():
                        last_error = tool_result_text
                continue
            if content_raw:
                logger.debug("Queen output", output=content_raw)
            return _normalize_plain_text(content_raw)
        if had_tool_calls:
            return "Task accepted. I am processing it."
        if last_error and _looks_like_tool_error(last_error):
            return "I couldn't complete that request. The tooling failed and needs correction."
        return last_error or ""
    response_raw = await provider.complete(messages)
    logger.debug("Queen output", output=response_raw)
    return _normalize_plain_text(response_raw)



def _log_system_prompt(messages: list, label: str) -> None:
    system_lengths = [len(m.content) for m in messages if m.role == "system" and m.content]
    if system_lengths:
        logger.debug(
            "Queen system prompt",
            label=label,
            parts=len(system_lengths),
            total_chars=sum(system_lengths),
        )

def _get_queen_tools(queen: Queen, chat_id: int) -> tuple[list[ToolSpec], dict[str, object]]:
    perms = {
        "filesystem_read": True,
        "filesystem_write": True,
        "worker_manage": True,
        "llm_subtask": True,
        "canon_manage": True,
        "network": True,
        "exec": True,
        "service_read": True,
        "service_control": True,
        "deploy_control": True,
        "db_admin": True,
        "security_audit": True,
        "self_control": True,
    }
    ctx = {"base_dir": Path(os.getenv("BROODMIND_WORKSPACE_DIR", "workspace")).resolve(), "queen": queen, "chat_id": chat_id}
    tool_specs = filter_tools(get_tools(), permissions=perms)
    return tool_specs, ctx

async def _handle_queen_tool_call(call: dict, tools: list[ToolSpec], ctx: dict[str, object]) -> str:
    function = call.get("function") or {}
    name = function.get("name")
    args_raw = function.get("arguments", "{}")
    try:
        args = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
    except Exception:
        args = {}
    logger.debug("Queen tool call", tool_name=name, args=args)
    for spec in tools:
        if spec.name == name:
            if spec.is_async:
                result = await spec.handler(args, ctx)
            else:
                result = await asyncio.to_thread(spec.handler, args, ctx)
            logger.debug("Queen tool result", tool_name=name, result_preview=f"{str(result)[:200]}...")
            return result
    return f"Unknown tool: {name}"
