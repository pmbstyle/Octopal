from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import structlog
from broodmind.memory.service import MemoryService
from broodmind.policy.engine import PolicyEngine
from broodmind.providers.base import InferenceProvider, Message
from broodmind.queen.prompt_builder import (
    BootstrapContext,
    build_bootstrap_context_prompt,
    build_queen_prompt,
)
from broodmind.store.base import Store
from broodmind.telegram.approvals import ApprovalManager
from broodmind.tools.registry import ToolSpec, filter_tools
from broodmind.tools.tools import get_tools
from broodmind.utils import utc_now
from broodmind.workers.contracts import TaskRequest, WorkerResult, WorkerSpec
from broodmind.workers.runtime import WorkerRuntime

logger = structlog.get_logger(__name__)
_FOLLOWUP_QUEUES: dict[int, asyncio.Queue] = {}
_FOLLOWUP_TASKS: dict[int, asyncio.Task] = {}
_INTERNAL_QUEUES: dict[int, asyncio.Queue] = {}
_INTERNAL_TASKS: dict[int, asyncio.Task] = {}


async def _followup_worker(chat_id: int, queue: asyncio.Queue) -> None:
    while True:
        future, coro = await queue.get()
        try:
            result = await coro
            if not future.cancelled():
                future.set_result(result)
        except Exception as exc:
            if not future.cancelled():
                future.set_exception(exc)
        finally:
            queue.task_done()


def _enqueue_followup(chat_id: int, coro) -> asyncio.Future[str]:
    queue = _FOLLOWUP_QUEUES.get(chat_id)
    if not queue:
        queue = asyncio.Queue()
        _FOLLOWUP_QUEUES[chat_id] = queue
    if chat_id not in _FOLLOWUP_TASKS or _FOLLOWUP_TASKS[chat_id].done():
        _FOLLOWUP_TASKS[chat_id] = asyncio.create_task(_followup_worker(chat_id, queue))
    loop = asyncio.get_running_loop()
    future: asyncio.Future[str] = loop.create_future()
    queue.put_nowait((future, coro))
    return future


async def _internal_worker(queen: "Queen", chat_id: int, queue: asyncio.Queue) -> None:
    """Process completed worker results.

    Worker results are logged and stored in memory but NOT automatically sent to the user.
    The queen decides what to communicate based on worker results.
    """
    while True:
        task_text, result = await queue.get()
        try:
            # Add worker result to memory for context, but don't auto-send
            if result.summary:
                await queen.memory.add_message(
                    "system",
                    f"Worker completed: {result.summary}",
                    {"worker_result": True, "task": task_text}
                )
            if result.error:
                await queen.memory.add_message(
                    "system",
                    f"Worker error: {result.error}",
                    {"worker_result": True, "task": task_text}
                )
            logger.debug("Worker result processed (not auto-sent)", summary_len=len(result.summary or ""))
        except Exception:
            logger.exception("Failed to process internal worker result")
        finally:
            queue.task_done()


def _enqueue_internal_result(queen: "Queen", chat_id: int, task_text: str, result: WorkerResult) -> None:
    queue = _INTERNAL_QUEUES.get(chat_id)
    if not queue:
        queue = asyncio.Queue()
        _INTERNAL_QUEUES[chat_id] = queue
    if chat_id not in _INTERNAL_TASKS or _INTERNAL_TASKS[chat_id].done():
        _INTERNAL_TASKS[chat_id] = asyncio.create_task(_internal_worker(queen, chat_id, queue))
    queue.put_nowait((task_text, result))


@dataclass
class Queen:
    provider: InferenceProvider
    store: Store
    policy: PolicyEngine
    runtime: WorkerRuntime
    approvals: ApprovalManager
    memory: MemoryService
    internal_send: callable | None = None
    _cleanup_task: asyncio.Task | None = None
    _recent_tasks: set[str] = None  # Track tasks in current conversation to detect duplicates

    def __post_init__(self):
        if self._recent_tasks is None:
            self._recent_tasks = set()

    async def _periodic_cleanup(self, interval_seconds: int):
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                deleted = self.store.cleanup_old_workers()
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
                        await bot.send_message(chat_id=target_chat_id, text=text)
                        logger.debug("Sent initialization message", chat_id=target_chat_id)
                    except Exception as e:
                        logger.warning("Failed to send to chat_id", chat_id=target_chat_id, error=e)
            self.internal_send = send_to_allowed_chats
        else:
            logger.warning("No ALLOWED_TELEGRAM_CHAT_IDS configured; queen will not send ready message.")
            self.internal_send = None
        try:
            bootstrap_context = build_bootstrap_context_prompt(self.store, system_chat_id)
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
    ) -> "QueenReply":
        # Clear recent tasks at the start of each new user message
        self._recent_tasks.clear()
        logger.info("Handling message", chat_id=chat_id)
        logger.debug("Received message text", text_len=len(text), text=text[:500])
        await self.memory.add_message("user", text, {"chat_id": chat_id})
        bootstrap_context = build_bootstrap_context_prompt(self.store, chat_id)
        if bootstrap_context.files:
            files_summary = ", ".join([f"{name} ({size} chars)" for name, size in bootstrap_context.files])
            logger.debug("Queen bootstrap files", files=files_summary, hash=bootstrap_context.hash)
        reply_text = await _route_or_reply(
            self, self.provider, self.memory, text, chat_id, bootstrap_context.content
        )
        logger.info("Queen response ready")
        await self.memory.add_message("assistant", reply_text, {"chat_id": chat_id})
        if bootstrap_context.hash:
            self.store.set_chat_bootstrap_hash(chat_id, bootstrap_context.hash, utc_now())
        return QueenReply(immediate=_normalize_plain_text(reply_text), followup=None)

    def _start_worker_async(
        self,
        worker_id: str,
        task: str,
        chat_id: int,
        inputs: dict[str, Any] | None,
        tools: list[str] | None,
        timeout_seconds: int | None,
    ) -> str:
        from broodmind.logging_config import correlation_id_var

        # Create a task signature for duplicate detection
        task_signature = f"{worker_id}:{task[:100]}"  # First 100 chars is enough to detect duplicates
        if task_signature in self._recent_tasks:
            logger.warning("Duplicate worker task detected, skipping", worker_id=worker_id, task_prefix=task[:50])
            # Return a fake run_id but don't actually start the worker
            return f"skipped-duplicate-{uuid4().hex[:8]}"

        self._recent_tasks.add(task_signature)
        task_request = TaskRequest(
            worker_id=worker_id,
            task=task,
            inputs=inputs or {},
            tools=tools,
            timeout_seconds=timeout_seconds,
            correlation_id=correlation_id_var.get(),
        )
        run_id = str(uuid4())
        async def _runner() -> None:
            try:
                result = await self.runtime.run_task(task_request)
            except Exception as exc:
                result = WorkerResult(summary=f"Worker error: {exc}", output={"error": str(exc)})
            _enqueue_internal_result(self, chat_id, task, result)
        asyncio.create_task(_runner())
        return run_id


@dataclass
class QueenReply:
    immediate: str
    followup: "asyncio.Task[str] | None"


def _normalize_plain_text(text: str) -> str:
    cleaned = text.replace("\r\n", "\n").replace("\r", "\n")
    return cleaned.strip()


def _looks_like_tool_error(text: str) -> bool:
    lowered = text.lower()
    return " error" in lowered or "failed" in lowered


async def _route_or_reply(queen: Queen, provider: InferenceProvider, memory: MemoryService, user_text: str, chat_id: int, bootstrap_context: str) -> str:
    messages = await build_queen_prompt(
        store=queen.store, memory=memory, user_text=user_text, chat_id=chat_id, bootstrap_context=bootstrap_context
    )
    _log_system_prompt(messages, "route")
    queen_tools, ctx = _get_queen_tools(queen, chat_id)
    tool_capable = getattr(provider, "complete_with_tools", None)
    if callable(tool_capable):
        tools = [spec.to_openai_tool() for spec in queen_tools]
        last_error: str | None = None
        max_attempts = 10
        for _ in range(max_attempts):
            result = await provider.complete_with_tools(messages, tools=tools, tool_choice="auto")
            content_raw = result.get("content", "")
            tool_calls = result.get("tool_calls") or []
            if tool_calls:
                for call in tool_calls:
                    tool_result = await _handle_queen_tool_call(call, queen_tools, ctx)
                    messages.append({"role": "tool", "tool_call_id": call.get("id"), "content": tool_result})
                    if "error" in tool_result.lower() or "failed" in tool_result.lower():
                        last_error = tool_result
                if not last_error:
                    last_error = "No tool call completed. Use a tool to make progress."
                continue
            if content_raw:
                logger.debug("Queen output", output=content_raw)
            return _normalize_plain_text(content_raw)
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
    perms = {"filesystem_read": True, "filesystem_write": True, "worker_manage": True, "llm_subtask": True}
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
                result = spec.handler(args, ctx)
            logger.debug("Queen tool result", tool_name=name, result_preview=f"{str(result)[:200]}...")
            return result
    return f"Unknown tool: {name}"