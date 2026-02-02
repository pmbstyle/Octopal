from __future__ import annotations

from dataclasses import dataclass
import asyncio
import hashlib
import logging
import os
from datetime import datetime, timedelta, timezone
import json
from uuid import uuid4

from broodmind.memory.service import MemoryService
from broodmind.policy.engine import PolicyEngine
from broodmind.providers.base import InferenceProvider, Message
from broodmind.store.base import Store
from broodmind.telegram.approvals import ApprovalManager
from broodmind.workers.contracts import TaskRequest, WorkerResult, WorkerSpec
from broodmind.workers.runtime import WorkerRuntime
from broodmind.tools.registry import ToolSpec, filter_tools
from broodmind.tools.tools import get_tools
from pathlib import Path

from broodmind.utils import utc_now

logger = logging.getLogger(__name__)
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
    while True:
        task_text, result = await queue.get()
        try:
            response = await _compose_user_reply(queen.provider, queen.memory, task_text, chat_id, result)
            if queen.internal_send and response.strip():
                await queen.internal_send(chat_id, response)
            else:
                logger.debug("Internal send skipped (no sender or empty response)")
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

    async def _periodic_cleanup(self, interval_seconds: int):
        """Periodically clean up old worker records."""
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                deleted = self.store.cleanup_old_workers()
                if deleted > 0:
                    logger.info("Queen periodic cleanup: removed %d old worker records", deleted)
            except Exception:
                logger.exception("Error during periodic worker cleanup")

    def start_background_tasks(self, cleanup_interval_seconds: int = 3600):
        """Start background tasks like periodic cleanup."""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._periodic_cleanup(cleanup_interval_seconds))
            logger.info("Started periodic worker cleanup task.")

    async def stop_background_tasks(self):
        """Stop all running background tasks."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                logger.info("Stopped periodic worker cleanup task.")

    async def initialize_system(self, bot=None, allowed_chat_ids: list[int] | None = None) -> None:
        """Initialize queen system on startup - read key files and build context."""
        system_chat_id = 0  # Special chat_id for system initialization

        logger.info("Queen waking up - system initialization")

        self.start_background_tasks()

        wake_up_prompt = """You are waking up.

        Please read and familiarize yourself with:
        1. AGENTS.md, follow it's instructions.
        2. Check what worker templates are available using list_workers.

        Use fs_read to read these files. Use list_workers to see available workers.

        Build your internal understanding. When you're ready, respond with: "I'm ready. All systems operational." or you can rephrase the message to be more friendly.
        """

        # Store original send function
        original_send = self.internal_send

        # Set up send to allowed users if configured
        chat_ids = allowed_chat_ids or []
        if chat_ids and bot:
            logger.info("Queen will send initialization message to %d allowed chat(s): %s", len(chat_ids), chat_ids)
            # Create a send function that routes to all allowed chats
            async def send_to_allowed_chats(chat_id, text):
                # Send to all configured chat IDs
                for target_chat_id in chat_ids:
                    try:
                        await bot.send_message(chat_id=target_chat_id, text=text)
                        logger.debug("Sent initialization message to chat_id=%s", target_chat_id)
                    except Exception as e:
                        logger.warning("Failed to send to chat_id=%s: %s", target_chat_id, e)
            self.internal_send = send_to_allowed_chats
        else:
            # No allowed chats configured, silent wake up
            logger.warning("No ALLOWED_TELEGRAM_CHAT_IDS configured - queen will not send ready message. Set ALLOWED_TELEGRAM_CHAT_IDS in .env file (comma-separated chat IDs).")
            self.internal_send = None

        try:
            bootstrap_context = _get_bootstrap_context(self.store, system_chat_id)
            result = await _route_or_reply(
                self,
                self.provider,
                self.memory,
                wake_up_prompt,
                system_chat_id,
                bootstrap_context.content,
            )
            logger.info("Queen wake up complete: result=%s", result[:100] if result else "empty")

            # Send the ready message to allowed users
            if self.internal_send and result:
                try:
                    await self.internal_send(system_chat_id, result)
                    logger.info("Queen ready message sent successfully")
                except Exception as e:
                    logger.warning("Failed to send queen ready message: %s", e)
        finally:
            # Restore original send function
            self.internal_send = original_send

        # Note: We don't mark system as bootstrapped anymore - we want to wake up on every restart

    async def handle_message(
        self,
        text: str,
        chat_id: int,
        approval_requester=None,
    ) -> "QueenReply":
        logger.info("Queen received message: chat_id=%s len=%s", chat_id, len(text))

        await self.memory.add_message("user", text, {"chat_id": chat_id})
        bootstrap_context = _get_bootstrap_context(self.store, chat_id)
        if bootstrap_context.files:
            files_summary = ", ".join([f"{name} ({size} chars)" for name, size in bootstrap_context.files])
            logger.debug("Queen bootstrap files: %s", files_summary)
            logger.debug("Queen bootstrap hash: %s", bootstrap_context.hash)
            if bootstrap_context.content:
                logger.debug("Queen bootstrap injected: yes")
            else:
                logger.debug("Queen bootstrap injected: no (unchanged)")
        reply_text = await _route_or_reply(
            self,
            self.provider,
            self.memory,
            text,
            chat_id,
            bootstrap_context.content,
        )
        logger.info("Queen response ready")
        await self.memory.add_message("assistant", reply_text, {"chat_id": chat_id})
        if bootstrap_context.hash:
            self.store.set_chat_bootstrap_hash(chat_id, bootstrap_context.hash, utc_now())
        return QueenReply(immediate=_normalize_plain_text(reply_text), followup=None)

    async def _run_worker(
        self,
        worker_id: str,
        task: str,
        chat_id: int,
        inputs: dict[str, Any] | None,
        tools: list[str] | None,
        timeout_seconds: int | None,
    ) -> WorkerResult:
        """Run a worker task synchronously."""
        task_request = TaskRequest(
            worker_id=worker_id,
            task=task,
            inputs=inputs or {},
            tools=tools,
            timeout_seconds=timeout_seconds,
        )
        result = await self.runtime.run_task(task_request)
        logger.info(
            "Queen worker completed: worker_id=%s summary_len=%s questions=%s",
            worker_id,
            len(result.summary),
            len(result.questions),
        )
        return result

    def _start_worker_async(
        self,
        worker_id: str,
        task: str,
        chat_id: int,
        inputs: dict[str, Any] | None,
        tools: list[str] | None,
        timeout_seconds: int | None,
    ) -> str:
        """Start a worker task asynchronously."""
        task_request = TaskRequest(
            worker_id=worker_id,
            task=task,
            inputs=inputs or {},
            tools=tools,
            timeout_seconds=timeout_seconds,
        )
        run_id = str(uuid4())

        async def _runner() -> None:
            try:
                result = await self.runtime.run_task(task_request)
            except Exception as exc:
                result = WorkerResult(
                    summary=f"Worker error: {exc}",
                    output={"error": str(exc)},
                )
            _enqueue_internal_result(self, chat_id, task, result)

        asyncio.create_task(_runner())
        return run_id

    async def _run_and_compose_reply(
        self,
        task: str,
        chat_id: int,
        permissions: dict[str, bool] | None = None,
        lifecycle: str | None = None,
        worker_files: dict[str, str] | None = None,
    ) -> str:
        logger.info("Queen awaiting worker result")
        try:
            result = await self._run_worker(
                task,
                chat_id,
                permissions,
                lifecycle,
                worker_files,
            )
        except Exception as exc:
            logger.exception("Worker execution failed")
            result = WorkerResult(summary=f"Worker error: {exc}", output={"error": str(exc)}, evidence=[])
        logger.info("Queen queuing worker result for internal processing")
        _enqueue_internal_result(self, chat_id, task, result)
        return ""


@dataclass
class QueenReply:
    immediate: str
    followup: "asyncio.Task[str] | None"


@dataclass
class BootstrapContext:
    content: str
    hash: str
    files: list[tuple[str, int]]


def _format_worker_result(result: WorkerResult) -> str:
    summary = _normalize_plain_text(result.summary)
    lines = [f"Worker summary: {summary}"]
    if result.intents_executed:
        lines.append(f"Intents executed: {len(result.intents_executed)}")
    if result.evidence:
        evidence = result.evidence[0]
        snippet = _normalize_plain_text(str(evidence.content))[:200] if evidence.content is not None else ""
        if snippet:
            lines.append(f"Evidence: {snippet}")
    return "\n".join(lines)


QUEEN_SYSTEM_PROMPT = ""


def _build_worker_prompt(
    task: str,
    granted_capabilities: list[Capability],
    task_instructions: str,
) -> str:
    permissions = _format_permissions(granted_capabilities)
    worker_prompt = _load_prompt("worker_system.md")
    template = (
        f"{worker_prompt}\n\n"
        f"{_current_datetime_prompt()}\n\n"
        "Permissions\n"
        f"{permissions}\n\n"
        "Task\n"
        f"{task}\n\n"
        "Task-Specific Instructions\n"
        f"{task_instructions}\n"
    )
    return template


def _format_capabilities(capabilities: list[Capability]) -> str:
    if not capabilities:
        return "none"
    lines = []
    for cap in capabilities:
        lines.append(f"- {cap.type}: {cap.scope} (read_only={cap.read_only})")
    return "\n".join(lines)


def _format_permissions(capabilities: list[Capability]) -> str:
    flags = {
        "network": False,
        "filesystem_read": False,
        "filesystem_write": False,
        "exec": False,
        "email": False,
        "payment": False,
    }
    for cap in capabilities:
        if cap.type == "network":
            flags["network"] = True
        elif cap.type == "filesystem":
            flags["filesystem_read"] = True
            if not cap.read_only:
                flags["filesystem_write"] = True
        elif cap.type == "exec":
            flags["exec"] = True
        elif cap.type == "email":
            flags["email"] = True
        elif cap.type == "payment":
            flags["payment"] = True
    return "\n".join([f"- {key}: {str(value).lower()}" for key, value in flags.items()])


def _normalize_plain_text(text: str) -> str:
    cleaned = text.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = cleaned.replace("* ", "- ")
    cleaned = cleaned.replace("**", "")
    cleaned = cleaned.replace("__", "")
    cleaned = cleaned.replace("`", "")
    cleaned = cleaned.replace("#", "")
    cleaned = cleaned.replace("> ", "")
    return cleaned.strip()


def _looks_like_tool_error(text: str) -> bool:
    lowered = text.lower()
    if text.startswith("Queen cannot read") or text.startswith("Queen can only write"):
        return True
    if lowered.startswith("unknown tool"):
        return True
    if lowered.startswith("fs_") and "error" in lowered:
        return True
    if "tool" in lowered and "error" in lowered:
        return True
    return " error" in lowered or "failed" in lowered


def _staged_skill_dir_from_path(path_value: str) -> Path | None:
    if not path_value:
        return None
    workspace = Path(os.getenv("BROODMIND_WORKSPACE_DIR", "workspace")).resolve()
    normalized = path_value.replace("\\", "/").lstrip("/")
    try:
        candidate = (workspace / normalized).resolve()
    except Exception:
        return None
    if "skills_staging" not in candidate.parts:
        return None
    parts = list(candidate.parts)
    try:
        idx = parts.index("skills_staging")
    except ValueError:
        return None
    if len(parts) <= idx + 1:
        return None
    return Path(*parts[: idx + 2])


def _cleanup_failed_staged_skills(staged_dirs: dict[Path, bool]) -> None:
    if not staged_dirs:
        return
    for staged_dir, existed in staged_dirs.items():
        if existed:
            continue
        try:
            if staged_dir.exists():
                import shutil

                shutil.rmtree(staged_dir, ignore_errors=True)
                logger.info("Cleaned up failed staged skill: %s", staged_dir)
        except Exception:
            logger.exception("Failed to cleanup staged skill: %s", staged_dir)


def _load_prompt(filename: str) -> str:
    prompt_path = Path(__file__).parent / "prompts" / filename
    try:
        return prompt_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return ""


def _get_queen_system_prompt() -> str:
    global QUEEN_SYSTEM_PROMPT
    if not QUEEN_SYSTEM_PROMPT:
        QUEEN_SYSTEM_PROMPT = _load_prompt("queen_system.md")
    return QUEEN_SYSTEM_PROMPT


def _get_queen_section(title: str) -> str:
    content = _get_queen_system_prompt()
    if not content:
        return ""
    marker = f"## {title}"
    start = content.find(marker)
    if start == -1:
        return ""
    start = start + len(marker)
    end = content.find("\n## ", start)
    section = content[start:end] if end != -1 else content[start:]
    section = section.strip()
    if not section:
        return ""
    return f"{title}:\n{section}"


def _get_persona_prompt() -> str:
    workspace = os.getenv("BROODMIND_WORKSPACE_DIR", "workspace")
    persona_path = Path(workspace) / "PERSONA.MD"
    if not persona_path.exists():
        return ""
    try:
        return persona_path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _get_bootstrap_context(store: Store, chat_id: int) -> BootstrapContext:
    workspace = Path(os.getenv("BROODMIND_WORKSPACE_DIR", "workspace")).resolve()
    memory_dir = workspace / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    memory_files = [
        memory_dir / f"{today.isoformat()}.md",
        memory_dir / f"{yesterday.isoformat()}.md",
    ]
    for path in memory_files:
        if not path.exists():
            path.write_text("", encoding="utf-8")

    required_files = [
        workspace / "AGENTS.md",
        workspace / "USER.md",
        workspace / "skills" / "registry.json",
    ]
    optional_files = [
        workspace / "HEARTBEAT.md",
        workspace / "MEMORY.md",
    ]

    file_entries: list[tuple[str, str]] = []
    for path in required_files:
        if not path.exists():
            continue
        content = path.read_text(encoding="utf-8")
        file_entries.append((path.name, content))

    for path in optional_files:
        if not path.exists():
            continue
        content = path.read_text(encoding="utf-8")
        if content.strip():
            file_entries.append((path.name, content))

    for path in memory_files:
        content = path.read_text(encoding="utf-8")
        rel = path.relative_to(workspace).as_posix()
        file_entries.append((rel, content))

    if not file_entries:
        return BootstrapContext(content="", hash="", files=[])

    bundle_hash = hashlib.sha256()
    for name, content in file_entries:
        bundle_hash.update(name.encode("utf-8"))
        bundle_hash.update(b"\n")
        bundle_hash.update(content.encode("utf-8"))
        bundle_hash.update(b"\n")
    hash_value = bundle_hash.hexdigest()
    files_with_sizes = [(name, len(content)) for name, content in file_entries]

    parts = ["<workspace>"]
    for name, content in file_entries:
        parts.append(f"<file name=\"{name}\">")
        parts.append(content)
        parts.append("</file>")
    parts.append("</workspace>")
    content = "\n".join(parts)
    return BootstrapContext(content=content, hash=hash_value, files=files_with_sizes)


def _read_workspace_file(filename: str, max_chars: int) -> str:
    workspace = os.getenv("BROODMIND_WORKSPACE_DIR", "workspace")
    target = Path(workspace) / filename
    if not target.exists():
        return ""
    try:
        data = target.read_text(encoding="utf-8")
    except Exception:
        return ""
    if len(data) <= max_chars:
        return data.strip()
    return data[:max_chars].rstrip() + "\n...[truncated]"





def _workers_root() -> Path:
    workspace = os.getenv("BROODMIND_WORKSPACE_DIR", "workspace")
    return Path(workspace) / "workers"


def _workers_registry_path() -> Path:
    return _workers_root() / "registry.json"


def _get_workers_registry() -> list[dict]:
    registry_path = _workers_registry_path()
    if not registry_path.exists():
        return []
    try:
        import json

        return json.loads(registry_path.read_text(encoding="utf-8"))
    except Exception:
        return []


def _format_workers_registry(entries: list[dict]) -> str:
    if not entries:
        return "No workers registered."
    lines = ["Workers:"]
    for entry in entries:
        wid = entry.get("id", "unknown")
        module = entry.get("module", "unknown")
        task = entry.get("task", "unknown")
        last_used = entry.get("last_used_at", "unknown")
        lifecycle = entry.get("lifecycle", "unknown")
        status = entry.get("status", "unknown")
        lines.append(
            f"- {wid} | {module} | {task} | lifecycle={lifecycle} | status={status} | last_used={last_used}"
        )
    return "\n".join(lines)


def _workers_registry_note() -> str:
    entries = _get_workers_registry()
    if not entries:
        return ""
    summary = ", ".join([e.get("id", "unknown") for e in entries[:5]])
    more = ""
    if len(entries) > 5:
        more = f" (+{len(entries) - 5} more)"
    return (
        "Workers registry available. IDs: "
        f"{summary}{more}. Registry path: {str(_workers_registry_path())}"
    )




async def _route_or_reply(
    queen: Queen,
    provider: InferenceProvider,
    memory: MemoryService,
    user_text: str,
    chat_id: int,
    bootstrap_context: str,
) -> str:
    memory_context = await memory.get_context(user_text)
    recent_history = memory.get_recent_history(chat_id, limit=8)
    if recent_history and recent_history[-1][0] == "user" and recent_history[-1][1] == user_text:
        recent_history = recent_history[:-1]
    messages = [Message(role="system", content=_get_queen_system_prompt())]
    persona = _get_persona_prompt()
    if persona:
        messages.append(Message(role="system", content=f"<persona>\n{persona}\n</persona>"))
    if bootstrap_context:
        messages.append(Message(role="system", content=bootstrap_context))
    messages.append(Message(role="system", content=_current_datetime_prompt()))
    messages.append(Message(role="system", content=_language_instruction(user_text)))
    registry_note = _workers_registry_note()
    if registry_note:
        messages.append(Message(role="system", content=registry_note))
    route_prompt = _get_queen_section("Route Instructions")
    if route_prompt:
        messages.append(Message(role="system", content=route_prompt))
    if memory_context:
        messages.append(
            Message(
                role="system",
                content="<context>\n" + "\n".join(memory_context) + "\n</context>",
            )
        )
    if recent_history:
        for role, content in recent_history:
            if role == "user":
                messages.append(Message(role="user", content=content))
            elif role == "assistant":
                messages.append(Message(role="assistant", content=content))
    messages.append(Message(role="user", content=user_text))
    _log_system_prompt(messages, "route")
    queen_tools, ctx = _get_queen_tools(queen, chat_id)
    tool_capable = getattr(provider, "complete_with_tools", None)
    if callable(tool_capable):
        tools = [spec.to_openai_tool() for spec in queen_tools]
        last_error: str | None = None
        staged_dirs: dict[Path, bool] = {}
        max_attempts = 10
        for _ in range(max_attempts):
            result = await provider.complete_with_tools(messages, tools=tools, tool_choice="auto")
            content_raw = result.get("content", "")
            tool_calls = result.get("tool_calls") or []
            if tool_calls:
                for call in tool_calls:
                    function = call.get("function") or {}
                    name = function.get("name")
                    args_raw = function.get("arguments", "{}")
                    args: dict[str, object] = {}
                    if isinstance(args_raw, str):
                        try:
                            args = json.loads(args_raw)
                        except Exception:
                            args = {}
                    if name == "fs_write":
                        path_value = str(args.get("path", "")).strip()
                        staged_dir = _staged_skill_dir_from_path(path_value)
                        if staged_dir and staged_dir not in staged_dirs:
                            staged_dirs[staged_dir] = staged_dir.exists()
                    tool_result = await _handle_queen_tool_call(call, queen_tools, ctx)
                    tool_result_lower = tool_result.lower()
                    tool_failed = (
                        tool_result.startswith("Queen cannot read")
                        or tool_result.startswith("Queen can only write")
                        or tool_result.startswith("fs_read error:")
                        or tool_result.startswith("fs_list error:")
                        or tool_result.startswith("fs_write error:")
                        or tool_result.startswith("fs_move error:")
                        or tool_result.startswith("fs_delete error:")
                        or tool_result_lower.startswith("unknown tool")
                        or " error" in tool_result_lower
                        or "failed" in tool_result_lower
                    )
                    if tool_failed:
                        last_error = tool_result
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": call.get("id"),
                            "content": tool_result,
                        }
                    )
                if not last_error:
                    last_error = (
                        "No tool call completed. Use list_workers to see available worker templates, "
                        "then call start_worker with a worker_id and task description."
                    )
                continue
            if content_raw:
                logger.debug("Queen route-or-reply output: %s", _truncate_for_log(content_raw))
            content = _normalize_plain_text(content_raw)
            return content
        if last_error and _looks_like_tool_error(last_error):
            _cleanup_failed_staged_skills(staged_dirs)
            return "I couldn't complete that request. The tooling failed and needs correction."
        _cleanup_failed_staged_skills(staged_dirs)
        return last_error or ""
    response_raw = await provider.complete(messages)
    logger.debug("Queen route-or-reply output: %s", _truncate_for_log(response_raw))
    response = _normalize_plain_text(response_raw)
    return response


async def _draft_interim_reply(
    provider: InferenceProvider,
    memory: MemoryService,
    user_text: str,
    chat_id: int,
    bootstrap_context: str,
) -> str:
    memory_context = await memory.get_context(user_text)
    recent_history = memory.get_recent_history(chat_id, limit=6)
    if recent_history and recent_history[-1][0] == "user" and recent_history[-1][1] == user_text:
        recent_history = recent_history[:-1]
    messages = [Message(role="system", content=_get_queen_system_prompt())]
    persona = _get_persona_prompt()
    if persona:
        messages.append(Message(role="system", content=f"<persona>\n{persona}\n</persona>"))
    if bootstrap_context:
        messages.append(Message(role="system", content=bootstrap_context))
    messages.append(Message(role="system", content=_current_datetime_prompt()))
    messages.append(Message(role="system", content=_language_instruction(user_text)))
    registry_note = _workers_registry_note()
    if registry_note:
        messages.append(Message(role="system", content=registry_note))
    interim_prompt = _get_queen_section("Interim Reply Instructions")
    if interim_prompt:
        messages.append(Message(role="system", content=interim_prompt))
    if memory_context:
        messages.append(
            Message(
                role="system",
                content="<context>\n" + "\n".join(memory_context) + "\n</context>",
            )
        )
    if recent_history:
        for role, content in recent_history:
            if role == "user":
                messages.append(Message(role="user", content=content))
            elif role == "assistant":
                messages.append(Message(role="assistant", content=content))
    messages.append(Message(role="user", content=user_text))
    _log_system_prompt(messages, "interim")
    response = await provider.complete(messages)
    logger.debug("Queen interim reply: %s", _truncate_for_log(response))
    cleaned = _normalize_plain_text(response)
    return cleaned


async def _compose_user_reply(
    provider: InferenceProvider,
    memory: MemoryService,
    user_text: str,
    chat_id: int,
    result: WorkerResult,
) -> str:
    import json

    direct_error = _format_worker_error(result)
    if direct_error:
        return direct_error

    worker_payload = json.dumps(result.model_dump(mode="json"), ensure_ascii=False)
    memory_context = await memory.get_context(user_text)
    recent_history = memory.get_recent_history(chat_id, limit=8)
    if recent_history and recent_history[-1][0] == "user" and recent_history[-1][1] == user_text:
        recent_history = recent_history[:-1]
    messages = [
        Message(role="system", content=_get_queen_system_prompt()),
        Message(role="system", content=_current_datetime_prompt()),
        Message(role="system", content=_language_instruction(user_text)),
        Message(role="system", content=_get_queen_section("Followup Reply Instructions")),
    ]
    persona = _get_persona_prompt()
    if persona:
        messages.insert(1, Message(role="system", content=f"<persona>\n{persona}\n</persona>"))
    registry_note = _workers_registry_note()
    if registry_note:
        messages.append(Message(role="system", content=registry_note))
    if memory_context:
        messages.append(
            Message(
                role="system",
                content="<context>\n" + "\n".join(memory_context) + "\n</context>",
            )
        )
    if recent_history:
        for role, content in recent_history:
            if role == "user":
                messages.append(Message(role="user", content=content))
            elif role == "assistant":
                messages.append(Message(role="assistant", content=content))
    messages.append(Message(role="system", content=f"<worker_result>\n{worker_payload}\n</worker_result>"))
    messages.append(Message(role="user", content=user_text))
    _log_system_prompt(messages, "followup")
    response = await provider.complete(messages)
    logger.debug("Queen followup reply: %s", _truncate_for_log(response))
    return _normalize_plain_text(response)


def _format_worker_error(result: WorkerResult) -> str | None:
    summary = _normalize_plain_text(result.summary or "")
    output = result.output
    if isinstance(output, dict) and output.get("error"):
        return f"Worker error: {_normalize_plain_text(str(output.get('error')))}"
    if summary:
        lower = summary.lower()
        error_markers = (
            "missing required permissions",
            "output schema validation failed",
            "worker execution produced no output",
            "worker returned non-json output",
            "worker tool loop exceeded iteration limit",
            "runner failed",
        )
        if any(marker in lower for marker in error_markers):
            return f"Worker error: {summary}"
    return None


def _language_instruction(text: str) -> str:
    if _contains_cyrillic(text):
        return (
            "Respond in Russian. Use plain text only (no markdown, no tables). "
            "Do not switch languages mid-conversation."
        )
    return (
        "Respond in English. Use plain text only (no markdown, no tables). "
        "Do not switch languages mid-conversation."
    )


def _contains_cyrillic(text: str) -> bool:
    return any("а" <= ch.lower() <= "я" for ch in text)


def _current_datetime_prompt() -> str:
    now = datetime.now().astimezone()
    return f"Current date/time: {now.isoformat()}"




def _truncate_for_log(text: str, limit: int = 1200) -> str:
    if text is None:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + "...[truncated]"


def _log_system_prompt(messages: list, label: str) -> None:
    system_lengths: list[int] = []
    for msg in messages:
        role = None
        content = ""
        if hasattr(msg, "role"):
            role = getattr(msg, "role")
            content = getattr(msg, "content", "")
        elif isinstance(msg, dict):
            role = msg.get("role")
            content = msg.get("content", "")
        if role == "system":
            system_lengths.append(len(content))
    if system_lengths:
        total = sum(system_lengths)
        logger.debug(
            "Queen %s system prompt: parts=%s total_chars=%s",
            label,
            len(system_lengths),
            total,
        )


def _bundle_hash(worker_files: dict[str, str]) -> str:
    digest = hashlib.sha256()
    for name in sorted(worker_files.keys()):
        digest.update(name.encode("utf-8"))
        digest.update(b"\n")
        digest.update(worker_files[name].encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _normalize_required_permissions(worker_files: dict[str, str]) -> dict[str, str]:
    raw = worker_files.get("skill.json")
    if not raw:
        return worker_files
    try:
        payload = json.loads(raw)
    except Exception:
        return worker_files
    perms = payload.get("required_permissions")
    if isinstance(perms, list):
        perms = {str(key): True for key in perms}
        payload["required_permissions"] = perms
    if not isinstance(perms, dict):
        return worker_files
    updated = False
    allowed = {"network", "filesystem_read", "filesystem_write", "exec"}
    extra = [key for key in perms.keys() if key not in allowed]
    for key in extra:
        perms.pop(key, None)
        updated = True
    for key in ("network", "filesystem_read", "filesystem_write", "exec"):
        if key not in perms:
            perms[key] = False
            updated = True
    if updated:
        payload["required_permissions"] = perms
        worker_files["skill.json"] = json.dumps(payload, ensure_ascii=False, indent=2)
        logger.warning("Normalized required_permissions (removed unknown keys, added missing keys).")
    return worker_files







def _expand_worker_files(worker_files: dict[str, str]) -> dict[str, str]:
    workspace = Path(os.getenv("BROODMIND_WORKSPACE_DIR", "workspace")).resolve()
    expanded: dict[str, str] = {}
    for name, value in worker_files.items():
        if not isinstance(value, str):
            expanded[name] = str(value)
            continue
        # If the value looks like full file content (multiline or very long), keep as-is.
        trimmed = value.strip()
        if "\n" in value or len(value) > 300:
            expanded[name] = value
            continue
        # If the value looks like inline JSON content, keep as-is.
        if (trimmed.startswith("{") and trimmed.endswith("}")) or (
            trimmed.startswith("[") and trimmed.endswith("]")
        ):
            expanded[name] = value
            continue
        candidate = value.replace("\\", "/").lstrip("/")
        path = (workspace / candidate).resolve()
        if path.exists() and path.is_file():
            try:
                expanded[name] = path.read_text(encoding="utf-8")
                continue
            except Exception:
                pass
        expanded[name] = value
    return expanded


def _get_queen_tools(queen: Queen, chat_id: int) -> tuple[list[ToolSpec], dict[str, object]]:
    perms = {"filesystem_read": True, "filesystem_write": True, "worker_manage": True}
    ctx = {
        "base_dir": Path(os.getenv("BROODMIND_WORKSPACE_DIR", "workspace")).resolve(),
        "queen": queen,
        "chat_id": chat_id,
    }
    tool_specs = filter_tools(get_tools(), permissions=perms)
    tool_specs.extend(_worker_tool_specs())
    return tool_specs, ctx


def _worker_tool_specs() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="list_workers",
            description="List available worker templates with their capabilities.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False},
            permission="worker_manage",
            handler=_tool_list_workers,
        ),
        ToolSpec(
            name="start_worker",
            description="Start a worker task with the specified worker template. Returns run_id and status.",
            parameters={
                "type": "object",
                "properties": {
                    "worker_id": {
                        "type": "string",
                        "description": "ID of the worker template to use (e.g., 'web_researcher', 'web_fetcher'). Use list_workers to see available workers.",
                    },
                    "task": {
                        "type": "string",
                        "description": "Natural language task description for the worker.",
                    },
                    "inputs": {
                        "type": "object",
                        "description": "Task-specific input data.",
                        "additionalProperties": True,
                    },
                    "tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Override default tools for this task (optional).",
                    },
                    "timeout_seconds": {
                        "type": "number",
                        "description": "Override default timeout (optional).",
                    },
                },
                "required": ["worker_id", "task"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_start_worker,
        ),
        ToolSpec(
            name="stop_worker",
            description="Stop a running worker by worker_id.",
            parameters={
                "type": "object",
                "properties": {"worker_id": {"type": "string"}},
                "required": ["worker_id"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_stop_worker,
        ),
        ToolSpec(
            name="get_worker_status",
            description="Get the current status and details of a specific worker by ID.",
            parameters={
                "type": "object",
                "properties": {
                    "worker_id": {
                        "type": "string",
                        "description": "The worker ID to check.",
                    }
                },
                "required": ["worker_id"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_get_worker_status,
        ),
        ToolSpec(
            name="list_active_workers",
            description="List all active workers (running or completed in the last 10 minutes).",
            parameters={
                "type": "object",
                "properties": {
                    "older_than_minutes": {
                        "type": "number",
                        "description": "Include workers updated in the last N minutes (default: 10).",
                    }
                },
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_list_active_workers,
        ),
        ToolSpec(
            name="get_worker_result",
            description="Get the result/output of a completed worker by ID.",
            parameters={
                "type": "object",
                "properties": {
                    "worker_id": {
                        "type": "string",
                        "description": "The worker ID to get results from.",
                    }
                },
                "required": ["worker_id"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_get_worker_result,
        ),
        ToolSpec(
            name="create_worker_template",
            description="Create a new worker template in the database.",
            parameters={
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Unique worker ID (e.g., 'my_researcher'). Use lowercase with underscores.",
                    },
                    "name": {
                        "type": "string",
                        "description": "Human-readable name (e.g., 'My Researcher').",
                    },
                    "description": {
                        "type": "string",
                        "description": "What this worker does.",
                    },
                    "system_prompt": {
                        "type": "string",
                        "description": "Worker's personality, purpose, and instructions.",
                    },
                    "available_tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Tool names this worker can use (e.g., ['web_search', 'web_fetch']).",
                    },
                    "required_permissions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Permissions needed: 'network', 'filesystem_read', 'filesystem_write', 'exec'.",
                    },
                    "max_thinking_steps": {
                        "type": "number",
                        "description": "Max reasoning iterations (default: 10).",
                    },
                    "default_timeout_seconds": {
                        "type": "number",
                        "description": "Default timeout in seconds (default: 300).",
                    },
                },
                "required": ["id", "name", "description", "system_prompt"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_create_worker_template,
        ),
        ToolSpec(
            name="update_worker_template",
            description="Update an existing worker template.",
            parameters={
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Worker ID to update.",
                    },
                    "name": {"type": "string", "description": "New name (optional)."},
                    "description": {"type": "string", "description": "New description (optional)."},
                    "system_prompt": {"type": "string", "description": "New system prompt (optional)."},
                    "available_tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "New tool list (optional).",
                    },
                    "required_permissions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "New permissions (optional).",
                    },
                    "max_thinking_steps": {"type": "number", "description": "New max steps (optional)."},
                    "default_timeout_seconds": {"type": "number", "description": "New timeout (optional)."},
                },
                "required": ["id"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_update_worker_template,
        ),
        ToolSpec(
            name="delete_worker_template",
            description="Delete a worker template from the database.",
            parameters={
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Worker ID to delete.",
                    }
                },
                "required": ["id"],
                "additionalProperties": False,
            },
            permission="worker_manage",
            handler=_tool_delete_worker_template,
        ),
    ]


def _tool_list_workers(args: dict[str, object], ctx: dict[str, object]) -> str:
    """List available worker templates."""
    queen: Queen = ctx["queen"]

    templates = queen.store.list_worker_templates()
    template_list = []
    for t in templates:
        template_list.append({
            "worker_id": t.id,
            "name": t.name,
            "description": t.description,
            "available_tools": t.available_tools,
            "required_permissions": t.required_permissions,
            "default_timeout_seconds": t.default_timeout_seconds,
        })

    return json.dumps({
        "count": len(template_list),
        "workers": template_list,
    }, ensure_ascii=False)


def _tool_create_worker_template(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Create a new worker template in the database."""
    from datetime import datetime, timezone
    
    queen: Queen = ctx["queen"]
    
    worker_id = str(args.get("id", "")).strip()
    name = str(args.get("name", "")).strip()
    description = str(args.get("description", "")).strip()
    system_prompt = str(args.get("system_prompt", "")).strip()
    
    if not worker_id:
        return "create_worker_template error: id is required."
    if not name:
        return "create_worker_template error: name is required."
    if not description:
        return "create_worker_template error: description is required."
    if not system_prompt:
        return "create_worker_template error: system_prompt is required."
    
    # Check if worker already exists
    existing = queen.store.get_worker_template(worker_id)
    if existing:
        return f"create_worker_template error: worker '{worker_id}' already exists. Use update_worker_template to modify it."
    
    # Get optional parameters with defaults
    available_tools = args.get("available_tools") if isinstance(args.get("available_tools"), list) else []
    required_permissions = args.get("required_permissions") if isinstance(args.get("required_permissions"), list) else []
    max_thinking_steps = int(args.get("max_thinking_steps")) if args.get("max_thinking_steps") else 10
    default_timeout_seconds = int(args.get("default_timeout_seconds")) if args.get("default_timeout_seconds") else 300
    
    # Create worker template record
    now = datetime.now(timezone.utc)
    from broodmind.store.models import WorkerTemplateRecord
    record = WorkerTemplateRecord(
        id=worker_id,
        name=name,
        description=description,
        system_prompt=system_prompt,
        available_tools=available_tools,
        required_permissions=required_permissions,
        max_thinking_steps=max_thinking_steps,
        default_timeout_seconds=default_timeout_seconds,
        created_at=now,
        updated_at=now,
    )
    
    # Save to store
    queen.store.upsert_worker_template(record)
    
    return json.dumps({
        "status": "created",
        "worker_id": worker_id,
        "name": name,
        "description": description,
        "available_tools": available_tools,
        "required_permissions": required_permissions,
        "message": f"Worker template '{name}' created successfully."
    }, ensure_ascii=False)


def _tool_update_worker_template(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Update an existing worker template."""
    from datetime import datetime, timezone
    
    queen: Queen = ctx["queen"]
    
    worker_id = str(args.get("id", "")).strip()
    if not worker_id:
        return "update_worker_template error: id is required."
    
    # Get existing template
    existing = queen.store.get_worker_template(worker_id)
    if not existing:
        return f"update_worker_template error: worker '{worker_id}' not found. Use create_worker_template to create it."
    
    # Update fields if provided
    name = str(args.get("name", existing.name)).strip() if args.get("name") else existing.name
    description = str(args.get("description", existing.description)).strip() if args.get("description") else existing.description
    system_prompt = str(args.get("system_prompt", existing.system_prompt)).strip() if args.get("system_prompt") else existing.system_prompt
    available_tools = args.get("available_tools") if isinstance(args.get("available_tools"), list) else existing.available_tools
    required_permissions = args.get("required_permissions") if isinstance(args.get("required_permissions"), list) else existing.required_permissions
    max_thinking_steps = int(args.get("max_thinking_steps")) if args.get("max_thinking_steps") else existing.max_thinking_steps
    default_timeout_seconds = int(args.get("default_timeout_seconds")) if args.get("default_timeout_seconds") else existing.default_timeout_seconds
    
    # Create updated record
    now = datetime.now(timezone.utc)
    from broodmind.store.models import WorkerTemplateRecord
    record = WorkerTemplateRecord(
        id=worker_id,
        name=name,
        description=description,
        system_prompt=system_prompt,
        available_tools=available_tools,
        required_permissions=required_permissions,
        max_thinking_steps=max_thinking_steps,
        default_timeout_seconds=default_timeout_seconds,
        created_at=existing.created_at,
        updated_at=now,
    )
    
    # Save to store
    queen.store.upsert_worker_template(record)
    
    return json.dumps({
        "status": "updated",
        "worker_id": worker_id,
        "name": name,
        "description": description,
        "message": f"Worker template '{name}' updated successfully."
    }, ensure_ascii=False)


def _tool_delete_worker_template(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Delete a worker template from the database."""
    queen: Queen = ctx["queen"]
    
    worker_id = str(args.get("id", "")).strip()
    if not worker_id:
        return "delete_worker_template error: id is required."
    
    # Check if worker exists
    existing = queen.store.get_worker_template(worker_id)
    if not existing:
        return f"delete_worker_template error: worker '{worker_id}' not found."
    
    # Delete from store (need to add this method to store)
    try:
        queen.store.delete_worker_template(worker_id)
        return json.dumps({
            "status": "deleted",
            "worker_id": worker_id,
            "name": existing.name,
            "message": f"Worker template '{existing.name}' deleted successfully."
        }, ensure_ascii=False)
    except NotImplementedError:
        return f"delete_worker_template error: delete_worker_template method not yet implemented in store."
    except Exception as e:
        return f"delete_worker_template error: {str(e)}"


def _tool_start_worker(args: dict[str, object], ctx: dict[str, object]) -> str:
    """Start a worker task with the specified worker template."""
    queen: Queen = ctx["queen"]
    chat_id = int(ctx.get("chat_id") or 0)

    worker_id = str(args.get("worker_id", "")).strip()
    task = str(args.get("task", "")).strip()
    inputs = args.get("inputs") if isinstance(args.get("inputs"), dict) else {}
    tools = args.get("tools") if isinstance(args.get("tools"), list) else None
    timeout_seconds = int(args.get("timeout_seconds")) if args.get("timeout_seconds") else None

    if not worker_id:
        return "start_worker error: worker_id is required. Use list_worker_templates to see available workers."
    if not task:
        return "start_worker error: task is required."

    # Verify worker template exists
    template = queen.store.get_worker_template(worker_id)
    if not template:
        return f"start_worker error: worker '{worker_id}' not found. Use list_worker_templates to see available workers."

    run_id = queen._start_worker_async(
        worker_id=worker_id,
        task=task,
        chat_id=chat_id,
        inputs=inputs,
        tools=tools,
        timeout_seconds=timeout_seconds,
    )
    return json.dumps({
        "status": "started",
        "worker_id": worker_id,
        "run_id": run_id,
        "message": f"Worker '{template.name}' started. Use get_worker_status to check progress."
    }, ensure_ascii=False)


def _tool_stop_worker(args: dict[str, object], ctx: dict[str, object]) -> str:
    queen: Queen = ctx["queen"]
    worker_id = str(args.get("worker_id", "")).strip()
    if not worker_id:
        return "stop_worker error: worker_id is required."
    stopped = queen.runtime.stop_worker(worker_id)
    return json.dumps({"status": "stopped" if stopped else "not_found", "worker_id": worker_id}, ensure_ascii=False)


def _tool_get_worker_status(args: dict[str, object], ctx: dict[str, object]) -> str:
    queen: Queen = ctx["queen"]
    worker_id = str(args.get("worker_id", "")).strip()
    if not worker_id:
        return "get_worker_status error: worker_id is required."

    worker = queen.store.get_worker(worker_id)
    if not worker:
        return json.dumps({
            "status": "not_found",
            "worker_id": worker_id,
            "message": "Worker not found in database. It may be from an old conversation or never existed."
        }, ensure_ascii=False)

    return json.dumps({
        "status": worker.status,
        "worker_id": worker.id,
        "task": worker.task,
        "created_at": worker.created_at.isoformat(),
        "updated_at": worker.updated_at.isoformat(),
        "summary": worker.summary,
        "error": worker.error,
    }, ensure_ascii=False)


def _tool_list_active_workers(args: dict[str, object], ctx: dict[str, object]) -> str:
    queen: Queen = ctx["queen"]
    older_than_minutes = int(args.get("older_than_minutes") or 10)

    workers = queen.store.get_active_workers(older_than_minutes=older_than_minutes)
    worker_list = []
    for w in workers:
        worker_list.append({
            "worker_id": w.id,
            "status": w.status,
            "task": w.task,
            "created_at": w.created_at.isoformat(),
            "updated_at": w.updated_at.isoformat(),
            "summary": w.summary,
            "error": w.error,
        })

    return json.dumps({
        "count": len(worker_list),
        "workers": worker_list,
    }, ensure_ascii=False)


def _tool_get_worker_result(args: dict[str, object], ctx: dict[str, object]) -> str:
    queen: Queen = ctx["queen"]
    worker_id = str(args.get("worker_id", "")).strip()
    if not worker_id:
        return "get_worker_result error: worker_id is required."

    worker = queen.store.get_worker(worker_id)
    if not worker:
        return json.dumps({
            "status": "not_found",
            "worker_id": worker_id,
            "message": "Worker not found in database."
        }, ensure_ascii=False)

    if worker.status == "completed":
        return json.dumps({
            "status": "completed",
            "worker_id": worker.id,
            "summary": worker.summary,
            "output": worker.output,
        }, ensure_ascii=False)
    elif worker.status == "failed":
        return json.dumps({
            "status": "failed",
            "worker_id": worker.id,
            "error": worker.error or "Unknown error",
        }, ensure_ascii=False)
    else:
        return json.dumps({
            "status": worker.status,
            "worker_id": worker.id,
            "message": f"Worker is still {worker.status}. Result not available yet.",
        }, ensure_ascii=False)


async def _handle_queen_tool_call(
    call: dict,
    tools: list[ToolSpec],
    ctx: dict[str, object],
) -> str:
    function = call.get("function") or {}
    name = function.get("name")
    args_raw = function.get("arguments", "{}")
    try:
        import json

        args = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
    except Exception:
        args = {}
    logger.debug("Queen tool call: %s", name)
    for spec in tools:
        if spec.name == name:
            if spec.is_async:
                result = await spec.handler(args, ctx)
            else:
                result = spec.handler(args, ctx)

            if os.getenv("BROODMIND_DEBUG_PROMPTS", "").lower() in {"1", "true", "yes"}:
                logger.debug("Queen tool result %s: %s", name, _truncate_for_log(str(result)))
            return result
    return "Unknown tool."


def _capabilities_from_permissions(permissions: dict[str, bool] | None) -> list[Capability]:
    if not permissions:
        return []
    caps: list[Capability] = []
    if permissions.get("network"):
        caps.append(Capability(type="network", scope="*", read_only=True))
    if permissions.get("filesystem_read") or permissions.get("filesystem_write"):
        caps.append(
            Capability(
                type="filesystem",
                scope="/workspace/**",
                read_only=not bool(permissions.get("filesystem_write")),
            )
        )
    if permissions.get("exec"):
        caps.append(Capability(type="exec", scope="*", read_only=False))
    if permissions.get("email"):
        caps.append(Capability(type="email", scope="*", read_only=False))
    if permissions.get("payment"):
        caps.append(Capability(type="payment", scope="*", read_only=False))
    return caps
