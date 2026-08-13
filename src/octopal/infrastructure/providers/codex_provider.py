"""Codex CLI app-server backed inference provider."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import structlog

from octopal.infrastructure.config.models import LLMConfig
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.providers.base import Message
from octopal.infrastructure.providers.profile_resolver import resolve_litellm_profile

CODEX_CONTROL_REQUEST_TIMEOUT_SECONDS = 30.0
CODEX_TURN_EVENT_IDLE_TIMEOUT_SECONDS = 180.0
CODEX_TOOL_INTERRUPT_GRACE_SECONDS = 2.0
# Backward-compatible aliases for callers that imported the original constants.
CODEX_REQUEST_TIMEOUT_SECONDS = CODEX_CONTROL_REQUEST_TIMEOUT_SECONDS
CODEX_TURN_TIMEOUT_SECONDS = CODEX_TURN_EVENT_IDLE_TIMEOUT_SECONDS
CODEX_SESSION_TTL_DAYS = 30
CODEX_SESSION_STATE_VERSION = 1
CODEX_APP_SERVER_CAPACITY = 2
CODEX_BACKGROUND_CAPACITY = 1
CODEX_BACKGROUND_MAX_WAIT_SECONDS = 30.0
CODEX_INTERACTIVE_BURST_LIMIT = 4
CODEX_LANE_INTERACTIVE = "interactive"
CODEX_LANE_BACKGROUND = "background"

logger = structlog.get_logger(__name__)


class CodexAppServerError(RuntimeError):
    pass


class CodexToolResultTransportError(CodexAppServerError):
    """Transport ended after a tool may have taken effect; automatic replay is unsafe."""

    tool_execution_may_have_completed = True


_ToolExecutor = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class _CodexSession:
    thread_id: str
    config_fingerprint: str
    config_components: dict[str, str]
    message_fingerprints: tuple[str, ...]
    updated_at: datetime


@dataclass(frozen=True)
class _CodexSessionConfiguration:
    fingerprint: str
    components: dict[str, str]
    tool_count: int
    tool_digest: str


@dataclass(eq=False)
class _AdmissionTicket:
    lane: str
    queued_at: float


class _CodexAdmissionController:
    """Fair provider-local admission with one slot reserved from background work."""

    def __init__(
        self,
        *,
        capacity: int = CODEX_APP_SERVER_CAPACITY,
        background_capacity: int = CODEX_BACKGROUND_CAPACITY,
    ) -> None:
        self._capacity = capacity
        self._background_capacity = background_capacity
        self._condition = asyncio.Condition()
        self._interactive: deque[_AdmissionTicket] = deque()
        self._background: deque[_AdmissionTicket] = deque()
        self._active_total = 0
        self._active_background = 0
        self._interactive_streak = 0

    @contextlib.asynccontextmanager
    async def acquire(self, lane: str) -> Any:
        normalized_lane = _normalize_lane(lane)
        loop = asyncio.get_running_loop()
        ticket = _AdmissionTicket(normalized_lane, loop.time())
        queue = self._interactive if normalized_lane == CODEX_LANE_INTERACTIVE else self._background
        async with self._condition:
            queue.append(ticket)
            try:
                while not self._can_admit(ticket, now=loop.time()):
                    await self._condition.wait()
                queue.remove(ticket)
                self._active_total += 1
                if normalized_lane == CODEX_LANE_BACKGROUND:
                    self._active_background += 1
                    self._interactive_streak = 0
                else:
                    self._interactive_streak += 1
                self._condition.notify_all()
            except BaseException:
                with contextlib.suppress(ValueError):
                    queue.remove(ticket)
                self._condition.notify_all()
                raise

        try:
            yield {
                "queue_wait_ms": (loop.time() - ticket.queued_at) * 1000,
                "active_total": self._active_total,
                "active_background": self._active_background,
            }
        finally:
            async with self._condition:
                self._active_total -= 1
                if normalized_lane == CODEX_LANE_BACKGROUND:
                    self._active_background -= 1
                self._condition.notify_all()

    def _can_admit(self, ticket: _AdmissionTicket, *, now: float) -> bool:
        if self._active_total >= self._capacity:
            return False
        oldest_background = self._background[0] if self._background else None
        background_is_aged = bool(
            oldest_background
            and now - oldest_background.queued_at >= CODEX_BACKGROUND_MAX_WAIT_SECONDS
        )
        background_gets_fair_turn = bool(
            oldest_background
            and background_is_aged
            and self._interactive_streak >= CODEX_INTERACTIVE_BURST_LIMIT
            and self._active_background < self._background_capacity
        )
        if ticket.lane == CODEX_LANE_INTERACTIVE:
            return self._interactive[0] is ticket and not background_gets_fair_turn
        return bool(
            self._background[0] is ticket
            and self._active_background < self._background_capacity
            and (not self._interactive or background_gets_fair_turn)
        )


class _CodexSessionStore:
    """Small provider-local registry for resumable Codex thread IDs."""

    def __init__(self, state_dir: Path) -> None:
        self._path = state_dir / "codex_sessions.json"

    def get(self, session_ref: str) -> tuple[_CodexSession | None, str | None]:
        payload = self._read()
        raw = (payload.get("sessions") or {}).get(session_ref)
        if not isinstance(raw, dict):
            return None, None
        try:
            updated_at = datetime.fromisoformat(str(raw["updated_at"]))
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=UTC)
            raw_components = raw.get("config_components")
            config_components = (
                {str(key): str(value) for key, value in raw_components.items()}
                if isinstance(raw_components, dict)
                else {}
            )
            session = _CodexSession(
                thread_id=str(raw["thread_id"]),
                config_fingerprint=str(raw["config_fingerprint"]),
                config_components=config_components,
                message_fingerprints=tuple(
                    str(value) for value in (raw.get("message_fingerprints") or [])
                ),
                updated_at=updated_at,
            )
        except (KeyError, TypeError, ValueError):
            self.delete(session_ref)
            return None, "invalid_state"
        if datetime.now(UTC) - session.updated_at > timedelta(days=CODEX_SESSION_TTL_DAYS):
            self.delete(session_ref)
            return None, "expired"
        return session, None

    def put(self, session_ref: str, session: _CodexSession) -> None:
        payload = self._read()
        sessions = payload.setdefault("sessions", {})
        if not isinstance(sessions, dict):
            sessions = {}
            payload["sessions"] = sessions
        sessions[session_ref] = {
            "thread_id": session.thread_id,
            "config_fingerprint": session.config_fingerprint,
            "config_components": dict(session.config_components),
            "message_fingerprints": list(session.message_fingerprints),
            "updated_at": session.updated_at.astimezone(UTC).isoformat(),
        }
        self._write(payload)

    def delete(self, session_ref: str) -> None:
        payload = self._read()
        sessions = payload.get("sessions")
        if not isinstance(sessions, dict) or session_ref not in sessions:
            return
        sessions.pop(session_ref, None)
        self._write(payload)

    def _read(self) -> dict[str, Any]:
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {"version": CODEX_SESSION_STATE_VERSION, "sessions": {}}
        except (OSError, json.JSONDecodeError):
            logger.warning("Codex session registry is unreadable; starting clean")
            return {"version": CODEX_SESSION_STATE_VERSION, "sessions": {}}
        if not isinstance(payload, dict) or payload.get("version") != CODEX_SESSION_STATE_VERSION:
            return {"version": CODEX_SESSION_STATE_VERSION, "sessions": {}}
        return payload

    def _write(self, payload: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{self._path.name}.", suffix=".tmp", dir=str(self._path.parent)
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, self._path)
            if os.name != "nt":
                os.chmod(self._path, 0o600)
        except Exception:
            temporary_path.unlink(missing_ok=True)
            raise


class _CodexAppServerClient:
    def __init__(self, command: str, args: list[str], env: dict[str, str]) -> None:
        self._command = command
        self._args = args
        self._env = env
        self._process: asyncio.subprocess.Process | None = None
        self._next_id = 1
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._notifications: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._requests: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._deferred_events: deque[tuple[str, dict[str, Any]]] = deque()
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._process_watch_task: asyncio.Task[None] | None = None
        self._stderr_tail = ""
        self._transport_failed = asyncio.Event()
        self._transport_error: CodexAppServerError | None = None
        self._transport_reason: str | None = None
        self._transport_type: str | None = None
        self._transport_category: str | None = None
        self._protocol_error_count = 0
        self._closing = False

    async def start(self) -> None:
        if self._process is not None:
            return

        if os.name == "nt" and self._command.lower().endswith((".cmd", ".bat")):
            self._process = await asyncio.create_subprocess_shell(
                subprocess.list2cmdline([self._command, *self._args]),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self._env,
            )
        else:
            self._process = await asyncio.create_subprocess_exec(
                self._command,
                *self._args,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self._env,
            )
        self._reader_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        self._process_watch_task = asyncio.create_task(self._watch_process())
        try:
            await self.request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "octopal",
                        "title": "Octopal",
                        "version": "runtime",
                    },
                    "capabilities": {"experimentalApi": True},
                },
                timeout=CODEX_CONTROL_REQUEST_TIMEOUT_SECONDS,
            )
            self.notify("initialized", {})
        except BaseException:
            await self.close()
            raise

    async def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float = CODEX_CONTROL_REQUEST_TIMEOUT_SECONDS,
    ) -> Any:
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        wire_started_at: float | None = None
        process = self._require_process()
        if process.stdin is None:
            raise CodexAppServerError("codex app-server stdin is unavailable")
        if self._transport_error is not None:
            raise self._transport_error

        request_id = self._next_id
        self._next_id += 1
        message: dict[str, Any] = {"method": method, "id": request_id}
        if params is not None:
            message["params"] = params

        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            process.stdin.write((json.dumps(message) + "\n").encode("utf-8"))
            await process.stdin.drain()
            wire_started_at = loop.time()
            result = await asyncio.wait_for(future, timeout=timeout)
        except asyncio.CancelledError as exc:
            finished_at = loop.time()
            _log_app_server_request(
                method,
                outcome="cancelled",
                elapsed_ms=(finished_at - started_at) * 1000,
                write_wait_ms=((wire_started_at or finished_at) - started_at) * 1000,
                wire_wait_ms=(
                    (finished_at - wire_started_at) * 1000 if wire_started_at is not None else 0.0
                ),
                process_exit_code=process.returncode,
                stderr_tail=self._stderr_tail,
                error=type(exc).__name__,
                exception=exc,
                transport_reason=self._transport_reason,
                transport_type=self._transport_type,
                transport_category=self._transport_category,
                protocol_error_count=self._protocol_error_count,
            )
            raise
        except Exception as exc:
            finished_at = loop.time()
            _log_app_server_request(
                method,
                outcome="failed",
                elapsed_ms=(finished_at - started_at) * 1000,
                write_wait_ms=((wire_started_at or finished_at) - started_at) * 1000,
                wire_wait_ms=(
                    (finished_at - wire_started_at) * 1000 if wire_started_at is not None else 0.0
                ),
                process_exit_code=process.returncode,
                stderr_tail=self._stderr_tail,
                error=type(exc).__name__,
                exception=exc,
                transport_reason=self._transport_reason,
                transport_type=self._transport_type,
                transport_category=self._transport_category,
                protocol_error_count=self._protocol_error_count,
            )
            raise
        else:
            finished_at = loop.time()
            _log_app_server_request(
                method,
                outcome="completed",
                elapsed_ms=(finished_at - started_at) * 1000,
                write_wait_ms=(wire_started_at - started_at) * 1000,
                wire_wait_ms=(finished_at - wire_started_at) * 1000,
                process_exit_code=process.returncode,
                stderr_tail=self._stderr_tail,
                protocol_error_count=self._protocol_error_count,
            )
            return result
        finally:
            self._pending.pop(request_id, None)

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        process = self._require_process()
        if process.stdin is None:
            return
        message: dict[str, Any] = {"method": method}
        if params is not None:
            message["params"] = params
        process.stdin.write((json.dumps(message) + "\n").encode("utf-8"))

    async def respond(self, request_id: int | str, result: dict[str, Any]) -> None:
        process = self._require_process()
        if process.stdin is None:
            return
        process.stdin.write(
            (json.dumps({"id": request_id, "result": result}) + "\n").encode("utf-8")
        )
        await process.stdin.drain()

    async def respond_error(self, request_id: int | str, message: str) -> None:
        process = self._require_process()
        if process.stdin is None:
            return
        payload = {"id": request_id, "error": {"code": -32000, "message": message}}
        process.stdin.write((json.dumps(payload) + "\n").encode("utf-8"))
        await process.stdin.drain()

    async def next_event(self, timeout: float) -> tuple[str, dict[str, Any]]:
        if self._deferred_events:
            return self._deferred_events.popleft()

        notification_task = asyncio.create_task(self._notifications.get())
        request_task = asyncio.create_task(self._requests.get())
        transport_task = asyncio.create_task(self._transport_failed.wait())
        done, pending = await asyncio.wait(
            {notification_task, request_task, transport_task},
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if not done:
            raise TimeoutError("codex app-server turn timed out")
        if request_task in done:
            if notification_task in done:
                self._deferred_events.append(("notification", notification_task.result()))
            return "request", request_task.result()
        if notification_task in done:
            return "notification", notification_task.result()
        raise self._transport_error or CodexAppServerError("codex app-server transport closed")

    async def close(self) -> None:
        self._closing = True
        process = self._process
        if process and process.stdin:
            try:
                process.stdin.close()
                await process.stdin.wait_closed()
            except Exception:
                pass
        if process:
            if process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=2)
                except TimeoutError:
                    with contextlib.suppress(ProcessLookupError):
                        process.kill()
                    await process.wait()
            else:
                await process.wait()
        for task in (self._reader_task, self._stderr_task, self._process_watch_task):
            if task:
                task.cancel()
        await asyncio.gather(
            *(
                task
                for task in (self._reader_task, self._stderr_task, self._process_watch_task)
                if task is not None
            ),
            return_exceptions=True,
        )
        for future in self._pending.values():
            if not future.done():
                future.set_exception(CodexAppServerError("codex app-server closed"))
        self._pending.clear()
        self._process = None

    def _require_process(self) -> asyncio.subprocess.Process:
        if self._process is None:
            raise CodexAppServerError("codex app-server is not running")
        if self._process.returncode is not None:
            raise CodexAppServerError(
                f"codex app-server exited with code {self._process.returncode}"
            )
        return self._process

    async def _read_stdout(self) -> None:
        process = self._process
        if process is None:
            return
        assert process.stdout is not None
        try:
            async for raw_line in process.stdout:
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    self._note_protocol_error("invalid_json", process.returncode)
                    continue
                if not isinstance(message, dict):
                    self._note_protocol_error("invalid_message_shape", process.returncode)
                    continue
                if "id" in message and "method" in message:
                    await self._requests.put(message)
                    continue
                if "id" in message:
                    future = self._pending.get(message["id"])
                    if future and not future.done():
                        if message.get("error"):
                            error = message["error"]
                            future.set_exception(
                                CodexAppServerError(error.get("message") or "Codex request failed")
                            )
                        else:
                            future.set_result(message.get("result"))
                    continue
                if "method" in message:
                    await self._notifications.put(message)
                    continue
                self._note_protocol_error("invalid_message_shape", process.returncode)
        finally:
            if not self._closing:
                self._fail_transport(
                    CodexAppServerError("codex app-server stdout closed"),
                    reason="stdout_eof",
                    transport_type="eof",
                    category="stream",
                    process_exit_code=process.returncode,
                )

    async def _read_stderr(self) -> None:
        process = self._process
        if process is None:
            return
        assert process.stderr is not None
        async for raw_chunk in process.stderr:
            text = raw_chunk.decode("utf-8", errors="replace")
            self._stderr_tail = f"{self._stderr_tail}{text}"[-4000:]

    async def _watch_process(self) -> None:
        process = self._process
        if process is None:
            return
        return_code = await process.wait()
        if not self._closing:
            self._fail_transport(
                CodexAppServerError(f"codex app-server exited with code {return_code}"),
                reason=_process_exit_reason(return_code),
                transport_type="child_exit",
                category="process",
                process_exit_code=return_code,
            )

    def _note_protocol_error(self, reason: str, process_exit_code: int | None) -> None:
        self._protocol_error_count += 1
        _log_app_server_transport(
            reason=reason,
            transport_type="protocol_error",
            category="protocol",
            process_exit_code=process_exit_code,
            stderr_tail=self._stderr_tail,
            protocol_error_count=self._protocol_error_count,
            primary_failure=False,
        )

    def _fail_transport(
        self,
        error: CodexAppServerError,
        *,
        reason: str,
        transport_type: str,
        category: str,
        process_exit_code: int | None,
    ) -> None:
        primary_failure = self._transport_error is None
        if primary_failure:
            self._transport_error = error
            self._transport_reason = reason
            self._transport_type = transport_type
            self._transport_category = category
        _log_app_server_transport(
            reason=reason,
            transport_type=transport_type,
            category=category,
            process_exit_code=process_exit_code,
            stderr_tail=self._stderr_tail,
            protocol_error_count=self._protocol_error_count,
            primary_failure=primary_failure,
        )
        for future in self._pending.values():
            if not future.done():
                future.set_exception(self._transport_error)
        self._transport_failed.set()


class CodexProvider:
    """Inference provider backed by a locally authenticated Codex CLI."""

    def __init__(
        self,
        settings: Settings,
        model: str | None = None,
        config: LLMConfig | None = None,
        trace_sink: object | None = None,
    ) -> None:
        self._settings = settings
        self._profile = resolve_litellm_profile(
            settings, model_override=model, config_override=config
        )
        self._model = cast(str, self._profile.raw_model or self._profile.model)
        self._sessions = _CodexSessionStore(Path(getattr(settings, "state_dir", Path("data"))))
        self._session_locks: dict[str, asyncio.Lock] = {}
        self._admission = _CodexAdmissionController()

    @property
    def provider_id(self) -> str:
        return "codex"

    @property
    def model_id(self) -> str:
        return self._model

    async def complete(self, messages: list[Message | dict], **kwargs: object) -> str:
        result = await self._run_turn(
            messages,
            tools=None,
            on_partial=None,
            session_key=_session_key_from_kwargs(kwargs),
            lane=_lane_from_kwargs(kwargs),
        )
        return cast(str, result["content"])

    async def complete_stream(
        self,
        messages: list[Message | dict],
        *,
        on_partial: Callable[[str], Awaitable[None]],
        **kwargs: object,
    ) -> str:
        result = await self._run_turn(
            messages,
            tools=None,
            on_partial=on_partial,
            session_key=_session_key_from_kwargs(kwargs),
            lane=_lane_from_kwargs(kwargs),
        )
        return cast(str, result["content"])

    async def complete_with_tools(
        self,
        messages: list[Message | dict],
        *,
        tools: list[dict],
        tool_choice: str = "auto",
        **kwargs: object,
    ) -> dict:
        result = await self._run_turn(
            messages,
            tools=tools,
            on_partial=None,
            session_key=_session_key_from_kwargs(kwargs),
            lane=_lane_from_kwargs(kwargs),
            tool_executor=_tool_executor_from_kwargs(kwargs),
        )
        return {
            "content": result["content"],
            "tool_calls": result["tool_calls"],
            "tool_results_submitted": bool(result.get("tool_results_submitted", False)),
            "usage": {},
        }

    async def _run_turn(
        self,
        messages: list[Message | dict],
        *,
        tools: list[dict] | None,
        on_partial: Callable[[str], Awaitable[None]] | None,
        session_key: str | None,
        lane: str,
        tool_executor: _ToolExecutor | None = None,
    ) -> dict[str, Any]:
        if session_key:
            phase = _session_phase(tools)
            session_ref = _session_ref(session_key, phase=phase, lane=lane)
            lock = self._session_locks.setdefault(session_ref, asyncio.Lock())
            queued_at = asyncio.get_running_loop().time()
            async with lock:
                _log_session_queue_wait(
                    session_ref,
                    phase=phase,
                    queue_wait_ms=(asyncio.get_running_loop().time() - queued_at) * 1000,
                )
                async with self._admission.acquire(lane) as admission:
                    _log_admission_wait(session_ref, lane=lane, phase=phase, **admission)
                    return await self._run_session_turn(
                        messages,
                        tools=tools,
                        on_partial=on_partial,
                        session_ref=session_ref,
                        phase=phase,
                        tool_executor=tool_executor,
                    )
        async with self._admission.acquire(lane) as admission:
            _log_admission_wait(None, lane=lane, phase=_session_phase(tools), **admission)
            return await self._run_ephemeral_turn(
                messages,
                tools=tools,
                on_partial=on_partial,
                tool_executor=tool_executor,
            )

    async def _run_ephemeral_turn(
        self,
        messages: list[Message | dict],
        *,
        tools: list[dict] | None,
        on_partial: Callable[[str], Awaitable[None]] | None,
        tool_executor: _ToolExecutor | None,
    ) -> dict[str, Any]:
        client = _CodexAppServerClient(_codex_command(), _codex_args(), _codex_env())
        await client.start()
        try:
            instructions, input_items = _messages_to_codex_input(messages)
            dynamic_tools = _tools_to_dynamic_tools(tools or [])
            cwd = str(Path.cwd())
            thread = await client.request(
                "thread/start",
                _compact(
                    {
                        "model": self._model,
                        "cwd": cwd,
                        "approvalPolicy": "never",
                        "sandbox": "read-only",
                        "developerInstructions": instructions or None,
                        "personality": "none",
                        "serviceName": "octopal",
                        "ephemeral": True,
                        "environments": [],
                        "dynamicTools": dynamic_tools or None,
                    }
                ),
                timeout=CODEX_CONTROL_REQUEST_TIMEOUT_SECONDS,
            )
            thread_id = ((thread or {}).get("thread") or {}).get("id")
            if not thread_id:
                raise CodexAppServerError("Codex did not return a thread id")

            turn = await client.request(
                "turn/start",
                _compact(
                    {
                        "threadId": thread_id,
                        "input": input_items,
                        "cwd": cwd,
                        "model": self._model,
                        "approvalPolicy": "never",
                        "sandboxPolicy": {"type": "readOnly", "networkAccess": False},
                        "effort": _normalize_effort(
                            getattr(self._settings, "codex_reasoning_effort", None)
                        ),
                        "environments": [],
                    }
                ),
                timeout=CODEX_CONTROL_REQUEST_TIMEOUT_SECONDS,
            )
            turn_id = ((turn or {}).get("turn") or {}).get("id")
            return await _collect_turn_with_cancellation(
                client,
                thread_id=thread_id,
                turn_id=turn_id,
                on_partial=on_partial,
                tool_executor=tool_executor,
            )
        finally:
            await client.close()

    async def _run_session_turn(
        self,
        messages: list[Message | dict],
        *,
        tools: list[dict] | None,
        on_partial: Callable[[str], Awaitable[None]] | None,
        session_ref: str,
        phase: str,
        tool_executor: _ToolExecutor | None,
    ) -> dict[str, Any]:
        client = _CodexAppServerClient(_codex_command(), _codex_args(), _codex_env())
        await client.start()
        resumed = False
        recovery_fresh_process = False
        try:
            instructions, full_input_items = _messages_to_codex_input(messages)
            message_fingerprints = _message_fingerprints(messages)
            dynamic_tools = _tools_to_dynamic_tools(tools or [])
            cwd = str(Path.cwd())
            configuration = _session_configuration(
                model=self._model,
                cwd=cwd,
                effort=_normalize_effort(getattr(self._settings, "codex_reasoning_effort", None)),
                dynamic_tools=dynamic_tools,
            )
            session, reset_reason = self._sessions.get(session_ref)
            changed_components: list[str] | None = None
            if session is not None and session.config_fingerprint != configuration.fingerprint:
                changed_components = _changed_config_components(
                    session.config_components, configuration.components
                )
                self._sessions.delete(session_ref)
                session = None
                reset_reason = "configuration_changed"
            if reset_reason:
                _log_session_state(
                    "reset",
                    session_ref,
                    phase=phase,
                    reason=reset_reason,
                    changed_components=changed_components,
                    tool_count=configuration.tool_count,
                    tool_digest=configuration.tool_digest,
                )

            thread_id: str | None = None
            input_items = full_input_items
            if session is not None:
                try:
                    resumed_thread = await client.request(
                        "thread/resume",
                        {
                            "threadId": session.thread_id,
                            "model": self._model,
                            "cwd": cwd,
                            "approvalPolicy": "never",
                            "sandbox": "read-only",
                            "personality": "none",
                        },
                        timeout=CODEX_CONTROL_REQUEST_TIMEOUT_SECONDS,
                    )
                    thread_id = ((resumed_thread or {}).get("thread") or {}).get("id")
                    if not thread_id:
                        raise CodexAppServerError("Codex did not return a resumed thread id")
                    input_items = _incremental_codex_input(
                        messages,
                        previous_fingerprints=session.message_fingerprints,
                    )
                    resumed = True
                    _log_session_state(
                        "resumed",
                        session_ref,
                        phase=phase,
                        thread_id=thread_id,
                        tool_count=configuration.tool_count,
                        tool_digest=configuration.tool_digest,
                    )
                except Exception as exc:
                    self._sessions.delete(session_ref)
                    _log_session_state(
                        "reset",
                        session_ref,
                        phase=phase,
                        thread_id=session.thread_id,
                        reason="resume_failed",
                        error=type(exc).__name__,
                        tool_count=configuration.tool_count,
                        tool_digest=configuration.tool_digest,
                    )
                    with contextlib.suppress(Exception):
                        await client.close()
                    client = _CodexAppServerClient(_codex_command(), _codex_args(), _codex_env())
                    recovery_fresh_process = True
                    _log_session_state(
                        "recovery",
                        session_ref,
                        phase=phase,
                        reason="resume_failed",
                        recovery_fresh_process=True,
                        tool_count=configuration.tool_count,
                        tool_digest=configuration.tool_digest,
                    )
                    await client.start()

            if thread_id is None:
                thread = await client.request(
                    "thread/start",
                    _compact(
                        {
                            "model": self._model,
                            "cwd": cwd,
                            "approvalPolicy": "never",
                            "sandbox": "read-only",
                            "developerInstructions": instructions or None,
                            "personality": "none",
                            "serviceName": "octopal",
                            "ephemeral": False,
                            "environments": [],
                            "dynamicTools": dynamic_tools or None,
                        }
                    ),
                    timeout=CODEX_CONTROL_REQUEST_TIMEOUT_SECONDS,
                )
                thread_id = ((thread or {}).get("thread") or {}).get("id")
                if not thread_id:
                    raise CodexAppServerError("Codex did not return a thread id")
                input_items = full_input_items
                _log_session_state(
                    "created",
                    session_ref,
                    phase=phase,
                    thread_id=thread_id,
                    recovery_fresh_process=recovery_fresh_process or None,
                    tool_count=configuration.tool_count,
                    tool_digest=configuration.tool_digest,
                )

            turn = await client.request(
                "turn/start",
                _compact(
                    {
                        "threadId": thread_id,
                        "input": input_items,
                        "cwd": cwd,
                        "model": self._model,
                        "approvalPolicy": "never",
                        "sandboxPolicy": {"type": "readOnly", "networkAccess": False},
                        "effort": _normalize_effort(
                            getattr(self._settings, "codex_reasoning_effort", None)
                        ),
                        "environments": [],
                    }
                ),
                timeout=CODEX_CONTROL_REQUEST_TIMEOUT_SECONDS,
            )
            turn_id = ((turn or {}).get("turn") or {}).get("id")
            result = await _collect_turn_with_cancellation(
                client,
                thread_id=thread_id,
                turn_id=turn_id,
                on_partial=on_partial,
                tool_executor=tool_executor,
            )
            if result.pop("terminal", False):
                self._sessions.put(
                    session_ref,
                    _CodexSession(
                        thread_id=thread_id,
                        config_fingerprint=configuration.fingerprint,
                        config_components=configuration.components,
                        message_fingerprints=message_fingerprints,
                        updated_at=datetime.now(UTC),
                    ),
                )
            else:
                self._sessions.delete(session_ref)
                _log_session_state(
                    "reset",
                    session_ref,
                    phase=phase,
                    thread_id=thread_id,
                    reason="turn_not_terminal",
                    tool_count=configuration.tool_count,
                    tool_digest=configuration.tool_digest,
                )
            return result
        except BaseException:
            if resumed:
                self._sessions.delete(session_ref)
                _log_session_state(
                    "reset",
                    session_ref,
                    phase=phase,
                    reason="resumed_turn_failed",
                    tool_count=configuration.tool_count,
                    tool_digest=configuration.tool_digest,
                )
            raise
        finally:
            await client.close()


async def _collect_turn(
    client: _CodexAppServerClient,
    *,
    thread_id: str,
    turn_id: str | None,
    on_partial: Callable[[str], Awaitable[None]] | None,
    tool_executor: _ToolExecutor | None = None,
) -> dict[str, Any]:
    output = ""
    tool_calls: list[dict[str, Any]] = []
    tool_execution_may_have_completed = False

    while True:
        try:
            kind, event = await client.next_event(CODEX_TURN_EVENT_IDLE_TIMEOUT_SECONDS)
        except (CodexAppServerError, TimeoutError):
            if tool_execution_may_have_completed:
                raise CodexToolResultTransportError(
                    "provider transport ended after tool execution; automatic replay disabled"
                ) from None
            raise
        payload = event.get("params") or {}
        if kind == "request":
            method = str(event.get("method") or "")
            if method == "item/tool/call":
                call = _tool_call_from_codex_request(payload)
                if call:
                    tool_calls.append(call)
                response = {
                    "success": False,
                    "contentItems": [
                        {
                            "type": "inputText",
                            "text": "The requested tool is unavailable for this turn.",
                        }
                    ],
                }
                if call is not None and tool_executor is not None:
                    try:
                        execution = await tool_executor(call)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception(
                            "Dynamic tool executor failed",
                            tool_name=str((call.get("function") or {}).get("name") or ""),
                        )
                        execution = {
                            "success": False,
                            "content": "Tool execution failed before a result was available.",
                        }
                    tool_execution_may_have_completed = True
                    response = _dynamic_tool_response(execution)
                try:
                    await client.respond(event["id"], response)
                except (CodexAppServerError, OSError):
                    if tool_execution_may_have_completed:
                        raise CodexToolResultTransportError(
                            "provider transport ended while submitting a tool result; automatic replay disabled"
                        ) from None
                    raise
                continue
            await _respond_to_auxiliary_request(client, event)
            continue

        method = str(event.get("method") or "")
        if payload.get("threadId") and payload.get("threadId") != thread_id:
            continue
        if method == "item/agentMessage/delta":
            delta = str(payload.get("delta") or "")
            output += delta
            if on_partial:
                await on_partial(output)
            continue
        if method == "turn/completed":
            turn = payload.get("turn")
            status = str(turn.get("status") or "") if isinstance(turn, dict) else ""
            terminal = status not in {"failed", "interrupted"}
            result: dict[str, Any] = {
                "content": output,
                "tool_calls": tool_calls,
                "terminal": terminal,
            }
            if tool_executor is not None and tool_calls:
                result["tool_results_submitted"] = True
            return result
        if method == "error":
            if tool_execution_may_have_completed:
                raise CodexToolResultTransportError(
                    "provider failed after tool execution; automatic replay disabled"
                ) from None
            raise CodexAppServerError(json.dumps(payload, ensure_ascii=False))


async def _collect_turn_with_cancellation(
    client: _CodexAppServerClient,
    *,
    thread_id: str,
    turn_id: str | None,
    on_partial: Callable[[str], Awaitable[None]] | None,
    tool_executor: _ToolExecutor | None,
) -> dict[str, Any]:
    try:
        return await _collect_turn(
            client,
            thread_id=thread_id,
            turn_id=turn_id,
            on_partial=on_partial,
            tool_executor=tool_executor,
        )
    except asyncio.CancelledError:
        if turn_id:
            with contextlib.suppress(Exception):
                await client.request(
                    "turn/interrupt",
                    {"threadId": thread_id, "turnId": turn_id},
                    timeout=CODEX_TOOL_INTERRUPT_GRACE_SECONDS,
                )
        raise


def _dynamic_tool_response(execution: dict[str, Any]) -> dict[str, Any]:
    content = execution.get("content")
    if not isinstance(content, str):
        content = json.dumps(content, ensure_ascii=False, default=str)
    return {
        "success": bool(execution.get("success", False)),
        "contentItems": [{"type": "inputText", "text": content}],
    }


async def _respond_to_auxiliary_request(
    client: _CodexAppServerClient, event: dict[str, Any]
) -> None:
    method = str(event.get("method") or "")
    if method == "item/tool/requestUserInput":
        await client.respond(event["id"], {"answers": {}})
        return
    if method == "item/permissions/requestApproval":
        await client.respond(event["id"], {"permissions": {}, "scope": "turn"})
        return
    if method.endswith("/requestApproval"):
        await client.respond(event["id"], {"decision": "decline"})
        return
    await client.respond_error(event["id"], f"Unsupported Codex app-server request: {method}")


def _messages_to_codex_input(messages: list[Message | dict]) -> tuple[str, list[dict[str, str]]]:
    instructions: list[str] = []
    chunks: list[str] = []
    for message in messages:
        data = message.to_dict() if isinstance(message, Message) else dict(message)
        role = str(data.get("role") or "message").lower()
        content = _content_to_text(data.get("content"))
        if not content:
            continue
        if role == "system":
            instructions.append(content)
        else:
            chunks.append(f"{role.upper()}:\n{content}")
    text = "\n\n".join(chunks).strip() or "Continue."
    return "\n\n".join(instructions).strip(), [{"type": "text", "text": text}]


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                if item.get("type") == "text":
                    parts.append(str(item.get("text") or "").strip())
                elif item.get("type") == "image_url":
                    parts.append("[image omitted]")
            elif item is not None:
                parts.append(str(item).strip())
        return "\n".join(part for part in parts if part).strip()
    if content is None:
        return ""
    return json.dumps(content, ensure_ascii=False)


def _session_key_from_kwargs(kwargs: dict[str, object]) -> str | None:
    value = kwargs.get("codex_session_key")
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _tool_executor_from_kwargs(kwargs: dict[str, object]) -> _ToolExecutor | None:
    executor = kwargs.get("tool_executor")
    return cast(_ToolExecutor, executor) if callable(executor) else None


def _lane_from_kwargs(kwargs: dict[str, object]) -> str:
    return _normalize_lane(str(kwargs.get("codex_request_lane") or ""))


def _normalize_lane(value: str) -> str:
    return (
        CODEX_LANE_INTERACTIVE
        if str(value or "").strip().lower() == CODEX_LANE_INTERACTIVE
        else CODEX_LANE_BACKGROUND
    )


def _message_fingerprints(messages: list[Message | dict]) -> tuple[str, ...]:
    return tuple(_fingerprint(_message_payload(message)) for message in messages)


def _message_payload(message: Message | dict) -> str:
    data = message.to_dict() if isinstance(message, Message) else dict(message)
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _incremental_codex_input(
    messages: list[Message | dict],
    *,
    previous_fingerprints: tuple[str, ...],
) -> list[dict[str, str]]:
    current_fingerprints = _message_fingerprints(messages)
    suffix: list[Message | dict]
    prefix_len = len(previous_fingerprints)
    if (
        prefix_len <= len(current_fingerprints)
        and current_fingerprints[:prefix_len] == previous_fingerprints
    ):
        suffix = messages[prefix_len:]
    else:
        suffix = []
        for index in range(len(messages) - 1, -1, -1):
            message = messages[index]
            data = message.to_dict() if isinstance(message, Message) else dict(message)
            if str(data.get("role") or "").lower() == "user":
                suffix = messages[index:]
                break

    chunks: list[str] = []
    for message in suffix:
        data = message.to_dict() if isinstance(message, Message) else dict(message)
        role = str(data.get("role") or "message").upper()
        content = _content_to_text(data.get("content"))
        if content:
            chunks.append(f"{role}:\n{content}")
    text = "\n\n".join(chunks).strip() or "Continue."
    return [{"type": "text", "text": text}]


def _session_configuration(
    *,
    model: str,
    cwd: str,
    effort: str | None,
    dynamic_tools: list[dict[str, Any]],
) -> _CodexSessionConfiguration:
    tool_catalog = sorted(dynamic_tools, key=lambda item: str(item.get("name") or ""))
    payload = {
        "bridgeVersion": CODEX_SESSION_STATE_VERSION,
        "model": model,
        "cwd": cwd,
        "effort": effort,
        "approvalPolicy": "never",
        "sandboxPolicy": {"type": "readOnly", "networkAccess": False},
        "dynamicTools": tool_catalog,
        "command": _codex_command(),
        "args": _codex_args(),
    }
    components = {
        "bridge_version": _config_component_fingerprint(payload["bridgeVersion"]),
        "model": _config_component_fingerprint(payload["model"]),
        "cwd": _config_component_fingerprint(payload["cwd"]),
        "reasoning_effort": _config_component_fingerprint(payload["effort"]),
        "approval_policy": _config_component_fingerprint(payload["approvalPolicy"]),
        "sandbox_policy": _config_component_fingerprint(payload["sandboxPolicy"]),
        "tool_catalog": _config_component_fingerprint(payload["dynamicTools"]),
        "command": _config_component_fingerprint(payload["command"]),
        "args": _config_component_fingerprint(payload["args"]),
    }
    return _CodexSessionConfiguration(
        fingerprint=_fingerprint(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        ),
        components=components,
        tool_count=len(tool_catalog),
        tool_digest=components["tool_catalog"],
    )


def _session_config_fingerprint(
    *,
    model: str,
    cwd: str,
    effort: str | None,
    dynamic_tools: list[dict[str, Any]],
) -> str:
    return _session_configuration(
        model=model,
        cwd=cwd,
        effort=effort,
        dynamic_tools=dynamic_tools,
    ).fingerprint


def _config_component_fingerprint(value: Any) -> str:
    return _fingerprint(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )


def _changed_config_components(previous: dict[str, str], current: dict[str, str]) -> list[str]:
    if not previous:
        return ["unknown"]
    return sorted(
        key for key in set(previous) | set(current) if previous.get(key) != current.get(key)
    )


def _session_phase(tools: list[dict] | None) -> str:
    return "executor" if tools is not None else "planner"


def _session_ref(session_key: str, *, phase: str, lane: str = CODEX_LANE_BACKGROUND) -> str:
    return _fingerprint(
        json.dumps(
            {"session_key": session_key, "phase": phase, "lane": _normalize_lane(lane)},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _log_session_queue_wait(session_ref: str, *, phase: str, queue_wait_ms: float) -> None:
    logger.debug(
        "Codex provider session queue wait",
        session_ref=session_ref[:12],
        phase=phase,
        queue_wait_ms=round(queue_wait_ms, 3),
    )


def _log_admission_wait(
    session_ref: str | None,
    *,
    lane: str,
    phase: str,
    queue_wait_ms: float,
    active_total: int,
    active_background: int,
) -> None:
    fields = {
        "session_ref": session_ref[:12] if session_ref else None,
        "lane": lane,
        "phase": phase,
        "queue_wait_ms": round(queue_wait_ms, 3),
        "active_total": active_total,
        "active_background": active_background,
    }
    if queue_wait_ms >= 1000:
        logger.info("Codex provider admission wait", **fields)
    else:
        logger.debug("Codex provider admission wait", **fields)


def _log_session_state(
    state: str,
    session_ref: str,
    *,
    phase: str,
    thread_id: str | None = None,
    reason: str | None = None,
    error: str | None = None,
    changed_components: list[str] | None = None,
    tool_count: int | None = None,
    tool_digest: str | None = None,
    recovery_fresh_process: bool | None = None,
) -> None:
    logger.info(
        "Codex provider session state changed",
        state=state,
        session_ref=session_ref[:12],
        phase=phase,
        thread_ref=_fingerprint(thread_id)[:12] if thread_id else None,
        reason=reason,
        error=error,
        changed_components=changed_components,
        tool_count=tool_count,
        tool_digest=tool_digest[:12] if tool_digest else None,
        recovery_fresh_process=recovery_fresh_process,
    )


def _log_app_server_request(
    stage: str,
    *,
    outcome: str,
    elapsed_ms: float,
    write_wait_ms: float,
    wire_wait_ms: float,
    process_exit_code: int | None,
    stderr_tail: str,
    error: str | None = None,
    exception: BaseException | None = None,
    transport_reason: str | None = None,
    transport_type: str | None = None,
    transport_category: str | None = None,
    protocol_error_count: int = 0,
) -> None:
    stderr_category, stderr_digest = _stderr_diagnostics(stderr_tail)
    failure_reason, failure_type, failure_category = _request_failure_diagnostics(
        outcome=outcome,
        exception=exception,
        transport_reason=transport_reason,
        transport_type=transport_type,
        transport_category=transport_category,
    )
    process_exit_kind, process_exit_signal = _process_exit_diagnostics(process_exit_code)
    log = logger.debug if outcome == "completed" else logger.warning
    log(
        "Codex app-server request finished",
        request_stage=stage,
        outcome=outcome,
        elapsed_ms=round(elapsed_ms, 3),
        write_wait_ms=round(write_wait_ms, 3),
        wire_wait_ms=round(wire_wait_ms, 3),
        process_exit_code=process_exit_code,
        process_exit_kind=process_exit_kind,
        process_exit_signal=process_exit_signal,
        failure_reason=failure_reason,
        failure_type=failure_type,
        failure_category=failure_category,
        stderr_category=stderr_category,
        stderr_digest=stderr_digest,
        likely_diagnostic_categories=_stderr_diagnostic_categories(stderr_tail),
        protocol_error_count=protocol_error_count,
        error=error,
    )


def _log_app_server_transport(
    *,
    reason: str,
    transport_type: str,
    category: str,
    process_exit_code: int | None,
    stderr_tail: str,
    protocol_error_count: int,
    primary_failure: bool,
) -> None:
    stderr_category, stderr_digest = _stderr_diagnostics(stderr_tail)
    process_exit_kind, process_exit_signal = _process_exit_diagnostics(process_exit_code)
    logger.warning(
        "Codex app-server transport observed",
        transport_reason=reason,
        transport_type=transport_type,
        transport_category=category,
        primary_failure=primary_failure,
        process_exit_code=process_exit_code,
        process_exit_kind=process_exit_kind,
        process_exit_signal=process_exit_signal,
        stderr_category=stderr_category,
        stderr_digest=stderr_digest,
        likely_diagnostic_categories=_stderr_diagnostic_categories(stderr_tail),
        protocol_error_count=protocol_error_count,
    )


def _request_failure_diagnostics(
    *,
    outcome: str,
    exception: BaseException | None,
    transport_reason: str | None,
    transport_type: str | None,
    transport_category: str | None,
) -> tuple[str | None, str | None, str | None]:
    if outcome == "completed":
        return None, None, None
    if transport_reason:
        return transport_reason, transport_type or "transport", transport_category or "transport"
    if isinstance(exception, TimeoutError):
        return "request_timeout", "timeout", "request"
    if isinstance(exception, asyncio.CancelledError):
        return "request_cancelled", "cancellation", "request"
    return "request_failed", "exception", "request"


def _process_exit_reason(return_code: int) -> str:
    if return_code < 0:
        return "child_exit_signal"
    if return_code == 0:
        return "child_exit_clean"
    return "child_exit_nonzero"


def _process_exit_diagnostics(return_code: int | None) -> tuple[str, int | None]:
    if return_code is None:
        return "running_or_unknown", None
    if return_code < 0:
        return "signal", abs(return_code)
    if return_code == 0:
        return "clean", None
    return "nonzero", None


def _stderr_diagnostics(stderr_tail: str) -> tuple[str, str | None]:
    normalized = stderr_tail.strip()
    if not normalized:
        return "empty", None
    lowered = normalized.lower()
    if "panic" in lowered or "fatal" in lowered:
        category = "fatal"
    elif "error" in lowered or "exception" in lowered:
        category = "error"
    elif "warn" in lowered:
        category = "warning"
    else:
        category = "other"
    return category, _fingerprint(normalized)[:12]


def _stderr_diagnostic_categories(stderr_tail: str) -> list[str]:
    lowered = stderr_tail.strip().lower()
    if not lowered:
        return []
    categories: list[str] = []
    markers = (
        ("authentication", ("unauthorized", "authentication", "login required")),
        ("configuration", ("configuration", "invalid option", "unknown argument")),
        ("permission", ("permission denied", "operation not permitted")),
        (
            "resource_exhaustion",
            ("out of memory", "too many open files", "no space left"),
        ),
        ("network", ("network", "connection", "dns", "tls")),
        ("protocol", ("protocol", "json-rpc", "invalid json")),
        ("state_store", ("database", "sqlite", "state db", "rollout")),
        ("thread_state", ("thread not found", "unknown thread", "cannot resume")),
        ("startup", ("failed to initialize", "startup")),
        ("sandbox", ("sandbox",)),
        ("runtime_failure", ("panic", "fatal", "exception")),
    )
    for category, candidates in markers:
        if any(candidate in lowered for candidate in candidates):
            categories.append(category)
    return categories or ["unclassified"]


def _tools_to_dynamic_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dynamic_tools: list[dict[str, Any]] = []
    for tool in tools:
        if tool.get("type") != "function":
            continue
        raw_function = tool.get("function")
        function: dict[str, Any] = raw_function if isinstance(raw_function, dict) else {}
        name = str(function.get("name") or "").strip()
        if not name:
            continue
        parameters = function.get("parameters")
        dynamic_tools.append(
            {
                "name": name,
                "description": str(function.get("description") or name),
                "inputSchema": (
                    parameters
                    if isinstance(parameters, dict)
                    else {"type": "object", "properties": {}}
                ),
            }
        )
    return dynamic_tools


def _tool_call_from_codex_request(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    tool = str(payload.get("tool") or "").strip()
    if not tool:
        return None
    arguments = payload.get("arguments")
    return {
        "id": str(payload.get("callId") or f"codex-call-{tool}"),
        "type": "function",
        "function": {
            "name": tool,
            "arguments": json.dumps(arguments if arguments is not None else {}, ensure_ascii=False),
        },
    }


def _compact(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None}


def _normalize_effort(value: Any) -> str | None:
    if value == "minimal":
        return "low"
    return value if isinstance(value, str) and value else None


def _codex_command() -> str:
    configured = os.getenv("OCTOPAL_CODEX_COMMAND")
    if configured:
        return configured
    return shutil.which("codex") or "codex"


def _codex_args() -> list[str]:
    raw = os.getenv("OCTOPAL_CODEX_ARGS", "app-server")
    return [part for part in raw.split() if part]


def _codex_env() -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if value is not None}
    env.pop("OPENAI_API_KEY", None)
    env.pop("CODEX_API_KEY", None)
    return env
