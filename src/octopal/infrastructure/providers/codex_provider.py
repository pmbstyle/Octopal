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

CODEX_REQUEST_TIMEOUT_SECONDS = 30.0
CODEX_TURN_TIMEOUT_SECONDS = 180.0
CODEX_SESSION_TTL_DAYS = 30
CODEX_SESSION_STATE_VERSION = 1

logger = structlog.get_logger(__name__)


class CodexAppServerError(RuntimeError):
    pass


@dataclass(frozen=True)
class _CodexSession:
    thread_id: str
    config_fingerprint: str
    message_fingerprints: tuple[str, ...]
    updated_at: datetime


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
            session = _CodexSession(
                thread_id=str(raw["thread_id"]),
                config_fingerprint=str(raw["config_fingerprint"]),
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
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._stderr_tail = ""

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
        )
        self.notify("initialized", {})

    async def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float = CODEX_REQUEST_TIMEOUT_SECONDS,
    ) -> Any:
        process = self._require_process()
        if process.stdin is None:
            raise CodexAppServerError("codex app-server stdin is unavailable")

        request_id = self._next_id
        self._next_id += 1
        message: dict[str, Any] = {"method": method, "id": request_id}
        if params is not None:
            message["params"] = params

        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        process.stdin.write((json.dumps(message) + "\n").encode("utf-8"))
        await process.stdin.drain()
        try:
            return await asyncio.wait_for(future, timeout=timeout)
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
        notification_task = asyncio.create_task(self._notifications.get())
        request_task = asyncio.create_task(self._requests.get())
        done, pending = await asyncio.wait(
            {notification_task, request_task},
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        if not done:
            raise TimeoutError("codex app-server turn timed out")
        task = done.pop()
        return ("request" if task is request_task else "notification", task.result())

    async def close(self) -> None:
        process = self._process
        if process and process.stdin:
            try:
                process.stdin.close()
                await process.stdin.wait_closed()
            except Exception:
                pass
        if process:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=2)
            except TimeoutError:
                process.kill()
                await process.wait()
        if self._reader_task:
            self._reader_task.cancel()
        if self._stderr_task:
            self._stderr_task.cancel()
        await asyncio.gather(
            *(task for task in (self._reader_task, self._stderr_task) if task is not None),
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
        return self._process

    async def _read_stdout(self) -> None:
        process = self._require_process()
        assert process.stdout is not None
        async for raw_line in process.stdout:
            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
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

    async def _read_stderr(self) -> None:
        process = self._require_process()
        assert process.stderr is not None
        async for raw_chunk in process.stderr:
            text = raw_chunk.decode("utf-8", errors="replace")
            self._stderr_tail = f"{self._stderr_tail}{text}"[-4000:]


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
        )
        return {
            "content": result["content"],
            "tool_calls": result["tool_calls"],
            "usage": {},
        }

    async def _run_turn(
        self,
        messages: list[Message | dict],
        *,
        tools: list[dict] | None,
        on_partial: Callable[[str], Awaitable[None]] | None,
        session_key: str | None,
    ) -> dict[str, Any]:
        if session_key:
            session_ref = _fingerprint(session_key)
            lock = self._session_locks.setdefault(session_ref, asyncio.Lock())
            async with lock:
                return await self._run_session_turn(
                    messages,
                    tools=tools,
                    on_partial=on_partial,
                    session_ref=session_ref,
                )
        return await self._run_ephemeral_turn(messages, tools=tools, on_partial=on_partial)

    async def _run_ephemeral_turn(
        self,
        messages: list[Message | dict],
        *,
        tools: list[dict] | None,
        on_partial: Callable[[str], Awaitable[None]] | None,
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
                timeout=CODEX_TURN_TIMEOUT_SECONDS,
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
                timeout=CODEX_TURN_TIMEOUT_SECONDS,
            )
            turn_id = ((turn or {}).get("turn") or {}).get("id")
            return await _collect_turn(
                client, thread_id=thread_id, turn_id=turn_id, on_partial=on_partial
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
    ) -> dict[str, Any]:
        client = _CodexAppServerClient(_codex_command(), _codex_args(), _codex_env())
        await client.start()
        resumed = False
        try:
            instructions, full_input_items = _messages_to_codex_input(messages)
            message_fingerprints = _message_fingerprints(messages)
            dynamic_tools = _tools_to_dynamic_tools(tools or [])
            cwd = str(Path.cwd())
            config_fingerprint = _session_config_fingerprint(
                model=self._model,
                cwd=cwd,
                effort=_normalize_effort(getattr(self._settings, "codex_reasoning_effort", None)),
                dynamic_tools=dynamic_tools,
            )
            session, reset_reason = self._sessions.get(session_ref)
            if session is not None and session.config_fingerprint != config_fingerprint:
                self._sessions.delete(session_ref)
                session = None
                reset_reason = "configuration_changed"
            if reset_reason:
                _log_session_state("reset", session_ref, reason=reset_reason)

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
                        timeout=CODEX_TURN_TIMEOUT_SECONDS,
                    )
                    thread_id = ((resumed_thread or {}).get("thread") or {}).get("id")
                    if not thread_id:
                        raise CodexAppServerError("Codex did not return a resumed thread id")
                    input_items = _incremental_codex_input(
                        messages,
                        previous_fingerprints=session.message_fingerprints,
                    )
                    resumed = True
                    _log_session_state("resumed", session_ref, thread_id=thread_id)
                except Exception as exc:
                    self._sessions.delete(session_ref)
                    _log_session_state(
                        "reset",
                        session_ref,
                        thread_id=session.thread_id,
                        reason="resume_failed",
                        error=type(exc).__name__,
                    )

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
                    timeout=CODEX_TURN_TIMEOUT_SECONDS,
                )
                thread_id = ((thread or {}).get("thread") or {}).get("id")
                if not thread_id:
                    raise CodexAppServerError("Codex did not return a thread id")
                input_items = full_input_items
                _log_session_state("created", session_ref, thread_id=thread_id)

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
                timeout=CODEX_TURN_TIMEOUT_SECONDS,
            )
            turn_id = ((turn or {}).get("turn") or {}).get("id")
            result = await _collect_turn(
                client, thread_id=thread_id, turn_id=turn_id, on_partial=on_partial
            )
            self._sessions.put(
                session_ref,
                _CodexSession(
                    thread_id=thread_id,
                    config_fingerprint=config_fingerprint,
                    message_fingerprints=message_fingerprints,
                    updated_at=datetime.now(UTC),
                ),
            )
            return result
        except Exception:
            if resumed:
                self._sessions.delete(session_ref)
                _log_session_state("reset", session_ref, reason="resumed_turn_failed")
            raise
        finally:
            await client.close()


async def _collect_turn(
    client: _CodexAppServerClient,
    *,
    thread_id: str,
    turn_id: str | None,
    on_partial: Callable[[str], Awaitable[None]] | None,
) -> dict[str, Any]:
    output = ""
    tool_calls: list[dict[str, Any]] = []
    current_turn_id = turn_id

    while True:
        kind, event = await client.next_event(CODEX_TURN_TIMEOUT_SECONDS)
        payload = event.get("params") or {}
        if kind == "request":
            method = str(event.get("method") or "")
            if method == "item/tool/call":
                call = _tool_call_from_codex_request(payload)
                if call:
                    tool_calls.append(call)
                if current_turn_id:
                    with contextlib.suppress(Exception):
                        await client.request(
                            "turn/interrupt",
                            {"threadId": thread_id, "turnId": current_turn_id},
                            timeout=CODEX_REQUEST_TIMEOUT_SECONDS,
                        )
                await client.respond(
                    event["id"],
                    {
                        "success": False,
                        "contentItems": [
                            {
                                "type": "inputText",
                                "text": "Tool execution is handled by Octopal after the provider returns the tool call.",
                            }
                        ],
                    },
                )
                return {"content": output, "tool_calls": tool_calls}
            await _respond_to_auxiliary_request(client, event)
            continue

        method = str(event.get("method") or "")
        if payload.get("threadId") and payload.get("threadId") != thread_id:
            continue
        if payload.get("turnId"):
            current_turn_id = str(payload.get("turnId"))
        if method == "item/agentMessage/delta":
            delta = str(payload.get("delta") or "")
            output += delta
            if on_partial:
                await on_partial(output)
            continue
        if method == "turn/completed":
            return {"content": output, "tool_calls": tool_calls}
        if method == "error":
            raise CodexAppServerError(json.dumps(payload, ensure_ascii=False))


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


def _session_config_fingerprint(
    *,
    model: str,
    cwd: str,
    effort: str | None,
    dynamic_tools: list[dict[str, Any]],
) -> str:
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
    return _fingerprint(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _log_session_state(
    state: str,
    session_ref: str,
    *,
    thread_id: str | None = None,
    reason: str | None = None,
    error: str | None = None,
) -> None:
    logger.info(
        "Codex provider session state changed",
        state=state,
        session_ref=session_ref[:12],
        thread_ref=_fingerprint(thread_id)[:12] if thread_id else None,
        reason=reason,
        error=error,
    )


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
