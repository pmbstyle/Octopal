from __future__ import annotations

import contextlib
import json
import os
import queue
import signal
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from octopal.runtime.metrics import update_component_gauges

# Registry for background processes: {session_id: {"process": Popen, "start_time": float, "buffer": list}}
_PROCESS_REGISTRY: dict[str, dict[str, Any]] = {}
_MAX_BACKGROUND_SESSIONS = 64
_ENDED_SESSION_TTL_SECONDS = 600.0


def _publish_runtime_metrics() -> None:
    running = 0
    ended = 0
    for session in _PROCESS_REGISTRY.values():
        proc = session.get("process")
        if proc is not None and proc.poll() is None:
            running += 1
        else:
            ended += 1
    update_component_gauges(
        "exec_run",
        {
            "background_sessions_total": len(_PROCESS_REGISTRY),
            "background_sessions_running": running,
            "background_sessions_ended": ended,
        },
    )


def exec_run(args: dict[str, Any], base_dir: Path) -> str:
    """
    Execute a shell command. Supports foreground (blocking) and background (async) execution.

    Args:
        command (str): Shell command to run.
        timeout_seconds (int): Timeout for blocking calls (default 20).
        background (bool): If True, runs process in background and returns session_id.
        action (str): Management action: "start" (default), "poll", "kill", "write", "read".
        session_id (str): Required for "poll", "kill", "write", "read".
        input_data (str): Data to write to stdin (for "write" action).
    """
    _prune_process_registry()
    _publish_runtime_metrics()
    action = args.get("action", "start")

    if action == "start":
        return _handle_start(args, base_dir)
    elif action in {"poll", "kill", "write", "read"}:
        return _handle_management(action, args)
    else:
        return f"exec_run error: Unknown action '{action}'"


def cleanup_background_sessions() -> int:
    """Terminate all tracked background sessions for the current worker process."""
    session_ids = list(_PROCESS_REGISTRY.keys())
    cleaned = 0
    for session_id in session_ids:
        session = _PROCESS_REGISTRY.get(session_id)
        if session is None:
            continue
        _terminate_session_process(session)
        _close_session_pipes(session)
        _PROCESS_REGISTRY.pop(session_id, None)
        cleaned += 1
    if cleaned:
        _publish_runtime_metrics()
    return cleaned


# -------------------------------------------------------------------------
# THREADED BUFFER IMPLEMENTATION FOR ROBUST CROSS-PLATFORM NON-BLOCKING I/O
# -------------------------------------------------------------------------
# To properly solve the reading issue on Windows, we spin up daemon threads
# that consume stdout/stderr and push to a queue.


class ProcessBuffer:
    def __init__(self, process: subprocess.Popen):
        self.process = process
        self.stdout_queue = queue.Queue()
        self.stderr_queue = queue.Queue()

        self.t_out = threading.Thread(target=self._reader, args=(process.stdout, self.stdout_queue))
        self.t_out.daemon = True
        self.t_out.start()

        self.t_err = threading.Thread(target=self._reader, args=(process.stderr, self.stderr_queue))
        self.t_err.daemon = True
        self.t_err.start()

    def _reader(self, pipe, q):
        try:
            for line in iter(pipe.readline, ''):
                q.put(line)
        except ValueError:
            pass # Pipe closed

    def read_stdout(self) -> str:
        return self._read_queue(self.stdout_queue)

    def read_stderr(self) -> str:
        return self._read_queue(self.stderr_queue)

    def _read_queue(self, q: queue.Queue) -> str:
        lines = []
        while True:
            try:
                line = q.get_nowait()
                lines.append(line)
            except queue.Empty:
                break
        return "".join(lines)


def _handle_start(args: dict[str, Any], base_dir: Path) -> str:
    command = str(args.get("command", "")).strip()
    if not command:
        return "exec_run error: command is required for 'start' action."

    timeout_seconds = int(args.get("timeout_seconds", 20) or 20)
    is_background = bool(args.get("background", False))

    try:
        if is_background:
            if len(_PROCESS_REGISTRY) >= _MAX_BACKGROUND_SESSIONS:
                return (
                    "exec_run error: too many active background sessions. "
                    "Kill old sessions before starting new ones."
                )
            # Start background process
            process = subprocess.Popen(
                command,
                cwd=str(base_dir),
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE,
                text=True,
                bufsize=1, # Line buffered
                **_background_popen_kwargs(),
            )
            session_id = str(uuid.uuid4())

            # Use threaded buffer wrapper
            pb = ProcessBuffer(process)

            _PROCESS_REGISTRY[session_id] = {
                "process": process,
                "start_time": time.time(),
                "command": command,
                "buffer": pb,
                "ended_at": None,
            }
            _publish_runtime_metrics()
            return json.dumps({
                "status": "started",
                "session_id": session_id,
                "message": "Process started in background."
            })
        else:
            # Blocking execution (legacy behavior)
            result = subprocess.run(
                command,
                cwd=str(base_dir),
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
            payload = {
                "returncode": result.returncode,
                "stdout": result.stdout[:4000],
                "stderr": result.stderr[:4000],
            }
            return json.dumps(payload, ensure_ascii=False)

    except subprocess.TimeoutExpired:
        return "exec_run error: Command timed out."
    except Exception as exc:
        return f"exec_run error: {exc}"

def _handle_management(action: str, args: dict[str, Any]) -> str:
    session_id = args.get("session_id")
    if not session_id:
        return "exec_run error: session_id is required for management actions."

    session = _PROCESS_REGISTRY.get(session_id)
    if not session:
        return f"exec_run error: Session '{session_id}' not found or expired."

    proc = session["process"]
    pb = session["buffer"]

    if action == "poll":
        returncode = proc.poll()
        is_running = returncode is None
        if not is_running and session.get("ended_at") is None:
            session["ended_at"] = time.time()

        stdout_chunk = pb.read_stdout()
        stderr_chunk = pb.read_stderr()

        return json.dumps({
            "session_id": session_id,
            "running": is_running,
            "returncode": returncode,
            "stdout_new": stdout_chunk,
            "stderr_new": stderr_chunk,
        })

    elif action == "read":
        if proc.poll() is not None and session.get("ended_at") is None:
            session["ended_at"] = time.time()
        stdout_chunk = pb.read_stdout()
        stderr_chunk = pb.read_stderr()
        return json.dumps({
            "session_id": session_id,
            "stdout_new": stdout_chunk,
            "stderr_new": stderr_chunk,
        })

    elif action == "write":
        input_data = args.get("input_data", "")
        if not input_data:
            return "exec_run error: input_data required for write."

        if proc.poll() is not None:
             if session.get("ended_at") is None:
                 session["ended_at"] = time.time()
             return "exec_run error: Process is not running."

        try:
            if proc.stdin:
                proc.stdin.write(input_data)
                proc.stdin.flush()
                return json.dumps({"status": "ok", "message": f"Wrote {len(input_data)} chars"})
            else:
                return "exec_run error: Process stdin is not available."
        except Exception as e:
             return f"exec_run error: Failed to write to process: {e}"

    elif action == "kill":
        _terminate_session_process(session)
        _close_session_pipes(session)
        del _PROCESS_REGISTRY[session_id]
        _publish_runtime_metrics()
        return json.dumps({"status": "killed", "session_id": session_id})

    return "exec_run error: Unreachable."


def _prune_process_registry(now: float | None = None) -> None:
    if now is None:
        now = time.time()

    stale: list[str] = []
    for session_id, session in _PROCESS_REGISTRY.items():
        proc = session.get("process")
        ended_at = session.get("ended_at")
        if proc is None:
            stale.append(session_id)
            continue
        if proc.poll() is None:
            continue
        if ended_at is None:
            session["ended_at"] = now
            ended_at = now
        if (now - float(ended_at)) > _ENDED_SESSION_TTL_SECONDS:
            stale.append(session_id)

    for session_id in stale:
        _PROCESS_REGISTRY.pop(session_id, None)
    if stale:
        _publish_runtime_metrics()


def _background_popen_kwargs() -> dict[str, Any]:
    if os.name == "nt":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    return {"start_new_session": True}


def _terminate_session_process(session: dict[str, Any]) -> None:
    proc = session.get("process")
    if proc is None or proc.poll() is not None:
        return

    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=2)
        return

    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGTERM)
    try:
        proc.wait(timeout=2)
        return
    except subprocess.TimeoutExpired:
        pass
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGKILL)
    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(timeout=2)


def _close_session_pipes(session: dict[str, Any]) -> None:
    proc = session.get("process")
    if proc is None:
        return
    for pipe_name in ("stdin", "stdout", "stderr"):
        pipe = getattr(proc, pipe_name, None)
        if pipe is None:
            continue
        with contextlib.suppress(Exception):
            pipe.close()
