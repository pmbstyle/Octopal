from __future__ import annotations

import json
import subprocess
import time
import uuid
import shlex
import sys
from pathlib import Path
from typing import Any

# Registry for background processes: {session_id: {"process": Popen, "start_time": float, "buffer": list}}
_PROCESS_REGISTRY: dict[str, dict[str, Any]] = {}


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
    action = args.get("action", "start")
    
    if action == "start":
        return _handle_start(args, base_dir)
    elif action in {"poll", "kill", "write", "read"}:
        return _handle_management(action, args)
    else:
        return f"exec_run error: Unknown action '{action}'"


def _read_stream(stream: Any) -> str:
    """Non-blocking read from a stream."""
    if not stream:
        return ""
    
    # Try reading available lines without blocking
    output = []
    import sys
    
    if sys.platform == "win32":
        # Windows doesn't support select() on pipes.
        # We can't easily peek into the pipe without blocking or threads.
        # However, since we are in a synchronous tool call, simply falling back 
        # to a short blocking read or just returning what we can isn't trivial.
        #
        # A robust solution for Windows sync non-blocking reads requires 
        # named pipes or threads. For this MVP, we will assume the caller 
        # accepts that 'poll' might not return ALL output immediately 
        # unless we use threads to populate a buffer.
        
        # NOTE: For this specific implementation, we'll return a note that 
        # full streaming requires the threaded implementation (see below).
        return "(Output streaming on Windows requires threaded buffer implementation)"
    else:
        # Unix-like systems can use select
        import select
        while True:
            reads, _, _ = select.select([stream], [], [], 0.0)
            if stream in reads:
                line = stream.readline()
                if line:
                    output.append(line)
                else:
                    break
            else:
                break
                
    return "".join(output)

# -------------------------------------------------------------------------
# THREADED BUFFER IMPLEMENTATION FOR ROBUST CROSS-PLATFORM NON-BLOCKING I/O
# -------------------------------------------------------------------------
# To properly solve the reading issue on Windows, we spin up daemon threads
# that consume stdout/stderr and push to a queue.

import threading
import queue

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
            )
            session_id = str(uuid.uuid4())
            
            # Use threaded buffer wrapper
            pb = ProcessBuffer(process)
            
            _PROCESS_REGISTRY[session_id] = {
                "process": process,
                "start_time": time.time(),
                "command": command,
                "buffer": pb
            }
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
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
        
        del _PROCESS_REGISTRY[session_id]
        return json.dumps({"status": "killed", "session_id": session_id})

    return "exec_run error: Unreachable."

def _read_stream(stream: Any) -> str:
    # Deprecated by ProcessBuffer
    return ""
