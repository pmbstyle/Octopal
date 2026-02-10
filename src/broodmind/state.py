from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from broodmind.config.settings import Settings


def _status_path(settings: Settings) -> Path:
    return settings.state_dir / "status.json"


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def write_start_status(settings: Settings) -> None:
    settings.state_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "pid": _current_pid(),
        "started_at": _utc_now_iso(),
        "last_message_at": None,
    }
    _status_path(settings).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def update_last_message(settings: Settings) -> None:
    settings.state_dir.mkdir(parents=True, exist_ok=True)
    path = _status_path(settings)
    payload = {}
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {}
    payload["last_message_at"] = _utc_now_iso()
    if "pid" not in payload:
        payload["pid"] = _current_pid()
    if "started_at" not in payload:
        payload["started_at"] = _utc_now_iso()
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def read_status(settings: Settings) -> dict | None:
    path = _status_path(settings)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _current_pid() -> int:
    import os

    return os.getpid()


def is_pid_running(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        return _is_pid_running_impl(pid)
    except Exception:
        return False


def list_broodmind_runtime_pids() -> list[int]:
    """Return running PIDs that look like `broodmind start` runtime processes."""
    current_pid = os.getpid()
    pids: list[int] = []
    for pid, cmdline in _iter_process_cmdlines():
        if pid == current_pid:
            continue
        if _looks_like_broodmind_runtime_cmd(cmdline):
            pids.append(pid)
    # Stable output for user-facing display and deterministic behavior.
    return sorted(set(pids))


def pid_command_line(pid: int) -> str:
    """Best-effort command line lookup for a PID."""
    for found_pid, cmdline in _iter_process_cmdlines():
        if found_pid == pid:
            return cmdline
    return ""


def _is_pid_running_impl(pid: int) -> bool:
    import os
    import platform

    if platform.system() != "Windows":
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    try:
        import ctypes
        import ctypes.wintypes

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION, False, pid
        )
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    except Exception:
        return False


def _looks_like_broodmind_runtime_cmd(cmdline: str) -> bool:
    lowered = cmdline.lower()
    if "broodmind.cli start" in lowered:
        return True
    if " broodmind start" in f" {lowered}":
        return True
    if " -m broodmind.cli start" in lowered:
        return True
    return False


def _iter_process_cmdlines() -> list[tuple[int, str]]:
    import platform

    if platform.system() == "Linux":
        return _iter_process_cmdlines_linux_procfs()
    return _iter_process_cmdlines_ps()


def _iter_process_cmdlines_linux_procfs() -> list[tuple[int, str]]:
    rows: list[tuple[int, str]] = []
    proc = Path("/proc")
    if not proc.exists():
        return rows
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        cmdline_path = entry / "cmdline"
        try:
            raw = cmdline_path.read_bytes()
        except (PermissionError, FileNotFoundError, ProcessLookupError, OSError):
            continue
        if not raw:
            continue
        parts = [part for part in raw.decode("utf-8", errors="replace").split("\x00") if part]
        if not parts:
            continue
        rows.append((pid, " ".join(parts)))
    return rows


def _iter_process_cmdlines_ps() -> list[tuple[int, str]]:
    rows: list[tuple[int, str]] = []
    try:
        out = subprocess.check_output(
            ["ps", "-eo", "pid=,args="],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except Exception:
        return rows
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(maxsplit=1)
        if not parts:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        cmdline = parts[1] if len(parts) > 1 else ""
        rows.append((pid, cmdline))
    return rows
