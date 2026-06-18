from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from octopal.infrastructure.config.settings import load_settings
from octopal.infrastructure.jsonl import read_jsonl_dicts

CONTROL_REQUESTS_FILE = "control_requests.jsonl"
CONTROL_ACKS_FILE = "control_acks.jsonl"
PENDING_RESTART_RESUME_FILE = "pending_restart_resume.json"
SELF_RESTART_ACTION = "restart_service"
SELF_RESTART_REQUESTED_BY = "octo_self_restart"
SELF_UPDATE_ACTION = "update_service"
SELF_UPDATE_REQUESTED_BY = "octo_self_update"
RECENT_CONTROL_ACTION_WINDOW_SECONDS = 15 * 60

_CONTROL_SUCCESS_STATUSES = {"executed", "restart_executed"}
_CONTROL_FAILURE_STATUSES = {"cleared", "error", "update_failed", "restart_failed"}


def append_control_request(
    state_dir: Path,
    *,
    action: str,
    reason: str,
    requested_by: str,
    delay_seconds: int = 5,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    created_at = datetime.now(UTC)
    item = {
        "request_id": request_id,
        "created_at": created_at.isoformat(),
        "not_before": (created_at + timedelta(seconds=max(0, int(delay_seconds)))).isoformat(),
        "action": action,
        "reason": reason,
        "requested_by": requested_by,
        "metadata": dict(metadata or {}),
    }
    _append_jsonl(state_dir / CONTROL_REQUESTS_FILE, item)
    return item


def append_control_ack(
    state_dir: Path,
    request_id: str,
    *,
    status: str,
    source: str,
    message: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item = {
        "request_id": request_id,
        "acked_at": datetime.now(UTC).isoformat(),
        "status": status,
        "source": source,
        "message": message,
        "metadata": dict(metadata or {}),
    }
    _append_jsonl(state_dir / CONTROL_ACKS_FILE, item)
    return item


def find_recent_control_action(
    state_dir: Path,
    *,
    action: str,
    requested_by: str,
    chat_id: int | None = None,
    now: datetime | None = None,
    window_seconds: int = RECENT_CONTROL_ACTION_WINDOW_SECONDS,
) -> dict[str, Any] | None:
    current = now or datetime.now(UTC)
    cutoff = current - timedelta(seconds=max(0, int(window_seconds)))
    requests = _read_jsonl(state_dir / CONTROL_REQUESTS_FILE)
    acks = _read_jsonl(state_dir / CONTROL_ACKS_FILE)
    acks_by_id: dict[str, list[dict[str, Any]]] = {}
    for ack in acks:
        request_id = str(ack.get("request_id", "") or "").strip()
        if request_id:
            acks_by_id.setdefault(request_id, []).append(ack)

    for item in reversed(requests):
        if str(item.get("action", "") or "").strip() != action:
            continue
        if str(item.get("requested_by", "") or "").strip() != requested_by:
            continue
        created_at = _parse_datetime(str(item.get("created_at", "") or ""))
        if created_at is None or created_at < cutoff:
            continue
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        if chat_id is not None:
            try:
                item_chat_id = int(metadata.get("chat_id"))
            except Exception:
                item_chat_id = None
            if item_chat_id != int(chat_id):
                continue

        request_id = str(item.get("request_id", "") or "").strip()
        request_acks = acks_by_id.get(request_id, [])
        state = _control_action_state(request_acks)
        if state == "failed":
            continue
        return {
            "request": item,
            "state": state,
            "ack_statuses": [
                str(ack.get("status", "") or "").strip()
                for ack in request_acks
                if str(ack.get("status", "") or "").strip()
            ],
        }
    return None


def _control_action_state(acks: list[dict[str, Any]]) -> str:
    statuses = [str(ack.get("status", "") or "").strip() for ack in acks]
    if any(status in _CONTROL_SUCCESS_STATUSES for status in statuses):
        return "completed"
    if any(status in _CONTROL_FAILURE_STATUSES for status in statuses):
        return "failed"
    return "pending"


def list_unacked_control_requests(state_dir: Path) -> list[dict[str, Any]]:
    requests = _read_jsonl(state_dir / CONTROL_REQUESTS_FILE)
    acks = _read_jsonl(state_dir / CONTROL_ACKS_FILE)
    acked_ids = {str(item.get("request_id", "")).strip() for item in acks}
    return [
        item
        for item in requests
        if str(item.get("request_id", "")).strip()
        and str(item.get("request_id", "")).strip() not in acked_ids
    ]


def due_self_restart_requests(
    state_dir: Path, *, now: datetime | None = None
) -> list[dict[str, Any]]:
    return _due_control_requests(
        state_dir,
        action=SELF_RESTART_ACTION,
        requested_by=SELF_RESTART_REQUESTED_BY,
        now=now,
    )


def due_self_update_requests(
    state_dir: Path, *, now: datetime | None = None
) -> list[dict[str, Any]]:
    return _due_control_requests(
        state_dir,
        action=SELF_UPDATE_ACTION,
        requested_by=SELF_UPDATE_REQUESTED_BY,
        now=now,
    )


def _due_control_requests(
    state_dir: Path,
    *,
    action: str,
    requested_by: str,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    current = now or datetime.now(UTC)
    due: list[dict[str, Any]] = []
    for item in list_unacked_control_requests(state_dir):
        if str(item.get("action", "")).strip() != action:
            continue
        if str(item.get("requested_by", "")).strip() != requested_by:
            continue
        not_before = _parse_datetime(str(item.get("not_before", "") or ""))
        if not_before is not None and not_before > current:
            continue
        due.append(item)
    return due


def check_update_status(project_root: Path | None = None) -> dict[str, Any]:
    from octopal import __version__
    from octopal.cli.main import (
        _detect_release_repo_slug,
        _get_latest_release_info,
        _git_checkout_ready_for_update,
        _is_remote_version_newer,
    )
    from octopal.cli.main import (
        _project_root as cli_project_root,
    )

    root = (project_root or cli_project_root()).resolve()
    settings = load_settings()
    ready, reason = _git_checkout_ready_for_update(root)
    latest_release = _get_latest_release_info(settings)
    latest_version = latest_release[0] if latest_release else None
    release_url = latest_release[1] if latest_release else None
    update_available = (
        _is_remote_version_newer(__version__, latest_version)
        if latest_version is not None
        else False
    )
    return {
        "status": "ok",
        "local_version": __version__,
        "latest_version": latest_version,
        "release_url": release_url,
        "repo": _detect_release_repo_slug(),
        "project_root": str(root),
        "git_ready": bool(ready),
        "git_blocker": reason,
        "update_available": update_available,
        "can_update": bool(ready),
        "update_command": "python -m octopal.cli update",
        "restart_command": "python -m octopal.cli restart",
    }


def write_pending_restart_resume(state_dir: Path, payload: dict[str, Any]) -> Path:
    path = state_dir / PENDING_RESTART_RESUME_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def read_pending_restart_resume(state_dir: Path) -> dict[str, Any] | None:
    path = state_dir / PENDING_RESTART_RESUME_FILE
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def mark_restart_resume_consumed(state_dir: Path) -> None:
    path = state_dir / PENDING_RESTART_RESUME_FILE
    if not path.exists():
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}
    payload["consumed_at"] = datetime.now(UTC).isoformat()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def launch_restart_helper(
    state_dir: Path,
    *,
    request_id: str,
    project_root: Path,
    delay_seconds: int = 1,
) -> None:
    _launch_helper(
        state_dir,
        request_id=request_id,
        project_root=project_root,
        delay_seconds=delay_seconds,
        mode="restart",
    )


def launch_update_helper(
    state_dir: Path,
    *,
    request_id: str,
    project_root: Path,
    delay_seconds: int = 1,
) -> None:
    _launch_helper(
        state_dir,
        request_id=request_id,
        project_root=project_root,
        delay_seconds=delay_seconds,
        mode="update",
    )


def _launch_helper(
    state_dir: Path,
    *,
    request_id: str,
    project_root: Path,
    delay_seconds: int,
    mode: str,
) -> None:
    log_dir = state_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    src_dir = project_root / "src"
    existing_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{src_dir}{os.pathsep}{existing_pp}" if existing_pp else str(src_dir)
    env["OCTOPAL_SKIP_VERSION_CHECK"] = "1"
    args = [
        sys.executable,
        "-m",
        "octopal.runtime.self_control",
        "--request-id",
        request_id,
        "--project-root",
        str(project_root),
        "--state-dir",
        str(state_dir),
        "--delay-seconds",
        str(max(0, int(delay_seconds))),
        "--mode",
        mode,
    ]
    with (
        open(log_dir / "self_restart_stdout.log", "a", encoding="utf-8") as out_file,
        open(log_dir / "self_restart_stderr.log", "a", encoding="utf-8") as err_file,
    ):
        if platform.system() == "Windows":
            subprocess.Popen(
                args,
                cwd=str(project_root),
                env=env,
                stdout=out_file,
                stderr=err_file,
                stdin=subprocess.DEVNULL,
                creationflags=0x00000008,
                close_fds=False,
            )
        else:
            subprocess.Popen(
                args,
                cwd=str(project_root),
                env=env,
                stdout=out_file,
                stderr=err_file,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )


def run_update_helper(
    *,
    request_id: str,
    project_root: Path,
    state_dir: Path,
    delay_seconds: int,
) -> int:
    append_control_ack(
        state_dir,
        request_id,
        status="helper_started",
        source="self_update_helper",
        message="Update helper started.",
    )
    if delay_seconds > 0:
        time.sleep(delay_seconds)

    update_result = _run_cli_command(
        ["update"],
        project_root=project_root,
        timeout_seconds=180,
    )
    append_control_ack(
        state_dir,
        request_id,
        status="update_executed" if update_result["returncode"] == 0 else "update_failed",
        source="self_update_helper",
        message="Update command completed.",
        metadata=update_result,
    )
    if update_result["returncode"] != 0:
        return int(update_result["returncode"])

    restart_result = _run_cli_command(
        ["restart"],
        project_root=project_root,
        timeout_seconds=120,
    )
    append_control_ack(
        state_dir,
        request_id,
        status="restart_executed" if restart_result["returncode"] == 0 else "restart_failed",
        source="self_update_helper",
        message="Restart command completed after update.",
        metadata=restart_result,
    )
    return int(restart_result["returncode"])


def run_restart_helper(
    *,
    request_id: str,
    project_root: Path,
    state_dir: Path,
    delay_seconds: int,
) -> int:
    append_control_ack(
        state_dir,
        request_id,
        status="helper_started",
        source="self_restart_helper",
        message="Restart helper started.",
    )
    if delay_seconds > 0:
        time.sleep(delay_seconds)
    result = _run_cli_command(["restart"], project_root=project_root, timeout_seconds=120)
    status = "executed" if result["returncode"] == 0 else "error"
    append_control_ack(
        state_dir,
        request_id,
        status=status,
        source="self_restart_helper",
        message="Restart command completed.",
        metadata=result,
    )
    return int(result["returncode"])


def _run_cli_command(
    args: list[str],
    *,
    project_root: Path,
    timeout_seconds: int,
) -> dict[str, Any]:
    command = [sys.executable, "-m", "octopal.cli", *args]
    env = os.environ.copy()
    src_dir = project_root / "src"
    existing_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{src_dir}{os.pathsep}{existing_pp}" if existing_pp else str(src_dir)
    env["OCTOPAL_SKIP_VERSION_CHECK"] = "1"
    try:
        proc = subprocess.run(
            command,
            cwd=str(project_root),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except Exception as exc:
        return {
            "returncode": 1,
            "stdout_tail": "",
            "stderr_tail": str(exc),
            "command": " ".join(command),
        }
    return {
        "returncode": proc.returncode,
        "stdout_tail": (proc.stdout or "")[-4000:],
        "stderr_tail": (proc.stderr or "")[-4000:],
        "command": " ".join(command),
    }


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows, _report = read_jsonl_dicts(path, repair=True)
    return rows


def _append_jsonl(path: Path, item: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--project-root", default=str(_project_root()))
    parser.add_argument("--state-dir", default="")
    parser.add_argument("--delay-seconds", type=int, default=1)
    parser.add_argument("--mode", choices=["restart", "update"], default="restart")
    args = parser.parse_args()
    state_dir = Path(args.state_dir).resolve() if args.state_dir else load_settings().state_dir
    kwargs = {
        "request_id": str(args.request_id),
        "project_root": Path(args.project_root).resolve(),
        "state_dir": state_dir,
        "delay_seconds": int(args.delay_seconds),
    }
    if args.mode == "update":
        return run_update_helper(**kwargs)
    return run_restart_helper(**kwargs)


if __name__ == "__main__":
    raise SystemExit(main())
