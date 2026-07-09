from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def main() -> None:
    state_dir = Path(os.getenv("OCTOPAL_STATE_DIR", "data")).resolve()
    req_file = state_dir / "control_requests.jsonl"
    ack_file = state_dir / "control_acks.jsonl"
    offset_file = state_dir / ".control_supervisor.offset"
    state_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info("watching control requests: %s", req_file)
    while True:
        try:
            start = int(offset_file.read_text(encoding="utf-8")) if offset_file.exists() else 0
        except Exception:
            start = 0

        lines = (
            req_file.read_text(encoding="utf-8", errors="ignore").splitlines()
            if req_file.exists()
            else []
        )
        for idx in range(start, len(lines)):
            line = lines[idx].strip()
            if not line:
                continue
            try:
                req = json.loads(line)
                if not isinstance(req, dict):
                    continue
            except Exception:
                continue
            ack = _handle_request(req)
            with ack_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(ack, ensure_ascii=False) + "\n")

        offset_file.write_text(str(len(lines)), encoding="utf-8")
        time.sleep(2.0)


def _handle_request(req: dict[str, Any]) -> dict[str, Any]:
    request_id = str(req.get("request_id", ""))
    action = str(req.get("action", "")).strip()
    reason = str(req.get("reason", "")).strip()
    if action == "status":
        return _ack(request_id, action, "ignored", "status action is informational only")

    if action == "reload_config":
        # Placeholder: in current architecture this is equivalent to restart.
        action = "restart_service"

    if action == "restart_service":
        rc, out, err = _run_shell("python -m octopal.cli restart")
        return _ack(
            request_id,
            "restart_service",
            "ok" if rc == 0 else "error",
            f"reason={reason}",
            rc,
            out,
            err,
        )

    if action == "graceful_shutdown":
        rc, out, err = _run_shell("python -m octopal.cli stop")
        return _ack(
            request_id,
            "graceful_shutdown",
            "ok" if rc == 0 else "error",
            f"reason={reason}",
            rc,
            out,
            err,
        )

    return _ack(request_id, action, "error", f"unsupported action: {action}")


def _ack(
    request_id: str,
    action: str,
    status: str,
    message: str,
    returncode: int | None = None,
    stdout: str | None = None,
    stderr: str | None = None,
) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "action": action,
        "status": status,
        "message": message,
        "returncode": returncode,
        "stdout": (stdout or "")[-2000:],
        "stderr": (stderr or "")[-2000:],
        "acked_at": datetime.now(UTC).isoformat(),
    }


def _run_shell(command: str) -> tuple[int, str, str]:
    try:
        proc = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=120)
        return proc.returncode, proc.stdout, proc.stderr
    except Exception as exc:
        return 1, "", str(exc)


if __name__ == "__main__":
    main()
