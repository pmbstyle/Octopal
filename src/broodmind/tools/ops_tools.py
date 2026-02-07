from __future__ import annotations

import glob
import json
import os
import re
import shlex
import shutil
import socket
import sqlite3
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from broodmind.config.settings import load_settings


def service_health(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    mode = str(args.get("mode", "http")).strip().lower()
    if mode == "http":
        url = str(args.get("url", "")).strip()
        if not url:
            return "service_health error: url is required for mode=http."
        timeout = float(args.get("timeout_seconds", 10) or 10)
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                resp = client.get(url)
            payload = {
                "mode": "http",
                "url": url,
                "ok": 200 <= resp.status_code < 400,
                "status_code": resp.status_code,
            }
            return _json(payload)
        except Exception as exc:
            return _json({"mode": "http", "url": url, "ok": False, "error": str(exc)})

    if mode == "port":
        host = str(args.get("host", "127.0.0.1")).strip() or "127.0.0.1"
        try:
            port = int(args.get("port"))
        except Exception:
            return "service_health error: integer port is required for mode=port."
        timeout = float(args.get("timeout_seconds", 2) or 2)
        ok = False
        err = ""
        try:
            with socket.create_connection((host, port), timeout=timeout):
                ok = True
        except Exception as exc:
            err = str(exc)
        return _json({"mode": "port", "host": host, "port": port, "ok": ok, "error": err})

    if mode == "process":
        name = str(args.get("name", "")).strip()
        if not name:
            return "service_health error: name is required for mode=process."
        if os.name == "nt":
            cmd = f'tasklist /FI "IMAGENAME eq {name}" /FO CSV'
        else:
            cmd = f"ps -eo comm | grep -E '^{re.escape(name)}$' || true"
        rc, out, err = _run_shell(cmd, timeout_seconds=10)
        ok = bool(out and name.lower() in out.lower())
        return _json({"mode": "process", "name": name, "ok": ok, "returncode": rc, "stderr": err[:500]})

    if mode == "docker":
        container = str(args.get("container", "")).strip()
        if not container:
            return "service_health error: container is required for mode=docker."
        cmd = f"docker ps --filter name={_shell_escape(container)} --format '{{{{.Names}}}}'"
        rc, out, err = _run_shell(cmd, timeout_seconds=10)
        names = [line.strip() for line in out.splitlines() if line.strip()]
        ok = any(container in n for n in names)
        return _json({"mode": "docker", "container": container, "ok": ok, "returncode": rc, "stderr": err[:500]})

    return f"service_health error: unsupported mode '{mode}'."


def service_logs(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    mode = str(args.get("mode", "docker")).strip().lower()
    lines = int(args.get("lines", 100) or 100)
    lines = max(10, min(2000, lines))
    grep = str(args.get("grep", "")).strip()

    if mode == "docker":
        container = str(args.get("container", "")).strip()
        if not container:
            return "service_logs error: container is required for mode=docker."
        since = str(args.get("since", "")).strip()
        cmd = f"docker logs --tail {lines} { _shell_escape(container) }"
        if since:
            cmd = f"{cmd} --since {_shell_escape(since)}"
        rc, out, err = _run_shell(cmd, timeout_seconds=20)
        text = out if out else err
        text = _grep_filter(text, grep)
        return _json({"mode": "docker", "container": container, "returncode": rc, "logs": text[-12000:]})

    if mode == "file":
        base_dir = _base_dir(ctx)
        path_raw = str(args.get("path", "")).strip()
        if not path_raw:
            return "service_logs error: path is required for mode=file."
        target = (base_dir / path_raw).resolve()
        if not _is_within(base_dir, target) or not target.exists() or not target.is_file():
            return "service_logs error: invalid file path."
        content = target.read_text(encoding="utf-8", errors="replace")
        tail = "\n".join(content.splitlines()[-lines:])
        tail = _grep_filter(tail, grep)
        return _json({"mode": "file", "path": str(target.relative_to(base_dir)), "logs": tail[-12000:]})

    return f"service_logs error: unsupported mode '{mode}'."


def docker_compose_control(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    action = str(args.get("action", "")).strip().lower()
    allowed = {"ps", "up", "down", "restart", "logs", "exec"}
    if action not in allowed:
        return f"docker_compose_control error: action must be one of {sorted(allowed)}."
    if action in {"down", "restart", "exec"}:
        maybe_error = _require_confirmation(args, action)
        if maybe_error:
            return maybe_error

    services = args.get("services") if isinstance(args.get("services"), list) else []
    services = [str(s).strip() for s in services if str(s).strip()]
    allowed_services = _allowed_services()
    if services and not all(s in allowed_services for s in services):
        return f"docker_compose_control error: service not allowed. Allowed: {sorted(allowed_services)}"

    compose_file = str(args.get("compose_file", "docker-compose.yml")).strip() or "docker-compose.yml"
    base_dir = _base_dir(ctx)
    compose_path = (base_dir / compose_file).resolve()
    if not compose_path.exists():
        return f"docker_compose_control error: compose file not found: {compose_file}"

    cmd = ["docker", "compose", "-f", str(compose_path), action]
    if action == "up" and bool(args.get("detach", True)):
        cmd.append("-d")
    if action == "logs":
        cmd.extend(["--tail", str(int(args.get("lines", 100) or 100))])
    if action == "exec":
        if not services:
            return "docker_compose_control error: exec requires exactly one service."
        exec_command = str(args.get("command", "")).strip()
        if not exec_command:
            return "docker_compose_control error: command is required for action=exec."
        try:
            exec_args = shlex.split(exec_command, posix=True)
        except ValueError as exc:
            return f"docker_compose_control error: invalid command: {exc}"
        if not exec_args:
            return "docker_compose_control error: command is required for action=exec."
        cmd.append(services[0])
        cmd.extend(exec_args)
        services = []
    cmd.extend(services)

    rc, out, err = _run_command(cmd, cwd=base_dir, timeout_seconds=int(args.get("timeout_seconds", 90) or 90))
    return _json({"action": action, "services": services, "returncode": rc, "stdout": out[-12000:], "stderr": err[-4000:]})


def git_ops(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    action = str(args.get("action", "")).strip().lower()
    allowed = {"status", "fetch", "pull", "branch", "log", "show"}
    if action not in allowed:
        return f"git_ops error: action must be one of {sorted(allowed)}."

    base_dir = _base_dir(ctx)
    repo_path = str(args.get("repo_path", ".")).strip() or "."
    repo = (base_dir / repo_path).resolve()
    if not repo.exists():
        return "git_ops error: repo_path does not exist."
    if not _is_within(base_dir, repo):
        return "git_ops error: repo_path outside workspace."

    cmd = ["git", "-C", str(repo)]
    if action == "status":
        cmd.extend(["status", "--short", "--branch"])
    elif action == "fetch":
        cmd.append("fetch")
    elif action == "pull":
        cmd.extend(["pull", "--ff-only"])
    elif action == "branch":
        cmd.extend(["branch", "-vv"])
    elif action == "log":
        limit = max(1, min(100, int(args.get("limit", 20) or 20)))
        cmd.extend(["log", f"-n{limit}", "--oneline", "--decorate"])
    elif action == "show":
        ref = str(args.get("ref", "HEAD")).strip() or "HEAD"
        cmd.extend(["show", "--stat", "--oneline", ref])

    rc, out, err = _run_command(cmd, cwd=repo, timeout_seconds=int(args.get("timeout_seconds", 60) or 60))
    return _json({"action": action, "repo": str(repo.relative_to(base_dir)), "returncode": rc, "stdout": out[-12000:], "stderr": err[-4000:]})


def process_inspect(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    action = str(args.get("action", "list")).strip().lower()
    if action == "list":
        if os.name == "nt":
            cmd = "tasklist /FO TABLE"
        else:
            cmd = "ps -eo pid,comm,%cpu,%mem --sort=-%cpu | head -n 40"
        rc, out, err = _run_shell(cmd, timeout_seconds=15)
        return _json({"action": action, "returncode": rc, "stdout": out[-12000:], "stderr": err[-4000:]})
    if action == "ports":
        cmd = "netstat -ano" if os.name == "nt" else "ss -ltnp || netstat -ltnp"
        rc, out, err = _run_shell(cmd, timeout_seconds=20)
        return _json({"action": action, "returncode": rc, "stdout": out[-12000:], "stderr": err[-4000:]})
    return "process_inspect error: action must be one of ['list','ports']."


def db_backup(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    db = _resolve_db_path(args, ctx)
    if isinstance(db, str):
        return db
    backup_dir = _state_dir() / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    target = backup_dir / f"{db.stem}-{ts}.db"
    try:
        shutil.copy2(db, target)
        return _json({"status": "ok", "backup": str(target), "size": target.stat().st_size})
    except Exception as exc:
        return f"db_backup error: {exc}"


def db_restore(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    maybe_error = _require_confirmation(args, "db_restore")
    if maybe_error:
        return maybe_error
    db = _resolve_db_path(args, ctx)
    if isinstance(db, str):
        return db
    backup_path = str(args.get("backup_path", "")).strip()
    if not backup_path:
        return "db_restore error: backup_path is required."
    src = Path(backup_path).resolve()
    if not src.exists():
        return "db_restore error: backup_path does not exist."
    if src.suffix != ".db":
        return "db_restore error: backup_path must be a .db file."
    try:
        shutil.copy2(src, db)
        return _json({"status": "ok", "restored_to": str(db), "source": str(src)})
    except Exception as exc:
        return f"db_restore error: {exc}"


def db_maintenance(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    db = _resolve_db_path(args, ctx)
    if isinstance(db, str):
        return db
    action = str(args.get("action", "integrity_check")).strip().lower()
    sql = "PRAGMA integrity_check;" if action == "integrity_check" else "VACUUM;"
    if action not in {"integrity_check", "vacuum"}:
        return "db_maintenance error: action must be 'integrity_check' or 'vacuum'."
    try:
        conn = sqlite3.connect(db)
        cur = conn.execute(sql)
        rows = cur.fetchall()
        conn.commit()
        conn.close()
        return _json({"status": "ok", "action": action, "rows": rows[:10]})
    except Exception as exc:
        return f"db_maintenance error: {exc}"


def db_query_readonly(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    db = _resolve_db_path(args, ctx)
    if isinstance(db, str):
        return db
    query = str(args.get("query", "")).strip()
    if not query:
        return "db_query_readonly error: query is required."
    ql = query.lower().lstrip()
    if not ql.startswith("select"):
        return "db_query_readonly error: only SELECT queries are allowed."
    limit = max(1, min(200, int(args.get("limit", 100) or 100)))
    try:
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(query)
        rows = [dict(r) for r in cur.fetchmany(limit)]
        conn.close()
        return _json({"status": "ok", "rows": rows, "count": len(rows)})
    except Exception as exc:
        return f"db_query_readonly error: {exc}"


def secret_scan(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    base_dir = _base_dir(ctx)
    path = str(args.get("path", ".")).strip() or "."
    root = (base_dir / path).resolve()
    if not _is_within(base_dir, root) or not root.exists():
        return "secret_scan error: invalid path."
    patterns = [
        re.compile(r"(?i)api[_-]?key\s*[:=]\s*['\"][^'\"]{8,}"),
        re.compile(r"(?i)token\s*[:=]\s*['\"][^'\"]{8,}"),
        re.compile(r"(?i)secret\s*[:=]\s*['\"][^'\"]{8,}"),
        re.compile(r"-----BEGIN (RSA|EC|OPENSSH) PRIVATE KEY-----"),
    ]
    hits: list[dict[str, Any]] = []
    files = list(root.rglob("*")) if root.is_dir() else [root]
    for file in files:
        if not file.is_file():
            continue
        if file.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".db", ".zip", ".exe", ".dll"}:
            continue
        try:
            text = file.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for idx, line in enumerate(text.splitlines(), start=1):
            for pat in patterns:
                if pat.search(line):
                    hits.append(
                        {
                            "file": str(file.relative_to(base_dir)),
                            "line": idx,
                            "snippet": line[:200],
                        }
                    )
                    break
            if len(hits) >= 200:
                break
        if len(hits) >= 200:
            break
    return _json({"status": "ok", "hits": hits, "count": len(hits)})


def config_audit(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    base_dir = _base_dir(ctx)
    env_file = (base_dir / ".env").resolve()
    required = ["TELEGRAM_BOT_TOKEN", "BROODMIND_LLM_PROVIDER", "ALLOWED_TELEGRAM_CHAT_IDS"]
    present: dict[str, bool] = {}
    config_error = None

    try:
        settings = load_settings()
        present = {
            "TELEGRAM_BOT_TOKEN": bool(settings.telegram_bot_token),
            "BROODMIND_LLM_PROVIDER": bool(settings.llm_provider),
            "ALLOWED_TELEGRAM_CHAT_IDS": bool(settings.allowed_telegram_chat_ids),
            "OPENROUTER_API_KEY": bool(settings.openrouter_api_key),
            "ZAI_API_KEY": bool(settings.zai_api_key),
            "OPENAI_API_KEY": bool(settings.openai_api_key),
        }
    except Exception as exc:
        config_error = str(exc)
        if env_file.exists():
            for line in env_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                if "=" in line and not line.strip().startswith("#"):
                    key = line.split("=", 1)[0].strip()
                    present[key] = True
        present.setdefault("OPENROUTER_API_KEY", False)
        present.setdefault("ZAI_API_KEY", False)
        present.setdefault("OPENAI_API_KEY", False)

    missing = [k for k in required if not present.get(k, False)]
    return _json(
        {
            "status": "ok",
            "env_exists": env_file.exists(),
            "config_error": config_error,
            "missing_required": missing,
            "has_openrouter_key": bool(present.get("OPENROUTER_API_KEY", False)),
            "has_zai_key": bool(present.get("ZAI_API_KEY", False)),
            "has_openai_key": bool(present.get("OPENAI_API_KEY", False)),
        }
    )


def test_run(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    command = str(args.get("command", "python -m pytest -q")).strip()
    try:
        argv = shlex.split(command, posix=True)
    except ValueError as exc:
        return f"test_run error: invalid command: {exc}"
    if not argv:
        return "test_run error: command is required."
    if _contains_shell_control_tokens(argv):
        return "test_run error: shell control operators are not allowed."
    if not _is_allowed_test_command(argv):
        return "test_run error: command must be pytest/ruff/mypy (direct or python -m ...)."
    base_dir = _base_dir(ctx)
    timeout = int(args.get("timeout_seconds", 300) or 300)
    rc, out, err = _run_command(argv, cwd=base_dir, timeout_seconds=timeout)
    return _json({"status": "ok", "returncode": rc, "stdout": out[-12000:], "stderr": err[-4000:]})


def coverage_report(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    base_dir = _base_dir(ctx)
    cov_file = (base_dir / "coverage.xml").resolve()
    if cov_file.exists():
        text = cov_file.read_text(encoding="utf-8", errors="ignore")
        rate_match = re.search(r'line-rate="([0-9.]+)"', text)
        return _json(
            {
                "status": "ok",
                "coverage_xml": str(cov_file.relative_to(base_dir)),
                "line_rate": float(rate_match.group(1)) if rate_match else None,
            }
        )
    return "coverage_report error: coverage.xml not found. Run tests with coverage first."


def artifact_collect(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    base_dir = _base_dir(ctx)
    pattern = str(args.get("pattern", "**/*.log")).strip() or "**/*.log"
    matches = [Path(p) for p in glob.glob(str(base_dir / pattern), recursive=True)]
    files = [str(p.resolve().relative_to(base_dir)) for p in matches if p.is_file()][:200]
    return _json({"status": "ok", "pattern": pattern, "count": len(files), "files": files})


def release_snapshot(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    snapshots_path = _state_dir() / "release_snapshots.json"
    action = str(args.get("action", "create")).strip().lower()
    data = _read_json_file(snapshots_path, default=[])
    if not isinstance(data, list):
        data = []
    if action == "list":
        return _json({"status": "ok", "count": len(data), "snapshots": data[-20:]})
    if action != "create":
        return "release_snapshot error: action must be create or list."
    base_dir = _base_dir(ctx)
    rc, commit, _ = _run_command(["git", "-C", str(base_dir), "rev-parse", "HEAD"], cwd=base_dir, timeout_seconds=15)
    snapshot = {
        "id": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
        "created_at": datetime.now(UTC).isoformat(),
        "commit": commit.strip() if rc == 0 else "",
        "note": str(args.get("note", "")).strip(),
    }
    data.append(snapshot)
    _write_json_file(snapshots_path, data)
    return _json({"status": "ok", "snapshot": snapshot})


def rollback_release(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    maybe_error = _require_confirmation(args, "rollback_release")
    if maybe_error:
        return maybe_error
    snapshots_path = _state_dir() / "release_snapshots.json"
    data = _read_json_file(snapshots_path, default=[])
    if not isinstance(data, list) or not data:
        return "rollback_release error: no snapshots available."
    snapshot_id = str(args.get("snapshot_id", "")).strip()
    snap = next((s for s in data if s.get("id") == snapshot_id), None) if snapshot_id else data[-1]
    if not snap:
        return f"rollback_release error: snapshot '{snapshot_id}' not found."
    commit = str(snap.get("commit", "")).strip()
    if not commit:
        return "rollback_release error: snapshot has no commit."
    base_dir = _base_dir(ctx)
    rc, out, err = _run_command(["git", "-C", str(base_dir), "checkout", commit], cwd=base_dir, timeout_seconds=30)
    return _json({"status": "ok" if rc == 0 else "error", "snapshot": snap, "returncode": rc, "stdout": out[-4000:], "stderr": err[-4000:]})


def self_control(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    action = str(args.get("action", "")).strip().lower()
    allowed = {"restart_service", "graceful_shutdown", "reload_config", "status"}
    if action not in allowed:
        return f"self_control error: action must be one of {sorted(allowed)}."

    req_file = _state_dir() / "control_requests.jsonl"
    ack_file = _state_dir() / "control_acks.jsonl"
    if action == "status":
        req = _read_jsonl(req_file)[-5:]
        ack = _read_jsonl(ack_file)[-5:]
        return _json({"status": "ok", "recent_requests": req, "recent_acks": ack})
    maybe_error = _require_confirmation(args, action)
    if maybe_error:
        return maybe_error

    request_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    item = {
        "request_id": request_id,
        "created_at": datetime.now(UTC).isoformat(),
        "action": action,
        "reason": str(args.get("reason", "")).strip(),
        "requested_by": "queen",
    }
    _append_jsonl(req_file, item)
    return _json({"status": "requested", "request": item})


def _base_dir(ctx: dict[str, Any]) -> Path:
    base = ctx.get("base_dir")
    if isinstance(base, Path):
        return base.resolve()
    if isinstance(base, str):
        return Path(base).resolve()
    return Path("workspace").resolve()


def _state_dir() -> Path:
    p = Path(os.getenv("BROODMIND_STATE_DIR", "data")).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _resolve_db_path(args: dict[str, Any], ctx: dict[str, Any]) -> Path | str:
    db_path_raw = str(args.get("db_path", "")).strip()
    db = Path(db_path_raw).resolve() if db_path_raw else (_state_dir() / "broodmind.db").resolve()
    if not db.exists():
        return f"database file does not exist: {db}"
    return db


def _run_shell(command: str, cwd: Path | None = None, timeout_seconds: int = 30) -> tuple[int, str, str]:
    try:
        proc = subprocess.run(
            command,
            shell=True,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except Exception as exc:
        return 1, "", str(exc)


def _run_command(command: list[str], cwd: Path | None = None, timeout_seconds: int = 30) -> tuple[int, str, str]:
    try:
        proc = subprocess.run(
            command,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except Exception as exc:
        return 1, "", str(exc)


def _allowed_services() -> set[str]:
    raw = os.getenv("BROODMIND_ALLOWED_SERVICES", "broodmind,ast,tts,infer,translator")
    return {s.strip() for s in raw.split(",") if s.strip()}


def _shell_escape(value: str) -> str:
    return json.dumps(value)


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _grep_filter(text: str, pattern: str) -> str:
    if not pattern:
        return text
    regex = re.compile(pattern, flags=re.IGNORECASE)
    return "\n".join([line for line in text.splitlines() if regex.search(line)])


def _is_allowed_test_command(argv: list[str]) -> bool:
    if not argv:
        return False
    if argv[0] in {"pytest", "ruff", "mypy"}:
        return True
    return len(argv) >= 3 and argv[0] == "python" and argv[1] == "-m" and argv[2] in {"pytest", "ruff", "mypy"}


def _contains_shell_control_tokens(argv: list[str]) -> bool:
    blocked = {"&&", "||", ";", "|", ">", ">>", "<"}
    return any(token in blocked for token in argv)


test_run.__test__ = False


def _is_within(base_dir: Path, target: Path) -> bool:
    try:
        base = base_dir.resolve()
        return base == target or base in target.parents
    except Exception:
        return False


def _append_jsonl(path: Path, item: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
            if isinstance(item, dict):
                out.append(item)
        except Exception:
            continue
    return out


def _read_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _require_confirmation(args: dict[str, Any], action: str) -> str | None:
    if bool(args.get("confirm", False)):
        return None
    return f"{action} requires explicit confirmation: set confirm=true."
