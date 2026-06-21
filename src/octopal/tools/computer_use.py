from __future__ import annotations

import json
import platform
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

_DEFAULT_TIMEOUT_SECONDS = 45
_MAX_TIMEOUT_SECONDS = 120
_SAFE_ACTIONS = {"status", "check_permissions", "list_apps", "list_windows", "capture", "wait"}
_MUTATING_ACTIONS = {"click", "type", "key", "scroll"}
_ALLOWED_ACTIONS = _SAFE_ACTIONS | _MUTATING_ACTIONS
_KEY_ALIASES = {
    "command": "cmd",
    "control": "ctrl",
    "alt": "option",
}
_BLOCKED_KEY_COMBOS = {
    frozenset({"cmd", "shift", "backspace"}),
    frozenset({"cmd", "option", "backspace"}),
    frozenset({"cmd", "ctrl", "q"}),
    frozenset({"cmd", "shift", "q"}),
    frozenset({"cmd", "option", "shift", "q"}),
}
_BLOCKED_TYPE_PATTERNS = (
    re.compile(r"curl\s+[^|]*\|\s*(?:bash|sh)", re.IGNORECASE),
    re.compile(r"wget\s+[^|]*\|\s*(?:bash|sh)", re.IGNORECASE),
    re.compile(r"\bsudo\s+rm\s+-[rf]", re.IGNORECASE),
    re.compile(r"\brm\s+-rf\s+/\s*$", re.IGNORECASE),
    re.compile(r":\s*\(\)\s*\{\s*:\|:\s*&\s*\}", re.IGNORECASE),
)


COMPUTER_USE_TOOL_PARAMETERS: dict[str, Any] = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": sorted(_ALLOWED_ACTIONS),
            "description": (
                "Desktop action. Start with status/check_permissions, then list_windows or "
                "capture. Mutating actions require explicit user intent."
            ),
        },
        "pid": {"type": "integer", "description": "Target process ID from list_windows/list_apps."},
        "window_id": {"type": "integer", "description": "Target window ID from list_windows."},
        "capture_mode": {
            "type": "string",
            "enum": ["som", "vision", "ax"],
            "description": "Capture mode for capture: som=AX+screenshot, vision=screenshot, ax=AX only.",
        },
        "query": {"type": "string", "description": "Optional capture tree filter."},
        "on_screen_only": {"type": "boolean", "description": "Only list on-screen windows."},
        "element_index": {"type": "integer", "description": "Element index from the last capture."},
        "x": {"type": "number", "description": "Window-local screenshot x coordinate."},
        "y": {"type": "number", "description": "Window-local screenshot y coordinate."},
        "text": {"type": "string", "description": "Text to type into the target process/element."},
        "key": {
            "type": "string",
            "description": "Key name for key action, e.g. return, escape, tab, s.",
        },
        "modifiers": {
            "type": "array",
            "items": {"type": "string", "enum": ["cmd", "shift", "option", "alt", "ctrl", "fn"]},
            "description": "Modifier keys for key/click.",
        },
        "direction": {
            "type": "string",
            "enum": ["up", "down", "left", "right"],
            "description": "Scroll direction.",
        },
        "amount": {"type": "integer", "minimum": 1, "maximum": 50, "description": "Scroll amount."},
        "by": {"type": "string", "enum": ["line", "page"], "description": "Scroll granularity."},
        "seconds": {"type": "number", "minimum": 0, "maximum": 30, "description": "Wait duration."},
        "session": {"type": "string", "description": "Optional cua-driver session id."},
    },
    "required": ["action"],
    "additionalProperties": False,
}


def computer_use(args: dict[str, Any], ctx: dict[str, Any] | None = None) -> str:
    """Drive the local macOS desktop through cua-driver."""
    ctx = ctx or {}
    action = str((args or {}).get("action") or "").strip().lower()
    if action not in _ALLOWED_ACTIONS:
        return _json_error(
            "unsupported_action",
            f"Unsupported computer_use action {action!r}.",
            {"allowed_actions": sorted(_ALLOWED_ACTIONS)},
        )
    if platform.system() != "Darwin":
        return _json_error("unsupported_platform", "computer_use is only available on macOS.")

    driver = _resolve_driver_command()
    if driver is None:
        return _json_error(
            "driver_missing",
            "cua-driver is not installed or not on PATH.",
            {
                "install_hint": (
                    '/bin/bash -c "$(curl -fsSL '
                    'https://raw.githubusercontent.com/trycua/cua/main/libs/cua-driver/scripts/install.sh)"'
                )
            },
        )

    blocked = _blocked_action_reason(action, args or {})
    if blocked:
        return _json_error("blocked", blocked, {"action": action})

    if action == "status":
        return _handle_status(driver)
    if action == "wait":
        seconds = max(0.0, min(float((args or {}).get("seconds", 1.0) or 1.0), 30.0))
        time.sleep(seconds)
        return _json_ok({"action": "wait", "seconds": seconds})

    command_args, tool_args = _build_cua_command(action, args or {}, ctx)
    if command_args is None:
        return _json_error(
            "bad_arguments", "Required arguments are missing or invalid.", {"action": action}
        )

    timeout = _timeout_for_action(action)
    result = subprocess.run(
        [driver, *command_args, json.dumps(tool_args, ensure_ascii=False)],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return _normalize_cli_result(action, command_args[0], tool_args, result)


def _resolve_driver_command() -> str | None:
    configured = shutil.which("cua-driver")
    if configured:
        return configured
    app_binary = Path("/Applications/CuaDriver.app/Contents/MacOS/cua-driver")
    if app_binary.exists():
        return str(app_binary)
    return None


def _build_cua_command(
    action: str,
    args: dict[str, Any],
    ctx: dict[str, Any],
) -> tuple[list[str], dict[str, Any]] | tuple[None, None]:
    if action == "check_permissions":
        return ["check_permissions"], {}
    if action == "list_apps":
        return ["list_apps"], {}
    if action == "list_windows":
        payload: dict[str, Any] = {}
        if "pid" in args:
            payload["pid"] = _coerce_int(args.get("pid"))
        if "on_screen_only" in args:
            payload["on_screen_only"] = bool(args.get("on_screen_only"))
        return ["list_windows"], _drop_none(payload)
    if action == "capture":
        pid = _coerce_int(args.get("pid"))
        window_id = _coerce_int(args.get("window_id"))
        if pid is None or window_id is None:
            return None, None
        payload = {
            "pid": pid,
            "window_id": window_id,
            "capture_mode": str(args.get("capture_mode") or "som"),
        }
        if args.get("query"):
            payload["query"] = str(args["query"])
        if args.get("session"):
            payload["session"] = str(args["session"])
        if payload["capture_mode"] != "ax":
            payload["screenshot_out_file"] = str(_screenshot_path(ctx))
        return ["get_window_state"], payload
    if action == "click":
        pid = _coerce_int(args.get("pid"))
        if pid is None:
            return None, None
        payload = {
            "pid": pid,
            "window_id": _coerce_int(args.get("window_id")),
            "element_index": _coerce_int(args.get("element_index")),
            "x": _coerce_number(args.get("x")),
            "y": _coerce_number(args.get("y")),
            "modifier": _normalize_modifiers(args.get("modifiers")),
        }
        if args.get("session"):
            payload["session"] = str(args["session"])
        return ["click"], _drop_none(payload)
    if action == "type":
        pid = _coerce_int(args.get("pid"))
        text = args.get("text")
        if pid is None or text is None:
            return None, None
        payload = {
            "pid": pid,
            "text": str(text),
            "window_id": _coerce_int(args.get("window_id")),
            "element_index": _coerce_int(args.get("element_index")),
        }
        if args.get("session"):
            payload["session"] = str(args["session"])
        return ["type_text"], _drop_none(payload)
    if action == "key":
        pid = _coerce_int(args.get("pid"))
        key = str(args.get("key") or "").strip()
        if pid is None or not key:
            return None, None
        payload = {
            "pid": pid,
            "key": key,
            "window_id": _coerce_int(args.get("window_id")),
            "element_index": _coerce_int(args.get("element_index")),
            "modifiers": _normalize_modifiers(args.get("modifiers")),
        }
        if args.get("session"):
            payload["session"] = str(args["session"])
        return ["press_key"], _drop_none(payload)
    if action == "scroll":
        pid = _coerce_int(args.get("pid"))
        direction = str(args.get("direction") or "").strip().lower()
        if pid is None or direction not in {"up", "down", "left", "right"}:
            return None, None
        payload = {
            "pid": pid,
            "direction": direction,
            "amount": max(1, min(_coerce_int(args.get("amount")) or 3, 50)),
            "by": str(args.get("by") or "line"),
            "window_id": _coerce_int(args.get("window_id")),
            "element_index": _coerce_int(args.get("element_index")),
        }
        if args.get("session"):
            payload["session"] = str(args["session"])
        return ["scroll"], _drop_none(payload)
    return None, None


def _handle_status(driver: str) -> str:
    version = _run_probe([driver, "--version"])
    permissions = _run_probe([driver, "check_permissions", "{}"])
    permissions_error = None
    if permissions.get("returncode"):
        permissions_error = permissions.get("stderr") or None
    return _json_ok(
        {
            "action": "status",
            "driver": driver,
            "version": version.get("stdout", "").strip() or None,
            "permissions": _parse_json_or_text(permissions.get("stdout", "")),
            "permissions_error": permissions_error,
        }
    )


def _normalize_cli_result(
    action: str,
    cua_tool: str,
    tool_args: dict[str, Any],
    result: subprocess.CompletedProcess[str],
) -> str:
    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    payload: dict[str, Any] = {
        "ok": result.returncode == 0,
        "action": action,
        "cua_tool": cua_tool,
        "returncode": result.returncode,
        "result": _parse_json_or_text(stdout),
    }
    screenshot = tool_args.get("screenshot_out_file")
    if screenshot and Path(str(screenshot)).exists():
        payload["screenshot_path"] = str(screenshot)
    if stderr:
        payload["stderr"] = stderr[-4000:]
    if result.returncode != 0:
        payload["error"] = stderr or stdout or f"cua-driver {cua_tool} failed"
    return json.dumps(payload, ensure_ascii=False)


def _run_probe(command: list[str]) -> dict[str, Any]:
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return {
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }
    except Exception as exc:
        return {"returncode": 1, "stdout": "", "stderr": str(exc)}


def _blocked_action_reason(action: str, args: dict[str, Any]) -> str | None:
    if action == "type":
        text = str(args.get("text") or "")
        for pattern in _BLOCKED_TYPE_PATTERNS:
            if pattern.search(text):
                return "Dangerous shell text cannot be typed via computer_use."
    if action == "key":
        combo = frozenset(
            _normalize_modifiers(args.get("modifiers"))
            + [str(args.get("key") or "").strip().lower()]
        )
        if any(blocked.issubset(combo) for blocked in _BLOCKED_KEY_COMBOS):
            return "Destructive macOS system shortcuts are blocked."
    return None


def _normalize_modifiers(value: Any) -> list[str]:
    if value is None:
        return []
    raw_values = value if isinstance(value, list) else [value]
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        item = _KEY_ALIASES.get(str(raw).strip().lower(), str(raw).strip().lower())
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _screenshot_path(ctx: dict[str, Any]) -> Path:
    workspace = (
        getattr((ctx or {}).get("octo"), "workspace_dir", None)
        or (ctx or {}).get("workspace_dir")
        or (ctx or {}).get("base_dir")
        or (ctx or {}).get("workspace_root")
    )
    base = Path(workspace) if workspace else Path.cwd() / "workspace"
    out_dir = base / "artifacts" / "computer_use"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"capture-{uuid4().hex}.png"


def _timeout_for_action(action: str) -> int:
    if action == "capture":
        return _MAX_TIMEOUT_SECONDS
    return _DEFAULT_TIMEOUT_SECONDS


def _parse_json_or_text(value: str) -> Any:
    stripped = value.strip()
    if not stripped:
        return ""
    if stripped.startswith(("{", "[")):
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return stripped
    return stripped


def _drop_none(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None and item != []}


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_number(value: Any) -> float | int | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else number


def _json_ok(payload: dict[str, Any]) -> str:
    return json.dumps({"ok": True, **payload}, ensure_ascii=False)


def _json_error(code: str, message: str, details: dict[str, Any] | None = None) -> str:
    payload: dict[str, Any] = {"ok": False, "error": message, "code": code}
    if details:
        payload["details"] = details
    return json.dumps(payload, ensure_ascii=False)
