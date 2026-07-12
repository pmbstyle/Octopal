from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
from ipaddress import ip_address
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from octopal.infrastructure.config.settings import load_settings

_DEFAULT_MAX_CHARS = 20000
_MIN_TIMEOUT_SECONDS = 1.0
_MAX_TIMEOUT_SECONDS = 300.0


def webclaw_enabled() -> bool:
    return bool(_runtime_config()["enabled"])


def webclaw_prefer_local() -> bool:
    return bool(_runtime_config()["prefer_local"])


def webclaw_fetch(args: dict[str, Any]) -> str:
    """Extract one public HTTP(S) page with the local WebClaw binary.

    The hosted WebClaw API is intentionally disabled by removing its API key
    from the child environment. This adapter is an extraction fast path, not a
    replacement for generic HTTP methods handled by ``web_fetch``.
    """

    url = str(args.get("url") or "").strip()
    if not url:
        return _result(ok=False, url=url, error="url is required")
    if not _is_safe_url(url):
        return _result(ok=False, url=url, error="url not allowed")

    runtime = _runtime_config()
    enabled = _coerce_bool(args.get("enabled"), default=bool(runtime["enabled"]))
    if not enabled:
        return _result(
            ok=False,
            url=url,
            error="WebClaw is disabled.",
            failure_reason="disabled",
            available=False,
        )

    binary_setting = str(args.get("binary") or runtime["binary"] or "webclaw").strip()
    binary = _resolve_binary(binary_setting)
    if binary is None:
        return _result(
            ok=False,
            url=url,
            error=f"WebClaw binary not found: {binary_setting}",
            failure_reason="binary_missing",
            available=False,
        )

    max_chars = _bounded_int(args.get("max_chars"), _DEFAULT_MAX_CHARS, 200, 200000)
    timeout_seconds = _bounded_float(
        args.get("timeout_seconds"),
        float(runtime["timeout_seconds"]),
        _MIN_TIMEOUT_SECONDS,
        _MAX_TIMEOUT_SECONDS,
    )
    only_main_content = _coerce_bool(args.get("only_main_content"), default=False)

    command = [binary, url, "--format", "llm", "--timeout", str(max(1, int(timeout_seconds)))]
    if only_main_content:
        command.append("--only-main-content")

    child_env = os.environ.copy()
    child_env.pop("WEBCLAW_API_KEY", None)
    child_env.pop("WEBCLAW_WEBHOOK_URL", None)

    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout_seconds + 2.0,
            env=child_env,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return _result(
            ok=False,
            url=url,
            error=f"WebClaw timed out after {timeout_seconds:g}s.",
            failure_reason="timeout",
            available=True,
        )
    except OSError as exc:
        return _result(
            ok=False,
            url=url,
            error=f"WebClaw failed to start: {exc}",
            failure_reason="start_failed",
            available=False,
        )

    stdout = completed.stdout.strip()
    stderr = completed.stderr.strip()
    if completed.returncode != 0:
        diagnostic = stderr or stdout or f"WebClaw exited with status {completed.returncode}."
        return _result(
            ok=False,
            url=url,
            error=diagnostic[:1000],
            failure_reason=_classify_failure(diagnostic),
            available=True,
            exit_code=completed.returncode,
        )
    warning_failure = _classify_warning(stderr)
    if warning_failure is not None:
        return _result(
            ok=False,
            url=url,
            error=stderr[:1000],
            failure_reason=warning_failure,
            available=True,
            exit_code=completed.returncode,
        )
    if not stdout:
        return _result(
            ok=False,
            url=url,
            error="WebClaw returned empty content.",
            failure_reason="empty_content",
            available=True,
            exit_code=completed.returncode,
        )

    return _result(
        ok=True,
        url=url,
        snippet=stdout[:max_chars],
        content_type="text/markdown",
        available=True,
        exit_code=completed.returncode,
        truncated=len(stdout) > max_chars,
    )


def _runtime_config() -> dict[str, Any]:
    settings = load_settings()
    return {
        "enabled": settings.webclaw_enabled,
        "binary": settings.webclaw_binary,
        "timeout_seconds": settings.webclaw_timeout_seconds,
        "prefer_local": settings.webclaw_prefer_local,
    }


def _resolve_binary(value: str) -> str | None:
    candidate = Path(value).expanduser()
    if candidate.is_absolute() or candidate.parent != Path("."):
        return str(candidate) if candidate.is_file() and os.access(candidate, os.X_OK) else None
    return shutil.which(value)


def _is_safe_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.hostname or "").lower()
    if not host or host == "localhost" or host.endswith(".localhost"):
        return False
    try:
        literal = ip_address(host)
    except ValueError:
        literal = None
    if literal is not None:
        return literal.is_global

    try:
        addresses = {
            ip_address(sockaddr[0])
            for _family, _kind, _proto, _canonname, sockaddr in socket.getaddrinfo(
                host,
                parsed.port or (443 if parsed.scheme == "https" else 80),
                type=socket.SOCK_STREAM,
            )
        }
    except (OSError, ValueError):
        return False
    return bool(addresses) and all(address.is_global for address in addresses)


def _classify_failure(message: str) -> str:
    return _classify_warning(message) or _classify_process_failure(message)


def _classify_warning(message: str) -> str | None:
    lowered = message.lower()
    if "antibot" in lowered or "anti-bot" in lowered or "cloudflare" in lowered:
        return "anti_bot"
    if "javascript" in lowered or "js rendering" in lowered or "spa" in lowered:
        return "js_rendering_required"
    if "consent wall" in lowered or "cookie consent" in lowered:
        return "consent_wall"
    return None


def _classify_process_failure(message: str) -> str:
    lowered = message.lower()
    if "timed out" in lowered or "request timeout" in lowered:
        return "timeout"
    return "process_error"


def _bounded_int(value: Any, default: int, low: int, high: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(low, min(high, parsed))


def _bounded_float(value: Any, default: float, low: float, high: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = default
    return max(low, min(high, parsed))


def _coerce_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _result(*, ok: bool, url: str, **extra: Any) -> str:
    payload = {
        "ok": ok,
        "degraded": False,
        "fallback_used": False,
        "rate_limited": False,
        "source": "webclaw",
        "url": url,
        "cloud_used": False,
        **extra,
    }
    return json.dumps(payload, ensure_ascii=False)
