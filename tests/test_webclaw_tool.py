from __future__ import annotations

import json
import socket
import subprocess
from types import SimpleNamespace

import pytest

import octopal.tools.web.webclaw as webclaw_mod


@pytest.fixture(autouse=True)
def _public_dns(monkeypatch):
    monkeypatch.setattr(
        webclaw_mod.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))
        ],
    )


def _runtime(*, enabled: bool = True) -> dict[str, object]:
    return {
        "enabled": enabled,
        "binary": "webclaw",
        "timeout_seconds": 30.0,
        "prefer_local": False,
    }


def test_webclaw_fetch_runs_local_binary_without_cloud_credentials(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]
        return SimpleNamespace(returncode=0, stdout="# Example\n\nUseful body", stderr="")

    monkeypatch.setattr(webclaw_mod, "_runtime_config", _runtime)
    monkeypatch.setattr(webclaw_mod, "_resolve_binary", lambda _value: "/opt/webclaw")
    monkeypatch.setattr(webclaw_mod.subprocess, "run", fake_run)
    monkeypatch.setenv("WEBCLAW_API_KEY", "must-not-leak")
    monkeypatch.setenv("WEBCLAW_WEBHOOK_URL", "https://example.com/hook")

    payload = json.loads(
        webclaw_mod.webclaw_fetch(
            {
                "url": "https://example.com",
                "max_chars": 200,
                "only_main_content": True,
            }
        )
    )

    assert payload["ok"] is True
    assert payload["source"] == "webclaw"
    assert payload["cloud_used"] is False
    assert payload["snippet"].startswith("# Example")
    assert captured["command"] == [
        "/opt/webclaw",
        "https://example.com",
        "--format",
        "llm",
        "--timeout",
        "30",
        "--only-main-content",
    ]
    assert "WEBCLAW_API_KEY" not in captured["env"]
    assert "WEBCLAW_WEBHOOK_URL" not in captured["env"]


def test_webclaw_fetch_keeps_complete_extractor_output(monkeypatch) -> None:
    content = "Start " + ("evidence " * 4_000) + "End"

    monkeypatch.setattr(webclaw_mod, "_runtime_config", _runtime)
    monkeypatch.setattr(webclaw_mod, "_resolve_binary", lambda _value: "/opt/webclaw")
    monkeypatch.setattr(
        webclaw_mod.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout=content, stderr=""),
    )

    payload = json.loads(
        webclaw_mod.webclaw_fetch({"url": "https://example.com", "max_chars": 200})
    )

    assert payload["snippet"] == content
    assert payload["content_chars"] == len(content)
    assert payload["truncated"] is False


def test_webclaw_fetch_reports_missing_binary(monkeypatch) -> None:
    monkeypatch.setattr(webclaw_mod, "_runtime_config", _runtime)
    monkeypatch.setattr(webclaw_mod, "_resolve_binary", lambda _value: None)

    payload = json.loads(webclaw_mod.webclaw_fetch({"url": "https://example.com"}))

    assert payload["ok"] is False
    assert payload["available"] is False
    assert payload["failure_reason"] == "binary_missing"


def test_webclaw_fetch_classifies_antibot_failure(monkeypatch) -> None:
    monkeypatch.setattr(webclaw_mod, "_runtime_config", _runtime)
    monkeypatch.setattr(webclaw_mod, "_resolve_binary", lambda _value: "/opt/webclaw")
    monkeypatch.setattr(
        webclaw_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="Anti-bot protection detected; JS rendering required",
        ),
    )

    payload = json.loads(webclaw_mod.webclaw_fetch({"url": "https://example.com"}))

    assert payload["ok"] is False
    assert payload["failure_reason"] == "anti_bot"
    assert payload["cloud_used"] is False


def test_webclaw_fetch_reports_timeout(monkeypatch) -> None:
    monkeypatch.setattr(webclaw_mod, "_runtime_config", _runtime)
    monkeypatch.setattr(webclaw_mod, "_resolve_binary", lambda _value: "/opt/webclaw")

    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="webclaw", timeout=5)

    monkeypatch.setattr(webclaw_mod.subprocess, "run", fake_run)

    payload = json.loads(
        webclaw_mod.webclaw_fetch({"url": "https://example.com", "timeout_seconds": 5})
    )

    assert payload["ok"] is False
    assert payload["failure_reason"] == "timeout"


def test_webclaw_fetch_rejects_success_exit_with_js_warning(monkeypatch) -> None:
    monkeypatch.setattr(webclaw_mod, "_runtime_config", _runtime)
    monkeypatch.setattr(webclaw_mod, "_resolve_binary", lambda _value: "/opt/webclaw")
    monkeypatch.setattr(
        webclaw_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout="> URL: https://example.com\n> Word count: 0",
            stderr="No content extracted. This site requires JavaScript rendering (SPA).",
        ),
    )

    payload = json.loads(webclaw_mod.webclaw_fetch({"url": "https://example.com"}))

    assert payload["ok"] is False
    assert payload["failure_reason"] == "js_rendering_required"


def test_webclaw_fetch_rejects_local_targets(monkeypatch) -> None:
    monkeypatch.setattr(webclaw_mod, "_runtime_config", _runtime)

    payload = json.loads(webclaw_mod.webclaw_fetch({"url": "http://127.0.0.1:8000"}))

    assert payload["ok"] is False
    assert payload["error"] == "url not allowed"


def test_webclaw_fetch_rejects_hostname_resolving_to_private_address(monkeypatch) -> None:
    monkeypatch.setattr(webclaw_mod, "_runtime_config", _runtime)
    monkeypatch.setattr(
        webclaw_mod.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("169.254.169.254", 80))
        ],
    )

    payload = json.loads(webclaw_mod.webclaw_fetch({"url": "http://metadata.internal"}))

    assert payload["ok"] is False
    assert payload["error"] == "url not allowed"
