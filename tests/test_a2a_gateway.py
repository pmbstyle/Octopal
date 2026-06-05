from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from octopal.gateway.app import build_app
from octopal.infrastructure.config.models import A2AConfig, A2APeerConfig
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.logging import correlation_id_var
from octopal.interop.a2a.client import A2AClientError, _message_send_endpoint
from octopal.interop.a2a.routes import register_a2a_routes
from octopal.runtime.tool_payloads import render_tool_result_for_llm
from octopal.tools.communication import a2a as a2a_tools
from octopal.tools.communication.a2a import a2a_list_peers


class _DummyOcto:
    def __init__(self, immediate: str = "hello peer") -> None:
        self.immediate = immediate
        self.calls: list[dict[str, object]] = []
        self.suppressed_channel_followups: list[dict[str, object]] = []

    async def handle_message(self, text: str, chat_id: int, **kwargs):
        self.calls.append(
            {
                "text": text,
                "chat_id": chat_id,
                "kwargs": kwargs,
                "correlation_id": correlation_id_var.get(),
            }
        )
        return SimpleNamespace(immediate=self.immediate)

    def suppress_channel_followups(self, correlation_id: str, *, reason: str | None = None):
        self.suppressed_channel_followups.append(
            {"correlation_id": correlation_id, "reason": reason}
        )


def _app(config: A2AConfig, octo: object | None = None, state_dir: Path | None = None) -> FastAPI:
    app = FastAPI()
    app.state.settings = SimpleNamespace(a2a=config, state_dir=state_dir or Path("data"))
    app.state.octo = octo
    register_a2a_routes(app)
    return app


def test_agent_card_is_hidden_when_a2a_disabled() -> None:
    client = TestClient(_app(A2AConfig(enabled=False)))

    response = client.get("/.well-known/agent-card.json")

    assert response.status_code == 404


def test_agent_card_exposes_minimal_public_capabilities_when_enabled() -> None:
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                public_base_url="https://octo.example",
                agent_name="Alice",
            )
        )
    )

    response = client.get("/.well-known/agent-card.json")

    assert response.status_code == 200
    payload = response.json()
    assert payload["name"] == "Alice"
    assert payload["supportedInterfaces"][0]["url"] == "https://octo.example/a2a/v1"
    assert payload["securityRequirements"] == [{"peerBearer": []}]
    assert "application/json" in payload["defaultInputModes"]
    assert "application/octet-stream" in payload["skills"][0]["inputModes"]
    assert payload["skills"][0]["id"] == "peer-chat"


def test_agent_card_route_is_registered_before_dashboard_catchall() -> None:
    app = build_app(
        Settings(
            a2a=A2AConfig(
                enabled=True,
                public_base_url="https://octo.example",
            )
        ),
        octo=None,
    )
    client = TestClient(app)

    response = client.get("/.well-known/agent-card.json")

    assert response.status_code == 200
    assert response.json()["supportedInterfaces"][0]["url"] == "https://octo.example/a2a/v1"


def test_message_send_requires_configured_peer_token() -> None:
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                peers={"bob": A2APeerConfig(token="secret")},
            ),
            octo=_DummyOcto(),
        )
    )

    response = client.post(
        "/a2a/v1/message:send",
        json={"message": {"role": "ROLE_USER", "parts": [{"text": "hi"}]}},
    )

    assert response.status_code == 401


def test_message_send_rejects_unknown_peer_token() -> None:
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                peers={"bob": A2APeerConfig(token="secret")},
            ),
            octo=_DummyOcto(),
        )
    )

    response = client.post(
        "/a2a/v1/message:send",
        headers={"Authorization": "Bearer wrong"},
        json={"message": {"role": "ROLE_USER", "parts": [{"text": "hi"}]}},
    )

    assert response.status_code == 403


def test_message_send_rejects_invalid_payload() -> None:
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                peers={"bob": A2APeerConfig(token="secret")},
            ),
            octo=_DummyOcto(),
        )
    )

    response = client.post(
        "/a2a/v1/message:send",
        headers={"Authorization": "Bearer secret"},
        json={"not_message": {}},
    )

    assert response.status_code == 400


def test_message_send_rejects_unknown_task_id_until_task_store_exists() -> None:
    octo = _DummyOcto()
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                peers={"bob": A2APeerConfig(token="secret")},
            ),
            octo=octo,
        )
    )

    response = client.post(
        "/a2a/v1/message:send",
        headers={"Authorization": "Bearer secret"},
        json={
            "message": {
                "role": "ROLE_USER",
                "taskId": "client-supplied-task",
                "parts": [{"text": "hi"}],
            }
        },
    )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "TaskNotFoundError"
    assert octo.calls == []


def test_message_send_rejects_unsupported_a2a_version() -> None:
    octo = _DummyOcto()
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                protocol_version="1.0",
                peers={"bob": A2APeerConfig(token="secret")},
            ),
            octo=octo,
        )
    )

    response = client.post(
        "/a2a/v1/message:send",
        headers={"Authorization": "Bearer secret", "A2A-Version": "2.0"},
        json={"message": {"role": "ROLE_USER", "parts": [{"text": "hi"}]}},
    )

    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "VersionNotSupportedError"
    assert octo.calls == []


def test_message_send_accepts_supported_a2a_patch_version() -> None:
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                protocol_version="1.0",
                peers={"bob": A2APeerConfig(token="secret")},
            ),
            octo=_DummyOcto(),
        )
    )

    response = client.post(
        "/a2a/v1/message:send",
        headers={"Authorization": "Bearer secret", "A2A-Version": "1.0.3"},
        json={"message": {"role": "ROLE_USER", "parts": [{"text": "hi"}]}},
    )

    assert response.status_code == 200


def test_message_send_routes_authenticated_peer_message_to_octo() -> None:
    octo = _DummyOcto()
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                peers={"bob": A2APeerConfig(name="Bob", token="secret")},
            ),
            octo=octo,
        )
    )

    response = client.post(
        "/a2a/v1/message:send",
        headers={"Authorization": "Bearer secret"},
        json={"message": {"role": "ROLE_USER", "parts": [{"text": "hi Alice"}]}},
    )

    assert response.status_code == 200
    payload = response.json()
    wrapped_render = render_tool_result_for_llm(
        json.dumps({"status_code": response.status_code, "json": payload}),
        tool_name="http_post",
    )
    assert payload["taskState"] == "TASK_STATE_COMPLETED"
    assert payload["replyText"] == "hello peer"
    assert payload["task"]["status"]["state"] == "TASK_STATE_COMPLETED"
    assert payload["task"]["status"]["message"]["parts"] == [{"text": "hello peer"}]
    assert "hello peer" in wrapped_render.text
    assert str(payload["task"]["contextId"]).startswith("a2a-context-")
    assert payload["task"]["metadata"]["octopalPeerId"] == "bob"
    assert len(octo.calls) == 1
    call = octo.calls[0]
    assert "Peer ID: bob" in str(call["text"])
    assert f"A2A context ID: {payload['task']['contextId']}" in str(call["text"])
    assert "hi Alice" in str(call["text"])
    assert "Do not call `a2a_send_message` back to this same peer" in str(call["text"])
    assert call["chat_id"] > 0
    assert call["correlation_id"] == payload["task"]["id"]
    assert call["kwargs"]["is_ws"] is True
    assert call["kwargs"]["include_wakeup"] is False
    assert octo.suppressed_channel_followups == [
        {"correlation_id": payload["task"]["id"], "reason": "a2a_peer_message"}
    ]


def test_message_send_deduplicates_same_peer_message_id() -> None:
    octo = _DummyOcto()
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                peers={"bob": A2APeerConfig(name="Bob", token="secret")},
            ),
            octo=octo,
        )
    )
    body = {
        "message": {
            "role": "ROLE_USER",
            "messageId": "peer-message-1",
            "contextId": "ctx-dedupe",
            "parts": [{"text": "hi Alice"}],
        }
    }
    headers = {"Authorization": "Bearer secret"}

    first = client.post("/a2a/v1/message:send", headers=headers, json=body)
    second = client.post("/a2a/v1/message:send", headers=headers, json=body)

    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    assert len(octo.calls) == 1
    assert len(octo.suppressed_channel_followups) == 1


def test_message_send_routes_data_and_file_url_parts_to_octo_prompt() -> None:
    octo = _DummyOcto()
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                peers={
                    "bob": A2APeerConfig(
                        name="Bob",
                        token="secret",
                        capabilities=["chat", "data", "files:url"],
                    )
                },
            ),
            octo=octo,
        )
    )

    response = client.post(
        "/a2a/v1/message:send",
        headers={"Authorization": "Bearer secret"},
        json={
            "message": {
                "role": "ROLE_USER",
                "parts": [
                    {"text": "please inspect this"},
                    {"data": {"intent": "summarize", "priority": 2}},
                    {
                        "url": "https://example.test/report.pdf",
                        "filename": "report.pdf",
                        "mediaType": "application/pdf",
                    },
                ],
            }
        },
    )

    assert response.status_code == 200
    call_text = str(octo.calls[0]["text"])
    assert "please inspect this" in call_text
    assert "Structured data part 2" in call_text
    assert '"intent": "summarize"' in call_text
    assert "File URL part 3" in call_text
    assert "https://example.test/report.pdf" in call_text


def test_message_send_rejects_rich_parts_without_peer_capability() -> None:
    octo = _DummyOcto()
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                peers={"bob": A2APeerConfig(name="Bob", token="secret")},
            ),
            octo=octo,
        )
    )

    response = client.post(
        "/a2a/v1/message:send",
        headers={"Authorization": "Bearer secret"},
        json={
            "message": {
                "role": "ROLE_USER",
                "parts": [
                    {"text": "please inspect this"},
                    {"data": {"intent": "summarize"}},
                ],
            }
        },
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "A2A peer lacks 'data' capability"
    assert octo.calls == []


def test_message_send_saves_raw_part_as_attachment(tmp_path: Path) -> None:
    octo = _DummyOcto()
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                peers={
                    "bob": A2APeerConfig(
                        name="Bob",
                        token="secret",
                        capabilities=["chat", "files:raw"],
                    )
                },
            ),
            octo=octo,
            state_dir=tmp_path,
        )
    )
    encoded = base64.b64encode(b"hello from peer file").decode("ascii")

    response = client.post(
        "/a2a/v1/message:send",
        headers={"Authorization": "Bearer secret"},
        json={
            "message": {
                "role": "ROLE_USER",
                "contextId": "ctx-files",
                "parts": [
                    {"text": "file attached"},
                    {"raw": encoded, "filename": "note.txt", "mediaType": "text/plain"},
                ],
            }
        },
    )

    assert response.status_code == 200
    saved_paths = octo.calls[0]["kwargs"]["saved_file_paths"]
    assert len(saved_paths) == 1
    saved_path = Path(saved_paths[0])
    assert saved_path.name == "note.txt"
    assert saved_path.read_bytes() == b"hello from peer file"
    assert tmp_path in saved_path.parents
    assert response.json()["task"]["metadata"]["octopalSavedFilePaths"] == [str(saved_path)]


def test_message_send_scopes_peer_chat_by_a2a_context() -> None:
    octo = _DummyOcto()
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                peers={"bob": A2APeerConfig(token="secret")},
            ),
            octo=octo,
        )
    )
    headers = {"Authorization": "Bearer secret"}

    first = client.post(
        "/a2a/v1/message:send",
        headers=headers,
        json={
            "message": {
                "role": "ROLE_USER",
                "contextId": "ctx-one",
                "parts": [{"text": "first"}],
            }
        },
    )
    second = client.post(
        "/a2a/v1/message:send",
        headers=headers,
        json={
            "message": {
                "role": "ROLE_USER",
                "contextId": "ctx-two",
                "parts": [{"text": "second"}],
            }
        },
    )
    third = client.post(
        "/a2a/v1/message:send",
        headers=headers,
        json={
            "message": {
                "role": "ROLE_USER",
                "contextId": "ctx-one",
                "parts": [{"text": "third"}],
            }
        },
    )

    assert first.status_code == second.status_code == third.status_code == 200
    assert len(octo.calls) == 3
    assert octo.calls[0]["chat_id"] != octo.calls[1]["chat_id"]
    assert octo.calls[0]["chat_id"] == octo.calls[2]["chat_id"]


def test_message_send_generates_fresh_context_when_absent() -> None:
    octo = _DummyOcto()
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                peers={"bob": A2APeerConfig(token="secret")},
            ),
            octo=octo,
        )
    )
    body = {"message": {"role": "ROLE_USER", "parts": [{"text": "hi"}]}}
    headers = {"Authorization": "Bearer secret"}

    first = client.post("/a2a/v1/message:send", headers=headers, json=body)
    second = client.post("/a2a/v1/message:send", headers=headers, json=body)

    assert first.status_code == second.status_code == 200
    assert first.json()["task"]["contextId"] != second.json()["task"]["contextId"]
    assert octo.calls[0]["chat_id"] != octo.calls[1]["chat_id"]


def test_message_send_does_not_mark_empty_reply_completed() -> None:
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                peers={"bob": A2APeerConfig(token="secret")},
            ),
            octo=_DummyOcto(immediate=""),
        )
    )

    response = client.post(
        "/a2a/v1/message:send",
        headers={"Authorization": "Bearer secret"},
        json={"message": {"role": "ROLE_USER", "parts": [{"text": "hi"}]}},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["task"]["status"]["state"] == "TASK_STATE_FAILED"
    assert payload["task"]["artifacts"][0]["parts"] == [{"text": ""}]


def test_message_send_enforces_payload_size() -> None:
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                max_payload_chars=3,
                peers={"bob": A2APeerConfig(token="secret")},
            ),
            octo=_DummyOcto(),
        )
    )

    response = client.post(
        "/a2a/v1/message:send",
        headers={"Authorization": "Bearer secret"},
        json={"message": {"role": "ROLE_USER", "parts": [{"text": "too long"}]}},
    )

    assert response.status_code == 413


def test_message_send_enforces_peer_rate_limit() -> None:
    client = TestClient(
        _app(
            A2AConfig(
                enabled=True,
                max_requests_per_minute=1,
                peers={"bob": A2APeerConfig(token="secret")},
            ),
            octo=_DummyOcto(),
        )
    )
    body = {"message": {"role": "ROLE_USER", "parts": [{"text": "hi"}]}}
    headers = {"Authorization": "Bearer secret"}

    assert client.post("/a2a/v1/message:send", headers=headers, json=body).status_code == 200
    response = client.post("/a2a/v1/message:send", headers=headers, json=body)

    assert response.status_code == 429


def test_a2a_list_peers_exposes_enabled_configured_peers_without_tokens() -> None:
    ctx = {
        "octo": SimpleNamespace(
            runtime=SimpleNamespace(
                settings=SimpleNamespace(
                    a2a=A2AConfig(
                        enabled=True,
                        peers={
                            "bob": A2APeerConfig(
                                name="Bob",
                                token="secret",
                                base_url="https://bob.example/a2a/v1",
                            ),
                            "off": A2APeerConfig(enabled=False, token="hidden"),
                        },
                    )
                )
            )
        )
    }

    payload = a2a_list_peers({}, ctx)

    assert '"enabled": true' in payload
    assert '"peer_id": "bob"' in payload
    assert '"Bob"' in payload
    assert "secret" not in payload
    assert '"peer_id": "off"' not in payload


def test_a2a_send_message_exposes_reply_text_above_protocol_envelope(monkeypatch) -> None:
    async def fake_send_peer_message(
        _config: A2AConfig,
        *,
        peer_id: str,
        text: str | None = None,
        data: Any = None,
        file_urls: list[dict[str, Any]] | None = None,
        raw_files: list[dict[str, Any]] | None = None,
        context_id: str | None = None,
    ) -> dict[str, Any]:
        assert data is None
        assert file_urls is None
        assert raw_files is None
        return {
            "taskState": "TASK_STATE_COMPLETED",
            "replyText": f"Readable reply to: {text}",
            "task": {
                "id": "a2a-task-1",
                "contextId": context_id or f"octopal-peer-{peer_id}",
                "status": {
                    "state": "TASK_STATE_COMPLETED",
                    "message": {
                        "role": "ROLE_AGENT",
                        "parts": [{"text": f"Readable reply to: {text}"}],
                    },
                },
                "artifacts": [
                    {
                        "name": "response",
                        "parts": [{"text": "Readable reply to: hello"}],
                    }
                ],
            },
        }

    monkeypatch.setattr(a2a_tools, "send_peer_message", fake_send_peer_message)
    ctx = {
        "octo": SimpleNamespace(
            runtime=SimpleNamespace(
                settings=SimpleNamespace(
                    a2a=A2AConfig(
                        enabled=True,
                        peers={
                            "bob": A2APeerConfig(
                                token="secret",
                                base_url="https://bob.example/a2a/v1",
                            )
                        },
                    )
                )
            )
        )
    }

    raw = asyncio.run(a2a_tools.a2a_send_message({"peer_id": "bob", "text": "hello"}, ctx))
    payload = json.loads(raw)
    rendered = render_tool_result_for_llm(raw, tool_name="a2a_send_message")

    assert payload["task_state"] == "TASK_STATE_COMPLETED"
    assert payload["reply_text"] == "Readable reply to: hello"
    assert "Readable reply to: hello" in rendered.text


def test_a2a_send_message_labels_non_transport_errors(monkeypatch) -> None:
    async def fake_send_peer_message(
        _config: A2AConfig,
        *,
        peer_id: str,
        text: str | None = None,
        data: Any = None,
        file_urls: list[dict[str, Any]] | None = None,
        raw_files: list[dict[str, Any]] | None = None,
        context_id: str | None = None,
    ) -> dict[str, Any]:
        raise A2AClientError("A2A peer 'bob' returned HTTP 400: Invalid A2A message payload")

    monkeypatch.setattr(a2a_tools, "send_peer_message", fake_send_peer_message)
    ctx = {
        "octo": SimpleNamespace(
            runtime=SimpleNamespace(
                settings=SimpleNamespace(
                    a2a=A2AConfig(
                        enabled=True,
                        peers={
                            "bob": A2APeerConfig(
                                token="secret",
                                base_url="https://bob.example/a2a/v1",
                            )
                        },
                    )
                )
            )
        )
    }

    raw = asyncio.run(a2a_tools.a2a_send_message({"peer_id": "bob", "text": "hello"}, ctx))
    payload = json.loads(raw)

    assert payload["status"] == "error"
    assert payload["ok"] is False
    assert payload["error_type"] == "validation"
    assert payload["transport_error"] is False
    assert "Do not describe the A2A bridge as down" in payload["diagnosis"]


def test_a2a_send_message_marks_upstream_transport_errors(monkeypatch) -> None:
    async def fake_send_peer_message(
        _config: A2AConfig,
        *,
        peer_id: str,
        text: str | None = None,
        data: Any = None,
        file_urls: list[dict[str, Any]] | None = None,
        raw_files: list[dict[str, Any]] | None = None,
        context_id: str | None = None,
    ) -> dict[str, Any]:
        raise A2AClientError("A2A peer 'bob' returned HTTP 503: temporarily unavailable")

    monkeypatch.setattr(a2a_tools, "send_peer_message", fake_send_peer_message)
    ctx = {
        "octo": SimpleNamespace(
            runtime=SimpleNamespace(
                settings=SimpleNamespace(
                    a2a=A2AConfig(
                        enabled=True,
                        peers={
                            "bob": A2APeerConfig(
                                token="secret",
                                base_url="https://bob.example/a2a/v1",
                            )
                        },
                    )
                )
            )
        )
    }

    raw = asyncio.run(a2a_tools.a2a_send_message({"peer_id": "bob", "text": "hello"}, ctx))
    payload = json.loads(raw)

    assert payload["error_type"] == "upstream_unavailable"
    assert payload["transport_error"] is True


def test_a2a_send_message_forwards_structured_data_and_file_urls(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    async def fake_send_peer_message(
        _config: A2AConfig,
        *,
        peer_id: str,
        text: str | None = None,
        data: Any = None,
        file_urls: list[dict[str, Any]] | None = None,
        raw_files: list[dict[str, Any]] | None = None,
        context_id: str | None = None,
    ) -> dict[str, Any]:
        captured.update(
            {
                "peer_id": peer_id,
                "text": text,
                "data": data,
                "file_urls": file_urls,
                "raw_files": raw_files,
                "context_id": context_id,
            }
        )
        return {
            "taskState": "TASK_STATE_COMPLETED",
            "replyText": "received",
            "task": {
                "id": "a2a-task-1",
                "contextId": context_id or f"octopal-peer-{peer_id}",
                "status": {
                    "state": "TASK_STATE_COMPLETED",
                    "message": {"role": "ROLE_AGENT", "parts": [{"text": "received"}]},
                },
                "artifacts": [
                    {
                        "artifactId": "artifact-1",
                        "name": "structured",
                        "parts": [{"data": {"ok": True}, "mediaType": "application/json"}],
                    }
                ],
            },
        }

    monkeypatch.setattr(a2a_tools, "send_peer_message", fake_send_peer_message)
    ctx = {
        "octo": SimpleNamespace(
            runtime=SimpleNamespace(
                settings=SimpleNamespace(
                    a2a=A2AConfig(
                        enabled=True,
                        peers={
                            "bob": A2APeerConfig(
                                token="secret",
                                base_url="https://bob.example/a2a/v1",
                                capabilities=["chat", "data", "files:url"],
                            )
                        },
                    )
                )
            )
        )
    }

    raw = asyncio.run(
        a2a_tools.a2a_send_message(
            {
                "peer_id": "bob",
                "text": "analyze",
                "data": {"intent": "compare"},
                "file_urls": [
                    {
                        "url": "https://example.test/a.csv",
                        "filename": "a.csv",
                        "media_type": "text/csv",
                    }
                ],
            },
            ctx,
        )
    )
    payload = json.loads(raw)

    assert captured["peer_id"] == "bob"
    assert captured["data"] == {"intent": "compare"}
    assert captured["file_urls"][0]["url"] == "https://example.test/a.csv"
    assert payload["reply_text"] == "received"
    assert payload["artifacts"][0]["parts"][0]["kind"] == "data"
    assert payload["artifacts"][0]["parts"][0]["data"] == {"ok": True}


def test_a2a_send_message_skips_duplicate_payload_in_same_turn(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    async def fake_send_peer_message(
        _config: A2AConfig,
        *,
        peer_id: str,
        text: str | None = None,
        data: Any = None,
        file_urls: list[dict[str, Any]] | None = None,
        raw_files: list[dict[str, Any]] | None = None,
        context_id: str | None = None,
    ) -> dict[str, Any]:
        calls.append(
            {
                "peer_id": peer_id,
                "text": text,
                "data": data,
                "file_urls": file_urls,
                "raw_files": raw_files,
                "context_id": context_id,
            }
        )
        return {"taskState": "TASK_STATE_COMPLETED", "replyText": "ok"}

    monkeypatch.setattr(a2a_tools, "send_peer_message", fake_send_peer_message)
    ctx = {
        "octo": SimpleNamespace(
            runtime=SimpleNamespace(
                settings=SimpleNamespace(
                    a2a=A2AConfig(
                        enabled=True,
                        peers={
                            "bob": A2APeerConfig(
                                token="secret",
                                base_url="https://bob.example/a2a/v1",
                            )
                        },
                    )
                )
            )
        )
    }
    args = {"peer_id": "bob", "text": "hello", "context_id": "ctx-1"}

    first = json.loads(asyncio.run(a2a_tools.a2a_send_message(args, ctx)))
    second = json.loads(asyncio.run(a2a_tools.a2a_send_message(args, ctx)))

    assert first["status"] == "ok"
    assert second["status"] == "skipped_duplicate"
    assert len(calls) == 1


def test_a2a_send_message_rejects_data_without_peer_capability() -> None:
    ctx = {
        "octo": SimpleNamespace(
            runtime=SimpleNamespace(
                settings=SimpleNamespace(
                    a2a=A2AConfig(
                        enabled=True,
                        peers={
                            "bob": A2APeerConfig(
                                token="secret",
                                base_url="https://bob.example/a2a/v1",
                            )
                        },
                    )
                )
            )
        )
    }

    raw = asyncio.run(
        a2a_tools.a2a_send_message(
            {"peer_id": "bob", "text": "analyze", "data": {"intent": "compare"}},
            ctx,
        )
    )
    payload = json.loads(raw)

    assert payload["status"] == "error"
    assert payload["error_type"] == "validation"
    assert payload["transport_error"] is False
    assert "does not allow required capabilities: data" in payload["message"]


def test_a2a_send_message_forwards_raw_files_when_allowed(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    async def fake_send_peer_message(
        _config: A2AConfig,
        *,
        peer_id: str,
        text: str | None = None,
        data: Any = None,
        file_urls: list[dict[str, Any]] | None = None,
        raw_files: list[dict[str, Any]] | None = None,
        context_id: str | None = None,
    ) -> dict[str, Any]:
        captured.update(
            {
                "peer_id": peer_id,
                "text": text,
                "raw_files": raw_files,
            }
        )
        return {"taskState": "TASK_STATE_COMPLETED", "replyText": "received"}

    monkeypatch.setattr(a2a_tools, "send_peer_message", fake_send_peer_message)
    ctx = {
        "octo": SimpleNamespace(
            runtime=SimpleNamespace(
                settings=SimpleNamespace(
                    a2a=A2AConfig(
                        enabled=True,
                        peers={
                            "bob": A2APeerConfig(
                                token="secret",
                                base_url="https://bob.example/a2a/v1",
                                capabilities=["chat", "files:raw"],
                            )
                        },
                    )
                )
            )
        )
    }

    raw = asyncio.run(
        a2a_tools.a2a_send_message(
            {
                "peer_id": "bob",
                "text": "inspect",
                "raw_files": [
                    {
                        "raw": base64.b64encode(b"hello").decode("ascii"),
                        "filename": "note.txt",
                        "media_type": "text/plain",
                    }
                ],
            },
            ctx,
        )
    )
    payload = json.loads(raw)

    assert captured["peer_id"] == "bob"
    assert captured["raw_files"][0]["filename"] == "note.txt"
    assert payload["reply_text"] == "received"


def test_message_send_endpoint_can_be_derived_from_agent_card_url() -> None:
    peer = A2APeerConfig(
        agent_card_url="https://peer.example/.well-known/agent-card.json",
        token="secret",
    )

    assert _message_send_endpoint(peer) == "https://peer.example/a2a/v1/message:send"
