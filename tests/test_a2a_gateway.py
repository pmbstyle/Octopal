from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from octopal.gateway.app import build_app
from octopal.infrastructure.config.models import A2AConfig, A2APeerConfig
from octopal.infrastructure.config.settings import Settings
from octopal.interop.a2a.client import _message_send_endpoint
from octopal.interop.a2a.routes import register_a2a_routes
from octopal.tools.communication.a2a import a2a_list_peers


class _DummyOcto:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def handle_message(self, text: str, chat_id: int, **kwargs):
        self.calls.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
        return SimpleNamespace(immediate="hello peer")


def _app(config: A2AConfig, octo: object | None = None) -> FastAPI:
    app = FastAPI()
    app.state.settings = SimpleNamespace(a2a=config)
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
    assert payload["task"]["status"]["state"] == "TASK_STATE_COMPLETED"
    assert payload["task"]["status"]["message"]["parts"] == [{"text": "hello peer"}]
    assert payload["task"]["contextId"] == "octopal-peer-bob"
    assert payload["task"]["metadata"]["octopalPeerId"] == "bob"
    assert len(octo.calls) == 1
    call = octo.calls[0]
    assert "Peer ID: bob" in str(call["text"])
    assert "hi Alice" in str(call["text"])
    assert "Do not call `a2a_send_message` back to this same peer" in str(call["text"])
    assert call["chat_id"] > 0
    assert call["kwargs"]["is_ws"] is True
    assert call["kwargs"]["include_wakeup"] is False


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


def test_message_send_endpoint_can_be_derived_from_agent_card_url() -> None:
    peer = A2APeerConfig(
        agent_card_url="https://peer.example/.well-known/agent-card.json",
        token="secret",
    )

    assert _message_send_endpoint(peer) == "https://peer.example/a2a/v1/message:send"
