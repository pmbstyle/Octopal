from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from octopal.channels.whatsapp.routes import register_whatsapp_routes


class _FakeRuntime:
    def __init__(self) -> None:
        self.settings = type("Settings", (), {"whatsapp_callback_token": "secret"})()

    async def handle_inbound(self, payload):
        return {"accepted": True, "echo": payload["text"]}


def test_whatsapp_inbound_route_rejects_bad_token() -> None:
    app = FastAPI()
    app.state.whatsapp_runtime = _FakeRuntime()
    register_whatsapp_routes(app)

    client = TestClient(app)
    response = client.post("/api/channels/whatsapp/inbound", json={"sender": "+1", "text": "hi"})

    assert response.status_code == 403


def test_whatsapp_inbound_route_accepts_valid_token() -> None:
    app = FastAPI()
    app.state.whatsapp_runtime = _FakeRuntime()
    register_whatsapp_routes(app)

    client = TestClient(app)
    response = client.post(
        "/api/channels/whatsapp/inbound",
        headers={"x-octopal-whatsapp-token": "secret"},
        json={"sender": "+1", "text": "hi"},
    )

    assert response.status_code == 200
    assert response.json() == {"accepted": True, "echo": "hi"}


def test_whatsapp_inbound_route_rejects_missing_configured_token() -> None:
    app = FastAPI()
    runtime = _FakeRuntime()
    runtime.settings.whatsapp_callback_token = ""
    app.state.whatsapp_runtime = runtime
    register_whatsapp_routes(app)

    client = TestClient(app)
    response = client.post(
        "/api/channels/whatsapp/inbound",
        headers={"x-octopal-whatsapp-token": "anything"},
        json={"sender": "+1", "text": "hi"},
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "WhatsApp callback token is not configured"}
