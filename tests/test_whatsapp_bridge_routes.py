from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from broodmind.channels.whatsapp.routes import register_whatsapp_routes


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
        headers={"x-broodmind-whatsapp-token": "secret"},
        json={"sender": "+1", "text": "hi"},
    )

    assert response.status_code == 200
    assert response.json() == {"accepted": True, "echo": "hi"}
