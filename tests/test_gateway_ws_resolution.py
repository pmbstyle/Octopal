from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from octopal.gateway.ws import (
    WsApprovalManager,
    _ActiveWsSession,
    _build_ws_file_payload,
    _handle_message,
    _resolve_ws_chat_id,
    register_ws_routes,
)
from octopal.runtime.octo.core import OctoReply


def test_resolve_ws_chat_id_returns_positive_when_no_allowlist() -> None:
    settings = SimpleNamespace(allowed_telegram_chat_ids="")
    assert _resolve_ws_chat_id(settings) > 0


def test_resolve_ws_chat_id_uses_first_allowed_id_when_valid() -> None:
    settings = SimpleNamespace(allowed_telegram_chat_ids="42,100")
    assert _resolve_ws_chat_id(settings) == 42


def test_new_websocket_connection_takes_over_previous_session() -> None:
    class DummyOcto:
        def __init__(self) -> None:
            self.owner: str | None = None

        def set_output_channel(self, is_ws: bool, **kwargs) -> bool:
            owner_id = kwargs.get("owner_id")
            force = bool(kwargs.get("force"))
            if is_ws:
                if self.owner and owner_id and self.owner != owner_id and not force:
                    return False
                self.owner = owner_id
                return True
            if self.owner and owner_id and self.owner != owner_id and not force:
                return False
            self.owner = None
            return True

    app = FastAPI()
    app.state.settings = SimpleNamespace(tailscale_ips="testclient", allowed_telegram_chat_ids="")
    app.state.octo = DummyOcto()
    register_ws_routes(app)

    with TestClient(app) as client, client.websocket_connect("/ws") as ws_one:
        assert ws_one.receive_json() == {"type": "workers_snapshot", "workers": []}
        with client.websocket_connect("/ws") as ws_two:
            assert ws_two.receive_json() == {"type": "workers_snapshot", "workers": []}
            payload = ws_one.receive_json()
            assert payload["type"] == "warning"
            assert "took over" in payload["message"]
            ws_two.send_json({"type": "ping"})
            assert ws_two.receive_json() == {"type": "pong"}


def test_build_ws_file_payload_includes_base64_metadata(tmp_path: Path) -> None:
    file_path = tmp_path / "report.txt"
    file_path.write_text("hello ws", encoding="utf-8")

    payload = _build_ws_file_payload(str(file_path), caption="Attached")

    assert payload["name"] == "report.txt"
    assert payload["mime_type"] == "text/plain"
    assert payload["encoding"] == "base64"
    assert payload["caption"] == "Attached"
    assert payload["path"] == str(file_path.resolve())


def test_websocket_message_is_delivered_as_client_expects() -> None:
    class DummyStore:
        def get_active_workers(self):
            return []

    class DummyOcto:
        def __init__(self) -> None:
            self.owner: str | None = None
            self.store = DummyStore()

        def set_output_channel(self, is_ws: bool, **kwargs) -> bool:
            owner_id = kwargs.get("owner_id")
            force = bool(kwargs.get("force"))
            if is_ws:
                if self.owner and owner_id and self.owner != owner_id and not force:
                    return False
                self.owner = owner_id
                return True
            if self.owner and owner_id and self.owner != owner_id and not force:
                return False
            self.owner = None
            return True

        async def handle_message(self, text: str, chat_id: int, **kwargs) -> OctoReply:
            assert text == "hello"
            assert chat_id > 0
            return OctoReply(immediate="Hi from Octo", followup=None, followup_required=False)

    app = FastAPI()
    app.state.settings = SimpleNamespace(tailscale_ips="testclient", allowed_telegram_chat_ids="")
    app.state.octo = DummyOcto()
    register_ws_routes(app)

    with TestClient(app) as client, client.websocket_connect("/ws") as ws:
        assert ws.receive_json() == {"type": "workers_snapshot", "workers": []}
        ws.send_json({"type": "message", "text": "hello"})
        assert ws.receive_json() == {"type": "message", "text": "Hi from Octo"}


@pytest.mark.asyncio
async def test_websocket_message_handler_serializes_octo_turns() -> None:
    class FakeSocket:
        def __init__(self) -> None:
            self.sent: list[dict] = []

        async def send_json(self, payload: dict) -> None:
            self.sent.append(payload)

    class DummyOcto:
        def __init__(self) -> None:
            self.active = 0
            self.max_active = 0

        async def handle_message(self, text: str, chat_id: int, **kwargs) -> OctoReply:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            await asyncio.sleep(0.01)
            self.active -= 1
            return OctoReply(immediate=f"reply:{text}", followup=None, followup_required=False)

    socket = FakeSocket()
    session = _ActiveWsSession(connection_id="test", socket=socket)  # type: ignore[arg-type]
    approvals = WsApprovalManager(send=lambda payload: None)
    octo = DummyOcto()
    lock = asyncio.Lock()

    await asyncio.gather(
        _handle_message(session, octo, approvals, {"type": "message", "text": "one"}, 42, lock),
        _handle_message(session, octo, approvals, {"type": "message", "text": "two"}, 42, lock),
    )

    assert octo.max_active == 1
    assert [payload["text"] for payload in socket.sent] == ["reply:one", "reply:two"]
