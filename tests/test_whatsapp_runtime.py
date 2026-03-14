from __future__ import annotations

import asyncio
from types import SimpleNamespace

import broodmind.channels.whatsapp.runtime as whatsapp_runtime_module
from broodmind.channels.whatsapp.runtime import WhatsAppRuntime


class _FakeBridgeController:
    def __init__(self, settings) -> None:
        self.settings = settings
        self.sent: list[tuple[str, str]] = []

    def send_message(self, to: str, text: str) -> dict:
        self.sent.append((to, text))
        return {"ok": True}

    def start(self, *, callback_url: str | None = None) -> None:
        return None

    def stop(self) -> None:
        return None

    def status(self) -> dict:
        return {"connected": True}


class _FakeReply:
    def __init__(self, immediate: str) -> None:
        self.immediate = immediate


class _FakeQueen:
    def __init__(self) -> None:
        self.handled: list[tuple[str, int]] = []
        self.initialized: list[int] = []
        self.internal_send = None

    async def initialize_system(self, *, bot=None, allowed_chat_ids=None) -> None:
        self.initialized = list(allowed_chat_ids or [])

    async def handle_message(self, text: str, chat_id: int):
        self.handled.append((text, chat_id))
        return _FakeReply("hello back")

    async def stop_background_tasks(self) -> None:
        return None


def _make_settings(*, mode: str, allowed_numbers: str) -> SimpleNamespace:
    return SimpleNamespace(
        whatsapp_mode=mode,
        allowed_whatsapp_numbers=allowed_numbers,
        gateway_port=8000,
        whatsapp_callback_token="",
        whatsapp_bridge_host="127.0.0.1",
        whatsapp_bridge_port=8765,
        whatsapp_auth_dir=None,
        state_dir="data",
        workspace_dir="workspace",
        whatsapp_node_command="node",
    )


def test_whatsapp_runtime_accepts_personal_self_chat(monkeypatch) -> None:
    fake_queen = _FakeQueen()
    monkeypatch.setattr(whatsapp_runtime_module, "build_queen", lambda settings: fake_queen)
    monkeypatch.setattr(whatsapp_runtime_module, "WhatsAppBridgeController", _FakeBridgeController)
    monkeypatch.setattr(whatsapp_runtime_module, "update_component_gauges", lambda *args, **kwargs: None)
    monkeypatch.setattr(whatsapp_runtime_module, "update_last_message", lambda *args, **kwargs: None)

    runtime = WhatsAppRuntime(_make_settings(mode="personal", allowed_numbers="+15551234567"))
    runtime.attach_queen_output()

    async def scenario() -> None:
        result = await runtime.handle_inbound(
            {
                "sender": "+15551234567",
                "conversation": "+15551234567",
                "self": "+15551234567",
                "fromMe": True,
                "selfChat": True,
                "text": "hello from self",
            }
        )
        assert result["accepted"] is True
        assert fake_queen.handled
        assert runtime.bridge.sent == [("+15551234567", "hello back")]

    asyncio.run(scenario())


def test_whatsapp_runtime_ignores_from_me_outside_personal_mode(monkeypatch) -> None:
    fake_queen = _FakeQueen()
    monkeypatch.setattr(whatsapp_runtime_module, "build_queen", lambda settings: fake_queen)
    monkeypatch.setattr(whatsapp_runtime_module, "WhatsAppBridgeController", _FakeBridgeController)
    monkeypatch.setattr(whatsapp_runtime_module, "update_component_gauges", lambda *args, **kwargs: None)
    monkeypatch.setattr(whatsapp_runtime_module, "update_last_message", lambda *args, **kwargs: None)

    runtime = WhatsAppRuntime(_make_settings(mode="separate", allowed_numbers="+15551234567"))
    runtime.attach_queen_output()

    async def scenario() -> None:
        result = await runtime.handle_inbound(
            {
                "sender": "+15551234567",
                "conversation": "+15551234567",
                "self": "+15551234567",
                "fromMe": True,
                "selfChat": True,
                "text": "ignore me",
            }
        )
        assert result == {"accepted": False, "reason": "from_me_ignored"}
        assert fake_queen.handled == []
        assert runtime.bridge.sent == []

    asyncio.run(scenario())


def test_whatsapp_runtime_ignores_from_me_non_self_chat(monkeypatch) -> None:
    fake_queen = _FakeQueen()
    monkeypatch.setattr(whatsapp_runtime_module, "build_queen", lambda settings: fake_queen)
    monkeypatch.setattr(whatsapp_runtime_module, "WhatsAppBridgeController", _FakeBridgeController)
    monkeypatch.setattr(whatsapp_runtime_module, "update_component_gauges", lambda *args, **kwargs: None)
    monkeypatch.setattr(whatsapp_runtime_module, "update_last_message", lambda *args, **kwargs: None)

    runtime = WhatsAppRuntime(_make_settings(mode="personal", allowed_numbers="+15551234567"))
    runtime.attach_queen_output()

    async def scenario() -> None:
        result = await runtime.handle_inbound(
            {
                "sender": "+15551234567",
                "conversation": "+447700900123",
                "self": "+15551234567",
                "fromMe": True,
                "selfChat": False,
                "text": "should ignore",
            }
        )
        assert result == {"accepted": False, "reason": "not_self_chat"}
        assert fake_queen.handled == []
        assert runtime.bridge.sent == []

    asyncio.run(scenario())
