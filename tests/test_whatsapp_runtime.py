from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import broodmind.channels.whatsapp.runtime as whatsapp_runtime_module
from broodmind.channels.whatsapp.runtime import WhatsAppRuntime


class _FakeBridgeController:
    def __init__(self, settings) -> None:
        self.settings = settings
        self.sent: list[tuple[str, str]] = []
        self.reactions: list[dict] = []

    def send_message(self, to: str, text: str) -> dict:
        self.sent.append((to, text))
        return {"ok": True}

    def send_reaction(
        self,
        to: str,
        emoji: str,
        *,
        message_id: str,
        remote_jid: str | None = None,
        target_from_me: bool = False,
    ) -> dict:
        self.reactions.append(
            {
                "to": to,
                "emoji": emoji,
                "message_id": message_id,
                "remote_jid": remote_jid,
                "target_from_me": target_from_me,
            }
        )
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
        self.handled: list[dict] = []
        self.initialized: list[int] = []
        self.internal_send = None

    async def initialize_system(self, *, bot=None, allowed_chat_ids=None) -> None:
        self.initialized = list(allowed_chat_ids or [])

    async def handle_message(self, text: str, chat_id: int, **kwargs):
        self.handled.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
        return _FakeReply("hello back")

    async def stop_background_tasks(self) -> None:
        return None


def _make_settings(*, mode: str, allowed_numbers: str) -> SimpleNamespace:
    return SimpleNamespace(
        whatsapp_mode=mode,
        allowed_whatsapp_numbers=allowed_numbers,
        user_message_grace_seconds=0.0,
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


def test_whatsapp_runtime_accepts_image_only_payload_and_saves_path(monkeypatch, tmp_path) -> None:
    fake_queen = _FakeQueen()
    monkeypatch.setattr(whatsapp_runtime_module, "build_queen", lambda settings: fake_queen)
    monkeypatch.setattr(whatsapp_runtime_module, "WhatsAppBridgeController", _FakeBridgeController)
    monkeypatch.setattr(whatsapp_runtime_module, "update_component_gauges", lambda *args, **kwargs: None)
    monkeypatch.setattr(whatsapp_runtime_module, "update_last_message", lambda *args, **kwargs: None)

    settings = _make_settings(mode="personal", allowed_numbers="+15551234567")
    settings.workspace_dir = tmp_path
    runtime = WhatsAppRuntime(settings)
    runtime.attach_queen_output()

    async def scenario() -> None:
        result = await runtime.handle_inbound(
            {
                "sender": "+15551234567",
                "conversation": "+15551234567",
                "self": "+15551234567",
                "fromMe": True,
                "selfChat": True,
                "text": "",
                "imageMimeType": "image/jpeg",
                "imageDataUrl": "data:image/jpeg;base64,SGVsbG8=",
            }
        )
        assert result["accepted"] is True
        handled = fake_queen.handled[-1]
        assert handled["text"] == ""
        assert handled["kwargs"]["images"] == ["data:image/jpeg;base64,SGVsbG8="]
        saved_paths = handled["kwargs"]["saved_file_paths"]
        assert len(saved_paths) == 1
        assert saved_paths[0].endswith(".jpg")
        assert Path(saved_paths[0]).is_file()
        assert str(tmp_path) in saved_paths[0]

    asyncio.run(scenario())


def test_whatsapp_runtime_aggregates_messages_within_grace_window(monkeypatch) -> None:
    fake_queen = _FakeQueen()
    monkeypatch.setattr(whatsapp_runtime_module, "build_queen", lambda settings: fake_queen)
    monkeypatch.setattr(whatsapp_runtime_module, "WhatsAppBridgeController", _FakeBridgeController)
    monkeypatch.setattr(whatsapp_runtime_module, "update_component_gauges", lambda *args, **kwargs: None)
    monkeypatch.setattr(whatsapp_runtime_module, "update_last_message", lambda *args, **kwargs: None)

    settings = _make_settings(mode="personal", allowed_numbers="+15551234567")
    settings.user_message_grace_seconds = 0.05
    runtime = WhatsAppRuntime(settings)
    runtime.attach_queen_output()

    async def scenario() -> None:
        first = await runtime.handle_inbound(
            {
                "sender": "+15551234567",
                "conversation": "+15551234567",
                "self": "+15551234567",
                "fromMe": True,
                "selfChat": True,
                "text": "hello",
            }
        )
        second = await runtime.handle_inbound(
            {
                "sender": "+15551234567",
                "conversation": "+15551234567",
                "self": "+15551234567",
                "fromMe": True,
                "selfChat": True,
                "text": "and another thing",
            }
        )
        assert first["accepted"] is True
        assert second["accepted"] is True
        assert fake_queen.handled == []

        await asyncio.sleep(0.12)

        assert len(fake_queen.handled) == 1
        assert fake_queen.handled[0]["text"] == "hello\n\nand another thing"

    asyncio.run(scenario())


def test_whatsapp_runtime_applies_reaction_and_strips_tag(monkeypatch) -> None:
    fake_queen = _FakeQueen()

    async def _handle_message(text: str, chat_id: int, **kwargs):
        fake_queen.handled.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
        return _FakeReply("<react>✅</react> All done.")

    fake_queen.handle_message = _handle_message  # type: ignore[method-assign]

    monkeypatch.setattr(whatsapp_runtime_module, "build_queen", lambda settings: fake_queen)
    monkeypatch.setattr(whatsapp_runtime_module, "WhatsAppBridgeController", _FakeBridgeController)
    monkeypatch.setattr(whatsapp_runtime_module, "update_component_gauges", lambda *args, **kwargs: None)
    monkeypatch.setattr(whatsapp_runtime_module, "update_last_message", lambda *args, **kwargs: None)

    runtime = WhatsAppRuntime(_make_settings(mode="personal", allowed_numbers="+15551234567"))
    runtime.attach_queen_output()
    chat_id = whatsapp_runtime_module.whatsapp_chat_id("+15551234567")
    runtime._number_by_chat_id[chat_id] = "+15551234567"

    async def scenario() -> None:
        await runtime._flush_pending_turn(
            chat_id,
            "hello from self",
            [],
            [],
            {
                "message_id": "wamid-1",
                "remote_jid": "15551234567@s.whatsapp.net",
                "target_from_me": True,
            },
        )
        assert runtime.bridge.reactions == [
            {
                "to": "+15551234567",
                "emoji": "🤔",
                "message_id": "wamid-1",
                "remote_jid": "15551234567@s.whatsapp.net",
                "target_from_me": True,
            },
            {
                "to": "+15551234567",
                "emoji": "👍",
                "message_id": "wamid-1",
                "remote_jid": "15551234567@s.whatsapp.net",
                "target_from_me": True,
            }
        ]
        assert runtime.bridge.sent == [("+15551234567", "All done.")]

    asyncio.run(scenario())
