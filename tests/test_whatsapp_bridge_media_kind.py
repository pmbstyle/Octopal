from __future__ import annotations

from types import SimpleNamespace

from octopal.channels.whatsapp.bridge import WhatsAppBridgeController


def _make_settings() -> SimpleNamespace:
    return SimpleNamespace(
        whatsapp_bridge_host="127.0.0.1",
        whatsapp_bridge_port=8765,
        whatsapp_auth_dir=None,
        state_dir="data",
        whatsapp_callback_token="",
        whatsapp_node_command="node",
    )


def test_detect_media_kind_recognizes_images() -> None:
    assert WhatsAppBridgeController._detect_media_kind("image.png") == "image"
    assert WhatsAppBridgeController._detect_media_kind("photo.jpeg") == "image"
    assert WhatsAppBridgeController._detect_media_kind("clip.mp4") == "video"
    assert WhatsAppBridgeController._detect_media_kind("sound.mp3") == "audio"
    assert WhatsAppBridgeController._detect_media_kind("loop.gif") == "document"
    assert WhatsAppBridgeController._detect_media_kind("report.pdf") == "document"


def test_send_file_includes_media_kind(monkeypatch) -> None:
    controller = WhatsAppBridgeController(_make_settings())
    captured: dict[str, object] = {}

    def fake_request(method: str, path: str, json=None):
        captured["method"] = method
        captured["path"] = path
        captured["json"] = json
        return {"ok": True}

    monkeypatch.setattr(controller, "_request", fake_request)

    controller.send_file("+15551234567", "C:/tmp/image.png", caption="Preview")

    assert captured == {
        "method": "POST",
        "path": "/send-file",
        "json": {
            "to": "+15551234567",
            "path": "C:/tmp/image.png",
            "caption": "Preview",
            "kind": "image",
        },
    }
